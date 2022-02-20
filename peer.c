#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#define __USE_XOPEN
#include <time.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>

#define POLLING_TIME 1 //Tempo di attesa
#define CMD_LEN 6      //Massima lunghezza di un comando
#define DATE_LEN 11    //Lunghezza data in formato dd:mm:yyyy

/*Data di inizio del conteggio dei casi di covid della rete*/
#define FIRST_DAY 1
#define FIRST_MONTH 1
#define FIRST_YEAR 2020

#define CLOSURE_TIME 18 //Orario di chiusura dei register
#define BUF_LEN 4096    //Lunghezza buffer

int main(int argc, char *argv[])
{
    int sdUDP, listener, sdTCP, new_sd; //Descrittori di socket
    int ret, sdmax, i, j;               //Variabili di comodo
    int DS_port, requester, peer_port;  //Numeri di porta usati in varie funzioni
    int my_port = atoi(argv[1]);        //Numero di porta del peer
    unsigned int len;                   //Variabile di lunghezza
    int started = 0;                    //Variabile di controllo del boot del peer
    int sent = 0;                       //Variabile di controllo di invio messaggio

    /*Numero di porta dei vicini*/
    int prev_neighbor = 0;
    int next_neighbor = 0;

    struct sockaddr_in my_addr, srv_addr, peer_addr; //Strutture di indirizzo dei socket

    /*Buffer usati in varie funzioni*/
    char buffer[BUF_LEN*4];
    char missing_data[BUF_LEN*3];
    char peer_list[BUF_LEN];
    char cmd[CMD_LEN];
    char c[2];

    char DS_addr[16]; //Ip del DS
    uint32_t lmsg;    //Variabile per invio e ricezione delle lunghezza dei messaggi

    /*Insieme di descrittori*/
    fd_set master;
    fd_set read_fds;

    time_t rawtime, start_t, end_t, temp_t; //Variabili per la gestione delle date

    /*Strutture per la gestione delle date*/
    struct tm *timeinfo;
    struct tm time1 = {0};
    struct tm time2 = {0};
    struct tm temp_time = {0};

    /*Variabili contenenti i dati necessari per la gestione delle entries*/
    char *res;
    char date1[DATE_LEN], date2[DATE_LEN];
    char temp_date[2 * DATE_LEN], date[2 * DATE_LEN];
    char timestamp[6];
    int qty, sum1, sum2;
    int cases, swabs;
    int hour;
    char type, aggr, temp_type, temp_aggr;
    char calc_data[BUF_LEN];

    /*Variabili per la gestione dei file*/
    FILE *fptr;
    DIR *dirptr;
    struct dirent *in_file;

    sdUDP = socket(AF_INET, SOCK_DGRAM, 0);     //Socket UDP per connettersi al DS
    listener = socket(AF_INET, SOCK_STREAM, 0); //Socket di ascolto

    /*Azzeramento insiemi di descrittori*/
    FD_ZERO(&master);
    FD_ZERO(&read_fds);

    FD_SET(STDIN_FILENO, &master); //Inserimento di stdin nell'insieme dei descrittori master
    sdmax = listener;

    printf("***************************** PEER COVID STARTED *****************************\n");
    printf("Digita un comando:\n");
    printf("1) start <DS_addr:DS_port> --> Connette il peer al DS\n");
    printf("2) add <type> <quantity> --> Aggiunge al register corrente l'evento\n");
    printf("    type con quantità quantity.\n");
    printf("    Type --> T: Tampone     C: Caso\n");
    printf("3) get <aggr> <type> [period] --> Richiede il dato aggregato aggr\n");
    printf("    di tipo type nel lasso di tempo period\n");
    printf("    Aggr --> T: Totale      V: Variazione\n");
    printf("    Type --> T: Tampone     C: Caso\n");
    printf("    Period ha il formato dd:mm:yyyy-dd:mm:yyyy\n");
    printf("    Period può essere omesso o una delle date può essere '*'.\n");
    printf("    In tal caso la data o le date omesse indicano la mancanza di vincoli\n");
    printf("4) stop --> Chiude il peer dopo aver mandato i propri dati ai vicini\n");
    printf("\n");

    while (1)
    {
        read_fds = master;
        select(sdmax + 1, &read_fds, NULL, NULL, NULL); //Selezione dei socket pronti in lettura
        for (i = 0; i <= sdmax; i++)
        {
            if (FD_ISSET(i, &read_fds))
            {
                if (i == listener) //Se è pronto il socket di ascolto
                {
                    len = sizeof(peer_addr);
                    new_sd = accept(listener, (struct sockaddr *)&peer_addr, &len); //Accettazione connessione
                    FD_SET(new_sd, &master);

                    //Inserimento socket di comunicazione nell'insieme dei descrittori
                    /*Aggiornamento di sdmax*/
                    if (new_sd > sdmax)
                        sdmax = new_sd;
                }
                else if (i == STDIN_FILENO) //Se viene ricevuto un comando da stdin
                {
                    /*Controllo del comando*/
                    fgets(buffer, BUF_LEN, stdin);
                    sscanf(buffer, "%s", cmd);

                    if (strcmp(cmd, "start") == 0) //Se è il comando start
                    {
                        sscanf(buffer, "%s %[^:\n]:%d", cmd, DS_addr, &DS_port); //Salvataggio IP e porta del DS

                        /*Creazione indirizzo del DS*/
                        memset(&srv_addr, 0, sizeof(srv_addr));
                        srv_addr.sin_family = AF_INET;
                        srv_addr.sin_port = htons(DS_port);
                        inet_pton(AF_INET, DS_addr, &srv_addr.sin_addr);

                        /*Creazione indirizzo del peer*/
                        memset(&my_addr, 0, sizeof(my_addr));
                        my_addr.sin_family = AF_INET;
                        my_addr.sin_port = htons(my_port);
                        inet_pton(AF_INET, DS_addr, &my_addr.sin_addr);

                        /*Collegamento socket di ascolto all'indirizzo del peer*/
                        ret = bind(listener, (struct sockaddr *)&my_addr, sizeof(my_addr));
                        if (ret < 0)
                        {
                            printf("Errore bind TCP: \n");
                            exit(-1);
                        }

                        /*Ricezione richieste di ascolto*/
                        ret = listen(listener, 10);
                        if (ret < 0)
                        {
                            printf("Errore listen\n");
                            exit(-1);
                        }

                        FD_SET(listener, &master); //Inserimento del listener nell'insieme di descrittori

                        /*Connessione al DS*/
                        len = sizeof(srv_addr);
                        sprintf(buffer, "%s %s:%d", cmd, DS_addr, my_port);
                        printf("Richiesta di connessione al DS...\n");
                        ret = sendto(sdUDP, buffer, sizeof(buffer), 0,
                                     (struct sockaddr *)&srv_addr, sizeof(srv_addr));
                        if (ret < 0)
                        {
                            printf("Start fallita\n");
                            exit(0);
                        }

                        /*Attesa risposta*/
                        for (j = 0; j < 5; j++)
                        {
                            ret = recvfrom(sdUDP, buffer, sizeof(buffer), MSG_DONTWAIT,
                                           (struct sockaddr *)&srv_addr, &len);
                            if (ret < 0)
                            {
                                printf("Aspetto risposta...\n");
                                sleep(POLLING_TIME);
                            }
                            else
                            {
                                printf("%s\n", buffer);
                                break;
                            }
                        }
                        if (j == 5)
                        { //Se non si riceve risposta dal DS
                            printf("Start fallita\n");
                            exit(0);
                        }
                        else
                        {
                            /*Creazione cartella del peer*/
                            started = 1;
                            sprintf(buffer, "./%d", my_port);
                            ret = mkdir(buffer, 0777);
                            if (ret != 0 && errno != EEXIST)
                                printf("Errore creazione cartella\n");
                        }
                        close(sdUDP); //Chiusura socket UDP
                    }
                    else if (!started) //Se il primo comando eseguito non è start
                        printf("Eseguire il comando start prima di effettuare operazioni\n");
                    else if (strcmp(cmd, "add") == 0) //Se il comando è add
                    {
                        /*Controllo correttezza di tipo e quantità*/
                        sscanf(buffer, "%s %c %d", cmd, &type, &qty);
                        if ((type == 'N' || type == 'T') && qty > 0)
                        {
                            /*Gestione della data*/
                            time(&rawtime);
                            timeinfo = localtime(&rawtime);
                            hour = timeinfo->tm_hour;
                            if (hour >= CLOSURE_TIME)
                                timeinfo->tm_mday++;
                            temp_time.tm_mday = timeinfo->tm_mday;
                            temp_time.tm_mon = timeinfo->tm_mon;
                            temp_time.tm_year = timeinfo->tm_year;
                            temp_time.tm_hour = timeinfo->tm_hour;
                            temp_time.tm_min = timeinfo->tm_min;
                            mktime(&temp_time);

                            /*Apertura del register odierno del peer*/
                            strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                            sprintf(buffer, "./%d/%s", my_port, date1);
                            ret = mkdir(buffer, 0777);       //Creazione cartella della data odierna
                            if (ret == 0 || errno == EEXIST) //Se la cartella è creata correttamente o esiste già
                            {
                                /*Apertura register*/
                                sprintf(buffer, "./%d/%s/%d.txt", my_port, date1, my_port);
                                fptr = fopen(buffer, "a");
                                if (fptr != NULL)
                                {
                                    /*Inserimento della entry*/
                                    strftime(timestamp, 6, "%H:%M", &temp_time);
                                    fprintf(fptr, "%s %c %d\n", timestamp, type, qty);
                                    printf("Entry aggiunta\n");
                                    fclose(fptr);
                                }
                                else
                                    printf("Errore apertura file\n"); //Gestione errore fopen
                            }
                            else
                                printf("Errore creazione cartella\n"); //Gestione errore creazione cartella
                        }
                        else
                            printf("Tipo o quantità errati\n"); //Gestione tipo o quantità errati
                    }
                    else if (strcmp(cmd, "get") == 0) //Se il comando è get
                    {
                        /*Controllo correttezza formato comando*/
                        sscanf(buffer, "%s %c %c  %[^-\n]-%s", cmd, &aggr, &type, date1, date2);
                        if ((aggr == 'T' || aggr == 'V') && (type == 'N' || type == 'T'))
                        {
                            if (strlen(buffer) == 8) //Se data non specificata
                            {
                                /*Data iniziale settata alla data del primo register della rete*/
                                time1.tm_mday = FIRST_DAY;
                                time1.tm_mon = FIRST_MONTH - 1;
                                time1.tm_year = FIRST_YEAR - 1900;

                                /*Data finale settata alla data dell'ultimo register chiuso*/
                                time(&rawtime);
                                timeinfo = localtime(&rawtime);
                                hour = timeinfo->tm_hour;
                                if (hour < CLOSURE_TIME)
                                    timeinfo->tm_mday--;
                                time2.tm_mday = timeinfo->tm_mday;
                                time2.tm_mon = timeinfo->tm_mon;
                                time2.tm_year = timeinfo->tm_year;
                            }
                            else if (strcmp(date1, "*") == 0 && strcmp(date2, "*") != 0) //Se data iniziale = *
                            {
                                /*Data iniziale settata alla data del primo register della rete*/
                                time1.tm_mday = FIRST_DAY;
                                time1.tm_mon = FIRST_MONTH - 1;
                                time1.tm_year = FIRST_YEAR - 1900;

                                /*Data finale settata alla data inserita*/
                                strptime(date2, "%d:%m:%Y", &time2);
                            }
                            else if (strcmp(date2, "*") == 0 && strcmp(date1, "*") != 0) //Se data finale = *
                            {
                                /*Data finale settata alla data dell'ultimo register chiuso*/
                                time(&rawtime);
                                timeinfo = localtime(&rawtime);
                                hour = timeinfo->tm_hour;
                                if (hour < CLOSURE_TIME)
                                    timeinfo->tm_mday--;
                                time2.tm_mday = timeinfo->tm_mday;
                                time2.tm_mon = timeinfo->tm_mon;
                                time2.tm_year = timeinfo->tm_year;

                                /*Data iniziale settata alla data inserita*/
                                strptime(date1, "%d:%m:%Y", &time1);
                            }
                            else //Se sono inserite entrambe le date
                            {
                                /*Date settate alle date inserite*/
                                strptime(date1, "%d:%m:%Y", &time1);
                                strptime(date2, "%d:%m:%Y", &time2);
                            }
                            /*Controllo validità data*/
                            start_t = mktime(&time1);
                            end_t = mktime(&time2);
                            time(&rawtime);
                            timeinfo = localtime(&rawtime);
                            if (hour < CLOSURE_TIME)
                                timeinfo->tm_mday--;
                            temp_time.tm_mday = timeinfo->tm_mday;
                            temp_time.tm_mon = timeinfo->tm_mon;
                            temp_time.tm_year = timeinfo->tm_year;
                            temp_t = mktime(&temp_time);
                            strftime(date1, DATE_LEN, "%d:%m:%Y", &time1);

                            /*Se data iniziale < data finale e data finale < data ultimo registro chiuso oppure data iniziale = dafa finale e aggr = totale*/
                            if ((difftime(end_t, start_t) > 0 && difftime(temp_t, end_t) >= 0) || (difftime(end_t, start_t) == 0 && aggr == 'T'))
                            {
                                /*Se data iniziale < data apertura primo registro allora data iniziale = data apertura primo registro*/
                                temp_time.tm_mday = FIRST_DAY;
                                temp_time.tm_mon = FIRST_MONTH - 1;
                                temp_time.tm_year = FIRST_YEAR - 1900;
                                temp_t = mktime(&temp_time);
                                if (difftime(temp_t, start_t) > 0)
                                {
                                    time1 = temp_time;
                                    start_t = mktime(&temp_time);
                                }
                                /*Memorizzazione intervallo date richiesto*/
                                sprintf(date, "%02d:%02d:%02d-%02d:%02d:%02d", time1.tm_mday,
                                        time1.tm_mon + 1, time1.tm_year + 1900, time2.tm_mday,
                                        time2.tm_mon + 1, time2.tm_year + 1900);
                                printf("Ricerca periodo %s\n", date);

                                /*Apertura file dati già calcolati*/
                                sprintf(buffer, "./%d/Calc_data.txt", my_port);
                                strcpy(calc_data, "\0");
                                if (access(buffer, F_OK) != 0)
                                {
                                    fptr = fopen(buffer, "w");
                                    if (fptr != NULL)
                                        fclose(fptr);
                                    else
                                        printf("Errore apertura file\n");
                                }
                                fptr = fopen(buffer, "r");
                                if (fptr != NULL)
                                {
                                    /*Controllo se il dato è già stato calcolato dal peer*/
                                    while (1)
                                    {
                                        res = fgets(buffer, BUF_LEN, fptr);
                                        if (res == NULL)
                                            break;
                                        sscanf(buffer, "%c %c %s %s", &temp_aggr,
                                               &temp_type, temp_date, calc_data);
                                        if (temp_aggr == aggr && temp_type == type &&
                                            strcmp(temp_date, date) == 0)
                                        {
                                            printf("Ho già il dato calcolato\n");

                                            /*Stampa del dato calcolato*/
                                            if (aggr == 'T')
                                                printf("Totale periodo %s: %s\n", date, calc_data);
                                            else
                                            {
                                                j = 0;
                                                c[1] = '\0';
                                                temp_time = time1;
                                                buffer[0] = '\0';
                                                printf("Variazioni:\n");
                                                while (calc_data[j] != '\0')
                                                {
                                                    c[0] = calc_data[j];
                                                    if (calc_data[j] != ',')
                                                        strcat(buffer, c);
                                                    else
                                                    {
                                                        strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                        printf("%s - ", date1);
                                                        temp_time.tm_mday++;
                                                        temp_t = mktime(&temp_time);
                                                        strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                        printf("%s: %s\n", date1, buffer);
                                                        buffer[0] = '\0';
                                                    }
                                                    j++;
                                                }
                                                /*Stampa ultimo valore*/
                                                strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                printf("%s - ", date1);
                                                temp_time.tm_mday++;
                                                temp_t = mktime(&temp_time);
                                                strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                printf("%s: %s\n", date1, buffer);
                                            }
                                            break;
                                        }
                                        strcpy(calc_data, "\0");
                                    }
                                    fclose(fptr);
                                }
                                else
                                    printf("Errore apertura file\n"); //Gestione errore fopen

                                if (strlen(calc_data) == 0) //Se il dato non è presente
                                {
                                    printf("Non ho il dato già calcolato\n");
                                    printf("Controllo se ho tutte le entries\n");

                                    /*Se non si hanno tutte le entries di alcune date, queste vengono salvate in missing_data*/
                                    missing_data[0] = '\0';
                                    sprintf(buffer, "./%d/Entries.txt", my_port);

                                    /*Se non esiste il file con le entries si crea*/
                                    if (access(buffer, F_OK) != 0)
                                    {
                                        fptr = fopen(buffer, "w");
                                        if (fptr != NULL)
                                            fclose(fptr);
                                        else
                                            printf("Errore apertura file\n");
                                    }

                                    /*Apertura del file e salvataggio delle date delle entries mancanti*/
                                    fptr = fopen(buffer, "r");
                                    if (fptr != NULL)
                                    {
                                        temp_time = time1;
                                        temp_t = mktime(&temp_time);
                                        while (difftime(end_t, temp_t) >= 0)
                                        {
                                            strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                            while (fgets(buffer, BUF_LEN, fptr) != NULL)
                                            {
                                                sscanf(buffer, "%s %d %d", date2, &cases, &swabs);
                                                if (strcmp(date1, date2) == 0)
                                                    goto next;
                                            }
                                            strcat(missing_data, date1);
                                            strcat(missing_data, "-");
                                        next:
                                            temp_time.tm_mday++;
                                            temp_t = mktime(&temp_time);
                                            fseek(fptr, 0, SEEK_SET);
                                        }
                                        fclose(fptr);
                                    }
                                    else
                                        printf("Errore apertura file\n"); //Gestione errore fopen

                                    if (strlen(missing_data) == 0) //Se ho tutte le entries
                                    {
                                        printf("Ho tutte le entries\n");

                                        /*Scorro le entries complete per calcolare il dato aggregato*/
                                        temp_time = time1;
                                        temp_t = mktime(&temp_time);
                                        sum1 = sum2 = 0;
                                        sprintf(buffer, "./%d/Entries.txt", my_port);
                                        strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                        fptr = fopen(buffer, "r");
                                        if (fptr != NULL)
                                        {
                                            while (difftime(end_t, temp_t) >= 0) //Scorro le entries del periodo richiesto
                                            {
                                                /*Calcolo e stampa del dato aggregato*/
                                                res = fgets(buffer, BUF_LEN, fptr);
                                                if (res == NULL)
                                                    printf("Errore scan\n");
                                                sscanf(buffer, "%s %d %d", date2, &cases, &swabs);
                                                if (strcmp(date1, date2) == 0)
                                                {
                                                    /*Calcolo del totale*/
                                                    if (aggr == 'T')
                                                    {
                                                        if (type == 'T')
                                                            sum1 += swabs;
                                                        else
                                                            sum1 += cases;

                                                        temp_time.tm_mday++;
                                                        temp_t = mktime(&temp_time);
                                                        strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                                        fseek(fptr, 0, SEEK_SET);
                                                    }
                                                    else
                                                    {
                                                        /*Calcolo delle variazioni e salvataggio di queste in calc_data*/
                                                        if (type == 'T')
                                                        {
                                                            sum1 = sum2;
                                                            sum2 = 0;
                                                            sum2 += swabs;
                                                        }
                                                        else
                                                        {
                                                            sum1 = sum2;
                                                            sum2 = 0;
                                                            sum2 += cases;
                                                        }

                                                        if (difftime(temp_t, start_t) != 0)
                                                        {
                                                            sprintf(buffer, "%d", sum2 - sum1);
                                                            strcat(calc_data, buffer);
                                                            if (difftime(end_t, temp_t) != 0)
                                                                strcat(calc_data, ",");
                                                            printf("%s\n", buffer);
                                                        }
                                                        else
                                                        {
                                                            printf("Variazioni:\n");
                                                            calc_data[0] = '\0';
                                                        }
                                                        if (difftime(end_t, temp_t) != 0)
                                                        {
                                                            strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                            printf("%s - ", date1);
                                                            temp_time.tm_mday++;
                                                            temp_t = mktime(&temp_time);
                                                            strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                            printf("%s: ", date1);
                                                            fseek(fptr, 0, SEEK_SET);
                                                            strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                                        }
                                                        else
                                                            break;
                                                    }
                                                }
                                            }
                                            fclose(fptr);

                                            /*Salvataggio del totale in calc_data e stampa*/
                                            if (aggr == 'T')
                                            {
                                                sprintf(buffer, "%d", sum1);
                                                strcpy(calc_data, buffer);
                                                printf("Totale periodo %s: %s\n", date, calc_data);
                                            }

                                            /*Salvataggio del dato aggregato tra i dati già calcolati*/
                                            sprintf(buffer, "./%d/Calc_data.txt", my_port);
                                            fptr = fopen(buffer, "a");
                                            if (fptr != NULL)
                                            {
                                                fprintf(fptr, "%c %c %s %s\n", aggr, type, date, calc_data);
                                                fclose(fptr);
                                            }
                                            else
                                                printf("Errore apertura file\n"); //Gestione errore fopen
                                        }
                                        else
                                            printf("Errore apertura file\n"); //Gestione errore fopen
                                    }
                                    else if (next_neighbor == 0) //Se non ho vicini aggiorno il file con le entries complete e calcolo il dato
                                    {
                                        j = 0;

                                        /*Gestione strutture date*/
                                        sscanf(date, "%[^-\n]-%s", date1, date2);
                                        strptime(date1, "%d:%m:%Y", &temp_time);
                                        strptime(date2, "%d:%m:%Y", &time2);
                                        temp_t = mktime(&temp_time);
                                        end_t = mktime(&time2);

                                        sprintf(buffer, "./%d/", my_port);
                                        printf("Salvataggio entries...\n");
                                        while (difftime(end_t, temp_t) >= 0) //Scorro tutte le entries che mi servono
                                        {
                                            strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                            strcat(buffer, date1);
                                            swabs = 0;
                                            cases = 0;
                                            dirptr = opendir(buffer); //Apertura cartella di una certa data
                                            if (dirptr != NULL)
                                            {
                                                while ((in_file = readdir(dirptr)) != NULL) //Scorro tutti i register della data
                                                {
                                                    if (strcmp(in_file->d_name, ".") == 0)
                                                        continue;
                                                    if (strcmp(in_file->d_name, "..") == 0)
                                                        continue;
                                                    sprintf(buffer, "./%d/%s/", my_port, date1);
                                                    strcat(buffer, in_file->d_name);
                                                    fptr = fopen(buffer, "r");
                                                    if (fptr != NULL)
                                                    {
                                                        while (1)
                                                        {
                                                            /*Sommo le quantità di tamponi e casi di tutti i register di una certa data*/
                                                            res = fgets(buffer, BUF_LEN, fptr);
                                                            if (res == NULL)
                                                                break;
                                                            sscanf(buffer, "%s %c %d", timestamp, &temp_type, &qty);
                                                            if (temp_type == 'T')
                                                                swabs += qty;
                                                            else
                                                                cases += qty;
                                                        }
                                                        fclose(fptr);
                                                    }
                                                    else
                                                        printf("Errore apertura file\n");
                                                }
                                                closedir(dirptr);
                                            }
                                            /*Se non esiste la cartella di una data viene creata*/
                                            else if (errno == ENOENT)
                                            {
                                                ret = mkdir(buffer, 0777);
                                                if (ret < 0)
                                                    printf("Errore creazione cartella\n");
                                            }
                                            else
                                                printf("Errore apertura cartella\n");

                                            sprintf(buffer, "./%d/Entries.txt", my_port);
                                            fptr = fopen(buffer, "a"); //Apertura file entries
                                            if (fptr != NULL)
                                            {
                                                /*Salvataggio del totale di tamponi e casi di una certa data nel file delle entries complete*/
                                                fprintf(fptr, "%s %d %d\n", date1, cases, swabs);
                                                fclose(fptr);
                                            }
                                            else
                                                printf("Errore apertura file\n");

                                            temp_time.tm_mday++;
                                            temp_t = mktime(&temp_time);
                                            sprintf(buffer, "./%d/", my_port);
                                        }
                                        printf("Salvate\n");

                                        /*Calcolo il dato aggregato e lo salvo nel file dei dati
                                        già calcolati*/
                                        printf("Calcolo del dato richiesto\n");

                                        /*Gestione delle strutture delle date*/
                                        sscanf(date, "%[^-\n]-%s", date1, date2);
                                        strptime(date1, "%d:%m:%Y", &time1);
                                        strptime(date2, "%d:%m:%Y", &time2);
                                        temp_time = time1;
                                        temp_t = mktime(&temp_time);
                                        sum1 = sum2 = 0;
                                        sprintf(buffer, "./%d/Entries.txt", my_port);
                                        strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                        fptr = fopen(buffer, "r");
                                        if (fptr != NULL)
                                        {
                                            while (difftime(end_t, temp_t) >= 0) //Scorro le entries del periodo richiesto
                                            {
                                                res = fgets(buffer, BUF_LEN, fptr);
                                                if (res == NULL)
                                                    printf("Errore scan\n");

                                                /*Calcolo del dato aggregato*/
                                                sscanf(buffer, "%s %d %d", date2, &cases, &swabs);
                                                if (strcmp(date1, date2) == 0)
                                                {
                                                    /*Calcolo del totale*/
                                                    if (aggr == 'T')
                                                    {
                                                        if (type == 'T')
                                                            sum1 += swabs;
                                                        else
                                                            sum1 += cases;

                                                        temp_time.tm_mday++;
                                                        temp_t = mktime(&temp_time);
                                                        strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                                        fseek(fptr, 0, SEEK_SET);
                                                    }
                                                    else
                                                    {
                                                        /*Calcolo e stampa delle variazioni*/
                                                        if (type == 'T')
                                                        {
                                                            sum1 = sum2;
                                                            sum2 = 0;
                                                            sum2 += swabs;
                                                        }
                                                        else
                                                        {
                                                            sum1 = sum2;
                                                            sum2 = 0;
                                                            sum2 += cases;
                                                        }

                                                        if (difftime(temp_t, start_t) != 0)
                                                        {
                                                            sprintf(buffer, "%d", sum2 - sum1);
                                                            strcat(calc_data, buffer);
                                                            if (difftime(end_t, temp_t) != 0)
                                                                strcat(calc_data, ",");
                                                            printf("%s\n", buffer);
                                                        }
                                                        else
                                                        {
                                                            printf("Variazioni:\n");
                                                            calc_data[0] = '\0';
                                                        }
                                                        if (difftime(end_t, temp_t) != 0)
                                                        {
                                                            strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                            printf("%s - ", date1);
                                                            temp_time.tm_mday++;
                                                            temp_t = mktime(&temp_time);
                                                            strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                            printf("%s: ", date1);
                                                            strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                                            fseek(fptr, 0, SEEK_SET);
                                                        }
                                                        else
                                                            break;
                                                    }
                                                }
                                            }
                                            fclose(fptr);

                                            /*Salvataggio del totale in calc_data e stampa*/
                                            if (aggr == 'T')
                                            {
                                                sprintf(calc_data, "%d", sum1);
                                                printf("Totale periodo %s: %s\n", date, calc_data);
                                            }

                                            /*Salvataggio del dato aggregato tra i dati calcolati*/
                                            sprintf(buffer, "./%d/Calc_data.txt", my_port);
                                            fptr = fopen(buffer, "a");
                                            if (fptr != NULL)
                                            {
                                                fprintf(fptr, "%c %c %s %s\n", aggr, type, date, calc_data);
                                                fclose(fptr);
                                            }
                                            else
                                                printf("Errore apertura file\n"); //Gestione errore fopen
                                        }
                                        else
                                            printf("Errore apertura file\n"); //Gestione errore fopen
                                    }
                                    else //Se non ho tutte le entries
                                    {
                                        printf("Non ho tutte le entries\n");

                                        /*Chiedo al vicino (il precedente) se ha già il dato calcolato*/
                                        /*Salvataggio indirizzo del vicino*/
                                        memset(&peer_addr, 0, sizeof(peer_addr));
                                        peer_addr.sin_family = AF_INET;
                                        peer_addr.sin_port = htons(prev_neighbor);
                                        inet_pton(AF_INET, DS_addr, &peer_addr.sin_addr);

                                        /*Connessione al vicino*/
                                        sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                                        ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                                        if (ret < 0)
                                        {
                                            printf("Errore connect\n");
                                            continue;
                                        }

                                        /*Invio messaggio di richiesta*/
                                        printf("Chiedo a %d se ha già il dato calcolato\n", prev_neighbor);
                                        sprintf(buffer, "REQ %c %c %s", aggr, type, date);
                                        len = strlen(buffer) + 1;
                                        lmsg = htons(len);
                                        ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore invio lmsg\n");
                                            continue;
                                        }
                                        ret = send(sdTCP, (void *)&buffer, len, 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore invio buffer\n");
                                            continue;
                                        }
                                        ret = recv(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione lmsg\n");
                                            continue;
                                        }
                                        len = ntohs(lmsg);
                                        ret = recv(sdTCP, (void *)&buffer, len, 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione messaggio\n");
                                            continue;
                                        }
                                        close(sdTCP);

                                        /*Se il vicino non ha il dato (messaggio vuoto)*/
                                        /*Chiedo all'altro vicino (il successivo) se ha il dato*/
                                        if (strlen(buffer) == 0)
                                        {
                                            /*Salvataggio indirizzo del vicino*/
                                            memset(&peer_addr, 0, sizeof(peer_addr));
                                            peer_addr.sin_family = AF_INET;
                                            peer_addr.sin_port = htons(next_neighbor);
                                            inet_pton(AF_INET, DS_addr, &peer_addr.sin_addr);

                                            /*Connessione al vicino*/
                                            sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                                            ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                                            if (ret < 0)
                                            {
                                                printf("Errore connect\n");
                                                continue;
                                            }

                                            printf("Chiedo a %d se ha già il dato calcolato\n", next_neighbor);
                                            sprintf(buffer, "REQ %c %c %s", aggr, type, date);
                                            len = strlen(buffer) + 1;
                                            lmsg = htons(len);
                                            ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                            if (ret < 0)
                                            {
                                                printf("Errore invio lmsg\n");
                                                continue;
                                            }
                                            ret = send(sdTCP, (void *)&buffer, len, 0);
                                            if (ret < 0)
                                            {
                                                printf("Errore invio buffer\n");
                                                continue;
                                            }
                                            ret = recv(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                            if (ret < 0)
                                            {
                                                printf("Errore ricezione lmsg\n");
                                                continue;
                                            }
                                            len = ntohs(lmsg);
                                            ret = recv(sdTCP, (void *)&buffer, len, 0);
                                            if (ret < 0)
                                            {
                                                printf("Errore ricezione messaggio\n");
                                                continue;
                                            }
                                            close(sdTCP);

                                            /*Se anche l'altro vicino non ha il dato invio un messaggio di flooding*/
                                            if (strlen(buffer) == 0)
                                            {
                                                /*Salvataggio indirizzo del vicino*/
                                                memset(&peer_addr, 0, sizeof(peer_addr));
                                                peer_addr.sin_family = AF_INET;
                                                peer_addr.sin_port = htons(next_neighbor);
                                                inet_pton(AF_INET, DS_addr, &peer_addr.sin_addr);

                                                /*Connessione al vicino*/
                                                sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                                                ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                                                if (ret < 0)
                                                {
                                                    printf("Errore connect\n");
                                                    continue;
                                                }

                                                /*Invio messaggio di flooding*/
                                                printf("Dato non presente. Invio messaggio di flooding\n");
                                                sprintf(buffer, "FLOOD %d - %c %c %s %s", my_port,
                                                        aggr, type, date, missing_data);
                                                len = strlen(buffer) + 1;
                                                lmsg = htons(len);
                                                ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                                if (ret < 0)
                                                {
                                                    printf("Errore invio lmsg\n");
                                                    continue;
                                                }
                                                ret = send(sdTCP, (void *)&buffer, len, 0);
                                                if (ret < 0)
                                                {
                                                    printf("Errore invio buffer\n");
                                                    continue;
                                                }
                                                close(sdTCP);
                                            }
                                        }
                                        /*Se uno dei vicini ha il dato calcolato*/
                                        else
                                        {
                                            printf("Ricevuto dato calcolato\n");

                                            /*Salvataggio del dato tra i dati calcolati e stampa a schermo*/
                                            sprintf(calc_data, "%s", buffer);
                                            sprintf(buffer, "./%d/Calc_data.txt", my_port);
                                            fptr = fopen(buffer, "a");
                                            if (fptr != NULL)
                                            {
                                                fprintf(fptr, "%c %c %s %s\n", aggr, type, date, calc_data);
                                                fclose(fptr);
                                            }
                                            else
                                                printf("Errore apertura file\n"); //Gestione errore fopen
                                            if (aggr == 'T')
                                                printf("Totale periodo %s: %s\n", date, calc_data);
                                            else
                                            {
                                                temp_time = time1;
                                                j = 0;
                                                c[1] = '\0';
                                                buffer[0] = '\0';
                                                while (calc_data[j] != '\0')
                                                {
                                                    c[0] = calc_data[j];
                                                    if (calc_data[j] != ',')
                                                        strcat(buffer, c);
                                                    else
                                                    {
                                                        strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                        printf("%s - ", date1);
                                                        temp_time.tm_mday++;
                                                        temp_t = mktime(&temp_time);
                                                        strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                        printf("%s: %s\n", date1, buffer);
                                                        buffer[0] = '\0';
                                                    }
                                                    j++;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            else
                                printf("Formato data errato\n"); //Gestione data errata
                        }
                        else
                            printf("Tipo errato\n"); //Gestione tipo o quantità errati
                    }
                    else if (strcmp(cmd, "stop") == 0) //Se si riceve il comando stop
                    {
                        if (next_neighbor != 0)
                        {
                            /*Salvataggio indirizzo del vicino*/
                            memset(&peer_addr, 0, sizeof(peer_addr));
                            peer_addr.sin_family = AF_INET;
                            peer_addr.sin_port = htons(prev_neighbor);
                            inet_pton(AF_INET, DS_addr, &peer_addr.sin_addr);

                            /*Connessione al vicino*/
                            sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                            ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                            if (ret < 0)
                            {
                                printf("Errore connect\n");
                                continue;
                            }

                            sprintf(buffer, "./%d", my_port);
                            dirptr = opendir(buffer); //Apertura cartella peer
                            printf("Invio le mie entries a %d\n", prev_neighbor);
                            sent = 0; //Se non ho entries da inviare questa variabile resterà settata a 0
                            if (dirptr != NULL)
                            {
                                while ((in_file = readdir(dirptr)) != NULL) //Si scorrono le cartelle di tutte le date
                                {
                                    if (strcmp(in_file->d_name, ".") == 0)
                                        continue;
                                    if (strcmp(in_file->d_name, "..") == 0)
                                        continue;
                                    if (strcmp(in_file->d_name, "Calc_data.txt") == 0)
                                        continue;
                                    if (strcmp(in_file->d_name, "Entries.txt") == 0)
                                        continue;
                                    sprintf(buffer, "./%d/%s/%d.txt", my_port, in_file->d_name, my_port);
                                    if (access(buffer, F_OK) == 0) //Se ho le entries di una certa data
                                    {
                                        sent = 1; //Ho almeno una entry da inviare

                                        /*Chiedo al vicino se ha già le entry della data target*/
                                        sprintf(buffer, "SEND %d %s", my_port, in_file->d_name);
                                        len = strlen(buffer) + 1;
                                        lmsg = htons(len);
                                        ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore invio lmsg\n");
                                            continue;
                                        }
                                        ret = send(sdTCP, (void *)&buffer, len, 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore invio buffer\n");
                                            continue;
                                        }

                                        /*Ricevo la risposta dal vicino*/
                                        ret = recv(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione lmsg\n");
                                            continue;
                                        }
                                        len = ntohs(lmsg);
                                        ret = recv(sdTCP, (void *)&buffer, len, 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione messaggio\n");
                                            continue;
                                        }

                                        if (strcmp(buffer, "OK") == 0) //Se il vicino non ha le entries della data target
                                        {
                                            /*Apertura del file contente le entries*/
                                            sprintf(buffer, "./%d/%s/%d.txt", my_port, in_file->d_name, my_port);
                                            fptr = fopen(buffer, "r");
                                            if (fptr != NULL)
                                            {
                                                while (1)
                                                {
                                                    /*Invio delle entries*/
                                                    res = fgets(buffer, BUF_LEN, fptr);
                                                    if (res == NULL)
                                                        break;
                                                    len = strlen(buffer) + 1;
                                                    lmsg = htons(len);
                                                    ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                                    if (ret < 0)
                                                    {
                                                        printf("Errore invio lmsg\n");
                                                        continue;
                                                    }
                                                    ret = send(sdTCP, (void *)&buffer, len, 0);
                                                    if (ret < 0)
                                                    {
                                                        printf("Errore invio buffer\n");
                                                        continue;
                                                    }
                                                }

                                                /*Comunico al vicino che ho inviato tutte le entries della data target*/
                                                strcpy(buffer, "NEXT");
                                                len = strlen(buffer) + 1;
                                                lmsg = htons(len);
                                                ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                                if (ret < 0)
                                                {
                                                    printf("Errore invio lmsg\n");
                                                    continue;
                                                }
                                                ret = send(sdTCP, (void *)&buffer, len, 0);
                                                if (ret < 0)
                                                {
                                                    printf("Errore invio buffer\n");
                                                    continue;
                                                }
                                            }
                                            else
                                                printf("Errore apertura file\n");
                                        }
                                    }
                                }
                                /*Dopo aver inviato tutte le entries comunico al vicino che non devo inviargli più niente*/
                                if (sent == 1) //Invio il messaggio di END solo se ho inviato almeno una entry
                                {
                                    strcpy(buffer, "END");
                                    len = strlen(buffer) + 1;
                                    lmsg = htons(len);
                                    ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                    if (ret < 0)
                                    {
                                        printf("Errore invio lmsg\n");
                                        continue;
                                    }
                                    ret = send(sdTCP, (void *)&buffer, len, 0);
                                    if (ret < 0)
                                    {
                                        printf("Errore invio buffer\n");
                                        continue;
                                    }
                                }
                            }
                            printf("Inviate\n");
                            close(sdTCP);

                            /*Salvataggio indirizzo del vicino*/
                            memset(&peer_addr, 0, sizeof(peer_addr));
                            peer_addr.sin_family = AF_INET;
                            peer_addr.sin_port = htons(next_neighbor);
                            inet_pton(AF_INET, DS_addr, &peer_addr.sin_addr);

                            /*Connessione al vicino*/
                            sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                            ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                            if (ret < 0)
                            {
                                printf("Errore connect\n");
                                continue;
                            }

                            sprintf(buffer, "./%d", my_port);
                            dirptr = opendir(buffer); //Apertura cartella peer
                            printf("Invio le mie entries a %d\n", next_neighbor);
                            if (dirptr != NULL)
                            {
                                while ((in_file = readdir(dirptr)) != NULL) //Si scorrono le cartelle di tutte le date
                                {
                                    if (strcmp(in_file->d_name, ".") == 0)
                                        continue;
                                    if (strcmp(in_file->d_name, "..") == 0)
                                        continue;
                                    if (strcmp(in_file->d_name, "Calc_data.txt") == 0)
                                        continue;
                                    if (strcmp(in_file->d_name, "Entries.txt") == 0)
                                        continue;
                                    sprintf(buffer, "./%d/%s/%d.txt", my_port, in_file->d_name, my_port);
                                    if (access(buffer, F_OK) == 0) //Se ho le entries di una certa data
                                    {
                                        sent = 1; //Ho almeno una entry da inviare

                                        /*Chiedo al vicino se ha le entries della data target*/
                                        sprintf(buffer, "SEND %d %s", my_port, in_file->d_name);
                                        len = strlen(buffer) + 1;
                                        lmsg = htons(len);
                                        ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore invio lmsg\n");
                                            continue;
                                        }
                                        ret = send(sdTCP, (void *)&buffer, len, 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore invio buffer\n");
                                            continue;
                                        }

                                        /*Controllo la risposta del vicino*/
                                        ret = recv(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione lmsg\n");
                                            continue;
                                        }
                                        len = ntohs(lmsg);
                                        ret = recv(sdTCP, (void *)&buffer, len, 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione messaggio\n");
                                            continue;
                                        }
                                        if (strcmp(buffer, "OK") == 0) //Se il vicino non ha le entries
                                        {
                                            sprintf(buffer, "./%d/%s/%d.txt", my_port, in_file->d_name, my_port);
                                            fptr = fopen(buffer, "r"); //Apertura del file con le entries della data target
                                            if (fptr != NULL)
                                            {
                                                while (1)
                                                {
                                                    /*Invio delle entries*/
                                                    res = fgets(buffer, BUF_LEN, fptr);
                                                    if (res == NULL)
                                                        break;
                                                    len = strlen(buffer) + 1;
                                                    lmsg = htons(len);
                                                    ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                                    if (ret < 0)
                                                    {
                                                        printf("Errore invio lmsg\n");
                                                        continue;
                                                    }
                                                    ret = send(sdTCP, (void *)&buffer, len, 0);
                                                    if (ret < 0)
                                                    {
                                                        printf("Errore invio buffer\n");
                                                        continue;
                                                    }
                                                }
                                                /*Comunico al vicino che ho inviato tutte le entries della data target*/
                                                strcpy(buffer, "NEXT");
                                                len = strlen(buffer) + 1;
                                                lmsg = htons(len);
                                                ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                                if (ret < 0)
                                                {
                                                    printf("Errore invio lmsg\n");
                                                    continue;
                                                }
                                                ret = send(sdTCP, (void *)&buffer, len, 0);
                                                if (ret < 0)
                                                {
                                                    printf("Errore invio buffer\n");
                                                    continue;
                                                }
                                            }
                                            else
                                                printf("Errore apertura file\n");
                                        }
                                    }
                                }
                                if (sent == 1) //Invio il messaggio di END solo se ho inviato almeno una entry
                                {
                                    /*Dopo aver inviato tutte le entries comunico al vicino che non ho più niente da inviargli*/
                                    strcpy(buffer, "END");
                                    len = strlen(buffer) + 1;
                                    lmsg = htons(len);
                                    ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                    if (ret < 0)
                                    {
                                        printf("Errore invio lmsg\n");
                                        continue;
                                    }
                                    ret = send(sdTCP, (void *)&buffer, len, 0);
                                    if (ret < 0)
                                    {
                                        printf("Errore invio buffer\n");
                                        continue;
                                    }
                                }
                            }
                            printf("Inviate\n");
                            close(sdTCP);
                        }
                        /*Invio al DS la richiesta di chiusura*/
                        sdUDP = socket(AF_INET, SOCK_DGRAM, 0);
                        printf("Richiesta di disconnessione...\n");
                        sprintf(buffer, "close %d", my_port);
                        ret = sendto(sdUDP, buffer, sizeof(buffer), 0,
                                     (struct sockaddr *)&srv_addr, sizeof(srv_addr));
                        if (ret < 0)
                        {
                            printf("Close fallita\n");
                            continue;
                        }

                        /*Attendo la risposta del DS*/
                        for (j = 0; j < 5; j++)
                        {
                            ret = recvfrom(sdUDP, buffer, sizeof(buffer), MSG_DONTWAIT,
                                           (struct sockaddr *)&srv_addr, &len);
                            if (ret < 0)
                            {
                                printf("Aspetto risposta...\n");
                                sleep(POLLING_TIME);
                            }
                            else
                            {
                                printf("%s\n", buffer);
                                break;
                            }
                        }
                        if (j == 5) //Se non si riceve risposta
                            printf("Close fallita\n");
                        else
                        {
                            /*Terminazione del peer*/
                            printf("Disconnesso\n");
                            close(sdUDP);
                            exit(0);
                        }
                    }
                    else
                        printf("Comando errato\n");
                }
                else //Se viene ricevuto un comando da un altro peer
                {
                    ret = recv(i, (void *)&lmsg, sizeof(uint32_t), 0);
                    if (ret < 0)
                    {
                        printf("Errore ricezione lmsg\n");
                        continue;
                    }
                    len = ntohs(lmsg);
                    ret = recv(i, (void *)&buffer, len, 0);
                    if (ret < 0)
                    {
                        printf("Errore ricezione messaggio\n");
                        continue;
                    }
                    sscanf(buffer, "%s", cmd);

                    /*Se il comando richiede un dato già calcolato*/
                    if (strcmp(cmd, "REQ") == 0)
                    {
                        printf("Controllo se ho il dato aggregato per il vicino\n");

                        /*Controllo se il dato è presente tra quelli già calcolati*/
                        sscanf(buffer, "%s %c %c %s", cmd, &aggr, &type, date);
                        sprintf(buffer, "./%d/Calc_data.txt", my_port);

                        /*Se non è presente il file dei dati calcolati si crea*/
                        if (access(buffer, F_OK) != 0)
                        {
                            fptr = fopen(buffer, "w");
                            if (fptr != NULL)
                                fclose(fptr);
                            else
                                printf("Errore apertura file\n");
                        }
                        strcpy(calc_data, "\0");
                        fptr = fopen(buffer, "r"); //Apertura file dati già calcolati
                        if (fptr != NULL)
                        {
                            while (1)
                            {
                                res = fgets(buffer, BUF_LEN, fptr);
                                if (res == NULL)
                                    break;
                                sscanf(buffer, "%c %c %s %s", &temp_aggr,
                                       &temp_type, temp_date, calc_data);

                                /*Se trovo il dato calcolato lo salvo in calc_data*/
                                if (temp_aggr == aggr && temp_type == type &&
                                    strcmp(temp_date, date) == 0)
                                {
                                    sprintf(buffer, "%s", calc_data);
                                    break;
                                }
                                /*Se non trovo il dato calcolato invio un messaggio vuoto*/
                                strcpy(calc_data, "\0");
                            }
                            fclose(fptr);
                        }
                        else
                            printf("Errore apertura file\n"); //Gestione errore nella fopen

                        /*Invio del dato calcolato*/
                        printf("Invio del dato aggregato al vicino\n");
                        sprintf(buffer, "%s", calc_data);
                        len = strlen(buffer) + 1;
                        lmsg = htons(len);
                        ret = send(i, (void *)&lmsg, sizeof(uint32_t), 0);
                        if (ret < 0)
                        {
                            printf("Errore invio lmsg\n");
                            continue;
                        }
                        ret = send(i, (void *)&buffer, len, 0);
                        if (ret < 0)
                        {
                            printf("Errore invio buffer\n");
                            continue;
                        }
                    }
                    /*Se si riceve un comando di flooding*/
                    else if (strcmp(cmd, "FLOOD") == 0)
                    {
                        sscanf(buffer, "%s %d %s %c %c %s %s", cmd, &requester,
                               peer_list, &aggr, &type, date, missing_data);

                        /*Se non sono il requester*/
                        if (requester != my_port)
                        {
                            printf("Ricevuto comando di flooding\n");

                            /*Controllo se ho almeno una delle entries che servono al requester*/
                            sprintf(buffer, "./%d/", my_port);
                            j = 0;
                            c[1] = '\0';

                            /*Apro le cartelle delle date richieste*/
                            while (missing_data[j] != '\0')
                            {
                                c[0] = missing_data[j];
                                if (missing_data[j] != '-')
                                    strcat(buffer, c);
                                else
                                {
                                    /*Per controllare se ho delle entries controllo se ho qualche register
                                    della data richiesta*/
                                    dirptr = opendir(buffer);

                                    /*Se ho almeno una entry salvo il mio numero di porta tra quelli
                                    che il requester dovrà contattare per avere le entries mancanti*/
                                    if (dirptr != NULL)
                                    {
                                        sprintf(buffer, "%d-", my_port);
                                        strcat(peer_list, buffer);
                                        closedir(dirptr);
                                        break;
                                    }
                                    sprintf(buffer, "./%d/", my_port);
                                }
                                j++;
                            }
                            /*Inoltro il messaggio di flooding al prossimo vicino*/
                            printf("Inoltro messaggio di flooding\n");
                            sprintf(buffer, "FLOOD %d %s %c %c %s %s", requester,
                                    peer_list, aggr, type, date, missing_data);

                            /*Salvataggio indirizzo del vicino*/
                            memset(&peer_addr, 0, sizeof(peer_addr));
                            peer_addr.sin_family = AF_INET;
                            peer_addr.sin_port = htons(next_neighbor);
                            inet_pton(AF_INET, DS_addr, &peer_addr.sin_addr);

                            /*Connessione al vicino*/
                            sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                            ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                            if (ret < 0)
                            {
                                printf("Errore connect\n");
                                continue;
                            }
                            len = strlen(buffer) + 1;
                            lmsg = htons(len);
                            ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                            if (ret < 0)
                            {
                                printf("Errore invio lmsg\n");
                                continue;
                            }
                            ret = send(sdTCP, (void *)&buffer, len, 0);
                            if (ret < 0)
                            {
                                printf("Errore invio buffer\n");
                                continue;
                            }
                        }
                        /*Se sono il requester (e quindi il messaggio di flooding
                        ha fatto il giro dell'anello)*/
                        else
                        {
                            printf("Flooding effettuato\n");
                            buffer[0] = '\0';
                            j = 1;
                            c[1] = '\0';

                            /*Scorro la lista dei peer che hanno entries mancanti*/
                            while (peer_list[j] != '\0')
                            {
                                c[0] = peer_list[j];
                                if (peer_list[j] != '-')
                                    strcat(buffer, c);
                                else
                                {
                                    /*Connessione al peer target*/
                                    sscanf(buffer, "%d", &peer_port);

                                    memset(&peer_addr, 0, sizeof(peer_addr));
                                    peer_addr.sin_family = AF_INET;
                                    peer_addr.sin_port = htons(peer_port);
                                    inet_pton(AF_INET, DS_addr, &peer_addr.sin_addr);

                                    sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                                    ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                                    if (ret < 0)
                                    {
                                        printf("Errore connect\n");
                                        continue;
                                    }

                                    printf("Richiesta entries a %d\n", peer_port);

                                    /*Invio un messaggio per richiedere le entries mancanti*/
                                    sscanf(buffer, "%s %s", cmd, missing_data);
                                    sprintf(buffer, "GET %s", missing_data);
                                    len = strlen(buffer) + 1;
                                    lmsg = htons(len);
                                    ret = send(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                    if (ret < 0)
                                    {
                                        printf("Errore invio lmsg\n");
                                        continue;
                                    }
                                    ret = send(sdTCP, (void *)&buffer, len, 0);
                                    if (ret < 0)
                                    {
                                        printf("Errore invio buffer\n");
                                        continue;
                                    }

                                    /*Ricevo tutte le entries mancanti di un certo peer*/
                                    while (1)
                                    {
                                        ret = recv(sdTCP, (void *)&lmsg, sizeof(uint32_t), 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione lmsg\n");
                                            continue;
                                        }
                                        len = ntohs(lmsg);
                                        ret = recv(sdTCP, (void *)&buffer, len, 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione messaggio\n");
                                            continue;
                                        }

                                        /*Ricevute tutte le entries passo al prossimo peer target (se c'è)*/
                                        if (strcmp(buffer, "END") == 0)
                                            break;

                                        /*Salvo le entries nel register corrispondente*/
                                        sscanf(buffer, "%s %c %d %s", timestamp, &temp_type, &qty, date1);

                                        /*Creo la cartella della data se non esiste*/
                                        sprintf(buffer, "./%d/%s", my_port, date1);
                                        ret = mkdir(buffer, 0777);
                                        if (ret != 0 && errno != EEXIST)
                                            printf("Errore creazione cartella\n");

                                        /*Se non ho già le entries del peer le salvo*/
                                        sprintf(buffer, "./%d/%s/%d.txt", my_port, date1, peer_port);
                                        if (access(buffer, F_OK) != 0)
                                        {
                                            fptr = fopen(buffer, "a");
                                            if (fptr != NULL)
                                            {
                                                fprintf(fptr, "%s %c %d\n", timestamp, temp_type, qty);
                                                fclose(fptr);
                                            }
                                            else
                                                printf("Errore apertura file\n"); //Gestione errore nella fopen
                                        }
                                    }
                                    printf("Ricevute\n");
                                    close(sdTCP);
                                    buffer[0] = '\0';
                                }
                                j++;
                            }

                            /*Adesso ho tutte le entries rischieste quindi le salvo nel file con le entries complete*/
                            j = 0;

                            /*Gestione strutture date*/
                            sscanf(date, "%[^-\n]-%s", date1, date2);
                            strptime(date1, "%d:%m:%Y", &temp_time);
                            strptime(date2, "%d:%m:%Y", &time2);
                            temp_t = mktime(&temp_time);
                            end_t = mktime(&time2);

                            sprintf(buffer, "./%d/", my_port);
                            printf("Salvataggio entries...\n");
                            while (difftime(end_t, temp_t) >= 0) //Scorro tutte le entries che mi servono
                            {
                                strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                strcat(buffer, date1);
                                swabs = 0;
                                cases = 0;
                                dirptr = opendir(buffer); //Apertura cartella di una certa data
                                if (dirptr != NULL)
                                {
                                    while ((in_file = readdir(dirptr)) != NULL) //Scorro tutti i register della data
                                    {
                                        if (strcmp(in_file->d_name, ".") == 0)
                                            continue;
                                        if (strcmp(in_file->d_name, "..") == 0)
                                            continue;
                                        sprintf(buffer, "./%d/%s/", my_port, date1);
                                        strcat(buffer, in_file->d_name);
                                        fptr = fopen(buffer, "r");
                                        if (fptr != NULL)
                                        {
                                            while (1)
                                            {
                                                /*Sommo le quantità di tamponi e casi di tutti i register di una certa data*/
                                                res = fgets(buffer, BUF_LEN, fptr);
                                                if (res == NULL)
                                                    break;
                                                sscanf(buffer, "%s %c %d", timestamp, &temp_type, &qty);
                                                if (temp_type == 'T')
                                                    swabs += qty;
                                                else
                                                    cases += qty;
                                            }
                                            fclose(fptr);
                                        }
                                        else
                                            printf("Errore apertura file\n");
                                    }
                                    closedir(dirptr);
                                }
                                /*Se non esiste la cartella di una data viene creata*/
                                else if (errno == ENOENT)
                                {
                                    ret = mkdir(buffer, 0777);
                                    if (ret < 0)
                                        printf("Errore creazione cartella\n");
                                }
                                else
                                    printf("Errore apertura cartella\n");

                                sprintf(buffer, "./%d/Entries.txt", my_port);
                                fptr = fopen(buffer, "a"); //Apertura file entries
                                if (fptr != NULL)
                                {
                                    /*Salvataggio del totale di tamponi e casi di una certa data nel file delle entries complete*/
                                    fprintf(fptr, "%s %d %d\n", date1, cases, swabs);
                                    fclose(fptr);
                                }
                                else
                                    printf("Errore apertura file\n");

                                temp_time.tm_mday++;
                                temp_t = mktime(&temp_time);
                                sprintf(buffer, "./%d/", my_port);
                            }
                            printf("Salvate\n");

                            /*Calcolo il dato aggregato e lo salvo nel file dei dati
                            già calcolati*/
                            printf("Calcolo del dato richiesto\n");

                            /*Gestione delle strutture delle date*/
                            sscanf(date, "%[^-\n]-%s", date1, date2);
                            strptime(date1, "%d:%m:%Y", &time1);
                            strptime(date2, "%d:%m:%Y", &time2);
                            temp_time = time1;
                            temp_t = mktime(&temp_time);
                            sum1 = sum2 = 0;
                            sprintf(buffer, "./%d/Entries.txt", my_port);
                            strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                            fptr = fopen(buffer, "r");
                            if (fptr != NULL)
                            {
                                while (difftime(end_t, temp_t) >= 0) //Scorro le entries del periodo richiesto
                                {
                                    res = fgets(buffer, BUF_LEN, fptr);
                                    if (res == NULL)
                                        printf("Errore scan\n");

                                    /*Calcolo del dato aggregato*/
                                    sscanf(buffer, "%s %d %d", date2, &cases, &swabs);
                                    if (strcmp(date1, date2) == 0)
                                    {
                                        /*Calcolo del totale*/
                                        if (aggr == 'T')
                                        {
                                            if (type == 'T')
                                                sum1 += swabs;
                                            else
                                                sum1 += cases;

                                            temp_time.tm_mday++;
                                            temp_t = mktime(&temp_time);
                                            strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                            fseek(fptr, 0, SEEK_SET);
                                        }
                                        else
                                        {
                                            /*Calcolo e stampa delle variazioni*/
                                            if (type == 'T')
                                            {
                                                sum1 = sum2;
                                                sum2 = 0;
                                                sum2 += swabs;
                                            }
                                            else
                                            {
                                                sum1 = sum2;
                                                sum2 = 0;
                                                sum2 += cases;
                                            }

                                            if (difftime(temp_t, start_t) != 0)
                                            {
                                                sprintf(buffer, "%d", sum2 - sum1);
                                                strcat(calc_data, buffer);
                                                if (difftime(end_t, temp_t) != 0)
                                                    strcat(calc_data, ",");
                                                printf("%s\n", buffer);
                                            }
                                            else
                                            {
                                                printf("Variazioni:\n");
                                                calc_data[0] = '\0';
                                            }
                                            if (difftime(end_t, temp_t) != 0)
                                            {
                                                strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                printf("%s - ", date1);
                                                temp_time.tm_mday++;
                                                temp_t = mktime(&temp_time);
                                                strftime(date1, DATE_LEN, "%d:%m:%Y", &temp_time);
                                                printf("%s: ", date1);
                                                strftime(date1, DATE_LEN, "%Y_%m_%d", &temp_time);
                                                fseek(fptr, 0, SEEK_SET);
                                            }
                                            else
                                                break;
                                        }
                                    }
                                }
                                fclose(fptr);

                                /*Salvataggio del totale in calc_data e stampa*/
                                if (aggr == 'T')
                                {
                                    sprintf(calc_data, "%d", sum1);
                                    printf("Totale periodo %s: %s\n", date, calc_data);
                                }

                                /*Salvataggio del dato aggregato tra i dati calcolati*/
                                sprintf(buffer, "./%d/Calc_data.txt", my_port);
                                fptr = fopen(buffer, "a");
                                if (fptr != NULL)
                                {
                                    fprintf(fptr, "%c %c %s %s\n", aggr, type, date, calc_data);
                                    fclose(fptr);
                                }
                                else
                                    printf("Errore apertura file\n"); //Gestione errore fopen
                            }
                            else
                                printf("Errore apertura file\n"); //Gestione errore fopen
                        }
                    }
                    /*Se si riceve il comando di richiesta delle entries mancanti*/
                    else if (strcmp(cmd, "GET") == 0)
                    {
                        printf("Invio entries mancanti\n");
                        sscanf(buffer, "%s %s", cmd, missing_data);
                        sprintf(buffer, "./%d/", my_port);
                        j = 0;
                        c[1] = '\0';
                        date1[0] = '\0';

                        /*Si controlla, per ogni data, se si è in possesso delle entries corrispondenti*/
                        while (missing_data[j] != '\0')
                        {
                            c[0] = missing_data[j];
                            if (missing_data[j] != '-')
                            {
                                strcat(buffer, c);
                                strcat(date1, c);
                            }
                            else
                            {
                                /*Se ho le entries di una certa data*/
                                dirptr = opendir(buffer);
                                if (dirptr != NULL)
                                {
                                    while ((in_file = readdir(dirptr)) != NULL) //Scorro i register della data target
                                    {
                                        if (strcmp(in_file->d_name, ".") == 0)
                                            continue;
                                        if (strcmp(in_file->d_name, "..") == 0)
                                            continue;
                                        sprintf(buffer, "./%d/%s/%s", my_port, date1, in_file->d_name);
                                        fptr = fopen(buffer, "r");
                                        if (fptr != NULL)
                                        {
                                            while (1)
                                            {
                                                /*Invio le entries del register con annessa la data a cui si riferiscono*/
                                                res = fgets(buffer, BUF_LEN, fptr);
                                                if (res == NULL)
                                                    break;
                                                strcat(buffer, " ");
                                                strcat(buffer, date1);
                                                len = strlen(buffer) + 1;
                                                lmsg = htons(len);
                                                ret = send(i, (void *)&lmsg, sizeof(uint32_t), 0);
                                                if (ret < 0)
                                                {
                                                    printf("Errore invio lmsg\n");
                                                    continue;
                                                }
                                                ret = send(i, (void *)&buffer, len, 0);
                                                if (ret < 0)
                                                {
                                                    printf("Errore invio buffer\n");
                                                    continue;
                                                }
                                            }
                                            fclose(fptr);
                                        }
                                        else
                                            printf("Errore apertura file\n");
                                    }
                                    closedir(dirptr);
                                }
                                /*Se non ho le entries della data target passo alla prossima*/
                                else if (errno == ENOENT)
                                {
                                    sprintf(buffer, "./%d/", my_port);
                                    date1[0] = '\0';
                                    j++;
                                    continue;
                                }
                                else
                                    printf("Errore apertura cartella\n");

                                sprintf(buffer, "./%d/", my_port);
                                date1[0] = '\0';
                            }
                            j++;
                        }
                        /*Dopo aver inviato le entries richieste dico al richiedente di aver finito*/
                        strcpy(buffer, "END");
                        len = strlen(buffer) + 1;
                        lmsg = htons(len);
                        ret = send(i, (void *)&lmsg, sizeof(uint32_t), 0);
                        if (ret < 0)
                        {
                            printf("Errore invio lmsg\n");
                            continue;
                        }
                        ret = send(i, (void *)&buffer, len, 0);
                        if (ret < 0)
                        {
                            printf("Errore invio buffer\n");
                            continue;
                        }
                        printf("Inviate\n");
                    }
                    /*Se si riceve il comando di invio delle entries da un buffer in chiusura*/
                    else if (strcmp(cmd, "SEND") == 0)
                    {
                        printf("Ricezione entries da un peer in chiusura\n");
                        do
                        {
                            /*Si controlla se ho già il register che mi verrà inviato*/
                            sscanf(buffer, "%s %d %s", cmd, &peer_port, date1);
                            sprintf(buffer, "./%d/%s/%d.txt", my_port, date1, peer_port);
                            if (access(buffer, F_OK) != 0) //Se non ho il register
                            {
                                /*Comunico che non ho il register*/
                                strcpy(buffer, "OK");
                                len = strlen(buffer) + 1;
                                lmsg = htons(len);
                                ret = send(i, (void *)&lmsg, sizeof(uint32_t), 0);
                                if (ret < 0)
                                {
                                    printf("Errore invio lmsg\n");
                                    continue;
                                }
                                ret = send(i, (void *)&buffer, len, 0);
                                if (ret < 0)
                                {
                                    printf("Errore invio buffer\n");
                                    continue;
                                }

                                /*Creo la cartella della data se non esiste*/
                                sprintf(buffer, "./%d/%s", my_port, date1);
                                ret = mkdir(buffer, 0777);
                                if (ret != 0 && errno != EEXIST)
                                    printf("Errore creazione cartella\n");

                                /*Apro la cartella in cui salvare il register*/
                                sprintf(buffer, "./%d/%s/%d.txt", my_port, date1, peer_port);
                                fptr = fopen(buffer, "a");
                                if (fptr != NULL)
                                {
                                    /*Salvo le entries nel register*/
                                    while (1)
                                    {
                                        ret = recv(i, (void *)&lmsg, sizeof(uint32_t), 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione lmsg\n");
                                            continue;
                                        }
                                        len = ntohs(lmsg);
                                        ret = recv(i, (void *)&buffer, len, 0);
                                        if (ret < 0)
                                        {
                                            printf("Errore ricezione messaggio\n");
                                            continue;
                                        }
                                        if (strcmp(buffer, "NEXT") == 0)
                                            break;
                                        fprintf(fptr, "%s", buffer);
                                    }
                                    fclose(fptr);
                                }
                                else
                                    printf("Errore apertura file\n");
                            }
                            else //Se ho già il register lo comunico
                            {
                                strcpy(buffer, "NEXT");
                                len = strlen(buffer) + 1;
                                lmsg = htons(len);
                                ret = send(i, (void *)&lmsg, sizeof(uint32_t), 0);
                                if (ret < 0)
                                {
                                    printf("Errore invio lmsg\n");
                                    continue;
                                }
                                ret = send(i, (void *)&buffer, len, 0);
                                if (ret < 0)
                                {
                                    printf("Errore invio buffer\n");
                                    continue;
                                }
                            }
                            /*Ricevo il prossimo register dal peer in chiusura*/
                            ret = recv(i, (void *)&lmsg, sizeof(uint32_t), 0);
                            if (ret < 0)
                            {
                                printf("Errore ricezione lmsg\n");
                                continue;
                            }
                            len = ntohs(lmsg);
                            ret = recv(i, (void *)&buffer, len, 0);
                            if (ret < 0)
                            {
                                printf("Errore ricezione messaggio\n");
                                continue;
                            }
                        } while (strcmp(buffer, "END") != 0); //Smetto di ricevere una volta ricevuto il messaggio END
                        printf("Ricevute\n");
                    }
                    /*Se si riceve il messaggio di terminazione dal DS*/
                    else if (strcmp(buffer, "CLOSE") == 0)
                    {
                        printf("Peer terminato dal DS\n");
                        exit(0); //Termino il programma
                    }
                    /*Se ricevo i nuovi vicini dal DS*/
                    else if (strcmp(cmd, "NEIGH") == 0)
                    {
                        /*Ricevo i vicini e li salvo*/
                        sscanf(buffer, "%s %d %d", cmd, &prev_neighbor, &next_neighbor);
                        printf("Ricevuti vicini dal DS\n");
                        if (next_neighbor == 0)
                            printf("Non ho nessun vicino\n");
                        else
                            printf("Vicini: %d - %d\n", prev_neighbor, next_neighbor);
                    }
                    else
                        printf("Ricevuto comando non valido\n"); //Caso comando non valido
                    close(i);                                    //Chiusura socket
                    FD_CLR(i, &master);                          //Il descrittore di socket viene tolto dall'insieme dei descrittori
                }
            }
        }
    }
    close(listener); //Chiusura listener
    return 0;
}