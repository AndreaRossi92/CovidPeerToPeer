#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>

#define BUF_LEN 4096   //Capacità buffer
#define POLLING_TIME 1 //Tempo di attesa
#define CMD_LEN 6      //Massima lunghezza di un comando

/*Si crea una lista bidirezionale circolare ordinata per la gestione dei peer*/

/*Struttura di ogni elemento della lista*/
struct node
{
    int data;
    struct node *next;
    struct node *prev;
};

typedef struct node *list;

/*Funzione di inserimento in lista*/
/*La funzione restituisce un puntatore all'elemento inserito*/
list insert(list *s, int value)
{
    list new, ptr;
    if (*s == NULL) //Caso lista vuota
    {
        new = (list)malloc(sizeof(struct node));
        new->data = value;
        new->next = NULL;
        new->prev = NULL;
        *s = new;
        return *s;
    }
    if ((*s)->data > value) //Caso inserimento in testa
    {
        if ((*s)->next == NULL) //Inserimento in testa se è presente un solo elemento
        {
            new = (list)malloc(sizeof(struct node));
            new->data = value;
            new->next = *s;
            new->prev = *s;
            (*s)->next = new;
            (*s)->prev = new;
            *s = new;
            return *s;
        }
        new = (list)malloc(sizeof(struct node));
        new->data = value;
        ptr = *s;
        while (ptr->next != *s)
            ptr = ptr->next;
        new->prev = ptr;
        ptr->next = new;
        new->next = *s;
        (*s)->prev = new;
        *s = new;
        return *s;
    }
    /*Inserimento non in testa*/
    ptr = *s;
    /*Scorro la lista e inserisco*/
    while (ptr->next != *s && ptr->next != NULL)
    {
        if (ptr->next->data == value) //Se il valore è già presente in lista
            return NULL;
        if (ptr->next->data > value)
        {
            new = (list)malloc(sizeof(struct node));
            new->data = value;
            new->prev = ptr;
            new->next = ptr->next;
            ptr->next->prev = new;
            ptr->next = new;
            return ptr->next;
        }
        ptr = ptr->next;
    }
    if ((*s)->next == NULL) //Inserimento in testa se è presente un solo elemento
    {
        if ((*s)->data == value) //Se il valore è già presente in lista
            return NULL;
        new = (list)malloc(sizeof(struct node));
        new->data = value;
        new->next = *s;
        new->prev = *s;
        (*s)->next = new;
        (*s)->prev = new;
        return new;
    }
    /*Se arrivo in fondo alla lista inserisco in fondo*/
    new = (list)malloc(sizeof(struct node));
    new->data = value;
    ptr->next = new;
    new->prev = ptr;
    new->next = *s;
    (*s)->prev = new;
    return new;
}

/*Funzione di eliminazione dalla lista*/
/*La funzione restituisce un puntatore all'elemento precedente a quello eliminato*/
/*Nei casi in cui non venga eliminato nessun elemento o restino meno di due elementi in lista restituisce NULL*/
list delete (list *s, int value)
{
    list ptr, next, prev;
    if ((*s)->data == value) //Eliminazione in testa
    {
        if ((*s)->next == NULL) //Caso in cui è presente un solo elemento in lista
        {
            next = *s;
            *s = NULL;
            free(next);
            return NULL;
        }
        ptr = *s;
        prev = ptr->prev;
        next = ptr->next;
        if (prev != next) //Se resta più di un elemento in lista
        {
            prev->next = next;
            next->prev = prev;
        }
        else //Se resta un solo elemento in lista
        {
            next->next = NULL;
            next->prev = NULL;
        }
        *s = next;
        free(ptr);
        return (*s)->prev;
    }
    prev = (*s)->prev;
    /*Eliminazione non in testa*/
    do
    {
        prev = prev->next;
        if (prev->next->data == value)
        {
            ptr = prev->next;
            next = ptr->next;
            if (prev != next) //Se resta più di un elemento in lista
            {
                prev->next = next;
                next->prev = prev;
            }
            else //Se resta un solo elemento in lista
            {
                (*s)->next = NULL;
                (*s)->prev = NULL;
                prev = NULL;
            }
            free(ptr);
            return prev;
        }
    } while (prev->next != *s);
    return NULL; //Se il valore non è presente in lista
}

int main(int argc, char *argv[])
{
    int sdUDP, sdTCP;                      //Descrittori di socket
    int ret;                               //Variabile di ritorno
    unsigned int len, addr_len;            //Variabili di lunghezza
    int peer_port;                         //Variabile per salvare il numero di porta di un peer
    int my_port = atoi(argv[1]);           //Numero di porta del peer stesso
    struct sockaddr_in my_addr, peer_addr; //Strutture di indirizzo dei socket

    /*Buffer usati in varie funzioni*/
    char buffer[BUF_LEN];
    char peer_ip[16];
    char cmd[CMD_LEN];

    uint32_t lmsg; //Variabile per invio e ricezione lunghezza messaggi

    /*Insieme di descrittori*/
    fd_set master;
    fd_set read_fds;

    /*Variabili per la gestione della lista dei peer*/
    list peer = NULL;
    list cur_peer = NULL;

    sdUDP = socket(AF_INET, SOCK_DGRAM, 0); //Socket UDP

    /*Creazione indirizzo DS*/
    memset(&my_addr, 0, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(my_port);
    my_addr.sin_addr.s_addr = INADDR_ANY;

    /*Collegamento del socket UDP all'indirizzo del DS*/
    ret = bind(sdUDP, (struct sockaddr *)&my_addr, sizeof(my_addr));
    if (ret < 0)
    {
        perror("Errore bind UDP: \n");
        exit(-1);
    }

    /*Azzeramento insieme dei descrittori*/
    FD_ZERO(&master);
    FD_ZERO(&read_fds);

    /*Inserimento del socket e di stdin nell'insieme dei descrittori*/
    FD_SET(sdUDP, &master);
    FD_SET(STDIN_FILENO, &master);

    printf("***************************** DS COVID STARTED *****************************\n");
    printf("Digita un comando:\n");
    printf("1) help\n");
    printf("2) showpeers\n");
    printf("3) showneighbor [peer]\n");
    printf("4) esc\n");
    printf("\n");

    while (1)
    {
        read_fds = master;
        select(sdUDP + 1, &read_fds, NULL, NULL, NULL);
        if (FD_ISSET(sdUDP, &read_fds)) //Se è pronto il socket UDP
        {
            memset(&peer_addr, 0, sizeof(peer_addr));
            addr_len = sizeof(peer_addr);

            /*Ricezione messaggio*/
            ret = recvfrom(sdUDP, buffer, sizeof(buffer), 0,
                           (struct sockaddr *)&peer_addr, &addr_len);
            if (ret < 0)
            {
                printf("Errore ricezione UDP\n");
                continue;
            }
            else
                sscanf(buffer, "%s", cmd); //Controllo del comando

            if (strcmp(cmd, "start") == 0) //Se viene ricevuto un comando di start
            {
                sscanf(buffer, "%s %[^:\n]:%d", cmd, peer_ip, &peer_port);
                printf("Ricevuto comando di %s da %s:%d\n", cmd, peer_ip, peer_port);

                /*Invio messaggio di ACK*/
                strcpy(buffer, "ACK");
                ret = sendto(sdUDP, buffer, sizeof(buffer), 0,
                             (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                if (ret < 0)
                {
                    printf("Errore invio UDP\n");
                    continue;
                }
                printf("%s inviato\n", buffer);

                /*Creazione indirizzo del peer*/
                memset(&peer_addr, 0, sizeof(peer_addr));
                peer_addr.sin_family = AF_INET;
                peer_addr.sin_port = htons(peer_port);
                inet_pton(AF_INET, peer_ip, &peer_addr.sin_addr);

                /*Inserimento peer nella lista*/
                printf("Inserimento peer %d\n", peer_port);
                cur_peer = insert(&peer, peer_port);

                /*Creazione del socket TCP per comunicare coi peer*/
                sdTCP = socket(AF_INET, SOCK_STREAM, 0);

                /*Connessione al peer inserito*/
                ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                if (ret < 0)
                {
                    printf("Errore connect\n");
                    continue;
                }

                /*Invio vicini al peer*/
                if (cur_peer == NULL) //Il peer era già presente e non verrà reinserito
                    continue;
                else if (cur_peer->next == NULL) //Se è il primo peer del network
                    sprintf(buffer, "NEIGH 0 0");
                else
                    sprintf(buffer, "NEIGH %d %d", cur_peer->prev->data, cur_peer->next->data);
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
                printf("Inviati neighbor a %d\n", cur_peer->data);
                if (cur_peer->next != NULL) //Se non c'è un unico peer si aggiornano i vicini dei peer
                {
                    /*Si aggiornano i vicini del peer precedente a quello inserito*/
                    cur_peer = cur_peer->prev;
                    peer_addr.sin_port = htons(cur_peer->data);

                    /*Connessione al vicino*/
                    sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                    ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                    if (ret < 0)
                    {
                        printf("Errore connect\n");
                        continue;
                    }

                    /*Comunico i nuovi vicini al peer*/
                    sprintf(buffer, "NEIGH %d %d", cur_peer->prev->data, cur_peer->next->data);
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
                        printf("Errore invio messaggio\n");
                        continue;
                    }
                    close(sdTCP);
                    printf("Aggiornati neighbors di %d\n", cur_peer->data);

                    if (cur_peer != cur_peer->next->next) //Se ci sono più di due elementi in lista
                    {
                        /*Si aggiornano i vicini del peer successivo a quello inserito*/
                        cur_peer = cur_peer->next->next;
                        peer_addr.sin_port = htons(cur_peer->data);

                        /*Connessione al vicino*/
                        sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                        ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                        if (ret < 0)
                        {
                            printf("Errore connect\n");
                            continue;
                        }

                        /*Invio i nuovi vicini al peer*/
                        sprintf(buffer, "NEIGH %d %d", cur_peer->prev->data, cur_peer->next->data);
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
                            printf("Errore invio messaggio\n");
                            continue;
                        }
                        close(sdTCP);
                        printf("Aggiornati neighbors di %d\n", cur_peer->data);
                    }
                }
            }
            else if (strcmp(cmd, "close") == 0) //Se si riceve un messaggio di disconnessione dal network
            {
                sscanf(buffer, "%s %d", cmd, &peer_port);
                printf("Disconnessione di %d dal network\n", peer_port);

                /*Invio messaggio di ACK*/
                strcpy(buffer, "ACK");
                ret = sendto(sdUDP, buffer, sizeof(buffer), 0,
                             (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                if (ret < 0)
                {
                    printf("Errore invio UDP\n");
                    continue;
                }
                printf("%s inviato\n", buffer);

                /*Eliminazione del peer dalla lista*/
                cur_peer = delete (&peer, peer_port);
                printf("%d disconnesso\n", peer_port);
                if (peer == NULL) //Se non sono rimasti peer non si fa niente
                    continue;
                else if (peer->next == NULL) //Se è rimasto un solo peer
                {
                    /*Creazione indirizzo del peer rimasto*/
                    memset(&peer_addr, 0, sizeof(peer_addr));
                    peer_addr.sin_family = AF_INET;
                    peer_addr.sin_port = htons(peer->data);
                    inet_pton(AF_INET, peer_ip, &peer_addr.sin_addr);

                    /*Connessione al vicino*/
                    sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                    ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                    if (ret < 0)
                    {
                        printf("Errore connect\n");
                        continue;
                    }

                    /*Si comunica al peer rimasto che non ha più vicini*/
                    strcpy(buffer, "NEIGH 0 0");
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
                        printf("Errore invio messaggio\n");
                        continue;
                    }
                    close(sdTCP);
                    printf("Aggiornati neighbors di %d\n", peer->data);
                }
                else    //Si aggiornano i vicini dei peer adiacenti a quello eliminato
                {
                    /*Creazione indirizzo del vicino precedente al peer eliminato*/
                    memset(&peer_addr, 0, sizeof(peer_addr));
                    peer_addr.sin_family = AF_INET;
                    peer_addr.sin_port = htons(cur_peer->data);
                    inet_pton(AF_INET, peer_ip, &peer_addr.sin_addr);

                    /*Connessione al vicino*/
                    sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                    ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                    if (ret < 0)
                    {
                        printf("Errore connect\n");
                        continue;
                    }

                    /*Invio dei nuovi vicini al peer*/
                    sprintf(buffer, "NEIGH %d %d", cur_peer->prev->data, cur_peer->next->data);
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
                    printf("Aggiornati neighbor di %d\n", cur_peer->data);

                    /*Creazione indirizzo del vicino successivo al peer eliminato*/
                    cur_peer = cur_peer->next;
                    peer_addr.sin_port = htons(cur_peer->data);

                    /*Connessione al vicino*/
                    sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                    ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                    if (ret < 0)
                    {
                        printf("Errore connect\n");
                        continue;
                    }

                    /*Invio dei nuovi vicini al peer*/
                    sprintf(buffer, "NEIGH %d %d", cur_peer->prev->data, cur_peer->next->data);
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
                        printf("Errore invio messaggio\n");
                        continue;
                    }
                    close(sdTCP);
                    printf("Aggiornati neighbors di %d\n", cur_peer->data);
                }
            }
            else //Se si riceve un comando non valido
                printf("Ricevuto comando non valido\n");
        }

        /*Se ricevo dati da stdin*/
        else
        {
            fgets(buffer, BUF_LEN, stdin);
            sscanf(buffer, "%s", cmd); //Controllo il comando

            if (strcmp(cmd, "help") == 0) //Se si riceve il comando help
            {
                /*Si stampa la guida comandi*/
                printf("\n");
                printf("1) help --> Mostra i dettagli dei comandi\n");
                printf("2) showpeers --> Mostra un elenco dei peer connessi\n");
                printf("3) showneighbor [peer] --> Mostra i neighbor di un peer.\n");
                printf("    Se non viene inserito nessun peer vengono mostrati\n");
                printf("    i neighbor di ciascun peer\n");
                printf("4) esc --> Chiude il DS. Tutti i peer verranno terminati\n");
                printf("\n");
            }
            else if (strcmp(cmd, "showpeers") == 0) //Se si riceve il comando showpeers
            {
                if (peer == NULL) //Se non ci sono peer connessi
                    printf("Nessun peer connesso\n");
                else
                {
                    /*Si scorre la lista stampando tutti i peer*/
                    cur_peer = peer;
                    printf("Lista peer:\n");
                    do
                    {
                        printf("%d\n", cur_peer->data);
                        cur_peer = cur_peer->next;
                    } while (cur_peer != peer && cur_peer != NULL);
                }
            }
            else if (strcmp(cmd, "showneighbor") == 0) //Se si riceve il comando showneighbor
            {
                if (peer == NULL)
                    printf("Non ci sono peer connessi\n");
                else if (strlen(buffer) == 13) //Se non viene specificato un peer
                {
                    /*Si scorre la lista e si stampa ogni peer con annessi i propri vicini*/
                    printf("Lista vicini di ogni peer:\n");
                    cur_peer = peer;
                    do
                    {
                        if (cur_peer->next == NULL)
                        {
                            printf("%d: Nessun vicino\n", cur_peer->data);
                            break;
                        }
                        printf("%d: %d - %d\n", cur_peer->data, cur_peer->prev->data, cur_peer->next->data);
                        cur_peer = cur_peer->next;
                    } while (cur_peer != peer);
                }
                else //Se viene specificato il peer
                {
                    /*Si scorre la lista finchè non si trova il peer e si stampano i vicini*/
                    sscanf(buffer, "%s %d", cmd, &peer_port);
                    cur_peer = peer;
                    do
                    {
                        if (cur_peer->data == peer_port)
                            break;
                        else
                            cur_peer = cur_peer->next;
                    } while (cur_peer != peer);
                    if (cur_peer->data == peer_port) //Se il peer è nella lista
                    {
                        if (cur_peer->next != NULL) //Se il peer ha dei vicini
                        {
                            printf("I vicini di %d sono:\n", peer_port);
                            printf("%d  %d\n", cur_peer->prev->data, cur_peer->next->data);
                        }
                        else //Se il peer non ha vicini
                            printf("Il peer %d non ha vicini\n", peer_port);
                    }
                    else //Se il peer non è nella lista
                        printf("Peer non trovato\n");
                }
            }
            else if (strcmp(cmd, "esc") == 0) //Se si riceve il comando esc
            {
                printf("Invio messaggio di terminazione a tutti i peer\n");
                cur_peer = peer;
                /*Si invia un messaggio di terminazione ad ogni peer*/
                /*Si scorre la lista dei peer*/
                if (peer != NULL)
                {
                    do
                    {
                        /*Creazione indirizzo peer*/
                        memset(&peer_addr, 0, sizeof(peer_addr));
                        peer_addr.sin_family = AF_INET;
                        peer_addr.sin_port = htons(cur_peer->data);
                        inet_pton(AF_INET, peer_ip, &peer_addr.sin_addr);

                        /*Connessione al peer*/
                        sdTCP = socket(AF_INET, SOCK_STREAM, 0);
                        ret = connect(sdTCP, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                        if (ret < 0)
                        {
                            printf("Errore connect\n");
                            continue;
                        }

                        /*Invio messaggio di terminazione al peer*/
                        strcpy(buffer, "CLOSE");
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
                        cur_peer = cur_peer->next;
                    } while (cur_peer != peer && cur_peer != NULL);
                    printf("Peer terminati\n");
                }
                printf("DS terminato\n");
                break; //Terminazione del DS
            }
            else
                printf("Comando non valido\n"); //Se il comando non è valido
        }
    }
    close(sdUDP); //Chiusura socket
    return 0;
}