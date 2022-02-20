# CovidPeerToPeer
Architettura peer to peer per lo scambio di dati relativi ai casi di Covid-19

Progetto universatorio di sviluppo di un'architettura peer to peer in C.

Per l'esecuzione del progetto si è usata una macchina virtuale contentente Debian 8.

Il file "ds.c" implementa un discovery server a cui i peer si devono registrare e mantiene un registro contente i neighbor di ogni peer.
Il file "peer.c" implementa le funzionalità dei peer.

Il comportamento del discovery server e dei peer è descritto nel file "Specifiche_progetto.pdf" insieme alle altre specifiche del progetto.

I dettagli implementativi sono contenuti nel file "Documentazione progetto.pdf".

L'esecuzione del file "exec.sh" avvia un discovery server e 5 peer.
