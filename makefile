all: peer ds

peer.o: peer.c
	gcc -Wall -c peer.c

peer: peer.o
	gcc -Wall -o peer peer.o

ds.o: ds.c
	gcc -Wall -c ds.c

ds: ds.o
	gcc -Wall -o ds ds.o

clean:
	rm *o peer ds
