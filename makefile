libasync: async.o
	ar rcs libasync.a async.o

async.o: async.c
	gcc -c async.c -o async.o

libasync_d: async_d.o
	ar rcs libasync_d.a async_d.o

async_d.o: async.c
	gcc -g -c async.c -o async_d.o -DLOG

tst_d: tst.c libasync_d.a async.h
	gcc -g tst.c -o tst_d -lasync_d -L. -I.
 
tst: tst.c libasync.a async.h
	gcc -s tst.c -o tst -lasync -L. -DLOG -I.