MRUBY_ROOT = ../src/tmp/mruby
MRUBY_LIB := $(MRUBY_ROOT)/lib/libmruby.a
LDFLAGS = $(MRUBY_LIB) ../src/libmrb_redis.a  -lm -lhiredis

INCLUDES = -I$(MRUBY_ROOT)/include -I.. -I../../mruby-redis/src
CFLAGS = $(INCLUDES) -O3 -g -Wall -Werror-implicit-function-declaration

CC = gcc
LL = gcc
AR = ar

PROGS = tester

all : $(PROGS)
	@echo done

tester : tester.c
	gcc $(CFLAGS) -o tester tester.c $(LDFLAGS)

clean :
	rm -f *.o $(PROGS)
