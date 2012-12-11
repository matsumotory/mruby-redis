MRUBY_ROOT = ../mruby

INCLUDES = -I$(MRUBY_ROOT)/include -I$(MRUBY_ROOT)/src -I.
CFLAGS = $(INCLUDES) -O3 -g -Wall -Werror-implicit-function-declaration

CC = gcc
LL = gcc
AR = ar

all : libmrb_redis.a
	@echo done

mrb_redis.o : mrb_redis.c mrb_redis.h
	gcc -c $(CFLAGS) mrb_redis.c

libmrb_redis.a : mrb_redis.o
	$(AR) r libmrb_redis.a mrb_redis.o

clean :
	rm -f *.o libmrb_redis.a
