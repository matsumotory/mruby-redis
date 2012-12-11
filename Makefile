MRUBY_ROOT = tmp/mruby

INCLUDES = -I$(MRUBY_ROOT)/include -I$(MRUBY_ROOT)/src -I.
CFLAGS = $(INCLUDES) -O3 -g -Wall -Werror-implicit-function-declaration

CC = gcc
LL = gcc
AR = ar

all : libmruby.a libmrb_redis.a
	@echo done

mrb_redis.o : mrb_redis.c mrb_redis.h
	gcc -c $(CFLAGS) mrb_redis.c

libmrb_redis.a : mrb_redis.o
	$(AR) r libmrb_redis.a mrb_redis.o

tmp/mruby:
	mkdir -p tmp
	cd tmp; git clone git://github.com/mruby/mruby.git

libmruby.a: tmp/mruby
	cd tmp/mruby && make CFLAGS="-O3 -fPIC"

clean :
	rm -f *.o libmrb_redis.a

#   clobber
clobber: clean
	-rm -rf tmp
