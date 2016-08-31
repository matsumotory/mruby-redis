/*
** mrb_redis - redis class for mruby
**
** Copyright (c) mod_mruby developers 2012-
**
** Permission is hereby granted, free of charge, to any person obtaining
** a copy of this software and associated documentation files (the
** "Software"), to deal in the Software without restriction, including
** without limitation the rights to use, copy, modify, merge, publish,
** distribute, sublicense, and/or sell copies of the Software, and to
** permit persons to whom the Software is furnished to do so, subject to
** the following conditions:
**
** The above copyright notice and this permission notice shall be
** included in all copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
** EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
** MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
** IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
** CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
** TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
** SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
**
** [ MIT license: http://www.opensource.org/licenses/mit-license.php ]
*/

#include <mruby/redis.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <hiredis/hiredis.h>
#include "mruby.h"
#include "mruby/data.h"
#include "mruby/variable.h"
#include "mruby/numeric.h"
#include "mruby/array.h"
#include "mruby/hash.h"
#include "mruby/string.h"
#include "mruby/class.h"
#include "mrb_redis.h"
#include <errno.h>
#include <mruby/error.h>
#include <mruby/throw.h>

#define DONE mrb_gc_arena_restore(mrb, 0);

#define CREATE_REDIS_COMMAND_ARG1(argv, lens, cmd, arg1)                                                               \
  argv[0] = cmd;                                                                                                       \
  argv[1] = RSTRING_PTR(arg1);                                                                                         \
  lens[0] = sizeof(cmd) - 1;                                                                                           \
  lens[1] = RSTRING_LEN(arg1)
#define CREATE_REDIS_COMMAND_ARG2(argv, lens, cmd, arg1, arg2)                                                         \
  argv[0] = cmd;                                                                                                       \
  argv[1] = RSTRING_PTR(arg1);                                                                                         \
  argv[2] = RSTRING_PTR(arg2);                                                                                         \
  lens[0] = sizeof(cmd) - 1;                                                                                           \
  lens[1] = RSTRING_LEN(arg1);                                                                                         \
  lens[2] = RSTRING_LEN(arg2)
#define CREATE_REDIS_COMMAND_ARG3(argv, lens, cmd, arg1, arg2, arg3)                                                   \
  argv[0] = cmd;                                                                                                       \
  argv[1] = RSTRING_PTR(arg1);                                                                                         \
  argv[2] = RSTRING_PTR(arg2);                                                                                         \
  argv[3] = RSTRING_PTR(arg3);                                                                                         \
  lens[0] = sizeof(cmd) - 1;                                                                                           \
  lens[1] = RSTRING_LEN(arg1);                                                                                         \
  lens[2] = RSTRING_LEN(arg2);                                                                                         \
  lens[3] = RSTRING_LEN(arg3)

static void redisContext_free(mrb_state *mrb, void *p)
{
  redisFree(p);
}

static const struct mrb_data_type redisContext_type = {
    "redisContext", redisContext_free,
};

static inline void mrb_redis_check_error(redisContext *context, mrb_state *mrb)
{
  if (context->err != 0) {
    if (errno != 0) {
      mrb_sys_fail(mrb, context->errstr);
    } else {
      switch (context->err) {
      case REDIS_ERR_EOF:
        mrb_raise(mrb, E_EOF_ERROR, context->errstr);
        break;
      case REDIS_ERR_PROTOCOL:
        mrb_raise(mrb, E_REDIS_ERR_PROTOCOL, context->errstr);
        break;
      case REDIS_ERR_OOM:
        mrb_raise(mrb, E_REDIS_ERR_OOM, context->errstr);
        break;
      default:
        mrb_raise(mrb, E_REDIS_ERROR, context->errstr);
      }
    }
  }
}

static mrb_value mrb_redis_connect(mrb_state *mrb, mrb_value self)
{
  mrb_value host, port;
  mrb_int timeout = 1;
  struct timeval timeout_struct = {timeout, 0};

  redisContext *rc = (redisContext *)DATA_PTR(self);
  if (rc) {
    mrb_free(mrb, rc);
  }
  DATA_TYPE(self) = &redisContext_type;
  DATA_PTR(self) = NULL;

  if (mrb_get_args(mrb, "oo|i", &host, &port, &timeout) == 3) {
    timeout_struct.tv_sec = timeout;
  }

  rc = redisConnectWithTimeout(mrb_str_to_cstr(mrb, host), mrb_fixnum(port), timeout_struct);
  if (rc->err) {
    mrb_raise(mrb, E_REDIS_ERROR, "redis connection failed.");
  }

  DATA_PTR(self) = rc;

  return self;
}

static mrb_value mrb_redis_select(mrb_state *mrb, mrb_value self)
{
  mrb_value database;
  redisReply *rs;

  redisContext *rc = DATA_PTR(self);
  mrb_get_args(mrb, "o", &database);

  if (mrb_type(database) != MRB_TT_FIXNUM) {
    mrb_raisef(mrb, E_TYPE_ERROR, "type mismatch: %S given", database);
  }

  rs = redisCommand(rc, "SELECT %d", mrb_fixnum(database));
  freeReplyObject(rs);

  return self;
}

static mrb_value mrb_redis_set(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val;
  redisReply *rs;
  const char *argv[3];
  size_t lens[3];
  redisContext *rc = DATA_PTR(self);

  mrb_get_args(mrb, "oo", &key, &val);

  CREATE_REDIS_COMMAND_ARG2(argv, lens, "SET", key, val);

  rs = redisCommandArgv(rc, 3, argv, lens);
  freeReplyObject(rs);

  return self;
}

static mrb_value mrb_redis_get(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rs;

  mrb_get_args(mrb, "o", &key);

  CREATE_REDIS_COMMAND_ARG1(argv, lens, "GET", key);

  rs = redisCommandArgv(rc, 2, argv, lens);
  if (rs->type == REDIS_REPLY_STRING) {
    mrb_value str = mrb_str_new(mrb, rs->str, rs->len);
    freeReplyObject(rs);
    return str;
  } else {
    freeReplyObject(rs);
    return mrb_nil_value();
  }
}

static mrb_value mrb_redis_keys(mrb_state *mrb, mrb_value self)
{
  mrb_value pattern, array = mrb_nil_value();
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "o", &pattern);
  rr = redisCommand(rc, "KEYS %s", mrb_str_to_cstr(mrb, pattern));
  if (rr->type == REDIS_REPLY_ARRAY) {
    if (rr->elements > 0) {
      int i;

      array = mrb_ary_new(mrb);
      for (i = 0; i < rr->elements; i++) {
        mrb_ary_push(mrb, array, mrb_str_new_cstr(mrb, rr->element[i]->str));
      }
    }
  }
  freeReplyObject(rr);
  return array;
}

static mrb_value mrb_redis_exists(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int counter;
  const char *argv[2];
  size_t lens[2];
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);

  CREATE_REDIS_COMMAND_ARG1(argv, lens, "EXISTS", key);

  rr = redisCommandArgv(rc, 2, argv, lens);
  counter = rr->integer;
  freeReplyObject(rr);

  return counter ? mrb_true_value() : mrb_false_value();
}

static mrb_value mrb_redis_expire(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int expire, counter;
  redisContext *rc = DATA_PTR(self);
  mrb_value val;
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  mrb_get_args(mrb, "oi", &key, &expire);
  val = mrb_fixnum_to_str(mrb, mrb_fixnum_value(expire), 10);

  CREATE_REDIS_COMMAND_ARG2(argv, lens, "EXPIRE", key, val);

  rr = redisCommandArgv(rc, 3, argv, lens);
  counter = rr->integer;
  freeReplyObject(rr);

  return mrb_bool_value(counter == 1);
}

static mrb_value mrb_redis_flushdb(mrb_state *mrb, mrb_value self)
{
  redisContext *rc = DATA_PTR(self);
  mrb_value str;
  redisReply *rs = redisCommand(rc, "FLUSHDB");

  str = mrb_str_new(mrb, rs->str, rs->len);
  freeReplyObject(rs);
  return str;
}

static mrb_value mrb_redis_flushall(mrb_state *mrb, mrb_value self)
{
  redisContext *rc = DATA_PTR(self);
  mrb_value str;
  redisReply *rs = redisCommand(rc, "FLUSHALL");

  str = mrb_str_new(mrb, rs->str, rs->len);
  freeReplyObject(rs);
  return str;
}

static mrb_value mrb_redis_randomkey(mrb_state *mrb, mrb_value self)
{
  redisContext *rc = DATA_PTR(self);

  redisReply *rs = redisCommand(rc, "RANDOMKEY");
  if (rs->type == REDIS_REPLY_STRING) {
    mrb_value str = mrb_str_new(mrb, rs->str, rs->len);
    freeReplyObject(rs);
    return str;
  } else {
    freeReplyObject(rs);
    return mrb_nil_value();
  }
}

static mrb_value mrb_redis_del(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rs;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "DEL", key);

  rs = redisCommandArgv(rc, 2, argv, lens);
  freeReplyObject(rs);

  return self;
}

static mrb_value mrb_redis_incr(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int counter;
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "INCR", key);
  rr = redisCommandArgv(rc, 2, argv, lens);
  counter = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(counter);
}

static mrb_value mrb_redis_decr(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int counter;
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "DECR", key);
  rr = redisCommandArgv(rc, 2, argv, lens);
  counter = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(counter);
}

static mrb_value mrb_redis_incrby(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int val, counter;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;
  mrb_value val_str;

  mrb_get_args(mrb, "oi", &key, &val);
  val_str = mrb_fixnum_to_str(mrb, mrb_fixnum_value(val), 10);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "INCRBY", key, val_str);
  rr = redisCommandArgv(rc, 3, argv, lens);
  counter = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(counter);
}

static mrb_value mrb_redis_decrby(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int val, counter;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;
  mrb_value val_str;

  mrb_get_args(mrb, "oi", &key, &val);
  val_str = mrb_fixnum_to_str(mrb, mrb_fixnum_value(val), 10);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "DECRBY", key, val_str);
  rr = redisCommandArgv(rc, 3, argv, lens);
  counter = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(counter);
}

static mrb_value mrb_redis_llen(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int integer;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  rr = redisCommand(rc, "LLEN %s", mrb_str_to_cstr(mrb, key));
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_rpush(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val;
  mrb_int integer;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  mrb_get_args(mrb, "oo", &key, &val);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "RPUSH", key, val);
  rr = redisCommandArgv(rc, 3, argv, lens);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_lpush(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val;
  mrb_int integer;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  mrb_get_args(mrb, "oo", &key, &val);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "LPUSH", key, val);
  rr = redisCommandArgv(rc, 3, argv, lens);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_rpop(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "RPOP", key);
  rr = redisCommandArgv(rc, 2, argv, lens);
  if (rr->type == REDIS_REPLY_STRING) {
    mrb_value str = mrb_str_new(mrb, rr->str, rr->len);
    freeReplyObject(rr);
    return str;
  } else {
    freeReplyObject(rr);
    return mrb_nil_value();
  }
}

static mrb_value mrb_redis_lpop(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "LPOP", key);
  rr = redisCommandArgv(rc, 2, argv, lens);
  if (rr->type == REDIS_REPLY_STRING) {
    mrb_value str = mrb_str_new(mrb, rr->str, rr->len);
    freeReplyObject(rr);
    return str;
  } else {
    freeReplyObject(rr);
    return mrb_nil_value();
  }
}

static mrb_value mrb_redis_lrange(mrb_state *mrb, mrb_value self)
{
  int i;
  mrb_value list, array;
  mrb_int arg1, arg2;
  redisContext *rc = DATA_PTR(self);
  mrb_value val_str1, val_str2;
  const char *argv[4];
  size_t lens[4];
  redisReply *rr;

  mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
  val_str1 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg1), 10);
  val_str2 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg2), 10);

  CREATE_REDIS_COMMAND_ARG3(argv, lens, "LRANGE", list, val_str1, val_str2);
  rr = redisCommandArgv(rc, 4, argv, lens);
  if (rr->type == REDIS_REPLY_ARRAY) {
    array = mrb_ary_new(mrb);
    for (i = 0; i < rr->elements; i++) {
      mrb_ary_push(mrb, array, mrb_str_new(mrb, rr->element[i]->str, rr->element[i]->len));
    }
  } else {
    freeReplyObject(rr);
    return mrb_nil_value();
  }

  freeReplyObject(rr);

  return array;
}

static mrb_value mrb_redis_ltrim(mrb_state *mrb, mrb_value self)
{
  mrb_value list;
  mrb_int arg1, arg2, integer;
  redisContext *rc = DATA_PTR(self);
  mrb_value val_str1, val_str2;
  const char *argv[4];
  size_t lens[4];
  redisReply *rr;

  mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
  val_str1 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg1), 10);
  val_str2 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg2), 10);
  CREATE_REDIS_COMMAND_ARG3(argv, lens, "LTRIM", list, val_str1, val_str2);
  rr = redisCommandArgv(rc, 4, argv, lens);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_lindex(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int pos;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "oi", &key, &pos);
  rr = redisCommand(rc, "LINDEX %s %d", RSTRING_PTR(key), pos);
  if (rr->type == REDIS_REPLY_STRING) {
    mrb_value str = mrb_str_new(mrb, rr->str, rr->len);
    freeReplyObject(rr);
    return str;
  } else {
    freeReplyObject(rr);
    return mrb_nil_value();
  }
}

static mrb_value mrb_redis_sadd(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val;
  mrb_int integer;
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  redisContext *rc = DATA_PTR(self);

  mrb_get_args(mrb, "oo", &key, &val);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "SADD", key, val);

  rr = redisCommandArgv(rc, 3, argv, lens);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_sismember(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val;
  mrb_int integer;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  mrb_get_args(mrb, "oo", &key, &val);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "SISMEMBER", key, val);
  rr = redisCommandArgv(rc, 3, argv, lens);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_smembers(mrb_state *mrb, mrb_value self)
{
  int i;
  mrb_value array, key;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  rr = redisCommand(rc, "SMEMBERS %s", mrb_str_to_cstr(mrb, key));
  if (rr->type == REDIS_REPLY_ARRAY) {
    array = mrb_ary_new(mrb);
    for (i = 0; i < rr->elements; i++) {
      mrb_ary_push(mrb, array, mrb_str_new(mrb, rr->element[i]->str, rr->element[i]->len));
    }
  } else {
    freeReplyObject(rr);
    return mrb_nil_value();
  }

  freeReplyObject(rr);

  return array;
}

static mrb_value mrb_redis_scard(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int integer;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  rr = redisCommand(rc, "SCARD %s", mrb_str_to_cstr(mrb, key));
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_spop(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "SPOP", key);
  rr = redisCommandArgv(rc, 2, argv, lens);
  if (rr->type == REDIS_REPLY_STRING) {
    mrb_value str = mrb_str_new(mrb, rr->str, rr->len);
    freeReplyObject(rr);
    return str;
  } else {
    freeReplyObject(rr);
    return mrb_nil_value();
  }
}

static mrb_value mrb_redis_hset(mrb_state *mrb, mrb_value self)
{
  mrb_value key, field, val;
  redisContext *rc = DATA_PTR(self);
  mrb_int integer;
  const char *argv[4];
  size_t lens[4];
  redisReply *rs;

  mrb_get_args(mrb, "ooo", &key, &field, &val);
  CREATE_REDIS_COMMAND_ARG3(argv, lens, "HSET", key, field, val);
  rs = redisCommandArgv(rc, 4, argv, lens);
  integer = rs->integer;
  freeReplyObject(rs);

  return integer ? mrb_true_value() : mrb_false_value();
}

static mrb_value mrb_redis_hget(mrb_state *mrb, mrb_value self)
{
  mrb_value key, field;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rs;

  mrb_get_args(mrb, "oo", &key, &field);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "HGET", key, field);
  rs = redisCommandArgv(rc, 3, argv, lens);
  if (rs->type == REDIS_REPLY_STRING) {
    mrb_value str = mrb_str_new(mrb, rs->str, rs->len);
    freeReplyObject(rs);
    return str;
  } else {
    freeReplyObject(rs);
    return mrb_nil_value();
  }
}

static mrb_value mrb_redis_hgetall(mrb_state *mrb, mrb_value self)
{
  mrb_value obj, hash = mrb_nil_value();
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &obj);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "HGETALL", obj);
  rr = redisCommandArgv(rc, 2, argv, lens);
  if (rr->type == REDIS_REPLY_ARRAY) {
    if (rr->elements > 0) {
      int i;

      hash = mrb_hash_new(mrb);
      for (i = 0; i < rr->elements; i += 2) {
        mrb_hash_set(mrb, hash, mrb_str_new(mrb, rr->element[i]->str, rr->element[i]->len),
                     mrb_str_new(mrb, rr->element[i + 1]->str, rr->element[i + 1]->len));
      }
    }
  }
  freeReplyObject(rr);
  return hash;
}

static mrb_value mrb_redis_hdel(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val;
  mrb_int integer;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  mrb_get_args(mrb, "oo", &key, &val);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "HDEL", key, val);
  rr = redisCommandArgv(rc, 3, argv, lens);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_hexists(mrb_state *mrb, mrb_value self)
{
  mrb_value key, field;
  mrb_int counter;
  const char *argv[3];
  size_t lens[3];
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "oo", &key, &field);

  CREATE_REDIS_COMMAND_ARG2(argv, lens, "HEXISTS", key, field);

  rr = redisCommandArgv(rc, 3, argv, lens);
  counter = rr->integer;
  freeReplyObject(rr);

  return counter ? mrb_true_value() : mrb_false_value();
}

static mrb_value mrb_redis_hkeys(mrb_state *mrb, mrb_value self)
{
  mrb_value key, array = mrb_nil_value();
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "HKEYS", key);
  rr = redisCommandArgv(rc, 2, argv, lens);
  if (rr->type == REDIS_REPLY_ARRAY) {
    if (rr->elements > 0) {
      int i;

      array = mrb_ary_new(mrb);
      for (i = 0; i < rr->elements; i++) {
        mrb_ary_push(mrb, array, mrb_str_new(mrb, rr->element[i]->str, rr->element[i]->len));
      }
    }
  }
  freeReplyObject(rr);
  return array;
}

static mrb_value mrb_redis_hmget(mrb_state *mrb, mrb_value self)
{
  mrb_value *mrb_argv;
  mrb_int argc = 0;
  int i;

  mrb_get_args(mrb, "*", &mrb_argv, &argc);
  argc++;

  const char *argv[argc];
  size_t argvlen[argc];
  argv[0] = "HMGET";
  argvlen[0] = sizeof("HMGET") - 1;

  int ai = mrb_gc_arena_save(mrb);
  mrb_int argc_current;
  for (argc_current = 1; argc_current < argc; argc_current++) {
    mrb_value curr = mrb_str_to_str(mrb, mrb_argv[argc_current - 1]);
    argv[argc_current] = RSTRING_PTR(curr);
    argvlen[argc_current] = RSTRING_LEN(curr);
    mrb_gc_arena_restore(mrb, ai);
  }

  mrb_value array = mrb_nil_value();
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;
  rr = redisCommandArgv(rc, argc, argv, argvlen);
  if (rr->type == REDIS_REPLY_ARRAY) {
    if (rr->elements > 0) {
      array = mrb_ary_new(mrb);

      for (i = 0; i < rr->elements; i++) {
        if (rr->element[i]->len > 0) {
          mrb_ary_push(mrb, array, mrb_str_new(mrb, rr->element[i]->str, rr->element[i]->len));
        } else {
          mrb_ary_push(mrb, array, mrb_nil_value());
        }
      }
    }
  } else {
    mrb_raise(mrb, E_ARGUMENT_ERROR, rr->str);
  }

  freeReplyObject(rr);
  return array;
}

static mrb_value mrb_redis_hmset(mrb_state *mrb, mrb_value self)
{
  mrb_value *mrb_argv;
  mrb_int argc = 0;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "*", &mrb_argv, &argc);
  argc++;

  const char *argv[argc];
  size_t argvlen[argc];
  argv[0] = "HMSET";
  argvlen[0] = sizeof("HMSET") - 1;

  int ai = mrb_gc_arena_save(mrb);
  mrb_int argc_current;
  for (argc_current = 1; argc_current < argc; argc_current++) {
    mrb_value curr = mrb_str_to_str(mrb, mrb_argv[argc_current - 1]);
    argv[argc_current] = RSTRING_PTR(curr);
    argvlen[argc_current] = RSTRING_LEN(curr);
    mrb_gc_arena_restore(mrb, ai);
  }

  rr = redisCommandArgv(rc, argc, argv, argvlen);
  if (rr->type == REDIS_REPLY_STATUS && rr != NULL) {
    freeReplyObject(rr);
  } else {
    mrb_raise(mrb, E_ARGUMENT_ERROR, rr->str);
  }

  return self;
}

static mrb_value mrb_redis_hvals(mrb_state *mrb, mrb_value self)
{
  mrb_value key, array = mrb_nil_value();
  redisContext *rc = DATA_PTR(self);
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "HVALS", key);
  rr = redisCommandArgv(rc, 2, argv, lens);
  if (rr->type == REDIS_REPLY_ARRAY) {
    if (rr->elements > 0) {
      int i;

      array = mrb_ary_new(mrb);
      for (i = 0; i < rr->elements; i++) {
        mrb_ary_push(mrb, array, mrb_str_new(mrb, rr->element[i]->str, rr->element[i]->len));
      }
    }
  }
  freeReplyObject(rr);
  return array;
}

static mrb_value mrb_redis_ttl(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  redisContext *rc = DATA_PTR(self);
  mrb_int integer;
  const char *argv[2];
  size_t lens[2];
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, "TTL", key);
  rr = redisCommandArgv(rc, 2, argv, lens);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_zadd(mrb_state *mrb, mrb_value self)
{
  mrb_value key, member;
  mrb_float score;
  redisContext *rc = DATA_PTR(self);
  mrb_value score_str;
  const char *argv[4];
  size_t lens[4];
  redisReply *rs;

  mrb_get_args(mrb, "ofo", &key, &score, &member);
  score_str = mrb_float_to_str(mrb, mrb_float_value(mrb, score), "%f");
  CREATE_REDIS_COMMAND_ARG3(argv, lens, "ZADD", key, score_str, member);
  rs = redisCommandArgv(rc, 4, argv, lens);
  freeReplyObject(rs);

  return self;
}

static mrb_value mrb_redis_zcard(mrb_state *mrb, mrb_value self)
{
  mrb_value key;
  mrb_int integer;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "o", &key);
  rr = redisCommand(rc, "ZCARD %s", mrb_str_to_cstr(mrb, key));
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_basic_zrange(mrb_state *mrb, mrb_value self, const char *cmd)
{
  int i;
  mrb_value list, array;
  mrb_int arg1, arg2;
  redisContext *rc = DATA_PTR(self);
  mrb_value arg1_str, arg2_str;
  const char *argv[4];
  size_t lens[4];
  redisReply *rr;

  mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
  arg1_str = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg1), 10);
  arg2_str = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg2), 10);
  CREATE_REDIS_COMMAND_ARG3(argv, lens, "ZADD", list, arg1_str, arg2_str);
  rr = redisCommandArgv(rc, 4, argv, lens);
  if (rr->type == REDIS_REPLY_ARRAY) {
    array = mrb_ary_new(mrb);
    for (i = 0; i < rr->elements; i++) {
      mrb_ary_push(mrb, array, mrb_str_new(mrb, rr->element[i]->str, rr->element[i]->len));
    }
  } else {
    freeReplyObject(rr);
    return mrb_nil_value();
  }

  freeReplyObject(rr);

  return array;
}

static mrb_value mrb_redis_zrange(mrb_state *mrb, mrb_value self)
{
  return mrb_redis_basic_zrange(mrb, self, "ZRANGE");
}

static mrb_value mrb_redis_zrevrange(mrb_state *mrb, mrb_value self)
{
  return mrb_redis_basic_zrange(mrb, self, "ZREVRANGE");
}

static mrb_value mrb_redis_basic_zrank(mrb_state *mrb, mrb_value self, const char *cmd)
{
  mrb_value key, member;
  mrb_int rank;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  mrb_get_args(mrb, "oo", &key, &member);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, cmd, key, member);
  rr = redisCommandArgv(rc, 3, argv, lens);
  rank = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(rank);
}

static mrb_value mrb_redis_zrank(mrb_state *mrb, mrb_value self)
{
  return mrb_redis_basic_zrank(mrb, self, "ZRANK");
}

static mrb_value mrb_redis_zrevrank(mrb_state *mrb, mrb_value self)
{
  return mrb_redis_basic_zrank(mrb, self, "ZREVRANK");
}

static mrb_value mrb_redis_zscore(mrb_state *mrb, mrb_value self)
{
  mrb_value key, member, score;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  mrb_get_args(mrb, "oo", &key, &member);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "ZSCORE", key, member);
  rr = redisCommandArgv(rc, 3, argv, lens);
  score = mrb_str_new(mrb, rr->str, rr->len);
  freeReplyObject(rr);

  return score;
}

static mrb_value mrb_redis_pub(mrb_state *mrb, mrb_value self)
{
  mrb_value channel, msg, res;
  redisContext *rc = DATA_PTR(self);
  const char *argv[3];
  size_t lens[3];
  redisReply *rr;

  mrb_get_args(mrb, "oo", &channel, &msg);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, "PUBLISH", channel, msg);
  rr = redisCommandArgv(rc, 3, argv, lens);

  if (rr->type == REDIS_REPLY_INTEGER) {
    res = mrb_fixnum_value(rr->integer);
  } else {
    res = mrb_nil_value();
  }

  freeReplyObject(rr);
  return res;
}

static mrb_value mrb_redis_pfadd(mrb_state *mrb, mrb_value self)
{
  mrb_value key, *mrb_rest_argv;
  mrb_int argc = 0, rest_argc = 0;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;
  mrb_int integer;

  mrb_get_args(mrb, "o*", &key, &mrb_rest_argv, &rest_argc);
  argc = rest_argc + 2;

  const char *argv[argc];
  size_t argvlen[argc];
  CREATE_REDIS_COMMAND_ARG1(argv, argvlen, "PFADD", key);

  if (argc > 2) {
    int ai = mrb_gc_arena_save(mrb);
    mrb_int argc_current;
    for (argc_current = 2; argc_current < argc; argc_current++) {
      mrb_value curr = mrb_str_to_str(mrb, mrb_rest_argv[argc_current - 2]);
      argv[argc_current] = RSTRING_PTR(curr);
      argvlen[argc_current] = RSTRING_LEN(curr);
      mrb_gc_arena_restore(mrb, ai);
    }
  }

  rr = redisCommandArgv(rc, argc, argv, argvlen);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_pfcount(mrb_state *mrb, mrb_value self)
{
  mrb_value key, *mrb_rest_argv;
  mrb_int argc = 0, rest_argc = 0;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;
  mrb_int integer;

  mrb_get_args(mrb, "o*", &key, &mrb_rest_argv, &rest_argc);
  argc = rest_argc + 2;

  const char *argv[argc];
  size_t argvlen[argc];
  CREATE_REDIS_COMMAND_ARG1(argv, argvlen, "PFCOUNT", key);

  if (argc > 2) {
    int ai = mrb_gc_arena_save(mrb);
    mrb_int argc_current;
    for (argc_current = 2; argc_current < argc; argc_current++) {
      mrb_value curr = mrb_str_to_str(mrb, mrb_rest_argv[argc_current - 2]);
      argv[argc_current] = RSTRING_PTR(curr);
      argvlen[argc_current] = RSTRING_LEN(curr);
      mrb_gc_arena_restore(mrb, ai);
    }
  }

  rr = redisCommandArgv(rc, argc, argv, argvlen);
  integer = rr->integer;
  freeReplyObject(rr);

  return mrb_fixnum_value(integer);
}

static mrb_value mrb_redis_pfmerge(mrb_state *mrb, mrb_value self)
{
  mrb_value dest_struct, src_struct, *mrb_rest_argv;
  mrb_int argc = 0, rest_argc = 0;
  redisContext *rc = DATA_PTR(self);
  redisReply *rr;

  mrb_get_args(mrb, "oo*", &dest_struct, &src_struct, &mrb_rest_argv, &rest_argc);
  argc = rest_argc + 3;

  const char *argv[argc];
  size_t argvlen[argc];
  CREATE_REDIS_COMMAND_ARG2(argv, argvlen, "PFMERGE", dest_struct, src_struct);

  if (argc > 3) {
    int ai = mrb_gc_arena_save(mrb);
    mrb_int argc_current;
    for (argc_current = 3; argc_current < argc; argc_current++) {
      mrb_value curr = mrb_str_to_str(mrb, mrb_rest_argv[argc_current - 3]);
      argv[argc_current] = RSTRING_PTR(curr);
      argvlen[argc_current] = RSTRING_LEN(curr);
      mrb_gc_arena_restore(mrb, ai);
    }
  }

  rr = redisCommandArgv(rc, argc, argv, argvlen);
  if (rr->type == REDIS_REPLY_STRING) {
    mrb_value str = mrb_str_new(mrb, rr->str, rr->len);
    freeReplyObject(rr);
    return str;
  } else {
    freeReplyObject(rr);
    return mrb_nil_value();
  }
}

static mrb_value mrb_redis_close(mrb_state *mrb, mrb_value self)
{
  redisContext *rc = DATA_PTR(self);

  redisFree(rc);

  DATA_PTR(self) = NULL;
  DATA_TYPE(self) = NULL;

  return mrb_nil_value();
}

static inline mrb_value mrb_redis_get_ary_reply(redisReply *reply, mrb_state *mrb);

static inline mrb_value mrb_redis_get_reply(redisReply *reply, mrb_state *mrb)
{
  switch (reply->type) {
  case REDIS_REPLY_STRING:
    return mrb_str_new(mrb, reply->str, reply->len);
    break;
  case REDIS_REPLY_ARRAY:
    return mrb_redis_get_ary_reply(reply, mrb);
    break;
  case REDIS_REPLY_INTEGER: {
    if (FIXABLE(reply->integer))
      return mrb_fixnum_value(reply->integer);
    else
      return mrb_float_value(mrb, reply->integer);
  } break;
  case REDIS_REPLY_NIL:
    return mrb_nil_value();
    break;
  case REDIS_REPLY_STATUS: {
    mrb_sym status = mrb_intern(mrb, reply->str, reply->len);
    return mrb_symbol_value(status);
  } break;
  case REDIS_REPLY_ERROR: {
    mrb_value err = mrb_str_new(mrb, reply->str, reply->len);
    return mrb_exc_new_str(mrb, E_REDIS_REPLY_ERROR, err);
  } break;
  default:
    mrb_raise(mrb, E_REDIS_ERROR, "unknown reply type");
  }
}

static inline mrb_value mrb_redis_get_ary_reply(redisReply *reply, mrb_state *mrb)
{
  mrb_value ary = mrb_ary_new_capa(mrb, reply->elements);
  int ai = mrb_gc_arena_save(mrb);
  size_t element_couter;
  for (element_couter = 0; element_couter < reply->elements; element_couter++) {
    mrb_value element = mrb_redis_get_reply(reply->element[element_couter], mrb);
    mrb_ary_push(mrb, ary, element);
    mrb_gc_arena_restore(mrb, ai);
  }
  return ary;
}

static mrb_value mrb_redisAppendCommandArgv(mrb_state *mrb, mrb_value self)
{
  mrb_sym command;
  mrb_value *mrb_argv;
  mrb_int argc = 0;

  mrb_get_args(mrb, "n*", &command, &mrb_argv, &argc);
  argc++;

  const char *argv[argc];
  size_t argvlen[argc];
  mrb_int command_len;
  argv[0] = mrb_sym2name_len(mrb, command, &command_len);
  argvlen[0] = command_len;

  mrb_int argc_current;
  for (argc_current = 1; argc_current < argc; argc_current++) {
    mrb_value curr = mrb_str_to_str(mrb, mrb_argv[argc_current - 1]);
    argv[argc_current] = RSTRING_PTR(curr);
    argvlen[argc_current] = RSTRING_LEN(curr);
  }

  mrb_sym queue_counter_sym = mrb_intern_lit(mrb, "queue_counter");
  mrb_value queue_counter_val = mrb_iv_get(mrb, self, queue_counter_sym);
  mrb_int queue_counter = 1;
  if (mrb_fixnum_p(queue_counter_val)) {
    queue_counter = mrb_fixnum(queue_counter_val);
    if (mrb_int_add_overflow(queue_counter, 1, &queue_counter)) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "integer addition would overflow");
    }
  }

  redisContext *context = (redisContext *)DATA_PTR(self);
  errno = 0;
  int rc = redisAppendCommandArgv(context, argc, argv, argvlen);
  if (rc == REDIS_OK) {
    mrb_iv_set(mrb, self, queue_counter_sym, mrb_fixnum_value(queue_counter));
  } else {
    mrb_redis_check_error(context, mrb);
  }

  return self;
}

static mrb_value mrb_redisGetReply(mrb_state *mrb, mrb_value self)
{
  mrb_sym queue_counter_sym = mrb_intern_lit(mrb, "queue_counter");
  mrb_value queue_counter_val = mrb_iv_get(mrb, self, queue_counter_sym);
  mrb_int queue_counter = -1;
  if (mrb_fixnum_p(queue_counter_val)) {
    queue_counter = mrb_fixnum(queue_counter_val);
  }

  redisContext *context = (redisContext *)DATA_PTR(self);
  redisReply *reply = NULL;
  mrb_value reply_val = self;
  errno = 0;
  int rc = redisGetReply(context, (void **)&reply);
  if (rc == REDIS_OK && reply != NULL) {
    struct mrb_jmpbuf *prev_jmp = mrb->jmp;
    struct mrb_jmpbuf c_jmp;

    MRB_TRY(&c_jmp)
    {
      mrb->jmp = &c_jmp;
      reply_val = mrb_redis_get_reply(reply, mrb);
      if (queue_counter > 1) {
        mrb_iv_set(mrb, self, queue_counter_sym, mrb_fixnum_value(--queue_counter));
      } else {
        mrb_iv_remove(mrb, self, queue_counter_sym);
      }
      mrb->jmp = prev_jmp;
    }
    MRB_CATCH(&c_jmp)
    {
      mrb->jmp = prev_jmp;
      freeReplyObject(reply);
      MRB_THROW(mrb->jmp);
    }
    MRB_END_EXC(&c_jmp);

    freeReplyObject(reply);
  } else {
    mrb_redis_check_error(context, mrb);
  }

  return reply_val;
}

static mrb_value mrb_redisGetBulkReply(mrb_state *mrb, mrb_value self)
{
  mrb_value queue_counter_val = mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "queue_counter"));

  if (!mrb_fixnum_p(queue_counter_val))
    mrb_raise(mrb, E_RUNTIME_ERROR, "nothing queued yet");

  mrb_int queue_counter = mrb_fixnum(queue_counter_val);

  mrb_value bulk_reply = mrb_ary_new_capa(mrb, queue_counter);
  int ai = mrb_gc_arena_save(mrb);

  do {
    mrb_value reply = mrb_redisGetReply(mrb, self);
    mrb_ary_push(mrb, bulk_reply, reply);
    mrb_gc_arena_restore(mrb, ai);
  } while (--queue_counter > 0);

  return bulk_reply;
}

void mrb_mruby_redis_gem_init(mrb_state *mrb)
{
  struct RClass *redis, *redis_error;

  redis = mrb_define_class(mrb, "Redis", mrb->object_class);
  MRB_SET_INSTANCE_TT(redis, MRB_TT_DATA);

  redis_error = mrb_define_class_under(mrb, redis, "ConnectionError", E_RUNTIME_ERROR);
  mrb_define_class_under(mrb, redis, "ReplyError", redis_error);
  mrb_define_class_under(mrb, redis, "ProtocolError", redis_error);
  mrb_define_class_under(mrb, redis, "OOMError", redis_error);

  mrb_define_method(mrb, redis, "initialize", mrb_redis_connect, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "select", mrb_redis_select, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "set", mrb_redis_set, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "get", mrb_redis_get, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "exists?", mrb_redis_exists, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "expire", mrb_redis_expire, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "flushall", mrb_redis_flushall, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "flushdb", mrb_redis_flushdb, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "randomkey", mrb_redis_randomkey, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "[]=", mrb_redis_set, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "[]", mrb_redis_get, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "keys", mrb_redis_keys, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "del", mrb_redis_del, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "incr", mrb_redis_incr, MRB_ARGS_OPT(1));
  mrb_define_method(mrb, redis, "decr", mrb_redis_decr, MRB_ARGS_OPT(1));
  mrb_define_method(mrb, redis, "incrby", mrb_redis_incrby, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "decrby", mrb_redis_decrby, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "llen", mrb_redis_llen, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "rpush", mrb_redis_rpush, MRB_ARGS_OPT(2));
  mrb_define_method(mrb, redis, "lpush", mrb_redis_lpush, MRB_ARGS_OPT(2));
  mrb_define_method(mrb, redis, "rpop", mrb_redis_rpop, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "lpop", mrb_redis_lpop, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "lrange", mrb_redis_lrange, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "ltrim", mrb_redis_ltrim, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "lindex", mrb_redis_lindex, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "sadd", mrb_redis_sadd, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "sismember", mrb_redis_sismember, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "smembers", mrb_redis_smembers, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "scard", mrb_redis_scard, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "spop", mrb_redis_spop, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "hset", mrb_redis_hset, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, redis, "hget", mrb_redis_hget, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "hgetall", mrb_redis_hgetall, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "hdel", mrb_redis_hdel, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "hexists?", mrb_redis_hexists, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "hkeys", mrb_redis_hkeys, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "hmset", mrb_redis_hmset, (MRB_ARGS_REQ(3) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "hmget", mrb_redis_hmget, (MRB_ARGS_REQ(2) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "hvals", mrb_redis_hvals, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "ttl", mrb_redis_ttl, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "zadd", mrb_redis_zadd, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, redis, "zcard", mrb_redis_zcard, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "zrange", mrb_redis_zrange, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, redis, "zrevrange", mrb_redis_zrevrange, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, redis, "zrank", mrb_redis_zrank, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "zrevrank", mrb_redis_zrevrank, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "zscore", mrb_redis_zscore, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "pfadd", mrb_redis_pfadd, (MRB_ARGS_REQ(1) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "pfcount", mrb_redis_pfcount, (MRB_ARGS_REQ(1) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "pfmerge", mrb_redis_pfmerge, (MRB_ARGS_REQ(2) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "publish", mrb_redis_pub, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "close", mrb_redis_close, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "queue", mrb_redisAppendCommandArgv, (MRB_ARGS_REQ(1) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "reply", mrb_redisGetReply, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "bulk_reply", mrb_redisGetBulkReply, MRB_ARGS_NONE());
  DONE;
}

void mrb_mruby_redis_gem_final(mrb_state *mrb)
{
}

//#endif
