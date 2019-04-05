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

#include "mrb_redis.h"
#include "mruby.h"
#include "mruby/array.h"
#include "mruby/class.h"
#include "mruby/data.h"
#include "mruby/hash.h"
#include "mruby/numeric.h"
#include "mruby/string.h"
#include "mruby/variable.h"
#include <errno.h>
#include <hiredis/hiredis.h>
#include <mruby/error.h>
#include <mruby/redis.h>
#include <mruby/throw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "mrb_pointer.h"

#define DONE mrb_gc_arena_restore(mrb, 0);

#define CREATE_REDIS_COMMAND_ARG1(argv, lens, cmd, arg1)                                                               \
  argv[0] = cmd;                                                                                                       \
  argv[1] = RSTRING_PTR(arg1);                                                                                         \
  lens[0] = strlen(cmd);                                                                                               \
  lens[1] = RSTRING_LEN(arg1)
#define CREATE_REDIS_COMMAND_ARG2(argv, lens, cmd, arg1, arg2)                                                         \
  argv[0] = cmd;                                                                                                       \
  argv[1] = RSTRING_PTR(arg1);                                                                                         \
  argv[2] = RSTRING_PTR(arg2);                                                                                         \
  lens[0] = strlen(cmd);                                                                                               \
  lens[1] = RSTRING_LEN(arg1);                                                                                         \
  lens[2] = RSTRING_LEN(arg2)
#define CREATE_REDIS_COMMAND_ARG3(argv, lens, cmd, arg1, arg2, arg3)                                                   \
  argv[0] = cmd;                                                                                                       \
  argv[1] = RSTRING_PTR(arg1);                                                                                         \
  argv[2] = RSTRING_PTR(arg2);                                                                                         \
  argv[3] = RSTRING_PTR(arg3);                                                                                         \
  lens[0] = strlen(cmd);                                                                                               \
  lens[1] = RSTRING_LEN(arg1);                                                                                         \
  lens[2] = RSTRING_LEN(arg2);                                                                                         \
  lens[3] = RSTRING_LEN(arg3)

typedef struct ReplyHandlingRule {
  mrb_bool status_to_symbol;
  mrb_bool integer_to_bool;
  mrb_bool emptyarray_to_nil;
  mrb_bool return_exception;
} ReplyHandlingRule;

#define DEFAULT_REPLY_HANDLING_RULE                                                                                    \
  {                                                                                                                    \
    .status_to_symbol = FALSE, .integer_to_bool = FALSE, .emptyarray_to_nil = FALSE, .return_exception = FALSE,        \
  }

static inline mrb_value mrb_redis_get_reply(redisReply *reply, mrb_state *mrb, const ReplyHandlingRule *rule);
static inline int mrb_redis_create_command_noarg(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens);
static inline int mrb_redis_create_command_str(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens);
static inline int mrb_redis_create_command_int(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens);
static inline int mrb_redis_create_command_str_str(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens);
static inline int mrb_redis_create_command_str_int(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens);
static inline int mrb_redis_create_command_str_str_str(mrb_state *mrb, const char *cmd, const char **argv,
                                                       size_t *lens);
static inline int mrb_redis_create_command_str_str_int(mrb_state *mrb, const char *cmd, const char **argv,
                                                       size_t *lens);
static inline int mrb_redis_create_command_str_int_int(mrb_state *mrb, const char *cmd, const char **argv,
                                                       size_t *lens);
static inline int mrb_redis_create_command_str_float_str(mrb_state *mrb, const char *cmd, const char **argv,
                                                         size_t *lens);
static inline redisContext *mrb_redis_get_context(mrb_state *mrb, mrb_value self);
static inline mrb_value mrb_redis_execute_command(mrb_state *mrb, mrb_value self, int argc, const char **argv,
                                                  const size_t *lens, const ReplyHandlingRule *rule);

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

static mrb_value mrb_redis_connect_set_raw(mrb_state *mrb, mrb_value self)
{
  redisContext *rc;
  mrb_value host, port;
  mrb_int timeout = 1;
  struct timeval timeout_struct = {timeout, 0};

  if (mrb_get_args(mrb, "oo|i", &host, &port, &timeout) == 3) {
    timeout_struct.tv_sec = timeout;
  }

  rc = redisConnectWithTimeout(mrb_str_to_cstr(mrb, host), mrb_fixnum(port), timeout_struct);
  if (rc->err) {
    mrb_raise(mrb, E_REDIS_ERROR, "redis connection failed.");
  }

  mrb_udptr_set(mrb, (void *)rc);

  return self;
}

static mrb_value mrb_redis_connect(mrb_state *mrb, mrb_value self)
{
  mrb_value host, port;
  mrb_int timeout = 1;
  struct timeval timeout_struct = {timeout, 0};
  mrb_int argc = 0;

  redisContext *rc = (redisContext *)DATA_PTR(self);
  if (rc) {
    redisFree(rc);
  }
  DATA_TYPE(self) = &redisContext_type;
  DATA_PTR(self) = NULL;

  argc = mrb_get_args(mrb, "|oo|i", &host, &port, &timeout);

  if (argc == 3) {
    timeout_struct.tv_sec = timeout;
    rc = redisConnectWithTimeout(mrb_str_to_cstr(mrb, host), mrb_fixnum(port), timeout_struct);
  } else if (argc == 2) {
    rc = redisConnectWithTimeout(mrb_str_to_cstr(mrb, host), mrb_fixnum(port), timeout_struct);
  } else if (argc == 0) {
    rc = (redisContext *)mrb_udptr_get(mrb);
  }

  if (rc->err) {
    redisFree(rc);
    mrb_raise(mrb, E_REDIS_ERROR, "redis connection failed.");
  }

  DATA_PTR(self) = rc;

  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "keepalive"), mrb_symbol_value(mrb_intern_lit(mrb, "off")));

  return self;
}

static mrb_value mrb_redis_enable_keepalive(mrb_state *mrb, mrb_value self)
{
  redisContext *rc = mrb_redis_get_context(mrb, self);
  errno = 0;
  if (redisEnableKeepAlive(rc) == REDIS_OK) {
    mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "keepalive"), mrb_symbol_value(mrb_intern_lit(mrb, "on")));
  } else {
    mrb_sys_fail(mrb, rc->errstr);
  }
  return mrb_nil_value();
}

static mrb_value mrb_redis_keepalive(mrb_state *mrb, mrb_value self)
{
  mrb_redis_get_context(mrb, self); // Check if closed
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "keepalive"));
}

static mrb_value mrb_redis_host(mrb_state *mrb, mrb_value self)
{
  redisContext *rc = mrb_redis_get_context(mrb, self);
  if (rc->connection_type != REDIS_CONN_TCP) {
    // Never expect to come here since mruby-redis does not support unix socket connection...
    return mrb_nil_value();
  }
  return mrb_str_new_cstr(mrb, rc->tcp.host);
}

static mrb_value mrb_redis_port(mrb_state *mrb, mrb_value self)
{
  redisContext *rc = mrb_redis_get_context(mrb, self);
  if (rc->connection_type != REDIS_CONN_TCP) {
    // Never expect to come here since mruby-redis does not support unix socket connection...
    return mrb_nil_value();
  }
  return mrb_fixnum_value((mrb_int)(rc->tcp.port));
}

static mrb_value mrb_redis_ping(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "PING", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_auth(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "AUTH", argv, lens);
  ReplyHandlingRule rule = {.return_exception = TRUE};
  mrb_value reply = mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
  if (mrb_exception_p(reply)) {
    if (mrb_obj_is_instance_of(mrb, reply, E_REDIS_REPLY_ERROR)) {
      mrb_raisef(mrb, E_REDIS_ERR_AUTH, "incorrect password");
    } else {
      mrb_exc_raise(mrb, reply);
    }
  }
  return reply;
}

static mrb_value mrb_redis_select(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_int(mrb, "SELECT", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_set(mrb_state *mrb, mrb_value self)
{
  mrb_value key, val, opt;
  mrb_bool b = 0;
  const char *argv[7];
  size_t lens[7];
  int c = 3;
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;

  mrb_get_args(mrb, "SS|H?", &key, &val, &opt, &b);

  CREATE_REDIS_COMMAND_ARG2(argv, lens, "SET", key, val);
  if (b) {
    mrb_value ex = mrb_hash_delete_key(mrb, opt, mrb_str_new_cstr(mrb, "EX"));
    mrb_value px = mrb_hash_delete_key(mrb, opt, mrb_str_new_cstr(mrb, "PX"));
    mrb_bool nx = mrb_bool(mrb_hash_delete_key(mrb, opt, mrb_str_new_cstr(mrb, "NX")));
    mrb_bool xx = mrb_bool(mrb_hash_delete_key(mrb, opt, mrb_str_new_cstr(mrb, "XX")));

    if (!mrb_nil_p(ex) && !mrb_nil_p(px)) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Only one of EX or PX can be set");
    }

    if (nx && xx) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Either NX or XX is true");
    }

    if (!mrb_nil_p(ex)) {
      argv[c] = "EX";
      lens[c] = strlen("EX");
      c++;
      if (mrb_fixnum_p(ex)) {
        ex = mrb_fixnum_to_str(mrb, ex, 10);
      }
      if (!mrb_string_p(ex)) {
        mrb_raisef(mrb, E_TYPE_ERROR, "EX should be int or str, but %S given", ex);
      }
      argv[c] = RSTRING_PTR(ex);
      lens[c] = RSTRING_LEN(ex);
      c++;
    }

    if (!mrb_nil_p(px)) {
      argv[c] = "PX";
      lens[c] = strlen("PX");
      c++;
      if (mrb_fixnum_p(px)) {
        px = mrb_fixnum_to_str(mrb, px, 10);
      }
      if (!mrb_string_p(px)) {
        mrb_raisef(mrb, E_TYPE_ERROR, "PX should be int or str, but %S given", px);
      }
      argv[c] = RSTRING_PTR(px);
      lens[c] = RSTRING_LEN(px);
      c++;
    }

    if (nx) {
      argv[c] = "NX";
      lens[c] = strlen("NX");
      c++;
    }

    if (xx) {
      argv[c] = "XX";
      lens[c] = strlen("XX");
      c++;
    }

    if (!mrb_hash_empty_p(mrb, opt)) {
      mrb_raisef(mrb, E_ARGUMENT_ERROR, "unknown option(s) specified %S (note: only string can be key, not the symbol",
                 mrb_hash_keys(mrb, opt));
    }
  }

  return mrb_redis_execute_command(mrb, self, c, argv, lens, &rule);
}

static mrb_value mrb_redis_get(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "GET", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_keys(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "KEYS", argv, lens);
  ReplyHandlingRule rule = {.emptyarray_to_nil = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_exists(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "EXISTS", argv, lens);
  ReplyHandlingRule rule = {.integer_to_bool = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_expire(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_int(mrb, "EXPIRE", argv, lens);
  ReplyHandlingRule rule = {.integer_to_bool = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_flushdb(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "FLUSHDB", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_flushall(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "FLUSHALL", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_randomkey(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "RANDOMKEY", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_del(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "DEL", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_incr(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "INCR", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_decr(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "DECR", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_incrby(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_int(mrb, "INCRBY", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_decrby(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_int(mrb, "DECRBY", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_llen(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "LLEN", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_rpush(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "RPUSH", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_lpush(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "LPUSH", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_rpop(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "RPOP", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_lpop(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "LPOP", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_lrange(mrb_state *mrb, mrb_value self)
{
  const char *argv[4];
  size_t lens[4];
  int argc = mrb_redis_create_command_str_int_int(mrb, "LRANGE", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_ltrim(mrb_state *mrb, mrb_value self)
{
  const char *argv[4];
  size_t lens[4];
  int argc = mrb_redis_create_command_str_int_int(mrb, "LTRIM", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_lindex(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_int(mrb, "LINDEX", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_sadd(mrb_state *mrb, mrb_value self)
{
  mrb_value key, *members;
  mrb_int members_len;
  const char **argv;
  size_t *lens;
  size_t argc;
  int i;

  mrb_get_args(mrb, "o*", &key, &members, &members_len);
  if (members_len == 0) {
    mrb_raise(mrb, E_ARGUMENT_ERROR, "too few arguments");
  }
  argc = 2 + members_len;

  argv = (const char **)alloca(argc * sizeof(char *));
  lens = (size_t *)alloca(argc * sizeof(size_t));

  CREATE_REDIS_COMMAND_ARG1(argv, lens, "SADD", key);
  for (i = 0; i < members_len; i++) {
    argv[i + 2] = RSTRING_PTR(members[i]);
    lens[i + 2] = RSTRING_LEN(members[i]);
  }

  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_srem(mrb_state *mrb, mrb_value self)
{
  mrb_value key, *members;
  mrb_int members_len;
  const char **argv;
  size_t *lens, argc;
  int i;

  mrb_get_args(mrb, "o*", &key, &members, &members_len);
  if (members_len < 1) {
    mrb_raise(mrb, E_ARGUMENT_ERROR, "wrong number of arguments");
  }
  argc = members_len + 2;

  argv = (const char **)alloca(argc * sizeof(char *));
  lens = (size_t *)alloca(argc * sizeof(size_t));

  CREATE_REDIS_COMMAND_ARG1(argv, lens, "SREM", key);
  for (i = 0; i < members_len; i++) {
    argv[i + 2] = RSTRING_PTR(members[i]);
    lens[i + 2] = RSTRING_LEN(members[i]);
  }

  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_sismember(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "SISMEMBER", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_smembers(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "SMEMBERS", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_scard(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "SCARD", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_spop(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "SPOP", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_hset(mrb_state *mrb, mrb_value self)
{
  const char *argv[4];
  size_t lens[4];
  int argc = mrb_redis_create_command_str_str_str(mrb, "HSET", argv, lens);
  ReplyHandlingRule rule = {.integer_to_bool = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_hsetnx(mrb_state *mrb, mrb_value self)
{
  const char *argv[4];
  size_t lens[4];
  int argc = mrb_redis_create_command_str_str_str(mrb, "HSETNX", argv, lens);
  ReplyHandlingRule rule = {.integer_to_bool = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_hget(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "HGET", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_hgetall(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "HGETALL", argv, lens);
  ReplyHandlingRule rule = {.emptyarray_to_nil = TRUE};
  mrb_value reply = mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
  if (mrb_array_p(reply)) {
    // Convert [k1, v1, ..., kN, vN] --> {k1 => v1, ..., kN =>}
    mrb_value hash = mrb_hash_new_capa(mrb, RARRAY_LEN(reply) / 2);
    for (mrb_int i = 0; i < RARRAY_LEN(reply); i += 2) {
      mrb_hash_set(mrb, hash, mrb_ary_ref(mrb, reply, i), mrb_ary_ref(mrb, reply, i + 1));
    }
    return hash;
  } else {
    return reply;
  }
}

static mrb_value mrb_redis_hdel(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "HDEL", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_hexists(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "HEXISTS", argv, lens);
  ReplyHandlingRule rule = {.integer_to_bool = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_hkeys(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "HKEYS", argv, lens);
  ReplyHandlingRule rule = {.emptyarray_to_nil = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_hmget(mrb_state *mrb, mrb_value self)
{
  mrb_value *mrb_argv, array;
  mrb_int argc = 0;
  int i, ai;
  const char **argv;
  size_t *argvlen;
  mrb_int argc_current;
  redisContext *rc;
  redisReply *rr;

  mrb_get_args(mrb, "*", &mrb_argv, &argc);
  argc++;

  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

  argv[0] = "HMGET";
  argvlen[0] = sizeof("HMGET") - 1;

  ai = mrb_gc_arena_save(mrb);
  for (argc_current = 1; argc_current < argc; argc_current++) {
    mrb_value curr = mrb_str_to_str(mrb, mrb_argv[argc_current - 1]);
    argv[argc_current] = RSTRING_PTR(curr);
    argvlen[argc_current] = RSTRING_LEN(curr);
    mrb_gc_arena_restore(mrb, ai);
  }

  array = mrb_nil_value();
  rc = mrb_redis_get_context(mrb, self);
  rr = redisCommandArgv(rc, argc, argv, argvlen);
  if (rc->err) {
    mrb_redis_check_error(rc, mrb);
  }
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
    mrb_value msg = mrb_str_new_cstr(mrb, rr->str);
    freeReplyObject(rr);
    mrb_raisef(mrb, E_ARGUMENT_ERROR, "%S", msg);
  }

  freeReplyObject(rr);
  return array;
}

static mrb_value mrb_redis_hmset(mrb_state *mrb, mrb_value self)
{
  mrb_value *mrb_argv;
  mrb_int argc = 0;
  redisContext *rc = mrb_redis_get_context(mrb, self);
  redisReply *rr;
  const char **argv;
  size_t *argvlen;
  int ai;
  mrb_int argc_current;

  mrb_get_args(mrb, "*", &mrb_argv, &argc);
  argc++;
  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

  argv[0] = "HMSET";
  argvlen[0] = sizeof("HMSET") - 1;

  ai = mrb_gc_arena_save(mrb);
  for (argc_current = 1; argc_current < argc; argc_current++) {
    mrb_value curr = mrb_str_to_str(mrb, mrb_argv[argc_current - 1]);
    argv[argc_current] = RSTRING_PTR(curr);
    argvlen[argc_current] = RSTRING_LEN(curr);
    mrb_gc_arena_restore(mrb, ai);
  }

  rr = redisCommandArgv(rc, argc, argv, argvlen);
  if (rc->err) {
    mrb_redis_check_error(rc, mrb);
  }
  if (rr->type == REDIS_REPLY_STATUS && rr != NULL) {
    freeReplyObject(rr);
  } else {
    mrb_value msg = mrb_str_new_cstr(mrb, rr->str);
    freeReplyObject(rr);
    mrb_raisef(mrb, E_ARGUMENT_ERROR, "%S", msg);
  }

  return self;
}

static mrb_value mrb_redis_hvals(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "HVALS", argv, lens);
  ReplyHandlingRule rule = {.emptyarray_to_nil = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_hincrby(mrb_state *mrb, mrb_value self)
{
  const char *argv[4];
  size_t lens[4];
  int argc = mrb_redis_create_command_str_str_int(mrb, "HINCRBY", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_mset(mrb_state *mrb, mrb_value self)
{
  mrb_value *mrb_argv;
  mrb_int argc = 0;
  redisContext *rc = mrb_redis_get_context(mrb, self);
  redisReply *rr;
  const char **argv;
  size_t *argvlen;
  int ai;
  mrb_int argc_current;

  mrb_get_args(mrb, "*", &mrb_argv, &argc);
  argc++;
  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

  argv[0] = "MSET";
  argvlen[0] = sizeof("MSET") - 1;

  ai = mrb_gc_arena_save(mrb);
  for (argc_current = 1; argc_current < argc; argc_current++) {
    mrb_value curr = mrb_str_to_str(mrb, mrb_argv[argc_current - 1]);
    argv[argc_current] = RSTRING_PTR(curr);
    argvlen[argc_current] = RSTRING_LEN(curr);
    mrb_gc_arena_restore(mrb, ai);
  }

  rr = redisCommandArgv(rc, argc, argv, argvlen);
  if (rc->err) {
    mrb_redis_check_error(rc, mrb);
  }
  if (rr->type == REDIS_REPLY_STATUS && rr != NULL) {
    freeReplyObject(rr);
  } else {
    mrb_value msg = mrb_str_new_cstr(mrb, rr->str);
    freeReplyObject(rr);
    mrb_raisef(mrb, E_ARGUMENT_ERROR, "%S", msg);
  }

  return self;
}

static mrb_value mrb_redis_mget(mrb_state *mrb, mrb_value self)
{
  mrb_value *mrb_argv, array;
  mrb_int argc = 0;
  int i, ai;
  const char **argv;
  size_t *argvlen;
  mrb_int argc_current;
  redisContext *rc;
  redisReply *rr;

  mrb_get_args(mrb, "*", &mrb_argv, &argc);
  argc++;

  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

  argv[0] = "MGET";
  argvlen[0] = sizeof("MGET") - 1;

  ai = mrb_gc_arena_save(mrb);
  for (argc_current = 1; argc_current < argc; argc_current++) {
    mrb_value curr = mrb_str_to_str(mrb, mrb_argv[argc_current - 1]);
    argv[argc_current] = RSTRING_PTR(curr);
    argvlen[argc_current] = RSTRING_LEN(curr);
    mrb_gc_arena_restore(mrb, ai);
  }

  array = mrb_nil_value();
  rc = mrb_redis_get_context(mrb, self);
  rr = redisCommandArgv(rc, argc, argv, argvlen);
  if (rc->err) {
    mrb_redis_check_error(rc, mrb);
  }
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
    char const *msg = rr->str;
    freeReplyObject(rr);
    mrb_raise(mrb, E_ARGUMENT_ERROR, msg);
  }

  freeReplyObject(rr);
  return array;
}



static mrb_value mrb_redis_ttl(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "TTL", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_zadd(mrb_state *mrb, mrb_value self)
{
  const char *argv[4];
  size_t lens[4];
  int argc = mrb_redis_create_command_str_float_str(mrb, "ZADD", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_zcard(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "ZCARD", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_basic_zrange(mrb_state *mrb, mrb_value self, const char *cmd)
{
  const char *argv[4];
  size_t lens[4];
  int argc = mrb_redis_create_command_str_int_int(mrb, cmd, argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
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
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, cmd, argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
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
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "ZSCORE", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_pub(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "PUBLISH", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_pfadd(mrb_state *mrb, mrb_value self)
{
  mrb_value key, *mrb_rest_argv;
  mrb_int argc = 0, rest_argc = 0;
  const char **argv;
  size_t *argvlen;

  mrb_get_args(mrb, "o*", &key, &mrb_rest_argv, &rest_argc);
  argc = rest_argc + 2;

  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

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

  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, argvlen, &rule);
}

static mrb_value mrb_redis_pfcount(mrb_state *mrb, mrb_value self)
{
  mrb_value key, *mrb_rest_argv;
  mrb_int argc = 0, rest_argc = 0;
  const char **argv;
  size_t *argvlen;

  mrb_get_args(mrb, "o*", &key, &mrb_rest_argv, &rest_argc);
  argc = rest_argc + 2;

  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

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

  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, argvlen, &rule);
}

static mrb_value mrb_redis_pfmerge(mrb_state *mrb, mrb_value self)
{
  mrb_value dest_struct, src_struct, *mrb_rest_argv;
  mrb_int argc = 0, rest_argc = 0;
  const char **argv;
  size_t *argvlen;

  mrb_get_args(mrb, "oo*", &dest_struct, &src_struct, &mrb_rest_argv, &rest_argc);
  argc = rest_argc + 3;

  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

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

  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, argvlen, &rule);
}

static mrb_value mrb_redis_close(mrb_state *mrb, mrb_value self)
{
  redisContext *rc = DATA_PTR(self);

  redisFree(rc);

  DATA_PTR(self) = NULL;
  DATA_TYPE(self) = NULL;

  return mrb_nil_value();
}

static inline mrb_value mrb_redis_get_ary_reply(redisReply *reply, mrb_state *mrb, const ReplyHandlingRule *rule);

static inline mrb_value mrb_redis_get_reply(redisReply *reply, mrb_state *mrb, const ReplyHandlingRule *rule)
{
  switch (reply->type) {
  case REDIS_REPLY_STRING:
    return mrb_str_new(mrb, reply->str, reply->len);
    break;
  case REDIS_REPLY_ARRAY:
    return mrb_redis_get_ary_reply(reply, mrb, rule);
    break;
  case REDIS_REPLY_INTEGER: {
    if (rule->integer_to_bool)
      return mrb_bool_value(reply->integer);
    else if (FIXABLE(reply->integer))
      return mrb_fixnum_value(reply->integer);
    else
      return mrb_float_value(mrb, reply->integer);
  } break;
  case REDIS_REPLY_NIL:
    return mrb_nil_value();
    break;
  case REDIS_REPLY_STATUS: {
    if (rule->status_to_symbol) {
      mrb_sym status = mrb_intern(mrb, reply->str, reply->len);
      return mrb_symbol_value(status);
    } else {
      return mrb_str_new(mrb, reply->str, reply->len);
    }
  } break;
  case REDIS_REPLY_ERROR: {
    mrb_value err = mrb_str_new(mrb, reply->str, reply->len);
    mrb_value exc = mrb_exc_new_str(mrb, E_REDIS_REPLY_ERROR, err);
    if (rule->return_exception) {
      return exc;
    } else {
      freeReplyObject(reply);
      mrb_exc_raise(mrb, exc);
    }
  } break;
  default:
    freeReplyObject(reply);
    mrb_raise(mrb, E_REDIS_ERROR, "unknown reply type");
  }
}

static inline mrb_value mrb_redis_get_ary_reply(redisReply *reply, mrb_state *mrb, const ReplyHandlingRule *rule)
{
  if (rule->emptyarray_to_nil && reply->elements == 0) {
    return mrb_nil_value();
  }
  mrb_value ary = mrb_ary_new_capa(mrb, reply->elements);
  int ai = mrb_gc_arena_save(mrb);
  size_t element_couter;
  for (element_couter = 0; element_couter < reply->elements; element_couter++) {
    mrb_value element = mrb_redis_get_reply(reply->element[element_couter], mrb, rule);
    mrb_ary_push(mrb, ary, element);
    mrb_gc_arena_restore(mrb, ai);
  }
  return ary;
}

static mrb_value mrb_redisAppendCommandArgv(mrb_state *mrb, mrb_value self)
{
  mrb_sym command;
  mrb_value *mrb_argv;
  mrb_int argc = 0, argc_current;
  const char **argv;
  size_t *argvlen;
  mrb_int command_len;
  mrb_sym queue_counter_sym;
  mrb_value queue_counter_val;
  mrb_int queue_counter;
  redisContext *context;
  int rc;

  mrb_get_args(mrb, "n*", &command, &mrb_argv, &argc);
  argc++;

  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

  argv[0] = mrb_sym2name_len(mrb, command, &command_len);
  argvlen[0] = command_len;

  for (argc_current = 1; argc_current < argc; argc_current++) {
    mrb_value curr = mrb_str_to_str(mrb, mrb_argv[argc_current - 1]);
    argv[argc_current] = RSTRING_PTR(curr);
    argvlen[argc_current] = RSTRING_LEN(curr);
  }

  queue_counter_sym = mrb_intern_lit(mrb, "queue_counter");
  queue_counter_val = mrb_iv_get(mrb, self, queue_counter_sym);
  queue_counter = 1;
  if (mrb_fixnum_p(queue_counter_val)) {
    queue_counter = mrb_fixnum(queue_counter_val);
    if (mrb_int_add_overflow(queue_counter, 1, &queue_counter)) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "integer addition would overflow");
    }
  }

  context = mrb_redis_get_context(mrb, self);
  errno = 0;
  rc = redisAppendCommandArgv(context, argc, argv, argvlen);
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
  redisContext *context;
  redisReply *reply = NULL;
  mrb_value reply_val;
  ReplyHandlingRule rule = {
      .status_to_symbol = TRUE,
      .return_exception = TRUE,
  };
  int rc;

  if (mrb_fixnum_p(queue_counter_val)) {
    queue_counter = mrb_fixnum(queue_counter_val);
  }

  context = mrb_redis_get_context(mrb, self);
  reply_val = self;
  errno = 0;
  rc = redisGetReply(context, (void **)&reply);
  if (rc == REDIS_OK && reply != NULL) {
    struct mrb_jmpbuf *prev_jmp = mrb->jmp;
    struct mrb_jmpbuf c_jmp;

    MRB_TRY(&c_jmp)
    {
      mrb->jmp = &c_jmp;
      reply_val = mrb_redis_get_reply(reply, mrb, &rule);
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
  mrb_int queue_counter;
  mrb_value bulk_reply;
  int ai;

  if (!mrb_fixnum_p(queue_counter_val))
    mrb_raise(mrb, E_RUNTIME_ERROR, "nothing queued yet");

  queue_counter = mrb_fixnum(queue_counter_val);
  bulk_reply = mrb_ary_new_capa(mrb, queue_counter);
  ai = mrb_gc_arena_save(mrb);

  do {
    mrb_value reply = mrb_redisGetReply(mrb, self);
    mrb_ary_push(mrb, bulk_reply, reply);
    mrb_gc_arena_restore(mrb, ai);
  } while (--queue_counter > 0);

  return bulk_reply;
}

static mrb_value mrb_redis_multi(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "MULTI", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_exec(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "EXEC", argv, lens);
  ReplyHandlingRule rule = {.emptyarray_to_nil = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_discard(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "DISCARD", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_watch(mrb_state *mrb, mrb_value self)
{
  mrb_int argc = 0, rest_argc = 0;
  mrb_value key, *mrb_rest_argv;
  const char **argv;
  size_t *argvlen;

  mrb_get_args(mrb, "o*", &key, &mrb_rest_argv, &rest_argc);
  argc = rest_argc + 2;

  argv = (const char **)alloca(argc * sizeof(char *));
  argvlen = (size_t *)alloca(argc * sizeof(size_t));

  CREATE_REDIS_COMMAND_ARG1(argv, argvlen, "WATCH", key);

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

  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, argvlen, &rule);
}

static mrb_value mrb_redis_unwatch(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "UNWATCH", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_setnx(mrb_state *mrb, mrb_value self)
{
  const char *argv[3];
  size_t lens[3];
  int argc = mrb_redis_create_command_str_str(mrb, "SETNX", argv, lens);
  ReplyHandlingRule rule = {.integer_to_bool = TRUE};
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_cluster(mrb_state *mrb, mrb_value self)
{
  const char *argv[2];
  size_t lens[2];
  int argc = mrb_redis_create_command_str(mrb, "CLUSTER", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static mrb_value mrb_redis_asking(mrb_state *mrb, mrb_value self)
{
  const char *argv[1];
  size_t lens[1];
  int argc = mrb_redis_create_command_noarg(mrb, "ASKING", argv, lens);
  ReplyHandlingRule rule = DEFAULT_REPLY_HANDLING_RULE;
  return mrb_redis_execute_command(mrb, self, argc, argv, lens, &rule);
}

static inline int mrb_redis_create_command_noarg(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens)
{
  argv[0] = cmd;
  lens[0] = strlen(cmd);
  return 1;
}
static inline int mrb_redis_create_command_str(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens)
{
  mrb_value str1;
  mrb_get_args(mrb, "S", &str1);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, cmd, str1);
  return 2;
}
static inline int mrb_redis_create_command_int(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens)
{
  mrb_value str1;
  mrb_int int1;
  mrb_get_args(mrb, "i", &int1);
  str1 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(int1), 10);
  CREATE_REDIS_COMMAND_ARG1(argv, lens, cmd, str1);
  return 2;
}
static inline int mrb_redis_create_command_str_str(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens)
{
  mrb_value str1, str2;
  mrb_get_args(mrb, "SS", &str1, &str2);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, cmd, str1, str2);
  return 3;
}
static inline int mrb_redis_create_command_str_int(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens)
{
  mrb_value str1, str2;
  mrb_int int2;
  mrb_get_args(mrb, "Si", &str1, &int2);
  str2 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(int2), 10);
  CREATE_REDIS_COMMAND_ARG2(argv, lens, cmd, str1, str2);
  return 3;
}
static inline int mrb_redis_create_command_str_str_str(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens)
{
  mrb_value str1, str2, str3;
  mrb_get_args(mrb, "SSS", &str1, &str2, &str3);
  CREATE_REDIS_COMMAND_ARG3(argv, lens, cmd, str1, str2, str3);
  return 4;
}
static inline int mrb_redis_create_command_str_str_int(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens)
{
  mrb_value str1, str2, str3;
  mrb_int int3;
  mrb_get_args(mrb, "SSi", &str1, &str2, &int3);
  str3 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(int3), 10);
  CREATE_REDIS_COMMAND_ARG3(argv, lens, cmd, str1, str2, str3);
  return 4;
}
static inline int mrb_redis_create_command_str_int_int(mrb_state *mrb, const char *cmd, const char **argv, size_t *lens)
{
  mrb_value str1, str2, str3;
  mrb_int int2, int3;
  mrb_get_args(mrb, "Sii", &str1, &int2, &int3);
  str2 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(int2), 10);
  str3 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(int3), 10);
  CREATE_REDIS_COMMAND_ARG3(argv, lens, cmd, str1, str2, str3);
  return 4;
}
static inline int mrb_redis_create_command_str_float_str(mrb_state *mrb, const char *cmd, const char **argv,
                                                         size_t *lens)
{
  mrb_value str1, str2, str3;
  mrb_float float2;
  mrb_get_args(mrb, "SfS", &str1, &float2, &str3);
  str2 = mrb_float_to_str(mrb, mrb_float_value(mrb, float2), "%g");
  CREATE_REDIS_COMMAND_ARG3(argv, lens, cmd, str1, str2, str3);
  return 4;
}

static inline redisContext *mrb_redis_get_context(mrb_state *mrb, mrb_value self)
{
  redisContext *context = DATA_PTR(self);
  if (!context) {
    mrb_raise(mrb, E_REDIS_ERR_CLOSED, "connection is already closed or not initialized yet.");
  }
  return context;
}

static inline mrb_value mrb_redis_execute_command(mrb_state *mrb, mrb_value self, int argc, const char **argv,
                                                  const size_t *lens, const ReplyHandlingRule *rule)
{
  mrb_value ret;
  redisReply *reply;
  redisContext *context = mrb_redis_get_context(mrb, self);

  reply = redisCommandArgv(context, argc, argv, lens);
  if (!reply) {
    mrb_raise(mrb, E_REDIS_ERROR, "could not read reply");
  }

  ret = mrb_redis_get_reply(reply, mrb, rule);
  freeReplyObject(reply);
  return ret;
}

void mrb_mruby_redis_gem_init(mrb_state *mrb)
{
  struct RClass *redis;

  redis = mrb_define_class(mrb, "Redis", mrb->object_class);
  MRB_SET_INSTANCE_TT(redis, MRB_TT_DATA);

  mrb_define_class_under(mrb, redis, "ConnectionError", E_RUNTIME_ERROR);
  mrb_define_class_under(mrb, redis, "ConnectionError", E_RUNTIME_ERROR);
  mrb_define_class_under(mrb, redis, "ReplyError", E_RUNTIME_ERROR);
  mrb_define_class_under(mrb, redis, "ProtocolError", E_RUNTIME_ERROR);
  mrb_define_class_under(mrb, redis, "OOMError", E_RUNTIME_ERROR);
  mrb_define_class_under(mrb, redis, "AuthError", E_RUNTIME_ERROR);
  mrb_define_class_under(mrb, redis, "ClosedError", E_RUNTIME_ERROR);

  mrb_define_method(mrb, redis, "initialize", mrb_redis_connect, MRB_ARGS_ANY());

  /* use mruby-pointer for sharing between mrb_states */
  mrb_define_class_method(mrb, redis, "connect_set_raw", mrb_redis_connect_set_raw, MRB_ARGS_ANY());
  mrb_define_class_method(mrb, redis, "connect_set_udptr", mrb_redis_connect_set_raw, MRB_ARGS_ANY());

  mrb_define_method(mrb, redis, "enable_keepalive", mrb_redis_enable_keepalive, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "keepalive", mrb_redis_keepalive, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "auth", mrb_redis_auth, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "select", mrb_redis_select, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "ping", mrb_redis_ping, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "host", mrb_redis_host, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "port", mrb_redis_port, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "set", mrb_redis_set, MRB_ARGS_ARG(2, 1));
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
  mrb_define_method(mrb, redis, "mset", mrb_redis_mset, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "mget", mrb_redis_mget, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "sadd", mrb_redis_sadd, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "srem", mrb_redis_srem, MRB_ARGS_ANY());
  mrb_define_method(mrb, redis, "sismember", mrb_redis_sismember, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "smembers", mrb_redis_smembers, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "scard", mrb_redis_scard, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "spop", mrb_redis_spop, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "hset", mrb_redis_hset, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, redis, "hsetnx", mrb_redis_hsetnx, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, redis, "hget", mrb_redis_hget, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "hgetall", mrb_redis_hgetall, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "hdel", mrb_redis_hdel, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "hexists?", mrb_redis_hexists, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "hkeys", mrb_redis_hkeys, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "hmset", mrb_redis_hmset, (MRB_ARGS_REQ(3) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "hmget", mrb_redis_hmget, (MRB_ARGS_REQ(2) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "hvals", mrb_redis_hvals, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "hincrby", mrb_redis_hincrby, MRB_ARGS_REQ(3));
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
  mrb_define_method(mrb, redis, "multi", mrb_redis_multi, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "exec", mrb_redis_exec, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "discard", mrb_redis_discard, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "watch", mrb_redis_watch, (MRB_ARGS_REQ(1) | MRB_ARGS_REST()));
  mrb_define_method(mrb, redis, "unwatch", mrb_redis_unwatch, MRB_ARGS_NONE());
  mrb_define_method(mrb, redis, "setnx", mrb_redis_setnx, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, redis, "cluster", mrb_redis_cluster, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, redis, "asking", mrb_redis_asking, MRB_ARGS_NONE());
  DONE;
}

void mrb_mruby_redis_gem_final(mrb_state *mrb)
{
}

//#endif
