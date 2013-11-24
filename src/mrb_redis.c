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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <hiredis/hiredis.h>
#include "mruby.h"
#include "mruby/data.h"
#include "mruby/variable.h"
#include "mruby/array.h"
#include "mruby/string.h"
#include "mruby/class.h"
#include "mrb_redis.h"

#define DONE mrb_gc_arena_restore(mrb, 0);

//#ifdef ENABLE_REDIS
//static struct mrb_data_type redis_object_data_type;
//static struct redis_object_data { int hoge; };

static void redisContext_free(mrb_state *mrb, void *p)
{
    redisFree(p);
}

static const struct mrb_data_type redisContext_type = {
    "redisContext", redisContext_free,
};

static redisContext *mrb_redis_get_context(mrb_state *mrb,  mrb_value self)
{
    redisContext *c;
    mrb_value context;

    context = mrb_iv_get(mrb, self, mrb_intern(mrb, "redis_c"));
    Data_Get_Struct(mrb, context, &redisContext_type, c);
    if (!c)
        mrb_raise(mrb, E_RUNTIME_ERROR, "redisContext get from mrb_iv_get redis_c failed");

    return c;
}

mrb_value mrb_redis_connect(mrb_state *mrb, mrb_value self)
{
    mrb_value host, port;

    mrb_get_args(mrb, "oo", &host, &port);

    redisContext *rc = redisConnect(RSTRING_PTR(host), mrb_fixnum(port));
    if (rc->err)
        mrb_raise(mrb, E_RUNTIME_ERROR, "redis connection faild.");

    mrb_iv_set(mrb
        , self
        , mrb_intern(mrb, "redis_c")
        , mrb_obj_value(Data_Wrap_Struct(mrb
            , mrb->object_class
            , &redisContext_type
            , (void*) rc)
        )
    );

    return self;
}

mrb_value mrb_redis_select(mrb_state *mrb, mrb_value self)
{
    mrb_value database;

    mrb_get_args(mrb, "o", &database);
    redisContext *rc = mrb_redis_get_context(mrb, self);

    if (mrb_type(database) != MRB_TT_FIXNUM) {
      mrb_raisef(mrb, E_TYPE_ERROR, "type mismatch: %S given", database);
    }

    redisReply *rs = redisCommand(rc, "SELECT %d", mrb_fixnum(database));
    freeReplyObject(rs);

    return  self;
}

mrb_value mrb_redis_set(mrb_state *mrb, mrb_value self)
{
    mrb_value key, val;

    mrb_get_args(mrb, "oo", &key, &val);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rs = redisCommand(rc,"SET %s %s", RSTRING_PTR(key), RSTRING_PTR(val));
    freeReplyObject(rs);

    return  self;
}

mrb_value mrb_redis_get(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    char *val;

    mrb_get_args(mrb, "o", &key);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rs = redisCommand(rc, "GET %s", RSTRING_PTR(key));
    if (rs->type == REDIS_REPLY_STRING) {
        val = strdup(rs->str);
        freeReplyObject(rs);
        return mrb_str_new(mrb, val, strlen(val));
    } else {
        freeReplyObject(rs);
        return mrb_nil_value();
    }
}

mrb_value mrb_redis_exists(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int counter;

    mrb_get_args(mrb, "o", &key);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc, "EXISTS %s", RSTRING_PTR(key));
    counter = rr->integer;
    freeReplyObject(rr);

    return counter ? mrb_true_value() : mrb_false_value();
}

mrb_value mrb_redis_randomkey (mrb_state *mrb, mrb_value self)
{
    char *val;

    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rs = redisCommand(rc, "RANDOMKEY");
    if (rs->type == REDIS_REPLY_STRING) {
        val = strdup(rs->str);
        freeReplyObject(rs);
        return mrb_str_new(mrb, val, strlen(val));
    } else {
        freeReplyObject(rs);
        return mrb_nil_value();
    }
}

mrb_value mrb_redis_del(mrb_state *mrb, mrb_value self)
{
    mrb_value key;

    mrb_get_args(mrb, "o", &key);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rs = redisCommand(rc, "DEL %s", RSTRING_PTR(key));
    freeReplyObject(rs);

    return  self;
}

mrb_value mrb_redis_incr(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int counter;

    mrb_get_args(mrb, "o", &key);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc, "INCR %s", RSTRING_PTR(key));
    counter = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(counter);
}

mrb_value mrb_redis_decr(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int counter;

    mrb_get_args(mrb, "o", &key);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc, "DECR %s", RSTRING_PTR(key));
    counter = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(counter);
}

mrb_value mrb_redis_rpush(mrb_state *mrb, mrb_value self)
{
    mrb_value key, val;
    mrb_int integer;

    mrb_get_args(mrb, "oo", &key, &val);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc,"RPUSH %s %s", RSTRING_PTR(key), RSTRING_PTR(val));
    integer = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(integer);
}

mrb_value mrb_redis_lpush(mrb_state *mrb, mrb_value self)
{
    mrb_value key, val;
    mrb_int integer;

    mrb_get_args(mrb, "oo", &key, &val);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc,"LPUSH %s %s", RSTRING_PTR(key), RSTRING_PTR(val));
    integer = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(integer);
}

mrb_value mrb_redis_lrange(mrb_state *mrb, mrb_value self)
{
    int i;
    mrb_value list, array;
    mrb_int arg1, arg2;

    mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc,"LRANGE %s %d %d", RSTRING_PTR(list), arg1, arg2);
    if (rr->type == REDIS_REPLY_ARRAY) {
        array = mrb_ary_new(mrb);
        for (i = 0; i < rr->elements; i++) {
            mrb_ary_push(mrb, array, mrb_str_new_cstr(mrb, rr->element[i]->str));
        }
    } else {
        freeReplyObject(rr);
        return mrb_nil_value();
    }

    freeReplyObject(rr);

    return array;
}

mrb_value mrb_redis_ltrim(mrb_state *mrb, mrb_value self)
{
    mrb_value list;
    mrb_int arg1, arg2, integer;

    mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc,"LTRIM %s %d %d", RSTRING_PTR(list), arg1, arg2);
    integer = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(integer);
}

mrb_value mrb_redis_zadd(mrb_state *mrb, mrb_value self)
{
    mrb_value key, member;
    mrb_float score;

    mrb_get_args(mrb, "ofo", &key, &score, &member);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rs = redisCommand(rc, "ZADD %s %f %s", RSTRING_PTR(key), score, RSTRING_PTR(member));
    freeReplyObject(rs);

    return  self;
}

mrb_value mrb_redis_basic_zrange(mrb_state *mrb, mrb_value self, const char *cmd)
{
    int i;
    mrb_value list, array;
    mrb_int arg1, arg2;

    mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc, "%s %s %d %d", cmd, RSTRING_PTR(list), arg1, arg2);
    if (rr->type == REDIS_REPLY_ARRAY) {
        array = mrb_ary_new(mrb);
        for (i = 0; i < rr->elements; i++) {
            mrb_ary_push(mrb, array, mrb_str_new_cstr(mrb, rr->element[i]->str));
        }
    } else {
        freeReplyObject(rr);
        return mrb_nil_value();
    }

    freeReplyObject(rr);

    return array;
}

mrb_value mrb_redis_zrange(mrb_state *mrb, mrb_value self)
{
    return mrb_redis_basic_zrange(mrb, self, "ZRANGE");
}

mrb_value mrb_redis_zrevrange(mrb_state *mrb, mrb_value self)
{
    return mrb_redis_basic_zrange(mrb, self, "ZREVRANGE");
}

mrb_value mrb_redis_zrank(mrb_state *mrb, mrb_value self)
{
    mrb_value key, member;
    mrb_int integer;

    mrb_get_args(mrb, "oo", &key, &member);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc, "ZRANK %s %s", RSTRING_PTR(key), RSTRING_PTR(member));
    integer = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(integer);
}

mrb_value mrb_redis_pub(mrb_state *mrb, mrb_value self)
{
    mrb_value channel, msg;

    mrb_get_args(mrb, "oo", &channel, &msg);
    redisContext *rc = mrb_redis_get_context(mrb, self);
    redisReply *rr = redisCommand(rc,"PUBLISH %s %s", RSTRING_PTR(channel), RSTRING_PTR(msg));
    freeReplyObject(rr);

    return  self;
}

mrb_value mrb_redis_close(mrb_state *mrb, mrb_value self)
{
    redisContext *rc;
    mrb_value context;

    context = mrb_iv_get(mrb, self, mrb_intern(mrb, "redis_c"));
    Data_Get_Struct(mrb, context, &redisContext_type, rc);

    return mrb_nil_value();
}

void mrb_mruby_redis_gem_init(mrb_state *mrb)
{
    struct RClass *redis;

    redis = mrb_define_class(mrb, "Redis", mrb->object_class);

    mrb_define_method(mrb, redis, "initialize", mrb_redis_connect, ARGS_ANY());
    mrb_define_method(mrb, redis, "select", mrb_redis_select, ARGS_REQ(1));
    mrb_define_method(mrb, redis, "set", mrb_redis_set, ARGS_ANY());
    mrb_define_method(mrb, redis, "get", mrb_redis_get, ARGS_ANY());
    mrb_define_method(mrb, redis, "exists?", mrb_redis_exists, ARGS_REQ(1));
    mrb_define_method(mrb, redis, "randomkey", mrb_redis_randomkey, ARGS_NONE());
    mrb_define_method(mrb, redis, "[]=", mrb_redis_set, ARGS_ANY());
    mrb_define_method(mrb, redis, "[]", mrb_redis_get, ARGS_ANY());
    mrb_define_method(mrb, redis, "del", mrb_redis_del, ARGS_ANY());
    mrb_define_method(mrb, redis, "incr", mrb_redis_incr, ARGS_OPT(1));
    mrb_define_method(mrb, redis, "decr", mrb_redis_decr, ARGS_OPT(1));
    mrb_define_method(mrb, redis, "rpush", mrb_redis_rpush, ARGS_OPT(2));
    mrb_define_method(mrb, redis, "lpush", mrb_redis_lpush, ARGS_OPT(2));
    mrb_define_method(mrb, redis, "lrange", mrb_redis_lrange, ARGS_ANY());
    mrb_define_method(mrb, redis, "ltrim", mrb_redis_ltrim, ARGS_ANY());
    mrb_define_method(mrb, redis, "zadd", mrb_redis_zadd, ARGS_REQ(3));
    mrb_define_method(mrb, redis, "zrange", mrb_redis_zrange, ARGS_REQ(3));
    mrb_define_method(mrb, redis, "zrevrange", mrb_redis_zrevrange, ARGS_REQ(3));
    mrb_define_method(mrb, redis, "zrank", mrb_redis_zrank, ARGS_REQ(2));
    mrb_define_method(mrb, redis, "publish", mrb_redis_pub, ARGS_ANY());
    mrb_define_method(mrb, redis, "close", mrb_redis_close, ARGS_NONE());
    DONE;
}

void mrb_mruby_redis_gem_final(mrb_state *mrb)
{
}

//#endif
