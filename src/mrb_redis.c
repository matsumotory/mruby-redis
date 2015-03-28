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
#include "mruby/numeric.h"
#include "mruby/array.h"
#include "mruby/hash.h"
#include "mruby/string.h"
#include "mruby/class.h"
#include "mrb_redis.h"

#define DONE mrb_gc_arena_restore(mrb, 0);

static void redisContext_free(mrb_state *mrb, void *p)
{
    redisFree(p);
}

static const struct mrb_data_type redisContext_type = {
    "redisContext", redisContext_free,
};

mrb_value mrb_redis_connect(mrb_state *mrb, mrb_value self)
{
    mrb_value host, port;
    redisContext *rc = (redisContext *)DATA_PTR(self);
    if (rc) {
        mrb_free(mrb, rc);
    }
    DATA_TYPE(self) = &redisContext_type;
    DATA_PTR(self) = NULL;

    mrb_get_args(mrb, "oo", &host, &port);

    rc = redisConnect(mrb_str_to_cstr(mrb, host), mrb_fixnum(port));
    if (rc->err)
        mrb_raise(mrb, E_RUNTIME_ERROR, "redis connection faild.");

    DATA_PTR(self) = rc;

    return self;
}

mrb_value mrb_redis_select(mrb_state *mrb, mrb_value self)
{
    mrb_value database;

    redisContext *rc = DATA_PTR(self);
    mrb_get_args(mrb, "o", &database);

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
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oo", &key, &val);

    const char *argv[] = {"SET", RSTRING_PTR(key), RSTRING_PTR(val)};
    size_t lens[] = {3, RSTRING_LEN(key), RSTRING_LEN(val)};
    redisReply *rs = redisCommandArgv(rc, 3, argv, lens);
    freeReplyObject(rs);

    return  self;
}

mrb_value mrb_redis_get(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    char *val;
    int len;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"GET", RSTRING_PTR(key)};
    size_t lens[] = {3, RSTRING_LEN(key)};
    redisReply *rs = redisCommandArgv(rc, 2, argv, lens);
    if (rs->type == REDIS_REPLY_STRING) {
        val = malloc((rs->len + 1) * sizeof(char));
        memcpy(val, rs->str, (rs->len + 1) * sizeof(char));
        len = rs->len;
        freeReplyObject(rs);
        return mrb_str_new(mrb, val, len);
    } else {
        freeReplyObject(rs);
        return mrb_nil_value();
    }
}

mrb_value mrb_redis_exists(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int counter;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"EXISTS", RSTRING_PTR(key)};
    size_t lens[] = {6, RSTRING_LEN(key)};
    redisReply *rr = redisCommandArgv(rc, 2, argv, lens);
    counter = rr->integer;
    freeReplyObject(rr);

    return counter ? mrb_true_value() : mrb_false_value();
}

mrb_value mrb_redis_expire(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int expire, counter;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oi", &key, &expire);
    mrb_value val = mrb_fixnum_to_str(mrb, mrb_fixnum_value(expire), 10);
    const char *argv[] = {"EXPIRE", RSTRING_PTR(key), RSTRING_PTR(val)};
    size_t lens[] = {6, RSTRING_LEN(key), RSTRING_LEN(val)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    counter = rr->integer;
    freeReplyObject(rr);

    return mrb_bool_value(counter == 1);
}

mrb_value mrb_redis_randomkey (mrb_state *mrb, mrb_value self)
{
    char *val;
    int len;
    redisContext *rc = DATA_PTR(self);

    redisReply *rs = redisCommand(rc, "RANDOMKEY");
    if (rs->type == REDIS_REPLY_STRING) {
        val = malloc((rs->len + 1) * sizeof(char));
        memcpy(val, rs->str, (rs->len + 1) * sizeof(char));
        len = rs->len;
        freeReplyObject(rs);
        return mrb_str_new(mrb, val, len);
    } else {
        freeReplyObject(rs);
        return mrb_nil_value();
    }
}

mrb_value mrb_redis_del(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"DEL", RSTRING_PTR(key)};
    size_t lens[] = {3, RSTRING_LEN(key)};
    redisReply *rs = redisCommandArgv(rc, 2, argv, lens);
    freeReplyObject(rs);

    return  self;
}

mrb_value mrb_redis_incr(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int counter;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"INCR", RSTRING_PTR(key)};
    size_t lens[] = {4, RSTRING_LEN(key)};
    redisReply *rr = redisCommandArgv(rc, 2, argv, lens);
    counter = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(counter);
}

mrb_value mrb_redis_decr(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int counter;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"DECR", RSTRING_PTR(key)};
    size_t lens[] = {4, RSTRING_LEN(key)};
    redisReply *rr = redisCommandArgv(rc, 2, argv, lens);
    counter = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(counter);
}

mrb_value mrb_redis_incrby(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int val, counter;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oi", &key, &val);
    mrb_value val_str = mrb_fixnum_to_str(mrb, mrb_fixnum_value(val), 10);
    const char *argv[] = {"INCRBY", RSTRING_PTR(key), RSTRING_PTR(val_str)};
    size_t lens[] = {6, RSTRING_LEN(key), RSTRING_LEN(val_str)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    counter = rr->integer;
    freeReplyObject(rr);

    return mrb_fixnum_value(counter);
}

mrb_value mrb_redis_decrby(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    mrb_int val, counter;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oi", &key, &val);
    mrb_value val_str = mrb_fixnum_to_str(mrb, mrb_fixnum_value(val), 10);
    const char *argv[] = {"DECRBY", RSTRING_PTR(key), RSTRING_PTR(val_str)};
    size_t lens[] = {6, RSTRING_LEN(key), RSTRING_LEN(val_str)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    counter = rr->integer;
    freeReplyObject(rr);

    return mrb_fixnum_value(counter);
}

mrb_value mrb_redis_rpush(mrb_state *mrb, mrb_value self)
{
    mrb_value key, val;
    mrb_int integer;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oo", &key, &val);
    const char *argv[] = {"RPUSH", RSTRING_PTR(key), RSTRING_PTR(val)};
    size_t lens[] = {5, RSTRING_LEN(key), RSTRING_LEN(val)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    integer = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(integer);
}

mrb_value mrb_redis_lpush(mrb_state *mrb, mrb_value self)
{
    mrb_value key, val;
    mrb_int integer;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oo", &key, &val);
    const char *argv[] = {"LPUSH", RSTRING_PTR(key), RSTRING_PTR(val)};
    size_t lens[] = {5, RSTRING_LEN(key), RSTRING_LEN(val)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    integer = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(integer);
}

mrb_value mrb_redis_rpop(mrb_state *mrb, mrb_value self)
{
    char *val;
    int len;
    mrb_value key;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"RPOP", RSTRING_PTR(key)};
    size_t lens[] = {4, RSTRING_LEN(key)};
    redisReply *rr = redisCommandArgv(rc, 2, argv, lens);
    if (rr->type == REDIS_REPLY_STRING) {
        val = malloc((rr->len + 1) * sizeof(char));
        memcpy(val, rr->str, (rr->len + 1) * sizeof(char));
        len = rr->len;
        freeReplyObject(rr);
        return mrb_str_new(mrb, val, len);
    } else {
        freeReplyObject(rr);
        return mrb_nil_value();
    }
}

mrb_value mrb_redis_lpop(mrb_state *mrb, mrb_value self)
{
    char *val;
    int len;
    mrb_value key;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"LPOP", RSTRING_PTR(key)};
    size_t lens[] = {4, RSTRING_LEN(key)};
    redisReply *rr = redisCommandArgv(rc, 2, argv, lens);
    if (rr->type == REDIS_REPLY_STRING) {
        val = malloc((rr->len + 1) * sizeof(char));
        memcpy(val, rr->str, (rr->len + 1) * sizeof(char));
        len = rr->len;
        freeReplyObject(rr);
        return mrb_str_new(mrb, val, len);
    } else {
        freeReplyObject(rr);
        return mrb_nil_value();
    }
}

mrb_value mrb_redis_lrange(mrb_state *mrb, mrb_value self)
{
    int i;
    mrb_value list, array;
    mrb_int arg1, arg2;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
    mrb_value val_str1 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg1), 10);
    mrb_value val_str2 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg2), 10);
    const char *argv[] = {"LRANGE", RSTRING_PTR(list), RSTRING_PTR(val_str1), RSTRING_PTR(val_str2)};
    size_t lens[] = {6, RSTRING_LEN(list), RSTRING_LEN(val_str1), RSTRING_LEN(val_str2)};
    redisReply *rr = redisCommandArgv(rc, 4, argv, lens);
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

mrb_value mrb_redis_ltrim(mrb_state *mrb, mrb_value self)
{
    mrb_value list;
    mrb_int arg1, arg2, integer;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
    mrb_value val_str1 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg1), 10);
    mrb_value val_str2 = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg2), 10);
    const char *argv[] = {"LTRIM", RSTRING_PTR(list), RSTRING_PTR(val_str1), RSTRING_PTR(val_str2)};
    size_t lens[] = {6, RSTRING_LEN(list), RSTRING_LEN(val_str1), RSTRING_LEN(val_str2)};
    redisReply *rr = redisCommandArgv(rc, 4, argv, lens);
    integer = rr->integer;
    freeReplyObject(rr);

    return  mrb_fixnum_value(integer);
}

mrb_value mrb_redis_hset(mrb_state *mrb, mrb_value self) {
    mrb_value key, field, val;
    redisContext *rc = DATA_PTR(self);
    mrb_int integer;

    mrb_get_args(mrb, "ooo", &key, &field, &val);
    const char *argv[] = {"HSET", RSTRING_PTR(key), RSTRING_PTR(field), RSTRING_PTR(val)};
    size_t lens[] = {4, RSTRING_LEN(key), RSTRING_LEN(field), RSTRING_LEN(val)};
    redisReply *rs = redisCommandArgv(rc, 4, argv, lens);
    integer = rs->integer;
    freeReplyObject(rs);

    return integer ? mrb_true_value() : mrb_false_value();
}

mrb_value mrb_redis_hget(mrb_state *mrb, mrb_value self) {
    mrb_value key, field;
    char *val;
    int len;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oo", &key, &field);
    const char *argv[] = {"HGET", RSTRING_PTR(key), RSTRING_PTR(field)};
    size_t lens[] = {4, RSTRING_LEN(key), RSTRING_LEN(field)};
    redisReply *rs = redisCommandArgv(rc, 3, argv, lens);
    if (rs->type == REDIS_REPLY_STRING) {
        val = malloc((rs->len + 1) * sizeof(char));
        memcpy(val, rs->str, (rs->len + 1) * sizeof(char));
        len = rs->len;
        freeReplyObject(rs);
        return mrb_str_new(mrb, val, len);
    } else {
        freeReplyObject(rs);
        return mrb_nil_value();
    }
}

mrb_value mrb_redis_hgetall(mrb_state *mrb, mrb_value self)
{
    mrb_value obj, hash = mrb_nil_value();
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &obj);
    const char *argv[] = {"HGETALL", RSTRING_PTR(obj)};
    size_t lens[] = {7, RSTRING_LEN(obj)};
    redisReply *rr = redisCommandArgv(rc, 2, argv, lens);
    if (rr->type == REDIS_REPLY_ARRAY) {
        if (rr->elements > 0) {
            int i;
            
            hash = mrb_hash_new(mrb);
            for (i = 0; i < rr->elements; i += 2) {
                mrb_hash_set(mrb, hash,
                             mrb_str_new(mrb, rr->element[i]->str, rr->element[i]->len), 
                             mrb_str_new(mrb, rr->element[i + 1]->str, rr->element[i + 1]->len));
            }
        }
    }
    freeReplyObject(rr);
    return hash;
}

mrb_value mrb_redis_hdel(mrb_state *mrb, mrb_value self) {
    mrb_value key, val;
    mrb_int integer;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oo", &key, &val);
    const char *argv[] = {"HDEL", RSTRING_PTR(key), RSTRING_PTR(val)};
    size_t lens[] = {4, RSTRING_LEN(key), RSTRING_LEN(val)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    integer = rr->integer;
    freeReplyObject(rr);

    return mrb_fixnum_value(integer);
}

mrb_value mrb_redis_hkeys(mrb_state *mrb, mrb_value self)
{
    mrb_value key, array = mrb_nil_value();
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"HKEYS", RSTRING_PTR(key)};
    size_t lens[] = {5, RSTRING_LEN(key)};
    redisReply *rr = redisCommandArgv(rc, 2, argv, lens);
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

mrb_value mrb_redis_ttl(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    redisContext *rc = DATA_PTR(self);
    mrb_int integer;

    mrb_get_args(mrb, "o", &key);
    const char *argv[] = {"TTL", RSTRING_PTR(key)};
    size_t lens[] = {3, RSTRING_LEN(key)};
    redisReply *rr = redisCommandArgv(rc, 2, argv, lens);
    integer = rr->integer;
    freeReplyObject(rr);

    return mrb_fixnum_value(integer);
}

mrb_value mrb_redis_zadd(mrb_state *mrb, mrb_value self)
{
    mrb_value key, member;
    mrb_float score;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "ofo", &key, &score, &member);
    mrb_value score_str = mrb_float_to_str(mrb, mrb_float_value(mrb, score), "%f");
    const char *argv[] = {"ZADD", RSTRING_PTR(key), RSTRING_PTR(score_str), RSTRING_PTR(member)};
    size_t lens[] = {4, RSTRING_LEN(key), RSTRING_LEN(score_str), RSTRING_LEN(member)};
    redisReply *rs = redisCommandArgv(rc, 4, argv, lens);
    freeReplyObject(rs);

    return  self;
}

mrb_value mrb_redis_basic_zrange(mrb_state *mrb, mrb_value self, const char *cmd)
{
    int i;
    mrb_value list, array;
    mrb_int arg1, arg2;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oii", &list, &arg1, &arg2);
    mrb_value arg1_str = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg1), 10);
    mrb_value arg2_str = mrb_fixnum_to_str(mrb, mrb_fixnum_value(arg2), 10);
    const char *argv[] = {"ZADD", RSTRING_PTR(list), RSTRING_PTR(arg1_str), RSTRING_PTR(arg2_str)};
    size_t lens[] = {4, RSTRING_LEN(list), RSTRING_LEN(arg1_str), RSTRING_LEN(arg2_str)};
    redisReply *rr = redisCommandArgv(rc, 4, argv, lens);
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

mrb_value mrb_redis_zrange(mrb_state *mrb, mrb_value self)
{
    return mrb_redis_basic_zrange(mrb, self, "ZRANGE");
}

mrb_value mrb_redis_zrevrange(mrb_state *mrb, mrb_value self)
{
    return mrb_redis_basic_zrange(mrb, self, "ZREVRANGE");
}

mrb_value mrb_redis_basic_zrank(mrb_state *mrb, mrb_value self, const char* cmd)
{
    mrb_value key, member;
    mrb_int rank;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oo", &key, &member);
    const char *argv[] = {cmd, RSTRING_PTR(key), RSTRING_PTR(member)};
    size_t lens[] = {strlen(cmd), RSTRING_LEN(key), RSTRING_LEN(member)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    rank = rr->integer;
    freeReplyObject(rr);

    return mrb_fixnum_value(rank);
}

mrb_value mrb_redis_zrank(mrb_state *mrb, mrb_value self)
{
    return mrb_redis_basic_zrank(mrb, self, "ZRANK");
}

mrb_value mrb_redis_zrevrank(mrb_state *mrb, mrb_value self)
{
    return mrb_redis_basic_zrank(mrb, self, "ZREVRANK");
}

mrb_value mrb_redis_zscore(mrb_state *mrb, mrb_value self)
{
    mrb_value key, member, score;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oo", &key, &member);
    const char *argv[] = {"ZSCORE", RSTRING_PTR(key), RSTRING_PTR(member)};
    size_t lens[] = {6, RSTRING_LEN(key), RSTRING_LEN(member)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    score = mrb_str_new(mrb, rr->str, rr->len);
    freeReplyObject(rr);

    return score;
}

mrb_value mrb_redis_pub(mrb_state *mrb, mrb_value self)
{
    mrb_value channel, msg;
    redisContext *rc = DATA_PTR(self);

    mrb_get_args(mrb, "oo", &channel, &msg);
    const char *argv[] = {"PUBLISH", RSTRING_PTR(channel), RSTRING_PTR(msg)};
    size_t lens[] = {7, RSTRING_LEN(channel), RSTRING_LEN(msg)};
    redisReply *rr = redisCommandArgv(rc, 3, argv, lens);
    freeReplyObject(rr);

    return  self;
}

mrb_value mrb_redis_close(mrb_state *mrb, mrb_value self)
{
    redisContext *rc = DATA_PTR(self);

    redisFree(rc);

    return mrb_nil_value();
}

void mrb_mruby_redis_gem_init(mrb_state *mrb)
{
    struct RClass *redis;

    redis = mrb_define_class(mrb, "Redis", mrb->object_class);

    mrb_define_method(mrb, redis, "initialize", mrb_redis_connect, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "select", mrb_redis_select, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, redis, "set", mrb_redis_set, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "get", mrb_redis_get, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "exists?", mrb_redis_exists, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, redis, "expire", mrb_redis_expire, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "randomkey", mrb_redis_randomkey, MRB_ARGS_NONE());
    mrb_define_method(mrb, redis, "[]=", mrb_redis_set, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "[]", mrb_redis_get, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "del", mrb_redis_del, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "incr", mrb_redis_incr, MRB_ARGS_OPT(1));
    mrb_define_method(mrb, redis, "decr", mrb_redis_decr, MRB_ARGS_OPT(1));
    mrb_define_method(mrb, redis, "incrby", mrb_redis_incrby, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "decrby", mrb_redis_decrby, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "rpush", mrb_redis_rpush, MRB_ARGS_OPT(2));
    mrb_define_method(mrb, redis, "lpush", mrb_redis_lpush, MRB_ARGS_OPT(2));
    mrb_define_method(mrb, redis, "rpop", mrb_redis_rpop, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, redis, "lpop", mrb_redis_lpop, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, redis, "lrange", mrb_redis_lrange, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "ltrim", mrb_redis_ltrim, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "hset", mrb_redis_hset, MRB_ARGS_REQ(3));
    mrb_define_method(mrb, redis, "hget", mrb_redis_hget, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "hgetall", mrb_redis_hgetall, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, redis, "hdel", mrb_redis_hdel, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "hkeys", mrb_redis_hkeys, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, redis, "ttl", mrb_redis_ttl, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "zadd", mrb_redis_zadd, MRB_ARGS_REQ(3));
    mrb_define_method(mrb, redis, "zrange", mrb_redis_zrange, MRB_ARGS_REQ(3));
    mrb_define_method(mrb, redis, "zrevrange", mrb_redis_zrevrange, MRB_ARGS_REQ(3));
    mrb_define_method(mrb, redis, "zrank", mrb_redis_zrank, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "zrevrank", mrb_redis_zrevrank, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "zscore", mrb_redis_zscore, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, redis, "publish", mrb_redis_pub, MRB_ARGS_ANY());
    mrb_define_method(mrb, redis, "close", mrb_redis_close, MRB_ARGS_NONE());
    DONE;
}

void mrb_mruby_redis_gem_final(mrb_state *mrb)
{
}

//#endif
