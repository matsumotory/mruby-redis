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
    //struct redis_object_data *data = malloc(sizeof(struct redis_object_data));

    //return mrb_obj_value(Data_Wrap_Struct(mrb, mrb_class_ptr(self), &redis_object_data_type, data));
    return self;
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

mrb_value mrb_redis_close(mrb_state *mrb, mrb_value self)
{
    redisContext *rc;
    mrb_value context;

    context = mrb_iv_get(mrb, self, mrb_intern(mrb, "redis_c"));
    Data_Get_Struct(mrb, context, &redisContext_type, rc);

    return mrb_nil_value();
}

/*
mrb_value mrb_redis_mget(mrb_state *mrb, mrb_value self)
{
    long int i;
    mrb_value key, array;
    redisContext *rc;
    mrb_value context;
    char **val;
    request_rec *r = ap_mrb_get_request();

    mrb_get_args(mrb, "o", &key);

    context = mrb_iv_get(mrb, self, mrb_intern(mrb, "redis_c"));
    Data_Get_Struct(mrb, context, NULL, rc);

    redisReply *rs;
    //rs = redisCommand(rc,"mget %s", RSTRING_PTR(key));
    rs = redisCommand(rc,"keys *");
    val = apr_pcalloc(r->pool, sizeof(char **));
    if (rs->type == REDIS_REPLY_ARRAY ) {
        for(i = 0; i < rs->elements; i++){
            array = mrb_ary_new(mrb);
        ap_log_error(APLOG_MARK
            , APLOG_ERR
            , 0
            , NULL
            , "%s ERROR %s: key=(%s) redis array result: %d) %s"
            , MODULE_NAME
            , __func__
            , RSTRING_PTR(key)
            , i
            , rs->element[i]->str
        );
            //mrb_ary_push(mrb, array, mrb_str_new2(mrb, rs->element[i]->str));
            val[i] = apr_pstrdup(r->pool, rs->element[i]->str);
            mrb_ary_set(mrb, array, i, mrb_str_new2(mrb, val[i]));
        }
        for (i = 0; i < rs->elements; i++){
        ap_log_error(APLOG_MARK
            , APLOG_ERR
            , 0
            , NULL
            , "%s ERROR %s: key=(%s) redis array val: %d) %s"
            , MODULE_NAME
            , __func__
            , RSTRING_PTR(key)
            , i
            , val[i]
        );
            
        }
        freeReplyObject(rs);
        return array;
    } else {
        return mrb_nil_value();
    }
}
*/

void mrb_redis_init(mrb_state *mrb)
{
    struct RClass *redis;
    //struct RClass *redis_basic;

    //redis = mrb_define_module(mrb, "Redis");
    redis = mrb_define_class(mrb, "Redis", mrb->object_class);
    //MRB_SET_INSTANCE_TT(redis, MRB_TT_DATA);

    //redis_basic = mrb_define_class_under(mrb, redis, "Basic", mrb->object_class);
    //mrb_define_class_method(mrb, redis, "new", mrb_redis_connect, ARGS_OPT(2));
    //mrb_define_class_method(mrb, redis, "open", mrb_redis_connect, ARGS_OPT(2));
    mrb_define_method(mrb, redis, "initialize", mrb_redis_connect, ARGS_ANY());
    mrb_define_method(mrb, redis, "set", mrb_redis_set, ARGS_ANY());
    mrb_define_method(mrb, redis, "get", mrb_redis_get, ARGS_ANY());
    mrb_define_method(mrb, redis, "[]=", mrb_redis_set, ARGS_ANY());
    mrb_define_method(mrb, redis, "[]", mrb_redis_get, ARGS_ANY());
    mrb_define_method(mrb, redis, "del", mrb_redis_del, ARGS_ANY());
    mrb_define_method(mrb, redis, "incr", mrb_redis_incr, ARGS_OPT(1));
    mrb_define_method(mrb, redis, "decr", mrb_redis_decr, ARGS_OPT(1));
    mrb_define_method(mrb, redis, "close", mrb_redis_close, ARGS_NONE());
    DONE;
}

//#endif
