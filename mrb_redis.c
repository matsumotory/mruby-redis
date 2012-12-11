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
#include <mruby.h>
#include <mruby/data.h>
#include <mruby/variable.h>
#include <mruby/array.h>
#include <mruby/string.h>
//#include <mrb_redis.h>

//#ifdef ENABLE_REDIS

mrb_value mrb_redis_connect(mrb_state *mrb, mrb_value self)
{
    mrb_value host, port;

    mrb_get_args(mrb, "oo", &host, &port);

    redisContext *rc = redisConnect(RSTRING_PTR(host), mrb_fixnum(port));
    if (rc->err)
        mrb_raise(mrb, E_RUNTIME_ERROR, "redis connection faild.");

    mrb_iv_set(mrb, self, mrb_intern(mrb, "redis_c"), mrb_obj_value(Data_Wrap_Struct(mrb, mrb->object_class, NULL, (void*) rc)));
    return self;
}

mrb_value mrb_redis_set(mrb_state *mrb, mrb_value self)
{
    mrb_value key, val;
    redisContext *rc;
    mrb_value context;

    mrb_get_args(mrb, "oo", &key, &val);

    context = mrb_iv_get(mrb, self, mrb_intern(mrb, "redis_c"));
    Data_Get_Struct(mrb, context, NULL, rc);
    if (!rc)
        mrb_raise(mrb, E_RUNTIME_ERROR, "mrb_iv_get redis_c failed");

    redisReply *rs;
    rs = redisCommand(rc,"set %s %s", RSTRING_PTR(key), RSTRING_PTR(val));
    freeReplyObject(rs);

    return  self;
}

mrb_value mrb_redis_get(mrb_state *mrb, mrb_value self)
{
    mrb_value key;
    redisContext *rc;
    mrb_value context;
    char *val;

    mrb_get_args(mrb, "o", &key);

    context = mrb_iv_get(mrb, self, mrb_intern(mrb, "redis_c"));
    Data_Get_Struct(mrb, context, NULL, rc);

    redisReply *rs;
    rs = redisCommand(rc,"get %s", RSTRING_PTR(key));
    if (rs->type == REDIS_REPLY_STRING) {
        val = strdup(rs->str);
        freeReplyObject(rs);
        return mrb_str_new(mrb, val, strlen(val));
    } else {
        return mrb_nil_value();
    }
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

void mrb_redis_init(mrb_state *mrb, struct RClass *class_core)
{
    struct RClass *redis;

    redis = mrb_define_module(mrb, "Redis");
    mrb_define_method(mrb, redis, "initialize", mrb_redis_connect, ARGS_ANY());
    mrb_define_method(mrb, redis, "set", mrb_redis_set, ARGS_ANY());
    mrb_define_method(mrb, redis, "get", mrb_redis_get, ARGS_ANY());
}

//#endif
