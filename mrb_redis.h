/*
// mrb_redis.h - to provide redis methods
//
// See Copyright Notice in mrb_redis.c
*/

#ifndef MRB_REDIS_H
#define MRB_REDIS_H

mrb_value mrb_redis_connect(mrb_state *mrb, mrb_value self);
mrb_value mrb_redis_set(mrb_state *mrb, mrb_value self);
mrb_value mrb_redis_get(mrb_state *mrb, mrb_value self);
void mrb_redis_init(mrb_state *mrb);

#endif
