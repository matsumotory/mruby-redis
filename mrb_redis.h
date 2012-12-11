#ifndef MRB_REDIS_H
#define MRB_REDIS_H

mrb_value mrb_redis_connect(mrb_state *mrb, mrb_value self);
mrb_value mrb_redis_set(mrb_state *mrb, mrb_value self);
mrb_value mrb_redis_get(mrb_state *mrb, mrb_value self);
//mrb_value ap_mrb_redis_mget(mrb_state *mrb, mrb_value self);

#endif
