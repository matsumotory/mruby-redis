/*
** host_ud.c - test helper: sets mrb->ud the way a host application does
**
** A host application that embeds mruby may keep its own data in mrb->ud
** (ngx_mruby's stream module keeps its context there). The tests in
** redis.rb use this helper to place such data before they call the gem,
** and to check that the gem left it unchanged.
*/

#include <mruby.h>
#include <string.h>

#define HOST_UD_SIZE 64
#define HOST_UD_BYTE 0xa5

static unsigned char host_ud[HOST_UD_SIZE];

static mrb_value host_ud_set(mrb_state *mrb, mrb_value self)
{
  memset(host_ud, HOST_UD_BYTE, sizeof(host_ud));
  mrb->ud = host_ud;
  return mrb_nil_value();
}

static mrb_value host_ud_clear(mrb_state *mrb, mrb_value self)
{
  mrb->ud = NULL;
  return mrb_nil_value();
}

/* true when mrb->ud still points to the host's data and the data is unchanged */
static mrb_value host_ud_intact(mrb_state *mrb, mrb_value self)
{
  size_t i;

  if (mrb->ud != (void *)host_ud) {
    return mrb_false_value();
  }
  for (i = 0; i < sizeof(host_ud); i++) {
    if (host_ud[i] != HOST_UD_BYTE) {
      return mrb_false_value();
    }
  }
  return mrb_true_value();
}

static mrb_value host_ud_null(mrb_state *mrb, mrb_value self)
{
  return mrb_bool_value(mrb->ud == NULL);
}

void mrb_mruby_redis_gem_test(mrb_state *mrb)
{
  struct RClass *m = mrb_define_module(mrb, "HostUserData");

  mrb_define_module_function(mrb, m, "set", host_ud_set, MRB_ARGS_NONE());
  mrb_define_module_function(mrb, m, "clear", host_ud_clear, MRB_ARGS_NONE());
  mrb_define_module_function(mrb, m, "intact?", host_ud_intact, MRB_ARGS_NONE());
  mrb_define_module_function(mrb, m, "null?", host_ud_null, MRB_ARGS_NONE());
}
