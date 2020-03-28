/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2020 The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_time.h>
#include <msgpack.h>

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "klog.h"
#include <zlib.h>

#ifdef FLB_SYSTEM_WINDOWS
#define NEWLINE "\r\n"
#else
#define NEWLINE "\n"
#endif

#define ZLIB_GZIP_ENCODING (16)

struct flb_file_conf {
    const char *out_file;
    time_t last_file_creation;
    int rotate_duration;
    gzFile *gfp;
    struct flb_output_instance *ins;
    char tmp_current_file_name[128];
    char current_file_name[128];
};


static int cb_klog_init(struct flb_output_instance *ins,
                        struct flb_config *config,
                        void *data)
{
    int ret;
    const char *tmp;
    char *ret_str;
    (void) config;
    (void) data;
    struct flb_file_conf *ctx;

    ctx = flb_calloc(1, sizeof(struct flb_file_conf));
    if (!ctx) {
        flb_errno();
        return -1;
    }
    ctx->ins = ins;
    ctx->rotate_duration = 0;
    ctx->last_file_creation = 0;
    ctx->gfp=NULL;
    ctx->tmp_current_file_name[0]=0;
    ctx->current_file_name[0]=0;

    ret = flb_output_config_map_set(ins, (void *) ctx);
    if (ret == -1) {
        flb_free(ctx);
        return -1;
    }
    tmp = flb_output_get_property("Rotate_Duration", ins);
    if (tmp) {
        ctx->rotate_duration = (int)strtof(tmp, NULL); 
    }


    /* Set the context */
    flb_output_set_context(ins, ctx);

    return 0;
}


static int gzip_plain_output(gzFile *gfp, msgpack_object *obj, size_t alloc_size)
{
    char *buf;

    buf = flb_msgpack_to_json_str(alloc_size, obj);
    if (buf) {
        int len=strlen(buf);
        int ret=gzwrite(gfp,buf,len);
        flb_debug("[output] gzipped %d/%d bytes into %p",ret,len,gfp);
        flb_free(buf);
    }
    return 0;
}


static void close_file(struct flb_file_conf *ctx) 
{    
    if (ctx->gfp) {
        flb_debug("[output] close gz %p",ctx->gfp);
        gzflush(ctx->gfp,Z_FULL_FLUSH);
        if (gzclose(ctx->gfp) != Z_OK)  {
            flb_debug("[output] error closing gzip file");
        }
        flb_debug("[output] rename file %s=>%s",ctx->tmp_current_file_name,ctx->current_file_name);
        rename ( ctx->tmp_current_file_name,ctx->current_file_name );

        ctx->current_file_name[0]=0;
        ctx->tmp_current_file_name[0]=0;
        ctx->gfp=NULL;
    }
    ctx->last_file_creation=0;
}


static void open_file(struct flb_file_conf *ctx,const char* file_name_template) 
{
    time_t now=time(NULL);
    if (ctx->rotate_duration>0 && ctx->last_file_creation!=0) {
       time_t t=ctx->last_file_creation;
       t -= (t % ctx->rotate_duration);
       double diff_t = difftime(now, t);
       if (diff_t>ctx->rotate_duration) {        
           flb_debug("rotate file!");
           close_file(ctx);
       }
    }

    if (!ctx->gfp) {
        struct tm * timeinfo;
        timeinfo = localtime (&now);
        strftime (ctx->current_file_name,sizeof(ctx->current_file_name),file_name_template,timeinfo);
        sprintf(ctx->tmp_current_file_name,"%s.tmp",ctx->current_file_name);
        ctx->gfp = (gzFile *)gzopen(ctx->tmp_current_file_name ,"wb4");
        flb_debug("gzip file opened %s %p",ctx->tmp_current_file_name,ctx->gfp);
        ctx->last_file_creation=now;
    }

}
static void cb_klog_flush(const void *data, size_t bytes,
                          const char *tag, int tag_len,
                          struct flb_input_instance *i_ins,
                          void *out_context,
                          struct flb_config *config)
{
    int ret;
    msgpack_unpacked result;
    size_t off = 0;
    size_t last_off = 0;
    size_t alloc_size = 0;
    size_t total;
    const char *out_file;
    char *buf;
    char *tag_buf;
    msgpack_object *obj;
    struct flb_file_conf *ctx = out_context;
    struct flb_time tm;
    (void) i_ins;
    (void) config;

    /* Set the right output */
    if (!ctx->out_file) {
        out_file = tag;
    }
    else {
        out_file = ctx->out_file;
    }

    flb_debug("Output instace %p %s %s",ctx,out_file,tag);

    /* Open output file with default name as the Tag */
    open_file(ctx,out_file);
    if (ctx->gfp == NULL) {
        flb_errno();
        FLB_OUTPUT_RETURN(FLB_ERROR);
    }

    tag_buf = flb_malloc(tag_len + 1);
    if (!tag_buf) {
        flb_errno();
        close_file(ctx);
        FLB_OUTPUT_RETURN(FLB_RETRY);
    }
    memcpy(tag_buf, tag, tag_len);
    tag_buf[tag_len] = '\0';


    /*
     * Upon flush, for each array, lookup the time and the first field
     * of the map to use as a data point.
     */
    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off) == MSGPACK_UNPACK_SUCCESS) {
        alloc_size = (off - last_off) + 128; /* JSON is larger than msgpack */
        last_off = off;

        flb_time_pop_from_msgpack(&tm, &result, &obj);
        gzip_plain_output(ctx->gfp, obj, alloc_size);           
    }

    flb_free(tag_buf);
    msgpack_unpacked_destroy(&result);

    FLB_OUTPUT_RETURN(FLB_OK);
}

static int cb_klog_exit(void *data, struct flb_config *config)
{
    struct flb_file_conf *ctx = data;

    close_file(ctx);

    if (!ctx) {
        return 0;
    }

    flb_free(ctx);
    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "path", NULL,
     0, FLB_TRUE, offsetof(struct flb_file_conf, out_file),
     NULL
    },
    {
     FLB_CONFIG_MAP_STR, "rotate_duration", NULL,
     0, FLB_FALSE, 0,
     NULL
    },

    /* EOF */
    {0}
};

struct flb_output_plugin out_klog_plugin = {
    .name         = "klog",
    .description  = "Generate klog file",
    .cb_init      = cb_klog_init,
    .cb_flush     = cb_klog_flush,
    .cb_exit      = cb_klog_exit,
    .config_map   = config_map,
    .flags        = 0,
};
