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
#include "hash.h"

#ifdef FLB_SYSTEM_WINDOWS
#define NEWLINE "\r\n"
#else
#define NEWLINE "\n"
#endif

#define ZLIB_GZIP_ENCODING (16)

#define GZIP_CHUNK_SIZE 16384

struct flb_klog_conf {
    const char *out_file_template;
    int rotate_duration;
    struct flb_output_instance *ins;
    struct flb_regex* tag_regex;
    struct hash_t hash_table;
};

struct flb_klog_file_output_ctx {
    char tmp_current_file_name[128];
    char current_file_name[128];
    unsigned char* out_buffer;
    z_stream zstream;
    FILE* file;
};

struct flb_klog_container_ctx {
    struct flb_klog_file_output_ctx files[2];
    time_t last_file_creation;
    int rotate_duration;
    char  pod_name[128];
};


static void cb_results(const char *name, const char *value,
                       size_t vlen, void *data)
{
    struct flb_klog_container_ctx *ctx = data;

    if (vlen == 0) {
        return;
    }
    if (strcmp(name, "pod_name") == 0) {
        memcpy(ctx->pod_name,value,vlen);
        flb_debug("[cb_results] ctx=%p, podname=%s",ctx,ctx->pod_name);
    }
    return;
}

struct flb_klog_container_ctx* get_container_log(struct flb_klog_conf* ctx,const char* tag,int tag_len) {

    struct flb_klog_container_ctx *container_ctx = NULL;

    container_ctx=hash_lookup(&ctx->hash_table, tag);
    if (container_ctx==NULL) {
        flb_debug("[klog] [get_container_log] didn't find key %s in hash allocating",tag);
        container_ctx = flb_calloc(1,sizeof(struct flb_klog_container_ctx));
        if (!container_ctx) {
            return NULL;
        }
        for (int i=0;i<2;i++) {
            container_ctx->files[i].out_buffer=flb_malloc(GZIP_CHUNK_SIZE);
        }
        container_ctx->rotate_duration=ctx->rotate_duration;

        struct flb_regex_search result;

        flb_regex_do(ctx->tag_regex, tag, tag_len, &result);
        flb_regex_parse(ctx->tag_regex, &result, cb_results, container_ctx);
    
        hash_insert(&ctx->hash_table,tag, tag_len, container_ctx);

        flb_debug("[klog] [get_container_log] inserted instance %p  (pod=%s)",container_ctx, container_ctx->pod_name);
    } else {
        flb_debug("[klog] [get_container_log] found  key %s in hash - %p",tag,container_ctx);
    }
    return container_ctx;
}


static int cb_klog_init(struct flb_output_instance *ins,
                        struct flb_config *config,
                        void *data)
{
    int ret;
    const char *tmp;
    (void) config;
    (void) data;
    struct flb_klog_conf *ctx;

    ctx = flb_calloc(1, sizeof(struct flb_klog_conf));
    if (!ctx) {
        flb_errno();
        return -1;
    }
    ctx->ins = ins;
    ctx->rotate_duration = 0;
    ctx->tag_regex=flb_regex_create(KUBE_TAG_TO_REGEX);
    hash_init(&ctx->hash_table, 256);

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
    
    flb_debug("[klog] cb_klog_init %p",ctx);

    return 0;
}

static int write_gzip(struct flb_klog_file_output_ctx *ctx,char *buf,size_t len) 
{
    ctx->zstream.avail_in=len;
    ctx->zstream.next_in=buf;
    do {
        ctx->zstream.avail_out = GZIP_CHUNK_SIZE;
        ctx->zstream.next_out = ctx->out_buffer;
        int flush = len==0 ? Z_FINISH : Z_NO_FLUSH;
        int ret = deflate(&ctx->zstream, flush);    /* no bad return value */
        assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
        int have = GZIP_CHUNK_SIZE - ctx->zstream.avail_out;
        //flb_debug("[klog] write_gzip %p %d / %d %d %d bytes into file",ctx->file,have,len,flush, ctx->zstream.avail_out);
        if (have>0) {
            if (fwrite(ctx->out_buffer, 1, have, ctx->file) != have || ferror(ctx->file)) {
                (void)deflateEnd(&ctx->zstream);
                return Z_ERRNO;
            }
        } 
    } while (ctx->zstream.avail_out == 0);
    return Z_ERRNO;
}

int find_next_quote(const char *source,int from,int len) {
    for (int i=from;i<len;i++) {
        if (source[i]=='\\') {
            continue;
        }
        if (source[i]=='"')
            return i+1;
    }
    return -1;
}

static int gzip_plain_output(struct flb_klog_container_ctx *ctx, msgpack_object *obj, size_t alloc_size)
{
    const char *buf=NULL;
    int len=0;
    msgpack_object_kv *kv;

    for (int i = 0; i < obj->via.map.size; i++) {
        kv = obj->via.map.ptr + i;
        if (!strncmp(kv->key.via.str.ptr, "log", 3)) {
            buf=kv->val.via.str.ptr;
            len=kv->val.via.str.size;
        }
        //flb_debug("[klog] key=%s value=%s %d",kv->key.via.str.ptr,buf,len);
    }

    //buf = flb_msgpack_to_json_str(alloc_size, obj);
    if (buf!=NULL) {
        write_gzip(&ctx->files[0],buf,len);
        //flb_debug("[klog] gzipped %d bytes into %p",len,ctx);
    }
    return 0;
}


static void close_file(struct flb_klog_file_output_ctx *ctx) 
{
    if (ctx->file!=NULL) {
        flb_debug("[klog] %p close gz %p (%s)",ctx,ctx->file,ctx->tmp_current_file_name);
        write_gzip(ctx,NULL,0);
        deflateEnd(&ctx->zstream);
        fclose(ctx->file);
        flb_debug("[klog] rename file %s=>%s",ctx->tmp_current_file_name,ctx->current_file_name);
        rename ( ctx->tmp_current_file_name,ctx->current_file_name );

        ctx->current_file_name[0]=0;
        ctx->tmp_current_file_name[0]=0;
        ctx->file   =NULL;
    }
}


static void close_container_files(struct flb_klog_container_ctx *ctx) 
{    
    flb_debug("[klog] %p close_file",ctx);
    for (int i=0;i<2;i++) {
        close_file(&ctx->files[i]);
    }
    ctx->last_file_creation=0;
}


static bool open_file(struct flb_klog_file_output_ctx *ctx,const char* type,const char* file_name_template) 
{
    time_t now=time(NULL);

    if (ctx->file==NULL) {
        struct tm * timeinfo;
        timeinfo = localtime (&now);
        char tmp[1024];
        strftime (tmp,sizeof(tmp),file_name_template,timeinfo);
        sprintf(ctx->current_file_name,"%s-%s.gz",tmp,type);
        sprintf(ctx->tmp_current_file_name,"%s.tmp",ctx->current_file_name);
        memset(&ctx->zstream, 0, sizeof(z_stream));

       // ctx->zstream.zalloc = flb_malloc;
       // ctx->zstream.zfree = flb_free;
       // ctx->zstream.avail_out = GZIP_CHUNK_SIZE;
       /// ctx->zstream.next_out = ctx->out_buffer;
        int rc = deflateInit2(&ctx->zstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS | ZLIB_GZIP_ENCODING, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);
        if (rc != Z_OK)
        {  
            flb_warn("compressor_thread: deflateInit2 failed %d", rc);
            return false;
        }
        ctx->file = fopen (ctx->tmp_current_file_name, "ab");
        
        //ctx->gfp = gzopen(ctx->tmp_current_file_name ,"wb4");
        flb_debug("[klog] %p gzip file opened %s %p",ctx,ctx->tmp_current_file_name,ctx->file);
    }
    return true;

}

static bool open_containers_file(struct flb_klog_container_ctx *ctx,const char* file_name_template) 
{
    time_t now=time(NULL);
    if (ctx->rotate_duration>0 && ctx->last_file_creation!=0) {
       time_t t=ctx->last_file_creation;
       t -= (t % ctx->rotate_duration);
       double diff_t = difftime(now, t);
       if (diff_t>ctx->rotate_duration) {        
           flb_debug("[klog] rotate file! for ctx=%p",ctx);
           close_container_files(ctx);
       }
    }

    for (int i=0;i<2;i++) {
        open_file(&ctx->files[i],i==0 ? "stdout" : "stderr",file_name_template);
    }
    return true;

}



static void cb_klog_flush(const void *data, size_t bytes,
                          const char *tag, int tag_len,
                          struct flb_input_instance *i_ins,
                          void *out_context,
                          struct flb_config *config)
{

    msgpack_unpacked result;
    size_t off = 0;
    size_t last_off = 0;
    size_t alloc_size = 0;
    msgpack_object *obj;
    struct flb_klog_conf *ctx = out_context;
    struct flb_time tm;
    (void) i_ins;
    (void) config;

    /* Set the right output */



    struct flb_klog_container_ctx* container_ctx=get_container_log(ctx,tag,tag_len);

    flb_debug("[klog] instance %p %p tag=%s",ctx,container_ctx,tag);

    /* Open output file with default name as the Tag */
    if (!open_containers_file(container_ctx,ctx->out_file_template)) {
        flb_errno();
        FLB_OUTPUT_RETURN(FLB_ERROR);
    }

    /*
     * Upon flush, for each array, lookup the time and the first field
     * of the map to use as a data point.
     */
    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off) == MSGPACK_UNPACK_SUCCESS) {
        alloc_size = (off - last_off) + 128; /* JSON is larger than msgpack */
        //flb_debug("[klog] msgpack_unpack_next %d %d %d %d",bytes,alloc_size,last_off,off);
        last_off = off;

        flb_time_pop_from_msgpack(&tm, &result, &obj);
        gzip_plain_output(container_ctx, obj, alloc_size);           
    }

    msgpack_unpacked_destroy(&result);

    FLB_OUTPUT_RETURN(FLB_OK);
}
static void cb_delete_container_context(char* key,void *data) 
{
    struct flb_klog_container_ctx *ctx = data;
    flb_debug("[klog] delete container context key=%s %p",key,ctx);
    close_container_files(ctx);

    flb_debug("[klog] ~delete container context key=%s %p",key,ctx);
}

static int cb_klog_exit(void *data, struct flb_config *config)
{
    struct flb_klog_conf *ctx = data;

    if (!ctx) {
        return 0;
    }
    flb_debug("[klog] exit");

    hash_destroy(&ctx->hash_table,cb_delete_container_context);

    flb_regex_destroy(ctx->tag_regex);
    flb_free(ctx);
    flb_debug("[klog] ~exit");

    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "path", NULL,
     0, FLB_TRUE, offsetof(struct flb_klog_conf, out_file_template),
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
