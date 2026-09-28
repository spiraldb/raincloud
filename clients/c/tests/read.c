/* SPDX-License-Identifier: Apache-2.0 */
/* Usage: read-c OPTIONS.json MISSING.json [no-vortex]
 * MISSING.json selects the same catalog with an empty data_dir (offline);
 * no-vortex expects a library built without the Vortex reader. */
#include "raincloud.h"
#include "raincloud_arrow_abi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/* Always on, unlike assert(): every call below has side effects, and a
 * Release (NDEBUG) build of this consumer must still make and check them. */
#define CHECK(cond) do { if(!(cond)) { \
  fprintf(stderr,"%s:%d: check failed: %s\n",__FILE__,__LINE__,#cond); exit(1); } } while(0)
static char *slurp(const char *path) {
  FILE *f=fopen(path,"rb"); CHECK(f);
  CHECK(fseek(f,0,SEEK_END)==0); long size=ftell(f); CHECK(size>=0); rewind(f);
  char *text=calloc((size_t)size+1,1); CHECK(text);
  size_t got=fread(text,1,(size_t)size,f); CHECK(got==(size_t)size); fclose(f);
  return text;
}
static raincloud_dataset *open_or_die(const char *options,const char *format) {
  raincloud_dataset *ds=NULL; raincloud_error error={0};
  int code=raincloud_open(options,"tiny",format,&ds,&error);
  if(code){fprintf(stderr,"open %s: %d %s\n",format,code,error.message?error.message:"(no message)");exit(1);}
  return ds;
}
int main(int argc,char **argv) {
  CHECK(argc==3||argc==4);
  int no_vortex=argc==4&&strcmp(argv[3],"no-vortex")==0;
  char *options=slurp(argv[1]), *missing=slurp(argv[2]);
  const char *formats[]={"arrow","parquet","vortex"};
  for(int i=0;i<3;++i) {
    raincloud_dataset *ds=open_or_die(options,formats[i]);
    struct ArrowArrayStream stream={0}; raincloud_error error={0};
    int code=raincloud_batches(ds,2,&stream,&error);
    raincloud_close(ds);
    if(no_vortex&&i==2) {
      CHECK(code==RAINCLOUD_FORMAT_UNAVAILABLE); CHECK(!stream.release);
      raincloud_error_free(&error); continue;
    }
    if(code){fprintf(stderr,"batches %s: %d %s\n",formats[i],code,error.message?error.message:"(no message)");return 1;}
    struct ArrowSchema schema={0}; CHECK(stream.get_schema(&stream,&schema)==0);
    CHECK(schema.n_children==4); schema.release(&schema);
    int64_t rows=0;
    while(1) {
      struct ArrowArray batch={0};
      if(stream.get_next(&stream,&batch)!=0){fprintf(stderr,"%s\n",stream.get_last_error(&stream));return 1;}
      if(!batch.release)break;
      CHECK(batch.length<=2);
      struct ArrowArray *id=batch.children[0];
      for(int64_t j=0;j<batch.length;++j) {
        uint64_t expected=rows+j==7 ? UINT64_MAX : (uint64_t)(rows+j);
        CHECK(((const uint64_t*)id->buffers[1])[id->offset+j]==expected);
      }
      rows+=batch.length; batch.release(&batch);
    }
    CHECK(rows==8); stream.release(&stream);
  }
  /* A resolution failure keeps its code. */
  raincloud_dataset *ds=open_or_die(missing,"arrow");
  char *path=NULL; raincloud_error error={0};
  CHECK(raincloud_path(ds,&path,&error)==RAINCLOUD_OFFLINE_MISS); CHECK(!path);
  CHECK(error.code==RAINCLOUD_OFFLINE_MISS&&error.message);
  raincloud_error_free(&error); raincloud_close(ds);
  free(options); free(missing);
  puts(no_vortex ? "C: IPC and Parquet, eight rows, Vortex refused, typed offline miss"
                 : "C: three formats, eight rows, independent stream lifetime, typed offline miss");
  return 0;
}
