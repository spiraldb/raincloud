// SPDX-License-Identifier: Apache-2.0
// Usage: read-cpp OPTIONS.json MISSING.json [no-vortex]  (see read.c)
#include "raincloud_arrow.hpp"
#include <arrow/record_batch.h>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
// Always on, unlike assert(): the calls it wraps must run in a Release build.
#define CHECK(cond) do { if(!(cond)) { \
  std::cerr<<__FILE__<<":"<<__LINE__<<": check failed: "<<#cond<<"\n"; std::exit(1); } } while(0)
static std::string slurp(const char* path) {
  std::ifstream file(path); CHECK(file.good());
  return std::string((std::istreambuf_iterator<char>(file)),std::istreambuf_iterator<char>());
}
int main(int argc,char** argv) {
  CHECK(argc==3||argc==4);
  const bool no_vortex=argc==4&&std::string(argv[3])=="no-vortex";
  const std::string options=slurp(argv[1]), missing=slurp(argv[2]);
  for(const std::string format:{"arrow","parquet","vortex"}) {
    std::shared_ptr<arrow::RecordBatchReader> reader;
    {
      raincloud::Dataset ds("tiny",format,options);
      CHECK(ds.metadata().find("reader-fixture")!=std::string::npos);
      if(no_vortex&&format=="vortex") {
        try { (void)raincloud::batches(ds,2); CHECK(false); }
        catch(const raincloud::Error& e) { CHECK(e.code==RAINCLOUD_FORMAT_UNAVAILABLE); }
        continue;
      }
      auto result=raincloud::batches(ds,2);
      if(!result.ok()) { std::cerr<<result.status().ToString()<<"\n"; return 1; }
      reader=*result;
    }
    int64_t rows=0;
    while(true) {
      auto result=reader->Next();
      if(!result.ok()) { std::cerr<<result.status().ToString()<<"\n"; return 1; }
      auto batch=*result; if(!batch) break;
      CHECK(batch->num_rows()<=2); rows+=batch->num_rows();
    }
    CHECK(rows==8);
  }
  // Resolution failures keep their Raincloud code through the Arrow helper.
  raincloud::Dataset offline("tiny","arrow",missing);
  try { (void)raincloud::batches(offline,2); CHECK(false); }
  catch(const raincloud::Error& e) { CHECK(e.code==RAINCLOUD_OFFLINE_MISS); }
  std::cout<<(no_vortex ? "C++: IPC and Parquet readers, Vortex refused, typed offline miss\n"
                        : "C++: Arrow RecordBatchReader across three formats, typed offline miss\n");
}
