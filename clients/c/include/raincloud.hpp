// SPDX-License-Identifier: Apache-2.0
#ifndef RAINCLOUD_HPP
#define RAINCLOUD_HPP
#include "raincloud.h"
#include <memory>
#include <stdexcept>
#include <string>
namespace raincloud {
class Error : public std::runtime_error {
 public:
  const int code;
  Error(int code, const std::string& message) : std::runtime_error(message), code(code) {}
};
inline void check(int code, raincloud_error& error) {
  std::unique_ptr<raincloud_error, decltype(&raincloud_error_free)> cleanup(&error, raincloud_error_free);
  if (code) throw Error(code, error.message ? error.message : "Raincloud error");
}
class Dataset {
  std::unique_ptr<raincloud_dataset, decltype(&raincloud_close)> handle_{nullptr, raincloud_close};
 public:
  explicit Dataset(const std::string& slug, const std::string& format="auto", const std::string& options="{}") {
    raincloud_error error{}; raincloud_dataset* out=nullptr;
    int code=raincloud_open(options.c_str(), slug.c_str(), format.c_str(), &out, &error);
    handle_.reset(out); check(code,error);
  }
  Dataset(Dataset&&) noexcept=default;
  Dataset& operator=(Dataset&&) noexcept=default;
  raincloud_dataset* get() const noexcept { return handle_.get(); }
  std::string metadata() const { return string_call(raincloud_metadata); }
  std::string path() const { return string_call(raincloud_path); }
 private:
  std::string string_call(int32_t (*fn)(const raincloud_dataset*,char**,raincloud_error*)) const {
    raincloud_error error{}; char* out=nullptr;
    int code=fn(get(),&out,&error);
    std::unique_ptr<char, decltype(&raincloud_string_free)> owned(out,raincloud_string_free);
    check(code,error); return owned.get();
  }
};
}
#endif
