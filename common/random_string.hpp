//
// Created by kumagi on 2022/03/15.
//

#ifndef TINYLAMB_RANDOM_STRING_HPP
#define TINYLAMB_RANDOM_STRING_HPP

#include <random>

namespace tinylamb {

thread_local std::random_device seed_gen;
thread_local std::mt19937 engine(seed_gen());

std::string RandomString(size_t len = 16) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string ret;
  ret.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    ret.push_back(alphanum[engine() % (sizeof(alphanum) - 1)]);
  }
  return ret;
}

};  // namespace tinylamb

#endif  // TINYLAMB_RANDOM_STRING_HPP
