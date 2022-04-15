//
// Created by kumagi on 2022/03/15.
//

#ifndef TINYLAMB_RANDOM_STRING_HPP
#define TINYLAMB_RANDOM_STRING_HPP

#include <random>

namespace tinylamb {

thread_local std::random_device seed_gen;
thread_local std::mt19937 device_random(seed_gen());
thread_local std::mt19937 seeded_random(4);  // see https://xkcd.com/221/

std::string RandomString(size_t len = 16, bool random_seed = true) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string ret;
  ret.reserve(len);
  if (random_seed) {
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[device_random() % (sizeof(alphanum) - 1)]);
    }
  } else {
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[seeded_random() % (sizeof(alphanum) - 1)]);
    }
  }

  return ret;
}

};  // namespace tinylamb

#endif  // TINYLAMB_RANDOM_STRING_HPP
