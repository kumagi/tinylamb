//
// Created by kumagi on 22/08/07.
//

#ifndef TINYLAMB_FOSTER_PAIR_HPP
#define TINYLAMB_FOSTER_PAIR_HPP

#include <string>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"

namespace tinylamb {

struct FosterPair {
  FosterPair() = default;
  FosterPair(std::string_view k, page_id_t pid) : key(k), child_pid(pid) {}
  friend Encoder& operator<<(Encoder& e, const FosterPair& fp) {
    e << fp.key << fp.child_pid;
    return e;
  }
  friend Decoder& operator>>(Decoder& d, FosterPair& fp) {
    d >> fp.key >> fp.child_pid;
    return d;
  }
  std::string key;
  page_id_t child_pid;
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::FosterPair> {
 public:
  uint64_t operator()(const tinylamb::FosterPair& f) const {
    uint64_t ret = 0xf051e6;
    ret += std::hash<std::string>()(f.key);
    ret += std::hash<tinylamb::page_id_t>()(f.child_pid);
    return ret;
  }
};

}  // namespace std

#endif  // TINYLAMB_FOSTER_PAIR_HPP