//
// Created by kumagi on 2022/02/26.
//

#ifndef TINYLAMB_DECODER_HPP
#define TINYLAMB_DECODER_HPP

#include <cstdint>
#include <memory>
#include <sstream>
#include <vector>

#include "common/constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Decoder {
 public:
  explicit Decoder(std::istream& is) : is_(&is) {}
  Decoder& operator>>(std::string& str);
  Decoder& operator>>(uint8_t& u8);
  Decoder& operator>>(slot_t& slot);
  Decoder& operator>>(int64_t& i64);
  Decoder& operator>>(uint64_t& u64);
  Decoder& operator>>(double& d);
  Decoder& operator>>(ValueType& v);
  Decoder& operator>>(bool& v);

  template <typename T>
  Decoder& operator>>(std::vector<T>& vec) {
    size_t size = vec.size();
    is_->read(reinterpret_cast<char*>(&size), sizeof(size));
    vec.clear();
    vec.resize(size);
    for (size_t i = 0; i < size; ++i) {
      *this >> vec[i];
    }
    return *this;
  }

  template <typename T, typename U>
  Decoder& operator>>(std::pair<T, U>& p) {
    *this >> p.first >> p.second;
    return *this;
  }

 private:
  std::istream* is_;
};

template <typename T>
T Decode(std::string_view src) {
  std::string buffer(src);
  std::stringstream ss(buffer);
  Decoder dec(ss);
  T ret;
  dec >> ret;
  return ret;
}

}  // namespace tinylamb

#endif  // TINYLAMB_DECODER_HPP
