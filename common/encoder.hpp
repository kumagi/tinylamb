//
// Created by kumagi on 2022/02/26.
//

#ifndef TINYLAMB_ENCODER_HPP
#define TINYLAMB_ENCODER_HPP

#include <cstdint>
#include <memory>
#include <sstream>
#include <vector>

#include "common/constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Encoder {
 public:
  explicit Encoder(std::ostream& os) : os_(&os) {}
  Encoder& operator<<(std::string_view sv);
  Encoder& operator<<(uint8_t u8);
  Encoder& operator<<(slot_t slot);
  Encoder& operator<<(int64_t i64);
  Encoder& operator<<(uint64_t u64);
  Encoder& operator<<(double d);
  Encoder& operator<<(ValueType v);
  Encoder& operator<<(bool v);

  template <typename T>
  Encoder& operator<<(const std::vector<T>& vec) {
    std::size_t size = vec.size();
    os_->write(reinterpret_cast<const char*>(&size), sizeof(size));
    for (const auto& elm : vec) {
      *this << elm;
    }
    return *this;
  }

  template <typename T, typename U>
  Encoder& operator<<(const std::pair<T, U>& p) {
    *this << p.first << p.second;
    return *this;
  }

 private:
  std::ostream* os_;
};

template <typename T>
std::string Encode(T src) {
  std::stringstream ss;
  Encoder enc(ss);
  enc << src;
  return ss.str();
}

}  // namespace tinylamb

#endif  // TINYLAMB_ENCODER_HPP
