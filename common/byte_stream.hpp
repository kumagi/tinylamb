/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

#ifndef TINYLAMB_BYTE_STREAM_HPP
#define TINYLAMB_BYTE_STREAM_HPP

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace tinylamb {

// Bounded byte reader over a libFuzzer input buffer.  Every read past the end
// returns zero so truncated inputs always execute safely; that makes prefix
// truncation a valid mutation (a shorter operation stream) and keeps single
// byte edits correlated with single parameter changes, which is what lets the
// coverage-guided mutator steer an operation sequence instead of falling back
// to blind random search.
class ByteStream {
 public:
  ByteStream(const uint8_t* data, size_t size) : data_(data), size_(size) {}

  [[nodiscard]] bool Remaining() const { return pos_ < size_; }
  [[nodiscard]] size_t Consumed() const { return pos_; }

  uint8_t U8() {
    if (pos_ >= size_) {
      return 0;
    }
    return data_[pos_++];
  }

  uint16_t U16() {
    const uint16_t lo = U8();
    const uint16_t hi = U8();
    return static_cast<uint16_t>((hi << 8) | lo);
  }

  uint32_t U32() {
    const uint32_t a = U8();
    const uint32_t b = U8();
    const uint32_t c = U8();
    const uint32_t d = U8();
    return a | (b << 8) | (c << 16) | (d << 24);
  }

  uint64_t U64() {
    const uint64_t lo = U32();
    const uint64_t hi = U32();
    return lo | (hi << 32);
  }

  // Unsigned little-endian base-128 varint.  Most values occupy a single byte
  // so operation parameters stay dense in the corpus and small one-byte
  // mutations map to single parameter changes.
  size_t Varint() {
    size_t result = 0;
    size_t shift = 0;
    for (int i = 0; i < 10; ++i) {
      const uint8_t b = U8();
      result |= static_cast<size_t>(b & 0x7f) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return result;
  }

  // Biased pick in [0, bound).  Small values are far more likely, which keeps
  // the operation mix close to the common case while still allowing edge
  // values to be reached.
  size_t Pick(size_t bound) {
    if (bound <= 1) {
      return 0;
    }
    return Varint() % bound;
  }

  // Up to max_len raw bytes (fewer when the input is truncated).
  std::string_view Bytes(size_t max_len) {
    const size_t available = size_ - pos_;
    const size_t take = available < max_len ? available : max_len;
    std::string_view result(
        reinterpret_cast<const char*>(data_ + pos_), take);
    pos_ += take;
    return result;
  }

 private:
  const uint8_t* data_;
  size_t size_;
  size_t pos_ = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BYTE_STREAM_HPP
