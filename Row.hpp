//
// Created by kumagi on 2019/09/08.
//

#ifndef PEDASOS_ROW_HPP
#define PEDASOS_ROW_HPP

#include <ostream>
#include <cstring>

namespace tinylamb {

struct Column {
  Column() = delete;
  Column(const Column&) = delete;
  Column(Column&&) = delete;
  Column& operator=(const Column&) = delete;
  Column& operator=(Column&&) = delete;
  uint8_t size;
  uint8_t data[1];
};

struct Row {
  Row() = delete;
  Row(const Row&) = delete;
  Row(Row&&) = delete;
  Row& operator=(const Row& orig) {
    tid = orig.tid;
    bytes = orig.bytes;
    memcpy(payload, orig.payload, orig.bytes);
    return *this;
  }

  Row& operator=(Row&&) = delete;

  static size_t EmptyRowSize() {
    return sizeof(Row) - sizeof(payload);
  }
  [[nodiscard]] size_t RowSize() const {
    return EmptyRowSize() + bytes;
  }

  friend std::ostream& operator<<(std::ostream& o, const Row& r) {
    o << "[" << r.tid << "]: \"";
    for (size_t i = 0; i < r.bytes; ++i) {
      o << std::hex << r.payload[i] << std::dec;
    }
    o << "\"";
    return o;
  }

  uint64_t tid;   // Transaction ID.
  uint16_t bytes;  // Byte size of columns field.
  uint8_t payload[1];
};

}  // namespace tinylamb

#endif // PEDASOS_ROW_HPP
