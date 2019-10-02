//
// Created by kumagi on 2019/09/04.
//

#ifndef PEDASOS_PAGE_HPP
#define PEDASOS_PAGE_HPP

#include <cstddef>
#include <cstdint>
#include "Row.hpp"

namespace pedasos {

class Page {
public:
  static constexpr size_t kPageSize = 1024 * 32;
  struct Header {
    size_t last_lsn;
    uint16_t version;    // Schema Version.
    uint16_t flags;
    uint16_t row_begin;  // Offset to beginning of rows.
    uint16_t row_end;    // Offset to end of rows.
    uint16_t rows;
    uint16_t free_size;
    uint16_t row_pos[1];  // variable length struct.

    friend std::ostream& operator<<(std::ostream& o, const Header& h) {
      o << "PageHeader: {\n"
        << "  last_lsn: " << h.last_lsn << ",\n"
        << "  version: " << h.version << ",\n"
        << "  flags: " << h.flags << ",\n"
        << "  row_begin: " << h.row_begin << ",\n"
        << "  row_end: " << h.row_end << ",\n"
        << "  rows: " << h.rows << ",\n"
        << "  free_size: " << h.free_size << ",\n"
        << "}\n";
      return o;
    }
  };
  static constexpr size_t kHeaderSize = sizeof(Header) - sizeof(uint16_t);
  Header& GetHeader() {
    return *reinterpret_cast<Header*>(buffer_);
  }
  [[nodiscard]] const Header& GetHeader() const {
    return *reinterpret_cast<const Header*>(buffer_);
  }
  bool GetRow(size_t offset, const Row** row) const {
    const Header& header = GetHeader();
    if (header.rows < offset) {
      return false;
    }
    *row = reinterpret_cast<const Row*>(&buffer_[header.row_pos[offset]]);
    return true;
  }

  [[nodiscard]] uint16_t* BeginOfRowIndex() {
    Header& header = GetHeader();
    return &header.row_pos[0];
  }
  [[nodiscard]] const uint16_t* BeginOfRowIndex() const {
    const Header& header = GetHeader();
    return &header.row_pos[0];
  }
  [[nodiscard]] uint16_t* BeginOfFreeSpace() {
    const Header &header = GetHeader();
    return &BeginOfRowIndex()[header.rows];
  }
  [[nodiscard]] const uint16_t* BeginOfFreeSpace() const {
    const Header& header = GetHeader();
    return &BeginOfRowIndex()[header.rows];
  }

public:
  Page(FILE* src, size_t pid) {
    if (src == nullptr) {
      throw std::runtime_error("FILE is null");
    }
    int fd = fileno(src);
    size_t nread = ::pread(fd, buffer_, kPageSize, pid * kPageSize);
    if (nread == 0) {
      InitPage();
    }
  }

  bool Read(size_t offset, const Row* row) const {
    return GetRow(offset, &row);
  }

  bool Insert(const Row& new_row) {
    Header& header = GetHeader();
    size_t row_size = new_row.RowSize();
    uint16_t* new_idpos = BeginOfFreeSpace();
    if (reinterpret_cast<uint8_t*>(new_idpos + 1) <
        &buffer_[header.row_end - row_size]) {
      *new_idpos = header.row_end - row_size;
      header.row_begin += sizeof(uint16_t);
      header.row_end -= row_size;
      Row& target = *reinterpret_cast<Row*>(&buffer_[header.row_end]);
      target.tid = new_row.tid;
      target.bytes = new_row.bytes;
      memcpy(target.payload, new_row.payload, new_row.bytes);
      header.rows++;
      // copy the row.
    } else {
      // No enough space.
      return false;
    }
  }
  bool Delete(size_t offset) {
    uint16_t* idx = BeginOfRowIndex() + offset;
    *idx = 0;
  }

  bool Flush(FILE* fp, size_t pid) const {
    size_t nwrite = ::pwrite(fileno(fp),
        buffer_,
        kPageSize,
        pid * kPageSize);
    std::cout << "wrote: " << nwrite << "\n";
    return nwrite == kPageSize;
  }

  friend std::ostream& operator<<(std::ostream& o, const Page& p) {
    const Header& h = p.GetHeader();
    o << "Page: (" << h.rows << ") {\n";
    o << h;
    Row* r = nullptr;
    for (size_t i = 0; i < h.rows; ++i) {
      o << p.Read(i, r) << "\n";
      assert(r != nullptr);
      o << *r << "\n";
    }
    o << "}";
  }
private:
  void InitPage() {
    Header& header = GetHeader();
    header.last_lsn = 0;
    header.version = 0;
    header.flags = 0;
    header.row_begin = kHeaderSize;
    header.row_end = kPageSize;
    header.rows = 0;
    header.free_size = kPageSize - kHeaderSize;
    uint16_t row_pos[1];  // variable length struct.
    memset(&header.row_pos[0], 0, header.free_size);
  }
private:
  uint8_t buffer_[kPageSize];
};

}  // namespace pedasos
#endif // PEDASOS_PAGE_HPP
