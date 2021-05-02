#include "page.hpp"

#include <unistd.h>

#include <fstream>
#include <iostream>

namespace pedasus {

Page::Page(std::fstream& src, size_t page_id) {
  if (!src.is_open()) {
    throw std::runtime_error("cannot access file");
  }
  src.seekg(page_id * kPageSize, std::ios_base::beg);
  if (!src.fail()) {
    src.read(reinterpret_cast<char*>(this), kPageSize);
  }
  if (src.fail()) {
    src.clear();
    // If there is no data for the page, fill it.
    header_.page_id = page_id;
    header_.last_lsn = 0;
    header_.type = PageType::kUnknown;
    src.seekp(page_id * kPageSize, std::ios_base::beg);
    src.write(reinterpret_cast<const char*>(this), kPageSize);
    if (src.bad()) {
      throw std::runtime_error("cannot allocate a page: " + std::to_string(src.bad()));
    }
  }
  assert(page_id == header_.page_id);
}

void Page::WriteBack(std::fstream& src) {
  src.seekp(header_.page_id * kPageSize, std::ios_base::beg);
  if (src.fail()) {
    throw std::runtime_error("cannot seekp to page: " + std::to_string(header_.page_id));
  }
  src.write(reinterpret_cast<const char*>(this), kPageSize);
  if (src.fail()) {
    throw std::runtime_error("cannot write back page: " + std::to_string(src.bad()));
  }
}

}  // namespace pedasus