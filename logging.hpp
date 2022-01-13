#ifndef TINYLAMB_LOGGER_HPP
#define TINYLAMB_LOGGER_HPP

// this file is junk.

#include <fstream>
#include <iostream>
#include <map>

class Logging {
 public:
  Logging(std::string_view filename)
      : file_name_(filename),
        dst_(file_name_, std::ios_base::in | std::ios_base::out |
                             std::ios_base::ate | std::ios_base::binary) {
    if (dst_.fail()) {
      dst_.open(file_name_, std::ios_base::in | std::ios_base::out |
                                std::ios_base::ate | std::ios_base::binary |
                                std::ios_base::trunc);
      if (dst_.fail()) {
        throw std::runtime_error("failed to open file: " + file_name_);
      }
    }
  }

  bool Append(uint64_t lsn, std::string_view payload) {
    int64_t offset = dst_.tellp();
    lsn_map[lsn] = offset;
    dst_.write(reinterpret_cast<const char*>(&lsn), sizeof(lsn));
    uint32_t payload_size = payload.size();
    dst_.write(reinterpret_cast<const char*>(&payload_size),
               sizeof(payload_size));
    dst_.write(payload.data(), static_cast<int>(payload.length()));
    uint32_t length = sizeof(lsn) + sizeof(payload_size) + payload.length();
    dst_.write(reinterpret_cast<const char*>(&length), sizeof(length));
    return !dst_.bad();
  }

  bool Read(uint64_t lsn, std::string* payload) {
    auto offset = lsn_map.lower_bound(lsn);
    if (offset != lsn_map.end()) {
      dst_.seekg(offset->second, std::ios_base::beg);
    } else {
      int32_t length;
      dst_.seekg(-static_cast<int>(sizeof(length)), std::ios_base::end);
      dst_.read(reinterpret_cast<char*>(&length), sizeof(length));
      dst_.seekg(-(length + static_cast<int>(sizeof(length))),
                 std::ios_base::cur);
    }

    while (dst_.good()) {
      uint64_t current_lsn;
      int64_t current_pos = dst_.tellg();
      dst_.read(reinterpret_cast<char*>(&current_lsn), sizeof(current_lsn));
      lsn_map[current_lsn] = current_pos;
      if (current_lsn == lsn) {
        uint32_t content_length;
        dst_.read(reinterpret_cast<char*>(&content_length),
                  sizeof(content_length));
        payload->resize(content_length);
        dst_.read(payload->data(), content_length);
        return true;
      } else if (current_lsn < lsn) {
        return false;
      }
      int32_t length;
      dst_.seekg(current_pos - static_cast<int>(sizeof(length)));
      dst_.read(reinterpret_cast<char*>(&length), sizeof(length));
      dst_.seekg(-(length + static_cast<int>(sizeof(length))),
                 std::ios_base::cur);
    }
    return false;
  }

 private:
  std::string file_name_;
  std::fstream dst_;
  std::map<uint64_t, int64_t> lsn_map;
  uint64_t last_lsn;
};

#endif  // TINYLAMB_LOGGER_HPP
