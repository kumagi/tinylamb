//
// Created by kumagi on 2019/09/04.
//

#ifndef PEDASOS_PAGEPOOL_HPP
#define PEDASOS_PAGEPOOL_HPP

#include <assert.h>
#include <list>
#include <unordered_map>
#include <utility>

#include "Page.hpp"

namespace pedasos {

struct Place {
  size_t tid;
  size_t pid;
  bool operator==(const Place& rhs) const {
    return tid == rhs.tid && pid == rhs.pid;
  }
  bool operator!=(const Place& rhs) const {
    return !(*this == rhs);
  };
};

struct PlaceHash {
  typedef std::size_t result_type;
  size_t operator()(const Place& key) const {
    return std::hash<size_t>()(key.tid) ^ std::hash<size_t>()(key.pid);
  }
};

class PagePool {
  static constexpr int kMetaTableId = 0;
  struct Entry {
    bool pinned_;
    Place pid_;
    Page page_;
  };
  typedef std::list<struct Entry> LruType;

  LruType::iterator& FindEntry(size_t tid, size_t pid) {
    auto page_entry = pool_.find(Place{tid, pid});
    if (page_entry != pool_.end()) {
      return page_entry->second;
    } else {
      assert(!"Non buffered page accessed.");
    }
  }

  Entry* Pin(size_t tid, size_t pid) {
    auto page_entry = pool_.find(Place{tid, pid});
    if (page_entry != pool_.end()) {
      page_entry->second->pinned_ = true;
      return &*page_entry->second;
    } else {
      assert(!"Not implemented.");
      return nullptr;
    }
  }

public:
  class ScopedUnpin {
  public:
    explicit ScopedUnpin(PagePool* pp, size_t tid, size_t pid)
        : target_(pp->Pin(tid, pid)) {}
    ScopedUnpin() : target_(nullptr) {}
    ScopedUnpin(const ScopedUnpin&) = delete;
    ScopedUnpin(ScopedUnpin&& o) noexcept : target_(o.target_)  {
      o.target_ = nullptr;
    }
    ScopedUnpin& operator=(const ScopedUnpin&) = delete;
    ScopedUnpin& operator=(ScopedUnpin&& o) noexcept {
      target_ = o.target_;
      o.target_ = nullptr;
      return *this;
    }
    ~ScopedUnpin() {
      if (target_)
        target_->pinned_ = false;
    }
    Entry* target_;
  };

  PagePool(std::string_view root_name, size_t capacity)
      : name_(root_name), capacity_(capacity) {}
  size_t Capacity() const {
    return capacity_;
  }
  bool Read(size_t tid, size_t pid, size_t offset, size_t* size,
            uint8_t** ptr) {
    LruType::iterator& entry = FindEntry(tid, pid);
    assert(entry->pinned_);
    return entry->page_.Read(offset, size, ptr);
  }
  bool OverWrite(size_t tid, size_t pid, size_t offset, size_t size,
                 const uint8_t* value) {
    LruType::iterator& entry = FindEntry(tid, pid);
    assert(entry->pinned_);
    return entry->page_.OverWrite(offset, size, value);
  }
  bool Insert(size_t tid, size_t pid, size_t size, const uint8_t* value) {
    LruType::iterator& entry = FindEntry(tid, pid);
    assert(entry->pinned_);
    return entry->page_.Insert(size, value);
  }
  bool Delete(size_t tid, size_t pid, size_t offset) {
    LruType::iterator& entry = FindEntry(tid, pid);
    assert(entry->pinned_);
    return entry->page_.Delete(offset);
  }
  bool CreateTable(size_t tid) {
    ScopedUnpin up(this, kMetaTableId, 0);
    Page* page = &up.target_->page_;
    uint8_t msg[] = "yarn";
    page->Insert(4, msg);
  }
  bool DeleteTable(size_t tid) {
    ScopedUnpin up(this, kMetaTableId, 0);
    Page* page = &up.target_->page_;
    page->Delete(4);
  }
private:
  std::unordered_map<size_t, FILE*> tid_file_map_;
  std::string name_;
  size_t capacity_;
  LruType lru_;
  std::unordered_map<Place, LruType::iterator, PlaceHash> pool_;
  friend class ScopedUnpin;
};

}  // namespace pedasos
#endif // PEDASOS_PAGEPOOL_HPP
