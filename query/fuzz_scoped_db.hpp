/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_FUZZ_SCOPED_DB_HPP
#define TINYLAMB_FUZZ_SCOPED_DB_HPP

#include <memory>
#include <string>
#include <utility>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"

namespace tinylamb {

// Throwaway database for one fuzz iteration: deletes the .db/.log/master
// files when the scope exits.  Without this every iteration strands a fresh
// file pair next to the fuzzer's working directory; long sweeps eventually
// exhaust the disk (observed: "Disk quota exceeded" turning Database::Create
// into a LOG(FATAL) abort, which libFuzzer reports as a bogus crash).
class ScopedDb {
 public:
  explicit ScopedDb(std::string_view prefix)
      : name_(std::string(prefix) + "-" + RandomString(8)) {
    StatusOr<std::unique_ptr<Database>> created = Database::Create(name_);
    if (created.HasValue()) {
      db_ = created.MoveValue();
    }
  }

  ScopedDb(const ScopedDb&) = delete;
  ScopedDb& operator=(const ScopedDb&) = delete;
  ScopedDb(ScopedDb&&) = delete;
  ScopedDb& operator=(ScopedDb&&) = delete;

  ~ScopedDb() {
    if (db_ != nullptr) {
      db_->DeleteAll();
    }
    db_.reset();
  }

  [[nodiscard]] Database* operator->() const { return db_.get(); }
  [[nodiscard]] Database& operator*() const { return *db_; }
  [[nodiscard]] Database* get() const { return db_.get(); }
  [[nodiscard]] explicit operator bool() const { return db_ != nullptr; }

 private:
  std::string name_;
  std::unique_ptr<Database> db_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FUZZ_SCOPED_DB_HPP
