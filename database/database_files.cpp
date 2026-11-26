/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "database/database_files.hpp"

#include <algorithm>
#include <cstdio>
#include <mutex>
#include <tuple>
#include <unordered_set>

#include "common/log_message.hpp"
#include "database/database.hpp"

namespace tinylamb {
namespace {

// Base names of every database this process opened through Create().
// Guards RemoveCreatedDatabaseFiles(); see the header comment.
std::mutex created_databases_mutex;
std::unordered_set<std::string> created_databases;

}  // namespace

namespace database_files {

void RegisterCreated(std::string_view dbname) {
  const std::scoped_lock lock(created_databases_mutex);
  created_databases.emplace(dbname);
}

// Removes the WAL segment series `<wal_base>.<index>` for one database,
// Recycling may have already removed segments at the start of the series.
void RemoveWalSegmentFiles(const std::filesystem::path& wal_base) {
  const std::filesystem::path directory = wal_base.has_parent_path()
                                              ? wal_base.parent_path()
                                              : std::filesystem::path(".");
  const std::string prefix = wal_base.filename().string() + ".";
  std::error_code ec;
  std::filesystem::directory_iterator it(directory, ec);
  if (ec) {
    return;
  }
  for (const std::filesystem::directory_entry& entry : it) {
    const std::string filename = entry.path().filename().string();
    if (!filename.starts_with(prefix)) {
      continue;
    }
    const std::string suffix = filename.substr(prefix.size());
    if (!suffix.empty() && std::ranges::all_of(suffix, [](char c) {
          return c >= '0' && c <= '9';
        })) {
      std::filesystem::remove(entry.path(), ec);
    }
  }
}

void RemoveCompanionStorageFiles(const std::string& db_file_path) {
  std::ignore = std::remove((db_file_path + ".blocks").c_str());
  std::ignore = std::remove((db_file_path + ".lsm").c_str());
  const std::filesystem::path p(db_file_path);
  const std::filesystem::path parent =
      p.parent_path().empty() ? std::filesystem::path(".") : p.parent_path();
  const std::string base_blocks = p.filename().string() + ".blocks.";
  const std::string base_lsm = p.filename().string() + ".lsm.";
  const std::string base_tlcs = p.filename().string() + ".tlcs";
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(parent, ec)) {
    if (ec) {
      break;
    }
    const std::string fname = entry.path().filename().string();
    if (fname.starts_with(base_blocks) || fname.starts_with(base_lsm) ||
        fname.starts_with(base_tlcs)) {
      std::filesystem::remove(entry.path(), ec);
    }
  }
  // A constructor or increment failure leaves ec set with the loop already
  // at end; without this check a partial enumeration looks like a complete
  // one and companion files silently survive cleanup.
  if (ec) {
    LOG(WARN) << "RemoveCompanionStorageFiles: directory iteration over "
              << parent << " failed: " << ec.message();
  }
}
}  // namespace database_files

void Database::RemoveCreatedDatabaseFiles() { RemoveCreatedDatabaseFiles({}); }

void Database::RemoveCreatedDatabaseFiles(
    const std::unordered_set<std::string>& preserve_bases) {
  std::unordered_set<std::string> names;
  {
    const std::scoped_lock lock(created_databases_mutex);
    names = created_databases;
  }
  for (const std::string& name : names) {
    if (preserve_bases.contains(name)) {
      continue;
    }
    // Same file set as DeleteAll(): the page file, companion block/tlcs
    // files, the WAL segment series, the checkpoint marker and its stranded
    // temp sibling. Missing entries are the norm and simply ignored.
    std::ignore = std::remove((name + ".db").c_str());
    // The snapshot temp is <db>.tmp (page_manager.cpp); the old .db.ckpt
    // names are kept only to clean up files from retired versions.
    std::ignore = std::remove((name + ".tmp").c_str());
    std::ignore = std::remove((name + ".db.ckpt").c_str());
    std::ignore = std::remove((name + ".db.ckpt.tmp").c_str());
    database_files::RemoveCompanionStorageFiles(name + ".db");
    database_files::RemoveCompanionStorageFiles(name);
    std::ignore = std::remove((name + ".log").c_str());
    std::ignore = std::remove((name + ".log.meta").c_str());
    std::ignore = std::remove((name + ".log.meta.tmp").c_str());
    database_files::RemoveWalSegmentFiles(name + ".log");
    std::ignore = std::remove((name + ".last_checkpoint").c_str());
    std::ignore = std::remove((name + ".last_checkpoint.tmp").c_str());
  }
}

}  // namespace tinylamb
