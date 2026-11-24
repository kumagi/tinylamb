/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Compiled into every gtest binary (see add_simple_test in CMakeLists.txt).
// Tests open throwaway databases through relative paths, so the engine
// creates <name>.db / <name>.log plus WAL segment and companion files in the
// CURRENT WORKING DIRECTORY.  When a test skips its DeleteAll() cleanup, those
// files pile up next to the binary (hundreds per ctest sweep, and manual
// `./build/foo_test` runs pollute the project root).  Database::Create()
// registers every opened base name; this environment unlinks exactly the
// files THIS process created when the binary exits -- a directory diff
// would race parallel ctest jobs sharing one working directory.
// Files that existed before the run are never touched, and
// TINYLAMB_KEEP_TEST_DATABASES=1 disables the sweep for debugging.

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>

#include "common/platform/process.hpp"
#include "database/database.hpp"
#include "gtest/gtest.h"

namespace {

class TestDatabaseCleanup : public ::testing::Environment {
 public:
  void SetUp() override {
    // Record existing database bases before tests run. Database::Create also
    // sees reopen operations, so its process-local registration alone cannot
    // distinguish a test-created path from a user's pre-existing database.
    std::error_code ec;
    std::filesystem::directory_iterator it(".", ec);
    const std::filesystem::directory_iterator end;
    if (ec) {
      snapshot_complete_ = false;
    } else {
      for (; it != end; it.increment(ec)) {
        if (ec) {
          snapshot_complete_ = false;
          break;
        }
        const std::string filename = it->path().filename().string();
        constexpr std::string_view kDatabaseSuffix = ".db";
        if (filename.size() > kDatabaseSuffix.size() &&
            filename.ends_with(kDatabaseSuffix)) {
          preexisting_database_bases_.insert(
              filename.substr(0, filename.size() - kDatabaseSuffix.size()));
        }
      }
      if (ec) {
        snapshot_complete_ = false;
      }
    }
    // NOLINTNEXTLINE(concurrency-mt-unsafe): gtest environment setup runs
    // before test worker threads are started.
    const char* keep = ::tinylamb::platform::GetEnv("TINYLAMB_KEEP_TEST_DATABASES");
    const std::string_view value = keep == nullptr ? "" : keep;
    keep_artifacts_ = value == "1" || value == "true" || value == "yes" ||
                      value == "on";
  }

  // gtest's Environment::TearDown is a non-const virtual, so the const
  // transformation the checker suggests would silently drop the override.
  void TearDown() override {  // NOLINT(readability-make-member-function-const)
    if (!keep_artifacts_ && snapshot_complete_) {
      tinylamb::Database::RemoveCreatedDatabaseFiles(
          preexisting_database_bases_);
    }
  }

 private:
  bool keep_artifacts_{false};
  bool snapshot_complete_{true};
  std::unordered_set<std::string> preexisting_database_bases_;
};

// AddGlobalTestEnvironment must run before RUN_ALL_TESTS; a static
// initializer in a TU compiled into every test binary guarantees that.
// gtest takes ownership of the released pointer; the catch-all keeps the
// dynamic initialization from escaping (cert-err58-cpp): the lambda body is
// fully wrapped in try/catch, so no exception can leave the initializer.
// NOLINTNEXTLINE(cert-err58-cpp)
const bool kCleanupEnvironmentRegistered = [] {
  try {
    std::unique_ptr<::testing::Environment> env(new TestDatabaseCleanup);
    ::testing::AddGlobalTestEnvironment(env.release());
    return true;
  } catch (...) {
    return false;
  }
}();

}  // namespace
