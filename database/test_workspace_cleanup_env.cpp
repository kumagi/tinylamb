/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Compiled into every gtest binary (see add_simple_test in CMakeLists.txt).
// Tests open throwaway databases through relative paths, so the engine
// creates <name>.db / <name>.log / <name>.last_checkpoint in the CURRENT
// WORKING DIRECTORY.  When a test skips its DeleteAll() cleanup, those
// files pile up next to the binary (hundreds per ctest sweep, and manual
// `./build/foo_test` runs pollute the project root).  Database::Create()
// registers every opened base name; this environment unlinks exactly the
// files THIS process created when the binary exits -- a directory diff
// would race parallel ctest jobs sharing one working directory.
// Files that existed before the run are never touched, and
// TINYLAMB_KEEP_TEST_DATABASES=1 disables the sweep for debugging.

#include <cstdlib>
#include <memory>

#include "database/database.hpp"
#include "gtest/gtest.h"

namespace {

class TestDatabaseCleanup : public ::testing::Environment {
 public:
  void SetUp() override {
    const char* const keep = std::getenv("TINYLAMB_KEEP_TEST_DATABASES");
    keep_artifacts_ = keep != nullptr && keep[0] != '\0' && keep[0] != '0';
  }

  void TearDown() override {
    if (!keep_artifacts_) {
      tinylamb::Database::RemoveCreatedDatabaseFiles();
    }
  }

 private:
  bool keep_artifacts_{false};
};

// AddGlobalTestEnvironment must run before RUN_ALL_TESTS; a static
// initializer in a TU compiled into every test binary guarantees that.
// gtest takes ownership of the released pointer; the catch-all keeps the
// dynamic initialization from escaping (cert-err58-cpp).
// NOLINTNEXTLINE(cert-err58-cpp): the lambda body is fully wrapped in
// try/catch, so the initializer cannot escape an exception.
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
