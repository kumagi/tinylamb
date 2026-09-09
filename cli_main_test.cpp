/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// End-to-end tests for the `tinylamb` CLI executable (main.cpp): a SQL script
// arrives on standard input, runs as one implicit transaction, and buffered
// rows print only after the durability barrier.

#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include "gtest/gtest.h"

namespace {

class CliMainTest : public ::testing::Test {
 public:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("cli_main_test-" + std::to_string(::getpid())) /
           ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override { std::filesystem::remove_all(dir_); }

 protected:
  // Runs the sibling `tinylamb` executable with `sql` on stdin. Returns the
  // process exit status and fills stdout/stderr.
  int RunCli(std::string_view sql, std::string* out, std::string* err) {
    const std::filesystem::path exe = BinaryPath("tinylamb");
    if (!std::filesystem::exists(exe)) {
      *err = "tinylamb executable not found next to the test binary";
      return -1;
    }
    const std::filesystem::path input = dir_ / "input.sql";
    const std::filesystem::path stdout_path = dir_ / "stdout";
    const std::filesystem::path stderr_path = dir_ / "stderr";
    {
      std::ofstream in(input, std::ios::binary);
      in << sql;
    }
    const std::string command = "/bin/sh -c '" + exe.string() + " " +
                                (dir_ / "db").string() + " < " +
                                input.string() + " > " + stdout_path.string() +
                                " 2> " + stderr_path.string() + "'";
    const int status = std::system(command.c_str());
    ReadFile(stdout_path, out);
    ReadFile(stderr_path, err);
    if (WIFEXITED(status)) {
      return WEXITSTATUS(status);
    }
    return -1;
  }

 private:
  std::filesystem::path BinaryPath(std::string_view name) {
    char self[4096];
    const ssize_t len = ::readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (len <= 0) {
      return std::filesystem::path(name);
    }
    self[len] = '\0';
    return std::filesystem::path(self).parent_path() / name;
  }

  void ReadFile(const std::filesystem::path& path, std::string* out) {
    std::ifstream in(path, std::ios::binary);
    out->assign(std::istreambuf_iterator<char>(in),
                std::istreambuf_iterator<char>());
  }

  std::filesystem::path dir_;
};

TEST_F(CliMainTest, ScriptExecutesAndPrintsRowsAfterCommit) {
  std::string out;
  std::string err;
  const int code = RunCli(
      "CREATE TABLE t (k INT64, v VARCHAR);"
      "INSERT INTO t VALUES (1, 'one'), (2, 'two');"
      "SELECT k FROM t ORDER BY k;",
      &out, &err);
  EXPECT_EQ(code, 0) << true << err;
  // Row output uses Row's operator<< format; every buffered statement's
  // output (including command tags) prints only after the commit barrier.
  EXPECT_NE(out.find("[1]"), std::string::npos) << out;
  EXPECT_NE(out.find("[2]"), std::string::npos) << out;
  EXPECT_NE(out.find("Insert Rows"), std::string::npos) << out;
}

TEST_F(CliMainTest, FailedStatementAbortsTheWholeScript) {
  std::string out;
  std::string err;
  // The last statement fails (unknown column); nothing may be printed even
  // though the earlier SELECT produced a row.
  const int code = RunCli(
      "CREATE TABLE t (k INT64);"
      "INSERT INTO t VALUES (1);"
      "SELECT k FROM t;"
      "SELECT missing FROM t;",
      &out, &err);
  EXPECT_EQ(code, 1);
  EXPECT_TRUE(out.empty()) << out;
  EXPECT_FALSE(err.empty());
}

TEST_F(CliMainTest, EmptyStdinFailsWithUsage) {
  std::string out;
  std::string err;
  const int code = RunCli("", &out, &err);
  EXPECT_EQ(code, 2);
  EXPECT_TRUE(out.empty());
  EXPECT_NE(err.find("no SQL"), std::string::npos) << err;
}

}  // namespace
