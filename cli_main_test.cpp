/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// End-to-end tests for the `tinylamb` CLI executable (main.cpp): a SQL script
// arrives on standard input, runs as one implicit transaction, and buffered
// rows print only after the durability barrier.

#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include "common/platform/process.hpp"
#include "gtest/gtest.h"

namespace {

class CliMainTest : public ::testing::Test {
 public:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("cli_main_test-" + std::to_string(::getpid())) /
           ::testing::UnitTest::GetInstance()->current_test_info()->name();
    // T2805: a failed SetUp used to surface as a mysterious "CLI errored"
    // three steps later; state the environment failure at its source.
    std::error_code ec;
    std::filesystem::create_directories(dir_, ec);
    ASSERT_FALSE(ec) << "SetUp: create_directories(" << dir_
                     << ") failed: " << ec.message();
  }

  void TearDown() override {
    // T2805: leaked directories poison the next run's paths; report the
    // removal failure (a destructor may not throw, so EXPECT not ASSERT).
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
    EXPECT_FALSE(ec) << "TearDown: remove_all(" << dir_
                     << ") failed: " << ec.message();
  }

 protected:
  // Runs the sibling `tinylamb` executable with `sql` on stdin. Returns the
  // process exit status and fills stdout/stderr.
  int RunCli(std::string_view sql, std::string* out, std::string* err) {
    return RunCliRaw(sql, "'" + (dir_ / "db").string() + "'", dir_ / "stdout",
                     out, err);
  }

  // T2804: variant with the full argument string (flags, arity mistakes)
  // and the stdout redirect target under test control, so argument
  // handling, statement splitting, and write-failure paths get pinned
  // instead of only the happy path.
  int RunCliRaw(std::string_view sql, const std::string& args,
                const std::filesystem::path& stdout_path, std::string* out,
                std::string* err) {
    const std::filesystem::path exe = BinaryPath("tinylamb");
    if (!std::filesystem::exists(exe)) {
      *err = "tinylamb executable not found next to the test binary";
      return -1;
    }
    const std::filesystem::path input = dir_ / "input.sql";
    const std::filesystem::path stderr_path = dir_ / "stderr";
    {
      std::ofstream in(input, std::ios::binary);
      EXPECT_TRUE(in.is_open()) << "cannot create " << input;
      in << sql;
      EXPECT_TRUE(in.good()) << "cannot write " << input;
    }
    const auto shell_quote = [](const std::string& s) {
      std::string out = "'";
      out.reserve(s.size() * 4 + 2);
      for (char c : s) {
        if (c == '\'') {
          out += "'\\''";
        } else {
          out += c;
        }
      }
      out += "'";
      return out;
    };
    const std::string command =
        "/bin/sh -c " +
        shell_quote(exe.string() + " " + args + " < " + input.string() + " > " +
                    stdout_path.string() + " 2> " + stderr_path.string());
    // NOLINTNEXTLINE(cert-env33-c)
    const int status = std::system(command.c_str());
    // /dev/full READS back as an endless NUL stream (never EOF), so only
    // capture stdout when it was redirected to a regular file in dir_.
    if (stdout_path.parent_path() == dir_) {
      ReadFile(stdout_path, out);
    } else {
      out->clear();
    }
    ReadFile(stderr_path, err);
    if (WIFEXITED(status)) {
      return WEXITSTATUS(status);
    }
    return -1;
  }

 protected:  // NOLINT(readability-redundant-access-specifiers)
  std::filesystem::path dir_;

 private:
  static std::filesystem::path BinaryPath(std::string_view name) {
    const std::filesystem::path self =
        tinylamb::platform::CurrentExecutablePath();
    if (self.empty()) {
      return {name};
    }
    return self.parent_path() / name;
  }

  static void ReadFile(const std::filesystem::path& path, std::string* out) {
    std::ifstream in(path, std::ios::binary);
    out->assign(std::istreambuf_iterator<char>(in),
                std::istreambuf_iterator<char>());
  }

};

TEST_F(CliMainTest, ScriptExecutesAndPrintsRowsAfterCommit) {
  std::string out;
  std::string err;
  const int code = RunCli(
      "CREATE TABLE t (k INT64, v VARCHAR);"
      "INSERT INTO t VALUES (1, 'one'), (2, 'two');"
      "SELECT k FROM t ORDER BY k;",
      &out, &err);
  EXPECT_EQ(code, 0) << err;
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

// T2804: argument-handling coverage beyond the happy path (T2796/T2799
// behavior pins for the CLI).

TEST_F(CliMainTest, HelpFlagPrintsUsageAndExitsZero) {
  std::string out;
  std::string err;
  const int code =
      RunCliRaw("SELECT 1;", "--help", dir_ / "stdout", &out, &err);
  EXPECT_EQ(code, 0) << err;
  EXPECT_NE(out.find("usage:"), std::string::npos) << out;
  // --help must not touch the database: no positional was given at all.
  EXPECT_FALSE(std::filesystem::exists(dir_ / "db"));
}

TEST_F(CliMainTest, UnknownOptionRejectedAndNoDatabaseCreated) {
  std::string out;
  std::string err;
  const std::string db = (dir_ / "db").string();
  const int code = RunCliRaw("SELECT 1;", "--forse '" + db + "'",
                             dir_ / "stdout", &out, &err);
  EXPECT_EQ(code, 2);
  EXPECT_NE(err.find("unknown option: --forse"), std::string::npos) << err;
  // The old parser handed "--forse" to Database::Create as the file name;
  // a typo must leave no debris behind.
  EXPECT_FALSE(std::filesystem::exists(db));
  EXPECT_FALSE(std::filesystem::exists(db + ".db"));
  EXPECT_FALSE(std::filesystem::exists(dir_ / "--forse.db"));
}

TEST_F(CliMainTest, TwoPositionalArgsRejected) {
  std::string out;
  std::string err;
  const int code =
      RunCliRaw("SELECT 1;", "'" + (dir_ / "db").string() + "' extra",
                dir_ / "stdout", &out, &err);
  EXPECT_EQ(code, 2);
  EXPECT_NE(err.find("usage:"), std::string::npos) << err;
}

TEST_F(CliMainTest, ForceFlagAcceptedAndScriptRuns) {
  std::string out;
  std::string err;
  const int code = RunCliRaw(
      "CREATE TABLE f (k INT64); INSERT INTO f VALUES (1);",
      "--force '" + (dir_ / "db").string() + "'", dir_ / "stdout", &out, &err);
  EXPECT_EQ(code, 0) << err;
  // Database::Create appends the canonical suffixes: <arg>.db + WAL.
  EXPECT_TRUE(std::filesystem::exists(dir_ / "db.db")) << err;
}

TEST_F(CliMainTest, QuotedSemicolonsStayOneStatement) {
  // A split that ignored quotes would execute `INSERT INTO q VALUES (42` as
  // its own (failing) statement and the whole script would abort; exit 0
  // plus the intact literal proves the splitter walked the quoted ';' and,
  // for "it's", the escaped quote.
  std::string out;
  std::string err;
  const int code = RunCliRaw(
      "CREATE TABLE q (v VARCHAR);"
      "INSERT INTO q VALUES ('a;b');"
      "SELECT v FROM q;",
      "'" + (dir_ / "db").string() + "'", dir_ / "stdout", &out, &err);
  EXPECT_EQ(code, 0) << err;
  EXPECT_NE(out.find("\"a;b\""), std::string::npos) << out;
}

TEST_F(CliMainTest, StdoutWriteFailureExitsNonZero) {
  if (::access("/dev/full", W_OK) != 0) {
    GTEST_SKIP() << "/dev/full unavailable";
  }
  std::string out;
  std::string err;
  // Same script the happy-path test runs, stdout aimed at /dev/full: the
  // commit succeeds but every buffered row write fails (T2794).
  const int code = RunCliRaw(
      "CREATE TABLE t (k INT64);"
      "INSERT INTO t VALUES (1), (2), (3);"
      "SELECT k FROM t ORDER BY k;",
      "'" + (dir_ / "db").string() + "'", "/dev/full", &out, &err);
  EXPECT_EQ(code, 1) << "stdout: " << out << " stderr: " << err;
  EXPECT_NE(err.find("failed to write result output"), std::string::npos)
      << err;
}

}  // namespace
