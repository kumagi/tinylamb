/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/googlesql_compliance_file.hpp"

#include <gtest/gtest.h>

#include <cctype>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#ifdef TINYLAMB_GOOGLESQL_COMPLIANCE_RUN
#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/date.hpp"
#include "type/row.hpp"
#endif
#include "type/value.hpp"

namespace tinylamb {
namespace {

#ifndef TINYLAMB_GOOGLESQL_COMPLIANCE_DIR
#define TINYLAMB_GOOGLESQL_COMPLIANCE_DIR ""
#endif

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path);
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

#ifdef TINYLAMB_GOOGLESQL_COMPLIANCE_RUN
std::vector<Row> Drain(SqlEngine& engine, TransactionContext& context,
                       std::string_view sql, Status* status, std::string* error_msg = nullptr) {
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  if (!prepared.HasValue()) {
    *status = prepared.GetStatus();
    if (error_msg != nullptr) { *error_msg = engine.LastError(); }
    return {};
  }
  *status = Status::kSuccess;
  std::vector<Row> rows;
  Row row;
  try {
    while (prepared.Value()->Next(&row, nullptr)) { rows.push_back(row); }
  } catch (const std::exception& ex) {
    *status = Status::kUnknown;
    if (error_msg != nullptr) { *error_msg = ex.what(); }
    return {};
  }
  return rows;
}


bool RowsMatch(const std::vector<Row>& actual,
               const GoogleSqlComplianceCase& test_case, std::string* detail) {
  auto matches_expected = [&](const Row& row,
                              const std::vector<std::string>& expected) {
    if (row.values_.size() != expected.size()) { return false; }
    for (size_t i = 0; i < expected.size(); ++i) {
      if (!ComplianceValueMatches(row.values_[i], expected[i])) { return false; }
    }
    return true;
  };
  if (actual.size() != test_case.expected_rows.size()) {
    *detail = "row count actual=" + std::to_string(actual.size()) +
              " expected=" + std::to_string(test_case.expected_rows.size());
    return false;
  }
  if (test_case.unknown_order) {
    std::vector<bool> used(actual.size(), false);
    for (const auto& expected : test_case.expected_rows) {
      bool found = false;
      for (size_t i = 0; i < actual.size(); ++i) {
        if (used[i]) { continue; }
        if (matches_expected(actual[i], expected)) {
          used[i] = true;
          found = true;
          break;
        }
      }
      if (!found) {
        std::string exp_str = "{";
        for (size_t k = 0; k < expected.size(); ++k) {
          if (k != 0) { exp_str += ", "; }
          exp_str += expected[k];
        }
        exp_str += "}";
        std::string act_str = "[";
        for (size_t a = 0; a < actual.size(); ++a) {
          if (a != 0) { act_str += ", "; }
          act_str += "{";
          for (size_t b = 0; b < actual[a].values_.size(); ++b) {
            if (b != 0) { act_str += ", "; }
            act_str += FormatComplianceValue(actual[a].values_[b]);
          }
          act_str += "}";
        }
        act_str += "]";
        *detail = "missing expected row: " + exp_str + " actual rows: " + act_str;
        return false;
      }
    }
    return true;
  }
  for (size_t i = 0; i < actual.size(); ++i) {
    if (!matches_expected(actual[i], test_case.expected_rows[i])) {
      std::string row_str = "{";
      for (size_t j = 0; j < actual[i].values_.size(); ++j) {
        if (j != 0) { row_str += ", "; }
        row_str += FormatComplianceValue(actual[i].values_[j]);
      }
      row_str += "}";
      *detail = "row " + std::to_string(i) + " mismatch actual=" + row_str;
      return false;
    }
  }

  return true;
}
std::vector<std::string> SplitStatements(std::string_view sql) {
  std::vector<std::string> stmts;
  std::string current;
  bool in_string = false;
  char quote = '\0';
  for (size_t i = 0; i < sql.size(); ++i) {
    const char c = sql[i];
    if (in_string) {
      current.push_back(c);
      if (c == '\\' && i + 1 < sql.size()) {
        current.push_back(sql[++i]);
        continue;
      }
      if (c == quote) { in_string = false; }
      continue;
    }
    if (c == '"' || c == '\'') {
      in_string = true;
      quote = c;
      current.push_back(c);
      continue;
    }
    if (c == ';') {
      size_t b = 0;
      while (b < current.size() &&
             std::isspace(static_cast<unsigned char>(current[b])) != 0) {
        ++b;
      }
      size_t e = current.size();
      while (e > b &&
             std::isspace(static_cast<unsigned char>(current[e - 1])) != 0) {
        --e;
      }
      std::string trimmed = current.substr(b, e - b);
      if (!trimmed.empty()) { stmts.push_back(std::move(trimmed)); }
      current.clear();
      continue;
    }
    current.push_back(c);
  }
  size_t b = 0;
  while (b < current.size() &&
         std::isspace(static_cast<unsigned char>(current[b])) != 0) {
    ++b;
  }
  size_t e = current.size();
  while (e > b &&
         std::isspace(static_cast<unsigned char>(current[e - 1])) != 0) {
    --e;
  }
  std::string trimmed = current.substr(b, e - b);
  if (!trimmed.empty()) { stmts.push_back(std::move(trimmed)); }
  return stmts;
}

#endif  // TINYLAMB_GOOGLESQL_COMPLIANCE_RUN

}  // namespace

TEST(GoogleSqlComplianceFile, ParsesLiteralsAndErrors) {

  constexpr std::string_view kFile = R"test(
[name=one]
SELECT 1
--
ARRAY<STRUCT<INT64>>[{1}]
==
[name=not_null]
SELECT NOT null
--
ERROR: generic::invalid_argument: Operands of NOT cannot be literal NULL
==
[prepare_database]
CREATE TABLE t AS SELECT 1 AS a
--
ARRAY<STRUCT<a INT64>>[{1}]
==
[name=unordered]
SELECT a FROM t
--
ARRAY<STRUCT<INT64>>[unknown order:{2}, {1}]
==
)test";
  const std::vector<GoogleSqlComplianceCase> cases =
      ParseGoogleSqlComplianceFile("sample.test", kFile);
  ASSERT_EQ(cases.size(), 4U);
  EXPECT_EQ(cases[0].name, "one");
  EXPECT_EQ(cases[0].sql, "SELECT 1");
  ASSERT_EQ(cases[0].expected_rows.size(), 1U);
  ASSERT_EQ(cases[0].expected_rows[0].size(), 1U);
  EXPECT_EQ(cases[0].expected_rows[0][0], "1");
  EXPECT_TRUE(cases[1].expect_error);
  EXPECT_NE(cases[1].error_text.find("NOT"), std::string::npos);
  EXPECT_TRUE(cases[2].prepare_database);
  EXPECT_TRUE(cases[3].unknown_order);
  ASSERT_EQ(cases[3].expected_rows.size(), 2U);
}

TEST(GoogleSqlComplianceFile, MatchesIntBoolAndNull) {
  EXPECT_TRUE(ComplianceValueMatches(Value(int64_t{1}), "1"));
  EXPECT_TRUE(ComplianceValueMatches(Value(int64_t{1}), "true"));
  EXPECT_TRUE(ComplianceValueMatches(Value(int64_t{0}), "false"));
  EXPECT_TRUE(ComplianceValueMatches(Value(), "NULL"));
  EXPECT_TRUE(ComplianceValueMatches(Value(std::string("hi")), "\"hi\""));
  EXPECT_FALSE(ComplianceValueMatches(Value(int64_t{2}), "1"));
}

TEST(GoogleSqlComplianceFile, ParsesVendoredCorpus) {
  const std::string directory = TINYLAMB_GOOGLESQL_COMPLIANCE_DIR;
  if (directory.empty() || !std::filesystem::exists(directory)) {
    GTEST_SKIP() << "compliance testdata directory is not configured";
  }
  const std::vector<std::string> files =
      ListGoogleSqlComplianceFiles(directory);
  ASSERT_FALSE(files.empty());
  size_t case_count = 0;
  size_t error_count = 0;
  size_t prepare_count = 0;
  for (const std::string& name : files) {
    const std::filesystem::path path =
        std::filesystem::path(directory) / name;
    const std::vector<GoogleSqlComplianceCase> cases =
        ParseGoogleSqlComplianceFile(path.string(), ReadFile(path));
    if (name == "no_tests.test") {
      EXPECT_TRUE(cases.empty());
      continue;
    }
    EXPECT_FALSE(cases.empty()) << name;
    for (const GoogleSqlComplianceCase& test_case : cases) {

      ++case_count;
      if (test_case.expect_error) { ++error_count; }
      if (test_case.prepare_database) { ++prepare_count; }
      EXPECT_FALSE(test_case.sql.empty()) << name << " / " << test_case.name;
    }
  }
  EXPECT_GE(files.size(), 200U) << "vendored corpus looks truncated";
  EXPECT_GE(case_count, 1000U);
  EXPECT_GT(error_count, 0U);
  EXPECT_GT(prepare_count, 0U);
}

TEST(GoogleSqlComplianceFile, SkipsDifferentialPrivacy) {
  GoogleSqlComplianceCase test_case;
  test_case.sql = "SELECT * FROM t WITH DIFFERENTIAL_PRIVACY OPTIONS(epsilon=1)";
  EXPECT_TRUE(IsDifferentialPrivacyCase(test_case));
  test_case.sql = "SELECT 1";
  EXPECT_FALSE(IsDifferentialPrivacyCase(test_case));
}

#ifdef TINYLAMB_GOOGLESQL_COMPLIANCE_RUN
class GoogleSqlComplianceFileTest
    : public ::testing::TestWithParam<std::string> {};

TEST_P(GoogleSqlComplianceFileTest, RunsFile) {
  const std::string directory = TINYLAMB_GOOGLESQL_COMPLIANCE_DIR;
  if (directory.empty() || !std::filesystem::exists(directory)) {
    GTEST_SKIP() << "compliance testdata directory is not configured";
  }
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }

  const std::filesystem::path path =
      std::filesystem::path(directory) / GetParam();
  const std::vector<GoogleSqlComplianceCase> cases =
      ParseGoogleSqlComplianceFile(path.string(), ReadFile(path));

  const std::string path_prefix = "googlesql_compliance-" + RandomString(8);
  Database database(path_prefix);
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);

  for (const GoogleSqlComplianceCase& test_case : cases) {
    if (IsDifferentialPrivacyCase(test_case)) { continue; }
    SetDefaultTimeZone(test_case.default_time_zone.empty() ? "America/Los_Angeles" : test_case.default_time_zone);
    if (test_case.prepare_database) {
      const std::vector<std::string> stmts = SplitStatements(test_case.sql);
      for (const std::string& stmt : stmts) {
        Status s_status = Status::kSuccess;
        try {
          Drain(engine, context, stmt, &s_status);
        } catch (const std::exception& ex) {
          ADD_FAILURE() << GetParam() << " / " << test_case.name
                        << " prepare threw: " << ex.what() << "\n"
                        << stmt;
          break;
        }
        if (s_status != Status::kSuccess) {
          EXPECT_EQ(s_status, Status::kSuccess)
              << GetParam() << " / " << test_case.name
              << " prepare failed: " << engine.LastError() << "\n"
              << stmt;
          break;
        }
      }
      continue;
    }


    Status status = Status::kSuccess;
    std::string error_msg;
    std::vector<Row> rows;
    try {
      rows = Drain(engine, context, test_case.sql, &status, &error_msg);
    } catch (const std::exception& ex) {
      if (!test_case.expect_error) {
        ADD_FAILURE() << GetParam() << " / " << test_case.name
                      << " threw: " << ex.what() << "\n"
                      << test_case.sql;
      }
      continue;
    }

    if (test_case.expect_error) {
      EXPECT_NE(status, Status::kSuccess)
          << GetParam() << " / " << test_case.name
          << " expected error, engine said: " << error_msg << "\n"
          << test_case.sql;
    } else if (status != Status::kSuccess) {
      ADD_FAILURE() << GetParam() << " / " << test_case.name
                    << " execution failed: " << error_msg << "\n"
                    << test_case.sql;
    } else {
      std::string detail;
      EXPECT_TRUE(RowsMatch(rows, test_case, &detail))
          << GetParam() << " / " << test_case.name << " " << detail << "\n"
          << test_case.sql << "\n"
          << test_case.raw_result;
    }
  }
  database.DeleteAll();
}

std::vector<std::string> ComplianceFileNames() {
  const std::string directory = TINYLAMB_GOOGLESQL_COMPLIANCE_DIR;
  if (directory.empty() || !std::filesystem::exists(directory)) {
    return {"<missing>"};
  }
  std::vector<std::string> files = ListGoogleSqlComplianceFiles(directory);
  if (files.empty()) { return {"<empty>"}; }
  return files;
}

INSTANTIATE_TEST_SUITE_P(
    GoogleSqlCompliance, GoogleSqlComplianceFileTest,
    ::testing::ValuesIn(ComplianceFileNames()),
    [](const ::testing::TestParamInfo<std::string>& param) {
      std::string name = param.param;
      for (char& c : name) {
        if (std::isalnum(static_cast<unsigned char>(c)) == 0) { c = '_'; }
      }
      return name;
    });
#endif  // TINYLAMB_GOOGLESQL_COMPLIANCE_RUN

}  // namespace tinylamb
