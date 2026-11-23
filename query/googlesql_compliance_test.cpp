/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/string_util.hpp"
#include "query/googlesql_compliance_file.hpp"
#include "type/column_name.hpp"

#ifdef TINYLAMB_GOOGLESQL_COMPLIANCE_RUN
#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "index/index_schema.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
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
                       std::string_view sql, Status* status,
                       std::string* error_msg = nullptr) {
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  if (!prepared.HasValue()) {
    *status = prepared.GetStatus();
    if (error_msg != nullptr) {
      *error_msg = engine.LastError().empty() ? ToString(prepared.GetStatus())
                                              : engine.LastError();
    }
    return {};
  }
  *status = Status::kSuccess;
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  // Executors surface query errors as a sticky status (Next returns false
  // at EOF and on failure); inspect it after the drain to tell the two
  // apart.
  if (const Status st = prepared.Value()->GetStatus(); st != Status::kSuccess) {
    // Preserve the real sticky code: the runner passes it to the golden-file
    // category check, and kUnknown would falsely satisfy the generic
    // unimplemented allowance regardless of the actual error category.
    *status = st;
    if (error_msg != nullptr) {
      *error_msg =
          engine.LastError().empty() ? ToString(st) : engine.LastError();
    }
    return {};
  }
  return rows;
}

Status CreateComplianceSecondaryIndex(Database& database,
                                      TransactionContext& context,
                                      std::string_view specification) {
  const size_t colon = specification.find(':');
  const size_t open = specification.find('(', colon + 1);
  const size_t close = specification.rfind(')');
  if (colon == std::string_view::npos || open == std::string_view::npos ||
      close == std::string_view::npos || colon == 0 || open <= colon + 1 ||
      close <= open + 1) {
    return Status::kUnknown;
  }
  const std::string index_name(specification.substr(0, colon));
  const std::string table_name(
      specification.substr(colon + 1, open - colon - 1));
  const std::string column_name(
      specification.substr(open + 1, close - open - 1));
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, table, context.GetTable(table_name));
  const int offset = table->GetSchema().Offset(ColumnName(column_name));
  if (offset < 0) {
    return Status::kNotExists;
  }
  return database.CreateIndex(
      context, table_name,
      IndexSchema(index_name, {static_cast<slot_t>(offset)}, {},
                  IndexMode::kNonUnique));
}

bool RowsMatch(const std::vector<Row>& actual,
               const GoogleSqlComplianceCase& test_case, std::string* detail) {
  // A single-field "NULL" golden may denote a whole-row NULL printed for a
  // struct/proto value whose fields the engine flattens into several row
  // slots. That collapse is only credible when the golden itself contains
  // multi-field rows (proving the result is multi-column); when every
  // expected row is single-field, a multi-column all-NULL actual row is an
  // output-shape bug and must not match.
  const bool golden_has_multi_field_rows = std::ranges::any_of(
      test_case.expected_rows,
      [](const std::vector<std::string>& fields) { return fields.size() > 1; });
  auto matches_expected = [&](const Row& row,
                              const std::vector<std::string>& expected) {
    // Value-table goldens print a whole-row NULL as a single "NULL" token;
    // engines that store the struct flattened (one column per field) present
    // the same row as all-NULL columns.
    auto lower_trim = [](const std::string& s) {
      const char* ws = " \t\r\n";
      size_t b = s.find_first_not_of(ws);
      if (b == std::string::npos) {
        return std::string();
      }
      size_t e = s.find_last_not_of(ws);
      std::string out = s.substr(b, e - b + 1);
      for (char& c : out) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      return out;
    };
    if (expected.size() == 1 && lower_trim(expected[0]) == "null" &&
        row.values_.size() > 1 && golden_has_multi_field_rows) {
      return std::ranges::all_of(row.values_,
                                 [](const Value& v) { return v.IsNull(); });
    }
    if (row.values_.size() != expected.size()) {
      return false;
    }
    for (size_t i = 0; i < expected.size(); ++i) {
      if (!ComplianceValueMatches(row.values_[i], expected[i])) {
        return false;
      }
    }
    return true;
  };
  if (actual.size() != test_case.expected_rows.size()) {
    *detail = "row count actual=" + std::to_string(actual.size()) +
              " expected=" + std::to_string(test_case.expected_rows.size());
    return false;
  }
  if (test_case.unknown_order) {
    // Multiset match via bipartite matching (Kuhn): a greedy first-fit lets
    // an early expected row steal the only actual row a later expectation
    // can satisfy, producing false reds on legitimately unordered output.
    const size_t n = actual.size();
    std::vector<int64_t> match_actual(n, -1);
    const auto try_match = [&](auto&& self, size_t expected_idx,
                               std::vector<char>& seen) -> bool {
      for (size_t j = 0; j < n; ++j) {
        if (seen[j] != 0 ||
            !matches_expected(actual[j],
                              test_case.expected_rows[expected_idx])) {
          continue;
        }
        seen[j] = 1;
        if (match_actual[j] < 0 ||
            self(self, static_cast<size_t>(match_actual[j]), seen)) {
          match_actual[j] = static_cast<int64_t>(expected_idx);
          return true;
        }
      }
      return false;
    };
    for (size_t e = 0; e < test_case.expected_rows.size(); ++e) {
      std::vector<char> seen(n, 0);
      if (!try_match(try_match, e, seen)) {
        const auto& expected = test_case.expected_rows[e];
        std::string exp_str = "{";
        for (size_t k = 0; k < expected.size(); ++k) {
          if (k != 0) {
            exp_str += ", ";
          }
          exp_str += expected[k];
        }
        exp_str += "}";
        std::string act_str = "[";
        for (size_t a = 0; a < actual.size(); ++a) {
          if (a != 0) {
            act_str += ", ";
          }
          act_str += "{";
          for (size_t b = 0; b < actual[a].values_.size(); ++b) {
            if (b != 0) {
              act_str += ", ";
            }
            act_str += FormatComplianceValue(actual[a].values_[b]);
          }
          act_str += "}";
        }
        act_str += "]";
        *detail = "missing expected row: ";
        *detail += exp_str;
        *detail += " actual rows: ";
        *detail += act_str;
        return false;
      }
    }
    return true;
  }
  for (size_t i = 0; i < actual.size(); ++i) {
    if (!matches_expected(actual[i], test_case.expected_rows[i])) {
      std::string row_str = "{";
      for (size_t j = 0; j < actual[i].values_.size(); ++j) {
        if (j != 0) {
          row_str += ", ";
        }
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
  // Splits on top-level ';' only: string literals (including triple-quoted
  // ones) and '#' / '--' comments may contain semicolons, quotes and
  // apostrophes that must not disturb the scanner state.
  enum class Mode { kCode, kString, kComment };
  Mode mode = Mode::kCode;
  char quote = '\0';
  // Triple-quoted strings ('''...''' / """...""") delimit with three quote
  // runs; interior quotes of any count must not toggle the string state.
  bool triple = false;
  auto run_length = [&](size_t i) {
    size_t n = 0;
    while (i + n < sql.size() && sql[i + n] == quote && n < 3) {
      ++n;
    }
    return n;
  };
  for (size_t i = 0; i < sql.size(); ++i) {
    const char c = sql[i];
    if (mode == Mode::kComment) {
      current.push_back(c);
      if (c == '\n') {
        mode = Mode::kCode;
      }
      continue;
    }
    if (mode == Mode::kString) {
      current.push_back(c);
      if (!triple && c == '\\' && i + 1 < sql.size()) {
        current.push_back(sql[++i]);
        continue;
      }
      if (c == quote) {
        const size_t run = run_length(i);
        if (triple ? run == 3 : true) {
          for (size_t k = 1; k < (triple ? run : 1); ++k) {
            current.push_back(sql[++i]);
          }
          mode = Mode::kCode;
          triple = false;
        }
      }
      continue;
    }
    if (c == '"' || c == '\'') {
      mode = Mode::kString;
      quote = c;
      triple = run_length(i) == 3;
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
      if (!trimmed.empty()) {
        stmts.push_back(std::move(trimmed));
      }
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
  if (!trimmed.empty()) {
    stmts.push_back(std::move(trimmed));
  }
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

TEST(GoogleSqlComplianceFile, DoubleGoldenDistinguishesSignedZero) {
  EXPECT_TRUE(ComplianceValueMatches(Value(0.0), "0.0"));
  EXPECT_TRUE(ComplianceValueMatches(Value(-0.0), "-0.0"));
  EXPECT_FALSE(ComplianceValueMatches(Value(0.0), "-0.0"));
  EXPECT_FALSE(ComplianceValueMatches(Value(-0.0), "0.0"));
}

TEST(GoogleSqlComplianceFile, DefaultUnknownOrderPersistsAndCanBeOverridden) {
  const auto cases = ParseGoogleSqlComplianceFile("defaults.test", R"test(
[default unknown_order]
SELECT 1
--
1
==
[unknown_order=false]
SELECT 2
--
2
==
SELECT 3
--
3
==
[default unknown_order=false]
SELECT 4
--
4
==
SELECT 5
--
5
)test");
  ASSERT_EQ(cases.size(), 5U);
  EXPECT_TRUE(cases[0].unknown_order);
  EXPECT_FALSE(cases[1].unknown_order);
  EXPECT_TRUE(cases[2].unknown_order);
  EXPECT_FALSE(cases[3].unknown_order);
  EXPECT_FALSE(cases[4].unknown_order);
}

// P846: the `[unknown_order]` option line was absorbed by
// kIgnoredOptionKeys without setting the flag, so the harness compared
// strict row order against order-unspecified expectations.
TEST(GoogleSqlComplianceFile, UnknownOrderOptionLineSetsFlag) {
  constexpr std::string_view kFile = R"test(
[unknown_order]
[name=opt]
SELECT id FROM t
--
ARRAY<STRUCT<id INT64>>[{2}, {1}]
==
[unknown_order=false]
[name=opt_off]
SELECT id FROM t
--
ARRAY<STRUCT<id INT64>>[{1}, {2}]
==
[mode=explain]
[unknown_order]
[name=opt_keep]
SELECT id FROM t
--
ARRAY<STRUCT<id INT64>>[{2}, {1}]
==
)test";
  const std::vector<GoogleSqlComplianceCase> cases =
      ParseGoogleSqlComplianceFile("unknown_order.test", kFile);
  ASSERT_EQ(cases.size(), 3U);
  EXPECT_TRUE(cases[0].unknown_order);
  EXPECT_FALSE(cases[1].unknown_order);
  EXPECT_TRUE(cases[2].unknown_order);
  // Consumed as options: none of the lines leaks into the statement text.
  EXPECT_EQ(cases[0].sql, "SELECT id FROM t");
  EXPECT_EQ(cases[1].sql, "SELECT id FROM t");
  EXPECT_EQ(cases[2].sql, "SELECT id FROM t");
}

TEST(GoogleSqlComplianceFile, AuditNestedArrayScalarKeepsInnerBrackets) {
  const auto cases = ParseGoogleSqlComplianceFile("nested.test", R"test(
[name=nested]
SELECT 1
--
ARRAY<ARRAY<INT64>>[[1, 2], [3, [4, 5]], []]
==
)test");
  ASSERT_EQ(cases.size(), 1U);
  EXPECT_EQ(cases[0].expected_rows, (std::vector<std::vector<std::string>>{
                                        {"[1, 2]"}, {"[3, [4, 5]]"}, {"[]"}}));
  const auto truncated = ParseGoogleSqlComplianceFile("truncated.test", R"test(
[name=truncated]
SELECT 1
--
ARRAY<INT64>[unknown order:
==
)test");
  ASSERT_EQ(truncated.size(), 1U);
  EXPECT_TRUE(truncated[0].expected_rows.empty());
}

TEST(GoogleSqlComplianceFile,
     StringArrayWithNonStringElementDoesNotReadVarchar) {
  // A declared ARRAY<STRING>/ARRAY<BYTES> whose elements carry a non-VARCHAR
  // tag must not read value.varchar_value (union confusion); each element is
  // formatted by its actual type instead.
  const Value confused_strings = Value::Array({Value(1), Value(2)}, "STRING");
  EXPECT_EQ(FormatComplianceValue(confused_strings),
            "ARRAY<STRING>[known order:1, 2]");
  const Value confused_bytes = Value::Array({Value(3)}, "BYTES");
  EXPECT_EQ(FormatComplianceValue(confused_bytes), "ARRAY<BYTES>[3]");
}

TEST(GoogleSqlComplianceFile, MatchesIntBoolAndNull) {
  EXPECT_TRUE(ComplianceValueMatches(Value(int64_t{1}), "1"));
  EXPECT_TRUE(ComplianceValueMatches(Value(int64_t{1}), "true"));
  EXPECT_TRUE(ComplianceValueMatches(Value(int64_t{0}), "false"));
  EXPECT_TRUE(ComplianceValueMatches(Value(), "NULL"));
  EXPECT_TRUE(ComplianceValueMatches(Value(std::string("hi")), "\"hi\""));
  EXPECT_FALSE(ComplianceValueMatches(Value(int64_t{2}), "1"));
}

TEST(GoogleSqlComplianceFile, Int64GoldenRejectsPartialNumericParse) {
  // std::stoll accepts a numeric prefix; a DOUBLE golden must not match an
  // INT64 whose fractional/exponent part was silently discarded.
  EXPECT_FALSE(ComplianceValueMatches(Value(int64_t{2}), "2.5"));
  EXPECT_FALSE(ComplianceValueMatches(Value(int64_t{1}), "1e3"));
  EXPECT_FALSE(ComplianceValueMatches(Value(int64_t{1}), "1abc"));
  EXPECT_FALSE(ComplianceValueMatches(Value(int64_t{0}), "-0.5"));
  // Fully consumed integers and UINT64 magnitudes still match.
  EXPECT_TRUE(ComplianceValueMatches(Value(int64_t{42}), "42"));
  EXPECT_TRUE(ComplianceValueMatches(
      Value(static_cast<int64_t>(UINT64_C(18446744073709551615))),
      "18446744073709551615"));
}

TEST(GoogleSqlComplianceFile, ParsesExplainPlanAssertions) {
  constexpr std::string_view kFile = R"test(
[mode=explain_analyze]
[plan_contains=IndexScan: orders]
[plan_contains=Actual Rows: 1]
[plan_not_contains=FullScan: orders]
[name=point_lookup_with_profile]
SELECT id FROM orders WHERE id = 7
--
ARRAY<STRUCT<id INT64>>[{7}]
==
)test";
  const std::vector<GoogleSqlComplianceCase> cases =
      ParseGoogleSqlComplianceFile("optimizer.test", kFile);
  ASSERT_EQ(cases.size(), 1U);
  EXPECT_EQ(cases[0].mode, "explain_analyze");
  EXPECT_EQ(cases[0].plan_contains,
            (std::vector<std::string>{"IndexScan: orders", "Actual Rows: 1"}));
  EXPECT_EQ(cases[0].plan_not_contains,
            (std::vector<std::string>{"FullScan: orders"}));
}

TEST(GoogleSqlComplianceFile, DefaultTimeZoneScopeAndUnknownOptions) {
  const std::vector<GoogleSqlComplianceCase> cases =
      ParseGoogleSqlComplianceFile("tz.test", R"test(
[default default_time_zone=UTC]
[name=first]
SELECT 1
--
ARRAY<INT64>[1]
==
[name=second]
SELECT 1
--
ARRAY<INT64>[1]
==
[name=third]
[default_time_zone=PST]
SELECT 1
--
ARRAY<INT64>[1]
==
[name=fourth]
SELECT 1
--
ARRAY<INT64>[1]
==
[name=fifth]
[bogus_option=1]
SELECT 2
--
ARRAY<INT64>[2]
==
)test");
  ASSERT_EQ(cases.size(), 5U);
  EXPECT_EQ(cases[0].default_time_zone, "UTC");
  // "default default_time_zone" propagates to later segments.
  EXPECT_EQ(cases[1].default_time_zone, "UTC");
  // A bare "default_time_zone" is case-local and does not leak.
  EXPECT_EQ(cases[2].default_time_zone, "PST");
  EXPECT_EQ(cases[3].default_time_zone, "UTC");
  // An unrecognized option key is not swallowed: it lands in the SQL text
  // and fails loudly at parse time.
  EXPECT_NE(cases[4].sql.find("[bogus_option=1]"), std::string::npos);
}

TEST(GoogleSqlComplianceFile, ParsesSecondaryIndexSetup) {
  constexpr std::string_view kFile = R"test(
[prepare_database]
[secondary_index=by_category:items(category)]
[secondary_index=by_score:items(score)]
CREATE TABLE items AS SELECT 1 AS id, 2 AS category, 3 AS score
--
ARRAY<STRUCT<id INT64, category INT64, score INT64>>[]
==
)test";
  const std::vector<GoogleSqlComplianceCase> cases =
      ParseGoogleSqlComplianceFile("indexes.test", kFile);
  ASSERT_EQ(cases.size(), 1U);
  EXPECT_EQ(cases[0].secondary_indexes,
            (std::vector<std::string>{"by_category:items(category)",
                                      "by_score:items(score)"}));
}

TEST(GoogleSqlComplianceFile, ParsesVendoredCorpus) {
  const std::string directory = TINYLAMB_GOOGLESQL_COMPLIANCE_DIR;
  if (directory.empty() || !std::filesystem::exists(directory)) {
    GTEST_SKIP() << "skipped";
  }
  const std::vector<std::string> files =
      ListGoogleSqlComplianceFiles(directory);
  ASSERT_FALSE(files.empty());
  size_t case_count = 0;
  size_t error_count = 0;
  size_t prepare_count = 0;
  for (const std::string& name : files) {
    const std::filesystem::path path = std::filesystem::path(directory) / name;
    const std::vector<GoogleSqlComplianceCase> cases =
        ParseGoogleSqlComplianceFile(path.string(), ReadFile(path));
    if (name == "no_tests.test") {
      EXPECT_TRUE(cases.empty());
      continue;
    }
    EXPECT_FALSE(cases.empty()) << name;
    for (const GoogleSqlComplianceCase& test_case : cases) {
      ++case_count;
      if (test_case.expect_error) {
        ++error_count;
      }
      if (test_case.prepare_database) {
        ++prepare_count;
      }
      if (test_case.sql.empty() && !test_case.prepare_database) {
        // Empty-SQL segments are dropped unless they still carry an expected
        // result or error; the runner fails those loudly at execution time.
        const bool verifiable =
            !test_case.expected_rows.empty() || test_case.expect_error;
        EXPECT_TRUE(verifiable) << name << " / " << test_case.name;
      }
    }
  }
  EXPECT_GE(files.size(), 200U);
  EXPECT_GE(case_count, 1000U);
  EXPECT_GT(error_count, 0U);
  EXPECT_GT(prepare_count, 0U);
}

TEST(GoogleSqlComplianceFile, SkipsDifferentialPrivacy) {
  GoogleSqlComplianceCase test_case;
  test_case.sql =
      "SELECT * FROM t WITH DIFFERENTIAL_PRIVACY OPTIONS(epsilon=1)";
  EXPECT_TRUE(IsDifferentialPrivacyCase(test_case));
  test_case.sql = "SELECT 1";
  EXPECT_FALSE(IsDifferentialPrivacyCase(test_case));
}

#ifdef TINYLAMB_GOOGLESQL_COMPLIANCE_RUN
namespace {
// Features this engine claims for compliance feature-gating. A case tagged
// with required_features outside this set is skipped, mirroring the reference
// TestDriver contract (unsupported-feature cases are neither pass nor fail).
bool CaseFeatureSupported(const GoogleSqlComplianceCase& test_case) {
  // Features this engine implements well enough to run their cases; every
  // other required_features tag skips the case (and file-level setup for
  // whole-file features), mirroring the reference TestDriver contract where
  // unsupported-feature cases are neither pass nor fail.
  static const std::unordered_set<std::string> kSupported = {
      "INLINE_LAMBDA_ARGUMENT",         // ARRAY_TRANSFORM/ARRAY_FILTER lambdas
      "TEMPLATE_FUNCTIONS",             // ANY TYPE templated SQL functions
      "ANY_STRING_TEMPLATED_ARGUMENT",  // ANY STRING templated parameters
      "WITH_ON_SUBQUERY",               // WITH clauses inside function bodies
  };
  if (!std::ranges::all_of(test_case.required_features,
                           [&](const std::string& feature) {
                             return kSupported.contains(feature);
                           })) {
    return false;
  }
  // forbidden_features gates the opposite direction: the case is only
  // valid while the engine LACKS the listed capability. tinylamb implements
  // none of the corpus's forbidden flags today (the DISALLOW_* primary-key
  // switches, TIMESTAMP_NANOS, DISABLE_ARRAY_MIN_AND_MAX,
  // DISALLOW_LEGACY_UNICODE_COLLATION); the set stays empty so landing a
  // capability later turns the skip on automatically.
  static const std::unordered_set<std::string> kForbiddenCapabilities;
  return std::ranges::all_of(test_case.forbidden_features,
                             [&](const std::string& feature) {
                               return !kForbiddenCapabilities.contains(feature);
                             });
}

bool IsMutatingSql(std::string_view sql) {
  auto prefix = [](std::string_view text, std::string_view keyword) {
    if (text.size() < keyword.size()) {
      return false;
    }
    if (!text.starts_with(keyword)) {
      return false;
    }
    if (text.size() == keyword.size()) {
      return true;
    }
    const char next = text[keyword.size()];
    return !std::isalnum(static_cast<unsigned char>(next)) && next != '_';
  };

  // ParseSegment keeps '--' comment lines in case SQL, so a DML statement
  // can start behind a comment banner. Skip leading whitespace and comments
  // and classify the first real token, not the first byte.
  size_t begin = 0;
  while (begin < sql.size()) {
    if (std::isspace(static_cast<unsigned char>(sql[begin])) != 0) {
      ++begin;
      continue;
    }
    if (sql[begin] == '#' || (sql[begin] == '-' && begin + 1 < sql.size() &&
                              sql[begin + 1] == '-')) {
      const size_t eol = sql.find('\n', begin);
      if (eol == std::string_view::npos) {
        return false;  // comment-only SQL cannot mutate state
      }
      begin = eol + 1;
      continue;
    }
    if (sql[begin] == '/' && begin + 1 < sql.size() && sql[begin + 1] == '*') {
      const size_t end = sql.find("*/", begin + 2);
      if (end == std::string_view::npos) {
        return false;  // unterminated comment: no statement to classify
      }
      begin = end + 2;
      continue;
    }
    break;
  }
  // SQL keywords are case-insensitive; normalize before matching.
  const std::string upper = AsciiToUpper(sql.substr(begin));
  return prefix(upper, "INSERT") || prefix(upper, "UPDATE") ||
         prefix(upper, "DELETE") || prefix(upper, "MERGE") ||
         prefix(upper, "TRUNCATE") || prefix(upper, "DROP");
}
}  // namespace

TEST(GoogleSqlComplianceFile, IsMutatingSqlDetectsCommentPrefixesAndDdl) {
  EXPECT_TRUE(IsMutatingSql("INSERT INTO t VALUES (1)"));
  EXPECT_TRUE(IsMutatingSql("  update t set a = 1"));
  EXPECT_TRUE(IsMutatingSql("-- setup\nINSERT INTO t VALUES (1)"));
  EXPECT_TRUE(IsMutatingSql("# comment\nDELETE FROM t"));
  EXPECT_TRUE(IsMutatingSql("/* banner */ MERGE INTO t USING s ON TRUE"));
  EXPECT_TRUE(IsMutatingSql("TRUNCATE TABLE t"));
  EXPECT_TRUE(IsMutatingSql("DROP TABLE t"));
  EXPECT_TRUE(IsMutatingSql("\n-- a\n-- b\nMERGE INTO t USING s ON t.k = s.k"));
  // Non-mutating statements (and comment-only SQL) do not force isolation.
  EXPECT_FALSE(IsMutatingSql("SELECT 1"));
  EXPECT_FALSE(IsMutatingSql("-- just a comment\nSELECT 1"));
  EXPECT_FALSE(IsMutatingSql("INSERTION IS NOT INSERT"));
  EXPECT_FALSE(IsMutatingSql("-- nothing but a comment"));
  EXPECT_FALSE(IsMutatingSql(""));
}

class GoogleSqlComplianceFileTest
    : public ::testing::TestWithParam<std::string> {};

TEST_P(GoogleSqlComplianceFileTest, RunsFile) {
  const std::string directory = TINYLAMB_GOOGLESQL_COMPLIANCE_DIR;
  if (directory.empty() || !std::filesystem::exists(directory)) {
    GTEST_SKIP() << "skipped";
  }
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "skipped";
  }

  const std::filesystem::path path =
      std::filesystem::path(directory) / GetParam();
  const std::vector<GoogleSqlComplianceCase> cases =
      ParseGoogleSqlComplianceFile(path.string(), ReadFile(path));

  bool file_mutates_state = false;
  for (const GoogleSqlComplianceCase& test_case : cases) {
    if (!test_case.prepare_database && IsMutatingSql(test_case.sql)) {
      file_mutates_state = true;
      break;
    }
  }

  // Restore the thread-local compliance flag on every exit path.
  struct ModeReset {  // NOLINT(cppcoreguidelines-special-member-functions)
    ~ModeReset() { SqlEngine::SetCompliancePrimaryKeyMode(false); }
  } mode_reset;
  const std::string path_prefix = "googlesql_compliance-" + RandomString(8);
  auto database = Database::Create(path_prefix).MoveValue();
  auto context = std::make_unique<TransactionContext>(database->BeginContext());
  auto engine = std::make_unique<SqlEngine>(*database);

  struct DatabaseCleaner {
    std::unique_ptr<SqlEngine>& engine;
    std::unique_ptr<TransactionContext>& context;
    std::unique_ptr<Database>& database;
    DatabaseCleaner(std::unique_ptr<SqlEngine>& engine_in,
                    std::unique_ptr<TransactionContext>& context_in,
                    std::unique_ptr<Database>& database_in)
        : engine(engine_in), context(context_in), database(database_in) {}
    DatabaseCleaner(const DatabaseCleaner&) = delete;
    DatabaseCleaner& operator=(const DatabaseCleaner&) = delete;
    DatabaseCleaner(DatabaseCleaner&&) = delete;
    DatabaseCleaner& operator=(DatabaseCleaner&&) = delete;
    ~DatabaseCleaner() {
      engine.reset();
      context.reset();
      if (database != nullptr) {
        database->DeleteAll();
        database.reset();
      }
    }
  } cleaner{engine, context, database};

  // Files whose cases mutate tables get per-case isolation: each case runs
  // against a freshly created database with the prepared state replayed,
  // mirroring the reference driver's per-case test database. A distinct
  // storage prefix per rebuild keeps a destroyed instance's WAL from being
  // recovered into its replacement.
  struct PreparedSegment {
    std::vector<std::string> statements{};
    bool first_column_is_primary_key{false};
    std::vector<std::string> secondary_indexes{};
  };
  std::vector<PreparedSegment> prepare_segments;
  int environment_generation = 0;
  // A failed replay leaves the fresh database in an unknown partial state;
  // every later case in the file would then compare against wrong data, so
  // the first failure is reported and all remaining cases are skipped.
  bool replay_failed = false;
  auto replay_prepared_state = [&]() {
    for (const PreparedSegment& segment : prepare_segments) {
      SqlEngine::SetCompliancePrimaryKeyMode(
          segment.first_column_is_primary_key);
      for (const std::string& stmt : segment.statements) {
        Status s_status = Status::kSuccess;
        std::string replay_error;
        Drain(*engine, *context, stmt, &s_status, &replay_error);
        if (s_status != Status::kSuccess) {
          ADD_FAILURE() << GetParam()
                        << " prepared-state replay failed: " << replay_error
                        << "\n"
                        << stmt;
          replay_failed = true;
          return;
        }
      }
      for (const std::string& index : segment.secondary_indexes) {
        const Status index_status =
            CreateComplianceSecondaryIndex(*database, *context, index);
        if (index_status != Status::kSuccess) {
          ADD_FAILURE() << GetParam() << " prepared-state index replay failed: "
                        << ToString(index_status.GetCode()) << " " << index;
          replay_failed = true;
          return;
        }
      }
    }
  };

  for (const GoogleSqlComplianceCase& test_case : cases) {
    if (IsDifferentialPrivacyCase(test_case)) {
      continue;
    }
    // Feature gating applies to prepare_database setup too: whole-file
    // features (SQL_GRAPH, RANGE_TYPE, ...) gate their file's setup blocks,
    // exactly like the reference driver skips unsupported feature files.
    if (!CaseFeatureSupported(test_case)) {
      continue;
    }
    if (replay_failed) {
      // A previous state rebuild failed; this case would run against a
      // partial database, so skip it explicitly rather than report noise.
      continue;
    }
    SetDefaultTimeZone(test_case.default_time_zone.empty()
                           ? "America/Los_Angeles"
                           : test_case.default_time_zone);
    if (test_case.prepare_database) {
      const std::vector<std::string> stmts = SplitStatements(test_case.sql);
      const bool first_column_is_primary_key =
          test_case.primary_key_mode == "first_column_is_primary_key";
      prepare_segments.push_back(PreparedSegment{
          .statements = stmts,
          .first_column_is_primary_key = first_column_is_primary_key,
          .secondary_indexes = test_case.secondary_indexes});
      SqlEngine::SetCompliancePrimaryKeyMode(first_column_is_primary_key);
      for (const std::string& stmt : stmts) {
        Status s_status = Status::kSuccess;
        try {
          Drain(*engine, *context, stmt, &s_status);
        } catch (const std::exception& ex) {
          ADD_FAILURE() << GetParam() << " / " << test_case.name
                        << " prepare threw: " << ex.what() << "\n"
                        << stmt;
          break;
        }
        if (s_status != Status::kSuccess) {
          EXPECT_EQ(s_status, Status::kSuccess)
              << GetParam() << " / " << test_case.name
              << " prepare failed: " << engine->LastError() << "\n"
              << stmt;
          break;
        }
      }
      for (const std::string& index : test_case.secondary_indexes) {
        const Status index_status =
            CreateComplianceSecondaryIndex(*database, *context, index);
        EXPECT_EQ(index_status, Status::kSuccess)
            << GetParam() << " / " << test_case.name
            << " secondary index failed: " << index;
      }
      continue;
    }

    if (file_mutates_state) {
      engine.reset();
      context.reset();
      database->DeleteAll();
      database.reset();
      const std::string case_prefix =
          path_prefix + "-" + std::to_string(++environment_generation);
      database = Database::Create(case_prefix).MoveValue();
      context = std::make_unique<TransactionContext>(database->BeginContext());
      engine = std::make_unique<SqlEngine>(*database);
      replay_prepared_state();
    }
    SqlEngine::SetCompliancePrimaryKeyMode(test_case.primary_key_mode ==
                                           "first_column_is_primary_key");

    if (test_case.sql.empty()) {
      // The parser keeps result-bearing segments with no SQL so malformed
      // corpus entries cannot vanish silently.
      ADD_FAILURE() << GetParam() << " / " << test_case.name
                    << " has an expected result but empty SQL\n"
                    << test_case.raw_result;
      continue;
    }
    if (!test_case.unparseable_parameters.empty()) {
      ADD_FAILURE() << GetParam() << " / " << test_case.name
                    << " has unparsable [parameters=] item(s):";
      for (const std::string& item : test_case.unparseable_parameters) {
        ADD_FAILURE() << "  " << item;
      }
      continue;
    }

    Status status = Status::kSuccess;
    std::string error_msg;
    std::vector<Row> rows;
    try {
      rows = Drain(*engine, *context, test_case.sql, &status, &error_msg);
      const std::optional<StatementType> executed_type =
          engine->LastStatementType();
      if (status == Status::kSuccess && executed_type.has_value()) {
        const StatementType executed = *executed_type;
        if (executed == StatementType::kInsert ||
            executed == StatementType::kUpdate ||
            executed == StatementType::kDelete) {
          // The reference driver reports num_rows_modified plus the full
          // post-statement table contents. This harness currently compares
          // only the post-statement rows (the scalar affected-row count is
          // not captured by the compliance case parser), so it cannot detect
          // a wrong DML row count on its own.
          rows = Drain(*engine, *context,
                       "SELECT * FROM `" + engine->LastDmlTable() + "`",
                       &status, &error_msg);
        }
      }
    } catch (const std::exception& ex) {
      if (!test_case.expect_error) {
        ADD_FAILURE() << GetParam() << " / " << test_case.name
                      << " threw: " << ex.what() << "\n"
                      << test_case.sql;
      } else {
        EXPECT_TRUE(ComplianceErrorTextMatches(test_case.error_text, ex.what(),
                                               std::string_view{}))
            << GetParam() << " / " << test_case.name
            << " expected error text: " << test_case.error_text
            << ", engine threw: " << ex.what() << "\n"
            << test_case.sql;
      }
      continue;
    }

    if (test_case.expect_error) {
      EXPECT_NE(status, Status::kSuccess)
          << GetParam() << " / " << test_case.name
          << " expected error, engine said: " << error_msg << "\n"
          << test_case.sql;
      EXPECT_TRUE(ComplianceErrorTextMatches(test_case.error_text, error_msg,
                                             ToString(status.GetCode())))
          << GetParam() << " / " << test_case.name
          << " expected error text: " << test_case.error_text
          << ", engine said: " << error_msg << "\n"
          << test_case.sql;
    } else if (status != Status::kSuccess) {
      ADD_FAILURE() << GetParam() << " / " << test_case.name
                    << " execution failed: " << error_msg << "\n"
                    << test_case.sql;
    } else {
      std::string detail;
      bool matched = false;
      try {
        matched = RowsMatch(rows, test_case, &detail);
      } catch (const std::exception& ex) {
        matched = false;
        detail = std::string("RowsMatch threw: ") + ex.what();
      }
      EXPECT_TRUE(matched) << GetParam() << " / " << test_case.name << " "
                           << detail << "\n"
                           << test_case.sql << "\n"
                           << test_case.raw_result;

      if (!test_case.plan_contains.empty() ||
          !test_case.plan_not_contains.empty()) {
        Status explain_status = Status::kSuccess;
        std::string explain_error;
        const std::string prefix = test_case.mode == "explain_analyze"
                                       ? "EXPLAIN ANALYZE "
                                       : "EXPLAIN ";
        const std::vector<Row> explain_rows =
            Drain(*engine, *context, prefix + test_case.sql, &explain_status,
                  &explain_error);
        EXPECT_EQ(explain_status, Status::kSuccess)
            << GetParam() << " / " << test_case.name
            << " EXPLAIN failed: " << explain_error << "\n"
            << test_case.sql;
        if (explain_status != Status::kSuccess) {
          continue;
        }
        std::string plan;
        for (const Row& explain_row : explain_rows) {
          if (!explain_row.values_.empty()) {
            if (!plan.empty()) {
              plan.push_back('\n');
            }
            // Match the EXPLAIN text itself, including quoted SQL literals.
            plan += explain_row[0].AsStringView();
          }
        }
        for (const std::string& fragment : test_case.plan_contains) {
          EXPECT_NE(plan.find(fragment), std::string::npos)
              << GetParam() << " / " << test_case.name
              << " missing plan fragment: " << fragment << "\n"
              << plan;
        }
        for (const std::string& fragment : test_case.plan_not_contains) {
          EXPECT_EQ(plan.find(fragment), std::string::npos)
              << GetParam() << " / " << test_case.name
              << " unexpected plan fragment: " << fragment << "\n"
              << plan;
        }
      }
    }
  }
  // database->DeleteAll() handled by cleaner RAII
}

namespace {
std::vector<std::string> ComplianceFileNames() {
  const std::string directory = TINYLAMB_GOOGLESQL_COMPLIANCE_DIR;
  if (directory.empty() || !std::filesystem::exists(directory)) {
    return {"<missing>"};
  }
  std::vector<std::string> files = ListGoogleSqlComplianceFiles(directory);
  if (files.empty()) {
    return {"<empty>"};
  }
  return files;
}
}  // namespace

INSTANTIATE_TEST_SUITE_P(
    GoogleSqlCompliance, GoogleSqlComplianceFileTest,
    ::testing::ValuesIn(ComplianceFileNames()),
    [](const ::testing::TestParamInfo<std::string>& param) {
      std::string name = param.param;
      for (char& c : name) {
        if (std::isalnum(static_cast<unsigned char>(c)) == 0) {
          c = '_';
        }
      }
      return name;
    });
#endif  // TINYLAMB_GOOGLESQL_COMPLIANCE_RUN

}  // namespace tinylamb
