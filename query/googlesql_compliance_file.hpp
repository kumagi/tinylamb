/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GOOGLESQL_COMPLIANCE_FILE_HPP
#define TINYLAMB_GOOGLESQL_COMPLIANCE_FILE_HPP

#include <string>
#include <string_view>
#include <vector>

#include "type/value.hpp"

namespace tinylamb {

// One segment of a GoogleSQL compliance *.test file (file-based test driver).
struct GoogleSqlComplianceCase {
  std::string file;
  std::string name;
  std::string sql;
  bool prepare_database{false};
  bool expect_error{false};
  std::string error_text;
  // Top-level STRUCT fields of each expected row, as raw token text
  // (NULL, true, 1, "hi", 3.0, ...). Nested values stay as a single token.
  std::vector<std::vector<std::string>> expected_rows;
  bool unknown_order{false};
  std::vector<std::string> required_features;
  // primary_key_mode option: "" (driver default), "no_primary_key", or
  // "first_column_is_primary_key".
  std::string primary_key_mode;
  std::vector<std::pair<std::string, std::string>> parameters;
  std::string default_time_zone;
  std::string raw_result;
};


[[nodiscard]] std::vector<GoogleSqlComplianceCase> ParseGoogleSqlComplianceFile(
    std::string_view path, std::string_view contents);

[[nodiscard]] std::vector<std::string> ListGoogleSqlComplianceFiles(
    std::string_view directory);

[[nodiscard]] bool IsDifferentialPrivacyCase(
    const GoogleSqlComplianceCase& test_case);

// Compare a tinylamb Value with a golden token from the compliance file.
[[nodiscard]] bool ComplianceValueMatches(const Value& actual,
                                          std::string_view expected);

[[nodiscard]] std::string FormatComplianceValue(const Value& value);

}  // namespace tinylamb

#endif  // TINYLAMB_GOOGLESQL_COMPLIANCE_FILE_HPP
