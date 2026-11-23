/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <charconv>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "query/sql_session_fuzzer.hpp"

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "usage: sql_session_regression_refresh FILE.test "
                 "SQL_COUNT REFRESHABLE_SQL... [FILE.test SQL_COUNT "
                 "REFRESHABLE_SQL...]\n";
    return 2;
  }

  bool failed = false;
  for (int i = 1; i < argc;) {
    if (i + 1 >= argc) {
      std::cerr << "missing SQL count for " << argv[i] << "\n";
      return 2;
    }
    const std::string path = argv[i];
    size_t sql_count = 0;
    const std::string count_text = argv[i + 1];
    const auto parsed = std::from_chars(
        count_text.data(), count_text.data() + count_text.size(), sql_count);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != count_text.data() + count_text.size() || sql_count == 0 ||
        std::cmp_greater(sql_count, argc - i - 2)) {
      std::cerr << "invalid SQL count for " << path << "\n";
      return 2;
    }
    std::vector<std::string> refreshable_sql;
    refreshable_sql.reserve(sql_count);
    for (size_t j = 0; j < sql_count; ++j) {
      refreshable_sql.emplace_back(argv[i + 2 + static_cast<int>(j)]);
    }
    i += 2 + static_cast<int>(sql_count);
    const std::string error =
        tinylamb::RefreshSessionRegressionFile(path, refreshable_sql);
    if (!error.empty()) {
      std::cerr << error << "\n";
      failed = true;
      continue;
    }
    std::cout << "refreshed " << path << "\n";
  }
  return failed ? 1 : 0;
}
