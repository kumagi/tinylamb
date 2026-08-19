/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/googlesql_frontend.hpp"

#include <gtest/gtest.h>

namespace tinylamb {

TEST(GoogleSqlFrontendTest, ReturnsParserAst) {
  GoogleSqlParseResult result = GoogleSqlFrontend::Parse(
      "select c_id from customer where c_w_id=1 order by c_id limit 1");
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_NE(result.ast.find("QueryStatement"), std::string::npos);
  EXPECT_NE(result.ast.find("OrderBy"), std::string::npos);
}

TEST(GoogleSqlFrontendTest, RejectsInvalidSqlWhenAvailable) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "GoogleSQL parser disabled for this platform";
  }
  GoogleSqlParseResult result = GoogleSqlFrontend::Parse("SELECT 1 + ;");
  EXPECT_FALSE(result.ok);
  EXPECT_FALSE(result.error.empty());
}

}  // namespace tinylamb
