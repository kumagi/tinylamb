//
// Created by kumagi on 2022/02/06.
//
#include "type/column.hpp"

#include "common/test_util.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(ColumnTest, Construct) {
  Column c(ColumnName("test_column"), ValueType::kInt64);
  Column d(ColumnName("next_column"), ValueType::kVarChar,
           Constraint(Constraint::kUnique));
}

TEST(ColumnTest, SerializeDeserialize) {
  SerializeDeserializeTest(
      Column(ColumnName("test_column"), ValueType::kInt64));
  SerializeDeserializeTest(Column(ColumnName("next_column"), ValueType::kDouble,
                                  Constraint(Constraint::kUnique)));
}

TEST(ColumnTest, Dump) {
  LOG(INFO) << Column(ColumnName("test_column"), ValueType::kInt64);
  LOG(ERROR) << Column(ColumnName("next_column"), ValueType::kDouble,
                       Constraint(Constraint::kUnique));
}

}  // namespace tinylamb
