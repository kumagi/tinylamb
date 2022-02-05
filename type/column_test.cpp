//
// Created by kumagi on 2022/02/06.
//
#include "type/column.hpp"

#include "common/test_util.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(ColumnTest, Construct) {
  Column c("test_column", ValueType::kInt64);
  Column d("next_column", ValueType::kVarChar, Constraint(Constraint::kUnique));
}

TEST(ColumnTest, SerializeDeserialize) {
  SerializeDeserializeTest(Column("test_column", ValueType::kInt64));
  SerializeDeserializeTest(Column("next_column", ValueType::kDouble,
                                  Constraint(Constraint::kUnique)));
}

TEST(ColumnTest, Dump) {
  LOG(INFO) << Column("test_column", ValueType::kInt64);
  LOG(ERROR) << Column("next_column", ValueType::kDouble,
                       Constraint(Constraint::kUnique));
}

}  // namespace tinylamb
