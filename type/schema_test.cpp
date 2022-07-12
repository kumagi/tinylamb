
#include "schema.hpp"

#include "common/test_util.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(SchemaTest, Construct) {
  Schema s("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                             Constraint(Constraint::kPrimaryKey)),
                      Column(ColumnName("c2"), ValueType::kDouble)});
  Schema t("next_schema", {Column(ColumnName("c1"), ValueType::kInt64,
                                  Constraint(Constraint::kPrimaryKey)),
                           Column(ColumnName("c2"), ValueType::kDouble),
                           Column(ColumnName("c3"), ValueType::kVarChar)});
}

TEST(SchemaTest, SerializeDeserialize) {
  SerializeDeserializeTest(
      Schema("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                               Constraint(Constraint::kPrimaryKey)),
                        Column(ColumnName("c2"), ValueType::kDouble)}));
  SerializeDeserializeTest(
      Schema("next_schema", {Column(ColumnName("c1"), ValueType::kInt64,
                                    Constraint(Constraint::kPrimaryKey)),
                             Column(ColumnName("c2"), ValueType::kDouble),
                             Column(ColumnName("c3"), ValueType::kVarChar)}));
}

TEST(SchemaTest, Dump) {
  LOG(INFO) << Schema("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                                        Constraint(Constraint::kPrimaryKey)),
                                 Column(ColumnName("c2"), ValueType::kDouble)});
  LOG(WARN) << Schema("next_schema",
                      {Column(ColumnName("c1"), ValueType::kInt64,
                              Constraint(Constraint::kPrimaryKey)),
                       Column(ColumnName("c2"), ValueType::kDouble),
                       Column(ColumnName("c3"), ValueType::kVarChar)});
}
}  // namespace tinylamb