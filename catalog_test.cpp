#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "catalog.hpp"
#include "schema_params.hpp"

namespace tinylamb {

class CatalogTest : public ::testing::Test {
 protected:
  static constexpr char kFileName[] = "catalog_test.db";
  void SetUp() override {
    Recover();
    c_->Initialize();
  }
  void Recover() {
    c_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kFileName, 10);
    c_ = std::make_unique<Catalog>(p_.get());
  }

  void TearDown() override {
    std::remove(kFileName);
  }

  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Catalog> c_;
};

TEST_F(CatalogTest, Construction) {
}

TEST_F(CatalogTest, CreateTable) {
  ASSERT_TRUE(c_->CreateTable(
      SchemaParams("test_table_for_create",
                   {AttributeParameter::IntType("inttype"),
                    AttributeParameter::VarcharType("varchartype", 10)
                   })));
  ASSERT_EQ(c_->Schemas(), 1u);
}

TEST_F(CatalogTest, GetTable) {
  EXPECT_TRUE(c_->CreateTable(
      SchemaParams("test_table_for_get",
                   {AttributeParameter::IntType("inttype"),
                    AttributeParameter::VarcharType("varchartype", 10)
                   })));
  ASSERT_NE(c_->GetTable("test_table_for_get"), nullptr);
  ASSERT_EQ(c_->GetTable("test_table_for_get")->GetName(),
            "test_table_for_get");
}

TEST_F(CatalogTest, Recover) {
  SchemaParams params("test_table_for_recover",
                      {AttributeParameter::IntType("inttype"),
                       AttributeParameter::VarcharType("varchartype", 10)
                      });
  EXPECT_TRUE(c_->CreateTable(params));
  ASSERT_NE(c_->GetTable("test_table_for_recover"), nullptr);
  Recover();
  ASSERT_NE(c_->GetTable("test_table_for_recover"), nullptr);
  ASSERT_EQ(c_->Schemas(), 1u);
}

TEST_F(CatalogTest, MultipleTables) {
  constexpr int kTables = 50;
  for (int i = 0; i < kTables; ++i) {
    ASSERT_TRUE(c_->CreateTable(
        SchemaParams(std::to_string(i) + "_table",
                     {AttributeParameter::VarcharType("value1", i),
                      AttributeParameter::VarcharType("value2", i * 2)})));
  }
  Recover();
  ASSERT_EQ(c_->Schemas(), kTables);
  for (int i = 0; i < kTables; ++i) {
    auto* info = c_->GetTable(std::to_string(i) + "_table");
    ASSERT_EQ(info->GetName(), std::to_string(i) + "_table");
    ASSERT_EQ(info->Attributes(), 2);
    ASSERT_EQ(info->GetAttribute(0).GetName(), "value1");
    ASSERT_EQ(info->GetAttribute(0).value_type, ValueType::kVarCharType);
    ASSERT_EQ(info->GetAttribute(0).value_length, i);
    ASSERT_EQ(info->GetAttribute(1).GetName(), "value2");
    ASSERT_EQ(info->GetAttribute(1).value_type, ValueType::kVarCharType);
    ASSERT_EQ(info->GetAttribute(1).value_length, i * 2);
  }
}

}  // namespace tinylamb