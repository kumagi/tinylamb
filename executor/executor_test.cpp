//
// Created by kumagi on 2022/02/23.
//

#include "gtest/gtest.h"
#include "table/fake_table.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

class ExecutorTest : public ::testing::Test {
 public:
  void SetUp() override {}

  FakeTable table_{{Row({Value(0), Value("hello"), Value(1.2)})},
                   {Row({Value(3), Value("piyo"), Value(12.2)})},
                   {Row({Value(1), Value("world"), Value(4.9)})},
                   {Row({Value(2), Value("arise"), Value(4.14)})}};
};

TEST_F(ExecutorTest, Construct) {}

TEST_F(ExecutorTest, FullScan) {

}

}  // namespace tinylamb