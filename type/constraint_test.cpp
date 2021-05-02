//
// Created by kumagi on 2022/02/06.
//
#include "type/constraint.hpp"

#include "common/log_message.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(Constraint, Construct) {
  Constraint c(Constraint::kNothing);
  Constraint s(Constraint::kDefault, Value("hello"));
}

TEST(Constraint, SerializeDeserialize) {
  SerializeDeserializeTest(Constraint(Constraint::kNothing));
  SerializeDeserializeTest(Constraint(Constraint::kDefault, Value(2)));
  SerializeDeserializeTest(Constraint(Constraint::kUnique));
  SerializeDeserializeTest(Constraint(Constraint::kPrimaryKey));
  SerializeDeserializeTest(Constraint(Constraint::kNotNull));
}

TEST(Constraint, Dump) {
  LOG(INFO) << Constraint(Constraint::kNothing);
  LOG(WARN) << Constraint(Constraint::kDefault, Value(2));
}

}  // namespace tinylamb