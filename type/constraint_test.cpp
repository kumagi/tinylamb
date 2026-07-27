/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "type/constraint.hpp"

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "value.hpp"

namespace tinylamb {

TEST(Constraint, Construct) {
  // Arrange -- nothing more than default Constraint kinds
  // Act -- construct two Constraints of different kinds/values
  Constraint c(Constraint::kNothing);
  Constraint s(Constraint::kDefault, Value("hello"));
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(Constraint, SerializeDeserialize) {
  // Arrange -- nothing more than default Constraint kinds
  // Act -- serialize+deserialize round-trip for 5 Constraint variants
  SerializeDeserializeTest(Constraint(Constraint::kNothing));
  SerializeDeserializeTest(Constraint(Constraint::kDefault, Value(2)));
  SerializeDeserializeTest(Constraint(Constraint::kUnique));
  SerializeDeserializeTest(Constraint(Constraint::kPrimaryKey));
  SerializeDeserializeTest(Constraint(Constraint::kNotNull));
  // Assert -- implicit; SerializeDeserializeTest macro asserts equality
}

TEST(Constraint, Dump) {
  // Arrange -- nothing more than two Constraints of different kinds
  // Act -- stream Constraints to LOG (no assertion; output-only)
  LOG(INFO) << Constraint(Constraint::kNothing);
  LOG(WARN) << Constraint(Constraint::kDefault, Value(2));
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

}  // namespace tinylamb