//
// Created by kumagi on 2022/02/06.
//
#include "type/value.hpp"

#include "common/log_message.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(ValueTest, Construct) { Value v; }

void SerializeDeserializeTest(const Value& v) {
  std::string buff;
  const size_t estimated_footprint = v.Size();
  buff.resize(estimated_footprint);
  ASSERT_EQ(estimated_footprint, v.Serialize(buff.data()));
  Value another;
  ASSERT_NE(v, another);
  another.Deserialize(buff.data(), v.type);
  ASSERT_EQ(v, another);
}

TEST(ValueTest, SerializeDesrialize) {
  SerializeDeserializeTest(Value(1));
  SerializeDeserializeTest(Value(int64_t(-301)));
  SerializeDeserializeTest(Value("hello"));
  SerializeDeserializeTest(Value(439.3));
}

TEST(ValueTest, Compare) {
  ASSERT_TRUE(Value(1) < Value(2));
  ASSERT_TRUE(Value(-123.0) < Value(23.0));
  ASSERT_TRUE(Value("abc") < Value("d"));
}

TEST(ValueTest, Dump) {
  LOG(TRACE) << Value(12);
  LOG(DEBUG) << Value(120214143342323);
  LOG(INFO) << Value("foo-bar");
  LOG(WARN) << Value(1.23e3);
  LOG(ERROR) << Value();
  LOG(FATAL) << Value("foo") << " " << Value();
}

}  // namespace tinylamb
