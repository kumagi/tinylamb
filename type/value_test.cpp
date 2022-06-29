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

void MemcomparableFormatEncodeTest(const std::vector<Value>& input) {
  std::vector<Value> values(input);
  std::sort(values.begin(), values.end());
  std::vector<std::string> encoded;
  encoded.reserve(values.size());
  for (const auto& v : values) {
    encoded.push_back(v.EncodeMemcomparableFormat());
  }
  for (size_t i = 0; i < encoded.size(); ++i) {
    for (size_t j = i + 1; j < encoded.size(); ++j) {
      EXPECT_LT(encoded[i], encoded[j]);
    }
  }
}

TEST(ValueTest, MemcomparableOrderInt) {
  MemcomparableFormatEncodeTest({Value(1), Value(2), Value(3)});
  MemcomparableFormatEncodeTest({Value(-1), Value(-2), Value(-3)});
  MemcomparableFormatEncodeTest({Value(std::numeric_limits<int64_t>::max()),
                                 Value(std::numeric_limits<int64_t>::min()),
                                 Value(1), Value(0), Value(-1)});
}

TEST(ValueTest, MemcomparableVarchar) {
  EXPECT_EQ(Value("a").EncodeMemcomparableFormat(),
            std::string(
                {'\2', 'a', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\x01'}));
  EXPECT_EQ(Value("ab").EncodeMemcomparableFormat(),
            std::string(
                {'\2', 'a', 'b', '\0', '\0', '\0', '\0', '\0', '\0', '\x02'}));
  EXPECT_EQ(
      Value("abc").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', '\0', '\0', '\0', '\0', '\0', '\x03'}));
  EXPECT_EQ(
      Value("abc").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', '\0', '\0', '\0', '\0', '\0', '\x03'}));
  EXPECT_EQ(
      Value("abcd").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', '\0', '\0', '\0', '\0', '\x04'}));
  EXPECT_EQ(
      Value("abcde").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', '\0', '\0', '\0', '\x05'}));
  EXPECT_EQ(
      Value("abcdef").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', '\0', '\0', '\x06'}));
  EXPECT_EQ(
      Value("abcdefg").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', '\0', '\x07'}));
  EXPECT_EQ(
      Value("abcdefgh").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x08'}));
  EXPECT_EQ(Value("abcdefghi").EncodeMemcomparableFormat(),
            std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x09',
                         'i', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\1'}));
  EXPECT_EQ(
      Value("abcdefghij").EncodeMemcomparableFormat(),
      std::string({'\2', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', '\x09', 'i',
                   'j', '\0', '\0', '\0', '\0', '\0', '\0', '\x02'}));
  EXPECT_EQ(
      Value("\x60\x70\x10\x11\x12\x80\x90\x01").EncodeMemcomparableFormat(),
      std::string("\x02\x60\x70\x10\x11\x12\x80\x90\x01\x08"));
}

TEST(ValueTest, MemcomparableOrderVarchar) {
  MemcomparableFormatEncodeTest({Value("a"), Value("aa"), Value("aaa")});
  MemcomparableFormatEncodeTest({Value("a"), Value("b"), Value("c")});
  MemcomparableFormatEncodeTest(
      {Value("blah,blah,blah"), Value("this is a pen"), Value("0123456789")});
}

TEST(ValueTest, MemcomparableDouble) {
  EXPECT_EQ(
      Value(1.0).EncodeMemcomparableFormat(),
      std::string({'\3', '\xbf', '\xf0', '\0', '\0', '\0', '\0', '\0', '\0'}));
  EXPECT_EQ(Value(0.0).EncodeMemcomparableFormat(),
            std::string(
                {'\3', '\x80', '\x00', '\0', '\0', '\0', '\0', '\0', '\x00'}));

  EXPECT_EQ(Value(-1.0).EncodeMemcomparableFormat(),
            std::string({'\3', '\x40', '\x0F', '\xff', '\xff', '\xff', '\xff',
                         '\xff', '\xff'}));
}

TEST(ValueTest, MemcomparableOrderDouble) {
  MemcomparableFormatEncodeTest({Value(1.0), Value(2.0), Value(3.0)});
  MemcomparableFormatEncodeTest({Value(-1.0), Value(-2.0), Value(-3.0)});
  MemcomparableFormatEncodeTest({Value(-1.0), Value(0.0), Value(1.0)});
  MemcomparableFormatEncodeTest({Value(std::numeric_limits<double>::max()),
                                 Value(std::numeric_limits<double>::min()),
                                 Value(-1.0), Value(0.0), Value(1.0)});
}

void EncodeDecodeTest(const Value& v) {
  std::string encoded = v.EncodeMemcomparableFormat();
  Value another;
  const char* src = encoded.c_str();

  another.DecodeMemcomparableFormat(src);
  ASSERT_EQ(v, another);
}

TEST(ValueTest, EncodeDecodeInt) {
  EncodeDecodeTest(Value(std::numeric_limits<int64_t>::max()));
  EncodeDecodeTest(Value(12));
  EncodeDecodeTest(Value(0));
  EncodeDecodeTest(Value(-1));
  EncodeDecodeTest(Value(std::numeric_limits<int64_t>::min()));
}

TEST(ValueTest, EncodeDecodeVarchar) {
  EncodeDecodeTest(Value("a"));
  EncodeDecodeTest(Value(""));
  EncodeDecodeTest(Value("hello"));
  EncodeDecodeTest(Value("A bit long string"));
  EncodeDecodeTest(Value("12345678"));
  EncodeDecodeTest(Value("\x50\x60\x70\x10\x11\x12\x80\x02\x01"));
  EncodeDecodeTest(Value("\x60\x70\x10\x11\x12\x80\x90\x08"));
  EncodeDecodeTest(Value("\x60\x70\x10\x11\x12\x90\x80\x08"));
  EncodeDecodeTest(Value("49p2u3po32u423pori2pouropiu"));
}

TEST(ValueTest, EncodeDecodeDouble) {
  EncodeDecodeTest(Value(std::numeric_limits<double>::max()));
  EncodeDecodeTest(Value(12.0));
  EncodeDecodeTest(Value(0.0));
  EncodeDecodeTest(Value(-1.0));
  EncodeDecodeTest(Value(std::numeric_limits<double>::min()));
}

void MemcomparableFormatDecodeTest(const std::vector<std::string>& input) {
  std::vector<std::string> values(input);
  std::sort(values.begin(), values.end());
  std::vector<Value> decoded;
  decoded.reserve(values.size());
  for (const auto& value : values) {
    Value v;
    v.DecodeMemcomparableFormat(value.c_str());
    decoded.push_back(v);
  }
  for (size_t i = 0; i < decoded.size(); ++i) {
    for (size_t j = i + 1; j < decoded.size(); ++j) {
      ASSERT_LT(decoded[i], decoded[j]);
    }
  }
}

TEST(ValueTest, MemComparableFormatDecodeInt) {
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  ASSERT_EQ(src.size(), 7);
  std::vector<std::string> targets;
  do {
    targets.emplace_back("\x01" + src + "\x01");
  } while (std::next_permutation(src.begin(), src.end()));
  MemcomparableFormatDecodeTest(targets);
}

TEST(ValueTest, MemComparableFormatDecodeVarchar) {
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  ASSERT_EQ(src.size(), 7);
  std::vector<std::string> targets;
  do {
    std::string v = "\x02" + src;
    v.push_back(0);
    v.push_back(static_cast<char>(v.size() - 1));
    ASSERT_EQ(v.size(), 10);
    targets.push_back(v);
  } while (std::next_permutation(src.begin(), src.end()));
  MemcomparableFormatDecodeTest(targets);
}

TEST(ValueTest, MemComparableFormatDecodeDouble) {
  std::string src = "\x60\x70\x80\x90\x10\x11\x12";
  std::vector<std::string> targets;
  do {
    targets.emplace_back("\x03" + src + "\x01");
  } while (std::next_permutation(src.begin(), src.end()));
  MemcomparableFormatDecodeTest(targets);
}

}  // namespace tinylamb
