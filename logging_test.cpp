#include "gtest/gtest.h"
#include "logging.hpp"

namespace tinylamb {

class LoggingTest : public ::testing::Test {
 protected:
  static constexpr char kFileName[] = "logger_test.log";
  void SetUp() override {
    Recover();
  }

  void Recover() {
    l_ = std::make_unique<Logging>(kFileName);
  }

  void TearDown() override {
    std::remove(kFileName);
  }
  std::unique_ptr<Logging> l_;
};

TEST_F(LoggingTest, DoNothing) {
}

TEST_F(LoggingTest, AppendOne) {
  EXPECT_TRUE(l_->Append(1, "foo bar"));
}

TEST_F(LoggingTest, EmptyRead) {
  std::string content;
  EXPECT_FALSE(l_->Read(1, &content));
}

TEST_F(LoggingTest, ReadOne) {
  ASSERT_TRUE(l_->Append(1, "foo bar"));
  std::string content;
  EXPECT_TRUE(l_->Read(1, &content));
  EXPECT_EQ(content, "foo bar");
}

TEST_F(LoggingTest, NotFound) {
  ASSERT_TRUE(l_->Append(10, "foo bar"));
  std::string content;
  EXPECT_FALSE(l_->Read(3, &content));
  EXPECT_FALSE(l_->Read(2000, &content));
}

TEST_F(LoggingTest, AppendMany) {
  for (int i = 0; i < 1000; ++i) {
    EXPECT_TRUE(l_->Append(i, "foo bar"));
  }
}

TEST_F(LoggingTest, ReadMany) {
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(l_->Append(i, "payload" + std::to_string(i)));
  }
  for (int i = 0; i < 1000; ++i) {
    std::string content;
    ASSERT_TRUE(l_->Read(i, &content));
    ASSERT_EQ(content, "payload" + std::to_string(i));
  }
}

TEST_F(LoggingTest, ReadManyNotFound) {
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(l_->Append(i * 2, "payload" + std::to_string(i)));
  }
  for (int i = 0; i < 1000; ++i) {
    std::string content;
    ASSERT_FALSE(l_->Read(i * 2 + 1, &content));
  }
}

TEST_F(LoggingTest, RecoverOne) {
  ASSERT_TRUE(l_->Append(1, "foo bar"));
  Recover();
  std::string content;
  EXPECT_TRUE(l_->Read(1, &content));
  EXPECT_EQ(content, "foo bar");
}

TEST_F(LoggingTest, RecoverMan) {
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(l_->Append(i, "payload" + std::to_string(i)));
  }
  Recover();
  for (int i = 0; i < 1000; ++i) {
    std::string content;
    ASSERT_TRUE(l_->Read(i, &content));
    ASSERT_EQ(content, "payload" + std::to_string(i));
  }
}

} // namespace tinylamb