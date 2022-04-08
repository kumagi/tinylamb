//
// Created by kumagi on 2022/03/16.
//
#include <thread>

#include "gtest/gtest.h"
#include "page/row_page_test.hpp"

namespace tinylamb {

constexpr int kThreads = 8;
TEST_F(RowPageTest, InsertInsert) {
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([this]() {
      for (int j = 0; j < 100; ++j) {
        EXPECT_TRUE(InsertRow(RandomString()));
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(RowPageTest, DISABLED_InsertUpdate) {
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int j = 0; j < 100; ++j) {
    EXPECT_TRUE(InsertRow(RandomString()));
  }
  for (int i = 0; i < kThreads / 2; ++i) {
    threads.emplace_back([this]() {
      for (int j = 0; j < 100; ++j) {
        EXPECT_TRUE(InsertRow(RandomString()));
      }
    });
  }
  for (int i = 0; i < kThreads / 2; ++i) {
    threads.emplace_back([&]() {
      unsigned int seed = i;
      for (int j = 0; j < 100; ++j) {
        UpdateRow(rand_r(&seed) % 100, RandomString());
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(RowPageTest, UpdateUpdate) {
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  thread_local std::random_device seed_gen;
  thread_local std::mt19937 engine(seed_gen());
  while (InsertRow(RandomString(engine() % 64)))
    ;
  size_t rows = GetRowCount();
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < 100; ++j) {
        UpdateRow(engine() % rows, RandomString(engine() % 64));
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

}  // namespace tinylamb