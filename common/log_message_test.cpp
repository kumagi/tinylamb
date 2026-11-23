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

#include "common/log_message.hpp"

#include <gtest/gtest.h>

#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {
// TINYLAMB_LOG_LEVEL may raise the process threshold above TRACE and filter
// severities these tests assert are emitted. Pin the level for the test's
// duration and restore it afterwards so the ambient environment cannot turn
// the assertions red. The threshold is process-global, so two overlapping
// scopes must not interleave their writes: hold a process-wide lock for the
// whole scope (not just the constructor) to serialize them.
std::mutex& TraceLogLevelMutex() {
  static std::mutex mutex;
  return mutex;
}

class ScopedTraceLogLevel {
 public:
  ScopedTraceLogLevel()
      : lock_(TraceLogLevelMutex()), previous_(::tinylamb::detail::MinLogLevel()) {
    ::tinylamb::detail::SetMinLogLevel(TRACE);
  }
  ~ScopedTraceLogLevel() { ::tinylamb::detail::SetMinLogLevel(previous_); }

  ScopedTraceLogLevel(const ScopedTraceLogLevel&) = delete;
  ScopedTraceLogLevel& operator=(const ScopedTraceLogLevel&) = delete;

 private:
  std::lock_guard<std::mutex> lock_;
  ::LogLevel previous_;
};
}  // namespace

TEST(LogMessageTest, Log_WithSurvivableSeverities_StreamsWithoutError) {
  const ScopedTraceLogLevel trace_level;
  ::testing::internal::CaptureStderr();
  LOG(ERROR) << "sev-ERROR";
  LOG(ALERT) << "sev-ALERT";
  LOG(WARN) << "sev-WARN";
  LOG(NOTICE) << "sev-NOTICE";
  LOG(INFO) << "sev-INFO";
  LOG(USER) << "sev-USER";
  LOG(DEBUG) << "sev-DEBUG";
  LOG(TRACE) << "sev-TRACE";
  const std::string output = ::testing::internal::GetCapturedStderr();
  for (const char* sev :
       {"ERROR", "ALERT", "WARN", "NOTICE", "INFO", "USER", "DEBUG", "TRACE"}) {
    EXPECT_NE(output.find(std::string("sev-") + sev), std::string::npos)
        << "LOG(" << sev << ") produced no stderr output";
  }
}

TEST(LogMessageTest, Log_WithFatalSeverity_AbortsProcess) {
  ASSERT_DEATH({ LOG(FATAL) << "FATAL"; }, "FATAL");
}

TEST(LogMessageTest, UnknownSeverityReportsTheMessageWithoutAborting) {
  const ScopedTraceLogLevel trace_level;
  ::testing::internal::CaptureStderr();
  LogMessage(static_cast<LogLevel>(65535), __FILE__, __LINE__, __func__, true)
          .stream()
      << "unrecognized-severity-message";
  const std::string output = ::testing::internal::GetCapturedStderr();
  EXPECT_NE(output.find("UNKNOWN LOG LEVEL"), std::string::npos);
  EXPECT_NE(output.find("unrecognized-severity-message"), std::string::npos);
}

TEST(LogMessageTest, AuditConcurrentMessagesStayOnOneLine) {
  const ScopedTraceLogLevel trace_level;
  ::testing::internal::CaptureStderr();
  std::vector<std::thread> workers;
  workers.reserve(8);
  for (int worker = 0; worker < 8; ++worker) {
    workers.emplace_back([worker] {
      const std::string marker(256, static_cast<char>('A' + worker));
      for (int line = 0; line < 100; ++line) {
        LOG(INFO) << "audit-begin:" << marker << ":audit-end";
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  std::istringstream output(::testing::internal::GetCapturedStderr());
  std::string line;
  size_t count = 0;
  while (std::getline(output, line)) {
    const auto begin = line.find("audit-begin:");
    ASSERT_NE(begin, std::string::npos);
    const auto end = line.find(":audit-end", begin);
    ASSERT_NE(end, std::string::npos);
    const auto marker = line.substr(begin + 12, end - begin - 12);
    ASSERT_EQ(marker.size(), 256U);
    EXPECT_EQ(marker, std::string(256, marker.front()));
    EXPECT_EQ(line.find("audit-begin:", begin + 1), std::string::npos);
    ++count;
  }
  EXPECT_EQ(count, 800U);
}
