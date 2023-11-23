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

TEST(LogMessage, Log) {
  LOG(FATAL) << "FATAL";
  LOG(ERROR) << "ERROR";
  LOG(ALERT) << "ALERT";
  LOG(WARN) << "WARN";
  LOG(NOTICE) << "NOTICE";
  LOG(INFO) << "INFO";
  LOG(USER) << "USER";
  LOG(DEBUG) << "DEBUG";
  LOG(TRACE) << "TRACE";
}