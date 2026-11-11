/**
 * Copyright 2026 KUMAZAKI Hiroki
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

// Contract tests for common/platform: every expectation here must hold on
// both Linux (epoll, eventfd, accept4, tzdb) and macOS (kqueue, pipe,
// fcntl, CFTimeZone).

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <thread>

#include "common/platform/cpu_affinity.hpp"
#include "common/platform/endian.hpp"
#include "common/platform/event_poller.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/os.hpp"
#include "common/platform/process.hpp"
#include "common/platform/socket.hpp"
#include "common/platform/synced_stream.hpp"
#include "common/platform/time_zone.hpp"
#include "gtest/gtest.h"

namespace tinylamb::platform {
#ifdef __SIZEOF_INT128__
TEST(Int128ConversionAuditTest, SignedAndUnsignedBoundarySaturation) {
  const unsigned __int128 sign_bit = static_cast<unsigned __int128>(1) << 127;
  const auto maximum = static_cast<__int128>(sign_bit - 1);
  const __int128 minimum = -maximum - 1;
  EXPECT_EQ(SaturatingDoubleToInt128(-0x1p127), minimum);
  EXPECT_EQ(SaturatingDoubleToInt128(-0x1p128), minimum);
  EXPECT_EQ(SaturatingDoubleToInt128(0x1p127), maximum);
  EXPECT_EQ(SaturatingDoubleToInt128(0x1p128), maximum);
  EXPECT_EQ(SaturatingDoubleToInt128(-42.75), -42);
  EXPECT_EQ(SaturatingDoubleToInt128(42.75), 42);
  EXPECT_EQ(DoubleToUInt128Magnitude(0x1p127), sign_bit);
  EXPECT_EQ(DoubleToUInt128Magnitude(0x1.8p127), sign_bit + (sign_bit >> 1));
  EXPECT_EQ(DoubleToUInt128Magnitude(0x1p128),
            ~static_cast<unsigned __int128>(0));
  EXPECT_EQ(SaturatingDoubleToInt128(std::numeric_limits<double>::quiet_NaN()),
            0);
  EXPECT_EQ(SaturatingDoubleToInt128(-std::numeric_limits<double>::infinity()),
            minimum);
}
#endif

TEST(SoftExtendedAuditTest,
     IntegralConversionsSaturateWithoutUndefinedBehavior) {
  EXPECT_EQ(static_cast<int64_t>(SoftExtended(1e100)), INT64_MAX);
  EXPECT_EQ(static_cast<int64_t>(SoftExtended(-1e100)), INT64_MIN);
  EXPECT_EQ(static_cast<uint64_t>(SoftExtended(1e100)), UINT64_MAX);
  EXPECT_EQ(static_cast<uint64_t>(SoftExtended(-1e100)), 0U);
  EXPECT_EQ(static_cast<int64_t>(SoftExtended(INT64_MAX)), INT64_MAX);
  EXPECT_EQ(static_cast<int64_t>(SoftExtended(INT64_MIN)), INT64_MIN);
  EXPECT_EQ(static_cast<uint64_t>(SoftExtended(UINT64_MAX)), UINT64_MAX);
  EXPECT_EQ(static_cast<int8_t>(SoftExtended(128)), INT8_MAX);
  EXPECT_EQ(static_cast<int8_t>(SoftExtended(-129)), INT8_MIN);
  EXPECT_EQ(static_cast<uint8_t>(SoftExtended(-1)), 0);
  EXPECT_EQ(static_cast<int>(SoftExtended(-3.75)), -3);
  EXPECT_EQ(static_cast<int>(SoftExtended(0.75)), 0);
  EXPECT_EQ(
      static_cast<int>(SoftExtended(std::numeric_limits<double>::quiet_NaN())),
      0);
}

#ifndef TINYLAMB_OS_WINDOWS
TEST(ProcessAuditTest, SpawnOverridesEnvironmentOnlyInChild) {
  ASSERT_EQ(SetEnv("TINYLAMB_AUDIT_SPAWN_ENV", "parent", 1), 0);
  std::string error;
  EXPECT_EQ(RunProcess("/bin/sh",
                       {"-c",
                        "test \"$TINYLAMB_AUDIT_SPAWN_ENV\" = 'child value' && "
                        "test \"$1\" = 'argument with spaces'",
                        "audit", "argument with spaces"},
                       {{"TINYLAMB_AUDIT_SPAWN_ENV", "child value"}}, &error),
            0)
      << error;
  EXPECT_STREQ(std::getenv("TINYLAMB_AUDIT_SPAWN_ENV"), "parent");
  EXPECT_EQ(RunProcess("/bin/sh", {"-c", "exit 23"}, {}, &error), 23);
  EXPECT_EQ(RunProcess("/path/that/does/not/exist", {}, {}, &error), -1);
  EXPECT_FALSE(error.empty());
  EXPECT_EQ(UnsetEnv("TINYLAMB_AUDIT_SPAWN_ENV"), 0);
}
#endif

namespace {

uint32_t WaitFor(EventPoller& poller, int fd, int timeout_ms = 1000) {
  std::array<PollResult, 8> results{};
  const int count = poller.Wait(results, timeout_ms);
  uint32_t events = 0;
  int seen = 0;
  for (int i = 0; i < count; ++i) {
    if (results[static_cast<size_t>(i)].fd == fd) {
      events |= results[static_cast<size_t>(i)].events;
      ++seen;
    }
  }
  EXPECT_LE(seen, 1) << "one result per descriptor per Wait";
  return events;
}

// A connected loopback TCP pair: {client, server-side accepted}.
std::array<int, 2> TcpPair() {
  const int listener = OpenSocket(AF_INET, SOCK_STREAM, 0, {});
  EXPECT_GE(listener, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  EXPECT_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
            0);
  EXPECT_EQ(::listen(listener, 1), 0);
  socklen_t size = sizeof(addr);
  EXPECT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &size),
            0);
  const int client = OpenSocket(AF_INET, SOCK_STREAM, 0, {});
  EXPECT_EQ(::connect(client, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
            0);
  const int server =
      AcceptSocket(listener, {.nonblocking = true, .close_on_exec = true});
  EXPECT_GE(server, 0);
  CloseSocket(listener);
  return {client, server};
}

TEST(PlatformEndianTest, BigEndianRoundTrip) {
  const uint64_t value = 0x0102030405060708ULL;
  const uint64_t be = htobe64(value);
  const auto* bytes = reinterpret_cast<const unsigned char*>(&be);
  EXPECT_EQ(bytes[0], 0x01);
  EXPECT_EQ(bytes[7], 0x08);
  EXPECT_EQ(be64toh(be), value);
  EXPECT_EQ(be32toh(htobe32(0xA1B2C3D4U)), 0xA1B2C3D4U);
  EXPECT_EQ(le64toh(htole64(value)), value);
}

TEST(PlatformFileIoTest, DataSyncAndPreallocate) {
  const std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      ("platform_file_io_" + std::to_string(GetPid()));
  const int fd = OpenFile(
      path, {.mode = OpenMode::kReadWrite, .create = true, .truncate = true},
      0644);
  ASSERT_GE(fd, 0);
  EXPECT_EQ(Preallocate(fd, 8192), 0);
  ASSERT_EQ(Write(fd, "abc", 3), 3);
  EXPECT_EQ(DataSync(fd), 0);
  AdviseDontNeed(fd, 0, 3);  // best effort; must not crash
  CloseFile(fd);
  EXPECT_EQ(DataSync(fd), -1);  // closed descriptor reports failure
  std::filesystem::remove(path);
}

TEST(PlatformSocketTest, FlagsAreApplied) {
  const int blocking = OpenSocket(AF_INET, SOCK_STREAM, 0, {});
  ASSERT_GE(blocking, 0);
  EXPECT_TRUE(IsCloseOnExec(blocking));
  EXPECT_FALSE(IsNonBlocking(blocking));
  CloseSocket(blocking);

  const int nonblocking = OpenSocket(
      AF_INET, SOCK_STREAM, 0, {.nonblocking = true, .close_on_exec = false});
  ASSERT_GE(nonblocking, 0);
  EXPECT_FALSE(IsCloseOnExec(nonblocking));
  EXPECT_TRUE(IsNonBlocking(nonblocking));
  CloseSocket(nonblocking);

  std::array<int, 2> fds{-1, -1};
  ASSERT_EQ(OpenPipe(fds, {.nonblocking = true, .close_on_exec = true}), 0);
  for (int fd : fds) {
    EXPECT_TRUE(IsCloseOnExec(fd));
    EXPECT_TRUE(IsNonBlocking(fd));
    CloseSocket(fd);
  }
}

TEST(PlatformSocketTest, AcceptAppliesFlagsAndSendSkipsSigPipe) {
  auto [client, server] = TcpPair();
  EXPECT_TRUE(IsCloseOnExec(server));
  EXPECT_TRUE(IsNonBlocking(server));
  CloseSocket(server);
  // Writing into a connection the peer closed must fail with EPIPE/ECONNRESET
  // instead of killing the process with SIGPIPE.
  ssize_t sent = 0;
  for (int i = 0; i < 100 && sent >= 0; ++i) {
    sent = SendNoSignal(client, "x", 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_LT(sent, 0);
  CloseSocket(client);
}

TEST(PlatformEventPollerTest, EmptyPollerWaitsForRegistrationAndReadiness) {
  EventPoller poller;
  Waker waker;
  ASSERT_TRUE(poller.Open());
  ASSERT_TRUE(waker.Open());
  std::promise<void> entered;
  auto started = entered.get_future();
  auto waiting = std::async(std::launch::async, [&] {
    std::array<PollResult, 1> results{};
    entered.set_value();
    return poller.Wait(results, -1);
  });
  started.wait();
  EXPECT_EQ(waiting.wait_for(std::chrono::milliseconds(25)),
            std::future_status::timeout);
  EXPECT_TRUE(poller.Add(waker.ReadFd(), kPollReadable));
  waker.Wake();
  EXPECT_EQ(waiting.get(), 1);
}

TEST(PlatformEventPollerTest, LevelTriggeredReadWriteAndPeerClose) {
  EventPoller poller;
  ASSERT_TRUE(poller.Open());
  auto [client, server] = TcpPair();

  ASSERT_TRUE(poller.Add(server, kPollReadable | kPollPeerClosed));
  EXPECT_EQ(WaitFor(poller, server, 50), 0U);  // nothing to read yet

  ASSERT_EQ(SendNoSignal(client, "hi", 2), 2);
  EXPECT_NE(WaitFor(poller, server) & kPollReadable, 0U);
  // Level-triggered: still readable until drained.
  EXPECT_NE(WaitFor(poller, server) & kPollReadable, 0U);
  std::array<char, 8> buffer{};
  ASSERT_EQ(RecvSocket(server, buffer.data(), buffer.size()), 2);
  EXPECT_EQ(WaitFor(poller, server, 50), 0U);

  // Read and write readiness of one descriptor come back as one result.
  ASSERT_EQ(SendNoSignal(client, "x", 1), 1);
  // Let the byte arrive before asking for write readiness too, which is
  // immediate and would otherwise win the race on a non-synchronous loopback.
  EXPECT_NE(WaitFor(poller, server) & kPollReadable, 0U);
  ASSERT_TRUE(
      poller.Modify(server, kPollReadable | kPollPeerClosed | kPollWritable));
  const uint32_t both = WaitFor(poller, server);
  EXPECT_NE(both & kPollReadable, 0U);
  EXPECT_NE(both & kPollWritable, 0U);
  ASSERT_EQ(RecvSocket(server, buffer.data(), buffer.size()), 1);

  // Dropping read interest stops read reports.
  ASSERT_TRUE(poller.Modify(server, kPollWritable));
  ASSERT_EQ(SendNoSignal(client, "y", 1), 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const uint32_t write_only = WaitFor(poller, server);
  EXPECT_NE(write_only & kPollWritable, 0U);
  EXPECT_EQ(write_only & kPollReadable, 0U);
  ASSERT_EQ(RecvSocket(server, buffer.data(), buffer.size()), 1);

  // Peer half-close is reported as readable + peer-closed.
  ASSERT_TRUE(poller.Modify(server, kPollReadable | kPollPeerClosed));
  ASSERT_EQ(ShutdownSocket(client, SHUT_WR), 0);
  const uint32_t closed = WaitFor(poller, server);
  EXPECT_NE(closed & kPollReadable, 0U);
  EXPECT_NE(closed & kPollPeerClosed, 0U);

  ASSERT_TRUE(poller.Remove(server));
  EXPECT_EQ(WaitFor(poller, server, 50), 0U);
  EXPECT_FALSE(poller.Remove(server));
  CloseSocket(client);
  CloseSocket(server);
}

TEST(PlatformEventPollerTest, WakerWakesWaitFromAnotherThread) {
  EventPoller poller;
  Waker waker;
  ASSERT_TRUE(poller.Open());
  ASSERT_TRUE(waker.Open());
  ASSERT_TRUE(poller.Add(waker.ReadFd(), kPollReadable));
  EXPECT_EQ(WaitFor(poller, waker.ReadFd(), 20), 0U);

  std::thread wake_thread([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    for (int i = 0; i < 1000; ++i) {
      waker.Wake();  // coalesces, never blocks
    }
  });
  EXPECT_NE(WaitFor(poller, waker.ReadFd(), 5000) & kPollReadable, 0U);
  wake_thread.join();
  waker.Drain();
  EXPECT_EQ(WaitFor(poller, waker.ReadFd(), 20), 0U);
}

TEST(PlatformCpuAffinityTest, CurrentThreadMatchesSupport) {
  const std::optional<CpuSet> current = CpuSet::CurrentThread();
  EXPECT_EQ(current.has_value(), CpuSet::Supported());
  if (!current.has_value()) {
    return;
  }
  EXPECT_GE(current->Count(), 1);
  const int first = current->First();
  ASSERT_GE(first, 0);
  ASSERT_TRUE(CpuSet::Single(first).ApplyToCurrentThread());
  const std::optional<CpuSet> pinned = CpuSet::CurrentThread();
  ASSERT_TRUE(pinned.has_value());
  if (!pinned.has_value()) {
    return;
  }
  EXPECT_EQ(pinned->Count(), 1);
  ASSERT_TRUE(current->ApplyToCurrentThread());
  const std::optional<CpuSet> restored = CpuSet::CurrentThread();
  ASSERT_TRUE(restored.has_value());
  if (!restored.has_value()) {
    return;
  }
  EXPECT_TRUE(*restored == *current);
}

TEST(PlatformTimeZoneTest, KnownZonesAreExactAndCaseSensitive) {
  EXPECT_TRUE(IsKnownTimeZone("Asia/Tokyo"));
  EXPECT_TRUE(IsKnownTimeZone("America/New_York"));
  EXPECT_TRUE(IsKnownTimeZone("UTC"));
  EXPECT_TRUE(IsKnownTimeZone("Etc/UTC"));
  EXPECT_TRUE(IsKnownTimeZone("Pacific/Chatham"));
  EXPECT_FALSE(IsKnownTimeZone("asia/tokyo"));
  EXPECT_FALSE(IsKnownTimeZone("Foo/Bar"));
  EXPECT_FALSE(IsKnownTimeZone(""));
  EXPECT_FALSE(IsKnownTimeZone("../etc/passwd"));
  EXPECT_FALSE(TimeZoneOffsetAtLocalTime("Foo/Bar", 2024, 1, 1, 0, 0, 0));
}

TEST(PlatformTimeZoneTest, OffsetsFollowTzdbLocalInfoFirst) {
  auto offset = [](const char* zone, int y, unsigned mo, unsigned d, int h,
                   int mi) {
    return TimeZoneOffsetAtLocalTime(zone, y, mo, d, h, mi, 0).value_or(-1);
  };
  EXPECT_EQ(offset("Asia/Tokyo", 2024, 6, 1, 12, 0), 9 * 3600);
  EXPECT_EQ(offset("UTC", 2024, 6, 1, 12, 0), 0);
  EXPECT_EQ(offset("Asia/Kolkata", 2024, 6, 1, 12, 0), 19800);
  EXPECT_EQ(offset("America/New_York", 2024, 1, 15, 12, 0), -5 * 3600);
  EXPECT_EQ(offset("America/New_York", 2024, 7, 15, 12, 0), -4 * 3600);
  // Spring-forward gap: the offset before the gap.
  EXPECT_EQ(offset("America/New_York", 2024, 3, 10, 2, 30), -5 * 3600);
  EXPECT_EQ(offset("America/New_York", 2024, 3, 10, 3, 30), -4 * 3600);
  // Fall-back overlap: the earlier (daylight) offset.
  EXPECT_EQ(offset("America/New_York", 2024, 11, 3, 1, 30), -4 * 3600);
  EXPECT_EQ(offset("America/New_York", 2024, 11, 3, 2, 30), -5 * 3600);
  // Far future follows the zone's standing rule.
  EXPECT_EQ(offset("America/New_York", 2060, 7, 1, 0, 0), -4 * 3600);
  // Half-hour DST shift, overlap: earlier offset (+11:00).
  EXPECT_EQ(offset("Australia/Lord_Howe", 2024, 4, 7, 1, 45), 11 * 3600);
}

TEST(PlatformSyncedStreamTest, FlushesOnDestruction) {
  std::ostringstream target;
  {
    SyncedOStream(target) << "a=" << 1 << '\n';
    SyncedOStream(target) << "b=" << 2 << '\n';
  }
  EXPECT_EQ(target.str(), "a=1\nb=2\n");
}

}  // namespace
}  // namespace tinylamb::platform
