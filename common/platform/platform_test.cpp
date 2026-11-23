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

// Contract tests for common/platform: expectations apply on Linux (epoll,
// eventfd, accept4, tzdb), macOS (kqueue, pipe, fcntl, CFTimeZone) and
// Windows (WSAPoll, Winsock SOCKET); POSIX-only cases are guarded out there.

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/platform/cpu_affinity.hpp"
#include "common/platform/endian.hpp"
#include "common/platform/event_poller.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/hash.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/os.hpp"
#include "common/platform/process.hpp"
#include "common/platform/random.hpp"
#include "common/platform/socket.hpp"
#include "common/platform/synced_stream.hpp"
#include "common/platform/time_format.hpp"
#include "common/platform/time_zone.hpp"
#include "common/random_string.hpp"
#include "gtest/gtest.h"

#ifndef TINYLAMB_OS_WINDOWS
#include <fcntl.h>
#include <unistd.h>
#endif

namespace tinylamb::platform {

// Socket descriptors must survive a round trip without losing any bits: the
// -1 error sentinel needs a signed type, and on Win64 a Winsock SOCKET is a
// 64-bit UINT_PTR that must not be narrowed to int (bug 10586).
static_assert(static_cast<SocketFd>(-1) < 0,
              "SocketFd must be signed for the -1 error sentinel");
#ifdef TINYLAMB_OS_WINDOWS
static_assert(sizeof(SocketFd) >= sizeof(SOCKET),
              "SocketFd must hold a full Winsock SOCKET without truncation");
static_assert(IsValidSocketFd(std::numeric_limits<SocketFd>::min()));
static_assert(!IsValidSocketFd(kInvalidSocketFd));
#endif

TEST(PlatformTimeTest, TwoDigitYearUsesPosixPivot) {
  for (const auto& [text, year] : std::array<std::pair<const char*, int>, 4>{
           {{"00", 100}, {"68", 168}, {"69", 69}, {"99", 99}}}) {
    std::tm parsed{};
    const char* end = platform::ParseTime(text, "%y", &parsed);
    ASSERT_NE(end, nullptr);
    EXPECT_EQ(*end, '\0');
    EXPECT_EQ(parsed.tm_year, year) << text;
  }
}

TEST(PlatformTimeTest, LocalTimeReportsUnrepresentableEpoch) {
  std::tm tm{};
  EXPECT_TRUE(platform::LocalTime(0, &tm));
  EXPECT_FALSE(
      platform::LocalTime(std::numeric_limits<std::time_t>::max(), &tm));
  if constexpr (std::numeric_limits<std::time_t>::is_signed) {
    EXPECT_FALSE(
        platform::LocalTime(std::numeric_limits<std::time_t>::min(), &tm));
  }
}

#ifdef __SIZEOF_INT128__
TEST(Int128ConversionAuditTest, SignedAndUnsignedBoundarySaturation) {
  const UInt128 sign_bit = static_cast<UInt128>(1) << 127;
  const auto maximum = static_cast<Int128>(sign_bit - 1);
  const Int128 minimum = -maximum - 1;
  EXPECT_EQ(SaturatingDoubleToInt128(-0x1p127), minimum);
  EXPECT_EQ(SaturatingDoubleToInt128(-0x1p128), minimum);
  EXPECT_EQ(SaturatingDoubleToInt128(0x1p127), maximum);
  EXPECT_EQ(SaturatingDoubleToInt128(0x1p128), maximum);
  EXPECT_EQ(SaturatingDoubleToInt128(-42.75), -42);
  EXPECT_EQ(SaturatingDoubleToInt128(42.75), 42);
  EXPECT_EQ(DoubleToUInt128Magnitude(0x1p127), sign_bit);
  EXPECT_EQ(DoubleToUInt128Magnitude(0x1.8p127), sign_bit + (sign_bit >> 1));
  EXPECT_EQ(DoubleToUInt128Magnitude(0x1p128), ~static_cast<UInt128>(0));
  EXPECT_EQ(SaturatingDoubleToInt128(std::numeric_limits<double>::quiet_NaN()),
            0);
  EXPECT_EQ(SaturatingDoubleToInt128(-std::numeric_limits<double>::infinity()),
            minimum);
}
#endif

TEST(PlatformRandomTest, UniformIntAndShuffleMatchLibstdcxxVectors) {
  std::mt19937 uniform_engine(12345);
  for (int expected : {59, 55, 8, -7, -2, -14, -1, 50}) {
    EXPECT_EQ(UniformInt(uniform_engine, -17, 64), expected);
  }
  std::mt19937 shuffle_engine(12345);
  std::array values{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  Shuffle(values.begin(), values.end(), shuffle_engine);
  EXPECT_EQ(values, (std::array{6, 8, 3, 5, 1, 2, 9, 7, 4, 0}));
}

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

TEST(SoftExtendedAuditTest, BoolConversionMatchesArithmeticSemantics) {
  // bool is a C++ arithmetic conversion (non-zero -> true), not a truncating
  // integral conversion: -0.5 and huge negatives must not become false.
  EXPECT_TRUE(static_cast<bool>(SoftExtended(-1e100)));
  EXPECT_TRUE(static_cast<bool>(SoftExtended(1e100)));
  EXPECT_TRUE(static_cast<bool>(SoftExtended(-0.5)));
  EXPECT_TRUE(static_cast<bool>(SoftExtended(0.5)));
  EXPECT_TRUE(
      static_cast<bool>(SoftExtended(std::numeric_limits<double>::infinity())));
  EXPECT_TRUE(static_cast<bool>(
      SoftExtended(std::numeric_limits<double>::quiet_NaN())));
  EXPECT_FALSE(static_cast<bool>(SoftExtended(0.0)));
  EXPECT_FALSE(static_cast<bool>(SoftExtended(-0.0)));
}

TEST(SoftExtendedAuditTest, ArithmeticRetainsLowBitsAndRoundsEven) {
  const SoftExtended two_to_53(uint64_t{1} << 53);
  const SoftExtended one(1);
  EXPECT_EQ(static_cast<int64_t>((two_to_53 + one) - two_to_53), 1);
  EXPECT_EQ(static_cast<double>(two_to_53 + one),
            static_cast<double>(two_to_53));
  EXPECT_EQ(static_cast<double>(SoftExtended(1.5) * SoftExtended(3.0)), 4.5);
  EXPECT_EQ(static_cast<double>(SoftExtended(9.0) / SoftExtended(4.0)), 2.25);
}

TEST(ProcessAuditTest, InvalidOverrideLeavesEnvironmentUnchanged) {
  constexpr char kEnv[] = "TINYLAMB_AUDIT_INVALID_OVERRIDE";
  ASSERT_EQ(SetEnv(kEnv, "parent", 1), 0);
  std::string error;
  EXPECT_EQ(RunProcess("unused_program", {},
                       {{kEnv, "child"}, {"bad=key", "value"}}, &error),
            -1);
  EXPECT_EQ(error, "invalid environment override");
  EXPECT_STREQ(std::getenv(kEnv), "parent");
  EXPECT_EQ(UnsetEnv(kEnv), 0);
}

#ifdef TINYLAMB_OS_WINDOWS
TEST(ProcessAuditTest, ConcurrentChildrenReceiveIndependentEnvironmentBlocks) {
  constexpr char kEnv[] = "TINYLAMB_AUDIT_CHILD_ENV";
  const char* shell = std::getenv("ComSpec");
  ASSERT_NE(shell, nullptr);
  const std::filesystem::path executable(shell);
  ASSERT_EQ(SetEnv(kEnv, "parent", 1), 0);
  const auto launch = [&](std::string value) {
    std::string error;
    const std::string command = std::string("if %") + kEnv + "%==" + value +
                                " (exit /b 23) else (exit /b 1)";
    const int code = RunProcess(executable, {"/d", "/s", "/c", command},
                                {{"tinylamb_audit_child_env", value}}, &error);
    EXPECT_EQ(code, 23) << error;
  };
  auto first = std::async(std::launch::async, launch, "first");
  auto second = std::async(std::launch::async, launch, "second");
  first.get();
  second.get();
  EXPECT_STREQ(std::getenv(kEnv), "parent");
  EXPECT_EQ(UnsetEnv(kEnv), 0);
  EXPECT_EQ(std::getenv(kEnv), nullptr);
}
#else
TEST(ProcessAuditTest, SpawnOverridesEnvironmentOnlyInChild) {
  ASSERT_EQ(SetEnv("TINYLAMB_AUDIT_SPAWN_ENV", "parent", 1), 0);
  // A failed EXPECT below still unwinds this guard: the variable is
  // process-wide, so an early ASSERT/return must not leak it to later tests.
  struct EnvGuard {
    ~EnvGuard() { EXPECT_EQ(UnsetEnv("TINYLAMB_AUDIT_SPAWN_ENV"), 0); }
  } env_guard;
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
}
#if defined(__APPLE__) && defined(POSIX_SPAWN_CLOEXEC_DEFAULT)
TEST(ProcessAuditTest, SpawnInheritsStandardStreamsAndClosesOtherDescriptors) {
  const int fd = ::fcntl(STDOUT_FILENO, F_DUPFD, 50);
  ASSERT_GE(fd, 50);
  struct FdGuard {
    int fd;
    ~FdGuard() {
      if (fd >= 0) {
        (void)::close(fd);
      }
    }
  } fd_guard{fd};
  std::string error;
  const int result = RunProcess(
      "/bin/sh",
      {"-c",
       "test -e /dev/fd/1 && test -e /dev/fd/2 && test ! -e \"/dev/fd/$1\"",
       "audit", std::to_string(fd)},
      {}, &error);
  EXPECT_EQ(result, 0) << error;
}
#endif
#endif

namespace {

uint32_t WaitFor(EventPoller& poller, SocketFd fd, int timeout_ms = 1000) {
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
std::optional<std::array<SocketFd, 2>> TcpPair() {
  const SocketFd listener = OpenSocket(AF_INET, SOCK_STREAM, 0, {});
  if (!IsValidSocketFd(listener)) {
    ADD_FAILURE() << "could not open loopback listener";
    return std::nullopt;
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ADD_FAILURE() << "could not bind loopback listener";
    CloseSocket(listener);
    return std::nullopt;
  }
  if (::listen(listener, 1) != 0) {
    ADD_FAILURE() << "could not listen on loopback socket";
    CloseSocket(listener);
    return std::nullopt;
  }
  socklen_t size = sizeof(addr);
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &size) != 0) {
    ADD_FAILURE() << "could not read loopback listener address";
    CloseSocket(listener);
    return std::nullopt;
  }
  const SocketFd client = OpenSocket(AF_INET, SOCK_STREAM, 0, {});
  if (!IsValidSocketFd(client)) {
    ADD_FAILURE() << "could not open loopback client";
    CloseSocket(listener);
    return std::nullopt;
  }
  if (::connect(client, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ADD_FAILURE() << "could not connect loopback client";
    CloseSocket(client);
    CloseSocket(listener);
    return std::nullopt;
  }
  const SocketFd server =
      AcceptSocket(listener, {.nonblocking = true, .close_on_exec = true});
  if (!IsValidSocketFd(server)) {
    ADD_FAILURE() << "could not accept loopback client";
    CloseSocket(client);
    CloseSocket(listener);
    return std::nullopt;
  }
  CloseSocket(listener);
  return std::array<SocketFd, 2>{client, server};
}

TEST(PlatformHashTest, StableHashBytesPinsWireByteOrder) {
  EXPECT_EQ(StableHashBytes(""), 0x553e93901e462a6eULL);
  EXPECT_EQ(StableHashBytes("abcdefgh"), 0x783db3e38db898bbULL);
  EXPECT_EQ(StableHashBytes("abcdefghijk"), 0x32f6faa7d12fbce5ULL);
}

TEST(PlatformEndianTest, BigEndianRoundTrip) {
  const uint64_t value = 0x0102030405060708ULL;
  const uint64_t be = htobe64(value);
  const auto* bytes = reinterpret_cast<const unsigned char*>(&be);
  // Memory image must be big-endian on every host: byte i holds hex digit
  // pair i of the original value. Checking all eight bytes keeps this
  // endian-independent (a host-order leak fails on either endianness).
  for (size_t i = 0; i < 8; ++i) {
    EXPECT_EQ(bytes[i],
              static_cast<unsigned char>((value >> (56 - 8 * i)) & 0xFF))
        << "byte " << i;
  }
  EXPECT_EQ(be64toh(be), value);
  EXPECT_EQ(be32toh(htobe32(0xA1B2C3D4U)), 0xA1B2C3D4U);
  EXPECT_EQ(le64toh(htole64(value)), value);
}

#ifdef __GLIBCXX__
// random.hpp's contract is bit-for-bit libstdc++ (GCC >= 11) reproduction so
// seeded fuzzers and pinned regression traces replay identically everywhere.
// Compare every draw against the real std:: facilities on identically-seeded
// engines: any drift by one output fails here instead of silently
// invalidating committed .test replays. Covers both shuffle paths (the
// two-positions-per-draw optimization and the single-draw fallback).
TEST(PlatformRandomTest, MatchesLibstdcxxDraws) {
  for (const uint32_t seed :
       {uint32_t{0}, uint32_t{1}, uint32_t{0xDEADBEEFU}}) {
    {
      std::mt19937 ours(seed);
      std::mt19937 theirs(seed);
      std::uniform_int_distribution<int64_t> dist_i64{-1000, 1000};
      std::uniform_int_distribution<uint32_t> dist_u32{0, 0xFFFFFFFEU};
      for (int i = 0; i < 64; ++i) {
        EXPECT_EQ(UniformInt(ours, int64_t{-1000}, int64_t{1000}),
                  dist_i64(theirs))
            << "seed=" << seed << " i=" << i;
        EXPECT_EQ(UniformInt(ours, uint32_t{0}, uint32_t{0xFFFFFFFEU}),
                  dist_u32(theirs))
            << "seed=" << seed << " i=" << i;
      }
    }
    for (const size_t n : {size_t{50}, size_t{70000}}) {
      std::vector<int> a(n), b(n);
      for (size_t i = 0; i < n; ++i) {
        a[i] = b[i] = static_cast<int>(i);
      }
      std::mt19937 ours(seed);
      std::mt19937 theirs(seed);
      Shuffle(a.begin(), a.end(), ours);
      std::shuffle(b.begin(), b.end(), theirs);
      EXPECT_EQ(a, b) << "seed=" << seed << " n=" << n;
    }
  }
}
#endif

TEST(PlatformFileIoTest, DataSyncAndPreallocate) {
  // PID alone is not unique across PID namespaces: ctest may run the same
  // binary in separate containers with the same PID, so add random entropy.
  const std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      ("platform_file_io_" + std::to_string(GetPid()) + "_" + RandomString());
  const std::filesystem::path empty_path =
      std::filesystem::temp_directory_path() /
      ("platform_empty_map_" + std::to_string(GetPid()) + "_" + RandomString());
  struct Cleanup {
    std::array<std::filesystem::path, 2> paths;
    std::array<int, 4> fds{-1, -1, -1, -1};
    ~Cleanup() {
      for (int& fd : fds) {
        if (fd >= 0) {
          CloseFile(fd);
        }
      }
      std::error_code ec;
      for (const auto& path : paths) {
        std::filesystem::remove(path, ec);
        ec.clear();
      }
    }
  } cleanup{.paths={path, empty_path}};
  const int fd = OpenFile(
      path, {.mode = OpenMode::kReadWrite, .create = true, .truncate = true},
      0644);
  cleanup.fds[0] = fd;
  ASSERT_GE(fd, 0);
  EXPECT_EQ(Preallocate(fd, 8192), 0);
  ASSERT_EQ(Write(fd, "abc", 3), 3);
  EXPECT_EQ(DataSync(fd), 0);
  AdviseDontNeed(fd, 0, 3);  // best effort; must not crash
  CloseFile(fd);
  cleanup.fds[0] = -1;
  const int readonly_fd = OpenFile(path, {});
  cleanup.fds[1] = readonly_fd;
  ASSERT_GE(readonly_fd, 0);
  EXPECT_EQ(DataSync(readonly_fd), 0);
  EXPECT_EQ(CloseFile(readonly_fd), 0);
  cleanup.fds[1] = -1;
  // -1 is never a valid descriptor; reusing the just-closed integer would
  // make this check depend on fd allocation and flake if it got reopened.
  EXPECT_EQ(DataSync(-1), -1);  // invalid descriptor reports failure
  // Also cover a closed-but-nonnegative descriptor: on Windows this drives
  // the _get_osfhandle() == -1 / EBADF conversion in DataSync(), which the
  // -1 case alone does not exercise. Open and close immediately so no other
  // operation in this single-threaded test can reuse the number in between.
  const int closed_fd = OpenFile(path, {.mode = OpenMode::kReadWrite}, 0644);
  cleanup.fds[2] = closed_fd;
  ASSERT_GE(closed_fd, 0);
  CloseFile(closed_fd);
  cleanup.fds[2] = -1;
  EXPECT_EQ(DataSync(closed_fd), -1);

  MemoryMap mapped;
  ASSERT_EQ(mapped.Open(path), 0);
  EXPECT_TRUE(mapped.IsValid());
  ASSERT_EQ(mapped.Size(), 8192U);
  ASSERT_NE(mapped.Data(), nullptr);
  EXPECT_EQ(mapped.Data()[0], 'a');
  EXPECT_EQ(mapped.Data()[1], 'b');
  EXPECT_EQ(mapped.Data()[2], 'c');
  mapped.AdviseSequential(0, mapped.Size());
  mapped.AdviseWillNeed(0, 128);
  mapped.AdviseRandom(0, mapped.Size());

  MemoryMap moved(std::move(mapped));
  // MemoryMap guarantees that moving releases the source handle.  //
  // NOLINT(clang-analyzer-cplusplus.Move)
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_FALSE(mapped.IsValid());
  EXPECT_TRUE(moved.IsValid());
  EXPECT_EQ(moved.Data()[0], 'a');
  moved.Close();
  EXPECT_FALSE(moved.IsValid());

  const int empty_fd = OpenFile(
      empty_path,
      {.mode = OpenMode::kReadWrite, .create = true, .truncate = true}, 0644);
  cleanup.fds[3] = empty_fd;
  ASSERT_GE(empty_fd, 0);
  CloseFile(empty_fd);
  cleanup.fds[3] = -1;
  MemoryMap empty_map;
  ASSERT_EQ(empty_map.Open(empty_path), 0);
  EXPECT_TRUE(empty_map.IsValid());
  EXPECT_EQ(empty_map.Size(), 0U);
  EXPECT_NE(empty_map.Data(), nullptr);
  empty_map.Close();
  EXPECT_TRUE(std::filesystem::remove(empty_path));

  MemoryMap missing;
  EXPECT_EQ(missing.Open(path.string() + ".nonexistent"), -1);
  EXPECT_FALSE(missing.IsValid());

  EXPECT_TRUE(std::filesystem::remove(path));
}

TEST(PlatformSocketTest, FlagsAreApplied) {
  const SocketFd blocking = OpenSocket(AF_INET, SOCK_STREAM, 0, {});
  ASSERT_TRUE(IsValidSocketFd(blocking));
  EXPECT_TRUE(IsCloseOnExec(blocking));
  EXPECT_FALSE(IsNonBlocking(blocking));
  CloseSocket(blocking);

  const SocketFd nonblocking = OpenSocket(
      AF_INET, SOCK_STREAM, 0, {.nonblocking = true, .close_on_exec = false});
  ASSERT_TRUE(IsValidSocketFd(nonblocking));
  EXPECT_FALSE(IsCloseOnExec(nonblocking));
  EXPECT_TRUE(IsNonBlocking(nonblocking));
  CloseSocket(nonblocking);

  std::array<SocketFd, 2> fds{-1, -1};
  ASSERT_EQ(OpenPipe(fds, {.nonblocking = true, .close_on_exec = true}), 0);
  // SocketFd is intptr_t on Win64 (a full SOCKET); narrowing it to int here
  // would close an unrelated handle when the descriptor exceeds INT_MAX.
  for (const SocketFd fd : fds) {
    EXPECT_TRUE(IsCloseOnExec(fd));
    EXPECT_TRUE(IsNonBlocking(fd));
    CloseSocket(fd);
  }
}

TEST(PlatformSocketTest, AcceptAppliesFlagsAndSendSkipsSigPipe) {
  const auto pair = TcpPair();
  ASSERT_TRUE(pair.has_value());
  auto [client, server] = *pair;
  ASSERT_TRUE(IsValidSocketFd(client) && IsValidSocketFd(server));
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
  EXPECT_TRUE(poller.Add(waker.ReadFd(), kPollReadable));
  waker.Wake();
  EXPECT_EQ(waiting.get(), 1);
}

TEST(PlatformEventPollerTest, LevelTriggeredReadWriteAndPeerClose) {
  EventPoller poller;
  ASSERT_TRUE(poller.Open());
  const auto pair = TcpPair();
  ASSERT_TRUE(pair.has_value());
  auto [client, server] = *pair;
  ASSERT_TRUE(IsValidSocketFd(client) && IsValidSocketFd(server));

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

// T3719: Close() used to race an unsynchronized write_fd_ read in Wake()
// (any-thread contract). The fd slot is an atomic now and Close publishes
// -1 before the close, so a Wake() after Close must be a no-op. The Waker
// contract still requires joining every Wake() source before Close/destroy,
// so the workers below are stopped first; a late Wake() is then checked.
TEST(PlatformEventPollerTest, WakeAfterCloseIsNoOp) {
  Waker waker;
  ASSERT_TRUE(waker.Open());
  std::atomic<bool> stop{false};
  std::vector<std::thread> wakers;
  for (int i = 0; i < 4; ++i) {
    wakers.emplace_back(  // NOLINT(performance-inefficient-vector-operation)
        [&] {             // NOLINT(performance-inefficient-vector-operation)
          while (!stop.load(std::memory_order_relaxed)) {
            waker.Wake();
          }
        });
  }
  for (int i = 0; i < 1000; ++i) {
    waker.Drain();
  }
  // Contract: every Wake() source must be joined before the Waker is closed
  // or destroyed (Close on Windows may otherwise reuse a stale SOCKET and
  // write into an unrelated connection).
  stop.store(true);
  for (std::thread& waker_thread : wakers) {
    waker_thread.join();
  }
  waker.Close();
  waker.Wake();  // no-op after Close
}

TEST(PlatformCpuAffinityTest, EqualityComparesBitmaskContents) {
  // P67: ParallelPool::Work() compares batch affinities once per task
  // batch.  Equality must distinguish distinct masks, agree with a fresh
  // sched_getaffinity snapshot, and stay a fixed-size POD compare (the
  // implementation contracts on std::memcmp over the raw words).
  if (!CpuSet::Supported()) {
    GTEST_SKIP() << "CpuSet affinity is not supported on this platform";
  }
  const CpuSet a = CpuSet::Single(0);
  const CpuSet b = CpuSet::Single(0);
  const CpuSet c = CpuSet::Single(1);
  EXPECT_TRUE(a == b);
  EXPECT_FALSE(a == c);
  if (const std::optional<CpuSet> current = CpuSet::CurrentThread();
      current.has_value()) {
    const std::optional<CpuSet> again = CpuSet::CurrentThread();
    if (!again.has_value()) {
      FAIL() << "current affinity disappeared";
    }
    EXPECT_TRUE(*current == *again);
  }
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
  // Restore the original affinity on every exit path: a fatal assertion
  // below must not leave the whole test binary pinned to a single CPU.
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct
      AffinityRestore {  // NOLINT(cppcoreguidelines-special-member-functions)
    ~AffinityRestore() { (void)set.ApplyToCurrentThread(); }
    CpuSet set;
  } affinity_restore{*current};
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

// Cover the at_utc=true path and invalid civil-date rejection; the local-time
// tests above exercise only the default (at_utc=false) branch.
TEST(PlatformTimeZoneTest, AtUtcAndInvalidCivilInputs) {
  // Asia/Tokyo has no DST, so the offset is the same at any instant.
  EXPECT_EQ(TimeZoneOffsetAtLocalTime("Asia/Tokyo", 2024, 1, 1, 0, 0, 0, true),
            9 * 3600);
  // A spring-forward gap is nonexistent in local time but unambiguous in UTC.
  const auto gap_local = TimeZoneOffsetAtLocalTime("America/New_York", 2024, 3,
                                                   10, 2, 30, 0, false);
  const auto gap_utc = TimeZoneOffsetAtLocalTime(
      "America/New_York", 2024, 3, 10, 2, 30, 0,
      true);  // NOLINT(bugprone-unchecked-optional-access)
  EXPECT_TRUE(gap_local.has_value());
  EXPECT_TRUE(gap_utc.has_value());
  // Fields outside civil bounds must not be accepted.
  EXPECT_FALSE(
      TimeZoneOffsetAtLocalTime("UTC", 2024, 13, 1, 0, 0, 0).has_value());
  EXPECT_FALSE(
      TimeZoneOffsetAtLocalTime("UTC", 2024, 1, 0, 0, 0, 0).has_value());
  EXPECT_FALSE(
      TimeZoneOffsetAtLocalTime("UTC", 2024, 1, 1, 24, 0, 0).has_value());
  EXPECT_FALSE(
      TimeZoneOffsetAtLocalTime("UTC", 2024, 1, 1, 0, 60, 0).has_value());
}

// T2093: Linux now memoises zone lookups (positive and negative).  Repeated
// resolution of the same name must stay stable and identical to the uncached
// semantics (per-call locate_zone) that macOS always had.
TEST(PlatformTimeZoneTest, CacheKeepsRepeatedLookupsStable) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(IsKnownTimeZone("Asia/Tokyo"));
    EXPECT_FALSE(IsKnownTimeZone("Foo/Bar/Baz"));
    const auto offset =
        TimeZoneOffsetAtLocalTime("Asia/Tokyo", 2024, 6, 1, 12, 0, 0);
    ASSERT_TRUE(offset.has_value());
    EXPECT_EQ(*offset, 9 * 3600);  // NOLINT(bugprone-unchecked-optional-access)
    EXPECT_FALSE(TimeZoneOffsetAtLocalTime("Foo/Bar/Baz", 2024, 6, 1, 12, 0, 0)
                     .has_value());
  }
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

TEST(PlatformSyncedStreamTest, EmitsOnDemandWithoutRepeatingOutput) {
  std::ostringstream target;
  {
    SyncedOStream stream(target);
    stream << "first";
    stream.emit();
    EXPECT_EQ(target.str(), "first");
    stream << " second";
  }
  EXPECT_EQ(target.str(), "first second");
}

TEST(PlatformUnlinkedTempFileTest, ReadsWritesAndLeavesNoDirectoryEntries) {
  std::error_code ec;
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path(ec) /
      ("tinylamb_unlinked_test_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  ASSERT_TRUE(std::filesystem::create_directories(dir, ec)) << ec.message();
  struct DirCleanup {
    explicit DirCleanup(std::filesystem::path p) : path(std::move(p)) {}
    DirCleanup(const DirCleanup&) = delete;
    DirCleanup& operator=(const DirCleanup&) = delete;
    DirCleanup(DirCleanup&&) = delete;
    DirCleanup& operator=(DirCleanup&&) = delete;
    ~DirCleanup() {
      std::error_code clean_ec;
      std::filesystem::remove_all(path, clean_ec);
    }
    std::filesystem::path path;
  } cleanup{dir};

  const int fd = OpenUnlinkedTempFile(dir);
  ASSERT_GE(fd, 0) << "errno=" << errno;
  // Directory must remain empty even while `fd` is open.
  EXPECT_TRUE(std::filesystem::is_empty(dir, ec)) << ec.message();

  const std::string payload = "hello_unlinked_spill_block";
  ASSERT_EQ(Pwrite(fd, payload.data(), payload.size(), 64),
            static_cast<ssize_t>(payload.size()));
  int64_t fsize = -1;
  ASSERT_EQ(FileSize(fd, &fsize), 0);
  EXPECT_EQ(fsize, 64 + static_cast<int64_t>(payload.size()));

  std::string out(payload.size(), '\0');
  ASSERT_EQ(Pread(fd, out.data(), out.size(), 64),
            static_cast<ssize_t>(payload.size()));
  EXPECT_EQ(out, payload);

  AdviseDontNeed(fd, 0, fsize);
  ASSERT_EQ(Truncate(fd, 0), 0);
  ASSERT_EQ(FileSize(fd, &fsize), 0);
  EXPECT_EQ(fsize, 0);

  EXPECT_EQ(CloseFile(fd), 0);
  EXPECT_TRUE(std::filesystem::is_empty(dir, ec)) << ec.message();
}

TEST(PlatformUnlinkedTempFileTest, NonexistentDirectoryFailsCleanly) {
  // Unique name (the negative test must not depend on a fixed path being
  // absent in a shared temp dir).
  const std::filesystem::path missing =
      std::filesystem::temp_directory_path() /
      ("tinylamb_missing_unlinked_dir_" + std::to_string(GetPid()) + "_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  ASSERT_FALSE(std::filesystem::exists(missing));
  const int fd = OpenUnlinkedTempFile(missing);
  EXPECT_LT(fd, 0);
}

// #982: the memory budget's RSS gate calls this to reclaim freed-but-retained
// heap before refusing a reservation, so concurrent waiters must not all pay
// for the purge: only the first caller inside a ~20 ms window purges.
TEST(PlatformHeapPurgeTest, ReleaseFreeHeapToOsIsRateLimited) {
#ifndef __linux__
  GTEST_SKIP() << "purge hook is only compiled on Linux builds";
#elif !defined(__GLIBC__)
  if (!ReleaseFreeHeapToOs()) {
    GTEST_SKIP() << "no purge hook on this libc";
  }
#else
  // The ~20 ms rate-limit window is process-global: another thread (or an
  // earlier caller) may hold it when this test starts, and a concurrent
  // caller can steal it between our checks.  Wait out any active window and
  // retry a bounded number of times instead of asserting single-shot wins.
  bool purged = false;
  for (int attempt = 0; attempt < 10 && !purged; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    purged = ReleaseFreeHeapToOs();
  }
  ASSERT_TRUE(purged) << "purge window never became available to this thread";
  EXPECT_FALSE(ReleaseFreeHeapToOs())
      << "second immediate call must be rate-limited";
  purged = false;
  for (int attempt = 0; attempt < 10 && !purged; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    purged = ReleaseFreeHeapToOs();
  }
  EXPECT_TRUE(purged) << "purge must become available again after the window";
#endif
}

#ifndef TINYLAMB_OS_WINDOWS
// T2621: exec-family strings terminate at the first NUL; RunProcess must
// reject NUL-bearing args up front rather than silently exec'ing truncated.
TEST(ProcessAuditTest, EmbeddedNulInArgumentsIsRejected) {
  std::string error;
  EXPECT_EQ(
      RunProcess("/bin/sh",
                 {std::string("-c\0trailing", sizeof("-c\0trailing") - 1), "x"},
                 {}, &error),
      -1);  // NOLINT(readability-uppercase-literal-suffix)
  EXPECT_NE(error.find("embedded NUL"), std::string::npos) << error;
  // A NUL in the executable path is rejected the same way.
  EXPECT_EQ(RunProcess(std::filesystem::path(std::string(
                           "/bin/sh\0junk", sizeof("/bin/sh\0junk") - 1)),
                       {}, {}, &error),
            -1);
  EXPECT_NE(error.find("embedded NUL"), std::string::npos) << error;
}

// T2627: signal death reports the signal number instead of a generic
// "terminated abnormally".
TEST(ProcessAuditTest, SignalDeathReportsSignalNumber) {
  std::string error;
  EXPECT_EQ(RunProcess("/bin/sh", {"-c", "kill -SEGV $$"}, {}, &error), -1);
  EXPECT_NE(error.find("signal 11"), std::string::npos) << error;
  error.clear();
  EXPECT_EQ(RunProcess("/bin/sh", {"-c", "kill -TERM $$"}, {}, &error), -1);
  EXPECT_NE(error.find("signal 15"), std::string::npos) << error;
}

// T2630: PhysicalMemoryBytes must not exceed the cgroup memory limit; a
// container auto-sizing its budgets off HOST RAM gets OOM-killed.
TEST(ProcessMemoryAuditTest, PhysicalMemoryRespectsCgroupLimit) {
  const uint64_t phys = PhysicalMemoryBytes();
  EXPECT_GT(phys, 0u);  // NOLINT(readability-uppercase-literal-suffix)
  const long pages = ::sysconf(_SC_PHYS_PAGES);
  const long page_size = ::sysconf(_SC_PAGESIZE);
  ASSERT_GT(pages, 0);
  ASSERT_GT(page_size, 0);
  const uint64_t host =
      static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);
  auto read_limit = [](const char* path) -> uint64_t {
    std::ifstream in(path);
    std::string text;
    if (!in || !std::getline(in, text)) {
      return 0;
    }
    if (text == "max" || text.empty()) {
      return 0;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long v = std::strtoull(text.c_str(), &end, 10);
    if (errno == ERANGE || end == text.c_str() ||
        end != text.c_str() + text.size() || v >= (1ULL << 60)) {
      return 0;
    }
    return static_cast<uint64_t>(v);
  };
  const uint64_t v2 = read_limit("/sys/fs/cgroup/memory.max");
  const uint64_t v1 = read_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes");
  uint64_t expected = host;
  for (const uint64_t limit : {v2, v1}) {
    if (limit > 0 && limit < expected) {
      expected = limit;
    }
  }
  EXPECT_EQ(phys, expected);
}

// T2623: the mkstemp fallback must honor the non-inheritable promise of
// the header (O_TMPFILE/_O_NOINHERIT paths do); assert CLOEXEC is set.
TEST(PlatformFileIoTest, UnlinkedTempFdIsCloseOnExec) {
  std::error_code ec;
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path(ec) /
      ("tinylamb_cloexec_test_" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  ASSERT_TRUE(std::filesystem::create_directories(dir, ec)) << ec.message();
  const int fd = OpenUnlinkedTempFile(dir);
  ASSERT_GE(fd, 0) << "errno=" << errno;
  const int flags = ::fcntl(fd, F_GETFD);
  ASSERT_GE(flags, 0);
  EXPECT_NE(flags & FD_CLOEXEC, 0);
  (void)::close(fd);
  std::error_code clean_ec;
  std::filesystem::remove_all(dir, clean_ec);
  EXPECT_FALSE(clean_ec) << clean_ec.message();
}
#endif

}  // namespace
}  // namespace tinylamb::platform
