/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// #1055: Decode() must parse directly over the caller's bytes (a non-owning
// streambuf get area), not through a std::string copy fed to a
// std::stringstream.  These pins cover every behaviour the in-place buffer
// could break: EOF-at-end failbit on truncation, embedded NULs, the AtEnd()
// optional-trailing-field contract that peeks exactly one byte, and the
// zero-allocation performance contract itself.

#include "common/decoder.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <new>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/encoder.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"

namespace {

// Allocation counter driven by the global operator new replacement below;
// only the measuring thread's allocations count, so sanitizer runtime
// noise from background threads cannot flip the pin.
std::atomic<uint64_t> g_alloc_count{0};
std::atomic<bool> g_counting{false};
std::thread::id g_measuring_thread;

inline void CountAllocationIfActive() {
  if (g_counting.load(std::memory_order_relaxed) &&
      std::this_thread::get_id() == g_measuring_thread) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  }
}

}  // namespace

// Replacement placement: this toolchain's <new> does not declare
// std::operator new, so the replacements delegate to malloc/free directly.
// Every allocation in the binary still pairs with a matching deallocation
// because all operator new/delete paths are replaced together.
void* operator new(std::size_t n) {
  CountAllocationIfActive();
  void* p = std::malloc(n == 0 ? 1 : n);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}
void* operator new[](std::size_t n) {
  CountAllocationIfActive();
  void* p = std::malloc(n == 0 ? 1 : n);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}
void* operator new(std::size_t n, std::align_val_t a) {
  CountAllocationIfActive();
  const auto align = static_cast<std::size_t>(a);
  std::size_t size = n == 0 ? 1 : n;
  size = (size + align - 1) & ~(align - 1);
  void* p = std::aligned_alloc(align, size);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}
void* operator new[](std::size_t n, std::align_val_t a) {
  return operator new(n, a);
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t /*unused*/) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t /*unused*/) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t /*unused*/) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t /*unused*/) noexcept { std::free(p); }
void operator delete(void* p, std::size_t /*unused*/, std::align_val_t /*unused*/) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::size_t /*unused*/, std::align_val_t /*unused*/) noexcept {
  std::free(p);
}

namespace tinylamb {

namespace {

struct ScopedAllocationCount {
  uint64_t count;
};

ScopedAllocationCount MeasureAllocations(const std::function<void()>& fn) {
  g_alloc_count.store(0, std::memory_order_relaxed);
  g_measuring_thread = std::this_thread::get_id();
  g_counting.store(true, std::memory_order_seq_cst);
  fn();
  g_counting.store(false, std::memory_order_seq_cst);
  return {g_alloc_count.load(std::memory_order_relaxed)};
}

// Reads an optional trailing double the way ColumnStats::correlation_ does:
// the payload may legally end after the first field.
struct TrailingField final {
  uint64_t head{0};
  double tail{0.0};

  friend Decoder& operator>>(Decoder& dec, TrailingField& f) {
    dec >> f.head;
    if (!dec.Failed() && !dec.AtEnd()) {
      dec >> f.tail;
    }
    return dec;
  }
  friend Encoder& operator<<(Encoder& enc, const TrailingField& f) {
    enc << f.head << f.tail;
    return enc;
  }
};

}  // namespace

TEST(DecoderTest, DecodeRoundTripsOverStringViewStorage) {
  std::string payload;
  {
    Encoder enc(payload);
    enc << uint64_t(0xdeadbeefcafeULL) << std::string("ab\0cd", 5)
        << uint32_t(7);
  }
  // A char vector has no NUL terminator guarantee at the view end; Decode
  // must rely on the length alone.
  const std::vector<char> storage(payload.begin(), payload.end());
  const std::string_view view(storage.data(), storage.size());

  using Head = std::pair<uint64_t, std::string>;
  ASSIGN_OR_ASSERT_FAIL(
      Head, head, Decode<Head>(view.substr(0, view.size() - sizeof(uint32_t))));
  EXPECT_EQ(head.first, 0xdeadbeefcafeULL);
  EXPECT_EQ(head.second, std::string("ab\0cd", 5));
}

TEST(DecoderTest, DecodeRejectsTruncatedPayload) {
  std::string payload;
  {
    Encoder enc(payload);
    enc << std::vector<uint64_t>{1, 2, 3};
  }
  for (size_t cut = 1; cut < payload.size(); ++cut) {
    auto decoded = Decode<std::vector<uint64_t>>(payload.substr(0, cut));
    ASSERT_FALSE(decoded.HasValue()) << "cut=" << cut;
    EXPECT_EQ(decoded.GetStatus(), Status::kCorrupt) << "cut=" << cut;
  }
  ASSIGN_OR_ASSERT_FAIL(std::vector<uint64_t>, full,
                        Decode<std::vector<uint64_t>>(payload));
  EXPECT_EQ(full, (std::vector<uint64_t>{1, 2, 3}));
}

TEST(DecoderTest, DecodeEmptyInputFailsCorrupt) {
  auto decoded = Decode<uint64_t>(std::string_view());
  ASSERT_FALSE(decoded.HasValue());
  EXPECT_EQ(decoded.GetStatus(), Status::kCorrupt);
}

TEST(DecoderTest, AtEndSeesTrueExactlyAtBufferEnd) {
  std::string head_only;
  {
    Encoder enc(head_only);
    enc << uint64_t(41);
  }
  std::string both;
  {
    Encoder enc(both);
    enc << TrailingField{.head=42, .tail=0.5};
  }
  ASSIGN_OR_ASSERT_FAIL(TrailingField, missing,
                        Decode<TrailingField>(head_only));
  EXPECT_EQ(missing.head, 41U);
  EXPECT_EQ(missing.tail, 0.0);
  ASSIGN_OR_ASSERT_FAIL(TrailingField, present, Decode<TrailingField>(both));
  EXPECT_EQ(present.head, 42U);
  EXPECT_EQ(present.tail, 0.5);
}

// #1055's performance contract: decoding a POD payload must not touch the
// heap at all.  The old body (std::string copy feeding a std::stringstream)
// allocated at least twice per Decode; the in-place get area does neither.
TEST(DecoderTest, DecodeOfPlainPayloadPerformsNoHeapAllocation) {
  std::string payload;
  {
    Encoder enc(payload);
    enc << TrailingField{.head=7, .tail=0.25};
  }
  // Warm up: the first Decode may pull in lazy static init outside the
  // measurement window.
  (void)Decode<TrailingField>(payload);
  std::atomic<uint64_t> sink{0};
  const auto measured = MeasureAllocations([&] {
    for (int i = 0; i < 100; ++i) {
      StatusOr<TrailingField> decoded = Decode<TrailingField>(payload);
      if (decoded.HasValue()) {
        sink.fetch_add(decoded.Value().head, std::memory_order_relaxed);
      }
    }
  });
  EXPECT_EQ(sink.load(), 700U);
  EXPECT_EQ(measured.count, 0U)
      << "Decode of a 16-byte POD payload allocated " << measured.count
      << " times (copy + stringstream regression?)";
}

}  // namespace tinylamb
