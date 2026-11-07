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

#ifndef TINYLAMB_COMMON_PLATFORM_EVENT_POLLER_HPP
#define TINYLAMB_COMMON_PLATFORM_EVENT_POLLER_HPP

#include <cstdint>
#include <mutex>
#include <span>
#include <unordered_map>

namespace tinylamb::platform {

// Readiness bits shared by interest sets and reported events.
enum PollEvent : uint32_t {
  kPollReadable = 1U << 0,    // EPOLLIN / EVFILT_READ
  kPollWritable = 1U << 1,    // EPOLLOUT / EVFILT_WRITE
  kPollPeerClosed = 1U << 2,  // EPOLLRDHUP / EVFILT_READ + EV_EOF
  // Reported only, never requested:
  kPollHangup = 1U << 3,  // EPOLLHUP / EVFILT_WRITE + EV_EOF
  kPollError = 1U << 4,   // EPOLLERR / EV_EOF with a pending socket error
};

struct PollResult {
  int fd{-1};
  uint32_t events{0};
};

// Level-triggered readiness notification over file descriptors: epoll(7) on
// Linux, kqueue(2) on macOS/BSD. Semantics follow epoll's level-triggered
// mode: a descriptor is reported on every Wait while it stays ready, and at
// most once per Wait (kqueue's separate read/write filters are merged).
// kPollPeerClosed is only observable together with kPollReadable interest
// (kqueue reports EOF on the read filter); request both.
//
// Add/Modify/Remove may be called from any thread; Wait from one thread.
class EventPoller {
 public:
  EventPoller() = default;
  ~EventPoller();
  EventPoller(const EventPoller&) = delete;
  EventPoller& operator=(const EventPoller&) = delete;

  // Creates the kernel object. Returns false with errno set on failure.
  bool Open();
  bool IsOpen() const { return poll_fd_ >= 0; }
  void Close();

  // Each returns false with errno set on failure.
  bool Add(int fd, uint32_t interest);
  bool Modify(int fd, uint32_t interest);
  bool Remove(int fd);

  // Blocks up to `timeout_ms` (-1 = forever) and fills `out`. Returns the
  // number of entries written, or -1 with errno set (EINTR included).
  int Wait(std::span<PollResult> out, int timeout_ms);

 private:
  int poll_fd_{-1};
#if !defined(__linux__)
  // kqueue registers read and write as independent filters, so Modify needs
  // the previous interest to know which filters to add or delete.
  std::mutex mu_;
  std::unordered_map<int, uint32_t> interest_;
#endif
};

// Cross-thread wake-up for an EventPoller: eventfd(2) on Linux, a
// non-blocking pipe elsewhere. Register ReadFd() for kPollReadable; any
// thread may call Wake(); the polling thread calls Drain() when ReadFd() is
// reported readable.
class Waker {
 public:
  Waker() = default;
  ~Waker();
  Waker(const Waker&) = delete;
  Waker& operator=(const Waker&) = delete;

  bool Open();  // false with errno set on failure
  bool IsOpen() const { return read_fd_ >= 0; }
  void Close();
  int ReadFd() const { return read_fd_; }
  void Wake() const;
  void Drain() const;

 private:
  int read_fd_{-1};
  int write_fd_{-1};  // == read_fd_ for eventfd
};

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_EVENT_POLLER_HPP
