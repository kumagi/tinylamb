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

#include "common/platform/event_poller.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>

#include "common/platform/os.hpp"
#include "common/platform/socket.hpp"

#ifdef TINYLAMB_OS_LINUX
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#elif defined(TINYLAMB_OS_MACOS)
#include <fcntl.h>
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#elif defined(TINYLAMB_OS_WINDOWS)
#include <chrono>
#include <thread>
#include <vector>
#endif

namespace tinylamb::platform {
namespace {

// Upper bound on kernel events fetched per Wait; callers loop anyway.
constexpr size_t kMaxBatch = 128;

}  // namespace

EventPoller::~EventPoller() { Close(); }

void EventPoller::Close() {
#ifdef TINYLAMB_OS_WINDOWS
  {
    std::scoped_lock lock(mu_);
    poll_fd_ = -1;
    interest_.clear();
  }
  interest_changed_.notify_all();
#else
  if (poll_fd_ >= 0) {
    ::close(poll_fd_);
    poll_fd_ = -1;
  }
#ifndef __linux__
  std::scoped_lock lock(mu_);
  interest_.clear();
#endif
#endif
}

#ifdef __linux__

namespace {

uint32_t ToEpoll(uint32_t interest) {
  uint32_t events = 0;
  if ((interest & kPollReadable) != 0U) {
    events |= EPOLLIN;
  }
  if ((interest & kPollWritable) != 0U) {
    events |= EPOLLOUT;
  }
  if ((interest & kPollPeerClosed) != 0U) {
    events |= EPOLLRDHUP;
  }
  return events;
}

uint32_t FromEpoll(uint32_t events) {
  uint32_t result = 0;
  if ((events & EPOLLIN) != 0U) {
    result |= kPollReadable;
  }
  if ((events & EPOLLOUT) != 0U) {
    result |= kPollWritable;
  }
  if ((events & EPOLLRDHUP) != 0U) {
    result |= kPollPeerClosed;
  }
  if ((events & EPOLLHUP) != 0U) {
    result |= kPollHangup;
  }
  if ((events & EPOLLERR) != 0U) {
    result |= kPollError;
  }
  return result;
}

}  // namespace

bool EventPoller::Open() {
  if (poll_fd_ >= 0) {
    return true;
  }
  poll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
  return poll_fd_ >= 0;
}

// NOLINTNEXTLINE(readability-make-member-function-const)
bool EventPoller::Add(int fd, uint32_t interest) {
  epoll_event event{};
  event.events = ToEpoll(interest);
  event.data.fd = fd;
  return ::epoll_ctl(poll_fd_, EPOLL_CTL_ADD, fd, &event) == 0;
}

// NOLINTNEXTLINE(readability-make-member-function-const)
bool EventPoller::Modify(int fd, uint32_t interest) {
  epoll_event event{};
  event.events = ToEpoll(interest);
  event.data.fd = fd;
  return ::epoll_ctl(poll_fd_, EPOLL_CTL_MOD, fd, &event) == 0;
}

// NOLINTNEXTLINE(readability-make-member-function-const)
bool EventPoller::Remove(int fd) {
  epoll_event ignored{};
  return ::epoll_ctl(poll_fd_, EPOLL_CTL_DEL, fd, &ignored) == 0;
}

// NOLINTNEXTLINE(readability-make-member-function-const)
int EventPoller::Wait(std::span<PollResult> out, int timeout_ms) {
  std::array<epoll_event, kMaxBatch> events{};
  const int capacity = static_cast<int>(std::min(out.size(), events.size()));
  const int count = ::epoll_wait(poll_fd_, events.data(), capacity, timeout_ms);
  if (count < 0) {
    return -1;
  }
  for (int i = 0; i < count; ++i) {
    const auto& event = events[static_cast<size_t>(i)];
    out[static_cast<size_t>(i)] = {.fd = event.data.fd,
                                   .events = FromEpoll(event.events)};
  }
  return count;
}

Waker::~Waker() { Close(); }

bool Waker::Open() {
  if (read_fd_ >= 0) {
    return true;
  }
  read_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  write_fd_ = read_fd_;
  return read_fd_ >= 0;
}

void Waker::Close() {
  if (read_fd_ >= 0) {
    ::close(read_fd_);
  }
  read_fd_ = -1;
  write_fd_ = -1;
}

void Waker::Wake() const {
  if (write_fd_ < 0) {
    return;
  }
  const uint64_t one = 1;
  ssize_t written = 0;
  do {
    written = ::write(write_fd_, &one, sizeof(one));
  } while (written < 0 && errno == EINTR);
}

void Waker::Drain() const {
  if (read_fd_ < 0) {
    return;
  }
  uint64_t ignored = 0;
  ssize_t got = 0;
  do {
    got = ::read(read_fd_, &ignored, sizeof(ignored));
  } while (got < 0 && errno == EINTR);
}

#elif defined(TINYLAMB_OS_WINDOWS)

bool EventPoller::Open() {
  std::scoped_lock lock(mu_);
  if (poll_fd_ >= 0) return true;
  poll_fd_ = 1;
  return true;
}

bool EventPoller::Add(int fd, uint32_t interest) {
  std::scoped_lock lock(mu_);
  if (poll_fd_ < 0 || fd < 0) {
    errno = EBADF;
    return false;
  }
  interest_[fd] = interest;
  interest_changed_.notify_all();
  return true;
}

bool EventPoller::Modify(int fd, uint32_t interest) {
  std::scoped_lock lock(mu_);
  if (poll_fd_ < 0) {
    errno = EBADF;
    return false;
  }
  const auto found = interest_.find(fd);
  if (found == interest_.end()) {
    errno = ENOENT;
    return false;
  }
  found->second = interest;
  return true;
}

bool EventPoller::Remove(int fd) {
  std::scoped_lock lock(mu_);
  if (poll_fd_ < 0) {
    errno = EBADF;
    return false;
  }
  const auto found = interest_.find(fd);
  if (found == interest_.end()) {
    errno = ENOENT;
    return false;
  }
  interest_.erase(found);
  return true;
}

int EventPoller::Wait(std::span<PollResult> out, int timeout_ms) {
  if (out.empty()) {
    errno = EINVAL;
    return -1;
  }
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(std::max(0, timeout_ms));
  std::vector<WSAPOLLFD> poll_fds;
  std::vector<uint32_t> interests;
  {
    std::unique_lock lock(mu_);
    const auto ready = [&] { return poll_fd_ < 0 || !interest_.empty(); };
    if (timeout_ms < 0) {
      interest_changed_.wait(lock, ready);
    } else if (!interest_changed_.wait_until(lock, deadline, ready)) {
      return 0;
    }
    if (poll_fd_ < 0) {
      errno = EBADF;
      return -1;
    }
    poll_fds.reserve(interest_.size());
    interests.reserve(interest_.size());
    for (const auto& [fd, interest] : interest_) {
      SHORT ev = 0;
      if ((interest & (kPollReadable | kPollPeerClosed)) != 0U) {
        ev |= POLLRDNORM;
      }
      if ((interest & kPollWritable) != 0U) {
        ev |= POLLWRNORM;
      }
      poll_fds.push_back(
          WSAPOLLFD{.fd = static_cast<SOCKET>(fd), .events = ev, .revents = 0});
      interests.push_back(interest);
    }
  }
  if (timeout_ms >= 0) {
    timeout_ms = static_cast<int>(
        std::max<int64_t>(0, std::chrono::ceil<std::chrono::milliseconds>(
                                 deadline - std::chrono::steady_clock::now())
                                 .count()));
  }
  const int count = PollSockets(poll_fds.data(), poll_fds.size(), timeout_ms);
  if (count < 0) return -1;
  if (count == 0) return 0;

  const size_t capacity = std::min(out.size(), kMaxBatch);
  int written = 0;
  for (size_t i = 0;
       i < poll_fds.size() && static_cast<size_t>(written) < capacity; ++i) {
    const SHORT rev = poll_fds[i].revents;
    if (rev == 0) continue;
    const int fd = static_cast<int>(poll_fds[i].fd);
    const uint32_t req = interests[i];
    uint32_t bits = 0;
    if ((rev & (POLLERR | POLLNVAL)) != 0) {
      bits |= kPollError;
    }
    if ((rev & POLLHUP) != 0) {
      if ((req & (kPollReadable | kPollPeerClosed)) != 0U) {
        bits |= kPollReadable | kPollPeerClosed;
      } else {
        bits |= kPollHangup;
      }
    }
    if ((rev & POLLIN) != 0) {
      bits |= kPollReadable;
      if ((req & kPollPeerClosed) != 0U) {
        char peek = 0;
        const int peeked = ::recv(static_cast<SOCKET>(fd), &peek, 1, MSG_PEEK);
        if (peeked == 0) {
          bits |= kPollPeerClosed;
        } else if (peeked == SOCKET_ERROR &&
                   ::WSAGetLastError() != WSAEWOULDBLOCK) {
          bits |= kPollError;
        }
      }
    }
    if ((rev & POLLOUT) != 0) {
      bits |= kPollWritable;
    }
    if (bits != 0U) {
      out[static_cast<size_t>(written++)] = {.fd = fd, .events = bits};
    }
  }
  return written;
}

Waker::~Waker() { Close(); }

bool Waker::Open() {
  if (read_fd_ >= 0) return true;
  std::array<int, 2> fds{-1, -1};
  if (OpenPipe(fds, {.nonblocking = true, .close_on_exec = true}) != 0) {
    return false;
  }
  read_fd_ = fds[0];
  write_fd_ = fds[1];
  return true;
}

void Waker::Close() {
  if (read_fd_ >= 0) CloseSocket(read_fd_);
  if (write_fd_ >= 0) CloseSocket(write_fd_);
  read_fd_ = -1;
  write_fd_ = -1;
}

void Waker::Wake() const {
  if (write_fd_ < 0) return;
  const char one = 1;
  ssize_t written = 0;
  do {
    written = SendNoSignal(write_fd_, &one, sizeof(one));
  } while (written < 0 && errno == EINTR);
}

void Waker::Drain() const {
  if (read_fd_ < 0) return;
  std::array<char, 256> buffer{};
  for (;;) {
    const ssize_t got = RecvSocket(read_fd_, buffer.data(), buffer.size());
    if (got > 0) continue;
    if (got < 0 && errno == EINTR) continue;
    break;
  }
}

#else  // kqueue

namespace {

// Applies one filter change. Deleting a filter that is not registered (or
// whose descriptor was already closed, which drops kqueue registrations
// implicitly) is not an error.
bool ChangeFilter(int kq, int fd, int16_t filter, bool enable) {
  struct kevent change{};
  EV_SET(&change, static_cast<uintptr_t>(fd), filter,
         enable ? (EV_ADD | EV_ENABLE) : EV_DELETE, 0, 0, nullptr);
  if (::kevent(kq, &change, 1, nullptr, 0, nullptr) == 0) return true;
  return !enable && (errno == ENOENT || errno == EBADF);
}

bool WantsRead(uint32_t interest) {
  return (interest & (kPollReadable | kPollPeerClosed)) != 0U;
}
bool WantsWrite(uint32_t interest) { return (interest & kPollWritable) != 0U; }

bool ApplyInterest(int kq, int fd, uint32_t before, uint32_t after,
                   bool fresh) {
  if (WantsRead(after) && (fresh || !WantsRead(before))) {
    if (!ChangeFilter(kq, fd, EVFILT_READ, true)) return false;
  } else if (!WantsRead(after) && (fresh || WantsRead(before))) {
    if (!ChangeFilter(kq, fd, EVFILT_READ, false)) return false;
  }
  if (WantsWrite(after) && (fresh || !WantsWrite(before))) {
    if (!ChangeFilter(kq, fd, EVFILT_WRITE, true)) return false;
  } else if (!WantsWrite(after) && (fresh || WantsWrite(before))) {
    if (!ChangeFilter(kq, fd, EVFILT_WRITE, false)) return false;
  }
  return true;
}

}  // namespace

bool EventPoller::Open() {
  if (poll_fd_ >= 0) return true;
  poll_fd_ = ::kqueue();
  if (poll_fd_ < 0) return false;
  const int flags = ::fcntl(poll_fd_, F_GETFD);
  if (flags >= 0) (void)::fcntl(poll_fd_, F_SETFD, flags | FD_CLOEXEC);
  return true;
}

bool EventPoller::Add(int fd, uint32_t interest) {
  std::scoped_lock lock(mu_);
  // A stale entry means the descriptor number was closed and reused without
  // Remove; the kernel already dropped its filters, so start fresh.
  if (!ApplyInterest(poll_fd_, fd, 0, interest, /*fresh=*/true)) return false;
  interest_[fd] = interest;
  return true;
}

bool EventPoller::Modify(int fd, uint32_t interest) {
  std::scoped_lock lock(mu_);
  const auto found = interest_.find(fd);
  if (found == interest_.end()) {
    errno = ENOENT;
    return false;
  }
  if (!ApplyInterest(poll_fd_, fd, found->second, interest, /*fresh=*/false)) {
    return false;
  }
  found->second = interest;
  return true;
}

bool EventPoller::Remove(int fd) {
  std::scoped_lock lock(mu_);
  const auto found = interest_.find(fd);
  if (found == interest_.end()) {
    errno = ENOENT;
    return false;
  }
  const uint32_t before = found->second;
  interest_.erase(found);
  return ApplyInterest(poll_fd_, fd, before, 0, /*fresh=*/false);
}

int EventPoller::Wait(std::span<PollResult> out, int timeout_ms) {
  std::array<struct kevent, kMaxBatch> events{};
  const int capacity = static_cast<int>(std::min(out.size(), events.size()));
  timespec timeout{};
  timespec* timeout_ptr = nullptr;
  if (timeout_ms >= 0) {
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1'000'000L;
    timeout_ptr = &timeout;
  }
  const int count =
      ::kevent(poll_fd_, nullptr, 0, events.data(), capacity, timeout_ptr);
  if (count < 0) return -1;
  int written = 0;
  for (int i = 0; i < count; ++i) {
    const struct kevent& event = events[static_cast<size_t>(i)];
    const int fd = static_cast<int>(event.ident);
    uint32_t bits = 0;
    if ((event.flags & EV_ERROR) != 0) {
      bits |= kPollError;
    } else if (event.filter == EVFILT_READ) {
      bits |= kPollReadable;
      if ((event.flags & EV_EOF) != 0) {
        bits |= kPollPeerClosed;
        if (event.fflags != 0) bits |= kPollError;
      }
    } else if (event.filter == EVFILT_WRITE) {
      bits |= kPollWritable;
      if ((event.flags & EV_EOF) != 0) {
        bits |= kPollHangup;
        if (event.fflags != 0) bits |= kPollError;
      }
    }
    // Merge the read and write filters of one descriptor into one result,
    // as epoll does; callers may close the fd while handling the first.
    bool merged = false;
    for (int j = 0; j < written; ++j) {
      if (out[static_cast<size_t>(j)].fd == fd) {
        out[static_cast<size_t>(j)].events |= bits;
        merged = true;
        break;
      }
    }
    if (!merged) {
      out[static_cast<size_t>(written++)] = {.fd = fd, .events = bits};
    }
  }
  return written;
}

Waker::~Waker() { Close(); }

bool Waker::Open() {
  if (read_fd_ >= 0) return true;
  std::array<int, 2> fds{-1, -1};
  if (OpenPipe(fds, {.nonblocking = true, .close_on_exec = true}) != 0) {
    return false;
  }
  read_fd_ = fds[0];
  write_fd_ = fds[1];
  return true;
}

void Waker::Close() {
  if (read_fd_ >= 0) ::close(read_fd_);
  if (write_fd_ >= 0) ::close(write_fd_);
  read_fd_ = -1;
  write_fd_ = -1;
}

void Waker::Wake() const {
  if (write_fd_ < 0) return;
  const char one = 1;
  ssize_t written = 0;
  do {
    written = ::write(write_fd_, &one, sizeof(one));
  } while (written < 0 && errno == EINTR);
  // EAGAIN: the pipe is full, so a wake-up is already pending.
}

void Waker::Drain() const {
  if (read_fd_ < 0) return;
  std::array<char, 256> buffer{};
  for (;;) {
    const ssize_t got = ::read(read_fd_, buffer.data(), buffer.size());
    if (got > 0) continue;
    if (got < 0 && errno == EINTR) continue;
    break;  // 0 (closed) or EAGAIN: drained
  }
}

#endif

}  // namespace tinylamb::platform
