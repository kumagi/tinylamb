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

#include "common/platform/socket.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>

namespace tinylamb::platform {
namespace {

#if !defined(__linux__)
// Applies `flags` to an already-created descriptor. On failure closes `fd`
// (preserving errno) and returns -1.
int ApplyFlags(int fd, FdFlags flags) {
  if (flags.close_on_exec) {
    const int fd_flags = ::fcntl(fd, F_GETFD);
    if (fd_flags < 0 || ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) != 0) {
      const int saved = errno;
      ::close(fd);
      errno = saved;
      return -1;
    }
  }
  if (flags.nonblocking) {
    const int status = ::fcntl(fd, F_GETFL);
    if (status < 0 || ::fcntl(fd, F_SETFL, status | O_NONBLOCK) != 0) {
      const int saved = errno;
      ::close(fd);
      errno = saved;
      return -1;
    }
  }
  return fd;
}
#endif

void DisableSigPipe(int fd) {
#if defined(SO_NOSIGPIPE)
  const int enabled = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#else
  (void)fd;
#endif
}

}  // namespace

int OpenSocket(int domain, int type, int protocol, FdFlags flags) {
#if defined(__linux__)
  if (flags.nonblocking) type |= SOCK_NONBLOCK;
  if (flags.close_on_exec) type |= SOCK_CLOEXEC;
  const int fd = ::socket(domain, type, protocol);
#else
  const int created = ::socket(domain, type, protocol);
  const int fd = created < 0 ? -1 : ApplyFlags(created, flags);
#endif
  if (fd >= 0) DisableSigPipe(fd);
  return fd;
}

int AcceptSocket(int listener, FdFlags flags) {
#if defined(__linux__)
  int accept_flags = 0;
  if (flags.nonblocking) accept_flags |= SOCK_NONBLOCK;
  if (flags.close_on_exec) accept_flags |= SOCK_CLOEXEC;
  const int fd = ::accept4(listener, nullptr, nullptr, accept_flags);
#else
  // BSD accept(2) inherits O_NONBLOCK from the listener; normalize to the
  // requested state either way.
  const int accepted = ::accept(listener, nullptr, nullptr);
  int fd = accepted < 0 ? -1 : ApplyFlags(accepted, flags);
  if (fd >= 0 && !flags.nonblocking) {
    const int status = ::fcntl(fd, F_GETFL);
    if (status >= 0) (void)::fcntl(fd, F_SETFL, status & ~O_NONBLOCK);
  }
#endif
  if (fd >= 0) DisableSigPipe(fd);
  return fd;
}

int OpenPipe(std::array<int, 2>& fds, FdFlags flags) {
#if defined(__linux__)
  int pipe_flags = 0;
  if (flags.nonblocking) pipe_flags |= O_NONBLOCK;
  if (flags.close_on_exec) pipe_flags |= O_CLOEXEC;
  return ::pipe2(fds.data(), pipe_flags);
#else
  if (::pipe(fds.data()) != 0) return -1;
  if (ApplyFlags(fds[0], flags) < 0) {
    const int saved = errno;
    ::close(fds[1]);
    errno = saved;
    return -1;
  }
  if (ApplyFlags(fds[1], flags) < 0) {
    const int saved = errno;
    ::close(fds[0]);
    errno = saved;
    return -1;
  }
  return 0;
#endif
}

ssize_t SendNoSignal(int fd, const void* data, size_t size, int extra_flags) {
  return ::send(fd, data, size, kSendNoSignal | extra_flags);
}

ssize_t SendMsgNoSignal(int fd, const msghdr* message, int extra_flags) {
  return ::sendmsg(fd, message, kSendNoSignal | extra_flags);
}

int InitSpawnAttributes(posix_spawnattr_t* attributes) {
  const int rc = ::posix_spawnattr_init(attributes);
  if (rc != 0) return rc;
#if defined(POSIX_SPAWN_CLOEXEC_DEFAULT)
  const int set = ::posix_spawnattr_setflags(
      attributes, static_cast<short>(POSIX_SPAWN_CLOEXEC_DEFAULT));
  if (set != 0) {
    (void)::posix_spawnattr_destroy(attributes);
    return set;
  }
#endif
  return 0;
}

}  // namespace tinylamb::platform
