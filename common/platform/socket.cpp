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

#include <cerrno>

#ifdef TINYLAMB_OS_WINDOWS
#include <algorithm>
#include <climits>
#include <mutex>
#include <unordered_map>

#include "common/log_message.hpp"
#else
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>
#endif

namespace tinylamb::platform {

#ifdef TINYLAMB_OS_WINDOWS

namespace {

void EnsureWinsockInitialized() {
  // WSAStartup returns 0 on success and a Winsock error code on failure. Do
  // not discard that code: without it every later Winsock call just fails
  // with WSANOTINITIALISED, which TranslateWsaError flattens into a generic
  // EIO with no hint about the real startup failure.
  static const int wsa_error = []() {
    WSADATA wsa_data{};
    return ::WSAStartup(MAKEWORD(2, 2), &wsa_data);
  }();
  if (wsa_error != 0) {
    LOG(WARN) << "WSAStartup failed with Winsock error " << wsa_error;
  }
}

int TranslateWsaError(int wsa_err) {
  switch (wsa_err) {
    case 0:
      return 0;
    case WSAEWOULDBLOCK:
      return EAGAIN;
    case WSAEINTR:
      return EINTR;
    case WSAECONNRESET:
      return ECONNRESET;
    case WSAECONNABORTED:
      return ECONNABORTED;
    case WSAENOTCONN:
      return ENOTCONN;
    case WSAESHUTDOWN:
      return EPIPE;
    case WSAETIMEDOUT:
      return ETIMEDOUT;
    case WSAEINVAL:
      return EINVAL;
    case WSAEMFILE:
      return EMFILE;
    case WSAENOBUFS:
      return ENOBUFS;
    case WSAEADDRINUSE:
      return EADDRINUSE;
    case WSAEADDRNOTAVAIL:
      return EADDRNOTAVAIL;
    case WSAECONNREFUSED:
      return ECONNREFUSED;
    case WSAEBADF:
    case WSAENOTSOCK:
      return EBADF;
    default:
      return EIO;
  }
}

void SetErrnoFromWsa() { errno = TranslateWsaError(::WSAGetLastError()); }

std::mutex& SocketFlagsMutex() {
  static std::mutex mu;
  return mu;
}

// Keyed by the lossless SocketFd (signed, pointer-sized on Win64), never by a
// truncated `int`. Two distinct SOCKET handles can share the same low 32 bits;
// using int here would collide them and make CloseSocket erase/close the wrong
// entry.
std::unordered_map<SocketFd, FdFlags>& SocketFlagsMap() {
  static std::unordered_map<SocketFd, FdFlags> map;
  return map;
}

SocketFd ApplyWinSocketFlags(SOCKET s, FdFlags flags) {
  if (s == INVALID_SOCKET) {
    SetErrnoFromWsa();
    return -1;
  }
  const HANDLE handle = reinterpret_cast<HANDLE>(s);
  if (!::SetHandleInformation(handle, HANDLE_FLAG_INHERIT,
                              flags.close_on_exec ? 0 : HANDLE_FLAG_INHERIT)) {
    SetErrnoFromWsa();
    const int saved = errno;
    ::closesocket(s);
    errno = saved;
    return -1;
  }
  u_long nonbio = flags.nonblocking ? 1UL : 0UL;
  if (::ioctlsocket(s, FIONBIO, &nonbio) != 0) {
    SetErrnoFromWsa();
    const int saved = errno;
    ::closesocket(s);
    errno = saved;
    return -1;
  }
  // Keep every bit of the native SOCKET handle (UINT_PTR / 64-bit on Win64).
  // static_cast<int>(s) used to truncate it, yielding a negative or aliased
  // fd that could identify a different socket on close/poll.
  const SocketFd fd = static_cast<SocketFd>(s);
  {
    std::scoped_lock lock(SocketFlagsMutex());
    SocketFlagsMap()[fd] = flags;
  }
  return fd;
}

const bool kAutoInitWinsock = []() {
  EnsureWinsockInitialized();
  return true;
}();

}  // namespace

void EnsureSocketSubsystemInitialized() { EnsureWinsockInitialized(); }

SocketFd OpenSocket(int domain, int type, int protocol, FdFlags flags) {
  EnsureWinsockInitialized();
  const SOCKET s = ::socket(domain, type, protocol);
  return ApplyWinSocketFlags(s, flags);
}

SocketFd AcceptSocket(SocketFd listener, FdFlags flags) {
  EnsureWinsockInitialized();
  const SOCKET accepted =
      ::accept(static_cast<SOCKET>(listener), nullptr, nullptr);
  return ApplyWinSocketFlags(accepted, flags);
}

int OpenPipe(std::array<SocketFd, 2>& fds, FdFlags flags) {
  EnsureWinsockInitialized();
  const SocketFd listener = OpenSocket(
      AF_INET, SOCK_STREAM, 0, {.nonblocking = false, .close_on_exec = true});
  if (!IsValidSocketFd(listener)) return -1;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  socklen_t addr_len = sizeof(addr);
  if (::bind(static_cast<SOCKET>(listener), reinterpret_cast<sockaddr*>(&addr),
             sizeof(addr)) != 0 ||
      ::listen(static_cast<SOCKET>(listener), 1) != 0 ||
      ::getsockname(static_cast<SOCKET>(listener),
                    reinterpret_cast<sockaddr*>(&addr), &addr_len) != 0) {
    SetErrnoFromWsa();
    const int saved = errno;
    CloseSocket(listener);
    errno = saved;
    return -1;
  }
  const SocketFd write_end =
      OpenSocket(AF_INET, SOCK_STREAM, 0,
                 {.nonblocking = false, .close_on_exec = flags.close_on_exec});
  if (!IsValidSocketFd(write_end)) {
    const int saved = errno;
    CloseSocket(listener);
    errno = saved;
    return -1;
  }
  // A blocking Winsock connect() has no guaranteed bound. If loopback is down
  // or firewalled, Waker::Open() would hang forever, so give it a deadline.
  constexpr int kConnectTimeoutMs = 5000;
  if (SetSocketTimeout(write_end, SO_SNDTIMEO, kConnectTimeoutMs) < 0) {
    const int saved = errno;
    CloseSocket(write_end);
    CloseSocket(listener);
    errno = saved;
    return -1;
  }
  if (::connect(static_cast<SOCKET>(write_end),
                reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    SetErrnoFromWsa();
    const int saved = errno;
    CloseSocket(write_end);
    CloseSocket(listener);
    errno = saved;
    return -1;
  }
  // Waker traffic is tiny; it must not inherit the connect deadline.
  (void)SetSocketTimeout(write_end, SO_SNDTIMEO, 0);
  const SocketFd read_end = AcceptSocket(listener, flags);
  CloseSocket(listener);
  if (!IsValidSocketFd(read_end)) {
    const int saved = errno;
    CloseSocket(write_end);
    errno = saved;
    return -1;
  }
  if (ApplyWinSocketFlags(static_cast<SOCKET>(write_end), flags) < 0) {
    const int saved = errno;
    CloseSocket(read_end);
    errno = saved;
    return -1;
  }
  const int nodelay = 1;
  if (SetSocketOptInt(read_end, IPPROTO_TCP, TCP_NODELAY, nodelay) < 0 ||
      SetSocketOptInt(write_end, IPPROTO_TCP, TCP_NODELAY, nodelay) < 0) {
    const int saved = errno;
    CloseSocket(read_end);
    CloseSocket(write_end);
    errno = saved;
    return -1;
  }
  fds[0] = read_end;
  fds[1] = write_end;
  return 0;
}

int CloseSocket(SocketFd fd) {
  if (!IsValidSocketFd(fd)) {
    errno = EBADF;
    return -1;
  }
  if (::closesocket(static_cast<SOCKET>(fd)) != 0) {
    SetErrnoFromWsa();
    return -1;
  }
  // Only drop the cached flags once the handle is really gone; otherwise a
  // failed close leaves a live fd whose queries report the default flags.
  {
    std::scoped_lock lock(SocketFlagsMutex());
    SocketFlagsMap().erase(fd);
  }
  return 0;
}

ssize_t RecvSocket(SocketFd fd, void* data, size_t size, int flags) {
  // Winsock's length parameter is an int; cap instead of narrowing so a
  // >INT_MAX buffer cannot wrap into a negative / short read.
  const int capped = static_cast<int>(std::min<size_t>(size, INT_MAX));
  const int got =
      ::recv(static_cast<SOCKET>(fd), static_cast<char*>(data), capped, flags);
  if (got == SOCKET_ERROR) {
    SetErrnoFromWsa();
    return -1;
  }
  return got;
}

ssize_t SendNoSignal(SocketFd fd, const void* data, size_t size,
                     int extra_flags) {
  // Winsock's length parameter is an int; cap instead of narrowing so a
  // >INT_MAX buffer cannot wrap into a negative / short send (the POSIX
  // branch passes size_t through unchanged). Pass extra_flags through as-is:
  // MSG_DONTWAIT is 0 on Windows, so there is nothing to strip, and keeping
  // it preserves the same contract on both platforms.
  const int capped = static_cast<int>(std::min<size_t>(size, INT_MAX));
  const int sent =
      ::send(static_cast<SOCKET>(fd), static_cast<const char*>(data), capped,
             kSendNoSignal | extra_flags);
  if (sent == SOCKET_ERROR) {
    SetErrnoFromWsa();
    return -1;
  }
  return sent;
}

int ShutdownSocket(SocketFd fd, int how) {
  if (::shutdown(static_cast<SOCKET>(fd), how) != 0) {
    SetErrnoFromWsa();
    return -1;
  }
  return 0;
}

int PollSockets(pollfd* fds, size_t nfds, int timeout_ms) {
  EnsureWinsockInitialized();
  const int rc = ::WSAPoll(fds, static_cast<ULONG>(nfds), timeout_ms);
  if (rc == SOCKET_ERROR) {
    SetErrnoFromWsa();
    return -1;
  }
  return rc;
}

int SetSocketOptInt(SocketFd fd, int level, int optname, int value) {
  if (::setsockopt(static_cast<SOCKET>(fd), level, optname,
                   reinterpret_cast<const char*>(&value), sizeof(value)) != 0) {
    SetErrnoFromWsa();
    return -1;
  }
  return 0;
}

int SetSocketTimeout(SocketFd fd, int optname, int timeout_ms) {
  if (timeout_ms < 0) {
    errno = EINVAL;
    return -1;
  }
  // Winsock treats DWORD 0 as "no timeout" (infinite wait), not "no wait".
  const DWORD val = static_cast<DWORD>(timeout_ms);
  if (::setsockopt(static_cast<SOCKET>(fd), SOL_SOCKET, optname,
                   reinterpret_cast<const char*>(&val), sizeof(val)) != 0) {
    SetErrnoFromWsa();
    return -1;
  }
  return 0;
}

bool IsCloseOnExec(SocketFd fd) {
  DWORD flags = 0;
  const HANDLE handle = reinterpret_cast<HANDLE>(static_cast<uintptr_t>(fd));
  if (::GetHandleInformation(handle, &flags) != 0) {
    return (flags & HANDLE_FLAG_INHERIT) == 0;
  }
  std::scoped_lock lock(SocketFlagsMutex());
  const auto it = SocketFlagsMap().find(fd);
  return it != SocketFlagsMap().end() && it->second.close_on_exec;
}

bool IsNonBlocking(SocketFd fd) {
  std::scoped_lock lock(SocketFlagsMutex());
  const auto it = SocketFlagsMap().find(fd);
  return it != SocketFlagsMap().end() && it->second.nonblocking;
}

#else  // TINYLAMB_OS_POSIX

namespace {

#ifndef __linux__
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
#ifdef SO_NOSIGPIPE
  const int enabled = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#else
  (void)fd;
#endif
}

}  // namespace

void EnsureSocketSubsystemInitialized() {}

SocketFd OpenSocket(int domain, int type, int protocol, FdFlags flags) {
#ifdef __linux__
  if (flags.nonblocking) {
    type |= SOCK_NONBLOCK;
  }
  if (flags.close_on_exec) {
    type |= SOCK_CLOEXEC;
  }
  const int fd = ::socket(domain, type, protocol);
#else
  const int created = ::socket(domain, type, protocol);
  const int fd = created < 0 ? -1 : ApplyFlags(created, flags);
#endif
  if (fd >= 0) {
    DisableSigPipe(fd);
  }
  return fd;
}

SocketFd AcceptSocket(SocketFd listener, FdFlags flags) {
#ifdef __linux__
  int accept_flags = 0;
  if (flags.nonblocking) {
    accept_flags |= SOCK_NONBLOCK;
  }
  if (flags.close_on_exec) {
    accept_flags |= SOCK_CLOEXEC;
  }
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
  if (fd >= 0) {
    DisableSigPipe(fd);
  }
  return fd;
}

int OpenPipe(std::array<SocketFd, 2>& fds, FdFlags flags) {
#ifdef __linux__
  int pipe_flags = 0;
  if (flags.nonblocking) {
    pipe_flags |= O_NONBLOCK;
  }
  if (flags.close_on_exec) {
    pipe_flags |= O_CLOEXEC;
  }
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

int CloseSocket(SocketFd fd) { return ::close(fd); }

ssize_t RecvSocket(SocketFd fd, void* data, size_t size, int flags) {
  return ::recv(fd, data, size, flags);
}

int ShutdownSocket(SocketFd fd, int how) { return ::shutdown(fd, how); }

int PollSockets(pollfd* fds, size_t nfds, int timeout_ms) {
  return ::poll(fds, static_cast<nfds_t>(nfds), timeout_ms);
}

int SetSocketOptInt(SocketFd fd, int level, int optname, int value) {
  return ::setsockopt(fd, level, optname, &value, sizeof(value));
}

int SetSocketTimeout(SocketFd fd, int optname, int timeout_ms) {
  if (timeout_ms < 0) {
    errno = EINVAL;
    return -1;
  }
  // timeval{0, 0} means "no timeout" (infinite wait), matching Winsock's
  // DWORD 0. Reject negative values above instead of clamping them into an
  // accidental infinite wait.
  timeval tv{};
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = static_cast<decltype(tv.tv_usec)>(timeout_ms % 1000) * 1000;
  return ::setsockopt(fd, SOL_SOCKET, optname, &tv, sizeof(tv));
}

bool IsCloseOnExec(SocketFd fd) {
  const int flags = ::fcntl(fd, F_GETFD);
  // fcntl returns -1 on a bad fd; -1 & FD_CLOEXEC would masquerade as set.
  return flags != -1 && (flags & FD_CLOEXEC) != 0;
}

bool IsNonBlocking(SocketFd fd) {
  const int flags = ::fcntl(fd, F_GETFL);
  return flags != -1 && (flags & O_NONBLOCK) != 0;
}

ssize_t SendNoSignal(SocketFd fd, const void* data, size_t size,
                     int extra_flags) {
  return ::send(fd, data, size, kSendNoSignal | extra_flags);
}

ssize_t SendMsgNoSignal(SocketFd fd, const msghdr* message, int extra_flags) {
  return ::sendmsg(fd, message, kSendNoSignal | extra_flags);
}

int InitSpawnAttributes(posix_spawnattr_t* attributes) {
  const int rc = ::posix_spawnattr_init(attributes);
  if (rc != 0) {
    return rc;
  }
#ifdef POSIX_SPAWN_CLOEXEC_DEFAULT
  const int set = ::posix_spawnattr_setflags(
      attributes, static_cast<short>(POSIX_SPAWN_CLOEXEC_DEFAULT));
  if (set != 0) {
    (void)::posix_spawnattr_destroy(attributes);
    return set;
  }
#endif
  return 0;
}

#endif  // TINYLAMB_OS_WINDOWS

}  // namespace tinylamb::platform
