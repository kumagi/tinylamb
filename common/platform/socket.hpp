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

#ifndef TINYLAMB_COMMON_PLATFORM_SOCKET_HPP
#define TINYLAMB_COMMON_PLATFORM_SOCKET_HPP

#include "common/platform/os.hpp"

#if defined(TINYLAMB_OS_WINDOWS)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <sys/types.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#ifdef GetMessage
#undef GetMessage
#endif
#ifdef ERROR
#undef ERROR
#endif

#ifndef SHUT_RD
#define SHUT_RD SD_RECEIVE
#endif
#ifndef SHUT_WR
#define SHUT_WR SD_SEND
#endif
#ifndef SHUT_RDWR
#define SHUT_RDWR SD_BOTH
#endif
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/types.h>
#endif

#include <array>
#include <cstddef>

namespace tinylamb::platform {

// Descriptor flags applied at creation time. Linux sets them atomically
// (SOCK_NONBLOCK/SOCK_CLOEXEC, accept4, pipe2). macOS has no such flags and
// applies them with fcntl right after creation; the close-on-exec race this
// opens against a concurrent fork/exec is closed for our own children by
// InitSpawnAttributes (POSIX_SPAWN_CLOEXEC_DEFAULT).
struct FdFlags {
  bool nonblocking{false};
  bool close_on_exec{true};
};

// Ensures the platform socket subsystem is initialized (WSAStartup on
// Windows; no-op on POSIX). Must be called before raw resolver calls such as
// getaddrinfo/inet_pton when no socket has been opened yet.
void EnsureSocketSubsystemInitialized();

// socket(2) with `flags` applied. Every socket is also configured not to
// raise SIGPIPE on a write to a closed peer where the platform supports a
// per-socket option (SO_NOSIGPIPE on macOS); combine with kSendNoSignal.
// Returns the descriptor, or -1 with errno set.
int OpenSocket(int domain, int type, int protocol, FdFlags flags);

// accept(2) with `flags` applied to the new connection (accept4 on Linux).
// Returns the descriptor, or -1 with errno set (EAGAIN/EWOULDBLOCK/EINTR
// pass through unchanged).
int AcceptSocket(int listener, FdFlags flags);

// pipe(2) with `flags` applied to both ends (pipe2 on Linux; loopback TCP
// socket pair on Windows). Returns 0, or -1 with errno set.
int OpenPipe(std::array<int, 2>& fds, FdFlags flags);

// Closes a socket descriptor (or pipe end created by OpenPipe).
int CloseSocket(int fd);

// recv(2) wrapper that normalizes platform socket errors into errno.
ssize_t RecvSocket(int fd, void* data, size_t size, int flags = 0);

// shutdown(2) wrapper that normalizes platform socket errors into errno.
int ShutdownSocket(int fd, int how);

// poll(2) / WSAPoll wrapper over socket descriptors.
int PollSockets(pollfd* fds, size_t nfds, int timeout_ms);

// Sets an integer socket option (e.g. SO_REUSEADDR, TCP_NODELAY).
int SetSocketOptInt(int fd, int level, int optname, int value);

// Sets SO_RCVTIMEO or SO_SNDTIMEO in milliseconds.
int SetSocketTimeout(int fd, int optname, int timeout_ms);

// Queries descriptor flags (used by platform contract tests).
bool IsCloseOnExec(int fd);
bool IsNonBlocking(int fd);

// send(2)/sendmsg(2) flag suppressing SIGPIPE for this call.
inline constexpr int kSendNoSignal = MSG_NOSIGNAL;

// Wrappers that add kSendNoSignal to `extra_flags`.
ssize_t SendNoSignal(int fd, const void* data, size_t size,
                     int extra_flags = 0);

#if defined(TINYLAMB_OS_POSIX)
ssize_t SendMsgNoSignal(int fd, const msghdr* message, int extra_flags = 0);

// posix_spawnattr_init plus the platform's "do not leak descriptors into the
// child" flag (POSIX_SPAWN_CLOEXEC_DEFAULT on macOS; Linux relies on the
// atomic O_CLOEXEC flags above). Only descriptors named in the file actions
// survive into the child. Returns 0 or an errno value, like posix_spawn.
int InitSpawnAttributes(posix_spawnattr_t* attributes);
#endif

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_SOCKET_HPP
