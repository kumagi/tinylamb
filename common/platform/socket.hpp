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

#include <spawn.h>
#include <sys/socket.h>
#include <sys/types.h>

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

// socket(2) with `flags` applied. Every socket is also configured not to
// raise SIGPIPE on a write to a closed peer where the platform supports a
// per-socket option (SO_NOSIGPIPE on macOS); combine with kSendNoSignal.
// Returns the descriptor, or -1 with errno set.
int OpenSocket(int domain, int type, int protocol, FdFlags flags);

// accept(2) with `flags` applied to the new connection (accept4 on Linux).
// Returns the descriptor, or -1 with errno set (EAGAIN/EWOULDBLOCK/EINTR
// pass through unchanged).
int AcceptSocket(int listener, FdFlags flags);

// pipe(2) with `flags` applied to both ends (pipe2 on Linux). Returns 0, or
// -1 with errno set.
int OpenPipe(std::array<int, 2>& fds, FdFlags flags);

// send(2)/sendmsg(2) flag suppressing SIGPIPE for this call.
inline constexpr int kSendNoSignal = MSG_NOSIGNAL;

// Wrappers that add kSendNoSignal to `extra_flags`.
ssize_t SendNoSignal(int fd, const void* data, size_t size,
                     int extra_flags = 0);
ssize_t SendMsgNoSignal(int fd, const msghdr* message, int extra_flags = 0);

// posix_spawnattr_init plus the platform's "do not leak descriptors into the
// child" flag (POSIX_SPAWN_CLOEXEC_DEFAULT on macOS; Linux relies on the
// atomic O_CLOEXEC flags above). Only descriptors named in the file actions
// survive into the child. Returns 0 or an errno value, like posix_spawn.
int InitSpawnAttributes(posix_spawnattr_t* attributes);

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_SOCKET_HPP
