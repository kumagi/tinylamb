#pragma once

#include <arpa/inet.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace distcomp {

inline bool WriteAll(int fd, const void* data, size_t len) {
  const auto* p = static_cast<const uint8_t*>(data);
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::send(fd, p + off, len - off, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    off += static_cast<size_t>(n);
  }
  return true;
}

inline bool WriteAllFd(int fd, const void* data, size_t len) {
  const auto* p = static_cast<const uint8_t*>(data);
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::write(fd, p + off, len - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    off += static_cast<size_t>(n);
  }
  return true;
}

inline bool ReadExact(int fd, void* data, size_t len) {
  auto* p = static_cast<uint8_t*>(data);
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::read(fd, p + off, len - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    off += static_cast<size_t>(n);
  }
  return true;
}

inline void AppendU32(std::string& buf, uint32_t v) {
  uint32_t net = htonl(v);
  buf.append(reinterpret_cast<const char*>(&net), sizeof(net));
}

inline void AppendI32(std::string& buf, int32_t v) {
  AppendU32(buf, static_cast<uint32_t>(v));
}

inline void AppendStr(std::string& buf, const std::string& s) {
  // The length prefix is uint32_t; a larger payload would silently wrap and
  // desync the receiver. Fail loudly instead (dev-tool protocol, no recovery).
  if (s.size() > 0xFFFFFFFFULL) {
    std::abort();
  }
  AppendU32(buf, static_cast<uint32_t>(s.size()));
  buf.append(s);
}

inline bool ReadU32(const uint8_t*& p, const uint8_t* end, uint32_t& out) {
  // Signed compare: an inverted range (p > end) must fail, not wrap.
  if (end - p < 4) return false;
  uint32_t net = 0;
  std::memcpy(&net, p, 4);
  p += 4;
  out = ntohl(net);
  return true;
}

inline bool ReadI32(const uint8_t*& p, const uint8_t* end, int32_t& out) {
  uint32_t u = 0;
  if (!ReadU32(p, end, u)) return false;
  out = static_cast<int32_t>(u);
  return true;
}

inline bool ReadStr(const uint8_t*& p, const uint8_t* end, std::string& out) {
  uint32_t len = 0;
  if (!ReadU32(p, end, len)) return false;
  if (end - p < static_cast<std::ptrdiff_t>(len)) return false;
  out.assign(reinterpret_cast<const char*>(p), len);
  p += len;
  return true;
}

// Constant-time equality for the shared DISTCOMP_TOKEN secret carried as the
// first field of coordinator -> worker frames (the only network-facing hop).
// The length is not secret (it is on the wire in the frame header), so an
// early return on mismatch is fine; the byte compare itself must not leak a
// prefix-match timing signal.
inline bool SecretEquals(const std::string& a, const std::string& b) {
  if (a.size() != b.size()) return false;
  unsigned diff = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
  }
  return diff == 0;
}

inline bool SendFrame(int fd, const std::string& payload, bool is_socket = true) {
  uint32_t len = htonl(static_cast<uint32_t>(payload.size()));
  if (is_socket) {
    return WriteAll(fd, &len, sizeof(len)) &&
           (payload.empty() || WriteAll(fd, payload.data(), payload.size()));
  }
  return WriteAllFd(fd, &len, sizeof(len)) &&
         (payload.empty() || WriteAllFd(fd, payload.data(), payload.size()));
}

inline bool RecvFrame(int fd, std::string& payload) {
  uint32_t net_len = 0;
  if (!ReadExact(fd, &net_len, sizeof(net_len))) return false;
  uint32_t len = ntohl(net_len);
  constexpr uint32_t kMaxFrameBytes = 256U * 1024 * 1024;  // 256 MiB
  if (len > kMaxFrameBytes) {
    return false;
  }
  // Read the payload incrementally: resizing to the announced length up
  // front would let an unauthenticated peer (the token is the first field
  // inside the frame, checked only after RecvFrame returns) pin a full
  // 256 MiB allocation per connection by sending just the length header.
  // Appending received chunks keeps memory proportional to bytes actually
  // on the wire.
  payload.clear();
  char chunk[64 * 1024];
  uint32_t remaining = len;
  while (remaining > 0) {
    const size_t want =
        remaining < sizeof(chunk) ? remaining : sizeof(chunk);
    if (!ReadExact(fd, chunk, want)) return false;
    payload.append(chunk, want);
    remaining -= static_cast<uint32_t>(want);
  }
  return true;
}

inline void TuneTcpSocket(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
}

}  // namespace distcomp
