#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/platform/process.hpp"
#include "protocol.hpp"

namespace fs = std::filesystem;

namespace {

// std::stoi throws on malformed input and wraps values outside the target
// range. Command-line parsing must fail closed with a diagnostic instead.
bool ParseIntArg(const char* text, int* out) {
  if (text == nullptr || *text == '\0') return false;
  errno = 0;
  char* end = nullptr;
  const long value = std::strtol(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || value < INT_MIN ||
      value > INT_MAX) {
    return false;
  }
  *out = static_cast<int>(value);
  return true;
}

void WaitClientClose(int fd) {
  char dummy;
  for (;;) {
    const ssize_t n = ::read(fd, &dummy, 1);
    if (n > 0 || (n < 0 && errno == EINTR)) continue;
    return;
  }
}

// ---------------------------------------------------------------------------
// Job tracing (JSONL) for build timeline visualization (scripts/build_gantt.py)
// ---------------------------------------------------------------------------

std::mutex g_trace_mu;
FILE* g_trace = nullptr;

int64_t NowUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string JsonEsc(const std::string& s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out += c;
        }
    }
  }
  return out;
}

void TraceEvent(const std::string& json) {
  std::lock_guard lk(g_trace_mu);
  if (!g_trace) return;
  std::fwrite(json.data(), 1, json.size(), g_trace);
  std::fputc('\n', g_trace);
  std::fflush(g_trace);
}

// Flush and close the trace stream under the same lock TraceEvent uses, so a
// still-running detached worker cannot write to a closed FILE* at shutdown.
void CloseTrace() {
  std::lock_guard lk(g_trace_mu);
  if (g_trace) {
    std::fclose(g_trace);
    g_trace = nullptr;
  }
}

struct RemoteSlot {
  int fd = -1;
  bool busy = false;
};

class Coordinator {
 public:
  Coordinator(std::string remote_ip, uint16_t remote_port, int local_cap,
              int remote_cap, std::string token)
      : remote_ip_(std::move(remote_ip)),
        remote_port_(remote_port),
        local_cap_(local_cap),
        remote_cap_(remote_cap),
        token_(std::move(token)),
        remote_slots_(static_cast<size_t>(remote_cap)),
        local_busy_(static_cast<size_t>(local_cap)) {}

  const std::string& Token() const { return token_; }

  struct SlotAssign {
    bool local = true;
    int idx = 0;  // local slot id when local, remote slot id otherwise
  };

  void WarmupConnections() {
    std::vector<std::thread> threads;
    for (int i = 0; i < remote_cap_; ++i) {
      threads.emplace_back([this, i] {
        int fd = ConnectRemote();
        std::lock_guard lk(mu_);
        remote_slots_[static_cast<size_t>(i)].fd = fd;
      });
    }
    for (auto& t : threads) t.join();
  }

  // Acquire a numbered slot, preferring the machine that is proportionally
  // less busy. Local slots get stable ids so the timeline viewer can render
  // one row per core on each machine.
  SlotAssign AcquireSlot() {
    std::unique_lock lk(mu_);
    for (;;) {
      // Balance proportionally between local and remote capacity. Widen to
      // 64-bit before multiplying so large --local-jobs/--remote-jobs values
      // cannot overflow signed int (undefined behavior).
      bool prefer_remote = (static_cast<int64_t>(remote_active_) * local_cap_ <=
                            static_cast<int64_t>(local_active_) * remote_cap_);
      if (prefer_remote) {
        int idx = FindFreeRemoteLocked();
        if (idx >= 0) {
          remote_slots_[static_cast<size_t>(idx)].busy = true;
          ++remote_active_;
          return {false, idx};
        }
      }
      int lidx = FindFreeLocalLocked();
      if (lidx >= 0) {
        local_busy_[static_cast<size_t>(lidx)] = true;
        ++local_active_;
        return {true, lidx};
      }
      int idx = FindFreeRemoteLocked();
      if (idx >= 0) {
        remote_slots_[static_cast<size_t>(idx)].busy = true;
        ++remote_active_;
        return {false, idx};
      }
      cv_.wait(lk);
    }
  }

  void ReleaseLocal(int idx) {
    std::lock_guard lk(mu_);
    local_busy_[static_cast<size_t>(idx)] = false;
    --local_active_;
    cv_.notify_one();
  }

  void ReleaseRemote(int idx, int new_fd) {
    std::lock_guard lk(mu_);
    remote_slots_[static_cast<size_t>(idx)].fd = new_fd;
    remote_slots_[static_cast<size_t>(idx)].busy = false;
    --remote_active_;
    cv_.notify_one();
  }

  int GetRemoteFd(int idx) {
    std::lock_guard lk(mu_);
    return remote_slots_[static_cast<size_t>(idx)].fd;
  }

  int ConnectRemote() const {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    distcomp::TuneTcpSocket(fd);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(remote_port_);
    if (::inet_pton(AF_INET, remote_ip_.c_str(), &addr.sin_addr) != 1) {
      ::close(fd);
      return -1;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      ::close(fd);
      return -1;
    }
    return fd;
  }

 private:
  int FindFreeRemoteLocked() const {
    for (size_t i = 0; i < remote_slots_.size(); ++i) {
      if (!remote_slots_[i].busy) return static_cast<int>(i);
    }
    return -1;
  }

  int FindFreeLocalLocked() const {
    for (size_t i = 0; i < local_busy_.size(); ++i) {
      if (!local_busy_[i]) return static_cast<int>(i);
    }
    return -1;
  }

  std::string remote_ip_;
  uint16_t remote_port_;
  std::string token_;
  int local_cap_;
  int remote_cap_;
  std::mutex mu_;
  std::condition_variable cv_;
  int local_active_ = 0;
  int remote_active_ = 0;
  std::vector<RemoteSlot> remote_slots_;
  std::vector<bool> local_busy_;
};

bool WriteFileAtomic(const std::string& path, const std::string& bytes) {
  // mkstemp gives each output a unique temporary name, so concurrent writers
  // (or a recycled PID with a leftover .tmp file) cannot clobber each other.
  std::string tmpl = path + ".tmp.XXXXXX";
  std::vector<char> name(tmpl.begin(), tmpl.end());
  name.push_back('\0');
  int fd = ::mkstemp(name.data());
  if (fd < 0) return false;
  const std::string tmp(name.data());
  bool ok =
      bytes.empty() || distcomp::WriteAllFd(fd, bytes.data(), bytes.size());
  ::close(fd);
  if (!ok) {
    ::unlink(tmp.c_str());
    return false;
  }
  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    ::unlink(tmp.c_str());
    return false;
  }
  return true;
}

bool IsDistributableCompile(const std::vector<std::string>& argv,
                            std::string& o_path, std::string& d_path) {
  bool has_c = false;
  for (size_t i = 1; i < argv.size(); ++i) {
    const auto& a = argv[i];
    if (a == "-c") {
      has_c = true;
    } else if (a == "-E" || a == "-S" || a == "-M" || a == "-MM" ||
               a.rfind("@", 0) == 0 || a == "-x") {
      return false;
    } else if (a == "-o" && i + 1 < argv.size()) {
      o_path = argv[++i];
    } else if (a.rfind("-o", 0) == 0 && a.size() > 2) {
      o_path = a.substr(2);
    } else if (a == "-MF" && i + 1 < argv.size()) {
      d_path = argv[++i];
    } else if (a.rfind("-MF", 0) == 0 && a.size() > 3) {
      d_path = a.substr(3);
    }
  }
  return has_c && !o_path.empty();
}

void HandleLocalClient(Coordinator& coord, int ufd) {
  const int64_t t_recv_us = NowUs();
  std::string req;
  if (!distcomp::RecvFrame(ufd, req)) {
    ::close(ufd);
    return;
  }
  // The local UNIX-socket hop is trusted; the remote worker is not. Prepend
  // the shared secret as the first field of the TCP frame so the worker can
  // authenticate before it feeds cwd/argv to execvp.
  std::string forward_req;
  distcomp::AppendStr(forward_req, coord.Token());
  forward_req.append(req);
  const auto* p = reinterpret_cast<const uint8_t*>(req.data());
  const auto* end = p + req.size();
  std::string cwd;
  uint32_t argc = 0;
  if (!distcomp::ReadStr(p, end, cwd) || !distcomp::ReadU32(p, end, argc)) {
    ::close(ufd);
    return;
  }
  // The local client supplies argc; cap it before allocating so a corrupt or
  // hostile frame cannot force a GiB-sized allocation.
  constexpr uint32_t kMaxArgv = 100000;
  if (argc > kMaxArgv) {
    ::close(ufd);
    return;
  }
  std::vector<std::string> argv(argc);
  for (uint32_t i = 0; i < argc; ++i) {
    if (!distcomp::ReadStr(p, end, argv[i])) {
      ::close(ufd);
      return;
    }
  }

  std::string src;
  for (const auto& a : argv) {
    if (!a.empty() && a[0] != '-' &&
        (a.ends_with(".cpp") || a.ends_with(".cc") || a.ends_with(".cxx") ||
         a.ends_with(".c") || a.ends_with(".C"))) {
      src = a;
    }
  }

  auto trace = [&](const char* kind, const char* mach, int slot,
                   int64_t t_acq_us, int64_t t_end_us, int exit_code,
                   int64_t o_bytes, const std::string& out, const char* note) {
    std::string j = "{\"ev\":\"job\",\"kind\":\"";
    j += kind;
    j += "\",\"mach\":\"";
    j += mach;
    j += "\",\"slot\":";
    j += std::to_string(slot);
    j += ",\"cwd\":\"" + JsonEsc(cwd) + "\"";
    j += ",\"out\":\"" + JsonEsc(out) + "\"";
    j += ",\"src\":\"" + JsonEsc(src) + "\"";
    j += ",\"recv_us\":" + std::to_string(t_recv_us);
    j += ",\"acq_us\":" + std::to_string(t_acq_us);
    j += ",\"end_us\":" + std::to_string(t_end_us);
    j += ",\"dur_us\":" + std::to_string(t_end_us - t_recv_us);
    j += ",\"exit\":" + std::to_string(exit_code);
    j += ",\"o_bytes\":" + std::to_string(o_bytes);
    if (note) j += std::string(",\"note\":\"") + JsonEsc(note) + "\"";
    j += "}";
    TraceEvent(j);
  };

  auto wait_client_close = [&] { WaitClientClose(ufd); };

  std::string o_path;
  std::string d_path;
  if (!IsDistributableCompile(argv, o_path, d_path)) {
    std::string resp;
    resp.push_back(0);  // 0 = run locally
    distcomp::SendFrame(ufd, resp);
    wait_client_close();
    trace("plain", "local", -1, t_recv_us, NowUs(), 0, 0, o_path, nullptr);
    ::close(ufd);
    return;
  }

  Coordinator::SlotAssign slot = coord.AcquireSlot();
  const int64_t t_acq_us = NowUs();
  if (slot.local) {
    std::string resp;
    resp.push_back(0);  // 0 = run locally (slot held until client closes ufd!)
    distcomp::SendFrame(ufd, resp);
    // Wait for client to finish local compile and close the socket.
    wait_client_close();
    const int64_t t_end_us = NowUs();
    coord.ReleaseLocal(slot.idx);
    trace("compile", "local", slot.idx, t_acq_us, t_end_us, 0, 0, o_path,
          nullptr);
    ::close(ufd);
    return;
  }

  // Remote execution on EVO-X2.
  int rfd = coord.GetRemoteFd(slot.idx);
  if (rfd < 0) {
    rfd = coord.ConnectRemote();
  }

  std::string remote_resp;
  bool remote_ok = false;
  if (rfd >= 0) {
    if (distcomp::SendFrame(rfd, forward_req) &&
        distcomp::RecvFrame(rfd, remote_resp)) {
      remote_ok = true;
    } else {
      ::close(rfd);
      rfd = coord.ConnectRemote();
      if (rfd >= 0 && distcomp::SendFrame(rfd, forward_req) &&
          distcomp::RecvFrame(rfd, remote_resp)) {
        remote_ok = true;
      } else if (rfd >= 0) {
        ::close(rfd);
        rfd = -1;
      }
    }
  }

  if (!remote_ok) {
    coord.ReleaseRemote(slot.idx, rfd);
    // Fallback to local cleanly without error.
    std::string resp;
    resp.push_back(0);
    distcomp::SendFrame(ufd, resp);
    wait_client_close();
    trace("compile", "local", -1, t_recv_us, NowUs(), 0, 0, o_path,
          "remote-fallback");
    ::close(ufd);
    return;
  }
  coord.ReleaseRemote(slot.idx, rfd);
  const int64_t t_remote_end_us = NowUs();

  const auto* rp = reinterpret_cast<const uint8_t*>(remote_resp.data());
  const auto* rend = rp + remote_resp.size();
  int32_t exit_code = 1;
  std::string out_str, err_str, o_bytes, d_bytes;
  if (!distcomp::ReadI32(rp, rend, exit_code) ||
      !distcomp::ReadStr(rp, rend, out_str) ||
      !distcomp::ReadStr(rp, rend, err_str) ||
      !distcomp::ReadStr(rp, rend, o_bytes) ||
      !distcomp::ReadStr(rp, rend, d_bytes)) {
    std::string resp;
    resp.push_back(0);
    distcomp::SendFrame(ufd, resp);
    wait_client_close();
    trace("compile", "local", -1, t_recv_us, t_remote_end_us, 0, 0, o_path,
          "bad-response-fallback");
    ::close(ufd);
    return;
  }
  trace("compile", "remote", slot.idx, t_acq_us, t_remote_end_us, exit_code,
        static_cast<int64_t>(o_bytes.size()), o_path, nullptr);

  if (exit_code == 0) {
    fs::path abs_o = fs::path(o_path).is_absolute() ? fs::path(o_path)
                                                    : (fs::path(cwd) / o_path);
    if (!WriteFileAtomic(abs_o.string(), o_bytes)) {
      std::string resp;
      resp.push_back(0);
      distcomp::SendFrame(ufd, resp);
      wait_client_close();
      trace("compile", "local", -1, t_recv_us, NowUs(), 1,
            static_cast<int64_t>(o_bytes.size()), o_path,
            "write-object-failed");
      ::close(ufd);
      return;
    }
    if (!d_path.empty()) {
      fs::path abs_d = fs::path(d_path).is_absolute()
                           ? fs::path(d_path)
                           : (fs::path(cwd) / d_path);
      if (!WriteFileAtomic(abs_d.string(), d_bytes)) {
        trace("write_depfile_failed", "local", -1, t_recv_us, NowUs(), 0, 0,
              abs_d.string(), "depfile-write-failed");
        // Fall back to a local compile so the client isn't left blocked in
        // RecvFrame (and the client fd is always closed).
        std::string resp;
        resp.push_back(0);
        distcomp::SendFrame(ufd, resp);
        wait_client_close();
        ::close(ufd);
        return;
      }
    }
  }

  std::string resp;
  resp.push_back(1);  // 1 = remote completed
  distcomp::AppendI32(resp, exit_code);
  distcomp::AppendStr(resp, out_str);
  distcomp::AppendStr(resp, err_str);
  distcomp::SendFrame(ufd, resp);
  ::close(ufd);
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  std::string sock_path = "/tmp/distcomp_coord.sock";
  std::string remote_ip = "192.168.3.56";
  uint16_t remote_port = 19876;
  int local_cap = 32;
  int remote_cap = 28;
  std::string trace_path;
  const char* token_env = ::tinylamb::platform::GetEnv("DISTCOMP_TOKEN");
  std::string token = (token_env && *token_env) ? token_env : "";

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--sock" && i + 1 < argc)
      sock_path = argv[++i];
    else if (a == "--remote-ip" && i + 1 < argc)
      remote_ip = argv[++i];
    else if (a == "--remote-port" && i + 1 < argc) {
      int parsed = 0;
      if (!ParseIntArg(argv[++i], &parsed) || parsed < 0 || parsed > 65535) {
        std::fprintf(stderr, "distcomp_coord: invalid --remote-port\n");
        return 2;
      }
      remote_port = static_cast<uint16_t>(parsed);
    } else if (a == "--local-jobs" && i + 1 < argc) {
      if (!ParseIntArg(argv[++i], &local_cap) || local_cap < 0) {
        std::fprintf(stderr, "distcomp_coord: invalid --local-jobs\n");
        return 2;
      }
    } else if (a == "--remote-jobs" && i + 1 < argc) {
      if (!ParseIntArg(argv[++i], &remote_cap) || remote_cap < 0) {
        std::fprintf(stderr, "distcomp_coord: invalid --remote-jobs\n");
        return 2;
      }
    } else if (a == "--trace" && i + 1 < argc)
      trace_path = argv[++i];
    else {
      std::fprintf(stderr,
                   "distcomp_coord: unknown option or missing value: %s\n",
                   a.c_str());
      return 2;
    }
  }

  if (local_cap == 0 && remote_cap == 0) {
    std::fprintf(stderr,
                 "distcomp_coord: --local-jobs and --remote-jobs cannot both "
                 "be 0 (AcquireSlot would wait forever)\n");
    return 2;
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  if (sock_path.size() >= sizeof(addr.sun_path)) {
    std::fprintf(stderr, "distcomp_coord: --sock path too long\n");
    return 1;
  }
  std::memcpy(addr.sun_path, sock_path.c_str(), sock_path.size() + 1);

  if (!trace_path.empty()) {
    g_trace = std::fopen(trace_path.c_str(), "w");
    if (!g_trace) {
      std::perror("fopen --trace");
      return 1;
    }
    TraceEvent("{\"ev\":\"hdr\",\"epoch_us\":" + std::to_string(NowUs()) +
               ",\"local_cap\":" + std::to_string(local_cap) +
               ",\"remote_cap\":" + std::to_string(remote_cap) +
               ",\"remote_ip\":\"" + JsonEsc(remote_ip) +
               "\",\"remote_port\":" + std::to_string(remote_port) + "}");
  }

  if (token.empty()) {
    std::cerr << "distcomp_coord: DISTCOMP_TOKEN is not set; remote jobs will "
                 "be rejected by the worker\n";
  }
  Coordinator coord(remote_ip, remote_port, local_cap, remote_cap, token);
  coord.WarmupConnections();

  // Never unlink a caller-selected socket path: it may belong to a live
  // coordinator or be a regular file. Let bind report EADDRINUSE instead.
  int srv = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (srv < 0) {
    std::perror("socket");
    CloseTrace();
    return 1;
  }
  if (::bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::perror("bind");
    ::close(srv);
    CloseTrace();
    return 1;
  }
  if (::listen(srv, 256) != 0) {
    std::perror("listen");
    ::close(srv);
    CloseTrace();
    return 1;
  }

  std::cout << "distcomp_coord ready on " << sock_path
            << " (local=" << local_cap << ", remote=" << remote_cap << "@"
            << remote_ip << ":" << remote_port << ")" << std::endl;

  std::vector<std::thread> client_threads;
  for (;;) {
    int cfd = ::accept(srv, nullptr, nullptr);
    if (cfd < 0) {
      if (errno == EINTR) continue;
      std::perror("accept");
      break;
    }
    client_threads.emplace_back(HandleLocalClient, std::ref(coord), cfd);
  }
  ::close(srv);
  // HandleLocalClient keeps a reference to coord and may still emit trace
  // events. Join every accepted request before either object leaves scope.
  for (std::thread& thread : client_threads) {
    if (thread.joinable()) thread.join();
  }
  CloseTrace();
  return 0;
}
