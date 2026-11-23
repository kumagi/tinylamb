#include <fcntl.h>
#include <sys/wait.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "protocol.hpp"

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_job_seq{0};
constexpr unsigned kMaxConnections = 256;
std::atomic<unsigned> g_active_connections{0};

// std::stoi throws on malformed input and silently wraps values outside the
// uint16_t range (--port -1 used to listen on 65535). Fail closed instead.
bool ParsePort(const char* text, uint16_t* port) {
  if (text == nullptr || *text == '\0' || *text == '+' || *text == '-') {
    return false;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long value = std::strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || value == 0 ||
      value > std::numeric_limits<uint16_t>::max()) {
    return false;
  }
  *port = static_cast<uint16_t>(value);
  return true;
}

bool ReadFileToString(const std::string& path, std::string& out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  char buf[65536];
  out.clear();
  for (;;) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) continue;
      ::close(fd);
      return false;
    }
    if (n == 0) break;
    out.append(buf, static_cast<size_t>(n));
  }
  ::close(fd);
  return true;
}

std::string ReadAllPipe(int fd) {
  // The coordinator controls which compiler runs on this worker; a wrapper
  // that loops emitting diagnostics must not be able to OOM the host. Keep a
  // bounded prefix and drain (but do not retain) the rest of the pipe.
  constexpr size_t kMaxCapturedBytes = size_t{8} << 20;
  std::string out;
  char buf[16384];
  bool truncated = false;
  for (;;) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (n == 0) break;
    const size_t count = static_cast<size_t>(n);
    if (!truncated && out.size() + count > kMaxCapturedBytes) {
      const size_t room = kMaxCapturedBytes - out.size();
      if (room != 0) {
        out.append(buf, room);
      }
      truncated = true;
    } else if (!truncated) {
      out.append(buf, count);
    }
  }
  if (truncated) {
    out.append("\n[distcomp: compiler output truncated at 8 MiB]\n");
  }
  return out;
}

void HandleClient(int client_fd, const std::string& expected_token) {
  distcomp::TuneTcpSocket(client_fd);
  std::string req;
  while (distcomp::RecvFrame(client_fd, req)) {
    const auto* p = reinterpret_cast<const uint8_t*>(req.data());
    const auto* end = p + req.size();

    // The frame carries cwd/argv that are passed straight to execvp, so
    // authenticate BEFORE parsing any of it. The coordinator prepends the
    // shared secret as the first field on this TCP hop; every other caller is
    // dropped without spawning a process.
    std::string token;
    if (!distcomp::ReadStr(p, end, token) ||
        !distcomp::SecretEquals(token, expected_token)) {
      std::cerr << "distcomp_worker: rejecting unauthenticated request on fd "
                << client_fd << "\n";
      break;
    }

    std::string cwd;
    uint32_t argc = 0;
    if (!distcomp::ReadStr(p, end, cwd) || !distcomp::ReadU32(p, end, argc)) {
      break;
    }
    // argc comes from a frame and, even when authenticated, must not be
    // allowed to force an unbounded allocation before the fields are read.
    constexpr uint32_t kMaxArgv = 100000;
    if (argc > kMaxArgv) break;
    std::vector<std::string> argv(argc);
    bool ok = true;
    for (uint32_t i = 0; i < argc; ++i) {
      if (!distcomp::ReadStr(p, end, argv[i])) {
        ok = false;
        break;
      }
    }
    if (!ok || argv.empty()) break;

    uint64_t seq = g_job_seq.fetch_add(1, std::memory_order_relaxed);
    std::string shm_prefix = "/dev/shm/distcomp_" + std::to_string(::getpid()) +
                             "_" + std::to_string(seq);
    std::string tmp_o = shm_prefix + ".o";
    std::string tmp_d = shm_prefix + ".d";

    bool has_o = false;
    bool has_d = false;
    std::vector<std::string> mod_argv = argv;
    for (size_t i = 1; i < mod_argv.size(); ++i) {
      if (mod_argv[i] == "-o" && i + 1 < mod_argv.size()) {
        mod_argv[i + 1] = tmp_o;
        has_o = true;
        ++i;
      } else if (mod_argv[i].rfind("-o", 0) == 0 && mod_argv[i].size() > 2) {
        mod_argv[i] = "-o" + tmp_o;
        has_o = true;
      } else if (mod_argv[i] == "-MF" && i + 1 < mod_argv.size()) {
        mod_argv[i + 1] = tmp_d;
        has_d = true;
        ++i;
      } else if (mod_argv[i].rfind("-MF", 0) == 0 && mod_argv[i].size() > 3) {
        mod_argv[i] = "-MF" + tmp_d;
        has_d = true;
      }
    }

    int out_pipe[2];
    int err_pipe[2];
    if (::pipe(out_pipe) != 0) {
      break;
    }
    if (::pipe(err_pipe) != 0) {
      // The first pipe was created; close it before leaving the loop.
      ::close(out_pipe[0]);
      ::close(out_pipe[1]);
      break;
    }

    pid_t pid = ::fork();
    if (pid < 0) {
      // Each client runs on its own thread and forks its own compiler. A
      // failed fork must never reach waitpid(pid, ...): pid == -1 would make
      // it reap an unrelated thread's child (or block until one exits).
      const int fork_errno = errno;
      ::close(out_pipe[0]);
      ::close(out_pipe[1]);
      ::close(err_pipe[0]);
      ::close(err_pipe[1]);
      errno = fork_errno;
      std::perror("distcomp_worker: fork");
      std::string error_frame;
      distcomp::AppendI32(error_frame, 1);
      distcomp::AppendStr(error_frame, "");
      distcomp::AppendStr(error_frame, "fork failed");
      distcomp::AppendStr(error_frame, "");
      distcomp::AppendStr(error_frame, "");
      if (!distcomp::SendFrame(client_fd, error_frame)) {
        break;
      }
      continue;
    }
    if (pid == 0) {
      ::close(out_pipe[0]);
      ::close(err_pipe[0]);
      ::dup2(out_pipe[1], STDOUT_FILENO);
      ::dup2(err_pipe[1], STDERR_FILENO);
      ::close(out_pipe[1]);
      ::close(err_pipe[1]);
      if (!cwd.empty()) {
        if (::chdir(cwd.c_str()) != 0) {
          std::perror("chdir");
          ::_exit(127);
        }
      }
      std::vector<char*> c_args;
      c_args.reserve(mod_argv.size() + 1);
      for (auto& s : mod_argv) c_args.push_back(s.data());
      c_args.push_back(nullptr);
      ::execvp(c_args[0], c_args.data());
      std::perror("execvp");
      ::_exit(127);
    }

    ::close(out_pipe[1]);
    ::close(err_pipe[1]);

    if (pid < 0) {
      // fork() failed: never reach waitpid(-1) below — it would reap a child
      // belonging to a sibling compile thread and corrupt its exit status.
      const int fork_errno = errno;
      ::close(out_pipe[0]);
      ::close(err_pipe[0]);
      std::string resp;
      distcomp::AppendI32(resp, 1);
      distcomp::AppendStr(resp, "");
      distcomp::AppendStr(
          resp, std::string("fork failed: ") + std::strerror(fork_errno));
      distcomp::AppendStr(resp, "");
      distcomp::AppendStr(resp, "");
      if (!distcomp::SendFrame(client_fd, resp)) {
        break;
      }
      continue;
    }

    std::string out_str;
    std::string err_str;
    std::thread t_out([&] {
      out_str = ReadAllPipe(out_pipe[0]);
      ::close(out_pipe[0]);
    });
    std::thread t_err([&] {
      err_str = ReadAllPipe(err_pipe[0]);
      ::close(err_pipe[0]);
    });
    t_out.join();
    t_err.join();

    int status = 0;
    pid_t waited;
    do {
      waited = ::waitpid(pid, &status, 0);
    } while (waited < 0 && errno == EINTR);
    int32_t exit_code = 1;
    if (waited < 0) {
      std::perror("waitpid");
    } else if (WIFEXITED(status)) {
      exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      const int signal = WTERMSIG(status);
      std::cerr << "compiler killed by signal " << signal << '\n';
      exit_code = 128 + signal;
    }

    std::string o_bytes;
    std::string d_bytes;
    if (exit_code == 0 && has_o) {
      if (!ReadFileToString(tmp_o, o_bytes)) {
        exit_code = 1;
      }
    }
    if (exit_code == 0 && has_d) {
      if (!ReadFileToString(tmp_d, d_bytes)) {
        exit_code = 1;
      }
    }
    ::unlink(tmp_o.c_str());
    ::unlink(tmp_d.c_str());

    std::string resp;
    resp.reserve(32 + out_str.size() + err_str.size() + o_bytes.size() +
                 d_bytes.size());
    distcomp::AppendI32(resp, exit_code);
    distcomp::AppendStr(resp, out_str);
    distcomp::AppendStr(resp, err_str);
    distcomp::AppendStr(resp, o_bytes);
    distcomp::AppendStr(resp, d_bytes);

    if (!distcomp::SendFrame(client_fd, resp)) {
      break;
    }
  }
  ::close(client_fd);
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  uint16_t port = 19876;
  std::string bind_ip = "0.0.0.0";
  const char* token_env = std::getenv("DISTCOMP_TOKEN");
  std::string expected_token = (token_env && *token_env) ? token_env : "";
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--port" && i + 1 < argc) {
      const char* value = argv[++i];
      if (!ParsePort(value, &port)) {
        std::cerr << "Invalid port: " << value << "\n";
        return 1;
      }
    } else if (arg == "--bind" && i + 1 < argc) {
      bind_ip = argv[++i];
    } else if (arg == "--token" && i + 1 < argc) {
      expected_token = argv[++i];
    }
  }
  // Fail closed: without a shared secret this socket would let any TCP
  // peer choose an arbitrary argv/cwd for execvp (remote code execution).
  if (expected_token.empty()) {
    std::cerr << "distcomp_worker: a shared secret is required; set "
                 "DISTCOMP_TOKEN or pass --token\n";
    return 1;
  }

  int srv = ::socket(AF_INET, SOCK_STREAM, 0);
  if (srv < 0) {
    std::perror("socket");
    return 1;
  }
  int one = 1;
  ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, bind_ip.c_str(), &addr.sin_addr) != 1) {
    std::cerr << "Invalid bind IP: " << bind_ip << "\n";
    ::close(srv);
    return 1;
  }
  if (::bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::perror("bind");
    ::close(srv);
    return 1;
  }
  if (::listen(srv, static_cast<int>(kMaxConnections)) != 0) {
    std::perror("listen");
    ::close(srv);
    return 1;
  }

  std::cout << "distcomp_worker listening on " << bind_ip << ":" << port
            << std::endl;
  for (;;) {
    int cfd = ::accept(srv, nullptr, nullptr);
    if (cfd < 0) {
      if (errno == EINTR) continue;
      std::perror("accept");
      break;
    }
    // Account for connections before authentication, including idle ones.
    if (g_active_connections.fetch_add(1) >= kMaxConnections) {
      g_active_connections.fetch_sub(1);
      ::close(cfd);
      continue;
    }
    try {
      std::thread([cfd, token = expected_token] {
        try {
          HandleClient(cfd, token);
        } catch (const std::exception& error) {
          std::cerr << "distcomp worker: " << error.what() << '\n';
          ::close(cfd);
        } catch (...) {
          ::close(cfd);
        }
        g_active_connections.fetch_sub(1);
      }).detach();
    } catch (const std::exception& error) {
      g_active_connections.fetch_sub(1);
      ::close(cfd);
      std::cerr << "distcomp worker: cannot start client handler: "
                << error.what() << '\n';
    }
  }
  ::close(srv);
  return 0;
}
