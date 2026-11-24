#include "common/platform/process.hpp"
#include "protocol.hpp"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

namespace {

[[noreturn]] void ExecLocal(int argc, char** argv) {
  std::vector<char*> args;
  args.reserve(static_cast<size_t>(argc));
  for (int i = 1; i < argc; ++i) args.push_back(argv[i]);
  args.push_back(nullptr);
  ::execvp(args[0], args.data());
  std::perror("execvp");
  std::exit(127);
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  if (argc < 2) {
    std::cerr << "Usage: distcomp <compiler> [args...]\n";
    return 1;
  }

  const char* sock_env = ::tinylamb::platform::GetEnv("DISTCOMP_SOCK");
  std::string sock_path = (sock_env && *sock_env) ? sock_env : "/tmp/distcomp_coord.sock";

  sockaddr_un addr{};
  if (sock_path.size() >= sizeof(addr.sun_path)) {
    std::cerr << "DISTCOMP_SOCK is too long, using local compiler\n";
    ExecLocal(argc, argv);
  }
  addr.sun_family = AF_UNIX;
  std::memcpy(addr.sun_path, sock_path.c_str(), sock_path.size() + 1);

  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) ExecLocal(argc, argv);

  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    ExecLocal(argc, argv);
  }

  char cwd_buf[4096];
  std::string cwd = ::getcwd(cwd_buf, sizeof(cwd_buf)) ? cwd_buf : "";

  std::string req;
  distcomp::AppendStr(req, cwd);
  distcomp::AppendU32(req, static_cast<uint32_t>(argc - 1));
  for (int i = 1; i < argc; ++i) {
    distcomp::AppendStr(req, argv[i]);
  }

  if (!distcomp::SendFrame(fd, req, false)) {
    ::close(fd);
    ExecLocal(argc, argv);
  }

  std::string resp;
  if (!distcomp::RecvFrame(fd, resp) || resp.empty()) {
    ::close(fd);
    ExecLocal(argc, argv);
  }

  if (resp[0] == 0) {
    // Keep the fd open while running locally: the coordinator tracks the local
    // slot until the fd closes, so execvp must not set CLOEXEC. When clang++
    // exits, the OS closes the fd.
    ExecLocal(argc, argv);
  }

  ::close(fd);
  const auto* p = reinterpret_cast<const uint8_t*>(resp.data() + 1);
  const auto* end = reinterpret_cast<const uint8_t*>(resp.data() + resp.size());
  int32_t exit_code = 1;
  std::string out_str, err_str;
  if (!distcomp::ReadI32(p, end, exit_code) ||
      !distcomp::ReadStr(p, end, out_str) ||
      !distcomp::ReadStr(p, end, err_str)) {
    // The remote already ran the compile and wrote its .o/.d atomically;
    // re-running locally would double the work and overwrite those files
    // with a different compiler instance than the trace recorded.
    std::fprintf(stderr,
                 "distcomp: malformed coordinator response; remote compile "
                 "result is unknown, refusing to re-run locally\n");
    return 1;
  }

  if (!out_str.empty()) {
    std::cout.write(out_str.data(), static_cast<std::streamsize>(out_str.size()));
  }
  if (!err_str.empty()) {
    std::cerr.write(err_str.data(), static_cast<std::streamsize>(err_str.size()));
  }
  return exit_code;
}
