/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

#include "server/postgres_server.hpp"

namespace {

tinylamb::PostgresServer* server = nullptr;

void StopServer(int /*signal*/) {
  if (server != nullptr) server->RequestStop();
}

void Usage(std::ostream& output) {
  output << "usage: tinylamb_server <database-file> [--host ADDRESS] "
            "[--port PORT] [--read-workers COUNT]\n";
}

bool ParseUnsigned(std::string_view input, unsigned long* result) {
  char* end = nullptr;
  const std::string value(input);
  const unsigned long parsed = std::strtoul(value.c_str(), &end, 10);
  if (end == value.c_str() || *end != '\0') return false;
  *result = parsed;
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    Usage(std::cerr);
    return 2;
  }
  tinylamb::PostgresServerOptions options;
  for (int i = 2; i < argc; ++i) {
    const std::string_view argument(argv[i]);
    if (argument == "--host" && i + 1 < argc) {
      options.listen_address = argv[++i];
    } else if (argument == "--port" && i + 1 < argc) {
      unsigned long port = 0;
      if (!ParseUnsigned(argv[++i], &port) ||
          port > std::numeric_limits<uint16_t>::max()) {
        std::cerr << "invalid port\n";
        return 2;
      }
      options.port = static_cast<uint16_t>(port);
    } else if (argument == "--read-workers" && i + 1 < argc) {
      unsigned long workers = 0;
      if (!ParseUnsigned(argv[++i], &workers) || workers == 0 ||
          workers > 1024) {
        std::cerr << "invalid read worker count\n";
        return 2;
      }
      options.read_worker_threads = static_cast<size_t>(workers);
    } else if (argument == "--help") {
      Usage(std::cout);
      return 0;
    } else {
      std::cerr << "unknown argument: " << argument << '\n';
      Usage(std::cerr);
      return 2;
    }
  }

  tinylamb::PostgresServer instance(argv[1], options);
  std::string error;
  if (!instance.Listen(&error)) {
    std::cerr << "server startup failed: " << error << '\n';
    return 1;
  }
  server = &instance;
  std::signal(SIGINT, StopServer);
  std::signal(SIGTERM, StopServer);
  std::cout << "tinylamb PostgreSQL server listening on "
            << options.listen_address << ':' << instance.BoundPort() << " with "
            << instance.ReadWorkerCount() << " read workers\n";
  const int result = instance.Run(&error);
  server = nullptr;
  if (result != 0) std::cerr << "server failed: " << error << '\n';
  return result;
}
