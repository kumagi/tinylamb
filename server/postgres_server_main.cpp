/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

#include "recovery/recovery_manager.hpp"
#include "server/postgres_server.hpp"

namespace {

// Atomic so the signal handler reads it without a data race; RequestStop()
// itself only performs an atomic store and a write(), which are
// async-signal-safe.
std::atomic<tinylamb::PostgresServer*> server{nullptr};

void StopServer(int /*signal*/) {
  tinylamb::PostgresServer* instance = server.load(std::memory_order_relaxed);
  if (instance != nullptr) {
    instance->RequestStop();
  }
}

void Usage(std::ostream& output) {
  output << "usage: tinylamb_server <database-file> [--host ADDRESS] "
            "[--port PORT] [--read-workers COUNT] "
            "[--max-connections COUNT] [--force]\n"
         << "  --host ADDRESS          listen address (default 127.0.0.1)\n"
         << "  --port PORT             TCP port (1..65535)\n"
         << "  --read-workers COUNT    reader threads (1..1024)\n"
         << "  --max-connections COUNT max clients (1..65536; env "
            "TINYLAMB_MAX_CONNECTIONS)\n"
         << "  --force                 truncate a torn WAL tail before opening "
            "(destructive)\n"
         << "  idle_timeout is 3600s (fixed; not configurable via CLI)\n";
}

bool ParseUnsigned(std::string_view input, unsigned long* result) {
  if (input.empty() || input.front() == '-' || input.front() == '+') {
    return false;
  }
  char* end = nullptr;
  const std::string value(input);
  errno = 0;
  const unsigned long parsed = std::strtoul(value.c_str(), &end, 10);
  if (errno != 0 || end == value.c_str() || *end != '\0') {
    return false;
  }
  *result = parsed;
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    Usage(std::cerr);
    return 2;
  }
  // argv[1] is the database file; recognize --help/-h before it is consumed
  // as a filename (the flag loop below starts at argv[2]).
  if (const std::string_view first(argv[1]);
      first == "--help" || first == "-h") {
    Usage(std::cout);
    return 0;
  }
  tinylamb::PostgresServerOptions options;
  // Connection cap can be preset through the environment; the command line
  // flag overrides it. Connections beyond the limit are shed on accept.
  if (const char* env = std::getenv("TINYLAMB_MAX_CONNECTIONS");
      env != nullptr && env[0] != '\0') {
    unsigned long connections = 0;
    // P382: the flag path validates 1..65536; the env preset must apply the
    // same bound (a huge value silently defeats the accept-time shed).
    if (!ParseUnsigned(env, &connections) || connections == 0 ||
        connections > 65536) {
      std::cerr << "invalid TINYLAMB_MAX_CONNECTIONS (expected 1..65536)\n";
      return 2;
    }
    options.max_connections = static_cast<size_t>(connections);
  }
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
    } else if (argument == "--max-connections" && i + 1 < argc) {
      unsigned long connections = 0;
      if (!ParseUnsigned(argv[++i], &connections) || connections == 0 ||
          connections > 65536) {
        std::cerr << "invalid max connection count\n";
        return 2;
      }
      options.max_connections = static_cast<size_t>(connections);
    } else if (argument == "--force") {
      // Truncate a torn/unparsable WAL tail to its intact prefix instead of
      // treating the corruption as fatal (the default).
      tinylamb::RecoveryManager::SetTornTailTruncationAllowed(true);
      options.force_recovery = true;
    } else if (argument == "--help") {
      Usage(std::cout);
      return 0;
    } else {
      std::cerr << "unknown argument: " << argument << '\n';
      Usage(std::cerr);
      return 2;
    }
  }

  std::unique_ptr<tinylamb::PostgresServer> instance;
  // P2090: install the signal handlers BEFORE constructing the server.
  // The constructor opens and recovers the database -- a WAL replay can
  // take seconds -- and with the default disposition an interrupt in that
  // window killed the process outright, skipping the clean shutdown that
  // joins the WAL logger and checkpoint workers. StopServer tolerates the
  // still-null pointer. (RequestStop()'s waker write is async-signal-safe
  // on POSIX; on Windows the loopback send is best-effort -- recorded here
  // so the comment above does not overstate the guarantee.)
  if (std::signal(SIGINT, StopServer) == SIG_ERR ||
      std::signal(SIGTERM, StopServer) == SIG_ERR) {
    std::cerr << "failed to install signal handlers; Ctrl-C will not stop "
                 "the server cleanly\n";
    return 1;
  }
  // The constructor opens (and recovers) the database; a startup failure is
  // reported by Listen() below instead of terminating the process.
  instance = std::make_unique<tinylamb::PostgresServer>(argv[1], options);
  std::string error;
  if (!instance->Listen(&error)) {
    std::cerr << "server startup failed: " << error << '\n';
    return 1;
  }
  server.store(instance.get(), std::memory_order_relaxed);
  std::cout << "tinylamb PostgreSQL server listening on "
            << options.listen_address << ':' << instance->BoundPort()
            << " with " << instance->ReadWorkerCount() << " read workers\n";
  const int result = instance->Run(&error);
  server.store(nullptr, std::memory_order_relaxed);
  if (result != 0) {
    std::cerr << "server failed: " << error << '\n';
  }
  return result;
}
