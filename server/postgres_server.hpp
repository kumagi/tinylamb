/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_POSTGRES_SERVER_HPP
#define TINYLAMB_POSTGRES_SERVER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace tinylamb {

struct PostgresServerOptions {
  std::string listen_address{"127.0.0.1"};
  uint16_t port{54321};
  int backlog{128};
  size_t max_message_bytes{16U * 1024U * 1024U};
  // Zero selects std::thread::hardware_concurrency().
  size_t read_worker_threads{0};
};

class PostgresServer {
 public:
  PostgresServer(std::string database_path, PostgresServerOptions options);
  ~PostgresServer();

  PostgresServer(const PostgresServer&) = delete;
  PostgresServer& operator=(const PostgresServer&) = delete;

  bool Listen(std::string* error);
  int Run(std::string* error);
  void RequestStop();
  [[nodiscard]] uint16_t BoundPort() const;
  [[nodiscard]] size_t ReadWorkerCount() const;
  [[nodiscard]] size_t PeakConcurrentReadQueries() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_POSTGRES_SERVER_HPP
