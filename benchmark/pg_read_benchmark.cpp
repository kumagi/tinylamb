/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

struct Options {
  std::string host{"127.0.0.1"};
  uint16_t port{54321};
  size_t clients{1};
  size_t warmup_seconds{2};
  size_t measurement_seconds{10};
  std::string user{"tinylamb"};
  std::string database{"tinylamb"};
  std::string query{"SELECT SUM(id) AS checksum FROM read_scale;"};
};

void AppendUint32(std::string* output, uint32_t value) {
  output->push_back(static_cast<char>((value >> 24U) & 0xffU));
  output->push_back(static_cast<char>((value >> 16U) & 0xffU));
  output->push_back(static_cast<char>((value >> 8U) & 0xffU));
  output->push_back(static_cast<char>(value & 0xffU));
}

uint32_t ReadUint32(std::string_view input, size_t offset) {
  const auto* data = reinterpret_cast<const unsigned char*>(input.data());
  return (static_cast<uint32_t>(data[offset]) << 24U) |
         (static_cast<uint32_t>(data[offset + 1]) << 16U) |
         (static_cast<uint32_t>(data[offset + 2]) << 8U) |
         static_cast<uint32_t>(data[offset + 3]);
}

bool SendAll(int fd, const std::string& bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t sent =
        send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    if (sent > 0) {
      offset += static_cast<size_t>(sent);
      continue;
    }
    if (sent < 0 && errno == EINTR) { continue;
}
    return false;
  }
  return true;
}

bool ReadUntilReady(int fd, bool* server_error) {
  std::string input;
  while (true) {
    pollfd descriptor{.fd=fd, .events=POLLIN, .revents=0};
    if (poll(&descriptor, 1, 30000) <= 0) { return false;
}
    std::array<char, 8192> buffer{};
    const ssize_t received = recv(fd, buffer.data(), buffer.size(), 0);
    if (received <= 0) { return false;
}
    input.append(buffer.data(), static_cast<size_t>(received));
    size_t cursor = 0;
    while (cursor + 5 <= input.size()) {
      const uint32_t length = ReadUint32(input, cursor + 1);
      if (length < 4 || cursor + 1 + length > input.size()) { break;
}
      const char type = input[cursor];
      if (type == 'E') { *server_error = true;
}
      cursor += 1 + length;
      if (type == 'Z') { return true;
}
    }
    if (cursor != 0) { input.erase(0, cursor);
}
  }
}

std::string StartupMessage(const Options& options) {
  std::string message;
  AppendUint32(&message, 0);
  AppendUint32(&message, 196608);
  message += "user";
  message.push_back('\0');
  message += options.user;
  message.push_back('\0');
  message += "database";
  message.push_back('\0');
  message += options.database;
  message.append(2, '\0');
  const auto length = static_cast<uint32_t>(message.size());
  message[0] = static_cast<char>((length >> 24U) & 0xffU);
  message[1] = static_cast<char>((length >> 16U) & 0xffU);
  message[2] = static_cast<char>((length >> 8U) & 0xffU);
  message[3] = static_cast<char>(length & 0xffU);
  return message;
}

std::string QueryMessage(std::string_view query) {
  std::string message(1, 'Q');
  AppendUint32(&message, static_cast<uint32_t>(query.size() + 5));
  message.append(query);
  message.push_back('\0');
  return message;
}

int Connect(const Options& options) {
  const int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) { return -1;
}
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(options.port);
  if (inet_pton(AF_INET, options.host.c_str(), &address.sin_addr) != 1 ||
      connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
          0 ||
      !SendAll(fd, StartupMessage(options))) {
    close(fd);
    return -1;
  }
  bool server_error = false;
  if (!ReadUntilReady(fd, &server_error) || server_error) {
    close(fd);
    return -1;
  }
  return fd;
}

bool ParsePositive(std::string_view input, size_t* result) {
  char* end = nullptr;
  const std::string value(input);
  const unsigned long parsed = std::strtoul(value.c_str(), &end, 10);
  if (end == value.c_str() || *end != '\0' || parsed == 0) { return false;
}
  *result = static_cast<size_t>(parsed);
  return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
  for (int i = 1; i < argc; ++i) {
    const std::string_view argument(argv[i]);
    if (i + 1 >= argc) { return false;
}
    const std::string_view value(argv[++i]);
    if (argument == "--host") {
      options->host = value;
    } else if (argument == "--port") {
      size_t port = 0;
      if (!ParsePositive(value, &port) ||
          port > std::numeric_limits<uint16_t>::max()) {
        return false;
      }
      options->port = static_cast<uint16_t>(port);
    } else if (argument == "--clients") {
      if (!ParsePositive(value, &options->clients)) { return false;
}
    } else if (argument == "--warmup") {
      if (!ParsePositive(value, &options->warmup_seconds)) { return false;
}
    } else if (argument == "--seconds") {
      if (!ParsePositive(value, &options->measurement_seconds)) { return false;
}
    } else if (argument == "--user") {
      options->user = value;
    } else if (argument == "--database") {
      options->database = value;
    } else if (argument == "--query") {
      options->query = value;
    } else {
      return false;
    }
  }
  return true;
}

void Usage() {
  std::cerr << "usage: tinylamb_pg_read_benchmark [--host ADDRESS] "
               "[--port PORT] [--clients COUNT] [--warmup SECONDS] "
               "[--seconds SECONDS] [--user USER] [--database DATABASE] "
               "[--query SQL]\n";
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!ParseOptions(argc, argv, &options)) {
    Usage();
    return 2;
  }

  std::barrier ready(static_cast<std::ptrdiff_t>(options.clients + 1));
  std::atomic<bool> measuring{false};
  std::atomic<bool> stopping{false};
  std::atomic<uint64_t> completed{0};
  std::atomic<uint64_t> errors{0};
  const std::string query_message = QueryMessage(options.query);
  std::vector<std::thread> clients;
  clients.reserve(options.clients);
  for (size_t i = 0; i < options.clients; ++i) {
    clients.emplace_back([&] {
      const int fd = Connect(options);
      if (fd < 0) {
        errors.fetch_add(1);
        ready.arrive_and_drop();
        return;
      }
      ready.arrive_and_wait();
      while (!stopping.load()) {
        bool server_error = false;
        if (!SendAll(fd, query_message) || !ReadUntilReady(fd, &server_error) ||
            server_error) {
          errors.fetch_add(1);
          break;
        }
        if (measuring.load()) { completed.fetch_add(1);
}
      }
      (void)SendAll(fd, std::string("X\0\0\0\4", 5));
      close(fd);
    });
  }

  ready.arrive_and_wait();
  std::this_thread::sleep_for(std::chrono::seconds(options.warmup_seconds));
  completed.store(0);
  measuring.store(true);
  const auto start = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(
      std::chrono::seconds(options.measurement_seconds));
  const auto end = std::chrono::steady_clock::now();
  measuring.store(false);
  stopping.store(true);
  for (std::thread& client : clients) { client.join();
}
  const double elapsed = std::chrono::duration<double>(end - start).count();
  const uint64_t transactions = completed.load();

  std::cout << "clients=" << options.clients << '\n'
            << "warmup_seconds=" << options.warmup_seconds << '\n'
            << "measurement_seconds=" << options.measurement_seconds << '\n'
            << "completed_queries=" << transactions << '\n'
            << "errors=" << errors.load() << '\n'
            << "qps=" << static_cast<double>(transactions) / elapsed << '\n';
  return errors.load() == 0 ? 0 : 1;
}
