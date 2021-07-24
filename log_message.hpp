#ifndef TINYLAMB_LOG_MESSAGE_HPP
#define TINYLAMB_LOG_MESSAGE_HPP

#include <sstream>
#include <string>

#define LOG(level) LogMessage(level, __FILE__, __LINE__, __func__).stream()

#define FATAL 9000
#define ERROR 5000
#define WARN 3000
#define INFO 2000
#define DEBUG 1000
#define TRACE 0


class LogMessage;
class LogStream {
  friend class LogMessage;
  LogStream() = default;  // Only LogMessage can construct it.
 public:
  template <typename T>
  LogStream &operator<<(const T &rhs) {
    message_ << rhs;
    return *this;
  }
  ~LogStream();

 private:
  std::stringstream message_;
};

class LogMessage {
 public:
  LogMessage(int log_level, const char *filename, int lineno,
             const char *func_name);
  LogStream &stream() { return ls; }

 private:
  LogStream ls;
};

#endif  // TINYLAMB_LOG_MESSAGE_HPP
