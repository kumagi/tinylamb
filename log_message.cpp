#include "log_message.hpp"
#include <iostream>
#include <chrono>
#include <ctime>

LogStream::~LogStream() { std::cerr << message_.str() << "\e[0;39;49m\n"; }


LogMessage::LogMessage(int log_level, const char *filename, int lineno,
           const char* func_name) {
  char buff[70];
  auto now = std::chrono::system_clock::now();
  std::time_t now_time = std::chrono::system_clock::to_time_t(now);
  std::tm now_tm = *std::localtime(&now_time);
  strftime(buff, sizeof(buff), "%Y-%m-%d %H:%M:%S ", &now_tm);

  switch (log_level) {
    case FATAL:
      ls << "\e[1;31m";
      break;
    case ERROR:
      ls << "\e[31m";
      break;
    case WARN:
      ls << "\e[33m";
      break;
    case INFO:
      break;  // Do nothing.
    case DEBUG:
      ls <<"\e[34m";
      break;
    case TRACE:
      ls <<"\e[4;36m";
      break;
    default:
      break;
  }
  ls << buff << filename << ":" << lineno << " " << func_name;
  switch (log_level) {
    case FATAL:
      ls << " FATAL ";
      break;
    case ERROR:
      ls << " ERROR ";
      break;
    case WARN:
      ls << " WARN ";
      break;
    case INFO:
      ls << " INFO ";
      break;
    case DEBUG:
      ls << " DEBUG ";
      break;
    case TRACE:
      ls << " TRACE ";
      break;
    default:
      ls << "UNKNOWN LOG LEVEL ";
  }
  ls << " - ";
}