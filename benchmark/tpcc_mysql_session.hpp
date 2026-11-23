/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_BENCHMARK_TPCC_MYSQL_SESSION_HPP
#define TINYLAMB_BENCHMARK_TPCC_MYSQL_SESSION_HPP

#include <mysql.h>

#include <charconv>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "benchmark/tpcc_workload.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Benchmark transport only: COM_QUERY, text rows, literal SQL, no prepared
// statements or retries. The transaction generator is shared with Tiny/PG.
class TpccMySqlSession final : public TpccSqlSession {
 public:
  ~TpccMySqlSession() override {
    if (connection_) mysql_close(connection_);
    if (thread_initialized_) mysql_thread_end();
  }

  Status Connect(int port) {
    if (connection_ != nullptr) {
      mysql_close(connection_);
      connection_ = nullptr;
    }
    if (!thread_initialized_) {
      if (mysql_thread_init() != 0) return Status::kIOError;
      thread_initialized_ = true;
    }
    connection_ = mysql_init(nullptr);
    if (!connection_) return Status::kIOError;
    unsigned int timeout = 15;
    const enum mysql_ssl_mode ssl_mode = SSL_MODE_DISABLED;
    if (mysql_options(connection_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout) != 0 ||
        mysql_options(connection_, MYSQL_OPT_READ_TIMEOUT, &timeout) != 0 ||
        mysql_options(connection_, MYSQL_OPT_WRITE_TIMEOUT, &timeout) != 0 ||
        mysql_options(connection_, MYSQL_OPT_SSL_MODE, &ssl_mode) != 0 ||
        mysql_options(connection_, MYSQL_SET_CHARSET_NAME, "utf8mb4") != 0) {
      error_ = mysql_error(connection_);
      return StatusError(StatusCode::kIOError, "MySQL client option failed");
    }
    if (!mysql_real_connect(connection_, "127.0.0.1", "root", "", "bench",
                            static_cast<unsigned int>(port), nullptr, 0)) {
      error_ = mysql_error(connection_);
      return Status::kIOError;
    }
    std::vector<Row> ignored;
    return Execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                   &ignored);
  }
  Status Begin(bool read_only) override {
    std::vector<Row> ignored;
    return Execute(read_only ? "START TRANSACTION READ ONLY" : "BEGIN",
                   &ignored);
  }
  Status Commit() override {
    std::vector<Row> ignored;
    return Execute("COMMIT", &ignored);
  }
  void Abort() override {
    std::vector<Row> ignored;
    const Status rollback_status = Execute("ROLLBACK", &ignored);
    if (!rollback_status.ok()) {
      const std::string detail = error_;
      error_ = "ROLLBACK failed: " +
               std::string(ToString(rollback_status)) +
               (detail.empty() ? std::string() : ": " + detail);
      rollback_failed_ = true;
    }
  }
  std::string_view LastError() const override { return error_; }

  Status Execute(std::string_view sql, std::vector<Row>* rows) override {
    rows->clear();
    if (rollback_failed_) {
      return StatusError(StatusCode::kIOError, error_);
    }
    error_.clear();
    if (mysql_real_query(connection_, sql.data(),
                         static_cast<unsigned long>(sql.size())) != 0) {
      const unsigned int code = mysql_errno(connection_);
      error_ = std::to_string(code) + ": " + mysql_error(connection_);
      // Only transactional lock failures are retryable conflicts. Syntax,
      // permission, connection, and other server errors must not be reported
      // as conflicts to the shared TPC-C harness.
      if (code == ER_LOCK_DEADLOCK || code == ER_LOCK_WAIT_TIMEOUT) {
        return Status::kConflicts;
      }
      return StatusError(StatusCode::kRuntimeError, error_);
    }
    std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> result(
        mysql_store_result(connection_), mysql_free_result);
    if (!result) {
      if (mysql_field_count(connection_) != 0) {
        error_ = mysql_error(connection_);
        return Status::kIOError;
      }
      if (sql.starts_with("INSERT ") || sql.starts_with("UPDATE ") ||
          sql.starts_with("DELETE ")) {
        const auto affected = mysql_affected_rows(connection_);
        if (affected > static_cast<unsigned long long>(
                           std::numeric_limits<int64_t>::max()))
          return Status::kCorrupt;
        rows->emplace_back(
            std::vector<Value>{Value(std::string("affected")),
                               Value(static_cast<int64_t>(affected))});
      }
      return Status::kSuccess;
    }
    const auto count = mysql_num_fields(result.get());
    const MYSQL_FIELD* fields = mysql_fetch_fields(result.get());
    while (MYSQL_ROW row = mysql_fetch_row(result.get())) {
      const unsigned long* lengths = mysql_fetch_lengths(result.get());
      if (!lengths) return Status::kCorrupt;
      std::vector<Value> values;
      values.reserve(count);
      for (unsigned int i = 0; i < count; ++i) {
        if (!row[i]) {
          values.emplace_back();
          continue;
        }
        const std::string_view text(row[i], lengths[i]);
        switch (fields[i].type) {
          case MYSQL_TYPE_TINY:
          case MYSQL_TYPE_SHORT:
          case MYSQL_TYPE_LONG:
          case MYSQL_TYPE_INT24:
          case MYSQL_TYPE_LONGLONG:
          case MYSQL_TYPE_YEAR: {
            int64_t value = 0;
            const auto [end, ec] =
                std::from_chars(text.data(), text.data() + text.size(), value);
            if (ec != std::errc() || end != text.data() + text.size())
              return Status::kCorrupt;
            values.emplace_back(value);
            break;
          }
          case MYSQL_TYPE_FLOAT:
          case MYSQL_TYPE_DOUBLE:
          case MYSQL_TYPE_DECIMAL:
          case MYSQL_TYPE_NEWDECIMAL: {
            double value = 0;
            const auto [end, ec] =
                std::from_chars(text.data(), text.data() + text.size(), value);
            if (ec != std::errc() || end != text.data() + text.size())
              return Status::kCorrupt;
            values.emplace_back(value);
            break;
          }
          default:
            values.emplace_back(std::string(text));
        }
      }
      rows->emplace_back(std::move(values));
    }
    if (mysql_errno(connection_) != 0) {
      error_ = mysql_error(connection_);
      return Status::kIOError;
    }
    return Status::kSuccess;
  }

 private:
  MYSQL* connection_{nullptr};
  bool thread_initialized_{false};
  bool rollback_failed_{false};
  std::string error_;
};
}  // namespace tinylamb
#endif
