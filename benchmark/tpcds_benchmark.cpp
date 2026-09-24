/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */

// TPC-DS subset benchmark: generates the tpcds_fixture.hpp schema in-process
// (no external dsdgen dependency), loads it through Table::Insert, ANALYZEs,
// then times the ported queries in tpcds_queries.hpp.
//
//   tinylamb_tpcds_benchmark <new-database-path> [--scale N] [--runs N]
//                             [--query qNN ...] [--plan]
//
// This harness is for engine workload measurement; it is not an official
// TPC-DS result publication.

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "benchmark/tpcds_fixture.hpp"
#include "benchmark/tpcds_queries.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "recovery/logger.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
  std::string database_path;
  int64_t scale{1};
  int64_t runs{1};
  int64_t batch_rows{10000};
  bool force{false};
  bool dump_plan{false};
  bool audit{false};
  std::vector<std::string> only_queries;
};

bool ParseInteger(std::string_view text, int64_t* value) {
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [ptr, ec] = std::from_chars(begin, end, *value);
  return ec == std::errc() && ptr == end;
}

bool ParseOptions(int argc, char** argv, Options* options) {
  if (argc < 2) {
    return false;
  }
  options->database_path = argv[1];
  for (int i = 2; i < argc; ++i) {
    const std::string_view argument = argv[i];
    const auto next = [&]() -> std::string_view {
      return i + 1 < argc ? std::string_view(argv[++i]) : std::string_view();
    };
    if (argument == "--scale" || argument == "--sf") {
      if (!ParseInteger(next(), &options->scale) || options->scale < 1) {
        return false;
      }
    } else if (argument == "--runs") {
      if (!ParseInteger(next(), &options->runs) || options->runs < 1) {
        return false;
      }
    } else if (argument == "--batch-rows") {
      if (!ParseInteger(next(), &options->batch_rows) ||
          options->batch_rows < 1) {
        return false;
      }
    } else if (argument == "--force") {
      options->force = true;
    } else if (argument == "--plan") {
      options->dump_plan = true;
    } else if (argument == "--audit") {
      options->audit = true;
    } else if (argument == "--query") {
      const std::string_view name = next();
      if (name.empty()) {
        return false;
      }
      options->only_queries.emplace_back(name);
    } else {
      return false;
    }
  }
  for (const std::string& requested : options->only_queries) {
    const bool known = std::ranges::any_of(
        tinylamb::tpcds::kQueries,
        [&requested](const tinylamb::tpcds::TpcdsQuery& query) {
          return query.name == requested;
        });
    if (!known || std::count(options->only_queries.begin(),
                             options->only_queries.end(), requested) != 1) {
      return false;
    }
  }
  return !options->database_path.empty();
}

bool PrepareDatabasePath(const Options& options, std::string* error) {
  const std::filesystem::path database_path(options.database_path + ".db");
  const std::filesystem::path log_path = options.database_path + ".log";
  const std::filesystem::path checkpoint_path =
      options.database_path + ".last_checkpoint";
  const std::filesystem::path metadata_path = log_path.string() + ".meta";
  std::error_code ec;
  const bool database_exists = std::filesystem::exists(database_path, ec);
  if (ec) {
    *error = "cannot inspect database path: " + ec.message();
    return false;
  }
  const bool log_exists = std::filesystem::exists(log_path, ec);
  if (ec) {
    *error = "cannot inspect WAL path: " + ec.message();
    return false;
  }
  const bool checkpoint_exists = std::filesystem::exists(checkpoint_path, ec);
  if (ec) {
    *error = "cannot inspect checkpoint path: " + ec.message();
    return false;
  }
  const bool metadata_exists = std::filesystem::exists(metadata_path, ec);
  if (ec) {
    *error = "cannot inspect WAL metadata path: " + ec.message();
    return false;
  }
  if (!options.force &&
      (database_exists || log_exists || metadata_exists || checkpoint_exists)) {
    *error = "database path already exists; pass --force to replace it";
    return false;
  }

  bool has_segment = false;
  const tinylamb::StatusOr<uint64_t> highest =
      tinylamb::Logger::FindHighestSegment(log_path, &has_segment);
  if (!highest.HasValue()) {
    *error = "cannot inspect WAL segments: " + highest.GetStatus().GetMessage();
    return false;
  }
  const uint64_t last_segment = has_segment ? highest.Value() : 0;
  for (uint64_t index = 0; index <= last_segment; ++index) {
    const std::filesystem::path segment =
        tinylamb::Logger::SegmentPath(log_path, index);
    if (!std::filesystem::remove(segment, ec) && ec) {
      *error = "cannot remove " + segment.string() + ": " + ec.message();
      return false;
    }
  }
  if (!std::filesystem::remove(database_path, ec) && ec) {
    *error = "cannot remove database: " + ec.message();
    return false;
  }
  if (!std::filesystem::remove(checkpoint_path, ec) && ec) {
    *error = "cannot remove checkpoint: " + ec.message();
    return false;
  }
  if (!std::filesystem::remove(metadata_path, ec) && ec) {
    *error = "cannot remove WAL metadata: " + ec.message();
    return false;
  }
  if (!std::filesystem::remove(metadata_path.string() + ".tmp", ec) && ec) {
    *error = "cannot remove WAL metadata temporary file: " + ec.message();
    return false;
  }
  return true;
}

int Usage(std::ostream& out) {
  out << "usage: tinylamb_tpcds_benchmark <new-database-path> [--scale N] "
         "[--runs N] [--batch-rows N] [--query qNN] [--plan] [--audit] "
         "[--force]\n";
  return 2;
}

bool RunDdl(tinylamb::Database& database, std::string_view sql,
            std::string* error) {
  tinylamb::TransactionContext context = database.BeginContext();
  tinylamb::SqlEngine engine(database);
  tinylamb::StatusOr<tinylamb::QueryResult> result =
      engine.Execute(context, sql);
  if (!result.HasValue()) {
    *error = engine.LastError();
    context.Abort();
    return false;
  }
  result.Value().Drain();
  if (result.Value().GetStatus() != tinylamb::Status::kSuccess) {
    *error = result.Value().GetStatus().GetMessage();
    context.Abort();
    return false;
  }
  if (context.PreCommit() != tinylamb::Status::kSuccess) {
    *error = "DDL commit failed";
    return false;
  }
  return true;
}

bool LoadTable(tinylamb::Database& database, const Options& options,
               std::string_view table, const std::vector<tinylamb::Row>& rows,
               std::string* error) {
  tinylamb::TransactionContext context = database.BeginContext();
  tinylamb::StatusOr<tinylamb::Table> found = database.GetTable(context, table);
  if (!found.HasValue()) {
    *error = "cannot open table " + std::string(table);
    context.Abort();
    return false;
  }
  tinylamb::Table destination = std::move(found.Value());
  int64_t in_transaction = 0;
  for (const tinylamb::Row& row : rows) {
    if (!destination.Insert(context.txn_, row).HasValue()) {
      *error = "insert failed for " + std::string(table);
      context.Abort();
      return false;
    }
    if (++in_transaction == options.batch_rows) {
      if (context.PreCommit() != tinylamb::Status::kSuccess) {
        *error = "load commit failed for " + std::string(table);
        return false;
      }
      context = database.BeginContext();
      tinylamb::StatusOr<tinylamb::Table> reopened =
          database.GetTable(context, table);
      if (!reopened.HasValue()) {
        *error = "cannot reopen table " + std::string(table);
        context.Abort();
        return false;
      }
      destination = std::move(reopened.Value());
      in_transaction = 0;
    }
  }
  if (context.PreCommit() != tinylamb::Status::kSuccess) {
    *error = "final load commit failed for " + std::string(table);
    return false;
  }
  std::cout << "loaded_rows." << table << '=' << rows.size() << '\n';
  return true;
}

bool AnalyzeAllTables(tinylamb::Database& database, std::string* error) {
  tinylamb::TransactionContext context = database.BeginContext();
  tinylamb::SqlEngine engine(database);
  tinylamb::StatusOr<tinylamb::QueryResult> result =
      engine.Execute(context, "ANALYZE;");
  if (!result.HasValue()) {
    *error = engine.LastError().empty() ? "ANALYZE failed" : engine.LastError();
    context.Abort();
    return false;
  }
  result.Value().Drain();
  if (result.Value().GetStatus() != tinylamb::Status::kSuccess) {
    *error = result.Value().GetStatus().GetMessage();
    context.Abort();
    return false;
  }
  if (context.PreCommit() != tinylamb::Status::kSuccess) {
    *error = "ANALYZE commit failed";
    return false;
  }
  return true;
}

struct QueryProfile {
  std::string name;
  size_t rows{0};
  double milliseconds{0};
  double plan_milliseconds{0};
  std::string plan;
  std::string error;
};

size_t CountOccurrences(std::string_view text, std::string_view needle) {
  size_t count = 0;
  for (size_t position = text.find(needle); position != std::string_view::npos;
       position = text.find(needle, position + needle.size())) {
    ++count;
  }
  return count;
}

size_t MetricValue(std::string_view text, std::string_view metric) {
  size_t position = text.find(metric);
  if (position == std::string_view::npos) {
    return 0;
  }
  position += metric.size();
  size_t value = 0;
  while (position < text.size() && text[position] >= '0' &&
         text[position] <= '9') {
    value = value * 10 + static_cast<size_t>(text[position] - '0');
    ++position;
  }
  return value;
}

QueryProfile RunQuery(tinylamb::Database& database, std::string_view name,
                      std::string_view sql, bool dump_plan) {
  QueryProfile profile;
  profile.name = std::string(name);
  tinylamb::TransactionContext context = database.BeginReadOnlyContext();
  tinylamb::SqlEngine engine(database);
  tinylamb::SqlEngine::SetThreadRuntimeProfiling(true);
  const Clock::time_point begin = Clock::now();
  tinylamb::StatusOr<tinylamb::QueryResult> result =
      engine.Execute(context, sql);
  if (!result.HasValue()) {
    profile.error = engine.LastError();
    tinylamb::SqlEngine::SetThreadRuntimeProfiling(false);
    context.Abort();
    return profile;
  }
  profile.rows = result.Value().Drain();
  profile.milliseconds =
      std::chrono::duration<double, std::milli>(Clock::now() - begin).count();
  if (result.Value().GetStatus() != tinylamb::Status::kSuccess) {
    profile.error = result.Value().GetStatus().GetMessage();
    tinylamb::SqlEngine::SetThreadRuntimeProfiling(false);
    context.Abort();
    return profile;
  }
  // prepare_ns covers SqlEngine::Prepare. Set operations execute every
  // branch inside Prepare, so for ROLLUP this figure is planning plus
  // operand execution; exec below is only the final drain.
  profile.plan_milliseconds =
      static_cast<double>(
          tinylamb::SqlEngine::ThreadRuntimeStats().prepare_ns) /
      1.0e6;
  tinylamb::SqlEngine::SetThreadRuntimeProfiling(false);
  if (dump_plan) {
    std::ostringstream plan;
    result.Value().Dump(plan, 0);
    profile.plan = plan.str();
  }
  if (context.PreCommit() != tinylamb::Status::kSuccess) {
    profile.error = "read transaction failed";
  }
  return profile;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!ParseOptions(argc, argv, &options)) {
    return Usage(std::cerr);
  }

  std::cout << "benchmark=tinylamb_tpcds_subset\n"
            << "scale=" << options.scale << '\n'
            << "database=" << options.database_path << '\n';

  std::string error;
  if (!PrepareDatabasePath(options, &error)) {
    std::cerr << error << '\n';
    return 1;
  }
  tinylamb::StatusOr<std::unique_ptr<tinylamb::Database>> database =
      tinylamb::Database::Create(options.database_path);
  if (!database.HasValue()) {
    std::cerr << "cannot create database: " << database.GetStatus() << '\n';
    return 1;
  }
  tinylamb::Database* db = database.Value().get();

  const Clock::time_point load_begin = Clock::now();
  for (const std::string_view ddl : tinylamb::tpcds::Ddl()) {
    if (!RunDdl(*db, ddl, &error)) {
      std::cerr << "DDL failed: " << error << '\n';
      return 1;
    }
  }
  const tinylamb::tpcds::GeneratedData data =
      tinylamb::tpcds::Generate(options.scale);
  for (const std::string_view ddl : tinylamb::tpcds::Ddl()) {
    static constexpr std::string_view kCreate = "CREATE TABLE ";
    const size_t begin = ddl.find(kCreate) + kCreate.size();
    const size_t end = ddl.find(' ', begin);
    const std::string_view table = ddl.substr(begin, end - begin);
    if (!LoadTable(*db, options, table, tinylamb::tpcds::RowsOf(data, table),
                   &error)) {
      std::cerr << "load failed: " << error << '\n';
      return 1;
    }
  }
  if (!AnalyzeAllTables(*db, &error)) {
    std::cerr << error << '\n';
    return 1;
  }
  const double load_seconds =
      std::chrono::duration<double>(Clock::now() - load_begin).count();
  std::cout << "load_seconds=" << load_seconds << '\n';

  double total_ms = 0;
  size_t failed = 0;
  for (const tinylamb::tpcds::TpcdsQuery& query : tinylamb::tpcds::kQueries) {
    if (!options.only_queries.empty() &&
        std::find(options.only_queries.begin(), options.only_queries.end(),
                  query.name) == options.only_queries.end()) {
      continue;
    }
    QueryProfile best;
    best.milliseconds = -1;
    bool any_ok = false;
    bool any_failed = false;
    std::string first_error;
    std::string first_plan;
    for (int64_t run = 0; run < options.runs; ++run) {
      QueryProfile profile =
          RunQuery(*db, query.name, query.sql,
                   (options.dump_plan || options.audit) && run == 0);
      if ((options.dump_plan || options.audit) && run == 0) {
        first_plan = profile.plan;
      }
      if (!profile.error.empty()) {
        any_failed = true;
        if (first_error.empty()) {
          first_error = profile.error;
        }
        if (best.milliseconds < 0) {
          best = std::move(profile);
        }
        continue;
      }
      any_ok = true;
      if (best.milliseconds < 0 || profile.milliseconds < best.milliseconds) {
        best = std::move(profile);
      }
    }
    if (!any_ok || any_failed) {
      ++failed;
      std::cout << "TPCDS_PROFILE " << best.name << " ERROR "
                << (first_error.empty() ? best.error : first_error) << '\n';
      continue;
    }
    if (best.plan.empty()) {
      best.plan = std::move(first_plan);
    }
    total_ms += best.milliseconds;
    const double exec_ms =
        std::max(0.0, best.milliseconds - best.plan_milliseconds);
    std::cout << "TPCDS_PROFILE " << best.name << ' ' << std::fixed
              << std::setprecision(1) << best.milliseconds
              << " ms plan=" << best.plan_milliseconds << " exec=" << exec_ms
              << " rows=" << best.rows;
    if (!best.plan.empty() && (!options.audit || options.dump_plan)) {
      std::cout << ' ' << best.plan;
    }
    if (options.audit) {
      const bool grouped_core =
          best.plan.find("GroupByFinish") != std::string::npos;
      const bool relational =
          best.plan.find("RelationalExecutor") != std::string::npos;
      const char* route = "physical_plan";
      if (grouped_core) {
        route = "cascades_grouped_core";
      } else if (relational) {
        route = "relational_fallback";
      } else if (query.sql.find("UNION") != std::string_view::npos ||
                 query.sql.find("ROLLUP") != std::string_view::npos) {
        route = "setop_eager_finish";
      }
      const bool cross_join =
          best.plan.find("Cross Join") != std::string::npos ||
          best.plan.find("CrossJoin") != std::string::npos;
      const size_t nested_loop_joins =
          MetricValue(best.plan, "nested_loop_joins=");
      const size_t adaptive_hash_switches = CountOccurrences(
          best.plan, "AdaptiveJoin initial=NestedLoop final=HashJoin");
      const bool nested_loop = nested_loop_joins > adaptive_hash_switches;
      const bool correlated =
          MetricValue(best.plan, "correlated_index_builds=") > 0;
      std::cout << " route=" << route << " cross_join=" << cross_join
                << " nested_loop=" << nested_loop
                << " adaptive_hash=" << adaptive_hash_switches
                << " correlated=" << correlated;
    }
    std::cout << '\n';
  }
  std::cout << "TPCDS_TOTAL " << std::fixed << std::setprecision(1) << total_ms
            << " ms failed=" << failed << '\n';
  return failed == 0 ? 0 : 1;
}
