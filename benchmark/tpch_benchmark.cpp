/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "benchmark/tpch_queries.hpp"
#include "database/database.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

#ifndef TINYLAMB_TPCH_DBGEN_EXECUTABLE
#define TINYLAMB_TPCH_DBGEN_EXECUTABLE "dbgen"
#endif

#ifndef TINYLAMB_TPCH_DBGEN_CONFIG
#define TINYLAMB_TPCH_DBGEN_CONFIG "."
#endif

#ifndef TINYLAMB_TPCH_DBGEN_COMMIT
#define TINYLAMB_TPCH_DBGEN_COMMIT "external"
#endif

namespace {

using Clock = std::chrono::steady_clock;
namespace fs = std::filesystem;

enum class FieldKind { kInteger, kNumeric, kString, kDate };

struct TableSpec {
  std::string_view name;
  std::string_view ddl;
  std::vector<FieldKind> fields;
  uint64_t sf1_rows;
  bool fixed_size{false};
};

const std::array<TableSpec, 8>& Tables() {
  static const std::array<TableSpec, 8> tables = {{
      {"region",
       "CREATE TABLE region (r_regionkey INT64, r_name STRING, r_comment "
       "STRING);",
       {FieldKind::kInteger, FieldKind::kString, FieldKind::kString},
       5,
       true},
      {"nation",
       "CREATE TABLE nation (n_nationkey INT64, n_name STRING, n_regionkey "
       "INT64, n_comment STRING);",
       {FieldKind::kInteger, FieldKind::kString, FieldKind::kInteger,
        FieldKind::kString},
       25,
       true},
      {"supplier",
       "CREATE TABLE supplier (s_suppkey INT64, s_name STRING, s_address "
       "STRING, s_nationkey INT64, s_phone STRING, s_acctbal NUMERIC, "
       "s_comment STRING);",
       {FieldKind::kInteger, FieldKind::kString, FieldKind::kString,
        FieldKind::kInteger, FieldKind::kString, FieldKind::kNumeric,
        FieldKind::kString},
       10000},
      {"customer",
       "CREATE TABLE customer (c_custkey INT64, c_name STRING, c_address "
       "STRING, c_nationkey INT64, c_phone STRING, c_acctbal NUMERIC, "
       "c_mktsegment STRING, c_comment STRING);",
       {FieldKind::kInteger, FieldKind::kString, FieldKind::kString,
        FieldKind::kInteger, FieldKind::kString, FieldKind::kNumeric,
        FieldKind::kString, FieldKind::kString},
       150000},
      {"part",
       "CREATE TABLE part (p_partkey INT64, p_name STRING, p_mfgr STRING, "
       "p_brand STRING, p_type STRING, p_size INT64, p_container STRING, "
       "p_retailprice NUMERIC, p_comment STRING);",
       {FieldKind::kInteger, FieldKind::kString, FieldKind::kString,
        FieldKind::kString, FieldKind::kString, FieldKind::kInteger,
        FieldKind::kString, FieldKind::kNumeric, FieldKind::kString},
       200000},
      {"partsupp",
       "CREATE TABLE partsupp (ps_partkey INT64, ps_suppkey INT64, "
       "ps_availqty INT64, ps_supplycost NUMERIC, ps_comment STRING);",
       {FieldKind::kInteger, FieldKind::kInteger, FieldKind::kInteger,
        FieldKind::kNumeric, FieldKind::kString},
       800000},
      {"orders",
       "CREATE TABLE orders (o_orderkey INT64, o_custkey INT64, "
       "o_orderstatus STRING, o_totalprice NUMERIC, o_orderdate DATE, "
       "o_orderpriority STRING, o_clerk STRING, o_shippriority INT64, "
       "o_comment STRING);",
       {FieldKind::kInteger, FieldKind::kInteger, FieldKind::kString,
        FieldKind::kNumeric, FieldKind::kDate, FieldKind::kString,
        FieldKind::kString, FieldKind::kInteger, FieldKind::kString},
       1500000},
      {"lineitem",
       "CREATE TABLE lineitem (l_orderkey INT64, l_partkey INT64, "
       "l_suppkey INT64, l_linenumber INT64, l_quantity NUMERIC, "
       "l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, "
       "l_returnflag STRING, l_linestatus STRING, l_shipdate DATE, "
       "l_commitdate DATE, l_receiptdate DATE, l_shipinstruct STRING, "
       "l_shipmode STRING, l_comment STRING);",
       {FieldKind::kInteger, FieldKind::kInteger, FieldKind::kInteger,
        FieldKind::kInteger, FieldKind::kNumeric, FieldKind::kNumeric,
        FieldKind::kNumeric, FieldKind::kNumeric, FieldKind::kString,
        FieldKind::kString, FieldKind::kDate, FieldKind::kDate,
        FieldKind::kDate, FieldKind::kString, FieldKind::kString,
        FieldKind::kString},
       6001215},
  }};
  return tables;
}

struct Options {
  fs::path database_path;
  fs::path data_dir;
  fs::path dbgen{TINYLAMB_TPCH_DBGEN_EXECUTABLE};
  fs::path dbgen_config{TINYLAMB_TPCH_DBGEN_CONFIG};
  double scale_factor{0};
  size_t batch_rows{10000};
  std::optional<size_t> query;
  bool force{false};
  bool reuse_database{false};
  bool generate_only{false};
  bool load_only{false};
};

void Usage(std::ostream& output, std::string_view program) {
  output
      << "usage: " << program
      << " <new-database-path> --scale-factor SF [options]\n"
      << "  --scale-factor SF  DBGEN scale factor (SF1 is about 1 GB)\n"
      << "  --data-dir PATH    .tbl directory (default: <database>.tpch-data)\n"
      << "  --dbgen PATH       override the CMake-built DBGEN executable\n"
      << "  --dbgen-config DIR directory containing dists.dss\n"
      << "  --batch-rows N     rows per load transaction (default: 10000)\n"
      << "  --query N          run only query 1..22\n"
      << "  --generate-only    stop after DBGEN and cardinality validation\n"
      << "  --load-only        stop after loading all eight tables\n"
      << "  --reuse-database   query an already loaded benchmark database\n"
      << "  --force            replace this benchmark database\n";
}

template <typename Integer>
bool ParseInteger(std::string_view text, Integer* destination) {
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed, error] = std::from_chars(begin, end, *destination);
  return error == std::errc() && parsed == end;
}

bool ParseDouble(std::string_view text, double* destination) {
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed, error] = std::from_chars(begin, end, *destination);
  return error == std::errc() && parsed == end && std::isfinite(*destination);
}

bool ParseOptions(int argc, char** argv, Options* options) {
  if (argc < 2) return false;
  options->database_path = argv[1];
  for (int i = 2; i < argc; ++i) {
    const std::string_view argument(argv[i]);
    if (argument == "--force") {
      options->force = true;
      continue;
    }
    if (argument == "--reuse-database") {
      options->reuse_database = true;
      continue;
    }
    if (argument == "--generate-only") {
      options->generate_only = true;
      continue;
    }
    if (argument == "--load-only") {
      options->load_only = true;
      continue;
    }
    if (argument == "--help" || i + 1 >= argc) return false;
    const std::string_view value(argv[++i]);
    bool parsed = true;
    if (argument == "--scale-factor" || argument == "--sf") {
      parsed = ParseDouble(value, &options->scale_factor);
    } else if (argument == "--data-dir") {
      options->data_dir = value;
    } else if (argument == "--dbgen") {
      options->dbgen = value;
    } else if (argument == "--dbgen-config") {
      options->dbgen_config = value;
    } else if (argument == "--batch-rows") {
      parsed = ParseInteger(value, &options->batch_rows);
    } else if (argument == "--query") {
      size_t query = 0;
      parsed = ParseInteger(value, &query);
      if (parsed) options->query = query;
    } else {
      parsed = false;
    }
    if (!parsed) return false;
  }
  if (options->data_dir.empty()) {
    options->data_dir = options->database_path.string() + ".tpch-data";
  }
  return options->scale_factor > 0 && options->batch_rows > 0 &&
         (!options->query || (*options->query >= 1 && *options->query <= 22)) &&
         !(options->generate_only && options->load_only) &&
         !(options->force && options->reuse_database);
}

std::string ScaleText(double scale_factor) {
  std::ostringstream output;
  output << std::setprecision(15) << scale_factor;
  return output.str();
}

fs::path TablePath(const Options& options, std::string_view table) {
  return options.data_dir / (std::string(table) + ".tbl");
}

fs::path ManifestPath(const Options& options) {
  return options.data_dir / "tinylamb-tpch.manifest";
}

bool ExistingDataMatches(const Options& options) {
  std::ifstream manifest(ManifestPath(options));
  std::string key;
  double scale = 0;
  if (!(manifest >> key >> scale) || key != "scale_factor") return false;
  if (std::abs(scale - options.scale_factor) > 1e-12) return false;
  return std::ranges::all_of(Tables(), [&](const TableSpec& table) {
    return fs::is_regular_file(TablePath(options, table.name));
  });
}

bool GenerateData(const Options& options, std::string* error) {
  if (ExistingDataMatches(options)) {
    std::cout << "data_generation=reused\n";
    return true;
  }
  std::error_code filesystem_error;
  fs::create_directories(options.data_dir, filesystem_error);
  if (filesystem_error) {
    *error = "cannot create data directory: " + filesystem_error.message();
    return false;
  }
  for (const TableSpec& table : Tables()) {
    if (fs::exists(TablePath(options, table.name))) {
      *error =
          "data directory contains .tbl files for a different or "
          "unrecorded scale; choose another --data-dir";
      return false;
    }
  }
  if (!fs::is_regular_file(options.dbgen)) {
    *error = "DBGEN executable not found: " + options.dbgen.string();
    return false;
  }
  if (!fs::is_regular_file(options.dbgen_config / "dists.dss")) {
    *error = "dists.dss not found under: " + options.dbgen_config.string();
    return false;
  }

  const std::string executable = fs::absolute(options.dbgen).string();
  const std::string data_dir = fs::absolute(options.data_dir).string();
  const std::string config_dir = fs::absolute(options.dbgen_config).string();
  const std::string scale = ScaleText(options.scale_factor);
  const pid_t child = fork();
  if (child < 0) {
    *error = "fork failed";
    return false;
  }
  if (child == 0) {
    setenv("DSS_PATH", data_dir.c_str(), 1);
    setenv("DSS_CONFIG", config_dir.c_str(), 1);
    setenv("DSS_DIST", "dists.dss", 1);
    execl(executable.c_str(), executable.c_str(), "-q", "-f", "-s",
          scale.c_str(), static_cast<char*>(nullptr));
    _exit(127);
  }
  int status = 0;
  if (waitpid(child, &status, 0) != child || !WIFEXITED(status) ||
      WEXITSTATUS(status) != 0) {
    *error = "DBGEN failed with status " + std::to_string(status);
    return false;
  }
  std::ofstream manifest(ManifestPath(options), std::ios::trunc);
  manifest << "scale_factor " << std::setprecision(17) << options.scale_factor
           << '\n'
           << "dbgen_commit " << TINYLAMB_TPCH_DBGEN_COMMIT << '\n';
  if (!manifest) {
    *error = "cannot write data manifest";
    return false;
  }
  std::cout << "data_generation=generated\n";
  return true;
}

uint64_t CountLines(const fs::path& path) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("cannot read " + path.string());
  uint64_t rows = 0;
  std::string line;
  while (std::getline(input, line)) ++rows;
  return rows;
}

uint64_t ScaledRows(uint64_t sf1_rows, double scale_factor) {
  return static_cast<uint64_t>(static_cast<long double>(sf1_rows) *
                               scale_factor);
}

bool ValidateGeneratedData(const Options& options,
                           std::vector<uint64_t>* row_counts,
                           std::string* error) {
  row_counts->clear();
  row_counts->reserve(Tables().size());
  uint64_t order_rows = 0;
  for (const TableSpec& table : Tables()) {
    const uint64_t rows = CountLines(TablePath(options, table.name));
    row_counts->push_back(rows);
    std::cout << "generated_rows." << table.name << '=' << rows << '\n';
    if (table.name == "orders") order_rows = rows;
    if (table.name == "lineitem") {
      if (options.scale_factor == 1.0 && rows != table.sf1_rows) {
        *error = "lineitem cardinality does not match DBGEN SF1";
        return false;
      }
      if (rows < order_rows || rows > order_rows * 7) {
        *error = "lineitem cardinality is outside the TPC-H 1..7/order bound";
        return false;
      }
      continue;
    }
    const uint64_t expected =
        table.fixed_size ? table.sf1_rows
                         : ScaledRows(table.sf1_rows, options.scale_factor);
    if (rows != expected) {
      *error = std::string(table.name) + " cardinality " +
               std::to_string(rows) + " != expected " +
               std::to_string(expected);
      return false;
    }
  }
  return true;
}

bool RunSql(tinylamb::Database& database, tinylamb::TransactionContext& context,
            std::string_view sql, std::string* error) {
  tinylamb::SqlEngine engine(database);
  tinylamb::StatusOr<tinylamb::Executor> prepared =
      engine.Prepare(context, sql);
  if (!prepared.HasValue()) {
    *error = engine.LastError();
    return false;
  }
  tinylamb::Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
  }
  return true;
}

bool CreateSchema(tinylamb::Database& database, std::string* error) {
  tinylamb::TransactionContext context = database.BeginContext();
  for (const TableSpec& table : Tables()) {
    if (!RunSql(database, context, table.ddl, error)) {
      context.Abort();
      return false;
    }
  }
  if (context.PreCommit() != tinylamb::Status::kSuccess) {
    *error = "schema transaction failed";
    return false;
  }
  return true;
}

std::vector<std::string_view> SplitFields(std::string_view line) {
  std::vector<std::string_view> fields;
  size_t begin = 0;
  while (begin < line.size()) {
    const size_t end = line.find('|', begin);
    if (end == std::string_view::npos) {
      fields.push_back(line.substr(begin));
      break;
    }
    fields.push_back(line.substr(begin, end - begin));
    begin = end + 1;
  }
  return fields;
}

bool ParseRow(std::string_view line, const TableSpec& table, tinylamb::Row* row,
              std::string* error) {
  const std::vector<std::string_view> fields = SplitFields(line);
  if (fields.size() != table.fields.size()) {
    *error = std::string(table.name) + " field count " +
             std::to_string(fields.size()) +
             " != " + std::to_string(table.fields.size());
    return false;
  }
  std::vector<tinylamb::Value> values;
  values.reserve(fields.size());
  for (size_t i = 0; i < fields.size(); ++i) {
    switch (table.fields[i]) {
      case FieldKind::kInteger: {
        int64_t value = 0;
        if (!ParseInteger(fields[i], &value)) {
          *error = std::string(table.name) + " invalid integer at column " +
                   std::to_string(i + 1);
          return false;
        }
        values.emplace_back(value);
        break;
      }
      case FieldKind::kNumeric: {
        double value = 0;
        if (!ParseDouble(fields[i], &value)) {
          *error = std::string(table.name) + " invalid numeric at column " +
                   std::to_string(i + 1);
          return false;
        }
        values.emplace_back(value);
        break;
      }
      case FieldKind::kString:
      case FieldKind::kDate:
        values.emplace_back(std::string(fields[i]));
        break;
    }
  }
  *row = tinylamb::Row(std::move(values));
  return true;
}

bool LoadTable(tinylamb::Database& database, const Options& options,
               const TableSpec& table, uint64_t expected_rows,
               std::string* error) {
  std::ifstream input(TablePath(options, table.name));
  if (!input) {
    *error = "cannot open " + TablePath(options, table.name).string();
    return false;
  }
  tinylamb::TransactionContext context = database.BeginContext();
  tinylamb::StatusOr<tinylamb::Table> found =
      database.GetTable(context, table.name);
  if (!found.HasValue()) {
    *error = "cannot open table " + std::string(table.name);
    context.Abort();
    return false;
  }
  tinylamb::Table destination = std::move(found.Value());
  uint64_t rows = 0;
  size_t in_transaction = 0;
  std::string line;
  while (std::getline(input, line)) {
    tinylamb::Row row;
    if (!ParseRow(line, table, &row, error)) {
      context.Abort();
      return false;
    }
    if (!destination.Insert(context.txn_, row).HasValue()) {
      *error = "insert failed for " + std::string(table.name) +
               " at input row " + std::to_string(rows + 1);
      context.Abort();
      return false;
    }
    ++rows;
    ++in_transaction;
    if (in_transaction == options.batch_rows) {
      if (context.PreCommit() != tinylamb::Status::kSuccess) {
        *error = "load commit failed for " + std::string(table.name);
        return false;
      }
      context = database.BeginContext();
      in_transaction = 0;
      if (rows % 100000 == 0 || rows == expected_rows) {
        std::cout << "load_progress." << table.name << '=' << rows << '/'
                  << expected_rows << '\n';
      }
    }
  }
  if (context.PreCommit() != tinylamb::Status::kSuccess) {
    *error = "final load commit failed for " + std::string(table.name);
    return false;
  }
  if (rows != expected_rows) {
    *error = "loaded row count changed for " + std::string(table.name);
    return false;
  }
  std::cout << "loaded_rows." << table.name << '=' << rows << '\n';
  return true;
}

bool DatabaseFilesExist(const fs::path& base) {
  return fs::exists(base.string() + ".db") ||
         fs::exists(base.string() + ".log") ||
         fs::exists(base.string() + ".last_checkpoint");
}

void RemoveDatabaseFiles(const fs::path& base) {
  std::error_code ignored;
  fs::remove(base.string() + ".db", ignored);
  fs::remove(base.string() + ".log", ignored);
  fs::remove(base.string() + ".last_checkpoint", ignored);
}

struct QueryProfile {
  size_t query{0};
  size_t rows{0};
  double milliseconds{0};
  std::string plan;
};

bool RunQuery(tinylamb::Database& database, size_t query, QueryProfile* profile,
              std::string* error) {
  tinylamb::TransactionContext context = database.BeginReadOnlyContext();
  tinylamb::SqlEngine engine(database);
  const Clock::time_point begin = Clock::now();
  tinylamb::StatusOr<tinylamb::Executor> prepared =
      engine.Prepare(context, tinylamb::kTpchBenchmarkQueries.at(query - 1));
  if (!prepared.HasValue()) {
    *error = "Q" + std::to_string(query) + ": " + engine.LastError();
    context.Abort();
    return false;
  }
  tinylamb::Row row;
  size_t rows = 0;
  try {
    while (prepared.Value()->Next(&row, nullptr)) ++rows;
  } catch (const std::exception& exception) {
    *error = "Q" + std::to_string(query) + ": " + exception.what();
    context.Abort();
    return false;
  }
  const double milliseconds =
      std::chrono::duration<double, std::milli>(Clock::now() - begin).count();
  std::ostringstream plan;
  prepared.Value()->Dump(plan, 0);
  if (context.PreCommit() != tinylamb::Status::kSuccess) {
    *error = "Q" + std::to_string(query) + ": read transaction failed";
    return false;
  }
  *profile = {query, rows, milliseconds, plan.str()};
  std::cout << "TPCH_PROFILE Q" << query << ' ' << milliseconds
            << " ms rows=" << rows << ' ' << profile->plan << '\n';
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!ParseOptions(argc, argv, &options)) {
    Usage(std::cerr, argc == 0 ? "tinylamb_tpch_benchmark" : argv[0]);
    return 2;
  }

  std::cout << "benchmark=tinylamb_tpch\n"
            << "scale_factor=" << ScaleText(options.scale_factor) << '\n'
            << "database=" << options.database_path << '\n'
            << "data_dir=" << options.data_dir << '\n'
            << "dbgen=" << options.dbgen << '\n'
            << "official_sf="
            << (options.scale_factor == 1 || options.scale_factor == 10 ||
                        options.scale_factor == 100 ||
                        options.scale_factor == 300 ||
                        options.scale_factor == 1000 ||
                        options.scale_factor == 3000 ||
                        options.scale_factor == 10000 ||
                        options.scale_factor == 30000 ||
                        options.scale_factor == 100000
                    ? "true"
                    : "false")
            << '\n';

  std::string error;
  const Clock::time_point generation_begin = Clock::now();
  if (!GenerateData(options, &error)) {
    std::cerr << "generation failed: " << error << '\n';
    return 1;
  }
  std::vector<uint64_t> row_counts;
  try {
    if (!ValidateGeneratedData(options, &row_counts, &error)) {
      std::cerr << "data validation failed: " << error << '\n';
      return 1;
    }
  } catch (const std::exception& exception) {
    std::cerr << "data validation failed: " << exception.what() << '\n';
    return 1;
  }
  std::cout
      << "generation_seconds="
      << std::chrono::duration<double>(Clock::now() - generation_begin).count()
      << '\n';
  if (options.generate_only) return 0;

  const bool database_exists = DatabaseFilesExist(options.database_path);
  if (options.reuse_database && !database_exists) {
    std::cerr << "--reuse-database requires an existing database\n";
    return 1;
  }
  if (database_exists && !options.reuse_database) {
    if (!options.force) {
      std::cerr << "database already exists; use a new path or --force\n";
      return 1;
    }
    RemoveDatabaseFiles(options.database_path);
  }

  const Clock::time_point load_begin = Clock::now();
  tinylamb::Database database(options.database_path.string());
  if (!options.reuse_database) {
    if (!CreateSchema(database, &error)) {
      std::cerr << "schema initialization failed: " << error << '\n';
      return 1;
    }
    for (size_t i = 0; i < Tables().size(); ++i) {
      const Clock::time_point table_begin = Clock::now();
      if (!LoadTable(database, options, Tables()[i], row_counts[i], &error)) {
        std::cerr << "load failed: " << error << '\n';
        return 1;
      }
      std::cout
          << "load_seconds." << Tables()[i].name << '='
          << std::chrono::duration<double>(Clock::now() - table_begin).count()
          << '\n';
    }
  }
  std::cout << (options.reuse_database ? "database_open_seconds="
                                       : "load_seconds.total=")
            << std::chrono::duration<double>(Clock::now() - load_begin).count()
            << '\n';
  if (options.load_only) return 0;

  std::vector<QueryProfile> profiles;
  const size_t first_query = options.query.value_or(1);
  const size_t last_query = options.query.value_or(22);
  for (size_t query = first_query; query <= last_query; ++query) {
    QueryProfile profile;
    if (!RunQuery(database, query, &profile, &error)) {
      std::cerr << "query failed: " << error << '\n';
      return 1;
    }
    profiles.push_back(std::move(profile));
  }
  std::ranges::sort(profiles, std::greater{}, &QueryProfile::milliseconds);
  std::cout << "TPCH_SLOWEST_FIRST\n";
  for (const QueryProfile& profile : profiles) {
    std::cout << "Q" << profile.query << ' ' << profile.milliseconds
              << " ms rows=" << profile.rows << '\n';
  }
  return 0;
}
