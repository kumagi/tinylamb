/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Native half of comparison/tpcds.py. Loading and JSON serialization are
// outside the Execute + Collect timing window. Input is a private, generated
// manifest, not SQL supplied by a network client.
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/query_parallelism.hpp"
#include "database/database.hpp"
#include "query/sql_engine.hpp"
#include "table/table.hpp"
#include "type/date.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace {
using namespace tinylamb;

std::string Json(std::string_view text) {
  std::string out = "\"";
  out.reserve(text.size() * 6 + 2);
  constexpr std::string_view hex = "0123456789abcdef";
  for (const char raw_c : text) {
    const auto c = static_cast<unsigned char>(raw_c);
    if (c == '"' || c == '\\') {
      out += '\\';
      out += raw_c;
    } else if (c < 32) {
      out += "\\u00";
      out += hex[static_cast<size_t>(c >> 4U)];
      out += hex[static_cast<size_t>(c & 15U)];
    } else {
      out += raw_c;
    }
  }
  return out + '"';
}

void SplitInto(std::string_view line, std::vector<std::string_view>& fields) {
  fields.clear();
  size_t begin = 0;
  for (;;) {
    const size_t end = line.find('\t', begin);
    fields.push_back(
        line.substr(begin, end == std::string_view::npos ? end : end - begin));
    if (end == std::string_view::npos) {
      return;
    }
    begin = end + 1;
  }
}

std::vector<std::string_view> Split(std::string_view line) {
  std::vector<std::string_view> fields;
  SplitInto(line, fields);
  return fields;
}

template <class T>
T Number(std::string_view text) {
  T result{};
  auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), result);
  if (error != std::errc() || end != text.data() + text.size()) {
    throw std::runtime_error("invalid number: " + std::string(text));
  }
  return result;
}

std::string Unhex(std::string_view text) {
  if (text.size() % 2 != 0) {
    throw std::runtime_error("odd hex string");
  }
  std::string out;
  out.reserve(text.size() / 2);
  for (size_t i = 0; i < text.size(); i += 2) {
    unsigned int byte{};
    auto [end, error] =
        std::from_chars(text.data() + i, text.data() + i + 2, byte, 16);
    if (error != std::errc() || end != text.data() + i + 2) {
      throw std::runtime_error("invalid hex string");
    }
    out += static_cast<char>(byte);
  }
  return out;
}

void Check(const Status& status) {
  if (status != Status::kSuccess) {
    throw std::runtime_error(status.GetMessage());
  }
}

void Ddl(Database& db, std::string_view sql) {
  auto ctx = db.BeginContext();
  SqlEngine engine(db);
  auto result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    // Some DDL/parser routes return a Status without populating LastError();
    // fall back to the Status text so the failure is not an empty exception.
    const std::string message = engine.LastError().empty()
                                    ? result.GetStatus().GetMessage()
                                    : engine.LastError();
    throw std::runtime_error(message);
  }
  result.Value().Drain();
  Check(result.Value().GetStatus());
  Check(ctx.PreCommit());
}

void Load(Database& db, const std::filesystem::path& directory,
          bool compact_after) {
  std::ifstream manifest(directory / "manifest.tsv");
  if (!manifest) {
    throw std::runtime_error("cannot read manifest");
  }
  std::vector<std::string> loaded_tables;
  std::vector<std::string_view> cells;
  std::string line;
  while (std::getline(manifest, line)) {
    const auto fields = Split(line);
    if (fields.size() != 4) {
      throw std::runtime_error("invalid manifest record");
    }
    const std::string table(fields[0]);
    const std::string kinds(fields[1]);
    const auto expected = Number<size_t>(fields[2]);
    Ddl(db, fields[3]);
    loaded_tables.push_back(table);
    std::ifstream input(directory / (table + ".tsv"));
    if (!input) {
      throw std::runtime_error("cannot read " + table);
    }
    auto ctx = db.BeginContext();
    auto found = db.GetTable(ctx, table);
    if (!found.HasValue()) {
      throw std::runtime_error("missing table " + table);
    }
    auto destination = std::move(found.Value());
    size_t count = 0;
    std::string record;
    while (std::getline(input, record)) {
      SplitInto(record, cells);
      if (cells.size() != kinds.size()) {
        throw std::runtime_error("field count: " + table);
      }
      std::vector<Value> values;
      values.reserve(cells.size());
      for (size_t i = 0; i < cells.size(); ++i) {
        if (cells[i] == "\\N") {
          values.emplace_back();
          continue;
        }
        switch (kinds[i]) {
          case 'I':
            values.emplace_back(Number<int64_t>(cells[i]));
            break;
          case 'F':
            values.emplace_back(Number<double>(cells[i]));
            break;
          case 'S':
            values.emplace_back(Unhex(cells[i]));
            break;
          case 'D':
            values.push_back(Value::Date(cells[i]));
            break;
          default:
            throw std::runtime_error("unknown field kind");
        }
      }
      auto inserted = destination.Insert(ctx.txn_, Row(std::move(values)));
      if (!inserted.HasValue()) {
        throw std::runtime_error(inserted.GetStatus().GetMessage());
      }
      if (++count % 10000 == 0) {
        Check(ctx.PreCommit());
        if (count % 1000000 == 0) {
          std::cerr << "loading " << table << ' ' << count << '\n';
        }
        ctx = db.BeginContext();
        auto reopened = db.GetTable(ctx, table);
        if (!reopened.HasValue()) {
          throw std::runtime_error("reopen " + table);
        }
        destination = std::move(reopened.Value());
      }
    }
    if (!input.eof() || count != expected) {
      throw std::runtime_error("row count: " + table);
    }
    Check(ctx.PreCommit());
    std::cerr << "loaded " << table << ' ' << count << '\n';
  }
  if (!manifest.eof()) {
    throw std::runtime_error("manifest read failed");
  }
  if (compact_after) {
    for (const auto& table : loaded_tables) {
      auto ctx = db.BeginContext();
      Check(db.CompactTableToTlcs(ctx, table, /*write_checkpoint=*/false,
                                  /*refresh_statistics=*/false));
      Check(ctx.PreCommit());
      std::cerr << "compacted " << table << '\n';
    }
  }
  Ddl(db, "ANALYZE");
  if (compact_after) {
    Check(db.WriteCheckpoint().GetStatus());
    std::cerr << "checkpointed\n";
  }
}

void WriteValue(const Value& v) {
  if (v.IsNull()) {
    std::cout << "null";
  } else if (v.type == ValueType::kInt64) {
    std::cout << v.AsInt64();
  } else if (v.type == ValueType::kDouble) {
    const double x = v.AsDouble();
    if (std::isnan(x)) {
      std::cout << R"({"nonfinite":"nan"})";
    } else if (std::isinf(x)) {
      std::cout << "{\"nonfinite\":" << Json(x > 0 ? "inf" : "-inf") << '}';
    } else {
      std::cout << std::setprecision(17) << x;
    }
  } else if (v.type == ValueType::kVarChar) {
    std::cout << Json(v.AsStringView());
  } else {
    std::cout << Json(v.AsString());
  }
}

void Query(Database& db, std::string_view sql, bool profile) {
  auto ctx = db.BeginReadOnlyContext();
  SqlEngine engine(db);
  const auto before = SqlEngine::ThreadRuntimeStats();
  const auto begin = std::chrono::steady_clock::now();
  auto result = engine.Execute(ctx, sql);
  if (!result.HasValue()) {
    throw std::runtime_error(engine.LastError());
  }
  auto rows = result.Value().Collect();
  const double ms = std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
  Check(result.Value().GetStatus());
  Check(ctx.PreCommit());
  if (profile) {
    result.Value().Dump(std::cerr, 0);
  }
  std::cout << "{\"ms\":" << std::setprecision(17) << ms << ",\"prepare_ms\":"
            << static_cast<double>(SqlEngine::ThreadRuntimeStats().prepare_ns -
                                   before.prepare_ns) /
                   1e6
            << ",\"rows\":[";
  bool first_row = true;
  for (const auto& row : rows) {
    if (!first_row) {
      std::cout << ',';
    }
    first_row = false;
    std::cout << '[';
    bool first_cell = true;
    for (const auto& value : row.values_) {
      if (!first_cell) {
        std::cout << ',';
      }
      first_cell = false;
      WriteValue(value);
    }
    std::cout << ']';
  }
  std::cout << "]}" << '\n';
}
}  // namespace

int main(int argc, char** argv) {
  const std::string_view mode = argc >= 3 ? std::string_view(argv[2]) : "";
  if (argc < 3 || argc > 6 ||
      (mode != "--load" && mode != "--load-compact" && mode != "--serve" &&
       mode != "--compact" && mode != "--compact-all") ||
      (mode == "--compact-all" && argc != 3) ||
      // Only --serve takes the optional threads / --profile arguments;
      // --load/--load-compact/--compact take exactly one operand.
      ((mode == "--load" || mode == "--load-compact" || mode == "--compact") &&
       argc != 4)) {
    std::cerr << "usage: tinylamb_tpcds_compare DB --load DATASET | DB "
                 "--load-compact DATASET | DB --compact TABLE | DB "
                 "--compact-all | DB --serve QUERY_DIR [THREADS] [--profile]\n";
    return 2;
  }
  try {
    // Match the comparison drivers' UTC session, independently of the
    // engine's GoogleSQL-compatible default or the host's TZ environment.
    tinylamb::SetDefaultTimeZone("UTC");
    const bool loading = mode == "--load" || mode == "--load-compact";
    const bool load_compact = loading;
    const bool compacting = mode == "--compact";
    const bool compacting_all = mode == "--compact-all";
    const bool exists = std::filesystem::exists(std::string(argv[1]) + ".db");
    if (loading == exists) {
      throw std::runtime_error(loading ? "database already exists"
                                       : "database missing");
    }
    auto opened = Database::Create(argv[1]);
    if (!opened.HasValue()) {
      throw std::runtime_error(opened.GetStatus().GetMessage());
    }
    auto& db = *opened.Value();
    if (loading) {
      Load(db, argv[3], load_compact);
    } else if (compacting) {
      auto ctx = db.BeginContext();
      Check(db.CompactTableToTlcs(ctx, argv[3]));
      Check(ctx.PreCommit());
      Ddl(db, "ANALYZE");
      Check(db.WriteCheckpoint().GetStatus());
      std::cerr << "compacted " << argv[3] << '\n';
    } else if (compacting_all) {
      std::vector<std::string> tables;
      {
        auto ctx = db.BeginContext();
        tables = db.ListTables(ctx);
        Check(ctx.PreCommit());
      }
      for (const std::string& table : tables) {
        const auto t0 = std::chrono::steady_clock::now();
        auto ctx = db.BeginContext();
        Check(db.CompactTableToTlcs(ctx, table, /*write_checkpoint=*/false,
                                    /*refresh_statistics=*/false));
        Check(ctx.PreCommit());
        const auto t1 = std::chrono::steady_clock::now();
        const double sec = std::chrono::duration<double>(t1 - t0).count();
        std::cerr << "compacted " << table << " in " << sec << "s\n";
      }
      Check(db.CompactAllTablesToTlcs());
      Ddl(db, "ANALYZE");
      Check(db.WriteCheckpoint().GetStatus());
      std::cerr << "compacted all (" << tables.size() << " tables)\n";
    } else {
      const size_t threads = argc >= 5 ? Number<size_t>(argv[4]) : 1;
      if (threads == 0) {
        throw std::runtime_error("threads must be positive");
      }
      std::atomic<size_t> active{1};
      ScopedQueryParallelismBudget budget(&active, threads);
      const bool profile =
          argc == 6 && std::string_view(argv[5]) == "--profile";
      SqlEngine::SetThreadRuntimeProfiling(profile);
      std::cout << "{\"ready\":true}" << '\n';
      std::string name;
      while (std::getline(std::cin, name)) {
        try {
          std::ifstream input(std::filesystem::path(argv[3]) / name);
          if (!input) {
            throw std::runtime_error("cannot read query " + name);
          }
          const std::string sql((std::istreambuf_iterator<char>(input)), {});
          if (profile) {
            std::cerr << "\nQUERY " << name << '\n';
          }
          Query(db, sql, profile);
        } catch (const std::exception& error) {
          std::cout << "{\"error\":" << Json(error.what()) << '}' << '\n';
        }
      }
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
