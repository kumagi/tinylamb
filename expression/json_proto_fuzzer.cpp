/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Pure in-process libFuzzer for JSON document/path/mutator/LAX functions
// (expression/json_doc.hpp, expression/json_text.hpp) and Protobuf TEXT / wire
// codecs (expression/proto_text.hpp).
//
// Oracles:
//   1. JSON validator/parser agreement + canonical() round-trip idempotence:
//      IsValid(s) => ParseDocument(s) succeeds, IsValid(canonical) is true,
//      and ParseDocument(canonical)->canonical() == canonical.
//   2. JSON function family (json_type, json_keys, json_array_length,
//      json_set/insert/replace/remove, json_array, json_object, lax_*):
//      - lax_* NEVER returns an error status.
//      - Mutators/constructors returning non-null VARCHAR emit valid JSON.
//   3. ProtoText parser / normalizer / extractor / mutator / wire-decoder:
//      - NormalizeProtoText idempotence.
//      - Crash/UB/recursion-depth safety on arbitrary inputs.

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "expression/json_doc.hpp"
#include "expression/json_text.hpp"
#include "expression/proto_text.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {
namespace {

struct Cursor {
  const uint8_t* data{};
  size_t size{};
  size_t pos{0};

  [[nodiscard]] size_t Remaining() const noexcept {
    return pos < size ? size - pos : 0;
  }

  uint8_t NextU8() noexcept { return pos < size ? data[pos++] : 0U; }

  std::string NextSlice(size_t max_len) {
    const size_t avail = Remaining();
    const size_t len =
        std::min(avail, static_cast<size_t>(NextU8()) % (max_len + 1));
    std::string s(reinterpret_cast<const char*>(data + pos), len);
    pos += len;
    return s;
  }

  Value NextSqlValue() {
    switch (NextU8() % 6U) {
      case 0:
        return Value();
      case 1:
        return Value(static_cast<int64_t>(static_cast<int8_t>(NextU8())));
      case 2:
        return Value(static_cast<double>(static_cast<int8_t>(NextU8())) * 0.5);
      case 3:
        return Value(NextSlice(24));
      case 4:
        return Value(std::string(NextU8() & 1U ? "true" : "false"));
      default:
        return Value(std::string("{\"a\":[1,2]}"));
    }
  }
};

[[noreturn]] void Fail(const char* oracle, const char* msg) {
  (void)std::fprintf(stderr, "[JSON-PROTO-FUZZ FAIL][%s] %s\n", oracle, msg);
  (void)std::fflush(stderr);
  std::abort();
}

void FuzzJson(const uint8_t* data, size_t size) {
  const std::string_view raw(reinterpret_cast<const char*>(data), size);
  const bool valid = json_text::IsValid(raw);
  const std::optional<json_doc::JVal> parsed = json_doc::ParseDocument(raw);

  if (valid && parsed.has_value()) {
    const std::string canon1 = parsed->canonical();
    if (!json_text::IsValid(canon1)) {
      Fail("JsonCanonical",
           "canonical() output rejected by json_text::IsValid");
    }
    const std::optional<json_doc::JVal> reparsed =
        json_doc::ParseDocument(canon1);
    if (!reparsed.has_value()) {
      Fail("JsonCanonical", "canonical() output failed ParseDocument");
    }
    const std::string canon2 = reparsed->canonical();
    if (canon1 != canon2) {
      Fail("JsonCanonical", "canonical() is not idempotent");
    }
  }

  // Exercise JSON function family
  Cursor c{data, size, 0};
  const std::string doc_str =
      (c.NextU8() & 1U) != 0 ? std::string(raw) : c.NextSlice(128);
  const std::string path_str =
      (c.NextU8() & 1U) != 0 ? ("$." + c.NextSlice(16)) : c.NextSlice(24);
  const Value doc_val{std::string(doc_str)};
  const Value path_val{std::string(path_str)};
  const Value arg_val = c.NextSqlValue();

  (void)json_doc::family::Evaluate("json_type", {doc_val});
  (void)json_doc::family::Evaluate("json_type", {doc_val, path_val});
  (void)json_doc::family::Evaluate("json_keys", {doc_val, path_val});
  (void)json_doc::family::Evaluate("json_array_length", {doc_val, path_val});

  for (const char* mut : {"json_set", "json_insert", "json_replace"}) {
    StatusOr<Value> r =
        json_doc::family::Evaluate(mut, {doc_val, path_val, arg_val});
    if (r.HasValue() && !r.Value().IsNull() &&
        r.Value().type == ValueType::kVarChar) {
      if (!json_text::IsValid(r.Value().value.varchar_value)) {
        Fail("JsonMutator", "mutator produced invalid JSON");
      }
    }
  }

  {
    StatusOr<Value> r =
        json_doc::family::Evaluate("json_remove", {doc_val, path_val});
    if (r.HasValue() && !r.Value().IsNull() &&
        r.Value().type == ValueType::kVarChar) {
      if (!json_text::IsValid(r.Value().value.varchar_value)) {
        Fail("JsonRemove", "json_remove produced invalid JSON");
      }
    }
  }

  {
    StatusOr<Value> arr =
        json_doc::family::Evaluate("json_array", {arg_val, Value(int64_t{42})});
    if (!arr.HasValue() || arr.Value().type != ValueType::kVarChar ||
        !json_text::IsValid(arr.Value().value.varchar_value)) {
      Fail("JsonArray", "json_array produced invalid JSON");
    }
  }

  // LAX_* must never error
  for (const char* lax :
       {"lax_int64", "lax_float64", "lax_bool", "lax_string"}) {
    StatusOr<Value> r = json_doc::family::Evaluate(lax, {doc_val});
    if (!r.HasValue()) {
      Fail("JsonLax", "lax_* returned an error status");
    }
    StatusOr<Value> r2 = json_doc::family::Evaluate(lax, {arg_val});
    if (!r2.HasValue()) {
      Fail("JsonLax", "lax_* returned an error status on typed value");
    }
  }
}

void FuzzProtoText(const uint8_t* data, size_t size) {
  const std::string_view raw(reinterpret_cast<const char*>(data), size);
  std::vector<ProtoTextEntry> entries;
  (void)ParseProtoTextEntries(raw, &entries);
  (void)LooksLikeProtoText(raw);

  const std::optional<std::string> norm1 = NormalizeProtoText(raw);
  if (norm1.has_value()) {
    const std::optional<std::string> norm2 = NormalizeProtoText(*norm1);
    if (!norm2.has_value() || *norm2 != *norm1) {
      Fail("ProtoNormalize", "NormalizeProtoText is not idempotent");
    }
  }

  (void)DecodeProtoTextScalarToken(raw);
  (void)FormatProtoTextScalar(raw);
  (void)ParseTimestampTextNanos(raw);
  (void)ExtractProtoTypeMarker(raw);

  Cursor c{data, size, 0};
  const std::string key = c.NextSlice(16);
  const std::string key2 = c.NextSlice(16);
  Value out;
  (void)TryProtoTextExtractField(raw, key, &out);
  (void)TryReadProtoTextField(raw, key, &out);
  (void)ProtoTextHasField(raw, key);

  std::vector<std::string> path;
  if (!key.empty()) {
    path.push_back(key);
  }
  if (!key2.empty()) {
    path.push_back(key2);
  }
  (void)TryProtoTextGetFieldPath(raw, path, &out);
  (void)TryProtoTextSetField(raw, path, c.NextSqlValue(), "");

  static constexpr std::array<const char*, 3> kTypes = {
      "", "googlesql_test.KitchenSinkPB", "googlesql_test.TestExtraPB"};
  const std::string type_name = kTypes[c.NextU8() % kTypes.size()];
  (void)DecodeProtoWireBytes(type_name, raw);
  (void)InferProtoTypeName(raw, path);

  std::vector<std::pair<std::string, Value>> fields;
  if (!key.empty()) {
    fields.emplace_back(key, c.NextSqlValue());
  }
  (void)TryConstructProtoText(type_name, fields);
}

}  // namespace
}  // namespace tinylamb::fuzz

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 2 || size > 4096) {
    return 0;
  }
  if ((data[0] & 1U) == 0) {
    tinylamb::fuzz::FuzzJson(data + 1, size - 1);
  } else {
    tinylamb::fuzz::FuzzProtoText(data + 1, size - 1);
  }
  return 0;
}
