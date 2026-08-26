/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */
#ifndef TINYLAMB_EXPRESSION_PROTO_SCHEMA_HPP
#define TINYLAMB_EXPRESSION_PROTO_SCHEMA_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// Static schema knowledge for the GoogleSQL compliance protos. The engine
// represents protos as text-format strings and enums as member-name strings;
// these tables let casts, NEW constructors, and SELECT AS <proto> validate
// enum membership, required-field presence, and repeated-field nulls the way
// GoogleSQL does instead of accepting anything.

struct ProtoFieldSchema {
  std::string name;
  std::string type_name;  // scalar SQL-ish type or nested message short name
  bool required{false};
  bool repeated{false};
  bool is_message{false};  // group/nested message (or enum handled via type)
  bool is_enum{false};
};

struct EnumSchema {
  std::string short_name;                                // e.g. "TestEnum"
  std::vector<std::pair<std::string, int64_t>> members;  // (name, ordinal)
  bool closed{true};  // proto2 closed vs proto3 open enum
};

// Returns nullptr when the message is unknown.
[[nodiscard]] const std::vector<ProtoFieldSchema>* FindProtoMessageFields(
    const std::string& full_name);

// Exact member lookup: returns the canonical member name for an ordinal, or
// empty when unknown.
[[nodiscard]] std::optional<std::string> EnumMemberForValue(
    std::string_view enum_short_name, int64_t value);

// Returns true and sets *ordinal when `name` is a member of the enum.
[[nodiscard]] bool EnumValueForMember(std::string_view enum_short_name,
                                      std::string_view name, int64_t* ordinal);

// Looks a bare string up across every registered enum: reads an enum-valued
// string back out as its ordinal ("TESTENUMNEGATIVE" -> -1).
[[nodiscard]] std::optional<int64_t> OrdinalForEnumMemberName(
    std::string_view name);

// True when `enum_short_name` is a registered enum type.
[[nodiscard]] bool IsKnownEnum(std::string_view enum_short_name);

// Proto3 enums are open (unknown numeric values legal); proto2 enums closed.
[[nodiscard]] bool EnumIsOpen(std::string_view enum_short_name);

// Resolves "googlesql_test.KitchenSinkPB" / "googlesql_test.TestEnum" to
// their short names for registry lookups.
[[nodiscard]] std::string_view ShortTypeName(std::string_view full_name);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_PROTO_SCHEMA_HPP
