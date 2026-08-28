/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */
#include "expression/proto_schema.hpp"

#include <algorithm>
#include <map>
#include <utility>

namespace tinylamb {
namespace {

// Member lists mirror googlesql/testdata/test_schema.proto and
// test_proto3.proto of the pinned GoogleSQL distribution.
std::vector<EnumSchema> BuildEnums() {
  std::vector<EnumSchema> enums;
  enums.push_back({"TestEnum",
                   {{"TESTENUM0", 0},
                    {"TESTENUM1", 1},
                    {"TESTENUM2", 2},
                    {"TESTENUM2147483647", 2147483647},
                    {"TESTENUMNEGATIVE", -1}},
                   true});
  enums.push_back({"AnotherTestEnum",
                   {{"ANOTHERTESTENUM0", 0},
                    {"ANOTHERTESTENUM1", 1},
                    {"ANOTHERTESTENUM2", 2}},
                   true});
  enums.push_back({"TestEnumWithAlias",
                   {{"FIRSTENUM", 0},
                    {"FIRSTENUM_WITHALIAS", 0},
                    {"SECOND_ENUM", 1},
                    {"FIRSTENUM_WITHSECONDALIAS", 0}},
                   true});
  enums.push_back({"TestEnumWithAnnotations",
                   {{"TESTENUM_NO_ANNOTATION", 1},
                    {"TESTENUM_STRING_ANNOTATED", 2},
                    {"TESTENUM_INT64_ANNOTATED", 3},
                    {"TESTENUM_MULTIPLE_ANNOTATION", 4},
                    {"ANOTHERTESTENUM_DATE_ANNOTATED", 5},
                    {"ANOTHERTESTENUM_MESSAGE_ANNOTATED", 6}},
                   true});
  enums.push_back({"TestProto3Enum",
                   {{"ENUM0", 0},
                    {"ENUM1", 1},
                    {"ENUM2", 2},
                    {"ENUM2147483647", 2147483647}},
                   false});
  return enums;
}

const std::vector<EnumSchema>& Enums() {
  static const std::vector<EnumSchema> enums = BuildEnums();
  return enums;
}

bool NameMatches(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    const char lc = left[i] >= 'A' && left[i] <= 'Z'
                        ? static_cast<char>(left[i] - 'A' + 'a')
                        : left[i];
    const char rc = right[i] >= 'A' && right[i] <= 'Z'
                        ? static_cast<char>(right[i] - 'A' + 'a')
                        : right[i];
    if (lc != rc) {
      return false;
    }
  }
  return true;
}

std::vector<ProtoFieldSchema> KitchenSinkFields() {
  return {
      {"int64_key_1", "int64", true, false, false, false},
      {"int64_key_2", "int64", true, false, false, false},
      {"int32_val", "int32", false, false, false, false},
      {"uint32_val", "uint32", false, false, false, false},
      {"int64_val", "int64", false, false, false, false},
      {"uint64_val", "uint64", false, false, false, false},
      {"string_val", "string", false, false, false, false},
      {"float_val", "float", false, false, false, false},
      {"double_val", "double", false, false, false, false},
      {"bytes_val", "bytes", false, false, false, false},
      {"bool_val", "bool", false, false, false, false},
      {"repeated_int32_val", "int32", false, true, false, false},
      {"repeated_uint32_val", "uint32", false, true, false, false},
      {"repeated_int64_val", "int64", false, true, false, false},
      {"repeated_uint64_val", "uint64", false, true, false, false},
      {"repeated_string_val", "string", false, true, false, false},
      {"repeated_int32_packed", "int32", false, true, false, false},
      {"repeated_int64_packed", "int64", false, true, false, false},
      {"repeated_double_packed", "double", false, true, false, false},
      {"repeated_bool_packed", "bool", false, true, false, false},
      {"repeated_double_val", "double", false, true, false, false},
      {"repeated_bool_val", "bool", false, true, false, false},
      {"nested_value", "Nested", false, false, true, false},
      {"nested_repeated_value", "Nested", false, true, true, false},
      {"test_enum", "TestEnum", false, false, false, true},
      {"repeated_test_enum", "TestEnum", false, true, false, true},
      {"optional_group", "OptionalGroup", false, false, true, false},
      {"optional_group_field", "OptionalGroup", false, false, true, false},
      {"repeated_enum_packed", "TestEnum", false, true, false, true},
      {"timestamp_millis", "int64", false, false, false, false},
      {"date", "int32", false, false, false, false},
      {"date64", "int64", false, false, false, false},
      {"where", "int64", false, false, false, false},
  };
}

std::vector<ProtoFieldSchema> NestedFields() {
  return {
      {"nested_int64", "int64", false, false, false, false},
      {"nested_repeated_int64", "int64", false, true, false, false},
      {"nested_repeated_int32", "int32", false, true, false, false},
      {"value", "int32", false, true, false, false},
      {"timestamp_millis", "int64", false, false, false, false},
      {"date", "int32", false, false, false, false},
  };
}

std::vector<ProtoFieldSchema> OptionalGroupFields() {
  return {
      {"int64_val", "int64", true, false, false, false},
      {"string_val", "string", false, false, false, false},
      {"OptionalGroupNested", "OptionalGroup.OptionalGroupNested", false, true,
       true, false},
  };
}

std::vector<ProtoFieldSchema> OptionalGroupNestedFields() {
  return {{"int64_val", "int64", true, false, false, false}};
}

std::vector<ProtoFieldSchema> KitchenSinkEnumFields() {
  return {
      {"required_test_enum", "TestEnum", true, false, false, true},
      {"test_enum", "TestEnum", false, false, false, true},
      {"repeated_test_enum", "TestEnum", false, true, false, true},
      {"repeated_enum_packed", "TestEnum", false, true, false, true},
  };
}

std::vector<ProtoFieldSchema> Proto3KitchenSinkFields() {
  return {
      {"int32_val", "int32", false, false, false, false},
      {"uint32_val", "uint32", false, false, false, false},
      {"int64_val", "int64", false, false, false, false},
      {"uint64_val", "uint64", false, false, false, false},
      {"float_val", "float", false, false, false, false},
      {"double_val", "double", false, false, false, false},
      {"string_val", "string", false, false, false, false},
      {"bytes_val", "bytes", false, false, false, false},
      {"bool_val", "bool", false, false, false, false},
      {"fixed32_val", "uint32", false, false, false, false},
      {"fixed64_val", "uint64", false, false, false, false},
      {"sfixed32_val", "int32", false, false, false, false},
      {"sfixed64_val", "int64", false, false, false, false},
      {"sint32_val", "int32", false, false, false, false},
      {"sint64_val", "int64", false, false, false, false},
      {"repeated_int32_val", "int32", false, true, false, false},
      {"repeated_uint32_val", "uint32", false, true, false, false},
      {"repeated_int64_val", "int64", false, true, false, false},
      {"repeated_uint64_val", "uint64", false, true, false, false},
      {"repeated_string_val", "string", false, true, false, false},
      {"repeated_float_val", "float", false, true, false, false},
      {"repeated_double_val", "double", false, true, false, false},
      {"repeated_bytes_val", "bytes", false, true, false, false},
      {"repeated_bool_val", "bool", false, true, false, false},
      {"repeated_fixed32_val", "uint32", false, true, false, false},
      {"repeated_fixed64_val", "uint64", false, true, false, false},
      {"repeated_sfixed32_val", "int32", false, true, false, false},
      {"repeated_sfixed64_val", "int64", false, true, false, false},
      {"repeated_sint32_val", "int32", false, true, false, false},
      {"repeated_sint64_val", "int64", false, true, false, false},
      {"test_enum", "TestProto3Enum", false, false, false, true},
      {"repeated_test_enum", "TestProto3Enum", false, true, false, true},
      {"empty_message", "EmptyMessage", false, false, true, false},
      {"test_struct", "Proto3AnnotatedStruct", false, false, true, false},
      {"small_message_no_use_defaults", "Proto3SmallMessageWithNoUseDefaults",
       false, false, true, false},
  };
}

std::vector<ProtoFieldSchema> TestExtraPBFields() {
  return {
      {"int32_val1", "int32", false, false, false, false},
      {"int32_val2", "int32", false, false, false, false},
      {"str_value", "string", false, true, false, false},
  };
}

using MessageMap = std::map<std::string, std::vector<ProtoFieldSchema>>;

MessageMap BuildMessages() {
  MessageMap messages;
  const char* prefix = "googlesql_test.";
  auto add = [&](const std::string& name,
                 std::vector<ProtoFieldSchema> fields) {
    messages.emplace(prefix + name, std::move(fields));
  };
  add("KitchenSinkPB", KitchenSinkFields());
  add("KitchenSinkPB.Nested", NestedFields());
  add("KitchenSinkPB.NestedDates", NestedFields());
  add("KitchenSinkPB.NestedWithRequiredNumericFields",
      {{"nested_int64", "int64", false, false, false, false},
       {"nested_required_int32_val", "int32", true, false, false, false}});
  add("KitchenSinkPB.OptionalGroup", OptionalGroupFields());
  add("KitchenSinkPB.OptionalGroup.OptionalGroupNested",
      OptionalGroupNestedFields());
  add("KitchenSinkPB.NestedRepeatedGroup",
      {{"id", "int64", true, false, false, false},
       {"idstr", "string", false, false, false, false},
       {"NestedRepeatedGroupNested",
        "NestedRepeatedGroup.NestedRepeatedGroupNested", false, true, true,
        false}});
  add("KitchenSinkPB.NestedRepeatedGroup.NestedRepeatedGroupNested",
      {{"id", "int64", true, false, false, false}});
  add("KitchenSinkEnumPB", KitchenSinkEnumFields());
  add("Proto3KitchenSink", Proto3KitchenSinkFields());
  add("TestExtraPB", TestExtraPBFields());
  add("EmptyMessage", {});
  add("NullableInt", {{"value", "int64", false, false, false, false}});
  add("NullableDate", {{"value", "date", false, false, false, false}});
  add("PackedRepeatablePB",
      {{"repeated_bool_packed", "bool", false, true, false, false}});
  return messages;
}

const MessageMap& Messages() {
  static const MessageMap messages = BuildMessages();
  return messages;
}

}  // namespace

const std::vector<ProtoFieldSchema>* FindProtoMessageFields(
    const std::string& full_name) {
  const MessageMap& messages = Messages();
  auto found = messages.find(full_name);
  if (found != messages.end()) {
    return &found->second;
  }
  return nullptr;
}

std::optional<std::string> EnumMemberForValue(std::string_view enum_short_name,
                                              int64_t value) {
  for (const EnumSchema& schema : Enums()) {
    if (!NameMatches(schema.short_name, enum_short_name)) {
      continue;
    }
    for (const auto& [name, ordinal] : schema.members) {
      if (ordinal == value) {
        return name;
      }
    }
    break;
  }
  return std::nullopt;
}

bool EnumValueForMember(std::string_view enum_short_name, std::string_view name,
                        int64_t* ordinal) {
  for (const EnumSchema& schema : Enums()) {
    if (!NameMatches(schema.short_name, enum_short_name)) {
      continue;
    }
    for (const auto& [member, ordinal_value] : schema.members) {
      if (member == name) {
        if (ordinal != nullptr) {
          *ordinal = ordinal_value;
        }
        return true;
      }
    }
    break;
  }
  return false;
}

std::optional<int64_t> OrdinalForEnumMemberName(std::string_view name) {
  for (const EnumSchema& schema : Enums()) {
    for (const auto& [member, ordinal] : schema.members) {
      if (member == name) {
        return ordinal;
      }
    }
  }
  return std::nullopt;
}

bool IsKnownEnum(std::string_view enum_short_name) {
  return std::any_of(Enums().begin(), Enums().end(),
                     [&](const EnumSchema& schema) {
                       return NameMatches(schema.short_name, enum_short_name);
                     });
}

bool EnumIsOpen(std::string_view enum_short_name) {
  for (const EnumSchema& schema : Enums()) {
    if (NameMatches(schema.short_name, enum_short_name)) {
      return !schema.closed;
    }
  }
  return false;
}

std::string_view ShortTypeName(std::string_view full_name) {
  const size_t dot = full_name.rfind('.');
  return dot == std::string_view::npos ? full_name : full_name.substr(dot + 1);
}

}  // namespace tinylamb
