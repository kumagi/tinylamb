/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_EXPRESSION_PROTO_TEXT_HPP
#define TINYLAMB_EXPRESSION_PROTO_TEXT_HPP

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "type/value.hpp"

namespace tinylamb {

// One top-level entry of a protobuf TEXT-format payload.
struct ProtoTextEntry {
  std::string name;  // bare field name, or "[ext.full.path]" for extensions.
  bool is_message{false};
  std::string text;  // scalar token, or message body WITHOUT outer braces.
};

// Parses the entries of a proto TEXT-format body (no outer braces).  Skips
// whitespace, '#' line comments and ',' / ';' separators.  Returns false when
// the text is not parseable as proto text at all.
bool ParseProtoTextEntries(std::string_view body,
                           std::vector<ProtoTextEntry>* entries);

// True when the text plausibly holds a whole proto message body ("field: v"
// entries); false for plain strings, numbers or empty text.
bool LooksLikeProtoText(std::string_view text);

// Canonicalizes a proto TEXT payload: one space between tokens, messages as
// "name { ... }", extension entries ([...]) moved after plain fields (their
// relative order preserved).
std::optional<std::string> NormalizeProtoText(std::string_view text);

// Formats one scalar as the right-hand side of a proto text entry: enum-like
// UPPER_SNAKE tokens render bare, other strings quoted, booleans true/false.
std::string FormatProtoTextScalar(std::string_view raw_token);

// Extracts `key` (case-insensitive) from a proto TEXT payload (outer braces
// optional).  A single match yields the scalar/message value; repeated
// matches yield an array.  Date/timestamp FORMAT annotations implied by the
// field name are applied to the produced values.  Returns false when absent.
bool ProtoTextExtractField(std::string_view text, std::string_view key,
                           Value* out);

// Presence test used for has_xxx pseudo fields.
bool ProtoTextHasField(std::string_view text, std::string_view key);

// Sets (non-null value), clears (null value) or creates a field addressed by
// a dotted path inside a proto TEXT payload.  Existing entries keep their
// position; new entries append at the end of their containing message.
// Throws std::runtime_error for required-field violations.
std::optional<std::string> ProtoTextSetField(
    std::string_view text, const std::vector<std::string>& path,
    const Value& new_value, const std::string& type_name);

// Builds a whole proto TEXT payload from ordered (field, value) pairs.
// NULL scalars are omitted, arrays fan out into repeated entries, message-
// looking strings nest as "field { ... }".  Enforces required fields for the
// known compliance protos; throws std::runtime_error on violation.
std::string ConstructProtoText(
    const std::string& type_name,
    const std::vector<std::pair<std::string, Value>>& fields);

// Rejects values that cannot be stored into an enum-typed field: INT64 into
// proto2 enums (proto3 keeps unknown numeric members), and strings outside
// the declared member list.  No-op for unmodelled (type, field) pairs.
void ValidateEnumFieldValue(const std::string& type_name,
                            const std::string& field_name,
                            const Value& value);

// Decodes a minimal protobuf wire-format byte payload for protos whose field
// layout this engine models.  Returns nullopt for unknown types / payloads.
std::optional<std::string> DecodeProtoWireBytes(const std::string& type_name,
                                                std::string_view bytes);

// One-stop proto TEXT field read used by scalar field access: only applies
// when the text plausibly holds proto TEXT entries; false otherwise.
bool TryProtoTextGetField(std::string_view text, std::string_view key,
                          Value* out);

// Appends the engine's proto-type marker comment ("# tinylamb-proto-type=...")
// so per-type unset-field defaults resolve at extraction time.
std::string AppendProtoTypeMarker(const std::string& payload,
                                  const std::string& type_name);

// Extracts the lowered proto type recorded by AppendProtoTypeMarker; empty
// when the payload carries no marker.
std::string ExtractProtoTypeMarker(std::string_view text);

// Best-effort proto type resolution for payloads stored without a marker:
// scores the modelled compliance protos by their signature fields, counting
// both payload entries and the caller's SET-target path segments.  Returns
// the display-form type name, or empty when nothing matches confidently.
std::string InferProtoTypeName(
    std::string_view payload, const std::vector<std::string>& hint_fields);

// True for dotted type paths that name known ENUM types (as opposed to the
// dotted proto message paths this engine stores as TEXT payloads).
bool IsKnownEnumTypeName(const std::string& type_name);

// Parses GoogleSQL-style timestamp text ("YYYY-MM-DD[ T]HH:MM:SS[.fff][±tz]")
// into epoch nanoseconds (UTC); nullopt when unparsable.
std::optional<int64_t> ParseTimestampTextNanos(std::string_view text);

// True when the field belongs to a proto compiled with required fields that
// this engine models (used to reject null/missing constructions).
bool RequiredProtoField(const std::string& type_name,
                        const std::string& field);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_PROTO_TEXT_HPP
