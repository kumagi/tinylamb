/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_POSTGRES_PROTOCOL_HPP
#define TINYLAMB_POSTGRES_PROTOCOL_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "type/row.hpp"
#include "type/value_type.hpp"

namespace tinylamb::pgwire {

constexpr uint32_t kProtocolVersion30 = 196608;
// T3102: protocol request codes carry the official wire-message spelling
// (SSLRequest / GSSENCRequest / CancelRequest) so a grep for `SSL` finds
// the constant, the parser branch, and the tests in one sweep. Internal
// feature identifiers elsewhere in the tree keep camelCase (kWal...,
// kJit...); these three name protocol messages, not features.
constexpr uint32_t kSSLRequestCode = 80877103;
constexpr uint32_t kCancelRequestCode = 80877102;
constexpr uint32_t kGSSEncRequestCode = 80877104;

struct StartupPacket {
  uint32_t protocol_version{0};
  std::unordered_map<std::string, std::string> parameters;
};

struct ColumnDescription {
  std::string name;
  ValueType type{ValueType::kVarChar};
};

StatusOr<uint16_t> ReadUint16(std::string_view bytes, size_t offset);
StatusOr<uint32_t> ReadUint32(std::string_view bytes, size_t offset);
void AppendUint16(std::string* output, uint16_t value);
void AppendUint32(std::string* output, uint32_t value);

std::optional<StartupPacket> ParseStartupPacket(std::string_view packet,
                                                std::string* error);
// Roll back a withheld response, preserving the earlier bytes still queued.
void DiscardQueuedReply(std::string& output, size_t sent_prefix,
                        size_t earlier_pending_bytes);
std::string AuthenticationOk();
std::string ParameterStatus(std::string_view name, std::string_view value);
// Appends one ParameterStatus frame directly into `output` (no temporary
// string), so a batch of frames can be built in a single buffer.
void AppendParameterStatus(std::string* output, std::string_view name,
                           std::string_view value);
std::string BackendKeyData(uint32_t process_id, uint32_t secret_key);
std::string ReadyForQuery(char transaction_status);
// `severity` fills the S/V fields. PostgreSQL's convention: "ERROR" for
// statements that keep the session, "FATAL" when the server closes the
// connection right after the frame (P655). Default matches Simple Query
// statement errors.
std::string ErrorResponse(std::string_view message,
                          std::string_view sqlstate = "XX000",
                          std::string_view severity = "ERROR",
                          std::optional<size_t> position = std::nullopt);

struct SqlStatement {
  std::string text;
  size_t position{1};  // One-based UTF-8 character position in Query message.
};

// Maps a GoogleSQL parser diagnostic to a position in the original query.
std::optional<size_t> SqlErrorPosition(std::string_view message,
                                       const SqlStatement& statement,
                                       size_t parsed_byte_offset);
// NoticeResponse ('N'): same field layout as ErrorResponse. The caller chooses
// NOTICE or WARNING severity according to the PostgreSQL condition being sent.
std::string NoticeResponse(std::string_view message,
                           std::string_view sqlstate = "00000",
                           std::string_view severity = "NOTICE");
std::string EmptyQueryResponse();
// Callers check that column and value counts fit uint16_t before encoding;
// the encoder CHECK_MSGs as a last resort rather than desynchronize the wire.
std::string RowDescription(const std::vector<ColumnDescription>& columns);
std::string DataRow(const Row& row);
// Appends one PostgreSQL DataRow wire frame straight from columnar storage,
// byte-for-byte identical to `wire_out->append(DataRow(chunk.RowAt(row_idx)))`.
// Lean proof: lean/TinylambOpt/ResearchProofs/ColumnarToPgWireChunkedFramingEquiv.lean (sound)
Status AppendDataRowFromChunk(std::string* wire_out, const DataChunk& chunk,
                              size_t row_idx);
std::string CommandComplete(std::string_view tag);
std::string NegotiateProtocolVersion(
    uint32_t newest_minor,
    const std::vector<std::string>& unsupported_parameters);

// Cap on the statements one split emits, bounding what a single 16 MiB Q
// message can make the splitter allocate (`a;a;a;...` is ~8M non-empty
// one-character statements). Overflow returns kInvalidArgument instead of
// hundreds of MB of transient strings.
inline constexpr size_t kMaxSqlStatements = 100000;

StatusOr<std::vector<std::string>> SplitSqlStatements(std::string_view sql);
StatusOr<std::vector<SqlStatement>> SplitSqlStatementsWithPositions(
    std::string_view sql);

}  // namespace tinylamb::pgwire

#endif  // TINYLAMB_POSTGRES_PROTOCOL_HPP
