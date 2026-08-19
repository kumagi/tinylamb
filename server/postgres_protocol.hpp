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

#include "type/row.hpp"
#include "type/value_type.hpp"

namespace tinylamb::pgwire {

constexpr uint32_t kProtocolVersion30 = 196608;
constexpr uint32_t kSslRequestCode = 80877103;
constexpr uint32_t kCancelRequestCode = 80877102;
constexpr uint32_t kGssEncRequestCode = 80877104;

struct StartupPacket {
  uint32_t protocol_version{0};
  std::unordered_map<std::string, std::string> parameters;
};

struct ColumnDescription {
  std::string name;
  ValueType type{ValueType::kVarChar};
};

uint16_t ReadUint16(std::string_view bytes, size_t offset);
uint32_t ReadUint32(std::string_view bytes, size_t offset);
void AppendUint16(std::string* output, uint16_t value);
void AppendUint32(std::string* output, uint32_t value);

std::optional<StartupPacket> ParseStartupPacket(std::string_view packet,
                                                std::string* error);
std::string AuthenticationOk();
std::string ParameterStatus(std::string_view name, std::string_view value);
std::string BackendKeyData(uint32_t process_id, uint32_t secret_key);
std::string ReadyForQuery(char transaction_status);
std::string ErrorResponse(std::string_view message,
                          std::string_view sqlstate = "XX000");
std::string EmptyQueryResponse();
std::string RowDescription(const std::vector<ColumnDescription>& columns);
std::string DataRow(const Row& row);
std::string CommandComplete(std::string_view tag);
std::string NegotiateProtocolVersion(
    uint32_t newest_minor,
    const std::vector<std::string>& unsupported_parameters);

std::vector<std::string> SplitSqlStatements(std::string_view sql);

}  // namespace tinylamb::pgwire

#endif  // TINYLAMB_POSTGRES_PROTOCOL_HPP
