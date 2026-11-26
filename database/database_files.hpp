/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <filesystem>
#include <string>
#include <string_view>

namespace tinylamb::database_files {

void RegisterCreated(std::string_view dbname);
void RemoveWalSegmentFiles(const std::filesystem::path& wal_base);
void RemoveCompanionStorageFiles(const std::string& db_file_path);

}  // namespace tinylamb::database_files
