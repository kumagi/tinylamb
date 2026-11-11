# Storage integrity checks

Individual in-memory `Page` / `RowPage` objects do not carry a persistent
page checksum. `PagePool`, `Page::SetChecksum`, `Page::IsValid`, `DecodeDisk`
and the `TYB1` page format belong to the retired page storage implementation.
They are not interfaces or format requirements of the current engine.

`PageManager::WriteFuzzyCheckpoint` writes a checksum over the complete
snapshot payload before the final checksum field. `LoadFuzzyCheckpoint`
verifies it before decoding the payload and reports `Status::kCorrupt` on a
mismatch. This uses standard CRC-32C (`Crc32C`, initial and final XOR
`0xffffffff`). PAX chunk data/footer checksums use the separate raw-register
`Crc32cOf` convention. See [the format descriptions](page_format.md).

WAL record checksums and fixed-segment metadata have their own framing and
validation rules; see [WAL format](wal_format.md). A snapshot checksum does
not replace WAL-before-data ordering or protect unpersisted in-memory bytes.

Regression sources: `common/crc32c_test.cpp`, `page/page_manager_test.cpp`,
`recovery/checkpoint_manager_test.cpp`, and the PAX chunk tests under `table/`.
