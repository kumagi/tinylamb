# Common

Foundational utilities shared by the database layers. See [AGENTS.md](AGENTS.md)
for dependency rules and the platform abstraction contract.

- `constants.hpp`: status codes, storage identifiers and shared constants.
- `status_or.hpp`: checked values and status propagation macros.
- `encoder.*`, `decoder.*`, `serdes.*`: streaming and primitive serialization.
- `log_message.*`: logging with source location and severity.
- `crc32c.hpp`: standard CRC-32C and the raw-state checkpoint checksum API.
- `runtime_filter.*`: exact-set, range and Bloom runtime filters.
- `parallel_work.*`, `query_parallelism.hpp`: task execution and query budgets.
- `regex_guard.*`: bounded regular-expression compilation and evaluation.
- `string_util.hpp`, `random_string.hpp`, `test_util.hpp`: text and test helpers.
- `platform/`: operating-system APIs for files, sockets, clocks and polling.

The retired `RingBuffer`, `VMCache`, `converter.hpp` and `debug.hpp` are not
components of the current tree. Row pages are owned by `page/PageManager`.
