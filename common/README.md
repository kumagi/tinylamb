# Common

This directory provides a suite of foundational utilities and data structures that are leveraged throughout the entire database system. These components are designed to be generic and reusable, forming the essential building blocks for more complex functionalities.

## Key Components

- **`constants.hpp`**: Defines a wide range of global constants, status codes, and enumerations that are critical for the consistent operation of the system. This includes page sizes, status enums (e.g., `kSuccess`, `kNotFound`), and binary operation types for query processing.

- **Serialization/Deserialization (`serdes`, `Encoder`, `Decoder`)**: A robust set of tools for converting data structures into a format suitable for storage or network transmission, and for reconstructing them back into their original form.
  - **`serdes.hpp` / `serdes.cpp`**: Contains low-level functions for serializing and deserializing primitive C++ types and fundamental database types like `page_id_t` and `slot_t`. These functions are optimized for performance and space efficiency.
  - **`encoder.hpp` / `encoder.cpp`**: Implements the `Encoder` class, which provides a streaming interface (`operator<<`) for writing various data types to an output stream. It simplifies the process of serializing complex objects by chaining multiple write operations.
  - **`decoder.hpp` / `decoder.cpp`**: Features the `Decoder` class, the counterpart to `Encoder`. It offers a streaming interface (`operator>>`) for reading data from an input stream and populating C++ objects, handling the conversion from a serialized format back to in-memory representation.

- **`StatusOr<T>`**: A specialized utility class for functions that need to return either a value of type `T` or a `Status` code indicating an error. This eliminates the need for cumbersome out-parameters or exception handling for flow control, leading to cleaner and more readable code. It is accompanied by macros like `ASSIGN_OR_RETURN` to streamline error checking.

- **`LogMessage`**: A flexible and powerful logging framework that supports various log levels (e.g., `INFO`, `WARN`, `FATAL`). It automatically captures contextual information such as file name, line number, and function name, making debugging and tracing system behavior significantly easier.

- **`RingBuffer`**: A high-performance, thread-safe, single-producer/single-consumer ring buffer. This data structure is ideal for asynchronous communication between different components of the system, allowing for efficient, lock-free data exchange.

- **`VMCache`**: An advanced virtual memory-based caching mechanism designed to manage large datasets that exceed available physical memory. It intelligently maps file regions into memory, providing fast access to frequently used data while minimizing disk I/O. This component is crucial for achieving high performance in a memory-constrained environment.

- **Utility Headers**:
  - **`converter.hpp`**: Includes helper functions for converting between different container types, such as creating a `std::unordered_set` from a `std::vector`.
  - **`debug.hpp`**: Provides useful functions for debugging, such as `Hex` for printing data in hexadecimal format and `OmittedString` for truncating long strings for concise display.
  - **`random_string.hpp`**: A simple utility for generating random strings, which is particularly useful for creating unique identifiers or for populating test data.
  - **`test_util.hpp`**: Contains a collection of macros and helper functions specifically designed to simplify the writing of unit tests, such as `ASSERT_SUCCESS` to verify that an operation completed without errors.