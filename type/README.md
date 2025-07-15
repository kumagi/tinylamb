# Type

This directory defines the fundamental data types and structures that are used to represent and manage data within the database system. It forms the basis for how data is stored, manipulated, and interpreted at all levels of the system.

## Core Components

- **`Value` and `ValueType`**: These are the most fundamental components for data representation.
  - **`ValueType`**: An enumeration that defines the set of supported data types in the database, such as `kInt64`, `kVarChar`, and `kDouble`.
  - **`Value`**: A versatile, type-tagged union that can hold a value of any of the supported `ValueType`s. It is used to represent individual data items within a row. The `Value` class overloads various operators (`+`, `-`, `==`, `<`, etc.) to provide type-safe operations, ensuring that, for example, you cannot add a string to an integer. It also includes methods for serialization and for creating a memory-comparable format, which is crucial for efficient key comparisons in indexes.

- **`Row`**: Represents a single row (or tuple) in a table. It is essentially a `std::vector<Value>`, where each `Value` corresponds to a column in the row. The `Row` class provides methods for serialization, which is necessary for storing rows in `RowPage`s, and for extracting a subset of its values, which is useful for creating index keys.

- **`Column` and `ColumnName`**: These classes define the properties of a column.
  - **`ColumnName`**: A simple struct to represent the name of a column, which can be either unqualified (e.g., `id`) or qualified with a table name (e.g., `users.id`).
  - **`Column`**: Represents a column in a table's schema. It contains the `ColumnName`, the `ValueType` of the column, and any associated `Constraint`s.

- **`Constraint`**: Represents a constraint that can be applied to a column, such as `kNotNull`, `kUnique`, or `kPrimaryKey`. These constraints are used to enforce data integrity rules at the database level.

- **`Schema`**: Defines the logical structure of a table or an index. It is a collection of `Column` objects and also includes the name of the table or index. The `Schema` is essential for interpreting the data in a `Row`, as it provides the mapping from column names to their positions and types within the row. It also provides methods for combining schemas, which is necessary for join operations.

## How It Works

The components in this directory work together to provide a comprehensive and type-safe system for data representation.

-   When a table is created, a `Schema` is defined, which specifies the columns and their properties.
-   When a `Row` is inserted into a table, it is composed of a series of `Value` objects, each corresponding to a column in the `Schema`.
-   The `Row` is then serialized and stored in a `RowPage`. The `Schema` is used to correctly interpret the serialized data when the row is read back from the page.
-   When an expression is evaluated (as defined in the `expression` directory), it operates on `Value` objects extracted from a `Row`, and the `Schema` is used to resolve column names and ensure type correctness.

This well-defined type system is a cornerstone of the database's reliability and correctness, ensuring that data is handled consistently and predictably throughout the system.