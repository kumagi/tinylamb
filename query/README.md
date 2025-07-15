# Query

This directory contains the data structures used to represent a parsed SQL query before it is processed by the query optimizer. It serves as an intermediate representation that captures the essential components of a query in a structured format.

## Core Components

- **`QueryData`**: A central struct that holds all the information about a parsed SQL query. It is the primary data structure that is passed to the query optimizer to begin the planning process. It contains the following key fields:
  - **`from_`**: A `std::vector<std::string>` that lists the names of the tables involved in the query (the `FROM` clause).
  - **`where_`**: An `Expression` tree that represents the conditions in the `WHERE` clause of the query. This tree is built using the components from the `expression` directory.
  - **`select_`**: A `std::vector<NamedExpression>` that represents the columns and expressions in the `SELECT` list. Each `NamedExpression` pairs an expression with an optional alias.

## The Rewrite Process

Before the `QueryData` object can be used by the optimizer, it needs to go through a `Rewrite` phase. This is a critical step that resolves any ambiguities in the query and prepares it for optimization. The `Rewrite` method performs the following key tasks:

1.  **Column Name Resolution**: In a query with multiple tables, it's possible for different tables to have columns with the same name. The `Rewrite` process examines all the `ColumnValue` expressions in the `where_` and `select_` clauses and resolves any unqualified column names (e.g., `id`) to their fully qualified form (e.g., `users.id`).
    -   It builds a map of column names to table names.
    -   If a column name is unique across all tables in the `FROM` clause, it automatically adds the table name as a qualifier.
    -   If a column name is ambiguous (i.e., it exists in more than one table), it returns an error, forcing the user to provide a fully qualified name.

2.  **Asterisk (`*`) Expansion**: It handles the `SELECT *` syntax by replacing the `*` with a list of all the columns from all the tables in the `FROM` clause. This ensures that the `ProjectionPlan` has a complete and explicit list of the columns it needs to produce.

After the `Rewrite` process is complete, the `QueryData` object is in a canonical form, with all column references fully resolved and all expressions ready to be analyzed by the optimizer. This clean and unambiguous representation is essential for the optimizer to be able to generate a correct and efficient execution plan.