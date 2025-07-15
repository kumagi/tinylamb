# Expression

This directory is dedicated to the representation and evaluation of SQL expressions, which are a fundamental part of query processing. Expressions are used in various parts of a SQL query, such as in the `SELECT` list, the `WHERE` clause, and `JOIN` conditions. The components in this directory provide a flexible and extensible framework for handling these expressions.

## Core Concepts

- **`ExpressionBase`**: An abstract base class that defines the common interface for all expression types. It follows a tree-like structure, where each node in the tree is an expression. The most critical method is `Evaluate()`, which computes the result of the expression given a specific `Row` and `Schema`. This design allows for complex expressions to be built by composing simpler ones.

- **Expression Types**: The framework supports several types of expressions, each representing a different kind of operation or value.

  - **`ConstantValue`**: Represents a literal value, such as a number, a string, or a boolean. This is the simplest type of expression and serves as a leaf node in the expression tree. Its `Evaluate()` method simply returns the constant value it holds.

  - **`ColumnValue`**: Represents a reference to a column in a table. When evaluated, it extracts the value of the specified column from the current `Row`. This is another type of leaf node and is essential for accessing data from tables.

  - **`BinaryExpression`**: Represents an operation that takes two operands (a left and a right expression), such as arithmetic operations (`+`, `-`, `*`, `/`), comparison operations (`=`, `<`, `>`), and logical operations (`AND`, `OR`). This is an internal node in the expression tree, and its `Evaluate()` method first evaluates its children and then applies the specified operation to their results.

- **`NamedExpression`**: A utility struct that pairs an `Expression` with a name (an alias). This is primarily used in the `SELECT` list of a query to assign a name to the result of an expression, which then becomes the column name in the output of the query.

## How It Works

The expression evaluation system is designed to be both powerful and efficient. Here's a breakdown of the typical workflow:

1.  **Parsing and Tree Construction**: When a SQL query is parsed, the expressions within it are converted into a tree of `Expression` objects. For example, the expression `price * 1.1` would be represented as a `BinaryExpression` node with the `*` operator, a `ColumnValue` node for `price` as its left child, and a `ConstantValue` node for `1.1` as its right child.

2.  **Evaluation**: During query execution, for each row being processed, the `Evaluate()` method is called on the root of the expression tree. This call is recursively propagated down the tree.
    -   Leaf nodes (`ConstantValue`, `ColumnValue`) return their respective values.
    -   Internal nodes (`BinaryExpression`) first evaluate their children and then perform their operation on the results.

3.  **Result**: The value returned by the root of the expression tree is the final result of the expression for the given row.

This tree-based evaluation model is a standard and effective way to handle complex, nested expressions in a database system. It provides a clean separation between the representation of an expression and its evaluation, making the system modular and easy to extend with new types of expressions or functions.