# Plan

This directory is dedicated to the logical representation of a query and the process of optimizing it into an efficient physical execution plan. The query planner is a critical component of any database system, as it is responsible for choosing the most efficient way to execute a query from a vast number of possibilities.

## Core Concepts

- **`PlanBase`**: An abstract base class that defines the common interface for all logical plan nodes. A logical plan is a tree-like structure that represents the operations of a query in a declarative way, without specifying the exact algorithms to be used. Each node in the tree is a `Plan` object, and it has methods to estimate the cost and the number of rows it will produce.

- **Logical Plan Nodes**: Each class derived from `PlanBase` represents a specific logical operation in a query.
  - **`FullScanPlan`**: Represents a full scan of a table.
  - **`IndexScanPlan`**: Represents a scan of a table using an index.
  - **`IndexOnlyScanPlan`**: Represents a scan that can be satisfied entirely from an index.
  - **`ProjectionPlan`**: Represents a projection operation (the `SELECT` clause).
  - **`SelectionPlan`**: Represents a selection operation (the `WHERE` clause).
  - **`ProductPlan`**: Represents a join operation between two tables.

- **`Optimizer`**: The central component of the query planning process. It takes a logical query plan (represented by a `QueryData` object) and transforms it into an optimized physical execution plan. The optimizer uses a cost-based approach to evaluate different execution strategies and choose the one with the lowest estimated cost.

## The Optimization Process

The query optimization process is a multi-stage process that involves several key steps:

1.  **Parsing and Semantic Analysis**: The initial SQL query is parsed into an abstract syntax tree (AST). This tree is then analyzed to ensure that it is semantically correct (e.g., all tables and columns exist, and data types are compatible). This process results in a `QueryData` object, which is a high-level representation of the query.

2.  **Logical Plan Generation**: The `QueryData` object is then converted into an initial logical plan. This plan is a direct translation of the query and is not yet optimized. For example, a join between two tables would be represented as a `ProductPlan` followed by a `SelectionPlan`.

3.  **Cost-Based Optimization**: This is the core of the optimization process. The `Optimizer` explores a wide range of equivalent logical plans and physical execution strategies. For each possible plan, it estimates the cost based on statistics about the data, such as the number of rows in each table and the selectivity of predicates.
    -   **Predicate Pushdown**: The optimizer will try to move `SelectionPlan` nodes as far down the plan tree as possible. This reduces the number of rows that need to be processed by higher-level operators, such as joins.
    -   **Join Order Selection**: For queries with multiple joins, the optimizer will explore different join orders to find the one that minimizes the size of the intermediate results.
    -   **Index Selection**: The optimizer will examine the available indexes on the tables and determine if using an index would be more efficient than a full table scan. It will consider both `IndexScanPlan` and `IndexOnlyScanPlan` options.
    -   **Join Algorithm Selection**: For each join, the optimizer will choose the most appropriate join algorithm (e.g., `HashJoin`, `IndexJoin`) based on the properties of the input data and the available indexes.

4.  **Physical Plan Generation**: Once the optimizer has chosen the best logical plan, it is converted into a physical execution plan. This is a tree of `Executor` objects, where each executor implements a specific physical algorithm (e.g., `HashJoin`, `IndexScan`). This physical plan is then passed to the execution engine to be executed.

The goal of the optimizer is to find a physical execution plan that is functionally equivalent to the original query but has the lowest possible execution cost. This is a complex and challenging task, but it is essential for achieving good performance in a database system.