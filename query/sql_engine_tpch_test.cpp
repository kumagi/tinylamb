/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "benchmark/tpch_queries.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

namespace tinylamb {
namespace {

constexpr std::array<std::string_view, 22> kTpchQueries = {
    R"sql(SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty,
      sum(l_extendedprice) AS sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
      avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price,
      avg(l_discount) AS avg_disc, COUNT(*) AS count_order
      FROM lineitem
      WHERE l_shipdate <= date_sub(date '1998-12-01', INTERVAL 74 day)
      GROUP BY l_returnflag, l_linestatus
      ORDER BY l_returnflag, l_linestatus;)sql",
    R"sql(SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address,
      s_phone, s_comment
      FROM part, supplier, partsupp, nation, region
      WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
      AND p_size = 19 AND p_type LIKE '%COPPER'
      AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
      AND r_name = 'MIDDLE EAST' AND ps_supplycost =
        (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region
         WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
         AND r_name = 'MIDDLE EAST')
      ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;)sql",
    R"sql(SELECT l_orderkey,
      sum(l_extendedprice * (1 - l_discount)) AS revenue,
      o_orderdate, o_shippriority FROM customer, orders, lineitem
      WHERE c_mktsegment = 'FURNITURE' AND c_custkey = o_custkey
      AND l_orderkey = o_orderkey AND o_orderdate < date '1995-03-29'
      AND l_shipdate > date '1995-03-29'
      GROUP BY l_orderkey, o_orderdate, o_shippriority
      ORDER BY revenue DESC, o_orderdate LIMIT 10;)sql",
    R"sql(SELECT o_orderpriority, COUNT(*) AS order_count FROM orders
      WHERE o_orderdate >= date '1997-06-01'
      AND o_orderdate < date_add(date '1997-06-01', INTERVAL 3 month)
      AND EXISTS(SELECT * FROM lineitem WHERE l_orderkey = o_orderkey
                 AND l_commitdate < l_receiptdate)
      GROUP BY o_orderpriority ORDER BY o_orderpriority;)sql",
    R"sql(SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
      FROM customer, orders, lineitem, supplier, nation, region
      WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
      AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
      AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
      AND r_name = 'AFRICA' AND o_orderdate >= date '1994-01-01'
      AND o_orderdate < date_add(date '1994-01-01', INTERVAL 1 year)
      GROUP BY n_name ORDER BY revenue DESC;)sql",
    R"sql(SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem
      WHERE l_shipdate >= date '1994-01-01'
      AND l_shipdate < date_add(date '1994-01-01', INTERVAL 1 year)
      AND l_discount BETWEEN 0.08 - 0.01 AND 0.08 + 0.01
      AND l_quantity < 25;)sql",
    R"sql(SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue
      FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
                   EXTRACT(year FROM l_shipdate) AS l_year,
                   l_extendedprice * (1 - l_discount) AS volume
            FROM supplier, lineitem, orders, customer, nation n1, nation n2
            WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey
            AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey
            AND c_nationkey = n2.n_nationkey
            AND ((n1.n_name = 'SAUDI ARABIA' AND n2.n_name = 'UNITED KINGDOM')
              OR (n1.n_name = 'UNITED KINGDOM' AND n2.n_name = 'SAUDI ARABIA'))
            AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31') shipping
      GROUP BY supp_nation, cust_nation, l_year
      ORDER BY supp_nation, cust_nation, l_year;)sql",
    R"sql(SELECT o_year,
      sum(CASE WHEN nation = 'PERU' THEN volume ELSE 0 END) / sum(volume)
        AS mkt_share
      FROM (SELECT EXTRACT(year FROM o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount) AS volume,
                   n2.n_name AS nation
            FROM part, supplier, lineitem, orders, customer, nation n1,
                 nation n2, region
            WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
            AND l_orderkey = o_orderkey AND o_custkey = c_custkey
            AND c_nationkey = n1.n_nationkey
            AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA'
            AND s_nationkey = n2.n_nationkey
            AND o_orderdate BETWEEN date '1993-01-01' AND date '1997-12-31'
            AND p_type = 'STANDARD POLISHED TIN') all_nations
      GROUP BY o_year ORDER BY o_year;)sql",
    R"sql(SELECT nation, o_year, sum(amount) AS sum_profit
      FROM (SELECT n_name AS nation, EXTRACT(year FROM o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount)
                     - ps_supplycost * l_quantity AS amount
            FROM part, supplier, lineitem, partsupp, orders, nation
            WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
            AND p_name LIKE '%tomato%') profit
      GROUP BY nation, o_year ORDER BY nation, o_year DESC;)sql",
    R"sql(SELECT c_custkey, c_name,
      sum(l_extendedprice * (1 - l_discount)) AS revenue,
      c_acctbal, n_name, c_address, c_phone, c_comment
      FROM customer, orders, lineitem, nation
      WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
      AND o_orderdate >= date '1994-02-01'
      AND o_orderdate < date_add(date '1994-02-01', INTERVAL 3 month)
      AND l_returnflag = 'R' AND c_nationkey = n_nationkey
      GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address,
               c_comment ORDER BY revenue DESC LIMIT 20;)sql",
    R"sql(SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
      FROM partsupp, supplier, nation
      WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey
      AND n_name = 'PERU' GROUP BY ps_partkey
      HAVING sum(ps_supplycost * ps_availqty) >
        (SELECT sum(ps_supplycost * ps_availqty) * 0.0001000000
         FROM partsupp, supplier, nation
         WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey
         AND n_name = 'PERU') ORDER BY value DESC;)sql",
    R"sql(SELECT l_shipmode,
      sum(CASE WHEN o_orderpriority = '1-URGENT'
                OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END)
        AS high_line_count,
      sum(CASE WHEN o_orderpriority <> '1-URGENT'
                AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END)
        AS low_line_count
      FROM orders, lineitem WHERE o_orderkey = l_orderkey
      AND l_shipmode IN ('MAIL', 'AIR') AND l_commitdate < l_receiptdate
      AND l_shipdate < l_commitdate AND l_receiptdate >= date '1997-01-01'
      AND l_receiptdate < date_add(date '1997-01-01', INTERVAL 1 year)
      GROUP BY l_shipmode ORDER BY l_shipmode;)sql",
    R"sql(SELECT c_count, COUNT(*) AS custdist
      FROM (SELECT c_custkey, COUNT(o_orderkey) c_count FROM customer
            LEFT OUTER JOIN orders ON c_custkey = o_custkey
              AND o_comment NOT LIKE '%unusual%packages%'
            GROUP BY c_custkey) c_orders
      GROUP BY c_count ORDER BY custdist DESC, c_count DESC;)sql",
    R"sql(SELECT 100.00 *
      sum(CASE WHEN p_type LIKE 'PROMO%'
               THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
      / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
      FROM lineitem, part WHERE l_partkey = p_partkey
      AND l_shipdate >= date '1994-03-01'
      AND l_shipdate < date_add(date '1994-03-01', INTERVAL 1 month);)sql",
    R"sql(WITH revenue AS
      (SELECT l_suppkey AS supplier_no,
              sum(l_extendedprice * (1 - l_discount)) AS total_revenue
       FROM lineitem WHERE l_shipdate >= date '1997-05-01'
       AND l_shipdate < date_add(date '1997-05-01', INTERVAL 3 month)
       GROUP BY l_suppkey)
      SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
      FROM supplier, revenue WHERE s_suppkey = supplier_no
      AND total_revenue = (SELECT max(total_revenue) FROM revenue)
      ORDER BY s_suppkey;)sql",
    R"sql(SELECT p_brand, p_type, p_size,
      COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part
      WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#13'
      AND p_type NOT LIKE 'LARGE BURNISHED%'
      AND p_size IN (39, 47, 37, 5, 20, 11, 25, 27)
      AND ps_suppkey NOT IN
        (SELECT s_suppkey FROM supplier
         WHERE s_comment LIKE '%Customer%Complaints%')
      GROUP BY p_brand, p_type, p_size
      ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;)sql",
    R"sql(SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
      FROM lineitem, part WHERE p_partkey = l_partkey
      AND p_brand = 'Brand#33' AND p_container = 'LG DRUM'
      AND l_quantity <
        (SELECT 0.2 * avg(l_quantity) FROM lineitem
         WHERE l_partkey = p_partkey);)sql",
    R"sql(SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
      sum(l_quantity) FROM customer, orders, lineitem
      WHERE o_orderkey IN
        (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey
         HAVING sum(l_quantity) > 230)
      AND c_custkey = o_custkey
      AND o_orderkey = l_orderkey
      GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
      ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;)sql",
    R"sql(SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
      FROM lineitem, part WHERE p_partkey = l_partkey AND
      ((p_partkey = l_partkey AND p_brand = 'Brand#53'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 5 AND l_quantity <= 5 + 10
        AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
       OR (p_partkey = l_partkey AND p_brand = 'Brand#41'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 15 AND l_quantity <= 15 + 10
        AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
       OR (p_partkey = l_partkey AND p_brand = 'Brand#21'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 29 AND l_quantity <= 29 + 10
        AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'));)sql",
    R"sql(SELECT s_name, s_address FROM supplier, nation
      WHERE s_suppkey IN
        (SELECT ps_suppkey FROM partsupp,
           (SELECT p_partkey FROM part WHERE p_name LIKE 'tan%') selected_parts
         WHERE ps_partkey = p_partkey AND ps_availqty >
           (SELECT 0.5 * sum(l_quantity) FROM lineitem
            WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
            AND l_shipdate >= date '1996-01-01'
            AND l_shipdate < date_add(date '1996-01-01', INTERVAL 1 year)))
      AND s_nationkey = n_nationkey AND n_name = 'PERU' ORDER BY s_name;)sql",
    R"sql(SELECT s_name, COUNT(*) AS numwait
      FROM supplier, lineitem l1, orders, nation
      WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
      AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
      AND EXISTS(SELECT * FROM lineitem l2
                 WHERE l2.l_orderkey = l1.l_orderkey
                 AND l2.l_suppkey <> l1.l_suppkey)
      AND NOT EXISTS(SELECT * FROM lineitem l3
                     WHERE l3.l_orderkey = l1.l_orderkey
                     AND l3.l_suppkey <> l1.l_suppkey
                     AND l3.l_receiptdate > l3.l_commitdate)
      AND s_nationkey = n_nationkey AND n_name = 'PERU'
      GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;)sql",
    R"sql(SELECT cntrycode, COUNT(*) AS numcust, sum(c_acctbal) AS totacctbal
      FROM (SELECT substr(c_phone, 1, 2) AS cntrycode, c_acctbal
            FROM customer WHERE substr(c_phone, 1, 2)
              IN ('10', '19', '14', '22', '23', '31', '13')
            AND c_acctbal >
              (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0.00
               AND substr(c_phone, 1, 2)
                 IN ('10', '19', '14', '22', '23', '31', '13'))
            AND NOT EXISTS(SELECT * FROM orders
                           WHERE o_custkey = c_custkey)) custsale
      GROUP BY cntrycode ORDER BY cntrycode;)sql"};

static_assert(kTpchQueries == kTpchBenchmarkQueries,
              "benchmark and unit-test TPC-H queries must stay identical");

class SqlEngineTpchTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = "sql_engine_tpch_test-" + RandomString();
    database_ = std::make_unique<Database>(path_);
  }
  void TearDown() override { database_->DeleteAll(); }

  std::vector<Row> Run(TransactionContext& context, std::string_view sql) {
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(context, sql);
    EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                      << engine.LastError();
    if (!prepared.HasValue()) return {};
    std::vector<Row> rows;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) rows.push_back(row);
    return rows;
  }

  void CreateSchema(TransactionContext& context) {
    Run(context,
        "CREATE TABLE region (r_regionkey INT64, r_name STRING, "
        "r_comment STRING);");
    Run(context,
        "CREATE TABLE nation (n_nationkey INT64, n_name STRING, "
        "n_regionkey INT64, n_comment STRING);");
    Run(context,
        "CREATE TABLE supplier (s_suppkey INT64, s_name STRING, "
        "s_address STRING, s_nationkey INT64, s_phone STRING, "
        "s_acctbal NUMERIC, s_comment STRING);");
    Run(context,
        "CREATE TABLE part (p_partkey INT64, p_name STRING, "
        "p_mfgr STRING, p_brand STRING, p_type STRING, p_size INT64, "
        "p_container STRING, p_retailprice NUMERIC, p_comment STRING);");
    Run(context,
        "CREATE TABLE partsupp (ps_partkey INT64, ps_suppkey INT64, "
        "ps_availqty INT64, ps_supplycost NUMERIC, ps_comment STRING);");
    Run(context,
        "CREATE TABLE customer (c_custkey INT64, c_name STRING, "
        "c_address STRING, c_nationkey INT64, c_phone STRING, "
        "c_acctbal NUMERIC, c_mktsegment STRING, c_comment STRING);");
    Run(context,
        "CREATE TABLE orders (o_orderkey INT64, o_custkey INT64, "
        "o_orderstatus STRING, o_totalprice NUMERIC, "
        "o_orderdate DATE, o_orderpriority STRING, o_clerk STRING, "
        "o_shippriority INT64, o_comment STRING);");
    Run(context,
        "CREATE TABLE lineitem (l_orderkey INT64, l_partkey INT64, "
        "l_suppkey INT64, l_linenumber INT64, l_quantity NUMERIC, "
        "l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, "
        "l_returnflag STRING, l_linestatus STRING, l_shipdate DATE, "
        "l_commitdate DATE, l_receiptdate DATE, l_shipinstruct STRING, "
        "l_shipmode STRING, l_comment STRING);");
  }

  void Seed(TransactionContext& context) {
    Run(context,
        "INSERT INTO region VALUES (1,'AMERICA',''), (2,'AFRICA',''), "
        "(3,'MIDDLE EAST','');");
    Run(context,
        "INSERT INTO nation VALUES (1,'PERU',1,''), "
        "(2,'SAUDI ARABIA',3,''), (3,'UNITED KINGDOM',3,'');");
    Run(context,
        "INSERT INTO supplier VALUES "
        "(1,'Supplier#1','Address 1',1,'10-111',100.0,'ok'), "
        "(2,'Supplier#2','Address 2',2,'19-222',200.0,'ok');");
    Run(context,
        "INSERT INTO part VALUES "
        "(1,'tan tomato part','MFGR','Brand#33',"
        "'STANDARD POLISHED TIN',19,'LG DRUM',10.0,''), "
        "(2,'promo part','MFGR','Brand#53','PROMO COPPER',5,"
        "'SM BOX',20.0,'');");
    Run(context,
        "INSERT INTO partsupp VALUES "
        "(1,1,500,2.0,''), (2,2,300,3.0,'');");
    Run(context,
        "INSERT INTO customer VALUES "
        "(1,'Customer#1','C Address',1,'10-123456',100.0,"
        "'FURNITURE',''), (2,'Customer#2','C Address',3,"
        "'19-123456',50.0,'BUILDING','');");
    Run(context,
        "INSERT INTO orders VALUES "
        "(1,1,'F',1000.0,'1994-02-15','1-URGENT','Clerk',0,'normal'), "
        "(2,2,'F',500.0,'1997-06-15','2-HIGH','Clerk',0,'normal');");
    Run(context,
        "INSERT INTO lineitem VALUES "
        "(1,1,1,1,10.0,100.0,0.08,0.02,'R','O','1994-03-15',"
        "'1994-03-20','1994-03-25','DELIVER IN PERSON','AIR',''), "
        "(2,2,2,1,20.0,200.0,0.05,0.01,'N','F','1997-06-20',"
        "'1997-06-21','1997-06-22','DELIVER IN PERSON','MAIL','');");
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(SqlEngineTpchTest, ExecutesAllTwentyTwoQueries) {
  TransactionContext context = database_->BeginContext();
  CreateSchema(context);
  Seed(context);
  constexpr std::array<size_t, 22> kExpectedRowCounts = {
      2, 0, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0};
  for (size_t i = 0; i < kTpchQueries.size(); ++i) {
    SCOPED_TRACE("TPC-H Q" + std::to_string(i + 1));
    std::vector<Row> rows = Run(context, kTpchQueries[i]);
    EXPECT_EQ(rows.size(), kExpectedRowCounts[i]);
    if (i == 5) {
      ASSERT_EQ(rows.size(), 1);
      EXPECT_EQ(rows[0][0], Value(8.0));
    }
  }
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
}

TEST_F(SqlEngineTpchTest, ExplainReturnsPlanAndAnalyzeReturnsRuntimeProfile) {
  TransactionContext context = database_->BeginContext();
  CreateSchema(context);
  Seed(context);

  auto collect_plan = [&](std::string_view prefix) {
    SqlEngine engine(*database_);
    StatusOr<Executor> prepared = engine.Prepare(
        context, std::string(prefix) + std::string(kTpchQueries[0]));
    EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
    EXPECT_EQ(engine.ResultColumnNames(),
              std::vector<std::string>({"QUERY PLAN"}));
    std::string plan;
    if (!prepared.HasValue()) return plan;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      if (!plan.empty()) plan += '\n';
      plan += std::string(row[0].value.varchar_value);
    }
    return plan;
  };

  const std::string plan = collect_plan("EXPLAIN ");
  EXPECT_NE(plan.find("strategy=greedy_filtered_cardinality"),
            std::string::npos)
      << plan;
  EXPECT_NE(plan.find("Planning Time:"), std::string::npos) << plan;
  EXPECT_EQ(plan.find("Execution Time:"), std::string::npos) << plan;

  const std::string analyzed = collect_plan("EXPLAIN ANALYZE ");
  EXPECT_NE(analyzed.find("hash_joins="), std::string::npos) << analyzed;
  EXPECT_NE(analyzed.find("Actual Rows: 2"), std::string::npos) << analyzed;
  EXPECT_NE(analyzed.find("Execution Time:"), std::string::npos) << analyzed;
  EXPECT_NE(analyzed.find("scan_ms="), std::string::npos) << analyzed;
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
}

TEST_F(SqlEngineTpchTest, FusesAllAggregatesIntoOnePassPerInputRow) {
  TransactionContext context = database_->BeginContext();
  Run(context, "CREATE TABLE metrics (g STRING, v NUMERIC);");
  Run(context,
      "INSERT INTO metrics VALUES ('A',1.0),('A',2.0),('A',2.0),"
      "('A',NULL),('B',NULL);");

  SqlEngine engine(*database_);
  StatusOr<Executor> prepared = engine.Prepare(
      context,
      "SELECT g, COUNT(*) AS row_count, COUNT(v) AS value_count, SUM(v) AS total, "
      "AVG(v) AS mean, MIN(v) AS smallest, MAX(v) AS largest, "
      "COUNT(DISTINCT v) AS unique_values FROM metrics GROUP BY g "
      "HAVING COUNT(*) >= 1 ORDER BY g;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
  std::ostringstream profile;
  prepared.Value()->Dump(profile, 0);
  EXPECT_NE(profile.str().find("aggregate_input_rows=5"), std::string::npos)
      << profile.str();
  EXPECT_NE(profile.str().find("aggregate_groups=2"), std::string::npos)
      << profile.str();

  Row row;
  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  ASSERT_EQ(row.values_.size(), 8);
  EXPECT_EQ(row[0], Value("A"));
  EXPECT_EQ(row[1], Value(4));
  EXPECT_EQ(row[2], Value(3));
  EXPECT_EQ(row[3], Value(5.0));
  EXPECT_NEAR(row[4].value.double_value, 5.0 / 3.0, 1e-9);
  EXPECT_EQ(row[5], Value(1.0));
  EXPECT_EQ(row[6], Value(2.0));
  EXPECT_EQ(row[7], Value(2));
  ASSERT_TRUE(prepared.Value()->Next(&row, nullptr));
  EXPECT_EQ(row[0], Value("B"));
  EXPECT_EQ(row[1], Value(1));
  EXPECT_EQ(row[2], Value(0));
  EXPECT_TRUE(row[3].IsNull());
  EXPECT_TRUE(row[4].IsNull());
  EXPECT_TRUE(row[5].IsNull());
  EXPECT_TRUE(row[6].IsNull());
  EXPECT_EQ(row[7], Value(0));
  EXPECT_FALSE(prepared.Value()->Next(&row, nullptr));

  Run(context, "CREATE TABLE empty_metrics (v NUMERIC);");
  std::vector<Row> empty =
      Run(context,
          "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) "
          "FROM empty_metrics;");
  ASSERT_EQ(empty.size(), 1);
  EXPECT_EQ(empty[0][0], Value(0));
  for (size_t i = 1; i < empty[0].values_.size(); ++i) {
    EXPECT_TRUE(empty[0][i].IsNull());
  }
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
}

TEST_F(SqlEngineTpchTest, PrunesScanColumnsAndFiltersBeforeMaterialization) {
  TransactionContext context = database_->BeginContext();
  Run(context,
      "CREATE TABLE wide (a INT64, b INT64, c STRING, d NUMERIC);");
  Run(context,
      "INSERT INTO wide VALUES (1,10,'x',1.0),(2,20,'y',2.0),"
      "(3,30,'z',3.0),(4,40,'w',4.0);");

  SqlEngine engine(*database_);
  StatusOr<Executor> prepared = engine.Prepare(
      context,
      "SELECT SUM(a) AS total FROM wide WHERE b >= 20 AND b < 40 "
      "HAVING SUM(a) > 0;");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
  std::ostringstream profile;
  prepared.Value()->Dump(profile, 0);
  EXPECT_NE(profile.str().find("scan_rows=4"), std::string::npos)
      << profile.str();
  EXPECT_NE(profile.str().find("scan_output_rows=2"), std::string::npos)
      << profile.str();
  EXPECT_NE(profile.str().find("scan_values_decoded=8"), std::string::npos)
      << profile.str();
  EXPECT_NE(profile.str().find("scan_values_available=16"), std::string::npos)
      << profile.str();
  EXPECT_NE(profile.str().find("aggregate_input_rows=2"), std::string::npos)
      << profile.str();
  Row result;
  ASSERT_TRUE(prepared.Value()->Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(5));

  StatusOr<Executor> count =
      engine.Prepare(context,
                     "SELECT COUNT(*) FROM wide HAVING COUNT(*) >= 0;");
  ASSERT_TRUE(count.HasValue()) << engine.LastError();
  std::ostringstream count_profile;
  count.Value()->Dump(count_profile, 0);
  EXPECT_NE(count_profile.str().find("scan_values_decoded=0"),
            std::string::npos)
      << count_profile.str();
  ASSERT_TRUE(count.Value()->Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(4));
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
}

TEST_F(SqlEngineTpchTest, UsesHashJoinsWithoutMaterializingCartesianProducts) {
  TransactionContext context = database_->BeginContext();
  CreateSchema(context);

  std::ostringstream customers;
  std::ostringstream orders;
  std::ostringstream lineitems;
  customers << "INSERT INTO customer VALUES ";
  orders << "INSERT INTO orders VALUES ";
  lineitems << "INSERT INTO lineitem VALUES ";
  constexpr size_t kRows = 256;
  for (size_t i = 1; i <= kRows; ++i) {
    if (i != 1) {
      customers << ",";
      orders << ",";
      lineitems << ",";
    }
    customers << "(" << i
              << ",'Customer','Address',1,'10-000',1.0,'BUILDING','')";
    orders << "(" << i << "," << i
           << ",'O',10.0,'1995-01-01','1-URGENT','Clerk',0,'')";
    lineitems << "(" << i << ",1,1,1,1.0,10.0,0.0,0.0,'N','O',"
              << "'1995-01-02','1995-01-03','1995-01-04',"
              << "'DELIVER IN PERSON','AIR','')";
  }
  customers << ";";
  orders << ";";
  lineitems << ";";
  Run(context, customers.str());
  Run(context, orders.str());
  Run(context, lineitems.str());

  SqlEngine engine(*database_);
  StatusOr<Executor> prepared = engine.Prepare(
      context,
      "SELECT COUNT(*) AS matched FROM customer, orders, lineitem "
      "WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey "
      "AND l_shipdate >= date '1995-01-01';");
  ASSERT_TRUE(prepared.HasValue()) << engine.LastError();
  std::ostringstream plan;
  prepared.Value()->Dump(plan, 0);
  EXPECT_NE(plan.str().find("hash_joins=2"), std::string::npos) << plan.str();
  EXPECT_NE(plan.str().find("nested_loop_joins=0"), std::string::npos)
      << plan.str();
  EXPECT_NE(plan.str().find("join_comparisons=512"), std::string::npos)
      << plan.str();

  Row result;
  ASSERT_TRUE(prepared.Value()->Next(&result, nullptr));
  ASSERT_EQ(result.values_.size(), 1);
  EXPECT_EQ(result[0], Value(static_cast<int64_t>(kRows)));
  EXPECT_FALSE(prepared.Value()->Next(&result, nullptr));

  StatusOr<Executor> correlated = engine.Prepare(
      context,
      "SELECT o_orderpriority, COUNT(*) AS order_count FROM orders "
      "WHERE EXISTS (SELECT * FROM lineitem "
      "WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) "
      "GROUP BY o_orderpriority;");
  ASSERT_TRUE(correlated.HasValue()) << engine.LastError();
  std::ostringstream correlated_plan;
  correlated.Value()->Dump(correlated_plan, 0);
  EXPECT_NE(correlated_plan.str().find("correlated_index_builds=1"),
            std::string::npos)
      << correlated_plan.str();
  EXPECT_NE(correlated_plan.str().find("correlated_index_probes=256"),
            std::string::npos)
      << correlated_plan.str();
  ASSERT_TRUE(correlated.Value()->Next(&result, nullptr));
  EXPECT_EQ(result[1], Value(static_cast<int64_t>(kRows)));

  StatusOr<Executor> uncorrelated =
      engine.Prepare(context,
                     "SELECT COUNT(*) FROM orders WHERE o_orderkey IN "
                     "(SELECT l_orderkey FROM lineitem GROUP BY l_orderkey "
                     "HAVING sum(l_quantity) > 0);");
  ASSERT_TRUE(uncorrelated.HasValue()) << engine.LastError();
  std::ostringstream uncorrelated_plan;
  uncorrelated.Value()->Dump(uncorrelated_plan, 0);
  EXPECT_NE(uncorrelated_plan.str().find("uncorrelated_cache_hits=255"),
            std::string::npos)
      << uncorrelated_plan.str();
  EXPECT_NE(uncorrelated_plan.str().find("uncorrelated_hash_builds=1"),
            std::string::npos)
      << uncorrelated_plan.str();
  EXPECT_NE(uncorrelated_plan.str().find("uncorrelated_hash_probes=256"),
            std::string::npos)
      << uncorrelated_plan.str();
  ASSERT_TRUE(uncorrelated.Value()->Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(static_cast<int64_t>(kRows)));

  StatusOr<Executor> reused_base = engine.Prepare(
      context,
      "SELECT COUNT(*) FROM lineitem WHERE l_orderkey IN "
      "(SELECT l_orderkey FROM lineitem GROUP BY l_orderkey "
      "HAVING SUM(l_quantity) > 0);");
  ASSERT_TRUE(reused_base.HasValue()) << engine.LastError();
  std::ostringstream reused_base_plan;
  reused_base.Value()->Dump(reused_base_plan, 0);
  EXPECT_NE(reused_base_plan.str().find("base_scan_cache_hits=2"),
            std::string::npos)
      << reused_base_plan.str();

  StatusOr<Executor> left_join =
      engine.Prepare(context,
                     "SELECT COUNT(*) FROM customer LEFT JOIN orders "
                     "ON c_custkey = o_custkey;");
  ASSERT_TRUE(left_join.HasValue()) << engine.LastError();
  std::ostringstream left_join_plan;
  left_join.Value()->Dump(left_join_plan, 0);
  EXPECT_NE(left_join_plan.str().find("hash_joins=1"), std::string::npos)
      << left_join_plan.str();
  EXPECT_NE(left_join_plan.str().find("nested_loop_joins=0"), std::string::npos)
      << left_join_plan.str();
  EXPECT_NE(left_join_plan.str().find("join_comparisons=256"),
            std::string::npos)
      << left_join_plan.str();
  ASSERT_TRUE(left_join.Value()->Next(&result, nullptr));
  EXPECT_EQ(result[0], Value(static_cast<int64_t>(kRows)));
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
}

}  // namespace
}  // namespace tinylamb
