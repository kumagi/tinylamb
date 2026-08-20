/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_BENCHMARK_TPCH_QUERIES_HPP
#define TINYLAMB_BENCHMARK_TPCH_QUERIES_HPP

#include <array>
#include <string_view>

namespace tinylamb {

// TPC-H query templates with fixed substitution parameters, expressed in the
// GoogleSQL-compatible dialect accepted by tinylamb.
inline constexpr std::array<std::string_view, 22> kTpchBenchmarkQueries = {
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
      FROM supplier, lineitem l1, orders, nation,
           (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey
            HAVING COUNT(DISTINCT l_suppkey) > 1) multi,
           (SELECT l_orderkey FROM lineitem
            WHERE l_receiptdate > l_commitdate GROUP BY l_orderkey
            HAVING COUNT(DISTINCT l_suppkey) = 1) only_late
      WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
      AND l1.l_orderkey = multi.l_orderkey
      AND l1.l_orderkey = only_late.l_orderkey
      AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
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
            AND c_custkey NOT IN (SELECT o_custkey FROM orders)) custsale
      GROUP BY cntrycode ORDER BY cntrycode;)sql"};

}  // namespace tinylamb

#endif  // TINYLAMB_BENCHMARK_TPCH_QUERIES_HPP
