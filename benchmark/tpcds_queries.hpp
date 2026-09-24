/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */
#ifndef TINYLAMB_BENCHMARK_TPCDS_QUERIES_HPP
#define TINYLAMB_BENCHMARK_TPCDS_QUERIES_HPP

// TPC-DS v2.7 query templates (the Apache Spark test-resources rendering of
// the official dsqgen output) ported to tinylamb's GoogleSQL dialect over the
// tpcds_fixture.hpp schema subset.  Adaptations against the official text:
//  - DECIMAL columns/casts are DOUBLE in this fixture (engine numeric
//    surface), so q18's CAST(... AS DECIMAL(12,2)) wrappers are dropped;
//  - q47's ORDER BY ordinal `3` is spelled out as `s_store_name` (the column
//    the ordinal denotes) because ORDER BY ordinals over `SELECT *` are not
//    resolved on every planning route;
//  - each query keeps its template LIMIT 100 / ORDER BY as generated.
// Not an official TPC-DS result publication: this is an engine-workload
// harness, not a benchmark compliance submission.

#include <array>
#include <string_view>

namespace tinylamb::tpcds {

struct TpcdsQuery {
  std::string_view name;
  std::string_view sql;  // R"sql(...)sql"
};
inline constexpr std::string_view kQ1Sql = R"sql(WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100)sql";
inline constexpr std::string_view kQ3Sql = R"sql(SELECT
  dt.d_year,
  item.i_brand_id brand_id,
  item.i_brand brand,
  SUM(ss_ext_sales_price) sum_agg
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manufact_id = 128
  AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100)sql";
inline constexpr std::string_view kQ6Sql = R"sql(SELECT
  a.ca_state state,
  count(*) cnt
FROM
  customer_address a, customer c, store_sales s, date_dim d, item i
WHERE a.ca_address_sk = c.c_current_addr_sk
  AND c.c_customer_sk = s.ss_customer_sk
  AND s.ss_sold_date_sk = d.d_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND d.d_month_seq =
  (SELECT DISTINCT (d_month_seq)
  FROM date_dim
  WHERE d_year = 2000 AND d_moy = 1)
  AND i.i_current_price > 1.2 *
  (SELECT avg(j.i_current_price)
  FROM item j
  WHERE j.i_category = i.i_category)
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt
LIMIT 100)sql";
inline constexpr std::string_view kQ7Sql = R"sql(SELECT
  i_item_id,
  avg(ss_quantity) agg1,
  avg(ss_list_price) agg2,
  avg(ss_coupon_amt) agg3,
  avg(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk AND
  ss_item_sk = i_item_sk AND
  ss_cdemo_sk = cd_demo_sk AND
  ss_promo_sk = p_promo_sk AND
  cd_gender = 'M' AND
  cd_marital_status = 'S' AND
  cd_education_status = 'College' AND
  (p_channel_email = 'N' OR p_channel_event = 'N') AND
  d_year = 2000
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100)sql";
inline constexpr std::string_view kQ12Sql = R"sql(SELECT
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(ws_ext_sales_price) AS itemrevenue,
  sum(ws_ext_sales_price) * 100 / sum(sum(ws_ext_sales_price))
  OVER
  (PARTITION BY i_class) AS revenueratio
FROM
  web_sales, item, date_dim
WHERE
  ws_item_sk = i_item_sk
    AND i_category IN ('Sports', 'Books', 'Home')
    AND ws_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('1999-02-22' AS DATE)
  AND (cast('1999-02-22' AS DATE) + INTERVAL 30 days)
GROUP BY
  i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY
  i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100)sql";
inline constexpr std::string_view kQ17Sql = R"sql(SELECT
  i_item_id,
  i_item_desc,
  s_state,
  count(ss_quantity) AS store_sales_quantitycount,
  avg(ss_quantity) AS store_sales_quantityave,
  stddev_samp(ss_quantity) AS store_sales_quantitystdev,
  stddev_samp(ss_quantity) / avg(ss_quantity) AS store_sales_quantitycov,
  count(sr_return_quantity) as_store_returns_quantitycount,
  avg(sr_return_quantity) as_store_returns_quantityave,
  stddev_samp(sr_return_quantity) as_store_returns_quantitystdev,
  stddev_samp(sr_return_quantity) / avg(sr_return_quantity) AS store_returns_quantitycov,
  count(cs_quantity) AS catalog_sales_quantitycount,
  avg(cs_quantity) AS catalog_sales_quantityave,
  stddev_samp(cs_quantity) / avg(cs_quantity) AS catalog_sales_quantitystdev,
  stddev_samp(cs_quantity) / avg(cs_quantity) AS catalog_sales_quantitycov
FROM store_sales, store_returns, catalog_sales, date_dim d1, date_dim d2, date_dim d3, store, item
WHERE d1.d_quarter_name = '2001Q1'
  AND d1.d_date_sk = ss_sold_date_sk
  AND i_item_sk = ss_item_sk
  AND s_store_sk = ss_store_sk
  AND ss_customer_sk = sr_customer_sk
  AND ss_item_sk = sr_item_sk
  AND ss_ticket_number = sr_ticket_number
  AND sr_returned_date_sk = d2.d_date_sk
  AND d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
  AND sr_customer_sk = cs_bill_customer_sk
  AND sr_item_sk = cs_item_sk
  AND cs_sold_date_sk = d3.d_date_sk
  AND d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
GROUP BY i_item_id, i_item_desc, s_state
ORDER BY i_item_id, i_item_desc, s_state
LIMIT 100)sql";
inline constexpr std::string_view kQ18Sql = R"sql(SELECT
  i_item_id,
  ca_country,
  ca_state,
  ca_county,
  avg((cs_quantity)) agg1,
  avg((cs_list_price)) agg2,
  avg((cs_coupon_amt)) agg3,
  avg((cs_sales_price)) agg4,
  avg((cs_net_profit)) agg5,
  avg((c_birth_year)) agg6,
  avg((cd1.cd_dep_count)) agg7
FROM catalog_sales, customer_demographics cd1,
  customer_demographics cd2, customer, customer_address, date_dim, item
WHERE cs_sold_date_sk = d_date_sk AND
  cs_item_sk = i_item_sk AND
  cs_bill_cdemo_sk = cd1.cd_demo_sk AND
  cs_bill_customer_sk = c_customer_sk AND
  cd1.cd_gender = 'F' AND
  cd1.cd_education_status = 'Unknown' AND
  c_current_cdemo_sk = cd2.cd_demo_sk AND
  c_current_addr_sk = ca_address_sk AND
  c_birth_month IN (1, 6, 8, 9, 12, 2) AND
  d_year = 1998 AND
  ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS')
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country, ca_state, ca_county, i_item_id
LIMIT 100)sql";
inline constexpr std::string_view kQ21Sql = R"sql(SELECT *
FROM (
       SELECT
         w_warehouse_name,
         i_item_id,
         sum(CASE WHEN (cast(d_date AS DATE) < cast('2000-03-11' AS DATE))
           THEN inv_quantity_on_hand
             ELSE 0 END) AS inv_before,
         sum(CASE WHEN (cast(d_date AS DATE) >= cast('2000-03-11' AS DATE))
           THEN inv_quantity_on_hand
             ELSE 0 END) AS inv_after
       FROM inventory, warehouse, item, date_dim
       WHERE i_current_price BETWEEN 0.99 AND 1.49
         AND i_item_sk = inv_item_sk
         AND inv_warehouse_sk = w_warehouse_sk
         AND inv_date_sk = d_date_sk
         AND d_date BETWEEN (cast('2000-03-11' AS DATE) - INTERVAL 30 days)
       AND (cast('2000-03-11' AS DATE) + INTERVAL 30 days)
       GROUP BY w_warehouse_name, i_item_id) x
WHERE (CASE WHEN inv_before > 0
  THEN inv_after / inv_before
       ELSE NULL
       END) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100)sql";
inline constexpr std::string_view kQ26Sql = R"sql(SELECT
  i_item_id,
  avg(cs_quantity) agg1,
  avg(cs_list_price) agg2,
  avg(cs_coupon_amt) agg3,
  avg(cs_sales_price) agg4
FROM catalog_sales, customer_demographics, date_dim, item, promotion
WHERE cs_sold_date_sk = d_date_sk AND
  cs_item_sk = i_item_sk AND
  cs_bill_cdemo_sk = cd_demo_sk AND
  cs_promo_sk = p_promo_sk AND
  cd_gender = 'M' AND
  cd_marital_status = 'S' AND
  cd_education_status = 'College' AND
  (p_channel_email = 'N' OR p_channel_event = 'N') AND
  d_year = 2000
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100)sql";
inline constexpr std::string_view kQ42Sql = R"sql(SELECT
  dt.d_year,
  item.i_category_id,
  item.i_category,
  sum(ss_ext_sales_price)
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manager_id = 1
  AND dt.d_moy = 11
  AND dt.d_year = 2000
GROUP BY dt.d_year
  , item.i_category_id
  , item.i_category
ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year
  , item.i_category_id
  , item.i_category
LIMIT 100)sql";
inline constexpr std::string_view kQ52Sql = R"sql(SELECT
  dt.d_year,
  item.i_brand_id brand_id,
  item.i_brand brand,
  sum(ss_ext_sales_price) ext_price
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manager_id = 1
  AND dt.d_moy = 11
  AND dt.d_year = 2000
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, ext_price DESC, brand_id
LIMIT 100)sql";
inline constexpr std::string_view kQ55Sql = R"sql(SELECT
  i_brand_id brand_id,
  i_brand brand,
  sum(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 28
  AND d_moy = 11
  AND d_year = 1999
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, brand_id
LIMIT 100)sql";
inline constexpr std::string_view kQ61Sql = R"sql(SELECT
  promotions,
  total,
  cast(promotions AS DECIMAL(15, 4)) / cast(total AS DECIMAL(15, 4)) * 100
FROM
  (SELECT sum(ss_ext_sales_price) promotions
  FROM store_sales, store, promotion, date_dim, customer, customer_address, item
  WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_promo_sk = p_promo_sk
    AND ss_customer_sk = c_customer_sk
    AND ca_address_sk = c_current_addr_sk
    AND ss_item_sk = i_item_sk
    AND ca_gmt_offset = -5
    AND i_category = 'Jewelry'
    AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
    AND s_gmt_offset = -5
    AND d_year = 1998
    AND d_moy = 11) promotional_sales,
  (SELECT sum(ss_ext_sales_price) total
  FROM store_sales, store, date_dim, customer, customer_address, item
  WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_customer_sk = c_customer_sk
    AND ca_address_sk = c_current_addr_sk
    AND ss_item_sk = i_item_sk
    AND ca_gmt_offset = -5
    AND i_category = 'Jewelry'
    AND s_gmt_offset = -5
    AND d_year = 1998
    AND d_moy = 11) all_sales
ORDER BY promotions, total
LIMIT 100)sql";
inline constexpr std::string_view kQ68Sql = R"sql(SELECT
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number,
  extended_price,
  extended_tax,
  list_price
FROM (SELECT
  ss_ticket_number,
  ss_customer_sk,
  ca_city bought_city,
  sum(ss_ext_sales_price) extended_price,
  sum(ss_ext_list_price) list_price,
  sum(ss_ext_tax) extended_tax
FROM store_sales, date_dim, store, household_demographics, customer_address
WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
  AND store_sales.ss_store_sk = store.s_store_sk
  AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  AND store_sales.ss_addr_sk = customer_address.ca_address_sk
  AND date_dim.d_dom BETWEEN 1 AND 2
  AND (household_demographics.hd_dep_count = 4 OR
  household_demographics.hd_vehicle_count = 3)
  AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
  AND store.s_city IN ('Midway', 'Fairview')
GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city) dn,
  customer,
  customer_address current_addr
WHERE ss_customer_sk = c_customer_sk
  AND customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name, ss_ticket_number
LIMIT 100)sql";
inline constexpr std::string_view kQ73Sql = R"sql(SELECT
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
FROM
  (SELECT
    ss_ticket_number,
    ss_customer_sk,
    count(*) cnt
  FROM store_sales, date_dim, store, household_demographics
  WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND store_sales.ss_store_sk = store.s_store_sk
    AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND date_dim.d_dom BETWEEN 1 AND 2
    AND (household_demographics.hd_buy_potential = '>10000' OR
    household_demographics.hd_buy_potential = 'unknown')
    AND household_demographics.hd_vehicle_count > 0
    AND CASE WHEN household_demographics.hd_vehicle_count > 0
    THEN
      household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
        ELSE NULL END > 1
    AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
    AND store.s_county IN ('Williamson County', 'Franklin Parish', 'Bronx County', 'Orange County')
  GROUP BY ss_ticket_number, ss_customer_sk) dj, customer
WHERE ss_customer_sk = c_customer_sk
  AND cnt BETWEEN 1 AND 5
ORDER BY cnt DESC)sql";
inline constexpr std::string_view kQ89Sql = R"sql(SELECT *
FROM (
       SELECT
         i_category,
         i_class,
         i_brand,
         s_store_name,
         s_company_name,
         d_moy,
         sum(ss_sales_price) sum_sales,
         avg(sum(ss_sales_price))
         OVER
         (PARTITION BY i_category, i_brand, s_store_name, s_company_name)
         avg_monthly_sales
       FROM item, store_sales, date_dim, store
       WHERE ss_item_sk = i_item_sk AND
         ss_sold_date_sk = d_date_sk AND
         ss_store_sk = s_store_sk AND
         d_year IN (1999) AND
         ((i_category IN ('Books', 'Electronics', 'Sports') AND
           i_class IN ('computers', 'stereo', 'football'))
           OR (i_category IN ('Men', 'Jewelry', 'Women') AND
           i_class IN ('shirts', 'birdal', 'dresses')))
       GROUP BY i_category, i_class, i_brand,
         s_store_name, s_company_name, d_moy) tmp1
WHERE CASE WHEN (avg_monthly_sales <> 0)
  THEN (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
      ELSE NULL END > 0.1
ORDER BY sum_sales - avg_monthly_sales, s_store_name
LIMIT 100)sql";
inline constexpr std::string_view kQ2Sql = R"sql(WITH wscs AS
( SELECT
    sold_date_sk,
    sales_price
  FROM (SELECT
    ws_sold_date_sk sold_date_sk,
    ws_ext_sales_price sales_price
  FROM web_sales) x
  UNION ALL
  (SELECT
    cs_sold_date_sk sold_date_sk,
    cs_ext_sales_price sales_price
  FROM catalog_sales)),
    wswscs AS
  ( SELECT
    d_week_seq,
    sum(CASE WHEN (d_day_name = 'Sunday')
      THEN sales_price
        ELSE NULL END)
    sun_sales,
    sum(CASE WHEN (d_day_name = 'Monday')
      THEN sales_price
        ELSE NULL END)
    mon_sales,
    sum(CASE WHEN (d_day_name = 'Tuesday')
      THEN sales_price
        ELSE NULL END)
    tue_sales,
    sum(CASE WHEN (d_day_name = 'Wednesday')
      THEN sales_price
        ELSE NULL END)
    wed_sales,
    sum(CASE WHEN (d_day_name = 'Thursday')
      THEN sales_price
        ELSE NULL END)
    thu_sales,
    sum(CASE WHEN (d_day_name = 'Friday')
      THEN sales_price
        ELSE NULL END)
    fri_sales,
    sum(CASE WHEN (d_day_name = 'Saturday')
      THEN sales_price
        ELSE NULL END)
    sat_sales
  FROM wscs, date_dim
  WHERE d_date_sk = sold_date_sk
  GROUP BY d_week_seq)
SELECT
  d_week_seq1,
  round(sun_sales1 / sun_sales2, 2),
  round(mon_sales1 / mon_sales2, 2),
  round(tue_sales1 / tue_sales2, 2),
  round(wed_sales1 / wed_sales2, 2),
  round(thu_sales1 / thu_sales2, 2),
  round(fri_sales1 / fri_sales2, 2),
  round(sat_sales1 / sat_sales2, 2)
FROM
  (SELECT
    wswscs.d_week_seq d_week_seq1,
    sun_sales sun_sales1,
    mon_sales mon_sales1,
    tue_sales tue_sales1,
    wed_sales wed_sales1,
    thu_sales thu_sales1,
    fri_sales fri_sales1,
    sat_sales sat_sales1
  FROM wswscs, date_dim
  WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001) y,
  (SELECT
    wswscs.d_week_seq d_week_seq2,
    sun_sales sun_sales2,
    mon_sales mon_sales2,
    tue_sales tue_sales2,
    wed_sales wed_sales2,
    thu_sales thu_sales2,
    fri_sales fri_sales2,
    sat_sales sat_sales2
  FROM wswscs, date_dim
  WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001 + 1) z
WHERE d_week_seq1 = d_week_seq2 - 53
ORDER BY d_week_seq1)sql";
inline constexpr std::string_view kQ9Sql = R"sql(SELECT
  CASE WHEN (SELECT count(*)
  FROM store_sales
  WHERE ss_quantity BETWEEN 1 AND 20) > 62316685
    THEN (SELECT avg(ss_ext_discount_amt)
    FROM store_sales
    WHERE ss_quantity BETWEEN 1 AND 20)
  ELSE (SELECT avg(ss_net_paid)
  FROM store_sales
  WHERE ss_quantity BETWEEN 1 AND 20) END bucket1,
  CASE WHEN (SELECT count(*)
  FROM store_sales
  WHERE ss_quantity BETWEEN 21 AND 40) > 19045798
    THEN (SELECT avg(ss_ext_discount_amt)
    FROM store_sales
    WHERE ss_quantity BETWEEN 21 AND 40)
  ELSE (SELECT avg(ss_net_paid)
  FROM store_sales
  WHERE ss_quantity BETWEEN 21 AND 40) END bucket2,
  CASE WHEN (SELECT count(*)
  FROM store_sales
  WHERE ss_quantity BETWEEN 41 AND 60) > 365541424
    THEN (SELECT avg(ss_ext_discount_amt)
    FROM store_sales
    WHERE ss_quantity BETWEEN 41 AND 60)
  ELSE (SELECT avg(ss_net_paid)
  FROM store_sales
  WHERE ss_quantity BETWEEN 41 AND 60) END bucket3,
  CASE WHEN (SELECT count(*)
  FROM store_sales
  WHERE ss_quantity BETWEEN 61 AND 80) > 216357808
    THEN (SELECT avg(ss_ext_discount_amt)
    FROM store_sales
    WHERE ss_quantity BETWEEN 61 AND 80)
  ELSE (SELECT avg(ss_net_paid)
  FROM store_sales
  WHERE ss_quantity BETWEEN 61 AND 80) END bucket4,
  CASE WHEN (SELECT count(*)
  FROM store_sales
  WHERE ss_quantity BETWEEN 81 AND 100) > 184483884
    THEN (SELECT avg(ss_ext_discount_amt)
    FROM store_sales
    WHERE ss_quantity BETWEEN 81 AND 100)
  ELSE (SELECT avg(ss_net_paid)
  FROM store_sales
  WHERE ss_quantity BETWEEN 81 AND 100) END bucket5
FROM reason
WHERE r_reason_sk = 1)sql";
inline constexpr std::string_view kQ10Sql = R"sql(SELECT
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
FROM
  customer c, customer_address ca, customer_demographics
WHERE
  c.c_current_addr_sk = ca.ca_address_sk AND
    ca_county IN ('Rush County', 'Toole County', 'Jefferson County',
                  'Dona Ana County', 'La Porte County') AND
    cd_demo_sk = c.c_current_cdemo_sk AND
    exists(SELECT *
           FROM store_sales, date_dim
           WHERE c.c_customer_sk = ss_customer_sk AND
             ss_sold_date_sk = d_date_sk AND
             d_year = 2002 AND
             d_moy BETWEEN 1 AND 1 + 3) AND
    (exists(SELECT *
            FROM web_sales, date_dim
            WHERE c.c_customer_sk = ws_bill_customer_sk AND
              ws_sold_date_sk = d_date_sk AND
              d_year = 2002 AND
              d_moy BETWEEN 1 AND 1 + 3) OR
      exists(SELECT *
             FROM catalog_sales, date_dim
             WHERE c.c_customer_sk = cs_ship_customer_sk AND
               cs_sold_date_sk = d_date_sk AND
               d_year = 2002 AND
               d_moy BETWEEN 1 AND 1 + 3))
GROUP BY cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
ORDER BY cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
LIMIT 100)sql";
inline constexpr std::string_view kQ11Sql = R"sql(WITH year_total AS (
  SELECT
    c_customer_id customer_id,
    c_first_name customer_first_name,
    c_last_name customer_last_name,
    c_preferred_cust_flag customer_preferred_cust_flag,
    c_birth_country customer_birth_country,
    c_login customer_login,
    c_email_address customer_email_address,
    d_year dyear,
    sum(ss_ext_list_price - ss_ext_discount_amt) year_total,
    's' sale_type
  FROM customer, store_sales, date_dim
  WHERE c_customer_sk = ss_customer_sk
    AND ss_sold_date_sk = d_date_sk
  GROUP BY c_customer_id
    , c_first_name
    , c_last_name
    , d_year
    , c_preferred_cust_flag
    , c_birth_country
    , c_login
    , c_email_address
  UNION ALL
  SELECT
    c_customer_id customer_id,
    c_first_name customer_first_name,
    c_last_name customer_last_name,
    c_preferred_cust_flag customer_preferred_cust_flag,
    c_birth_country customer_birth_country,
    c_login customer_login,
    c_email_address customer_email_address,
    d_year dyear,
    sum(ws_ext_list_price - ws_ext_discount_amt) year_total,
    'w' sale_type
  FROM customer, web_sales, date_dim
  WHERE c_customer_sk = ws_bill_customer_sk
    AND ws_sold_date_sk = d_date_sk
  GROUP BY
    c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country,
    c_login, c_email_address, d_year)
SELECT t_s_secyear.customer_preferred_cust_flag
FROM year_total t_s_firstyear
  , year_total t_s_secyear
  , year_total t_w_firstyear
  , year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 2001
  AND t_s_secyear.dyear = 2001 + 1
  AND t_w_firstyear.dyear = 2001
  AND t_w_secyear.dyear = 2001 + 1
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE WHEN t_w_firstyear.year_total > 0
  THEN t_w_secyear.year_total / t_w_firstyear.year_total
      ELSE NULL END
  > CASE WHEN t_s_firstyear.year_total > 0
  THEN t_s_secyear.year_total / t_s_firstyear.year_total
    ELSE NULL END
ORDER BY t_s_secyear.customer_preferred_cust_flag
LIMIT 100)sql";
inline constexpr std::string_view kQ13Sql = R"sql(SELECT
  avg(ss_quantity),
  avg(ss_ext_sales_price),
  avg(ss_ext_wholesale_cost),
  sum(ss_ext_wholesale_cost)
FROM store_sales
  , store
  , customer_demographics
  , household_demographics
  , customer_address
  , date_dim
WHERE s_store_sk = ss_store_sk
  AND ss_sold_date_sk = d_date_sk AND d_year = 2001
  AND ((ss_hdemo_sk = hd_demo_sk
  AND cd_demo_sk = ss_cdemo_sk
  AND cd_marital_status = 'M'
  AND cd_education_status = 'Advanced Degree'
  AND ss_sales_price BETWEEN 100.00 AND 150.00
  AND hd_dep_count = 3
) OR
  (ss_hdemo_sk = hd_demo_sk
    AND cd_demo_sk = ss_cdemo_sk
    AND cd_marital_status = 'S'
    AND cd_education_status = 'College'
    AND ss_sales_price BETWEEN 50.00 AND 100.00
    AND hd_dep_count = 1
  ) OR
  (ss_hdemo_sk = hd_demo_sk
    AND cd_demo_sk = ss_cdemo_sk
    AND cd_marital_status = 'W'
    AND cd_education_status = '2 yr Degree'
    AND ss_sales_price BETWEEN 150.00 AND 200.00
    AND hd_dep_count = 1
  ))
  AND ((ss_addr_sk = ca_address_sk
  AND ca_country = 'United States'
  AND ca_state IN ('TX', 'OH', 'TX')
  AND ss_net_profit BETWEEN 100 AND 200
) OR
  (ss_addr_sk = ca_address_sk
    AND ca_country = 'United States'
    AND ca_state IN ('OR', 'NM', 'KY')
    AND ss_net_profit BETWEEN 150 AND 300
  ) OR
  (ss_addr_sk = ca_address_sk
    AND ca_country = 'United States'
    AND ca_state IN ('VA', 'TX', 'MS')
    AND ss_net_profit BETWEEN 50 AND 250
  )))sql";
inline constexpr std::string_view kQ15Sql = R"sql(SELECT
  ca_zip,
  sum(cs_sales_price)
FROM catalog_sales, customer, customer_address, date_dim
WHERE cs_bill_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND (substr(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475',
                                '85392', '85460', '80348', '81792')
  OR ca_state IN ('CA', 'WA', 'GA')
  OR cs_sales_price > 500)
  AND cs_sold_date_sk = d_date_sk
  AND d_qoy = 2 AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100)sql";
inline constexpr std::string_view kQ16Sql = R"sql(SELECT
  count(DISTINCT cs_order_number) AS `order count `,
  sum(cs_ext_ship_cost) AS `total shipping cost `,
  sum(cs_net_profit) AS `total net profit `
FROM
  catalog_sales cs1, date_dim, customer_address, call_center
WHERE
  d_date BETWEEN '2002-02-01' AND (CAST('2002-02-01' AS DATE) + INTERVAL 60 days)
    AND cs1.cs_ship_date_sk = d_date_sk
    AND cs1.cs_ship_addr_sk = ca_address_sk
    AND ca_state = 'GA'
    AND cs1.cs_call_center_sk = cc_call_center_sk
    AND cc_county IN
    ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
    AND EXISTS(SELECT *
               FROM catalog_sales cs2
               WHERE cs1.cs_order_number = cs2.cs_order_number
                 AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
    AND NOT EXISTS(SELECT *
                   FROM catalog_returns cr1
                   WHERE cs1.cs_order_number = cr1.cr_order_number)
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100)sql";
inline constexpr std::string_view kQ19Sql = R"sql(SELECT
  i_brand_id brand_id,
  i_brand brand,
  i_manufact_id,
  i_manufact,
  sum(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item, customer, customer_address, store
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 8
  AND d_moy = 11
  AND d_year = 1998
  AND ss_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
  AND ss_store_sk = s_store_sk
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, brand, brand_id, i_manufact_id, i_manufact
LIMIT 100)sql";

inline constexpr std::string_view kQ40Sql = R"sql(SELECT
  w_state,
  i_item_id,
  sum(CASE WHEN (cast(d_date AS DATE) < cast('2000-03-11' AS DATE))
    THEN cs_sales_price - coalesce(cr_refunded_cash, 0)
      ELSE 0 END) AS sales_before,
  sum(CASE WHEN (cast(d_date AS DATE) >= cast('2000-03-11' AS DATE))
    THEN cs_sales_price - coalesce(cr_refunded_cash, 0)
      ELSE 0 END) AS sales_after
FROM
  catalog_sales
  LEFT OUTER JOIN catalog_returns ON
                                    (cs_order_number = cr_order_number
                                      AND cs_item_sk = cr_item_sk)
  , warehouse, item, date_dim
WHERE
  i_current_price BETWEEN 0.99 AND 1.49
    AND i_item_sk = cs_item_sk
    AND cs_warehouse_sk = w_warehouse_sk
    AND cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN (cast('2000-03-11' AS DATE) - INTERVAL 30 days)
  AND (cast('2000-03-11' AS DATE) + INTERVAL 30 days)
GROUP BY w_state, i_item_id
ORDER BY w_state, i_item_id
LIMIT 100)sql";

inline constexpr std::string_view kQ46Sql = R"sql(SELECT
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number,
  amt,
  profit
FROM
  (SELECT
    ss_ticket_number,
    ss_customer_sk,
    ca_city bought_city,
    sum(ss_coupon_amt) amt,
    sum(ss_net_profit) profit
  FROM store_sales, date_dim, store, household_demographics, customer_address
  WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND store_sales.ss_store_sk = store.s_store_sk
    AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND store_sales.ss_addr_sk = customer_address.ca_address_sk
    AND (household_demographics.hd_dep_count = 4 OR
    household_demographics.hd_vehicle_count = 3)
    AND date_dim.d_dow IN (6, 0)
    AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
    AND store.s_city IN ('Fairview', 'Midway', 'Fairview', 'Fairview', 'Fairview')
  GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city) dn, customer,
  customer_address current_addr
WHERE ss_customer_sk = c_customer_sk
  AND customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
LIMIT 100)sql";

inline constexpr std::string_view kQ47Sql = R"sql(WITH v1 AS (
  SELECT
    i_category,
    i_brand,
    s_store_name,
    s_company_name,
    d_year,
    d_moy,
    sum(ss_sales_price) sum_sales,
    avg(sum(ss_sales_price))
    OVER
    (PARTITION BY i_category, i_brand,
      s_store_name, s_company_name, d_year)
    avg_monthly_sales,
    rank()
    OVER
    (PARTITION BY i_category, i_brand,
      s_store_name, s_company_name
      ORDER BY d_year, d_moy) rn
  FROM item, store_sales, date_dim, store
  WHERE ss_item_sk = i_item_sk AND
    ss_sold_date_sk = d_date_sk AND
    ss_store_sk = s_store_sk AND
    (
      d_year = 1999 OR
        (d_year = 1999 - 1 AND d_moy = 12) OR
        (d_year = 1999 + 1 AND d_moy = 1)
    )
  GROUP BY i_category, i_brand,
    s_store_name, s_company_name,
    d_year, d_moy),
    v2 AS (
    SELECT
      v1.i_category,
      v1.i_brand,
      v1.s_store_name,
      v1.s_company_name,
      v1.d_year,
      v1.d_moy,
      v1.avg_monthly_sales,
      v1.sum_sales,
      v1_lag.sum_sales psum,
      v1_lead.sum_sales nsum
    FROM v1, v1 v1_lag, v1 v1_lead
    WHERE v1.i_category = v1_lag.i_category AND
      v1.i_category = v1_lead.i_category AND
      v1.i_brand = v1_lag.i_brand AND
      v1.i_brand = v1_lead.i_brand AND
      v1.s_store_name = v1_lag.s_store_name AND
      v1.s_store_name = v1_lead.s_store_name AND
      v1.s_company_name = v1_lag.s_company_name AND
      v1.s_company_name = v1_lead.s_company_name AND
      v1.rn = v1_lag.rn + 1 AND
      v1.rn = v1_lead.rn - 1)
SELECT *
FROM v2
WHERE d_year = 1999 AND
  avg_monthly_sales > 0 AND
  CASE WHEN avg_monthly_sales > 0
    THEN abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
  ELSE NULL END > 0.1
ORDER BY sum_sales - avg_monthly_sales, s_store_name
LIMIT 100)sql";

inline constexpr std::array<TpcdsQuery, 27> kQueries = {{
    {"q1", kQ1Sql},   {"q2", kQ2Sql},   {"q3", kQ3Sql},   {"q6", kQ6Sql},
    {"q7", kQ7Sql},   {"q9", kQ9Sql},   {"q10", kQ10Sql}, {"q11", kQ11Sql},
    {"q12", kQ12Sql}, {"q13", kQ13Sql}, {"q15", kQ15Sql}, {"q16", kQ16Sql},
    {"q17", kQ17Sql}, {"q18", kQ18Sql}, {"q21", kQ21Sql}, {"q26", kQ26Sql},
    {"q42", kQ42Sql}, {"q52", kQ52Sql}, {"q55", kQ55Sql}, {"q61", kQ61Sql},
    {"q68", kQ68Sql}, {"q73", kQ73Sql}, {"q89", kQ89Sql}, {"q19", kQ19Sql},
    {"q40", kQ40Sql}, {"q46", kQ46Sql}, {"q47", kQ47Sql},
}};

}  // namespace tinylamb::tpcds

#endif  // TINYLAMB_BENCHMARK_TPCDS_QUERIES_HPP
