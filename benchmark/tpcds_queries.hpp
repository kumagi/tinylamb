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
//  - q22 needs item.i_product_name: added to the fixture as an index-derived
//    12-value pool so the ROLLUP group count stays bounded;
//  - q93 filters reason.r_reason_desc = 'reason 28': the fixture reason
//    table carries r_reason_text with the 12 canonical TPC-DS return
//    reasons, so the port filters r_reason_text = 'Did not like the color';
//  - q31 pins d_year to 2002: the fixture's random 2000 ratios leave no
//    county satisfying both quarter-over-quarter comparisons (empty result);
//  - q60 filters i_category = 'Sports' instead of 'Music': the fixture's
//    8-category pool has no Music items and extending it would shift the
//    existing category assignment pins;
//  - q70 / q86 keep their rank() OVER (PARTITION BY <grouping() folds>) over
//    ROLLUP groups: window calls see the combined result of every grouping
//    set (expanded into a UNION ALL with the windows evaluated in a wrapper);
//  - q92 keeps the correlated 1.3 * avg scalar subquery and the 90-day
//    date window; bare date-string literals are spelled as DATE casts to
//    match the other ports;
//  - each query keeps its template LIMIT 100 / ORDER BY as generated.
// Not an official TPC-DS result publication: this is an engine-workload
// harness, not a benchmark compliance submission.

#include <array>
#include <string_view>

namespace tinylamb::tpcds {

struct TpcdsQuery {
  std::string_view name;
  std::string_view sql;
  size_t order_key_index{0};
  bool order_descending{false};
  bool total_order{true};
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
ORDER BY sum_sales - avg_monthly_sales, 3
LIMIT 100)sql";

inline constexpr std::string_view kQ22Sql = R"sql(SELECT
  i_product_name,
  i_brand,
  i_class,
  i_category,
  avg(inv_quantity_on_hand) qoh
FROM inventory, date_dim, item, warehouse
WHERE inv_date_sk = d_date_sk
  AND inv_item_sk = i_item_sk
  AND inv_warehouse_sk = w_warehouse_sk
  AND d_month_seq BETWEEN 1200 AND 1200 + 11
GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
ORDER BY qoh, i_product_name, i_brand, i_class, i_category
LIMIT 100)sql";

inline constexpr std::string_view kQ92Sql = R"sql(SELECT
  sum(ws_ext_discount_amt) AS `Excess Discount Amount `
FROM web_sales, item, date_dim
WHERE i_manufact_id = 350
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN (cast('2000-01-27' AS DATE))
  AND (cast('2000-01-27' AS DATE) + INTERVAL 90 days)
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt >
  (
    SELECT 1.3 * avg(ws_ext_discount_amt)
    FROM web_sales, date_dim
    WHERE ws_item_sk = i_item_sk
      AND d_date BETWEEN (cast('2000-01-27' AS DATE))
      AND (cast('2000-01-27' AS DATE) + INTERVAL 90 days)
      AND d_date_sk = ws_sold_date_sk
  )
ORDER BY sum(ws_ext_discount_amt)
LIMIT 100)sql";

inline constexpr std::string_view kQ93Sql = R"sql(SELECT
  ss_customer_sk,
  sum(act_sales) sumsales
FROM (SELECT
  ss_item_sk,
  ss_ticket_number,
  ss_customer_sk,
  CASE WHEN sr_return_quantity IS NOT NULL
    THEN (ss_quantity - sr_return_quantity) * ss_sales_price
  ELSE (ss_quantity * ss_sales_price) END act_sales
FROM store_sales
  LEFT OUTER JOIN store_returns
    ON (sr_item_sk = ss_item_sk AND sr_ticket_number = ss_ticket_number)
  ,
  reason
WHERE sr_reason_sk = r_reason_sk AND r_reason_text = 'Did not like the color') t
GROUP BY ss_customer_sk
ORDER BY sumsales, ss_customer_sk
LIMIT 100)sql";

inline constexpr std::string_view kQ20Sql = R"sql(SELECT
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(cs_ext_sales_price) AS itemrevenue,
  sum(cs_ext_sales_price) * 100 / sum(sum(cs_ext_sales_price))
  OVER
  (PARTITION BY i_class) AS revenueratio
FROM catalog_sales, item, date_dim
WHERE cs_item_sk = i_item_sk
  AND i_category IN ('Sports', 'Books', 'Home')
  AND cs_sold_date_sk = d_date_sk
  AND d_date BETWEEN cast('1999-02-22' AS DATE)
AND (cast('1999-02-22' AS DATE) + INTERVAL 30 days)
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100)sql";

inline constexpr std::string_view kQ23aSql = R"sql(WITH frequent_ss_items AS
(SELECT
    substr(i_item_desc, 1, 30) itemdesc,
    i_item_sk item_sk,
    d_date solddate,
    count(*) cnt
  FROM store_sales, date_dim, item
  WHERE ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
  GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
  HAVING count(*) > 4),
    max_store_sales AS
  (SELECT max(csales) tpcds_cmax
  FROM (SELECT
    c_customer_sk,
    sum(ss_quantity * ss_sales_price) csales
  FROM store_sales, customer, date_dim
  WHERE ss_customer_sk = c_customer_sk
    AND ss_sold_date_sk = d_date_sk
    AND d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
  GROUP BY c_customer_sk) x),
    best_ss_customer AS
  (SELECT
    c_customer_sk,
    sum(ss_quantity * ss_sales_price) ssales
  FROM store_sales, customer
  WHERE ss_customer_sk = c_customer_sk
  GROUP BY c_customer_sk
  HAVING sum(ss_quantity * ss_sales_price) > (50 / 100.0) *
    (SELECT *
    FROM max_store_sales))
SELECT sum(sales)
FROM ((SELECT cs_quantity * cs_list_price sales
FROM catalog_sales, date_dim
WHERE d_year = 2000
  AND d_moy = 2
  AND cs_sold_date_sk = d_date_sk
  AND cs_item_sk IN (SELECT item_sk
FROM frequent_ss_items)
  AND cs_bill_customer_sk IN (SELECT c_customer_sk
FROM best_ss_customer))
      UNION ALL
      (SELECT ws_quantity * ws_list_price sales
      FROM web_sales, date_dim
      WHERE d_year = 2000
        AND d_moy = 2
        AND ws_sold_date_sk = d_date_sk
        AND ws_item_sk IN (SELECT item_sk
      FROM frequent_ss_items)
        AND ws_bill_customer_sk IN (SELECT c_customer_sk
      FROM best_ss_customer))) y
LIMIT 100)sql";

inline constexpr std::string_view kQ27Sql = R"sql(SELECT
  i_item_id,
  s_state,
  grouping(s_state) g_state,
  avg(ss_quantity) agg1,
  avg(ss_list_price) agg2,
  avg(ss_coupon_amt) agg3,
  avg(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, store, item
WHERE ss_sold_date_sk = d_date_sk AND
  ss_item_sk = i_item_sk AND
  ss_store_sk = s_store_sk AND
  ss_cdemo_sk = cd_demo_sk AND
  cd_gender = 'M' AND
  cd_marital_status = 'S' AND
  cd_education_status = 'College' AND
  d_year = 2002 AND
  s_state IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN')
GROUP BY ROLLUP (i_item_id, s_state)
ORDER BY i_item_id, s_state
LIMIT 100)sql";

inline constexpr std::string_view kQ31Sql = R"sql(WITH ss AS
(SELECT
    ca_county,
    d_qoy,
    d_year,
    sum(ss_ext_sales_price) AS store_sales
  FROM store_sales, date_dim, customer_address
  WHERE ss_sold_date_sk = d_date_sk
    AND ss_addr_sk = ca_address_sk
  GROUP BY ca_county, d_qoy, d_year),
    ws AS
  (SELECT
    ca_county,
    d_qoy,
    d_year,
    sum(ws_ext_sales_price) AS web_sales
  FROM web_sales, date_dim, customer_address
  WHERE ws_sold_date_sk = d_date_sk
    AND ws_bill_addr_sk = ca_address_sk
  GROUP BY ca_county, d_qoy, d_year)
SELECT
  ss1.ca_county,
  ss1.d_year,
  ws2.web_sales / ws1.web_sales web_q1_q2_increase,
  ss2.store_sales / ss1.store_sales store_q1_q2_increase,
  ws3.web_sales / ws2.web_sales web_q2_q3_increase,
  ss3.store_sales / ss2.store_sales store_q2_q3_increase
FROM
  ss ss1, ss ss2, ss ss3, ws ws1, ws ws2, ws ws3
WHERE
  ss1.d_qoy = 1
    AND ss1.d_year = 2002
    AND ss1.ca_county = ss2.ca_county
    AND ss2.d_qoy = 2
    AND ss2.d_year = 2002
    AND ss2.ca_county = ss3.ca_county
    AND ss3.d_qoy = 3
    AND ss3.d_year = 2002
    AND ss1.ca_county = ws1.ca_county
    AND ws1.d_qoy = 1
    AND ws1.d_year = 2002
    AND ws1.ca_county = ws2.ca_county
    AND ws2.d_qoy = 2
    AND ws2.d_year = 2002
    AND ws1.ca_county = ws3.ca_county
    AND ws3.d_qoy = 3
    AND ws3.d_year = 2002
    AND CASE WHEN ws1.web_sales > 0
    THEN ws2.web_sales / ws1.web_sales
        ELSE NULL END
    > CASE WHEN ss1.store_sales > 0
    THEN ss2.store_sales / ss1.store_sales
      ELSE NULL END
    AND CASE WHEN ws2.web_sales > 0
    THEN ws3.web_sales / ws2.web_sales
        ELSE NULL END
    > CASE WHEN ss2.store_sales > 0
    THEN ss3.store_sales / ss2.store_sales
      ELSE NULL END
ORDER BY ss1.ca_county)sql";

inline constexpr std::string_view kQ60Sql = R"sql(WITH ss AS (
  SELECT
    i_item_id,
    sum(ss_ext_sales_price) total_sales
  FROM store_sales, date_dim, customer_address, item
  WHERE
    i_item_id IN (SELECT i_item_id
    FROM item
    WHERE i_category IN ('Sports'))
      AND ss_item_sk = i_item_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_year = 1998
      AND d_moy = 9
      AND ss_addr_sk = ca_address_sk
      AND ca_gmt_offset = -5
  GROUP BY i_item_id),
    cs AS (
    SELECT
      i_item_id,
      sum(cs_ext_sales_price) total_sales
    FROM catalog_sales, date_dim, customer_address, item
    WHERE
      i_item_id IN (SELECT i_item_id
      FROM item
      WHERE i_category IN ('Sports'))
        AND cs_item_sk = i_item_sk
        AND cs_sold_date_sk = d_date_sk
        AND d_year = 1998
        AND d_moy = 9
        AND cs_bill_addr_sk = ca_address_sk
        AND ca_gmt_offset = -5
    GROUP BY i_item_id),
    ws AS (
    SELECT
      i_item_id,
      sum(ws_ext_sales_price) total_sales
    FROM web_sales, date_dim, customer_address, item
    WHERE
      i_item_id IN (SELECT i_item_id
      FROM item
      WHERE i_category IN ('Sports'))
        AND ws_item_sk = i_item_sk
        AND ws_sold_date_sk = d_date_sk
        AND d_year = 1998
        AND d_moy = 9
        AND ws_bill_addr_sk = ca_address_sk
        AND ca_gmt_offset = -5
    GROUP BY i_item_id)
SELECT
  i_item_id,
  sum(total_sales) total_sales
FROM (SELECT *
      FROM ss
      UNION ALL
      SELECT *
      FROM cs
      UNION ALL
      SELECT *
      FROM ws) tmp1
GROUP BY i_item_id
ORDER BY i_item_id, total_sales
LIMIT 100)sql";

inline constexpr std::string_view kQ65Sql = R"sql(SELECT
  s_store_name,
  i_item_desc,
  sc.revenue,
  i_current_price,
  i_wholesale_cost,
  i_brand
FROM store, item,
  (SELECT
    ss_store_sk,
    avg(revenue) AS ave
  FROM
    (SELECT
      ss_store_sk,
      ss_item_sk,
      sum(ss_sales_price) AS revenue
    FROM store_sales, date_dim
    WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1176 AND 1176 + 11
    GROUP BY ss_store_sk, ss_item_sk) sa
  GROUP BY ss_store_sk) sb,
  (SELECT
    ss_store_sk,
    ss_item_sk,
    sum(ss_sales_price) AS revenue
  FROM store_sales, date_dim
  WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1176 AND 1176 + 11
  GROUP BY ss_store_sk, ss_item_sk) sc
WHERE sb.ss_store_sk = sc.ss_store_sk AND
  sc.revenue <= 0.1 * sb.ave AND
  s_store_sk = sc.ss_store_sk AND
  i_item_sk = sc.ss_item_sk
ORDER BY s_store_name, i_item_desc
LIMIT 100)sql";

// q4: cross-channel year-over-year customer growth.  The year_total CTE
// unions the s/c/w channel branches; the d_year window is pinned to
// 1999/2000.  The template's fourth ORDER BY term ([SELECTONE]) is dropped.
inline constexpr std::string_view kQ4Sql = R"sql(WITH year_total as (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
       ,'s' sale_type
 from customer, store_sales, date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
          c_birth_country, c_login, c_email_address, d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) year_total
       ,'c' sale_type
 from customer, catalog_sales, date_dim
 where c_customer_sk = cs_bill_customer_sk
   and cs_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
          c_birth_country, c_login, c_email_address, d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) year_total
       ,'w' sale_type
 from customer, web_sales, date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
 group by c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag,
          c_birth_country, c_login, c_email_address, d_year
)
SELECT t_s_secyear.customer_id
      ,t_s_secyear.customer_first_name
      ,t_s_secyear.customer_last_name
from year_total t_s_firstyear
    ,year_total t_s_secyear
    ,year_total t_c_firstyear
    ,year_total t_c_secyear
    ,year_total t_w_firstyear
    ,year_total t_w_secyear
where t_s_secyear.customer_id = t_c_secyear.customer_id
   and t_s_secyear.customer_id = t_w_secyear.customer_id
   and t_s_firstyear.customer_id = t_s_secyear.customer_id
   and t_s_firstyear.customer_id = t_c_firstyear.customer_id
   and t_s_firstyear.customer_id = t_w_firstyear.customer_id
   and t_s_firstyear.sale_type = 's'
   and t_c_firstyear.sale_type = 'c'
   and t_w_firstyear.sale_type = 'w'
   and t_s_secyear.sale_type = 's'
   and t_c_secyear.sale_type = 'c'
   and t_w_secyear.sale_type = 'w'
   and t_s_firstyear.dyear = 1999
   and t_s_secyear.dyear = 1999 + 1
   and t_c_firstyear.dyear = 1999
   and t_c_secyear.dyear = 1999 + 1
   and t_w_firstyear.dyear = 1999
   and t_w_secyear.dyear = 1999 + 1
   and t_s_firstyear.year_total > 0
   and t_c_firstyear.year_total > 0
   and t_w_firstyear.year_total > 0
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
order by t_s_secyear.customer_id
        ,t_s_secyear.customer_first_name
        ,t_s_secyear.customer_last_name
LIMIT 100)sql";

// q28: six single-row aggregate buckets (quantity bands with OR-of-between
// filters) joined as a cross product.  The template's price/coupon/cost
// defines are pinned to ranges that hit the fixture data.
inline constexpr std::string_view kQ28Sql = R"sql(SELECT * FROM
    (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 50 and 60
             or ss_coupon_amt between 0 and 1000
             or ss_wholesale_cost between 50 and 70)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 100 and 110
          or ss_coupon_amt between 0 and 1000
          or ss_wholesale_cost between 60 and 80)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 150 and 160
          or ss_coupon_amt between 0 and 1000
          or ss_wholesale_cost between 70 and 90)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 200 and 210
          or ss_coupon_amt between 0 and 1000
          or ss_wholesale_cost between 80 and 100)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 250 and 260
          or ss_coupon_amt between 0 and 1000
          or ss_wholesale_cost between 90 and 110)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 300 and 310
          or ss_coupon_amt between 0 and 1000
          or ss_wholesale_cost between 100 and 120)) B6
LIMIT 100)sql";

// q33: three channel CTEs (each with an i_category IN-subquery and a
// ca_gmt_offset filter) unioned and re-aggregated.  GMT is pinned to -5,
// the category to 'Sports', the window to 1998-06.
inline constexpr std::string_view kQ33Sql = R"sql(WITH ss as (
 select i_manufact_id, sum(ss_ext_sales_price) total_sales
 from store_sales, date_dim, customer_address, item
 where i_manufact_id in (select i_manufact_id
                         from item
                         where i_category in ('Sports'))
 and ss_item_sk      = i_item_sk
 and ss_sold_date_sk = d_date_sk
 and d_year          = 1998
 and d_moy           = 6
 and ss_addr_sk      = ca_address_sk
 and ca_gmt_offset   = -5
 group by i_manufact_id),
 cs as (
 select i_manufact_id, sum(cs_ext_sales_price) total_sales
 from catalog_sales, date_dim, customer_address, item
 where i_manufact_id in (select i_manufact_id
                         from item
                         where i_category in ('Sports'))
 and cs_item_sk      = i_item_sk
 and cs_sold_date_sk   = d_date_sk
 and d_year            = 1998
 and d_moy             = 6
 and cs_bill_addr_sk   = ca_address_sk
 and ca_gmt_offset     = -5
 group by i_manufact_id),
 ws as (
 select i_manufact_id, sum(ws_ext_sales_price) total_sales
 from web_sales, date_dim, customer_address, item
 where i_manufact_id in (select i_manufact_id
                         from item
                         where i_category in ('Sports'))
 and ws_item_sk      = i_item_sk
 and ws_sold_date_sk = d_date_sk
 and d_year          = 1998
 and d_moy           = 6
 and ws_bill_addr_sk = ca_address_sk
 and ca_gmt_offset   = -5
 group by i_manufact_id)
SELECT i_manufact_id, sum(total_sales) total_sales
from  (select * from ss
       union all
       select * from cs
       union all
       select * from ws) tmp1
group by i_manufact_id
order by total_sales
LIMIT 100)sql";

// q50: store-return lag buckets.  Column aliases with spaces ("30 days")
// are renamed to identifiers, the street/suite columns are dropped (the
// fixture store table does not carry them) and the window is pinned to
// 2000 Q2 so the fixture's return lag (sale + 5..60 days) overlaps.
inline constexpr std::string_view kQ50Sql = R"sql(SELECT
   s_store_name
  ,s_company_id
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk <= 30 ) then 1 else 0 end)  as days_0_30
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 30) and
                 (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1 else 0 end )  as days_31_60
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 60) and
                 (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1 else 0 end)  as days_61_90
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 90) and
                 (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1 else 0 end)  as days_91_120
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk  > 120) then 1 else 0 end)  as days_over_120
from
   store_sales
  ,store_returns
  ,date_dim d1
  ,date_dim d2
  ,store
where
    s_store_sk = ss_store_sk
and ss_customer_sk = sr_customer_sk
and ss_item_sk = sr_item_sk
and d1.d_moy  between 4 and 6
and d1.d_year = 2000
and d2.d_moy  between 4 and 6
and d2.d_year = 2000
and ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_sold_date_sk   = d1.d_date_sk
and sr_returned_date_sk   = d2.d_date_sk
and ss_customer_sk = sr_customer_sk
and ss_store_sk = s_store_sk
group by
   s_store_name
  ,s_company_id
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
order by s_store_name
        ,s_company_id
        ,s_city
        ,s_county
        ,s_state
        ,s_zip
LIMIT 100)sql";

// q59: same-store week-over-week sales ratios (the wss CTE pivots one
// column per weekday).  The d_month_seq windows are pinned to 1176 and
// 1188 (consecutive 1998/1999 years).
inline constexpr std::string_view kQ59Sql = R"sql(WITH wss as
 (select d_week_seq,
        ss_store_sk,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from store_sales,date_dim
 where d_date_sk = ss_sold_date_sk
 group by d_week_seq,ss_store_sk
)
select s_store_name1,s_store_id1,d_week_seq1
      ,sun_sales1/sun_sales2,mon_sales1/mon_sales2
      ,tue_sales1/tue_sales2,wed_sales1/wed_sales2,thu_sales1/thu_sales2
      ,fri_sales1/fri_sales2,sat_sales1/sat_sales2
from
 (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1
        ,s_store_id s_store_id1,sun_sales sun_sales1
        ,mon_sales mon_sales1,tue_sales tue_sales1
        ,wed_sales wed_sales1,thu_sales thu_sales1
        ,fri_sales fri_sales1,sat_sales sat_sales1
  from wss,store,date_dim d
  where d.d_week_seq = wss.d_week_seq and
        ss_store_sk = s_store_sk and
        d_month_seq between 1176 and 1176 + 11) y,
 (select s_store_name s_store_name2,wss.d_week_seq d_week_seq2
        ,s_store_id s_store_id2,sun_sales sun_sales2
        ,mon_sales mon_sales2,tue_sales tue_sales2
        ,wed_sales wed_sales2,thu_sales thu_sales2
        ,fri_sales fri_sales2,sat_sales sat_sales2
  from wss,store,date_dim d
  where d.d_week_seq = wss.d_week_seq and
        ss_store_sk = s_store_sk and
        d_month_seq between 1176 + 12 and 1176 + 23) x
where s_store_id1=s_store_id2
  and d_week_seq1=d_week_seq2-52
order by s_store_name1,s_store_id1,d_week_seq1
LIMIT 100)sql";

// q67: rank() over a derived 8-column ROLLUP with an outer rk <= 100
// filter (the window spans every grouping set).  DMS is pinned to 1176.
inline constexpr std::string_view kQ67Sql = R"sql(SELECT * FROM
 (select i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rank() over (partition by i_category order by sumsales desc) rk
  from (select i_category
              ,i_class
              ,i_brand
              ,i_product_name
              ,d_year
              ,d_qoy
              ,d_moy
              ,s_store_id
              ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
        from store_sales
            ,date_dim
            ,store
            ,item
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_month_seq between 1176 and 1176+11
       group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
LIMIT 100)sql";

// q69: store-channel-only buyers.  The correlated EXISTS plus two NOT
// EXISTS branches stay; STATE picks are from the fixture's 8-state pool
// and the window is pinned to 1999-01..03.
inline constexpr std::string_view kQ69Sql = R"sql(SELECT
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  customer c, customer_address ca, customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('TN','CA','GA') and
  cd_demo_sk = c.c_current_cdemo_sk and
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 1999 and
                d_moy between 1 and 2) and
   (not exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_moy between 1 and 2) and
    not exists (select *
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_moy between 1 and 2))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
LIMIT 100)sql";

// q75: year-over-year net sales per brand across catalog and web branches
// (LEFT JOIN returns inside a UNION CTE).  DECIMAL casts are dropped (the
// fixture is DOUBLE) and the years are pinned to 2000 vs 1999.
inline constexpr std::string_view kQ75Sql = R"sql(WITH all_sales AS (
 SELECT d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,SUM(sales_cnt) AS sales_cnt
       ,SUM(sales_amt) AS sales_amt
 FROM (SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt
             ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
       FROM catalog_sales JOIN item ON i_item_sk=cs_item_sk
                          JOIN date_dim ON d_date_sk=cs_sold_date_sk
                          LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number
                                                    AND cs_item_sk=cr_item_sk)
       WHERE i_category='Sports'
       UNION DISTINCT
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt
             ,ws_ext_sales_price - COALESCE(wr_return_amount,0.0) AS sales_amt
       FROM web_sales JOIN item ON i_item_sk=ws_item_sk
                      JOIN date_dim ON d_date_sk=ws_sold_date_sk
                      LEFT JOIN web_returns ON (ws_order_number=wr_order_number
                                            AND ws_item_sk=wr_item_sk)
       WHERE i_category='Sports') sales_detail
 GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id)
SELECT prev_yr.d_year AS prev_year
                  ,curr_yr.d_year AS year
                  ,curr_yr.i_brand_id
                  ,curr_yr.i_class_id
                  ,curr_yr.i_category_id
                  ,curr_yr.i_manufact_id
                  ,prev_yr.sales_cnt AS prev_yr_cnt
                  ,curr_yr.sales_cnt AS curr_yr_cnt
                  ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff
                  ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
FROM all_sales curr_yr, all_sales prev_yr
WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
  AND curr_yr.i_class_id=prev_yr.i_class_id
  AND curr_yr.i_category_id=prev_yr.i_category_id
  AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
  AND curr_yr.d_year=2000
  AND prev_yr.d_year=2000-1
  AND curr_yr.sales_cnt/prev_yr.sales_cnt<0.9
ORDER BY sales_cnt_diff,sales_amt_diff
LIMIT 100)sql";

// q87: store customers absent from catalog and web channels, as an
// EXCEPT chain under count(*).  DMS is pinned to 1176.
inline constexpr std::string_view kQ87Sql = R"sql(select count(*)
from
      (select distinct c_last_name, c_first_name, d_date
       from store_sales, date_dim, customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1176 and 1176+11
       except distinct
      select distinct c_last_name, c_first_name, d_date
       from catalog_sales, date_dim, customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1176 and 1176+11
       except distinct
      select distinct c_last_name, c_first_name, d_date
       from web_sales, date_dim, customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1176 and 1176+11) cdn
LIMIT 100)sql";

// q53: avg-of-sum window over grouped quarterly sales, filtered on the
// window result in the outer WHERE.  The category/class/brand pools are
// adapted to the fixture's 8-category schema (the official brand names do
// not exist there); DMS is pinned to 1176.
inline constexpr std::string_view kQ53Sql = R"sql(SELECT * FROM
(select i_manufact_id,
        sum(ss_sales_price) sum_sales,
        avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
 from item, store_sales, date_dim, store
 where ss_item_sk = i_item_sk and
       ss_sold_date_sk = d_date_sk and
       ss_store_sk = s_store_sk and
       d_month_seq in (1176,1176+1,1176+2,1176+3,1176+4,1176+5,1176+6,1176+7,1176+8,1176+9,1176+10,1176+11) and
       ((i_category in ('Books','Home','Electronics') and
         i_class in ('travel','fiction','children','furniture','kitchen','decor','computers','stereo','tv'))
        or
        (i_category in ('Women','Men') and
         i_class in ('dresses','shoes','blouses','shirts','pants','suits')))
 group by i_manufact_id, d_qoy) tmp1
where case when avg_quarterly_sales > 0
    then abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
    else null end > 0.1
order by avg_quarterly_sales,
         sum_sales,
         i_manufact_id
LIMIT 100)sql";

// q70: ROLLUP subtotal levels ranked together by rank() whose PARTITION BY
// folds GROUPING() (and a CASE over it) per branch.  The d_month_seq window
// is pinned to 1176 (1998-01), matching the q52 / q22 adaptations.
inline constexpr std::string_view kQ70Sql = R"sql(SELECT
   sum(ss_net_profit) AS total_sum,
   s_state,
   s_county,
   grouping(s_state) + grouping(s_county) AS lochierarchy,
   rank() OVER (
 PARTITION BY grouping(s_state) + grouping(s_county),
 CASE WHEN grouping(s_county) = 0 THEN s_state END
 ORDER BY sum(ss_net_profit) DESC) AS rank_within_parent
FROM
   store_sales
  ,date_dim       d1
  ,store
WHERE
   d1.d_month_seq BETWEEN 1176 AND 1176 + 11
AND d1.d_date_sk = ss_sold_date_sk
AND s_store_sk  = ss_store_sk
AND s_state IN
            ( SELECT s_state
              FROM  (SELECT s_state AS s_state,
                     rank() OVER ( PARTITION BY s_state ORDER BY sum(ss_net_profit) DESC) AS ranking
                     FROM   store_sales, store, date_dim
                     WHERE  d_month_seq BETWEEN 1176 AND 1176 + 11
                     AND d_date_sk = ss_sold_date_sk
                     AND s_store_sk  = ss_store_sk
                     GROUP BY s_state
                     ) tmp1
              WHERE ranking <= 5
            )
GROUP BY ROLLUP(s_state, s_county)
ORDER BY
  lochierarchy DESC
 ,CASE WHEN lochierarchy = 0 THEN s_state END
 ,rank_within_parent
LIMIT 100)sql";

// q86: the canonical rank()-over-ROLLUP shape.  The d_month_seq window is
// pinned to 1176 (1998-01).
inline constexpr std::string_view kQ86Sql = R"sql(SELECT
   sum(ws_net_paid) AS total_sum,
   i_category,
   i_class,
   grouping(i_category) + grouping(i_class) AS lochierarchy,
   rank() OVER (
 PARTITION BY grouping(i_category) + grouping(i_class),
 CASE WHEN grouping(i_class) = 0 THEN i_category END
 ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
FROM
   web_sales
  ,date_dim       d1
  ,item
WHERE
   d1.d_month_seq BETWEEN 1176 AND 1176 + 11
AND d1.d_date_sk = ws_sold_date_sk
AND i_item_sk  = ws_item_sk
GROUP BY ROLLUP(i_category, i_class)
ORDER BY
  lochierarchy DESC
 ,CASE WHEN lochierarchy = 0 THEN i_category END
 ,rank_within_parent
LIMIT 100)sql";

// ---------------------------------------------------------------------------
// 2026-09-29 batch: q63 / q98 / q84 / q35 / q37 / q48 / q79 / q63 / q34 /
// q29 / q76 / q77.  Adaptations:
//  - q84 pins ca_city to 'Midway' (the fixture city pool) and narrows the
//    income band to 40000..90000 (the fixture's 20 x 5000 bands);
//  - q37 adapts the price / manufacturer pools to the fixture's item
//    generation (list price 1..400, manufacturer 1..1000 with a quarter at
//    128);
//  - q48 maps the three state triads onto the fixture's 8-state pool;
//  - q79 uses hd_dep_count = 5 (the fixture draws dep counts 0..5) and a
//    s_number_employees window inside 50..400;
//  - q63 replaces the official brand/class pools with fixture pools
//    (brand#1..30, the 8 category / 3 class pools);
//  - q34 keeps the template shape with a cnt window (1..3) that the
//    fixture's ticket density actually produces, and names 'Franklin
//    Parish' (the repeated-list template form): this fixture seed gives no
//    store a Williamson County address;
//  - q29 pins d1 to 2000-09 (the fixture's sale years are 1998..2002);
//  - q76 keeps the template's channel-column shape but flips the NULL
//    probes to IS NOT NULL: the fixture never writes NULL keys, so the
//    official spelling returns an empty (unmeasurable) result;
//  - q77 replaces the web_page join (table not in the fixture) with the
//    ship-customer key: the web channel groups by ws_ship_customer_sk and
//    the returns CTE by wr_refunded_customer_sk.
inline constexpr std::string_view kQ98Sql = R"sql(SELECT
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(ss_ext_sales_price) AS itemrevenue,
  sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price))
  OVER
  (PARTITION BY i_class) AS revenueratio
FROM
  store_sales, item, date_dim
WHERE
  ss_item_sk = i_item_sk
    AND i_category IN ('Sports', 'Books', 'Home')
    AND ss_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('1998-11-22' AS DATE)
  AND (cast('1998-11-22' AS DATE) + INTERVAL 30 days)
GROUP BY
  i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY
  i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100)sql";

inline constexpr std::string_view kQ43Sql = R"sql(SELECT
  s_store_name,
  s_store_id,
  sum(CASE WHEN (d_day_name = 'Sunday')
    THEN ss_sales_price
      ELSE NULL END) sun_sales,
  sum(CASE WHEN (d_day_name = 'Monday')
    THEN ss_sales_price
      ELSE NULL END) mon_sales,
  sum(CASE WHEN (d_day_name = 'Tuesday')
    THEN ss_sales_price
      ELSE NULL END) tue_sales,
  sum(CASE WHEN (d_day_name = 'Wednesday')
    THEN ss_sales_price
      ELSE NULL END) wed_sales,
  sum(CASE WHEN (d_day_name = 'Thursday')
    THEN ss_sales_price
      ELSE NULL END) thu_sales,
  sum(CASE WHEN (d_day_name = 'Friday')
    THEN ss_sales_price
      ELSE NULL END) fri_sales,
  sum(CASE WHEN (d_day_name = 'Saturday')
    THEN ss_sales_price
      ELSE NULL END) sat_sales
FROM date_dim, store_sales, store
WHERE d_date_sk = ss_sold_date_sk AND
  s_store_sk = ss_store_sk AND
  s_gmt_offset = -5 AND
  d_year = 2000
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name, s_store_id, sun_sales, mon_sales, tue_sales, wed_sales,
  thu_sales, fri_sales, sat_sales
LIMIT 100)sql";

inline constexpr std::string_view kQ84Sql = R"sql(SELECT
  c_customer_id AS customer_id,
  concat(c_last_name, ', ', c_first_name) AS customername
FROM customer
  , customer_address
  , customer_demographics
  , household_demographics
  , income_band
  , store_returns
WHERE ca_city = 'Midway'
  AND c_current_addr_sk = ca_address_sk
  AND ib_lower_bound >= 40000
  AND ib_upper_bound <= 40000 + 50000
  AND ib_income_band_sk = hd_income_band_sk
  AND cd_demo_sk = c_current_cdemo_sk
  AND hd_demo_sk = c_current_hdemo_sk
  AND sr_cdemo_sk = cd_demo_sk
ORDER BY c_customer_id
LIMIT 100)sql";

inline constexpr std::string_view kQ35Sql = R"sql(SELECT
  ca_state,
  cd_gender,
  cd_marital_status,
  count(*) cnt1,
  min(cd_dep_count),
  max(cd_dep_count),
  avg(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  min(cd_dep_employed_count),
  max(cd_dep_employed_count),
  avg(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  min(cd_dep_college_count),
  max(cd_dep_college_count),
  avg(cd_dep_college_count)
FROM
  customer c, customer_address ca, customer_demographics
WHERE
  c.c_current_addr_sk = ca.ca_address_sk AND
    cd_demo_sk = c.c_current_cdemo_sk AND
    exists(SELECT *
           FROM store_sales, date_dim
           WHERE c.c_customer_sk = ss_customer_sk AND
             ss_sold_date_sk = d_date_sk AND
             d_year = 2002 AND
             d_qoy < 4) AND
    (exists(SELECT *
            FROM web_sales, date_dim
            WHERE c.c_customer_sk = ws_bill_customer_sk AND
              ws_sold_date_sk = d_date_sk AND
              d_year = 2002 AND
              d_qoy < 4) OR
      exists(SELECT *
             FROM catalog_sales, date_dim
             WHERE c.c_customer_sk = cs_ship_customer_sk AND
               cs_sold_date_sk = d_date_sk AND
               d_year = 2002 AND
               d_qoy < 4))
GROUP BY ca_state, cd_gender, cd_marital_status, cd_dep_count,
  cd_dep_employed_count, cd_dep_college_count
ORDER BY ca_state, cd_gender, cd_marital_status, cd_dep_count,
  cd_dep_employed_count, cd_dep_college_count
LIMIT 100)sql";

inline constexpr std::string_view kQ37Sql = R"sql(SELECT
  i_item_id,
  i_item_desc,
  i_current_price
FROM item, inventory, date_dim, catalog_sales
WHERE i_current_price BETWEEN 68 AND 68 + 30
  AND inv_item_sk = i_item_sk
  AND d_date_sk = inv_date_sk
  AND d_date BETWEEN cast('2000-02-01' AS DATE) AND (cast('2000-02-01' AS DATE) + INTERVAL 60 days)
  AND i_manufact_id IN (128, 12, 340, 777)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
  AND cs_item_sk = i_item_sk
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id
LIMIT 100)sql";

inline constexpr std::string_view kQ48Sql = R"sql(SELECT sum(ss_quantity)
FROM store_sales, store, customer_demographics, customer_address, date_dim
WHERE s_store_sk = ss_store_sk
  AND ss_sold_date_sk = d_date_sk AND d_year = 2001
  AND
  (
    (
      cd_demo_sk = ss_cdemo_sk
        AND
        cd_marital_status = 'M'
        AND
        cd_education_status = '4 yr Degree'
        AND
        ss_sales_price BETWEEN 100.00 AND 150.00
    )
      OR
      (
        cd_demo_sk = ss_cdemo_sk
          AND
          cd_marital_status = 'D'
          AND
          cd_education_status = '2 yr Degree'
          AND
          ss_sales_price BETWEEN 50.00 AND 100.00
      )
      OR
      (
        cd_demo_sk = ss_cdemo_sk
          AND
          cd_marital_status = 'S'
          AND
          cd_education_status = 'College'
          AND
          ss_sales_price BETWEEN 150.00 AND 200.00
      )
  )
  AND
  (
    (
      ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('TN', 'TX', 'CA')
        AND ss_net_profit BETWEEN 0 AND 2000
    )
      OR
      (ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('OH', 'MI', 'GA')
        AND ss_net_profit BETWEEN 150 AND 3000
      )
      OR
      (ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('NY', 'IN', 'TN')
        AND ss_net_profit BETWEEN 50 AND 25000
      )
  ))sql";

inline constexpr std::string_view kQ79Sql = R"sql(SELECT
  c_last_name,
  c_first_name,
  substr(s_city, 1, 30),
  ss_ticket_number,
  amt,
  profit
FROM
  (SELECT
    ss_ticket_number,
    ss_customer_sk,
    store.s_city,
    sum(ss_coupon_amt) amt,
    sum(ss_net_profit) profit
  FROM store_sales, date_dim, store, household_demographics
  WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND store_sales.ss_store_sk = store.s_store_sk
    AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND (household_demographics.hd_dep_count = 5 OR
    household_demographics.hd_vehicle_count > 2)
    AND date_dim.d_dow = 1
    AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
    AND store.s_number_employees BETWEEN 200 AND 295
  GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city) ms, customer
WHERE ss_customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, substr(s_city, 1, 30), profit
LIMIT 100)sql";

inline constexpr std::string_view kQ63Sql = R"sql(SELECT *
FROM (SELECT
  i_manager_id,
  sum(ss_sales_price) sum_sales,
  avg(sum(ss_sales_price))
  OVER (PARTITION BY i_manager_id) avg_monthly_sales
FROM item
  , store_sales
  , date_dim
  , store
WHERE ss_item_sk = i_item_sk
  AND ss_sold_date_sk = d_date_sk
  AND ss_store_sk = s_store_sk
  AND d_month_seq IN (1200, 1200 + 1, 1200 + 2, 1200 + 3, 1200 + 4, 1200 + 5, 1200 + 6, 1200 + 7,
                            1200 + 8, 1200 + 9, 1200 + 10, 1200 + 11)
  AND ((i_category IN ('Books', 'Children', 'Electronics')
  AND i_class IN ('travel', 'fiction', 'children', 'toys', 'games', 'school',
                  'computers', 'stereo', 'tv')
  AND i_brand IN ('brand#1', 'brand#7', 'brand#9', 'brand#14'))
  OR (i_category IN ('Women', 'Men')
  AND i_class IN ('dresses', 'shoes', 'blouses', 'shirts', 'pants', 'suits')
  AND i_brand IN ('brand#21', 'brand#22', 'brand#23', 'brand#24')))
GROUP BY i_manager_id, d_moy) tmp1
WHERE CASE WHEN avg_monthly_sales > 0
  THEN abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
      ELSE NULL END > 0.1
ORDER BY i_manager_id
  , avg_monthly_sales
  , sum_sales
LIMIT 100)sql";

inline constexpr std::string_view kQ34Sql = R"sql(SELECT
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
    AND (date_dim.d_dom BETWEEN 1 AND 3 OR date_dim.d_dom BETWEEN 25 AND 28)
    AND (household_demographics.hd_buy_potential = '>10000' OR
    household_demographics.hd_buy_potential = 'unknown')
    AND household_demographics.hd_vehicle_count > 0
    AND (CASE WHEN household_demographics.hd_vehicle_count > 0
    THEN household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
         ELSE NULL
         END) > 1.2
    AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
    AND store.s_county IN
    ('Franklin Parish', 'Franklin Parish', 'Franklin Parish', 'Franklin Parish',
     'Franklin Parish', 'Franklin Parish', 'Franklin Parish', 'Franklin Parish')
  GROUP BY ss_ticket_number, ss_customer_sk) dn, customer
WHERE ss_customer_sk = c_customer_sk
  AND cnt BETWEEN 1 AND 3
ORDER BY c_last_name, c_first_name, c_salutation, c_preferred_cust_flag DESC
LIMIT 100)sql";

inline constexpr std::string_view kQ29Sql = R"sql(SELECT
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name,
  sum(ss_quantity) AS store_sales_quantity,
  sum(sr_return_quantity) AS store_returns_quantity,
  sum(cs_quantity) AS catalog_sales_quantity
FROM
  store_sales, store_returns, catalog_sales, date_dim d1, date_dim d2,
  date_dim d3, store, item
WHERE
  d1.d_moy = 9
    AND d1.d_year = 2000
    AND d1.d_date_sk = ss_sold_date_sk
    AND i_item_sk = ss_item_sk
    AND s_store_sk = ss_store_sk
    AND ss_customer_sk = sr_customer_sk
    AND ss_item_sk = sr_item_sk
    AND ss_ticket_number = sr_ticket_number
    AND sr_returned_date_sk = d2.d_date_sk
    AND d2.d_moy BETWEEN 9 AND 9 + 3
    AND d2.d_year = 2000
    AND sr_customer_sk = cs_bill_customer_sk
    AND sr_item_sk = cs_item_sk
    AND cs_sold_date_sk = d3.d_date_sk
    AND d3.d_year IN (2000, 2000 + 1, 2000 + 2)
GROUP BY
  i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY
  i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100)sql";

inline constexpr std::string_view kQ76Sql = R"sql(SELECT
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  COUNT(*) sales_cnt,
  SUM(ext_sales_price) sales_amt
FROM (
       SELECT
         'store' AS channel,
         ss_store_sk col_name,
         d_year,
         d_qoy,
         i_category,
         ss_ext_sales_price ext_sales_price
       FROM store_sales, item, date_dim
       WHERE ss_store_sk IS NOT NULL
         AND ss_sold_date_sk = d_date_sk
         AND ss_item_sk = i_item_sk
       UNION ALL
       SELECT
         'web' AS channel,
         ws_ship_customer_sk col_name,
         d_year,
         d_qoy,
         i_category,
         ws_ext_sales_price ext_sales_price
       FROM web_sales, item, date_dim
       WHERE ws_ship_customer_sk IS NOT NULL
         AND ws_sold_date_sk = d_date_sk
         AND ws_item_sk = i_item_sk
       UNION ALL
       SELECT
         'catalog' AS channel,
         cs_ship_addr_sk col_name,
         d_year,
         d_qoy,
         i_category,
         cs_ext_sales_price ext_sales_price
       FROM catalog_sales, item, date_dim
       WHERE cs_ship_addr_sk IS NOT NULL
         AND cs_sold_date_sk = d_date_sk
         AND cs_item_sk = i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
LIMIT 100)sql";

inline constexpr std::string_view kQ77Sql = R"sql(WITH ss AS
(SELECT
    s_store_sk,
    sum(ss_ext_sales_price) AS sales,
    sum(ss_net_profit) AS profit
  FROM store_sales, date_dim, store
  WHERE ss_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
    AND ss_store_sk = s_store_sk
  GROUP BY s_store_sk),
    sr AS
  (SELECT
    s_store_sk,
    sum(sr_return_amt) AS returns,
    sum(sr_return_amt) AS profit_loss
  FROM store_returns, date_dim, store
  WHERE sr_returned_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
    AND sr_store_sk = s_store_sk
  GROUP BY s_store_sk),
    cs AS
  (SELECT
    cs_call_center_sk,
    sum(cs_ext_sales_price) AS sales,
    sum(cs_net_profit) AS profit
  FROM catalog_sales, date_dim
  WHERE cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
  GROUP BY cs_call_center_sk),
    cr AS
  (SELECT
    sum(cr_return_amount) AS returns,
    sum(cr_net_loss) AS profit_loss
  FROM catalog_returns, date_dim
  WHERE cr_returned_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)),
    ws AS
  (SELECT
    ws_ship_customer_sk,
    sum(ws_ext_sales_price) AS sales,
    sum(ws_net_profit) AS profit
  FROM web_sales, date_dim
  WHERE ws_sold_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
  GROUP BY ws_ship_customer_sk),
    wr AS
  (SELECT
    wr_refunded_customer_sk,
    sum(wr_return_amount) AS returns,
    sum(wr_net_loss) AS profit_loss
  FROM web_returns, date_dim
  WHERE wr_returned_date_sk = d_date_sk
    AND d_date BETWEEN cast('2000-08-03' AS DATE) AND
  (cast('2000-08-03' AS DATE) + INTERVAL 30 days)
  GROUP BY wr_refunded_customer_sk)
SELECT
  channel,
  id,
  sum(sales) AS sales,
  sum(returns) AS returns,
  sum(profit) AS profit
FROM
  (SELECT
     'store channel' AS channel,
     ss.s_store_sk AS id,
     sales,
     coalesce(returns, 0) AS returns,
     (profit - coalesce(profit_loss, 0)) AS profit
   FROM ss
     LEFT JOIN sr
       ON ss.s_store_sk = sr.s_store_sk
   UNION ALL
   SELECT
     'catalog channel' AS channel,
     cs_call_center_sk AS id,
     sales,
     returns,
     (profit - profit_loss) AS profit
   FROM cs, cr
   UNION ALL
   SELECT
     'web channel' AS channel,
     ws.ws_ship_customer_sk AS id,
     sales,
     coalesce(returns, 0) returns,
     (profit - coalesce(profit_loss, 0)) AS profit
   FROM ws
     LEFT JOIN wr
       ON ws.ws_ship_customer_sk = wr.wr_refunded_customer_sk
  ) x
GROUP BY ROLLUP (channel, id)
ORDER BY channel, id
LIMIT 100)sql";

// ---------------------------------------------------------------------------
// Wave-4 ports (web/channel dimensions + previously unreachable shapes).
// Fixture adaptations are noted per query; conventions follow the waves
// above (DOUBLE instead of DECIMAL, DATE casts for bare date strings,
// backtick aliases renamed to identifiers).
// ---------------------------------------------------------------------------

// q25: sr_net_loss -> sr_return_amt (the fixture store_returns carries no
// net-loss column; q77 set the precedent of substituting the return amount).
inline constexpr std::string_view kQ25Sql = R"sql(SELECT
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name,
  MAX(ss_net_profit) AS store_sales_profit,
  MAX(sr_return_amt) AS store_returns_loss,
  MAX(cs_net_profit) AS catalog_sales_profit
FROM store_sales,
     store_returns,
     catalog_sales,
     date_dim d1,
     date_dim d2,
     date_dim d3,
     store,
     item
WHERE d1.d_moy = 4
  AND d1.d_year = 2001
  AND d1.d_date_sk = ss_sold_date_sk
  AND i_item_sk = ss_item_sk
  AND s_store_sk = ss_store_sk
  AND ss_customer_sk = sr_customer_sk
  AND ss_item_sk = sr_item_sk
  AND ss_ticket_number = sr_ticket_number
  AND sr_returned_date_sk = d2.d_date_sk
  AND d2.d_moy BETWEEN 4 AND 10
  AND d2.d_year = 2001
  AND sr_customer_sk = cs_bill_customer_sk
  AND sr_item_sk = cs_item_sk
  AND cs_sold_date_sk = d3.d_date_sk
  AND d3.d_moy BETWEEN 4 AND 10
  AND d3.d_year = 2001
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100)sql";

// q32: official i_manufact_id = 610 misses the fixture's manufacturer pool
// (a quarter of items are 128, the rest draw 1..1000), so the port filters
// 128 like q3; the correlated 1.3 * avg scalar subquery is kept verbatim.
inline constexpr std::string_view kQ32Sql = R"sql(SELECT
  SUM(cs_ext_discount_amt) AS excess_discount_amount
FROM catalog_sales, item, date_dim
WHERE i_manufact_id = 128
  AND i_item_sk = cs_item_sk
  AND d_date BETWEEN CAST('2001-03-04' AS DATE)
                 AND (CAST('2001-03-04' AS DATE) + INTERVAL 90 day)
  AND d_date_sk = cs_sold_date_sk
  AND cs_ext_discount_amt >
      (SELECT 1.3 * AVG(cs_ext_discount_amt)
       FROM catalog_sales, item, date_dim
       WHERE cs_item_sk = i_item_sk
         AND d_date BETWEEN CAST('2001-03-04' AS DATE)
                        AND (CAST('2001-03-04' AS DATE) + INTERVAL 90 day)
         AND d_date_sk = cs_sold_date_sk)
LIMIT 100)sql";

// q36: ROLLUP(i_category, i_class) with GROUPING() folds in the window
// PARTITION BY — exercises the window-over-grouping-sets expansion; the
// s_state list is collapsed to the fixture's Tennessee stores as generated.
inline constexpr std::string_view kQ36Sql = R"sql(SELECT
  SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
  i_category,
  i_class,
  GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
  RANK() OVER (
    PARTITION BY GROUPING(i_category) + GROUPING(i_class),
                 CASE WHEN GROUPING(i_class) = 0 THEN i_category END
    ORDER BY SUM(ss_net_profit) / SUM(ss_ext_sales_price) ASC)
    AS rank_within_parent
FROM store_sales, date_dim d1, item, store
WHERE d1.d_year = 2000
  AND d1.d_date_sk = ss_sold_date_sk
  AND i_item_sk = ss_item_sk
  AND s_store_sk = ss_store_sk
  AND s_state = 'TN'
GROUP BY ROLLUP(i_category, i_class)
ORDER BY lochierarchy DESC,
         CASE WHEN lochierarchy = 0 THEN i_category END,
         rank_within_parent
LIMIT 100)sql";

// q38: three-channel customer/date INTERSECT.  The pinned parser requires
// an explicit ALL/DISTINCT after each set operator (same constraint as the
// q87 port), so INTERSECT is spelled INTERSECT DISTINCT.
inline constexpr std::string_view kQ38Sql = R"sql(SELECT COUNT(*)
FROM (SELECT DISTINCT c_last_name, c_first_name, d_date
      FROM store_sales, date_dim, customer
      WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
        AND store_sales.ss_customer_sk = customer.c_customer_sk
        AND d_month_seq BETWEEN 1188 AND 1188 + 11
      INTERSECT DISTINCT
      SELECT DISTINCT c_last_name, c_first_name, d_date
      FROM catalog_sales, date_dim, customer
      WHERE catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
        AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
        AND d_month_seq BETWEEN 1188 AND 1188 + 11
      INTERSECT DISTINCT
      SELECT DISTINCT c_last_name, c_first_name, d_date
      FROM web_sales, date_dim, customer
      WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk
        AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
        AND d_month_seq BETWEEN 1188 AND 1188 + 11) hot_cust
LIMIT 100)sql";

// q39: month-over-month coefficient-of-variation self-join over the
// inventory stddev; the template's two stream variants are the same query.
inline constexpr std::string_view kQ39Sql = R"sql(WITH inv AS
(SELECT w_warehouse_name,
        w_warehouse_sk,
        i_item_sk,
        d_moy,
        stdev,
        mean,
        CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov
 FROM (SELECT w_warehouse_name,
              w_warehouse_sk,
              i_item_sk,
              d_moy,
              STDDEV_SAMP(inv_quantity_on_hand) stdev,
              AVG(inv_quantity_on_hand) mean
       FROM inventory, item, warehouse, date_dim
       WHERE inv_item_sk = i_item_sk
         AND inv_warehouse_sk = w_warehouse_sk
         AND inv_date_sk = d_date_sk
         AND d_year = 2002
       GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo
 WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1)
SELECT inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
FROM inv inv1, inv inv2
WHERE inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1
  AND inv2.d_moy = 1 + 1
ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean,
         inv1.cov, inv2.d_moy, inv2.mean, inv2.cov
LIMIT 100)sql";

// q44: best/worst performing items by avg net profit per store.  The
// template's `ss_cdemo_sk IS NULL` population is empty in this fixture
// (cdemo keys are never NULL), so the port pins the 0.9 * avg reference to
// the top demo key like the other single-population adaptations.
inline constexpr std::string_view kQ44Sql = R"sql(SELECT
  asceding.rnk,
  i1.i_product_name best_performing,
  i2.i_product_name worst_performing
FROM (SELECT * FROM
       (SELECT item_sk, RANK() OVER (ORDER BY rank_col ASC) rnk
        FROM (SELECT ss_item_sk item_sk, AVG(ss_net_profit) rank_col
              FROM store_sales ss1
              WHERE ss_store_sk = 4
              GROUP BY ss_item_sk
              HAVING AVG(ss_net_profit) > 0.9 *
                     (SELECT AVG(ss_net_profit)
                      FROM store_sales
                      WHERE ss_store_sk = 4 AND ss_cdemo_sk = 200
                      GROUP BY ss_store_sk)) V1) V11
       WHERE rnk < 11) asceding,
     (SELECT * FROM
       (SELECT item_sk, RANK() OVER (ORDER BY rank_col DESC) rnk
        FROM (SELECT ss_item_sk item_sk, AVG(ss_net_profit) rank_col
              FROM store_sales ss1
              WHERE ss_store_sk = 4
              GROUP BY ss_item_sk
              HAVING AVG(ss_net_profit) > 0.9 *
                     (SELECT AVG(ss_net_profit)
                      FROM store_sales
                      WHERE ss_store_sk = 4 AND ss_cdemo_sk = 200
                      GROUP BY ss_store_sk)) V2) V21
       WHERE rnk < 11) descending,
     item i1,
     item i2
WHERE asceding.rnk = descending.rnk
  AND i1.i_item_sk = asceding.item_sk
  AND i2.i_item_sk = descending.item_sk
ORDER BY asceding.rnk
LIMIT 100)sql";

// q45: disjunctive predicate over zip prefix / item id IN-subquery, kept
// verbatim; the fixture's random zips rarely collide with the template's
// literals so the productive side of the OR is the item branch.
inline constexpr std::string_view kQ45Sql = R"sql(SELECT
  ca_zip,
  ca_state,
  SUM(ws_sales_price)
FROM web_sales, customer, customer_address, date_dim, item
WHERE ws_bill_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND ws_item_sk = i_item_sk
  AND (SUBSTR(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405',
                                '86475', '85392', '85460', '80348', '81792')
       OR i_item_id IN (SELECT i_item_id
                        FROM item
                        WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)))
  AND ws_sold_date_sk = d_date_sk
  AND d_qoy = 1
  AND d_year = 2000
GROUP BY ca_zip, ca_state
ORDER BY ca_zip, ca_state
LIMIT 100)sql";

// q5: per-channel sales/returns rollup over the 14-day window ending
// 2002-08-22.  The web branch keeps the official web_returns LEFT JOIN
// web_sales shape; DECIMAL(7,2) zero casts become plain 0.
inline constexpr std::string_view kQ5Sql = R"sql(WITH ssr AS
(SELECT s_store_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
 FROM (SELECT ss_store_sk AS store_sk,
              ss_sold_date_sk AS date_sk,
              ss_ext_sales_price AS sales_price,
              ss_net_profit AS profit,
              0 AS return_amt,
              0 AS net_loss
       FROM store_sales
       UNION ALL
       SELECT sr_store_sk AS store_sk,
              sr_returned_date_sk AS date_sk,
              0 AS sales_price,
              0 AS profit,
              sr_return_amt AS return_amt,
              sr_return_amt AS net_loss
       FROM store_returns) salesreturns,
      date_dim,
      store
 WHERE date_sk = d_date_sk
   AND d_date BETWEEN CAST('2002-08-22' AS DATE)
                  AND (CAST('2002-08-22' AS DATE) + INTERVAL 14 day)
   AND store_sk = s_store_sk
 GROUP BY s_store_id),
csr AS
(SELECT cp_catalog_page_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
 FROM (SELECT cs_catalog_page_sk AS page_sk,
              cs_sold_date_sk AS date_sk,
              cs_ext_sales_price AS sales_price,
              cs_net_profit AS profit,
              0 AS return_amt,
              0 AS net_loss
       FROM catalog_sales
       UNION ALL
       SELECT cr_catalog_page_sk AS page_sk,
              cr_returned_date_sk AS date_sk,
              0 AS sales_price,
              0 AS profit,
              cr_return_amount AS return_amt,
              cr_net_loss AS net_loss
       FROM catalog_returns) salesreturns,
      date_dim,
      catalog_page
 WHERE date_sk = d_date_sk
   AND d_date BETWEEN CAST('2002-08-22' AS DATE)
                  AND (CAST('2002-08-22' AS DATE) + INTERVAL 14 day)
   AND page_sk = cp_catalog_page_sk
 GROUP BY cp_catalog_page_id),
wsr AS
(SELECT web_site_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
 FROM (SELECT ws_web_site_sk AS wsr_web_site_sk,
              ws_sold_date_sk AS date_sk,
              ws_ext_sales_price AS sales_price,
              ws_net_profit AS profit,
              0 AS return_amt,
              0 AS net_loss
       FROM web_sales
       UNION ALL
       SELECT ws_web_site_sk AS wsr_web_site_sk,
              wr_returned_date_sk AS date_sk,
              0 AS sales_price,
              0 AS profit,
              wr_return_amount AS return_amt,
              wr_net_loss AS net_loss
       FROM web_returns
       LEFT OUTER JOIN web_sales
         ON (wr_item_sk = ws_item_sk AND wr_order_number = ws_order_number))
      salesreturns,
      date_dim,
      web_site
 WHERE date_sk = d_date_sk
   AND d_date BETWEEN CAST('2002-08-22' AS DATE)
                  AND (CAST('2002-08-22' AS DATE) + INTERVAL 14 day)
   AND wsr_web_site_sk = web_site_sk
 GROUP BY web_site_id)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns1) AS returns1,
       SUM(profit) AS profit
FROM (SELECT 'store channel' AS channel,
             'store' || s_store_id AS id,
             sales,
             returns1,
             (profit - profit_loss) AS profit
      FROM ssr
      UNION ALL
      SELECT 'catalog channel' AS channel,
             'catalog_page' || cp_catalog_page_id AS id,
             sales,
             returns1,
             (profit - profit_loss) AS profit
      FROM csr
      UNION ALL
      SELECT 'web channel' AS channel,
             'web_site' || web_site_id AS id,
             sales,
             returns1,
             (profit - profit_loss) AS profit
      FROM wsr) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100)sql";

// q30: web returns concentration by state, over the returning-customer
// address columns added to the fixture; c_last_review_date is carried in
// the SELECT / ORDER BY like the template.
inline constexpr std::string_view kQ30Sql = R"sql(WITH customer_total_return AS
(SELECT wr_returning_customer_sk AS ctr_customer_sk,
        ca_state AS ctr_state,
        SUM(wr_return_amount) AS ctr_total_return
 FROM web_returns, date_dim, customer_address
 WHERE wr_returned_date_sk = d_date_sk
   AND d_year = 2000
   AND wr_returning_addr_sk = ca_address_sk
 GROUP BY wr_returning_customer_sk, ca_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       c_preferred_cust_flag,
       c_birth_day,
       c_birth_month,
       c_birth_year,
       c_birth_country,
       c_login,
       c_email_address,
       c_last_review_date,
       ctr_total_return
FROM customer_total_return ctr1, customer_address, customer
WHERE ctr1.ctr_total_return >
      (SELECT AVG(ctr_total_return) * 1.2
       FROM customer_total_return ctr2
       WHERE ctr1.ctr_state = ctr2.ctr_state)
  AND ca_address_sk = c_current_addr_sk
  AND ca_state = 'IN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id, c_salutation, c_first_name, c_last_name,
         c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year,
         c_birth_country, c_login, c_email_address, c_last_review_date,
         ctr_total_return
LIMIT 100)sql";

// q49: per-channel return/currency ratio ranks.  The template's return
// thresholds (> 10000) sit at the extreme tail of the fixture's sale
// prices, so the port lowers them to 5000 (same shape, three channels).
// Bare INTERSECT-style set operators are not needed here; the UNIONs are
// DISTINCT by default in GoogleSQL.
inline constexpr std::string_view kQ49Sql = R"sql(SELECT 'web' AS channel,
       web.item,
       web.return_ratio,
       web.return_rank,
       web.currency_rank
FROM (SELECT item,
             return_ratio,
             currency_ratio,
             RANK() OVER (ORDER BY return_ratio) AS return_rank,
             RANK() OVER (ORDER BY currency_ratio) AS currency_rank
      FROM (SELECT ws.ws_item_sk AS item,
                   (SUM(COALESCE(wr.wr_return_quantity, 0)) /
                    SUM(COALESCE(ws.ws_quantity, 0))) AS return_ratio,
                   (SUM(COALESCE(wr.wr_return_amount, 0)) /
                    SUM(COALESCE(ws.ws_net_paid, 0))) AS currency_ratio
            FROM web_sales ws
            LEFT OUTER JOIN web_returns wr
              ON (ws.ws_order_number = wr.wr_order_number
                  AND ws.ws_item_sk = wr.wr_item_sk),
            date_dim
            WHERE wr.wr_return_amount > 5000
              AND ws.ws_net_profit > 1
              AND ws.ws_net_paid > 0
              AND ws.ws_quantity > 0
              AND ws_sold_date_sk = d_date_sk
              AND d_year = 1999
              AND d_moy = 12
            GROUP BY ws.ws_item_sk) in_web) web
WHERE (web.return_rank <= 10 OR web.currency_rank <= 10)
UNION DISTINCT
SELECT 'catalog' AS channel,
       catalog.item,
       catalog.return_ratio,
       catalog.return_rank,
       catalog.currency_rank
FROM (SELECT item,
             return_ratio,
             currency_ratio,
             RANK() OVER (ORDER BY return_ratio) AS return_rank,
             RANK() OVER (ORDER BY currency_ratio) AS currency_rank
      FROM (SELECT cs.cs_item_sk AS item,
                   (SUM(COALESCE(cr.cr_return_quantity, 0)) /
                    SUM(COALESCE(cs.cs_quantity, 0))) AS return_ratio,
                   (SUM(COALESCE(cr.cr_return_amount, 0)) /
                    SUM(COALESCE(cs.cs_net_paid, 0))) AS currency_ratio
            FROM catalog_sales cs
            LEFT OUTER JOIN catalog_returns cr
              ON (cs.cs_order_number = cr.cr_order_number
                  AND cs.cs_item_sk = cr.cr_item_sk),
            date_dim
            WHERE cr.cr_return_amount > 5000
              AND cs.cs_net_profit > 1
              AND cs.cs_net_paid > 0
              AND cs.cs_quantity > 0
              AND cs_sold_date_sk = d_date_sk
              AND d_year = 1999
              AND d_moy = 12
            GROUP BY cs.cs_item_sk) in_cat) catalog
WHERE (catalog.return_rank <= 10 OR catalog.currency_rank <= 10)
UNION DISTINCT
SELECT 'store' AS channel,
       store.item,
       store.return_ratio,
       store.return_rank,
       store.currency_rank
FROM (SELECT item,
             return_ratio,
             currency_ratio,
             RANK() OVER (ORDER BY return_ratio) AS return_rank,
             RANK() OVER (ORDER BY currency_ratio) AS currency_rank
      FROM (SELECT sts.ss_item_sk AS item,
                   (SUM(COALESCE(sr.sr_return_quantity, 0)) /
                    SUM(COALESCE(sts.ss_quantity, 0))) AS return_ratio,
                   (SUM(COALESCE(sr.sr_return_amt, 0)) /
                    SUM(COALESCE(sts.ss_net_paid, 0))) AS currency_ratio
            FROM store_sales sts
            LEFT OUTER JOIN store_returns sr
              ON (sts.ss_ticket_number = sr.sr_ticket_number
                  AND sts.ss_item_sk = sr.sr_item_sk),
            date_dim
            WHERE sr.sr_return_amt > 5000
              AND sts.ss_net_profit > 1
              AND sts.ss_net_paid > 0
              AND sts.ss_quantity > 0
              AND ss_sold_date_sk = d_date_sk
              AND d_year = 1999
              AND d_moy = 12
            GROUP BY sts.ss_item_sk) in_store) store
WHERE (store.return_rank <= 10 OR store.currency_rank <= 10)
ORDER BY 1, 4, 5
LIMIT 100)sql";

// q51: cumulative sales race between web and store per (item, date); the
// FULL OUTER JOIN + grouped-window running sums are kept verbatim.
inline constexpr std::string_view kQ51Sql = R"sql(WITH web_v1 AS
(SELECT ws_item_sk item_sk,
        d_date,
        SUM(SUM(ws_sales_price)) OVER
          (PARTITION BY ws_item_sk ORDER BY d_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
 FROM web_sales, date_dim
 WHERE ws_sold_date_sk = d_date_sk
   AND d_month_seq BETWEEN 1192 AND 1192 + 11
   AND ws_item_sk IS NOT NULL
 GROUP BY ws_item_sk, d_date),
store_v1 AS
(SELECT ss_item_sk item_sk,
        d_date,
        SUM(SUM(ss_sales_price)) OVER
          (PARTITION BY ss_item_sk ORDER BY d_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
 FROM store_sales, date_dim
 WHERE ss_sold_date_sk = d_date_sk
   AND d_month_seq BETWEEN 1192 AND 1192 + 11
   AND ss_item_sk IS NOT NULL
 GROUP BY ss_item_sk, d_date)
SELECT *
FROM (SELECT item_sk,
             d_date,
             web_sales,
             store_sales,
             MAX(web_sales) OVER
               (PARTITION BY item_sk ORDER BY d_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
               web_cumulative,
             MAX(store_sales) OVER
               (PARTITION BY item_sk ORDER BY d_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
               store_cumulative
      FROM (SELECT CASE WHEN web.item_sk IS NOT NULL THEN web.item_sk
                        ELSE store.item_sk END item_sk,
                   CASE WHEN web.d_date IS NOT NULL THEN web.d_date
                        ELSE store.d_date END d_date,
                   web.cume_sales web_sales,
                   store.cume_sales store_sales
            FROM web_v1 web
            FULL OUTER JOIN store_v1 store
              ON (web.item_sk = store.item_sk AND web.d_date = store.d_date)) x)
      y
WHERE web_cumulative > store_cumulative
ORDER BY item_sk, d_date
LIMIT 100)sql";

// q54: Sports customers (fitness class -> the fixture's football class)
// and their subsequent store revenue segments.
inline constexpr std::string_view kQ54Sql = R"sql(WITH my_customers AS
(SELECT DISTINCT c_customer_sk, c_current_addr_sk
 FROM (SELECT cs_sold_date_sk sold_date_sk,
              cs_bill_customer_sk customer_sk,
              cs_item_sk item_sk
       FROM catalog_sales
       UNION ALL
       SELECT ws_sold_date_sk sold_date_sk,
              ws_bill_customer_sk customer_sk,
              ws_item_sk item_sk
       FROM web_sales) cs_or_ws_sales,
      item,
      date_dim,
      customer
 WHERE sold_date_sk = d_date_sk
   AND item_sk = i_item_sk
   AND i_category = 'Sports'
   AND i_class = 'football'
   AND c_customer_sk = cs_or_ws_sales.customer_sk
   AND d_moy = 5
   AND d_year = 2000),
my_revenue AS
(SELECT c_customer_sk,
        SUM(ss_ext_sales_price) AS revenue
 FROM my_customers, store_sales, customer_address, store, date_dim
 WHERE c_current_addr_sk = ca_address_sk
   AND ca_county = s_county
   AND ca_state = s_state
   AND ss_sold_date_sk = d_date_sk
   AND c_customer_sk = ss_customer_sk
   AND d_month_seq BETWEEN (SELECT DISTINCT d_month_seq + 1
                            FROM date_dim WHERE d_year = 2000 AND d_moy = 5)
                       AND (SELECT DISTINCT d_month_seq + 3
                            FROM date_dim WHERE d_year = 2000 AND d_moy = 5)
 GROUP BY c_customer_sk),
segments AS
(SELECT CAST((revenue / 50) AS INT64) AS segment
 FROM my_revenue)
SELECT segment,
       COUNT(*) AS num_customers,
       segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment, num_customers
LIMIT 100)sql";

// q56: three-channel item revenue in March 1998 for a fixed gmt offset;
// the color pool is re-spelled to the fixture's palette and the template's
// ca_gmt_offset = -6 to the generated -5 band.
inline constexpr std::string_view kQ56Sql = R"sql(WITH ss AS
(SELECT i_item_id,
        SUM(ss_ext_sales_price) total_sales
 FROM store_sales, date_dim, customer_address, item
 WHERE i_item_id IN (SELECT i_item_id FROM item
                     WHERE i_color IN ('red', 'white'))
   AND ss_item_sk = i_item_sk
   AND ss_sold_date_sk = d_date_sk
   AND d_year = 1998
   AND d_moy = 3
   AND ss_addr_sk = ca_address_sk
   AND ca_gmt_offset = -5
 GROUP BY i_item_id),
cs AS
(SELECT i_item_id,
        SUM(cs_ext_sales_price) total_sales
 FROM catalog_sales, date_dim, customer_address, item
 WHERE i_item_id IN (SELECT i_item_id FROM item
                     WHERE i_color IN ('red', 'white'))
   AND cs_item_sk = i_item_sk
   AND cs_sold_date_sk = d_date_sk
   AND d_year = 1998
   AND d_moy = 3
   AND cs_bill_addr_sk = ca_address_sk
   AND ca_gmt_offset = -5
 GROUP BY i_item_id),
ws AS
(SELECT i_item_id,
        SUM(ws_ext_sales_price) total_sales
 FROM web_sales, date_dim, customer_address, item
 WHERE i_item_id IN (SELECT i_item_id FROM item
                     WHERE i_color IN ('red', 'white'))
   AND ws_item_sk = i_item_sk
   AND ws_sold_date_sk = d_date_sk
   AND d_year = 1998
   AND d_moy = 3
   AND ws_bill_addr_sk = ca_address_sk
   AND ca_gmt_offset = -5
 GROUP BY i_item_id)
SELECT i_item_id,
       SUM(total_sales) total_sales
FROM (SELECT * FROM ss
      UNION ALL
      SELECT * FROM cs
      UNION ALL
      SELECT * FROM ws) tmp1
GROUP BY i_item_id
ORDER BY total_sales
LIMIT 100)sql";

// q57: monthly sales vs the category/brand/center average with a lag/lead
// self-join over the rank sequence (cc_name now exists in the fixture).
inline constexpr std::string_view kQ57Sql = R"sql(WITH v1 AS
(SELECT i_category,
        i_brand,
        cc_name,
        d_year,
        d_moy,
        SUM(cs_sales_price) sum_sales,
        AVG(SUM(cs_sales_price)) OVER
          (PARTITION BY i_category, i_brand, cc_name, d_year)
          avg_monthly_sales,
        RANK() OVER (PARTITION BY i_category, i_brand, cc_name
                     ORDER BY d_year, d_moy) rn
 FROM item, catalog_sales, date_dim, call_center
 WHERE cs_item_sk = i_item_sk
   AND cs_sold_date_sk = d_date_sk
   AND cc_call_center_sk = cs_call_center_sk
   AND (d_year = 2000
        OR (d_year = 2000 - 1 AND d_moy = 12)
        OR (d_year = 2000 + 1 AND d_moy = 1))
 GROUP BY i_category, i_brand, cc_name, d_year, d_moy),
v2 AS
(SELECT v1.i_brand,
        v1.d_year,
        v1.avg_monthly_sales,
        v1.sum_sales,
        v1_lag.sum_sales psum,
        v1_lead.sum_sales nsum
 FROM v1, v1 v1_lag, v1 v1_lead
 WHERE v1.i_category = v1_lag.i_category
   AND v1.i_category = v1_lead.i_category
   AND v1.i_brand = v1_lag.i_brand
   AND v1.i_brand = v1_lead.i_brand
   AND v1.cc_name = v1_lag.cc_name
   AND v1.cc_name = v1_lead.cc_name
   AND v1.rn = v1_lag.rn + 1
   AND v1.rn = v1_lead.rn - 1)
SELECT *, sum_sales - avg_monthly_sales AS sales_diff
FROM v2
WHERE d_year = 2000
  AND avg_monthly_sales > 0
  AND CASE WHEN avg_monthly_sales > 0
           THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
           ELSE NULL END > 0.1
ORDER BY sales_diff, 3
LIMIT 100)sql";

// q58: channel revenue convergence.  The template anchors on one week of
// 2002-02-25 with +-10% bands; the fixture's weekly per-item three-channel
// overlap is too sparse to guarantee rows, so the port sums over the
// quarter of the anchor date and widens the bands to +-25% (same
// convergence shape, adapted populations).
inline constexpr std::string_view kQ58Sql = R"sql(WITH ss_items AS
(SELECT i_item_id item_id,
        SUM(ss_ext_sales_price) ss_item_rev
 FROM store_sales, item, date_dim
 WHERE ss_item_sk = i_item_sk
   AND d_month_seq BETWEEN (SELECT d_month_seq FROM date_dim
                            WHERE d_date = CAST('2002-02-25' AS DATE))
                       AND (SELECT d_month_seq + 2 FROM date_dim
                            WHERE d_date = CAST('2002-02-25' AS DATE))
   AND ss_sold_date_sk = d_date_sk
 GROUP BY i_item_id),
cs_items AS
(SELECT i_item_id item_id,
        SUM(cs_ext_sales_price) cs_item_rev
 FROM catalog_sales, item, date_dim
 WHERE cs_item_sk = i_item_sk
   AND d_month_seq BETWEEN (SELECT d_month_seq FROM date_dim
                            WHERE d_date = CAST('2002-02-25' AS DATE))
                       AND (SELECT d_month_seq + 2 FROM date_dim
                            WHERE d_date = CAST('2002-02-25' AS DATE))
   AND cs_sold_date_sk = d_date_sk
 GROUP BY i_item_id),
ws_items AS
(SELECT i_item_id item_id,
        SUM(ws_ext_sales_price) ws_item_rev
 FROM web_sales, item, date_dim
 WHERE ws_item_sk = i_item_sk
   AND d_month_seq BETWEEN (SELECT d_month_seq FROM date_dim
                            WHERE d_date = CAST('2002-02-25' AS DATE))
                       AND (SELECT d_month_seq + 2 FROM date_dim
                            WHERE d_date = CAST('2002-02-25' AS DATE))
   AND ws_sold_date_sk = d_date_sk
 GROUP BY i_item_id)
SELECT ss_items.item_id,
       ss_item_rev,
       ss_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 ss_dev,
       cs_item_rev,
       cs_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 cs_dev,
       ws_item_rev,
       ws_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 ws_dev,
       (ss_item_rev + cs_item_rev + ws_item_rev) / 3 average
FROM ss_items, cs_items, ws_items
WHERE ss_items.item_id = cs_items.item_id
  AND ss_items.item_id = ws_items.item_id
  AND ss_item_rev BETWEEN 0.75 * cs_item_rev AND 1.25 * cs_item_rev
  AND ss_item_rev BETWEEN 0.75 * ws_item_rev AND 1.25 * ws_item_rev
  AND cs_item_rev BETWEEN 0.75 * ss_item_rev AND 1.25 * ss_item_rev
  AND cs_item_rev BETWEEN 0.75 * ws_item_rev AND 1.25 * ws_item_rev
  AND ws_item_rev BETWEEN 0.75 * ss_item_rev AND 1.25 * ss_item_rev
  AND ws_item_rev BETWEEN 0.75 * cs_item_rev AND 1.25 * cs_item_rev
ORDER BY ss_items.item_id
LIMIT 100)sql";

// q62: web shipping-delay histogram by warehouse / ship mode / site; the
// backtick aliases are renamed (days_0_30 ... over_120) per the q50
// precedent.
inline constexpr std::string_view kQ62Sql = R"sql(SELECT
  SUBSTR(w_warehouse_name, 1, 20),
  sm_type,
  web_name,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1 ELSE 0 END)
    AS days_0_30,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
            AND (ws_ship_date_sk - ws_sold_date_sk <= 60)
           THEN 1 ELSE 0 END) AS days_31_60,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
            AND (ws_ship_date_sk - ws_sold_date_sk <= 90)
           THEN 1 ELSE 0 END) AS days_61_90,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
            AND (ws_ship_date_sk - ws_sold_date_sk <= 120)
           THEN 1 ELSE 0 END) AS days_91_120,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1 ELSE 0 END)
    AS over_120
FROM web_sales, warehouse, ship_mode, web_site, date_dim
WHERE d_month_seq BETWEEN 1222 AND 1222 + 11
  AND ws_ship_date_sk = d_date_sk
  AND ws_warehouse_sk = w_warehouse_sk
  AND ws_ship_mode_sk = sm_ship_mode_sk
  AND ws_web_site_sk = web_site_sk
GROUP BY SUBSTR(w_warehouse_name, 1, 20), sm_type, web_name
ORDER BY SUBSTR(w_warehouse_name, 1, 20), sm_type, web_name
LIMIT 100)sql";

// q71: time-of-day sales by brand across the three channels; i_manager_id
// = 1 stays (the fixture manager pool spans 1..64) and t_meal_time bands
// follow the generated breakfast/dinner rows of time_dim.
inline constexpr std::string_view kQ71Sql = R"sql(SELECT
  i_brand_id brand_id,
  i_brand brand,
  t_hour,
  t_minute,
  SUM(ext_price) ext_price
FROM item,
     (SELECT ws_ext_sales_price AS ext_price,
             ws_item_sk AS sold_item_sk,
             ws_sold_time_sk AS time_sk
      FROM web_sales, date_dim
      WHERE d_date_sk = ws_sold_date_sk
        AND d_moy = 11
        AND d_year = 2001
      UNION ALL
      SELECT cs_ext_sales_price AS ext_price,
             cs_item_sk AS sold_item_sk,
             cs_sold_time_sk AS time_sk
      FROM catalog_sales, date_dim
      WHERE d_date_sk = cs_sold_date_sk
        AND d_moy = 11
        AND d_year = 2001
      UNION ALL
      SELECT ss_ext_sales_price AS ext_price,
             ss_item_sk AS sold_item_sk,
             ss_sold_time_sk AS time_sk
      FROM store_sales, date_dim
      WHERE d_date_sk = ss_sold_date_sk
        AND d_moy = 11
        AND d_year = 2001) AS tmp,
     time_dim
WHERE sold_item_sk = i_item_sk
  AND i_manager_id = 1
  AND time_sk = t_time_sk
  AND (t_meal_time = 'breakfast' OR t_meal_time = 'dinner')
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id
LIMIT 100)sql";

// q72: inventory vs shipped catalog sales with the promotion / returns
// outer joins; hd_buy_potential is re-spelled to the fixture's
// '1000-5000' band (template literal '501-1000' is not in the pool), and
// the sold-out check reads inv_quantity_on_hand < cs_quantity * 20 — the
// template's raw comparison leaves ~1 surviving row at fixture scale
// (sold quantities are 1..20 against 0..500 on-hand pools).
inline constexpr std::string_view kQ72Sql = R"sql(SELECT
  i_item_desc,
  w_warehouse_name,
  d1.d_week_seq,
  SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo,
  SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo,
  COUNT(*) total_cnt
FROM catalog_sales
JOIN inventory ON (cs_item_sk = inv_item_sk)
JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
JOIN item ON (i_item_sk = cs_item_sk)
JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT OUTER JOIN promotion ON (cs_promo_sk = p_promo_sk)
LEFT OUTER JOIN catalog_returns
  ON (cr_item_sk = cs_item_sk AND cr_order_number = cs_order_number)
WHERE d1.d_week_seq = d2.d_week_seq
  AND inv_quantity_on_hand < cs_quantity * 20
  AND d3.d_date > d1.d_date + INTERVAL 5 day
  AND hd_buy_potential = '1000-5000'
  AND d1.d_year = 2002
  AND cd_marital_status = 'M'
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d_week_seq
LIMIT 100)sql";

// q41: attribute-qualified item population per manufacturer.  The template
// literal pools (units/sizes/colors) do not exist in this fixture, so the
// port re-spells them over the generated color/size pools and drops the
// i_units conjuncts (the fixture has no units column).
inline constexpr std::string_view kQ41Sql =
    R"sql(SELECT DISTINCT(i_product_name)
FROM item i1
WHERE i_manufact_id BETWEEN 765 AND 765 + 40
  AND (SELECT COUNT(*) AS item_cnt
       FROM item
       WHERE (i_manufact = i1.i_manufact
              AND ((i_category = 'Women'
                    AND (i_color = 'green' OR i_color = 'purple')
                    AND (i_size = 'XS' OR i_size = 'S'))
                   OR (i_category = 'Women'
                       AND (i_color = 'green' OR i_color = 'black')
                       AND (i_size = 'M' OR i_size = 'L'))
                   OR (i_category = 'Men'
                       AND (i_color = 'blue' OR i_color = 'black')
                       AND (i_size = 'XL' OR i_size = 'XXL'))
                   OR (i_category = 'Men'
                       AND (i_color = 'white' OR i_color = 'yellow')
                       AND (i_size = 'S' OR i_size = 'M'))))
          OR (i_manufact = i1.i_manufact
              AND ((i_category = 'Women'
                    AND (i_color = 'red' OR i_color = 'white')
                    AND (i_size = 'XS' OR i_size = 'S'))
                   OR (i_category = 'Women'
                       AND (i_color = 'purple' OR i_color = 'yellow')
                       AND (i_size = 'M' OR i_size = 'L'))
                   OR (i_category = 'Men'
                       AND (i_color = 'green' OR i_color = 'white')
                       AND (i_size = 'XL' OR i_size = 'XXL'))
                   OR (i_category = 'Men'
                       AND (i_color = 'black' OR i_color = 'red')
                       AND (i_size = 'S' OR i_size = 'M'))))) > 0
ORDER BY i_product_name
LIMIT 100)sql";

// q74: customers whose web revenue grew faster than their store revenue
// year-over-year (1999 -> 2000); four-way self-join of the year_total CTE.
inline constexpr std::string_view kQ74Sql = R"sql(WITH year_total AS
(SELECT c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        d_year AS year1,
        SUM(ss_net_paid) year_total,
        's' sale_type
 FROM customer, store_sales, date_dim
 WHERE c_customer_sk = ss_customer_sk
   AND ss_sold_date_sk = d_date_sk
   AND d_year IN (1999, 1999 + 1)
 GROUP BY c_customer_id, c_first_name, c_last_name, d_year
 UNION ALL
 SELECT c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        d_year AS year1,
        SUM(ws_net_paid) year_total,
        'w' sale_type
 FROM customer, web_sales, date_dim
 WHERE c_customer_sk = ws_bill_customer_sk
   AND ws_sold_date_sk = d_date_sk
   AND d_year IN (1999, 1999 + 1)
 GROUP BY c_customer_id, c_first_name, c_last_name, d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.year1 = 1999
  AND t_s_secyear.year1 = 1999 + 1
  AND t_w_firstyear.year1 = 1999
  AND t_w_secyear.year1 = 1999 + 1
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE WHEN t_w_firstyear.year_total > 0
           THEN t_w_secyear.year_total / t_w_firstyear.year_total
           ELSE NULL END
      > CASE WHEN t_s_firstyear.year_total > 0
             THEN t_s_secyear.year_total / t_s_firstyear.year_total
             ELSE NULL END
ORDER BY 1, 2, 3
LIMIT 100)sql";

// q78: store-channel items against the web/catalog aggregates.  The
// template groups every channel by (year, item, customer) and requires
// the same triple in all three; that co-occurrence is empty at fixture
// scale, so the port aggregates per (year, item) and keeps the
// anti-join + comparison shape.
inline constexpr std::string_view kQ78Sql = R"sql(WITH ws AS
(SELECT d_year AS ws_sold_year,
        ws_item_sk,
        ws_bill_customer_sk ws_customer_sk,
        SUM(ws_quantity) ws_qty,
        SUM(ws_wholesale_cost) ws_wc,
        SUM(ws_sales_price) ws_sp
 FROM web_sales
 LEFT JOIN web_returns
   ON wr_order_number = ws_order_number AND ws_item_sk = wr_item_sk
 JOIN date_dim ON ws_sold_date_sk = d_date_sk
 WHERE wr_order_number IS NULL
 GROUP BY d_year, ws_item_sk),
cs AS
(SELECT d_year AS cs_sold_year,
        cs_item_sk,
        cs_bill_customer_sk cs_customer_sk,
        SUM(cs_quantity) cs_qty,
        SUM(cs_wholesale_cost) cs_wc,
        SUM(cs_sales_price) cs_sp
 FROM catalog_sales
 LEFT JOIN catalog_returns
   ON cr_order_number = cs_order_number AND cs_item_sk = cr_item_sk
 JOIN date_dim ON cs_sold_date_sk = d_date_sk
 WHERE cr_order_number IS NULL
 GROUP BY d_year, cs_item_sk),
ss AS
(SELECT d_year AS ss_sold_year,
        ss_item_sk,
        ss_customer_sk,
        SUM(ss_quantity) ss_qty,
        SUM(ss_wholesale_cost) ss_wc,
        SUM(ss_sales_price) ss_sp
 FROM store_sales
 LEFT JOIN store_returns
   ON sr_ticket_number = ss_ticket_number AND ss_item_sk = sr_item_sk
 JOIN date_dim ON ss_sold_date_sk = d_date_sk
 WHERE sr_ticket_number IS NULL
 GROUP BY d_year, ss_item_sk)
SELECT ss_item_sk,
       ROUND(ss_qty / (COALESCE(ws_qty + cs_qty, 1)), 2) ratio,
       ss_qty store_qty,
       ss_wc store_wholesale_cost,
       ss_sp store_sales_price,
       COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0) other_chan_qty,
       COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0) other_chan_wholesale_cost,
       COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0) other_chan_sales_price
FROM ss
LEFT JOIN ws
  ON (ws_sold_year = ss_sold_year AND ws_item_sk = ss_item_sk)
LEFT JOIN cs
  ON (cs_sold_year = ss_sold_year AND cs_item_sk = ss_item_sk)
WHERE COALESCE(ws_qty, 0) > 0
  AND COALESCE(cs_qty, 0) > 0
  AND ss_sold_year = 1999
ORDER BY ss_item_sk, ss_qty DESC, ss_wc DESC, ss_sp DESC, other_chan_qty,
         other_chan_wholesale_cost, other_chan_sales_price,
         ROUND(ss_qty / (COALESCE(ws_qty + cs_qty, 1)), 2)
LIMIT 100)sql";

// q80: per-channel sales/returns rollup with the TV-promotion filter, over
// the 30-day window ending 2000-08-26 (web_site / catalog_page keys from
// the wave-4 fixture columns).  store_returns carries no net-loss column,
// so the profit subtraction reads COALESCE(sr_return_amt, 0) (q25/q77
// precedent).
inline constexpr std::string_view kQ80Sql = R"sql(WITH ssr AS
(SELECT s_store_id AS store_id,
        SUM(ss_ext_sales_price) AS sales,
        SUM(COALESCE(sr_return_amt, 0)) AS returns1,
        SUM(ss_net_profit - COALESCE(sr_return_amt, 0)) AS profit
 FROM store_sales
 LEFT OUTER JOIN store_returns
   ON (ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number),
     date_dim, store, item, promotion
 WHERE ss_sold_date_sk = d_date_sk
   AND d_date BETWEEN CAST('2000-08-26' AS DATE)
                  AND (CAST('2000-08-26' AS DATE) + INTERVAL 30 day)
   AND ss_store_sk = s_store_sk
   AND ss_item_sk = i_item_sk
   AND i_current_price > 50
   AND ss_promo_sk = p_promo_sk
   AND p_channel_tv = 'N'
 GROUP BY s_store_id),
csr AS
(SELECT cp_catalog_page_id AS catalog_page_id,
        SUM(cs_ext_sales_price) AS sales,
        SUM(COALESCE(cr_return_amount, 0)) AS returns1,
        SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
 FROM catalog_sales
 LEFT OUTER JOIN catalog_returns
   ON (cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number),
     date_dim, catalog_page, item, promotion
 WHERE cs_sold_date_sk = d_date_sk
   AND d_date BETWEEN CAST('2000-08-26' AS DATE)
                  AND (CAST('2000-08-26' AS DATE) + INTERVAL 30 day)
   AND cs_catalog_page_sk = cp_catalog_page_sk
   AND cs_item_sk = i_item_sk
   AND i_current_price > 50
   AND cs_promo_sk = p_promo_sk
   AND p_channel_tv = 'N'
 GROUP BY cp_catalog_page_id),
wsr AS
(SELECT web_site_id,
        SUM(ws_ext_sales_price) AS sales,
        SUM(COALESCE(wr_return_amount, 0)) AS returns1,
        SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
 FROM web_sales
 LEFT OUTER JOIN web_returns
   ON (ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number),
     date_dim, web_site, item, promotion
 WHERE ws_sold_date_sk = d_date_sk
   AND d_date BETWEEN CAST('2000-08-26' AS DATE)
                  AND (CAST('2000-08-26' AS DATE) + INTERVAL 30 day)
   AND ws_web_site_sk = web_site_sk
   AND ws_item_sk = i_item_sk
   AND i_current_price > 50
   AND ws_promo_sk = p_promo_sk
   AND p_channel_tv = 'N'
 GROUP BY web_site_id)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns1) AS returns1,
       SUM(profit) AS profit
FROM (SELECT 'store channel' AS channel,
             'store' || store_id AS id,
             sales, returns1, profit
      FROM ssr
      UNION ALL
      SELECT 'catalog channel' AS channel,
             'catalog_page' || catalog_page_id AS id,
             sales, returns1, profit
      FROM csr
      UNION ALL
      SELECT 'web channel' AS channel,
             'web_site' || web_site_id AS id,
             sales, returns1, profit
      FROM wsr) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100)sql";

// q81: catalog returns concentration by state.  The returning customer /
// address columns come from the wave-4 fixture; the template's street /
// suite / location-type output columns (not in the fixture) are dropped
// and cr_return_amt_inc_tax maps to cr_return_amount, following q50.
inline constexpr std::string_view kQ81Sql = R"sql(WITH customer_total_return AS
(SELECT cr_returning_customer_sk AS ctr_customer_sk,
        ca_state AS ctr_state,
        SUM(cr_return_amount) AS ctr_total_return
 FROM catalog_returns, date_dim, customer_address
 WHERE cr_returned_date_sk = d_date_sk
   AND d_year = 1999
   AND cr_returning_addr_sk = ca_address_sk
 GROUP BY cr_returning_customer_sk, ca_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       ca_city,
       ca_county,
       ca_state,
       ca_zip,
       ca_country,
       ca_gmt_offset,
       ctr_total_return
FROM customer_total_return ctr1, customer_address, customer
WHERE ctr1.ctr_total_return >
      (SELECT AVG(ctr_total_return) * 1.2
       FROM customer_total_return ctr2
       WHERE ctr1.ctr_state = ctr2.ctr_state)
  AND ca_address_sk = c_current_addr_sk
  AND ca_state = 'TX'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id, c_salutation, c_first_name, c_last_name, ca_city,
         ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset,
         ctr_total_return
LIMIT 100)sql";

// q82: overstocked items sold in the 60-day window from 1998-04-27;
// verbatim except the manufacturer id pool, re-spelled to the fixture's
// dense 128 slot.
inline constexpr std::string_view kQ82Sql = R"sql(SELECT
  i_item_id,
  i_item_desc,
  i_current_price
FROM item, inventory, date_dim, store_sales
WHERE i_current_price BETWEEN 63 AND 63 + 30
  AND inv_item_sk = i_item_sk
  AND d_date_sk = inv_date_sk
  AND d_date BETWEEN CAST('1998-04-27' AS DATE)
                 AND (CAST('1998-04-27' AS DATE) + INTERVAL 60 day)
  AND i_manufact_id IN (128, 293, 427, 320)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
  AND ss_item_sk = i_item_sk
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id
LIMIT 100)sql";

// q83: per-channel return quantity shares.  The template restricts every
// branch to the weeks of three anchor dates; at fixture scale those weeks
// have no three-channel item overlap (returns are ~10% of sales), so the
// port aggregates the full return history and keeps the share shape.  The
// date_dim join leaves the bodies with it (it only carried the anchor
// filter) and would degenerate into an unconnected cross product.
inline constexpr std::string_view kQ83Sql = R"sql(WITH sr_items AS
(SELECT i_item_id item_id,
        SUM(sr_return_quantity) sr_item_qty
 FROM store_returns, item
 WHERE sr_item_sk = i_item_sk
 GROUP BY i_item_id),
cr_items AS
(SELECT i_item_id item_id,
        SUM(cr_return_quantity) cr_item_qty
 FROM catalog_returns, item
 WHERE cr_item_sk = i_item_sk
 GROUP BY i_item_id),
wr_items AS
(SELECT i_item_id item_id,
        SUM(wr_return_quantity) wr_item_qty
 FROM web_returns, item
 WHERE wr_item_sk = i_item_sk
 GROUP BY i_item_id)
SELECT sr_items.item_id,
       sr_item_qty,
       sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100
         sr_dev,
       cr_item_qty,
       cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100
         cr_dev,
       wr_item_qty,
       wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100
         wr_dev,
       (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 average
FROM sr_items, cr_items, wr_items
WHERE sr_items.item_id = cr_items.item_id
  AND sr_items.item_id = wr_items.item_id
ORDER BY sr_items.item_id, sr_item_qty
LIMIT 100)sql";

// q85: web returns by reason with marital/education gates.  The fixture
// reason table carries r_reason_text (q93 precedent); the education
// literals map to the generated pool ('College' / '2 yr Degree'), the
// refunded/returning demographics share the marital gate only (the two
// demo keys are independent draws, so the template's attribute-equality
// pair gate leaves no rows), and the price/profit bands sit on the
// fixture's actual value ranges.
inline constexpr std::string_view kQ85Sql = R"sql(SELECT
  SUBSTR(r_reason_text, 1, 20),
  AVG(ws_quantity),
  AVG(wr_refunded_cash),
  AVG(wr_fee)
FROM web_sales, web_returns, web_page, customer_demographics cd1,
     customer_demographics cd2, customer_address, date_dim, reason
WHERE ws_web_page_sk = wp_web_page_sk
  AND ws_item_sk = wr_item_sk
  AND ws_order_number = wr_order_number
  AND ws_sold_date_sk = d_date_sk
  AND d_year = 2001
  AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
  AND cd2.cd_demo_sk = wr_returning_cdemo_sk
  AND ca_address_sk = wr_refunded_addr_sk
  AND r_reason_sk = wr_reason_sk
  AND cd1.cd_marital_status = cd2.cd_marital_status
  AND ((cd1.cd_marital_status = 'W'
        AND cd1.cd_education_status = 'College'
        AND ws_sales_price BETWEEN 50.00 AND 300.00)
       OR (cd1.cd_marital_status = 'D'
           AND cd1.cd_education_status = '2 yr Degree'
           AND ws_sales_price BETWEEN 20.00 AND 300.00)
       OR (cd1.cd_marital_status = 'M'
           AND cd1.cd_education_status = 'Advanced Degree'
           AND ws_sales_price BETWEEN 50.00 AND 400.00))
  AND ((ca_country = 'United States'
        AND ca_state IN ('TN', 'TX', 'CA')
        AND ws_net_profit BETWEEN 200 AND 5000)
       OR (ca_country = 'United States'
           AND ca_state IN ('NY', 'OH', 'MI')
           AND ws_net_profit BETWEEN 300 AND 6000)
       OR (ca_country = 'United States'
           AND ca_state IN ('GA', 'IN')
           AND ws_net_profit BETWEEN 200 AND 4500))
GROUP BY r_reason_text
ORDER BY SUBSTR(r_reason_text, 1, 20), AVG(ws_quantity), AVG(wr_refunded_cash),
         AVG(wr_fee)
LIMIT 100)sql";

// q88: hourly store counts cross-joined over the eight time buckets.  The
// template's 'ese' store is re-spelled to the generated store_3.
inline constexpr std::string_view kQ88Sql = R"sql(SELECT * FROM
 (SELECT COUNT(*) h8_30_to_9
  FROM store_sales, household_demographics, time_dim, store
  WHERE ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 8
    AND time_dim.t_minute >= 30
    AND ((household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2)
         OR (household_demographics.hd_dep_count = 2
             AND household_demographics.hd_vehicle_count <= 2 + 2)
         OR (household_demographics.hd_dep_count = 3
             AND household_demographics.hd_vehicle_count <= 3 + 2))
    AND store.s_store_name = 'store_3') s1,
 (SELECT COUNT(*) h9_to_9_30
  FROM store_sales, household_demographics, time_dim, store
  WHERE ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 9
    AND time_dim.t_minute < 30
    AND ((household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2)
         OR (household_demographics.hd_dep_count = 2
             AND household_demographics.hd_vehicle_count <= 2 + 2)
         OR (household_demographics.hd_dep_count = 3
             AND household_demographics.hd_vehicle_count <= 3 + 2))
    AND store.s_store_name = 'store_3') s2,
 (SELECT COUNT(*) h9_30_to_10
  FROM store_sales, household_demographics, time_dim, store
  WHERE ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 9
    AND time_dim.t_minute >= 30
    AND ((household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2)
         OR (household_demographics.hd_dep_count = 2
             AND household_demographics.hd_vehicle_count <= 2 + 2)
         OR (household_demographics.hd_dep_count = 3
             AND household_demographics.hd_vehicle_count <= 3 + 2))
    AND store.s_store_name = 'store_3') s3,
 (SELECT COUNT(*) h10_to_10_30
  FROM store_sales, household_demographics, time_dim, store
  WHERE ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 10
    AND time_dim.t_minute < 30
    AND ((household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2)
         OR (household_demographics.hd_dep_count = 2
             AND household_demographics.hd_vehicle_count <= 2 + 2)
         OR (household_demographics.hd_dep_count = 3
             AND household_demographics.hd_vehicle_count <= 3 + 2))
    AND store.s_store_name = 'store_3') s4,
 (SELECT COUNT(*) h10_30_to_11
  FROM store_sales, household_demographics, time_dim, store
  WHERE ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 10
    AND time_dim.t_minute >= 30
    AND ((household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2)
         OR (household_demographics.hd_dep_count = 2
             AND household_demographics.hd_vehicle_count <= 2 + 2)
         OR (household_demographics.hd_dep_count = 3
             AND household_demographics.hd_vehicle_count <= 3 + 2))
    AND store.s_store_name = 'store_3') s5,
 (SELECT COUNT(*) h11_to_11_30
  FROM store_sales, household_demographics, time_dim, store
  WHERE ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 11
    AND time_dim.t_minute < 30
    AND ((household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2)
         OR (household_demographics.hd_dep_count = 2
             AND household_demographics.hd_vehicle_count <= 2 + 2)
         OR (household_demographics.hd_dep_count = 3
             AND household_demographics.hd_vehicle_count <= 3 + 2))
    AND store.s_store_name = 'store_3') s6,
 (SELECT COUNT(*) h11_30_to_12
  FROM store_sales, household_demographics, time_dim, store
  WHERE ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 11
    AND time_dim.t_minute >= 30
    AND ((household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2)
         OR (household_demographics.hd_dep_count = 2
             AND household_demographics.hd_vehicle_count <= 2 + 2)
         OR (household_demographics.hd_dep_count = 3
             AND household_demographics.hd_vehicle_count <= 3 + 2))
    AND store.s_store_name = 'store_3') s7,
 (SELECT COUNT(*) h12_to_12_30
  FROM store_sales, household_demographics, time_dim, store
  WHERE ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 12
    AND time_dim.t_minute < 30
    AND ((household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2)
         OR (household_demographics.hd_dep_count = 2
             AND household_demographics.hd_vehicle_count <= 2 + 2)
         OR (household_demographics.hd_dep_count = 3
             AND household_demographics.hd_vehicle_count <= 3 + 2))
    AND store.s_store_name = 'store_3') s8)sql";

// q90: AM/PM catalog-sales ratio over the web page char-count band; the
// template's hd_dep_count = 8 is outside the fixture pool (0..5), so the
// port pins 3.
inline constexpr std::string_view kQ90Sql = R"sql(SELECT
  CAST(amc AS FLOAT64) / CAST(pmc AS FLOAT64) am_pm_ratio
FROM (SELECT COUNT(*) amc
      FROM web_sales, household_demographics, time_dim, web_page
      WHERE ws_sold_time_sk = t_time_sk
        AND ws_ship_hdemo_sk = hd_demo_sk
        AND ws_web_page_sk = wp_web_page_sk
        AND t_hour BETWEEN 12 AND 12 + 1
        AND household_demographics.hd_dep_count = 3
        AND web_page.wp_char_count BETWEEN 5000 AND 5200) at1,
     (SELECT COUNT(*) pmc
      FROM web_sales, household_demographics, time_dim, web_page
      WHERE ws_sold_time_sk = t_time_sk
        AND ws_ship_hdemo_sk = hd_demo_sk
        AND ws_web_page_sk = wp_web_page_sk
        AND t_hour BETWEEN 20 AND 20 + 1
        AND household_demographics.hd_dep_count = 3
        AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER BY am_pm_ratio
LIMIT 100)sql";

// q91: call-center returns loss with the marital/education gates; the
// template's LIKE 'Unknown%' matches the generated lowercase pool and the
// gmt offset -7 band stays (half the addresses carry it).  The d_moy = 12
// slice is dropped (the December population is empty at fixture scale);
// the port filters on d_year = 1999 only.
inline constexpr std::string_view kQ91Sql = R"sql(SELECT
  cc_call_center_id Call_Center,
  cc_name Call_Center_Name,
  cc_manager Manager,
  SUM(cr_net_loss) Returns_Loss
FROM call_center, catalog_returns, date_dim, customer, customer_address,
     customer_demographics, household_demographics
WHERE cr_call_center_sk = cc_call_center_sk
  AND cr_returned_date_sk = d_date_sk
  AND cr_returning_customer_sk = c_customer_sk
  AND cd_demo_sk = c_current_cdemo_sk
  AND hd_demo_sk = c_current_hdemo_sk
  AND ca_address_sk = c_current_addr_sk
  AND d_year = 1999
  AND ((cd_marital_status = 'M' AND cd_education_status = 'Unknown')
       OR (cd_marital_status = 'W'
           AND cd_education_status = 'Advanced Degree'))
  AND hd_buy_potential LIKE 'unknown%'
  AND ca_gmt_offset = -7
GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status,
         cd_education_status
ORDER BY SUM(cr_net_loss) DESC)sql";

// q94: multi-warehouse orders shipped to one state's addresses without
// any return; web_company_name maps to the generated web_name band.
inline constexpr std::string_view kQ94Sql = R"sql(SELECT
  COUNT(DISTINCT ws_order_number) AS order_count,
  SUM(ws_ext_ship_cost) AS total_shipping_cost,
  SUM(ws_net_profit) AS total_net_profit
FROM web_sales ws1,
     date_dim,
     customer_address,
     web_site
WHERE d_date BETWEEN CAST('2000-03-01' AS DATE)
                 AND (CAST('2000-03-01' AS DATE) + INTERVAL 60 day)
  AND ws1.ws_ship_date_sk = d_date_sk
  AND ws1.ws_ship_addr_sk = ca_address_sk
  AND ca_state = 'TX'
  AND ws1.ws_web_site_sk = web_site_sk
  AND web_name = 'site_1'
  AND EXISTS (SELECT *
              FROM web_sales ws2
              WHERE ws1.ws_order_number = ws2.ws_order_number
                AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
  AND NOT EXISTS (SELECT *
                  FROM web_returns wr1
                  WHERE ws1.ws_order_number = wr1.wr_order_number)
ORDER BY COUNT(DISTINCT ws_order_number)
LIMIT 100)sql";

// q95: multi-warehouse orders partially returned, spelled over the ws_wh
// CTE like the template (IN subqueries over the self-join).
inline constexpr std::string_view kQ95Sql = R"sql(WITH ws_wh AS
(SELECT ws1.ws_order_number,
        ws1.ws_warehouse_sk wh1,
        ws2.ws_warehouse_sk wh2
 FROM web_sales ws1, web_sales ws2
 WHERE ws1.ws_order_number = ws2.ws_order_number
   AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
SELECT
  COUNT(DISTINCT ws_order_number) AS order_count,
  SUM(ws_ext_ship_cost) AS total_shipping_cost,
  SUM(ws_net_profit) AS total_net_profit
FROM web_sales ws1,
     date_dim,
     customer_address,
     web_site
WHERE d_date BETWEEN CAST('2000-04-01' AS DATE)
                 AND (CAST('2000-04-01' AS DATE) + INTERVAL 60 day)
  AND ws1.ws_ship_date_sk = d_date_sk
  AND ws1.ws_ship_addr_sk = ca_address_sk
  AND ca_state = 'IN'
  AND ws1.ws_web_site_sk = web_site_sk
  AND web_name = 'site_2'
  AND ws1.ws_order_number IN (SELECT ws_order_number FROM ws_wh)
  AND ws1.ws_order_number IN
      (SELECT wr_order_number
       FROM web_returns, ws_wh
       WHERE wr_order_number = ws_wh.ws_order_number)
ORDER BY COUNT(DISTINCT ws_order_number)
LIMIT 100)sql";

// q96: 3:30pm store sales count for one household shape; the template's
// 'ese' store is re-spelled to the generated store_3 and hd_dep_count = 7
// (outside the generated pool) pins 3 like q90.
inline constexpr std::string_view kQ96Sql = R"sql(SELECT COUNT(*)
FROM store_sales, household_demographics, time_dim, store
WHERE ss_sold_time_sk = time_dim.t_time_sk
  AND ss_hdemo_sk = household_demographics.hd_demo_sk
  AND ss_store_sk = s_store_sk
  AND time_dim.t_hour = 15
  AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 3
  AND store.s_store_name = 'store_3'
ORDER BY COUNT(*)
LIMIT 100)sql";

// q97: store-only / catalog-only / both customer-item coverage counts via
// a FULL OUTER JOIN of the two channel CTEs.
inline constexpr std::string_view kQ97Sql = R"sql(WITH ssci AS
(SELECT ss_customer_sk customer_sk,
        ss_item_sk item_sk
 FROM store_sales, date_dim
 WHERE ss_sold_date_sk = d_date_sk
   AND d_month_seq BETWEEN 1196 AND 1196 + 11
 GROUP BY ss_customer_sk, ss_item_sk),
csci AS
(SELECT cs_bill_customer_sk customer_sk,
        cs_item_sk item_sk
 FROM catalog_sales, date_dim
 WHERE cs_sold_date_sk = d_date_sk
   AND d_month_seq BETWEEN 1196 AND 1196 + 11
 GROUP BY cs_bill_customer_sk, cs_item_sk)
SELECT
  SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL
           THEN 1 ELSE 0 END) store_only,
  SUM(CASE WHEN ssci.customer_sk IS NULL AND csci.customer_sk IS NOT NULL
           THEN 1 ELSE 0 END) catalog_only,
  SUM(CASE WHEN ssci.customer_sk IS NOT NULL
            AND csci.customer_sk IS NOT NULL
           THEN 1 ELSE 0 END) store_and_catalog
FROM ssci
FULL OUTER JOIN csci
  ON (ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk)
LIMIT 100)sql";

// q99: catalog shipping-delay histogram by warehouse / ship mode / call
// center; backtick aliases renamed per the q62 port.
inline constexpr std::string_view kQ99Sql = R"sql(SELECT
  SUBSTR(w_warehouse_name, 1, 20),
  sm_type,
  cc_name,
  SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1 ELSE 0 END)
    AS days_0_30,
  SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 30)
            AND (cs_ship_date_sk - cs_sold_date_sk <= 60)
           THEN 1 ELSE 0 END) AS days_31_60,
  SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 60)
            AND (cs_ship_date_sk - cs_sold_date_sk <= 90)
           THEN 1 ELSE 0 END) AS days_61_90,
  SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 90)
            AND (cs_ship_date_sk - cs_sold_date_sk <= 120)
           THEN 1 ELSE 0 END) AS days_91_120,
  SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1 ELSE 0 END)
    AS over_120
FROM catalog_sales, warehouse, ship_mode, call_center, date_dim
WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
  AND cs_ship_date_sk = d_date_sk
  AND cs_warehouse_sk = w_warehouse_sk
  AND cs_ship_mode_sk = sm_ship_mode_sk
  AND cs_call_center_sk = cc_call_center_sk
GROUP BY SUBSTR(w_warehouse_name, 1, 20), sm_type, cc_name
ORDER BY SUBSTR(w_warehouse_name, 1, 20), sm_type, cc_name
LIMIT 100)sql";

inline constexpr std::array<TpcdsQuery, 94> kQueries = {{
    {.name = "q1", .sql = kQ1Sql},
    {.name = "q2", .sql = kQ2Sql},
    {.name = "q3", .sql = kQ3Sql},
    {.name = "q4",
     .sql = kQ4Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = true},
    {.name = "q6",
     .sql = kQ6Sql,
     .order_key_index = 1,
     .order_descending = false,
     .total_order = false},
    {.name = "q7", .sql = kQ7Sql},
    {.name = "q9", .sql = kQ9Sql},
    {.name = "q10", .sql = kQ10Sql},
    {.name = "q11", .sql = kQ11Sql},
    {.name = "q12", .sql = kQ12Sql},
    {.name = "q13", .sql = kQ13Sql},
    {.name = "q15", .sql = kQ15Sql},
    {.name = "q16", .sql = kQ16Sql},
    {.name = "q17", .sql = kQ17Sql},
    {.name = "q18", .sql = kQ18Sql},
    {.name = "q20", .sql = kQ20Sql},
    {.name = "q21", .sql = kQ21Sql},
    {.name = "q23a", .sql = kQ23aSql},
    {.name = "q26", .sql = kQ26Sql},
    {.name = "q27", .sql = kQ27Sql},
    {.name = "q28", .sql = kQ28Sql},
    {.name = "q31", .sql = kQ31Sql},
    {.name = "q33", .sql = kQ33Sql},
    {.name = "q42", .sql = kQ42Sql},
    {.name = "q50",
     .sql = kQ50Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q52", .sql = kQ52Sql},
    {.name = "q53",
     .sql = kQ53Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q55", .sql = kQ55Sql},
    {.name = "q59",
     .sql = kQ59Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q60", .sql = kQ60Sql},
    {.name = "q61", .sql = kQ61Sql},
    {.name = "q65",
     .sql = kQ65Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q67",
     .sql = kQ67Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q68", .sql = kQ68Sql},
    {.name = "q69",
     .sql = kQ69Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q70",
     .sql = kQ70Sql,
     .order_key_index = 3,
     .order_descending = true,
     .total_order = false},
    {.name = "q73",
     .sql = kQ73Sql,
     .order_key_index = 5,
     .order_descending = true,
     .total_order = false},
    {.name = "q75",
     .sql = kQ75Sql,
     .order_key_index = 8,
     .order_descending = false,
     .total_order = false},
    {.name = "q86",
     .sql = kQ86Sql,
     .order_key_index = 3,
     .order_descending = true,
     .total_order = false},
    {.name = "q87", .sql = kQ87Sql},
    {.name = "q89", .sql = kQ89Sql},
    {.name = "q19", .sql = kQ19Sql},
    {.name = "q40", .sql = kQ40Sql},
    {.name = "q46", .sql = kQ46Sql},
    {.name = "q47", .sql = kQ47Sql},
    {.name = "q22",
     .sql = kQ22Sql,
     .order_key_index = 4,
     .order_descending = false,
     .total_order = false},
    {.name = "q92", .sql = kQ92Sql},
    {.name = "q93", .sql = kQ93Sql},
    {.name = "q98",
     .sql = kQ98Sql,
     .order_key_index = 1,
     .order_descending = false,
     .total_order = false},
    {.name = "q43", .sql = kQ43Sql, .total_order = false},
    {.name = "q84", .sql = kQ84Sql, .total_order = false},
    {.name = "q35", .sql = kQ35Sql, .total_order = false},
    {.name = "q37", .sql = kQ37Sql, .total_order = false},
    {.name = "q48", .sql = kQ48Sql},
    {.name = "q79", .sql = kQ79Sql, .total_order = false},
    {.name = "q63", .sql = kQ63Sql, .total_order = false},
    {.name = "q34", .sql = kQ34Sql, .total_order = false},
    {.name = "q29", .sql = kQ29Sql, .total_order = false},
    {.name = "q76", .sql = kQ76Sql, .total_order = false},
    {.name = "q77", .sql = kQ77Sql, .total_order = false},
    {.name = "q25",
     .sql = kQ25Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q32", .sql = kQ32Sql},
    {.name = "q36",
     .sql = kQ36Sql,
     .order_key_index = 3,
     .order_descending = true,
     .total_order = false},
    {.name = "q38", .sql = kQ38Sql},
    {.name = "q39",
     .sql = kQ39Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q44",
     .sql = kQ44Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q45",
     .sql = kQ45Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q5",
     .sql = kQ5Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q30",
     .sql = kQ30Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q49",
     .sql = kQ49Sql,
     .order_key_index = 3,
     .order_descending = false,
     .total_order = false},
    {.name = "q51",
     .sql = kQ51Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q54",
     .sql = kQ54Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q56",
     .sql = kQ56Sql,
     .order_key_index = 1,
     .order_descending = false,
     .total_order = false},
    {.name = "q57",
     .sql = kQ57Sql,
     // The leading ORDER BY key (sum_sales - avg_monthly_sales) is exposed
     // as the trailing sales_diff output column; ordinal 3 (avg_monthly_
     // sales) only breaks ties.
     .order_key_index = 6,
     .order_descending = false,
     .total_order = false},
    {.name = "q58",
     .sql = kQ58Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q62",
     .sql = kQ62Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q71",
     .sql = kQ71Sql,
     .order_key_index = 4,
     .order_descending = true,
     .total_order = false},
    {.name = "q72",
     .sql = kQ72Sql,
     // The declared ordering leads with total_cnt DESC (output column 5);
     // i_item_desc / w_warehouse_name / d_week_seq only break ties.
     .order_key_index = 5,
     .order_descending = true,
     .total_order = false},
    {.name = "q41",
     .sql = kQ41Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q74",
     .sql = kQ74Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q78",
     .sql = kQ78Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q80",
     .sql = kQ80Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q81",
     .sql = kQ81Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q82",
     .sql = kQ82Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q83",
     .sql = kQ83Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q85",
     .sql = kQ85Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
    {.name = "q88", .sql = kQ88Sql},
    {.name = "q90", .sql = kQ90Sql},
    {.name = "q91",
     .sql = kQ91Sql,
     .order_key_index = 3,
     .order_descending = true,
     .total_order = false},
    {.name = "q94", .sql = kQ94Sql},
    {.name = "q95", .sql = kQ95Sql},
    {.name = "q96", .sql = kQ96Sql},
    {.name = "q97", .sql = kQ97Sql},
    {.name = "q99",
     .sql = kQ99Sql,
     .order_key_index = 0,
     .order_descending = false,
     .total_order = false},
}};

}  // namespace tinylamb::tpcds

#endif  // TINYLAMB_BENCHMARK_TPCDS_QUERIES_HPP
