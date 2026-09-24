/**
 * Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0.
 */
#ifndef TINYLAMB_BENCHMARK_TPCDS_FIXTURE_HPP
#define TINYLAMB_BENCHMARK_TPCDS_FIXTURE_HPP

// Self-contained TPC-DS subset fixture: schema DDL and a deterministic row
// generator for the tables touched by the queries in tpcds_queries.hpp.
// This is NOT the official dsdgen output; it produces the same shapes
// (star schema, TPC-DS value pools, d_date_sk/d_month_seq conventions) with
// fixed-seed randomness so benchmark and test runs are reproducible.
// DECIMAL columns are represented as DOUBLE, matching the engine's numeric
// surface; queries ported in tpcds_queries.hpp cast accordingly.

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb::tpcds {

// splitmix64: small, deterministic, well distributed.
struct Rng {
  uint64_t state{0};
  explicit Rng(uint64_t seed) : state(seed) {}
  uint64_t Next() {
    state += 0x9E3779B97F4A7C15ULL;
    uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
  }
  int64_t Below(int64_t bound) {
    return static_cast<int64_t>(Next() % static_cast<uint64_t>(bound));
  }
  int64_t Int(int64_t lo, int64_t hi) { return lo + Below(hi - lo + 1); }
  double Uniform(double lo, double hi) {
    return lo +
           (hi - lo) * (static_cast<double>(Next() >> 11) / 9007199254740992.0);
  }
  template <typename T>
  const T& Pick(const std::vector<T>& pool) {
    return pool[static_cast<size_t>(Below(static_cast<int64_t>(pool.size())))];
  }
};

struct ScaleConfig {
  int64_t factor{1};
  int64_t ItemRows() const { return 1200; }
  int64_t CustomerRows() const { return 2000; }
  int64_t AddressRows() const { return 3000; }
  int64_t StoreSalesRows() const { return 30000 * factor; }
  int64_t StoreReturnsRows() const { return 3000 * factor; }
  int64_t CatalogSalesRows() const { return 25000 * factor; }
  int64_t CatalogReturnsRows() const { return 2500 * factor; }
  int64_t WebSalesRows() const { return 25000 * factor; }
  int64_t WebReturnsRows() const { return 2500 * factor; }
  int64_t InventoryRows() const { return 20000 * factor; }
};

inline std::vector<std::string_view> Ddl() {
  return {
      // Dimension tables (columns follow the TPC-DS v2.x schema; only the
      // columns referenced by the ported queries plus key context columns).
      "CREATE TABLE date_dim (d_date_sk INT64, d_date_id STRING, d_date DATE, "
      "d_month_seq INT64, d_year INT64, d_dow INT64, d_moy INT64, "
      "d_dom INT64, d_qoy INT64, d_quarter_name STRING, d_week_seq INT64, "
      "d_day_name STRING);",
      "CREATE TABLE item (i_item_sk INT64, i_item_id STRING, i_item_desc "
      "STRING, i_current_price DOUBLE, i_wholesale_cost DOUBLE, i_brand_id "
      "INT64, i_brand STRING, i_class_id INT64, i_class STRING, "
      "i_category_id INT64, i_category STRING, i_manufact_id INT64, "
      "i_manufact STRING, i_manager_id INT64, i_size STRING, "
      "i_color STRING);",
      "CREATE TABLE store (s_store_sk INT64, s_store_id STRING, s_store_name "
      "STRING, s_number_employees INT64, s_floor_space INT64, s_county "
      "STRING, s_state STRING, s_city STRING, s_company_name STRING, "
      "s_company_id INT64, s_gmt_offset INT64, s_zip STRING);",
      "CREATE TABLE customer (c_customer_sk INT64, c_customer_id STRING, "
      "c_current_cdemo_sk INT64, c_current_hdemo_sk INT64, c_current_addr_sk "
      "INT64, c_birth_month INT64, c_birth_day INT64, c_birth_year INT64, "
      "c_gender STRING, c_salutation STRING, c_first_name STRING, "
      "c_last_name STRING, c_preferred_cust_flag STRING, c_email STRING, "
      "c_login STRING, c_email_address STRING);",
      "CREATE TABLE customer_address (ca_address_sk INT64, ca_address_id "
      "STRING, ca_city STRING, ca_county STRING, ca_state STRING, ca_zip "
      "STRING, ca_country STRING, ca_gmt_offset INT64);",
      "CREATE TABLE household_demographics (hd_demo_sk INT64, "
      "hd_income_band_sk INT64, hd_buy_potential STRING, hd_dep_count INT64, "
      "hd_vehicle_count INT64);",
      "CREATE TABLE customer_demographics (cd_demo_sk INT64, cd_gender "
      "STRING, cd_marital_status STRING, cd_education_status STRING, "
      "cd_purchase_estimate INT64, cd_credit_rating STRING, cd_dep_count "
      "INT64, cd_dep_employed_count INT64, cd_dep_college_count INT64);",
      "CREATE TABLE promotion (p_promo_sk INT64, p_promo_id STRING, "
      "p_channel_email STRING, p_channel_dmail STRING, p_channel_tv STRING, "
      "p_channel_event STRING);",
      "CREATE TABLE reason (r_reason_sk INT64, r_reason_id STRING, "
      "r_reason_text STRING);",
      "CREATE TABLE income_band (ib_income_band_sk INT64, ib_lower_bound "
      "INT64, ib_upper_bound INT64);",
      "CREATE TABLE warehouse (w_warehouse_sk INT64, w_warehouse_id STRING, "
      "w_warehouse_name STRING, w_city STRING, w_state STRING, "
      "w_gmt_offset INT64);",
      "CREATE TABLE call_center (cc_call_center_sk INT64, cc_call_center_id "
      "STRING, cc_county STRING, cc_state STRING);",
      // Fact tables.
      "CREATE TABLE store_sales (ss_sold_date_sk INT64, ss_item_sk INT64, "
      "ss_customer_sk INT64, ss_cdemo_sk INT64, ss_hdemo_sk INT64, "
      "ss_addr_sk INT64, ss_store_sk INT64, ss_promo_sk INT64, "
      "ss_ticket_number INT64, ss_quantity INT64, ss_wholesale_cost DOUBLE, "
      "ss_list_price DOUBLE, ss_sales_price DOUBLE, ss_ext_discount_amt "
      "DOUBLE, ss_ext_sales_price DOUBLE, ss_ext_wholesale_cost DOUBLE, "
      "ss_ext_list_price DOUBLE, ss_ext_tax DOUBLE, ss_coupon_amt DOUBLE, "
      "ss_net_paid DOUBLE, ss_net_paid_inc_tax DOUBLE, ss_net_profit DOUBLE);",
      "CREATE TABLE store_returns (sr_returned_date_sk INT64, sr_item_sk "
      "INT64, sr_customer_sk INT64, sr_cdemo_sk INT64, sr_hdemo_sk INT64, "
      "sr_addr_sk INT64, sr_store_sk INT64, sr_ticket_number INT64, "
      "sr_return_quantity INT64, sr_return_amt DOUBLE, sr_fee DOUBLE, "
      "sr_refunded_cash DOUBLE, sr_net_paid DOUBLE, sr_net_paid_inc_tax "
      "DOUBLE, sr_reason_sk INT64);",
      "CREATE TABLE catalog_sales (cs_sold_date_sk INT64, cs_item_sk INT64, "
      "cs_bill_customer_sk INT64, cs_bill_cdemo_sk INT64, cs_bill_hdemo_sk "
      "INT64, cs_bill_addr_sk INT64, cs_warehouse_sk INT64, cs_promo_sk "
      "INT64, cs_order_number INT64, cs_quantity INT64, cs_wholesale_cost "
      "DOUBLE, cs_list_price DOUBLE, cs_sales_price DOUBLE, "
      "cs_ext_discount_amt DOUBLE, cs_ext_sales_price DOUBLE, "
      "cs_ext_wholesale_cost DOUBLE, cs_ext_list_price DOUBLE, cs_ext_tax "
      "DOUBLE, cs_coupon_amt DOUBLE, cs_net_paid DOUBLE, "
      "cs_net_paid_inc_tax DOUBLE, cs_net_profit DOUBLE, "
      "cs_ship_customer_sk INT64, cs_ship_date_sk INT64, cs_ship_addr_sk "
      "INT64, cs_call_center_sk INT64, cs_ext_ship_cost DOUBLE);",
      "CREATE TABLE web_sales (ws_sold_date_sk INT64, ws_item_sk INT64, "
      "ws_bill_customer_sk INT64, ws_bill_cdemo_sk INT64, ws_bill_hdemo_sk "
      "INT64, ws_bill_addr_sk INT64, ws_ship_customer_sk INT64, ws_promo_sk "
      "INT64, ws_order_number INT64, ws_quantity INT64, ws_wholesale_cost "
      "DOUBLE, ws_list_price DOUBLE, ws_sales_price DOUBLE, "
      "ws_ext_discount_amt DOUBLE, ws_ext_sales_price DOUBLE, "
      "ws_ext_wholesale_cost DOUBLE, ws_ext_list_price DOUBLE, ws_ext_tax "
      "DOUBLE, ws_coupon_amt DOUBLE, ws_net_paid DOUBLE, "
      "ws_net_paid_inc_tax DOUBLE, ws_net_profit DOUBLE);",
      "CREATE TABLE inventory (inv_date_sk INT64, inv_item_sk INT64, "
      "inv_warehouse_sk INT64, inv_quantity_on_hand INT64);",
      // Return fact tables (column subset following the TPC-DS v2.x schema;
      // the returning_* / call-center / web-page slots are unused by the
      // ported queries).
      "CREATE TABLE catalog_returns (cr_returned_date_sk INT64, "
      "cr_item_sk INT64, cr_refunded_customer_sk INT64, cr_refunded_cdemo_sk "
      "INT64, cr_refunded_hdemo_sk INT64, cr_refunded_addr_sk INT64, "
      "cr_warehouse_sk INT64, cr_order_number INT64, cr_return_quantity "
      "INT64, cr_return_amount DOUBLE, cr_fee DOUBLE, cr_refunded_cash "
      "DOUBLE, cr_net_loss DOUBLE, cr_reason_sk INT64);",
      "CREATE TABLE web_returns (wr_returned_date_sk INT64, wr_item_sk "
      "INT64, wr_refunded_customer_sk INT64, wr_refunded_cdemo_sk INT64, "
      "wr_refunded_hdemo_sk INT64, wr_refunded_addr_sk INT64, "
      "wr_order_number INT64, wr_return_quantity INT64, wr_return_amount "
      "DOUBLE, wr_fee DOUBLE, wr_refunded_cash DOUBLE, wr_net_loss DOUBLE, "
      "wr_reason_sk INT64);",
  };
}

// days_since_1900 -> the TPC-DS d_date_sk convention (1900-01-01 = 2415022).
inline int64_t DateSkFromCivil(int64_t year, int64_t month, int64_t day) {
  // Count days from 1900-01-01 with the civil-calendar algorithm (Howard
  // Hattan's era-based day count, same shape as std::chrono).
  auto days_from_civil = [](int64_t y, int64_t m, int64_t d) -> int64_t {
    y -= m <= 2 ? 1 : 0;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const int64_t yoe = y - era * 400;
    const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
  };
  // 25567 anchors the civil count at 1900-01-01, 2415022 is the TPC-DS
  // surrogate-key base for that date.
  return 2415022 + days_from_civil(year, month, day) + 25567;
}

struct GeneratedData {
  std::vector<Row> date_dim;
  std::vector<Row> item;
  std::vector<Row> store;
  std::vector<Row> customer;
  std::vector<Row> customer_address;
  std::vector<Row> household_demographics;
  std::vector<Row> customer_demographics;
  std::vector<Row> promotion;
  std::vector<Row> reason;
  std::vector<Row> income_band;
  std::vector<Row> warehouse;
  std::vector<Row> call_center;
  std::vector<Row> store_sales;
  std::vector<Row> store_returns;
  std::vector<Row> catalog_sales;
  std::vector<Row> web_sales;
  std::vector<Row> catalog_returns;
  std::vector<Row> web_returns;
  std::vector<Row> inventory;
};

inline GeneratedData Generate(int64_t scale) {
  ScaleConfig config{.factor = scale};
  Rng rng(20260922ULL);
  // Independent stream for the columns added after the fixture was first
  // pinned (returns tables, cs_ship_customer_sk, c_login/c_email_address,
  // d_week_seq/d_day_name): reusing `rng` would shift every later draw and
  // silently change the data the existing tests and references pin.
  Rng aux_rng(424242ULL);

  static const std::vector<std::string_view> kCategories = {
      "Men",    "Women", "Children", "Jewelry",
      "Sports", "Books", "Home",     "Electronics"};
  static const std::vector<std::vector<std::string_view>> kClassesByCategory = {
      {"shirts", "pants", "suits"},       // Men
      {"dresses", "shoes", "blouses"},    // Women
      {"toys", "games", "school"},        // Children
      {"watches", "rings", "necklaces"},  // Jewelry
      {"football", "tennis", "golf"},     // Sports
      {"travel", "fiction", "children"},  // Books
      {"furniture", "kitchen", "decor"},  // Home
      {"computers", "stereo", "tv"},      // Electronics
  };
  static const std::vector<std::string_view> kSizes = {"XS", "S",  "M",
                                                       "L",  "XL", "XXL"};
  static const std::vector<std::string_view> kColors = {
      "red", "green", "blue", "black", "white", "yellow", "purple"};
  static const std::vector<std::string_view> kStates = {"TN", "TX", "CA", "NY",
                                                        "OH", "MI", "GA", "IN"};
  static const std::vector<std::string_view> kCounties = {
      "Williamson County", "Franklin Parish", "Bronx County", "Orange County",
      "Fairfield County", "Ziebach County",
      // q10's ca_county IN-list members; without them the query returns
      // no rows over this fixture.
      "Rush County", "Toole County", "Jefferson County", "Dona Ana County",
      "La Porte County"};
  static const std::vector<std::string_view> kCities = {
      "Midway",      "Fairview",  "Georgetown",
      "Springfield", "Riverside", "Franklin"};
  static const std::vector<std::string_view> kSalutations = {"Mr.", "Mrs.",
                                                             "Ms.", "Dr."};
  static const std::vector<std::string_view> kFirstNames = {
      "James",  "Mary",     "John",    "Patricia",
      "Robert", "Jennifer", "Michael", "Linda"};
  static const std::vector<std::string_view> kLastNames = {
      "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"};
  static const std::vector<std::string_view> kBuyPotential = {
      "unknown", "<1000", "1000-5000", "5000-10000", ">10000"};
  static const std::vector<std::string_view> kEducations = {
      "College", "2 yr Degree", "4 yr Degree", "Advanced Degree", "Unknown"};
  static const std::vector<std::string_view> kCreditRatings = {"Good", "Bad"};
  static const std::vector<std::string_view> kYN = {"Y", "N"};
  static const std::vector<std::string_view> kReasons = {
      "Did not like the color", "Wrong size",
      "Received a duplicate",   "Not needed",
      "Price too high",         "Defective product",
      "Changed mind",           "Found better price elsewhere",
      "Gift did not fit",       "Arrived too late",
      "Damaged in shipping",    "Wrong item delivered"};

  GeneratedData data;

  // date_dim: 1998-01-01 .. 2003-12-31 with d_month_seq matching the TPC-DS
  // convention (Jan 1900 = 0, so Jan 2000 = 1200).  d_week_seq numbers
  // 7-day buckets from the start of the dimension; d_day_name follows the
  // d_dow encoding (1998-01-01 is a Thursday, d_dow = 4).
  {
    static const std::vector<std::string_view> kDayNames = {
        "Sunday",   "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday"};
    const int64_t sk_epoch = DateSkFromCivil(1998, 1, 1);
    int64_t month_seq = (1998 - 1900) * 12;
    for (int64_t year = 1998; year <= 2003; ++year) {
      for (int64_t month = 1; month <= 12; ++month, ++month_seq) {
        static const int64_t kDaysInMonth[] = {31, 28, 31, 30, 31, 30,
                                               31, 31, 30, 31, 30, 31};
        const bool leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
        const int64_t days = month == 2 && leap ? 29 : kDaysInMonth[month - 1];
        for (int64_t day = 1; day <= days; ++day) {
          const int64_t sk = DateSkFromCivil(year, month, day);
          const int64_t dow = (sk + 5) % 7;  // 1998-01-01 is a Thursday(4)
          char id[32];
          std::snprintf(id, sizeof(id), "AAAAAA%08ld",
                        static_cast<long>(sk % 100000000));
          char date_text[64];
          std::snprintf(date_text, sizeof(date_text), "%04ld-%02ld-%02ld",
                        static_cast<long>(year), static_cast<long>(month),
                        static_cast<long>(day));
          char quarter[48];
          std::snprintf(quarter, sizeof(quarter), "%04ldQ%ld",
                        static_cast<long>(year),
                        static_cast<long>((month - 1) / 3 + 1));
          data.date_dim.emplace_back(Row(std::vector<Value>{
              Value(sk), Value(std::string(id)), Value::Date(date_text),
              Value(month_seq), Value(year), Value(dow), Value(month),
              Value(day), Value((month - 1) / 3 + 1),
              Value(std::string(quarter)), Value((sk - sk_epoch) / 7 + 1),
              Value(std::string(kDayNames[static_cast<size_t>(dow)]))}));
        }
      }
    }
  }

  // item
  for (int64_t i = 1; i <= config.ItemRows(); ++i) {
    const int64_t category_index = rng.Below(8);
    const auto& category = kCategories[static_cast<size_t>(category_index)];
    const auto& klass =
        rng.Pick(kClassesByCategory[static_cast<size_t>(category_index)]);
    const int64_t brand_id = static_cast<int64_t>(rng.Next() % 30 + 1);
    // q3 filters i_manufact_id = 128; keep a quarter of items there.
    const int64_t manufact_id = rng.Below(4) == 0 ? 128 : rng.Below(1000) + 1;
    char id[20];
    std::snprintf(id, sizeof(id), "AAAAAAAA%08ld", static_cast<long>(i));
    // A cheap tail keeps q21's i_current_price BETWEEN 0.99 AND 1.49 hit;
    // the tracked items (1..60, densely inventoried below) are all cheap.
    const bool cheap_tail = rng.Below(6) == 0;
    const double list_price = (i <= 60 || cheap_tail) ? rng.Uniform(1.0, 3.0)
                                                      : rng.Uniform(1.0, 400.0);
    data.item.emplace_back(Row(std::vector<Value>{
        Value(i), Value(std::string(id)),
        Value("lyricite " + std::string(klass) + " " + std::string(category)),
        Value(list_price), Value(list_price * 0.55), Value(brand_id),
        Value("brand#" + std::to_string(brand_id)),
        Value(static_cast<int64_t>(rng.Next() % 10 + 1)),
        Value(std::string(klass)), Value(category_index + 1),
        Value(std::string(category)), Value(manufact_id),
        Value("manufact#" + std::to_string(manufact_id)),
        Value(static_cast<int64_t>(rng.Next() % 64 +
                                   1)),  // q42 picks 1, q55 picks 28
        Value(std::string(rng.Pick(kSizes))),
        Value(std::string(rng.Pick(kColors)))}));
  }

  // store (12 fixed-size)
  for (int64_t i = 1; i <= 12; ++i) {
    const int64_t company_id = (i - 1) / 6 + 1;
    data.store.emplace_back(Row(std::vector<Value>{
        Value(i), Value("store#" + std::to_string(i)),
        Value("store_" + std::to_string(i)), Value(rng.Int(50, 400)),
        Value(rng.Int(1000, 20000)), Value(std::string(rng.Pick(kCounties))),
        Value(std::string(kStates[static_cast<size_t>(
            (i - 1) % static_cast<int64_t>(kStates.size()))])),
        Value(std::string(rng.Pick(kCities))),
        Value("company" + std::to_string(company_id)), Value(company_id),
        Value(rng.Below(2) == 0 ? -5 : -7), Value("zip" + std::to_string(i))}));
  }

  // customer_address
  for (int64_t i = 1; i <= config.AddressRows(); ++i) {
    char id[16];
    std::snprintf(id, sizeof(id), "AAAAAAA%08ld", static_cast<long>(i));
    char zip[12];
    std::snprintf(zip, sizeof(zip), "%05ld",
                  static_cast<long>(rng.Below(100000)));
    data.customer_address.emplace_back(Row(std::vector<Value>{
        Value(i), Value(std::string(id)), Value(std::string(rng.Pick(kCities))),
        Value(std::string(rng.Pick(kCounties))),
        Value(std::string(rng.Pick(kStates))), Value(std::string(zip)),
        Value(std::string("United States")),
        Value(rng.Below(2) == 0 ? -5 : -7)}));
  }

  // customer_demographics: every 2x5x5 gender x marital x education combo
  // in two estimate variants, keyed 1..200 (fact rows reference 1..200).
  {
    int64_t sk = 1;
    for (int64_t variant = 0; variant < 2 && sk <= 200; ++variant) {
      for (const std::string_view gender : {"M", "F"}) {
        for (const std::string_view marital : {"S", "M", "W", "D", "U"}) {
          for (const std::string_view education : kEducations) {
            data.customer_demographics.emplace_back(Row(std::vector<Value>{
                Value(sk), Value(std::string(gender)),
                Value(std::string(marital)), Value(std::string(education)),
                Value(rng.Int(1000, 20000)),
                Value(std::string(rng.Pick(kCreditRatings))),
                Value(rng.Int(0, 6)), Value(rng.Int(0, 7)),
                Value(rng.Int(0, 5))}));
            ++sk;
          }
        }
      }
    }
  }

  // household_demographics (100)
  for (int64_t i = 1; i <= 100; ++i) {
    data.household_demographics.emplace_back(
        Row(std::vector<Value>{Value(i), Value(rng.Int(1, 20)),
                               Value(std::string(rng.Pick(kBuyPotential))),
                               Value(rng.Int(0, 5)), Value(rng.Int(0, 4))}));
  }

  // customer
  for (int64_t i = 1; i <= config.CustomerRows(); ++i) {
    char id[32];
    std::snprintf(id, sizeof(id), "AAAAAAAA%08ld", static_cast<long>(i * 17));
    data.customer.emplace_back(Row(std::vector<Value>{
        Value(i), Value(std::string(id)), Value(rng.Int(1, 200)),
        Value(rng.Int(1, 100)), Value(rng.Int(1, config.AddressRows())),
        Value(rng.Int(1, 12)), Value(rng.Int(1, 28)),
        Value(rng.Int(1930, 2000)),
        Value(std::string(rng.Below(2) == 0 ? "M" : "F")),
        Value(std::string(rng.Pick(kSalutations))),
        Value(std::string(rng.Pick(kFirstNames))),
        Value(std::string(rng.Pick(kLastNames))),
        Value(std::string(rng.Below(4) == 0 ? "Y" : "N")),
        Value(std::string(rng.Pick(kFirstNames)) + "." +
              std::string(rng.Pick(kLastNames)) + "@" +
              std::to_string(rng.Below(100000)) + ".com"),
        Value("login" + std::to_string(i)),
        Value(std::string(aux_rng.Pick(kFirstNames)) + "." +
              std::string(aux_rng.Pick(kLastNames)) + "@" +
              std::to_string(aux_rng.Below(100000)) + ".org")}));
  }

  // promotion (50)
  for (int64_t i = 1; i <= 50; ++i) {
    data.promotion.emplace_back(Row(std::vector<Value>{
        Value(i), Value("promo#" + std::to_string(i)),
        Value(std::string(rng.Pick(kYN))), Value(std::string(rng.Pick(kYN))),
        Value(std::string(rng.Pick(kYN))), Value(std::string(rng.Pick(kYN)))}));
  }

  // reason
  for (int64_t i = 1; i <= static_cast<int64_t>(kReasons.size()); ++i) {
    char id[40];
    std::snprintf(id, sizeof(id), "AAAAAAAABAAAAAAA%02ld",
                  static_cast<long>(i));
    data.reason.emplace_back(Row(std::vector<Value>{
        Value(i), Value(std::string(id)),
        Value(std::string(kReasons[static_cast<size_t>(i - 1)]))}));
  }

  // income_band (20 fixed)
  for (int64_t i = 1; i <= 20; ++i) {
    data.income_band.emplace_back(Row(
        std::vector<Value>{Value(i), Value((i - 1) * 5000), Value(i * 5000)}));
  }

  // warehouse (5)
  for (int64_t i = 1; i <= 5; ++i) {
    data.warehouse.emplace_back(Row(
        std::vector<Value>{Value(i), Value("warehouse#" + std::to_string(i)),
                           Value("warehouse_" + std::to_string(i)),
                           Value(std::string(rng.Pick(kCities))),
                           Value(std::string(rng.Pick(kStates))),
                           Value(rng.Below(2) == 0 ? -5 : -7)}));
  }

  // call_center (6, matching the cs_call_center_sk key range; q16 filters
  // cc_county to Williamson County so a third of the centers sit there).
  // All draws come from the aux stream: this table was added after the
  // fixture's pinned data was generated.
  for (int64_t i = 1; i <= 6; ++i) {
    data.call_center.emplace_back(Row(std::vector<Value>{
        Value(i), Value("call_center#" + std::to_string(i)),
        Value(std::string(i % 3 == 0 ? "Williamson County"
                                     : aux_rng.Pick(kCounties))),
        Value(std::string(aux_rng.Pick(kStates)))}));
  }

  // Fact rows share one generator: sale dates 1998-01-01..2002-12-31,
  // prices derived from random cost; `seller_sk` is the store / warehouse /
  // ship-customer slot and `ticket` the ticket / order number slot.
  const auto make_sale_row =
      [&](int64_t date_sk, int64_t item_sk, int64_t customer_sk,
          int64_t cdemo_sk, int64_t hdemo_sk, int64_t addr_sk,
          int64_t seller_sk, int64_t promo_sk, int64_t ticket) {
        const int64_t quantity = rng.Int(1, 20);
        const double wholesale = rng.Uniform(1.0, 200.0);
        const double list_price = wholesale * rng.Uniform(1.2, 2.5);
        const double discount =
            rng.Uniform(0.0, 0.6) * static_cast<double>(quantity);
        const double sales_price = list_price * rng.Uniform(0.7, 1.05);
        const double ext_sales = sales_price * static_cast<double>(quantity);
        const double ext_list = list_price * static_cast<double>(quantity);
        const double ext_wholesale = wholesale * static_cast<double>(quantity);
        const double coupon = rng.Below(8) == 0 ? rng.Uniform(0.0, 50.0) : 0.0;
        const double tax = ext_sales * 0.08;
        const double net_paid = ext_sales - discount;
        return std::vector<Value>{
            Value(date_sk),        Value(item_sk),
            Value(customer_sk),    Value(cdemo_sk),
            Value(hdemo_sk),       Value(addr_sk),
            Value(seller_sk),      Value(promo_sk),
            Value(ticket),         Value(quantity),
            Value(wholesale),      Value(list_price),
            Value(sales_price),    Value(discount),
            Value(ext_sales),      Value(ext_wholesale),
            Value(ext_list),       Value(tax),
            Value(coupon),         Value(net_paid),
            Value(net_paid + tax), Value(net_paid - ext_wholesale * 0.1)};
      };

  const int64_t sk_min = DateSkFromCivil(1998, 1, 1);
  const int64_t sk_span = DateSkFromCivil(2002, 12, 31) - sk_min + 1;
  for (int64_t i = 1; i <= config.StoreSalesRows(); ++i) {
    data.store_sales.push_back(Row(std::vector<Value>(make_sale_row(
        sk_min + rng.Below(sk_span), rng.Int(1, config.ItemRows()),
        rng.Int(1, config.CustomerRows()), rng.Int(1, 200), rng.Int(1, 100),
        rng.Int(1, config.AddressRows()), rng.Int(1, 12), rng.Int(1, 50),
        rng.Int(1, 5000)))));
  }
  // store_returns: every k-th store_sales row becomes a return, keeping the
  // (item, customer, ticket) join keys so sr⋈ss predicates have matches.
  for (size_t i = 0; i < data.store_sales.size() &&
                     i < static_cast<size_t>(config.StoreReturnsRows());
       ++i) {
    const std::vector<Value>& sale =
        data.store_sales[i * data.store_sales.size() /
                         static_cast<size_t>(config.StoreReturnsRows())]
            .values_;
    const int64_t returned_sk = sale[0].value.int_value + rng.Int(5, 60);
    data.store_returns.emplace_back(Row(std::vector<Value>{
        Value(returned_sk), sale[1], sale[2], sale[3], sale[4], sale[5],
        sale[6], sale[8], Value(sale[9].value.int_value),
        Value(sale[14].value.double_value),        // sr_return_amt
        Value(rng.Uniform(1.0, 10.0)),             // sr_fee
        Value(sale[19].value.double_value),        // sr_refunded_cash
        Value(sale[19].value.double_value),        // sr_net_paid
        Value(sale[19].value.double_value + 1.0),  // sr_net_paid_inc_tax
        Value(rng.Int(1, static_cast<int64_t>(kReasons.size())))}));
  }

  for (int64_t i = 1; i <= config.CatalogSalesRows(); ++i) {
    // seller slot carries the warehouse key; ticket slot the order number.
    // Half the rows reuse a store_sale's (item, customer) pair so the
    // sr ⋈ cs customer/item join in q17 has guaranteed overlap.
    const bool reuse = !data.store_sales.empty() && rng.Below(2) == 0;
    const Row& donor = data.store_sales[static_cast<size_t>(
        rng.Below(static_cast<int64_t>(data.store_sales.size())))];
    std::vector<Value> sale = make_sale_row(
        sk_min + rng.Below(sk_span),
        reuse ? donor.values_[1].value.int_value
              : rng.Int(1, config.ItemRows()),
        reuse ? donor.values_[2].value.int_value
              : rng.Int(1, config.CustomerRows()),
        rng.Int(1, 200), rng.Int(1, 100), rng.Int(1, config.AddressRows()),
        rng.Int(1, 5), rng.Int(1, 50), rng.Int(1, 100000));
    // Trailing ship-slot columns (cs_ship_customer_sk / cs_ship_date_sk /
    // cs_ship_addr_sk / cs_call_center_sk / cs_ext_ship_cost).  q16 compares
    // warehouse sks across catalog_sales rows sharing an order number, so
    // ship dates keep a separate draw from the sale date.
    sale.push_back(Value(aux_rng.Int(1, config.CustomerRows())));
    sale.push_back(Value(aux_rng.Int(0, 30) + sale[0].value.int_value));
    sale.push_back(Value(aux_rng.Int(1, config.AddressRows())));
    sale.push_back(Value(aux_rng.Int(1, 6)));
    sale.push_back(Value(aux_rng.Uniform(100.0, 2000.0)));
    data.catalog_sales.push_back(Row(std::move(sale)));
  }
  for (int64_t i = 1; i <= config.WebSalesRows(); ++i) {
    // seller slot carries the ship-customer key; ticket slot the order number.
    data.web_sales.push_back(Row(std::vector<Value>(make_sale_row(
        sk_min + rng.Below(sk_span), rng.Int(1, config.ItemRows()),
        rng.Int(1, config.CustomerRows()), rng.Int(1, 200), rng.Int(1, 100),
        rng.Int(1, config.AddressRows()), rng.Int(1, config.CustomerRows()),
        rng.Int(1, 50), i))));
  }
  // catalog_returns / web_returns: every k-th sale row becomes a return,
  // keeping the (item, customer, order) join keys so wr⋈ws / cr⋈cs
  // predicates have matches, mirroring the store_returns derivation.
  for (size_t i = 0; i < data.catalog_sales.size() &&
                     i < static_cast<size_t>(config.CatalogReturnsRows());
       ++i) {
    const std::vector<Value>& sale =
        data.catalog_sales[i * data.catalog_sales.size() /
                           static_cast<size_t>(config.CatalogReturnsRows())]
            .values_;
    const int64_t returned_sk = sale[0].value.int_value + aux_rng.Int(5, 60);
    data.catalog_returns.emplace_back(Row(std::vector<Value>{
        Value(returned_sk), sale[1], sale[2], sale[3], sale[4], sale[5],
        sale[6], sale[8], Value(sale[9].value.int_value),
        Value(sale[14].value.double_value),        // cr_return_amount
        Value(aux_rng.Uniform(1.0, 10.0)),         // cr_fee
        Value(sale[19].value.double_value),        // cr_refunded_cash
        Value(sale[14].value.double_value * 0.1),  // cr_net_loss
        Value(aux_rng.Int(1, static_cast<int64_t>(kReasons.size())))}));
  }
  for (size_t i = 0; i < data.web_sales.size() &&
                     i < static_cast<size_t>(config.WebReturnsRows());
       ++i) {
    const std::vector<Value>& sale =
        data.web_sales[i * data.web_sales.size() /
                       static_cast<size_t>(config.WebReturnsRows())]
            .values_;
    const int64_t returned_sk = sale[0].value.int_value + aux_rng.Int(5, 60);
    data.web_returns.emplace_back(Row(std::vector<Value>{
        Value(returned_sk), sale[1], sale[2], sale[3], sale[4], sale[5],
        sale[8], Value(sale[9].value.int_value),
        Value(sale[14].value.double_value),        // wr_return_amount
        Value(aux_rng.Uniform(1.0, 10.0)),         // wr_fee
        Value(sale[19].value.double_value),        // wr_refunded_cash
        Value(sale[14].value.double_value * 0.1),  // wr_net_loss
        Value(aux_rng.Int(1, static_cast<int64_t>(kReasons.size())))}));
  }
  // Dense weekly inventory for the tracked items (see item generation) plus
  // sparse random rows elsewhere, so q21 sees both sides of its date split.
  for (int64_t day = 0; day < sk_span; day += 14) {
    for (int64_t warehouse = 1; warehouse <= 5; ++warehouse) {
      for (int64_t item_sk = 1; item_sk <= 60; ++item_sk) {
        data.inventory.emplace_back(
            Row(std::vector<Value>{Value(sk_min + day), Value(item_sk),
                                   Value(warehouse), Value(rng.Int(0, 500))}));
      }
    }
  }
  for (int64_t i = 1; i <= config.InventoryRows(); ++i) {
    data.inventory.emplace_back(
        Row(std::vector<Value>{Value(sk_min + rng.Below(sk_span)),
                               Value(rng.Int(61, config.ItemRows())),
                               Value(rng.Int(1, 5)), Value(rng.Int(0, 500))}));
  }
  return data;
}

// Table name -> row container, in DDL order for the loader.
inline const std::vector<Row>& RowsOf(const GeneratedData& data,
                                      std::string_view table) {
  if (table == "date_dim") return data.date_dim;
  if (table == "item") return data.item;
  if (table == "store") return data.store;
  if (table == "customer") return data.customer;
  if (table == "customer_address") return data.customer_address;
  if (table == "household_demographics") return data.household_demographics;
  if (table == "customer_demographics") return data.customer_demographics;
  if (table == "promotion") return data.promotion;
  if (table == "reason") return data.reason;
  if (table == "income_band") return data.income_band;
  if (table == "warehouse") return data.warehouse;
  if (table == "call_center") return data.call_center;
  if (table == "store_sales") return data.store_sales;
  if (table == "store_returns") return data.store_returns;
  if (table == "catalog_sales") return data.catalog_sales;
  if (table == "web_sales") return data.web_sales;
  if (table == "catalog_returns") return data.catalog_returns;
  if (table == "web_returns") return data.web_returns;
  if (table == "inventory") return data.inventory;
  static const std::vector<Row> kEmpty;
  return kEmpty;
}

}  // namespace tinylamb::tpcds

#endif  // TINYLAMB_BENCHMARK_TPCDS_FIXTURE_HPP
