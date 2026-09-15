# 第50章 PhysicalProperties — 要求の伝播と enforcement

- 状態: draft / 執筆基準リビジョン: `3880673` (2026-09-12)
- 状態: draft / 執筆基準リビジョン: `3880673` (2026-09-12)

本章では、前章の `OptimizeGroup` において下位ノードへ伝播されていた物理要求（`PhysicalProperties`）のデータ構造と、演算子ごとの要求変換規則（`RequiredChildProperties`）、および要求が満たされない場合のコスト強制（enforcement）の仕組みを解説する。対象は `plan/cascades.hpp` の `PhysicalProperties`、`plan/cascades.cpp` の `SearchEngine::RequiredChildProperties`、および `plan/optimizer.cpp` における初期要求の構築処理である。

## 50-1. `PhysicalProperties` — 実行結果に対する物理要求

`PhysicalProperties` は、プランの出力行に対して課される物理的制約やヒントを表現する（`plan/cascades.hpp`）。

```cpp
struct PhysicalProperties {
  bool require_row_position{false};
  bool wait_for_write_intent{true};
  std::vector<ColumnName> ordering;
  std::vector<bool> sort_ascending{};
  std::vector<std::optional<bool>> sort_nulls_first{};
  std::string collation{};
  size_t limit_hint{std::numeric_limits<size_t>::max()};
  AccessMethod access_method{AccessMethod::kAny};
  Distribution distribution{Distribution::kAny};
  bool distinct{false};
  std::vector<ColumnName> partition_by{};
  std::vector<ColumnName> bloom_filter_keys{};
  bool is_unique{false};
  JoinMultiplicity join_multiplicity{JoinMultiplicity::kUnknown};

  [[nodiscard]] std::string Key() const;
  bool operator==(const PhysicalProperties&) const = default;
};
```

各フィールドの定義と役割は以下のとおりである。

- **`require_row_position`**: ページ内における物理行位置（RID）を保持して出力することを要求する真偽値。`UPDATE` や `DELETE` 文の対象走査において、更新・削除対象の行を特定するために用いられる。行位置を保持できない演算子（ハッシュ結合など）はこの要求を満たせない。
- **`wait_for_write_intent`**: MVCC における書き込み意図（write-intent）の解消を待機するかどうかを指定する。スキャン時の同時実行制御動作を決定する。
- **`ordering`、`sort_ascending`、`sort_nulls_first`、`collation`**: 出力行に求められるソート順序を指定する。列名の配列、昇順/降順のフラグ列、NULL の配置規則、および文字列照合順序（collation）で構成される。
- **`limit_hint`**: 必要な最大行数の上限値（OFFSET ＋ LIMIT）。要求されたソート順序を提供する走査において、Top-K 探索によるコスト削減を見積もるために用いられる。上限がない場合は `size_t` の最大値が設定される。
- **`access_method`**: 利用可能なアクセスパスに対する選好ヒント（全表走査かインデックス走査の優先か）を示す。
- **`distribution`**: 分散実行環境におけるデータ配置（パーティショニング）の要求を表現するための予約フィールドである。
- **その他の補助属性**: 重複排除（`distinct`）、パーティション分割列（`partition_by`）、一意性（`is_unique`）など、実装規則が演算子を選択する際の判定基準として使用される。

`Key()` メソッドはこれらの属性を結合した文字列を生成する（`plan/cascades.cpp`）。前章で確認した `(Group, 物理要求)` のキャッシュキーにおいて、要求の一意性を識別するために用いられる。

## 50-2. 初期要求の生成 — クエリ根ノードの物理特性

物理要求の起点は、クエリ全体の構文解析結果に基づいて `plan/optimizer.cpp` の `Optimizer::Optimize` で構築される。

```cpp
  cascades::PhysicalProperties properties;
  properties.require_row_position = query.require_row_position_;
  properties.wait_for_write_intent = query.wait_for_write_intent_;
  properties.access_method = options.access_method;
  if (query.order_expressions_.size() == query.order_ascending_.size() &&
      !query.order_expressions_.empty()) {
    std::vector<ColumnName> ordering;
    bool all_columns = true;
    for (const Expression& order : query.order_expressions_) {
      if (order->Type() != TypeTag::kColumnValue) {
        all_columns = false;
        break;
      }
      ordering.push_back(order->AsColumnValue().GetColumnName());
    }
    if (all_columns) {
      properties.ordering = std::move(ordering);
    }
  }
  if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
    properties.limit_hint = query.limit_offset_ + query.limit_count_;
  }
```

- **単純列による ORDER BY のみ要求化される**: ソートキーが単一の列参照である場合のみ `properties.ordering` に設定される。計算式（`ORDER BY a + b` など）が含まれる場合は物理要求としては下位へ伝播させず、最上位のソート演算子（SortExecutor）に解決を委ねる。
- **`limit_hint` の計算**: `limit_offset_ + limit_count_` を算出することで、クエリ全体として読み出す必要のある最大行数を下位へ伝達する。

## 50-3. `RequiredChildProperties` — 親ノードから子ノードへの要求変換

親ノードに課された物理要求がそのまま子ノードへ伝播するとは限らない。演算子の性質に応じ、子ノードに伝達すべき要求が変換される（`plan/cascades.cpp` の `SearchEngine::RequiredChildProperties`）。

**1. データソース演算子**:
`kScan` や `kValues` など子ノードを持たない演算子は、空の配列を返す。

**2. 結合演算子**:
2 つの子を持つ結合ノード（`kJoin`、`kOuterJoin` 等）は、子ノードに対する順序要求を解除する。
```cpp
    case LogicalOperator::kJoin:
      if (expression.children.size() == 1) {
        return {required};
      }
      return {PhysicalProperties{}, PhysicalProperties{}};
```
結合処理は一般に入力行の順序を保存しないため、子ノードに対して親の順序を強制しない。ソート済みの入力を前提とする MergeJoin は、結合自身の実装規則内部でソート済みの子プランを選択することで実現される。

**3. 集約演算子**:
```cpp
    case LogicalOperator::kAggregation:
      return {PhysicalProperties{}};
```
集約操作は入力行を集約して再構成するため、入力側の整列順序および物理行位置は親の出力へ継承されない。したがって子ノードへの要求は初期化される。

**4. 単項のフィルタ・変形演算子**:
```cpp
    case LogicalOperator::kSelection:
      return {required};
```
`kSelection` や `kLimit` などの演算子は、行の相対順序を変更せずに通過させるため、親ノードの物理要求をそのまま子ノードへ伝達する。

**5. 集合演算子**:
```cpp
    case LogicalOperator::kUnion:
    case LogicalOperator::kUnionAll:
      return std::vector<PhysicalProperties>(expression.children.size(),
                                             PhysicalProperties{});
```
集合演算は入力間の順序を大域的に保存しないため、すべての子ノードに対する要求を空とする。

**6. ソート演算子（`kSort`）**:
```cpp
    case LogicalOperator::kSort: {
      PhysicalProperties child = required;
      child.ordering.clear();
      child.limit_hint = std::numeric_limits<size_t>::max();
      return {child};
    }
```
ソート演算子自身が出力行の整列順を確立するため、入力側に対する順序要求および `limit_hint` は解除される。ただし、行位置の保持要求（`require_row_position`）はそのまま子ノードへ伝播する。

**7. Top-N 演算子（`kTopN`）**:
```cpp
    case LogicalOperator::kTopN: {
      PhysicalProperties child;
      child.require_row_position = required.require_row_position;
      child.wait_for_write_intent = required.wait_for_write_intent;
      child.access_method = required.access_method;
      // ...(ソートキーに基づく child.ordering の設定)...
      const size_t needed =
          expression.limit_offset >
                  std::numeric_limits<size_t>::max() - expression.limit_count
              ? std::numeric_limits<size_t>::max()
              : expression.limit_count + expression.limit_offset;
      child.limit_hint = std::min(required.limit_hint, needed);
      return {child};
    }
```
Top-N 演算子は、自身の整列キーを子ノードに対する順序要求として提示する。下位のインデックス走査がその順序を自律的に満たせる場合、上位ヒープの構築が不要となり、推定コストが大幅に低減される。

**8. 射影演算子（`kProjection`）**:
```cpp
    case LogicalOperator::kProjection: {
      PhysicalProperties child = required;
      if (!child.ordering.empty()) {
        // target_list を逆引きし、出力列名を基底の入力列名へ変換する
      }
      return {child};
    }
```
親ノードから指定された順序要求は出力列名で記述されているため、射影リスト（`target_list`）を参照して入力側の列名へ変換した上で子ノードへ渡す。計算式などにより入力列へ一意に逆引きできないキーが存在する場合、順序要求の伝播を打ち切り、上位側での整列演算子の追加に委ねる。

## 50-4. 要求の執行（Enforcement）とコスト加算

下位ノードから得られた物理プランが親ノードの要求順序を満たしていない場合、オプティマイザはその不足をコストとして計上する（D6 規律）。

```cpp
      if (needs_ordering &&
          !alternative.plan->IsOrderedBy(context.query->order_expressions_,
                                         context.query->order_ascending_,
                                         context.query->order_nulls_first_)) {
        const double rows = std::max(alternative.estimated_rows, child_rows);
        cost += rows * std::log2(std::max(2.0, rows));
      }
```

- 各物理プランは自身が保証する順序を `IsOrderedBy` で判定する。要求を満たさない場合、実行エンジン側でソート演算子を補完することを想定し、ソートコスト $N \log_2 N$ を推定総コストに加算する。
- 実行エンジン側には最終的な安全網としてソート処理が配置されるため、コスト見積もりの精度に関わらず、クエリの出力結果における順序保証が破られることはない。

## 50-5. まとめ

- `PhysicalProperties` は、整列順序、行位置の保持、Top-K の上限行数、アクセス手法などの物理的制約・ヒントを保持する。
- `RequiredChildProperties` は演算子の意味論に基づき、親ノードの要求を子ノードへ変換・伝播させる。結合や集約では順序要求が解除され、TopN や射影では適切な変換を経て子へ要求が渡される。
- 要求を満たせない物理代替案に対しては、後続の補正処理（ソート）に要するコストが加算され、同一の基準でプラン選択が行われる。

次章（第 60 章）では、論理規則や実装規則を安全に追加・管理するための設計基準（D5 監査、事前条件ゲート、およびファジング検証）を解説する。

