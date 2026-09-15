# sort_merge_of_compatible_orders

- 状態: draft / 執筆基準リビジョン: `3880673` (2026-09-12)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `"sort_merge_of_compatible_orders"`）

## 概要

`sort_merge_of_compatible_orders` は、入れ子になった `Sort(Sort(X))` において、2 つのソートキーリストが互換な接頭辞関係にある場合に、それらを単一のソート演算子へ統合する論理等価 Rule である。

内側の順序が外側の要求する順序接頭辞を満たしている場合は内側のソートを残し、外側のキーが内側のキーを接頭辞として包含している場合は外側のキーで直接ソートを行う。これにより冗長な多重ソートを排除する。

## 変換前後の関係

内側が `(a ASC, b ASC)`、外側が `(a ASC)` の例:

```mermaid
graph TD
  subgraph before["変換前: Sort(Sort(items))"]
    O1["Sort a ASC"] --> I1["Sort a ASC, b ASC"] --> X1["Scan items"]
  end
  subgraph after["変換後: 内側のソート（強い順序）を 1 個だけに統合"]
    O2["Sort a ASC, b ASC（外側グループの代替）"] --> X2["Scan items"]
  end
```

## 適用条件

パターンは `Sort(Sort(Any(), "inner"))` であり、対象演算子は `LogicalOperator::kSort` である。以下のガード条件をすべて満たす場合に発火する。

1. 内側の式が `LogicalOperator::kSort` であり、子ノード数が 1 で、その子が現在のグループ自身でないこと。
2. 内側および外側の両方の式において、`target_list.size() == sort_ascending.size()` が成立していること（キーと昇降フラグの整合性）。
3. 各ソートキーの比較において、式の文字列表現（`ToString()`）、昇順・降順指定、および `sort_nulls_first` の設定が完全に一致すること。
4. 外側のキー列が内側のキー列の接頭辞であるか、あるいは内側のキー列が外側のキー列の接頭辞であること（接頭辞互換性）。

```cpp
            const auto same_key = [](const LogicalExpression& a, size_t ai,
                                     const LogicalExpression& b, size_t bi) {
              const std::optional<bool> a_null = ai < a.sort_nulls_first.size()
                                                     ? a.sort_nulls_first[ai]
                                                     : std::nullopt;
              const std::optional<bool> b_null = bi < b.sort_nulls_first.size()
                                                     ? b.sort_nulls_first[bi]
                                                     : std::nullopt;
              return a.target_list[ai].expression->ToString() ==
                         b.target_list[bi].expression->ToString() &&
                     a.sort_ascending[ai] == b.sort_ascending[bi] &&
                     a_null == b_null;
            };
```

昇降フラグの不一致、NULLS FIRST / LAST の相違、または接頭辞関係にないキー順序（例: `(a)` と `(b, a)`）の場合は発火しない。

## 意味論的根拠と物理実行の契約

多重ソートを 1 つにマージする正当性は、要求される物理順序（Physical Property Ordering）の包含関係に基づいている。

- **強い順序による満足**: 外側のソート要求が内側のソートキーの接頭辞である場合（例: 内側 `(a, b)`、外側 `(a)`）、内側のソート結果はすでに外側の順序要求を満たしている。外側のソート演算子は恒等写像として振る舞うため、内側のソート結果をそのまま外側グループの出力として採用できる。
- **拡張された外側ソート**: 外側のソート要求が内側のソートキーを接頭辞として含む場合（例: 内側 `(a)`、外側 `(a, b)`）、外側のソートキーで入力を直接ソートすれば、内側の要求順序 `(a)` を維持したまま第 2 キー `(b)` によるタイブレークが行われる。したがって中間のソートは完全に不要となる。
- **NULL 順序の厳密一致**: SQL において NULLS FIRST と NULLS LAST は明確に異なるタプル順序を生成する。昇降順だけでなく NULL の配置指定が一致しない限り、接頭辞とみなして統合することは許されない。本 Rule は NULL 順序の不一致を検知して安全に発火を抑止する。

## 実装の詳細

`plan/cascades.cpp` における変換処理は、接頭辞の方向に応じた 2 つの分岐からなる。

```cpp
            if (is_prefix(outer, inner)) {
              LogicalExpression merged = inner;
              merged.children = inner.children;
              memo.AddExpression(group, std::move(merged));
            } else if (is_prefix(inner, outer)) {
              LogicalExpression merged = outer;
              merged.children = inner.children;
              memo.AddExpression(group, std::move(merged));
            }
```

いずれの場合も、`merged.children` を内側ソートの子（`inner.children`）に差し替えることで、内側のソートノードをバイパスして基底ノードに直結させる。

## 最適化効果

計算量 $O(N \log N)$ を要するソート処理が 2 回から 1 回に削減される。

実行時 CPU 時間およびソートバッファ（メモリ・一時ディスク領域）の消費量が劇的に削減され、物理実装の選択（TopN やインデックス走査の活用）が単純化される。

## 関連 Rule との相互作用

- `eliminate_double_sort`: キーが完全一致する場合の冗長ソート削除に特化した Rule であり、本 Rule はその接頭辞一般化に位置づけられる。
- `eliminate_sort_under_unordered_consumer`: 親ノードがタプル順序を要求しない場合にソートを全削除する。
- `limit_push_through_sort`: ソートノードが TopN 演算子へ融合される際、本 Rule と連携して事前のキー縮約を行う。

## 検証テスト

- `plan/cascades_test.cpp` の `CascadesTest.CompatibleNestedSortsCollapseToOneOrder`: 内側 `(a ASC, b ASC)`、外側 `(a ASC)` の入れ子ソートに対し、スキャンに直結したキー数 2 の単一 Sort 代替式が生成されることを検証する。
