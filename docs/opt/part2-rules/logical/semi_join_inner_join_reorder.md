# semi_join_inner_join_reorder

- 状態: draft / 執筆基準リビジョン: `3880673` (2026-09-12)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `"semi_join_inner_join_reorder"`）

## 概要

`semi_join_inner_join_reorder` は、`(R SEMI JOIN S ON p1) JOIN T ON p2` の結合順序を `(R JOIN T ON p2) SEMI JOIN S ON p1` へ再結合（reorder）する論理等価 Rule です。

半結合が内側結合の左入力に配置されているとき、内側結合を先行して評価した後に半結合を適用する別経路を探索空間に導入します。これにより、結合順序の選択肢を増やし、インデックス利用や早期フィルタリングのコスト比較を可能にします。

## 変換前後の関係

```mermaid
graph TD
  subgraph before["変換前: (R ⋉ S ON p1) ⋈ T ON p2"]
    J1["Join ON p2"] --> J2["SemiJoin ON p1"] --> R1["Group R"]
    J2 --> S1["Group S"]
    J1 --> T1["Group T"]
  end
  subgraph after["変換後: (R ⋈ T ON p2) ⋉ S ON p1"]
    J3["SemiJoin ON p1"] --> RT["Group {R, T}(derived) Join ON p2"]
    J3 --> S2["Group S"]
  end
```

## 適用条件

パターンは `Join(Any("semi_child"), Any("t"))` であり、対象演算子は `LogicalOperator::kJoin` です。以下のガード条件をすべて満たす場合にのみ発火します。

1. 外側の式が `LogicalOperator::kJoin` であり、子ノード数が 2 であること。
2. 左の子グループ（`semi_child`）内に、子ノード数 2 の `LogicalOperator::kSemiJoin` 式 `(R ⋉ S ON p1)` が存在すること。
3. 外側の結合述語 `p2`（`expression.predicate`）が存在する場合、`p2` が参照する列がすべて `R` または `T` の関係に属すること（`S` の列を参照しないこと）。
4. 派生グループ（`semi_inner_reorder_rt` タグ）が、探索対象グループ、`R`、`T` のいずれとも一致しないこと（自己参照ループ防止）。

```cpp
          if (expression.operation != LogicalOperator::kJoin ||
              expression.children.size() != 2) {
            return;
          }
          // 左の子 Group 内から kSemiJoin 式を走査
          if (expression.predicate && *expression.predicate) {
            bool touches_only_rt = true;
            for (const auto& col :
                 (*expression.predicate)->TouchedColumns()) {
              if (std::ranges::find(r_rels, col.schema) == r_rels.end() &&
                  std::ranges::find(t_rels, col.schema) == t_rels.end()) {
                touches_only_rt = false;
                break;
              }
            }
            if (!touches_only_rt) {
              continue;
            }
          }
```

外側の結合述語 `p2` が `S` の列を参照している場合、組み替え後の内側結合 `(R JOIN T ON p2)` のスコープ内で `S` の属性が未解決となるため、本 Rule は発火を中止します。

## 意味論的根拠と多重度保存

半結合と内側結合の再結合が等価性を保つ根拠は、半結合の「多重度不変なフィルタ特性」と述語評価スコープの一致にあります。

- **行の一致と多重度保存**: `(R ⋉ S)` の出力は、述語 `p1` を満たす `S` 行が存在する `R` の行のみで構成されます。半結合は `S` の行を複製せず、`R` の属性のみを射影します。したがって、`(R ⋉ S) ⋈ T` で得られるタプルの多重度は、各 `R` 行と `T` 行のマッチ数によってのみ決定されます。先行して `(R ⋈ T)` を評価した後に `S` と半結合 `⋉ S` を適用しても、最終的に残るタプルの多重度は同一です。
- **述語参照スコープの厳密性**: 組み替え後の下位結合 `R ⋈ T` で述語 `p2` を評価するには、参照列が `{R, T}` に閉じている必要があります。`p2` が `S` の列を参照する場合（例: `p2 = (s.id = t.id)`）、下位結合ではその列を評価できません。本 Rule は `touches_only_rt` を必須条件とし、このような式が Memo に入ることを防ぎます。

## 実装の詳細

`plan/cascades.cpp` における変換処理は、派生グループの生成と多重登録の 2 段階で構成されます。

1. `memo.EnsureDerivedGroup(UnionRelations(r_rels, t_rels), "semi_inner_reorder_rt")` により、`R` と `T` の関係集合を持つ中間グループ `rt_group` を確保します。
2. `rt_group` に対して、子を `{r_id, t_id}`、述語を `expression.predicate`（`p2`）とする `LogicalOperator::kJoin` 式を登録します。
3. ルートグループ（元の結合グループ）に対して、子を `{rt_group, s_id}`、述語を半結合の述語 `semi_expr.predicate`（`p1`）とする `LogicalOperator::kSemiJoin` 式を登録します。
4. `target_list` および `output_schema` は元の式から忠実に継承され、グループの出力スキーマ契約を維持します。

## 最適化効果

元の結合順序と組み替え後の結合順序が Memo 内に並存し、コストエンジンによる比較が可能となります。

- `T` との結合述語 `p2` にインデックスが存在し、`R` と `T` を先に結合した方がコストが低い場合、あるいは `S` による半結合の選択度が高くなく先行評価の恩恵が小さい場合に、組み替え後のプランが有利となります。
- 逆に `S` との半結合によって大半の `R` を除外できる場合は、元のプランが選択されます。探索空間に両方の選択肢を登録し、推定コストで比較します。

## 関連 Rule との相互作用

- `semi_join_commutativity`: 半結合同士の交換可能性を探索する Rule であり、同様に参照列の局所性検査と派生グループ生成を用います。
- `push_semi_join_through_inner_join`: `(A JOIN B) SEMI JOIN C` を `(A SEMI JOIN C) JOIN B` などへ組み替える逆方向の移動を担います。本 Rule と組み合わさることで、半結合演算子のプラン木上下移動が対称的に探索されます。
- `in_list_to_semi_join` / `apply_to_join`: `IN` サブクエリ等から半結合を導出する Rule であり、導出された半結合が内側結合と隣接することで本 Rule の適用契機となります。

## 検証テスト

- `plan/cascades_test.cpp` の `SemiJoinInnerJoinReorder`: `(r ⋉ s) ⋈ t` の論理式を探索した際、ルートグループに右の子を `s` とする `kSemiJoin` 代替式（`(r ⋈ t) ⋉ s`）が正常に生成されることを検証します。
