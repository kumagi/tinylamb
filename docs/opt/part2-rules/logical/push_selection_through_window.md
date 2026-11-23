# push_selection_through_window

- 状態: done   /   執筆基準リビジョン: `3880673` (2026-09-12)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`(`built.Add(Rule("push_selection_through_window", …)`)
  登録式

## 概要

Window の上のフィルタ(Selection)の連言のうち、**PARTITION BY 列だけを触る
ものを Window の下へ押し込む** Rule です。登録前のコメントが条件を述べて
います。

```cpp
    // push_selection_through_window: Push Selection conjuncts that reference
    // only PARTITION BY columns below the Window operator.
```

フィルタの一部だけ押せる場合は、押せる部分だけ下へ置き、残りは上に
Selection として残す部分押し込みに対応しています。

## 変換前後の関係

`WHERE part_col = 10 AND rn <= 5`(`part_col` は PARTITION BY 列、`rn` は
ウィンドウ出力)の例です。

```mermaid
graph TD
  subgraph before["変換前"]
    S1["Selection part_col=10 AND rn<=5"] --> W1["Window PARTITION BY part_col"] --> X1["Scan t"]
  end
  subgraph after["変換後: part_col=10 だけが下へ、rn<=5 は上に残る"]
    S2["Selection rn<=5"] --> W2["Window PARTITION BY part_col"] --> S3["Selection part_col=10"] --> X2["Scan t"]
  end
```

全連言が押せる場合(残りなし)は、上の Selection は消えて
`Window(Selection(X))` だけになります。

## 適用条件

パターンは `Selection(Any("input"))`、`target = kSelection` です。ガードは
次のとおりです。

(1) 述語を持ち、入力グループに `kWindow`(子 1 個、**PARTITION BY が空で
ない**)の代替があること。

```cpp
            if (win_expr.operation != LogicalOperator::kWindow ||
                win_expr.children.size() != 1 ||
                win_expr.partition_by.empty()) {
              continue;
            }
```

(2) PARTITION BY 式が**単純な列参照**(`kColumnValue`)であるものだけを
パーティション列の集合に加えます。式(例: `p + 1`)は列集合に寄与しません。
各ウィンドウ関数の PARTITION BY にも現れる列だけを残します。この共通部分が空なら発火しません。

```cpp
            std::vector<ColumnName> partition_cols;
            for (const auto& p : win_expr.partition_by) {
              if (p && p->Type() == TypeTag::kColumnValue) {
                partition_cols.push_back(p->AsColumnValue().GetColumnName());
              }
            }
```

(3) 連言の各項は次の全てを満たすとき「押せる」になります。

```cpp
              bool can_push = SafeToReduceEvaluationCount(conjunct);
              auto touched = conjunct->TouchedColumns();
              if (touched.empty()) {
                can_push = false;
              }
              for (const auto& col : touched) {
                const bool in_partition = std::ranges::any_of(
                    partition_cols, [&](const ColumnName& partition) {
                      return SameColumnReference(col, partition);
                    });
                if (window_outputs.contains(col.name) || !in_partition) {
                  can_push = false;
                  break;
                }
              }
```

- 触れる列が 1 つもない(定数述語など)場合は押しません。
- 触れる列が 1 つでも**ウィンドウの出力名**(`rn` など)であれば押しません。
- 触れる列が 1 つでも共通のパーティション列（`SameColumnReference` で照合）に
  含まれなければ押しません。

(4) 押せる項が 1 つもなければ発火しません。派生グループ
(`"win_part_push:" + GroupIdsTag({win_child}) + combined->ToString()`)の循環チェックもあります。子 ID を指紋に含むため、同じウィンドウ出力を異なる子 Group で扱っても混線しません。

## 意味論的根拠とパーティション独立性

ウィンドウ関数の値は「同じパーティション内の行たち」だけで決まります。
PARTITION BY 列でフィルタしても**パーティションの構成員が変わらない**
(1 つのパーティションの中から条件を満たす行が抜けるだけ)ため、下で
フィルタしても上のウィンドウ関数の値は 1 つも変わりません。これが意味保存の
論拠です。

- **ウィンドウ出力列(`rn` 等)でのフィルタは押せません。** `rn <= 5` は
  ウィンドウ関数の計算結果があって初めて評価できる条件で、下に押すと
  その列が存在しないためです(押してしまうと結果が変わる、というより評価
  自体が不能です)。`window_outputs` チェックがこれを遮ります。
- **非パーティション列でのフィルタは押せません。** `WHERE v > 10` を下へ
  押すと、パーティション内の行の一部が欠け、ウィンドウ関数の値(例:
  `SUM(v) OVER (PARTITION BY p)`)が変わります。行数制限と同じく、ウィンドウ
  は「全パーティション行」を見る演算子なので、下押し込みは一般に意味を
  変えます。
- 定数述語(`1 = 1` など、列を参照しない述語)は押し込みません。入力行を
  減らせず、Window 演算の処理量も変わらないためです。

部分押し込みの残差処理の正確さにも注意が必要です。押せなかった項は
**元の意味のまま上に残る**ため、全体として元のフィルタと同じ行集合です。

## 実装の詳細

押せる項だけを下に置く部分です。

```cpp
            memo.AddExpression(
                win_child_filtered,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {win_child},
                                  .predicate = CombineConjuncts(pushable)});

            if (remaining.empty()) {
              LogicalExpression new_win = win_expr;
              new_win.children = {win_child_filtered};
              memo.AddExpression(group, std::move(new_win));
            } else {
              const GroupId new_win_group = memo.EnsureDerivedGroup(
                  memo.Get(input_id).relations,
                  "win_pushed:" + GroupIdsTag({win_child_filtered}) +
                      TargetListFingerprint(win_expr.target_list));
```

- `remaining` が空なら、Window をそのままフィルタ済みの子に付け替えた式を
  外側グループへ追加します(上の Selection はこの代替では消えます)。
- `remaining` が空でなければ、Window を載せた派生グループ(`win_pushed`)を
  経由して、残りの連言を上に被せた形を外側グループへ追加します
  (`memo.AddExpression(group, …)` で `Selection(new_win_group, remaining)`)。
  この派生グループのタグは `"win_pushed:" + GroupIdsTag({win_child_filtered}) + TargetListFingerprint(win_expr.target_list)` で、
  子 ID とウィンドウの target list の指紋を含みます。
- 押し込み先の派生グループのタグ `win_part_push:` + `GroupIdsTag({win_child})` + `combined->ToString()`
  は押し込む述語に加えて子 ID も指紋に含むため、別の述語の押し込みや異なる子 Group の
  ウィンドウ出力が同じ下位グループに混線しません。

この Rule の登録は一つです。共通のパーティション列として扱うのは
各 `PARTITION BY` に現れる単純な列参照であり、`p + 1` のような式から
`p` を取り出して押し込み可能とする処理はありません。

## 最適化効果

- パーティション列のフィルタ選択率が高い場合、ウィンドウ計算の入力行が
  減り、パーティション構築のコストが下がります。
- 押し込まれた Selection は、スキャンフィルタへの統合
  (`push_selection_into_scan`)やインデックス選択の材料にもなります。

## 関連 Rule との相互作用

- `no_op_window_elimination`: フィルタとウィンドウの組み合わせの簡約の
  うち、ウィンドウ結果自体が不要な場合を扱います。

## 検証テスト

- `plan/cascades_test_part3.cpp` の
  `CascadesTest.PushSelectionThroughWindowOnPartitionColumns` —
  `part_col = 10 AND rn <= 5` のうち `part_col = 10` が Window の下の
  Selection が Window の下に現れることを検証しますが、その述語式が `part_col = 10` であることは assertion していません。したがって window 結果依存述語の誤押し込みをこのテスト単独では検出できません。
- 本ルールの単体登録確認テストは個別には定義されていません。
