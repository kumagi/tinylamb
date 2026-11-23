# CASE 内の相関スカラサブクエリを遅延エラー付き索引に変換する

`CASE WHEN active THEN (SELECT v FROM inner WHERE inner.k = outer.k) END` を、内表を一度読む索引構築と外表の順次 probe に分ける。索引には各キーについて単一行の値か複数行かを格納する。参照側は外行ごとに相関条件を適用する。

## 提出する命題

[DemandScalar.Goal](../../TinylambOpt/ResearchGoals/DemandScalar.lean) の定義を以下に示す。提出物はこのモジュールを import した別ファイルの `theorem sound : Goal`。

```lean
def Goal : Prop := ∀ inner outer, probe (index inner) outer = reference inner 0 outer
```

複数行エラーを返すタイミングは、そのキーを有効な外行が最初に読む時点。提出型は正常時の全出力、失敗時の出力済みの列と最初の外行番号を比較する。純粋な列参照と評価済みの CASE 条件を扱う。

## 境界例と適用範囲

同じキーの値がどちらも NULL でも 2 行ならエラー。無効な CASE と NULL 結合キーを経て正常値 7 を返したあと、外行番号 3 で初めて失敗する例を両評価器で固定した。
