# numeric_widening_cast

- 状態: draft   /   執筆基準リビジョン: `3880673` (2026-09-12)
- 定義位置: `expression/rewrite.cpp` の `ExpressionRuleSet::Default()` 内
  `built.Add(ExpressionRule("numeric_widening_cast", ...))`

## 概要

二重の型キャスト `CAST(CAST(x AS T) AS T)` を外側の 1 段に統合するネスト解消と、定数リテラルに対する型キャスト `CAST(定数 AS 型)` をコンパイル時に計算する定数畳み込みを担う Rule です。

キャストのネスト解消は、内側のキャストと外側のキャストの目標型および `SAFE_CAST` 意味論が完全に一致しており、内側のキャストが真に冗長である場合に限定して適用されます。

## 変換前後の関係

```mermaid
graph TD
  subgraph before["変換前: CAST(CAST(x AS INT64) AS INT64)"]
    O["CAST → INT64"] --> I["CAST → INT64"] --> C["列 x"]
  end
  subgraph after["変換後: CAST(x AS INT64)"]
    O2["CAST → INT64"] --> C2["列 x"]
  end
```

## 適用条件

パターンは `Is(TypeTag::kCastExp)` です。ラムダ式内部で以下の 2 経路を判定します。

1. **定数キャストの畳み込み経路**: キャストの子ノードが定数リテラルである場合、`TryEvaluate` を試みます。

   ```cpp
   if (cast.Child()->Type() == TypeTag::kConstantValue) {
     if (StatusOr<Value> folded =
             expression->TryEvaluate(Row(), Schema());
         folded.HasValue()) {
       return ConstantValueExp(folded.MoveValue());
     }
     return Expression{};
   }
   ```

   `CAST('abc' AS INT64)` のように変換が失敗する場合は `Expression{}` を返し、実行時エラーとして正しく残します。

2. **同一型ネストキャストの解消経路**: 子ノードが別の `CastExpression` である場合、大文字小文字を無視した目標型名と `ReturnNullOnError`（SAFE_CAST フラグ）が完全に一致するときに限り、内側のキャストを除去します。

   ```cpp
   if (ToUpper(inner.TargetTypeName()) ==
           ToUpper(cast.TargetTypeName()) &&
       inner.ReturnNullOnError() == cast.ReturnNullOnError()) {
     return CastExpressionExp(inner.Child(), cast.TargetTypeName(),
                              cast.ReturnNullOnError());
   }
   ```

   異なる型への遷移（例: INT64 → FLOAT64）や縮小変換が含まれる場合は変換を行いません。

## 意味論的根拠と三値論理・例外保護

異なる型を跨ぐ多段キャストを安易に消去することは極めて危険です。内側のキャストがドメインの縮小や精度の変化を伴う場合、そこで発生すべき例外や丸め・桁落ちが、外側のキャストのみでは再現されなくなります。

```cpp
// Dropping the inner cast is sound only when it is redundant:
// same target, same SAFE_CAST semantics.  Cross-domain casts are
// lossy or partial and must survive (oracle-found:
// CAST(CAST(-inf AS INT64) AS FLOAT64) folded to
// CAST(-inf AS FLOAT64), erasing the NaN/Inf throw; FLOAT64 ->
// INT64 also truncates, INT64 -> FLOAT64 loses precision).
```

実例として、`CAST(CAST(-inf AS INT64) AS FLOAT64)` において内側のキャストは浮動小数点数の無限大を整数へキャストできないため例外を投げます。もし内側のキャストを除去して `CAST(-inf AS FLOAT64)` へ統合してしまうと、例外が消失して `-inf` が静かに返されてしまいます。

また、`ReturnNullOnError`（SAFE_CAST）が一致しない場合も、エラー時に NULL を返すのか例外を送出するのかという例外契約が変化します。そのため、型名の一致と SAFE_CAST フラグの一致が厳格に検証されます。

## 実装の詳細

経路 2 の再構築は `CastExpressionExp(inner.Child(), cast.TargetTypeName(), cast.ReturnNullOnError())` の 1 行で行われ、内側のラッパーを取り除いて外側のキャスト設定を直下の式に再結合します。

## 最適化効果

重複する不要な型変換オーバーヘッドを削減します。また、キャストの多重構造が平坦化されることで、述語のインデックス適用可能性の判定や後続の式簡約が阻害されなくなります。

定数リテラルのキャスト畳み込み（例: `CAST('100' AS INT64)` → `100`）により、タプル評価時の動的な型パースコストも完全に除去されます。

## 関連 Rule との相互作用

- `collapse_nested_identical_cast`: 同一型キャストのネストを解消する Rule であり、本 Rule の経路 2 と同一の責務を共有します。
- `predicate_pushdown_case`: CASE 式を内包するキャストの分配を担当します。

## 検証テスト

`expression/rewrite_test.cpp` の `ExpressionRewriteTest.NumericWideningCast` において以下を検証しています。

- `CAST(CAST(x AS INT64) AS INT64)` が `CAST(x AS INT64)` に統合されること。
- クロスドメインの `CAST(CAST(x AS INT64) AS FLOAT64)` や `CAST(CAST(x AS INT32) AS INT64)`、縮小変換のキャストが消去されずに保持されること。
- SAFE_CAST のセマンティクスが異なるキャストネストが保持されること。
- `CAST(123 AS DOUBLE)` が `123.0` 定数へ畳み込まれること。
