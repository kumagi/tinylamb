# json_path_constant_fold

- 状態: draft   /   執筆基準リビジョン: `3880673` (2026-09-12)
- 定義位置: `expression/rewrite.cpp` の `ExpressionRuleSet::Default()` 内
  `built.Add(ExpressionRule("json_path_constant_fold", ...))`

## 概要

JSON アクセサ関数（`JSON_EXTRACT`, `JSON_QUERY`, `JSON_VALUE`, `JSON_EXTRACT_SCALAR`, `JSON_EXTRACT_ARRAY`, `JSON_QUERY_ARRAY`, `JSON_VALUE_ARRAY`, `JSON_EXTRACT_STRING_ARRAY` の計 8 種）において、対象の JSON 文字列および JSON パスがともにコンパイル時定数である場合に、パス抽出を実行して定数リテラルへ畳み込む Rule です。

## 変換前後の関係

```mermaid
graph TD
  subgraph before["変換前: JSON_VALUE('{\"name\": \"Alice\"}', '$.name')"]
    F["json_value"] --> J["'...JSON 文字列...'"]
    F --> P["'$.name'"]
  end
  subgraph after["変換後: 'Alice'"]
    K["'Alice' 定数"]
  end
```

## 適用条件

パターンは `Is(TypeTag::kFunctionCallExp)` です。関数名が大文字化された 8 種の JSON ファミリーのいずれかに合致し、かつ以下の条件を満たす場合に発火します。

```cpp
if (fn.Args().empty() || !fn.Args()[0] ||
    fn.Args()[0]->Type() != TypeTag::kConstantValue) {
  return Expression{};
}
const Value json_val = fn.Args()[0]->AsConstantValue().GetValue();
if (json_val.IsNull() || json_val.type != ValueType::kVarChar) {
  return Expression{};
}
```

- **JSON 本体の定数性**: 第 1 引数が非 NULL の VARCHAR 定数であること。
- **JSON パスの定数性**: 第 2 引数が省略されている場合はデフォルトパス `"$"` を適用し、指定されている場合は非 NULL の VARCHAR 定数であること。
- パス解析ルーチン `EvaluateJsonFunction` を呼び出し、結果を `ConstantValueExp` として返却します。

## 意味論的根拠と三値論理・例外保護

JSON テキストや JSON パスが実行時列参照に依存している場合、結果はタプルごとに動的に変動するためコンパイル時に確定させることはできません。両引数が確定定数リテラルである場合に限り、コンパイル時評価が数学的に安全となります。

重要な設計規律として、本 Rule が呼び出す `EvaluateJsonFunction` は、行ごとの AST 実行評価器が用いる `EvaluateJsonFunctionCall` と完全に同一の実装コードを共有しています。

```cpp
// Executes the JSON_EXTRACT / JSON_QUERY / JSON_VALUE / JSON_EXTRACT_SCALAR /
// JSON_EXTRACT_ARRAY / JSON_QUERY_ARRAY / JSON_VALUE_ARRAY /
// JSON_EXTRACT_STRING_ARRAY family on raw JSON text.  Shared by the
// json_path_constant_fold rule (compile-time fold) and the AST evaluator
// (row-wise execution) so both paths produce identical results.
```

この共通化により、コンパイル時の畳み込み結果と実行時の評価結果の間に一切の乖離（divergence）が生じない構造的保証が与えられています。パスが存在しない場合や型が合致しない場合（たとえば `JSON_VALUE` が配列に到達した場合）は SQL 規約どおり NULL が返され、そのまま NULL 定数として安全に畳み込まれます。

## 実装の詳細

変換処理の中核は以下の数行で完結します。

```cpp
Value res = EvaluateJsonFunction(
    fn.FuncName(), json_val.value.varchar_value, path);
return ConstantValueExp(res);
```

`EvaluateJsonFunction` は JSON 文字列を逐次解析し、指定パスの要素を取り出します。計算量は入力される JSON 文字列の長さに比例します。

## 最適化効果

定数 JSON に対するパス抽出演算がコンパイル時に解消されます。

JSON のパースおよびツリー走査は計算負荷の高い処理であるため、タプルごとに繰り返される実行時の JSON パース処理を完全にゼロへと削減できる効果は、通常の算術定数畳み込みと比較しても極めて大きくなります。

## 関連 Rule との相互作用

- `fold_function`: 引数がすべてリテラルである決定的関数の畳み込みを担当します。JSON 関数群も `kImmutable` として登録されているため先行して発火し得ますが、同一の実装ロジックを共有しているため常に同値の結果へと収束します。
- `datetime_and_string_fold_extent`: 文字列操作関数に関する同様の定数畳み込みを担当します。

## 検証テスト

`expression/rewrite_test.cpp` の `ExpressionRewriteTest.JsonPathConstantFold` において以下を検証しています。

- `JSON_EXTRACT('{"a": 1, "b": "hello"}', '$.a')` が `'1'` に畳み込まれること。
- `JSON_VALUE('{"name": "Alice"}', '$.name')` が `'Alice'` に畳み込まれること。
- `JSON_QUERY('{"items": [10, 20]}', '$.items')` が `'[10,20]'` に畳み込まれること。
- パス省略時にルートパス `"$"` として正しく評価されること。
