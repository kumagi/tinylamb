# 第60章 Rule のガバナンス — 追加・無効化・事前条件ゲート・検証の網

- 状態: draft / 執筆基準リビジョン: `3880673` (2026-09-12)

Rule は、同値性を証明できる条件の範囲でだけ適用できます。この章では、
Rule の追加・無効化、D5 監査で求める事前条件ゲート、誤変換を検出する
テストとファジングを説明します。

## 60-1. 規則管理の設計 — 値としての Rule セット

tinylamb において Rule セットは大域変数ではなく、`OptimizerOptions` に格納して受け渡す値です（第 1 章 1-4 節）。値として管理するため、Rule の追加、差し替え、除外はすべて複製に対する局所操作で完結します。

論理 Rule セットの管理インターフェースは以下のとおりです（`plan/cascades.cpp` の `RuleSet`）。

```cpp
RuleSet& RuleSet::Add(Rule rule) {
  Remove(rule.Name());
  rules_.push_back(std::move(rule));
  return *this;
}

bool RuleSet::Remove(std::string_view name) {
  const size_t old_size = rules_.size();
  std::erase_if(rules_, [&](const Rule& rule) { return rule.Name() == name; });
  return old_size != rules_.size();
}
```

`Add` メソッドは、同一名の規則が既に存在する場合にそれを削除してから末尾に追加します。規則の識別名がユニーク ID として機能しており、同名での登録がそのまま安全な差し替え操作となります。

物理 Rule セットも同じ方法で変更します（`plan/optimizer.cpp` の `Optimizer::Optimize`）。

```cpp
  const cascades::ImplementationRuleSet* implementation_rules =
      &DefaultImplementationRules();
  cascades::ImplementationRuleSet customized;
  if (!options.disabled_implementation_rules.empty() ||
      !options.extra_implementation_rules.empty()) {
    customized = DefaultImplementationRules();
    for (const std::string& disabled : options.disabled_implementation_rules) {
      customized.Remove(disabled);
    }
    for (const auto& extra : options.extra_implementation_rules) {
      customized.Add(extra);
    }
    implementation_rules = &customized;
  }
```

無効化リスト（`disabled_implementation_rules`）に含まれる規則を除外し、追加規則（`extra_implementation_rules`）を反映します。オプションによる変更が指定されていない場合は、既定の静的インスタンスへのポインタを直接参照し、不要なオブジェクト複製を回避します。

なお、単一テーブル走査のみで構成されるクエリに対しては、Memo 探索をバイパスして直接アクセスパスのコスト評価を行うショートカット経路（`OptimizeSingleRelation`）が用意されています。この高速経路は、既定の規則セットが変更されていない場合にのみ選択されます。

## 60-2. 事前条件ゲート（D5 監査）— 意味論保存の証明

tinylamb における論理規則の基本原則は、「変換によって結果行の多重集合（multiset）と順序保証が変化しないことを証明できなければ発火させてはならない」ことです。この規律は `docs/design.md` の D5 監査項目として明文化されています。

1. **多重集合の保存証明**: すべての論理同値 Rule に、変換前後で行と重複度が変わらないことを証明する事前条件（guard）が必要です。
2. **証明不能時の適用停止**: 事前条件を静的にも動的にも証明できない場合、その Rule は適用しません。
3. **欠陥 Rule の即時無効化**: 誤った行の出力や行の脱落が確認された Rule は直ちに無効化し、その理由を設計文書に記録します。
4. **反例テストの固定**: 無効化された規則に対しては、誤適用によって結果が狂う具体的な反例テストが `plan/cascades_test.cpp` に恒久的に保存されます。

無効化された規則の再有効化に関する基準も厳格に定められています。

> Re-adding a disabled rule requires re-establishing its precondition gate and a counterexample test; a rule may not be re-enabled just to recover an optimization opportunity.

最適化の機会を回復したいという動機だけで、安全条件が証明されていない規則を安易に復活させることは禁止されています。

### 無効化された規則の事例

- **`push_down_limit_through_join`（無効化）**: LIMIT を内側結合の下層へ押し込む規則。左辺の各行が右辺と 1 行以上マッチすることが保証されない場合、結合によって脱落するはずの行が LIMIT の上限枠を消費し、最終結果の行数が不足します。反例テスト `PushDownLimitThroughJoinOnUniqueKey` がこれを検証しています。
- **`topn_push_through_inner_join`（無効化）**: 上記と同様の理由により、外部キー制約等による 1 行以上の対応が証明されない限り、Top-N 境界での行欠落が発生するため無効化されています。

## 60-3. 「最適化」と「正しさの変更」の境界

規則を評価する際は、述語の評価回数と配置場所の違いに注意する必要があります（`docs/cascades_optimizer.md` のサーチフロー節）。

- **複数テーブルの結合グループ**: 根ノードに配置された `kSelection` がすべての述語を包括的に再評価します。そのため、結合の下位スキャンノードへ述語を押し下げる変形は、後段で再評価される純粋な性能最適化です。
- **単一テーブルのスキャングループ**: 単一テーブルでは根ノードの選択演算子が省略され、**グループ所有のスキャン述語（`Group::filter`）が唯一の評価点**となります。したがって、単一テーブルのスキャン述語を変更する操作は、後段での再評価を伴わないため、最適化ではなくクエリの意味論そのものの変更となります。

このため、単一テーブルに対するフィルタの変形や推論規則の適用には、結合ノードに対する規則以上の厳格な安全性検証が要求されます。

## 60-4. 検証基盤 — テスト、オラクル、ファジング

最適化の安全性を検証するため、以下の自動検査機構が組み込まれています。

**1. Memo 不変条件のプロパティベーステスト（`plan/plan_memo_oracle.cpp`）**:
ランダムに生成された結合グラフを用いて、Memo 構造の健全性を検証します。
- 探索前後におけるリレーションマスクの分割整合性、式の指紋の一意性、およびスキャン述語・結合述語の配置正当性。
- 同一グラフの再探索における結果の決定性（グループ数および式数の一致）。
- 規則セットを削減した場合に、既定セットよりも多くの解が生成されないこと（探索の単調性）。
- 不整合が検知された場合、`ShrinkJoinGraph` によってグラフを自動的に最小化し、再現用コードを生成します。

**2. 層横断ファザー（`plan/rule_fuzzer.cpp`）**:
複数のテストオラクルを並列実行し、最適化結果の一致を網羅的に検査します。
- NoREC（No-Reference Engine Condition）に基づく述語押し下げ検証。
- 式簡約器と AST 評価器の出力比較（差分テスト）。
- SQL 全体の実行結果と代替実行パスとの照合。

これらの自動検証によって、手作業では確認しにくい複雑な結合木や境界条件での誤変換を検出します。

## 60-5. 規則の追加・更新チェックリスト

新しい規則を実装、または既存規則を変更する際は、以下の点検を行います。

1. **レイヤの選定**: 式書き換え（スカラー層）、論理同値規則（論理層）、または実装規則（物理層）のいずれに属するかを明確にします。
2. **事前条件ガードの実装**: 適用パターンの宣言および変換関数内部に、意味論保存に必要な前提条件を記述します。所属関係を証明できない未修飾列は拒否します。
3. **反例テストの作成**: 事前条件が欠落した場合に結果が狂う具体的なケースを `plan/cascades_test.cpp` にテストとして追加します。
4. **不変条件テストの実行**: `plan_memo_oracle` および `rule_fuzzer` を長時間実行し、既存規則との相互作用による不整合が生じないことを確認します。
5. **文書の同期**: 本書の該当章および規範文書（`docs/cascades_optimizer.md` の D5 監査表）に事前条件と状態を記録します。

このチェックリストを満たした Rule だけを既定セットに加えます。第 70 章では、
スカラー式を正規化する式書き換えフレームワークと、基本的な Rule を
説明します。
