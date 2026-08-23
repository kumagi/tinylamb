# docs/cascades_optimizer.md レビュー指摘事項

## サマリー

ドキュメントの骨格は実装と高い整合性を持つ。論理ルール4種(`join_commutativity` / `join_enumeration` / `join_associativity_left` / `join_associativity_right`)は plan/cascades.cpp:216,225,250,263 と、物理ルール5種(`index_scan` / `full_scan` / `hash_join` / `index_join` / `nested_loop_join`)は plan/optimizer.cpp:369-407 と、それぞれ名前まで完全一致する。スカラー書き換えの列挙(38-49行目)も expression/rewrite.cpp の実在ルール群にすべて対応し、DSL例のシグネチャも正しい。一方で、コストモデル・PhysicalProperties・探索の打ち切り条件・実装ルールの生成物など「計画選択に影響する重要な挙動」がほぼ文書化されておらず、実装者が誤解する余地が大きい。

## 指摘一覧

### O-1: コード例が戻り値型(StatusOr)を無視している
- 区分: 実態との乖離
- 対象: docs/cascades_optimizer.md:61 「`auto plan = tinylamb::Optimizer::Optimize(query, context, options);`」
- 問題: 例からは `Optimize` が直接 `Plan` を返すように読める。実際は `StatusOr<Plan>` を返し、実行可能プランが見つからなければ `Status::kNotImplemented` を返す。失敗チェックを省いたコードを真似すると未チェックの StatusOr を伝播させる。
- 根拠: plan/optimizer.hpp:57-61 `static StatusOr<Plan> Optimize(const QueryData& query, TransactionContext& ctx, const OptimizerOptions& options);` / plan/optimizer.cpp:524-526 `if (!best) return Status::kNotImplemented;` / plan/optimizer.cpp:423 `throw std::runtime_error("No table specified")`
- 提案: 例を `ASSIGN_OR_RETURN(auto plan, Optimizer::Optimize(...))` 形式に改め、`kNotImplemented`(実行プランなし)と例外パス(空FROM等)に言及する。

### O-2: コスト式が一切文書化されていない
- 区分: 粒度不足
- 対象: docs/cascades_optimizer.md:7-8, 20-21 「which are costed and cached by required physical properties」「The best physical plan is cached with a `(group, PhysicalProperties)` key」
- 問題: 「costed」とだけあり、代替案の優劣判定式が不明。実装者はカスタムルール追加時に自分の `local_cost` がどう効くかを予測できない。
- 根拠: plan/optimizer.cpp:453-467 — `double cost = static_cast<double>(plan->AccessRowCount());` さらにクエリ順序要件を満たす場合は `cost = std::min(cost, 1.0);`。plan/cascades.cpp:398,406 — 合計コストは `child_cost += child_plan->cost;` と `const double cost = child_cost + alternative.local_cost;` の累積。
- 提案: 「local_cost = AccessRowCount()(行数推定)、順序一致で1.0上限に圧縮、総コスト=子コスト合計+local_cost、最小コスト採用」という式を明記する。

### O-3: PhysicalProperties の内容と伝搬方法が未説明
- 区分: 粒度不足
- 対象: docs/cascades_optimizer.md:20-21, 111-112 「cached by required physical properties」「the required `PhysicalProperties`」
- 問題: プロパティの中身(何を要求できるか)と子への伝搬規則が書かれていない。実際は `require_row_position` と `ordering` の2フィールドのみで、rootでは常に `ordering = {}`、かつ同一プロパティが全子演算子にそのまま渡される(派生なし)。「required physical properties」という表現から、より豊富な要求機構があると誤読しうる。
- 根拠: plan/cascades.hpp:128-134 `struct PhysicalProperties { bool require_row_position{false}; std::vector<ColumnName> ordering; ... }` / plan/optimizer.cpp:450-451 `properties{.require_row_position = query.require_row_position_, .ordering = {}}` / plan/cascades.cpp:391-393 子の最適化に同じ `properties` をそのまま渡す
- 提案: フィールド一覧・rootでの初期値・「現状は子へ無変換伝搬(ordering要求の下位委譲なし)」を追記する。

### O-4: 探索の打ち切り条件と例外パスが未記載
- 区分: 粒度不足
- 対象: docs/cascades_optimizer.md:18-19 「explores logical rules to a fixed point」、同:48-49 「continues to a fixed point」
- 問題: 「不動点まで」とあるが、実装は上限付きである。論理ルール適用は64パスで打ち切られ、収束しなければ例外送出。`join_enumeration` は64リレーション以上で例外。式書き換えも32パスで例外。境界条件とエラー時挙動が文書になく、大規模結合や発散しうるカスタムルール導入時の挙動を読者が予測できない。
- 根拠: plan/cascades.cpp:328-339 `for (size_t pass = 0; pass < 64; ++pass) { ... if (pass == 63) throw std::runtime_error("cascades rules did not converge"); }` / plan/cascades.cpp:231-233 `if (relations.size() >= std::numeric_limits<uint64_t>::digits) { throw std::runtime_error("join graph is too large for enumeration"); }` / expression/rewrite.cpp:660-668 `for (size_t pass = 0; pass < 32; ++pass) { ... } throw std::runtime_error("expression rewrite did not converge");`
- 提案: 各上限(64パス/32パス/63リレーション)と超過時の `std::runtime_error` 送出を明記する。

### O-5: join_enumeration の対称性除去(半分空間探索)が未説明
- 区分: 粒度不足
- 対象: docs/cascades_optimizer.md:26 「`join_enumeration`」
- 問題: ルール名のみで挙動が未知。実装は3リレーション以上のグループにのみ発火し、さらに bit0 を立てた部分集合のみ生成することで左右鏡像の重複を排除している。この非対称性を知らずに列挙結果の完全性を検証するテストを書くと混乱する。
- 根拠: plan/cascades.cpp:229-230 `const std::vector<std::string> relations = memo.Get(group).relations; if (relations.size() < 3) return;` / plan/cascades.cpp:235-236 `if ((mask & 1U) == 0) continue;`
- 提案: 「3関係以上で発火」「先頭関係を必ず左側に含む分割のみ生成(鏡像重複の排除)、2関係の入れ替えは commutativity が担当」と追記する。

### O-6: 組込み実装ルールの thread_local 隠蔽依存が非公開
- 区分: 不明瞭
- 対象: docs/cascades_optimizer.md:10 「The layers depend on their small pattern and binding APIs, not on one another.」、同:109-115 「Adding a physical implementation」節
- 問題: 「グローバル状態なし・疎結合」という文脈で説明されているが、組込み物理ルールのコールバックは `Optimizer::Optimize` 内でだけ設定される thread_local ポインタ `tls_implement` を参照する。`DefaultImplementationRules()` や `SearchEngine` を `Optimize` 外で再利用すると、空の `std::function` 呼び出し(未定義動作/`std::bad_function_call`)になる。カスタムルール作者向けの制約が文書化されていない。
- 根拠: plan/optimizer.cpp:363 `thread_local OptimizerImplementContext* tls_implement = nullptr;` / plan/optimizer.cpp:370-376, 383-386 等 組込みルール内 `return tls_implement->scan_alternatives(...)` / plan/optimizer.cpp:500-506 `tls_implement = &implement_context;` と RAII での解除
- 提案: 「組込み実装ルールは `Optimizer::Optimize` 経由でのみ使用可能(TLSコンテキスト依存)。単体利用には自前の Implement を書くこと」と注意書きを追加する。

### O-7: 物理ルール名と実際に生成されるプランの対応が不明
- 区分: 粒度不足
- 対象: docs/cascades_optimizer.md:30-36 物理ルール一覧(`full_scan`, `index_scan`, `hash_join`, `index_join`, `nested_loop_join`)
- 問題: ルール名の一覧は正しいが、各ルールがどの Executor を持つプランを返すかに触れていない。(a) `index_scan`/`full_scan` ルールは被覆状況次第で IndexOnlyScanPlan を返す、(b) `hash_join` は kInMemory/kHybrid の2モード、(c) `index_join`/`nested_loop_join` も含め結合系はすべて ProductPlan 系で実装される——という実態がないと、新規実装ルール追加時の比較対象を取り違える。
- 根拠: plan/optimizer.cpp:132-137 `BuildIndexScan` が `IndexOnlyScanPlan` を返却(covered && !require_row_position 時)/ plan/optimizer.cpp:303-306 `HashJoinMode::kInMemory` と `kHybrid` の2候補 / plan/optimizer.cpp:289, 304-305, 320-321 結合候補はすべて `ProductPlan`
- 提案: 「rule → 生成プラン(Executor)」の対応表を追加する(index-only 変種・ハイブリッドハッシュを含む)。

### O-8: スカラールール間の適用順序セマンティクスが不明瞭
- 区分: 不明瞭
- 対象: docs/cascades_optimizer.md:64-65 「`Add` replaces a rule with the same name, which makes a local override explicit and avoids ordering two implementations with the same identity.」
- 問題: 同名置換については述べているが、異なるルール同士が同一ノードにマッチした場合の優先順位(登録順・最初に成功したものが勝ち)が書かれていない。オーバーラップするカスタムルールを足したときの結果が順序依存になることを読者は想定できない。
- 根拠: expression/rewrite.cpp:682-684 `for (const ExpressionRule& rule : rules_->Rules()) { if (Expression replacement = rule.Apply(current)) return replacement; }`(登録順・初勝利)/ expression/rewrite.cpp:170-181 `Add` は同名を erase してから push_back
- 提案: 「ルールはセット内の登録順に適用され、ノードごとに最初に書き換えを返したものが採用される。上書きしたい場合は同じ名前で Add して置換位置を制御する」ことを明記する。

### O-9: スカラールール列挙に関数呼び出しの定数畳み込みが欠落
- 区分: 粒度不足
- 対象: docs/cascades_optimizer.md:38-39 「constant folding for binary, unary, and `IN` expressions」
- 問題: 列挙は binary/unary/IN にとどまるが、実装はリテラル引数の関数呼び出し(`fold_function`)も畳む。ドキュメントの列挙を網羅的だと受け取った実装者が、fold 済み定数関数に依存する書き換えルールを書けないと誤認する可能性がある。
- 根拠: expression/rewrite.cpp:230-245 `built.Add(ExpressionRule("fold_function", Is(TypeTag::kFunctionCallExp), ...))` — 引数が全部 ConstantValue/Interval のとき `Evaluate` で定数化
- 提案: 列挙に「literal 引数の関数呼び出し」を加えるか、「主要なものを抜粋、完全な一覧は `ExpressionRuleSet::Default()` 参照」と注記する。

## 未検証事項

- 「Tests ... are in `expression/rewrite_test.cpp`, `plan/cascades_test.cpp`, `plan/optimizer_test.cpp`」(docs/cascades_optimizer.md:117-119)について、3ファイルの存在は確認したが、記載内容がテストの網羅範囲と一致するかまでは検証していない。
- DSL例(69-80行目、93-101行目)のコンパイル可否はシグネチャ照合による確認であり、実ビルドでの検証は行っていない。
