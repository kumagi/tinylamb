# docs/jit_profile.md レビュー指摘事項

## サマリー

本ドキュメントは11行のベンチマーク覚書であり、設計文書としての必須要素(機構の所在、閾値の定義、適用条件、フォールバック規則)がほぼ欠落している。実装側には JIT 昇格機構が実在し(`executor/selection.cpp` / `projection.cpp` / `aggregation.cpp`、既定閾値 20'000'000 行)、記載された数値テーブルもベンチマーク出力形式と整合する。ただし「SUM kernel」は並列集約経路では到達不能であること、filter の昇格条件がドキュメントより遥かに狭いこと、「累積2,000万行」の意味(クエリ実行単位・エグゼキュータインスタンス単位)など、粒度不足の指摘が多い。またタイトルに反してプロファイリング(動的ホットスポット検出)機構は存在しない。

## 指摘一覧

### J-1: 「プロファイル」と題しながらプロファイリング機構が実装されていない
- 区分: 実態との乖離
- 対象: docs/jit_profile.md タイトル「LLVM JIT target profile」および全体
- 問題: 動的なプロファイリング・ホットスポット検出(サンプリング、式ごとの評価回数計測、閾値自動調整)はコードに一切存在しない。実際にあるのは「エグゼキュータごとの通過行数が固定定数 20'000'000 を超えたら一度だけ JIT コンパイルを試みる」という静的ヒューリスティクスのみ。何が欠けているかを列挙すると:(a) 式単位・演算子単位の評価回数カウンタなし、(b) 閾値の実行時設定手段なし(SQL/GUC/環境変数いずれも不可)、(c) コストに基づく昇格判断なし、(d) 計測結果の永続化なし。
- 根拠: executor/selection.hpp:36 `size_t jit_threshold_rows = 20'000'000`(コンストラクタ既定値のみ)/ executor/selection.cpp:112-114 `rows_seen_ += input_batch_.Size(); if (bytecode_ && !jit_attempted_ && rows_seen_ >= jit_threshold_rows_) { jit_attempted_ = true; ... }` / `grep` により `jit_threshold_rows` の外部設定箇所は存在せず、plan/selection_plan.cpp:33-35 等の生成元はすべて既定値を使用
- 提案: 冒頭に「本稿は測定記録であり、昇格判定は固定閾値(20M行/エグゼキュータ)の静的ヒューリスティクスである。プロファイラによる動的判定は未実装」と明記する。

### J-2: 「累積2,000万行からJITへ昇格」は Selection 限定かつ単一クエリ実行内の計数
- 区分: 粒度不足
- 対象: docs/jit_profile.md:10 「Selectionは累積2,000万行からJITへ昇格する」
- 問題: (a) 同一ポリシーは Projection と AggregationExecutor にも存在するのに Selection しか書かれていない。(b) 「累積」の範囲が不明瞭——カウンタはエグゼキュータインスタンス内でリセットされるものであり、複数クエリ間やプロセス生存期間での累積ではない。(c) コンパイルは閾値到達後の最初のバッチで遅延実行され、そのバッチに約2ms級の一時停止が発生することが書かれていない(プロセス全体のカーネルキャッシュがあるため2回目以降は発生しない)。
- 根拠: executor/projection.hpp:36 `size_t jit_threshold_rows = 20'000'000` / executor/aggregation.hpp:35 同値 / executor/projection.cpp:115-120 `jit.rows_seen += ...; if (jit.eligible && !jit.attempted && jit.rows_seen >= jit_threshold_rows_) { jit.kernel = JitInt64Kernels::CompileProjection(); }` / executor/aggregation.cpp:62-66 / expression/jit.cpp:36-41 プロセスワイドの `filter_kernel_cache` 等 / executor/selection.hpp:67 `size_t rows_seen_{0};`(メンバ=インスタンス単位)
- 提案: 「Selection/Projection/(単一スレッド)Aggregation の各エグゼキュータが、1回のクエリ実行内で処理した行数が 20M 既定値を超えた最初のバッチで JIT を遅延コンパイルする(初回のみコンパイル時間を払う)」と正確化する。

### J-3: SUM kernel は大規模集約の実経路では到達不能
- 区分: 実態との乖離
- 対象: docs/jit_profile.md:11 「JIT対象はINT64 filter、線形projection、SUM aggregate kernelに限定し」
- 問題: SUM kernel は単一スレッドの AggregationExecutor にのみ組み込まれている。しかし推定行数が 8192 以上の集約は常に ParallelAggregationExecutor へ配線され、こちらには JIT パスが存在しない。つまり TPC-H 的な大規模 SUM では JIT は使われず、「SUM が JIT 対象」という記述は現行プラン配線の下でほぼ機能しないことを読者は知り得ない。
- 根拠: plan/aggregation_plan.cpp:42-46 `if (child_->EmitRowCount() >= kParallelAggregationMinRows) { return std::make_shared<ParallelAggregationExecutor>(...); } return std::make_shared<AggregationExecutor>(...);` / plan/parallel_thresholds.hpp:18 `inline constexpr size_t kParallelAggregationMinRows = 8192;` / `grep -n "jit" executor/parallel_aggregation.{cpp,hpp}` は JIT 参照ゼロ / executor/aggregation.cpp:63-65(JIT Sum は AggregationExecutor のみ)
- 提案: 「SUM JIT は非並列 AggregationExecutor 専用。推定8192行以上は並列集約へ迂回し JIT 非対応」の但し書きを追加する。

### J-4: filter の昇格条件は「INT64 filter」よりも遥かに狭い
- 区分: 粒度不足
- 対象: docs/jit_profile.md:11 「JIT対象はINT64 filter...に限定し」
- 問題: 実際の昇格条件は「bytecode がちょうど3命令 `[LoadColumn, LoadConstant, BinaryInt64]` であり、定数が INT64」であること。すなわち列対列比較(`a > b`)や算術を含む述語は INT64 でも JIT されない。「INT64 filter」とだけの記述では、どの形状が対象かを実装者が判断できない。
- 根拠: executor/selection.cpp:115-127 `if (instructions.size() == 3 && instructions[0].opcode == BytecodeOp::kLoadColumn && instructions[1].opcode == BytecodeOp::kLoadConstant && instructions[2].opcode == BytecodeOp::kBinaryInt64 && bytecode_->Constants()[instructions[1].operand].type == ValueType::kInt64)`
- 提案: 「単一列 vs INT64 定数の二項比較のみ。列対列・複合式は対象外(bytecode/式ツリーのまま)」と明記する。

### J-5: フォールバックの階層とゾーンマップの干渉が未説明
- 区分: 粒度不足
- 対象: docs/jit_profile.md:11 「複雑式・NULLを含むbatchはbytecodeへフォールバックする」
- 問題: 記載は正しいが不完全。(a) フォールバックは二段階ある——bytecode も作れない式(文字列比較以外の複雑式等)は行単位の式ツリー評価まで落ちる。(b) JIT/bytecode 以前にゾーンマップ pruning(BatchMayMatch)でバッチ丸ごとスキップする経路があり、JIT 効果の測定・解釈に影響する。(c) NULL 判定は列の ZoneMap NullCount==0 のみで、他列の NULL は関係ない、という細部も不明。
- 根拠: executor/selection.cpp:107-110(zone-map スキップ)、129(NULL ゲート `ZoneMapAt(jit_column_).NullCount() == 0`)、136-138(bytecode フォールバック)、139-146(predicates がない場合は `exp_->Evaluate` の行単位評価)/ expression/bytecode.cpp:48-98,102-118(CompileNode は int64/double/varchar/date の binary/unary 以外 nullopt)
- 提案: 「JIT → bytecode → 式ツリー評価」の三層構造と、ゾーンマップによる先行スキップの存在を追記する。

### J-6: 掲載数値の測定環境・再現手順が記録されていない
- 区分: 不明瞭
- 対象: docs/jit_profile.md:3-9(2026-08-19 実施、ORC compile 約2.36ms、性能テーブル)、同:10(損益分岐約2,097万評価)
- 問題: 数値自体はベンチマークの出力項目(`compile_ms`, `scalar_ms`, `jit_ms`, `break_even_evaluations`)と整合し、評価回数も `rows×repetitions`(65536×20=131万、262144×20=524万、1048576×20=2097万)と一致する。しかし CPU・LLVM バージョン・ビルドフラグ・コマンドが未記録で、BenchmarkHistory.md には該当記録がなく再現検証できない。さらに損益分岐 20,971,520 は測定グリッドの最大点と一致しており、「最大測定点でちょうど逆転した」のか「それ以上で逆転するはずが測っていない」のか区別できない。
- 根拠: benchmark/expression_jit_benchmark.cpp:18(`compile_ms=` 出力)、21-22(rows={64..1048576})、26(`repetitions = 20`)、45-47(break_even 判定)、51(`break_even_evaluations=` 出力)。BenchmarkHistory.md 内に JIT 関連記録なし(grep で確認)
- 提案: 実行環境(CPU/LLVM/ビルドオプション/コマンドライン)を併記し、損益分岐が測定境界値であることを注記する。BenchmarkHistory.md への記録も推奨。

## 未検証事項

- テーブル内の各測定値(0.69ms/0.27ms 等)および compile 2.36ms は、リポジトリ内に保存された実行ログがなく真偽を検証できなかった(形式の整合性のみ確認)。
- `TINYLAMB_HAS_LLVM` 未定義ビルドでの挙動(全 kernel が `std::nullopt` を返し bytecode のみで動作)はコード上確認したが、実ビルドでの動作確認は行っていない(expression/jit.cpp:110-113)。
