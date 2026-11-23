# 実生成 LLVM フィルタの本体・ベクトル余り・末尾を検証する

[翻訳検証](ir-translation-validation.md) の差し戻し。既存 JIT のテスト `JitTest.CompileFilter_ForEveryComparisonOperator_MatchesScalarReference` を `TINYLAMB_JIT_DUMP_DIR` 付きで実行して得た [等値フィルタのモジュール](fixtures/filter-eq.ll) を入力標本に固定した。32行単位の本体、8行単位の余り、スカラー末尾がある。

## 提出型

[`FilterLoopFootprint.Goal`](../../TinylambOpt/ResearchGoals/FilterLoopFootprint.lean) を import し、別モジュールで `theorem sound : TinylambOpt.ResearchGoals.FilterLoopFootprint.Goal` を提出する。

第一の提出物は、三段階のループのレーンが全要素を順にちょうど一度覆い、書込み先と 0/1 値が逐次 INT64 等値フィルタと一致する証明。41行では 8 レーンの組が5個とスカラー1個になる。第二の提出物は実ファイルを読む検査器。この IR の制御フロー、入力と出力の非重複・バッファ長・アラインメント、ポインタ算術と幅、poison とフラグの条件を確認して、検証済みのループモデルへ接続する。

## 適用範囲

Goal は第一の提出物用。Issue 全体の完了には、実ファイルを受理し、ベクトル部だけの eq→ne、末尾のアドレスずれ、未対応命令を拒否する検査が要る。ファイル名やハッシュだけでの受理は意味保存を保証しない。
