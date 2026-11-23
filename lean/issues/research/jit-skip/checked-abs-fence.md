# ABS のページ除外で最初のオーバーフローを保存する

**未証明。** checked INT64 の `ABS(x) < limit` を min/max で枝刈りする。数学的に一致行がなくても MIN の評価は失敗するので、そのページを読み飛ばせない。

## 提出型

[`CheckedAbsFence.Goal`](../../../TinylambOpt/ResearchGoals/CheckedAbsFence.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.CheckedAbsFence.Goal` を提出する。

逐次経路と、失敗までに返した行位置・最初の失敗位置が一致すること。読んだページ列も、逐次経路の到達ページから安全に飛ばせるものを除いた列に一致する。

## 境界例と適用範囲

limit が0で先頭の正常ページは不要でも、次ページの NULL に続く MIN で失敗する。負側の端点に等しいだけのページの除外も検査する。
