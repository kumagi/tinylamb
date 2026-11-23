# 引用符内の区切り文字を6段の prefix XOR で除く

**未証明。** 引用符・コンマのビットマスクを受け取り、64バイト中の外側コンマと次のチャンクに渡す引用状態を求める。

## 提出型

[`QuotedDelimiterMask.Goal`](../../../TinylambOpt/ResearchGoals/QuotedDelimiterMask.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.QuotedDelimiterMask.Goal` を提出する。

任意の有効長0〜64と開始状態で、6段のシフト XOR と逐次走査の出力マスク・終了状態が一致すること。末尾の無効レーンを除いてから状態を計算する。

## 境界例と適用範囲

同じ引用符マスクでも前チャンクが引用中かで出力が変わる。引用符とコンマは別のバイト。入力は引用符で状態を反転する字句層であり、不正な CSV のエラー処理までは扱わない。
