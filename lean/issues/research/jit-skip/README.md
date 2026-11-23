# JIT の分岐とブロック読取りを減らす未証明課題

**更新: 以下 16 件はすべて `theorem sound : Goal` を証明済み**（登録は [../RESULTS.md](../RESULTS.md) の第四波の項）。

各リンクは独立して提出できる Issue。各 `Goal` が要求するのは、候補カーネルの値・マスク・エラー、またはページ評価器の結果・読取りに関する性質である。生成命令での分岐数や実機での高速化には、IR・機械語の検査とベンチマークが別途必要になる。

## ビット演算で行ごとの分岐をまとめる（9件）

- [8バイトの ASCII 数字判定を1語の演算にする](swar-digit-ballot.md)
- [符号付き加算の失敗レーンをマスクで求める](checked-lane-add.md)
- [複合 IN の候補64行を二つのマスクで評価する](row-in-mask.md)
- [符号付きビットスライス列を復号せず SUM する](bit-plane-sum.md)
- [圧縮後の選択マスクを PDEP で元のレーンへ戻す](selection-mask-deposit.md)
- [引用符内の区切り文字を6段の prefix XOR で除く](quoted-delimiter-mask.md)
- [混合方向・NULL 順の複合キーを単一の符号なし比較にする](packed-order-key.md)
- [1バイト置換を許す検索を2本のシフトレジスタにする](hamming-one-search.md)
- [三値述語の「3個中2個」を論理回路にする](threshold-bit-counter.md)

## データページや payload の読取りを省く（7件）

- [ビット列の AND/OR 要約で COUNT のページを三分類する](bit-signature-count.md)
- [剰余述語をブロック内の差の GCD で判定する](congruence-count.md)
- [必須リテラルの隣接バイト対で LIKE ページを除外する](bigram-chain-skip.md)
- [既出キーだけのページを DISTINCT の先頭行選択で飛ばす](first-key-block.md)
- [区間 JOIN のページ対を全不一致・全一致・残余に分ける](overlap-pair-count.md)
- [OFFSET で捨てるページから payload を読まない](offset-bitmap-gather.md)
- [ABS のページ除外で最初のオーバーフローを保存する](checked-abs-fence.md)

AND/OR 要約、GCD 要約、隣接バイト対の3課題は、[共通のページ評価器](../../../TinylambOpt/ResearchGoals/BlockCountModel.lean)でヘッダから得る件数とペイロードを開く経路を区別する。各 Issue の前提に従い、要約は同じ可視データに対して正しいものとする。
