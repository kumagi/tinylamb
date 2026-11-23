# spill 再分割中の Bloom を probe に公開する

## 証明済みの命題

証明: `lean/TinylambOpt/Research/SpillBloomEpoch.lean` の主定理。

build の一部だけ完了した状態でも、完了証明を持つパーティションだけを除外すれば全量 build と等しい。

## モデルと適用条件

epoch とハッシュ設定を子分割へ引き継ぐ。古い要約、未封印、再読失敗の観測を定義する。

## 回帰例

親が空を報告した後に子の遅延ページから一致キーが届く。先に probe を捨てると結果が減る。

## 完了条件

任意の封印・spill・probe の有効な順序で証明し、偽陽性が追加 I/O だけを起こすことを示す。

実装との照合先は [executor/detail/pipeline/ooc_hash_join_sink.cpp](../../../executor/detail/pipeline/ooc_hash_join_sink.cpp)。
