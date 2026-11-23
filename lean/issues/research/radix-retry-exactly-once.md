# radix 再分割・再送の実行履歴から一回消費を導く

[adaptive radix](adaptive-radix-replay.md) の差し戻しから、所有権定理は `lean/TinylambOpt/ResearchProofs/RadixReplayOwnership.lean` の `theorem sound` で証明済み。既設の型をそのまま使う。入力に要求するのは出現 ID の一意性。

## 証明対象

[`RadixReplayOwnership.Goal`](../../TinylambOpt/ResearchGoals/RadixReplayOwnership.lean) を import し、提出済みの `theorem sound` は `ResearchProofs/RadixReplayOwnership.lean` にあります。

任意の drain・split・再送の接頭辞で、消費済みと pending の和が元の多重集合に等しく、各 pending 行の radix prefix と所有トークンが正しい。最後に全件を drain したときの一回消費と各グループの結果も示す。部分消費後の親を再送する境界例が同モジュールにある。

## 適用範囲

原子的な drain のモデル。クラッシュ中の spill 書込みについては永続化と所有権移転の順序を別途定める。
