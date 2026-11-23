# 符号付き加算の失敗レーンをマスクで求める

**モデル上で証明済み。** 選択済みレーンの nullable INT64 加算を wrapping add と符号ビットの論理式に変換し、失敗をマスクに集める。

## 提出型

[`CheckedLaneAdd.Goal`](../../../TinylambOpt/ResearchGoals/CheckedLaneAdd.lean) の証明は [`ResearchProofs/CheckedLaneAdd.lean`](../../../TinylambOpt/ResearchProofs/CheckedLaneAdd.lean) の `TinylambOpt.ResearchProofs.CheckedLaneAdd.sound` にある。

数学整数で範囲検査した各セルの状態と値が一致し、失敗マスクの最初のレーンが逐次評価の最初の失敗になること。非活性と SQL NULL は別の結果にした。

## 境界例と適用範囲

全レーンの生値が MAX+1 でも、選択・validity を適用するとエラーはレーン1・2だけ。MIN に −1 を足すケースも含む。LLVM に落とす際は wrapping add の契約が必要。
