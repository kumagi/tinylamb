import TinylambOpt.ResearchGoals.Atlas.RangePlans
import TinylambOpt.ResearchProofs.Atlas.RangePlans.WeekSessionAndVarianceIndexes

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `OverdueOrLost.ProbeTransposition` — `∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
  (ps.map (fun (p,w) => w * scan shape p rs)).sum = (rs.map (fun (r,w) => w * atPoint
  (compile shape b ps) r)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_OverdueOrLost_ProbeTransposition

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.OverdueOrLost

/-! `OverdueOrLost` のスタンシルは、期限より左かつ確定額より上にある行を
    負の b 側の矩形で、座標が一致し y=0 の行を単位矩形で表す。
    有界な build 行ではこの二矩形の符号付き重み和が
    `shape.qualifies` の指示器に等しくなる（端が b で切られることもない）。
    よって共通補題 `probeTransposition_of_stencil` を適用すれば、
    プローブ側の和とイベント側の和は行ごとに入れ替えて一致する。
    小さい入力の総当たり（b=0,1、座標 -2..2、プローブ/行リスト長 0..2、
    重み ±1）でも両辺は一致することを確認済み。 -/
theorem sound_OverdueOrLost_ProbeTransposition :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.OverdueOrLost.ProbeTransposition.Goal := by
  intro b ps rs hb
  exact (TinylambOpt.ResearchProofs.Atlas.RangePlans.WeekSessionAndVarianceIndexes.OverdueOrLost.sound_OverdueOrLost_ProbeTransposition)
    b ps rs hb

end TinylambOpt.ResearchProofs.Atlas.Goal_OverdueOrLost_ProbeTransposition
