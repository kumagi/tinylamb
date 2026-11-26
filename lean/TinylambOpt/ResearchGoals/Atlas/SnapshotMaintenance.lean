import TinylambOpt.ResearchGoals.ExecutionStrategies.PairAggregates

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.
-/

namespace TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

namespace Versions
/-! Latest physical event by (transaction time, log ordinal) covering a valid
time. A tombstone is an event with no payload, distinct from no candidate. -/
structure Event where
  key : Nat
  tx : Nat
  ordinal : Nat
  since : Int
  stop : Int
  payload : Option Int
  deriving BEq, DecidableEq
def eligible (k t : Nat) (v : Int) (e : Event) := e.key == k && e.tx ≤ t && e.since ≤ v && v < e.stop
def earlier (a b : Event) := a.tx < b.tx || (a.tx == b.tx && a.ordinal < b.ordinal)
def newest (es : List Event) := (es.mergeSort (fun a b => !earlier a b)).head?
def winner (es : List Event) (k t : Nat) (v : Int) := newest (es.filter (eligible k t v))
def readAt (es : List Event) (k t : Nat) (v : Int) := (winner es k t v).bind Event.payload
def frontier (es : List Event) (t : Nat) := (es.filter (·.tx ≤ t)).filter (fun e => !es.any
  (fun f => f.tx ≤ t && f.key == e.key && earlier e f &&
    f.since ≤ e.since && e.stop ≤ f.stop))

/-- 長方形を丸ごと覆う新しい版がある古いイベントを、固定スナップショットの索引から取り除く。 -/
def DominancePrune.Goal : Prop := ∀ es k t v, readAt (frontier es t) k t v = readAt es k t v

/-- ログの各 run から高々一件ずつ集め、tie の安定順を保って最新イベントを選ぶ。 -/
def RunTournament.Goal : Prop := ∀ (runs : List (List Event)) k t v,
  winner runs.flatten k t v = newest (runs.filterMap (fun r => winner r k t v))

/-- 有効時刻の区間端の間では勝者が一定なので、先頭時刻で一度だけ照会する。 -/
def SlabReuse.Goal : Prop := ∀ es k t l h, l ≤ h →
  (∀ e ∈ es, e.key = k → e.tx ≤ t → (e.since ≤ l ∨ h < e.since) ∧ (e.stop ≤ l ∨ h < e.stop)) →
  ∀ v, l ≤ v → v ≤ h → winner es k t v = winner es k t l

/-- 追記イベントが古い勝者より確実に後なら、影響矩形内の点だけを直接置換する。 -/
def LocalPublish.Goal : Prop := ∀ es e k t v, (∀ old ∈ es, earlier old e = true) →
  readAt (es++[e]) k t v = if eligible k t v e then e.payload else readAt es k t v

/-- 強仮説。payload を持つ候補だけを run から返して tombstone の伝送も省けるか。 -/
def DropTombstones.Goal : Prop := ∀ es k t v,
  readAt (es.filter (fun e => e.payload.isSome)) k t v = readAt es k t v
end Versions

namespace IntervalDeletes
abbrev Span := Nat × Nat
def hit (r : Span) (i : Nat) := r.1 ≤ i && i < r.2
def blocked (rs : List Span) (i : Nat) := rs.any (hit · i)
def live (n : Nat) (rs : List Span) := (List.range n).filter (fun i => !blocked rs i)
def depth (rs : List Span) (i : Nat) : Int :=
  (rs.countP (fun r => r.1 < r.2 && r.1 ≤ i):Int) - rs.countP (fun r => r.1 < r.2 && r.2 ≤ i)
def knots (n : Nat) (rs : List Span) :=
  (0 :: n :: rs.flatMap (fun (a,b) => [min a n,min b n])).mergeSort.eraseDups
def pieces (n : Nat) (rs : List Span) := let ks := knots n rs; ks.zip (ks.drop 1)
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-- 位置削除を符号付き端点に変え、重複・空区間が混ざっても深さゼロの行だけを読む。 -/
def SignedEndpoints.Goal : Prop := ∀ rs i, blocked rs i = decide (0 < depth rs i)

/-- 区間の間にある無削除の連続長と ordinal 和をフッタだけから求める。 -/
def FooterRuns.Goal : Prop := ∀ n rs,
  let gaps := (pieces n rs).filter (fun r => depth rs r.1 == 0)
  ((live n rs).length,(live n rs).sum) =
    ((gaps.map (fun (a,b) => b-a)).sum,(gaps.map (fun (a,b) => choose2 b-choose2 a)).sum)

/-- 削除ログ二群の union を重複適用せず、生存行の差分だけでファイルの残数を更新する。 -/
def OverlapDebt.Goal : Prop := ∀ n a b, (live n (a++b)).length +
  ((live n a).countP (blocked b)) = (live n a).length

/-- 有効行の第 k ordinal を、区間単位の残数を使って seek する。 -/
def seek : List Span → Nat → Option Nat
  | [],_ => none
  | (a,b)::rs,k => if k < b-a then some (a+k) else seek rs (k-(b-a))
def LiveSelect.Goal : Prop := ∀ n rs (k : Nat),
  seek ((pieces n rs).filter (fun r => !blocked rs r.1)) k = (live n rs)[k]?

/-- 連続ページで使う局所 ordinal に区間を平行移動し、負になる端だけ飽和させる。 -/
def RebaseMask.Goal : Prop := ∀ base n rs,
  ((List.range n).filter (fun i => !blocked rs (base+i))) =
    live n (rs.map (fun (a,b) => (a-base,b-base)))
end IntervalDeletes

namespace ThresholdViews
abbrev Bag := List (Nat × Int)
def weight (xs : Bag) (k : Nat) := ((xs.filter (fun r => r.1 == k)).map Prod.snd).sum
def keys (xs : Bag) := (xs.map Prod.fst).eraseDups
def active (xs : Bag) (k : Nat) := decide (0 < weight xs k)
def indicator (b : Bool) : Int := if b then 1 else 0
def count (xs : Bag) := (keys xs).countP (active xs)
def changes (old patch : Bag) := (keys patch).map (fun k =>
  (k,indicator (active (old++patch) k)-indicator (active old k)))

/-- COUNT DISTINCT の差分を、符号付き多重度がゼロ境界を跨いだキーだけで更新する。 -/
def ZeroCrossing.Goal : Prop := ∀ old patch,
  (count (old++patch):Int) - count old = ((changes old patch).map Prod.snd).sum

/-- 二回の訂正バッチを途中公開しても、一回に結合した場合と発行したビュー差分が一致する。 -/
def Coalesce.Goal : Prop := ∀ old a b k, weight (changes old (a++b)) k =
  weight (changes old a ++ changes (old++a) b) k

/-- セミ結合の左重み付き集計を、右の活性状態を跨ぐキーの左寄与だけで直す。 -/
def semi (left right : Bag) := ((left.filter (fun r => active right r.1)).map Prod.snd).sum
def SemijoinRepair.Goal : Prop := ∀ left right patch,
  semi left (right++patch) - semi left right =
    ((changes right patch).map (fun (k,d) => d*weight left k)).sum

/-- しきい値まで余裕がある hot key に対し、バッチの絶対変更量で DISTINCT 更新を棄却する。 -/
def MarginCertificate.Goal : Prop := ∀ old patch k,
  (weight old k).natAbs > ((patch.filter (fun r => r.1 == k)).map (fun r => r.2.natAbs)).sum →
  active old k = active (old++patch) k

/-- 差分をさらに符号付きキーバッグとして保持し、過去の活性索引を問い合わせ時に再構成する。 -/
def ReplayLedger.Goal : Prop := ∀ old p k,
  indicator (active old k) + weight (changes old p) k = indicator (active (old++p) k)
end ThresholdViews

namespace BatchWatermarks
abbrev Row := Nat × Int
def visible (t : Nat) (xs : List Row) := xs.filter (·.1 ≤ t)
def score (t : Nat) (xs : List Row) := ((visible t xs).map Prod.snd).sum
def stamp (xs : List Row) := (xs.map Prod.fst).max?.getD 0
def summaries (runs : List (List Row)) := runs.map (fun r => (stamp r,(r.map Prod.snd).sum))
def rounded (step : Nat) (xs : List Row) := xs.map (fun (t,w) => (t/step,w))

/-- 高水位以下の run はフッタ状態を採用し、水位を跨ぐ run だけ開く。 -/
def PartialOpen.Goal : Prop := ∀ runs t, score t runs.flatten =
  score t (summaries runs) + ((runs.filter (fun r => t < stamp r)).map (score t)).sum

/-- epoch を粗くした状態から境界バケットの将来寄与だけを差し引いて任意時点を復元する。 -/
def CoarseTime.Goal : Prop := ∀ xs t step, 0 < step →
  score t xs = score (t/step) (rounded step xs) -
    ((xs.filter (fun r => r.1/step == t/step && t < r.1)).map Prod.snd).sum

/-- 遅延到着が時刻範囲内だけなら、過去二時点間の差分はその範囲の寄与だけで訂正する。 -/
def HistoricalBand.Goal : Prop := ∀ xs late lo hi, lo ≤ hi →
  (score hi (xs++late)-score lo (xs++late)) - (score hi xs-score lo xs) =
    ((late.filter (fun r => lo < r.1 && r.1 ≤ hi)).map Prod.snd).sum

/-- 全ての時間 prefix に対する二つの訂正ログの交換可能性を時刻ごとの正味寄与で判定する。 -/
def EquivalentHistory.Goal : Prop := ∀ a b, (∀ t, score t a = score t b) ↔
  ∀ t, ((a.filter (fun r => r.1 == t)).map Prod.snd).sum =
       ((b.filter (fun r => r.1 == t)).map Prod.snd).sum

/-- 強仮説。公開時刻と最終総和が同じ二つの run を全ての中間スナップショットで共有できるか。 -/
def FinalSummary.Goal : Prop := ∀ a b, stamp a = stamp b → (a.map Prod.snd).sum = (b.map Prod.snd).sum →
  ∀ t, score t a = score t b
end BatchWatermarks

#guard let log : List Versions.Event := [⟨3,1,0,0,10,some 7⟩,⟨3,2,0,0,10,none⟩]
       Versions.readAt log 3 2 1 == none &&
         Versions.readAt (log.filter (fun e => e.payload.isSome)) 3 2 1 == some 7
#guard let a : List BatchWatermarks.Row := [(0,4),(1,-4)]
       let b : List BatchWatermarks.Row := [(0,0),(1,0)]
       BatchWatermarks.stamp a == BatchWatermarks.stamp b &&
         (a.map Prod.snd).sum == (b.map Prod.snd).sum &&
         BatchWatermarks.score 0 a == 4 && BatchWatermarks.score 0 b == 0

end TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
