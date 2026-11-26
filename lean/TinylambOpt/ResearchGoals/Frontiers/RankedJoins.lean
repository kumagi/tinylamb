import Std

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.
-/

namespace TinylambOpt.ResearchGoals.Frontiers.RankedJoins

/-! Ranked queries. Observations are bags of scores in ascending order (negate
scores for descending order). A payload tie-break must be added for ordered rows. -/
def best (k : Nat) (xs : List Int) := xs.mergeSort.take k
def sums (a b : List Int) := a.flatMap (fun x => b.map (x+·))
def qualified (a b : List Int) (ok : Int → Int → Bool) :=
  a.flatMap (fun x => (b.filter (ok x)).map (x+·))

/-- 順序保存的な積の上位 K は両入力の上位 K だけで計算し、巨大な直積を切り詰める。 -/
def BoundedProduct.Goal : Prop := ∀ a b k, best k (sums a b) = best k (sums (best k a) (best k b))

/-- 各行・各列の残余述語による欠落が最大 h 個なら、両入力 K+h 行から正しい上位を得る。 -/
def HolesPerAxis.Goal : Prop := ∀ a b ok k h,
  (∀ x ∈ a, b.countP (fun y => !ok x y) ≤ h) →
  (∀ y ∈ b, a.countP (fun x => !ok x y) ≤ h) →
  best k (qualified a b ok) = best k (qualified (best (k+h) a) (best (k+h) b) ok)

/-- 強仮説。欠落数の平均が h 以下という統計でも、各軸 K+h の境界を使えるか。 -/
def MeanHoleBudget.Goal : Prop := ∀ a b ok k h,
  (a.map (fun x => b.countP (fun y => !ok x y))).sum ≤ h * min a.length b.length →
  best k (qualified a b ok) = best k (qualified (best (k+h) a) (best (k+h) b) ok)

/-- 閾値以下の組数をソート済み片側の prefix 長から求め、第 K スコアを列挙なしで判定する。 -/
def RankByCut.Goal : Prop := ∀ a b k (bound : Int), k < a.length*b.length →
  ((sums a b).mergeSort[k]?.any (· ≤ bound) = true ↔
    k < (a.map (fun x => (b.mergeSort.takeWhile (· ≤ bound-x)).length)).sum)

def pick : List (Int × Nat) → Nat → Option Int
  | [],_ => none
  | (v,w)::xs,k => if k < w then some v else pick xs (k-w)

/-- 重複キーを展開せず、重み付き順位が指す一点を返す。重みゼロのキーも扱う。 -/
def WeightedSeek.Goal : Prop := ∀ (rows : List (Int × Nat)) k, pick (rows.mergeSort (fun a b => a.1 ≤ b.1)) k =
  (rows.flatMap (fun (v,w) => List.replicate w v)).mergeSort[k]?

/-- DISTINCT の前で上位を切るため、物理スコアの最大重複度を証明書に使う。 -/
def DistinctOverfetch.Goal : Prop := ∀ (xs : List Int) k cap, (∀ x ∈ xs, xs.count x ≤ cap) →
  ((best (k*cap) xs).eraseDups.take k) = best k xs.eraseDups

/-- 既知の勝者より確実に悪い区画を外し、同点を出す区画は開く。証明書には区画の最小スコアのみ必要。 -/
def BoxCertificate.Goal : Prop := ∀ (boxes : List (Int × List Int)) (seed : List Int) k cutoff,
  k ≤ seed.countP (· ≤ cutoff) → (∀ b ∈ boxes, ∀ v ∈ b.2, b.1 ≤ v) →
  best k (seed ++ boxes.flatMap Prod.snd) =
    best k (seed ++ (boxes.filter (·.1 ≤ cutoff)).flatMap Prod.snd)

/-- K 番目以降に同点が続く WITH TIES を重み付き境界の二項（厳密未満と同値）から復元する。 -/
def TieMass.Goal : Prop := ∀ (rows : List (Int × Nat)) k cutoff,
  pick (rows.mergeSort (fun a b => a.1 ≤ b.1)) k = some cutoff →
  let xs := rows.flatMap (fun (v,w) => List.replicate w v)
  let lt := ((rows.filter (·.1 < cutoff)).map Prod.snd).sum
  let eq := ((rows.filter (·.1 == cutoff)).map Prod.snd).sum
  lt ≤ k ∧ k < lt+eq ∧ (xs.mergeSort.takeWhile (· ≤ cutoff)).length = lt+eq

/-- 隣接時刻対の距離が d 以下の組数を、ソート後の左右対称な帯の長さから計算する。 -/
def NearPairs.Goal : Prop := ∀ (xs : List Int) (d : Nat),
  let a := xs.mergeSort
  let directed := (a.map (fun x => (a.takeWhile (· ≤ x+(d:Int))).length -
    (a.takeWhile (· < x-(d:Int))).length)).sum
  (directed-a.length)/2 =
    (xs.zipIdx.map (fun (x,i) => (xs.drop (i+1)).countP (fun y => (x-y).natAbs ≤ d))).sum

/-- 誤差区間の上端が落選側の全下端より小さければ、上位集合だけは近似値で確定できる。 -/
def IntervalWinners.Goal : Prop := ∀ (winners rest : List Nat) (lo hi score : Nat → Int),
  (∀ x ∈ winners ++ rest, lo x ≤ score x ∧ score x ≤ hi x) →
  (∀ x ∈ winners, ∀ y ∈ rest, hi x < lo y) →
  ((winners ++ rest).mergeSort (fun x y => score x ≤ score y)).take winners.length =
    winners.mergeSort (fun x y => score x ≤ score y)

private def axis := (List.range 9).map Int.ofNat
private def allowed (x y : Int) := !(x < 2 && y < 2)
#guard (axis.map (fun x => axis.countP (fun y => !allowed x y))).sum == 4 && axis.length == 9
#guard best 1 (qualified axis axis allowed) == [2] &&
  best 1 (qualified (best 2 axis) (best 2 axis) allowed) == []
#guard best 3 (sums [-1,-1,8] [2,7]) == [1,1,6]
#guard pick [(1,0),(2,2),(3,1)] 2 == some 3

end TinylambOpt.ResearchGoals.Frontiers.RankedJoins
