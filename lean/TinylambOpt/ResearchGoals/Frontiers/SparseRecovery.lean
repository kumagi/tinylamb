import Std

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.
-/

namespace TinylambOpt.ResearchGoals.Frontiers.SparseRecovery

/-! Iceberg deletes and mixed refresh. Ordinals denote identities within a pinned file.
Order-independent exact summaries use unbounded integers. Multiplicity is intentional. -/
def moment (xs : List Nat) (k : Nat) := (xs.map (fun x => x^k)).sum
def poly (xs : List Nat) (z : Int) := (xs.map (fun (x : Nat) => z-(x:Int))).prod
def probes (n : Nat) (xs : List Nat) := (List.range n).map (fun (z : Nat) => poly xs z)

/-- 最大 d 個の削除 ID の袋を d 次までのべき和で一意に復元できるか。0 次は件数を担う。 -/
def BoundedMoments.Goal : Prop := ∀ (a b : List Nat) d, a.length ≤ d → b.length ≤ d →
  (∀ k ≤ d, moment a k = moment b k) → a.Perm b

/-- 削除が二行以内のファイルでは候補位置を二次式で判定し、位置ビットマップを省く。 -/
def TwoRootMask.Goal : Prop := ∀ (xs : List Nat) p, xs.length = 2 →
  (p ∈ xs ↔ 2*p*p + moment xs 1 ^ 2 = 2*p*moment xs 1 + moment xs 2)

/-- 件数と一次・二次モーメントから、追加一行と削除一行の位置をログ展開なしで復元する。 -/
def SwapDecoder.Goal : Prop := ∀ (base : List Nat) (removed added : Nat), removed ≠ added →
  let d := (moment (added::base) 1:Int) - moment (removed::base) 1
  let q := (moment (added::base) 2:Int) - moment (removed::base) 2
  ((q/d-d)/2,(q/d+d)/2) = ((removed:Int),(added:Int))

/-- 同じ件数のファイル内容を、最高次が既知の特性多項式の d 点評価で正確に比較する。 -/
def MonicFingerprint.Goal : Prop := ∀ (a b : List Nat), a.length = b.length →
  probes a.length a = probes a.length b → a.Perm b

/-- 入出力が巨大でも変更量が疎なら、多項式比の交差積から削除・挿入の正味の差を復元する。 -/
def RationalDelta.Goal : Prop := ∀ (ins del altIns altDel : List Nat) d,
  ins.length + altDel.length ≤ d → del.length + altIns.length ≤ d →
  (∀ z ≤ d, poly ins (z:Nat) * poly altDel z = poly del z * poly altIns z) →
  ∀ x, (ins.count x:Int) - del.count x = (altIns.count x:Int) - altDel.count x

/-- 平方和が分散ゼロを示すバケットなら、全重複を一つの ID と件数から再生する。 -/
def UniformBucket.Goal : Prop := ∀ (xs : List Nat), xs ≠ [] →
  xs.length * moment xs 2 = moment xs 1 ^ 2 →
  xs = List.replicate xs.length (moment xs 1 / xs.length)

/-- 強仮説。三つの削除でも件数・和・平方和だけで正しい位置マスクを共有できるか。 -/
def ThreeFromTwo.Goal : Prop := ∀ (a b : List Nat), a.length = 3 → b.length = 3 →
  moment a 1 = moment b 1 → moment a 2 = moment b 2 → ∀ p, a.contains p = b.contains p

def buckets (xs : List Nat) (h : Nat → Nat) (key : Nat) := xs.countP (fun x => h x == h key)

/-- 大きな ordinal 空間を複数の小さい辞書へ射影し、各候補の混同行数の最小値を削除数の上界にする。 -/
def IsolatedHash.Goal : Prop := ∀ xs (hashes : List (Nat → Nat)) key,
  (∃ h ∈ hashes, ∀ x ∈ xs, h x = h key → x = key) →
  (hashes.map (fun h => buckets xs h key)).min? = some (xs.count key)

/-- バケット単位で既知の削除を剥がすと、新たに純粋になったバケットの decoded ID も正しくなる。 -/
def PeelingVariance.Goal : Prop := ∀ (known live : List Nat), live ≠ [] →
  let combined := known ++ live
  let n := combined.length - known.length
  let s := moment combined 1 - moment known 1
  let q := moment combined 2 - moment known 2
  n*q = s*s → ∀ x ∈ live, x = s/n

/-- 小さな二表の積結合でキー一致が存在するかを、片方の特性多項式の他方全キーでの積で判定する。 -/
def DisjointResultant.Goal : Prop := ∀ (a b : List Nat),
  (b.map (fun (x : Nat) => poly a x)).prod = 0 ↔ ∃ x ∈ a, x ∈ b

#guard moment [0,4,5] 1 == moment [1,2,6] 1 && moment [0,4,5] 2 == moment [1,2,6] 2
#guard [0,4,5].contains 4 && ![1,2,6].contains (4 : Nat)
#guard probes 2 [0,0] != probes 2 [0,1] && poly [] 0 == 1

end TinylambOpt.ResearchGoals.Frontiers.SparseRecovery
