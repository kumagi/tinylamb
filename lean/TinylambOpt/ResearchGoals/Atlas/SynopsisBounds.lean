import Std

/-!
A query can use a synopsis to avoid inspecting some input only when the claimed bounds
hold for the underlying data.
-/

namespace TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

namespace Samples
/-- Sampling physical row positions without replacement. Equal values do not
collapse samples. Enumerating them here defines a finite exact probability law. -/
def samples {α : Type} : Nat → List α → List (List α)
  | 0,_ => [[]]
  | _+1,[] => []
  | k+1,x::xs => (samples (k+1) xs) ++ (samples k xs).map (x::·)
def choose : Nat → Nat → Nat
  | _,0 => 1
  | 0,_+1 => 0
  | n+1,k+1 => choose n k + choose n (k+1)
def sums (xs : List Int) (k : Nat) := (samples k xs).map List.sum

/-- 結合入力の pilot サンプルで選択行が一つも現れない確率を、組合せ数の比で厳密に表す。 -/
def EmptySample.Goal : Prop := ∀ (xs : List Bool) k,
  (samples k xs).countP (fun s => !s.contains true) = choose (xs.count false) k

/-- 重複値を持つ実データでもサンプル SUM の全標本合計から無偏な全体推定を作れる。 -/
def SampleMean.Goal : Prop := ∀ xs k, 0 < k → k ≤ xs.length →
  (sums xs k).sum = (choose (xs.length-1) (k-1):Int)*xs.sum

/-- 標本 SUM の二次モーメントを二つのフッタ値だけから算出し、pilot のブレを見積もる。 -/
def SampleEnergy.Goal : Prop := ∀ xs k, 2 ≤ k → k ≤ xs.length →
  let q := (xs.map (·^2)).sum
  ((sums xs k).map (·^2)).sum =
    (choose (xs.length-1) (k-1):Int)*q + (choose (xs.length-2) (k-2):Int)*(xs.sum^2-q)

/-- 選択率の全ヒストグラムを、良・不良の個数だけから得る。 -/
def SelectivityLaw.Goal : Prop := ∀ (xs : List Bool) k h, h ≤ k →
  (samples k xs).countP (fun s => s.count true == h) =
    choose (xs.count true) h * choose (xs.count false) (k-h)

/-- シャードごとの局所サンプルを全域サンプルに合成する際、左右の採取数の全分配を含める。 -/
def ShardSampling.Goal : Prop := ∀ (a b : List Int) k,
  (sums (a++b) k).Perm ((List.range (k+1)).flatMap (fun i =>
    (sums a i).flatMap (fun x => (sums b (k-i)).map (x+·))))
end Samples

namespace Couplings
abbrev Rows := List (Int × Int)
def dotRows (rs : Rows) := (rs.map (fun (x,y) => x*y)).sum
def cost (rs : Rows) := (rs.map (fun (x,y) => (x-y).natAbs)).sum
def monotone (rs : Rows) := (rs.map Prod.fst).mergeSort.zip (rs.map Prod.snd).mergeSort
def counter (rs : Rows) := (rs.map Prod.fst).mergeSort.zip (rs.map Prod.snd).mergeSort.reverse

/-- 二列の周辺分布しかない場合でも、結合積和を二つのソート済み対応づけで挟む。 -/
def Rearrangement.Goal : Prop := ∀ rs,
  dotRows (counter rs) ≤ dotRows rs ∧ dotRows rs ≤ dotRows (monotone rs)

/-- 一致しない二列の距離を使うフィルタで、周辺度数から達成しうる最小の総差を計算する。 -/
def TransportLower.Goal : Prop := ∀ rs, cost (monotone rs) ≤ cost rs

/-- 周辺 CDF の差分を区間幅で重み付けして最小輸送費を復元し、個別行の整列を省く。 -/
def PrefixTransport.Goal : Prop := ∀ rs,
  let bins := ((rs.map Prod.fst)++(rs.map Prod.snd)).mergeSort.eraseDups
  cost (monotone rs) = ((bins.zip (bins.drop 1)).map (fun (a,b) =>
    (b-a).toNat * ((rs.countP (·.1 ≤ a):Int)-rs.countP (·.2 ≤ a)).natAbs)).sum

/-- 総距離予算から、許容誤差を超えられる行数の上界を planner に与える。 -/
def FarRows.Goal : Prop := ∀ rs d, (d+1) * rs.countP (fun (a,b) => d < (a-b).natAbs) ≤ cost rs

/-- 強仮説。周辺分布が一致する二列では、行ごとの差もすべて 0 とみなせるか。 -/
def AlignedMarginals.Goal : Prop := ∀ rs, (rs.map Prod.fst).Perm (rs.map Prod.snd) → cost rs = 0
end Couplings

namespace HeavyBounds
abbrev Bag := List Nat
def joinSize (a b : Bag) := (a.map b.count).sum
def rest (xs hot : Bag) := xs.filter (fun x => !hot.contains x)
def captured (a b hot : Bag) := (hot.eraseDups.map (fun x => a.count x*b.count x)).sum
def cap (xs : Bag) := (xs.eraseDups.map xs.count).max?.getD 0
def upper (a b hot : Bag) :=
  let l := rest a hot
  let r := rest b hot
  captured a b hot + min (l.length*cap r) (r.length*cap l)

/-- heavy hitter を別扱いし、残りの両側最大頻度からハッシュ結合の予約量を制限する。 -/
def TailFanout.Goal : Prop := ∀ a b hot, joinSize a b ≤ upper a b hot

/-- hot key を一つ追加して正確な寄与に置き換えるとメモリ見積り上界は増えない。 -/
def MonotoneRefine.Goal : Prop := ∀ a b hot h, upper a b (h::hot) ≤ upper a b hot

/-- キー集合を粗い辞書に投影した結合から偽一致数を求め、粗い計画が割く余剰容量を示す。 -/
def AliasError.Goal : Prop := ∀ a b (bucket : Nat → Nat),
  joinSize (a.map bucket) (b.map bucket) - joinSize a b =
    (a.map (fun x => b.countP (fun y => y != x && bucket x == bucket y))).sum

/-- 二つの独立性を仮定しないスケッチでも、各 build バケットの小さい上界を probe ごとに選べる。 -/
def TwoHashes.Goal : Prop := ∀ a b (h g : Nat → Nat),
  joinSize a b ≤ (a.map (fun x => min ((b.map h).count (h x)) ((b.map g).count (g x)))).sum

/-- ハッシュの組でキー同一性が確定する局所ドメインでは、元の長いキーを読まずに件数を返す。 -/
def JointKey.Goal : Prop := ∀ a b (h g : Nat → Nat),
  (∀ x ∈ a, ∀ y ∈ b, h x = h y → g x = g y → x = y) →
  joinSize a b = (a.map (fun x => b.countP (fun y => h x == h y && g x == g y))).sum
end HeavyBounds

namespace PageMoments
abbrev Page := List Int
def energy (s : Page) (c : Int) := (s.map (fun x => (x-c)^2)).sum
def trimmed (s : Page) (c : Int) (r : Nat) := s.filter (fun x => (x-c).natAbs ≤ r)

/-- ページの平均を丸めなくても、スケール済み距離と二次モーメントで外れ行数を抑える。 -/
def UnroundedTail.Goal : Prop := ∀ (s : Page) (r : Nat),
  (s.countP (fun x => r < ((s.length:Int)*x-s.sum).natAbs):Int)*(r+1:Nat)^2 ≤
    s.length*((s.length:Int)*(s.map (·^2)).sum-s.sum^2)

/-- page header の統計を別の中心に合わせ、同じ集約状態を多数の BETWEEN 述語で共有する。 -/
def ShiftOrigin.Goal : Prop := ∀ s a b,
  energy s b = energy s a + 2*(a-b)*(s.sum-s.length*a) + s.length*(a-b)^2

/-- 外れ部分の二次予算から、トリミングによって変化する中心化 SUM を制限する。 -/
def TrimError.Goal : Prop := ∀ s c r,
  let inside := trimmed s c r
  let err := (s.sum-(s.length:Int)*c) - (inside.sum-(inside.length:Int)*c)
  err^2 ≤ ((s.length-inside.length:Nat):Int)*(energy s c-energy inside c)

/-- 対称区間の外に一行でも存在する最低限のエネルギーに満たなければブロック全体を採用する。 -/
def AllInside.Goal : Prop := ∀ s c r, energy s c < ((r+1:Nat):Int)^2 → trimmed s c r = s

/-- 強仮説。同じ件数・和・二乗和を持つページでは、任意の上側しきい値の件数も共有できるか。 -/
def MomentHistogram.Goal : Prop := ∀ (a b : Page), a.length = b.length → a.sum = b.sum →
  energy a 0 = energy b 0 → ∀ (cut : Int), a.countP (· ≥ cut) = b.countP (· ≥ cut)
end PageMoments

#guard let rows : Couplings.Rows := [(0,1),(1,0)]
       (rows.map Prod.fst).mergeSort == (rows.map Prod.snd).mergeSort && Couplings.cost rows == 2
#guard let a : List Int := [-2,1,1]
       let b : List Int := [-1,-1,2]
       a.length == b.length && a.sum == b.sum && PageMoments.energy a 0 == PageMoments.energy b 0 &&
         a.countP (· ≥ 2) == 0 && b.countP (· ≥ 2) == 1

end TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
