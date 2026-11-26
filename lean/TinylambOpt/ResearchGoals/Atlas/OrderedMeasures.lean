import TinylambOpt.ResearchGoals.ExecutionStrategies.PairAggregates

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.
-/

namespace TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! A partition of non-NULL exact integer values, in a deterministic SQL order.
windows enumerates every nonempty contiguous frame once. Pairs/triples use
physical row positions; equal values in distinct positions remain distinct. -/
abbrev Series := List Int
def size (x : Series) : Int := x.length
def shifted (b : Int) (x : Series) := x.map (·+b)
def scaled (a : Int) (x : Series) := x.map (a*·)
def repeated (n : Nat) (a : Int) := List.replicate n a
def adj (x : Series) := x.zip (x.drop 1)
def pairs (x : Series) := x.zipIdx.flatMap (fun (a,i) => (x.drop (i+1)).map (fun b => (a,b)))
def triples (x : Series) := x.zipIdx.flatMap (fun (a,i) =>
  (pairs (x.drop (i+1))).map (fun (b,c) => (a,b,c)))
def cart (x y : Series) := x.flatMap (fun a => y.map (fun b => (a,b)))
def seam (x y : Series) (f : Int → Int → Int) :=
  (x.getLast?.bind (fun a => y.head?.map (f a))).getD 0
def prefs (x : Series) := (List.range x.length).map (fun i => (x.take (i+1)).sum)
def suffs (x : Series) := (List.range x.length).map (fun i => (x.drop i).sum)
def windows (x : Series) := (List.range x.length).flatMap (fun i =>
  (List.range (x.length-i)).map (fun j => (x.drop i).take (j+1)))
def max0 (x : Series) := max 0 (x.max?.getD 0)
open TinylambOpt.ResearchGoals.PairAggregates (choose2)
def choose3 (n : Nat) := n*(n-1)*(n-2)/6
def delta (x : Series) := (adj x).map (fun (a,b) => b-a)
def distance (a b : Int) : Int := (b-a).natAbs
def spread (x : Series) := x.max?.getD 0 - x.min?.getD 0
def matchesFirst (x : Series) := (x.takeWhile (fun a => some a == x.head?)).length
def rising (t : Int × Int) := decide (t.1 < t.2)
def falling (t : Int × Int) := decide (t.2 < t.1)
def strict3 (t : Int × Int × Int) := t.1 < t.2.1 && t.2.1 < t.2.2

namespace TotalVariation

/-- 隣接価格変動の絶対値和。 -/
def measure (x : Series) : Int := (delta x).map (fun v => (v.natAbs:Int)) |>.sum

/-- 区画境界の一対だけを補う。 -/
def Seam.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + seam x y distance

/-- 通貨換算を集約後に一度だけ掛ける。 -/
def Rescale.Goal : Prop :=
  ∀ a x,
  measure (scaled a x) = (a.natAbs:Int)*measure x

/-- 昇順ページを両端だけで評価する。 -/
def Monotone.Goal : Prop :=
  ∀ x, x.Pairwise (· ≤ ·) →
  measure x = spread x

/-- 連続複製したサンプルを代表一点に畳む。 -/
def DropDuplicates.Goal : Prop :=
  ∀ x n, 0 < n →
  measure (x.flatMap (repeated n)) = measure x

/-- 内部を未読のまま両端から HAVING 下限を出す。 -/
def EndpointFence.Goal : Prop :=
  ∀ x, distance (x.head?.getD 0) (x.getLast?.getD 0) ≤ measure x

end TotalVariation

namespace QuadraticVariation

/-- ジャンプの二乗損失。 -/
def measure (x : Series) : Int := ((delta x).map (·^2)).sum

/-- 境界ジャンプだけを残差として結合する。 -/
def SeamEnergy.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + seam x y (fun a b => (b-a)^2)

/-- 列の再正規化を平方係数に縮約する。 -/
def Affine.Goal : Prop :=
  ∀ a b x,
  measure (shifted b (scaled a x)) = a^2*measure x

/-- 二乗和の予算から区画の端点差を制限する。 -/
def EndpointEnergy.Goal : Prop :=
  ∀ x, (x.getLast?.getD 0-x.head?.getD 0)^2 ≤ (x.length-1:Int)*measure x

/-- 等差ページを隣接比較なしで集計する。 -/
def LinearRamp.Goal : Prop :=
  ∀ (n : Nat) (a d : Int),
  measure ((List.range n).map (fun (i : Nat) => a+d*i)) = ((n-1:Nat):Int)*d^2

/-- 一点の訂正で両隣に接する二差分だけを直す。 -/
def LocalEdit.Goal : Prop :=
  ∀ x y a b,
  measure (x++b::y)-measure (x++a::y) = seam x [b] (fun u v => (v-u)^2) - seam x [a] (fun u v => (v-u)^2) + seam [b] y (fun u v => (v-u)^2) - seam [a] y (fun u v => (v-u)^2)

end QuadraticVariation

namespace RisePairs

/-- 前方の値がより小さい行対の個数。 -/
def measure (x : Series) : Int := ((pairs x).countP rising : Int)

/-- 左右の値別頻度から境界を跨ぐ上昇対を得る。 -/
def HistogramBridge.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + ((x.eraseDups.map (fun a => x.count a * y.countP (a < ·))).sum:Nat)

/-- 逆順走査の結果を同点対で補正する。 -/
def ReverseDebt.Goal : Prop :=
  ∀ x,
  measure x + measure x.reverse + (x.eraseDups.map (fun a => choose2 (x.count a))).sum = (choose2 x.length:Int)

/-- 厳密に順位を保つ辞書への変換で評価する。 -/
def RankCoding.Goal : Prop :=
  ∀ x (f : Int →
  Int), (∀ a ∈ x, ∀ b ∈ x, f a < f b ↔ a < b) →
  measure (x.map f) = measure x

/-- 同じ区画の多数の反復を二項係数でまとめる。 -/
def BlockRepeat.Goal : Prop :=
  ∀ x n,
  measure (List.replicate n x |>.flatten) = (n:Int)*measure x + (choose2 n:Int)*(cart x x).countP rising

/-- ソート済みブロックの全対数から同値ペアだけ落とす。 -/
def MergeSorted.Goal : Prop :=
  ∀ x, x.Pairwise (· ≤ ·) →
  measure x = (choose2 x.length:Int) - (x.eraseDups.map (fun a => choose2 (x.count a))).sum

end RisePairs

namespace TiePairs

/-- 同一キーの自己結合で順序を持つ行対の個数。 -/
def measure (x : Series) : Int := ((pairs x).countP (fun (a,b) => a == b) : Int)

/-- 各頻度の二項係数で組数を求める。 -/
def Multiplicity.Goal : Prop :=
  ∀ x,
  measure x = ((x.eraseDups.map (fun a => choose2 (x.count a))).sum:Int)

/-- 同じキーだけの削除バッチを頻度から処理する。 -/
def DeleteBatch.Goal : Prop :=
  ∀ x a n,
  measure (repeated n a ++ x) - measure x = (choose2 n:Int)+(n:Int)*x.count a

/-- キー領域が交わらない二つの区画でクロス検査を省く。 -/
def DisjointKeys.Goal : Prop :=
  ∀ x y, (∀ a ∈ x, a ∉ y) →
  measure (x++y) = measure x + measure y

/-- 非単射辞書で新たに生じる偽一致の数を数える。 -/
def CollisionPenalty.Goal : Prop :=
  ∀ x (f : Int →
  Int),
  measure (x.map f) - measure x = (pairs x).countP (fun (a,b) => a != b && f a == f b)

/-- 一キーの最大重複度だけで二次中間表を制限する。 -/
def FanoutBound.Goal : Prop :=
  ∀ x (c : Nat), (∀ a ∈ x, x.count a ≤ c) →
  2*measure x ≤ size x*(c-1:Nat)

end TiePairs

namespace Drawdown

/-- 先行高値から後続安値への最大下落。 -/
def measure (x : Series) : Int := max0 ((pairs x).map (fun (a,b) => a-b))

/-- 区画内最大と左最大・右最小で結合する。 -/
def CrossExtreme.Goal : Prop :=
  ∀ x y, x ≠ [] →
  y ≠ [] →
  measure (x++y) = max (max (measure x) (measure y)) (x.max?.getD 0-y.min?.getD 0)

/-- 両方向の最大下落からレンジ幅を回収する。 -/
def Bidirectional.Goal : Prop :=
  ∀ x, max (measure x) (measure x.reverse) = spread x

/-- 逆方向の評価を価格の符号反転と交換する。 -/
def QuoteFlip.Goal : Prop :=
  ∀ x,
  measure (scaled (-1) x) = measure x.reverse

/-- 行が間引かれた区画では元の値を上界に使う。 -/
def DeleteMonotone.Goal : Prop :=
  ∀ x (keep : Int →
  Bool),
  measure (x.filter keep) ≤ measure x

/-- 同一区画を二回以上再生すると内部時系列を見ず全レンジを返せる。 -/
def DuplicatePeriods.Goal : Prop :=
  ∀ x n, 2 ≤ n →
  measure ((List.replicate n x).flatten) = spread x

end Drawdown

namespace BestFrame

/-- 任意の連続フレームの最大利益（空フレーム可）。 -/
def measure (x : Series) : Int := max0 ((windows x).map List.sum)

/-- 左右の最良接尾・接頭を結んで最適フレームを得る。 -/
def CrossFrame.Goal : Prop :=
  ∀ x y,
  measure (x++y) = max (max (measure x) (measure y)) (max0 (suffs x)+max0 (prefs y))

/-- 周期列の中間周期を正のときだけまとめて採用する。 -/
def RepeatedGain.Goal : Prop :=
  ∀ x n, 2 ≤ n →
  measure ((List.replicate n x).flatten) = max (measure x) (max0 (suffs x)+max0 (prefs x)+((n-2:Nat):Int)*max 0 x.sum)

/-- 全行が非正なら局所フレーム探索を省く。 -/
def NegativePage.Goal : Prop :=
  ∀ x, (∀ v ∈ x, v ≤ 0) →
  measure x = 0

/-- 累積和上の反方向の最大下落へ変換する。 -/
def PrefixDrawdown.Goal : Prop :=
  ∀ x,
  measure x = Drawdown.measure ((0::prefs x).reverse)

/-- 取り除いた負値の絶対和だけで最適値の増加を抑える。 -/
def DeletionFence.Goal : Prop :=
  ∀ x (keep : Int →
  Bool),
  measure (x.filter keep) ≤ measure x + ((x.filter (fun v => !keep v)).map (fun v => max 0 (-v))).sum

end BestFrame

namespace RunTransitions

/-- 隣接する辞書コードの切替回数。 -/
def measure (x : Series) : Int := ((adj x).countP (fun (a,b) => a != b) : Int)

/-- 両区画の代表端点を一度照合する。 -/
def PageBoundary.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + seam x y (fun a b => if a == b then 0 else 1)

/-- 粗い辞書で消える切替だけを特定する。 -/
def CoarseDictionary.Goal : Prop :=
  ∀ x (f : Int →
  Int),
  measure x - measure (x.map f) = (adj x).countP (fun (a,b) => a != b && f a == f b)

/-- 間引く前の切替数を索引による読取りの上限にする。 -/
def MaskedSubsequence.Goal : Prop :=
  ∀ x (keep : Int →
  Bool),
  measure (x.filter keep) ≤ measure x

/-- 同値区画を反復したときの切替を定数時間で得る。 -/
def PeriodicBoundary.Goal : Prop :=
  ∀ x n,
  measure ((List.replicate n x).flatten) = (n:Int)*measure x + ((n-1:Nat):Int)*seam x x (fun a b => if a == b then 0 else 1)

/-- 昇順ソート済みなら辞書長で切替数を置き換える。 -/
def SortedDictionary.Goal : Prop :=
  ∀ x, x.Pairwise (· ≤ ·) →
  measure x = ((x.eraseDups.length-1:Nat):Int)

end RunTransitions

namespace PlateauFrames

/-- 等しい値だけからなる非空連続フレーム数。 -/
def measure (x : Series) : Int := ((windows x).countP (fun w => w.all (fun a => some a == w.head?)) : Int)

/-- 境界の等値ラン長の積だけを追加する。 -/
def BoundaryRuns.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + seam x y (fun a b => if a == b then (matchesFirst x.reverse:Int)*matchesFirst y else 0)

/-- 定数区画の全フレームを三角数で表す。 -/
def Uniform.Goal : Prop :=
  ∀ n a,
  measure (repeated n a) = (choose2 (n+1):Int)

/-- 区画内で単射な局所辞書に変換して評価する。 -/
def InjectiveCodes.Goal : Prop :=
  ∀ x (f : Int →
  Int), (∀ a ∈ x, ∀ b ∈ x, f a = f b →
  a = b) →
  measure (x.map f) = measure x

/-- 各値を n 個ずつ展開するコストを対角と非対角に分ける。 -/
def ExpandRuns.Goal : Prop :=
  ∀ x n,
  measure (x.flatMap (repeated n)) = size x * choose2 (n+1) + (n:Int)^2*(measure x-size x)

/-- 連続性を捨てた同値対から最大状態数を見積もる。 -/
def TieFence.Goal : Prop :=
  ∀ x,
  measure x ≤ size x + TiePairs.measure x

end PlateauFrames

namespace PrefixExposure

/-- 全行の累積残高の合計。 -/
def measure (x : Series) : Int := (prefs x).sum

/-- 各入力の残存行数を係数として一回掛ける。 -/
def Multiplicities.Goal : Prop :=
  ∀ x,
  measure x = (x.zipIdx.map (fun (v,i) => v*((x.length-i:Nat):Int))).sum

/-- 左の終値を右の行数だけ反映して並列化する。 -/
def InheritedBalance.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + x.sum*size y

/-- 逆順側の状態を行数・総和と前方状態から復元する。 -/
def ReverseComplement.Goal : Prop :=
  ∀ x,
  measure x.reverse + measure x = (size x+1)*x.sum

/-- 全行への定額付加を三角数に置き換える。 -/
def Deposit.Goal : Prop :=
  ∀ b x,
  measure (shifted b x) = measure x + b*choose2 (x.length+1)

/-- 周期的なチャンクの n 回分を二項係数でまとめる。 -/
def BatchRepeat.Goal : Prop :=
  ∀ x n,
  measure ((List.replicate n x).flatten) = (n:Int)*measure x + (choose2 n:Int)*size x*x.sum

end PrefixExposure

namespace PrefixEnergy

/-- 累積残高の二乗和。 -/
def measure (x : Series) : Int := ((prefs x).map (·^2)).sum

/-- 後段の一次・二次状態を境界残高で平行移動する。 -/
def OffsetQuadratic.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + 2*x.sum*PrefixExposure.measure y + size y*x.sum^2

/-- 等額ページの三次多項式から集計する。 -/
def ConstantPage.Goal : Prop :=
  ∀ n a, 6*measure (repeated n a) = a^2*(n:Int)*(n+1)*(2*n+1)

/-- 情報量を落とさず狭い単位で集約する。 -/
def UnitScaling.Goal : Prop :=
  ∀ a x,
  measure (scaled a x) = a^2*measure x

/-- 累積状態の補集合から逆順のエネルギーを読む。 -/
def ReverseEnergy.Goal : Prop :=
  ∀ x,
  measure x.reverse = measure x - 2*x.sum*PrefixExposure.measure x + (size x+1)*x.sum^2

/-- 上限判定に必要な累積一次和を二次状態で抑える。 -/
def ThresholdBound.Goal : Prop :=
  ∀ x, (PrefixExposure.measure x)^2 ≤ size x * measure x

end PrefixEnergy

namespace LagProduct

/-- 相続く行の積による自己相関。 -/
def measure (x : Series) : Int := ((adj x).map (fun (a,b) => a*b)).sum

/-- バッチ境界の一積だけを追記する。 -/
def BoundaryProduct.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + seam x y (·*·)

/-- 中心化する前の状態と両端だけでシフト後の値を得る。 -/
def CenterShift.Goal : Prop :=
  ∀ x b, x ≠ [] →
  measure (shifted b x) = measure x + b*(2*x.sum-x.head?.getD 0-x.getLast?.getD 0) + b^2*((x.length-1:Nat):Int)

/-- 二乗差の状態から積を復元して乗算器を共有する。 -/
def EnergyIdentity.Goal : Prop :=
  ∀ x, 2*measure x = 2*(x.map (·^2)).sum - x.head?.getD 0 ^ 2 - x.getLast?.getD 0 ^ 2 - QuadraticVariation.measure x

/-- 定数ランは同じ値の二乗を一回だけ読む。 -/
def RunStretch.Goal : Prop :=
  ∀ x n, 0 < n →
  measure (x.flatMap (repeated n)) = measure x + ((n-1:Nat):Int)*(x.map (·^2)).sum

/-- 一点削除で新しく隣接する両端の積を補正する。 -/
def RemoveInterior.Goal : Prop :=
  ∀ x y a,
  measure (x++y) - measure (x++a::y) = seam x y (·*·) - seam x [a] (·*·) - seam [a] y (·*·)

end LagProduct

namespace FrameTotals

/-- 全連続フレームの和をさらに合計。 -/
def measure (x : Series) : Int := ((windows x).map List.sum).sum

/-- 各行を含むフレーム数を積として畳み込む。 -/
def RowIncidence.Goal : Prop :=
  ∀ x,
  measure x = (x.zipIdx.map (fun (v,i) => v*((i+1:Nat):Int)*((x.length-i:Nat):Int))).sum

/-- 境界を跨ぐフレームを接尾・接頭の一次状態で集計する。 -/
def PrefixSuffix.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + size y*(suffs x).sum + size x*(prefs y).sum

/-- 定数を追加した全フレームへの影響を三項係数で得る。 -/
def UniformShift.Goal : Prop :=
  ∀ x b,
  measure (shifted b x) = measure x + b*choose3 (x.length+2)

/-- 行位置一つの訂正の影響を区間数だけで計算する。 -/
def LocalCorrection.Goal : Prop :=
  ∀ x y a b,
  measure (x++b::y)-measure (x++a::y) = (b-a)*(size x+1)*(size y+1)

/-- 反対向きの格納ページを整列し直さず同じ結果を得る。 -/
def EvenSplitter.Goal : Prop :=
  ∀ x,
  measure x.reverse = measure x

end FrameTotals

namespace TripleRise

/-- スコアが二回上昇する三行の経路数。 -/
def measure (x : Series) : Int := ((triples x).countP strict3 : Int)

/-- 中央行を固定し、両側の順位カウントを掛ける。 -/
def MiddlePivot.Goal : Prop :=
  ∀ x,
  measure x = ((x.zipIdx.map (fun (v,i) => (x.take i).countP (· < v) * (x.drop (i+1)).countP (v < ·))).sum:Int)

/-- 左二行または右二行を使う境界経路を分ける。 -/
def Concatenate.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + ((pairs x).filter rising |>.map (fun t => y.countP (t.2 < ·))).sum + ((pairs y).filter rising |>.map (fun t => x.countP (· < t.1))).sum

/-- 左の全値が右より小さければ交差経路探索を省く。 -/
def DisjointRanges.Goal : Prop :=
  ∀ x y, (∀ a ∈ x, ∀ b ∈ y, a < b) →
  measure (x++y) = measure x + measure y + RisePairs.measure x*size y + size x*RisePairs.measure y

/-- 厳密昇順のページでは全三つ組を一括採用する。 -/
def StrictPage.Goal : Prop :=
  ∀ x, x.Pairwise (· < ·) →
  measure x = (choose3 x.length:Int)

/-- 各入力を n 回複写した際の寄与を三次の係数で求める。 -/
def StableExpand.Goal : Prop :=
  ∀ x (n : Nat),
  measure (x.flatMap (repeated n)) = (n:Int)^3*measure x

end TripleRise

namespace AlternatingBalance

/-- 奇数席・偶数席の符号付き残高。 -/
def measure (x : Series) : Int := (x.zipIdx.map (fun (v,i) => (-1:Int)^i*v)).sum

/-- 境界での位相だけを引き継ぐ。 -/
def ParityCarry.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + (-1:Int)^x.length*measure y

/-- 逆向き格納の位相を長さから回復する。 -/
def BackwardPhase.Goal : Prop :=
  ∀ x,
  measure x.reverse = (-1:Int)^(x.length-1)*measure x

/-- 偶数回の奇数長ブロックを消去する。 -/
def PeriodicParity.Goal : Prop :=
  ∀ x n,
  measure ((List.replicate n x).flatten) = if x.length%2 == 0 then (n:Int)*measure x else ((n%2:Nat):Int)*measure x

/-- 各サンプルを偶数回繰り返す符号列をまとめて消す。 -/
def DuplicateCancellation.Goal : Prop :=
  ∀ x n,
  measure (x.flatMap (repeated n)) = if n%2 == 0 then 0 else measure x

/-- 平行移動後の残差をページの偶奇から得る。 -/
def ShiftResidue.Goal : Prop :=
  ∀ b x,
  measure (shifted b x) = measure x + ((x.length%2:Nat):Int)*b

end AlternatingBalance

namespace PositiveJumps

/-- 正のジャンプの合計。 -/
def measure (x : Series) : Int := ((delta x).map (max 0)).sum

/-- 増分が正の境界だけを追加する。 -/
def BoundaryJump.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + seam x y (fun a b => max 0 (b-a))

/-- 上昇・下降の差を二つの端点に畳む。 -/
def ReverseBalance.Goal : Prop :=
  ∀ x,
  measure x - measure x.reverse = x.getLast?.getD 0-x.head?.getD 0

/-- 全変動と端点の差を共有し正方向だけを復元する。 -/
def SharedMagnitude.Goal : Prop :=
  ∀ x, 2*measure x = TotalVariation.measure x + x.getLast?.getD 0-x.head?.getD 0

/-- 降順ランをまとめてゼロにする。 -/
def NonincreasingSkip.Goal : Prop :=
  ∀ x, x.Pairwise (· ≥ ·) →
  measure x = 0

/-- 可視行が未確定でも総上昇量を上界として残す。 -/
def FilterBudget.Goal : Prop :=
  ∀ x (keep : Int →
  Bool),
  measure (x.filter keep) ≤ measure x

end PositiveJumps

namespace CenteredScatter

/-- 群の全行対の二乗差和。 -/
def measure (x : Series) : Int := ((pairs x).map (fun (a,b) => (a-b)^2)).sum

/-- 件数・一次・二次の三状態で全対を消去する。 -/
def MomentCollapse.Goal : Prop :=
  ∀ x,
  measure x = size x*(x.map (·^2)).sum-x.sum^2

/-- 二群の交差寄与を周辺モーメントだけから得る。 -/
def PairMerge.Goal : Prop :=
  ∀ x y,
  measure (x++y)-measure x-measure y = size y*(x.map (·^2)).sum + size x*(y.map (·^2)).sum-2*x.sum*y.sum

/-- 巨大な基点を先に落として狭いレーンで計算する。 -/
def Translation.Goal : Prop :=
  ∀ b x,
  measure (shifted b x) = measure x

/-- 重複した同じ群の結果を倍率の二乗で生成する。 -/
def ReplicatedGroup.Goal : Prop :=
  ∀ n x,
  measure ((List.replicate n x).flatten) = (n:Int)^2*measure x

/-- 群のレンジ幅から散布量の最大値を制限する。 -/
def OutlierCap.Goal : Prop :=
  ∀ x, 4*measure x ≤ size x^2 * spread x^2

end CenteredScatter

namespace GiniDistance

/-- 全行対の絶対偏差和。 -/
def measure (x : Series) : Int := ((pairs x).map (fun (a,b) => distance a b)).sum

/-- ソート順位を線形係数として各値を一回加算する。 -/
def SortedWeights.Goal : Prop :=
  ∀ x,
  measure x = (x.mergeSort.zipIdx.map (fun (v,i) => (2*(i:Int)+1-size x)*v)).sum

/-- 交わらない二値域のクロス偏差を平均差へ縮約する。 -/
def SeparatedBlocks.Goal : Prop :=
  ∀ x y, (∀ a ∈ x, ∀ b ∈ y, a ≤ b) →
  measure (x++y) = measure x + measure y + size x*y.sum-size y*x.sum

/-- 異なる辞書値対だけを頻度の積で計算する。 -/
def DictionaryPairs.Goal : Prop :=
  ∀ x, 2*measure x = ((cart x.eraseDups x.eraseDups).map (fun (a,b) => distance a b * x.count a * x.count b)).sum

/-- 一行を訂正した後の集約値に対する厳密な誤差予算を出す。 -/
def Lipschitz.Goal : Prop :=
  ∀ x a b, ((measure (a::x)-measure (b::x)).natAbs:Int) ≤ size x * distance a b

/-- 二乗差の状態で絶対偏差の上界を得る。 -/
def VarianceFence.Goal : Prop :=
  ∀ x,
  measure x^2 ≤ (choose2 x.length:Int)*CenteredScatter.measure x

end GiniDistance

namespace ZeroFrames

/-- 合計がゼロの非空連続フレーム数。 -/
def measure (x : Series) : Int := ((windows x).countP (fun w => w.sum == 0) : Int)

/-- 累積残高の同値対に変換してフレームを列挙しない。 -/
def PrefixCollision.Goal : Prop :=
  ∀ x,
  measure x = TiePairs.measure (0::prefs x)

/-- 左右の境界ヒストグラムでゼロ和の交差数を得る。 -/
def BoundaryBalances.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + ((suffs x).map (fun s => (prefs y).count (-s))).sum

/-- 非負ページではゼロだけからなる全ランを使う。 -/
def Nonnegative.Goal : Prop :=
  ∀ x, (∀ v ∈ x, 0 ≤ v) →
  measure x = (windows x).countP (fun w => w.all (· == 0))

/-- 定数ページで必要なのは件数とゼロ判定だけ。 -/
def Uniform.Goal : Prop :=
  ∀ n a,
  measure (repeated n a) = if a == 0 then (choose2 (n+1):Int) else 0

/-- 累積残高がすべて異なればハッシュ構築後の探索を省く。 -/
def EarliestPrune.Goal : Prop :=
  ∀ x,
  measure x = 0 ↔ (0::prefs x).Nodup

end ZeroFrames

namespace EdgeCurvature

/-- 三時点の離散二階差分のエネルギー。 -/
def measure (x : Series) : Int := QuadraticVariation.measure (delta x)

/-- 線形ドリフトを列単位で落としても結果を保つ。 -/
def AffineTrend.Goal : Prop :=
  ∀ x (a b : Int),
  measure (x.zipIdx.map (fun (v,i) => v+a*i+b)) = measure x

/-- 逆向きログの二階エネルギーを再配列なしで得る。 -/
def Backward.Goal : Prop :=
  ∀ x,
  measure x.reverse = measure x

/-- 交差する三つ組は左右端の最大二行だけから得られる。 -/
def EndpointRepair.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + measure ((x.reverse.take 2).reverse ++ y.take 2)

/-- 一定の二階差分なら長さで重み付けする。 -/
def FlatAcceleration.Goal : Prop :=
  ∀ x a, (∀ v ∈ delta (delta x), v = a) →
  measure x = ((x.length-2:Nat):Int)*a^2

/-- 二階エネルギーがゼロのページを一つの等差ランとして読む。 -/
def TrendCertificate.Goal : Prop :=
  ∀ x,
  measure x = 0 ↔ ∃ a d : Int, x = (List.range x.length).map (fun (i : Nat) => a+d*i)

end EdgeCurvature

namespace PeakCount

/-- 両隣より高い局所ピーク数。 -/
def measure (x : Series) : Int := ((x.zipIdx).countP (fun (v,i) => 0 < i && x[i-1]?.any (· < v) && x[i+1]?.any (· < v)) : Int)

/-- 両側の二行ずつから境界ピークだけを認識する。 -/
def BoundaryPeaks.Goal : Prop :=
  ∀ x y,
  measure (x++y) = measure x + measure y + measure ((x.reverse.take 2).reverse ++ y.take 2)

/-- 単調な辞書でデコードせず両隣を判定する。 -/
def OrderDictionary.Goal : Prop :=
  ∀ x (f : Int →
  Int), (∀ a ∈ x, ∀ b ∈ x, a < b ↔ f a < f b) →
  measure (x.map f) = measure x

/-- 交互配置の上限を使ってピーク索引の容量を決める。 -/
def AdjacentCap.Goal : Prop :=
  ∀ x,
  measure x ≤ ((x.length-1)/2:Nat)

/-- 強仮説。全行の二重化後も代表行に一度だけピークが残るか。 -/
def DuplicateLoss.Goal : Prop :=
  ∀ x,
  measure (x.flatMap (repeated 2)) = measure x

/-- 一点訂正後の変化量を定数に抑えて再集計を局所化する。 -/
def BoundedRepair.Goal : Prop :=
  ∀ x y a b, (measure (x++a::y)-measure (x++b::y)).natAbs ≤ 2

end PeakCount

#guard PeakCount.measure [0,1,0] == 1 && PeakCount.measure [0,0,1,1,0,0] == 0

end TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
