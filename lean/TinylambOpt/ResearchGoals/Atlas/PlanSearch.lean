import Std

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.
-/

namespace TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-! Explicit integer cost models. A job or view can be replayed on counterexample
inputs. None of the numerical objectives asserts an improvement in wall time. -/
namespace PredicateOrder

structure Test where
  price : Nat
  pass : Nat -- surviving fraction pass/den, under independence
def bill (den : Nat) : List Test → Nat
  | [] => 0
  | t::ts => t.price * den^ts.length + t.pass * bill den ts
def byRatio (den : Nat) (a b : Test) :=
  decide (a.price*(den-b.pass) ≤ b.price*(den-a.pass))
def valid (d : Nat) (ts : List Test) := ∀ t ∈ ts, t.pass ≤ d
def actual (ts : List (Nat × (Nat → Bool))) (rows : List Nat) : Nat :=
  match ts with
  | [] => 0
  | (cost,pass)::rest => cost*rows.length + actual rest (rows.filter pass)

/-- 独立なフィルタの費用と脱落率を整数で比較し、全順列の中から最安の順序を選ぶ。 -/
def RatioSort.Goal : Prop := ∀ d ts alt, 0 < d → valid d ts → (∀ t ∈ ts, 0 < t.price ∨ t.pass < d) → ts.Perm alt →
  bill d (ts.mergeSort (byRatio d)) ≤ bill d alt

/-- 相関する二述語でも交換する二点の直前の生存集合だけで正味の費用差を計算する。 -/
def ConditionalSwap.Goal : Prop := ∀ ca cb pa pb rest rows,
  (actual ((ca,pa)::(cb,pb)::rest) rows:Int) - actual ((cb,pb)::(ca,pa)::rest) rows =
    (ca:Int)*rows.countP (fun x => !pb x) - (cb:Int)*rows.countP (fun x => !pa x)

/-- 答えが同じ二述語のうち冗長な二回目を除き、実際の仕事量を差し引く。 -/
def RepeatedGuard.Goal : Prop := ∀ c p middle tail rows,
  actual ((c,p)::middle++(c,p)::tail) rows =
    actual ((c,p)::middle++tail) rows + c * (rows.filter (fun r => p r && middle.all (fun t => t.2 r))).length

/-- 常に通る高価な検査を最後に回し、そこで残った行だけに課金する。 -/
def DeferredPass.Goal : Prop := ∀ c tests rows,
  actual (tests ++ [(c,fun _ => true)]) rows ≤ actual ((c,fun _ => true)::tests) rows

/-- 強仮説。相関があっても、全表で脱落数が同じ二つの等費用述語を任意の中間段で交換できるか。 -/
def MarginalTie.Goal : Prop := ∀ (rows : List Nat) (a b gate : Nat → Bool),
  rows.countP a = rows.countP b →
  actual [(1,gate),(1,a),(1,b)] rows = actual [(1,gate),(1,b),(1,a)] rows

end PredicateOrder
namespace AlphabeticMerge

inductive Tree | leaf (w : Nat) | fork (l r : Tree)
def weights : Tree → List Nat | .leaf w => [w] | .fork l r => weights l ++ weights r
def work : Tree → Nat
  | .leaf _ => 0
  | .fork l r => work l + work r + (weights l).sum + (weights r).sum
def dp : Nat → List Nat → Nat
  | 0,_ => 0
  | fuel+1,ws => if ws.length ≤ 1 then 0 else ws.sum +
    ((List.range (ws.length-1)).map (fun i => dp fuel (ws.take (i+1)) + dp fuel (ws.drop (i+1)))).min?.getD 0
def best (ws : List Nat) := dp ws.length ws
def cut (ws : List Nat) := ((List.range (ws.length-1)).map (·+1)).mergeSort
  (fun i j => best (ws.take i)+best (ws.drop i) ≤ best (ws.take j)+best (ws.drop j)) |>.head?.getD 0

/-- 安定順を守る外部マージで区間 DP が実在する木の最小書込量を与える。 -/
def OptimalTree.Goal : Prop := ∀ ws, ws ≠ [] →
  (∃ t, weights t = ws ∧ work t = best ws) ∧ ∀ t, weights t = ws → best ws ≤ work t

/-- 右端を伸ばす際の最小分割位置は左へ動かないとして、分割候補の探索幅を減らす。 -/
def MonotoneCut.Goal : Prop := ∀ ws v, 2 ≤ ws.length → cut ws ≤ cut (ws++[v])

/-- 局所的な区間最適値に四辺形不等式が成り立てば、重複部分問題の候補を棄却できる。 -/
def Quadrangle.Goal : Prop := ∀ a b c, b ≠ [] →
  best (a++b) + best (b++c) ≤ best (a++b++c) + best b

/-- 高頻度の run を一個だけ増量した際の最適費用増を、以前の木での深さだけで制限する。 -/
def depth : Tree → Nat → Nat
  | .leaf _,_ => 0
  | .fork a b,i => 1 + if i < (weights a).length then depth a i else depth b (i-(weights a).length)
def ReweightBound.Goal : Prop := ∀ t i d, i < (weights t).length → work t = best (weights t) →
  best ((weights t).modify i (·+d)) ≤ work t + d*depth t i

/-- 同じ相対的な run 長を持つパーティションで DP の分割を共有する。 -/
def ScaleReuse.Goal : Prop := ∀ ws c, 0 < c → cut (ws.map (c*·)) = cut ws

end AlphabeticMerge
namespace ParallelCuts

def groups {α : Type} (xs : List α) (cuts : List Nat) : List (List α) :=
  match cuts with
  | [] => [xs]
  | c::cs => xs.take c :: groups (xs.drop c) cs
def peak (blocks : List (List Nat)) := (blocks.map List.sum).max?.getD 0
def pack (cap : Nat) (xs : List Nat) := xs.foldl (fun (s : Nat × Nat) w =>
  if s.2+w ≤ cap then (s.1,s.2+w) else (s.1+1,w)) (0,0)
def required (cap : Nat) (xs : List Nat) := if xs.isEmpty then 0 else (pack cap xs).1+1

/-- 連続 morsel の分割数が足りるかを一回の貪欲走査だけで判定する。 -/
def Feasible.Goal : Prop := ∀ cap xs k, (∀ w ∈ xs, w ≤ cap) →
  (required cap xs ≤ k ↔ ∃ blocks : List (List Nat),
    blocks.flatten = xs ∧ blocks.length ≤ k ∧ peak blocks ≤ cap)

/-- バイト数の上限を二分探索するとき、必要な morsel 数は単調に減る。 -/
def CapacitySearch.Goal : Prop := ∀ a b xs, a ≤ b → (∀ w ∈ xs, w ≤ a) → required b xs ≤ required a xs

/-- worker の実行順を反転しても連続分割の最適個数は変わらない。 -/
def ReverseScan.Goal : Prop := ∀ cap xs, (∀ w ∈ xs, w ≤ cap) → required cap xs.reverse = required cap xs

/-- 各入力ファイルで独立に分割を作った結果から、連結後の最適分割との差を境界一つに制限する。 -/
def BoundarySlack.Goal : Prop := ∀ cap a b, (∀ w ∈ a++b, w ≤ cap) →
  required cap a + required cap b - 1 ≤ required cap (a++b) ∧
  required cap (a++b) ≤ required cap a + required cap b

/-- 生成済みの ordinal 切断列を再利用し、同じ位置に行が追加されない限り重量訂正を一つの群に制限する。 -/
def LocalCorrection.Goal : Prop := ∀ xs cs i d,
  peak (groups (xs.modify i (·+d)) cs) ≤ peak (groups xs cs)+d

end ParallelCuts
namespace RiskEnvelope

abbrev Costs := List (Int × Nat)
def loss (xs : Costs) (z : Int) (a b : Nat) :=
  (xs.map (fun (x,w) => w*(a*(x-z).toNat + b*(z-x).toNat))).sum
def mass (xs : Costs) (p : Int → Bool) := ((xs.filter (fun x => p x.1)).map Prod.snd).sum

/-- spill 過小評価と過大予約を別々に課金したとき、重み付き分位点を予約量に選ぶ。 -/
def QuantileReserve.Goal : Prop := ∀ xs z a b,
  (a+b)*mass xs (· < z) ≤ a*mass xs (fun _ => true) →
  a*mass xs (fun _ => true) ≤ (a+b)*mass xs (· ≤ z) → ∀ y, loss xs z a b ≤ loss xs y a b

/-- 隣接する予約量の目的値の差を、累積件数から定数時間で求める。 -/
def ReservationSlope.Goal : Prop := ∀ xs z a b,
  (loss xs (z+1) a b:Int) - loss xs z a b = (b:Int)*mass xs (· ≤ z) - (a:Int)*mass xs (· > z)

/-- 重み付き負荷分布の同じ値を一つに束ねても全予約量の最適化目的を保存する。 -/
def HistogramFold.Goal : Prop := ∀ xs z a b, loss xs z a b =
  loss ((xs.map Prod.fst).eraseDups.map (fun v => (v,mass xs (· == v)))) z a b

/-- ワークロードに少量の外れ値が追加された際の旧予約量の regret を距離と料金で抑える。 -/
def RareWorkload.Goal : Prop := ∀ xs extra z a b, (∀ y, loss xs z a b ≤ loss xs y a b) →
  ∀ y, loss (xs++extra) z a b ≤ loss (xs++extra) y a b +
    max a b * (z-y).natAbs * mass extra (fun _ => true)

/-- 偏りの強い負荷分布でも平均予約量が絶対損失を最小化する、という強仮説を調べる。 -/
def MeanReserve.Goal : Prop := ∀ xs, 0 < mass xs (fun _ => true) →
  let avg : Int := (xs.map (fun (v,w) => v*(w:Int))).sum / (mass xs (fun _ => true):Int)
  ∀ z, loss xs avg 1 1 ≤ loss xs z 1 1

end RiskEnvelope

#guard RiskEnvelope.loss [(0,3),(8,1)] 2 1 1 == 12 && RiskEnvelope.loss [(0,3),(8,1)] 0 1 1 == 8

#guard let xs := [0,1]
       let gate := fun (x : Nat) => x == 0
       let other := fun (x : Nat) => x == 1
       xs.countP gate == xs.countP other &&
         PredicateOrder.actual [(1,gate),(1,gate),(1,other)] xs == 4 &&
         PredicateOrder.actual [(1,gate),(1,other),(1,gate)] xs == 3

end TinylambOpt.ResearchGoals.Atlas.PlanSearch
