import Std

namespace TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

/-! Physical models. One cache slot holds one page. Reads may bypass the
cache. Merge work counts records rewritten, and buffer peaks count live words. -/
abbrev Trace := List Nat
def lru (cap : Nat) : Trace → Trace → Nat
  | [],_ => 0
  | p::ps,cache => (if cache.contains p then 1 else 0) +
      lru cap ps ((p::cache.erase p).take cap)
def reuseDistance (past : Trace) (p : Nat) : Option Nat :=
  if past.contains p then some ((past.reverse.takeWhile (· != p)).eraseDups.length) else none

/-- spill の読み出し列から再参照距離を一度求め、全キャッシュ容量の LRU ヒット数を再実行なしで予測する。 -/
def StackDistance.Goal : Prop := ∀ trace cap, lru cap trace [] = trace.zipIdx.countP
  (fun (p,i) => (reuseDistance (trace.take i) p).any (· < cap))

abbrev Arc := Nat × Nat
def arcs (t : Trace) : List Arc := t.zipIdx.filterMap (fun (p,i) =>
  ((t.drop (i+1)).idxOf? p).map (fun j => (i,i+1+j)))
def fits (t : Trace) (cap : Nat) (chosen : List Arc) :=
  (List.range t.length).all (fun i => chosen.countP (fun (a,b) => a ≤ i && i < b) ≤ cap)
def legal (t : Trace) (cap : Nat) (state : Nat → List Nat) : Prop :=
  state 0 = [] ∧ ∀ i < t.length, (state (i+1)).length ≤ cap ∧
    ∀ p ∈ state (i+1), p ∈ state i ∨ t[i]? = some p
def saved (t : Trace) (state : Nat → List Nat) := t.zipIdx.countP (fun (p,i) => (state i).contains p)

/-- 読み込み間の常駐を半開区間とみなし、k 回以上ヒットするキャッシュ計画の存在を容量付き区間選択に帰着する。 -/
def OfflineCache.Goal : Prop := ∀ t cap k,
  (∃ state, legal t cap state ∧ k ≤ saved t state) ↔
    ∃ chosen : List Arc, chosen.Nodup ∧ (∀ e ∈ chosen, e ∈ arcs t) ∧ fits t cap chosen ∧ k ≤ chosen.length

def selectArcs (es : List Arc) (endAt : Nat) : Nat := match es with
  | [] => 0
  | (a,b)::rest => if endAt ≤ a then 1 + selectArcs rest b else selectArcs rest endAt

/-- バイパスを許す 1 枠キャッシュでは、終了の早い再参照区間を貪欲に採用すればヒット数が最大になる。 -/
def SingleSlot.Goal : Prop := ∀ t state, legal t 1 state →
  saved t state ≤ selectArcs ((arcs t).mergeSort (fun a b => a.2 ≤ b.2)) 0

inductive Tree
  | leaf (size : Nat)
  | node (out : Nat) (left right : Tree)
def width : Tree → Nat | .leaf n => n | .node n _ _ => n
def peak : Tree → Nat
  | .leaf n => n
  | .node n l r => max (peak l) (max (width l + peak r) (width l + width r + n))
def bestPeak : Tree → Nat
  | .leaf n => n
  | .node n l r =>
    let pl := bestPeak l
    let pr := bestPeak r
    max (width l + width r + n) (min (max pl (width l+pr)) (max pr (width r+pl)))
inductive Order : Tree → Tree → Prop
  | leaf (n) : Order (.leaf n) (.leaf n)
  | same {n a b c d} : Order a c → Order b d → Order (.node n a b) (.node n c d)
  | swap {n a b c d} : Order a c → Order b d → Order (.node n a b) (.node n d c)

/-- 可換な実行木の左右を局所で決定し、全体の同時生存バッファの最小値を線形サイズの状態から得る。 -/
def TreePeak.Goal : Prop := ∀ t,
  (∃ s, Order t s ∧ peak s = bestPeak t) ∧ ∀ s, Order t s → bestPeak t ≤ peak s

/-- 出力を保持する必要のある枝の実行順を、内部ピークと出力幅の差で決める。 -/
def ExcessFirst.Goal : Prop := ∀ l r n, bestPeak l - width l ≥ bestPeak r - width r →
  bestPeak (.node n l r) = max (width l+width r+n) (max (bestPeak l) (width l+bestPeak r))

inductive Runs | run (size : Nat) | merge (a b : Runs)
def leaves : Runs → List Nat | .run n => [n] | .merge a b => leaves a ++ leaves b
def writes : Runs → Nat
  | .run _ => 0
  | .merge a b => (leaves a).sum + (leaves b).sum + writes a + writes b
def huffman : Nat → List Nat → Nat
  | 0,_ => 0
  | k+1,runs => match runs.mergeSort with
    | a::b::rest => a+b + huffman k ((a+b)::rest)
    | _ => 0

/-- 任意の二つの run を混ぜられる外部ソートでは、最短二本ずつのマージが総書込行数を最小化する。 -/
def MinimumWrites.Goal : Prop := ∀ t, huffman (leaves t).length (leaves t) ≤ writes t

/-- 強仮説。入力順を保つ隣接 run のマージだけでも、任意ペアを許すときの最小書込量を達成できるか。 -/
def AdjacentRuns.Goal : Prop := ∀ (ns : List Nat), ns ≠ [] →
  ∃ t, leaves t = ns ∧ writes t = huffman ns.length ns

def adjacentThree (a b c : Nat) := min (a+b + (a+b+c)) (b+c + (a+b+c))
/-- 三本の順序制約付き run の最適コストを算出し、任意ペアの下限と一致する条件を planner に渡す。 -/
def AdjacentPenalty.Goal : Prop := ∀ a b c, adjacentThree a b c - huffman 3 [a,b,c] = b-max a c

def dispatch (loads jobs : List Nat) : List Nat :=
  jobs.foldl (fun ls j => match ls.mergeSort with | [] => [] | l::rest => (l+j)::rest) loads
def makespan (loads : List Nat) := loads.max?.getD 0

/-- 重い morsel 順で最も空いた worker に置く割当ての完了時間を、任意の理想的な非分割割当てと比較する。 -/
def LargestFirst.Goal : Prop := ∀ (assignment : List (List Nat)), 0 < assignment.length →
  let m := assignment.length
  let jobs := assignment.flatten.mergeSort (fun a b => a ≥ b)
  3*m*makespan (dispatch (List.replicate m 0) jobs) ≤ (4*m-1)*makespan (assignment.map List.sum)

/-- 各 morsel を所定の上限以下に分割できる場合、貪欲な割当ての負荷差を一片の最大長で制限する。 -/
def TileImbalance.Goal : Prop := ∀ (jobs : List Nat) m cap, 0 < m → (∀ j ∈ jobs, j ≤ cap) →
  let loads := dispatch (List.replicate m 0) jobs
  makespan loads - loads.min?.getD 0 ≤ cap

#guard huffman 3 [1,10,1] == 14 && adjacentThree 1 10 1 == 23
#guard lru 2 [1,2,1,3,2] [] == 1 && reuseDistance [1,2,1,3] 2 == some 2

end TinylambOpt.ResearchGoals.Frontiers.SpillScheduling
