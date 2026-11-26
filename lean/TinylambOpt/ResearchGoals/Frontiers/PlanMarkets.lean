import Std

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.
-/

namespace TinylambOpt.ResearchGoals.Frontiers.PlanMarkets

/-! Cascades alternatives and materialization. Costs are explicit model units:
startup, rows processed, temporary storage, and separately priced reads. -/
structure Line where
  startup : Nat
  perRow : Nat
  deriving BEq
def cost (p : Line) (n : Nat) := p.startup + p.perRow*n
def optimum (ps : List Line) (n : Nat) := (ps.map (cost · n)).min?

/-- 基数区間の両端で優越する代替案があれば、区間全体のメモから劣る線形プランを消せる。 -/
def EndpointEnvelope.Goal : Prop := ∀ a b rest lo hi, cost a lo ≤ cost b lo → cost a hi ≤ cost b hi →
  ∀ n, lo ≤ n → n ≤ hi → optimum (a::b::rest) n = optimum (a::rest) n

def regret (p : Line) (ps : List Line) (n : Nat) := cost p n - (optimum ps n).getD (cost p n)

/-- 各基数について最良の競合が異なる場合でも、固定プランの最悪絶対 regret を両端で評価できる。 -/
def RobustRegret.Goal : Prop := ∀ p ps lo hi n, lo ≤ n → n ≤ hi →
  regret p ps n ≤ max (regret p ps lo) (regret p ps hi)

/-- 強仮説。メモで両端の勝者だけを残しても、中間基数で最安のプランを失わないか。 -/
def TwoWinners.Goal : Prop := ∀ a b ps hi, optimum ps 0 = some (cost a 0) →
  optimum ps hi = some (cost b hi) → a ∈ ps → b ∈ ps →
  ∀ n ≤ hi, optimum [a,b] n = optimum ps n

/-- 基数が増えると最良線形プランの単位費用は単調に下がる。パラメータ順に一方向へ切替可能か。 -/
def SlopeSwitch.Goal : Prop := ∀ ps a b x y, x < y → a ∈ ps → b ∈ ps →
  optimum ps x = some (cost a x) → optimum ps y = some (cost b y) → b.perRow ≤ a.perRow

def rentThenBuy (requests read build : Nat) :=
  if requests*read ≤ build then requests*read else (build/read)*read + build

/-- 再利用回数が未知のスプールを、累積再計算費が構築費を超える境界で一度だけ構築する。 -/
def OnlineSpool.Goal : Prop := ∀ n read build,
  rentThenBuy n read build ≤ 2 * min (n*read) build

abbrev View := Nat
abbrev Workload := List (Nat × List View) -- value of a saved execution; suitable views
def saved (qs : Workload) (chosen : List View) :=
  (qs.filter (fun q => q.2.any chosen.contains)).map Prod.fst |>.sum
def gain (qs : Workload) (chosen : List View) (v : View) := saved qs (v::chosen) - saved qs chosen

/-- 複数ビューのどれか一つで要求を満たせるとき、既選択集合が増えると追加ビューの限界便益は減る。 -/
def DiminishingReuse.Goal : Prop := ∀ qs small large v, (∀ x ∈ small, x ∈ large) →
  gain qs large v ≤ gain qs small v

def greedy (qs : Workload) (pool : List View) : Nat → List View → List View
  | 0,selected => selected
  | n+1,selected =>
    let choices := (pool.filter (fun v => !selected.contains v)).mergeSort
      (fun a b => gain qs selected a ≥ gain qs selected b)
    match choices with | [] => selected | v::_ => greedy qs pool n (v::selected)

/-- ビューを K 個選ぶ貪欲法の便益を最良 K 個と整数だけで比較する。K=1 では厳密な最適性になる。 -/
def GreedyViews.Goal : Prop := ∀ qs pool k opt, 0 < k → opt.length ≤ k →
  (∀ x ∈ opt, x ∈ pool) →
  (k^k - (k-1)^k) * saved qs opt ≤ k^k * saved qs (greedy qs pool k [])

def bundles (qs : Workload) (chosen : List View) :=
  ((qs.filter (fun q => q.2.all chosen.contains)).map Prod.fst).sum

/-- 強仮説。二つの部分ビューを両方要求する場合にも、限界便益の逓減を再利用できるか。 -/
def ComplementaryViews.Goal : Prop := ∀ qs small large v, (∀ x ∈ small, x ∈ large) →
  bundles qs (v::large) - bundles qs large ≤ bundles qs (v::small) - bundles qs small

/-- メモを幅方向のバケットに縮約する。起動費・行単価が各々 ε 以内なら、全基数での劣化を制限できる。 -/
def ApproxMemo.Goal : Prop := ∀ (ps cache : List Line) e n, ps ≠ [] →
  (∀ p ∈ ps, ∃ q ∈ cache, q.startup ≤ p.startup+e ∧ q.perRow ≤ p.perRow+e) →
  (optimum cache n).getD 0 ≤ (optimum ps n).getD 0 + e*(n+1)

/-- 多次元パラメータの独立な区間で、線形コスト差を最悪にする角だけを一回評価する。 -/
def BoxWorst.Goal : Prop := ∀ (dims : List (Int × Int × Int × Int)),
  (∀ d ∈ dims, d.2.1 ≤ d.2.2.2 ∧ d.2.2.2 ≤ d.2.2.1) →
  (dims.map (fun (w,_,_,x) => w*x)).sum ≤
    (dims.map (fun (w,lo,hi,_) => w*(if 0 ≤ w then hi else lo))).sum

#guard optimum [⟨0,10⟩,⟨10,2⟩,⟨30,0⟩] 3 == some 16 && optimum [⟨0,10⟩,⟨30,0⟩] 3 == some 30
#guard let ps : List Line := [⟨0,10⟩,⟨10,2⟩,⟨30,0⟩]
       optimum ps 0 == some 0 && optimum ps 10 == some 30
#guard let ws : Workload := [(1,[0,1])]
       bundles ws [1,0] - bundles ws [0] == 1 && bundles ws [1] - bundles ws [] == 0

end TinylambOpt.ResearchGoals.Frontiers.PlanMarkets
