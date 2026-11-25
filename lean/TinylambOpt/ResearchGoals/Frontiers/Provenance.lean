import Std

namespace TinylambOpt.ResearchGoals.Frontiers.Provenance

/-! A witness is one derivation of a result from physical tuple IDs in a fixed
snapshot. Duplicate derivations count for BAG observations and collapse for EXISTS. -/
abbrev Witness := List Nat
abbrev Family := List Witness
def live (del w : Witness) := !w.any del.contains
def existsAfter (ws : Family) (del : Witness) := ws.any (live del)
def bagAfter (ws : Family) (del : Witness) := ws.countP (live del)
def subset (a b : Witness) := a.all b.contains
def minimal (ws : Family) := ws.filter (fun w => !ws.any (fun v => subset v w && !subset w v))
def product (a b : Family) := a.flatMap (fun p => b.map (p++·))

/-- 包含される導出を一度取り除けば、将来どの物理行を削除しても EXISTS の再評価には極小の証人だけで足りる。 -/
def Antichain.Goal : Prop := ∀ ws del, existsAfter (minimal ws) del = existsAfter ws del

/-- 二つの結合枝の証人の直積を展開せず、削除後も残る導出数を局所カウントの積から得る。 -/
def ProductCount.Goal : Prop := ∀ a b del,
  bagAfter (product a b) del = bagAfter a del * bagAfter b del

/-- 同一行を何度も使う自己結合でも、削除の判定用には ID を一意化し、同一署名の導出を重みで保存する。 -/
def SignatureWeights.Goal : Prop := ∀ (ws : Family) del,
  let keys := ws.map (fun w => w.mergeSort.eraseDups)
  bagAfter ws del = ((keys.eraseDups.filter (live del)).map keys.count).sum

/-- 同時に削除が数行届いたとき、変更 ID を含み、以前は生きていた導出だけを一回ずつ減算する。 -/
def IncidentDelta.Goal : Prop := ∀ ws old batch,
  bagAfter ws (old++batch) =
    bagAfter ws old - ws.countP (fun w => live old w && w.any batch.contains)

def cuts (ws : Family) (del : Witness) := !existsAfter ws del
def cutCost (ws : Family) (candidates : Family) :=
  ((candidates.filter (cuts ws)).map List.length).min?

/-- 結合した二枝のどちらかを完全に潰す最小削除数を求めれば、直積全体の耐障害性が分かる。 -/
def ProductCut.Goal : Prop := ∀ a b candidates ca cb,
  cutCost a candidates = some ca → cutCost b candidates = some cb →
  cutCost (product a b) candidates = some (min ca cb)

def disjoint (a b : Witness) := !a.any b.contains

/-- ID を共有しない導出が k 本あれば、k 未満の新規削除では EXISTS を再検査しなくてよい。 -/
def PackingCertificate.Goal : Prop := ∀ (ws pack : Family) del,
  (∀ w ∈ pack, w ∈ ws) → pack.Pairwise (fun a b => disjoint a b = true) →
  del.length < pack.length → existsAfter ws del = true

def choices {α : Type} : List α → List (List α)
  | [] => [[]]
  | x::xs => let rest := choices xs
             rest ++ rest.map (x::·)
def packing (ws : Family) := (((choices ws).filter
  (fun ps => ps.all (fun a => !a.isEmpty) && ps.zipIdx.all (fun (w,i) =>
    (ps.drop (i+1)).all (disjoint w)))).map List.length).max?.getD 0

/-- 強仮説。最小削除集合の大きさを、任意の多表結合でも互いに素な導出の最大本数から得られるか。 -/
def PackingExact.Goal : Prop := ∀ (ws : Family), (∀ w ∈ ws, w ≠ []) →
  cutCost ws (choices ws.flatten.eraseDups) = some (packing ws)

/-- 二表間に限れば、最小削除集合を物理行 ID が重ならない一致辺の最大本数で厳密に評価できるか。 -/
def BipartiteCut.Goal : Prop := ∀ (edges : List (Nat × Nat)),
  let ws := edges.map (fun (x,y) => [2*x,2*y+1])
  cutCost ws (choices ws.flatten.eraseDups) = some (packing ws)

/-- 右枝の削除が多くても、行ごとに耐えられる削除数を証明できれば anti-join の候補を開かず除外する。 -/
def GuardedAnti.Goal : Prop := ∀ (rows : List Nat) (alts : Nat → Family) del cap,
  del.length ≤ cap →
  rows.filter (fun r => !existsAfter (alts r) del) =
    (rows.filter (fun r => packing (alts r) ≤ cap)).filter (fun r => !existsAfter (alts r) del)

/-- 各結果キーの依存集合を比較し、包含関係が既知ならあらゆる削除に対する EXISTS の含意を静的判定できる。 -/
def UniversalImplication.Goal : Prop := ∀ (a b : Family),
  (∀ del, existsAfter a del = true → existsAfter b del = true) ↔
    ∀ p ∈ a, ∃ q ∈ b, subset q p = true

private def triangle : Family := [[0,1],[1,2],[0,2]]
#guard packing triangle == 1 && cutCost triangle (choices [0,1,2]) == some 2
#guard bagAfter [[0],[0,1]] [] == 2 && bagAfter (minimal [[0],[0,1]]) [] == 1

end TinylambOpt.ResearchGoals.Frontiers.Provenance
