import Std
import TinylambOpt.ThreeValued

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.
-/

namespace TinylambOpt.ResearchGoals.Frontiers.DecisionTables

/-! A finite relation of valuations for deterministic, total predicate atoms.
Sharing and minimization are relative to the valuations admitted by constraints. -/
open TinylambOpt (SqlBool)
abbrev Row := List SqlBool
abbrev Query := Row → SqlBool
def answers (qs : List Query) (r : Row) := qs.map (fun q => (q r).inWhere)
def signature (qs : List Query) (tails : List Row) (head : Row) :=
  tails.map (fun t => answers qs (head ++ t))
def classes (qs : List Query) (heads tails : List Row) :=
  (heads.map (signature qs tails)).eraseDups
def rep (qs : List Query) (heads tails : List Row) (h : Row) :=
  heads.find? (fun x => signature qs tails x == signature qs tails h)

/-- 複数の WHERE を同時にコンパイルするとき、残る入力に対する真理値表が等しい節を一状態に畳む。 -/
def SharedResidual.Goal : Prop := ∀ qs heads tails h t, h ∈ heads → t ∈ tails →
  ((rep qs heads tails h).map (fun x => answers qs (x++t))) = some (answers qs (h++t))

/-- 任意の共通決定プログラムが同じ出力を返すために必要な中間状態数の下限を、残余表の異なり数で与える。 -/
def StateLowerBound.Goal : Prop := ∀ qs heads tails (encode : Row → Nat) (finish : Nat → Row → List Bool),
  (∀ h ∈ heads, ∀ t ∈ tails, finish (encode h) t = answers qs (h++t)) →
  (classes qs heads tails).length ≤ (heads.map encode).eraseDups.length

/-- 後半の入力が辞書制約で減ると、旧状態を分割せず併合だけで決定表を再構築できる。 -/
def Coarsening.Goal : Prop := ∀ qs heads (old new : List Row), (∀ t ∈ new, t ∈ old) →
  (classes qs heads new).length ≤ (classes qs heads old).length

/-- 二つの独立した利用クエリ集合を同じ走査に載せるとき、必要な状態数を個別状態数の積で上から押さえる。 -/
def JointWidth.Goal : Prop := ∀ a b heads tails,
  (classes (a++b) heads tails).length ≤ (classes a heads tails).length * (classes b heads tails).length

def distinctAnswers (qs : List Query) (rows : List Row) (key : Row → Row) :=
  rows.map key |>.eraseDups.map (fun k => answers qs k)

/-- NULL を含む相関キーを一度ずつ評価し、物理行の重複度を復元して複数の FILTER 件数を同時に得る。 -/
def WeightedQueries.Goal : Prop := ∀ qs rows (key : Row → Row) (j : Nat),
  (∀ r ∈ rows, answers qs (key r) = answers qs r) →
  let keys := rows.map key
  let outputs := distinctAnswers qs rows key
  rows.countP (fun r => (answers qs r)[j]?.getD false) =
    ((keys.eraseDups.zip outputs).map (fun (k,v) => if v[j]?.getD false then keys.count k else 0)).sum

/-- GROUP ごとに定数となる判定は代表行で一度実行し、キー一致群の測度をそのまま加算する。 -/
def GroupDispatch.Goal : Prop := ∀ (rows : List (Row × Int)) (key : Row → Nat) (q : Query),
  (∀ x ∈ rows, ∀ y ∈ rows, key x.1 = key y.1 → (q x.1).inWhere = (q y.1).inWhere) →
  ((rows.filter (fun r => (q r.1).inWhere)).map Prod.snd).sum =
    (((rows.map (fun r => key r.1)).eraseDups).map (fun k =>
      let g := rows.filter (fun r => key r.1 == k)
      if g.head?.any (fun r => (q r.1).inWhere) then (g.map Prod.snd).sum else 0)).sum

/-- 強仮説。WHERE で一致する式を共有した後、否定を置く消費者にも同じビットを再利用できるか。 -/
def NegatedConsumer.Goal : Prop := ∀ (p q : Query) (rows : List Row),
  (∀ r ∈ rows, (p r).inWhere = (q r).inWhere) →
  rows.filter (fun r => (p r).not.inWhere) = rows.filter (fun r => (q r).not.inWhere)

/-- 後続で否定される式にも対応するため、式とその否定の採用ビットを対で保存する。 -/
def SignedConsumers.Goal : Prop := ∀ (p q : Query) (rows : List Row),
  (∀ r ∈ rows, ((p r).inWhere,(p r).not.inWhere) = ((q r).inWhere,(q r).not.inWhere)) →
  ∀ (consumer : SqlBool → SqlBool), rows.filter (fun r => (consumer (p r)).inWhere) =
    rows.filter (fun r => (consumer (q r)).inWhere)

def binaryRows : Nat → List Row
  | 0 => [[]]
  | n+1 =>
    let rest := binaryRows n
    rest.map (.tFalse :: ·) ++ rest.map (.tTrue :: ·)
def oneHot (n : Nat) := (List.range n).map (fun i => (List.range n).map
  (fun j => if i == j then SqlBool.tTrue else .tFalse))
def mux (n : Nat) : Query := fun r =>
  let i := (r.take n).findIdx SqlBool.inWhere
  (r.drop n)[i]?.getD .tNull

/-- CASE の選択列を先に読む順序では、n 個の出力候補に対する途中状態数を n 以下に抑えられる。 -/
def AddressFirst.Goal : Prop := ∀ n,
  (classes [mux n] (oneHot n) (binaryRows n)).length ≤ n

/-- 同じ CASE で値列を先に読むと、選択列が一意でも途中状態が指数個必要になるか。 -/
def PayloadFirst.Goal : Prop := ∀ n,
  let revMux : Query := fun r => mux n (r.drop n ++ r.take n)
  (classes [revMux] (binaryRows n) (oneHot n)).length = 2^n

#guard SqlBool.tFalse.inWhere == SqlBool.tNull.inWhere &&
  SqlBool.tFalse.not.inWhere != SqlBool.tNull.not.inWhere
#guard (classes [mux 3] (oneHot 3) (binaryRows 3)).length == 3

end TinylambOpt.ResearchGoals.Frontiers.DecisionTables
