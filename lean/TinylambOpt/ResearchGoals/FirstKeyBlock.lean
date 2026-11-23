import Std

namespace TinylambOpt.ResearchGoals.FirstKeyBlock

abbrev W := BitVec 64
structure Row where
  key : Option (Fin 63)
  payload : Int
  deriving DecidableEq
def flag (r : Row) : W := (1 : W) <<< (r.key.elim 63 Fin.val)
def summary (xs : List Row) := xs.foldl (fun m r => m ||| flag r) (0 : W)
structure Block where
  header : W
  rows : List Row

def reference (xs : List Row) : List Row :=
  xs.zipIdx.filterMap fun (r,i) =>
    if (xs.take i).any (fun earlier => earlier.key == r.key) then none else some r

structure State where
  seen : W := 0
  kept : List Row := []
  opened : List Nat := []

def scan (bs : List Block) : State :=
  bs.zipIdx.foldl (fun s (b,i) =>
    if (b.header &&& ~~~s.seen) = 0 then s else
    b.rows.foldl (fun st r =>
      if (flag r &&& st.seen) != 0 then st else
        {st with seen := st.seen ||| flag r, kept := r :: st.kept})
      {s with opened := i :: s.opened}) {}

def Goal : Prop := ∀ bs, (∀ b ∈ bs, b.header = summary b.rows) →
  let s := scan bs
  s.kept.reverse = reference (bs.flatMap Block.rows) ∧
  ∀ b ∈ bs.zipIdx, b.2 ∈ s.opened ↔
    ∃ r ∈ b.1.rows, ∀ old ∈ (bs.take b.2).flatMap Block.rows, old.key ≠ r.key

private def block (rs : List Row) : Block := ⟨summary rs,rs⟩
private def sample := scan [block [⟨none,1⟩,⟨some 2,7⟩],block [⟨none,9⟩],block [⟨some 3,8⟩]]
example : sample.kept.reverse.map Row.payload = [1,7,8] ∧ sample.opened.reverse = [0,2] := by decide

end TinylambOpt.ResearchGoals.FirstKeyBlock
