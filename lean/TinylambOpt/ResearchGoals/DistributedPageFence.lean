import TinylambOpt.ResearchGoals.SqlFetchWithTies

/-!
These claims describe which page state a distributed reader can observe after a fence.
SQL reads depend on enforcing the modeled visibility across ownership or version
changes.

Modeled claims

- `Goal`
-/

namespace TinylambOpt.ResearchGoals.DistributedPageFence

abbrev Row := Research.TieWatermark.Row
abbrev Spec := Research.TieWatermark.Spec

structure Block where
  low : Research.TieWatermark.Key
  rows : List Row

def valid (spec : Spec) (b : Block) : Prop :=
  (∃ r ∈ b.rows, r.key = b.low) ∧
  ∀ r ∈ b.rows, SqlFetchWithTies.lex spec b.low r.key != Ordering.gt

def ordered (spec : Spec) (rows : List Row) :=
  rows.mergeSort fun x y => match SqlFetchWithTies.lex spec x.key y.key with
    | .eq => x.id ≤ y.id
    | c => c == .lt

def page (spec : Spec) (off k : Nat) (rows : List Row) :=
  SqlFetchWithTies.fetch (fun x y => SqlFetchWithTies.lex spec x.key y.key == .lt)
    off k (ordered spec rows)

-- Only the directory is sorted at start. Opening a block records its
-- directory entry. low is an exact minimum; empty blocks are omitted.
def eligible (spec : Spec) (off k : Nat) (seen : List Row) (next : Block) :=
  match (ordered spec seen)[off + k - 1]? with
  | none => true
  | some cutoff => SqlFetchWithTies.lex spec next.low cutoff.key != Ordering.gt

def scan (spec : Spec) (off k : Nat) (seen : List Row) :
    List Block → List Row × List Block
  | [] => (seen, [])
  | b :: bs =>
    if k != 0 && eligible spec off k seen b then
      let (rows, reads) := scan spec off k (seen ++ b.rows) bs
      (rows, b :: reads)
    else (seen, [])

def Goal : Prop := ∀ spec off k (directory : List Block),
  (∀ b ∈ directory, valid spec b) →
  ((directory.flatMap Block.rows).map Research.TieWatermark.Row.id).Nodup →
  (∀ r ∈ directory.flatMap Block.rows, r.key.length = spec.length) →
  let blocks := directory.mergeSort fun x y => SqlFetchWithTies.lex spec x.low y.low != Ordering.gt
  let (seen, opened) := scan spec off k [] blocks
  let allRows := directory.flatMap Block.rows
  page spec off k seen = page spec off k allRows ∧
  opened = blocks.filter (fun b => k != 0 && eligible spec off k allRows b)

private def blk (key : Int) (id : Nat) : Block := ⟨[some key], [⟨[some key],key,id⟩]⟩
example : ((scan [(.ASC,.Last)] 0 1 [] [blk 1 0, blk 1 1, blk 9 2]).2.map Block.low) =
    [[some 1],[some 1]] := by cbv

end TinylambOpt.ResearchGoals.DistributedPageFence
