import TinylambOpt.ResearchGoals.SqlFetchWithTies

/-!
These claims compare incremental consumption of page runs with an expanded reference
stream. An ordered SQL scan can consume runs while preserving output row order.

Modeled claims

- `Goal` — `∀ off k (files : List (List Row)), PrefixSorted files.flatten →
  files.flatten.Pairwise (fun a b => a.id < b.id) → let s := run off k files s.output
  = SqlFetchWithTies.pageSQL off k files.flatten ∧ s.peak ≤ maxRunLen (splitRuns
  files.flatten) ∧ (k = 0 → s.peak = 0)`
-/

namespace TinylambOpt.ResearchGoals.StreamingRunPage

open Research.IncrementalSortPage

structure Cursor where
  pending : List Row := []
  output : List Row := []
  skip : Nat
  need : Nat
  boundary : Option (Int × Int) := none
  stopped : Bool := false
  peak : Nat := 0

def emit (s : Cursor) (r : Row) : Cursor :=
  if s.stopped then s else
  if s.skip > 0 then {s with skip := s.skip - 1} else
  if s.need > 0 then
    {s with need := s.need - 1, output := s.output ++ [r], boundary := some (rowKey r)}
  else if s.boundary == some (rowKey r) then {s with output := s.output ++ [r]}
  else {s with stopped := true}

def flush (s : Cursor) : Cursor :=
  (isort s.pending).foldl emit {s with pending := []}

def feed (s : Cursor) (r : Row) : Cursor :=
  if s.stopped then s else
  let next := if s.pending.head?.any (fun h => h.pkey != r.pkey) then flush s else s
  if next.stopped || (next.need == 0 && next.boundary.any (fun key => key.1 != r.pkey)) then
    {next with stopped := true}
  else
    let pending := r :: next.pending
    {next with pending, peak := max next.peak pending.length}

def run (off k : Nat) (files : List (List Row)) : Cursor :=
  flush (files.foldl (fun s file => file.foldl feed s)
    { skip := off, need := k, stopped := k == 0 })

def Goal : Prop := ∀ off k (files : List (List Row)),
  PrefixSorted files.flatten → files.flatten.Pairwise (fun a b => a.id < b.id) →
  let s := run off k files
  s.output = SqlFetchWithTies.pageSQL off k files.flatten ∧
  s.peak ≤ maxRunLen (splitRuns files.flatten) ∧ (k = 0 → s.peak = 0)

private def files : List (List Row) :=
  [[⟨1,9,90,0⟩], [], [⟨1,2,20,1⟩, ⟨1,2,21,2⟩, ⟨2,0,30,3⟩]]
example : ((run 0 1 files).output.map Row.payload, (run 0 1 files).peak) = ([20,21],3) := by decide

end TinylambOpt.ResearchGoals.StreamingRunPage
