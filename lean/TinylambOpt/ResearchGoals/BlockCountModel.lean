import Std

/-!
opened is recorded in reverse for constant-time append at observation.
-/

namespace TinylambOpt.ResearchGoals.BlockCountModel

inductive Mode where | skip | all | inspect
  deriving DecidableEq
structure Block (α σ : Type) where
  header : σ
  data : List α
  size : Nat
structure Result where
  count : Nat := 0
  opened : List Nat := []
  deriving DecidableEq

def execute {α σ : Type} (classify : σ → Mode) (test : α → Bool)
    (pages : List (Block α σ)) : Result :=
  pages.zipIdx.foldl (fun s (b,i) =>
    match classify b.header with
    | .skip => s
    | .all => {s with count := s.count + b.size}
    | .inspect => ⟨s.count + (b.data.filter test).length, i :: s.opened⟩) {}

/-- opened is recorded in reverse for constant-time append at observation. -/
def reads (r : Result) := r.opened.reverse

end TinylambOpt.ResearchGoals.BlockCountModel
