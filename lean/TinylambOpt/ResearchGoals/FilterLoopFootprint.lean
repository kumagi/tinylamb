import Std

/-!
These claims compare filter loops by their evaluations and selected rows. A SQL executor
can reduce predicate work subject to the observed evaluation and result contract.

Modeled claims

- `Goal` — `∀ (xs : List Int64) (k : Int64), (schedule xs.length).flatMap accessed =
  List.range xs.length ∧ writes xs k = xs.zipIdx.map (fun (x,i) => (i, some (if x ==
  k then 1 else 0)))`
-/

namespace TinylambOpt.ResearchGoals.FilterLoopFootprint

-- Element offsets for the 32-row main loop (4 vector loads), 8-row
-- epilogue, and scalar tail of the saved equality-filter module.
structure Access where
  first : Nat
  lanes : Nat
  deriving DecidableEq

def schedule (n : Nat) : List Access :=
  let main := (List.range (n / 32)).flatMap fun i =>
    (List.range 4).map fun j => ⟨32*i + 8*j, 8⟩
  let epilogue := (List.range ((n % 32) / 8)).map fun i => ⟨n / 32 * 32 + 8*i, 8⟩
  let tail := (List.range (n % 8)).map fun j => ⟨n / 8 * 8 + j, 1⟩
  main ++ epilogue ++ tail

def accessed (a : Access) := (List.range a.lanes).map (a.first + ·)

-- A failed read is observable rather than replaced with a default value.
def writes (xs : List Int64) (k : Int64) : List (Nat × Option UInt8) :=
  (schedule xs.length).flatMap fun a => (accessed a).map fun i =>
    (i, xs[i]?.map (fun x => if x == k then 1 else 0))

def Goal : Prop := ∀ (xs : List Int64) (k : Int64),
  (schedule xs.length).flatMap accessed = List.range xs.length ∧
  writes xs k = xs.zipIdx.map (fun (x,i) => (i, some (if x == k then 1 else 0)))

example : (schedule 41).map Access.lanes = [8,8,8,8,8,1] := by decide
example : schedule 0 = [] := by decide
example : writes [-1,0,-1] (-1) = [(0,some 1),(1,some 0),(2,some 1)] := by decide

end TinylambOpt.ResearchGoals.FilterLoopFootprint
