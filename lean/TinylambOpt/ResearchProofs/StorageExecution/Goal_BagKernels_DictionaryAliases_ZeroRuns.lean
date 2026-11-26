import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_ZeroRuns

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

/-!
`DictionaryAliases.ZeroRuns.Goal` is **not** declared in
`TinylambOpt.ResearchGoals.StorageExecution.BagKernels`: the only `ZeroRuns`
goal in that module lives in `MultiplicityRuns`, where runs with multiplicity
zero are filtered out of the representation before taking `live`.

The statement below is the natural DictionaryAliases analogue. There a run is
an ordinal selecting a dictionary slot, and its multiplicity is
`ids.count i`; an ordinal outside `0 .. d.length` resolves to no slot and
therefore contributes zero (the format comment calls such slots "invisible").
Discarding these zero-contribution ordinals, i.e. filtering `ids` down to
valid slots, leaves the live key set unchanged, exactly as dropping zero runs
does for `MultiplicityRuns`.
-/
private def ZeroRuns.Goal : Prop :=
  ∀ d ids, (live bag (d, ids.filter (fun i => i < d.length))).Perm (live bag (d, ids))

-- Sanity checks on small inputs (including an out-of-range ordinal).
#eval live bag ([5, 3, 5], [0, 1, 0, 9, 2])
#eval live bag ([5, 3, 5], [0, 1, 0, 9, 2].filter (fun i => i < 3))
example : (live bag ([5, 3, 5], [0, 1, 0, 9, 2].filter (fun i => i < 3))).Perm
    (live bag ([5, 3, 5], [0, 1, 0, 9, 2])) := by decide
example : (live bag ([7, 7, 4], [4, 0, 12, 1].filter (fun i => i < 2))).Perm
    (live bag ([7, 7, 4], [4, 0, 12, 1])) := by decide

private theorem snd_lt_length_of_mem_zipIdx {d : List Nat} {e : Nat × Nat}
    (h : e ∈ d.zipIdx) : e.2 < d.length := by
  obtain ⟨x, i⟩ := e
  have hi := List.mem_zipIdx h
  simpa using hi.2.1

private theorem sum_map_congr_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem mass_filter_valid (d ids : List Nat) (k : Nat) :
    bag.mass (d, ids.filter (fun i => i < d.length)) k = bag.mass (d, ids) k := by
  let L := d.zipIdx.filter (fun e => e.1 == k)
  have hcnt : ∀ e ∈ L,
      (ids.filter (fun i => i < d.length)).count e.2 = ids.count e.2 := by
    intro e he
    have hlt : e.2 < d.length :=
      snd_lt_length_of_mem_zipIdx (List.mem_filter.mp he).1
    exact List.count_filter (by simp [hlt])
  simpa [bag, L] using
    sum_map_congr_nat L
      (fun e => (ids.filter (fun i => i < d.length)).count e.2)
      (fun e => ids.count e.2) hcnt

theorem sound_ZeroRuns : ZeroRuns.Goal := by
  intro d ids
  have hmass : ∀ k,
      bag.mass (d, ids.filter (fun i => i < d.length)) k = bag.mass (d, ids) k :=
    mass_filter_valid d ids
  have hkeys₁ : bag.keys (d, ids.filter (fun i => i < d.length)) = d := by rfl
  have hkeys₂ : bag.keys (d, ids) = d := by rfl
  have hfun : (fun k => decide (0 < bag.mass (d, ids.filter (fun i => i < d.length)) k)) =
      (fun k => decide (0 < bag.mass (d, ids) k)) := by
    funext k
    rw [hmass k]
  have hlive : live bag (d, ids.filter (fun i => i < d.length)) = live bag (d, ids) := by
    unfold live
    rw [hkeys₁, hkeys₂, hfun]
  rw [hlive]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_ZeroRuns
