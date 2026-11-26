import TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
import TinylambOpt.ResearchProofs.BenchmarkRematch.Common

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpcdsClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
open TinylambOpt.ResearchGoals.Compression.Wire

theorem R : ∀ (rows : List (Option Nat)) (i v : Nat), rows[i]? = some (some v) →
    (rows.filterMap id)[((rows.map Option.isSome).take i).count true]? = some v := by
  intro rows
  induction rows with
  | nil => intro i v h; simp at h
  | cons a rows ih =>
    intro i v h
    cases i with
    | zero =>
      simp at h
      subst h
      simp
    | succ i =>
      simp only [List.getElem?_cons_succ] at h
      have := ih i v h
      cases a with
      | none => simpa using this
      | some x => simpa [List.count_cons] using this

theorem sound : TpcdsClickHouse.Goal := by
  intro rows base i w hr
  dsimp only
  cases hri : rows[i]? with
  | none =>
    have : (rows.map Option.isSome)[i]? = none := by simp [hri]
    simp [this]
  | some o =>
    cases o with
    | none =>
      have : (rows.map Option.isSome)[i]? = some false := by simp [hri]
      simp [this]
    | some v =>
      have hm : (rows.map Option.isSome)[i]? = some true := by simp [hri]
      have hmem : v ∈ rows.filterMap id := by
        rw [List.mem_filterMap]
        exact ⟨some v, List.mem_of_getElem? hri, rfl⟩
      have hv := hr v hmem
      have hj := R rows i v hri
      have hx : ∀ x ∈ (rows.filterMap id).map (· - base), x < 2 ^ w := by
        intro x hx
        simp only [List.mem_map] at hx
        obtain ⟨y, hy, rfl⟩ := hx
        exact (hr y hy).2
      have hjm : ((rows.filterMap id).map (· - base))[((rows.map Option.isSome).take i).count true]? = some (v - base) := by
        simp [hj]
      have := TinylambOpt.ResearchProofs.BenchmarkRematch.Common.takeAt_pack w _ hx _ _ hjm
      simp [hm, this]
      omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpcdsClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpcdsClickHouse.sound
