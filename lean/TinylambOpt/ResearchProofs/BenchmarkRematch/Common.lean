import TinylambOpt.ResearchGoals.Compression.Wire
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Common
open TinylambOpt.ResearchGoals.Compression.Wire

theorem little_lt (n k : Nat) : ∀ b ∈ little n k, b < 256 := by
  intro b hb
  simp only [little, List.mem_map] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide)

theorem takeAt_pack (w : Nat) (xs : List Nat) (hx : ∀ x ∈ xs, x < 2 ^ w) (j y : Nat)
    (hj : xs[j]? = some y) : takeAt (pack w xs) w j = y := by
  have hinv : ∀ w xs, (∀ x ∈ xs, x < 2^w) →
      (List.range xs.length).map (get (pack w xs) w) = xs :=
    TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
  have hloc : ∀ bs w i, (∀ b ∈ bs, b < 256) → takeAt bs w i = get bs w i :=
    TinylambOpt.ResearchProofs.Compression.Wire.sound_LocalRead
  rw [hloc _ _ _ (by intro b hb; exact little_lt _ _ b hb)]
  have hlt : j < xs.length := (List.getElem?_eq_some_iff.mp hj).1
  have h := hinv w xs hx
  have h3 := congrArg (fun l => l[j]?) h
  simp [hlt, hj] at h3
  rw [h3]
  exact (List.getElem?_eq_some_iff.mp hj).2

end TinylambOpt.ResearchProofs.BenchmarkRematch.Common
