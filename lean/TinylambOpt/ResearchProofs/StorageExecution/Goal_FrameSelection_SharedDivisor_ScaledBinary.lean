import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SharedDivisor.ScaledBinary` — `∀ g xs, 0<g → g∈xs → (∀ x∈xs, x=0 ∨ x=g) → (write
  xs).length = ubytes g+1+byteCount xs.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_ScaledBinary

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SharedDivisor

private theorem foldl_gcd_self (g : Nat) (ys : List Nat)
    (hys : ∀ x ∈ ys, x = 0 ∨ x = g) : ys.foldl Nat.gcd g = g := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have hy : y = 0 ∨ y = g := hys y (by simp)
      have htail : ∀ x ∈ ys, x = 0 ∨ x = g := by
        intro x hx
        exact hys x (by simp [hx])
      rcases hy with rfl | rfl <;> simpa using ih htail

private theorem fold_gcd_eq (g : Nat) (hg : 0 < g) (xs : List Nat)
    (hmem : g ∈ xs) (hvals : ∀ x ∈ xs, x = 0 ∨ x = g) :
    xs.foldl Nat.gcd 0 = g := by
  induction xs with
  | nil => simp at hmem
  | cons x xs ih =>
      have hx : x = 0 ∨ x = g := hvals x (by simp)
      have htail : ∀ y ∈ xs, y = 0 ∨ y = g := by
        intro y hy
        exact hvals y (by simp [hy])
      rcases hx with hx0 | hxg
      · subst x
        have hgxs : g ∈ xs := by simpa [Nat.ne_of_gt hg] using hmem
        simpa using ih hgxs htail
      · subst x
        simpa using foldl_gcd_self g xs htail

private theorem max_some_of_ne_nil : ∀ {ys : List Nat}, ys ≠ [] → ∃ m, ys.max? = some m := by
  intro ys hne
  cases ys with
  | nil => contradiction
  | cons y ys =>
      rw [List.max?_cons]
      exact ⟨ys.max?.elim y (max y), rfl⟩

private theorem peak_eq_one (ys : List Nat) (hmem : 1 ∈ ys)
    (hvals : ∀ y ∈ ys, y = 0 ∨ y = 1) : peak ys = 1 := by
  have hne : ys ≠ [] := by
    intro hnil
    subst ys
    simp at hmem
  obtain ⟨m, hm⟩ := max_some_of_ne_nil hne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hm_mem : m ∈ ys := hmprops.1
  have hm_le : m ≤ 1 := by
    rcases hvals m hm_mem with rfl | rfl <;> omega
  have hone_le : 1 ≤ m := hmprops.2 1 hmem
  have hm_eq : m = 1 := by omega
  simp [peak, hm, hm_eq]

theorem sound_ScaledBinary : ScaledBinary.Goal := by
  intro g xs hg hmem hvals
  have hfg : xs.foldl Nat.gcd 0 = g := fold_gcd_eq g hg xs hmem hvals
  have hone : 1 ∈ xs.map (fun x => x / g) := by
    exact List.mem_map.mpr ⟨g, hmem, Nat.div_self hg⟩
  have hbin : ∀ y ∈ xs.map (fun x => x / g), y = 0 ∨ y = 1 := by
    intro y hy
    rcases List.mem_map.mp hy with ⟨x, hx, rfl⟩
    rcases hvals x hx with rfl | rfl
    · left
      simp
    · right
      simp [Nat.div_self hg]
  have hpeak : peak (xs.map (fun x => x / g)) = 1 := peak_eq_one _ hone hbin
  have huleb : ∀ n, (uleb n).length = ubytes n :=
    TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge
  simp [write, hfg, hpeak, width, ubytes, huleb, pack, little, Nat.add_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_ScaledBinary
