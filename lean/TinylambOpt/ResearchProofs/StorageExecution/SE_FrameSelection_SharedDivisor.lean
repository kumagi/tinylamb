import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved (9 claims, including the following)

- `SharedDivisor.BoundedRaw` — A forced frame cap still limits worst-case expansion to
  raw cells and per-frame overhead.
- `SharedDivisor.ByteFrontier` — Use the actual codec and raw fallback in the size/seek
  optimum.
- `SharedDivisor.HotPointReads` — Raising point-read cost can only reduce the squared
  decoded lengths of optimal pages.

Refuted by counterexample

- `SharedDivisor.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
Storage Execution / Frame Selection research batch — model `SharedDivisor`.
Seven shared properties from `Common.lean`; `ScaleInvariance` and
`ScaledBinary` hold; `greedyCut` subadditivity fails for a concrete witness.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.SharedDivisor

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SharedDivisor

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, TinylambOpt.ResearchProofs.StorageExecution.SECommon.little_len]

private theorem hubytes1 : ubytes 1 = 1 := by simp [ubytes, width]

private theorem foldl_gcd_map_mul (g : Nat) : ∀ (xs : List Nat) (a : Nat),
    (xs.map (fun x => x * g)).foldl Nat.gcd (a * g) = (xs.foldl Nat.gcd a) * g := by
  intro xs
  induction xs with
  | nil => intro a; simp
  | cons x xt ih =>
      intro a
      simp only [List.map_cons, List.foldl_cons]
      rw [Nat.gcd_mul_right, ih]

theorem sound_ScaleInvariance : ScaleInvariance.Goal := by
  intro xs g hxs hg1 hg
  have hgs : (xs.map (fun x => x * g)).foldl Nat.gcd 0 = g := by
    have h := foldl_gcd_map_mul g xs 0
    rw [Nat.zero_mul] at h
    rw [h, hg1]
    simp
  have hdiv : ∀ x, (x * g) / g = x := fun x =>
    Nat.div_eq_of_eq_mul_right hg (Nat.mul_comm x g)
  have hvs : (xs.map (fun x => x * g)).map (fun v => v / g) = xs := by
    rw [List.map_map, show (fun v => v / g) ∘ (fun x => x * g) = fun x => x from by
      funext x
      exact hdiv x]
    simp
  have hvs1 : xs.map (fun x => x / 1) = xs := by simp [Nat.div_one]
  simp only [SharedDivisor.write, hgs, hg1, hvs, hvs1, List.length_append, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len,
    pack_length, hubytes1]
  omega

theorem sound_ScaledBinary : ScaledBinary.Goal := by
  intro g xs hg hgmem hall
  -- gcd of a list of 0/g values containing g is g
  have hdiv_all : ∀ (l : List Nat) (acc : Nat), l.foldl Nat.gcd acc ∣ acc := by
    intro l
    induction l with
    | nil => intro acc; exact Nat.dvd_refl acc
    | cons a l ih =>
        intro acc
        rw [List.foldl_cons]
        exact Nat.dvd_trans (ih (Nat.gcd acc a)) (Nat.gcd_dvd_left acc a)
  have hg_dvd_foldl : ∀ (l : List Nat) (acc : Nat), g ∣ acc → (∀ x ∈ l, g ∣ x) →
      g ∣ l.foldl Nat.gcd acc := by
    intro l
    induction l with
    | nil => intro acc h; simpa using h
    | cons a l ih =>
        intro acc h hmem
        show g ∣ l.foldl Nat.gcd (Nat.gcd acc a)
        refine ih (Nat.gcd acc a) (Nat.dvd_gcd h (hmem a (by simp))) ?_
        intro x hx; exact hmem x (by simp [hx])
  have hzero_foldl : ∀ (l : List Nat), (∀ x ∈ l, x = 0) → l.foldl Nat.gcd 0 = 0 := by
    intro l
    induction l with
    | nil => intro _; rfl
    | cons a l ih =>
        intro h
        rw [show List.foldl Nat.gcd 0 (a :: l) = l.foldl Nat.gcd (Nat.gcd 0 a) from by
          simp [List.foldl_cons]]
        rw [Nat.gcd_zero_left, h a (by simp)]
        exact ih (fun x hx => h x (by simp [hx]))
  have hgcd : xs.foldl Nat.gcd 0 = g := by
    induction xs with
    | nil => exact absurd hgmem (by simp)
    | cons a l ih =>
        rw [show List.foldl Nat.gcd 0 (a :: l) = List.foldl Nat.gcd a l from by
          simp [List.foldl_cons, Nat.gcd_zero_left]]
        cases hall a (by simp) with
        | inl h =>
            subst h
            cases List.mem_cons.mp hgmem with
            | inl h0 =>
                subst h0
                exact hzero_foldl l (fun x hx => by
                  rcases hall x (by simp [hx]) with h | h
                  · exact h
                  · exact h)
            | inr hl =>
                exact ih hl (fun x hx => hall x (by simp [hx]))
        | inr h =>
            rw [h]
            have h1 : l.foldl Nat.gcd g ∣ g := hdiv_all l g
            have h2 : g ∣ l.foldl Nat.gcd g :=
              hg_dvd_foldl l g (Nat.dvd_refl g) (fun x hx => by
                rcases hall x (by simp [hx]) with h' | h'
                · exact ⟨0, by simp [h']⟩
                · exact ⟨1, by simp [h']⟩)
            exact Nat.dvd_antisymm h1 h2
  have hpeak1 : peak (xs.map (fun x => x / g)) = 1 := by
    unfold peak
    have hmem : 1 ∈ xs.map (fun x => x / g) :=
      List.mem_map.mpr ⟨g, hgmem, by simp [Nat.div_self hg]⟩
    have hle : ∀ v ∈ xs.map (fun x => x / g), v ≤ 1 := by
      intro v hv
      rcases List.mem_map.mp hv with ⟨x, hx, rfl⟩
      rcases hall x hx with rfl | rfl
      · simp
      · simp [Nat.div_self hg]
    have hmax : (xs.map (fun x => x / g)).max? = some 1 := by
      rw [List.max?_eq_some_iff]
      exact ⟨hmem, fun b hb => hle b hb⟩
    rw [hmax]
    rfl
  have hw : width 1 = 1 := by simp [width]
  simp only [SharedDivisor.write, hgcd, hpeak1, hw, List.length_append, List.length_map,
    TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, pack_length]
  rw [Nat.mul_one, hubytes1]

/-- Mirror-verified counterexample: a=[1], b=[2^36], c=[0, 2^36]. -/
theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro hall
  exact absurd (hall [1] [2^36] [0, 2^36]) (by native_decide)

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.SharedDivisor
