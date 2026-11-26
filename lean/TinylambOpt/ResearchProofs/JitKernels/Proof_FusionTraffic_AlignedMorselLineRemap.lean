import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-! SOUND: Translating a morsel by a whole number `k` of lines shifts every fetched line by
exactly `k` and rebuilds nothing: `lineOf (base + k*width) = k + lineOf base` pointwise
(`Nat.add_mul_div_right`), and `eraseDups` commutes with the injective map `x ↦ k + x`
because equal elements share the same dedup verdict, so line groups are preserved. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_AlignedMorselLineRemap
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-- Deciding `k + c == k + a` is deciding `c == a`. -/
private theorem not_beq_add_left (k a c : Nat) :
    (!(k + c == k + a)) = (!(c == a)) := by
  have key : ∀ p q : Bool, (p = true) = (q = true) → p = q := by
    intro p q; cases p <;> cases q <;> simp
  refine key (!(k + c == k + a)) (!(c == a)) ?_
  rw [Nat.not_beq_eq_true_eq, Nat.not_beq_eq_true_eq]
  exact propext ⟨fun h => by omega, fun h => by omega⟩

/-- Filtering by the dedup predicate commutes with the offset map. -/
private theorem filter_map_add (k a : Nat) : ∀ as : List Nat,
    (as.map (k + ·)).filter (fun b => !(b == (k + a)))
      = (as.filter (fun c => !(c == a))).map (k + ·) := by
  intro as
  induction as with
  | nil => rfl
  | cons c cs ih =>
    rw [List.map_cons, List.filter_cons, not_beq_add_left, ih, List.filter_cons]
    by_cases h : (!(c == a)) = true
    · rw [if_pos h, if_pos h, List.map_cons]
    · rw [if_neg h, if_neg h]

/-- `eraseDups` commutes with the injective offset map. -/
private theorem eraseDups_map_add (k : Nat) : ∀ l : List Nat,
    (l.map (k + ·)).eraseDups = l.eraseDups.map (k + ·) := by
  intro l
  have key : ∀ n : Nat, ∀ m : List Nat, m.length ≤ n →
      (m.map (k + ·)).eraseDups = m.eraseDups.map (k + ·) := by
    intro n
    induction n with
    | zero =>
        intro m hle
        match m with
        | [] => rfl
        | a :: as =>
          rw [List.length_cons] at hle
          exact absurd hle (by omega)
    | succ r ih =>
        intro m hle
        match m with
        | [] => rfl
        | a :: as =>
          have has : as.length ≤ r := by
            rw [List.length_cons] at hle
            omega
          rw [List.map_cons, List.eraseDups_cons, filter_map_add,
            ih (as.filter fun c => !(c == a))
              (Nat.le_trans (List.length_filter_le (fun c => !(c == a)) as) has),
            List.eraseDups_cons, List.map_cons]
  exact key l.length l (Nat.le_refl l.length)

/-- Aligned remap: shifting by whole lines shifts the footprint pointwise. -/
theorem sound : AlignedMorselLineRemap.Goal := by
  intro base stride width sel k hw
  show (List.map (fun i => (base + k * width + stride * i) / width) sel).eraseDups
      = (List.map (fun i => (base + stride * i) / width) sel).eraseDups.map (k + ·)
  have hpt : ∀ i : Nat,
      (base + k * width + stride * i) / width = k + (base + stride * i) / width := by
    intro i
    have h1 : base + k * width + stride * i = base + stride * i + k * width := by omega
    rw [h1, Nat.add_mul_div_right _ _ hw]
    omega
  have heq : (fun i => (base + k * width + stride * i) / width)
      = (k + ·) ∘ (fun i => (base + stride * i) / width) := by
    funext i
    exact hpt i
  rw [heq, ← List.map_map]
  exact eraseDups_map_add k _

end TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_AlignedMorselLineRemap
