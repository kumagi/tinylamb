import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Samples_SelectivityLaw

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples

/-- Counting under a mapped list is counting the pulled-back predicate. -/
private theorem countP_map_comp {α β : Type} (ss : List α) (f : α → β) (p : β → Bool) :
    (ss.map f).countP p = ss.countP (fun s => p (f s)) := by
  induction ss with
  | nil => simp
  | cons s ss ih =>
      simp only [List.map_cons, List.countP_cons]
      rw [ih]

/-- Every enumerated sample has exactly the requested length. -/
private theorem samples_mem_length {α : Type} : ∀ (xs : List α) (k : Nat) (s : List α),
    s ∈ samples k xs → s.length = k
  | [], k, s, hs => by
      cases k with
      | zero => simp [samples] at hs; subst s; rfl
      | succ k => simp [samples] at hs
  | x :: xs, k, s, hs => by
      cases k with
      | zero => simp [samples] at hs; subst s; rfl
      | succ k =>
          rw [samples, List.mem_append] at hs
          rcases hs with hs | hs
          · exact samples_mem_length xs (k + 1) s hs
          · rw [List.mem_map] at hs
            rcases hs with ⟨t, ht, rfl⟩
            simp only [List.length_cons]
            rw [samples_mem_length xs k t ht]

private theorem count_true_le_length (s : List Bool) : s.count true ≤ s.length := by
  induction s with
  | nil => simp
  | cons x s ih =>
      cases x <;> simp <;> omega

/-- A sample shorter than h cannot contain h true entries. -/
private theorem countP_true_count_zero (ss : List (List Bool)) (k h : Nat)
    (hlen : ∀ s ∈ ss, s.length = k) (hh : k < h) :
    ss.countP (fun s => s.count true == h) = 0 := by
  apply List.countP_eq_zero.mpr
  intro s hs hp
  have hct := count_true_le_length s
  have hslen := hlen s hs
  have heq : s.count true = h := of_decide_eq_true hp
  omega

/-- The full histogram law: choosing h of the true rows and k-h of the
    false rows counts exactly the samples with h true entries. -/
private theorem selectivity_count : ∀ (xs : List Bool) (k h : Nat), h ≤ k →
    (samples k xs).countP (fun s => s.count true == h) =
      choose (xs.count true) h * choose (xs.count false) (k - h)
  | [], k, h, hh => by
      cases k with
      | zero =>
          have hz : h = 0 := by omega
          subst h
          simp [samples, choose]
      | succ k =>
          rw [show samples (k + 1) ([] : List Bool) = [] by rfl]
          simp only [List.countP_nil]
          cases h <;> simp [choose]
  | x :: xs, k, h, hh => by
      cases k with
      | zero =>
          have hz : h = 0 := by omega
          subst h
          simp [samples, choose]
      | succ k =>
          rw [samples, List.countP_append, countP_map_comp]
          have ih1 := selectivity_count xs (k + 1) h (by omega)
          have ihz := selectivity_count xs k 0 (by omega)
          cases h with
          | zero =>
              cases x with
              | false =>
                  have hpred : (fun s : List Bool => (false :: s).count true == 0) =
                      (fun s : List Bool => s.count true == 0) := by
                    funext s
                    simp
                  rw [hpred, ih1, ihz]
                  simp [choose]
                  grind
              | true =>
                  have hpred : (fun s : List Bool => (true :: s).count true == 0) =
                      (fun _ : List Bool => false) := by
                    funext s
                    simp
                  rw [hpred, ih1]
                  simp [choose]
          | succ h =>
              cases x with
              | false =>
                  have hpred : (fun s : List Bool => (false :: s).count true == h + 1) =
                      (fun s : List Bool => s.count true == h + 1) := by
                    funext s
                    simp
                  rw [hpred, ih1]
                  by_cases heq : h = k
                  · subst k
                    have hz := countP_true_count_zero (samples h xs) h (h + 1)
                      (fun s hs => samples_mem_length xs h s hs) (by omega)
                    rw [hz]
                    simp [choose]
                  · have ih0 := selectivity_count xs k (h + 1) (by omega)
                    rw [ih0]
                    have haddsub : k + 1 - (h + 1) = k - h := by omega
                    rw [haddsub]
                    have hm : k - h = (k - h - 1) + 1 := by omega
                    have hm' : k - (h + 1) = k - h - 1 := by omega
                    rw [hm, hm']
                    have hcf : (false :: xs).count false = xs.count false + 1 := by
                      simp
                    rw [hcf]
                    change choose (xs.count true) (h + 1) *
                          choose (xs.count false) (k - h - 1 + 1) +
                        choose (xs.count true) (h + 1) *
                          choose (xs.count false) (k - h - 1) =
                      choose (xs.count true) (h + 1) *
                        choose (xs.count false + 1) (k - h - 1 + 1)
                    rw [choose, Nat.mul_add]
                    grind
              | true =>
                  have ih0 := selectivity_count xs k h (by omega)
                  have hpred : (fun s : List Bool => (true :: s).count true == h + 1) =
                      (fun s : List Bool => s.count true == h) := by
                    funext s
                    simp
                  rw [hpred, ih1, ih0]
                  have haddsub : k + 1 - (h + 1) = k - h := by omega
                  rw [haddsub]
                  have hct : (true :: xs).count true = xs.count true + 1 := by
                    simp
                  rw [hct]
                  change choose (xs.count true) (h + 1) * choose (xs.count false) (k - h) +
                      choose (xs.count true) h * choose (xs.count false) (k - h) =
                    choose (xs.count true + 1) (h + 1) * choose (xs.count false) (k - h)
                  rw [choose]
                  grind

theorem sound_Samples_SelectivityLaw : Samples.SelectivityLaw.Goal := by
  intro xs k h hh
  exact selectivity_count xs k h hh

end TinylambOpt.ResearchProofs.Atlas.Goal_Samples_SelectivityLaw
