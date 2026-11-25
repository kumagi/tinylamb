import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_BoundarySlack

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

/-! The greedy scanner behind `pack`/`required`.  We show that starting a scan
with a different current load changes the number of block boundaries by at most
one, whatever suffix is scanned. -/

private def step (cap : Nat) (s : Nat × Nat) (w : Nat) : Nat × Nat :=
  if s.2 + w ≤ cap then (s.1, s.2 + w) else (s.1 + 1, w)

private def scan (cap : Nat) (s : Nat × Nat) : List Nat → Nat × Nat
  | [] => s
  | w :: ws => scan cap (step cap s w) ws

private theorem foldl_eq_scan (cap : Nat) (s : Nat × Nat) (xs : List Nat) :
    xs.foldl (fun t w => if t.2 + w ≤ cap then (t.1, t.2 + w) else (t.1 + 1, w)) s =
      scan cap s xs := by
  induction xs generalizing s with
  | nil => rfl
  | cons w ws ih =>
      rw [List.foldl_cons, ih]
      rfl

private theorem pack_eq_scan (cap : Nat) (xs : List Nat) :
    ParallelCuts.pack cap xs = scan cap (0, 0) xs := by
  unfold ParallelCuts.pack
  exact foldl_eq_scan cap (0, 0) xs

private theorem scan_append (cap : Nat) (s : Nat × Nat) (xs ys : List Nat) :
    scan cap s (xs ++ ys) = scan cap (scan cap s xs) ys := by
  induction xs generalizing s with
  | nil => rfl
  | cons w ws ih =>
      simp only [List.cons_append, scan, step]
      rw [ih]

private theorem scan_fst (cap n t : Nat) (xs : List Nat) :
    (scan cap (n, t) xs).1 = n + (scan cap (0, t) xs).1 := by
  induction xs generalizing n t with
  | nil => simp [scan]
  | cons w ws ih =>
      simp only [scan, step]
      by_cases h : t + w ≤ cap
      · simp only [ite_eq_left h]
        exact ih n (t + w)
      · simp only [ite_eq_right h]
        rw [ih (n + 1) w, ih 1 w]
        omega

private theorem scan_snd (cap n t : Nat) (xs : List Nat) :
    (scan cap (n, t) xs).2 = (scan cap (0, t) xs).2 := by
  induction xs generalizing n t with
  | nil => simp [scan]
  | cons w ws ih =>
      simp only [scan, step]
      by_cases h : t + w ≤ cap
      · simp only [ite_eq_left h]
        exact ih n (t + w)
      · simp only [ite_eq_right h, Nat.zero_add]
        rw [ih (n + 1) w, ih 1 w]

private theorem scan_snd_le (cap t : Nat) (xs : List Nat) (ht : t ≤ cap)
    (hall : ∀ w ∈ xs, w ≤ cap) : (scan cap (0, t) xs).2 ≤ cap := by
  induction xs generalizing t with
  | nil => simpa [scan] using ht
  | cons w ws ih =>
      have hw : w ≤ cap := hall w (by simp)
      have hws : ∀ v ∈ ws, v ≤ cap := fun v hv => hall v (by simp [hv])
      simp only [scan, step]
      by_cases h : t + w ≤ cap
      · simp only [ite_eq_left h]
        exact ih (t + w) h hws
      · simp only [ite_eq_right h, Nat.zero_add]
        rw [scan_snd cap 1 w ws]
        exact ih w hw hws

private def clos (cap t : Nat) (xs : List Nat) : Nat := (scan cap (0, t) xs).1

private theorem clos_cons (cap t w : Nat) (ws : List Nat) :
    clos cap t (w :: ws) =
      if t + w ≤ cap then clos cap (t + w) ws else clos cap w ws + 1 := by
  unfold clos
  simp only [scan, step, Nat.zero_add]
  by_cases h : t + w ≤ cap
  · simp only [ite_eq_left h]
  · simp only [ite_eq_right h]
    rw [scan_fst cap 1 w ws]
    omega

/-! Two mutually reinforcing facts about the number of boundaries produced by a
suffix: changing the initial load by an arbitrary amount changes it by at most
one, and a larger initial load never needs fewer boundaries. -/
private theorem clos_props (cap : Nat) : ∀ ws : List Nat,
    (∀ w ∈ ws, w ≤ cap) →
    (∀ t u, t ≤ cap → u ≤ cap → clos cap t ws ≤ clos cap u ws + 1) ∧
    (∀ t u, t ≤ u → clos cap t ws ≤ clos cap u ws) := by
  intro ws
  induction ws with
  | nil =>
      intro hall
      constructor <;> intro t u h <;> simp [clos, scan]
  | cons w ws ih =>
      intro hall
      have hw : w ≤ cap := hall w (by simp)
      have htail : ∀ v ∈ ws, v ≤ cap := fun v hv => hall v (by simp [hv])
      have hIH := ih htail
      constructor
      · intro t u ht hu
        rw [clos_cons, clos_cons]
        by_cases htw : t + w ≤ cap
        · by_cases huw : u + w ≤ cap
          · simp only [ite_eq_left htw, ite_eq_left huw]
            exact hIH.1 (t + w) (u + w) htw huw
          · simp only [ite_eq_left htw, ite_eq_right huw]
            have hP := hIH.1 (t + w) w htw hw
            omega
        · by_cases huw : u + w ≤ cap
          · simp only [ite_eq_right htw, ite_eq_left huw]
            have hQ := hIH.2 w (u + w) (by omega)
            omega
          · simp only [ite_eq_right htw, ite_eq_right huw]
            omega
      · intro t u htu
        rw [clos_cons, clos_cons]
        by_cases htw : t + w ≤ cap
        · by_cases huw : u + w ≤ cap
          · simp only [ite_eq_left htw, ite_eq_left huw]
            exact hIH.2 (t + w) (u + w) (by omega)
          · simp only [ite_eq_left htw, ite_eq_right huw]
            have hP := hIH.1 (t + w) w htw hw
            omega
        · have huw : ¬ u + w ≤ cap := by omega
          simp only [ite_eq_right htw, ite_eq_right huw]
          omega

private theorem isEmpty_false {xs : List Nat} (h : xs ≠ []) : xs.isEmpty = false := by
  cases xs with
  | nil => contradiction
  | cons x xs => rfl

theorem sound_BoundarySlack : ParallelCuts.BoundarySlack.Goal := by
  intro cap a b hall
  have halla : ∀ w ∈ a, w ≤ cap := fun w hw => hall w (by simp [hw])
  have hallb : ∀ w ∈ b, w ≤ cap := fun w hw => hall w (by simp [hw])
  by_cases ha : a = []
  · subst a
    simp [ParallelCuts.required]
  by_cases hb : b = []
  · subst b
    simp [ParallelCuts.required]
  rcases hA : scan cap (0, 0) a with ⟨na, ta⟩
  have hta : ta ≤ cap := by
    have h := scan_snd_le cap 0 a (by omega) halla
    simpa [hA] using h
  have hreq_a : ParallelCuts.required cap a = na + 1 := by
    unfold ParallelCuts.required
    rw [pack_eq_scan, hA, isEmpty_false ha]
    simp
  have hreq_b : ParallelCuts.required cap b = clos cap 0 b + 1 := by
    unfold ParallelCuts.required
    rw [pack_eq_scan, isEmpty_false hb]
    simp only [Bool.false_eq_true, ↓reduceIte]
    rfl
  have hreq_ab : ParallelCuts.required cap (a ++ b) = na + clos cap ta b + 1 := by
    unfold ParallelCuts.required
    rw [pack_eq_scan]
    rw [isEmpty_false (by simp [ha])]
    simp only [Bool.false_eq_true, ↓reduceIte]
    rw [scan_append, hA, scan_fst]
    rfl
  have hP1 : clos cap ta b ≤ clos cap 0 b + 1 :=
    (clos_props cap b hallb).1 ta 0 hta (by omega)
  have hP2 : clos cap 0 b ≤ clos cap ta b :=
    (clos_props cap b hallb).2 0 ta (by omega)
  rw [hreq_a, hreq_b, hreq_ab]
  have hsub : na + 1 + (clos cap 0 b + 1) - 1 = na + clos cap 0 b + 1 := by omega
  rw [hsub]
  omega

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_BoundarySlack
