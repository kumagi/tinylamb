import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `Drawdown.Bidirectional` — `∀ x, max (measure x) (measure x.reverse) = spread x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Drawdown_Bidirectional

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  simp only [pairs, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

private theorem pairs_singleton (a : Int) : pairs [a] = [] := by
  simp [pairs]

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem cart_singleton (xs : Series) (a : Int) :
    cart xs [a] = xs.map (fun b : Int => (b, a)) := by
  induction xs with
  | nil => simp [cart]
  | cons b bs ih => simp [cart_cons, ih]

private theorem mem_of_mem_pairs {x : Series} {a b : Int} (h : (a, b) ∈ pairs x) :
    a ∈ x ∧ b ∈ x := by
  induction x with
  | nil => simp [pairs] at h
  | cons c cs ih =>
      rw [pairs_cons] at h
      rcases List.mem_append.mp h with h | h
      · rcases List.mem_map.mp h with ⟨d, hd, heq⟩
        cases heq
        simp [hd]
      · have hh := ih h
        exact ⟨by simp [hh.1], by simp [hh.2]⟩

private theorem pairs_append_perm (x y : Series) :
    (pairs (x ++ y)).Perm (pairs x ++ pairs y ++ cart x y) := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.map_append]
      let A : List (Int × Int) := xs.map (fun b : Int => (a, b))
      let B : List (Int × Int) := y.map (fun b : Int => (a, b))
      let P : List (Int × Int) := pairs xs
      let Q : List (Int × Int) := pairs y
      let C : List (Int × Int) := cart xs y
      have h1 := (List.Perm.append_left (A ++ B)) ih
      have h2 := ((List.perm_append_comm (l₁ := B) (l₂ := P)).append_right (Q ++ C)).append_left A
      have h3 := ((List.perm_append_comm (l₁ := B) (l₂ := Q)).append_right C).append_left (A ++ P)
      have h1' : (A ++ (B ++ pairs (xs ++ y))).Perm (A ++ (B ++ (P ++ (Q ++ C)))) := by
        simpa [List.append_assoc] using h1
      have h2' : (A ++ (B ++ (P ++ (Q ++ C)))).Perm (A ++ (P ++ (B ++ (Q ++ C)))) := by
        simpa [List.append_assoc] using h2
      have h3' : (A ++ (P ++ (B ++ (Q ++ C)))).Perm (A ++ (P ++ (Q ++ (B ++ C)))) := by
        simpa [List.append_assoc] using h3
      simpa [List.append_assoc, A, B, P, Q, C] using (h1'.trans (h2'.trans h3'))

private theorem pairs_reverse_perm (x : Series) :
    (pairs x.reverse).Perm ((pairs x).map (fun p : Int × Int => (p.2, p.1))) := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.reverse_cons]
      have hp := pairs_append_perm xs.reverse [a]
      simp only [pairs_singleton, List.append_nil] at hp
      rw [cart_singleton] at hp
      have hrev : ((xs.reverse).map (fun b : Int => (b, a))).Perm
          (xs.map (fun b : Int => (b, a))) := by
        simp
      have h1 := ih.append hrev
      have h2 := List.perm_append_comm
        (l₁ := (pairs xs).map (fun p : Int × Int => (p.2, p.1)))
        (l₂ := xs.map (fun b : Int => (b, a)))
      simpa [pairs_cons, List.map_append, List.map_map, Function.comp_def] using
        (hp.trans (h1.trans h2))

private theorem max0_cons (a : Int) (xs : List Int) :
    max0 (a :: xs) = max 0 (max a (max0 xs)) := by
  simp only [max0, List.max?_cons]
  cases hx : xs.max? with
  | none => simp
  | some b =>
      simp only [Option.getD_some, Option.elim_some]
      change max 0 (max a b) = max 0 (max a (max 0 b))
      omega

private theorem max0_nonneg (xs : List Int) : 0 ≤ max0 xs := by
  simp only [max0]
  omega

private theorem le_max0_of_mem {xs : List Int} {a : Int} (ha : a ∈ xs) : a ≤ max0 xs := by
  induction xs with
  | nil => simp at ha
  | cons x xs ih =>
      rw [max0_cons]
      rcases List.mem_cons.mp ha with rfl | ha
      · omega
      · have hi := ih ha
        omega

private theorem max0_le {xs : List Int} {b : Int} (hb : 0 ≤ b)
    (h : ∀ a ∈ xs, a ≤ b) : max0 xs ≤ b := by
  induction xs with
  | nil => simpa [max0] using hb
  | cons x xs ih =>
      rw [max0_cons]
      exact Int.max_le.mpr ⟨hb, Int.max_le.mpr ⟨h x (by simp),
        ih (by intro a ha; exact h a (by simp [ha]))⟩⟩

private theorem max_mem_of_ne_nil {xs : Series} (h : xs ≠ []) : xs.max?.getD 0 ∈ xs := by
  cases xs with
  | nil => contradiction
  | cons a as =>
      cases hm : (a :: as).max? with
      | none => exact False.elim (h (List.max?_eq_none_iff.mp hm))
      | some M => exact (List.max?_eq_some_iff.mp hm).1

private theorem min_mem_of_ne_nil {xs : Series} (h : xs ≠ []) : xs.min?.getD 0 ∈ xs := by
  cases xs with
  | nil => contradiction
  | cons a as =>
      cases hm : (a :: as).min? with
      | none => exact False.elim (h (List.min?_eq_none_iff.mp hm))
      | some M => exact (List.min?_eq_some_iff.mp hm).1

private theorem le_max_of_mem {xs : Series} {a : Int} (ha : a ∈ xs) : a ≤ xs.max?.getD 0 := by
  cases xs with
  | nil => simp at ha
  | cons b bs =>
      cases hm : (b :: bs).max? with
      | none =>
          have heq : b :: bs = [] := List.max?_eq_none_iff.mp hm
          cases heq
      | some M =>
          have hspec := List.max?_eq_some_iff.mp hm
          simpa [hm] using hspec.2 a ha

private theorem min_le_of_mem {xs : Series} {a : Int} (ha : a ∈ xs) : xs.min?.getD 0 ≤ a := by
  cases xs with
  | nil => simp at ha
  | cons b bs =>
      cases hm : (b :: bs).min? with
      | none =>
          have heq : b :: bs = [] := List.min?_eq_none_iff.mp hm
          cases heq
      | some M =>
          have hspec := List.min?_eq_some_iff.mp hm
          simpa [hm] using hspec.2 a ha

private theorem spread_nonneg (x : Series) : 0 ≤ spread x := by
  by_cases hx : x = []
  · simp [hx, spread]
  · have hm := min_mem_of_ne_nil hx
    have hle := le_max_of_mem hm
    unfold spread
    omega

private theorem measure_le_spread (x : Series) : Drawdown.measure x ≤ spread x := by
  unfold Drawdown.measure
  apply max0_le (spread_nonneg x)
  intro v hv
  rcases List.mem_map.mp hv with ⟨⟨pa, pb⟩, hp, rfl⟩
  rcases mem_of_mem_pairs hp with ⟨ha, hb⟩
  change pa - pb ≤ spread x
  have hA := le_max_of_mem ha
  have hB := min_le_of_mem hb
  unfold spread
  omega

private theorem maxD_reverse (x : Series) : x.reverse.max?.getD 0 = x.max?.getD 0 := by
  by_cases hx : x = []
  · simp [hx]
  · cases hm : x.max? with
    | none => exact False.elim (hx (List.max?_eq_none_iff.mp hm))
    | some M =>
        have hs := List.max?_eq_some_iff.mp hm
        have hrev : x.reverse.max? = some M := List.max?_eq_some_iff.mpr ⟨by simpa using hs.1,
          by intro b hb; apply hs.2; simpa using hb⟩
        simp [hrev]

private theorem minD_reverse (x : Series) : x.reverse.min?.getD 0 = x.min?.getD 0 := by
  by_cases hx : x = []
  · simp [hx]
  · cases hm : x.min? with
    | none => exact False.elim (hx (List.min?_eq_none_iff.mp hm))
    | some M =>
        have hs := List.min?_eq_some_iff.mp hm
        have hrev : x.reverse.min? = some M := List.min?_eq_some_iff.mpr ⟨by simpa using hs.1,
          by intro b hb; apply hs.2; simpa using hb⟩
        simp [hrev]

private theorem spread_reverse (x : Series) : spread x.reverse = spread x := by
  simp [spread, maxD_reverse, minD_reverse]

private theorem pair_or_pair_rev {x : Series} {a b : Int} (hne : a ≠ b)
    (ha : a ∈ x) (hb : b ∈ x) : (a, b) ∈ pairs x ∨ (b, a) ∈ pairs x := by
  induction x with
  | nil => simp at ha
  | cons c cs ih =>
      rw [pairs_cons]
      rcases List.mem_cons.mp ha with ha | ha
      · subst a
        rcases List.mem_cons.mp hb with hb | hb
        · subst b
          exact False.elim (hne rfl)
        · exact Or.inl (List.mem_append_left _ (List.mem_map.mpr ⟨b, hb, rfl⟩))
      · rcases List.mem_cons.mp hb with hb | hb
        · subst b
          exact Or.inr (List.mem_append_left _ (List.mem_map.mpr ⟨a, ha, rfl⟩))
        · rcases ih ha hb with h | h
          · exact Or.inl (List.mem_append_right _ h)
          · exact Or.inr (List.mem_append_right _ h)

theorem sound_Bidirectional : Drawdown.Bidirectional.Goal := by
  intro x
  apply Int.le_antisymm
  · apply Int.max_le.mpr
    constructor
    · exact measure_le_spread x
    · rw [← spread_reverse x]
      exact measure_le_spread x.reverse
  · by_cases hx : x = []
    · subst x
      simp [spread, Drawdown.measure, max0, pairs]
    · have hM : x.max?.getD 0 ∈ x := max_mem_of_ne_nil hx
      have hm : x.min?.getD 0 ∈ x := min_mem_of_ne_nil hx
      by_cases heq : x.max?.getD 0 = x.min?.getD 0
      · have hnon : 0 ≤ Drawdown.measure x := by
          unfold Drawdown.measure
          exact max0_nonneg _
        have hnon2 : 0 ≤ Drawdown.measure x.reverse := by
          unfold Drawdown.measure
          exact max0_nonneg _
        unfold spread
        omega
      · rcases pair_or_pair_rev heq hM hm with h | h
        · have hv : x.max?.getD 0 - x.min?.getD 0 ∈
              (pairs x).map (fun p : Int × Int => p.1 - p.2) :=
            List.mem_map.mpr ⟨(x.max?.getD 0, x.min?.getD 0), h, rfl⟩
          have hle := le_max0_of_mem hv
          unfold spread
          unfold Drawdown.measure at hle ⊢
          exact Int.le_trans hle (Int.le_max_left _ _)
        · have hmemMap : (x.max?.getD 0, x.min?.getD 0) ∈
              (pairs x).map (fun p : Int × Int => (p.2, p.1)) :=
            List.mem_map.mpr ⟨(x.min?.getD 0, x.max?.getD 0), h, rfl⟩
          have hrev : (x.max?.getD 0, x.min?.getD 0) ∈ pairs x.reverse :=
            (pairs_reverse_perm x).symm.subset hmemMap
          have hv : x.max?.getD 0 - x.min?.getD 0 ∈
              (pairs x.reverse).map (fun p : Int × Int => p.1 - p.2) :=
            List.mem_map.mpr ⟨(x.max?.getD 0, x.min?.getD 0), hrev, rfl⟩
          have hle := le_max0_of_mem hv
          unfold spread
          unfold Drawdown.measure at hle ⊢
          exact Int.le_trans hle (Int.le_max_right _ _)

end TinylambOpt.ResearchProofs.Atlas.Goal_Drawdown_Bidirectional
