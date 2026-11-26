import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `Drawdown.Bidirectional` — `∀ x, max (measure x) (measure x.reverse) = spread x`
- `Drawdown.CrossExtreme` — `∀ x y, x ≠ [] → y ≠ [] → measure (x++y) = max (max
  (measure x) (measure y)) (x.max?.getD 0-y.min?.getD 0)`
- `Drawdown.DeleteMonotone` — `∀ x (keep : Int → Bool), measure (x.filter keep) ≤
  measure x`
- `Drawdown.DuplicatePeriods` — `∀ x n, 2 ≤ n → measure ((List.replicate n x).flatten)
  = spread x`
- `Drawdown.QuoteFlip` — `∀ x, measure (scaled (-1) x) = measure x.reverse`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.Drawdown

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! ### Generic helpers (pairs, max0, extremes) -/

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem pairs_nil : pairs [] = [] := by
  unfold pairs
  rw [List.zipIdx_nil]
  rfl

private theorem pairs_singleton (a : Int) : pairs [a] = [] := by
  simpa [pairs_nil] using pairs_cons a []

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.map_cons, pairs_cons, ih, List.map_append, List.map_map,
        Function.comp_def]

private theorem cart_singleton (xs : Series) (a : Int) :
    cart xs [a] = xs.map (fun b : Int => (b, a)) := by
  induction xs with
  | nil => simp [cart]
  | cons b bs ih => simp [cart_cons, ih]

private theorem cart_singleton_map_diff (xs : Series) (a : Int) :
    ((cart xs [a]).map (fun p : Int × Int => p.1 - p.2)) =
      xs.map (fun b : Int => b - a) := by
  rw [cart_singleton]
  simp

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

private theorem max0_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : max0 l₁ = max0 l₂ := by
  apply Int.le_antisymm
  · apply max0_le (max0_nonneg _)
    intro a ha
    exact le_max0_of_mem (p.subset ha)
  · apply max0_le (max0_nonneg _)
    intro a ha
    exact le_max0_of_mem (p.symm.subset ha)

private theorem max0_append (l₁ l₂ : List Int) : max0 (l₁ ++ l₂) = max (max0 l₁) (max0 l₂) := by
  induction l₁ with
  | nil => simp [max0]
  | cons a as ih =>
      rw [List.cons_append, max0_cons, ih, max0_cons]
      omega

private theorem max0_map_sublist {α : Type} (f : α → Int) {xs ys : List α}
    (h : xs.Sublist ys) : max0 (xs.map f) ≤ max0 (ys.map f) := by
  apply max0_le (max0_nonneg _)
  intro a ha
  exact le_max0_of_mem ((h.map f).mem ha)

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

private theorem max_cons_eq_of_ne_nil (a : Int) {xs : Series} (hxs : xs ≠ []) :
    (a :: xs).max?.getD 0 = max a (xs.max?.getD 0) := by
  cases hm : xs.max? with
  | none => exact False.elim (hxs (List.max?_eq_none_iff.mp hm))
  | some M => simp [List.max?_cons, hm]

private theorem min_cons_eq_of_ne_nil (a : Int) {xs : Series} (hxs : xs ≠ []) :
    (a :: xs).min?.getD 0 = min a (xs.min?.getD 0) := by
  cases hm : xs.min? with
  | none => exact False.elim (hxs (List.min?_eq_none_iff.mp hm))
  | some M => simp [List.min?_cons, hm]

private theorem spread_nonneg (x : Series) : 0 ≤ spread x := by
  by_cases hx : x = []
  · subst x
    simp [spread]
  · have hm := min_mem_of_ne_nil hx
    have hle := le_max_of_mem hm
    unfold spread
    omega

private theorem max0_cart_diff (x y : Series) (hx : x ≠ []) (hy : y ≠ []) :
    max0 ((cart x y).map (fun p : Int × Int => p.1 - p.2)) =
      max 0 (x.max?.getD 0 - y.min?.getD 0) := by
  have hMmem : x.max?.getD 0 ∈ x := max_mem_of_ne_nil hx
  have hmmem : y.min?.getD 0 ∈ y := min_mem_of_ne_nil hy
  apply Int.le_antisymm
  · apply max0_le (by omega : 0 ≤ max 0 (x.max?.getD 0 - y.min?.getD 0))
    intro v hv
    rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
    unfold cart at hp
    rcases List.mem_flatMap.mp hp with ⟨u, hu, hmem⟩
    rcases List.mem_map.mp hmem with ⟨b', hb', heq⟩
    cases heq
    have haM : a ≤ x.max?.getD 0 := le_max_of_mem hu
    have hmb : y.min?.getD 0 ≤ b := min_le_of_mem hb'
    omega
  · have hpair : (x.max?.getD 0, y.min?.getD 0) ∈ cart x y := by
      unfold cart
      apply List.mem_flatMap.mpr
      refine ⟨x.max?.getD 0, hMmem, ?_⟩
      exact List.mem_map.mpr ⟨y.min?.getD 0, hmmem, rfl⟩
    have hdiff : x.max?.getD 0 - y.min?.getD 0 ∈
        (cart x y).map (fun p : Int × Int => p.1 - p.2) :=
      List.mem_map.mpr ⟨(x.max?.getD 0, y.min?.getD 0), hpair, rfl⟩
    have hle := le_max0_of_mem hdiff
    have hnonneg := max0_nonneg ((cart x y).map (fun p : Int × Int => p.1 - p.2))
    omega

private theorem measure_le_spread (x : Series) : Drawdown.measure x ≤ spread x := by
  by_cases hx : x = []
  · subst x
    simp [Drawdown.measure, spread, max0, pairs]
  · apply max0_le (spread_nonneg x)
    intro v hv
    rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
    have hmem := mem_of_mem_pairs hp
    have haM := le_max_of_mem hmem.1
    have hmb := min_le_of_mem hmem.2
    unfold spread
    change a - b ≤ x.max?.getD 0 - x.min?.getD 0
    omega

private theorem measure_append_ge_left (x y : Series) :
    Drawdown.measure x ≤ Drawdown.measure (x ++ y) := by
  unfold Drawdown.measure
  apply max0_le (max0_nonneg _)
  intro v hv
  rcases List.mem_map.mp hv with ⟨p, hp, rfl⟩
  have hprhs : p ∈ pairs x ++ pairs y ++ cart x y := by simp [hp]
  have hp' : p ∈ pairs (x ++ y) := (pairs_append_perm x y).symm.subset hprhs
  exact le_max0_of_mem (List.mem_map.mpr ⟨p, hp', rfl⟩)

private theorem mem_flatten_replicate {a : Int} {k : Nat} {x : Series}
    (h : a ∈ (List.replicate k x).flatten) : a ∈ x := by
  induction k with
  | zero => simp at h
  | succ k ih =>
      simp only [List.replicate, List.flatten_cons, List.mem_append] at h
      rcases h with h | h
      · exact h
      · exact ih h

private theorem measure_le_of_mem (l x : Series) (hall : ∀ a ∈ l, a ∈ x) :
    Drawdown.measure l ≤ spread x := by
  apply max0_le (spread_nonneg x)
  intro v hv
  rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
  have hmem := mem_of_mem_pairs hp
  have ha := hall a hmem.1
  have hb := hall b hmem.2
  have haM := le_max_of_mem ha
  have hmb := min_le_of_mem hb
  unfold spread
  change a - b ≤ x.max?.getD 0 - x.min?.getD 0
  omega

private theorem measure_cons (a : Int) (xs : Series) :
    Drawdown.measure (a :: xs) =
      max (max0 (xs.map (fun b : Int => a - b))) (Drawdown.measure xs) := by
  unfold Drawdown.measure
  rw [pairs_cons]
  simp only [List.map_append, List.map_map, Function.comp_def]
  rw [max0_append]

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
      have h1 := List.Perm.append ih hrev
      have h2 := List.perm_append_comm
        (l₁ := (pairs xs).map (fun p : Int × Int => (p.2, p.1)))
        (l₂ := xs.map (fun b : Int => (b, a)))
      simpa [pairs_cons, List.map_append, List.map_map, Function.comp_def] using
        (hp.trans (h1.trans h2))

private theorem measure_reverse_append_singleton (a : Int) (xs : Series) :
    Drawdown.measure (xs.reverse ++ [a]) =
      max (Drawdown.measure xs.reverse) (max0 (xs.map (fun b : Int => b - a))) := by
  unfold Drawdown.measure
  have hp := (pairs_append_perm xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)
  have hm := max0_perm hp
  rw [hm]
  simp only [pairs_singleton, List.append_nil, List.map_append]
  rw [max0_append]
  have hcartmax : max0 ((cart xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)) =
      max0 (xs.map (fun b : Int => b - a)) := by
    calc
      max0 ((cart xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)) =
          max0 ((xs.reverse).map (fun b : Int => b - a)) := by
            rw [cart_singleton_map_diff]
      _ = max0 (xs.map (fun b : Int => b - a)) := by
        apply max0_perm
        simp
  rw [hcartmax]

/-! ### Goals -/

theorem sound_CrossExtreme :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.CrossExtreme.Goal := by
  intro x y hx hy
  unfold Drawdown.measure
  have hp := (pairs_append_perm x y).map (fun p : Int × Int => p.1 - p.2)
  have hm := max0_perm hp
  rw [hm]
  simp only [List.map_append]
  rw [max0_append, max0_append, max0_cart_diff x y hx hy]
  have hnonneg : 0 ≤ max0 ((pairs x).map (fun p : Int × Int => p.1 - p.2)) := max0_nonneg _
  omega

theorem sound_Bidirectional :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.Bidirectional.Goal := by
  intro x
  induction x with
  | nil =>
      simp [Drawdown.measure, spread, max0, pairs]
  | cons a xs ih =>
      by_cases hxs : xs = []
      · subst xs
        simp [Drawdown.measure, spread, max0, pairs]
      · rw [List.reverse_cons, measure_cons, measure_reverse_append_singleton]
        have hMmem : xs.max?.getD 0 ∈ xs := max_mem_of_ne_nil hxs
        have hmmem : xs.min?.getD 0 ∈ xs := min_mem_of_ne_nil hxs
        have hMle : ∀ b ∈ xs, b ≤ xs.max?.getD 0 := by
          intro b hb
          exact le_max_of_mem hb
        have hmle : ∀ b ∈ xs, xs.min?.getD 0 ≤ b := by
          intro b hb
          exact min_le_of_mem hb
        have hspread : 0 ≤ xs.max?.getD 0 - xs.min?.getD 0 := by
          have h := hMle (xs.min?.getD 0) hmmem
          omega
        have hAge : a - xs.min?.getD 0 ≤ max0 (xs.map (fun b : Int => a - b)) :=
          le_max0_of_mem (List.mem_map.mpr ⟨xs.min?.getD 0, hmmem, rfl⟩)
        have hBge : xs.max?.getD 0 - a ≤ max0 (xs.map (fun b : Int => b - a)) :=
          le_max0_of_mem (List.mem_map.mpr ⟨xs.max?.getD 0, hMmem, rfl⟩)
        have hih : max (Drawdown.measure xs) (Drawdown.measure xs.reverse) =
            xs.max?.getD 0 - xs.min?.getD 0 := by
          simpa [spread] using ih
        have hD_le_m : Drawdown.measure xs ≤ xs.max?.getD 0 - xs.min?.getD 0 := by
          rw [← hih]
          exact Int.le_max_left _ _
        have hR_le_m : Drawdown.measure xs.reverse ≤ xs.max?.getD 0 - xs.min?.getD 0 := by
          rw [← hih]
          exact Int.le_max_right _ _
        have hspread_cons : spread (a :: xs) =
            max a (xs.max?.getD 0) - min a (xs.min?.getD 0) := by
          cases hm : xs.max? with
          | none => exact False.elim (hxs (List.max?_eq_none_iff.mp hm))
          | some M =>
              cases hn : xs.min? with
              | none => exact False.elim (hxs (List.min?_eq_none_iff.mp hn))
              | some m =>
                  simp [spread, List.max?_cons, List.min?_cons, hm, hn]
        rw [hspread_cons]
        by_cases hle : a ≤ xs.min?.getD 0
        · have hMge_a : 0 ≤ xs.max?.getD 0 - a := by
            have h := hMle (xs.min?.getD 0) hmmem
            omega
          have hA_le_a : max0 (xs.map (fun b : Int => a - b)) ≤ xs.max?.getD 0 - a := by
            apply max0_le hMge_a
            intro v hv
            rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
            have hbm := hmle b hb
            omega
          have hB_le_a : max0 (xs.map (fun b : Int => b - a)) ≤ xs.max?.getD 0 - a := by
            apply max0_le hMge_a
            intro v hv
            rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
            have hbM := hMle b hb
            omega
          have hB_eq : max0 (xs.map (fun b : Int => b - a)) = xs.max?.getD 0 - a :=
            Int.le_antisymm hB_le_a hBge
          have hD_le_a : Drawdown.measure xs ≤ xs.max?.getD 0 - a := by omega
          have hR_le_a : Drawdown.measure xs.reverse ≤ xs.max?.getD 0 - a := by omega
          omega
        · by_cases hge : xs.max?.getD 0 ≤ a
          · have hamge : 0 ≤ a - xs.min?.getD 0 := by
              have h := hMle (xs.min?.getD 0) hmmem
              omega
            have hA_le_a : max0 (xs.map (fun b : Int => a - b)) ≤ a - xs.min?.getD 0 := by
              apply max0_le hamge
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbm := hmle b hb
              omega
            have hB_le_a : max0 (xs.map (fun b : Int => b - a)) ≤ a - xs.min?.getD 0 := by
              apply max0_le hamge
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbM := hMle b hb
              omega
            have hA_eq : max0 (xs.map (fun b : Int => a - b)) = a - xs.min?.getD 0 :=
              Int.le_antisymm hA_le_a hAge
            have hD_le_a : Drawdown.measure xs ≤ a - xs.min?.getD 0 := by omega
            have hR_le_a : Drawdown.measure xs.reverse ≤ a - xs.min?.getD 0 := by omega
            omega
          · have hRHS : max a (xs.max?.getD 0) - min a (xs.min?.getD 0) =
                xs.max?.getD 0 - xs.min?.getD 0 := by omega
            rw [hRHS]
            have hA_le_m : max0 (xs.map (fun b : Int => a - b)) ≤
                xs.max?.getD 0 - xs.min?.getD 0 := by
              apply max0_le hspread
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbm := hmle b hb
              omega
            have hB_le_m : max0 (xs.map (fun b : Int => b - a)) ≤
                xs.max?.getD 0 - xs.min?.getD 0 := by
              apply max0_le hspread
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbM := hMle b hb
              omega
            have hleft_le : max (max (max0 (xs.map (fun b : Int => a - b))) (Drawdown.measure xs))
                (max (Drawdown.measure xs.reverse) (max0 (xs.map (fun b : Int => b - a)))) ≤
                xs.max?.getD 0 - xs.min?.getD 0 := by omega
            have hleft_ge : xs.max?.getD 0 - xs.min?.getD 0 ≤
                max (max (max0 (xs.map (fun b : Int => a - b))) (Drawdown.measure xs))
                (max (Drawdown.measure xs.reverse) (max0 (xs.map (fun b : Int => b - a)))) := by omega
            exact Int.le_antisymm hleft_le hleft_ge

theorem sound_QuoteFlip :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.QuoteFlip.Goal := by
  intro x
  unfold Drawdown.measure scaled
  rw [pairs_map]
  have hneg : ((pairs x).map (fun p : Int × Int => ((-1 : Int) * p.1, (-1 : Int) * p.2))).map
        (fun p : Int × Int => p.1 - p.2) = (pairs x).map (fun p : Int × Int => p.2 - p.1) := by
    rw [List.map_map]
    apply List.map_congr_left
    intro p hp
    rcases p with ⟨u, v⟩
    simp
    omega
  rw [hneg]
  have hp := (pairs_reverse_perm x).map (fun p : Int × Int => p.1 - p.2)
  have hm := max0_perm hp
  rw [hm]
  rw [show ((pairs x).map (fun p : Int × Int => (p.2, p.1))).map
        (fun p : Int × Int => p.1 - p.2) =
      (pairs x).map (fun p : Int × Int => p.2 - p.1) by
        rw [List.map_map]
        apply List.map_congr_left
        intro p hp
        rcases p with ⟨u, v⟩
        simp]

private theorem pairs_filter_sublist (x : Series) (keep : Int → Bool) :
    (pairs (x.filter keep)).Sublist (pairs x) := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      by_cases h : keep a
      · simp [h, pairs_cons]
        exact List.Sublist.append
          ((List.Sublist.map (fun b : Int => (a, b)) (List.filter_sublist : (xs.filter keep).Sublist xs))) ih
      · simp [h]
        refine ih.trans ?_
        rw [pairs_cons]
        simp

theorem sound_DeleteMonotone :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.DeleteMonotone.Goal := by
  intro x keep
  unfold Drawdown.measure
  exact max0_map_sublist (fun p : Int × Int => p.1 - p.2) (pairs_filter_sublist x keep)

private theorem measure_self_concat (x : Series) : Drawdown.measure (x ++ x) = spread x := by
  by_cases hx : x = []
  · subst x
    simp [Drawdown.measure, spread, max0, pairs]
  · unfold Drawdown.measure
    have hp := (pairs_append_perm x x).map (fun p : Int × Int => p.1 - p.2)
    have hm := max0_perm hp
    rw [hm]
    simp only [List.map_append]
    rw [max0_append, max0_append, max0_cart_diff x x hx hx]
    unfold spread
    have hle : max0 ((pairs x).map (fun p : Int × Int => p.1 - p.2)) ≤
        x.max?.getD 0 - x.min?.getD 0 := by
      simpa [Drawdown.measure, spread] using measure_le_spread x
    have hs : 0 ≤ x.max?.getD 0 - x.min?.getD 0 := by
      have hm := min_mem_of_ne_nil hx
      have hle' := le_max_of_mem hm
      omega
    omega

theorem sound_DuplicatePeriods :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.DuplicatePeriods.Goal := by
  intro x n hn
  rcases Nat.exists_eq_add_of_le hn with ⟨k, hk⟩
  subst n
  have hrep : List.replicate (2 + k) x = x :: x :: List.replicate k x := by
    rw [show 2 + k = Nat.succ (Nat.succ k) by omega]
    simp only [List.replicate_succ]
  rw [hrep]
  simp only [List.flatten_cons]
  let rest : Series := (List.replicate k x).flatten
  change Drawdown.measure (x ++ (x ++ rest)) = spread x
  have hlow : spread x ≤ Drawdown.measure (x ++ (x ++ rest)) := by
    rw [← List.append_assoc]
    rw [← measure_self_concat x]
    exact measure_append_ge_left (x ++ x) rest
  have hall : ∀ a ∈ x ++ (x ++ rest), a ∈ x := by
    intro a ha
    simp only [List.mem_append] at ha
    rcases ha with ha | ha | ha
    · exact ha
    · exact ha
    · exact mem_flatten_replicate ha
  have hup : Drawdown.measure (x ++ (x ++ rest)) ≤ spread x :=
    measure_le_of_mem (x ++ (x ++ rest)) x hall
  exact Int.le_antisymm hup hlow

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.Drawdown
