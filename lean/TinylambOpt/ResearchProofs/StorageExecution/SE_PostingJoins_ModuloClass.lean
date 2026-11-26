import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0

namespace Helpers_ModuloClass
open List Nat
open TinylambOpt.ResearchGoals.Compression.Wire
  TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
-- ===== P1.lean =====
theorem length_filter_countP {α : Type} (l : List α) (p : α → Bool) :
    (l.filter p).length = l.countP p :=
  List.countP_eq_length_filter.symm
theorem width_two_pow_gt (n : Nat) : n < 2 ^ width n := by
  unfold width
  by_cases h : n = 0
  · subst h
    simp
  · have hc : (if n == 0 then 0 else n.log2 + 1) = n.log2 + 1 := by simp [h]
    rw [hc]
    exact (Nat.log2_lt h).mp (Nat.lt_succ_self _)
theorem get!_zero {α : Type} [Inhabited α] (a : α) (l : List α) : (a :: l)[0]! = a := rfl
theorem get!_succ {α : Type} [Inhabited α] (a : α) (l : List α) (n : Nat) :
    (a :: l)[n+1]! = l[n]! := rfl
/-- `l[i]!` agrees with the evidenced `l[i]` when `i` is in range. -/
theorem get!_eq_of_getElem {α : Type} [Inhabited α] (l : List α) (i : Nat)
    (hi : i < l.length) {a : α} (he : l[i] = a) : l[i]! = a := by
  match l, i, hi, he with
  | .cons x t, .zero, _, he => simpa using he
  | .cons x t, .succ j, hi, he => exact get!_eq_of_getElem t j (by simpa using hi) he
theorem getD_list (ys : List Nat) (i : Nat) (hi : i < ys.length) :
    (ys[i]?.getD 0) = ys[i]! := by
  match ys, i, hi with
  | .cons a t, .zero, _ => rfl
  | .cons a t, .succ j, hi => exact getD_list t j (by simpa using hi)
theorem getD_toArray (ys : List Nat) (i : Nat) (hi : i < ys.length) :
    (ys.toArray[i]?.getD 0) = ys[i]! := by
  rw [List.getElem?_toArray]
  exact getD_list ys i hi
theorem lt_bool {a v : Nat} : (decide (a < v)) = true ↔ a < v := by simp
theorem not_lt_bool {a v : Nat} : (decide (a < v)) = false ↔ ¬(a < v) := by simp
theorem get?_cons_zero (a : α) (l : List α) : (a :: l)[0]? = some a := rfl
theorem get?_cons_succ (a : α) (l : List α) (n : Nat) : (a :: l)[n+1]? = l[n]? := rfl
theorem get?_ge {α : Type} [Inhabited α] (xs : List α) (i : Nat)
    (h : xs.length ≤ i) : xs[i]? = none := by
  match xs, i with
  | .nil, _ => rfl
  | .cons a t, .zero => exact absurd h (by simp)
  | .cons a t, .succ k => exact get?_ge t k (by simpa using h)
theorem mem_of_get! {α : Type} [Inhabited α] (xs : List α) (i : Nat) (h : i < xs.length) :
    xs[i]! ∈ xs := by
  match xs, h with
  | .cons a t, h =>
      match i with
      | .zero => exact List.mem_cons_self
      | .succ k => exact List.mem_cons_of_mem _ (mem_of_get! t k (by simpa using h))
theorem exists_get!_eq_mem {α : Type} [Inhabited α] {a : α} {l : List α} (h : a ∈ l) :
    ∃ i, i < l.length ∧ l[i]! = a := by
  obtain ⟨i, hi, he⟩ := List.mem_iff_getElem.mp h
  exact ⟨i, hi, get!_eq_of_getElem l i hi he⟩
theorem pw_mono (l : List Nat) : Pairwise (fun x y => x ≤ y) l →
    ∀ k m, k ≤ m → m < l.length → l[k]! ≤ l[m]! := by
  induction l with
  | nil => simp
  | cons a t ih =>
      intro h k m hk hm
      rcases k with _|i
      · rcases m with _|j
        · exact Nat.le_refl _
        · exact rel_of_pairwise_cons h (mem_of_get! t j (by simpa using hm))
      · rcases m with _|j
        · omega
        · have hle : i ≤ j := by simpa using hk
          have hlt : j < t.length := by simpa using hm
          simpa using ih (Pairwise.of_cons h) i j hle hlt
theorem pw_head_le {α : Type} [LE α] (a : α) (l : List α) :
    Pairwise (fun x y => x ≤ y) (a :: l) → ∀ x ∈ l, a ≤ x := by
  intro h x hx
  exact rel_of_pairwise_cons h hx
theorem pw_of_take {α : Type} [Inhabited α] {r : α → α → Prop} (l : List α) (k : Nat) :
    Pairwise r l → Pairwise r (l.take k) := fun h => Pairwise.take h
theorem pw_of_filter {α : Type} [Inhabited α] {r : α → α → Prop} (l : List α) (p : α → Bool) :
    Pairwise r l → Pairwise r (l.filter p) := fun h => Pairwise.filter p h
theorem pw_of_drop {α : Type} [Inhabited α] {r : α → α → Prop} (l : List α) (k : Nat) :
    Pairwise r l → Pairwise r (l.drop k) := fun h => Pairwise.drop h
theorem pw_tail {α : Type} [Inhabited α] {r : α → α → Prop} {a : α} {l : List α} :
    Pairwise r (a :: l) → Pairwise r l := fun h => Pairwise.of_cons h
theorem countP_eq_len_of_forall {α : Type} [Inhabited α] (l : List α) (p : α → Bool) :
    (∀ x ∈ l, p x = true) → l.countP p = l.length := by
  induction l with
  | nil => simp
  | cons a t ih =>
      intro h
      rw [countP_cons, ih (fun x hx => h x (List.mem_cons_of_mem _ hx))]
      simp [h a List.mem_cons_self]
theorem countP_eq_zero_of {α : Type} [Inhabited α] (l : List α) (p : α → Bool) :
    (∀ x ∈ l, p x = false) → l.countP p = 0 := by
  induction l with
  | nil => simp
  | cons a t ih =>
      intro h
      rw [countP_cons, ih (fun x hx => h x (List.mem_cons_of_mem _ hx))]
      simp [h a List.mem_cons_self]
theorem countP_le_length {α : Type} [Inhabited α] (l : List α) (p : α → Bool) :
    l.countP p ≤ l.length := by
  induction l with
  | nil => simp
  | cons a t ih =>
      rw [countP_cons]
      by_cases h : p a <;> simp [h] <;> omega
theorem countP_le_countP {α : Type} [Inhabited α] (l : List α) (p q : α → Bool) :
    (∀ x ∈ l, p x → q x = true) → l.countP p ≤ l.countP q := by
  induction l with
  | nil => simp
  | cons a t ih =>
      intro h
      rw [countP_cons, countP_cons]
      have := ih (fun x hx hp => h x (List.mem_cons_of_mem _ hx) hp)
      have hqa : p a → q a = true := h a List.mem_cons_self
      by_cases ha : p a <;> simp_all <;> omega
theorem countP_congr {α : Type} [Inhabited α] (l : List α) (p q : α → Bool) :
    (∀ x ∈ l, p x = q x) → l.countP p = l.countP q := by
  induction l with
  | nil => simp
  | cons a t ih =>
      intro h
      rw [countP_cons, countP_cons, ih (fun x hx => h x (List.mem_cons_of_mem _ hx))]
      rw [h a List.mem_cons_self]
theorem countP_eq_zero_iff {α : Type} [Inhabited α] (l : List α) (p : α → Bool) :
    l.countP p = 0 ↔ ∀ x ∈ l, p x = false := by
  induction l with
  | nil => simp
  | cons a t ih =>
      constructor
      · intro h x hx
        obtain rfl | m := List.mem_cons.mp hx
        · by_cases hp : p x
          · rw [countP_cons] at h
            simp_all
          · exact by simpa using hp
        · rw [countP_cons] at h
          by_cases hp : p a
          · simp [hp] at h
          · rw [if_neg (by simpa using hp), Nat.add_zero] at h
            exact ih.mp h x m
      · intro h
        rw [countP_cons]
        rw [ih.mpr (fun x hx => h x (List.mem_cons_of_mem _ hx))]
        have ha : p a = false := h a List.mem_cons_self
        rw [if_neg (by simpa using ha)]
theorem countP_split {α : Type} [Inhabited α] (l : List α) (p : α → Bool) (k : Nat)
    (h1 : ∀ i, i < k → i < l.length ∧ p l[i]! = true)
    (h2 : ∀ i, k ≤ i → i < l.length → p l[i]! = false) :
    l.countP p = k := by
  induction l generalizing k with
  | nil =>
      match k with
      | .zero => simp
      | .succ m => exact absurd (h1 0 (Nat.succ_pos m)).1 (by simp)
  | cons a t ih =>
      match k with
      | .zero =>
          have ha : p a = false := h2 0 (by simp) (by simp)
          rw [countP_cons, if_neg (by simpa using ha)]
          exact countP_eq_zero_of t p (fun x hx => by
            obtain ⟨j, hj, he⟩ := exists_get!_eq_mem hx
            rw [← he]
            have hmem : j + 1 < (a :: t).length := by simpa using hj
            simpa [get!_succ] using h2 (j+1) (Nat.zero_le _) hmem)
      | .succ m =>
          have ha : p a = true := by
            have := h1 0 (Nat.succ_pos m)
            simpa using this.2
          have : t.countP p = m := by
            refine ih m ?_ ?_
            · intro i hi
              have := h1 (i+1) (by omega)
              exact ⟨by simpa using this.1, by simpa using this.2⟩
            · intro i hi1 hi2
              exact h2 (i+1) (by omega) (by simpa using hi2)
          rw [countP_cons, if_pos (by simpa using ha), this]
/-- Sorted lists: the `k = countP (· < v)` elements below `v` occupy exactly the
first `k` positions. -/
theorem countP_lt_split (ys : List Nat) (v : Nat) :
    Pairwise (fun a b => a ≤ b) ys →
    ∀ k, ys.countP (fun y => y < v) = k →
      (∀ i, i < k → i < ys.length ∧ ys[i]! < v) ∧
      (∀ i, k ≤ i → i < ys.length → ¬(ys[i]! < v)) := by
  induction ys with
  | nil =>
      intro _ k hc
      simp at hc
      subst k
      exact ⟨fun i hi => absurd hi (Nat.not_lt_zero i), fun i _ hi _ => absurd hi (by simp)⟩
  | cons a t ih =>
      intro hs k hc
      have hc0 := hc
      have hst : Pairwise (fun a b => a ≤ b) t := Pairwise.of_cons hs
      by_cases hlt : a < v
      · have hd : (fun y => decide (y < v)) a = true := by simpa using hlt
        have hct : t.countP (fun y => y < v) = k - 1 := by
          rw [countP_cons] at hc
          rw [if_pos hd] at hc
          omega
        obtain ⟨t1, t2⟩ := ih hst (k - 1) hct
        constructor
        · intro i hi
          match i with
          | .zero => exact ⟨by simp, hlt⟩
          | .succ j =>
              obtain ⟨b1, b2⟩ := t1 j (by omega)
              exact ⟨by simpa using b1, by simpa using b2⟩
        · intro i hi1 hi2
          cases i with
          | zero =>
              rw [countP_cons, if_pos hd] at hc0
              exact fun _ => by omega
          | succ j =>
              exact t2 j (by omega) (by simpa using hi2)
      · have hd : (fun y => decide (y < v)) a = false := by simpa using hlt
        have hzero : t.countP (fun y => y < v) = 0 :=
          countP_eq_zero_of t (fun y => y < v) (by
            intro y hy
            obtain ⟨j, hj, rfl⟩ := exists_get!_eq_mem hy
            have hle : a ≤ t[j]! :=
              rel_of_pairwise_cons hs (mem_of_get! t j hj)
            rw [not_lt_bool]
            exact fun hh => hlt (Nat.lt_of_le_of_lt hle hh))
        have hk : k = 0 := by
          rw [countP_cons, if_neg (by simpa using hd), hzero] at hc
          omega
        subst hk
        exact ⟨fun i hi => absurd hi (Nat.not_lt_zero i),
          fun i _ hi2 => by
            match i, hi2 with
            | .zero, _ => exact by simpa using hlt
            | .succ j, hlt' =>
                exact by
                  simpa using countP_eq_zero_iff t (fun y => y < v) |>.mp hzero t[j]!
                    (mem_of_get! t j (by simpa using hlt'))⟩
theorem searchBound_spec (xs : List Nat) (v : Nat)
    (hsort : Pairwise (fun a b => a ≤ b) xs) :
    ∀ fuel lo hi, lo ≤ hi → hi ≤ xs.length → hi - lo < 2 ^ fuel →
      (∀ i, i < lo → i < xs.length → xs[i]! < v) →
      (∀ i, hi ≤ i → i < xs.length → ¬(xs[i]! < v)) →
      searchBound xs.toArray v fuel lo hi = xs.countP (fun y => y < v) := by
  intro fuel
  induction fuel with
  | zero =>
      intro lo hi hlo hhi hlt h1 h2
      have hstep : searchBound xs.toArray v 0 lo hi = lo := rfl
      rw [hstep]
      exact (countP_split xs (fun y => y < v) lo
        (fun i hi' => ⟨by omega, by simpa using h1 i hi' (by omega)⟩)
        (fun i hi1 hi2 => by simpa using h2 i (by omega) hi2)).symm
  | succ k ih =>
      intro lo hi hlo hhi hlt h1 h2
      show searchBound xs.toArray v (k+1) lo hi = xs.countP (fun y => y < v)
      by_cases hbridge : lo ≥ hi
      · have hstep : searchBound xs.toArray v (k+1) lo hi = lo := by
          show (if lo ≥ hi then lo else _) = lo
          rw [if_pos hbridge]
        rw [hstep]
        exact (countP_split xs (fun y => y < v) lo
          (fun i hi' => ⟨by omega, by simpa using h1 i hi' (by omega)⟩)
          (fun i hi1 hi2 => by simpa using h2 i (by omega) hi2)).symm
      · have hlt' : lo < hi := by omega
        have hstep : searchBound xs.toArray v (k+1) lo hi =
            if xs.toArray[(lo + hi) / 2]?.getD 0 < v then
              searchBound xs.toArray v k ((lo + hi) / 2 + 1) hi
            else searchBound xs.toArray v k lo ((lo + hi) / 2) := by
          show (if lo ≥ hi then lo else _) = _
          rw [if_neg hbridge]
        rw [hstep]
        let mid := (lo + hi) / 2
        have hmid1 : lo ≤ mid := by omega
        have hmid2 : mid < hi := by omega
        have hp : 2 ^ (k + 1) = 2 * 2 ^ k := by
          rw [Nat.pow_succ]
          omega
        have hfuel1 : hi - (mid + 1) < 2 ^ k := by omega
        have hfuel2 : mid - lo < 2 ^ k := by omega
        have hmid_lt : mid < xs.length := by omega
        have hconv : (xs.toArray[mid]?.getD 0) = xs[mid]! := getD_toArray xs mid hmid_lt
        by_cases hcmp : xs[mid]! < v
        · have h1' : ∀ i, i < mid + 1 → i < xs.length → xs[i]! < v := by
            intro i hi' hlt'
            by_cases hlo' : i < lo
            · exact h1 i hlo' hlt'
            · have hle : xs[i]! ≤ xs[mid]! :=
                pw_mono xs hsort i mid (by omega) hmid_lt
              exact Nat.lt_of_le_of_lt hle hcmp
          rw [if_pos (by simpa [hconv] using hcmp)]
          exact ih (mid + 1) hi (by omega) hhi hfuel1 h1' h2
        · have hlt'' : v ≤ xs[mid]! := Nat.le_of_not_lt hcmp
          have h2' : ∀ i, mid ≤ i → i < xs.length → ¬(xs[i]! < v) := by
            intro i hi' hlt' hh
            have hle : xs[mid]! ≤ xs[i]! :=
              pw_mono xs hsort mid i (by omega) hlt'
            exact Nat.lt_irrefl _ (Nat.lt_of_lt_of_le hh (Nat.le_trans hlt'' hle))
          rw [if_neg (by simpa [hconv] using hcmp)]
          exact ih lo mid hmid1 (by omega) hfuel2 h1 h2'
theorem lower_spec (ys : List Nat) (v : Nat)
    (hsort : Pairwise (fun a b => a ≤ b) ys) :
    lower ys v = ys.countP (fun y => y < v) :=
  searchBound_spec ys v hsort (width ys.length + 1) 0 ys.length
    (by omega) (by omega)
    (by
      have h := width_two_pow_gt ys.length
      exact Nat.lt_of_lt_of_le h
        (Nat.pow_le_pow_of_le (by omega) (by omega)))
    (by simp)
    (by intro i h1 h2; omega)
private theorem searchBound_le_hi (xs : Array Nat) (v : Nat) :
    ∀ fuel lo hi, lo ≤ hi → searchBound xs v fuel lo hi ≤ hi := by
  intro fuel
  induction fuel with
  | zero => intro lo hi hle; exact hle
  | succ fuel ih =>
      intro lo hi hle
      by_cases hge : lo ≥ hi
      · show (if lo ≥ hi then lo else _) ≤ hi
        rw [if_pos hge]
        omega
      · show (if lo ≥ hi then lo else _) ≤ hi
        rw [if_neg hge]
        dsimp only
        split
        · refine ih (((lo + hi) / 2) + 1) hi ?_
          omega
        · refine Nat.le_trans (ih lo ((lo + hi) / 2) ?_) ?_
          · omega
          · exact (Nat.div_le_iff_le_mul_add_pred (by omega)).mpr (by omega)
theorem lower_le_length (ys : List Nat) (v : Nat) : lower ys v ≤ ys.length :=
  searchBound_le_hi ys.toArray v (width ys.length + 1) 0 ys.length (by omega)
theorem lower_mono (ys : List Nat) (a b : Nat)
    (hsort : Pairwise (fun x y => x ≤ y) ys) (hab : a ≤ b) :
    lower ys a ≤ lower ys b := by
  rw [lower_spec ys a hsort, lower_spec ys b hsort]
  exact countP_le_countP ys (fun y => y < a) (fun y => y < b)
    (fun y _ hy => by
      simpa using Nat.lt_of_lt_of_le (by simpa using hy) hab)
theorem filter_prefix {α : Type} [Inhabited α] (l : List α) (p : α → Bool) (k : Nat)
    (h1 : ∀ i, i < k → i < l.length ∧ p l[i]! = true)
    (h2 : ∀ i, k ≤ i → i < l.length → p l[i]! = false) :
    l.filter p = l.take k := by
  match l with
  | .nil => simp
  | .cons a t =>
      cases k with
      | zero =>
          have ha : p a = false := h2 0 (Nat.zero_le _) (by simp)
          rw [filter_cons_of_neg (by simpa using ha)]
          exact filter_prefix t p 0
            (fun i hi => absurd hi (Nat.not_lt_zero i))
            (by intro i _ hi2; exact h2 (i+1) (Nat.zero_le _) (by simpa using hi2))
      | succ m =>
          have ha : p a = true := by
            have := h1 0 (Nat.succ_pos m)
            simpa using this.2
          rw [filter_cons_of_pos (by simpa using ha)]
          show a :: t.filter p = a :: t.take m
          congr 1
          exact filter_prefix t p m
            (by intro i hi; exact ⟨by simpa using (h1 (i+1) (by simpa using hi)).1,
                  by simpa using (h1 (i+1) (by simpa using hi)).2⟩)
            (by intro i hi1 hi2; exact h2 (i+1) (by omega) (by simpa using hi2))
theorem filter_suffix_not {α : Type} [Inhabited α] (l : List α) (p : α → Bool) (k : Nat)
    (h1 : ∀ i, i < k → i < l.length ∧ p l[i]! = true)
    (h2 : ∀ i, k ≤ i → i < l.length → p l[i]! = false) :
    l.filter (fun x => !p x) = l.drop k := by
  match l with
  | .nil => simp
  | .cons a t =>
      cases k with
      | zero =>
          have ha : p a = false := h2 0 (Nat.zero_le _) (by simp)
          rw [filter_cons_of_pos (by simpa using ha)]
          show a :: filter (fun x => !p x) t = a :: drop 0 t
          refine congrArg (fun l => a :: l) ?_
          exact filter_suffix_not t p 0
            (fun i hi => absurd hi (Nat.not_lt_zero i))
            (by intro i _ hi2; exact h2 (i+1) (Nat.zero_le _) (by simpa using hi2))
      | succ m =>
          have ha : p a = true := by
            have := h1 0 (Nat.succ_pos m)
            simpa using this.2
          rw [filter_cons_of_neg (by simpa using ha)]
          exact filter_suffix_not t p m
            (by intro i hi; exact ⟨by simpa using (h1 (i+1) (by simpa using hi)).1,
                  by simpa using (h1 (i+1) (by simpa using hi)).2⟩)
            (by intro i hi1 hi2; exact h2 (i+1) (by omega) (by simpa using hi2))
theorem take_lower (ys : List Nat) (v : Nat)
    (hsort : Pairwise (fun a b => a ≤ b) ys) :
    ys.take (lower ys v) = ys.filter (fun y => y < v) := by
  have hs := countP_lt_split ys v hsort (lower ys v) (lower_spec ys v hsort).symm
  refine (filter_prefix ys (fun y => y < v) (lower ys v) ?_ ?_).symm
  · intro i hi
    exact ⟨(hs.1 i hi).1, by simpa using (hs.1 i hi).2⟩
  · intro i h1 h2
    exact by simpa using hs.2 i h1 h2
theorem drop_lower (ys : List Nat) (v : Nat)
    (hsort : Pairwise (fun a b => a ≤ b) ys) :
    ys.drop (lower ys v) = ys.filter (fun y => ¬(y < v)) := by
  have hs := countP_lt_split ys v hsort (lower ys v) (lower_spec ys v hsort).symm
  have key := filter_suffix_not ys (fun y => y < v) (lower ys v)
    (by intro i hi; exact ⟨(hs.1 i hi).1, by simpa using (hs.1 i hi).2⟩)
    (by intro i h1 h2; exact by simpa using hs.2 i h1 h2)
  exact key.symm
theorem get!_filter_agree {α : Type} [Inhabited α] (p : α → Bool) (ys : List α) (i : Nat)
    (hi : i < ys.length)
    (hall : ∀ m, m < i → p ys[m]! = true)
    (hp : p ys[i]! = true) : (ys.filter p)[i]! = ys[i]! := by
  match ys, i with
  | .nil, _ => exact absurd hi (by simp)
  | .cons a t, .zero =>
      have ha : p a = true := by simpa using hp
      rw [filter_cons_of_pos (by simpa using ha)]
      rfl
  | .cons a t, .succ j =>
      have h : p t[j]! = true := by simpa using hp
      have ha : p a = true := by
        have := hall 0 (by omega)
        simpa using this
      rw [filter_cons_of_pos (by simpa using ha)]
      rw [get!_succ, get!_succ]
      exact get!_filter_agree p t j (by simpa using hi)
        (by intro m hm; have := hall (m+1) (by omega); simpa using this) h
/-- The posting slice equals direct filtering. -/
theorem slice_eq_filter (ys : List Nat) (a b : Nat)
    (hsort : Pairwise (fun x y => x ≤ y) ys) :
    slice ys a b = ys.filter (fun y => a ≤ y && y < b) := by
  rcases Nat.le_total a b with hcase | hcase
  · have hla : lower ys a ≤ lower ys b := lower_mono ys a b hsort hcase
    have hsb := countP_lt_split ys b hsort (lower ys b) (lower_spec ys b hsort).symm
    have hsa := countP_lt_split ys a hsort (lower ys a) (lower_spec ys a hsort).symm
    have htb : ys.take (lower ys b) = ys.filter (fun y => y < b) := take_lower ys b hsort
    have hlen : (ys.filter (fun y => y < b)).length = lower ys b := by
      rw [← htb, List.length_take, Nat.min_eq_left (lower_le_length ys b)]
    have h1 : ∀ i, i < lower ys a →
        i < (ys.filter (fun y => y < b)).length ∧ (ys.filter (fun y => y < b))[i]! < a := by
      intro i hi
      obtain ⟨l1, hlt⟩ := hsa.1 i hi
      have hltb : ys[i]! < b := Nat.lt_of_lt_of_le hlt hcase
      exact ⟨by omega, by
        have hag := get!_filter_agree (fun y => y < b) ys i l1
          (by
            intro m hm
            obtain ⟨_, hlt'⟩ := hsa.1 m (by omega)
            simpa using Nat.lt_of_lt_of_le hlt' hcase)
          (by simpa using hltb)
        rw [hag]
        exact hlt⟩
    have h2 : ∀ i, lower ys a ≤ i → i < (ys.filter (fun y => y < b)).length →
        ¬((ys.filter (fun y => y < b))[i]! < a) := by
      intro i hi1 hi2
      have hi3 : i < ys.length := by
        have : (ys.filter (fun y => y < b)).length ≤ ys.length := by
          rw [length_filter_countP]; exact countP_le_length _ _
        omega
      have nlt := hsa.2 i hi1 hi3
      have hagree : (ys.filter (fun y => y < b))[i]! = ys[i]! :=
        get!_filter_agree (fun y => y < b) ys i hi3
          (by
            intro m hm
            have hmi : m < lower ys b := by omega
            obtain ⟨_, hlt2⟩ := hsb.1 m hmi
            simpa using hlt2)
          (by
            have hii : i < lower ys b := by omega
            obtain ⟨_, hlt3⟩ := hsb.1 i hii
            simpa using hlt3)
      exact fun hh => nlt (by rw [← hagree]; exact hh)
    have hdrop : (ys.take (lower ys b)).drop (lower ys a)
        = ys.filter (fun y => a ≤ y && y < b) := by
      have key := filter_suffix_not (ys.filter (fun y => y < b)) (fun y => y < a)
        (lower ys a)
        (by
          intro i hi
          obtain ⟨l1, hlt⟩ := h1 i hi
          exact ⟨l1, by simpa using hlt⟩)
        (by
          intro i hi1 hi2
          exact by simpa using h2 i hi1 hi2)
      have key' : (ys.take (lower ys b)).filter (fun y => !(y < a))
          = (ys.take (lower ys b)).drop (lower ys a) := by
        rw [htb]
        exact key
      rw [← key', htb, List.filter_filter]
      congr 1
      funext y
      by_cases hya : y < a
      · simp [hya, Nat.not_lt]
        omega
      · simp [hya, Nat.not_lt]
        omega
    have hstep : slice ys a b = (ys.take (lower ys b)).drop (lower ys a) := by
      unfold slice
      rw [List.take_drop, Nat.add_sub_of_le hla]
    rw [hstep, hdrop]
  · have hnil : ys.filter (fun y => a ≤ y && y < b) = [] := by
      refine List.eq_nil_of_length_eq_zero ?_
      rw [length_filter_countP]
      refine countP_eq_zero_of ys (fun y => a ≤ y && y < b) ?_
      intro y _
      by_cases h1 : a ≤ y
      · by_cases h2 : y < b
        · exfalso
          omega
        · simp [h2]
      · simp [h1]
    have hle : lower ys b ≤ lower ys a := lower_mono ys b a hsort hcase
    have hz : lower ys b - lower ys a = 0 := by omega
    unfold slice
    rw [hnil, hz, List.take_zero]
theorem slice_length (ys : List Nat) (a b : Nat)
    (hsort : Pairwise (fun x y => x ≤ y) ys) :
    (slice ys a b).length = lower ys b - lower ys a := by
  rcases Nat.le_total a b with hcase | hcase
  · have hla : lower ys a ≤ lower ys b := lower_mono ys a b hsort hcase
    have hlb : lower ys b ≤ ys.length := lower_le_length ys b
    unfold slice
    rw [List.length_take, List.length_drop]
    rw [Nat.min_eq_left (Nat.sub_le_sub_right hlb (lower ys a))]
  · rw [slice_eq_filter ys a b hsort, length_filter_countP]
    have hb := countP_eq_zero_of ys (fun y => a ≤ y && y < b) (by
      intro y _
      by_cases h1 : a ≤ y
      · by_cases h2 : y < b
        · exfalso
          omega
        · simp [h2]
      · simp [h1])
    have hla : lower ys b ≤ lower ys a := lower_mono ys b a hsort hcase
    rw [hb]
    omega
-- ===== P2a.lean =====
private theorem any_eq_false_of {α : Type} (rs : List α) (f : α → Bool)
    (h : ∀ p ∈ rs, f p = false) : rs.any f = false := by
  induction rs with
  | nil => rfl
  | cons p t ih =>
      rw [List.any_cons, ih (fun q hq => h q (List.mem_cons_of_mem p hq)),
        h p (by simp)]
      simp
theorem exists_of_member {rs : List (Nat × Nat)} {y : Nat} (h : member rs y = true) :
    ∃ p ∈ rs, p.1 ≤ y ∧ y < p.2 := by
  rw [member, List.any_eq_true] at h
  obtain ⟨p, hp, hpy⟩ := h
  exact ⟨p, hp, by simpa using hpy⟩
theorem member_cons (a b : Nat) (rs : List (Nat × Nat)) (y : Nat) :
    member ((a, b) :: rs) y = ((a ≤ y && y < b) || member rs y) := by
  simp [member, List.any_cons]
private theorem filter_eq_nil_of {α : Type} [Inhabited α] (ys : List α) (p : α → Bool)
    (h : ∀ y ∈ ys, p y = false) : ys.filter p = [] := by
  induction ys with
  | nil => rfl
  | cons a t ih =>
      simp only [List.filter_cons, h a (by simp)]
      exact ih (fun y hy => h y (by simp [hy]))
/-- Split a filter along a threshold: all `p`-elements lie below all `q`-elements. -/
theorem filter_partition (ys : List Nat) (p q : Nat → Bool) (b : Nat)
    (hsort : Pairwise (fun x y => x ≤ y) ys)
    (hp : ∀ y, y ∈ ys → p y = true → y < b)
    (hq : ∀ y, y ∈ ys → q y = true → b ≤ y) :
    filter (fun y => p y || q y) ys = filter p ys ++ filter q ys := by
  induction ys with
  | nil => rfl
  | cons z t ih =>
      obtain ⟨hzt, hrest⟩ := List.pairwise_cons.mp hsort
      have hih := ih hrest (fun y hy hz => hp y (by simp [hy]) hz)
        (fun y hy hz => hq y (by simp [hy]) hz)
      by_cases hpz : p z = true
      · have hqz : q z = false := by
          apply Bool.eq_false_of_ne_true
          intro h
          exact Nat.lt_irrefl z
            (Nat.lt_of_lt_of_le (hp z (by simp [hpz]) hpz) (hq z (by simp [h]) h))
        simp [List.filter_cons, hpz, hqz, hih]
      · by_cases hqz : q z = true
        · have hbz : b ≤ z := hq z (by simp [hqz]) hqz
          have hfp : filter p t = [] :=
            filter_eq_nil_of t p (fun w hw => Bool.eq_false_of_ne_true fun hwp =>
              Nat.lt_irrefl w
                (Nat.lt_of_lt_of_le (hp w (by simp [hw]) hwp) (Nat.le_trans hbz (hzt w hw))))
          simp [List.filter_cons, hpz, hqz, hih, hfp]
        · simp [List.filter_cons, hpz, hqz, hih]
/-- Ordered disjoint intervals: concatenating slices equals filtering by
membership in the interval union. -/
theorem flatMap_slices (ys : List Nat) (hsort : Pairwise (fun x y => x ≤ y) ys) :
    ∀ (rs : List (Nat × Nat)), Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) rs →
      ∀ q : Nat → Bool, (∀ y ∈ ys, q y = member rs y) →
        rs.flatMap (fun r => slice ys r.1 r.2) = ys.filter q := by
  intro rs
  induction rs with
  | nil =>
      intro _ q hq
      rw [List.flatMap_nil, List.filter_congr hq]
      simp [member]
  | cons r t ih =>
      obtain ⟨a, b⟩ := r
      intro hord q hq
      obtain ⟨hle, hord'⟩ := List.pairwise_cons.mp hord
      rw [List.filter_congr hq]
      rw [List.filter_congr (fun _ _ => member_cons a b t _)]
      rw [List.flatMap_cons, slice_eq_filter ys a b hsort,
        filter_partition ys (fun y => a ≤ y && y < b) (fun y => member t y) b hsort
          (fun y _ hy => ((by simpa using hy : a ≤ y ∧ y < b)).2)
          (fun y _ hy => by
            obtain ⟨p, hp, hpy⟩ := exists_of_member hy
            exact Nat.le_trans (hle p hp) hpy.1)]
      rw [ih hord' (fun y => member t y) (fun y hy => rfl)]
private theorem map_congr_of {α : Type} {β : Type} (rs : List α) (f g : α → β)
    (h : ∀ r ∈ rs, f r = g r) : rs.map f = rs.map g := by
  induction rs with
  | nil => rfl
  | cons a t ih => rw [List.map_cons, List.map_cons, h a (by simp), ih (fun r hr => h r (by simp [hr]))]
private theorem sum_map_length_flatMap (ys : List Nat) (rs : List (Nat × Nat)) :
    (rs.map (fun r => (slice ys r.1 r.2).length)).sum
      = (rs.flatMap (fun r => slice ys r.1 r.2)).length := by
  induction rs with
  | nil => rfl
  | cons r t ih =>
      rw [List.map_cons, List.sum_cons, List.flatMap_cons, ih]
      rw [← List.length_append]
private theorem sum_map_sum_flatMap (ys : List Nat) (rs : List (Nat × Nat)) :
    (rs.map (fun r => (slice ys r.1 r.2).sum)).sum
      = (rs.flatMap (fun r => slice ys r.1 r.2)).sum := by
  induction rs with
  | nil => rfl
  | cons r t ih =>
      rw [List.map_cons, List.sum_cons, List.flatMap_cons, List.sum_append, ih]
private theorem take_add_split (ys : List Nat) :
    ∀ i j, ys.take (i + j) = ys.take i ++ (ys.drop i).take j := by
  induction ys with
  | nil => intro i j; cases i <;> cases j <;> simp
  | cons a t ih =>
      intro i j
      cases i with
      | zero => rw [Nat.zero_add, take_zero, nil_append, drop_zero]
      | succ i =>
          rw [Nat.succ_add, List.take_succ_cons, List.take_succ_cons,
            List.drop_succ_cons, ih]
          rfl
private theorem drop_take_eq (ys : List Nat) (i j : Nat) :
    (ys.take (i + j)).drop i = (ys.drop i).take j := by
  rw [← List.take_drop]
private theorem take_sum_add (ys : List Nat) (i j : Nat) (hij : i ≤ j) :
    (ys.take j).sum = (ys.take i).sum + ((ys.take j).drop i).sum := by
  have h : ys.take j = ys.take (i + (j - i)) := by rw [Nat.add_sub_of_le hij]
  rw [h, drop_take_eq, take_add_split, List.sum_append]
private theorem slice_sum (ys : List Nat) (a b : Nat)
    (hsort : Pairwise (fun x y => x ≤ y) ys) :
    sumAt ys (lower ys b) - sumAt ys (lower ys a) = (slice ys a b).sum := by
  unfold sumAt
  obtain hcase : lower ys a ≤ lower ys b ∨ lower ys b ≤ lower ys a := Nat.le_total _ _
  rcases hcase with hle | hle
  · have hsd : (ys.take (lower ys b)).drop (lower ys a) = slice ys a b := by
      unfold slice
      rw [← Nat.add_sub_of_le hle, drop_take_eq, Nat.add_sub_cancel_left]
    have hj := take_sum_add ys (lower ys a) (lower ys b) hle
    rw [hsd] at hj
    omega
  · have hz : lower ys b - lower ys a = 0 := by omega
    have hnil : (slice ys a b).sum = 0 := by
      unfold slice
      rw [hz, take_zero, List.sum_nil]
    have hj := take_sum_add ys (lower ys b) (lower ys a) hle
    omega
/-- Top offset within concatenated slices agrees with `nth`. -/
theorem nth_slices (ys : List Nat) (rs : List (Nat × Nat))
    (hsort : Pairwise (fun x y => x ≤ y) ys) :
    ∀ k, nth ys rs k = (rs.flatMap (fun r => slice ys r.1 r.2))[k]? := by
  induction rs with
  | nil => intro k; simp [nth]
  | cons r t ih =>
      obtain ⟨a, b⟩ := r
      intro k
      have hn : lower ys b - lower ys a = (slice ys a b).length :=
        (slice_length ys a b hsort).symm
      show (if k < lower ys b - lower ys a then ys[lower ys a + k]?
        else nth ys t (k - (lower ys b - lower ys a))) = _
      rw [List.flatMap_cons, List.getElem?_append, ← hn]
      rcases Nat.lt_or_ge k (lower ys b - lower ys a) with hk | hk
      · rw [if_pos hk, if_pos hk]
        unfold slice
        rw [List.getElem?_take, if_pos hk, List.getElem?_drop]
      · rw [if_neg (Nat.not_lt.mpr hk), if_neg (Nat.not_lt.mpr hk)]
        exact ih (k - (lower ys b - lower ys a))
/-- Generic soundness: concatenating slices yields the filtered postings. -/
theorem generic_OrderedProbe (s : Search)
    (hspans : ∀ u t x y, y < u → s.test t x y = member (s.spans u t x) y)
    (hord : ∀ u t x, Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) (s.spans u t x)) :
    orderedProbe s := by
  intro u t x ys hvalid
  unfold seek
  refine flatMap_slices ys hvalid.1 (s.spans u t x) (hord u t x) (s.test t x) ?_
  intro y hy
  exact hspans u t x y (hvalid.2 y hy)
theorem generic_OffsetCount (s : Search)
    (hspans : ∀ u t x y, y < u → s.test t x y = member (s.spans u t x) y)
    (hord : ∀ u t x, Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) (s.spans u t x)) :
    offsetCount s := by
  intro u t x ys hvalid
  unfold fan
  have hlen : (s.spans u t x).map (fun (a, b) => lower ys b - lower ys a)
      = (s.spans u t x).map (fun r => (slice ys r.1 r.2).length) := by
    refine map_congr_of _ _ _ ?_
    intro r hr
    obtain ⟨a, b⟩ := r
    exact (slice_length ys a b hvalid.1).symm
  rw [hlen, sum_map_length_flatMap]
  rw [← length_filter_countP]
  exact congrArg List.length
    (flatMap_slices ys hvalid.1 (s.spans u t x) (hord u t x) (s.test t x)
      (fun y hy => hspans u t x y (hvalid.2 y hy)))
theorem generic_IntegralSum (s : Search)
    (hspans : ∀ u t x y, y < u → s.test t x y = member (s.spans u t x) y)
    (hord : ∀ u t x, Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) (s.spans u t x)) :
    integralSum s := by
  intro u t x ys hvalid
  unfold total
  have hsum : (s.spans u t x).map (fun (a, b) =>
      sumAt ys (lower ys b) - sumAt ys (lower ys a))
      = (s.spans u t x).map (fun r => (slice ys r.1 r.2).sum) := by
    refine map_congr_of _ _ _ ?_
    intro r hr
    obtain ⟨a, b⟩ := r
    exact slice_sum ys a b hvalid.1
  rw [hsum, sum_map_sum_flatMap]
  exact congrArg List.sum
    (flatMap_slices ys hvalid.1 (s.spans u t x) (hord u t x) (s.test t x)
      (fun y hy => hspans u t x y (hvalid.2 y hy)))
theorem generic_KthMatch (s : Search)
    (hspans : ∀ u t x y, y < u → s.test t x y = member (s.spans u t x) y)
    (hord : ∀ u t x, Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) (s.spans u t x)) :
    kthMatch s := by
  intro u t x ys k hvalid
  rw [nth_slices ys (s.spans u t x) hvalid.1,
    flatMap_slices ys hvalid.1 (s.spans u t x) (hord u t x) (s.test t x)
      (fun y hy => hspans u t x y (hvalid.2 y hy))]
theorem generic_PruneExtent (s : Search)
    (hspans : ∀ u t x y, y < u → s.test t x y = member (s.spans u t x) y) :
    pruneExtent s := by
  intro u t x ys lo hi hvalid hbound hsp
  refine List.eq_nil_of_length_eq_zero ?_
  rw [length_filter_countP]
  refine countP_eq_zero_of ys (s.test t x) ?_
  intro y hy
  have h1 : s.test t x y = member (s.spans u t x) y := hspans u t x y (hvalid.2 y hy)
  have h2 : member (s.spans u t x) y = false := by
    apply any_eq_false_of
    intro p hp
    obtain hcase := hsp p hp
    obtain ⟨hlo, hhi⟩ : lo ≤ y ∧ y < hi := by simpa using hbound y hy
    cases hcase with
    | inl hle =>
        apply Bool.eq_false_of_ne_true
        intro hmem
        obtain ⟨hp1, hp2⟩ : p.1 ≤ y ∧ y < p.2 := by simpa using hmem
        exact Nat.lt_irrefl y (Nat.lt_of_lt_of_le hp2 (Nat.le_trans hle hlo))
    | inr hge =>
        apply Bool.eq_false_of_ne_true
        intro hmem
        obtain ⟨hp1, hp2⟩ : p.1 ≤ y ∧ y < p.2 := by simpa using hmem
        exact Nat.lt_irrefl y (Nat.lt_of_lt_of_le hhi (Nat.le_trans hge hp1))
  rw [h1, h2]
-- ===== P2b.lean =====
/-! Counting machinery for the aggregate-level generic goals. -/
private theorem length_flatMap_sum {α : Type} {β : Type} [Inhabited β]
    (f : α → List β) (xs : List α) :
    (xs.flatMap f).length = (xs.map (fun x => (f x).length)).sum := by
  induction xs with
  | nil => rfl
  | cons a t ih =>
      rw [List.flatMap_cons, List.length_append, List.map_cons, List.sum_cons, ih]
private theorem map_sum_add (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.sum_cons, ih, List.map_cons, List.map_cons,
        List.sum_cons, List.sum_cons]
      omega
private theorem sum_map_zero (l : List Nat) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a t ih => rw [List.map_cons, List.sum_cons, ih]
private theorem nodup_eraseDups_aux : ∀ (n : Nat) (ds : List Nat), ds.length ≤ n →
    List.Nodup (ds.eraseDups) := by
  intro n
  induction n with
  | zero =>
      intro ds hlen
      cases ds with
      | nil => exact List.Pairwise.nil
      | cons z t => exact absurd hlen (by simp [List.length_cons])
  | succ m ih =>
      intro ds hlen
      cases ds with
      | nil => exact List.Pairwise.nil
      | cons z t =>
          rw [List.eraseDups_cons]
          refine List.Pairwise.cons ?_ (ih _ ?_)
          · intro x hx heq
            obtain ⟨hxt, hxz⟩ := List.mem_filter.mp (List.mem_eraseDups.mp hx)
            exact absurd (by simpa [heq] using hxz) (by simpa using hxz)
          · have hl1 : t.length ≤ m := by simpa using hlen
            exact Nat.le_trans (List.length_filter_le (fun b => !b == z) t) hl1
theorem nodup_eraseDups (ds : List Nat) : List.Nodup (ds.eraseDups) :=
  nodup_eraseDups_aux ds.length ds (Nat.le_refl _)
/-- First-occurrence extraction from a duplicate-free list. -/
private theorem first_sum (h : Nat → Nat) :
    ∀ (d : List Nat), List.Nodup d → ∀ a, a ∈ d →
      (d.map (fun x => if a == x then h x else 0)).sum = h a := by
  intro d
  induction d with
  | nil => intro _ a ha; exact absurd ha (by simp)
  | cons z t ih =>
      intro hd a ha
      obtain ⟨hne, ht⟩ := List.pairwise_cons.mp hd
      obtain hz | ht' := List.mem_cons.mp ha
      · subst hz
        rw [List.map_cons, List.sum_cons, if_pos (by simp)]
        have hzero : t.map (fun x => if a == x then h x else 0)
            = t.map (fun _ => 0) :=
          map_congr_of t _ _ (fun x hx => if_neg (fun hc =>
            absurd (by simpa using hc) (hne x hx)))
        rw [hzero, sum_map_zero]
        omega
      · rw [List.map_cons, List.sum_cons,
          if_neg (fun hc => absurd (Ne.symm (hne a ht')) (by simpa using hc))]
        rw [ih ht a ht']
        simp
/-- Grouping: summing over a list equals summing over distinct elements
weighted by their multiplicity. -/
private theorem grouping_sum (h : Nat → Nat) :
    ∀ (xs : List Nat),
      (xs.map h).sum = (xs.eraseDups.map (fun x => xs.count x * h x)).sum := by
  have key : ∀ (xs : List Nat) (d : List Nat), List.Nodup d → (∀ x ∈ xs, x ∈ d) →
      (xs.map h).sum = (d.map (fun x => xs.count x * h x)).sum := by
    intro xs
    induction xs with
    | nil =>
        intro d hd _
        have hz : (d.map (fun x => ([] : List Nat).count x * h x)) = d.map (fun _ => 0) :=
          map_congr_of d _ _ (fun x _ => by simp)
        rw [hz, sum_map_zero, List.map_nil, List.sum_nil]
    | cons a t ih =>
        intro d hd hmem
        have ha : a ∈ d := hmem a (by simp)
        have h1 := ih d hd (fun x hx => hmem x (by simp [hx]))
        have h2 : d.map (fun x => (a :: t).count x * h x)
            = d.map (fun x => t.count x * h x + (if a == x then h x else 0)) :=
          map_congr_of d _ _ (fun x _ => by
            rw [List.count_cons]
            by_cases hxa : a == x <;> simp [hxa, Nat.add_mul])
        rw [List.map_cons, List.sum_cons, h1, h2, map_sum_add]
        rw [first_sum h d hd a ha]
        omega
  intro xs
  refine key xs xs.eraseDups (nodup_eraseDups xs) ?_
  intro x hx
  exact List.mem_eraseDups.mpr hx
/-- Two lower-bound offsets per distinct left key give the aggregate count. -/
theorem generic_RepeatedProbe (s : Search)
    (hspans : ∀ u t x y, y < u → s.test t x y = member (s.spans u t x) y)
    (hord : ∀ u t x, Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) (s.spans u t x)) :
    repeatedProbe s := by
  intro u t xs ys hv
  have hf := generic_OffsetCount s hspans hord u t
  have h1 (x : Nat) : (ys.filter (s.test t x)).length = fan s u t x ys := by
    rw [hf x ys hv, length_filter_countP]
  have hconv : xs.eraseDups.map (fun x => xs.count x * fan s u t x ys)
      = xs.eraseDups.map (fun x => xs.count x * (ys.filter (s.test t x)).length) :=
    map_congr_of _ _ _ (fun x _ => by rw [h1 x])
  rw [length_flatMap_sum (fun x => ys.filter (s.test t x)) xs, hconv,
    ← grouping_sum (fun x => (ys.filter (s.test t x)).length) xs]
private theorem flatMap_congr_of {α β : Type} (f g : α → List β) (xs : List α)
    (h : ∀ x ∈ xs, f x = g x) : xs.flatMap f = xs.flatMap g := by
  induction xs with
  | nil => rfl
  | cons a t ih => rw [List.flatMap_cons, List.flatMap_cons, h a (by simp), ih (fun x hx => h x (by simp [hx]))]
private theorem flatMap_singleton_app {α β : Type} [Inhabited β] (f : α → List β) (a : α) :
    [a].flatMap f = f a := by
  rw [List.flatMap_cons, List.flatMap_nil, List.append_nil]
private theorem zipOuter_per_x (s : Search) (t : Nat) (xs ys : List (Option Nat)) :
    zipOuter s t xs ys = xs.flatMap (fun x => zipOuter s t [x] ys) := by
  unfold zipOuter
  refine Eq.trans (congrArg (List.flatMap · xs) (funext (fun x => ?_))) rfl
  rw [List.flatMap_cons, List.flatMap_nil, List.append_nil]
/-- One NULL-or-empty left row still charges exactly one outer placeholder. -/
theorem generic_LeftMultiplicity (s : Search)
    (hspans : ∀ u t x y, y < u → s.test t x y = member (s.spans u t x) y)
    (hord : ∀ u t x, Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) (s.spans u t x)) :
    leftMultiplicity s := by
  intro u t xs ys hv
  have hf := generic_OffsetCount s hspans hord u t
  have hrow (x : Option Nat) :
      (zipOuter s t [x] ys).length = max 1 ((x.map (fun v => fan s u t v (ys.filterMap id))).getD 0) := by
    unfold zipOuter
    rw [List.flatMap_cons, List.flatMap_nil, List.append_nil]
    rcases x with _ | v
    · have hnil : (ys.filterMap id).filter
          (fun y => (none : Option Nat).any (fun k => s.test t k y)) = [] :=
        filter_eq_nil_of _ _ (fun _ _ => rfl)
      rw [hnil]
      simp
    · have hc : (ys.filterMap id).filter (fun y => (some v).any (fun k => s.test t k y))
          = (ys.filterMap id).filter (s.test t v) :=
        List.filter_congr (fun _ _ => rfl)
      rw [hc]
      cases h : (ys.filterMap id).filter (s.test t v) with
      | nil =>
          have hcount : (ys.filterMap id).countP (s.test t v) = 0 := by
            rw [← length_filter_countP, h, List.length_nil]
          have hget : ((some v).map (fun w => fan s u t w (ys.filterMap id))).getD 0
              = fan s u t v (ys.filterMap id) := rfl
          rw [hget, hf v (ys.filterMap id) hv, hcount]
          simp
      | cons a l =>
          have hcount : (ys.filterMap id).countP (s.test t v) = l.length + 1 := by
            rw [← length_filter_countP, h, List.length_cons]
          have hget : ((some v).map (fun w => fan s u t w (ys.filterMap id))).getD 0
              = fan s u t v (ys.filterMap id) := rfl
          rw [hget, hf v (ys.filterMap id) hv, hcount]
          rw [Nat.max_eq_right (by omega : 1 ≤ l.length + 1)]
          simp
  rw [zipOuter_per_x s t xs ys]
  rw [length_flatMap_sum (fun x => zipOuter s t [x] ys) xs]
  refine congrArg List.sum (map_congr_of _ _ _ (fun x _ => ?_))
  have hx := hrow x
  simpa using hx
private theorem contains_eraseDups (ds : List Nat) (i : Nat) :
    ds.eraseDups.contains i = ds.contains i := by
  simp [List.mem_eraseDups]
private theorem countP_bool_split {α : Type} [Inhabited α] (l : List α) (p q : α → Bool) :
    l.countP p = l.countP (fun x => p x && q x) + l.countP (fun x => p x && !q x) := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.countP_cons, List.countP_cons, List.countP_cons, ih]
      by_cases ha : p a <;> by_cases hqa : q a <;> simp_all <;> omega
private theorem countP_or_excl {α : Type} [Inhabited α] (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, (p x && q x) = false) :
    l.countP (fun x => p x || q x) = l.countP p + l.countP q := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.countP_cons, List.countP_cons, List.countP_cons,
        ih (fun x hx => h x (by simp [hx]))]
      have := h a (by simp)
      by_cases ha : p a <;> by_cases hqa : q a <;> simp_all <;> omega
private theorem countP_range_beq (n z : Nat) (G : Nat → Bool) :
    (List.range n).countP (fun i => (z == i) && G i)
      = if z < n then (if G z then 1 else 0) else 0 := by
  induction n with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.countP_append, ih, List.countP_cons, List.countP_nil]
      by_cases hzm : z == m
      · obtain rfl : z = m := by simpa using hzm
        simp
      · have hne : ¬ z = m := fun e => hzm (by simpa using e)
        by_cases hz : z < m
        · have hs : z < m + 1 := Nat.lt.step hz
          by_cases hg : G z <;> simp [hz, hs, hg, hzm]
        · have hzn : ¬ z < m + 1 := by
            intro hlt
            have hle : z ≤ m := Nat.le_of_lt_succ hlt
            omega
          simp [hz, hzn, hzm]
private theorem countP_eq_countP_range (ys : List Nat) (F : Nat → Bool) :
    ys.countP F = (List.range ys.length).countP (fun i => F (ys[i]!)) := by
  induction ys with
  | nil => rfl
  | cons a t ih =>
      rw [List.length_cons, List.range_succ_eq_map, List.countP_cons, ih,
        List.countP_cons, List.countP_map]
      have hcomp : (fun i => F ((a :: t)[i]!)) ∘ Nat.succ = fun j => F (t[j]!) := by
        funext j
        simp [Function.comp_apply, Nat.succ_eq_add_one, get!_succ]
      rw [hcomp]
      exact congrArg (fun b => countP (fun j => F t[j]!) (range t.length) + if b = true then 1 else 0)
        (congrArg F (get!_zero a t))
private theorem any_get?_eq (ys : List Nat) (F : Nat → Bool) (i : Nat) :
    (ys[i]?.any F) = (decide (i < ys.length) && F (ys[i]!)) := by
  by_cases h : i < ys.length
  · have h1 : ys[i]? = some (ys[i]!) :=
      Eq.trans (List.getElem?_eq_getElem h)
        (congrArg some (get!_eq_of_getElem ys i h rfl).symm)
    rw [h1, Option.any_some, decide_eq_true h, Bool.true_and]
  · have h1 : ys[i]? = none := get?_ge ys i (by omega)
    rw [h1, Option.any_none, decide_eq_false h, Bool.false_and]
/-- Masked positions on `eraseDups` counted via `range`. -/
private theorem countP_range_contains (n : Nat) (G : Nat → Bool) :
    ∀ (ds : List Nat), List.Nodup ds →
      (List.range n).countP (fun i => ds.contains i && G i)
        = ds.countP (fun i => decide (i < n) && G i) := by
  intro ds
  induction ds with
  | nil =>
      intro _
      have hp : (fun i => ([] : List Nat).contains i && G i) = (fun _ => false) :=
        funext (fun i => by simp)
      rw [hp]
      exact countP_eq_zero_of _ _ (fun i _ => by simp)
  | cons z t ih =>
      intro hd
      obtain ⟨hne, ht⟩ := List.pairwise_cons.mp hd
      have hdec : (fun i => ((z :: t).contains i) && G i)
          = (fun i => ((z == i) && G i) || (t.contains i && G i)) := by
        refine funext (fun i => ?_)
        by_cases h1 : i = z <;> by_cases h2 : i ∈ t <;> by_cases h3 : G i <;>
          simp [h1, h2, h3] <;> omega
      have hexcl : ∀ i ∈ List.range n,
          ((z == i) && G i && (t.contains i && G i)) = false := by
        intro i _
        by_cases h1 : z = i
        · subst h1
          by_cases h2 : z ∈ t
          · exact absurd rfl (hne z h2)
          · simp [h2]
        · by_cases h3 : G i <;> by_cases h2 : i ∈ t <;> simp [h1, h2, h3] <;> omega
      have hsplit : (List.range n).countP (fun i => (z :: t).contains i && G i)
          = (List.range n).countP (fun i => (z == i) && G i)
              + (List.range n).countP (fun i => t.contains i && G i) := by
        rw [countP_congr _ _ _ (fun i _ => congrFun hdec i), countP_or_excl _ _ _ hexcl]
      rw [hsplit, countP_range_beq, ih ht, List.countP_cons]
      by_cases hzn : z < n
      · by_cases hgz : G z <;> simp [hzn, hgz] <;> omega
      · simp [hzn]
/-- Masked `eraseDups` counting via `range`. -/
private theorem countP_eraseDups_range (ys : List Nat) (ds : List Nat) (F : Nat → Bool) :
    ds.eraseDups.countP (fun i => (ys[i]?.any F))
      = (List.range ys.length).countP (fun i => ds.contains i && F (ys[i]!)) := by
  have hnd : List.Nodup ds.eraseDups := nodup_eraseDups ds
  rw [countP_congr _ _ _ (fun i _ => any_get?_eq ys F i)]
  rw [← countP_range_contains ys.length (fun i => F (ys[i]!)) ds.eraseDups hnd]
  exact countP_congr _ _ _ (fun i _ =>
    congrArg (fun b => b && F (ys[i]!)) (contains_eraseDups ds i))
private theorem countP_map_succ_congr (t : List Nat) (P Q : Nat → Bool)
    (h : ∀ j, P (Nat.succ j) = Q j) :
    (List.map Nat.succ (List.range t.length)).countP P
        = (List.range t.length).countP Q := by
  rw [List.countP_map]
  exact countP_congr _ _ _ (fun j _ => h j)
/-- Masked `zipIdx` scan counted via `range`. -/
private theorem countP_filter_zipIdx (ds : List Nat) (F : Nat → Bool) :
    ∀ (k : Nat) (ys : List Nat),
      (((ys.zipIdx k).filter fun p => !ds.contains p.2).map Prod.fst).countP F
        = (List.range ys.length).countP fun i => F (ys[i]!) && !ds.contains (k + i) := by
  intro k ys
  induction ys generalizing k with
  | nil => rfl
  | cons a t ih =>
      rw [List.zipIdx_cons, List.length_cons]
      have htail : (List.map Nat.succ (List.range t.length)).countP
              (fun i => F ((a :: t)[i]!) && !ds.contains (k + i))
          = (List.range t.length).countP
              (fun i => F (t[i]!) && !ds.contains (k + 1 + i)) :=
        countP_map_succ_congr t (fun i => F ((a :: t)[i]!) && !ds.contains (k + i))
            (fun i => F (t[i]!) && !ds.contains (k + 1 + i))
            (fun j => by
              rw [Nat.succ_eq_add_one, get!_succ a t j,
                show k + (j + 1) = k + 1 + j from by omega])
      rw [List.filter_cons]
      by_cases hc : ds.contains k = true
      · rw [if_neg (by simpa using hc), ih, List.range_succ_eq_map, List.countP_cons, htail]
        have h0 : (F ((a :: t)[0]!) && !ds.contains (k + 0)) = false := by
          rw [get!_zero, Nat.add_zero, hc]
          cases F a <;> rfl
        rw [h0]
        simp
      · have hnk : ds.contains k = false := by
          cases h : ds.contains k with
          | true => exact absurd h hc
          | false => exact rfl
        rw [if_pos (by simpa using hnk), List.map_cons, List.countP_cons, ih,
          List.range_succ_eq_map, List.countP_cons, htail]
        have h0 : (F ((a :: t)[0]!) && !ds.contains (k + 0)) = F a := by
          rw [get!_zero, Nat.add_zero, hnk]
          cases F a <;> rfl
        rw [h0]
/-- Pointwise-`toNat` additivity of `countP`. -/
private theorem countP_add {α : Type} [Inhabited α] (l : List α) (p q r : α → Bool)
    (h : ∀ y ∈ l, (p y).toNat + (q y).toNat = (r y).toNat) :
    l.countP p + l.countP q = l.countP r := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      have ha := h a (by simp)
      have hsum := ih (fun y hy => h y (by simp [hy]))
      rw [List.countP_cons, List.countP_cons, List.countP_cons]
      have hb : ∀ b : Bool, (if b = true then 1 else 0 : Nat) = b.toNat := by
        intro b; cases b <;> rfl
      rw [hb (p a), hb (q a), hb (r a)]
      omega
/-- `countP` as a sum of indicator values. -/
private theorem countP_eq_sum {α : Type} [Inhabited α] (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.countP_cons, List.map_cons, List.sum_cons, ih]
      cases h : p a <;> simp [h] <;> omega
theorem maskedJoin_of (s : Search) (hf : offsetCount s) : maskedJoin s := by
  intro u t x ys ds hv
  rw [hf u t x ys hv]
  have h1 : ys.countP (s.test t x)
      = (List.range ys.length).countP (fun i => s.test t x (ys[i]!)) :=
    countP_eq_countP_range ys (s.test t x)
  have h2 : ds.eraseDups.countP (fun i => (ys[i]?.any (s.test t x)))
      = (List.range ys.length).countP (fun i => ds.contains i && s.test t x (ys[i]!)) :=
    countP_eraseDups_range ys ds (s.test t x)
  have h3 : ((ys.zipIdx.filter fun p => !ds.contains p.2).map Prod.fst).countP (s.test t x)
      = (List.range ys.length).countP (fun i => s.test t x (ys[i]!) && !ds.contains i) :=
    (countP_filter_zipIdx ds (s.test t x) 0 ys).trans (by simp only [Nat.zero_add])
  have h4 : (List.range ys.length).countP (fun i => s.test t x (ys[i]!))
      = (List.range ys.length).countP (fun i => s.test t x (ys[i]!) && !ds.contains i)
        + (List.range ys.length).countP (fun i => ds.contains i && s.test t x (ys[i]!)) := by
    have h := countP_bool_split (List.range ys.length)
        (fun i => s.test t x (ys[i]!)) (fun i => ds.contains i)
    rw [h, Nat.add_comm _ _]
    refine Nat.add_right_inj.mpr ?_
    refine countP_congr _ _ _ (fun i _ => ?_)
    have e : ∀ a b : Bool, (a && b) = (b && a) := by
      intro a b; cases a <;> cases b <;> rfl
    exact e _ _
  rw [h1, h2, h3, h4]
  omega
theorem generic_MaskedJoin (s : Search)
    (hspans : ∀ u t x y, y < u → s.test t x y = member (s.spans u t x) y)
    (hord : ∀ u t x, Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) (s.spans u t x)) :
    maskedJoin s :=
  maskedJoin_of s (generic_OffsetCount s hspans hord)
/-- Interval membership primitives. -/
private theorem member_singleton (a b y : Nat) : member [(a, b)] y = (a ≤ y && y < b) := by
  simp [member]
private theorem member_optional (a b y : Nat) :
    member (optional a b) y = (a ≤ y && y < b) := by
  have h1 : optional a b = if a < b then [(a, b)] else [] := rfl
  rw [h1]
  by_cases h : a < b <;> simp [h, member] <;>
    (intro hy <;> omega)
/-- Pairwise chains of sorted intervals. -/
private theorem pw_one (a b : Nat) :
    Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) [(a, b)] :=
  List.Pairwise.cons (fun _ h => by cases h) List.Pairwise.nil
private theorem pw_two (a b c d : Nat) (h : b ≤ c) :
    Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) [(a, b), (c, d)] :=
  List.Pairwise.cons (fun p' hp => by
    simp at hp
    subst hp
    exact h)
    (pw_one c d)
/-- A pigeonhole bound for lists of distinct bounded numbers. -/
private theorem length_le_of_nodup_lt {l : List Nat} (n : Nat) (hd : l.Nodup)
    (h : ∀ x ∈ l, x < n) : l.length ≤ n := by
  have h1 : (List.range n).countP (fun i => l.contains i && true) = l.length := by
    rw [countP_range_contains n (fun _ => true) l hd]
    refine countP_eq_len_of_forall l (fun i => decide (i < n) && true) ?_
    intro i hi
    simp [h i hi]
  have h1' : (List.range n).countP (fun i => l.contains i) = l.length :=
    (countP_congr (List.range n) _ _ (fun i _ => by cases l.contains i <;> simp)).trans h1
  have h2 : (List.range n).countP (fun i => l.contains i) ≤ n := by
    refine Nat.le_trans (countP_le_countP (List.range n) (fun i => l.contains i)
        (fun _ => true) (fun i _ _ => rfl)) ?_
    rw [countP_eq_len_of_forall (List.range n) (fun _ => true) (fun _ _ => rfl)]
    exact Nat.le_of_eq (List.length_range)
  rw [← h1']
  exact h2
/-- Quotient/residue bridges. -/
private theorem div_bounds {y w q : Nat} (hw : 0 < w) (h : y / w = q) :
    w * q ≤ y ∧ y < w * q + w := by
  have hh := (Nat.div_eq_iff hw).mp h
  rw [Nat.mul_comm q w] at hh
  exact ⟨hh.1, by omega⟩
private theorem div_eqI {y w q : Nat} (h1 : w * q ≤ y) (h2 : y < w * q + w) :
    y / w = q := by
  rw [Nat.mul_comm w q] at h1 h2
  refine Nat.div_eq_of_lt_le h1 ?_
  rw [Nat.add_mul, Nat.one_mul]
  exact h2
-- ===== P2c.lean =====
/-! Extra machinery for the per-model special goals. -/
/-- Rewriting the proposition under `decide`. -/
private theorem decide_congr {p q : Prop} [dp : Decidable p] [dq : Decidable q] (h : p ↔ q) :
    (decide p : Bool) = decide q := by
  cases dp with
  | isTrue hp =>
      cases dq with
      | isTrue _ => rfl
      | isFalse hnq => exact absurd (h.mp hp) hnq
  | isFalse hnp =>
      cases dq with
      | isTrue hq => exact absurd (h.mpr hq) hnp
      | isFalse _ => rfl
private theorem decide_and (p q : Prop) [Decidable p] [Decidable q] :
    decide (p ∧ q) = (decide p && decide q) := by
  cases (inferInstance : Decidable p) <;> cases (inferInstance : Decidable q) <;> rfl
private theorem decide_or (p q : Prop) [Decidable p] [Decidable q] :
    decide (p ∨ q) = (decide p || decide q) := by
  cases (inferInstance : Decidable p) <;> cases (inferInstance : Decidable q) <;> rfl
private theorem beq_decide (a b : Nat) : (a == b) = decide (a = b) := rfl
private theorem any_pointwise {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.any p = l.any q := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.any_cons, List.any_cons, h a (by simp),
        ih (fun x hx => h x (by simp [hx]))]
private theorem any_mem_true {α : Type} (l : List α) (f : α → Bool) (a : α)
    (hf : f a = true) (ha : a ∈ l) : l.any f = true := by
  rw [List.any_eq_true]
  exact ⟨a, ha, hf⟩
private theorem member_append (r1 r2 : Ranges) (y : Nat) :
    member (r1 ++ r2) y = (member r1 y || member r2 y) := by
  simp only [member, List.any_append]
private theorem member_map_pair (l : List Nat) (g : Nat → Nat) (y : Nat) :
    member (l.map (fun i => (g i, g i + 1))) y =
      l.any (fun i => decide (g i ≤ y) && decide (y < g i + 1)) := by
  rw [member, List.any_map]
  exact any_pointwise l _ _ (fun i _ => rfl)
private theorem sum_map_const_zero (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a t ih => rw [List.map_cons, List.sum_cons, ih, Nat.zero_add]
private theorem sum_map_add (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun k => f k + g k)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.sum_cons, ih]
      simp only [List.map_cons, List.sum_cons]
      omega
private theorem sum_range_eq_pick (K m : Nat) (hm : m < K) (G : Nat → Nat) :
    ((List.range K).map (fun k => if m = k then G k else 0)).sum = G m := by
  induction K with
  | zero => exact absurd hm (Nat.not_lt_zero m)
  | succ K ih =>
      rcases Nat.lt_or_eq_of_le (Nat.le_of_lt_succ hm) with h | h
      · rw [List.range_succ, List.map_append, List.sum_append, ih h]
        rw [show (List.map (fun k => if m = k then G k else 0) [K]).sum = 0 from by
          rw [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero,
            if_neg (by omega : ¬(m = K))]]
        omega
      · rw [h]
        have hz : (List.range K).map (fun k => if K = k then G k else 0) =
            (List.range K).map (fun _ => 0) := by
          refine map_congr_of (List.range K) _ _ (fun r hr =>
            if_neg (Nat.ne_of_gt (List.mem_range.mp hr)))
        rw [List.range_succ, List.map_append, hz, List.sum_append, sum_map_const_zero,
          List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero, Nat.zero_add,
          if_pos rfl]
private theorem countP_range_eq (q T : Nat) (hq : q < T) :
    ((List.range T).countP (fun t => decide (q = t))) = 1 := by
  rw [countP_eq_sum]
  have h1 : (List.range T).map (fun t => if decide (q = t) = true then 1 else 0)
      = (List.range T).map (fun t => if q = t then 1 else 0) := by
    refine map_congr_of (List.range T) _ _ (fun t _ => ?_)
    by_cases h : q = t <;> simp [h]
  rw [h1, sum_range_eq_pick T q hq (fun _ => 1)]
private theorem sum_swap (f : Nat → Nat → Nat) (a b : List Nat) :
    (a.map (fun x => (b.map (f x)).sum)).sum =
      (b.map (fun y => (a.map (fun x => f x y)).sum)).sum := by
  induction a with
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      rw [sum_map_const_zero]
  | cons x t ih =>
      rw [List.map_cons, List.sum_cons, ih]
      simp only [List.map_cons, List.sum_cons]
      rw [← sum_map_add]
private theorem double_count (f : Nat → Nat) (xs ys : List Nat) (K : Nat)
    (h : ∀ x ∈ xs, f x < K) :
    (xs.map (fun x => ys.countP (fun y => decide (f x = f y)))).sum
      = ((List.range K).map (fun k =>
          xs.countP (fun x => decide (f x = k)) * ys.countP (fun y => decide (f y = k)))).sum := by
  induction xs with
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      have hcn : (List.range K).map (fun k =>
          (List.countP (fun x => decide (f x = k)) ([] : List Nat))
            * (ys.countP (fun y => decide (f y = k))))
          = (List.range K).map (fun _ => (0 : Nat)) :=
        map_congr_of (List.range K) _ _ (fun k _ => by rw [List.countP_nil, Nat.zero_mul])
      rw [hcn, sum_map_const_zero]
  | cons a t ih =>
      rw [List.map_cons, List.sum_cons, ih (fun z hz => h z (by simp [hz]))]
      have hK : f a < K := h a (by simp)
      have hc : (List.range K).map (fun k =>
          (a :: t).countP (fun x => decide (f x = k)) * ys.countP (fun y => decide (f y = k)))
          = (List.range K).map (fun k =>
            (t.countP (fun x => decide (f x = k)) + if f a = k then 1 else 0)
              * ys.countP (fun y => decide (f y = k))) := by
        refine map_congr_of (List.range K) _ _ (fun k _ => ?_)
        rw [List.countP_cons]
        by_cases hk : f a = k <;> simp [hk]
      have h2 : (List.range K).map (fun k =>
          (t.countP (fun x => decide (f x = k)) + if f a = k then 1 else 0)
            * ys.countP (fun y => decide (f y = k)))
          = (List.range K).map (fun k =>
            t.countP (fun x => decide (f x = k)) * ys.countP (fun y => decide (f y = k))
              + if f a = k then ys.countP (fun y => decide (f y = k)) else 0) := by
        refine map_congr_of (List.range K) _ _ (fun k _ => ?_)
        rw [Nat.add_mul]
        by_cases hk : f a = k <;> simp [hk]
      have h3 : ((List.range K).map (fun k =>
          t.countP (fun x => decide (f x = k)) * ys.countP (fun y => decide (f y = k))
            + if f a = k then ys.countP (fun y => decide (f y = k)) else 0)).sum
          = ((List.range K).map (fun k =>
            t.countP (fun x => decide (f x = k)) * ys.countP (fun y => decide (f y = k)))).sum
            + ((List.range K).map (fun k =>
              if f a = k then ys.countP (fun y => decide (f y = k)) else 0)).sum :=
        sum_map_add _ _ _
      have h4 : ((List.range K).map (fun k =>
          if f a = k then ys.countP (fun y => decide (f y = k)) else 0)).sum
          = ys.countP (fun y => decide (f a = f y)) :=
        (sum_range_eq_pick K (f a) hK _).trans
          (countP_congr ys _ _ (fun y _ =>
            decide_congr (Iff.intro (fun e : f y = f a => e.symm) (fun e => e.symm))))
      rw [hc, h2, h3, h4]
      omega
end Helpers_ModuloClass
namespace TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass
open List
theorem hspans : ∀ u t x y, y < u → search.test t x y = member (search.spans u t x) y := by
  intro u t x y hyu
  have hw : 0 < t+1 := Nat.succ_pos t
  have hsp : search.spans u t x =
      (List.range ((u+(t+1)-1-x%(t+1))/(t+1))).map
        (fun i => (x%(t+1)+i*(t+1), x%(t+1)+i*(t+1)+1)) := rfl
  generalize hr : x%(t+1) = r
  rw [hr] at hsp
  have hrlt : r < t+1 := by rw [← hr]; exact Nat.mod_lt x hw
  have hte : search.test t x y = (r == y%(t+1)) := by
    rw [show search.test t x y = (x%(t+1) == y%(t+1)) from rfl, hr]
  rw [hte, hsp, Helpers_ModuloClass.member_map_pair (List.range ((u+(t+1)-1-r)/(t+1))) (fun i => r+i*(t+1)) y]
  generalize hK : (u+(t+1)-1-r)/(t+1) = K
  by_cases hany : (List.range K).any
      (fun i => decide (r+i*(t+1) ≤ y) && decide (y < r+i*(t+1)+1))
  · obtain ⟨i, hi, hpi0⟩ := List.any_eq_true.mp hany
    have hpi : (decide (r+i*(t+1) ≤ y) && decide (y < r+i*(t+1)+1)) = true := hpi0
    have hand : ((decide (r+i*(t+1) ≤ y)) = true) ∧ ((decide (y < r+i*(t+1)+1)) = true) := by
      rw [← Bool.and_eq_true]; exact hpi
    have hb1 : r + i*(t+1) ≤ y := of_decide_eq_true hand.1
    have hb2 : y < r + i*(t+1) + 1 := of_decide_eq_true hand.2
    have hyr : y%(t+1) = r := by
      have h1 : (r + i*(t+1)) % (t+1) = r := by
        rw [Nat.mul_comm i (t+1), Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hrlt]
      have h2 : y = r + i*(t+1) := by omega
      rw [h2]; exact h1
    rw [hany, hyr]
    simp
  · rw [Helpers_ModuloClass.beq_decide r (y%(t+1)),
      Helpers_ModuloClass.decide_congr ⟨fun e => e.symm, fun e => e.symm⟩]
    by_cases hd : y%(t+1) = r
    · rw [decide_eq_true hd]
      have hjy : y = r + (y/(t+1))*(t+1) := by
        have hdm := Nat.div_add_mod y (t+1)
        rw [Nat.mul_comm (t+1) (y/(t+1))] at hdm
        rw [hd] at hdm
        omega
      have hstep : y/(t+1) + 1 ≤ (u+(t+1)-1-r)/(t+1) := by
        refine (Nat.le_div_iff_mul_le hw).mpr ?_
        rw [Nat.add_mul, Nat.one_mul]
        omega
      have hjK : y/(t+1) < K := by
        rw [← hK]
        exact Nat.lt_of_lt_of_le (Nat.lt_succ_self (y/(t+1))) hstep
      symm
      exact Helpers_ModuloClass.any_mem_true (List.range K) _ (y/(t+1))
        (by
          show (decide (r+(y/(t+1))*(t+1) ≤ y) && decide (y < r+(y/(t+1))*(t+1)+1)) = true
          rw [Bool.and_eq_true]
          exact ⟨decide_eq_true (by omega), decide_eq_true (by omega)⟩)
        (List.mem_range.mpr hjK)
    · rw [decide_eq_false hd]
      symm
      refine Bool.of_not_eq_true (fun heq => hd ?_)
      obtain ⟨i, hi, hpi0⟩ := List.any_eq_true.mp heq
      have hpi : (decide (r+i*(t+1) ≤ y) && decide (y < r+i*(t+1)+1)) = true := hpi0
      have hand : ((decide (r+i*(t+1) ≤ y)) = true) ∧ ((decide (y < r+i*(t+1)+1)) = true) := by
        rw [← Bool.and_eq_true]; exact hpi
      have hb1 : r + i*(t+1) ≤ y := of_decide_eq_true hand.1
      have hb2 : y < r + i*(t+1) + 1 := of_decide_eq_true hand.2
      have h2 : y = r + i*(t+1) := by omega
      rw [h2, Nat.mul_comm i (t+1), Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hrlt]
theorem hord : ∀ u t x, List.Pairwise (fun p q : Nat × Nat => p.2 ≤ q.1) (search.spans u t x) := by
  intro u t x
  have hw : 0 < t+1 := Nat.succ_pos t
  have hsp : search.spans u t x =
      (List.range ((u+(t+1)-1-x%(t+1))/(t+1))).map
        (fun i => (x%(t+1)+i*(t+1), x%(t+1)+i*(t+1)+1)) := rfl
  rw [hsp]
  generalize hr : x%(t+1) = r
  generalize hK : (u+(t+1)-1-r)/(t+1) = K
  clear hsp hK hr
  revert r
  induction K with
  | zero =>
      intro r
      rw [List.range_zero, List.map_nil]
      exact List.Pairwise.nil
  | succ K ih =>
      intro r
      have hcross : ∀ i j : Nat, i < j → r + i*(t+1) + 1 ≤ r + j*(t+1) := by
        intro i j hij
        have h2 : i*(t+1) + (t+1) = (i+1)*(t+1) :=
          ((Nat.add_mul i 1 (t+1)).trans (congrArg (i*(t+1) + ·) (Nat.one_mul (t+1)))).symm
        have h3 : (i+1)*(t+1) ≤ j*(t+1) :=
          Nat.mul_le_mul_right (t+1) (Nat.succ_le_of_lt hij)
        calc r + i*(t+1) + 1 ≤ r + i*(t+1) + (t+1) :=
            Nat.add_le_add_left (Nat.succ_le_of_lt hw) (r + i*(t+1))
          _ = r + (i*(t+1) + (t+1)) := Nat.add_assoc r (i*(t+1)) (t+1)
          _ = r + (i+1)*(t+1) := congrArg (r + ·) h2
          _ ≤ r + j*(t+1) := Nat.add_le_add_left h3 r
      rw [List.range_succ, List.map_append]
      rw [show (([K] : List Nat).map (fun i => (r+i*(t+1), r+i*(t+1)+1))) =
            [(r+K*(t+1), r+K*(t+1)+1)] from rfl]
      rw [List.pairwise_append]
      refine ⟨ih r, ⟨List.Pairwise.cons (fun y hy => absurd hy List.not_mem_nil)
        List.Pairwise.nil, ?_⟩⟩
      intro p hp q hq
      obtain ⟨i, hi, hpi⟩ := List.mem_map.mp hp
      obtain rfl := List.mem_singleton.mp hq
      rw [← hpi]
      exact show r + i*(t+1) + 1 ≤ r + K*(t+1) from hcross i K (List.mem_range.mp hi)
theorem sound_OrderedProbe : OrderedProbe.Goal :=
  Helpers_ModuloClass.generic_OrderedProbe search hspans hord
theorem sound_OffsetCount : OffsetCount.Goal :=
  Helpers_ModuloClass.generic_OffsetCount search hspans hord
theorem sound_IntegralSum : IntegralSum.Goal :=
  Helpers_ModuloClass.generic_IntegralSum search hspans hord
theorem sound_KthMatch : KthMatch.Goal :=
  Helpers_ModuloClass.generic_KthMatch search hspans hord
theorem sound_PruneExtent : PruneExtent.Goal :=
  Helpers_ModuloClass.generic_PruneExtent search hspans
theorem sound_LeftMultiplicity : LeftMultiplicity.Goal :=
  Helpers_ModuloClass.generic_LeftMultiplicity search hspans hord
theorem sound_RepeatedProbe : RepeatedProbe.Goal :=
  Helpers_ModuloClass.generic_RepeatedProbe search hspans hord
theorem sound_MaskedJoin : MaskedJoin.Goal :=
  Helpers_ModuloClass.generic_MaskedJoin search hspans hord
theorem sound_ResidueCache : ResidueCache.Goal := by
  intro u t x k ys
  have hmod : (x+k*(t+1))%(t+1) = x%(t+1) := by
    rw [Nat.mul_comm k (t+1), Nat.add_mul_mod_self_left]
  have spansEq : search.spans u t (x+k*(t+1)) = search.spans u t x := by
    rw [show search.spans u t (x+k*(t+1)) =
          (List.range ((u+(t+1)-1-(x+k*(t+1))%(t+1))/(t+1))).map
            (fun i => ((x+k*(t+1))%(t+1) + i*(t+1), (x+k*(t+1))%(t+1) + i*(t+1) + 1)) from rfl,
      show search.spans u t x =
          (List.range ((u+(t+1)-1-x%(t+1))/(t+1))).map
            (fun i => (x%(t+1) + i*(t+1), x%(t+1) + i*(t+1) + 1)) from rfl,
      hmod]
  show ((search.spans u t (x+k*(t+1))).flatMap (fun (a,b) => slice ys a b))
      = ((search.spans u t x).flatMap (fun (a,b) => slice ys a b))
  rw [spansEq]
theorem sound_ClassPopulation : ClassPopulation.Goal := by
  intro u t x
  have hsp : search.spans u t x =
      (List.range ((u+(t+1)-1-x%(t+1))/(t+1))).map
        (fun i => (x%(t+1)+i*(t+1), x%(t+1)+i*(t+1)+1)) := rfl
  rw [hsp, List.length_map, List.length_range]
  refine Nat.div_le_div_right ?_
  have heq : u + (t+1) - 1 = u + t := by omega
  rw [heq]
  exact Nat.sub_le (u+t) (x%(t+1))
end TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass