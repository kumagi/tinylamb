import TinylambOpt.ResearchGoals.ExecutionStrategies.BinaryText

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.BinaryText

open TinylambOpt.ResearchGoals.BinaryText
open List Std

/-!
# Binary collation proofs

Shared helpers first (`UInt8` order facts, `compare` ↔ `<`/`=` bridges for
`List UInt8`, `isPrefixOf` characterizations), then one `sound_<Goal>` theorem
per `*.Goal` declared in `TinylambOpt.ResearchGoals.BinaryText`.
-/

/-! ### `UInt8` arithmetic/order helpers -/

private theorem u8_toNat_add_one {c : UInt8} (hc : c.toNat < 255) :
    (c + 1).toNat = c.toNat + 1 := by
  rw [UInt8.toNat_add]
  have hone : (1 : UInt8).toNat = 1 := rfl
  rw [hone]
  have h256 : (2 : Nat) ^ 8 = 256 := rfl
  rw [h256]
  exact Nat.mod_eq_of_lt (by omega)

private theorem u8_succ_ne_self {c : UInt8} (hc : c ≠ 255) : c < c + 1 := by
  have hc' : c.toNat < 255 := by
    have h1 : c.toNat < 256 := UInt8.toNat_lt_size c
    have h2 : c.toNat ≠ 255 := by
      intro h
      exact hc ((UInt8.toNat_inj).mp (h : c.toNat = (255 : UInt8).toNat))
    omega
  rw [UInt8.lt_iff_toNat_lt, u8_toNat_add_one hc']
  omega

private theorem u8_255_not_lt (y : UInt8) : ¬ (255 : UInt8) < y := by
  rw [UInt8.lt_iff_toNat_lt]
  have h255 : (255 : UInt8).toNat = 255 := rfl
  rw [h255]
  have hsize : UInt8.size = 256 := rfl
  have h1 := UInt8.toNat_lt_size y
  rw [hsize] at h1
  omega

private theorem u8_zero_lt_of_ne {y : UInt8} (hy : y ≠ 0) : (0 : UInt8) < y := by
  rw [UInt8.lt_iff_toNat_lt]
  have h0 : (0 : UInt8).toNat = 0 := rfl
  rw [h0]
  have hy' : y.toNat ≠ 0 := by
    intro h
    apply hy
    exact (UInt8.toNat_inj).mp (h : y.toNat = (0 : UInt8).toNat)
  omega

private theorem u8_lt_succ_of_ne {c x : UInt8} (hc : c ≠ 255) (h : c < x) : c + 1 ≤ x := by
  have hc' : c.toNat < 255 := by
    have h1 : c.toNat < 256 := UInt8.toNat_lt_size c
    have h2 : c.toNat ≠ 255 := by
      intro hh
      exact hc ((UInt8.toNat_inj).mp (hh : c.toNat = (255 : UInt8).toNat))
    omega
  rw [UInt8.le_iff_toNat_le, u8_toNat_add_one hc']
  have hx : c.toNat < x.toNat := UInt8.lt_iff_toNat_lt.mp h
  omega

private theorem u8_beq_iff {a b : UInt8} : (a == b) = true ↔ a = b :=
  beq_iff_eq

/-! ### `compare` on `List UInt8` vs `<`/`=` -/

private theorem compareList_lt_iff {xs ys : Bytes} : compare xs ys = .lt ↔ xs < ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => simp
      | cons b bs => simp
  | cons a as ih =>
      cases ys with
      | nil => simp
      | cons b bs =>
          rw [List.compare_cons_cons, List.cons_lt_cons_iff]
          by_cases hab : a = b
          · subst hab
            rw [compare_self]
            simp only [lt_irrefl, false_or, true_and]
            exact ih
          · cases h : compare a b with
            | eq =>
                exfalso
                exact hab (compare_eq_iff_eq.mp h)
            | lt =>
                have halt : a < b := compare_eq_lt.mp h
                show (Ordering.lt : Ordering) = Ordering.lt ↔ a < b ∨ a = b ∧ as < bs
                simp [halt, hab]
            | gt =>
                have hgt : b < a := compare_eq_gt.mp h
                have hna : ¬ a < b := not_lt.mpr (le_of_lt hgt)
                have hne : a ≠ b := (ne_of_lt hgt).symm
                show (Ordering.gt : Ordering) = Ordering.lt ↔ a < b ∨ a = b ∧ as < bs
                simp [hna, hne]

private theorem compareList_gt_iff {xs ys : Bytes} : compare xs ys = .gt ↔ ys < xs := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => simp
      | cons b bs => simp
  | cons a as ih =>
      cases ys with
      | nil => simp
      | cons b bs =>
          rw [List.compare_cons_cons, List.cons_lt_cons_iff]
          by_cases hab : b = a
          · subst hab
            rw [compare_self]
            simp only [lt_irrefl, false_or, true_and]
            exact ih
          · cases h : compare a b with
            | eq =>
                exfalso
                exact hab (compare_eq_iff_eq.mp h).symm
            | lt =>
                have halt : a < b := compare_eq_lt.mp h
                have hnb : ¬ b < a := not_lt.mpr (le_of_lt halt)
                show (Ordering.lt : Ordering) = Ordering.gt ↔ b < a ∨ b = a ∧ bs < as
                simp [hnb, hab]
            | gt =>
                have hgt : b < a := compare_eq_gt.mp h
                show (Ordering.gt : Ordering) = Ordering.gt ↔ b < a ∨ b = a ∧ bs < as
                simp [hgt, hab]

private theorem compareList_eq_iff {xs ys : Bytes} : compare xs ys = .eq ↔ xs = ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys <;> simp
  | cons a as ih =>
      cases ys with
      | nil => simp
      | cons b bs =>
          rw [List.compare_cons_cons, Ordering.then_eq_eq]
          simp only [List.cons.injEq]
          constructor
          · rintro ⟨h1, h2⟩
            exact ⟨compare_eq_iff_eq.mp h1, ih.mp h2⟩
          · rintro ⟨hab, hbs⟩
            subst hab
            subst hbs
            exact ⟨compare_self, compare_self⟩

private theorem ordering_ne_gt_iff {o : Ordering} : o ≠ .gt ↔ o = .lt ∨ o = .eq := by
  cases o <;> simp

private theorem compareList_le_iff {xs ys : Bytes} : xs ≤ ys ↔ compare xs ys ≠ .gt := by
  have h := ordering_ne_gt_iff (o := compare xs ys)
  rw [h, compareList_lt_iff, compareList_eq_iff]
  exact List.le_iff_lt_or_eq

private theorem compareList_eq_self {xs : Bytes} : compare xs xs = .eq := compare_self

/-! ### `isPrefixOf` / `IsPrefix` helpers -/

private theorem isPrefixOf_iff {p w : Bytes} : p.isPrefixOf w = true ↔ p <+: w :=
  List.isPrefixOf_iff_prefix

private theorem drop_prefix_length {p t : Bytes} : (p ++ t).drop p.length = t :=
  List.drop_left

/-! ### Sorted-permutation uniqueness on `Bytes` -/

private theorem bytes_le_antisymm {a b : Bytes} (h₁ : a ≤ b) (h₂ : b ≤ a) : a = b :=
  List.le_antisymm h₁ h₂

/-- `mergeSort` output is `≤`-sorted (the default comparison is `decide (· ≤ ·)`). -/
private theorem mergeSort_pairwise_le {l : List Bytes} :
    l.mergeSort.Pairwise (· ≤ ·) := by
  have h := List.pairwise_mergeSort
    (le := fun a b : Bytes => decide (a ≤ b))
    (fun a b c h₁ h₂ =>
      decide_eq_true (List.le_trans (of_decide_eq_true h₁) (of_decide_eq_true h₂)))
    (fun a b => by
      rcases List.le_total a b with hab | hba
      · rw [Bool.or_eq_true]
        exact Or.inl (decide_eq_true hab)
      · rw [Bool.or_eq_true]
        exact Or.inr (decide_eq_true hba)) l
  exact h.imp fun {a b} hab => of_decide_eq_true hab

/-- Two `≤`-sorted permutations of the same list of `Bytes` are equal. -/
private theorem eq_of_pairwise_le {l₁ l₂ : List Bytes}
    (h₁ : l₁.Pairwise (· ≤ ·)) (h₂ : l₂.Pairwise (· ≤ ·)) (h : l₁ ~ l₂) : l₁ = l₂ :=
  List.Perm.eq_of_pairwise (fun _a _b _ _ h₁ h₂ => bytes_le_antisymm h₁ h₂) h₁ h₂ h

/-- Stripping the common prefix `p`, sorting the suffixes and restoring `p`
leaves `mergeSort` unchanged: both sides are sorted permutations of `xs`. -/
theorem sound_StripSort : StripSort.Goal := by
  intro p xs hpre
  -- `(xs.map (drop p.length)).map (p ++ ·) = xs`
  have hmap_aux : ∀ ys : List Bytes, (∀ y ∈ ys, p.isPrefixOf y = true) →
      ((ys.map (List.drop p.length)).map (p ++ ·)) = ys := by
    intro ys
    induction ys with
    | nil => intro _; rfl
    | cons y ys ih =>
        intro hys
        simp only [List.map_cons]
        congr 1
        · have hyp : p <+: y := isPrefixOf_iff.mp (hys y (by simp))
          obtain ⟨t, ht⟩ := hyp
          have hdrop : List.drop p.length y = t := by
            rw [← ht]
            exact List.drop_left
          rw [hdrop]
          exact ht
        · exact ih (fun z hz => hys z (List.mem_cons.mpr (Or.inr hz)))
  have hmap : ((xs.map (List.drop p.length)).map (p ++ ·)) = xs := hmap_aux xs hpre
  -- sortedness of the left side
  have hsuff : ((xs.map (List.drop p.length)).mergeSort).Pairwise (· ≤ ·) :=
    mergeSort_pairwise_le
  have hsortL : (((xs.map (List.drop p.length)).mergeSort).map (p ++ ·)).Pairwise (· ≤ ·) :=
    List.Pairwise.map _ (fun a b hab => (List.append_left_le_iff p).mpr hab) hsuff
  have hsortR : xs.mergeSort.Pairwise (· ≤ ·) := mergeSort_pairwise_le
  -- permutation of the two sides
  have hperm : (((xs.map (List.drop p.length)).mergeSort).map (p ++ ·)) ~ xs.mergeSort := by
    have p1 : (((xs.map (List.drop p.length)).mergeSort).map (p ++ ·)) ~
        ((xs.map (List.drop p.length)).map (p ++ ·)) :=
      List.Perm.map _ (List.mergeSort_perm _ (fun a b : Bytes => decide (a ≤ b)))
    have p2 : ((xs.map (List.drop p.length)).map (p ++ ·)) ~ xs :=
      List.Perm.of_eq hmap
    have p3 : xs ~ xs.mergeSort := (List.mergeSort_perm _ (fun a b : Bytes => decide (a ≤ b))).symm
    exact p1.trans (p2.trans p3)
  exact eq_of_pairwise_le hsortL hsortR hperm

/-- Membership facts about one-deletion signatures. -/
private theorem mem_deletes_tail {x : UInt8} {xs : Bytes} : xs ∈ deletes (x :: xs) := by
  rw [deletes, List.mem_map]
  refine ⟨0, by rw [List.mem_range]; simp, by simp⟩

private theorem mem_deletes_cons {x : UInt8} {xs t : Bytes} (h : t ∈ deletes xs) :
    x :: t ∈ deletes (x :: xs) := by
  rw [deletes, List.mem_map] at h ⊢
  obtain ⟨k, hk, rfl⟩ := h
  refine ⟨k + 1, ?_, ?_⟩
  · rw [List.mem_range] at hk ⊢
    simp only [List.length_cons] at hk ⊢
    omega
  · show (x :: xs).take (k + 1) ++ (x :: xs).drop (k + 1 + 1) =
        x :: (xs.take k ++ xs.drop (k + 1))
    rw [List.take_succ_cons, List.drop_succ_cons, List.cons_append]

/-- Deletion distance `≤ 1` implies the one-deletion signature sets intersect. -/
theorem sound_DeleteSignature : DeleteSignature.Goal := by
  intro a
  induction a with
  | nil =>
      intro b hnear
      have hb : b.length ≤ 1 := by
        have h2 : near [] b = decide (b.length ≤ 1) := rfl
        rw [h2] at hnear
        exact of_decide_eq_true hnear
      rw [List.any_eq_true]
      have hd : deletes ([] : Bytes) = [] := by simp [deletes]
      rw [hd]
      refine ⟨[], by simp, ?_⟩
      rw [List.contains_iff_mem]
      cases b with
      | nil => simp
      | cons c cs =>
          cases cs with
          | nil =>
              have hdc : deletes [c] = [[]] := by simp [deletes]
              rw [hdc]
              simp
          | cons c2 cs2 =>
              simp only [List.length_cons] at hb
              omega
  | cons x xs ih =>
      intro b hnear
      cases b with
      | nil =>
          have hxs : xs = [] := by
            have h2 : near (x :: xs) ([] : Bytes) = decide ((x :: xs).length ≤ 1) := rfl
            rw [h2] at hnear
            have h3 : (x :: xs).length ≤ 1 := of_decide_eq_true hnear
            simp only [List.length_cons] at h3
            have h4 : xs.length = 0 := by omega
            exact List.length_eq_zero_iff.mp h4
          subst hxs
          rw [List.any_eq_true]
          refine ⟨[], ?_, ?_⟩
          · have hd : deletes [x] = [[]] := by simp [deletes]
            rw [hd]
            simp
          · rw [List.contains_iff_mem]
            have hd : deletes ([] : Bytes) = [] := by simp [deletes]
            rw [hd]
            simp
      | cons y ys =>
          by_cases hxy : x = y
          · subst hxy
            have h2 : near (x :: xs) (x :: ys) = near xs ys := by simp [near]
            rw [h2] at hnear
            rw [List.any_eq_true]
            have hih := ih ys hnear
            rw [List.any_eq_true] at hih
            obtain ⟨s, hs1, hs2⟩ := hih
            rw [List.contains_iff_mem] at hs2
            refine ⟨x :: s, ?_, ?_⟩
            · rw [List.mem_cons]
              rcases List.mem_cons.mp hs1 with rfl | hs1'
              · exact Or.inl rfl
              · exact Or.inr (mem_deletes_cons hs1')
            · rw [List.contains_iff_mem, List.mem_cons]
              rcases List.mem_cons.mp hs2 with rfl | hs2'
              · exact Or.inl rfl
              · exact Or.inr (mem_deletes_cons hs2')
          · have hbeq : (x == y) ≠ true := by
              intro h
              exact hxy (u8_beq_iff.mp h)
            have h2 : near (x :: xs) (y :: ys) =
                (xs == ys || xs == y :: ys || x :: xs == ys) := by
              have h3 : near (x :: xs) (y :: ys) =
                  (if x == y then near xs ys else (xs == ys || xs == y :: ys || x :: xs == ys)) := rfl
              rw [h3]
              rw [ite_eq_right hbeq]
            rw [h2] at hnear
            rw [List.any_eq_true]
            rcases Bool.or_eq_true_iff.mp hnear with hbig | hlast
            · rcases Bool.or_eq_true_iff.mp hbig with h1 | h2a
              · -- xs == ys
                have hxy' : xs = ys := beq_iff_eq.mp h1
                subst hxy'
                refine ⟨xs, ?_, ?_⟩
                · exact List.mem_cons_of_mem _ (mem_deletes_tail)
                · rw [List.contains_iff_mem]
                  exact List.mem_cons_of_mem _ (mem_deletes_tail)
              · -- xs == y :: ys
                have hxy' : xs = y :: ys := beq_iff_eq.mp h2a
                subst hxy'
                refine ⟨y :: ys, ?_, ?_⟩
                · exact List.mem_cons_of_mem _ (mem_deletes_tail)
                · rw [List.contains_iff_mem]
                  exact List.mem_cons_self
            · -- x :: xs == ys
              have hxy' : ys = x :: xs := (beq_iff_eq.mp hlast).symm
              subst hxy'
              refine ⟨x :: xs, List.mem_cons_self, ?_⟩
              · rw [List.contains_iff_mem]
                exact List.mem_cons_of_mem _ (mem_deletes_tail)

end TinylambOpt.ResearchProofs.ExecutionStrategies.BinaryText
