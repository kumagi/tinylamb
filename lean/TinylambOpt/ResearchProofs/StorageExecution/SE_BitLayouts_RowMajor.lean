import Std
import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved (10 claims, including the following)

- `RowMajor.DictionaryProbe` — Read only lanes requested by a probe and translate them
  through a possibly short dictionary.
- `RowMajor.Lossless` — Read one lane from serialized bytes, including zero width.
- `RowMajor.MaskedPatch` — Replace one lane by clearing its mask and inserting a new
  value.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.BitLayouts.RowMajor
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.Compression.Wire
set_option maxHeartbeats 0

theorem h_foldl_one {β : Type} (f : β → Nat → β) (b : β) (a : Nat) :
    List.foldl f b [a] = f b a := rfl
theorem h_rb_test {w : Nat} (f : Nat → Nat) (x acc p : Nat) :
    ((List.range w).foldl (fun a b => if x.testBit b then a ||| 2 ^ f b else a) acc).testBit p =
      (acc.testBit p || (List.range w).any (fun b => decide (p = f b) && x.testBit b)) := by
  induction w generalizing acc with
  | zero => simp
  | succ w ih =>
    rw [List.range_succ, List.foldl_append, h_foldl_one, List.any_append, List.any_cons, List.any_nil,
      Bool.or_false]
    by_cases hx : x.testBit w
    · rw [if_pos hx, Nat.testBit_or, Nat.testBit_two_pow, ih, hx]
      by_cases hp : p = f w
      · subst hp; simp
      · have h2 : ¬ (f w = p) := fun hh => hp hh.symm
        simp [hp, h2]
    · rw [if_neg hx, ih]
      simp [hx]
theorem h_rb {w : Nat} (f : Nat → Nat) (x acc p : Nat) :
    ((List.range w).foldl (fun a b => if x.testBit b then a ||| 2 ^ f b else a) acc).testBit p ↔
      (acc.testBit p ∨ ∃ b < w, p = f b ∧ x.testBit b) := by
  rw [h_rb_test]
  have hor (a b : Bool) : (a || b) ↔ (a ∨ b) := by cases a <;> cases b <;> simp
  have hand (a b : Bool) : (a && b) ↔ (a ∧ b) := by cases a <;> cases b <;> simp
  rw [hor]
  constructor
  · rintro (h | h)
    · exact .inl h
    · rw [List.any_eq_true] at h
      obtain ⟨b, hb, t⟩ := h
      rw [List.mem_range] at hb
      simp only [hand] at t
      obtain ⟨t1, t2⟩ := t
      exact .inr ⟨b, hb, by simpa using t1, by simpa using t2⟩
  · rintro (h | ⟨b, hb, e, t⟩)
    · exact .inl h
    · exact .inr (by rw [List.any_eq_true]; exact ⟨b, by simpa using hb, by simp [e, t]⟩)
theorem h_imgF {l : Layout} (n0 w : Nat) : ∀ (ys : List Nat) (k acc p : Nat),
    (((ys.zipIdx k).foldl
        (fun a q => (List.range w).foldl (fun a b =>
          if q.fst.testBit b then a ||| 2 ^ (l.bit n0 w q.snd b) else a) a) acc).testBit p) ↔
      (acc.testBit p ∨ ∃ j < ys.length, ∃ b < w,
        p = l.bit n0 w (k + j) b ∧ (ys[j]?.getD 0).testBit b) := by
  intro ys
  induction ys with
  | nil => intro k acc p; simp
  | cons y ys ih =>
    intro k acc p
    rw [List.zipIdx_cons, List.foldl_cons, ih (k+1)]
    have step : (List.range w).foldl (fun a b =>
        if (y, k).fst.testBit b then a ||| 2 ^ (l.bit n0 w (y, k).snd b) else a) acc
      = (List.range w).foldl (fun a b => if y.testBit b then a ||| 2 ^ (l.bit n0 w k b) else a) acc := rfl
    rw [step, h_rb (f := fun b => l.bit n0 w k b)]
    refine Iff.intro ?_ ?_
    · intro h
      rcases h with (h|h) | ⟨j, hj, b, hb, e, t⟩
      · left; exact h
      · right
        obtain ⟨b, hb, e, t⟩ := h
        refine ⟨0, ?_, b, hb, ?_, ?_⟩ <;> simp_all
      · right
        have h2 : k + 1 + j = k + (j + 1) := by omega
        refine ⟨j+1, by simpa using hj, b, hb, ?_, by simpa using t⟩
        simpa [h2] using e
    · intro h
      rcases h with h | ⟨j, hj, b, hb, e, t⟩
      · left; left; exact h
      · rcases Nat.eq_zero_or_pos j with rfl | hjp
        · left; right
          refine ⟨b, hb, ?_, by simpa using t⟩
          simpa using e
        · right
          rcases j with _|j'
          · omega
          · simp at hjp e t ⊢
            have hsh : k + 1 + j' = k + (j' + 1) := by omega
            refine ⟨j', by simp only [List.length_cons] at hj; omega, b, hb, by simpa [hsh] using e, ?_⟩
            exact t

theorem h_img_full {l : Layout} (w : Nat) (xs : List Nat) (p : Nat) :
    (image l w xs).testBit p ↔
      ∃ j < xs.length, ∃ b < w, p = l.bit xs.length w j b ∧ (xs[j]?.getD 0).testBit b := by
  rw [show image l w xs =
      (xs.zipIdx).foldl (fun acc x_i =>
        (List.range w).foldl (fun a b =>
          if x_i.fst.testBit b then a ||| 2 ^ (l.bit xs.length w x_i.snd b) else a) acc) 0 from rfl]
  rw [h_imgF]
  simp only [Nat.zero_testBit, Nat.zero_add, Bool.false_eq_true, false_or]

theorem h_img_cell {l : Layout} (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w)
    (inj : ∀ j a, j < xs.length → a < w → l.bit xs.length w j a = l.bit xs.length w i b → j = i ∧ a = b) :
    (image l w xs).testBit (l.bit xs.length w i b) ↔ (xs[i]?.getD 0).testBit b := by
  rw [h_img_full]
  constructor
  · rintro ⟨j, hj, a, ha, e, t⟩
    obtain ⟨rfl, rfl⟩ := inj j a hj ha e.symm
    exact t
  · intro h
    exact ⟨i, hi, b, hb, rfl, h⟩

theorem h_setD_set {xs : List Nat} {i j : Nat} (x : Nat) (hj : j < xs.length) :
    (xs.set i x)[j]?.getD 0 = (if j = i then x else xs[j]?.getD 0) := by
  by_cases hij : j = i
  · subst hij
    rw [List.getElem?_set_self (h := hj)]
    simp
  · rw [List.getElem?_set_ne (Ne.symm hij)]
    simp [hij]

theorem h_sum_bits (x w : Nat) :
    ((List.range w).map (fun b => if x.testBit b then 2 ^ b else 0)).sum = x % 2 ^ w := by
  induction w with
  | zero => simp [Nat.mod_one]
  | succ w ih =>
    rw [List.range_succ, List.map_append, List.sum_append, ih, List.map_singleton]
    have hsing : [(if x.testBit w then 2 ^ w else 0 : Nat)].sum =
        (if x.testBit w then 2 ^ w else 0) := rfl
    rw [hsing]
    have h5 : 2 ^ (w + 1) = 2 ^ w * 2 := by rw [Nat.pow_succ, Nat.mul_two]
    have hd : 0 < 2 ^ w := Nat.pow_pos (by omega)
    by_cases hx : x.testBit w
    · rw [if_pos hx]
      have h1 : x / 2 ^ w % 2 = 1 := by
        have ht := Nat.testBit_eq_decide_div_mod_eq (x := x) (i := w)
        rw [hx] at ht
        simpa using ht
      have hq2 := Nat.mod_add_div (x / 2 ^ w) 2
      rw [h1] at hq2
      have hx2 : x = (x % 2 ^ w + 2 ^ w) + 2 ^ (w + 1) * (x / 2 ^ w / 2) := calc x
          = x % 2 ^ w + 2 ^ w * (x / 2 ^ w) := (Nat.mod_add_div x (2 ^ w)).symm
        _ = x % 2 ^ w + 2 ^ w * (1 + 2 * (x / 2 ^ w / 2)) :=
            congrArg (fun t => x % 2 ^ w + 2 ^ w * t) hq2.symm
        _ = x % 2 ^ w + (2 ^ w * 1 + 2 ^ w * (2 * (x / 2 ^ w / 2))) := by rw [Nat.mul_add]
        _ = (x % 2 ^ w + 2 ^ w) + 2 ^ (w + 1) * (x / 2 ^ w / 2) := by
          rw [Nat.mul_one, ← Nat.mul_assoc, ← h5, Nat.add_assoc]
      have hlt : x % 2 ^ w + 2 ^ w < 2 ^ (w + 1) := by
        have := Nat.mod_lt x hd
        rw [h5]
        omega
      have hstep1 : x % 2 ^ (w + 1) = (x % 2 ^ w + 2 ^ w) % 2 ^ (w + 1) := by
        rw [congrArg (fun t => t % 2 ^ (w + 1)) hx2, Nat.add_mul_mod_self_left]
      rw [hstep1, Nat.mod_eq_of_lt hlt]
    · rw [if_neg hx]
      have h1 : x / 2 ^ w % 2 = 0 := by
        rcases Nat.mod_two_eq_zero_or_one (x / 2 ^ w) with hz | ho
        · exact hz
        · have ht := Nat.testBit_eq_decide_div_mod_eq (x := x) (i := w)
          rw [ho] at ht
          have htrue : x.testBit w = true := ht.trans (by decide)
          exact absurd htrue (by simpa using hx)
      have hq2 := Nat.mod_add_div (x / 2 ^ w) 2
      rw [h1] at hq2
      have hx2 : x = x % 2 ^ w + 2 ^ (w + 1) * (x / 2 ^ w / 2) := calc x
          = x % 2 ^ w + 2 ^ w * (x / 2 ^ w) := (Nat.mod_add_div x (2 ^ w)).symm
        _ = x % 2 ^ w + 2 ^ w * (0 + 2 * (x / 2 ^ w / 2)) :=
            congrArg (fun t => x % 2 ^ w + 2 ^ w * t) hq2.symm
        _ = x % 2 ^ w + 2 ^ (w + 1) * (x / 2 ^ w / 2) := by
          rw [Nat.mul_add, Nat.mul_zero, Nat.zero_add, ← Nat.mul_assoc, ← h5]
      rw [Nat.add_zero]
      have hstep1 : x % 2 ^ (w + 1) = x % 2 ^ w % 2 ^ (w + 1) := by
        rw [congrArg (fun t => t % 2 ^ (w + 1)) hx2, Nat.add_mul_mod_self_left]
      rw [hstep1]
      have hlt2 : x % 2 ^ w < 2 ^ (w + 1) := by
        have h := Nat.mod_lt x hd
        rw [h5]
        omega
      exact (Nat.mod_eq_of_lt hlt2).symm

theorem h_pow256 (q : Nat) : 256 ^ q = 2 ^ (8 * q) := by
  induction q with
  | zero => rfl
  | succ q ih =>
    have h : 8 * (q + 1) = 8 * q + 8 := by omega
    rw [Nat.pow_succ, ih, h, Nat.pow_add]

theorem h_little_bit {v k p : Nat} (hp : p < 8 * k) :
    (atByte (little v k) (p / 8)).testBit (p % 8) = v.testBit p := by
  have hq : p / 8 < k := by omega
  have hb : atByte (little v k) (p/8) = (v / 256 ^ (p/8)) % 256 := by
    unfold atByte little
    rw [List.getElem?_map, List.getElem?_range (h := hq)]
    simp
  have h2 : (256 : Nat) = 2 ^ 8 := rfl
  rw [hb, h_pow256, h2, Nat.testBit_mod_two_pow, Nat.testBit_div_two_pow]
  have h8 : p % 8 < 8 := Nat.mod_lt _ (by omega)
  have h9 : p % 8 + 8 * (p / 8) = p := by rw [Nat.add_comm, Nat.div_add_mod]
  rw [h9]
  simp [h8]

theorem h_nodup_filter {α : Type} (xs : List α) (p : α → Bool) (hd : xs.Nodup) :
    (xs.filter p).Nodup := by
  induction xs with
  | nil => simp
  | cons a as ih =>
    rw [List.nodup_cons] at hd
    by_cases h : p a
    · rw [List.filter_cons_of_pos (pa := h), List.nodup_cons]
      refine ⟨?_, ih hd.2⟩
      intro hm
      exact hd.1 ((List.mem_filter.mp hm).1)
    · rw [List.filter_cons_of_neg (pa := by simpa using h)]
      exact ih hd.2

theorem h_nodup_erD2 {α : Type} [BEq α] [LawfulBEq α] :
    ∀ (xs : List α) (p : α → Bool), ((xs.filter p).eraseDups).Nodup := by
  intro xs
  induction xs with
  | nil => intro p; simp
  | cons a as ih =>
    intro p
    by_cases hp : p a
    · rw [List.filter_cons_of_pos (pa := hp), List.eraseDups_cons, List.filter_filter,
        List.nodup_cons]
      refine ⟨?_, ih _⟩
      intro h
      rw [List.mem_eraseDups] at h
      simp only [List.mem_filter] at h
      simp_all
    · rw [List.filter_cons_of_neg (pa := by simpa using hp)]
      exact ih p

theorem h_nodup_erD {α : Type} [BEq α] [LawfulBEq α] (xs : List α) : xs.eraseDups.Nodup := by
  have h := h_nodup_erD2 xs (fun _ => true)
  have e : xs.filter (fun _ => true) = xs :=
    List.filter_eq_self.mpr (by intro a _; simp)
  rwa [e] at h

theorem h_len_erD_le {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.length ≤ xs.length :=
  List.Nodup.length_le_of_subset (h_nodup_erD xs) (fun e he => List.mem_eraseDups.mp he)

theorem h_allEq_len {xs : List Nat} {c : Nat}
    (h : ∀ x ∈ xs, x = c) (hd : xs.Nodup) : xs.length ≤ 1 := by
  cases xs with
  | nil => simp
  | cons a as =>
    cases as with
    | nil => simp
    | cons b bs =>
      rw [List.nodup_cons] at hd
      exfalso
      refine hd.1 ?_
      rw [h a List.mem_cons_self, h b (List.mem_cons_of_mem a (List.mem_cons_self (a := b)))]
      exact List.mem_cons_self

theorem h_len_filter_add (p : Nat → Bool) : ∀ xs : List Nat,
    (xs.filter p).length + (xs.filter fun x => !p x).length = xs.length := by
  intro xs
  induction xs with
  | nil => simp
  | cons a as ih =>
    by_cases h : p a
    · rw [List.filter_cons_of_pos (pa := h),
        List.filter_cons_of_neg (pa := by simpa using h)]
      simp only [List.length_cons]
      omega
    · rw [List.filter_cons_of_neg (pa := h),
        List.filter_cons_of_pos (pa := by simpa using h)]
      simp only [List.length_cons]
      omega

theorem h_nodup_bound : ∀ (B : Nat), ∀ xs : List Nat,
    xs.Nodup → (∀ x ∈ xs, x < B) → xs.length ≤ B := by
  intro B
  induction B with
  | zero =>
    intro xs hd hlt
    rcases xs with _|⟨a, as⟩
    · simp
    · exact absurd (hlt a List.mem_cons_self) (Nat.not_lt_zero _)
  | succ C ih =>
    intro xs hd hlt
    have h1 := h_len_filter_add (fun x => x < C) xs
    have h2 : (xs.filter fun x => ! (x < C)).length ≤ 1 :=
      h_allEq_len (c := C) (fun x hx => by
        rcases List.mem_filter.mp hx with ⟨h1, h2⟩
        have hx' := hlt x h1
        have hnot : ¬ (x < C) := by
          intro h
          have hdt : decide (x < C) = true := by simp [h]
          rw [hdt] at h2
          exact absurd h2 (by decide)
        omega) (h_nodup_filter _ _ hd)
    have h3 : (xs.filter (fun x => x < C)).length ≤ C :=
      ih _ (h_nodup_filter _ _ hd) (fun x hx => by
        rcases List.mem_filter.mp hx with ⟨_, h2⟩
        exact of_decide_eq_true h2)
    rw [← h1]
    exact Nat.add_le_add h3 h2

theorem h_erD_bounded {B : Nat} (xs : List Nat) (h : ∀ x ∈ xs, x < B) :
    xs.eraseDups.length ≤ B :=
  h_nodup_bound _ _ (h_nodup_erD _) (fun x hx => h x (List.mem_eraseDups.mp hx))

theorem h_len_flat_le (W : Nat) (f : Nat → List Nat) (hf : ∀ i, (f i).length ≤ W) :
    ∀ rows : List Nat, (rows.flatMap f).length ≤ rows.length * W := by
  intro rows
  induction rows with
  | nil => simp
  | cons r rs ih =>
    rw [List.flatMap_cons, List.length_append, List.length_cons, Nat.add_mul, Nat.one_mul]
    have hr := hf r
    omega

theorem h_flatMap_erD_bound (W : Nat) (f : Nat → List Nat) (hf : ∀ i, (f i).length ≤ W) :
    ∀ rows : List Nat, ((rows.flatMap f).eraseDups).length ≤ rows.eraseDups.length * W := by
  intro rows
  have hsub : ∀ e ∈ (rows.flatMap f).eraseDups, e ∈ rows.eraseDups.flatMap f := by
    intro e he
    rw [List.mem_eraseDups] at he
    rw [List.mem_flatMap] at he ⊢
    obtain ⟨i, hi, hmem⟩ := he
    exact ⟨i, List.mem_eraseDups.mpr hi, hmem⟩
  calc ((rows.flatMap f).eraseDups).length
      ≤ (rows.eraseDups.flatMap f).length :=
        List.Nodup.length_le_of_subset (h_nodup_erD _) hsub
    _ ≤ rows.eraseDups.length * W := h_len_flat_le W f hf _

theorem h_nodup_range (n : Nat) : (List.range n).Nodup := List.nodup_range

theorem h_nodup_map_add_aux :
    ∀ (c : Nat) (l : List Nat), l.Nodup → (l.map fun x => c + x).Nodup := by
  intro c l
  induction l with
  | nil => intro _; exact List.nodup_nil
  | cons a as ih =>
    intro hd
    cases hd with
    | cons h1 h2 =>
      rw [List.map_cons]
      refine List.Pairwise.cons ?_ (ih h2)
      intro x hx hne
      obtain ⟨b, hb, e⟩ := List.mem_map.mp hx
      have e2 : c + b = x := e
      have h2 : c + a = x := hne
      exact h1 b hb (by omega : a = b)

theorem h_nodup_map_add {l : List Nat} (hd : l.Nodup) :
    (l.map fun x => c + x).Nodup := h_nodup_map_add_aux c l hd

theorem h_len_erD_map_add (l : List Nat) (c : Nat) :
    ((l.map fun x => c + x).eraseDups).length = l.eraseDups.length := by
  have ssub : (l.map fun x => c + x).eraseDups ⊆ l.eraseDups.map (fun x => c + x) := by
    intro z hz
    have hz' : z ∈ l.map (fun x => c + x) := List.mem_eraseDups.mp hz
    obtain ⟨y, hy, e⟩ := List.mem_map.mp hz'
    have e2 : c + y = z := e
    exact List.mem_map.mpr ⟨y, List.mem_eraseDups.mpr hy, e2⟩
  have tsub : l.eraseDups.map (fun x => c + x) ⊆ (l.map fun x => c + x).eraseDups := by
    intro z hz
    obtain ⟨y, hy, e⟩ := List.mem_map.mp hz
    have e2 : c + y = z := e
    exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨y, List.mem_eraseDups.mp hy, e2⟩)
  have tN : (l.eraseDups.map fun x => c + x).Nodup := h_nodup_map_add (h_nodup_erD _)
  refine Nat.le_antisymm ?_ ?_
  · exact Nat.le_trans (List.Nodup.length_le_of_subset (h_nodup_erD _) ssub)
      (Nat.le_of_eq (List.length_map _))
  · exact Nat.le_trans (Nat.le_of_eq (List.length_map _).symm)
      (List.Nodup.length_le_of_subset tN tsub)

theorem h_align_ge (m : Nat) : m ≤ align 8 m := by
  unfold align
  omega

theorem h_len_removeAll_le {xs ys : List Nat} : (xs.removeAll ys).length ≤ xs.length := by
  induction ys generalizing xs with
  | nil => simp
  | cons y ys ih =>
    rw [List.removeAll_cons]
    exact Nat.le_trans ih (List.length_filter_le _ _)

theorem h_nodup_map_sub : ∀ (lo : Nat) {xs : List Nat}, xs.Nodup →
    (∀ x ∈ xs, lo ≤ x) → (xs.map fun x => x - lo).Nodup := by
  intro lo xs
  induction xs with
  | nil => intro _ _; exact List.nodup_nil
  | cons a as ih =>
    intro hd hle
    cases hd with
    | cons h1 h2 =>
      rw [List.map_cons]
      refine List.Pairwise.cons ?_ (ih h2 (fun x hx => hle x (List.mem_cons_of_mem a hx)))
      intro x hx hne
      rw [List.mem_map] at hx
      obtain ⟨b, hb', e⟩ := hx
      have e1 : b - lo = x := e
      have e2 : a - lo = x := hne
      have hla := hle a List.mem_cons_self
      have hlb := hle b (List.mem_cons_of_mem a hb')
      exact h1 b hb' (by omega : a = b)

theorem h_interval_bound {xs : List Nat} {lo hi : Nat} (hd : xs.Nodup)
    (hb : ∀ x ∈ xs, lo ≤ x ∧ x ≤ hi) : xs.length ≤ hi - lo + 1 := by
  have hnd := h_nodup_map_sub lo hd (fun x hx => (hb x hx).1)
  have hlt : ∀ y ∈ xs.map fun x => x - lo, y < hi - lo + 1 := by
    intro y hy
    rw [List.mem_map] at hy
    obtain ⟨b, hb', e⟩ := hy
    have e2 : b - lo = y := e
    rcases hb b hb' with ⟨hlb, hub⟩
    omega
  have := h_nodup_bound _ _ hnd hlt
  rwa [List.length_map] at this
theorem h_rm_test {l : Layout} {n0 w i p : Nat} :
    (rowMask l n0 w i).testBit p ↔ (∃ b < w, p = l.bit n0 w i b) := by
  unfold rowMask lanes
  have key : ∀ (k acc : Nat),
      ((List.range k).foldl (fun a b => a ||| 2 ^ (l.bit n0 w i b)) acc).testBit p ↔
        (acc.testBit p ∨ ∃ b < k, p = l.bit n0 w i b) := by
    intro k
    induction k with
    | zero => intro acc; simp
    | succ k ih =>
      intro acc
      rw [List.range_succ, List.foldl_append]
      have hsing : List.foldl (fun a b => a ||| 2 ^ l.bit n0 w i b)
          ((List.range k).foldl (fun a b => a ||| 2 ^ l.bit n0 w i b) acc) [k] =
          (List.range k).foldl (fun a b => a ||| 2 ^ l.bit n0 w i b) acc ||| 2 ^ l.bit n0 w i k := by rfl
      rw [hsing, Nat.testBit_or, Nat.testBit_two_pow]
      rw [Bool.or_eq_true_iff, decide_eq_true_eq, ih, or_assoc]
      constructor
      · rintro (h | ⟨b, hb, e⟩ | h)
        · exact .inl h
        · exact .inr ⟨b, Nat.lt_trans hb (Nat.lt_succ_self k), e⟩
        · exact .inr ⟨k, Nat.lt_succ_self k, h.symm⟩
      · rintro (h | ⟨b, hb, e⟩)
        · exact .inl h
        · rcases Nat.lt_or_ge b k with hbk | hbk
          · exact .inr (Or.inl ⟨b, hbk, e⟩)
          · have hbkeq : b = k := by omega
            subst hbkeq
            exact .inr (Or.inr e.symm)
  have := key w 0
  simpa using this

theorem h_rb0_test {l : Layout} {n0 w i x p : Nat} :
    (rowBits l n0 w i x).testBit p ↔ (∃ b < w, p = l.bit n0 w i b ∧ x.testBit b) := by
  unfold rowBits lanes
  rw [h_rb (f := fun b => l.bit n0 w i b)]
  simp

theorem h_peel_range_map (k : Nat) (f : Nat → Nat) :
    (List.range (k+1)).map f = f 0 :: (List.range k).map (fun i => f (i+1)) := by
  induction k with
  | zero => rfl
  | succ k ih =>
    rw [List.range_succ, List.map_append, ih, List.range_succ, List.map_append]
    rfl

theorem h_getD_range_map (w : Nat) (xs : List Nat) :
    (List.range xs.length).map (fun i => (xs[i]?.getD 0) % 2 ^ w) =
      xs.map (fun x => x % 2 ^ w) := by
  induction xs with
  | nil => rfl
  | cons a as ih =>
    rw [List.length_cons, h_peel_range_map, List.map_cons]
    congr 1

theorem h_read_cell {l : Layout} (w : Nat) (xs : List Nat)
    (capb : ∀ n j b, j < n → b < w → l.bit n w j b < l.capacity n w)
    (inj : ∀ n j k a b, j < n → k < n → a < w → b < w → l.bit n w j a = l.bit n w k b → j = k ∧ a = b)
    (j b : Nat) (hj : j < xs.length) (hb : b < w) :
    (atByte (file l w xs) (l.bit xs.length w j b / 8)).testBit (l.bit xs.length w j b % 8) =
      (xs[j]?.getD 0).testBit b := by
  have plt : l.bit xs.length w j b < 8 * byteCount (l.capacity xs.length w) := by
    have hc := capb xs.length j b hj hb
    have e := Nat.div_add_mod (l.capacity xs.length w) 8
    unfold byteCount
    omega
  rw [show file l w xs = little (image l w xs) (byteCount (l.capacity xs.length w)) from rfl]
  rw [h_little_bit plt]
  exact Bool.eq_iff_iff.mpr (h_img_cell w xs j b hj hb (fun j' a hj' ha h => inj _ j' j a b hj' hj ha hb h))

theorem h_fetch_eq {l : Layout} (w : Nat) (xs : List Nat) (i : Nat) (hi : i < xs.length)
    (capb : ∀ n j b, j < n → b < w → l.bit n w j b < l.capacity n w)
    (inj : ∀ n j k a b, j < n → k < n → a < w → b < w → l.bit n w j a = l.bit n w k b → j = k ∧ a = b) :
    fetch l xs.length w i (atByte (file l w xs)) = xs[i]?.getD 0 % 2 ^ w := by
  have hcell : ∀ b < w,
      (atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit (l.bit xs.length w i b % 8) =
        (xs[i]?.getD 0).testBit b := fun b hb => h_read_cell w xs capb inj i b hi hb
  unfold fetch lanes
  rw [← h_sum_bits]
  congr 1
  apply List.map_congr_left
  intro b hb
  simp only [List.mem_range] at hb
  show (if (atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit
      (l.bit xs.length w i b % 8) then (2:Nat) ^ b else 0) = _
  rw [hcell b hb]

theorem h_lossless (l : Layout)
    (capb : ∀ n w i b, i < n → b < w → l.bit n w i b < l.capacity n w)
    (inj : ∀ n w i j a b, i < n → j < n → a < w → b < w → l.bit n w i a = l.bit n w j b → i = j ∧ a = b) :
    lossless l := by
  intro w xs i hi
  exact h_fetch_eq w xs i hi
    (fun n j hin hbn => capb n w j hin hbn)
    (fun n j k a b hjn hkn hab hbn => inj n w j k a b hjn hkn hab hbn)

theorem h_nonoverlap (l : Layout)
    (inj : ∀ n w i j a b, i < n → j < n → a < w → b < w → l.bit n w i a = l.bit n w j b → i = j ∧ a = b) :
    nonoverlap l := by
  intro n w i j a b hi hj ha hb
  exact ⟨fun h => inj n w i j a b hi hj ha hb h, fun ⟨rfl, rfl⟩ => rfl⟩

theorem h_filter_range_congr {n : Nat} {p q : Nat → Bool}
  (h : ∀ i ∈ List.range n, p i = q i) :
    (List.range n).filter p = (List.range n).filter q := by
  induction n with
  | zero => rfl
  | succ m ih =>
    rw [List.range_succ, List.filter_append, List.filter_append,
      ih (fun i hi => h i (List.mem_range.mpr (Nat.lt_succ_of_lt (List.mem_range.mp hi))))]
    congr 1
    have hc := h m (List.mem_range.mpr (Nat.lt_succ_self m))
    rcases hpm : p m with _ | _ <;> rcases hqm : q m with _ | _ <;>
      simp_all

theorem h_planePredicate (l : Layout)
    (capb : ∀ n w i b, i < n → b < w → l.bit n w i b < l.capacity n w)
    (inj : ∀ n w i j a b, i < n → j < n → a < w → b < w → l.bit n w i a = l.bit n w j b → i = j ∧ a = b) :
    planePredicate l := by
  intro w xs b hb
  unfold planeRows
  have hc : ∀ i ∈ List.range xs.length,
      ((atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit (l.bit xs.length w i b % 8) =
        (xs[i]?.getD 0).testBit b) := by
    intro i hi
    exact h_read_cell w xs (fun n j b' hin hbn => capb n w j b' hin hbn)
      (fun n j k a c hj' hk' ha' hc' heq => inj n w j k a c hj' hk' ha' hc' heq) i b
      (List.mem_range.mp hi) hb
  exact h_filter_range_congr hc

theorem h_transcode (l : Layout)
    (capb : ∀ n w i b, i < n → b < w → l.bit n w i b < l.capacity n w)
    (inj : ∀ n w i j a b, i < n → j < n → a < w → b < w → l.bit n w i a = l.bit n w j b → i = j ∧ a = b) :
    transcode l := by
  intro w xs
  have hf : ∀ i ∈ List.range xs.length,
      fetch l xs.length w i (atByte (file l w xs)) = (xs[i]?.getD 0) % 2 ^ w := by
    intro i hi
    exact h_fetch_eq w xs i (List.mem_range.mp hi)
      (fun n j b' hin hbn => capb n w j b' hin hbn)
      (fun n j k a b hj' hk' ha' hb' heq => inj n w j k a b hj' hk' ha' hb' heq)
  rw [List.map_congr_left hf]
  rw [h_getD_range_map]
  have hw : ∀ ys : List Nat, word w (ys.map (fun x => x % 2 ^ w)) = word w ys := by
    intro ys
    induction ys with
    | nil => rfl
    | cons a as ih =>
      rw [List.map_cons]
      show (a % 2 ^ w) % 2 ^ w + 2 ^ w * word w (as.map (fun x => x % 2 ^ w)) = _
      rw [Nat.mod_mod, ih]
      rfl
  rw [pack, pack, hw, List.length_map]

theorem h_sparseGather (l : Layout) : sparseGather l := by
  intro n w rows
  induction rows with
  | nil => intro a b h; rfl
  | cons r rs ih =>
    intro a b h
    rw [List.map_cons, ih a b (fun j hj => h j (by
      unfold addressSet
      rw [List.flatMap_cons, List.mem_eraseDups, List.mem_append]
      exact Or.inr (List.mem_eraseDups.mp hj)))]
    congr 1
    · unfold fetch lanes
      show ((List.range w).map (fun c =>
          if (a (l.bit n w r c / 8)).testBit (l.bit n w r c % 8) then (2:Nat) ^ c else 0)).sum
          = ((List.range w).map (fun c =>
          if (b (l.bit n w r c / 8)).testBit (l.bit n w r c % 8) then (2:Nat) ^ c else 0)).sum
      congr 1
      apply List.map_congr_left
      intro b' hb'
      simp only [List.mem_range] at hb'
      show (if (a (l.bit n w r b' / 8)).testBit (l.bit n w r b' % 8)
          then (2:Nat) ^ b' else 0) = _
      rw [h _ (by
        unfold addressSet
        rw [List.mem_eraseDups, List.mem_flatMap]
        refine ⟨r, List.mem_cons_self, ?_⟩
        unfold footprint lanes
        rw [List.mem_eraseDups, List.mem_map]
        exact ⟨b', List.mem_range.mpr hb', rfl⟩)]

theorem h_pageBudget (l : Layout)
    (capb : ∀ n w i b, i < n → b < w → l.bit n w i b < l.capacity n w) : pageBudget l := by
  intro n w rows hrows
  rw [Nat.le_min]
  constructor
  · have hbdd : ∀ e ∈ rows.flatMap (footprint l n w), e < byteCount (l.capacity n w) := by
      intro e he
      rw [List.mem_flatMap] at he
      obtain ⟨i, hi, hfp⟩ := he
      unfold footprint lanes at hfp
      rw [List.mem_eraseDups, List.mem_map] at hfp
      obtain ⟨b, hb, rfl⟩ := hfp
      have hc := capb n w i b (hrows i hi) (List.mem_range.mp hb)
      have e2 := Nat.div_add_mod (l.capacity n w) 8
      unfold byteCount
      omega
    rw [addressSet]
    exact h_erD_bounded _ hbdd
  · unfold addressSet
    have hle : ∀ i, (footprint l n w i).length ≤ w := by
      intro i
      unfold footprint lanes
      calc (((List.range w).map (fun b => l.bit n w i b / 8)).eraseDups).length
          ≤ ((List.range w).map (fun b => l.bit n w i b / 8)).length := h_len_erD_le _
        _ = w := by simp
    exact h_flatMap_erD_bound w _ hle _

theorem h_maskedPatch (l : Layout)
    (inj : ∀ n w i j a b, i < n → j < n → a < w → b < w → l.bit n w i a = l.bit n w j b → i = j ∧ a = b) :
    maskedPatch l := by
  intro w xs i x hi
  refine Nat.eq_of_testBit_eq (fun p => ?_)
  have lhs : (image l w (xs.set i x)).testBit p ↔
      ∃ j < xs.length, ∃ b < w, p = l.bit xs.length w j b ∧
        (if j = i then x else (xs[j]?.getD 0)).testBit b := by
    rw [h_img_full (w := w) (xs := xs.set i x) (p := p)]
    refine ⟨fun ⟨j, hj, b, hb, e, t⟩ => ⟨j, by simpa [List.length_set] using hj, b, hb,
          by simpa [List.length_set] using e, ?_⟩,
      fun ⟨j, hj, b, hb, e, t⟩ => ⟨j, by simpa [List.length_set] using hj, b, hb,
          by simpa [List.length_set] using e, ?_⟩⟩
    · by_cases h : j = i
      · subst h
        simpa [h_setD_set x (by simpa [List.length_set] using hj : j < xs.length)] using t
      · rw [h_setD_set x (by simpa [List.length_set] using hj : j < xs.length)] at t
        exact t
    · by_cases h : j = i
      · subst h
        simpa [h_setD_set x (by simpa [List.length_set] using hj : j < xs.length)] using t
      · simpa [h_setD_set x (by simpa [List.length_set] using hj : j < xs.length), h] using t
  rw [Bool.eq_iff_iff, lhs]
  have orig : (image l w xs).testBit p ↔ ∃ j < xs.length, ∃ b < w,
      p = l.bit xs.length w j b ∧ (xs[j]?.getD 0).testBit b := h_img_full w xs p
  have hxor : ∀ a b : Bool, ((a ^^ b) = true) ↔ (a ↔ ¬b) := by decide
  rw [Nat.testBit_or, Bool.or_eq_true_iff, h_rb0_test, Nat.testBit_xor, hxor,
    Nat.testBit_and, Bool.and_eq_true_iff, h_rm_test, orig]
  refine ⟨?_, ?_⟩
  · rintro ⟨j, hj, b, hb, e, t⟩
    by_cases hij : j = i
    · subst hij
      exact .inr ⟨b, hb, e, by simpa using t⟩
    · refine .inl ⟨fun _ h => absurd h.2
          (fun ⟨b', hb', e'⟩ =>
            hij ((inj xs.length w i j b' b hi hj hb' hb (e'.symm.trans e)).1.symm)),
        fun _ => ⟨j, hj, b, hb, e, by simpa [hij] using t⟩⟩
  · rintro (h | ⟨b, hb, e, t⟩)
    · by_cases hmo : ∃ b' < w, p = l.bit xs.length w i b'
      · by_cases hto : ∃ j < xs.length, ∃ b' < w,
            p = l.bit xs.length w j b' ∧ (xs[j]?.getD 0).testBit b'
        · exact absurd ⟨hto, hmo⟩ (h.mp hto)
        · exact absurd (h.mpr fun hh => hto hh.1) hto
      · by_cases hto : ∃ j < xs.length, ∃ b' < w,
            p = l.bit xs.length w j b' ∧ (xs[j]?.getD 0).testBit b'
        · obtain ⟨j, hj, b', hb', e', t'⟩ := hto
          by_cases hij : j = i
          · subst hij
            exact absurd ⟨b', hb', e'⟩ hmo
          · exact ⟨j, hj, b', hb', e', by simpa [if_neg hij] using t'⟩
        · exact absurd (h.mpr fun hh => hto hh.1) hto
    · exact ⟨i, hi, b, hb, e, by simpa using t⟩

theorem h_dictionaryProbe (l : Layout)
    (capb : ∀ n w i b, i < n → b < w → l.bit n w i b < l.capacity n w)
    (inj : ∀ n w i j a b, i < n → j < n → a < w → b < w → l.bit n w i a = l.bit n w j b → i = j ∧ a = b) :
    dictionaryProbe l := by
  intro w xs dict keys hkeys
  refine List.map_congr_left ?_
  intro i hi
  exact congrArg (fun v => dict[v]?)
    (h_fetch_eq w xs i (hkeys i hi)
      (fun n j b' hin hbn => capb n w j b' hin hbn)
      (fun n j k a b hj' hk' ha' hb' heq => inj n w j k a b hj' hk' ha' hb' heq))
theorem hbit : ∀ n w i b, RowMajor.layout.bit n w i b = i * w + b := fun _ _ _ _ => rfl
theorem hcap : ∀ n w, RowMajor.layout.capacity n w = n * w := fun _ _ => rfl

theorem hinj : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    RowMajor.layout.bit n w i a = RowMajor.layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb heq
  rw [hbit, hbit] at heq
  rcases Nat.lt_trichotomy i j with hlt | h | hgt
  · have h2 : i * w + w ≤ j * w := by
      have h1 : i * w + w = (i + 1) * w := by
        rw [Nat.add_mul, Nat.one_mul]
      rw [h1]
      exact Nat.mul_le_mul_right w (by omega : i + 1 ≤ j)
    exact absurd (by omega : False) False.elim
  · subst h
    exact ⟨rfl, Nat.add_left_cancel heq⟩
  · have h2 : j * w + w ≤ i * w := by
      have h1 : j * w + w = (j + 1) * w := by
        rw [Nat.add_mul, Nat.one_mul]
      rw [h1]
      exact Nat.mul_le_mul_right w (by omega : j + 1 ≤ i)
    exact absurd (by omega : False) False.elim

theorem hcapb : ∀ n w i b, i < n → b < w →
    RowMajor.layout.bit n w i b < RowMajor.layout.capacity n w := by
  intro n w i b hi hb
  rw [hbit, hcap]
  obtain ⟨k, rfl⟩ : ∃ k, n = k + 1 := ⟨n - 1, by omega⟩
  obtain ⟨m, rfl⟩ : ∃ m, w = m + 1 := ⟨w - 1, by omega⟩
  have hA : i * (m + 1) ≤ k * (m + 1) :=
    Nat.mul_le_mul_right (m + 1) (by omega : i ≤ k)
  rw [Nat.add_mul, Nat.one_mul]
  have h2 : b ≤ m := by omega
  omega

theorem h_fp_mem {n w i m : Nat} :
    m ∈ footprint RowMajor.layout n w i ↔ ∃ b < w, m = (i * w + b) / 8 := by
  unfold footprint lanes
  simp only [List.mem_eraseDups, List.mem_map, List.mem_range]
  refine ⟨fun ⟨b, hb, e⟩ => ⟨b, hb, e.symm⟩, fun ⟨b, hb, e⟩ => ⟨b, hb, e.symm⟩⟩

/-! The ten goals. -/

theorem sound_Lossless : RowMajor.Lossless.Goal :=
  h_lossless RowMajor.layout hcapb hinj

theorem sound_Nonoverlap : RowMajor.Nonoverlap.Goal :=
  h_nonoverlap RowMajor.layout hinj

theorem sound_MaskedPatch : RowMajor.MaskedPatch.Goal :=
  h_maskedPatch RowMajor.layout hinj

theorem sound_PlanePredicate : RowMajor.PlanePredicate.Goal :=
  h_planePredicate RowMajor.layout hcapb hinj

theorem sound_SparseGather : RowMajor.SparseGather.Goal :=
  h_sparseGather RowMajor.layout

theorem sound_Transcode : RowMajor.Transcode.Goal :=
  h_transcode RowMajor.layout hcapb hinj

theorem sound_DictionaryProbe : RowMajor.DictionaryProbe.Goal :=
  h_dictionaryProbe RowMajor.layout hcapb hinj

theorem sound_PageBudget : RowMajor.PageBudget.Goal :=
  h_pageBudget RowMajor.layout hcapb

theorem sound_SharedEdgeByte : RowMajor.SharedEdgeByte.Goal := by
  intro n w i hw
  refine Bool.eq_iff_iff.mpr ?_
  rw [List.any_eq_true]
  constructor
  · rintro ⟨m, hm1, hm2⟩
    rw [List.contains_eq_mem, decide_eq_true_eq] at hm2
    rw [h_fp_mem] at hm1 hm2
    obtain ⟨b1, hb1, e1⟩ := hm1
    obtain ⟨b2, hb2, e2⟩ := hm2
    rcases Nat.eq_zero_or_pos ((i+1)*w % 8) with hz | hz
    · exfalso
      have hw2 : (i + 1) * w = i * w + w := by
        rw [Nat.add_mul, Nat.one_mul]
      have dq := Nat.div_add_mod ((i+1)*w) 8
      have hle : (i * w + b1) / 8 ≤ ((i+1)*w - 1) / 8 :=
        Nat.div_le_div_right (by omega)
      have hge : ((i+1)*w) / 8 ≤ ((i+1)*w + b2) / 8 :=
        Nat.div_le_div_right (by omega)
      have hlt : ((i+1)*w - 1) / 8 < ((i+1)*w) / 8 := by
        have h1 : 0 < (i+1)*w := by
          rcases Nat.eq_zero_or_pos w with hz' | hz'
          · omega
          · exact Nat.mul_pos (by omega) hz'
        have h2 : ((i+1)*w - 1) / 8 = ((i+1)*w) / 8 - 1 := by
          apply Nat.div_eq_of_lt_le <;> omega
        omega
      omega
    · exact by
        rw [decide_eq_true_eq]
        exact Nat.ne_of_gt hz
  · intro h
    refine ⟨(i*w + (w-1)) / 8, (h_fp_mem (m := (i*w + (w-1)) / 8)).mpr ⟨w-1, by omega, rfl⟩, ?_⟩
    rw [List.contains_eq_mem, decide_eq_true_eq, h_fp_mem]
    refine ⟨0, by omega, ?_⟩
    have h1 : 1 ≤ w := hw
    have heq : i * w + (w - 1) = (i+1)*w - 1 := by
      rw [show (i+1)*w = i*w + w from by rw [Nat.add_mul, Nat.one_mul]]
      omega
    rw [heq]
    rw [decide_eq_true_eq] at h
    have dq := Nat.div_add_mod ((i+1)*w) 8
    have h8 : ((i+1)*w) % 8 < 8 := Nat.mod_lt _ (by decide : (0:Nat) < 8)
    rw [show ((i + 1) * w + 0) = (i + 1) * w from rfl]
    omega

theorem sound_PointSpan : RowMajor.PointSpan.Goal := by
  intro n w i
  have key : footprint RowMajor.layout n w i =
      ((List.range w).map (fun b => (i * w + b) / 8)).eraseDups := by
    unfold footprint lanes RowMajor.layout; rfl
  have h1 : ∀ e ∈ ((List.range w).map (fun b => (i * w + b) / 8)).eraseDups,
      (i * w) / 8 ≤ e ∧ e ≤ (i * w + (w - 1)) / 8 := by
    intro e he
    simp only [List.mem_eraseDups, List.mem_map, List.mem_range] at he
    obtain ⟨b, hb, rfl⟩ := he
    exact ⟨Nat.div_le_div_right (by omega),
      Nat.div_le_div_right (by have := Nat.le_sub_one_of_lt hb; omega)⟩
  have hnd : ((List.range w).map (fun b => (i * w + b) / 8)).eraseDups.Nodup :=
    h_nodup_erD _
  have h2 := h_interval_bound hnd h1
  have dq := Nat.div_add_mod (i * w) 8
  have hlt : (i * w) % 8 < 8 := Nat.mod_lt _ (by decide : (0:Nat) < 8)
  have hb : (i * w + (w - 1)) / 8 - (i * w) / 8 + 1 ≤ (w + 14) / 8 := by omega
  rw [key]
  unfold byteCount
  omega

end TinylambOpt.ResearchProofs.StorageExecution.BitLayouts.RowMajor
