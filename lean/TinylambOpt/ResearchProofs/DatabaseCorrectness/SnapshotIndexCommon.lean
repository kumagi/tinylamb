import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex

/-!
Shared list facts for snapshot index proofs. The journalled changed positions
plus the physical index cover exactly the candidate row positions; deduplicating
and rechecking a snapshot heap may not change which positions survive.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
open TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys (Bytes encode body)

theorem nodup_eraseDups : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | x :: xs => by
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hx
        have hxbase := List.mem_eraseDups.mp hx
        have hxfalse := (List.mem_filter.mp hxbase).2
        simp at hxfalse
      · exact nodup_eraseDups (xs.filter fun b => !(b == x))
termination_by l => l.length
decreasing_by simp only [List.length_cons]; have := List.length_filter_le (fun b => !b == x) xs; omega

theorem nodup_filter : ∀ (l : List Nat) (p : Nat → Bool), l.Nodup → (l.filter p).Nodup := by
  intro l p h
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.filter]
      split
      · apply List.nodup_cons.mpr
        constructor
        · intro hx
          exact (List.nodup_cons.mp h).1 ((List.mem_filter.mp hx).1)
        · exact ih (List.nodup_cons.mp h).2
      · exact ih (List.nodup_cons.mp h).2

theorem lt_append_self : ∀ {α : Type} [LT α] [Std.Trichotomous (· < · : α → α → Prop)]
    (l r : List α), r ≠ [] → l < l ++ r := by
  intro α instLT instTrich l
  induction l with
  | nil =>
      intro r hr
      cases r with
      | nil => contradiction
      | cons x xs => simp
  | cons x xs ih =>
      intro r hr
      simp [List.cons_lt_cons_iff, ih r hr]

theorem append_lt_append_left : ∀ {α : Type} [LT α] [Std.Trichotomous (· < · : α → α → Prop)]
    (l a b : List α), a < b → l ++ a < l ++ b := by
  intro α instLT instTrich l
  induction l with
  | nil => intro a b h; exact h
  | cons x xs ih =>
      intro a b h
      simp [List.cons_lt_cons_iff, ih a b h]

theorem image_append (a b : Key) : image (a ++ b) = image a ++ image b := by
  simp [image, List.flatMap_append]

theorem image_nonempty : ∀ k : Key, k ≠ [] → image k ≠ [] := by
  intro k hk
  cases k with
  | nil => contradiction
  | cons x xs =>
      simp [image, encode]

theorem image_head_lt_ff : ∀ k : Key, k ≠ [] → image k < [255] := by
  intro k hk
  cases k with
  | nil => contradiction
  | cons x xs =>
      have h2255 : (2 : UInt8) < 255 := by decide
      simp [image, encode, List.cons_lt_cons_iff, h2255]

theorem u8_zero_lt_or_eq (a : UInt8) : 0 < a ∨ 0 = a := by
  by_cases h : (0 : UInt8) = a
  · exact Or.inr h
  · left
    have hn : a.toNat ≠ 0 := by
      intro hn
      apply h
      exact (UInt8.toNat.inj (by simpa [eq_comm] using hn)).symm
    have hpos : 0 < a.toNat := Nat.pos_of_ne_zero hn
    change UInt8.ofNat 0 < a
    rw [UInt8.ofNat_lt_iff (by decide : 0 < 256)]
    exact hpos

/-! The all-zero encoding of an empty field is the minimum among all fields. -/
theorem body_empty_le : ∀ v : Bytes, body [] ≤ body v := by
  intro v
  match v with
  | [] => simp
  | [a] => simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]
  | [a, b] => simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]
  | [a, b, c] => simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]
  | [a, b, c, d] => simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]
  | [a, b, c, d, e] => simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]
  | [a, b, c, d, e, f] => simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]
  | [a, b, c, d, e, f, g] => simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]
  | [a, b, c, d, e, f, g, h] => simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]
  | a :: b :: c :: d :: e :: f :: g :: h :: i :: rest =>
      simp [body, List.cons_le_cons_iff, u8_zero_lt_or_eq]

theorem body_eq_empty : ∀ v : Bytes, body v = body [] ↔ v = [] := by
  intro v
  match v with
  | [] => simp
  | [a] => simp [body]
  | [a, b] => simp [body]
  | [a, b, c] => simp [body]
  | [a, b, c, d] => simp [body]
  | [a, b, c, d, e] => simp [body]
  | [a, b, c, d, e, f] => simp [body]
  | [a, b, c, d, e, f, g] => simp [body]
  | [a, b, c, d, e, f, g, h] => simp [body]
  | a :: b :: c :: d :: e :: f :: g :: h :: i :: rest => simp [body]

theorem encode_empty_le (v : Bytes) : encode [] ≤ encode v := by
  change 2 :: body [] ≤ 2 :: body v
  simp [List.cons_le_cons_iff, body_empty_le]

theorem encode_eq_empty (v : Bytes) : encode v = encode [] ↔ v = [] := by
  change 2 :: body v = 2 :: body [] ↔ v = []
  simp [body_eq_empty]

theorem count_eraseDups (a : Nat) : ∀ l : List Nat,
    l.eraseDups.count a = if a ∈ l then 1 else 0
  | [] => by simp
  | x :: xs => by
      rw [List.eraseDups_cons]
      by_cases hxa : x == a
      · have hxeq : x = a := by simpa using hxa
        subst x
        have hnotmem : a ∉ xs.filter (fun b => !(b == a)) := by
          intro hm
          have hf := (List.mem_filter.mp hm).2
          simp at hf
        have hc := count_eraseDups a (xs.filter fun b => !(b == a))
        simp [hnotmem] at hc
        rw [List.count_cons]
        simp [hc]
      · have hxne : x ≠ a := by
          intro heq
          subst x
          simp at hxa
        have haxne : a ≠ x := by intro h; exact hxne h.symm
        have hmemfilter : a ∈ xs.filter (fun b => !(b == x)) ↔ a ∈ xs := by
          simp [haxne]
        have hc := count_eraseDups a (xs.filter fun b => !(b == x))
        simp [hmemfilter] at hc
        have hcfilter : (xs.filter fun b => !(b == x)).count a = xs.count a := by
          exact List.count_filter (by simp [haxne])
        rw [List.count_cons]
        simp [hxa, haxne, hcfilter, hc]
termination_by l => l.length
decreasing_by
  all_goals
    exact Nat.lt_of_le_of_lt (List.length_filter_le _ xs) (by simp)

end TinylambOpt.ResearchProofs.DatabaseCorrectness.SnapshotIndexCommon
