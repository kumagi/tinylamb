import TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys
import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex

/-!
Block-wise order lemmas for the memcomparable binary VARCHAR encoding used by
compound index bounds. Splitting a field into eight-byte blocks with a marker
(9 for continuation blocks, final length otherwise) makes the encoded bytes an
order embedding of the raw bytes, and concatenating encoded fields preserves
prefix order. These facts justify translating encoded inclusive range tests
back to decoded tuple-prefix bounds.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharOrderLemmas

open TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys
open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex

theorem lex_eq_prefix : ∀ {α : Type} [LT α] [Std.Trichotomous (· < · : α → α → Prop)]
    [Std.Asymm (· < · : α → α → Prop)] (n : Nat) (l₁ l₂ : List α),
    n ≤ l₁.length → n ≤ l₂.length →
    (l₁ ≤ l₂ ↔ l₁.take n < l₂.take n ∨ (l₁.take n = l₂.take n ∧ l₁.drop n ≤ l₂.drop n)) := by
  intro α instLT instTrich instAsymm n
  induction n with
  | zero =>
      intro l₁ l₂ h₁ h₂
      simp
  | succ n ih =>
      intro l₁ l₂ h₁ h₂
      cases l₁ with
      | nil => simp at h₁
      | cons a as =>
          cases l₂ with
          | nil => simp at h₂
          | cons b bs =>
              simp only [List.length_cons, Nat.succ_le_succ_iff] at h₁ h₂
              have hi := ih as bs h₁ h₂
              simp only [List.take_succ_cons, List.drop_succ_cons]
              rw [List.cons_le_cons_iff]
              rw [List.cons_lt_cons_iff]
              rw [hi]
              constructor
              · intro h
                rcases h with h | ⟨rfl, h⟩
                · exact Or.inl (Or.inl h)
                · rcases h with hlt | ⟨heq, hle⟩
                  · exact Or.inl (Or.inr ⟨rfl, hlt⟩)
                  · exact Or.inr ⟨by simp [heq], hle⟩
              · intro h
                rcases h with h | ⟨heqcons, hle⟩
                · rcases h with h | ⟨rfl, hlt⟩
                  · exact Or.inl h
                  · exact Or.inr ⟨rfl, Or.inl hlt⟩
                · injection heqcons with hab htake
                  subst b
                  exact Or.inr ⟨rfl, Or.inr ⟨htake, hle⟩⟩

theorem append_lt_append_eq_len : ∀ {α : Type} [LT α] [Std.Trichotomous (· < · : α → α → Prop)]
    [Std.Asymm (· < · : α → α → Prop)] (l₁ l₂ : List α), l₁.length = l₂.length →
    ∀ a b : List α, l₁ ++ a < l₂ ++ b ↔ l₁ < l₂ ∨ (l₁ = l₂ ∧ a < b) := by
  intro α instLT instTrich instAsymm l₁
  induction l₁ with
  | nil =>
      intro l₂ hlen a b
      cases l₂ with
      | nil => simp
      | cons x xs => simp at hlen
  | cons x xs ih =>
      intro l₂ hlen a b
      cases l₂ with
      | nil => simp at hlen
      | cons y ys =>
          simp only [List.length_cons, Nat.succ.injEq] at hlen
          have hi := ih ys hlen a b
          simp only [List.cons_append, List.cons_lt_cons_iff]
          rw [hi]
          constructor
          · intro h
            rcases h with h | ⟨hxy, htail⟩
            · exact Or.inl (Or.inl h)
            · subst y
              rcases htail with hlt | ⟨heq, hlt'⟩
              · exact Or.inl (Or.inr ⟨rfl, hlt⟩)
              · exact Or.inr ⟨by simp [heq], hlt'⟩
          · intro h
            rcases h with h | ⟨heq, hab⟩
            · rcases h with h | ⟨hxy, hlt⟩
              · exact Or.inl h
              · subst y
                exact Or.inr ⟨rfl, Or.inl hlt⟩
            · injection heq with hxy hys
              subst y
              exact Or.inr ⟨rfl, Or.inr ⟨hys, hab⟩⟩

theorem append_le_append_eq_len : ∀ {α : Type} [LT α] [Std.Trichotomous (· < · : α → α → Prop)]
    [Std.Asymm (· < · : α → α → Prop)] (l₁ l₂ : List α), l₁.length = l₂.length →
    ∀ a b : List α, l₁ ++ a ≤ l₂ ++ b ↔ l₁ < l₂ ∨ (l₁ = l₂ ∧ a ≤ b) := by
  intro α instLT instTrich instAsymm l₁
  induction l₁ with
  | nil =>
      intro l₂ hlen a b
      cases l₂ with
      | nil => simp
      | cons x xs => simp at hlen
  | cons x xs ih =>
      intro l₂ hlen a b
      cases l₂ with
      | nil => simp at hlen
      | cons y ys =>
          simp only [List.length_cons, Nat.succ.injEq] at hlen
          have hi := ih ys hlen a b
          simp only [List.cons_append, List.cons_le_cons_iff, List.cons_lt_cons_iff]
          rw [hi]
          constructor
          · intro h
            rcases h with hxy | ⟨hxy, htail⟩
            · exact Or.inl (Or.inl hxy)
            · subst y
              rcases htail with hlt | ⟨heq, hle⟩
              · exact Or.inl (Or.inr ⟨rfl, hlt⟩)
              · exact Or.inr ⟨by simp [heq], hle⟩
          · intro h
            rcases h with h | ⟨heq, hle⟩
            · rcases h with hxy | ⟨hxy, hlt⟩
              · exact Or.inl hxy
              · subst y
                exact Or.inr ⟨rfl, Or.inl hlt⟩
            · injection heq with hxy hys
              subst y
              exact Or.inr ⟨rfl, Or.inr ⟨hys, hle⟩⟩

theorem list_lt_iff_le_ne : ∀ {α : Type} [LT α] [Std.Trichotomous (· < · : α → α → Prop)]
    [Std.Asymm (· < · : α → α → Prop)] (l₁ l₂ : List α),
    l₁ < l₂ ↔ l₁ ≤ l₂ ∧ l₁ ≠ l₂ := by
  intro α instLT instTrich instAsymm l₁ l₂
  constructor
  · intro hlt
    constructor
    · exact List.le_of_lt hlt
    · intro heq
      subst l₂
      exact (List.lt_irrefl l₁) hlt
  · intro h
    by_cases hlt : l₁ < l₂
    · exact hlt
    · have hle' : l₂ ≤ l₁ := (List.not_lex_lt (l₁ := l₁) (l₂ := l₂)).mp hlt
      exact False.elim (h.2 (List.le_antisymm h.1 hle'))

-- padding with marker
def padFullC (n c : Nat) (s : Bytes) : Bytes :=
  s ++ List.replicate (n - s.length) 0 ++ [(s.length + c).toUInt8]

theorem u8_ofNat_le_of_le {c d : Nat} (hd : d ≤ 16) (hcd : c ≤ d) :
    c.toUInt8 ≤ d.toUInt8 := by
  change UInt8.ofNat c ≤ UInt8.ofNat d
  rw [UInt8.ofNat_le_iff (m := c) (by omega : c < 256)]
  simpa [Nat.mod_eq_of_lt (by omega : d < 256)] using hcd

theorem u8_ofNat_lt_of_lt {c d : Nat} (hd : d ≤ 16) (hcd : c < d) :
    c.toUInt8 < d.toUInt8 := by
  change UInt8.ofNat c < UInt8.ofNat d
  rw [UInt8.ofNat_lt_iff (m := c) (by omega : c < 256)]
  simpa [Nat.mod_eq_of_lt (by omega : d < 256)] using hcd

theorem u8_not_lt_zero (x : UInt8) : ¬ x < 0 := by
  intro hx
  change x < UInt8.ofNat 0 at hx
  rw [UInt8.lt_ofNat_iff (m := 0) (by decide : 0 < 256)] at hx
  omega

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
    rw [UInt8.ofNat_lt_iff (m := 0) (by decide : 0 < 256)]
    exact hpos

theorem padFullC_length (n c : Nat) (s : Bytes) (hs : s.length ≤ n) :
    (padFullC n c s).length = n + 1 := by
  simp [padFullC]
  omega

theorem padFullC_nil_eq (n c : Nat) :
    padFullC n c [] = List.replicate n 0 ++ [c.toUInt8] := by
  simp [padFullC]

theorem padFullC_cons (n c : Nat) (x : UInt8) (xs : Bytes) (h : (x :: xs).length ≤ n) :
    padFullC n c (x :: xs) = x :: padFullC (n - 1) (c + 1) xs := by
  unfold padFullC
  simp only [List.length_cons, List.cons_append]
  have hsub : n - (xs.length + 1) = n - 1 - xs.length := by omega
  have hmark : xs.length + 1 + c = xs.length + (c + 1) := by omega
  rw [hsub, hmark]

theorem padFullC_nil_mono (n c d : Nat) (hd : d ≤ 16) (hcd : c ≤ d) :
    padFullC n c [] ≤ padFullC n d [] := by
  rw [padFullC_nil_eq, padFullC_nil_eq]
  have h := append_le_append_eq_len (List.replicate n 0) (List.replicate n 0) rfl
      [c.toUInt8] [d.toUInt8]
  rw [h]
  right
  constructor
  · rfl
  · have hsing : [c.toUInt8] ≤ [d.toUInt8] := by
      by_cases hEq : c = d
      · subst d
        simp
      · have hlt := u8_ofNat_lt_of_lt hd (Nat.lt_of_le_of_ne hcd hEq)
        rw [List.cons_le_cons_iff]
        exact Or.inl hlt
    exact hsing

theorem padFullC_nil_le (n c : Nat) : ∀ t : Bytes, t.length ≤ n →
    c + n ≤ 16 → padFullC n c [] ≤ padFullC n c t
  | [], ht, hb => by simp [padFullC]
  | y :: ys, ht, hb => by
      have ht0 : ys.length + 1 ≤ n := by simpa [List.length_cons] using ht
      have ht' : ys.length ≤ n - 1 := by omega
      have hn : 0 < n := by omega
      have hrep : List.replicate n (0 : UInt8) = 0 :: List.replicate (n - 1) 0 := by
        cases n with
        | zero => omega
        | succ n' => simp [List.replicate_succ]
      rw [padFullC_nil_eq, padFullC_cons n c y ys ht, hrep]
      simp only [List.cons_append, List.cons_le_cons_iff]
      rcases u8_zero_lt_or_eq y with hy | hy
      · exact Or.inl hy
      · subst y
        have ht1 : ys.length + 1 ≤ n := by simpa [List.length_cons] using ht
        have ht2 : ys.length ≤ n - 1 := by omega
        right
        constructor
        · rfl
        · rw [← padFullC_nil_eq (n - 1) c]
          exact List.le_trans (padFullC_nil_mono (n - 1) c (c + 1) (by omega) (by omega))
                (padFullC_nil_le (n - 1) (c + 1) ys ht2 (by omega))
termination_by t => t.length

theorem padFullC_gt_nil (n c d : Nat) (hdc : d < c) (hbudget : c + n ≤ 16) : ∀ s : Bytes,
    s.length ≤ n → ¬ padFullC n c s ≤ padFullC n d []
  | [], hs, hle => by
      rw [padFullC_nil_eq, padFullC_nil_eq] at hle
      have h := append_le_append_eq_len (List.replicate n 0) (List.replicate n 0) rfl
          [c.toUInt8] [d.toUInt8]
      rw [h] at hle
      rcases hle with hlt | ⟨heq, hle2⟩
      · exact False.elim ((List.lt_irrefl (List.replicate n 0)) hlt)
      · have hcd' : c ≤ d := by
          have hle' : c.toUInt8 < d.toUInt8 ∨ c.toUInt8 = d.toUInt8 := by
            simpa [List.cons_le_cons_iff, List.cons_lt_cons_iff] using hle2
          rcases hle' with hlt | heq
          · have hcvd : c < d := by
              change UInt8.ofNat c < UInt8.ofNat d at hlt
              rw [UInt8.ofNat_lt_iff (m := c) (by omega : c < 256)] at hlt
              simpa [Nat.mod_eq_of_lt (by omega : d < 256)] using hlt
            omega
          · have heq' : c = d := by
              change UInt8.ofNat c = UInt8.ofNat d at heq
              simpa [Nat.mod_eq_of_lt (by omega : c < 256), Nat.mod_eq_of_lt (by omega : d < 256)]
                using congrArg UInt8.toNat heq
            omega
        omega
  | x :: xs, hs, hle => by
      have hs0 : xs.length + 1 ≤ n := by simpa [List.length_cons] using hs
      have hs' : xs.length ≤ n - 1 := by omega
      rw [padFullC_cons n c x xs hs, padFullC_nil_eq] at hle
      have hrep : List.replicate n (0 : UInt8) = 0 :: List.replicate (n - 1) 0 := by
        cases n with
        | zero => omega
        | succ n' => simp [List.replicate_succ]
      rw [hrep] at hle
      simp only [List.cons_append, List.cons_le_cons_iff] at hle
      rcases hle with hx | ⟨hx, htail⟩
      · exact u8_not_lt_zero x hx
      · subst x
        rw [← padFullC_nil_eq (n - 1) d] at htail
        exact padFullC_gt_nil (n - 1) (c + 1) d (by omega) (by omega) xs hs' htail
termination_by s => s.length

theorem padFullC_le_iff (n c : Nat) : ∀ s t : Bytes, s.length ≤ n → t.length ≤ n →
    c + n ≤ 16 → (padFullC n c s ≤ padFullC n c t ↔ s ≤ t)
  | [], t, hs, ht, hb => by
      cases t with
      | nil => simp [padFullC]
      | cons y ys =>
          have hle := padFullC_nil_le n c (y :: ys) ht hb
          simp [hle]
  | x :: xs, t, hs, ht, hb => by
      cases t with
      | nil =>
          have h0 : xs.length + 1 ≤ n := by simpa [List.length_cons] using hs
          have hs' : xs.length ≤ n - 1 := by omega
          have hn : 0 < n := by omega
          have hnot := padFullC_gt_nil (n - 1) (c + 1) c (by omega) (by omega) xs hs'
          rw [padFullC_cons n c x xs hs, padFullC_nil_eq]
          have hrep : List.replicate n (0 : UInt8) = 0 :: List.replicate (n - 1) 0 := by
            cases n with
            | zero => omega
            | succ n' => simp [List.replicate_succ]
          rw [hrep]
          simp only [List.cons_append, List.cons_le_cons_iff]
          constructor
          · intro hbad
            rcases hbad with hxlt | ⟨hx, htail⟩
            · exact False.elim (u8_not_lt_zero x hxlt)
            · rw [← padFullC_nil_eq (n - 1) c] at htail
              exact False.elim (hnot (by simpa [hx] using htail))
          · intro hfalse
            simpa using hfalse
      | cons y ys =>
          have hs0 : xs.length + 1 ≤ n := by simpa [List.length_cons] using hs
          have ht0 : ys.length + 1 ≤ n := by simpa [List.length_cons] using ht
          have hn : 0 < n := by omega
          rw [padFullC_cons n c x xs hs, padFullC_cons n c y ys ht]
          rw [List.cons_le_cons_iff]
          rw [padFullC_le_iff (n - 1) (c + 1) xs ys (by omega) (by omega) (by omega)]
          rw [List.cons_le_cons_iff]
termination_by s => s.length

theorem padFullC_eq_iff (n c : Nat) (s t : Bytes) (hs : s.length ≤ n) (ht : t.length ≤ n)
    (hb : c + n ≤ 16) : padFullC n c s = padFullC n c t ↔ s = t := by
  constructor
  · intro h
    apply List.le_antisymm
    · exact (padFullC_le_iff n c s t hs ht hb).mp (by rw [h]; simp)
    · exact (padFullC_le_iff n c t s ht hs hb).mp (by rw [h]; simp)
  · intro h
    subst t
    rfl

theorem padFullC_lt_iff (n c : Nat) (s t : Bytes) (hs : s.length ≤ n) (ht : t.length ≤ n)
    (hb : c + n ≤ 16) : padFullC n c s < padFullC n c t ↔ s < t := by
  rw [list_lt_iff_le_ne, list_lt_iff_le_ne]
  rw [padFullC_le_iff n c s t hs ht hb]
  constructor
  · intro h
    constructor
    · exact h.1
    · intro heq
      exact h.2 (by subst t; rfl)
  · intro h
    constructor
    · exact h.1
    · intro heq
      exact h.2 ((padFullC_eq_iff n c s t hs ht hb).mp heq)

theorem replicate_le_of_length : ∀ (k : Nat) (l : Bytes), l.length = k → List.replicate k 0 ≤ l
  | 0, [], h => by simp
  | k + 1, y :: ys, h => by
      have hys : ys.length = k := by simpa [List.length_cons] using h
      simp only [List.replicate_succ, List.cons_le_cons_iff]
      rcases u8_zero_lt_or_eq y with hy | hy
      · exact Or.inl hy
      · exact Or.inr ⟨hy, replicate_le_of_length k ys hys⟩

theorem pad_le_take (n : Nat) (s t : Bytes) (hs : s.length ≤ n) (ht : n ≤ t.length) :
    (s ++ List.replicate (n - s.length) 0) ≤ t.take n ↔ s ≤ t.take s.length := by
  have hlen1 : s.length ≤ (s ++ List.replicate (n - s.length) 0).length := by simp
  have hlen2 : s.length ≤ (t.take n).length := by simpa [ht] using hs
  rw [lex_eq_prefix s.length _ _ hlen1 hlen2]
  simp only [List.take_append_of_le_length (Nat.le_refl s.length), List.take_take,
    Nat.min_eq_left hs, List.drop_append_of_le_length (Nat.le_refl s.length),
    List.take_length, List.drop_length, Nat.min_eq_left hs]
  have hdroplen : ((t.take n).drop s.length).length = n - s.length := by simp [ht]
  have hzero : List.replicate (n - s.length) 0 ≤ (t.take n).drop s.length :=
    replicate_le_of_length _ _ hdroplen
  constructor
  · intro h
    rcases h with hlt | ⟨heq, _⟩
    · exact List.le_of_lt hlt
    · rw [← heq]
      simp
  · intro h
    by_cases heq : s = t.take s.length
    · right
      exact ⟨heq, hzero⟩
    · left
      have hle := h
      rw [list_lt_iff_le_ne]
      exact ⟨hle, heq⟩

theorem le_take_iff (s t : Bytes) (hs : s.length ≤ t.length) :
    s ≤ t ↔ s ≤ t.take s.length := by
  have h := lex_eq_prefix s.length s t (Nat.le_refl _) hs
  rw [h]
  simp only [List.take_length, List.drop_length, Nat.min_eq_left hs]
  constructor
  · intro h
    rcases h with hlt | ⟨heq, _⟩
    · exact List.le_of_lt hlt
    · rw [← heq]
      simp
  · intro h
    by_cases heq : s = t.take s.length
    · right
      exact ⟨heq, by simp⟩
    · left
      rw [list_lt_iff_le_ne]
      exact ⟨h, heq⟩

theorem body_of_short : ∀ s : Bytes, s.length ≤ 8 → body s = padFullC 8 0 s := by
  intro s hs
  match s with
  | [] => simp [body, padFullC]
  | [a] => simp [body, padFullC]
  | [a, b] => simp [body, padFullC]
  | [a, b, c] => simp [body, padFullC]
  | [a, b, c, d] => simp [body, padFullC]
  | [a, b, c, d, e] => simp [body, padFullC]
  | [a, b, c, d, e, f] => simp [body, padFullC]
  | [a, b, c, d, e, f, g] => simp [body, padFullC]
  | [a, b, c, d, e, f, g, h] => simp [body, padFullC]
  | a :: b :: c :: d :: e :: f :: g :: h :: i :: rest => simp at hs

theorem body_of_long : ∀ s : Bytes, 9 ≤ s.length → body s = s.take 8 ++ [9] ++ body (s.drop 8) := by
  intro s hs
  match s with
  | [] => simp at hs
  | [a] => simp at hs
  | [a, b] => simp at hs
  | [a, b, c] => simp at hs
  | [a, b, c, d] => simp at hs
  | [a, b, c, d, e] => simp at hs
  | [a, b, c, d, e, f] => simp at hs
  | [a, b, c, d, e, f, g] => simp at hs
  | [a, b, c, d, e, f, g, h] => simp at hs
  | a :: b :: c :: d :: e :: f :: g :: h :: i :: rest => simp [body]

theorem padFullC_ne_block (x y : Bytes) (hx : x.length ≤ 8) (hy : 9 ≤ y.length) :
    padFullC 8 0 x ≠ y.take 8 ++ [9] := by
  intro h
  have hget := congrArg (fun l : Bytes => l.getD 8 0) h
  have hy8 : 8 ≤ y.length := by omega
  have htake : (y.take 8).length = 8 := by simp [List.length_take, Nat.min_eq_left hy8]
  have hgetR : (y.take 8 ++ [9]).getD 8 0 = 9 := by
    simp [List.getD, htake]
  rw [hgetR] at hget
  match x with
  | [] => simp [padFullC, List.getD] at hget
  | [a] => simp [padFullC, List.getD] at hget
  | [a, b] => simp [padFullC, List.getD] at hget
  | [a, b, c] => simp [padFullC, List.getD] at hget
  | [a, b, c, d] => simp [padFullC, List.getD] at hget
  | [a, b, c, d, e] => simp [padFullC, List.getD] at hget
  | [a, b, c, d, e, f] => simp [padFullC, List.getD] at hget
  | [a, b, c, d, e, f, g] => simp [padFullC, List.getD] at hget
  | [a, b, c, d, e, f, g, h] => simp [padFullC, List.getD] at hget

theorem padFullC_lt_block (x y : Bytes) (hx : x.length ≤ 8) (hy : 9 ≤ y.length) :
    padFullC 8 0 x < y.take 8 ++ [9] ↔ x < y := by
  have hxlen : (x ++ List.replicate (8 - x.length) 0).length = 8 := by
    simp
    omega
  have hy8 : 8 ≤ y.length := by omega
  have hylen : (y.take 8).length = 8 := by simp [List.length_take, Nat.min_eq_left hy8]
  have happ := append_lt_append_eq_len (x ++ List.replicate (8 - x.length) 0) (y.take 8)
      (by rw [hxlen, hylen]) [x.length.toUInt8] [9]
  have hmarker : (x.length.toUInt8 : UInt8) < 9 := by
    change UInt8.ofNat x.length < UInt8.ofNat 9
    rw [UInt8.ofNat_lt_iff (m := x.length) (by omega : x.length < 256)]
    simp [Nat.mod_eq_of_lt (by omega : x.length < 256)]
    omega
  rw [show padFullC 8 0 x = (x ++ List.replicate (8 - x.length) 0) ++ [x.length.toUInt8] by
    simp [padFullC]]
  rw [happ]
  have hmarkerList : [x.length.toUInt8] < ([9] : Bytes) := by simp [List.cons_lt_cons_iff, hmarker]
  simp [hmarkerList]
  have hpad := pad_le_take 8 x y hx hy8
  have htake := le_take_iff x y (by omega : x.length ≤ y.length)
  have hleft :
      (x ++ List.replicate (8 - x.length) 0 < y.take 8 ∨
        x ++ List.replicate (8 - x.length) 0 = y.take 8) ↔
      x ++ List.replicate (8 - x.length) 0 ≤ y.take 8 := by
    rw [list_lt_iff_le_ne]
    constructor
    · intro h
      rcases h with h | heq
      · exact h.1
      · rw [heq]
        simp
    · intro h
      by_cases heq : x ++ List.replicate (8 - x.length) 0 = y.take 8
      · exact Or.inr heq
      · exact Or.inl ⟨h, heq⟩
  rw [hleft, hpad]
  constructor
  · intro h
    have hle : x ≤ y := htake.mpr h
    rw [list_lt_iff_le_ne]
    constructor
    · exact hle
    · intro heq
      subst y
      omega
  · intro h
    exact htake.mp (List.le_of_lt h)




theorem not_lt_replicate_zero_of_length_eq : ∀ (l : Bytes) (k : Nat), l.length = k → ¬ l < List.replicate k 0
  | [], 0, hl, hlt => by simp at hlt
  | x :: xs, k + 1, hl, hlt => by
      have hxs : xs.length = k := by simpa [List.length_cons] using hl
      simp only [List.replicate_succ, List.cons_lt_cons_iff] at hlt
      rcases hlt with hxlt | ⟨hx, htail⟩
      · exact u8_not_lt_zero x hxlt
      · exact not_lt_replicate_zero_of_length_eq xs k hxs htail

theorem take_of_length_le {α : Type} : ∀ (l : List α) (n : Nat), l.length ≤ n → l.take n = l
  | [], n, h => by simp
  | x :: xs, 0, h => by simp at h
  | x :: xs, n + 1, h => by
      simp only [List.length_cons, Nat.succ_le_succ_iff] at h
      simp [List.take_succ_cons, take_of_length_le xs n h]

theorem block_lt_pad (n : Nat) (x y : Bytes) (hx : n ≤ x.length) (hy : y.length ≤ n) :
    x.take n < y ++ List.replicate (n - y.length) 0 ↔ x.take y.length < y := by
  have hlen1 : y.length ≤ (x.take n).length := by simpa [hx] using hy
  have hlen2 : y.length ≤ (y ++ List.replicate (n - y.length) 0).length := by simp
  have hpref := lex_eq_prefix y.length (x.take n) (y ++ List.replicate (n - y.length) 0) hlen1 hlen2
  have hdrop : ((x.take n).drop y.length).length = n - y.length := by simp [hx]
  have hdropEq : ∀ (l : Bytes) (k : Nat), l.length = k → l ≤ List.replicate k 0 → l = List.replicate k 0 := by
    intro l k hl hle
    exact List.le_antisymm hle (replicate_le_of_length k l hl)
  rw [list_lt_iff_le_ne]
  rw [hpref]
  simp only [List.take_take, Nat.min_eq_left hy, List.drop_take,
    List.take_append_of_le_length (Nat.le_refl y.length), List.drop_append_of_le_length (Nat.le_refl y.length),
    List.take_length, List.drop_length]
  constructor
  · intro h
    rcases h.1 with hlt | ⟨heq, htail⟩
    · exact hlt
    · have hdrop2 : (List.take (n - y.length) (x.drop y.length)).length = n - y.length := by
        simp
        omega
      have heqdrop : List.take (n - y.length) (x.drop y.length) = List.replicate (n - y.length) 0 :=
        hdropEq _ _ hdrop2 (by simpa using htail)
      have htaken : x.take n = y ++ List.take (n - y.length) (x.drop y.length) := by
        rw [← List.take_append_drop y.length x]
        rw [List.take_append]
        simp only [heq]
        simp [List.drop_append_of_le_length (Nat.le_refl y.length)]
        rw [take_of_length_le y n hy]
      have heqfull : x.take n = y ++ List.replicate (n - y.length) 0 := by
        rw [htaken, heqdrop]
      exact False.elim (h.2 heqfull)
  · intro h
    constructor
    · exact Or.inl h
    · intro heq
      have hne1 : (x.take n).take y.length = (y ++ List.replicate (n - y.length) 0).take y.length := by rw [heq]
      simp only [List.take_take, Nat.min_eq_left hy, List.take_append_of_le_length (Nat.le_refl y.length)] at hne1
      rw [hne1] at h
      exact (List.lt_irrefl y) (by simpa using h)

theorem lt_take_iff_of_long (x y : Bytes) (h : y.length < x.length) :
    x < y ↔ x.take y.length < y := by
  have hle : y.length ≤ x.length := Nat.le_of_lt h
  have hpref := lex_eq_prefix y.length x y hle (Nat.le_refl y.length)
  have hdropne : x.drop y.length ≠ [] := by
    intro hdrop
    have hlenle : x.length ≤ y.length := List.drop_eq_nil_iff.mp hdrop
    omega
  rw [list_lt_iff_le_ne]
  rw [hpref]
  simp only [List.take_length, List.drop_length]
  constructor
  · intro hgoal
    rcases hgoal.1 with hlt | ⟨heq, hdrop⟩
    · exact hlt
    · exact False.elim (hdropne (List.le_nil.mp hdrop))
  · intro hlt
    constructor
    · exact Or.inl hlt
    · intro heq
      subst y
      omega

theorem block_lt_padFullC (x y : Bytes) (hx : 9 ≤ x.length) (hy : y.length ≤ 8) :
    x.take 8 ++ [9] < padFullC 8 0 y ↔ x < y := by
  have hx8 : 8 ≤ x.length := by omega
  have hxylen : (x.take 8).length = 8 := by simp [List.length_take, Nat.min_eq_left hx8]
  have hyylen : (y ++ List.replicate (8 - y.length) 0).length = 8 := by
    simp
    omega
  have happ := append_lt_append_eq_len (x.take 8) (y ++ List.replicate (8 - y.length) 0)
      (by rw [hxylen, hyylen]) [9] [y.length.toUInt8]
  have hmark_le : y.length.toUInt8 ≤ (9 : UInt8) := by
    change UInt8.ofNat y.length ≤ UInt8.ofNat 9
    rw [UInt8.ofNat_le_iff (m := y.length) (by omega : y.length < 256)]
    simp [Nat.mod_eq_of_lt (by omega : y.length < 256)]
    omega
  have hmark_lt : y.length.toUInt8 < (9 : UInt8) := by
    change UInt8.ofNat y.length < UInt8.ofNat 9
    rw [UInt8.ofNat_lt_iff (m := y.length) (by omega : y.length < 256)]
    simp [Nat.mod_eq_of_lt (by omega : y.length < 256)]
    omega
  have hnotmark : ¬ ([9] : Bytes) < [y.length.toUInt8] := by
    intro hlt
    have hlt' : (9 : UInt8) < y.length.toUInt8 := by simpa [List.cons_lt_cons_iff] using hlt
    exact (Std.Asymm.asymm (r := (· < · : UInt8 → UInt8 → Prop)) (9 : UInt8) y.length.toUInt8 hlt' hmark_lt)
  rw [show padFullC 8 0 y = (y ++ List.replicate (8 - y.length) 0) ++ [y.length.toUInt8] by
    simp [padFullC]]
  rw [happ]
  simp [hnotmark]
  rw [block_lt_pad 8 x y hx8 hy]
  exact (lt_take_iff_of_long x y (by omega)).symm

theorem long_lt_of_take8 (x y : Bytes) (hx : 9 ≤ x.length) (hy : 9 ≤ y.length) :
    x < y ↔ x.take 8 < y.take 8 ∨ (x.take 8 = y.take 8 ∧ x.drop 8 < y.drop 8) := by
  have hpref := lex_eq_prefix 8 x y (by omega) (by omega)
  have heqdecomp : x = y ↔ x.take 8 = y.take 8 ∧ x.drop 8 = y.drop 8 := by
    constructor
    · intro h
      subst y
      exact ⟨rfl, rfl⟩
    · intro h
      rw [← List.take_append_drop 8 x, ← List.take_append_drop 8 y]
      rw [h.1, h.2]
  have hneq : x ≠ y ↔ ¬ (x.take 8 = y.take 8 ∧ x.drop 8 = y.drop 8) := by
    constructor
    · intro h hboth
      exact h (heqdecomp.mpr hboth)
    · intro h heq
      exact h (heqdecomp.mp heq)
  rw [list_lt_iff_le_ne]
  rw [hpref]
  rw [hneq]
  constructor
  · intro h
    rcases h.1 with hlt | ⟨heq, hle⟩
    · exact Or.inl hlt
    · have hdne : x.drop 8 ≠ y.drop 8 := by
        intro hd
        exact h.2 ⟨heq, hd⟩
      exact Or.inr ⟨heq, (list_lt_iff_le_ne (x.drop 8) (y.drop 8)).mpr ⟨hle, hdne⟩⟩
  · intro h
    rcases h with hlt | ⟨heq, hlt⟩
    · exact ⟨Or.inl hlt, by
        intro hboth
        have htake := hboth.1
        rw [htake] at hlt
        exact (List.lt_irrefl _) hlt⟩
    · exact ⟨Or.inr ⟨heq, List.le_of_lt hlt⟩, by
        intro heq'
        exact (List.lt_irrefl _) (by simpa [heq'] using hlt)⟩

theorem long_eq_of_take8 (x y : Bytes) (hx : 9 ≤ x.length) (hy : 9 ≤ y.length) :
    x = y ↔ x.take 8 = y.take 8 ∧ x.drop 8 = y.drop 8 := by
  constructor
  · intro h
    subst y
    exact ⟨rfl, rfl⟩
  · intro h
    rw [← List.take_append_drop 8 x, ← List.take_append_drop 8 y]
    rw [h.1, h.2]

theorem body_append_le_iff : ∀ x y : Bytes, ∀ a b : List UInt8,
    body x ++ a ≤ body y ++ b ↔ x < y ∨ (x = y ∧ a ≤ b) := by
  intro x y
  by_cases hxshort : x.length ≤ 8
  · by_cases hyshort : y.length ≤ 8
    · intro a b
      rw [body_of_short x hxshort, body_of_short y hyshort]
      have hxlen := padFullC_length 8 0 x hxshort
      have hylen := padFullC_length 8 0 y hyshort
      have happ := append_le_append_eq_len (padFullC 8 0 x) (padFullC 8 0 y)
          (by rw [hxlen, hylen]) a b
      rw [happ]
      rw [padFullC_lt_iff 8 0 x y hxshort hyshort (by decide : 0 + 8 ≤ 16)]
      rw [padFullC_eq_iff 8 0 x y hxshort hyshort (by decide : 0 + 8 ≤ 16)]
    · have hylong : 9 ≤ y.length := by omega
      intro a b
      rw [body_of_short x hxshort, body_of_long y hylong]
      rw [List.append_assoc (y.take 8 ++ [9]) (body (y.drop 8)) b]
      change padFullC 8 0 x ++ a ≤ (y.take 8 ++ [9]) ++ (body (y.drop 8) ++ b) ↔ x < y ∨ x = y ∧ a ≤ b
      have hxlen := padFullC_length 8 0 x hxshort
      have hylen : (y.take 8 ++ [9]).length = 9 := by simp [Nat.min_eq_left (by omega : 8 ≤ y.length)]
      have happ := append_le_append_eq_len (padFullC 8 0 x) (y.take 8 ++ [9])
          (by rw [hxlen, hylen]) a (body (y.drop 8) ++ b)
      rw [happ]
      have hne := padFullC_ne_block x y hxshort hylong
      have hlt := padFullC_lt_block x y hxshort hylong
      have hxney : x ≠ y := by intro heq; subst y; omega
      simp [hne, hlt, hxney]
  · have hxlong : 9 ≤ x.length := by omega
    by_cases hyshort : y.length ≤ 8
    · intro a b
      rw [body_of_long x hxlong, body_of_short y hyshort]
      simp only [List.append_assoc]
      rw [← List.append_assoc (x.take 8) [9] (body (x.drop 8) ++ a)]
      have hxylen : (x.take 8 ++ [9]).length = 9 := by simp [Nat.min_eq_left (by omega : 8 ≤ x.length)]
      have hylen := padFullC_length 8 0 y hyshort
      have happ := append_le_append_eq_len (x.take 8 ++ [9]) (padFullC 8 0 y)
          (by rw [hxylen, hylen]) (body (x.drop 8) ++ a) b
      rw [happ]
      have hxney : x ≠ y := by intro heq; subst y; omega
      have hne : x.take 8 ++ [9] ≠ padFullC 8 0 y := by
        intro heq
        have hne' := padFullC_ne_block y x hyshort hxlong
        exact hne' heq.symm
      have hlt := block_lt_padFullC x y hxlong hyshort
      simp [hne, hlt, hxney]
    · have hylong : 9 ≤ y.length := by omega
      intro a b
      rw [body_of_long x hxlong, body_of_long y hylong]
      simp only [List.append_assoc]
      rw [← List.append_assoc (x.take 8) [9] (body (x.drop 8) ++ a)]
      rw [← List.append_assoc (y.take 8) [9] (body (y.drop 8) ++ b)]
      have hlen1 : (x.take 8 ++ [9]).length = 9 := by simp [Nat.min_eq_left (by omega : 8 ≤ x.length)]
      have hlen2 : (y.take 8 ++ [9]).length = 9 := by simp [Nat.min_eq_left (by omega : 8 ≤ y.length)]
      have happ := append_le_append_eq_len (x.take 8 ++ [9]) (y.take 8 ++ [9])
          (by rw [hlen1, hlen2]) (body (x.drop 8) ++ a) (body (y.drop 8) ++ b)
      rw [happ]
      have hblocklt : x.take 8 ++ [9] < y.take 8 ++ [9] ↔ x.take 8 < y.take 8 := by
        have h := append_lt_append_eq_len (x.take 8) (y.take 8)
          (by simp [Nat.min_eq_left (by omega : 8 ≤ x.length), Nat.min_eq_left (by omega : 8 ≤ y.length)])
            ([9] : Bytes) [9]
        rw [h]
        simp
      have hblockeq : x.take 8 ++ [9] = y.take 8 ++ [9] ↔ x.take 8 = y.take 8 := by
        constructor
        · intro h
          have h' := congrArg (fun l : Bytes => l.take 8) h
          simpa [List.take_append, List.take_take,
                 Nat.min_eq_left (by omega : 8 ≤ x.length),
                 Nat.min_eq_left (by omega : 8 ≤ y.length)] using h'
        · intro h
          rw [h]
      rw [hblocklt, hblockeq]
      rw [body_append_le_iff (x.drop 8) (y.drop 8) a b]
      constructor
      · intro h
        rcases h with hlt8 | ⟨heq8, hrest⟩
        · exact Or.inl ((long_lt_of_take8 x y hxlong hylong).mpr (Or.inl hlt8))
        · rcases hrest with hdltd | ⟨hdeq, ha⟩
          · exact Or.inl ((long_lt_of_take8 x y hxlong hylong).mpr (Or.inr ⟨heq8, hdltd⟩))
          · exact Or.inr ⟨(long_eq_of_take8 x y hxlong hylong).mpr ⟨heq8, hdeq⟩, ha⟩
      · intro h
        rcases h with hlt | ⟨heq, ha⟩
        · have htake := (long_lt_of_take8 x y hxlong hylong).mp hlt
          rcases htake with hlt8 | ⟨heq8, hdlt⟩
          · exact Or.inl hlt8
          · exact Or.inr ⟨heq8, Or.inl hdlt⟩
        · have heq8 := (long_eq_of_take8 x y hxlong hylong).mp heq
          exact Or.inr ⟨heq8.1, Or.inr ⟨heq8.2, ha⟩⟩
termination_by x y => (x.length, y.length)
decreasing_by
  all_goals
    simp only [List.length_drop]
    omega

theorem encode_append_le_iff (x y : Bytes) (a b : List UInt8) :
    encode x ++ a ≤ encode y ++ b ↔ x < y ∨ (x = y ∧ a ≤ b) := by
  change 2 :: (body x ++ a) ≤ 2 :: (body y ++ b) ↔ _
  rw [List.cons_le_cons_iff]
  rw [body_append_le_iff]
  simp

theorem image_le_prefix (lo k : Key) (h : lo.length ≤ k.length) :
    image lo ≤ image k ↔ lo ≤ k.take lo.length := by
  induction lo generalizing k with
  | nil => simp [image]
  | cons x xs ih =>
      cases k with
      | nil => simp at h
      | cons y ys =>
          have hlen : xs.length ≤ ys.length := by
            simp only [List.length_cons] at h
            omega
          change encode x ++ image xs ≤ encode y ++ image ys ↔
            (x :: xs) ≤ (y :: ys).take (xs.length + 1)
          rw [encode_append_le_iff]
          simp only [List.take_succ_cons]
          rw [List.cons_le_cons_iff]
          rw [ih ys hlen]

theorem image_le_ff (hi k : Key) (h : hi.length ≤ k.length) :
    image k ≤ image hi ++ [255] ↔ k.take hi.length ≤ hi := by
  induction hi generalizing k with
  | nil =>
      cases k with
      | nil => simp [image]
      | cons y ys =>
          have h2255 : (2 : UInt8) < 255 := by decide
          simpa [image, encode, List.cons_le_cons_iff] using h2255
  | cons x xs ih =>
      cases k with
      | nil => simp at h
      | cons y ys =>
          have hlen : xs.length ≤ ys.length := by
            simp only [List.length_cons] at h
            omega
          change encode y ++ image ys ≤ encode x ++ image xs ++ [255] ↔
            (y :: ys).take (x :: xs).length ≤ x :: xs
          rw [List.append_assoc (encode x) (image xs) [255]]
          rw [encode_append_le_iff]
          simp only [List.length_cons, List.take_succ_cons]
          rw [List.cons_le_cons_iff]
          rw [ih ys hlen]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharOrderLemmas
