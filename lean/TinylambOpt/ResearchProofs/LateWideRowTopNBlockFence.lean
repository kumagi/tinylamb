import TinylambOpt.ResearchGoals.LateWideRowTopNBlockFence

/-!
Proved

- `Goal` — Block bitwise AND/OR point-lookup pruning (ClickBench Q20) and sorted block
  fencing + late wide-row payload decoding (ClickBench Q24): (1) for any non-empty
  block of `UInt64` keys, if `(target &&& ~~~orMask) ≠ 0` or `(~~~target &&& andMask)
  ≠ 0`, `target` cannot appear in the block; (2) under `ValidSortBlock`, skipping
  fenced blocks under `NULLS LAST` and decoding wide payloads only after final Top-N
  yields exact outputs with `payloadDecodes ≤ k`.

The reference model is defined in `TinylambOpt.ResearchGoals.LateWideRowTopNBlockFence`.
-/

namespace TinylambOpt.ResearchProofs.LateWideRowTopNBlockFence

open List
open TinylambOpt.ResearchGoals.LateWideRowTopNBlockFence

/-! ### Bit-level description of the block AND/OR summaries -/

private theorem toBV_fold_or (init : UInt64) (xs : List UInt64) :
    (xs.foldl (fun a b => a ||| b) init).toBitVec =
      xs.foldl (fun a b => a ||| b.toBitVec) init.toBitVec := by
  induction xs generalizing init with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      simpa [UInt64.toBitVec_or] using ih (init ||| x)

private theorem toBV_fold_and (init : UInt64) (xs : List UInt64) :
    (xs.foldl (fun a b => a &&& b) init).toBitVec =
      xs.foldl (fun a b => a &&& b.toBitVec) init.toBitVec := by
  induction xs generalizing init with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      simpa [UInt64.toBitVec_and] using ih (init &&& x)

private theorem bv_or_foldU_bit (init : BitVec 64) (xs : List UInt64) (i : Nat)
    (h : init.getLsbD i = true ∨ ∃ t ∈ xs, t.toBitVec.getLsbD i = true) :
    (xs.foldl (fun a b => a ||| b.toBitVec) init).getLsbD i = true := by
  induction xs generalizing init with
  | nil =>
      simpa using h
  | cons x xs ih =>
      rw [List.foldl_cons]
      apply ih (init ||| x.toBitVec)
      cases h with
      | inl hi =>
          left
          rw [BitVec.getLsbD_or, Bool.or_eq_true]
          exact Or.inl hi
      | inr h =>
          rcases h with ⟨y, hy, hyb⟩
          rw [List.mem_cons] at hy
          rcases hy with rfl | hy
          · left
            rw [BitVec.getLsbD_or, Bool.or_eq_true]
            exact Or.inr hyb
          · right
            exact ⟨y, hy, hyb⟩

private theorem bv_and_foldU_bit (init : BitVec 64) (xs : List UInt64) (i : Nat)
    (h : (xs.foldl (fun a b => a &&& b.toBitVec) init).getLsbD i = true) :
    init.getLsbD i = true ∧ ∀ t ∈ xs, t.toBitVec.getLsbD i = true := by
  induction xs generalizing init with
  | nil =>
      exact ⟨by simpa using h, by simp⟩
  | cons x xs ih =>
      rw [List.foldl_cons] at h
      have hi := ih (init &&& x.toBitVec) h
      rw [BitVec.getLsbD_and, Bool.and_eq_true] at hi
      exact ⟨hi.1.1,
        by intro y hy; rw [List.mem_cons] at hy; rcases hy with rfl | hy
           · exact hi.1.2
           · exact hi.2 y hy⟩

private theorem mask_part : ∀ (xs : List UInt64) (t : UInt64),
    xs ≠ [] → t ∈ xs → mayMatchMask (foldAndMask xs) (foldOrMask xs) t = true := by
  intro xs t hne ht
  rcases xs with _ | ⟨x, xs⟩
  · contradiction
  · simp only [mayMatchMask, Bool.and_eq_true]
    constructor
    · have hzero : t &&& ~~~foldOrMask (x :: xs) = 0 := by
        apply UInt64.toBitVec_inj.mp
        have hz : (0 : UInt64).toBitVec = 0 := by ext i; simp
        rw [hz]
        apply BitVec.eq_of_getLsbD_eq
        intro i hi
        cases htbit : t.toBitVec.getLsbD i with
        | true =>
            have hor : (foldOrMask (x :: xs)).toBitVec.getLsbD i = true := by
              rw [foldOrMask, toBV_fold_or]
              apply bv_or_foldU_bit
              rcases List.mem_cons.mp ht with rfl | ht'
              · left
                exact htbit
              · right
                exact ⟨t, ht', htbit⟩
            have htE : t.toBitVec[i] = true := by simpa [BitVec.getLsbD_eq_getElem hi] using htbit
            have hoE : (foldOrMask (x :: xs)).toBitVec[i] = true := by
              simpa [BitVec.getLsbD_eq_getElem hi] using hor
            rw [UInt64.toBitVec_and, UInt64.toBitVec_not, BitVec.getLsbD_and, BitVec.getLsbD_not]
            simp [hi, htE, hoE]
        | false =>
            have htE : t.toBitVec[i] = false := by simpa [BitVec.getLsbD_eq_getElem hi] using htbit
            rw [UInt64.toBitVec_and, UInt64.toBitVec_not, BitVec.getLsbD_and, BitVec.getLsbD_not]
            simp [hi, htE]
      simpa using hzero
    · have hzero : ~~~t &&& foldAndMask (x :: xs) = 0 := by
        apply UInt64.toBitVec_inj.mp
        have hz : (0 : UInt64).toBitVec = 0 := by ext i; simp
        rw [hz]
        apply BitVec.eq_of_getLsbD_eq
        intro i hi
        cases htbit : t.toBitVec.getLsbD i with
        | true =>
            have htE : t.toBitVec[i] = true := by simpa [BitVec.getLsbD_eq_getElem hi] using htbit
            rw [UInt64.toBitVec_and, UInt64.toBitVec_not, BitVec.getLsbD_and, BitVec.getLsbD_not]
            simp [hi, htE]
        | false =>
            have handfalse : (foldAndMask (x :: xs)).toBitVec.getLsbD i = false := by
              cases e : (foldAndMask (x :: xs)).toBitVec.getLsbD i with
              | false => rfl
              | true =>
                  have hall := bv_and_foldU_bit x.toBitVec xs i (by
                    simpa [foldAndMask, toBV_fold_and] using e)
                  have htbit' : t.toBitVec.getLsbD i = true := by
                    rcases List.mem_cons.mp ht with rfl | ht'
                    · exact hall.1
                    · exact hall.2 t ht'
                  exact (Bool.noConfusion (htbit.symm.trans htbit'))
            have htE : t.toBitVec[i] = false := by simpa [BitVec.getLsbD_eq_getElem hi] using htbit
            have haE : (foldAndMask (x :: xs)).toBitVec[i] = false := by
              simpa [BitVec.getLsbD_eq_getElem hi] using handfalse
            rw [UInt64.toBitVec_and, UInt64.toBitVec_not, BitVec.getLsbD_and, BitVec.getLsbD_not]
            simp [hi, htE, haE]
      simpa using hzero

/-! ### The total preorder used by `leNullsLast` -/

private abbrev WR := WideRow × Nat

private def rk (p : WR) : Nat := match p.1.sortKey with | none => 1 | some _ => 0
private def kv (p : WR) : Int := match p.1.sortKey with | none => 0 | some k => k
private def lexLe (a b : WR) : Prop :=
  rk a < rk b ∨ (rk a = rk b ∧ (kv a < kv b ∨ (kv a = kv b ∧ a.2 ≤ b.2)))

private def wrLe (a b : WR) : Prop := leNullsLast a b = true
private def wrLt (a b : WR) : Prop := ¬ wrLe b a

private theorem wrLe_char {a b : WR} : wrLe a b ↔ lexLe a b := by
  rcases a with ⟨⟨sa, _, _⟩, ia⟩
  rcases b with ⟨⟨sb, _, _⟩, ib⟩
  cases sa with
  | none =>
      cases sb with
      | none => simp [wrLe, leNullsLast, lexLe, rk, kv]
      | some kb => simp [wrLe, leNullsLast, lexLe, rk, kv]
  | some ka =>
      cases sb with
      | none => simp [wrLe, leNullsLast, lexLe, rk, kv]
      | some kb =>
          by_cases h : ka = kb
          · subst kb
            simp [wrLe, leNullsLast, lexLe, rk, kv]
          · have hne : (ka != kb) = true := by simp [h]
            simp [wrLe, leNullsLast, lexLe, rk, kv, h, hne]
            omega

private theorem wrLe_refl (a : WR) : wrLe a a := by
  rw [wrLe_char]
  dsimp [lexLe]
  omega

private theorem wrLe_total (a b : WR) : wrLe a b ∨ wrLe b a := by
  rw [wrLe_char, wrLe_char]
  dsimp [lexLe]
  omega

private theorem wrLe_trans {a b c : WR} (h1 : wrLe a b) (h2 : wrLe b c) : wrLe a c := by
  rw [wrLe_char] at h1 h2 ⊢
  dsimp [lexLe] at h1 h2 ⊢
  omega

private theorem wrLe_antisymm_key {a b : WR} (h1 : wrLe a b) (h2 : wrLe b a) : a.2 = b.2 := by
  rw [wrLe_char] at h1 h2
  dsimp [lexLe] at h1 h2
  omega

private theorem wrLt_of_le_of_idx_lt {a b : WR} (hle : wrLe a b) (hidx : a.2 < b.2) : wrLt a b := by
  intro hba
  have heq := wrLe_antisymm_key hle hba
  omega

private theorem not_wrLt_iff_le {a b : WR} : ¬ wrLt a b ↔ wrLe b a := by
  simp [wrLt]

private theorem wrLt_imp_ne {a b : WR} (h : wrLt a b) : a ≠ b := by
  intro heq
  subst b
  exact h (wrLe_refl a)

private theorem wrLt_of_wrLe_of_lt {a p b : WR} (h1 : wrLe a p) (h2 : wrLt p b) : wrLt a b := by
  intro hba
  exact h2 (wrLe_trans hba h1)

/-! ### Uniqueness bookkeeping for sorted lists -/

private def idxUnique (l : List WR) : Prop :=
  ∀ a ∈ l, ∀ b ∈ l, a.2 = b.2 → a = b

private def idxAgree (x : WR) (l : List WR) : Prop :=
  ∀ y ∈ l, x.2 = y.2 → x = y

private def compareUnique (l : List WR) : Prop :=
  ∀ a ∈ l, ∀ b ∈ l, wrLe a b → wrLe b a → a = b

private theorem compUniq_of_idxAgree {x : WR} {l : List WR} (h : idxAgree x l) :
    ∀ y ∈ l, wrLe x y → wrLe y x → x = y := by
  intro y hy h1 h2
  exact h y hy (wrLe_antisymm_key h1 h2)

private theorem compareUnique_of_idxUnique {l : List WR} (h : idxUnique l) : compareUnique l := by
  intro a ha b hb h1 h2
  exact h a ha b hb (wrLe_antisymm_key h1 h2)

private theorem compareUnique_perm {l₁ l₂ : List WR} (p : l₁ ~ l₂) :
    compareUnique l₁ ↔ compareUnique l₂ := by
  constructor
  · intro h a ha b hb
    exact h a (p.symm.subset ha) b (p.symm.subset hb)
  · intro h a ha b hb
    exact h a (p.subset ha) b (p.subset hb)

private theorem idxUnique_perm {l₁ l₂ : List WR} (p : l₁ ~ l₂) :
    idxUnique l₁ ↔ idxUnique l₂ := by
  constructor
  · intro h a ha b hb
    exact h a (p.symm.subset ha) b (p.symm.subset hb)
  · intro h a ha b hb
    exact h a (p.subset ha) b (p.subset hb)

private theorem idxUnique_cons {x : WR} {l : List WR} :
    idxUnique (x :: l) ↔ idxAgree x l ∧ idxUnique l := by
  constructor
  · intro h
    constructor
    · intro y hy hidx
      exact h x (by simp) y (by simp [hy]) hidx
    · intro a ha b hb hidx
      exact h a (by simp [ha]) b (by simp [hb]) hidx
  · rintro ⟨hx, hl⟩ a ha b hb hidx
    rw [List.mem_cons] at ha hb
    rcases ha with rfl | ha
    · rcases hb with rfl | hb
      · rfl
      · exact hx b hb hidx
    · rcases hb with rfl | hb
      · exact (hx a ha hidx.symm).symm
      · exact hl a ha b hb hidx

/-! ### Insertion-sort bookkeeping -/

private theorem mem_insertSortedWR {x y : WR} : ∀ l : List WR,
    y ∈ insertSortedWR x l ↔ y = x ∨ y ∈ l := by
  intro l
  induction l with
  | nil => simp [insertSortedWR]
  | cons z zs ih =>
      by_cases h : leNullsLast x z = true
      · simp [insertSortedWR, h]
      · simp [insertSortedWR, h, ih, or_assoc, or_left_comm, or_comm]

private theorem insertSortedWR_cons_pos (x z : WR) (l : List WR) (h : leNullsLast x z = true) :
    insertSortedWR x (z :: l) = x :: z :: l := by
  simp [insertSortedWR, h]

private theorem insertSortedWR_cons_neg (x z : WR) (l : List WR) (h : ¬ leNullsLast x z = true) :
    insertSortedWR x (z :: l) = z :: insertSortedWR x l := by
  simp [insertSortedWR, h]

private theorem insertSortedWR_perm (x : WR) : ∀ l : List WR, insertSortedWR x l ~ x :: l := by
  intro l
  induction l with
  | nil => exact Perm.refl _
  | cons z zs ih =>
      by_cases h : leNullsLast x z = true
      · rw [insertSortedWR_cons_pos _ _ _ h]
      · rw [insertSortedWR_cons_neg _ _ _ h]
        exact (Perm.cons z ih).trans (Perm.swap z x zs).symm

private theorem insertSortedWR_pairwise (x : WR) :
    ∀ l : List WR, l.Pairwise wrLe → (insertSortedWR x l).Pairwise wrLe := by
  intro l
  induction l with
  | nil => intro _; simp [insertSortedWR]
  | cons z zs ih =>
      intro h
      obtain ⟨hz, hzs⟩ := pairwise_cons.mp h
      by_cases hc : leNullsLast x z = true
      · rw [insertSortedWR_cons_pos _ _ _ hc]
        refine pairwise_cons.mpr ⟨fun y hy => ?_, pairwise_cons.mpr ⟨hz, hzs⟩⟩
        rcases List.mem_cons.mp hy with rfl | hy
        · exact hc
        · exact wrLe_trans hc (hz y hy)
      · rw [insertSortedWR_cons_neg _ _ _ hc]
        refine pairwise_cons.mpr ⟨fun y hy => ?_, ih hzs⟩
        have hy' := (mem_insertSortedWR (x := x) (y := y) zs).mp hy
        rcases hy' with hyx | hy
        · subst y
          exact (wrLe_total z x).resolve_right hc
        · exact hz y hy

private theorem insertSortedWR_compareUnique (x : WR) : ∀ l : List WR,
    compareUnique l → (∀ y ∈ l, wrLe x y → wrLe y x → x = y) →
    compareUnique (insertSortedWR x l) := by
  intro l
  induction l with
  | nil =>
      intro _ _ p hp q hq hpq hqp
      simp [insertSortedWR] at hp hq
      subst p
      subst q
      rfl
  | cons z zs ih =>
      intro hcu hx
      have hcu' : compareUnique zs := by
        intro a ha b hb hab hba
        exact hcu a (List.mem_cons_of_mem z ha) b (List.mem_cons_of_mem z hb) hab hba
      have hx' : ∀ y ∈ zs, wrLe x y → wrLe y x → x = y := by
        intro y hy
        exact hx y (List.mem_cons_of_mem z hy)
      by_cases hc : leNullsLast x z = true
      · rw [insertSortedWR_cons_pos _ _ _ hc]
        intro p hp q hq hpq hqp
        simp [List.mem_cons] at hp hq
        rcases hp with hp | hp | hp
        · subst p
          rcases hq with hq | hq | hq
          · subst q
            rfl
          · subst q
            exact hx z (by simp) hpq hqp
          · exact hx q (by simp [hq]) hpq hqp
        · subst p
          rcases hq with hq | hq | hq
          · subst q
            exact (hx z (by simp) hqp hpq).symm
          · subst q
            rfl
          · exact hcu z (by simp) q (by simp [hq]) hpq hqp
        · rcases hq with hq | hq | hq
          · subst q
            exact (hx p (by simp [hp]) hqp hpq).symm
          · subst q
            exact hcu p (by simp [hp]) z (by simp) hpq hqp
          · exact hcu' p hp q hq hpq hqp
      · rw [insertSortedWR_cons_neg _ _ _ hc]
        have hins : compareUnique (insertSortedWR x zs) := ih hcu' hx'
        intro p hp q hq hpq hqp
        have hp' := List.mem_cons.mp hp
        have hq' := List.mem_cons.mp hq
        rcases hp' with hpz | hp
        · subst p
          rcases hq' with hqz | hq
          · subst q
            rfl
          · have hq'' := (mem_insertSortedWR (x := x) (y := q) zs).mp hq
            rcases hq'' with hqx | hq
            · subst q
              exact (hx z (by simp) hqp hpq).symm
            · exact hcu z (by simp) q (List.mem_cons_of_mem z hq) hpq hqp
        · rcases hq' with hqz | hq
          · subst q
            have hp'' := (mem_insertSortedWR (x := x) (y := p) zs).mp hp
            rcases hp'' with hpx | hp
            · subst p
              exact hx z (by simp) hpq hqp
            · exact (hcu z (by simp) p (List.mem_cons_of_mem z hp) hqp hpq).symm
          · exact hins p hp q hq hpq hqp

private theorem sortWR_perm : ∀ l : List WR, sortWR l ~ l := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      calc sortWR (x :: xs) = insertSortedWR x (sortWR xs) := rfl
        _ ~ x :: sortWR xs := insertSortedWR_perm x _
        _ ~ x :: xs := Perm.cons x ih

private theorem sortWR_pairwise : ∀ l : List WR, (sortWR l).Pairwise wrLe := by
  intro l
  induction l with
  | nil => simp [sortWR]
  | cons x xs ih => exact insertSortedWR_pairwise x _ ih

private theorem sortWR_compareUnique (l : List WR) (h : compareUnique l) :
    compareUnique (sortWR l) :=
  (compareUnique_perm (sortWR_perm l)).mpr h

private theorem sorted_eq_of_perm_sorted {l₁ l₂ : List WR} (hcu : compareUnique l₁)
    (h₁ : l₁.Pairwise wrLe) (h₂ : l₂.Pairwise wrLe) (p : l₁ ~ l₂) : l₁ = l₂ :=
  Perm.eq_of_pairwise (fun a b ha hb hab hba => hcu a ha b (p.symm.subset hb) hab hba) h₁ h₂ p

private theorem sortWR_cong {l₁ l₂ : List WR} (p : l₁ ~ l₂) (hcu : compareUnique l₁) :
    sortWR l₁ = sortWR l₂ :=
  sorted_eq_of_perm_sorted (sortWR_compareUnique l₁ hcu) (sortWR_pairwise _) (sortWR_pairwise _)
    ((sortWR_perm l₁).trans (p.trans (sortWR_perm l₂).symm))

private theorem length_insertSortedWR (r : WR) : ∀ l : List WR,
    (insertSortedWR r l).length = l.length + 1 := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : leNullsLast r x = true
      · rw [insertSortedWR_cons_pos _ _ _ h]
        simp
      · rw [insertSortedWR_cons_neg _ _ _ h]
        simp [ih]

private theorem length_sortWR : ∀ l : List WR, (sortWR l).length = l.length := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      show (insertSortedWR x (sortWR xs)).length = (x :: xs).length
      rw [length_insertSortedWR, ih]
      simp

private theorem pairwise_take (l : List WR) (h : l.Pairwise wrLe) (i : Nat) :
    (l.take i).Pairwise wrLe :=
  Pairwise.sublist (take_sublist i l) h

private theorem take_drop_bound : ∀ (l : List WR) (i : Nat), l.Pairwise wrLe →
    ∀ c ∈ l.take i, ∀ d ∈ l.drop i, wrLe c d := by
  intro l
  induction l with
  | nil => intro i _ c hc; rw [take_nil] at hc; exact False.elim (not_mem_nil hc)
  | cons x xs ih =>
      intro i h c hc d hd
      cases i with
      | zero => rw [take_zero] at hc; exact False.elim (not_mem_nil hc)
      | succ m =>
          rw [take_succ_cons] at hc
          rw [drop_succ_cons] at hd
          rcases List.mem_cons.mp hc with rfl | hc
          · exact rel_of_pairwise_cons h (Sublist.mem hd (drop_sublist m xs))
          · exact ih m (pairwise_cons.mp h).2 c hc d hd

private theorem take_cons_take (x : WR) (xs : List WR) (m : Nat) :
    (x :: xs).take m = (x :: xs.take m).take m := by
  cases m with
  | zero => rfl
  | succ j =>
      rw [take_succ_cons, take_succ_cons, take_take, Nat.min_eq_left (Nat.le_succ j)]

private theorem take_insertSortedWR (r : WR) : ∀ (l : List WR) (k : Nat),
    (insertSortedWR r l).take k = (insertSortedWR r (l.take k)).take k := by
  intro l
  induction l with
  | nil => intro k; rw [take_nil]
  | cons x xs ih =>
      intro k
      cases k with
      | zero => rfl
      | succ m =>
          by_cases h : leNullsLast r x = true
          · rw [insertSortedWR_cons_pos _ _ _ h,
              show (x :: xs).take (m + 1) = x :: xs.take m from take_succ_cons,
              insertSortedWR_cons_pos _ _ _ h, take_succ_cons, take_succ_cons]
            exact congrArg (fun t => r :: t) (take_cons_take x xs m)
          · rw [take_succ_cons, insertSortedWR_cons_neg _ _ _ h,
              insertSortedWR_cons_neg _ _ _ h, take_succ_cons, take_succ_cons]
            exact congrArg (fun t => x :: t) (ih m)

private theorem take_fold_insert (cands : List WR) (P : List WR) (k : Nat) :
    (cands.foldl (fun acc c => insertSortedWR c acc) P).take k =
      (cands.foldl (fun acc c => insertSortedWR c acc) (P.take k)).take k := by
  induction cands generalizing P with
  | nil =>
      change (P.take k) = (P.take k).take k
      rw [take_take, Nat.min_self]
  | cons c cs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      calc
        (cs.foldl (fun acc y => insertSortedWR y acc) (insertSortedWR c P)).take k
            = (cs.foldl (fun acc y => insertSortedWR y acc)
                ((insertSortedWR c P).take k)).take k := ih (insertSortedWR c P)
        _ = (cs.foldl (fun acc y => insertSortedWR y acc)
                ((insertSortedWR c (P.take k)).take k)).take k := by
              rw [take_insertSortedWR]
        _ = (cs.foldl (fun acc y => insertSortedWR y acc)
                (insertSortedWR c (P.take k))).take k := (ih (insertSortedWR c (P.take k))).symm

private theorem insertSortedWR_cons_of_le_all {x : WR} {l : List WR}
    (h : ∀ y ∈ l, wrLe x y) : insertSortedWR x l = x :: l := by
  induction l with
  | nil => rfl
  | cons y ys ih =>
      rw [insertSortedWR_cons_pos _ _ _ (h y (by simp))]

private theorem sortWR_eq_self_of_pairwise : ∀ l : List WR, l.Pairwise wrLe → sortWR l = l := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x xs ih =>
      intro h
      dsimp [sortWR]
      rw [ih (pairwise_cons.mp h).2]
      apply insertSortedWR_cons_of_le_all
      intro y hy
      exact rel_of_pairwise_cons h hy

private theorem insertSortedWR_append_of_gt {x : WR} {l : List WR}
    (h : ∀ y ∈ l, wrLt y x) : insertSortedWR x l = l ++ [x] := by
  induction l with
  | nil => rfl
  | cons y ys ih =>
      have hnot : ¬ leNullsLast x y = true := h y (by simp)
      rw [insertSortedWR_cons_neg _ _ _ hnot, ih (by
        intro z hz
        exact h z (List.mem_cons_of_mem y hz))]
      rfl

private theorem compareUnique_append_singleton {base : List WR} {p : WR}
    (hbase : compareUnique base) (hp : ∀ y ∈ base, wrLe p y → wrLe y p → p = y) :
    compareUnique (base ++ [p]) := by
  intro a ha b hb hab hba
  simp at ha hb
  rcases ha with ha | ha
  · rcases hb with hb | hb
    · exact hbase a ha b hb hab hba
    · subst b
      exact (hp a ha hba hab).symm
  · subst a
    rcases hb with hb | hb
    · exact hp b hb hab hba
    · subst b
      rfl

private theorem sortWR_append_singleton : ∀ (U : List WR) (p : WR),
    compareUnique U → (∀ y ∈ U, wrLe p y → wrLe y p → p = y) →
    sortWR (U ++ [p]) = insertSortedWR p (sortWR U) := by
  intro U
  induction U with
  | nil => intro p _ _; rfl
  | cons x xs ih =>
      intro p hcu hpU
      have hcu_xs : compareUnique xs := by
        intro a ha b hb
        exact hcu a (List.mem_cons_of_mem x ha) b (List.mem_cons_of_mem x hb)
      have hxs_cu : compareUnique (sortWR xs) := sortWR_compareUnique xs hcu_xs
      have hp_xs : ∀ y ∈ sortWR xs, wrLe p y → wrLe y p → p = y := by
        intro y hy h1 h2
        exact hpU y (List.mem_cons_of_mem x ((sortWR_perm xs).subset hy)) h1 h2
      have key : insertSortedWR x (insertSortedWR p (sortWR xs)) =
          insertSortedWR p (insertSortedWR x (sortWR xs)) := by
        apply sorted_eq_of_perm_sorted
        · apply insertSortedWR_compareUnique
          · exact insertSortedWR_compareUnique p (sortWR xs) hxs_cu hp_xs
          · intro y hy h1 h2
            have hy' := (mem_insertSortedWR (x := p) (y := y) (sortWR xs)).mp hy
            rcases hy' with hyx | hy
            · subst y
              exact (hpU x (by simp) h2 h1).symm
            · exact hcu x (by simp) y (List.mem_cons_of_mem x ((sortWR_perm xs).subset hy)) h1 h2
        · exact insertSortedWR_pairwise x _ (insertSortedWR_pairwise p _ (sortWR_pairwise _))
        · exact insertSortedWR_pairwise p _ (insertSortedWR_pairwise x _ (sortWR_pairwise _))
        · have e1 := (insertSortedWR_perm x (insertSortedWR p (sortWR xs))).trans
            (Perm.cons x (insertSortedWR_perm p (sortWR xs)))
          have e2 : x :: (p :: sortWR xs) ~ p :: (x :: sortWR xs) :=
            (Perm.swap x p (sortWR xs)).symm
          have e3 := (Perm.cons p (insertSortedWR_perm x (sortWR xs))).symm
          have e4 := (insertSortedWR_perm p (insertSortedWR x (sortWR xs))).symm
          exact ((e1.trans e2).trans e3).trans e4
      calc sortWR ((x :: xs) ++ [p])
          = insertSortedWR x (sortWR (xs ++ [p])) := rfl
        _ = insertSortedWR x (insertSortedWR p (sortWR xs)) :=
            congrArg (insertSortedWR x) (ih p hcu_xs (by
              intro y hy h1 h2
              exact hpU y (List.mem_cons_of_mem x hy) h1 h2))
        _ = insertSortedWR p (insertSortedWR x (sortWR xs)) := key
        _ = insertSortedWR p (sortWR (x :: xs)) := rfl

private theorem mem_append_cons_of_mem_append {P cs : List WR} {c a : WR}
    (h : a ∈ P ++ cs) : a ∈ P ++ (c :: cs) := by
  rcases List.mem_append.mp h with hP | hcs
  · exact List.mem_append.mpr (Or.inl hP)
  · exact List.mem_append.mpr (Or.inr (List.mem_cons_of_mem c hcs))

private theorem perm_cons_append (p : WR) : ∀ s : List WR, p :: s ~ s ++ [p] := by
  intro s
  induction s with
  | nil => rfl
  | cons x xs ih =>
      calc p :: x :: xs ~ x :: p :: xs := Perm.swap x p xs
        _ ~ x :: (xs ++ [p]) := Perm.cons x ih

private theorem perm_shift (p : WR) : ∀ (Q s : List WR), Q ++ (p :: s) ~ (Q ++ s) ++ [p] := by
  intro Q
  induction Q with
  | nil => intro s; exact perm_cons_append p s
  | cons x xs ih =>
      intro s
      calc (x :: xs) ++ (p :: s) = x :: (xs ++ p :: s) := rfl
        _ ~ x :: ((xs ++ s) ++ [p]) := Perm.cons x (ih s)
        _ = (x :: xs ++ s) ++ [p] := rfl

private theorem take_sortWR_append_ge (P cands : List WR) (k : Nat)
    (hlen : P.length = k) (hp : P.Pairwise wrLe) (hcu : compareUnique (P ++ cands))
    (hcomp : ∀ c ∈ cands, ∀ y ∈ P ++ cands, wrLe c y → wrLe y c → c = y)
    (hge : ∀ c ∈ cands, ∀ y ∈ P, wrLt y c) :
    (sortWR (P ++ cands)).take k = P := by
  induction cands with
  | nil =>
      rw [append_nil, sortWR_eq_self_of_pairwise P hp]
      rw [← hlen]
      simp
  | cons c cs ih =>
      have hcuP : compareUnique P := by
        intro a ha b hb
        exact hcu a (List.mem_append.mpr (Or.inl ha)) b (List.mem_append.mpr (Or.inl hb))
      have hcuPcs : compareUnique (P ++ cs) := by
        intro a ha b hb
        exact hcu a (mem_append_cons_of_mem_append ha) b (mem_append_cons_of_mem_append hb)
      have hcompcs : ∀ d ∈ cs, ∀ y ∈ P ++ cs, wrLe d y → wrLe y d → d = y := by
        intro d hd y hy h1 h2
        exact hcomp d (by simp [hd]) y (mem_append_cons_of_mem_append hy) h1 h2
      have hcompP : ∀ y ∈ P, wrLe c y → wrLe y c → c = y := by
        intro y hy h1 h2
        exact hcomp c (by simp) y (List.mem_append.mpr (Or.inl hy)) h1 h2
      have hge_cs : ∀ d ∈ cs, ∀ y ∈ P, wrLt y d := by
        intro d hd y hy
        exact hge d (by simp [hd]) y hy
      calc
        (sortWR (P ++ (c :: cs))).take k
            = (sortWR ((P ++ cs) ++ [c])).take k := by
                congr 1
                exact sortWR_cong (perm_shift c P cs) hcu
        _ = (insertSortedWR c (sortWR (P ++ cs))).take k := by
                rw [sortWR_append_singleton (P ++ cs) c hcuPcs]
                intro y hy
                exact hcomp c (by simp) y (mem_append_cons_of_mem_append hy)
        _ = (insertSortedWR c ((sortWR (P ++ cs)).take k)).take k := take_insertSortedWR c _ _
        _ = (insertSortedWR c P).take k := by
                rw [ih hcuPcs hcompcs hge_cs]
        _ = ((P ++ [c]).take k) := by
                rw [insertSortedWR_append_of_gt]
                intro y hy
                exact hge c (by simp) y hy
        _ = P := by simp [hlen]

/-! ### Truncation-substitutivity for the bounded heap -/

private theorem exists_decomp_of_mem (b : WR) : ∀ {C : List WR}, b ∈ C → ∃ t r : List WR, C = t ++ b :: r := by
  intro C h
  induction C with
  | nil => exact False.elim (List.not_mem_nil h)
  | cons x xs ih =>
      rcases List.mem_cons.mp h with rfl | h
      · exact ⟨[], xs, rfl⟩
      · rcases ih h with ⟨t, r, heq⟩
        exact ⟨x :: t, r, by rw [heq, ← cons_append]⟩

private theorem length_le_of_count_le :
    ∀ (P Q : List WR), (∀ v : WR, count v P ≤ count v Q) → P.length ≤ Q.length := by
  intro P
  induction P with
  | nil => intro Q _; exact Nat.zero_le _
  | cons a P ih =>
      intro Q h
      have h0 : 0 < count a (a :: P) := by rw [count_cons_self]; omega
      have h1 := h a
      have haQ : a ∈ Q := count_pos_iff.mp (by omega : 0 < count a Q)
      rcases exists_decomp_of_mem a haQ with ⟨Q₁, Q₂, hQ⟩
      have hlen := ih (Q₁ ++ Q₂) (by
        intro v
        specialize h v
        rw [hQ, count_append, count_cons, count_cons] at h
        rw [count_append]
        omega)
      simp only [hQ, length_append, length_cons] at hlen ⊢
      omega

private theorem pref_small : ∀ (P₁ rest L : List WR) (k : Nat) (p : WR),
    P₁.length = k → (∀ a ∈ P₁, wrLe a p) →
    ∀ b ∈ (sortWR (P₁ ++ rest ++ L)).take k, wrLe b p := by
  intro P₁ rest L k p hlen hle b hb
  exact Classical.byContradiction (fun hnot => by
      have hpb : wrLt p b := hnot
      let S := sortWR (P₁ ++ rest ++ L)
      have hbS : b ∈ S.take k := hb
      have hpair : S.Pairwise wrLe := sortWR_pairwise _
      rcases exists_decomp_of_mem b hbS with ⟨front, r, hC⟩
      have h1 : S = (front ++ b :: r) ++ S.drop k := by
        rw [← hC, take_append_drop]
      have hBdeq : S = front ++ (b :: r ++ S.drop k) := h1.trans (append_assoc _ _ _)
      rw [hBdeq] at hpair
      obtain ⟨_, htail, _⟩ := pairwise_append.mp hpair
      have hb_le : ∀ x ∈ r ++ S.drop k, wrLe b x := by
        intro x hx
        exact rel_of_pairwise_cons htail hx
      have hcount : ∀ v : WR, count v P₁ ≤ count v front := by
        intro v
        by_cases hv : wrLt v b
        · have hzero : count v (b :: r ++ S.drop k) = 0 := by
            apply count_eq_zero_of_not_mem
            intro hx
            rcases List.mem_cons.mp hx with hvb | hx'
            · subst v
              exact absurd (wrLe_refl b) hv
            · rcases List.mem_append.mp hx' with hxr | hxd
              · exact absurd (hb_le v (List.mem_append.mpr (Or.inl hxr))) hv
              · exact absurd (hb_le v (List.mem_append.mpr (Or.inr hxd))) hv
          calc count v P₁
              _ ≤ count v (P₁ ++ rest) := by rw [count_append]; exact Nat.le_add_right _ _
              _ ≤ count v (P₁ ++ rest) + count v L := Nat.le_add_right _ _
              _ = count v (P₁ ++ rest ++ L) := count_append.symm
              _ = count v S := ((perm_iff_count (α := WR)).mp (sortWR_perm (P₁ ++ rest ++ L)) v).symm
              _ = count v front := by rw [hBdeq, count_append, hzero, Nat.add_zero]
        · have hzero : count v P₁ = 0 :=
            count_eq_zero_of_not_mem
              (fun hmem => absurd (wrLt_of_wrLe_of_lt (hle v hmem) hpb) hv)
          rw [hzero]
          exact Nat.zero_le _
      have hcard1 : P₁.length ≤ front.length := length_le_of_count_le P₁ front hcount
      have hcard2 : front.length + 1 ≤ k := by
        calc front.length + 1
            _ ≤ (front ++ b :: r).length := by rw [length_append, length_cons]; omega
            _ = (S.take k).length := by rw [← hC]
            _ = min k S.length := length_take
            _ ≤ k := Nat.min_le_left _ _
      have hlen2 : k ≤ front.length := by rw [← hlen]; exact hcard1
      omega)

private theorem p_notin_take : ∀ (P₁ rest L : List WR) (k : Nat) (p : WR),
    P₁.length = k → (∀ a ∈ P₁, wrLe a p) → p ∈ rest → p ∉ P₁ →
    idxAgree p (P₁ ++ rest ++ L) →
    p ∉ (sortWR (P₁ ++ rest ++ L)).take k := by
  intro P₁ rest L k p hlen hle hprest hpnotP hpidx hpS
  let S := sortWR (P₁ ++ rest ++ L)
  have hpair : S.Pairwise wrLe := sortWR_pairwise _
  rcases exists_decomp_of_mem p hpS with ⟨front, r, hC⟩
  have h1 : S = (front ++ p :: r) ++ S.drop k := by rw [← hC, take_append_drop]
  have hBdeq : S = front ++ (p :: r ++ S.drop k) := h1.trans (append_assoc _ _ _)
  rw [hBdeq] at hpair
  obtain ⟨_, htail, _⟩ := pairwise_append.mp hpair
  have hp_le : ∀ x ∈ r ++ S.drop k, wrLe p x := by
    intro x hx
    exact rel_of_pairwise_cons htail hx
  have hcount : ∀ v : WR, count v P₁ ≤ count v front := by
    intro v
    by_cases hv : wrLt v p
    · have hzero : count v (p :: r ++ S.drop k) = 0 := by
        apply count_eq_zero_of_not_mem
        intro hx
        rcases List.mem_cons.mp hx with hpx | hx'
        · subst v
          exact absurd (wrLe_refl p) hv
        · rcases List.mem_append.mp hx' with hxr | hxd
          · exact absurd (hp_le v (List.mem_append.mpr (Or.inl hxr))) hv
          · exact absurd (hp_le v (List.mem_append.mpr (Or.inr hxd))) hv
      calc count v P₁
          _ ≤ count v (P₁ ++ rest) := by rw [count_append]; exact Nat.le_add_right _ _
          _ ≤ count v (P₁ ++ rest) + count v L := Nat.le_add_right _ _
          _ = count v (P₁ ++ rest ++ L) := count_append.symm
          _ = count v S := ((perm_iff_count (α := WR)).mp (sortWR_perm (P₁ ++ rest ++ L)) v).symm
          _ = count v front := by rw [hBdeq, count_append, hzero, Nat.add_zero]
    · have hpu : wrLe p v := not_wrLt_iff_le.mp hv
      have hzero : count v P₁ = 0 := by
        apply count_eq_zero_of_not_mem
        intro hmem
        have hvp : wrLe v p := hle v hmem
        have hvall : v ∈ P₁ ++ rest ++ L :=
          List.mem_append.mpr (Or.inl (List.mem_append.mpr (Or.inl hmem)))
        have heq : p = v := hpidx v hvall (wrLe_antisymm_key hvp hpu).symm
        exact hpnotP (by simpa [heq] using hmem)
      rw [hzero]
      exact Nat.zero_le _
  have hcard1 : k ≤ front.length := by
    rw [← hlen]
    exact length_le_of_count_le P₁ front hcount
  have hcard2 : front.length + 1 ≤ k := by
    calc front.length + 1
        _ ≤ (front ++ p :: r).length := by rw [length_append, length_cons]; omega
        _ = (S.take k).length := by rw [← hC]
        _ = min k S.length := length_take
        _ ≤ k := Nat.min_le_left _ _
  omega

private theorem pref_idx_ne : ∀ (P₁ rest L : List WR) (k : Nat) (p : WR),
    P₁.length = k → (∀ a ∈ P₁, wrLe a p) → p ∈ rest → p ∉ P₁ →
    idxUnique (P₁ ++ rest ++ L) →
    ∀ b ∈ (sortWR (P₁ ++ rest ++ L)).take k, b.2 ≠ p.2 := by
  intro P₁ rest L k p hlen hle hprest hpnotP huni b hb hidx
  have hbp : wrLe b p := pref_small P₁ rest L k p hlen hle b hb
  have hbS : b ∈ sortWR (P₁ ++ rest ++ L) := Sublist.mem hb (take_sublist k _)
  have hbOrig : b ∈ P₁ ++ rest ++ L := (sortWR_perm (P₁ ++ rest ++ L)).subset hbS
  have hpOrig : p ∈ P₁ ++ rest ++ L := by simp [hprest]
  have beq : b = p := huni b hbOrig p hpOrig hidx
  have hpS : p ∈ (sortWR (P₁ ++ rest ++ L)).take k := by simpa [beq] using hb
  exact p_notin_take P₁ rest L k p hlen hle hprest hpnotP
    (fun y hy hEq => (huni y hy p hpOrig hEq.symm).symm) hpS

private theorem wrLt_of_le_of_idx_ne {a b : WR} (hle : wrLe a b) (hne : a.2 ≠ b.2) : wrLt a b := by
  intro hba
  exact hne (wrLe_antisymm_key hle hba)

private theorem ins_cut : ∀ (B : List WR) (p : WR) (k : Nat),
    B.Pairwise wrLe → k ≤ B.length →
    (∀ b ∈ B.take k, wrLt b p) → (insertSortedWR p B).take k = B.take k := by
  intro B
  induction B with
  | nil => intro p k _ hlen _; have hk : k = 0 := Nat.eq_zero_of_le_zero hlen; subst k; rfl
  | cons b B ih =>
      intro p k hB hlen hlt
      cases k with
      | zero => rfl
      | succ m =>
          by_cases hc : leNullsLast p b = true
          · have hbmem : b ∈ (b :: B).take (m + 1) := by rw [take_succ_cons]; simp
            exact False.elim ((hlt b hbmem) hc)
          · rw [insertSortedWR_cons_neg _ _ _ hc, take_succ_cons, take_succ_cons]
            apply congrArg (fun t => b :: t)
            apply ih p m (pairwise_cons.mp hB).2
            · simpa using hlen
            · intro c hc'
              exact hlt c (by rw [take_succ_cons]; exact List.mem_cons_of_mem b hc')

private theorem perm_rearrange (A C M : List WR) : A ++ (C ++ M) ~ C ++ (A ++ M) := by
  induction A with
  | nil => simp
  | cons x xs ih =>
      calc (x :: xs) ++ (C ++ M) = x :: (xs ++ (C ++ M)) := rfl
        _ ~ x :: (C ++ (xs ++ M)) := Perm.cons x ih
        _ = (x :: C) ++ (xs ++ M) := rfl
        _ ~ (C ++ [x]) ++ (xs ++ M) := Perm.append (perm_cons_append x C) (Perm.refl (xs ++ M))
        _ = C ++ ((x :: xs) ++ M) := by rw [append_assoc, cons_append, nil_append, ← cons_append]

private theorem take_sortWR_merge_trunc : ∀ (k : Nat) (P₁ P₂ L : List WR),
    (P₁.length = k ∨ P₂ = []) →
    (P₁ ++ P₂).Pairwise wrLe →
    idxUnique (P₁ ++ P₂ ++ L) →
    (P₁ ++ P₂ ++ L).Nodup →
    (∀ p ∈ P₂, ∀ y ∈ P₁, y.2 ≠ p.2) →
    (sortWR (P₁ ++ (P₂ ++ L))).take k = (sortWR (P₁ ++ L)).take k := by
  intro k P₁ P₂
  induction P₂ with
  | nil => intro L _ _ _ _ _; rfl
  | cons p P₂ ih =>
      intro L hcase hpair huni hnodup hdisj
      obtain hP₁ | hnil := hcase
      · obtain ⟨hP₁pair, hrest, hcros⟩ := pairwise_append.mp hpair
        have hP₂pair : (P₁ ++ P₂).Pairwise wrLe :=
          pairwise_append.mpr ⟨hP₁pair, (pairwise_cons.mp hrest).2,
            fun a ha b hb => hcros a ha b (List.mem_cons_of_mem p hb)⟩
        have hpP₁ : p ∉ P₁ := by
          intro hp1
          exact hdisj p (by simp) p hp1 rfl
        have hnodup_all : (P₁ ++ (p :: P₂ ++ L)).Nodup := by
          simpa [append_assoc] using hnodup
        have hn_tail : (p :: (P₂ ++ L)).Nodup := (List.nodup_append.mp hnodup_all).2.1
        have hpNotTail : p ∉ P₂ ++ L := (List.nodup_cons.mp hn_tail).1
        have hpNotBase : p ∉ P₁ ++ (P₂ ++ L) := by
          intro hb
          rcases List.mem_append.mp hb with hP | hPL
          · exact hpP₁ hP
          · exact hpNotTail hPL
        have hnodup_tail : (P₁ ++ P₂ ++ L).Nodup := by
          have hP₁n : P₁.Nodup := (List.nodup_append.mp hnodup_all).1
          have hcross : ∀ a ∈ P₁, ∀ b ∈ p :: (P₂ ++ L), a ≠ b :=
            (List.nodup_append.mp hnodup_all).2.2
          have hPLn : (P₂ ++ L).Nodup := (List.nodup_cons.mp hn_tail).2
          have hP₂n : P₂.Nodup := (List.nodup_append.mp hPLn).1
          have hLn : L.Nodup := (List.nodup_append.mp hPLn).2.1
          have hP12n : (P₁ ++ P₂).Nodup := by
            apply List.nodup_append.mpr
            refine ⟨hP₁n, hP₂n, ?_⟩
            intro a ha b hb
            exact hcross a ha b (List.mem_cons_of_mem p (List.mem_append.mpr (Or.inl hb)))
          apply List.nodup_append.mpr
          refine ⟨hP12n, hLn, ?_⟩
          intro a ha b hb
          rcases List.mem_append.mp ha with haP | haP2
          · exact hcross a haP b (List.mem_cons_of_mem p (List.mem_append.mpr (Or.inr hb)))
          · exact (List.nodup_append.mp hPLn).2.2 a haP2 b hb
        have mem_tail : ∀ {x : WR}, x ∈ P₁ ++ P₂ ++ L → x ∈ P₁ ++ (p :: P₂ ++ L) := by
          intro x hx
          simp at hx ⊢
          rcases hx with hP | hP2 | hL
          · exact Or.inl hP
          · exact Or.inr (Or.inr (Or.inl hP2))
          · exact Or.inr (Or.inr (Or.inr hL))
        have mem_tail' : ∀ {x : WR}, x ∈ P₁ ++ (P₂ ++ L) → x ∈ P₁ ++ (p :: P₂ ++ L) := by
          intro x hx
          exact mem_tail (by simpa [append_assoc] using hx)
        have huni_tail : idxUnique (P₁ ++ P₂ ++ L) := by
          intro a ha b hb hidx
          exact huni a (by simpa [append_assoc] using mem_tail ha)
            b (by simpa [append_assoc] using mem_tail hb) hidx
        have hcu_base : compareUnique (P₁ ++ (P₂ ++ L)) := by
          apply compareUnique_of_idxUnique
          intro a ha b hb hidx
          exact huni a (by simpa [append_assoc] using mem_tail' ha)
            b (by simpa [append_assoc] using mem_tail' hb) hidx
        have hp_comp : ∀ y ∈ P₁ ++ (P₂ ++ L), wrLe p y → wrLe y p → p = y := by
          intro y hy h1 h2
          exact (huni y (by simpa [append_assoc] using mem_tail' hy) p (by simp)
            (wrLe_antisymm_key h2 h1)).symm
        have hcu_l2 : compareUnique ((P₁ ++ (P₂ ++ L)) ++ [p]) :=
          compareUnique_append_singleton hcu_base hp_comp
        have hcu_l1 : compareUnique (P₁ ++ (p :: P₂ ++ L)) := by
          exact (compareUnique_perm (perm_shift p P₁ (P₂ ++ L))).mpr (by simpa using hcu_l2)
        have hSbase : (sortWR (P₁ ++ (P₂ ++ L))).Pairwise wrLe := sortWR_pairwise _
        have hle_pref : ∀ b ∈ (sortWR (P₁ ++ (P₂ ++ L))).take k, wrLe b p := by
          simpa [append_assoc] using
            (pref_small P₁ (P₂ ++ L) [] k p hP₁
              (fun a ha => hcros a ha p (by simp)))
        have hlt_pref : ∀ b ∈ (sortWR (P₁ ++ (P₂ ++ L))).take k, wrLt b p := by
          intro b hb
          apply wrLt_of_le_of_idx_ne (hle_pref b hb)
          intro hEq
          have hbS : b ∈ sortWR (P₁ ++ (P₂ ++ L)) := Sublist.mem hb (take_sublist k _)
          have hbBase : b ∈ P₁ ++ (P₂ ++ L) := (sortWR_perm _).subset hbS
          have hbOrig : b ∈ P₁ ++ (p :: P₂ ++ L) := mem_tail' hbBase
          have hpOrig : p ∈ P₁ ++ (p :: P₂ ++ L) := by simp
          have beq : b = p := huni b (by simpa [append_assoc] using hbOrig)
            p (by simpa [append_assoc] using hpOrig) hEq
          exact hpNotBase (by simpa [beq] using hbBase)
        have hlenS : k ≤ (sortWR (P₁ ++ (P₂ ++ L))).length := by
          rw [length_sortWR, length_append]
          rw [← hP₁]
          exact Nat.le_add_right _ _
        calc
          (sortWR (P₁ ++ (p :: P₂ ++ L))).take k
              = (sortWR ((P₁ ++ (P₂ ++ L)) ++ [p])).take k :=
                  congrArg (fun t : List WR => t.take k)
                    (sortWR_cong (perm_shift p P₁ (P₂ ++ L)) hcu_l1)
          _ = (insertSortedWR p (sortWR (P₁ ++ (P₂ ++ L)))).take k :=
                  congrArg (fun t : List WR => t.take k)
                    (sortWR_append_singleton (P₁ ++ (P₂ ++ L)) p hcu_base hp_comp)
          _ = (sortWR (P₁ ++ (P₂ ++ L))).take k :=
                  ins_cut (sortWR (P₁ ++ (P₂ ++ L))) p k hSbase hlenS hlt_pref
          _ = (sortWR (P₁ ++ L)).take k := ih L (Or.inl hP₁) hP₂pair huni_tail hnodup_tail (by
                intro q hq y hy
                exact hdisj q (List.mem_cons_of_mem p hq) y hy)
      · exfalso
        exact List.cons_ne_nil p P₂ hnil

/-! ### Block-level bookkeeping -/

private abbrev allFiltered (bs : List Block) : List WR :=
  ((bs.flatMap Block.rows).zipIdx).filter (fun p => p.1.filterPass)

private abbrev candidatesAt (off : Nat) (rows : List WideRow) : List WR :=
  rows.zipIdx.filterMap fun (r, localIdx) =>
    if r.filterPass then some (r, off + localIdx) else none

private def foldStep (k : Nat) (acc : List WR × Nat × Nat) (b : Block) : List WR × Nat × Nat :=
  let (heap, ev, globalIdx) := acc
  if k == 0 || blockPrunedByHeap k heap b then
    (heap, ev, globalIdx + b.rows.length)
  else
    let candidates := candidatesAt globalIdx b.rows
    let nextHeap := (sortWR (heap ++ candidates)).take k
    (nextHeap, ev + b.rows.length, globalIdx + b.rows.length)

private def sndNodup (l : List WR) : Prop := (l.map Prod.snd).Nodup

private theorem sndNodup_perm {l₁ l₂ : List WR} (p : l₁ ~ l₂) : sndNodup l₁ ↔ sndNodup l₂ := by
  unfold sndNodup
  exact List.Perm.nodup_iff (List.Perm.map Prod.snd p)

private theorem sndNodup_sublist {l₁ l₂ : List WR} (s : l₁ <+ l₂) (h : sndNodup l₂) : sndNodup l₁ := by
  unfold sndNodup at h ⊢
  exact List.Nodup.sublist (List.Sublist.map Prod.snd s) h

private theorem idxUnique_of_sndNodup {l : List WR} (h : sndNodup l) : idxUnique l := by
  induction l with
  | nil => intro a ha; cases ha
  | cons x xs ih =>
      have hc := List.nodup_cons.mp h
      apply (idxUnique_cons.mpr ⟨?_, ih hc.2⟩)
      intro y hy heq
      exfalso
      exact hc.1 (by rw [heq]; exact List.mem_map.mpr ⟨y, hy, rfl⟩)

private theorem nodup_of_sndNodup {l : List WR} (h : sndNodup l) : l.Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      apply List.nodup_cons.mpr
      constructor
      · intro hx
        exact (List.nodup_cons.mp h).1 (List.mem_map.mpr ⟨x, hx, rfl⟩)
      · exact ih (List.nodup_cons.mp h).2

private theorem sndNodup_filter (p : WR → Bool) {l : List WR} (h : sndNodup l) :
    sndNodup (l.filter p) :=
  sndNodup_sublist (List.filter_sublist (p := p) (l := l)) h

private theorem sndNodup_zipIdx_off (rows : List WideRow) (off : Nat) :
    sndNodup (rows.zipIdx off) := by
  unfold sndNodup
  induction rows generalizing off with
  | nil => simp
  | cons r rs ih =>
      simp only [List.zipIdx, List.map_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨p, hp, heq⟩
        have hb := List.mem_zipIdx hp
        omega
      · exact ih (off + 1)

private theorem sndNodup_zipIdx (rows : List WideRow) : sndNodup rows.zipIdx :=
  sndNodup_zipIdx_off rows 0

private theorem sndNodup_allFiltered (bs : List Block) : sndNodup (allFiltered bs) :=
  sndNodup_filter _ (sndNodup_zipIdx (bs.flatMap Block.rows))

private theorem zipIdx_offset2 (rows : List WideRow) (off n : Nat) :
    rows.zipIdx (off + n) = (rows.zipIdx n).map (fun p => (p.1, off + p.2)) := by
  induction rows generalizing off n with
  | nil => rfl
  | cons r rs ih =>
      change (r, off + n) :: rs.zipIdx (off + n + 1) =
        ((r, n) :: rs.zipIdx (n + 1)).map (fun p => (p.1, off + p.2))
      simp only [List.map_cons]
      rw [show off + n + 1 = off + (n + 1) by omega]
      rw [ih]

private theorem zipIdx_append (A B : List WideRow) (off : Nat) :
    (A ++ B).zipIdx off = A.zipIdx off ++ B.zipIdx (off + A.length) := by
  induction A generalizing off with
  | nil => simp
  | cons a as ih =>
      change (a :: (as ++ B)).zipIdx off =
        (a :: as).zipIdx off ++ B.zipIdx (off + (a :: as).length)
      simp only [List.zipIdx]
      rw [ih]
      simp only [List.length_cons]
      rw [show off + 1 + as.length = off + (as.length + 1) by omega]
      rw [List.cons_append]

private theorem candidatesAt_offset (off n : Nat) (rows : List WideRow) :
    (rows.zipIdx n).filterMap (fun (r, i) => if r.filterPass then some (r, off + i) else none) =
      ((rows.zipIdx n).filter (fun p => p.1.filterPass)).map (fun p => (p.1, off + p.2)) := by
  induction rows generalizing n with
  | nil => rfl
  | cons r rs ih =>
      by_cases hr : r.filterPass
      · simp [List.zipIdx, hr, ih, Nat.add_assoc]
      · simp [List.zipIdx, hr, ih, Nat.add_assoc]

private theorem candidatesAt_eq_filter (off : Nat) (rows : List WideRow) :
    candidatesAt off rows =
      ((rows.zipIdx).filter (fun p => p.1.filterPass)).map (fun p => (p.1, off + p.2)) := by
  unfold candidatesAt
  simpa using candidatesAt_offset off 0 rows

private theorem filterMap_zipIdx_shift (rows : List WideRow) (off n : Nat) :
    (rows.zipIdx n).filterMap (fun (r, i) => if r.filterPass then some (r, off + i) else none) =
      (rows.zipIdx).filterMap (fun (r, i) => if r.filterPass then some (r, off + n + i) else none) := by
  have hz : rows.zipIdx n = (rows.zipIdx).map (fun p => (p.1, n + p.2)) := by
    simpa using zipIdx_offset2 rows n 0
  rw [hz, List.filterMap_map]
  have hadd : ∀ i : Nat, off + (n + i) = i + (off + n) := by intro i; omega
  have hadd2 : ∀ i : Nat, i + (off + n) = off + n + i := by intro i; omega
  simpa [Function.comp_def, hadd, hadd2]

private theorem filter_map_commute (l : List WR) (n : Nat) :
    (l.map (fun p => (p.1, n + p.2))).filter (fun p => p.1.filterPass) =
      (l.filter (fun p => p.1.filterPass)).map (fun p => (p.1, n + p.2)) := by
  induction l with
  | nil => rfl
  | cons r rs ih => by_cases hr : r.1.filterPass <;> simp [hr, ih]

private theorem filter_map_shift (rows : List WideRow) (n off : Nat) :
    ((rows.zipIdx n).filter (fun p => p.1.filterPass)).map (fun p => (p.1, off + p.2)) =
      ((rows.zipIdx).filter (fun p => p.1.filterPass)).map (fun p => (p.1, off + n + p.2)) := by
  have hz : rows.zipIdx n = (rows.zipIdx).map (fun p => (p.1, n + p.2)) := by
    simpa using zipIdx_offset2 rows n 0
  rw [hz]
  rw [filter_map_commute]
  simp [Nat.add_assoc]

private theorem length_flatMap_rows (bs : List Block) :
    (bs.flatMap Block.rows).length = (bs.map (fun b => b.rows.length)).sum := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem allFiltered_snoc (pref : List Block) (b : Block) :
    allFiltered (pref ++ [b]) =
      allFiltered pref ++ candidatesAt (pref.flatMap Block.rows).length b.rows := by
  unfold allFiltered candidatesAt
  rw [List.flatMap_append]
  simp only [List.flatMap_cons, List.flatMap_nil, nil_append]
  rw [zipIdx_append]
  simp only [List.append_nil, Nat.zero_add]
  rw [List.filter_append]
  congr 1
  have hc : (b.rows.zipIdx (pref.flatMap Block.rows).length).filterMap
        (fun (r, i) => if r.filterPass then some (r, i) else none) =
      (b.rows.zipIdx (pref.flatMap Block.rows).length).filter
        (fun p => p.1.filterPass) := by
    simpa using candidatesAt_offset 0 (pref.flatMap Block.rows).length b.rows
  rw [← hc]
  let s := (pref.flatMap Block.rows).length
  change (b.rows.zipIdx s).filterMap (fun (r, i) => if r.filterPass then some (r, i) else none) =
    b.rows.zipIdx.filterMap (fun (r, i) => if r.filterPass then some (r, s + i) else none)
  have hz : b.rows.zipIdx s = (b.rows.zipIdx).map (fun p => (p.1, s + p.2)) := by
    simpa using zipIdx_offset2 b.rows s 0
  rw [hz, List.filterMap_map]
  congr 1

private theorem member_idx_lt_length (rows : List WideRow) {r : WideRow} {i : Nat}
    (h : (r, i) ∈ rows.zipIdx) : i < rows.length := by
  have h' := List.mem_zipIdx h
  simpa using h'.2.1

private theorem allFiltered_idx_lt (bs : List Block) {x : WR} (hx : x ∈ allFiltered bs) :
    x.2 < (bs.flatMap Block.rows).length := by
  have hx' : x ∈ (bs.flatMap Block.rows).zipIdx := (List.mem_filter.mp hx).1
  exact member_idx_lt_length _ hx'

private theorem candidate_idx_ge {off : Nat} {rows : List WideRow} {c : WR}
    (hc : c ∈ candidatesAt off rows) : off ≤ c.2 := by
  rw [candidatesAt_eq_filter] at hc
  rcases List.mem_map.mp hc with ⟨p, hp, rfl⟩
  omega

private theorem candidate_idx_lt {off : Nat} {rows : List WideRow} {c : WR}
    (hc : c ∈ candidatesAt off rows) : c.2 < off + rows.length := by
  rw [candidatesAt_eq_filter] at hc
  rcases List.mem_map.mp hc with ⟨p, hp, rfl⟩
  have := member_idx_lt_length rows ((List.mem_filter.mp hp).1)
  omega

private theorem mem_zipIdx_row {rows : List WideRow} {r : WideRow} {i : Nat}
    (h : (r, i) ∈ rows.zipIdx) : r ∈ rows := by
  have h' := List.mem_zipIdx h
  rw [h'.2.2]
  exact List.getElem_mem (by simpa using h'.2.1)

private theorem candidate_row {off : Nat} {rows : List WideRow} {c : WR}
    (hc : c ∈ candidatesAt off rows) : c.1 ∈ rows := by
  rw [candidatesAt_eq_filter] at hc
  rcases List.mem_map.mp hc with ⟨p, hp, rfl⟩
  exact mem_zipIdx_row ((List.mem_filter.mp hp).1)

private theorem le_getLast_of_mem {l : List WR} (hp : l.Pairwise wrLe) :
    ∀ y ∈ l, ∀ z, l.getLast? = some z → wrLe y z := by
  induction l with
  | nil => intro y hy; cases hy
  | cons x xs ih =>
      intro y hy z hz
      rcases List.mem_cons.mp hy with hyx | hy
      · subst y
        cases xs with
        | nil =>
            simp at hz
            subst z
            exact wrLe_refl x
        | cons x' xs' =>
            have hx : wrLe x x' := rel_of_pairwise_cons hp (by simp)
            exact wrLe_trans hx (ih (pairwise_cons.mp hp).2 x' (by simp) z (by simpa using hz))
      · cases xs with
        | nil => cases hy
        | cons x' xs' =>
            exact ih (pairwise_cons.mp hp).2 y hy z (by simpa using hz)

private theorem pruned_facts {k : Nat} {heap : List WR} {b : Block}
    (hpr : blockPrunedByHeap k heap b = true) :
    k ≠ 0 ∧ k ≤ heap.length ∧
      ∃ (z : WR) (tau : Int), heap.getLast? = some z ∧ z.1.sortKey = some tau ∧
        (b.allNullSort = true ∨ tau < b.minNonNullSort) := by
  unfold blockPrunedByHeap at hpr
  by_cases hc : k == 0 || heap.length < k
  · simp [hc] at hpr
  · simp [hc] at hpr
    have hc' : k ≠ 0 ∧ k ≤ heap.length := by simpa using hc
    cases hlast : heap.getLast? with
    | none => simp [hlast] at hpr
    | some z =>
        simp [hlast] at hpr
        cases hzkey : z.1.sortKey with
        | none => simp [hzkey] at hpr
        | some tau =>
            rw [hzkey] at hpr
            simp at hpr
            have hor : b.allNullSort = true ∨ tau < b.minNonNullSort := by
              simpa [Bool.or_eq_true, decide_eq_true_eq] using hpr
            have hzkeq : z.1.sortKey = some tau := by simp [hzkey]
            exact ⟨hc'.1, hc'.2, z, tau, rfl, hzkeq, hor⟩

private theorem pruned_candidate_gt {k off : Nat} {heap : List WR} {b : Block}
    (hpr : blockPrunedByHeap k heap b = true) (hv : ValidSortBlock b)
    (hp : heap.Pairwise wrLe) {z : WR} {tau : Int}
    (hz : heap.getLast? = some z) (hzkey : z.1.sortKey = some tau)
    (hor : b.allNullSort = true ∨ tau < b.minNonNullSort)
    {c : WR} (hc : c ∈ candidatesAt off b.rows)
    {y : WR} (hy : y ∈ heap) : wrLt y c := by
  have hyz : wrLe y z := le_getLast_of_mem hp y hy z hz
  have hykey : ∃ ka : Int, y.1.sortKey = some ka := by
    cases hykey' : y.1.sortKey with
    | none =>
        rw [wrLe_char] at hyz
        dsimp [lexLe] at hyz
        simp [rk, kv, hykey', hzkey] at hyz
    | some ka => exact ⟨ka, rfl⟩
  rcases hykey with ⟨ky, hykey⟩
  have hky_le : ky ≤ tau := by
    rw [wrLe_char] at hyz
    dsimp [lexLe] at hyz
    simp [rk, kv, hykey, hzkey] at hyz
    omega
  have hcrow : c.1 ∈ b.rows := candidate_row hc
  cases hckey : c.1.sortKey with
  | none =>
      cases hor with
      | inl hnull =>
          simp [wrLt, wrLe_char, lexLe, rk, kv, hykey, hckey]
      | inr hlt =>
          simp [wrLt, wrLe_char, lexLe, rk, kv, hykey, hckey]
  | some kc =>
      have hvrow := hv c.1 hcrow
      rw [hckey] at hvrow
      cases hor with
      | inl hnull =>
          exact False.elim (Bool.noConfusion (hvrow.1.symm.trans hnull))
      | inr hlt =>
          have hmin : b.minNonNullSort ≤ kc := hvrow.2
          simp [wrLt, wrLe_char, lexLe, rk, kv, hykey, hckey]
          omega

private theorem merge_step (k : Nat) (L cands : List WR) (heap : List WR)
    (hheap : heap = (sortWR L).take k)
    (huni : idxUnique (L ++ cands)) (hnodup : (L ++ cands).Nodup)
    (hdisj : ∀ p ∈ cands, ∀ y ∈ L, y.2 ≠ p.2) :
    (sortWR (heap ++ cands)).take k = (sortWR (L ++ cands)).take k := by
  subst heap
  let S := sortWR L
  have hpair : (S.take k ++ S.drop k).Pairwise wrLe :=
    pairwise_append.mpr ⟨pairwise_take S (sortWR_pairwise L) k,
      Pairwise.sublist (drop_sublist k S) (sortWR_pairwise L),
      take_drop_bound S k (sortWR_pairwise L)⟩
  have hlenS : S.length = L.length := by dsimp [S]; rw [length_sortWR]
  have hcase : (S.take k).length = k ∨ S.drop k = [] := by
    by_cases hlt : k < S.length
    · left
      rw [length_take, Nat.min_eq_left (Nat.le_of_lt hlt)]
    · right
      apply List.eq_nil_of_length_eq_zero
      rw [length_drop, hlenS]
      omega
  have hpart : (S.take k ++ S.drop k) ~ S := by
    simpa [List.take_append_drop]
  have hperm : (S.take k ++ S.drop k ++ cands) ~ (L ++ cands) := by
    exact (Perm.append hpart (Perm.refl cands)).trans
      (Perm.append (sortWR_perm L) (Perm.refl cands))
  have huni_part : idxUnique (S.take k ++ S.drop k ++ cands) :=
    (idxUnique_perm hperm).mpr huni
  have hnodup_part : (S.take k ++ S.drop k ++ cands).Nodup :=
    (List.Perm.nodup_iff hperm).mpr hnodup
  have hLuni : idxUnique L := by
    intro a ha b hb hidx
    exact huni a (List.mem_append.mpr (Or.inl ha)) b (List.mem_append.mpr (Or.inl hb)) hidx
  have hSuni : idxUnique S := (idxUnique_perm (sortWR_perm L)).mpr hLuni
  have hLnodup : L.Nodup := (List.nodup_append.mp hnodup).1
  have hSnodup : S.Nodup := (List.Perm.nodup_iff (sortWR_perm L)).mpr hLnodup
  have hTDnodup : (S.take k ++ S.drop k).Nodup :=
    (List.Perm.nodup_iff hpart).mpr hSnodup
  have hdisj₁ : ∀ p ∈ S.drop k, ∀ y ∈ S.take k, y.2 ≠ p.2 := by
    intro p hp y hy heq
    have hpy : y = p := hSuni y (Sublist.mem hy (take_sublist k S)) p
      (Sublist.mem hp (drop_sublist k S)) heq
    have hpTake : p ∈ S.take k := by simpa [hpy] using hy
    exact (List.nodup_append.mp hTDnodup).2.2 p hpTake p hp rfl
  have hdisj₂ : ∀ p ∈ S.drop k, ∀ y ∈ cands, y.2 ≠ p.2 := by
    intro p hp y hy heq
    have hpL : p ∈ L := (sortWR_perm L).subset (Sublist.mem hp (drop_sublist k S))
    exact hdisj y hy p hpL heq.symm
  have hmerge := take_sortWR_merge_trunc k (S.take k) (S.drop k) cands
    hcase hpair huni_part hnodup_part hdisj₁
  have hperm' := hperm
  rw [append_assoc] at hperm'
  have huni_part' := huni_part
  rw [append_assoc] at huni_part'
  have hsortEq : sortWR (S.take k ++ (S.drop k ++ cands)) = sortWR (L ++ cands) :=
    sortWR_cong hperm' (compareUnique_of_idxUnique huni_part')
  calc
    (sortWR (S.take k ++ cands)).take k
        = (sortWR (S.take k ++ (S.drop k ++ cands))).take k := hmerge.symm
    _ = (sortWR (L ++ cands)).take k := congrArg (fun t : List WR => t.take k) hsortEq

private theorem sndNodup_candidatesAt (off : Nat) (rows : List WideRow) :
    sndNodup (candidatesAt off rows) := by
  induction rows generalizing off with
  | nil => simp [sndNodup, candidatesAt]
  | cons r rs ih =>
      have htail : (rs.zipIdx 1).filterMap
          (fun (r', i) => if r'.filterPass then some (r', off + i) else none) =
          candidatesAt (off + 1) rs := by
        dsimp [candidatesAt]
        rw [filterMap_zipIdx_shift]
      by_cases hr : r.filterPass
      · simp only [candidatesAt, List.zipIdx, List.filterMap_cons, hr]
        unfold sndNodup
        apply List.nodup_cons.mpr
        constructor
        · intro hmem
          rcases List.mem_map.mp hmem with ⟨c, hc, heq⟩
          rw [htail] at hc
          have hge := candidate_idx_ge hc
          rw [heq] at hge
          omega
        · rw [htail]
          exact ih (off + 1)
      · simpa [candidatesAt, List.zipIdx, hr, htail] using ih (off + 1)

private theorem fold_spec (k : Nat) : ∀ (pref bs : List Block) (heap : List WR) (ev idx : Nat),
    idx = (pref.flatMap Block.rows).length →
    heap = (sortWR (allFiltered pref)).take k →
    (∀ b ∈ pref ++ bs, ValidSortBlock b) →
    (bs.foldl (foldStep k) (heap, ev, idx)).1 = (sortWR (allFiltered (pref ++ bs))).take k ∧
    (bs.foldl (foldStep k) (heap, ev, idx)).2.2 = ((pref ++ bs).flatMap Block.rows).length := by
  intro pref bs
  induction bs generalizing pref with
  | nil =>
      intro heap ev idx hidx hheap hv
      simp [hidx, hheap]
  | cons b bs ih =>
      intro heap ev idx hidx hheap hvAll
      have hv : ∀ x ∈ pref, ValidSortBlock x := fun x hx => hvAll x (by simp [hx])
      have hvcur : ValidSortBlock b := hvAll b (by simp)
      let L := allFiltered pref
      let cands := candidatesAt idx b.rows
      let acc1 := foldStep k (heap, ev, idx) b
      have hLsn : sndNodup L := by dsimp [L]; exact sndNodup_allFiltered pref
      have hLuni : idxUnique L := idxUnique_of_sndNodup hLsn
      have hLnodup : L.Nodup := nodup_of_sndNodup hLsn
      have hcandsn : sndNodup cands := by dsimp [cands]; exact sndNodup_candidatesAt idx b.rows
      have hcombSnd : sndNodup (L ++ cands) := by
        unfold sndNodup at hLsn hcandsn ⊢
        rw [List.map_append]
        apply List.nodup_append.mpr
        refine ⟨hLsn, hcandsn, ?_⟩
        intro a ha hb hbmem
        rcases List.mem_map.mp ha with ⟨pa, hpa, rfl⟩
        rcases List.mem_map.mp hbmem with ⟨pb, hpb, rfl⟩
        have hlt := allFiltered_idx_lt pref hpa
        rw [← hidx] at hlt
        have hge := candidate_idx_ge hpb
        omega
      have hcombUni : idxUnique (L ++ cands) := idxUnique_of_sndNodup hcombSnd
      have hdisjL : ∀ c ∈ cands, ∀ y ∈ L, y.2 ≠ c.2 := by
        intro c hc y hy heq
        have hlt := allFiltered_idx_lt pref hy
        rw [← hidx] at hlt
        have hge := candidate_idx_ge hc
        omega
      have hheapPair : heap.Pairwise wrLe := by
        rw [hheap]
        exact pairwise_take _ (sortWR_pairwise L) k
      have hheapSnd : sndNodup heap := by
        rw [hheap]
        apply sndNodup_sublist (take_sublist k (sortWR L))
        exact (sndNodup_perm (sortWR_perm L)).mpr hLsn
      have hlenHeap : heap.length ≤ k := by
        rw [hheap, length_take]
        exact Nat.min_le_left _ _
      have hheapSubL : ∀ y ∈ heap, y ∈ L := by
        intro y hy
        rw [hheap] at hy
        exact (sortWR_perm L).subset (Sublist.mem hy (take_sublist k (sortWR L)))
      have hhcSnd : sndNodup (heap ++ cands) := by
        unfold sndNodup at hheapSnd hcandsn ⊢
        rw [List.map_append]
        apply List.nodup_append.mpr
        refine ⟨hheapSnd, hcandsn, ?_⟩
        intro a ha b hb
        rcases List.mem_map.mp ha with ⟨pa, hpa, rfl⟩
        rcases List.mem_map.mp hb with ⟨pb, hpb, rfl⟩
        have hlt := allFiltered_idx_lt pref (hheapSubL pa hpa)
        rw [← hidx] at hlt
        have hge := candidate_idx_ge hpb
        omega
      have hmerge : (sortWR (heap ++ cands)).take k = (sortWR (L ++ cands)).take k :=
        merge_step k L cands heap hheap hcombUni (nodup_of_sndNodup hcombSnd) hdisjL
      have hacc1idx : acc1.2.2 = idx + b.rows.length := by
        dsimp [acc1, foldStep]
        by_cases hc : k == 0 || blockPrunedByHeap k heap b <;> simp [hc]
      have hacc1heap : acc1.1 = (sortWR (L ++ cands)).take k := by
        by_cases hk0 : k = 0
        · have hempty : heap = [] := by rw [hheap]; simp [hk0]
          dsimp [acc1, foldStep]
          simp [hk0, hempty]
        · by_cases hpr : blockPrunedByHeap k heap b = true
          · rcases pruned_facts hpr with ⟨_, hkle, z, tau, hz, hzkey, hor⟩
            have hlenEq : heap.length = k := by omega
            have hge : ∀ c ∈ cands, ∀ y ∈ heap, wrLt y c := by
              intro c hc y hy
              exact pruned_candidate_gt hpr hvcur hheapPair hz hzkey hor
                (by simpa [cands] using hc) hy
            have hcomp : ∀ c ∈ cands, ∀ y ∈ heap ++ cands,
                wrLe c y → wrLe y c → c = y := by
              intro c hc y hy h1 h2
              have hyall : y ∈ L ++ cands := by
                rcases List.mem_append.mp hy with hyheap | hycand
                · exact List.mem_append.mpr (Or.inl (hheapSubL y hyheap))
                · exact List.mem_append.mpr (Or.inr hycand)
              exact hcombUni c (List.mem_append.mpr (Or.inr hc)) y hyall
                (wrLe_antisymm_key h1 h2)
            have hptake := take_sortWR_append_ge heap cands k hlenEq hheapPair
              (compareUnique_of_idxUnique (idxUnique_of_sndNodup hhcSnd)) hcomp hge
            dsimp [acc1, foldStep]
            simp [hk0, hpr]
            exact hptake.symm.trans hmerge
          · dsimp [acc1, foldStep]
            simp [hk0, hpr]
            simpa [cands] using hmerge
      have hheap1 : acc1.1 = (sortWR (allFiltered (pref ++ [b]))).take k := by
        rw [allFiltered_snoc]
        simpa [L, cands, hidx] using hacc1heap
      have hidx1 : acc1.2.2 = ((pref ++ [b]).flatMap Block.rows).length := by
        rw [hacc1idx, hidx]
        simp [length_flatMap_rows, Nat.add_comm]
      have hv' : ∀ b' ∈ pref ++ [b] ++ bs, ValidSortBlock b' := by
        intro b' hb'
        exact hvAll b' (by simpa [append_assoc] using hb')
      have res := ih (pref ++ [b]) acc1.1 acc1.2.1 acc1.2.2 hidx1 hheap1 hv'
      simpa [acc1, append_assoc] using res

theorem sound : TinylambOpt.ResearchGoals.LateWideRowTopNBlockFence.Goal := by
  constructor
  · exact mask_part
  · intro k bs hvalid
    have hheap0 : ([] : List WR) = (sortWR (allFiltered [])).take k := by
      change ([] : List WR) = ([] : List WR).take k
      rw [List.take_nil]
    have hspec := fold_spec k [] bs [] 0 0 (by simp) hheap0 (by simpa using hvalid)
    constructor
    · change (bs.foldl (foldStep k) ([], 0, 0)).1.map (fun p => p.1.payload) =
        ((sortWR (((bs.flatMap Block.rows).zipIdx).filter (fun p => p.1.filterPass))).take k).map
          (fun p => p.1.payload)
      exact congrArg (fun t : List WR => t.map (fun p => p.1.payload)) hspec.1
    · change (bs.foldl (foldStep k) ([], 0, 0)).1.length ≤ k
      rw [hspec.1, length_take]
      exact Nat.min_le_left _ _

end TinylambOpt.ResearchProofs.LateWideRowTopNBlockFence
