import Std.Tactic

/-!
Sort direction of one ORDER BY term.

NULL placement of one ORDER BY term.

Named results include `optLt_irrefl`, `optLt_trans`, `optLt_trichotomy`.
-/
-- C++ counterpart: executor/topn.cpp (see issues/README.md).

/-!
# Lexicographic block fence for Top-N over mixed order keys

A sort key is a list of order terms `(value, direction, null placement)`.
`better` is the strict part of the total lexicographic preorder `leKey`
(mixed ASC/DESC and NULLS FIRST/LAST, ties are *not* better).

A `Fence` holds an **optimistic per-term best bound** of a block together
with the current K-th key.  The bound need not be a real row: it is the
term-wise best combination, and the proof shows it is nevertheless a
conservative lexicographic lower bound of every row in the block
(`termWiseBest_is_bound`).  Hence

* `blockCannotImprove f k` (K rows already fixed) implies `k` cannot beat
  the K-th row;
* `blockCannotImproveTies f k` implies `k` cannot even tie the K-th row;
  a block equal to the boundary is therefore **kept** under WITH TIES.

The reference Top-N is rank based: a row is kept iff fewer than K rows are
strictly better than it.  That keeps at least K rows and every row tied
with the K-th.  The main theorem shows that once K rows are fixed, reading
the fenced remainder leaves the reference answer unchanged.
-/

namespace TinylambOpt.TopNBlockFence

/-- Sort direction of one ORDER BY term. -/
inductive Dir where
  | ASC | DESC
  deriving DecidableEq, Repr, Inhabited

/-- NULL placement of one ORDER BY term. -/
inductive Nulls where
  | First | Last
  deriving DecidableEq, Repr, Inhabited

/-- One order term: a value plus its direction and NULL placement. -/
structure Term where
  value : Option Int
  dir : Dir
  nulls : Nulls
  deriving DecidableEq, Repr, Inhabited

/-- A composite order key. -/
abbrev Key := List Term

/-- The specification (direction / NULL placement) of a key. -/
def specOf (a : Key) : List (Dir × Nulls) := a.map (fun t => (t.dir, t.nulls))

/-- The values of a key, in order. -/
def valsOf (a : Key) : List (Option Int) := a.map Term.value

/- ------------------------------------------------------------------------ -
    Term-level total order on `Option Int`
    ------------------------------------------------------------------------ -/

/-- `nulls = First`: `null < v`.  `nulls = Last`: `v < null`.
    Non-NULL values compare by `Int`. -/
def optLt (n : Nulls) (a b : Option Int) : Bool :=
  match a, b with
  | none, none => false
  | none, some _ => n == Nulls.First
  | some _, none => n == Nulls.Last
  | some x, some y => x < y

theorem optLt_irrefl (n : Nulls) (a : Option Int) : optLt n a a = false := by
  cases n <;> cases a <;> simp [optLt]

theorem optLt_trans (n : Nulls) {a b c : Option Int}
    (hab : optLt n a b = true) (hbc : optLt n b c = true) :
    optLt n a c = true := by
  cases n <;> cases a <;> cases b <;> cases c <;> simp [optLt] at * <;> omega

theorem optLt_trichotomy (n : Nulls) (a b : Option Int) :
    optLt n a b = true ∨ a = b ∨ optLt n b a = true := by
  cases n <;> cases a <;> cases b <;> simp [optLt] <;> omega

theorem optLt_of_eq_left (n : Nulls) {a b c : Option Int}
    (h : a = b) (hbc : optLt n b c = true) : optLt n a c = true := by
  subst b
  simpa using hbc

theorem optLt_of_eq_right (n : Nulls) {a b c : Option Int}
    (h : b = c) (hab : optLt n a b = true) : optLt n a c = true := by
  subst c
  simpa using hab

/-- Direction-aware strict order: DESC reverses the value comparison. -/
def valLt (d : Dir) (n : Nulls) (a b : Option Int) : Bool :=
  match d with
  | Dir.ASC => optLt n a b
  | Dir.DESC => optLt n b a

def valLe (d : Dir) (n : Nulls) (a b : Option Int) : Bool :=
  valLt d n a b || decide (a = b)

theorem valLt_irrefl (d : Dir) (n : Nulls) (a : Option Int) :
    valLt d n a a = false := by
  cases d <;> simp [valLt, optLt_irrefl]

theorem valLt_trans (d : Dir) (n : Nulls) {a b c : Option Int}
    (hab : valLt d n a b = true) (hbc : valLt d n b c = true) :
    valLt d n a c = true := by
  cases d with
  | ASC => exact optLt_trans n hab hbc
  | DESC => exact optLt_trans n hbc hab

theorem valLt_trichotomy (d : Dir) (n : Nulls) (a b : Option Int) :
    valLt d n a b = true ∨ a = b ∨ valLt d n b a = true := by
  cases d with
  | ASC => exact optLt_trichotomy n a b
  | DESC =>
      rcases optLt_trichotomy n b a with hlt | heq | hgt
      · exact Or.inl (by simpa [valLt] using hlt)
      · exact Or.inr (Or.inl heq.symm)
      · exact Or.inr (Or.inr (by simpa [valLt] using hgt))

theorem valLe_of_not_lt (d : Dir) (n : Nulls) {a b : Option Int}
    (h : ¬ valLt d n a b = true) : valLe d n b a = true := by
  rcases valLt_trichotomy d n a b with hlt | heq | hgt
  · exact False.elim (h hlt)
  · subst b
    simp [valLe, valLt_irrefl]
  · simp [valLe, hgt]

theorem valLe_trans (d : Dir) (n : Nulls) {a b c : Option Int}
    (hab : valLe d n a b = true) (hbc : valLe d n b c = true) :
    valLe d n a c = true := by
  simp [valLe] at hab hbc ⊢
  rcases hab with hltab | heqab
  · rcases hbc with hltbc | heqbc
    · exact Or.inl (valLt_trans d n hltab hltbc)
    · subst c
      exact Or.inl hltab
  · subst b
    rcases hbc with hltbc | heqbc
    · exact Or.inl hltbc
    · subst c
      exact Or.inr rfl

/- ------------------------------------------------------------------------ -
    Lexicographic preorder on value lists, driven by a fixed spec
    ------------------------------------------------------------------------ -/

/-- `as ≤ bs` lexicographically, comparing term `i` with
    direction/NULL placement `spec[i]`. -/
def keyLe : List (Dir × Nulls) → List (Option Int) → List (Option Int) → Bool
  | [], _, _ => true
  | _ :: _, [], _ => true
  | _ :: _, _ :: _, [] => false
  | (d, n) :: spec, a :: as, b :: bs =>
      valLt d n a b || (decide (a = b) && keyLe spec as bs)

theorem keyLe_refl (spec : List (Dir × Nulls)) (as : List (Option Int)) :
    keyLe spec as as = true := by
  induction spec generalizing as with
  | nil => simp [keyLe]
  | cons s spec ih =>
      rcases s with ⟨d, n⟩
      cases as with
      | nil => simp [keyLe]
      | cons a as =>
          simp [keyLe, valLt_irrefl, ih]

theorem keyLe_total (spec : List (Dir × Nulls))
    (as bs : List (Option Int)) :
    keyLe spec as bs || keyLe spec bs as = true := by
  induction spec generalizing as bs with
  | nil => simp [keyLe]
  | cons s spec ih =>
      rcases s with ⟨d, n⟩
      cases as with
      | nil => simp [keyLe]
      | cons a as =>
        cases bs with
        | nil => simp [keyLe]
        | cons b bs =>
            rcases valLt_trichotomy d n a b with hlt | heq | hgt
            · simp [keyLe, hlt]
            · subst b
              simpa [keyLe, valLt_irrefl] using ih as bs
            · simp [keyLe, hgt]

theorem keyLe_trans (spec : List (Dir × Nulls))
    (as bs cs : List (Option Int))
    (hab : keyLe spec as bs = true) (hbc : keyLe spec bs cs = true) :
    keyLe spec as cs = true := by
  induction spec generalizing as bs cs with
  | nil => simp [keyLe]
  | cons s spec ih =>
      rcases s with ⟨d, n⟩
      cases as with
      | nil => simp [keyLe]
      | cons a as =>
        cases bs with
        | nil => simp [keyLe] at hab
        | cons b bs =>
          cases cs with
          | nil => simp [keyLe] at hbc
          | cons c cs =>
              simp [keyLe] at hab hbc ⊢
              rcases hab with hltab | ⟨heqab, habtail⟩
              · rcases hbc with hltbc | ⟨heqbc, hbctail⟩
                · exact Or.inl (valLt_trans d n hltab hltbc)
                · subst c
                  exact Or.inl hltab
              · subst b
                rcases hbc with hltbc | ⟨heqbc, hbctail⟩
                · exact Or.inl hltbc
                · subst c
                  exact Or.inr ⟨rfl, ih as bs cs habtail hbctail⟩

/- ------------------------------------------------------------------------ -
    Total lexicographic order on order keys
    ------------------------------------------------------------------------ -/

/-- Composite key order using each term's own direction / NULL placement. -/
def leKey (a b : Key) : Bool := keyLe (specOf a) (valsOf a) (valsOf b)

/-- Strict order: `a` is better than `b` iff `a ≤ b` but not `b ≤ a`.
    Ties (and NULL-vs-NULL) are therefore never "better". -/
def better (a b : Key) : Bool := leKey a b && !leKey b a

theorem leKey_refl (a : Key) : leKey a a = true := by
  simpa [leKey] using keyLe_refl (specOf a) (valsOf a)

theorem leKey_trans (a b c : Key)
    (hsab : specOf a = specOf b) (_hsbc : specOf b = specOf c)
    (hab : leKey a b = true) (hbc : leKey b c = true) :
    leKey a c = true := by
  have hab' : keyLe (specOf a) (valsOf a) (valsOf b) = true := by
    simpa [leKey] using hab
  have hbc' : keyLe (specOf a) (valsOf b) (valsOf c) = true := by
    rw [leKey] at hbc
    rw [← hsab] at hbc
    exact hbc
  simpa [leKey] using keyLe_trans (specOf a) (valsOf a) (valsOf b) (valsOf c) hab' hbc'

theorem leKey_of_better (a b : Key) (h : better a b = true) : leKey a b = true := by
  have hh : (leKey a b && !leKey b a) = true := by simpa [better] using h
  exact (Bool.and_eq_true (leKey a b) (!leKey b a)).mp hh |>.1

theorem not_leKey_of_better (a b : Key) (h : better a b = true) : leKey b a = false := by
  cases hl : leKey b a with
  | false => rfl
  | true => simp [better, hl] at h

theorem better_irrefl (a : Key) : better a a = false := by
  simp [better, leKey_refl]

theorem leKey_not_better (a b : Key) (h : leKey a b = true) : better b a = false := by
  cases hba : leKey b a <;> simp [better, hba, h]

/-- Strict + non-strict compose: `a ≤ b` and `b < c` imply `a < c`. -/
theorem better_of_leKey_of_better (a b c : Key)
    (hsab : specOf a = specOf b) (hsbc : specOf b = specOf c)
    (hab : leKey a b = true) (hbc : better b c = true) :
    better a c = true := by
  have hb_le_c : leKey b c = true := leKey_of_better b c hbc
  have ha_le_c : leKey a c = true := leKey_trans a b c hsab hsbc hab hb_le_c
  have hc_le_b : leKey c b = false := not_leKey_of_better b c hbc
  have hc_le_a : leKey c a = false := by
    cases hca : leKey c a with
    | false => rfl
    | true =>
        have hsac : specOf c = specOf a := (hsab.trans hsbc).symm
        have hca_le_b : leKey c b = true := leKey_trans c a b hsac hsab hca hab
        simp [hca_le_b] at hc_le_b
  simp [better, ha_le_c, hc_le_a]

/-- The other composition: `a < b` and `b ≤ c` imply `a < c`.
    Needed for WITH TIES: a strictly worse bound still certifies that every
    row at or below it is strictly worse than the K-th. -/
theorem better_of_better_of_leKey (a b c : Key)
    (hsab : specOf a = specOf b) (hsbc : specOf b = specOf c)
    (hab : better a b = true) (hbc : leKey b c = true) :
    better a c = true := by
  have ha_le_b : leKey a b = true := leKey_of_better a b hab
  have ha_le_c : leKey a c = true := leKey_trans a b c hsab hsbc ha_le_b hbc
  have hc_le_a : leKey c a = false := by
    cases hca : leKey c a with
    | false => rfl
    | true =>
        have hsac : specOf c = specOf a := hsbc.symm.trans hsab.symm
        have hcb' : better c b = true :=
          better_of_leKey_of_better c a b hsac hsab hca hab
        have : leKey b c = false := not_leKey_of_better c b hcb'
        simp [hbc] at this
  simp [better, ha_le_c, hc_le_a]

/- ------------------------------------------------------------------------ -
    Fences: optimistic per-term best bound + current K-th key
    ------------------------------------------------------------------------ -/

structure Fence where
  bound : Key
  kth : Key

/-- Row `k` cannot beat the current K-th row: the bound is below `k` and
    the K-th row is below the bound. -/
def blockCannotImprove (f : Fence) (k : Key) : Bool :=
  leKey f.kth f.bound && leKey f.bound k

/-- WITH TIES: only skip rows strictly worse than the K-th row; a row equal
    to the boundary is kept. -/
def blockCannotImproveTies (f : Fence) (k : Key) : Bool :=
  better f.kth f.bound && leKey f.bound k

theorem blockCannotImprove_sound (f : Fence) (k : Key)
    (hsKB : specOf f.kth = specOf f.bound) (hsk : specOf k = specOf f.kth)
    (h : blockCannotImprove f k = true) :
    better k f.kth = false := by
  have hh : (leKey f.kth f.bound && leKey f.bound k) = true := by
    simpa [blockCannotImprove] using h
  have hp := (Bool.and_eq_true (leKey f.kth f.bound) (leKey f.bound k)).mp hh
  have hle : leKey f.kth k = true :=
    leKey_trans f.kth f.bound k hsKB (hsKB.symm.trans hsk.symm) hp.1 hp.2
  exact leKey_not_better f.kth k hle

theorem blockCannotImproveTies_sound (f : Fence) (k : Key)
    (hsKB : specOf f.kth = specOf f.bound) (hsk : specOf k = specOf f.kth)
    (h : blockCannotImproveTies f k = true) :
    better f.kth k = true := by
  have hh : (better f.kth f.bound && leKey f.bound k) = true := by
    simpa [blockCannotImproveTies] using h
  have hp := (Bool.and_eq_true (better f.kth f.bound) (leKey f.bound k)).mp hh
  exact better_of_better_of_leKey f.kth f.bound k hsKB
    (hsKB.symm.trans hsk.symm) hp.1 hp.2

/-- A block whose optimistic bound equals the K-th key is *not* skipped
    under WITH TIES, so all its tied rows survive. -/
theorem with_ties_equal_boundary_kept (f : Fence) (k : Key)
    (heq : f.bound = f.kth) : blockCannotImproveTies f k = false := by
  simp [blockCannotImproveTies, heq, better_irrefl]

/- ------------------------------------------------------------------------ -
    Per-term best bound: the optimistic combination is conservative even
    when it is not an actual row.
    ------------------------------------------------------------------------ -/

/-- Best value for a term over a block, accumulated left to right. -/
def bestValGo (d : Dir) (n : Nulls) (acc : Option Int) : List (Option Int) → Option Int
  | [] => acc
  | v :: vs => bestValGo d n (if valLt d n acc v then acc else v) vs

def bestVal (d : Dir) (n : Nulls) : List (Option Int) → Option Int
  | [] => none
  | v :: vs => bestValGo d n v vs

theorem bestValGo_is_bound (d : Dir) (n : Nulls) (acc : Option Int) :
    ∀ vs : List (Option Int),
      valLe d n (bestValGo d n acc vs) acc = true ∧
        (∀ v ∈ vs, valLe d n (bestValGo d n acc vs) v = true) := by
  intro vs
  induction vs generalizing acc with
  | nil => exact ⟨by simp [bestValGo, valLe], by simp⟩
  | cons v vs ih =>
      let acc' := if valLt d n acc v then acc else v
      have ih' := ih acc'
      rcases ih' with ⟨hacc', hmem'⟩
      have hacc_le_acc : valLe d n acc' acc = true := by
        by_cases hlt : valLt d n acc v = true
        · simp [acc', hlt, valLe]
        · have hvle : valLe d n v acc = true := valLe_of_not_lt d n hlt
          simpa [acc', hlt] using hvle
      have hacc_le_v : valLe d n acc' v = true := by
        by_cases hlt : valLt d n acc v = true
        · simp [acc', hlt, valLe]
        · simp [acc', hlt, valLe]
      constructor
      · exact valLe_trans d n hacc' hacc_le_acc
      · intro x hx
        rcases List.mem_cons.mp hx with rfl | hxvs
        · exact valLe_trans d n hacc' hacc_le_v
        · exact hmem' x hxvs

theorem bestVal_is_bound (d : Dir) (n : Nulls) :
    ∀ (vs : List (Option Int)) (v : Option Int), v ∈ vs →
      valLe d n (bestVal d n vs) v = true := by
  intro vs
  cases vs with
  | nil => intro v hv; simp at hv
  | cons v0 vs =>
      intro v hv
      have hb := bestValGo_is_bound d n v0 vs
      rcases List.mem_cons.mp hv with rfl | hvtail
      · simpa [bestVal] using hb.1
      · simpa [bestVal] using hb.2 v hvtail

/-- Term-wise bound: each bound term is no greater than the corresponding
    row term in its own direction / NULL placement. -/
def dominates : Key → Key → Bool
  | [], [] => true
  | [], _ :: _ => true
  | _ :: _, [] => true
  | b :: bs, r :: rs => valLe b.dir b.nulls b.value r.value && dominates bs rs

/-- Term-wise dominance implies lexicographic `≤`. -/
theorem dominates_leKey (bound row : Key) (hs : specOf bound = specOf row)
    (h : dominates bound row = true) : leKey bound row = true := by
  cases bound with
  | nil =>
      cases row with
      | nil => simp [leKey, keyLe, specOf, valsOf]
      | cons r rs => simp [specOf] at hs
  | cons b bs =>
      cases row with
      | nil => simp [specOf] at hs
      | cons r rs =>
          have hh : (valLe b.dir b.nulls b.value r.value && dominates bs rs) = true := by
            simpa [dominates] using h
          have hp := (Bool.and_eq_true (valLe b.dir b.nulls b.value r.value)
            (dominates bs rs)).mp hh
          have hst : specOf bs = specOf rs := by
            simp [specOf] at hs
            exact hs.2
          have iht := dominates_leKey bs rs hst hp.2
          have hp1 : valLt b.dir b.nulls b.value r.value = true ∨ b.value = r.value := by
            simpa [valLe] using hp.1
          rcases hp1 with hlt | heq
          · simp [leKey, keyLe, specOf, valsOf, hlt]
          · have hlt' : valLt b.dir b.nulls r.value r.value = false := by
              simpa [heq] using valLt_irrefl b.dir b.nulls b.value
            simpa [leKey, keyLe, specOf, valsOf, hlt', heq] using iht

theorem getD_drop_zero {α : Type} (l : List α) (i : Nat) (d : α)
    (hi : i < l.length) : (l.drop i).getD 0 d = l.getD i d := by
  induction l generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      cases i with
      | zero => simp
      | succ i =>
          have hi' : i < xs.length := by simpa using hi
          exact ih i hi'

/-- Term-wise best combination over a list of keys. -/
def termWiseBestGo (i : Nat) (rows : List Key) : List (Dir × Nulls) → Key
  | [] => []
  | (d, n) :: spec =>
      let defTerm : Term := ⟨none, d, n⟩
      let v := bestVal d n (rows.map (fun k => (k.getD i defTerm).value))
      ⟨v, d, n⟩ :: termWiseBestGo (i + 1) rows spec

def termWiseBest (spec : List (Dir × Nulls)) (rows : List Key) : Key :=
  termWiseBestGo 0 rows spec

theorem termWiseBestGo_spec (i : Nat) (spec : List (Dir × Nulls)) (rows : List Key) :
    specOf (termWiseBestGo i rows spec) = spec := by
  induction spec generalizing i rows with
  | nil => simp [termWiseBestGo, specOf]
  | cons s spec ih =>
      rcases s with ⟨d, n⟩
      change (d, n) :: specOf (termWiseBestGo (i + 1) rows spec) = (d, n) :: spec
      simp [ih (i + 1) rows]

theorem termWiseBest_spec (spec : List (Dir × Nulls)) (rows : List Key) :
    specOf (termWiseBest spec rows) = spec := by
  simpa [termWiseBest] using termWiseBestGo_spec 0 spec rows

theorem termWiseBestGo_dominates (i : Nat) (spec : List (Dir × Nulls)) (rows : List Key)
    (hlen : ∀ k ∈ rows, i + spec.length ≤ k.length) :
    ∀ k ∈ rows, dominates (termWiseBestGo i rows spec) (k.drop i) = true := by
  induction spec generalizing i rows with
  | nil =>
      intro k hk
      cases hdrop : k.drop i with
      | nil => rfl
      | cons r rs => rfl
  | cons s spec ih =>
      rcases s with ⟨d, n⟩
      intro k hk
      let defTerm : Term := ⟨none, d, n⟩
      have hlenk : i + ((d, n) :: spec).length ≤ k.length := hlen k hk
      have hilt : i < k.length := by
        simp at hlenk
        omega
      have hkv : (k.drop i).getD 0 defTerm = k.getD i defTerm :=
        getD_drop_zero k i defTerm hilt
      have hvbound : valLe d n
          (bestVal d n (rows.map (fun z => (z.getD i defTerm).value)))
          ((k.drop i).getD 0 defTerm).value = true := by
        apply bestVal_is_bound
        exact List.mem_map.mpr ⟨k, hk, by simp⟩
      have hlenTail : ∀ z ∈ rows, (i + 1) + spec.length ≤ z.length := by
        intro z hz
        have hz' := hlen z hz
        simp at hz'
        omega
      have htailIH := ih (i + 1) rows hlenTail k hk
      have htail' : dominates (termWiseBestGo (i + 1) rows spec)
          ((k.drop i).drop 1) = true := by
        simpa [List.drop_drop, Nat.add_assoc] using htailIH
      cases hdropshape : k.drop i with
      | nil => rfl
      | cons r rs =>
          have hr : r = k.getD i defTerm := by simpa [hdropshape] using hkv
          have hrs : rs = (k.drop i).drop 1 := by simp [hdropshape]
          have hvbound' : valLe d n
              (bestVal d n (rows.map (fun z => (z.getD i defTerm).value)))
              r.value = true := by
            rw [hr, ← hkv]
            exact hvbound
          have htail'' : dominates (termWiseBestGo (i + 1) rows spec) rs = true := by
            simpa [hrs] using htail'
          simp [termWiseBestGo, dominates, htail'']
          exact hvbound'

/-- The term-wise best combination is at or below every key in the block
    (an optimistic lexicographic lower bound), even if no row realises it. -/
theorem termWiseBest_is_bound (spec : List (Dir × Nulls)) (rows : List Key)
    (hlen : ∀ k ∈ rows, specOf k = spec) :
    ∀ k ∈ rows, leKey (termWiseBest spec rows) k = true := by
  intro k hk
  have hlenk : ∀ z ∈ rows, 0 + spec.length ≤ z.length := by
    intro z hz
    have hz' := hlen z hz
    rw [← hz']
    simp [specOf]
  have hd := termWiseBestGo_dominates 0 spec rows hlenk k hk
  have hd' : dominates (termWiseBest spec rows) k = true := by
    simpa [termWiseBest] using hd
  have hspec : specOf (termWiseBest spec rows) = specOf k := by
    rw [termWiseBest_spec, hlen k hk]
  exact dominates_leKey (termWiseBest spec rows) k hspec hd'

/- ------------------------------------------------------------------------ -
    Reference Top-N and the main same-answer theorem
    ------------------------------------------------------------------------ -/

/-- Number of rows strictly better than `r`. -/
def rank (rows : List Key) (r : Key) : Nat :=
  (rows.filter (fun s => better s r)).length

/-- Reference Top-N (WITH TIES): keep a row iff fewer than K rows are
    strictly better than it.  This keeps at least K rows and every row tied
    with the K-th. -/
def refTopN (K : Nat) (rows : List Key) : List Key :=
  rows.filter (fun r => rank rows r < K)

theorem rank_append (p q : List Key) (r : Key) :
    rank (p ++ q) r = rank p r + rank q r := by
  simp [rank, List.filter_append]

theorem filter_false_nil {α : Type} (p : α → Bool) :
    ∀ (l : List α), (∀ x ∈ l, p x = false) → l.filter p = [] := by
  intro l h
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = false := h x (by simp)
      have ht : ∀ y ∈ xs, p y = false := fun y hy => h y (by simp [hy])
      simp [hx, ih ht]

theorem length_filter_le_filter {α : Type} (p : List α) (f g : α → Bool)
    (h : ∀ x ∈ p, f x = true → g x = true) :
    (p.filter f).length ≤ (p.filter g).length := by
  induction p with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y = true → g y = true := fun y hy => h y (by simp [hy])
      have ih' := ih ht
      by_cases hf : f x = true
      · have hg : g x = true := hx hf
        simp [hf, hg]
        omega
      · cases hg : g x with
        | false => simpa [hf, hg] using ih'
        | true =>
            simp [hf, hg]
            omega

/-- The K-th threshold has already been fixed: at least K rows are present
    and at least K rows are no worse than the threshold. -/
def validKth (K : Nat) (kth : Key) (rows : List Key) : Prop :=
  K ≤ rows.length ∧
    (rows.filter (fun r => better r kth)).length < K ∧
      K ≤ (rows.filter (fun r => leKey r kth)).length

theorem rank_ge_of_better (K : Nat) (kth r : Key) (pre : List Key)
    (hk : validKth K kth pre)
    (hsk : ∀ t ∈ pre, specOf t = specOf kth)
    (hsr : specOf r = specOf kth)
    (hbr : better kth r = true) :
    K ≤ rank pre r := by
  have hlarge : K ≤ (pre.filter (fun t => leKey t kth)).length := hk.2.2
  have hsub : (pre.filter (fun t => leKey t kth)).length ≤
      (pre.filter (fun t => better t r)).length := by
    apply length_filter_le_filter
    intro t ht hle'
    have hstk : specOf t = specOf kth := hsk t ht
    exact better_of_leKey_of_better t kth r hstk hsr.symm hle' hbr
  have : K ≤ (pre.filter (fun t => better t r)).length := Nat.le_trans hlarge hsub
  simpa [rank] using this

theorem rank_remaining_zero (K : Nat) (kth r : Key)
    (pre remaining : List Key)
    (hrp : rank pre r < K)
    (hk : validKth K kth pre)
    (hsk : ∀ t ∈ pre, specOf t = specOf kth)
    (hsr : specOf r = specOf kth)
    (hss : ∀ s ∈ remaining, specOf s = specOf kth)
    (hrem : ∀ s ∈ remaining, better kth s = true) :
    rank remaining r = 0 := by
  have hnone : ∀ s ∈ remaining, better s r = false := by
    intro s hs
    cases hsr' : better s r with
    | false => rfl
    | true =>
        have hks : better kth s = true := hrem s hs
        have hleks : leKey kth s = true := leKey_of_better kth s hks
        have hssk : specOf s = specOf kth := hss s hs
        have hskr : specOf kth = specOf r := hsr.symm
        have hsr_spec : specOf s = specOf r := hssk.trans hskr
        have hkr : better kth r = true :=
          better_of_leKey_of_better kth s r hssk.symm hsr_spec hleks hsr'
        have hrank := rank_ge_of_better K kth r pre hk hsk hsr hkr
        omega
  have hf : remaining.filter (fun s => better s r) = [] :=
    filter_false_nil (fun s => better s r) remaining hnone
  simp [rank, hf]

theorem filter_append_stable {α : Type} (p q : List α) (f g : α → Bool)
    (hstable : ∀ x ∈ p, f x = g x)
    (hdrop : ∀ x ∈ q, f x = false) :
    (p ++ q).filter f = p.filter g := by
  rw [List.filter_append]
  have hp : p.filter f = p.filter g := by
    apply List.filter_congr
    intro x hx
    exact hstable x hx
  rw [hp]
  have hq : q.filter f = [] := filter_false_nil f q hdrop
  rw [hq, List.append_nil]

/-- Once K rows are fixed and every remaining row is strictly worse than the
    K-th key, reading the remainder cannot change the reference WITH-TIES
    Top-N. -/
theorem refTopN_pre_eq (K : Nat) (kth : Key) (pre remaining : List Key)
    (hk : validKth K kth pre)
    (hsk : ∀ t ∈ pre, specOf t = specOf kth)
    (hss : ∀ s ∈ remaining, specOf s = specOf kth)
    (hrem : ∀ s ∈ remaining, better kth s = true) :
    refTopN K (pre ++ remaining) = refTopN K pre := by
  unfold refTopN
  apply filter_append_stable pre remaining
  · intro r hrp
    have hsr : specOf r = specOf kth := hsk r hrp
    by_cases hkp : rank pre r < K
    · have hz : rank remaining r = 0 :=
        rank_remaining_zero K kth r pre remaining hkp hk hsk hsr hss hrem
      have happ : rank (pre ++ remaining) r = rank pre r := by
        rw [rank_append, hz, Nat.add_zero]
      simp [happ, hkp]
    · have hge : K ≤ rank (pre ++ remaining) r := by
        rw [rank_append]
        omega
      have hnlt : ¬ rank (pre ++ remaining) r < K := by omega
      simp [hkp, hnlt]
  · intro s hs
    have hsx : specOf s = specOf kth := hss s hs
    have hge : K ≤ rank pre s :=
      rank_ge_of_better K kth s pre hk hsk hsx (hrem s hs)
    have hgeapp : K ≤ rank (pre ++ remaining) s := by
      rw [rank_append]
      omega
    simp [Nat.not_lt.mpr hgeapp]

/-- WITH TIES preservation: a row in the prefix whose key ties the K-th key
    is kept by the reference Top-N. -/
theorem tie_row_kept (K : Nat) (kth row : Key) (pre : List Key)
    (hk : validKth K kth pre)
    (hsp : ∀ t ∈ pre, specOf t = specOf kth)
    (hrow : row ∈ pre) (hsrow : specOf row = specOf kth)
    (htie : leKey row kth = true ∧ leKey kth row = true) :
    row ∈ refTopN K pre := by
  have hsub : (pre.filter (fun s => better s row)).length ≤
      (pre.filter (fun s => better s kth)).length := by
    apply length_filter_le_filter
    intro s hs hbetter
    have hss : specOf s = specOf kth := hsp s hs
    have hsrow' : specOf s = specOf row := hss.trans hsrow.symm
    exact better_of_better_of_leKey s row kth hsrow' hsrow hbetter htie.1
  have hrle : rank pre row ≤ rank pre kth := by
    simpa [rank] using hsub
  have hrlt : rank pre row < K := Nat.lt_of_le_of_lt hrle hk.2.1
  simp [refTopN, hrlt, hrow]

/-- **Fence soundness with the reference Top-N.**  When K rows are fixed,
    the term-wise bound lies between every remaining row and the K-th row,
    and is strictly worse than the K-th, the fence certifies that no
    remaining row can improve or even tie, and the reference answer is
    unchanged. -/
theorem topn_block_fence_sound (K : Nat) (pre remaining : List Key) (f : Fence)
    (hsKB : specOf f.kth = specOf f.bound)
    (hsp : ∀ t ∈ pre, specOf t = specOf f.bound)
    (hsr : ∀ s ∈ remaining, specOf s = specOf f.bound)
    (hk : validKth K f.kth pre)
    (hbound : ∀ s ∈ remaining, leKey f.bound s = true)
    (hstrict : better f.kth f.bound = true) :
    (∀ s ∈ remaining,
        blockCannotImprove f s = true ∧
          better s f.kth = false ∧
          blockCannotImproveTies f s = true ∧ better f.kth s = true) ∧
      refTopN K (pre ++ remaining) = refTopN K pre := by
  constructor
  · intro s hs
    have hbs : specOf f.bound = specOf s := (hsr s hs).symm
    have hks : specOf f.kth = specOf s := hsKB.trans hbs
    have hleKS : leKey f.kth s = true :=
      leKey_trans f.kth f.bound s hsKB hbs (leKey_of_better f.kth f.bound hstrict)
        (hbound s hs)
    have hnot : better s f.kth = false := leKey_not_better f.kth s hleKS
    have hbci : blockCannotImprove f s = true := by
      simp [blockCannotImprove, leKey_of_better f.kth f.bound hstrict, hbound s hs]
    have hbciT : blockCannotImproveTies f s = true := by
      simp [blockCannotImproveTies, hstrict, hbound s hs]
    have hbt : better f.kth s = true :=
      better_of_better_of_leKey f.kth f.bound s hsKB hbs
        hstrict (hbound s hs)
    exact ⟨hbci, hnot, hbciT, hbt⟩
  · have hsk : ∀ t ∈ pre, specOf t = specOf f.kth := by
      intro t ht
      exact (hsp t ht).trans hsKB.symm
    have hss : ∀ s ∈ remaining, specOf s = specOf f.kth := by
      intro s hs
      exact (hsr s hs).trans hsKB.symm
    have hrem : ∀ s ∈ remaining, better f.kth s = true := by
      intro s hs
      exact better_of_better_of_leKey f.kth f.bound s hsKB (hsr s hs).symm
        hstrict (hbound s hs)
    exact refTopN_pre_eq K f.kth pre remaining hk hsk hss hrem

/- ------------------------------------------------------------------------ -
    Concrete boundary examples
    ------------------------------------------------------------------------ -/

/-- ORDER BY a DESC, b ASC: a is equal, only b improves inside the block.
    The term-wise best bound (b = 1) is not a real row but is still a
    conservative lexicographic lower bound, so the plain fence does not skip
    the block. -/
theorem mixed_order_boundary_example :
    let spec : List (Dir × Nulls) := [(Dir.DESC, Nulls.Last), (Dir.ASC, Nulls.Last)]
    let kth : Key :=
      [⟨some 5, Dir.DESC, Nulls.Last⟩, ⟨some 3, Dir.ASC, Nulls.Last⟩]
    let bound : Key :=
      [⟨some 5, Dir.DESC, Nulls.Last⟩, ⟨some 1, Dir.ASC, Nulls.Last⟩]
    let row : Key :=
      [⟨some 5, Dir.DESC, Nulls.Last⟩, ⟨some 2, Dir.ASC, Nulls.Last⟩]
    (specOf row = spec ∧ specOf bound = spec ∧ specOf kth = spec) ∧
    better row kth = true ∧ leKey bound row = true ∧
    blockCannotImprove ⟨bound, kth⟩ row = false := by
  decide

/-- WITH TIES: a block whose bound equals the K-th key is kept, and a tied
    row is present in the reference Top-N. -/
theorem with_ties_equal_boundary_kept_example :
    let kth : Key := [⟨some 1, Dir.ASC, Nulls.Last⟩]
    let row : Key := [⟨some 1, Dir.ASC, Nulls.Last⟩]
    let f : Fence := ⟨kth, kth⟩
    blockCannotImproveTies f row = false ∧
      rank [kth] row = 0 ∧ row ∈ refTopN 1 [kth, row] := by
  decide

/-- NULLS FIRST vs NULLS LAST with mixed directions is a total comparison. -/
theorem nulls_placement_example :
    let nf : Key := [⟨none, Dir.ASC, Nulls.First⟩]
    let vf : Key := [⟨some 7, Dir.ASC, Nulls.First⟩]
    let nl : Key := [⟨none, Dir.ASC, Nulls.Last⟩]
    let vl : Key := [⟨some 7, Dir.ASC, Nulls.Last⟩]
    let df : Key := [⟨some 7, Dir.DESC, Nulls.First⟩]
    better nf vf = true ∧ better vl nl = true ∧
      better vf nf = false ∧ better nl vl = false ∧ better df vf = false := by
  decide

/-- Merge-based reference Top-N on two runs of payload rows. A certified
    retained prefix is emitted before any row of the skipped run. -/
private theorem take_merge_left (k : Nat) (xs ys : List α) (cmp : α → α → Bool)
    (len : k ≤ xs.length) (front : ∀ x ∈ xs.take k, ∀ y ∈ ys, cmp x y = true) :
    (xs.merge ys cmp).take k = xs.take k := by
  induction k generalizing xs with
  | zero => simp
  | succ k ih =>
    cases xs with
    | nil => simp at len
    | cons x xs =>
      cases ys with
      | nil => simp
      | cons y ys =>
        have hxy := front x (by simp) y (by simp)
        rw [List.merge, if_pos hxy]
        simp only [List.take_succ_cons, List.cons.injEq, true_and]
        apply ih _ (by simpa using len)
        intro z hz t ht
        exact front z (by simp [hz]) t ht

theorem sorted_payload_fence (key : α → Key) (k : Nat) (pre block : List α) (f : Fence)
    (hkb : specOf f.kth = specOf f.bound)
    (schema : ∀ x ∈ pre ++ block, specOf (key x) = specOf f.kth)
    (enough : k ≤ pre.length)
    (front : ∀ x ∈ (pre.mergeSort (fun a b => leKey (key a) (key b))).take k,
      leKey (key x) f.kth = true)
    (bound : ∀ y ∈ block, leKey f.bound (key y) = true)
    (strict : better f.kth f.bound = true) :
    let cmp := fun a b => leKey (key a) (key b)
    ((pre.mergeSort cmp).merge (block.mergeSort cmp) cmp).take k =
      (pre.mergeSort cmp).take k := by
  intro cmp
  apply take_merge_left _ _ _ _ (by simpa using enough)
  intro x hx y hy
  have hxpre : x ∈ pre := List.mem_mergeSort.mp (List.mem_of_mem_take hx)
  have hyb : y ∈ block := List.mem_mergeSort.mp hy
  have sx := schema x (List.mem_append_left _ hxpre)
  have sy := schema y (List.mem_append_right _ hyb)
  exact leKey_trans _ _ _ sx sy.symm (front x hx)
    (leKey_trans _ _ _ hkb (hkb.symm.trans sy.symm) (leKey_of_better _ _ strict) (bound y hyb))

end TinylambOpt.TopNBlockFence

-- C++ references to this model or its proof.
-- executor/topn.cpp
