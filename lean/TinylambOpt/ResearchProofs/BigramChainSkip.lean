import TinylambOpt.ResearchGoals.BigramChainSkip

/-!
# Soundness of bigram-chain LIKE page skipping

A page carries the 64-bit OR-sketch `summary` of the bigrams (`grams`, adjacent
byte pairs) of every row byte-string it stores.  A LIKE chain `%f1%f2%…` can
only match a row if every fragment `f` is a substring of the row, hence every
bigram of every fragment is a bigram of that row — so if any fragment's sketch
bit is missing from the page header, the page can be skipped safely.

The proof is organised as

* structural `List` lemmas: bigrams of a substring (`append_sublist`) and of a
  suffix (`drop_sublist`) are sublists of the bigrams; chained substrings
  (`chain_grams`) put every mandatory bigram into the matched row;
* a bit-level characterisation of `sketch` / `summary` (`sketch_bit`,
  `summary_bit`) via `getLsbD`, and the single-bit nature of `flag`;
* per-page mode correctness (`incr_eq`: a skipped page contributes no matching
  row) and an `execute` fold characterisation (`foldl_count`, `foldl_opened`,
  `reads_execute`, `mem_insIdx`);
* assembly in `sound`.
-/

namespace TinylambOpt.ResearchProofs.BigramChainSkip

open TinylambOpt.ResearchGoals.BlockCountModel
open TinylambOpt.ResearchGoals.BigramChainSkip
open List

/-! ## Generic `all` / `any` / Bool helpers -/

private theorem all_eq_true_iff {α : Type} :
    ∀ (xs : List α) (p : α → Bool), (xs.all p = true) ↔ ∀ x ∈ xs, p x = true := by
  intro xs
  induction xs with
  | nil => intro p; simp [List.all_nil]
  | cons a as ih =>
      intro p
      rw [List.all_cons, Bool.and_eq_true, ih p]
      simp only [List.mem_cons]
      constructor
      · rintro ⟨h1, h2⟩ x hx
        rcases hx with rfl | hx'
        · exact h1
        · exact h2 x hx'
      · intro h
        exact ⟨h a (Or.inl rfl), fun x hx => h x (Or.inr hx)⟩

private theorem all_eq_false_iff {α : Type} :
    ∀ (xs : List α) (p : α → Bool), (xs.all p = false) ↔ ∃ x ∈ xs, p x = false := by
  intro xs
  induction xs with
  | nil => intro p; simp [List.all_nil]
  | cons a as ih =>
      intro p
      rw [List.all_cons, Bool.and_eq_false_iff, ih p]
      simp only [List.mem_cons]
      constructor
      · rintro (h | h)
        · exact ⟨a, Or.inl rfl, h⟩
        · rcases h with ⟨x, hx, hpx⟩
          exact ⟨x, Or.inr hx, hpx⟩
      · rintro ⟨x, hx, hpx⟩
        rcases hx with rfl | hx'
        · exact Or.inl hpx
        · exact Or.inr ⟨x, hx', hpx⟩

private theorem any_eq_true_iff {α : Type} :
    ∀ (xs : List α) (p : α → Bool), (xs.any p = true) ↔ ∃ x ∈ xs, p x = true := by
  intro xs
  induction xs with
  | nil => intro p; simp [List.any_nil]
  | cons a as ih =>
      intro p
      rw [List.any_cons, Bool.or_eq_true, ih p]
      simp only [List.mem_cons]
      constructor
      · rintro (h | h)
        · exact ⟨a, Or.inl rfl, h⟩
        · rcases h with ⟨x, hx, hpx⟩
          exact ⟨x, Or.inr hx, hpx⟩
      · rintro ⟨x, hx, hpx⟩
        rcases hx with rfl | hx'
        · exact Or.inl hpx
        · exact Or.inr ⟨x, hx', hpx⟩

private theorem bfalse {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | true => exact absurd rfl h
  | false => rfl

/-! ## Structural lemmas about `grams` -/

private theorem grams_nil : grams ([] : Bytes) = [] := rfl

private theorem grams_cons (x : UInt8) : grams [x] = [] := rfl

private theorem grams_cons_cons (x y : UInt8) (z : Bytes) :
    grams (x :: y :: z) = (x, y) :: grams (y :: z) := rfl

private theorem mem_of_sublist {α : Type} {l₁ l₂ : List α} (h : l₁ <+ l₂) :
    ∀ {g : α}, g ∈ l₁ → g ∈ l₂ := fun hg => List.Sublist.mem hg h

/-- Bigrams of a prefix survive an append: they are contiguous in the result. -/
private theorem append_sublist : ∀ (u v : Bytes), grams u <+ grams (u ++ v) := by
  intro u
  induction u with
  | nil => intro v; exact List.nil_sublist _
  | cons x xs ih =>
      intro v
      cases xs with
      | nil =>
          rw [grams_cons]
          exact List.nil_sublist _
      | cons y ys =>
          rw [List.cons_append]
          have hL : grams (x :: y :: ys) = (x, y) :: grams (y :: ys) := grams_cons_cons x y ys
          have hR : grams (x :: ((y :: ys) ++ v)) = (x, y) :: grams ((y :: ys) ++ v) :=
            grams_cons_cons x y (ys ++ v)
          rw [hL, hR]
          exact List.Sublist.cons_cons (x, y) (ih v)

/-- Bigrams of a suffix are bigrams of the whole word (per-word pairs only). -/
private theorem drop_sublist : ∀ (s : Bytes) (i : Nat), grams (s.drop i) <+ grams s := by
  intro s
  induction s with
  | nil => intro i; rw [List.drop_nil, grams_nil]; exact List.Sublist.refl _
  | cons x xs ih =>
      intro i
      cases i with
      | zero => exact List.Sublist.refl _
      | succ i =>
          rw [List.drop_succ_cons]
          cases xs with
          | nil =>
              rw [List.drop_nil, grams_nil, grams_cons]
              exact List.Sublist.refl _
          | cons y ys =>
              rw [grams_cons_cons x y ys]
              exact List.Sublist.cons (x, y) (ih i)

/-! ## `chain` puts every mandatory bigram into the matched row -/

private theorem chain_cons (s t : Bytes) (ts : List Bytes) :
    chain s (t :: ts) =
      (List.range (s.length + 1)).any fun i =>
        decide (t <+: s.drop i) && chain (s.drop (i + t.length)) ts := rfl

private theorem chain_grams (frags : List Bytes) :
    ∀ (s : Bytes), chain s frags = true → ∀ f ∈ frags, grams f <+ grams s := by
  induction frags with
  | nil => intro _ _ f hf; exact absurd hf (by simp)
  | cons t ts ih =>
      intro s hc f hf
      rw [chain_cons] at hc
      obtain ⟨i, _, hP⟩ := (any_eq_true_iff _ _).mp hc
      obtain ⟨hpre, htail⟩ := Bool.and_eq_true_iff.mp hP
      rw [List.mem_cons] at hf
      rcases hf with rfl | hf
      · refine List.Sublist.trans ?_ (drop_sublist s i)
        obtain ⟨u, hu⟩ := of_decide_eq_true hpre
        rw [← hu]
        exact append_sublist _ u
      · exact List.Sublist.trans (ih _ htail f hf) (drop_sublist s _)

/-! ## Bit-level characterisation of the sketch folds -/

private theorem foldl_flag_bits (l : List (UInt8 × UInt8)) :
    ∀ (acc : W) (i : Nat),
      (l.foldl (fun a p => a ||| flag p) acc).getLsbD i =
        (acc.getLsbD i || l.any (fun p => (flag p).getLsbD i)) := by
  induction l with
  | nil => intro acc i; cases h : acc.getLsbD i <;> simp [h, List.any_nil]
  | cons p ps ih =>
      intro acc i
      rw [List.foldl_cons, ih (acc ||| flag p) i, List.any_cons, BitVec.getLsbD_or]
      cases h1 : acc.getLsbD i <;> cases h2 : (flag p).getLsbD i <;>
        cases h3 : ps.any (fun q => (flag q).getLsbD i) <;> exact rfl

private theorem sketch_bit (s : Bytes) (i : Nat) :
    (sketch s).getLsbD i = (grams s).any (fun p => (flag p).getLsbD i) := by
  unfold sketch
  rw [foldl_flag_bits]
  rw [show BitVec.getLsbD (0 : W) i = false from BitVec.getLsbD_zero]
  rfl

private theorem summary_bit (xs : List (Option Bytes)) (i : Nat) :
    (summary xs).getLsbD i = xs.any (fun x => (sketch (x.getD [])).getLsbD i) := by
  have key : ∀ (acc : W), (xs.foldl (fun a x => a ||| sketch (x.getD [])) acc).getLsbD i =
      (acc.getLsbD i || xs.any (fun x => (sketch (x.getD [])).getLsbD i)) := by
    induction xs with
    | nil => intro acc; cases h : acc.getLsbD i <;> simp [h, List.any_nil]
    | cons x xs ih =>
        intro acc
        rw [List.foldl_cons, ih (acc ||| sketch (x.getD [])), List.any_cons,
          BitVec.getLsbD_or]
        cases h1 : acc.getLsbD i <;> cases h2 : (sketch (x.getD [])).getLsbD i <;>
          cases h3 : xs.any (fun y => (sketch (y.getD [])).getLsbD i) <;> exact rfl
  unfold summary
  rw [key]
  rw [show BitVec.getLsbD (0 : W) i = false from BitVec.getLsbD_zero]
  rfl

/-- `flag g` is exactly the single bit at position `(17*g.1+g.2) % 64`. -/
private theorem flag_getLsbD (g : UInt8 × UInt8) (i : Nat) :
    (flag g).getLsbD i = decide (i = (17 * g.1.toNat + g.2.toNat) % 64) := by
  have hk : (17 * g.1.toNat + g.2.toNat) % 64 < 64 := Nat.mod_lt _ (by decide)
  have h1 : ∀ j : Nat, BitVec.getLsbD (1 : W) j = (decide (0 < 64) && decide (j = 0)) :=
    fun j => BitVec.getLsbD_one
  unfold flag
  rw [BitVec.getLsbD_shiftLeft, h1]
  rcases Nat.lt_trichotomy i ((17 * g.1.toNat + g.2.toNat) % 64) with hi | hi | hi
  · rw [decide_eq_true (by omega : i < 64), decide_eq_true hi, Bool.not_true,
      decide_eq_true (by omega : i - (17 * g.1.toNat + g.2.toNat) % 64 = 0),
      show decide (0 < 64) = true from rfl,
      decide_eq_false (by omega : ¬(i = (17 * g.1.toNat + g.2.toNat) % 64))]
    simp
  · subst hi
    rw [decide_eq_true hk, decide_eq_false (Nat.lt_irrefl _), Bool.not_false,
      Nat.sub_self, decide_eq_true rfl, show decide (0 < 64) = true from rfl,
      decide_eq_true rfl]
    simp
  · rw [decide_eq_false (by omega : ¬(i < (17 * g.1.toNat + g.2.toNat) % 64)), Bool.not_false,
      decide_eq_false (by omega : ¬(i - (17 * g.1.toNat + g.2.toNat) % 64 = 0)),
      show decide (0 < 64) = true from rfl,
      decide_eq_false (by omega : ¬(i = (17 * g.1.toNat + g.2.toNat) % 64))]
    simp

/-! ## Per-page mode correctness -/

private def cls (frags : List Bytes) : W → Mode :=
  fun m => if candidate frags m then .inspect else .skip

private theorem cls_inspect {frags : List Bytes} {m : W} (h : candidate frags m = true) :
    cls frags m = .inspect := by
  unfold cls
  rw [ite_eq_left h]

private theorem cls_skip_of_eq {frags : List Bytes} {m : W} (h : cls frags m = .skip) :
    candidate frags m = false := by
  unfold cls at h
  by_cases hc : candidate frags m
  · rw [ite_eq_left hc] at h
    exact absurd h (by decide)
  · exact bfalse hc

private theorem cls_ne_all (frags : List Bytes) (m : W) : cls frags m ≠ .all := by
  unfold cls
  cases h : candidate frags m <;> simp

/-- A row that matches the chain makes its page a candidate: every mandatory
    bigram bit is present in the page header. -/
private theorem test_candidate (frags : List Bytes) (s : Bytes) (xs : List (Option Bytes))
    (h : chain s frags = true) (hx : some s ∈ xs) :
    candidate frags (summary xs) = true := by
  rw [candidate, all_eq_true_iff]
  intro f hf
  have hbit : ∀ i, (sketch f).getLsbD i = true → (summary xs).getLsbD i = true := by
    intro i hi
    rw [sketch_bit] at hi
    obtain ⟨g, hg, hbit⟩ := (any_eq_true_iff _ _).mp hi
    have hgs : g ∈ grams s := mem_of_sublist (chain_grams frags s h f hf) hg
    rw [summary_bit]
    refine (any_eq_true_iff _ _).mpr ⟨some s, hx, ?_⟩
    rw [sketch_bit]
    exact (any_eq_true_iff _ _).mpr ⟨g, hgs, hbit⟩
  rw [show ((sketch f &&& summary xs) == sketch f) =
        decide (sketch f &&& summary xs = sketch f) from rfl]
  refine decide_eq_true (BitVec.eq_of_getLsbD_eq ?_)
  intro j _
  rw [BitVec.getLsbD_and]
  cases h1 : (sketch f).getLsbD j with
  | false => exact rfl
  | true =>
      have h2 : (summary xs).getLsbD j = true := hbit j h1
      rw [h2]
      exact rfl

/-- If a header misses the candidate test, some mandatory bigram bit is absent. -/
private theorem candidate_false (frags : List Bytes) (s : W)
    (h : candidate frags s = false) :
    ∃ f ∈ frags, ∃ g ∈ grams f, (flag g &&& s) = 0 := by
  rw [candidate, all_eq_false_iff] at h
  obtain ⟨f, hf, hne⟩ := h
  rw [show ((sketch f &&& s) == sketch f) = decide (sketch f &&& s = sketch f) from rfl] at hne
  have hne' : sketch f &&& s ≠ sketch f := of_decide_eq_false hne
  obtain ⟨i, hi1, hi2⟩ : ∃ i, (sketch f).getLsbD i = true ∧ s.getLsbD i = false := by
    rcases Classical.em (∃ i, (sketch f).getLsbD i = true ∧ s.getLsbD i = false) with he | he
    · exact he
    · exfalso
      apply hne'
      refine BitVec.eq_of_getLsbD_eq (x := sketch f &&& s) (y := sketch f) ?_
      intro j _
      rw [BitVec.getLsbD_and]
      cases h1 : (sketch f).getLsbD j with
      | false => exact rfl
      | true =>
          cases h2 : s.getLsbD j with
          | false => exact absurd ⟨j, h1, h2⟩ he
          | true => exact rfl
  rw [sketch_bit] at hi1
  obtain ⟨g, hg, hbit⟩ := (any_eq_true_iff _ _).mp hi1
  refine ⟨f, hf, g, hg, ?_⟩
  obtain rfl : i = (17 * g.1.toNat + g.2.toNat) % 64 := by
    rw [flag_getLsbD] at hbit
    exact of_decide_eq_true hbit
  refine BitVec.eq_of_getLsbD_eq (x := flag g &&& s) (y := 0) ?_
  intro j _
  rw [BitVec.getLsbD_and, show BitVec.getLsbD (0 : W) j = false from BitVec.getLsbD_zero,
    flag_getLsbD]
  by_cases hj : j = (17 * g.1.toNat + g.2.toNat) % 64
  · subst hj
    rw [decide_eq_true rfl, hi2]
    exact rfl
  · rw [decide_eq_false hj]
    exact Bool.false_and _

/-! ## `execute` fold characterisation -/

private def step (cl : W → Mode) (test : Option Bytes → Bool) (s : Result)
    (b : Block (Option Bytes) W) (i : Nat) : Result :=
  match cl b.header with
  | .skip => s
  | .all => {s with count := s.count + b.size}
  | .inspect => ⟨s.count + (b.data.filter test).length, i :: s.opened⟩

private def incr (cl : W → Mode) (test : Option Bytes → Bool)
    (b : Block (Option Bytes) W) : Nat :=
  match cl b.header with
  | .skip => 0
  | .all => b.size
  | .inspect => (b.data.filter test).length

private def insIdx (cl : W → Mode) :
    (pages : List (Block (Option Bytes) W)) → Nat → List Nat
  | [], _ => []
  | b :: bs, n => if cl b.header = .inspect then n :: insIdx cl bs (n + 1) else insIdx cl bs (n + 1)

private theorem execute_eq_fold (cl : W → Mode) (test : Option Bytes → Bool)
    (pages : List (Block (Option Bytes) W)) :
    execute cl test pages =
      (List.zipIdx pages 0).foldl (fun s p => step cl test s p.1 p.2) ({} : Result) := rfl

private theorem step_eq_match (cl : W → Mode) (test : Option Bytes → Bool) (s : Result)
    (b : Block (Option Bytes) W) (i : Nat) :
    step cl test s b i =
      match cl b.header with
      | .skip => s
      | .all => {s with count := s.count + b.size}
      | .inspect => ⟨s.count + (b.data.filter test).length, i :: s.opened⟩ := rfl

private theorem insIdx_cons_true (cl : W → Mode) (b : Block (Option Bytes) W)
    (bs : List (Block (Option Bytes) W)) (n : Nat) (h : cl b.header = .inspect) :
    insIdx cl (b :: bs) n = n :: insIdx cl bs (n + 1) := by
  simp only [insIdx, ite_eq_left h]

private theorem insIdx_cons_false (cl : W → Mode) (b : Block (Option Bytes) W)
    (bs : List (Block (Option Bytes) W)) (n : Nat) (h : ¬(cl b.header = .inspect)) :
    insIdx cl (b :: bs) n = insIdx cl bs (n + 1) := by
  simp only [insIdx, ite_eq_right h]

private theorem mem_insIdx (cl : W → Mode) :
    ∀ (pages : List (Block (Option Bytes) W)) (n i : Nat),
      i ∈ insIdx cl pages n ↔ ∃ p ∈ List.zipIdx pages n, p.2 = i ∧ cl p.1.header = .inspect := by
  intro pages
  induction pages with
  | nil => intro n i; simp [insIdx]
  | cons b bs ih =>
      intro n i
      rw [List.zipIdx_cons]
      simp only [List.mem_cons]
      by_cases hb : cl b.header = .inspect
      · rw [insIdx_cons_true cl b bs n hb]
        simp only [List.mem_cons, ih (n + 1) i]
        constructor
        · rintro (heq | ⟨p, hp, hpi, hclp⟩)
          · exact ⟨(b, n), Or.inl rfl, heq.symm, hb⟩
          · exact ⟨p, Or.inr hp, hpi, hclp⟩
        · rintro ⟨p, hp, hpi, hclp⟩
          rcases hp with heq | hrest
          · exact Or.inl (hpi ▸ congrArg Prod.snd heq)
          · exact Or.inr ⟨p, hrest, hpi, hclp⟩
      · rw [insIdx_cons_false cl b bs n hb, ih (n + 1) i]
        constructor
        · rintro ⟨p, hp, hpi, hclp⟩
          exact ⟨p, Or.inr hp, hpi, hclp⟩
        · rintro ⟨p, hp, hpi, hclp⟩
          rcases hp with heq | hrest
          · rw [heq] at hclp
            exact absurd hclp hb
          · exact ⟨p, hrest, hpi, hclp⟩

private theorem foldl_count (cl : W → Mode) (test : Option Bytes → Bool) :
    ∀ (pages : List (Block (Option Bytes) W)) (n : Nat) (s : Result),
      ((List.zipIdx pages n).foldl (fun s p => step cl test s p.1 p.2) s).count =
        (pages.map (incr cl test)).foldl (fun a x => a + x) s.count := by
  intro pages
  induction pages with
  | nil => intro n s; rfl
  | cons b bs ih =>
      intro n s
      rw [List.zipIdx_cons, List.foldl_cons, ih (n + 1) (step cl test s b n),
        List.map_cons, List.foldl_cons]
      match h : cl b.header with
      | .skip =>
          rw [show (step cl test s b n).count = s.count from by rw [step_eq_match, h],
            show incr cl test b = (0 : Nat) from by unfold incr; rw [h],
            Nat.add_zero]
      | .all =>
          rw [show (step cl test s b n).count = s.count + b.size from by rw [step_eq_match, h],
            show incr cl test b = b.size from by unfold incr; rw [h]]
      | .inspect =>
          rw [show (step cl test s b n).count = s.count + (b.data.filter test).length
            from by rw [step_eq_match, h],
            show incr cl test b = (b.data.filter test).length from by unfold incr; rw [h]]

private theorem foldl_opened (cl : W → Mode) (test : Option Bytes → Bool) :
    ∀ (pages : List (Block (Option Bytes) W)) (n : Nat) (s : Result),
      ((List.zipIdx pages n).foldl (fun s p => step cl test s p.1 p.2) s).opened =
        (insIdx cl pages n).reverse ++ s.opened := by
  intro pages
  induction pages with
  | nil => intro n s; rfl
  | cons b bs ih =>
      intro n s
      rw [List.zipIdx_cons, List.foldl_cons, ih (n + 1) (step cl test s b n)]
      match h : cl b.header with
      | .skip =>
          have hn : ¬(cl b.header = .inspect) := by rw [h]; decide
          rw [insIdx_cons_false cl b bs n hn,
            show step cl test s b n = s from by rw [step_eq_match, h]]
      | .all =>
          have hn : ¬(cl b.header = .inspect) := by rw [h]; decide
          rw [insIdx_cons_false cl b bs n hn,
            show (step cl test s b n).opened = s.opened from by rw [step_eq_match, h]]
      | .inspect =>
          rw [insIdx_cons_true cl b bs n h, List.reverse_cons, List.append_assoc,
            show (step cl test s b n).opened = n :: s.opened from by rw [step_eq_match, h],
            show ([n] ++ s.opened) = (n :: s.opened) from rfl]

private theorem reads_execute (cl : W → Mode) (test : Option Bytes → Bool)
    (pages : List (Block (Option Bytes) W)) :
    reads (execute cl test pages) = insIdx cl pages 0 := by
  unfold reads
  rw [execute_eq_fold, foldl_opened cl test pages 0 ({})]
  rw [show ({} : Result).opened = ([] : List Nat) from rfl, List.append_nil,
    List.reverse_reverse]

/-! ## Fold arithmetic helpers -/

private theorem map_congr' {α β : Type} (f g : α → β) :
    ∀ (l : List α), (∀ x ∈ l, f x = g x) → l.map f = l.map g := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a l ih =>
      intro h
      rw [List.map_cons, h a List.mem_cons_self,
        ih (fun x hx => h x (List.mem_cons_of_mem _ hx)), List.map_cons]

private theorem filter_flatten {α : Type} (p : α → Bool) :
    ∀ (L : List (List α)), (L.flatten).filter p = (L.map (fun l => l.filter p)).flatten := by
  intro L
  induction L with
  | nil => rfl
  | cons l L ih =>
      rw [List.flatten_cons, List.filter_append, List.map_cons, ih, List.flatten_cons]

private theorem foldl_add_eq_add_sum (l : List Nat) :
    ∀ acc : Nat, l.foldl (fun a x => a + x) acc = acc + l.sum := by
  induction l with
  | nil => intro acc; rfl
  | cons a l ih =>
      intro acc
      rw [List.foldl_cons, ih (acc + a), List.sum_cons, Nat.add_assoc]

private theorem incr_eq (frags : List Bytes) (b : Block (Option Bytes) W)
    (_hb : b.header = summary b.data)
    (hmode : (∃ row ∈ b.data, row.any (chain · frags) = true) → cls frags b.header = .inspect) :
    incr (cls frags) (fun row => row.any (chain · frags)) b =
      (b.data.filter (fun row => row.any (chain · frags))).length := by
  match h : cls frags b.header with
  | .skip =>
      have hf : b.data.filter (fun row => row.any (chain · frags)) = [] := by
        refine (List.filter_eq_nil_iff).mpr (fun row hrow ht => ?_)
        have h2 := hmode ⟨row, hrow, ht⟩
        rw [h] at h2
        exact absurd h2 (by decide)
      simp only [incr, h, hf, List.length_nil]
  | .all =>
      exact absurd h (cls_ne_all frags b.header)
  | .inspect =>
      simp only [incr, h]

/-! ## The soundness theorem -/

theorem sound : TinylambOpt.ResearchGoals.BigramChainSkip.Goal := by
  intro frags pages hsum
  show ((execute (cls frags) (fun row => row.any (chain · frags)) pages).count =
          ((pages.flatMap Block.data).filter (fun row => row.any (chain · frags))).length) ∧
      ∀ p ∈ List.zipIdx pages 0,
        p.2 ∉ reads (execute (cls frags) (fun row => row.any (chain · frags)) pages) →
          ∃ f ∈ frags, ∃ g ∈ grams f, (flag g &&& p.1.header) = 0
  refine ⟨?_, ?_⟩
  · have hmode : ∀ b ∈ pages,
        (∃ row ∈ b.data, row.any (chain · frags) = true) → cls frags b.header = .inspect := by
      intro b hb ⟨row, hrow, ht⟩
      cases row with
      | none =>
          have h0 : (none : Option Bytes).any (chain · frags) = false := rfl
          rw [h0] at ht
          exact absurd ht (by decide)
      | some s =>
          have h0 : (some s).any (chain · frags) = chain s frags := rfl
          rw [h0] at ht
          refine cls_inspect ?_
          rw [hsum b hb]
          exact test_candidate frags s b.data ht hrow
    have hincr : ∀ b ∈ pages,
        incr (cls frags) (fun row => row.any (chain · frags)) b =
          (b.data.filter (fun row => row.any (chain · frags))).length :=
      fun b hb => incr_eq frags b (hsum b hb) (hmode b hb)
    have hlen : ((pages.flatMap Block.data).filter (fun row => row.any (chain · frags))).length =
        (pages.map (fun b => (b.data.filter (fun row => row.any (chain · frags))).length)).sum := by
      rw [List.flatMap_def, filter_flatten, List.length_flatten, List.map_map, List.map_map]
      rfl
    rw [execute_eq_fold, foldl_count, map_congr' _ _ pages hincr, foldl_add_eq_add_sum,
      show ({} : Result).count = (0 : Nat) from rfl, Nat.zero_add, ← hlen]
  · intro p hpz hnr
    rw [reads_execute (cls frags) (fun row => row.any (chain · frags)) pages] at hnr
    have hnc : candidate frags p.1.header = false := by
      rcases Classical.em (candidate frags p.1.header = true) with hc | hc
      · exact absurd ((mem_insIdx (cls frags) pages 0 p.2).mpr ⟨p, hpz, rfl, cls_inspect hc⟩) hnr
      · exact bfalse hc
    exact candidate_false frags p.1.header hnc

end TinylambOpt.ResearchProofs.BigramChainSkip
