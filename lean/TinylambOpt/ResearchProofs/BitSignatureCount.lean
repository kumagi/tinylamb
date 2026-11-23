import TinylambOpt.ResearchGoals.BitSignatureCount

/-!
# Soundness of AND/OR bit-signature page classification for `COUNT(*)`

Each page carries the exact summary `⟨bitwise AND, bitwise OR⟩` of its 64-bit
rows and the exact row count.  From the query predicate `(x & mask) = (want & mask)`
`classify` derives one of three modes:

* `.skip` — some masked `1`-bit of `want` is `0` in the OR summary (no row has
  it set), or some masked `0`-bit of `want` is `1` in the AND summary (every
  row has it set); then no row of the page matches.
* `.all` — AND and OR agree on every mask bit and neither skip condition fires;
  then every row matches and the header size is added directly.
* `.inspect` — some mask bit where AND and OR disagree survives both skip
  tests; the page is opened, and such a bit witnesses both a `0` and a `1` in
  the page payload.

The proof proceeds per page through these three modes, feeding an
`execute` fold characterization (`foldl_count`, `foldl_opened`) proven here.
-/

namespace TinylambOpt.ResearchProofs.BitSignatureCount

open TinylambOpt.ResearchGoals.BlockCountModel
open TinylambOpt.ResearchGoals.BitSignatureCount
open List

/-! ## Small generic helpers -/

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

/-! ## `all` / `any` membership bridges -/

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

private theorem any_eq_false_iff {α : Type} :
    ∀ (xs : List α) (p : α → Bool), (xs.any p = false) ↔ ∀ x ∈ xs, p x = false := by
  intro xs
  induction xs with
  | nil => intro p; simp [List.any_nil]
  | cons a as ih =>
      intro p
      rw [List.any_cons, Bool.or_eq_false_iff, ih p]
      simp only [List.mem_cons]
      constructor
      · rintro ⟨h1, h2⟩ x hx
        rcases hx with rfl | hx'
        · exact h1
        · exact h2 x hx'
      · intro h
        exact ⟨h a (Or.inl rfl), fun x hx => h x (Or.inr hx)⟩

/-! ## BitVec helpers -/

private theorem getLsbD_zero' (i : Nat) : ((0 : W).getLsbD i) = false :=
  BitVec.getLsbD_zero (i := i)

private theorem bv_exists (v : W) (hv : v ≠ 0) : ∃ i < 64, v.getLsbD i = true := by
  rcases Classical.em (∃ i < 64, v.getLsbD i = true) with h | h
  · exact h
  · exfalso
    refine hv (BitVec.eq_of_getLsbD_eq_iff.mpr (fun i hi => ?_))
    cases h' : v.getLsbD i
    · exact (getLsbD_zero' i).symm
    · exact absurd ⟨i, hi, h'⟩ h

private theorem bv_bne (v : W) : ((v != (0 : W)) = true) ↔ v ≠ 0 := by
  have h1 : (v != (0 : W)) = decide (v ≠ 0) := rfl
  rw [h1]
  exact ⟨of_decide_eq_true, decide_eq_true⟩

private theorem bv_bne_false (v : W) : ((v != (0 : W)) = false) ↔ v = 0 := by
  constructor
  · intro h
    refine Classical.byContradiction (fun hn => ?_)
    have hb := (bv_bne v).mpr hn
    rw [hb] at h
    exact absurd h (by decide)
  · intro he
    rw [he]
    decide

private theorem bv_all_ones_bit (i : Nat) (hi : i < (64 : Nat)) :
    ((~~~(0 : W)) : W).getLsbD i = true := by
  rw [BitVec.getLsbD_not, getLsbD_zero', Bool.not_false, Bool.and_true,
    show (decide (i < (64 : Nat))) = true from decide_eq_true hi]

/-! ## Fold bitwise characterizations of `summary` -/

private theorem fold_and_bit (xs : List W) :
    ∀ (acc : W) (i : Nat),
      (xs.foldl (· &&& ·) acc).getLsbD i = (acc.getLsbD i && xs.all (fun x => x.getLsbD i)) := by
  induction xs with
  | nil =>
      intro acc i
      cases h : acc.getLsbD i <;> simp [h, List.all_nil]
  | cons a as ih =>
      intro acc i
      rw [List.foldl_cons, ih (acc &&& a) i, List.all_cons, BitVec.getLsbD_and]
      cases h1 : acc.getLsbD i <;> cases h2 : a.getLsbD i <;>
        cases h3 : as.all (fun x => x.getLsbD i) <;> simp [h1, h2, h3]

private theorem fold_or_bit (xs : List W) :
    ∀ (acc : W) (i : Nat),
      (xs.foldl (· ||| ·) acc).getLsbD i = (acc.getLsbD i || xs.any (fun x => x.getLsbD i)) := by
  induction xs with
  | nil =>
      intro acc i
      cases h : acc.getLsbD i <;> simp [h, List.any_nil]
  | cons a as ih =>
      intro acc i
      rw [List.foldl_cons, ih (acc ||| a) i, List.any_cons, BitVec.getLsbD_or]
      cases h1 : acc.getLsbD i <;> cases h2 : a.getLsbD i <;>
        cases h3 : as.any (fun x => x.getLsbD i) <;> simp [h1, h2, h3]

private theorem must_bit (xs : List W) (i : Nat) :
    (summary xs).must.getLsbD i = (decide (i < (64 : Nat)) && xs.all (fun x => x.getLsbD i)) := by
  show (xs.foldl (· &&& ·) (~~~(0 : W))).getLsbD i = _
  rw [fold_and_bit, BitVec.getLsbD_not, getLsbD_zero', Bool.not_false, Bool.and_true]

private theorem may_bit (xs : List W) (i : Nat) :
    (summary xs).may.getLsbD i = xs.any (fun x => x.getLsbD i) := by
  show (xs.foldl (· ||| ·) (0 : W)).getLsbD i = _
  rw [fold_or_bit, getLsbD_zero', Bool.false_or]

/-! ## Mode extraction from `classify` -/

private theorem classify_def (mask want : W) (s : Meta) :
    classify mask want s =
      if (mask &&& want) &&& ~~~s.may != 0 || (mask &&& ~~~want) &&& s.must != 0 then .skip
      else if (s.may ^^^ s.must) &&& mask = 0 then .all else .inspect := rfl

private theorem classify_eq_iff (mask want : W) (s : Meta) :
    (classify mask want s = .skip ↔
        (mask &&& want) &&& ~~~s.may ≠ 0 ∨ (mask &&& ~~~want) &&& s.must ≠ 0) ∧
    (classify mask want s = .all ↔
        (mask &&& want) &&& ~~~s.may = 0 ∧ (mask &&& ~~~want) &&& s.must = 0 ∧
          (s.may ^^^ s.must) &&& mask = 0) ∧
    (classify mask want s = .inspect ↔
        (mask &&& want) &&& ~~~s.may = 0 ∧ (mask &&& ~~~want) &&& s.must = 0 ∧
          (s.may ^^^ s.must) &&& mask ≠ 0) := by
  rw [classify_def]
  rcases Classical.em ((mask &&& want) &&& ~~~s.may ≠ 0 ∨ (mask &&& ~~~want) &&& s.must ≠ 0)
      with hAB | hAB
  · have hc : ((((mask &&& want) &&& ~~~s.may) != (0 : W)) ||
      (((mask &&& ~~~want) &&& s.must) != (0 : W)) : Bool) = true := by
      rw [Bool.or_eq_true]
      rcases hAB with h1 | h2
      · exact Or.inl ((bv_bne _).mpr h1)
      · exact Or.inr ((bv_bne _).mpr h2)
    rw [ite_eq_left hc]
    refine ⟨⟨fun _ => hAB, fun _ => rfl⟩,
      ⟨fun h => absurd h (by decide),
        fun ⟨ha, hb, _⟩ => hAB.elim (fun hn => absurd ha hn) (fun hn => absurd hb hn)⟩,
      ⟨fun h => absurd h (by decide),
        fun ⟨ha, hb, _⟩ => hAB.elim (fun hn => absurd ha hn) (fun hn => absurd hb hn)⟩⟩
  · have hA0 : (mask &&& want) &&& ~~~s.may = 0 :=
      Classical.byContradiction (fun hn => hAB (Or.inl hn))
    have hB0 : (mask &&& ~~~want) &&& s.must = 0 :=
      Classical.byContradiction (fun hn => hAB (Or.inr hn))
    have hc : ((((mask &&& want) &&& ~~~s.may) != (0 : W)) ||
      (((mask &&& ~~~want) &&& s.must) != (0 : W)) : Bool) = false := by
      rw [Bool.or_eq_false_iff]
      exact ⟨(bv_bne_false _).mpr hA0, (bv_bne_false _).mpr hB0⟩
    have hnotSkip : ¬ (((((mask &&& want) &&& ~~~s.may) != (0 : W)) ||
        (((mask &&& ~~~want) &&& s.must) != (0 : W)) : Bool) = true) :=
      fun hh => absurd hh (Bool.ne_true_of_eq_false hc)
    rw [ite_eq_right hnotSkip]
    rcases Classical.em (((s.may ^^^ s.must) &&& mask) = 0) with hC0 | hC0
    · rw [ite_eq_left hC0]
      refine ⟨⟨fun h => absurd h (by decide), fun hr => (hAB hr).elim⟩,
        ⟨fun _ => ⟨hA0, hB0, hC0⟩, fun _ => rfl⟩,
        ⟨fun h => absurd h (by decide), fun ⟨_, _, hc'⟩ => absurd hC0 hc'⟩⟩
    · rw [ite_eq_right hC0]
      refine ⟨⟨fun h => absurd h (by decide), fun hr => (hAB hr).elim⟩,
        ⟨fun h => absurd h (by decide), fun ⟨_, _, hc'⟩ => absurd hc' hC0⟩,
        ⟨fun _ => ⟨hA0, hB0, hC0⟩, fun _ => rfl⟩⟩

private theorem classify_skip {mask want : W} {s : Meta} (h : classify mask want s = .skip) :
    (mask &&& want) &&& ~~~s.may ≠ 0 ∨ (mask &&& ~~~want) &&& s.must ≠ 0 :=
  ((classify_eq_iff mask want s).1).mp h

private theorem classify_all {mask want : W} {s : Meta} (h : classify mask want s = .all) :
    (mask &&& want) &&& ~~~s.may = 0 ∧ (mask &&& ~~~want) &&& s.must = 0 ∧
      (s.may ^^^ s.must) &&& mask = 0 :=
  ((classify_eq_iff mask want s).2.1).mp h

private theorem classify_inspect {mask want : W} {s : Meta}
    (h : classify mask want s = .inspect) :
    (mask &&& want) &&& ~~~s.may = 0 ∧ (mask &&& ~~~want) &&& s.must = 0 ∧
      (s.may ^^^ s.must) &&& mask ≠ 0 :=
  ((classify_eq_iff mask want s).2.2).mp h

/-! ## Per-page mode correctness -/

private theorem mode_all (mask want : W) (b : Block W Meta)
    (hb : b.header = summary b.data)
    (hcl : classify mask want b.header = .all) :
    b.data.filter (test mask want) = b.data := by
  obtain ⟨h1, h2, h3⟩ := classify_all hcl
  rw [hb] at h1 h2 h3
  refine (List.filter_eq_self).mpr (fun x hx => ?_)
  show (x &&& mask) == (want &&& mask)
  refine beq_of_eq (BitVec.eq_of_getLsbD_eq_iff.mpr (fun i hi => ?_))
  have hd : (decide (i < (64 : Nat))) = true := decide_eq_true hi
  have hb1 : ((mask.getLsbD i && want.getLsbD i) && !(b.data.any (fun z => z.getLsbD i))) =
      false := by
    have hh := congrArg (fun v : W => v.getLsbD i) h1
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_not, getLsbD_zero', may_bit, hd,
      Bool.true_and] at hh
    exact hh
  have hb2 : ((mask.getLsbD i && !(want.getLsbD i)) && (b.data.all (fun z => z.getLsbD i))) =
      false := by
    have hh := congrArg (fun v : W => v.getLsbD i) h2
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_not, getLsbD_zero', must_bit, hd,
      Bool.true_and] at hh
    exact hh
  have hb3 : (((b.data.any (fun z => z.getLsbD i)) ^^ (b.data.all (fun z => z.getLsbD i))) &&
      mask.getLsbD i) = false := by
    have hh := congrArg (fun v : W => v.getLsbD i) h3
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_xor, getLsbD_zero', may_bit, must_bit, hd,
      Bool.true_and] at hh
    exact hh
  simp only [BitVec.getLsbD_and]
  cases hm : mask.getLsbD i with
  | false => rw [Bool.and_false, Bool.and_false]
  | true =>
      rw [hm, Bool.and_true] at hb3
      have hmu : b.data.any (fun z => z.getLsbD i) = b.data.all (fun z => z.getLsbD i) := by
        cases ha : b.data.any (fun z => z.getLsbD i) <;>
          cases hu : b.data.all (fun z => z.getLsbD i) <;>
            simp only [ha, hu, Bool.false_xor, Bool.true_xor, Bool.not_false, Bool.not_true]
              at hb3 <;> simp_all
      cases hu : b.data.all (fun z => z.getLsbD i) with
      | true =>
          have hxb : x.getLsbD i = true :=
            (all_eq_true_iff b.data (fun z => z.getLsbD i)).mp hu x hx
          cases hw : want.getLsbD i with
          | true => rw [hxb]
          | false =>
              rw [hw, hm, hu, Bool.true_and, Bool.and_true, Bool.not_false] at hb2
              exact absurd hb2 (by decide)
      | false =>
          have ha2 : b.data.any (fun z => z.getLsbD i) = false := hmu.trans hu
          have hxb : x.getLsbD i = false :=
            (any_eq_false_iff b.data (fun z => z.getLsbD i)).mp ha2 x hx
          cases hw : want.getLsbD i with
          | false => rw [hxb]
          | true =>
              rw [hw, hm, ha2, Bool.not_false] at hb1
              exact absurd hb1 (by decide)

private theorem mode_skip (mask want : W) (b : Block W Meta)
    (hb : b.header = summary b.data)
    (hcl : classify mask want b.header = .skip) :
    b.data.filter (test mask want) = [] := by
  refine (List.filter_eq_nil_iff).mpr (fun x hx htest => ?_)
  obtain hC := classify_skip hcl
  rw [hb] at hC
  obtain he : (x &&& mask) = (want &&& mask) := eq_of_beq htest
  rcases hC with hA | hB
  · obtain ⟨i, hi, hbit⟩ := bv_exists _ hA
    have hd : (decide (i < (64 : Nat))) = true := decide_eq_true hi
    have hb1 : ((mask.getLsbD i && want.getLsbD i) && !(b.data.any (fun z => z.getLsbD i))) =
        true := by
      have hh := hbit
      simp only [BitVec.getLsbD_and, BitVec.getLsbD_not, may_bit, hd, Bool.true_and] at hh
      exact hh
    have he1 : (x.getLsbD i && mask.getLsbD i) = (want.getLsbD i && mask.getLsbD i) := by
      have hh := congrArg (fun v : W => v.getLsbD i) he
      simp only [BitVec.getLsbD_and] at hh
      exact hh
    obtain ⟨hm, hw, ha⟩ : mask.getLsbD i = true ∧ want.getLsbD i = true ∧
        b.data.any (fun z => z.getLsbD i) = false := by
      cases h1 : mask.getLsbD i <;> cases h2 : want.getLsbD i <;>
        cases h3 : b.data.any (fun z => z.getLsbD i) <;>
          simp only [h1, h2, h3, Bool.and_true, Bool.and_false, Bool.true_and, Bool.false_and,
            Bool.not_true, Bool.not_false] at hb1 ⊢ <;>
          first
            | exact absurd hb1 (by decide)
            | exact ⟨True.intro, True.intro, True.intro⟩
    have hxb : x.getLsbD i = false := (any_eq_false_iff b.data (fun z => z.getLsbD i)).mp ha x hx
    rw [hm, hw, Bool.and_true, hxb] at he1
    exact absurd he1 (by decide)
  · obtain ⟨i, hi, hbit⟩ := bv_exists _ hB
    have hd : (decide (i < (64 : Nat))) = true := decide_eq_true hi
    have hb2 : ((mask.getLsbD i && !(want.getLsbD i)) && (b.data.all (fun z => z.getLsbD i))) =
        true := by
      have hh := hbit
      simp only [BitVec.getLsbD_and, BitVec.getLsbD_not, must_bit, hd, Bool.true_and] at hh
      exact hh
    have he1 : (x.getLsbD i && mask.getLsbD i) = (want.getLsbD i && mask.getLsbD i) := by
      have hh := congrArg (fun v : W => v.getLsbD i) he
      simp only [BitVec.getLsbD_and] at hh
      exact hh
    obtain ⟨hm, hwnot, hu⟩ : mask.getLsbD i = true ∧ want.getLsbD i = false ∧
        b.data.all (fun z => z.getLsbD i) = true := by
      cases h1 : mask.getLsbD i <;> cases h2 : want.getLsbD i <;>
        cases h3 : b.data.all (fun z => z.getLsbD i) <;>
          simp only [h1, h2, h3, Bool.and_true, Bool.and_false, Bool.true_and, Bool.false_and,
            Bool.not_true, Bool.not_false] at hb2 ⊢ <;>
          first
            | exact absurd hb2 (by decide)
            | exact ⟨True.intro, True.intro, True.intro⟩
    have hxb : x.getLsbD i = true := (all_eq_true_iff b.data (fun z => z.getLsbD i)).mp hu x hx
    rw [hm, hwnot, Bool.and_true, hxb] at he1
    exact absurd he1 (by decide)

private theorem mode_inspect (mask want : W) (b : Block W Meta)
    (hb : b.header = summary b.data)
    (hcl : classify mask want b.header = .inspect) :
    ∃ i < 64, mask.getLsbD i = true ∧ (∃ x ∈ b.data, x.getLsbD i = true) ∧
      (∃ y ∈ b.data, y.getLsbD i = false) := by
  obtain ⟨h1, h2, h3⟩ := classify_inspect hcl
  rw [hb] at h1 h2 h3
  obtain ⟨i, hi, hbit⟩ := bv_exists _ h3
  have hd : (decide (i < (64 : Nat))) = true := decide_eq_true hi
  have hb1 : ((mask.getLsbD i && want.getLsbD i) && !(b.data.any (fun z => z.getLsbD i))) =
      false := by
    have hh := congrArg (fun v : W => v.getLsbD i) h1
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_not, getLsbD_zero', may_bit, hd,
      Bool.true_and] at hh
    exact hh
  have hb2 : ((mask.getLsbD i && !(want.getLsbD i)) && (b.data.all (fun z => z.getLsbD i))) =
      false := by
    have hh := congrArg (fun v : W => v.getLsbD i) h2
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_not, getLsbD_zero', must_bit, hd,
      Bool.true_and] at hh
    exact hh
  have hb3 : (((b.data.any (fun z => z.getLsbD i)) ^^ (b.data.all (fun z => z.getLsbD i))) &&
      mask.getLsbD i) = true := by
    have hh := hbit
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_xor, may_bit, must_bit, hd, Bool.true_and] at hh
    exact hh
  cases hm : mask.getLsbD i with
  | false =>
      rw [hm, Bool.and_false] at hb3
      exact absurd hb3 (by decide)
  | true =>
      rw [hm, Bool.and_true] at hb3
      cases hu : b.data.all (fun z => z.getLsbD i) with
      | true =>
          exfalso
          cases hw : want.getLsbD i with
          | false =>
              rw [hw, hm, hu, Bool.true_and, Bool.and_true, Bool.not_false] at hb2
              exact absurd hb2 (by decide)
          | true =>
              cases ha : b.data.any (fun z => z.getLsbD i) with
              | true =>
                  rw [ha, hu, Bool.true_xor, Bool.not_true] at hb3
                  exact absurd hb3 (by decide)
              | false =>
                  rw [ha, hm, hw, Bool.true_and, Bool.not_false, Bool.and_true] at hb1
                  exact absurd hb1 (by decide)
      | false =>
          rw [hu, Bool.xor_false] at hb3
          refine ⟨i, hi, hm, ?_, ?_⟩
          · exact (any_eq_true_iff b.data (fun z => z.getLsbD i)).mp hb3
          · exact (all_eq_false_iff b.data (fun z => z.getLsbD i)).mp hu

/-! ## `execute` fold characterization -/

private def step (cl : Meta → Mode) (ptest : W → Bool) (s : Result)
    (b : Block W Meta) (i : Nat) : Result :=
  match cl b.header with
  | .skip => s
  | .all => {s with count := s.count + b.size}
  | .inspect => ⟨s.count + (b.data.filter ptest).length, i :: s.opened⟩

private def incr (cl : Meta → Mode) (ptest : W → Bool) (b : Block W Meta) : Nat :=
  match cl b.header with
  | .skip => 0
  | .all => b.size
  | .inspect => (b.data.filter ptest).length

private def insIdx (cl : Meta → Mode) :
    (pages : List (Block W Meta)) → Nat → List Nat
  | [], _ => []
  | b :: bs, n => if cl b.header = .inspect then n :: insIdx cl bs (n + 1) else insIdx cl bs (n + 1)

private theorem execute_eq_fold (cl : Meta → Mode) (ptest : W → Bool)
    (pages : List (Block W Meta)) :
    execute cl ptest pages =
      (List.zipIdx pages 0).foldl (fun s p => step cl ptest s p.1 p.2) ({} : Result) := rfl

private theorem insIdx_cons_true (cl : Meta → Mode) (b : Block W Meta)
    (bs : List (Block W Meta)) (n : Nat) (h : cl b.header = .inspect) :
    insIdx cl (b :: bs) n = n :: insIdx cl bs (n + 1) := by
  simp only [insIdx, ite_eq_left h]

private theorem insIdx_cons_false (cl : Meta → Mode) (b : Block W Meta)
    (bs : List (Block W Meta)) (n : Nat) (h : ¬(cl b.header = .inspect)) :
    insIdx cl (b :: bs) n = insIdx cl bs (n + 1) := by
  simp only [insIdx, ite_eq_right h]

private theorem mem_insIdx (cl : Meta → Mode) :
    ∀ (pages : List (Block W Meta)) (n i : Nat),
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

private theorem foldl_count (cl : Meta → Mode) (ptest : W → Bool) :
    ∀ (pages : List (Block W Meta)) (n : Nat) (s : Result),
      ((List.zipIdx pages n).foldl (fun s p => step cl ptest s p.1 p.2) s).count =
        (pages.map (incr cl ptest)).foldl (fun a x => a + x) s.count := by
  intro pages
  induction pages with
  | nil => intro n s; rfl
  | cons b bs ih =>
      intro n s
      rw [List.zipIdx_cons, List.foldl_cons, ih (n + 1) (step cl ptest s b n),
        List.map_cons, List.foldl_cons]
      match h : cl b.header with
      | .skip =>
          rw [show (step cl ptest s b n).count = s.count from by unfold step; rw [h],
            show incr cl ptest b = (0 : Nat) from by unfold incr; rw [h],
            Nat.add_zero]
      | .all =>
          rw [show (step cl ptest s b n).count = s.count + b.size from by unfold step; rw [h],
            show incr cl ptest b = b.size from by unfold incr; rw [h]]
      | .inspect =>
          rw [show (step cl ptest s b n).count = s.count + (b.data.filter ptest).length
            from by unfold step; rw [h],
            show incr cl ptest b = (b.data.filter ptest).length from by unfold incr; rw [h]]

private theorem foldl_opened (cl : Meta → Mode) (ptest : W → Bool) :
    ∀ (pages : List (Block W Meta)) (n : Nat) (s : Result),
      ((List.zipIdx pages n).foldl (fun s p => step cl ptest s p.1 p.2) s).opened =
        (insIdx cl pages n).reverse ++ s.opened := by
  intro pages
  induction pages with
  | nil => intro n s; rfl
  | cons b bs ih =>
      intro n s
      rw [List.zipIdx_cons, List.foldl_cons, ih (n + 1) (step cl ptest s b n)]
      match h : cl b.header with
      | .skip =>
          have hn : ¬(cl b.header = .inspect) := by rw [h]; decide
          rw [insIdx_cons_false cl b bs n hn,
            show step cl ptest s b n = s from by unfold step; rw [h]]
      | .all =>
          have hn : ¬(cl b.header = .inspect) := by rw [h]; decide
          rw [insIdx_cons_false cl b bs n hn,
            show (step cl ptest s b n).opened = s.opened from by unfold step; rw [h]]
      | .inspect =>
          rw [insIdx_cons_true cl b bs n h, List.reverse_cons, List.append_assoc,
            show (step cl ptest s b n).opened = n :: s.opened from by unfold step; rw [h],
            show ([n] ++ s.opened) = (n :: s.opened) from rfl]

private theorem reads_execute (cl : Meta → Mode) (ptest : W → Bool)
    (pages : List (Block W Meta)) :
    reads (execute cl ptest pages) = insIdx cl pages 0 := by
  unfold reads
  rw [execute_eq_fold, foldl_opened cl ptest pages 0 ({})]
  rw [show ({} : Result).opened = ([] : List Nat) from rfl, List.append_nil,
    List.reverse_reverse]

/-! ## Mode correctness transfers to the fold increments -/

private theorem incr_eq (mask want : W) (b : Block W Meta)
    (hb : b.header = summary b.data) (hsz : b.size = b.data.length) :
    incr (classify mask want) (test mask want) b =
      (b.data.filter (test mask want)).length := by
  match h : classify mask want b.header with
  | .skip =>
      simp only [incr, h, mode_skip mask want b hb h, List.length_nil]
  | .all =>
      simp only [incr, h, mode_all mask want b hb h]
      exact hsz
  | .inspect =>
      simp only [incr, h]

/-! ## The soundness theorem -/

theorem sound : TinylambOpt.ResearchGoals.BitSignatureCount.Goal := by
  intro mask want pages h
  show ((execute (classify mask want) (test mask want) pages).count =
          ((pages.flatMap Block.data).filter (test mask want)).length) ∧
      ∀ b ∈ List.zipIdx pages 0,
        b.2 ∈ reads (execute (classify mask want) (test mask want) pages) →
          ∃ i < 64, mask.getLsbD i = true ∧
            (∃ x ∈ b.1.data, x.getLsbD i = true) ∧ (∃ y ∈ b.1.data, y.getLsbD i = false)
  refine ⟨?_, ?_⟩
  · have hcl : ∀ b ∈ pages,
        incr (classify mask want) (test mask want) b
          = (b.data.filter (test mask want)).length :=
      fun b hb => incr_eq mask want b (h b hb).1 (h b hb).2
    have hlen : ((pages.flatMap Block.data).filter (test mask want)).length =
        (pages.map (fun b => (b.data.filter (test mask want)).length)).sum := by
      rw [List.flatMap_def, filter_flatten, List.length_flatten, List.map_map, List.map_map]
      rfl
    rw [execute_eq_fold, foldl_count, map_congr' _ _ pages hcl, foldl_add_eq_add_sum,
      show ({} : Result).count = (0 : Nat) from rfl, Nat.zero_add, ← hlen]
  · intro b hbz hmem
    rw [reads_execute (classify mask want) (test mask want) pages] at hmem
    obtain ⟨p, hpz, hpi, hclp⟩ := (mem_insIdx (classify mask want) pages 0 b.2).mp hmem
    have hb1 : b.1 = p.1 := by
      have hz1 := (List.mem_zipIdx_iff_getElem? (l := pages)).mp hbz
      have hz2 := (List.mem_zipIdx_iff_getElem? (l := pages)).mp hpz
      rw [hpi] at hz2
      exact Option.some.inj (hz1.symm.trans hz2)
    rw [← hb1] at hclp
    exact mode_inspect mask want b.1 (h b.1 (List.fst_mem_of_mem_zipIdx hbz)).1 hclp

end TinylambOpt.ResearchProofs.BitSignatureCount
