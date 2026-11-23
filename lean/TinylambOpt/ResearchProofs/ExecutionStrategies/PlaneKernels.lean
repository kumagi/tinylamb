import TinylambOpt.ResearchGoals.ExecutionStrategies.PlaneKernels

/-!
# Soundness of bit-sliced plane kernels

Each `Goal` states that a bit-plane kernel over 64 lanes of unsigned byte
codes agrees with the corresponding scalar (per-lane) computation.

Proof strategy shared by all goals:

* `pack_getLsbD`: bit `i` of `pack f` is `f i` for `i < 64` (fold induction).
* `planes_getLsbD`: bit `i` of `planes x` at plane `k` is bit `k` of `x i`.
* `mod_two_pow_succ`: `x % 2^(n+1) = x % 2^n + 2^n * (x.testBit n)` — the
  bridge between `BitVec` lanes and their bit-plane decomposition.
* Per-bit Boolean recurrences (`ltStep`, the ripple-carry step, the
  `changed` fold, ...) are verified by finite case analysis and lifted over
  `List.range 8` / `List.range 64` by fold inductions of the shape
  `*_foldl_getLsbD`.

`ParallelLess` is proved first: the low-to-high `lessMask` fold is shown,
bit by bit, to compute the unsigned `<` of each lane pair, hence
`lessMask (planes x) (planes y) = pack (fun i => decide (x i < y i))` and the
masked equality follows.
-/

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.PlaneKernels

open TinylambOpt.ResearchGoals.PlaneKernels
open TinylambOpt.ResearchGoals.BitPlaneSum


private def pstep (f : Nat → Bool) (w : W) (i : Nat) : W :=
  w ||| ((BitVec.ofNat 64 (f i).toNat) <<< i)

private theorem foldl_pstep (f : Nat → Bool) :
    ∀ (n : Nat) (acc : W) (i : Nat), i < 64 →
      ((List.range n).foldl (pstep f) acc).getLsbD i = (acc.getLsbD i || (decide (i < n) && f i)) := by
  intro n acc
  induction n with
  | zero =>
      intro i _
      simp
  | succ n ih =>
      intro j hj
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil]
      rw [pstep, BitVec.getLsbD_or, ih j hj]
      have hsh : ((BitVec.ofNat 64 (f n).toNat) <<< n).getLsbD j = (decide (j = n) && f n) := by
        rw [BitVec.getLsbD_shiftLeft, BitVec.getLsbD_ofNat]
        cases h : f n with
        | false =>
            have h0 : (0 : Nat).testBit (j - n) = false :=
              Nat.testBit_lt_two_pow (Nat.two_pow_pos _)
            rw [Bool.toNat_false, h0]
            simp
        | true =>
            have h1 : (1 : Nat).testBit (j - n) = decide (j - n = 0) := by
              show (2^0 : Nat).testBit (j - n) = decide (j - n = 0)
              rw [Nat.testBit_two_pow]
              by_cases hz : j - n = 0
              · rw [hz]
              · rw [decide_eq_false hz, decide_eq_false (fun h2 => hz h2.symm)]
            rw [Bool.toNat_true, h1, decide_eq_true hj, Bool.true_and]
            rcases Nat.lt_trichotomy j n with hlt | heq | hgt
            · rw [decide_eq_true hlt,
                decide_eq_true (Nat.sub_eq_zero_of_le (Nat.le_of_lt hlt)),
                decide_eq_false (by omega : ¬ (j = n))]
              simp
            · subst heq
              rw [decide_eq_false (Nat.lt_irrefl j), decide_eq_true (Nat.sub_self j),
                decide_eq_true rfl]
              simp
            · rw [decide_eq_false (by omega : ¬ (j < n)),
                decide_eq_false (by omega : ¬ (j - n = 0)),
                decide_eq_false (by omega : ¬ (j = n))]
              simp
      rw [hsh]
      have hA : decide (j < n+1) = (decide (j < n) || decide (j = n)) := by
        rcases Nat.lt_trichotomy j n with hlt | heq | hgt
        · rw [decide_eq_true (Nat.lt_succ_of_lt hlt), decide_eq_true hlt,
            decide_eq_false (by omega : ¬ (j = n))]
          simp
        · subst heq
          rw [decide_eq_true (Nat.lt_succ_self j), decide_eq_false (Nat.lt_irrefl j),
            decide_eq_true rfl]
          simp
        · rw [decide_eq_false (by omega : ¬ (j < n)), decide_eq_false (by omega : ¬ (j = n)),
            decide_eq_false (by omega : ¬ (j < n+1))]
          simp
      rw [hA]
      rcases Nat.lt_trichotomy j n with hlt | heq | hgt
      · rw [decide_eq_true hlt, decide_eq_false (by omega : ¬ (j = n))]
        simp
      · subst heq
        rw [decide_eq_false (Nat.lt_irrefl j), decide_eq_true rfl]
        simp
      · rw [decide_eq_false (by omega : ¬ (j < n)), decide_eq_false (by omega : ¬ (j = n))]
        simp

private theorem pack_getLsbD (f : Nat → Bool) (i : Nat) (hi : i < 64) :
    (pack f).getLsbD i = f i := by
  show ((List.range 64).foldl (pstep f) 0).getLsbD i = f i
  rw [foldl_pstep f 64 0 i hi]
  simp [hi]

private def ltStep (ak bk lt : Bool) : Bool :=
  (!(ak) && bk) || (!(ak ^^ bk) && lt)

private theorem ltStep_ft (d : Bool) : ltStep false true d = true := by simp [ltStep]
private theorem ltStep_tf (d : Bool) : ltStep true false d = false := by simp [ltStep]
private theorem ltStep_ff (d : Bool) : ltStep false false d = d := by simp [ltStep]
private theorem ltStep_tt (d : Bool) : ltStep true true d = d := by simp [ltStep]

private theorem mod_two_pow_succ (x n : Nat) :
    x % 2^(n+1) = x % 2^n + 2^n * (x.testBit n).toNat := by
  have hpow : 2^(n+1) = 2^n * 2 := by rw [Nat.pow_add, Nat.pow_one]
  have hlt : x % 2^n < 2^n := Nat.mod_lt _ (Nat.two_pow_pos _)
  have hq : x / 2^n = 2 * (x / 2^(n+1)) + (x / 2^n) % 2 := by
    have h1 : (x / 2^n) % 2 + 2 * ((x / 2^n) / 2) = x / 2^n := Nat.mod_add_div _ _
    have h2 : (x / 2^n) / 2 = x / 2^(n+1) := by
      rw [Nat.div_div_eq_div_mul, ← hpow]
    omega
  have htb : (x.testBit n).toNat = (x / 2^n) % 2 := by
    have h3 : x.testBit n = (x / 2^n).testBit 0 := by
      have h := Nat.testBit_add x 0 n
      rwa [Nat.zero_add] at h
    rw [h3, Nat.testBit_zero]
    have h4 : (x / 2^n) % 2 < 2 := Nat.mod_lt _ (by decide : (0:Nat) < 2)
    by_cases h5 : (x / 2^n) % 2 = 1
    · rw [decide_eq_true h5]
      simp [h5]
    · rw [decide_eq_false h5]
      have h6 : (x / 2^n) % 2 = 0 := by omega
      simp [h6]
  have hx : x = x % 2^n + 2^n * (x / 2^n) := by
    have := Nat.mod_add_div x (2^n)
    omega
  have hx2 : x % 2^(n+1) = (x % 2^n + 2^n * (x / 2^n)) % 2^(n+1) :=
    congrArg (fun t => t % 2^(n+1)) hx
  rw [hx2]
  have hq' : 2 ^ n * (x / 2 ^ n)
      = 2 ^ (n + 1) * (x / 2 ^ (n + 1)) + 2 ^ n * ((x / 2 ^ n) % 2) := by
    have e := congrArg (fun t => 2 ^ n * t) hq
    rw [Nat.mul_add] at e
    have e2 : 2 ^ n * (2 * (x / 2 ^ (n + 1))) = 2 ^ (n + 1) * (x / 2 ^ (n + 1)) := by
      rw [← Nat.mul_assoc, ← hpow]
    rw [e2] at e
    exact e
  have hdecomp : x % 2^n + 2^n * (x / 2^n)
      = (x % 2^n + 2^n * ((x / 2^n) % 2)) + 2^(n+1) * (x / 2^(n+1)) := by
    rw [hq']
    omega
  rw [hdecomp, Nat.add_mul_mod_self_left, htb]
  apply Nat.mod_eq_of_lt
  have h2m : (x / 2^n) % 2 < 2 := Nat.mod_lt _ (by decide : (0:Nat) < 2)
  have h2 : (x / 2^n) % 2 ≤ 1 := by omega
  have h3 : 2^n * ((x / 2^n) % 2) ≤ 2^n := by
    calc 2^n * ((x / 2^n) % 2) ≤ 2^n * 1 := Nat.mul_le_mul_left _ h2
      _ = 2^n := by simp
  have hpow2 : 2^(n+1) = 2^n + 2^n := Nat.two_pow_succ n
  omega

private theorem ltStep_foldl_spec (xn yn : Nat) (n : Nat) :
    (List.range n).foldl (fun lt k => ltStep (xn.testBit k) (yn.testBit k) lt) false
      = decide (xn % 2^n < yn % 2^n) := by
  induction n with
  | zero =>
      simp [Nat.mod_one]
  | succ n ih =>
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil]
      rw [ih, mod_two_pow_succ xn n, mod_two_pow_succ yn n]
      have hlt1 : xn % 2^n < 2^n := Nat.mod_lt _ (Nat.two_pow_pos _)
      have hlt2 : yn % 2^n < 2^n := Nat.mod_lt _ (Nat.two_pow_pos _)
      have hpos : 0 < 2^n := Nat.two_pow_pos _
      cases hxb : xn.testBit n <;> cases hyb : yn.testBit n <;>
        simp only [hxb, hyb, ltStep_ft, ltStep_tf, ltStep_ff, ltStep_tt,
          Bool.toNat_true, Bool.toNat_false, Nat.mul_zero, Nat.mul_one,
          Nat.add_zero] <;>
        first
        | rfl
        | (apply (decide_eq_true _).symm; omega)
        | (apply (decide_eq_false _).symm; omega)
        | (rw [decide_eq_decide]; exact (Nat.add_lt_add_iff_right).symm)

/-- bit `i` of `planes x` at plane `k` is bit `k` of `x i`. -/
private theorem planes_getLsbD (x : Nat → B) (k i : Nat) (hi : i < 64) :
    (planes x k).getLsbD i = (x i).getLsbD k := by
  show (pack fun i => (x i).getLsbD k).getLsbD i = _
  exact pack_getLsbD _ _ hi

/-- `getLsbD` commutes with the `lessMask` fold (for lanes `i < 64`). -/
private theorem lessMask_foldl_getLsbD (a b : Planes) (i : Nat) (hi : i < 64) (n : Nat) :
    ((List.range n).foldl (fun lt k => (~~~(a k) &&& b k) ||| (~~~(a k ^^^ b k) &&& lt)) 0).getLsbD i
      = (List.range n).foldl (fun lt k => ltStep ((a k).getLsbD i) ((b k).getLsbD i) lt) false := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.range_succ, List.foldl_append, List.foldl_cons, List.foldl_nil]
      have hstep : ∀ lt : W,
          ((~~~(a n) &&& b n) ||| (~~~(a n ^^^ b n) &&& lt)).getLsbD i
            = ltStep ((a n).getLsbD i) ((b n).getLsbD i) (lt.getLsbD i) := by
        intro lt
        rw [BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_and,
            BitVec.getLsbD_not, BitVec.getLsbD_not, BitVec.getLsbD_xor]
        simp [hi, ltStep]
      rw [hstep, ih]

private theorem foldl_ltStep_congr {f g : Bool → Nat → Bool} (l : List Nat) (init : Bool)
    (h : ∀ acc k, f acc k = g acc k) :
    l.foldl f init = l.foldl g init := by
  induction l generalizing init with
  | nil => rfl
  | cons k t ih =>
      rw [List.foldl_cons, List.foldl_cons, h init k]
      exact ih (g init k)

private theorem lessMask_getLsbD (x y : Nat → B) (i : Nat) (hi : i < 64) :
    (lessMask (planes x) (planes y)).getLsbD i = decide (x i < y i) := by
  have h1 := lessMask_foldl_getLsbD (planes x) (planes y) i hi 8
  -- rewrite plane bits to testBit of lane values
  have h2 : (List.range 8).foldl (fun lt k => ltStep ((planes x k).getLsbD i) ((planes y k).getLsbD i) lt) false
      = (List.range 8).foldl (fun lt k => ltStep ((x i).toNat.testBit k) ((y i).toNat.testBit k) lt) false := by
    apply foldl_ltStep_congr
    intro acc k
    rw [planes_getLsbD x k i hi, planes_getLsbD y k i hi]
    rfl
  rw [h2] at h1
  rw [ltStep_foldl_spec ((x i).toNat) ((y i).toNat) 8] at h1
  -- h1 : lessMask...getLsbD i = decide (xn % 256 < yn % 256)
  have hlt : (x i).toNat < 256 := (x i).isLt
  have hlt2 : (y i).toNat < 256 := (y i).isLt
  rw [Nat.mod_eq_of_lt hlt, Nat.mod_eq_of_lt hlt2] at h1
  -- h1 : ... = decide ((x i).toNat < (y i).toNat)
  exact h1

theorem sound_ParallelLess : ParallelLess.Goal := by
  intro x y m
  have hmask : lessMask (planes x) (planes y) = pack (fun i => decide (x i < y i)) := by
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    rw [lessMask_getLsbD x y i hi, pack_getLsbD _ _ hi]
  rw [hmask]

private theorem foldl_or_decide (g : Nat → Bool) (n : Nat) :
    (List.range n).foldl (fun acc k => acc || g k) false
      = decide (∃ k ∈ List.range n, g k) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil, ih]
      have hmem : (∃ k ∈ List.range n ++ [n], g k) ↔
          (∃ k ∈ List.range n, g k) ∨ g n := by
        constructor
        · rintro ⟨k, hk, hg⟩
          rw [List.mem_append] at hk
          rcases hk with hk | hk
          · exact Or.inl ⟨k, hk, hg⟩
          · rw [List.mem_singleton] at hk; subst hk; exact Or.inr hg
        · rintro (⟨k, hk, hg⟩ | hg)
          · exact ⟨k, List.mem_append.mpr (Or.inl hk), hg⟩
          · exact ⟨n, List.mem_append.mpr (Or.inr (List.mem_singleton_self n)), hg⟩
      rw [decide_eq_decide.mpr hmem, Bool.eq_iff_iff, Bool.or_eq_true_iff,
        decide_eq_true_iff, decide_eq_true_iff]

private theorem exists_bit_diff_iff_ne (x v : B) :
    (∃ k ∈ List.range 8, (x.getLsbD k) ≠ (v.getLsbD k)) ↔ x ≠ v := by
  have hext : x = v ↔ ∀ k, k < 8 → x.getLsbD k = v.getLsbD k :=
    BitVec.eq_of_getLsbD_eq_iff (w := 8)
  constructor
  · rintro ⟨k, hk, hne⟩ rfl
    have hk8 : k < 8 := List.mem_range.mp hk
    exact hne (hext.mp rfl k hk8)
  · intro hne
    by_cases hcon : ∃ k ∈ List.range 8, x.getLsbD k ≠ v.getLsbD k
    · exact hcon
    · exfalso
      apply hne
      apply hext.mpr
      intro k hk
      by_cases hdiff : x.getLsbD k = v.getLsbD k
      · exact hdiff
      · exact absurd ⟨k, List.mem_range.mpr hk, hdiff⟩ hcon



-- getLsbD of an ite
private theorem getLsbD_ite {c : Prop} [Decidable c] (x y : W) (i : Nat) :
    BitVec.getLsbD (if c then x else y) i = if c then BitVec.getLsbD x i else BitVec.getLsbD y i := by
  by_cases hc : c <;> simp [hc]

-- contribution of one plane of `changed` at lane i, as a Bool
private theorem changed_contrib (a : Planes) (v : B) (i n : Nat) (hi : i < 64) :
    BitVec.getLsbD (if v.getLsbD n = true then ~~~(a n) else a n) i
      = (if v.getLsbD n = true then !(a n).getLsbD i else (a n).getLsbD i) := by
  rw [getLsbD_ite, BitVec.getLsbD_not, decide_eq_true hi, Bool.true_and]

-- the ite contribution is true iff the two bits differ
private theorem contrib_iff_diff (xb vb : Bool) :
    ((if vb = true then !xb else xb) = true) ↔ (xb ≠ vb) := by
  cases xb <;> cases vb <;> simp

-- the `changed` fold commutes with getLsbD
private theorem changed_foldl_getLsbD (a : Planes) (v : B) (i : Nat) (hi : i < 64) (n : Nat) :
    BitVec.getLsbD
        ((List.range n).foldl
          (fun w k => w ||| (if v.getLsbD k = true then ~~~(a k) else a k)) 0) i
      = (List.range n).foldl
          (fun acc k => acc || (if v.getLsbD k = true then !(a k).getLsbD i else (a k).getLsbD i))
          false := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.range_succ, List.foldl_append, List.foldl_cons, List.foldl_nil]
      rw [BitVec.getLsbD_or, changed_contrib a v i n hi, ih]

private theorem exists_mem_congr {P Q : Nat → Prop} (l : List Nat) (h : ∀ k ∈ l, P k ↔ Q k) :
    (∃ k ∈ l, P k) ↔ (∃ k ∈ l, Q k) := by
  constructor
  · rintro ⟨k, hk, hp⟩; exact ⟨k, hk, (h k hk).mp hp⟩
  · rintro ⟨k, hk, hq⟩; exact ⟨k, hk, (h k hk).mpr hq⟩

private theorem changed_planes_getLsbD (x : Nat → B) (v : B) (i : Nat) (hi : i < 64) :
    BitVec.getLsbD (changed (planes x) v) i = decide (x i != v) := by
  show BitVec.getLsbD
      ((List.range 8).foldl
        (fun w k => w ||| (if v.getLsbD k = true then ~~~(planes x k) else planes x k)) 0) i
      = decide (x i != v)
  rw [changed_foldl_getLsbD (planes x) v i hi 8]
  have hcongr : (List.range 8).foldl
        (fun acc k => acc || (if v.getLsbD k = true then !((planes x k).getLsbD i) else (planes x k).getLsbD i)) false
      = (List.range 8).foldl
        (fun acc k => acc || (if v.getLsbD k = true then !((x i).getLsbD k) else (x i).getLsbD k)) false := by
    apply foldl_ltStep_congr
    intro acc k
    rw [planes_getLsbD x k i hi]
  rw [hcongr, foldl_or_decide _ 8]
  apply decide_eq_decide.mpr
  have h1 : ∀ k ∈ List.range 8,
      ((if v.getLsbD k = true then !((x i).getLsbD k) else (x i).getLsbD k) = true) ↔
        ((x i).getLsbD k ≠ v.getLsbD k) := by
    intro k _
    exact contrib_iff_diff _ _
  rw [exists_mem_congr _ h1]
  exact Iff.trans (exists_bit_diff_iff_ne (x i) v) (bne_iff_ne).symm

-- mask bit = lane selected AND differs
private theorem mask_changed_getLsbD (x : Nat → B) (v : B) (m : W) (i : Nat) (hi : i < 64) :
    BitVec.getLsbD (m &&& changed (planes x) v) i = (m.getLsbD i && decide (x i != v)) := by
  rw [BitVec.getLsbD_and, changed_planes_getLsbD x v i hi]



private theorem pairwise_range (n : Nat) : List.Pairwise (· < ·) (List.range n) := by
  induction n with
  | zero => exact List.Pairwise.nil
  | succ n ih =>
      rw [List.range_succ, List.pairwise_append]
      refine ⟨ih, List.pairwise_singleton _ _, ?_⟩
      intro a ha b hb
      rw [List.mem_singleton] at hb
      subst hb
      exact List.mem_range.mp ha

private theorem pairwise_lanes (m : W) : List.Pairwise (· < ·) (lanes m) := by
  show List.Pairwise (· < ·) ((List.range 64).filter m.getLsbD)
  exact List.Pairwise.filter _ (pairwise_range 64)

private theorem find?_eq_iff_of_pairwise {p : Nat → Bool} {l : List Nat}
    (hs : List.Pairwise (· < ·) l) {i : Nat} :
    l.find? p = some i ↔ i ∈ l ∧ p i = true ∧ ∀ j ∈ l, j < i → ¬ (p j = true) := by
  induction l with
  | nil => simp [List.find?]
  | cons a t ih =>
      have hst : List.Pairwise (· < ·) t := List.Pairwise.of_cons hs
      have hat : ∀ b ∈ t, a < b := (List.pairwise_cons.mp hs).1
      have hat_not : a ∉ t := by
        intro ha
        exact Nat.lt_irrefl a (hat a ha)
      rw [List.find?_cons]
      cases hpa : p a with
      | true =>
          dsimp only
          rw [Option.some.injEq]
          constructor
          · intro hai
            subst i
            refine ⟨List.mem_cons_self, hpa, ?_⟩
            intro j hj hjlt
            rcases List.mem_cons.mp hj with heq | hjt
            · subst j
              exact absurd hjlt (Nat.lt_irrefl a)
            · exact absurd (Nat.lt_trans hjlt (hat j hjt)) (Nat.lt_irrefl j)
          · rintro ⟨hmem, -, hforall⟩
            rcases List.mem_cons.mp hmem with heq | hit
            · subst i
              rfl
            · have hai : a < i := hat i hit
              have hna := hforall a List.mem_cons_self hai
              rw [hpa] at hna
              exact absurd rfl hna
      | false =>
          dsimp only
          rw [ih hst]
          have hpaF : p a = false := hpa
          constructor
          · rintro ⟨hit, hp, htf⟩
            have hmem : i ∈ a :: t := List.mem_cons.mpr (Or.inr hit)
            refine ⟨hmem, hp, ?_⟩
            intro j hj hjlt
            rcases List.mem_cons.mp hj with heq | hjt
            · subst j
              rw [hpaF]
              exact fun h => Bool.noConfusion h
            · exact htf j hjt hjlt
          · rintro ⟨hmem, hp, hcall⟩
            have hnot_a : i ≠ a := by
              intro hia
              subst i
              rw [hpaF] at hp
              exact Bool.noConfusion hp
            have hit : i ∈ t := by
              rcases List.mem_cons.mp hmem with h | h
              · exact absurd h hnot_a
              · exact h
            refine ⟨hit, hp, ?_⟩
            intro j hjt hjlt
            exact hcall j (List.mem_cons.mpr (Or.inr hjt)) hjlt


private theorem natCast64_toNat : ((↑64 : W)).toNat = 64 := by
  show (BitVec.ofNat 64 64).toNat = 64
  rw [BitVec.toNat_ofNat, Nat.mod_eq_of_lt (Nat.lt_pow_self (a := 2) (n := 64) (by decide))]

private theorem ctz_zero_toNat : ((0 : W).ctz).toNat = 64 := by
  have hrev : ((0 : W).reverse) = 0 := (BitVec.reverse_eq_zero_iff (x := (0 : W))).mpr rfl
  have h : (0 : W).ctz = (↑64 : W) := by
    rw [BitVec.ctz_eq_reverse_clz, hrev]
    exact (BitVec.clz_eq_iff_eq_zero (x := (0 : W))).mpr rfl
  rw [h, natCast64_toNat]

private theorem ne_zero_of_getLsbD {w : W} {i : Nat} (hi : i < 64)
    (h : w.getLsbD i = true) : w ≠ 0 := by
  intro hw
  rw [hw] at h
  simp at h

private theorem eq_zero_of_getLsbD {w : W} (h : ∀ i, i < 64 → w.getLsbD i = false) : w = 0 := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp [h i hi]

/-- If `i₀` is a set bit and all lower bits are clear, `ctz` lands on `i₀`. -/
private theorem ctz_toNat_eq_of_bits {w : W} (hw : w ≠ 0) {i₀ : Nat} (hi₀ : i₀ < 64)
    (hbit : w.getLsbD i₀ = true) (hbelow : ∀ j, j < i₀ → w.getLsbD j = false) :
    w.ctz.toNat = i₀ := by
  have hc : w.ctz.toNat < 64 := by
    have hlt := (BitVec.ctz_lt_iff_ne_zero (x := w)).mpr hw
    have h2 : (w.ctz).toNat < ((↑64 : W)).toNat := hlt
    rw [natCast64_toNat] at h2
    exact h2
  have hct : w.getLsbD w.ctz.toNat = true := BitVec.getLsbD_true_ctz_of_ne_zero hw
  have hcl : ∀ j, j < w.ctz.toNat → w.getLsbD j = false :=
    fun j hj => BitVec.getLsbD_false_of_lt_ctz hj
  rcases Nat.lt_trichotomy w.ctz.toNat i₀ with hlt | heq | hgt
  · have hb := hbelow _ hlt
    rw [hct] at hb
    simp at hb
  · exact heq
  · have hb := hcl _ hgt
    rw [hbit] at hb
    simp at hb

/-- bit `i` of `m &&& changed` is `m_i && (x_i ≠ v)` for `i < 64`. -/
private theorem maskbit_iff_diff (x : Nat → B) (v : B) (m : W) (i : Nat) (hi : i < 64) :
    ((m &&& changed (planes x) v).getLsbD i = true) ↔
      (m.getLsbD i = true ∧ (x i != v) = true) := by
  rw [mask_changed_getLsbD x v m i hi]
  have hd : decide (x i != v) = (x i != v) := Bool.decide_eq_true
  rw [hd, Bool.and_eq_true]

private theorem sound_RunBoundary : RunBoundary.Goal := by
  intro x v m
  cases hfind : (lanes m).find? (fun i => x i != v) with
  | none =>
      have hnone := (List.find?_eq_none).mp hfind
      have hw0 : m &&& changed (planes x) v = 0 := by
        apply eq_zero_of_getLsbD
        intro i hi
        rw [mask_changed_getLsbD x v m i hi]
        have hd : decide (x i != v) = (x i != v) := Bool.decide_eq_true
        rw [hd]
        by_cases hmi : m.getLsbD i = true
        · have himem : i ∈ lanes m := by
            show i ∈ (List.range 64).filter m.getLsbD
            exact List.mem_filter.mpr ⟨List.mem_range.mpr hi, hmi⟩
          have hnp := hnone i himem
          have hdf : (x i != v) = false := by
            cases h : (x i != v) with
            | true => exact absurd h hnp
            | false => rfl
          rw [hmi, hdf]
          rfl
        · have hmif : m.getLsbD i = false := by
            cases hc : m.getLsbD i <;> simp_all
          rw [hmif]
          simp
      rw [hw0, ctz_zero_toNat]
      rfl
  | some i₀ =>
      have hprop := (find?_eq_iff_of_pairwise (pairwise_lanes m)
        (p := fun i => x i != v)).mp hfind
      obtain ⟨hi₀mem, hp₀, hleast⟩ := hprop
      have hi₀64 : i₀ < 64 := List.mem_range.mp ((List.mem_filter.mp hi₀mem).1)
      have hm₀ : m.getLsbD i₀ = true := (List.mem_filter.mp hi₀mem).2
      have hbit₀ : (m &&& changed (planes x) v).getLsbD i₀ = true := by
        rw [maskbit_iff_diff x v m i₀ hi₀64]
        exact ⟨hm₀, hp₀⟩
      have hw : m &&& changed (planes x) v ≠ 0 := ne_zero_of_getLsbD hi₀64 hbit₀
      have hctz : (m &&& changed (planes x) v).ctz.toNat = i₀ := by
        apply ctz_toNat_eq_of_bits hw hi₀64 hbit₀
        intro j hj
        rw [mask_changed_getLsbD x v m j (by omega : j < 64)]
        have hj64 : j < 64 := by omega
        by_cases hmj : m.getLsbD j = true
        · have hjmem : j ∈ lanes m := by
            show j ∈ (List.range 64).filter m.getLsbD
            exact List.mem_filter.mpr ⟨List.mem_range.mpr hj64, hmj⟩
          have hnotp := hleast j hjmem hj
          have hdf : (x j != v) = false := by
            cases h : (x j != v) with
            | true => exact absurd h hnotp
            | false => rfl
          rw [hmj]
          have hd : decide (x j != v) = (x j != v) := Bool.decide_eq_true
          rw [hd, hdf]
          rfl
        · have hmjf : m.getLsbD j = false := by
            cases hc : m.getLsbD j <;> simp_all
          rw [hmjf]
          simp
      rw [hctz]
      rfl

end TinylambOpt.ResearchProofs.ExecutionStrategies.PlaneKernels
