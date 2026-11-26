import TinylambOpt.ResearchGoals.QuotedDelimiterMask

/-!
Proved

- `Goal` — `∀ prior len q c, len ≤ 64 → q &&& c = 0 → chunk prior len q c = scalar
  prior len q c`

The reference model is defined in `TinylambOpt.ResearchGoals.QuotedDelimiterMask`.
-/

namespace TinylambOpt.ResearchProofs.QuotedDelimiterMask

open TinylambOpt.ResearchGoals.QuotedDelimiterMask

/-! ## Small Bool helpers -/

private theorem band_false_right : ∀ b : Bool, (b && false) = false := by decide
private theorem band_false_left : ∀ b : Bool, (false && b) = false := fun _ => rfl
private theorem band_true_left : ∀ b : Bool, (true && b) = b := fun _ => rfl
private theorem band_true_right : ∀ b : Bool, (b && true) = b := by decide
private theorem bor_false_right : ∀ b : Bool, (b || false) = b := by decide
private theorem bor_false_left : ∀ b : Bool, (false || b) = b := fun _ => rfl
private theorem xor_bne : ∀ a b : Bool, (a ^^ b) = (a != b) := fun _ _ => rfl

/-! ## `bxor n x` is the parity (XOR) of bits `0 .. n-1` of the Nat `x`. -/

private def bxor : Nat → Nat → Bool
  | 0, _ => false
  | n + 1, x => bxor n x != x.testBit n

private theorem bxor_zero (x : Nat) : bxor 0 x = false := rfl

private theorem bxor_succ (n x : Nat) : bxor (n + 1) x = (bxor n x != x.testBit n) := rfl

private theorem bxor_add (n m x : Nat) :
    bxor (n + m) x = (bxor n x != bxor m (x >>> n)) := by
  induction m with
  | zero => rw [Nat.add_zero, bxor_zero, Bool.bne_false]
  | succ m ih =>
      rw [show n + (m + 1) = n + m + 1 from (Nat.add_assoc n m 1).symm, bxor_succ, ih,
        bxor_succ, Nat.testBit_shiftRight, Bool.bne_assoc]

private theorem bxor_congr {x y n : Nat} (h : ∀ j, j < n → x.testBit j = y.testBit j) :
    bxor n x = bxor n y := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [bxor_succ, bxor_succ, ih (fun j hj => h j (Nat.lt_succ_of_lt hj)),
        h n (Nat.lt_succ_self n)]

private theorem bxor_eq_false {x n : Nat} (h : ∀ j, j < n → x.testBit j = false) :
    bxor n x = false := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [bxor_succ, ih (fun j hj => h j (Nat.lt_succ_of_lt hj)), h n (Nat.lt_succ_self n)]
      exact Bool.false_bne false

private theorem testBit_toNat_self (e : Bool) (k : Nat) :
    e.toNat.testBit k = (decide (k = 0) && e) := by
  cases e with
  | false =>
      rw [Bool.toNat_false, Nat.testBit_lt_two_pow (Nat.two_pow_pos k), band_false_right]
  | true =>
      rw [Bool.toNat_true]
      rcases Nat.eq_zero_or_pos k with rfl | hk
      · decide
      · rw [Nat.testBit_lt_two_pow (Nat.one_lt_two_pow (Nat.ne_of_gt hk)),
          decide_eq_false (Nat.ne_of_gt hk), band_false_left]

/-! ## The six prefix-XOR stages, as a recursion over the stage number. -/

private def stage : Nat → W → W
  | 0,     w => w
  | k + 1, w => let x := stage k w; x ^^^ (x <<< (2 ^ k))

private theorem parity_eq_stage (w : W) : parity w = stage 6 w := rfl

/-- After `k` stages, bit `i` is the parity of the window of `min (i+1) (2^k)` bits
of `w` that ends at bit `i`. -/
private theorem stage_getLsbD (k : Nat) (w : W) : ∀ (i : Nat), i < 64 →
    (stage k w).getLsbD i
      = bxor (min (i + 1) (2 ^ k)) (w.toNat >>> (i + 1 - min (i + 1) (2 ^ k))) := by
  induction k with
  | zero =>
      intro i _
      have hm : min (i + 1) (2 ^ 0) = 1 := by
        rw [Nat.pow_zero, Nat.min_eq_right (Nat.succ_le_succ (Nat.zero_le i))]
      rw [hm, show i + 1 - 1 = i from by omega]
      show w.toNat.testBit i = bxor 1 (w.toNat >>> i)
      rw [show (1 : Nat) = 0 + 1 from rfl, bxor_succ, bxor_zero, Bool.false_bne,
        Nat.testBit_shiftRight, Nat.add_zero]
  | succ k ih =>
      intro i hi
      have hsh : (stage k w <<< 2 ^ k).getLsbD i =
          (decide (i < 64) && !decide (i < 2 ^ k) && (stage k w).getLsbD (i - 2 ^ k)) :=
        BitVec.getLsbD_shiftLeft (stage k w) (2 ^ k)
      show (stage k w ^^^ (stage k w <<< 2 ^ k)).getLsbD i = _
      rw [BitVec.getLsbD_xor, xor_bne, hsh, decide_eq_true hi, band_true_left,
        show (2 : Nat) ^ (k + 1) = 2 * 2 ^ k from by rw [Nat.pow_succ, Nat.mul_comm]]
      rcases Nat.lt_or_ge i (2 ^ k) with hlt | hge
      · -- the shifted copy contributes nothing at bit `i`
        have hm : min (i + 1) (2 ^ k) = i + 1 := Nat.min_eq_left (Nat.succ_le_of_lt hlt)
        have hm' : min (i + 1) (2 * 2 ^ k) = i + 1 := Nat.min_eq_left (by omega)
        rw [hm', Nat.sub_self (i + 1), Nat.shiftRight_zero,
          decide_eq_true hlt, Bool.not_true, band_false_left, Bool.bne_false,
          ih i hi, hm, Nat.sub_self (i + 1), Nat.shiftRight_zero]
      · -- the shifted copy contributes bit `i - 2^k`
        have hm : min (i + 1) (2 ^ k) = 2 ^ k := Nat.min_eq_right (by omega)
        rw [decide_eq_false (Nat.not_lt_of_ge hge), Bool.not_false, band_true_left,
          ih i hi, ih (i - 2 ^ k) (Nat.lt_of_le_of_lt (Nat.sub_le i (2 ^ k)) hi), hm,
          show (i - 2 ^ k) + 1 = i + 1 - 2 ^ k from by omega]
        rcases Nat.lt_or_ge (i + 1) (2 * 2 ^ k) with hb | hb
        · -- the window of length `2*2^k` still covers the whole prefix `0 .. i`
          have hm1 : min (i + 1) (2 * 2 ^ k) = i + 1 := Nat.min_eq_left (Nat.le_of_lt hb)
          have hma : min (i + 1 - 2 ^ k) (2 ^ k) = i + 1 - 2 ^ k := Nat.min_eq_left (by omega)
          rw [hm1, hma, Nat.sub_self (i + 1), Nat.sub_self (i + 1 - 2 ^ k),
            Nat.shiftRight_zero, bne_comm, ← bxor_add,
            show (i + 1 - 2 ^ k) + 2 ^ k = i + 1 from by omega]
        · -- the window of length `2*2^k` splits into two halves of length `2^k`
          have hm1 : min (i + 1) (2 * 2 ^ k) = 2 * 2 ^ k := Nat.min_eq_right (by omega)
          have hma : min (i + 1 - 2 ^ k) (2 ^ k) = 2 ^ k := Nat.min_eq_right (by omega)
          rw [hm1, hma]
          have hmerge : bxor (2 * 2 ^ k) (w.toNat >>> (i + 1 - 2 * 2 ^ k))
              = (bxor (2 ^ k) (w.toNat >>> (i + 1 - 2 ^ k - 2 ^ k))
                  != bxor (2 ^ k) (w.toNat >>> (i + 1 - 2 ^ k))) := by
            have h1 : (i + 1 - 2 * 2 ^ k : Nat) = i + 1 - 2 ^ k - 2 ^ k := by omega
            have h2 : (i + 1 - 2 ^ k - 2 ^ k + 2 ^ k : Nat) = i + 1 - 2 ^ k := by omega
            rw [h1, show (2 * 2 ^ k : Nat) = 2 ^ k + 2 ^ k from by omega, bxor_add,
              ← Nat.shiftRight_add, h2]
          rw [bne_comm, hmerge]

/-! ## Bit characterizations of `chunk`. -/

private theorem mask_getLsbD (len : Nat) (hlen : len ≤ 64) (i : Nat) :
    (mask len).getLsbD i = decide (i < len) := by
  have h1 : BitVec.toNat (1 : BitVec 64) = 1 := rfl
  have hp : 1 ≤ (2 : Nat) ^ len := Nat.two_pow_pos len
  have hto : (mask len).toNat = ((2^64 - 1) + 2^len % 2^64) % 2^64 := by
    unfold mask
    rw [BitVec.sub_def, BitVec.toNat_ofNat, BitVec.toNat_shiftLeft, h1,
      Nat.shiftLeft_eq, Nat.one_mul]
  show (mask len).toNat.testBit i = decide (i < len)
  rw [hto, Nat.testBit_mod_two_pow]
  rcases Nat.lt_or_ge i 64 with hi | hi
  · rw [decide_eq_true hi, band_true_left]
    rcases Nat.lt_or_ge len 64 with hlt | hge
    · rw [Nat.mod_eq_of_lt (Nat.pow_lt_pow_right (by decide : (1 : Nat) < 2) hlt),
        show (2^64 - 1) + 2^len = 2^64 + (2^len - 1) from by omega,
        Nat.testBit_two_pow_add_gt hi, Nat.testBit_two_pow_sub_one]
    · have h64 : len = 64 := by omega
      rw [h64, Nat.mod_self, Nat.add_zero, Nat.testBit_two_pow_sub_one]
  · rw [decide_eq_false (Nat.not_lt_of_ge hi), band_false_left,
      decide_eq_false (Nat.not_lt_of_ge (Nat.le_trans hlen hi))]

private theorem masked_bxor (q : W) (len : Nat) (hlen : len ≤ 64) (n : Nat) (hn : n ≤ len) :
    bxor n ((q &&& mask len).toNat) = bxor n q.toNat :=
  bxor_congr fun j hj => by
    have hjl : j < len := Nat.lt_of_lt_of_le hj hn
    show (q &&& mask len).getLsbD j = q.getLsbD j
    rw [BitVec.getLsbD_and, mask_getLsbD len hlen j, decide_eq_true hjl, band_true_right]

private theorem bxor_mask_full (q : W) (len : Nat) (hlen : len ≤ 64) :
    bxor 64 ((q &&& mask len).toNat) = bxor len q.toNat := by
  have hzero : bxor (64 - len) ((q &&& mask len).toNat >>> len) = false :=
    bxor_eq_false fun j _ => by
      rw [Nat.testBit_shiftRight]
      show (q &&& mask len).getLsbD (len + j) = false
      rw [BitVec.getLsbD_and, mask_getLsbD len hlen (len + j),
        decide_eq_false (by omega : ¬(len + j < len)), band_false_right]
  have hsum : (64 : Nat) = len + (64 - len) := by omega
  have hmerge : bxor 64 ((q &&& mask len).toNat)
      = (bxor len ((q &&& mask len).toNat)
          != bxor (64 - len) ((q &&& mask len).toNat >>> len)) :=
    (congrArg (fun n => bxor n (q &&& mask len).toNat) hsum).trans
      (bxor_add len (64 - len) ((q &&& mask len).toNat))
  rw [hmerge, hzero, Bool.bne_false, masked_bxor q len hlen len (Nat.le_refl len)]

private theorem ite_word_getLsbD (prior : Bool) (i : Nat) (hi : i < 64) :
    ((if prior then ~~~(0 : W) else (0 : W)) : W).getLsbD i = prior := by
  cases prior with
  | false =>
      show (0 : W).getLsbD i = false
      exact BitVec.getLsbD_zero
  | true =>
      show (~~~(0 : W)).getLsbD i = true
      rw [BitVec.getLsbD_not, show (0 : W).getLsbD i = false from BitVec.getLsbD_zero,
        Bool.not_false, band_true_right, decide_eq_true hi]

private theorem chunk_fst_getLsbD (prior : Bool) (len : Nat) (hlen : len ≤ 64)
    (q c : W) (i : Nat) :
    (chunk prior len q c).1.getLsbD i
      = (decide (i < len) && (c.getLsbD i && !(prior != bxor (i + 1) q.toNat))) := by
  show (c &&& mask len &&& ~~~(parity (q &&& mask len) ^^^
      (if prior then ~~~(0 : W) else (0 : W)))).getLsbD i = _
  rw [BitVec.getLsbD_and, BitVec.getLsbD_and, BitVec.getLsbD_not, BitVec.getLsbD_xor,
    xor_bne, mask_getLsbD len hlen i]
  rcases Nat.lt_or_ge i 64 with hi | hi
  · rw [decide_eq_true hi, band_true_left, ite_word_getLsbD prior i hi, parity_eq_stage,
      stage_getLsbD 6 (q &&& mask len) i hi, show (2 : Nat) ^ 6 = 64 from rfl,
      Nat.min_eq_left (by omega : i + 1 ≤ 64), Nat.sub_self (i + 1), Nat.shiftRight_zero]
    rcases Nat.lt_or_ge i len with hlt | hge
    · rw [decide_eq_true hlt,
        masked_bxor q len hlen (i + 1) (Nat.succ_le_of_lt hlt)]
      rw [band_true_right, band_true_left, bne_comm]
    · rw [decide_eq_false (Nat.not_lt_of_ge hge), band_false_right, band_false_left,
        band_false_left]
  · rw [decide_eq_false (Nat.not_lt_of_ge hi), band_false_left, band_false_right,
      decide_eq_false (Nat.not_lt_of_ge (Nat.le_trans hlen hi)), band_false_left]

private theorem chunk_snd (prior : Bool) (len : Nat) (hlen : len ≤ 64) (q c : W) :
    (chunk prior len q c).2 = (prior != bxor len q.toNat) := by
  show ((parity (q &&& mask len) ^^^
      (if prior then ~~~(0 : W) else (0 : W)))).getLsbD 63 = _
  rw [BitVec.getLsbD_xor, xor_bne, ite_word_getLsbD prior 63 (by omega),
    parity_eq_stage, stage_getLsbD 6 (q &&& mask len) 63 (by omega),
    show (2 : Nat) ^ 6 = 64 from rfl,
    show min (63 + 1) (64 : Nat) = 64 from rfl,
    show (63 + 1 - 64 : Nat) = 0 from by omega, Nat.shiftRight_zero,
    bxor_mask_full q len hlen, bne_comm]

/-! ## Bit characterization of the sequential scan `scalar`. -/

private def sstep (q c : W) (p : W × Bool) (i : Nat) : W × Bool :=
  let next := p.2 != q.getLsbD i
  let emit := c.getLsbD i && !next
  (p.1 ||| ((BitVec.ofNat 64 emit.toNat) <<< i), next)

private theorem emit_bit (e : Bool) (i j : Nat) (hi : i < 64) :
    ((BitVec.ofNat 64 e.toNat) <<< i).getLsbD j = (decide (j = i) && e) := by
  rw [BitVec.getLsbD_shiftLeft, BitVec.getLsbD_ofNat, testBit_toNat_self]
  rcases Nat.lt_trichotomy j i with hlt | heq | hgt
  · rw [decide_eq_true hlt, Bool.not_true, band_false_right, band_false_left,
      decide_eq_false (Nat.ne_of_lt hlt), band_false_left]
  · subst heq
    rw [decide_eq_true hi, band_true_left, decide_eq_false (Nat.lt_irrefl j), Bool.not_false,
      band_true_left, Nat.sub_self j,
      decide_eq_true (by omega : (0 : Nat) < 64), decide_eq_true (rfl : (0 : Nat) = 0),
      decide_eq_true (rfl : (j : Nat) = j), band_true_left, band_true_left]
  · rw [decide_eq_false (Nat.not_lt_of_ge (Nat.le_of_lt hgt)), Bool.not_false,
      band_true_right, decide_eq_false (Nat.ne_of_gt (Nat.sub_pos_of_lt hgt)),
      decide_eq_false (Nat.ne_of_gt hgt), band_false_left, band_false_right,
      band_false_right]

private theorem scalar_spec : ∀ (n : Nat), n ≤ 64 → ∀ (prior : Bool) (q c : W),
    ((List.range n).foldl (sstep q c) ((0 : W), prior)).2 = (prior != bxor n q.toNat) ∧
    ∀ i, ((List.range n).foldl (sstep q c) ((0 : W), prior)).1.getLsbD i
        = (decide (i < n) && (c.getLsbD i && !(prior != bxor (i + 1) q.toNat))) := by
  intro n
  induction n with
  | zero =>
      intro _ prior q c
      constructor
      · show prior = (prior != bxor 0 q.toNat)
        rw [bxor_zero, Bool.bne_false]
      · intro i
        show (0 : W).getLsbD i = (decide (i < 0) && _)
        rw [decide_eq_false (Nat.not_lt_zero i), band_false_left]
        exact BitVec.getLsbD_zero
  | succ n ih =>
      intro hn prior q c
      have hlt : n < 64 := by omega
      obtain ⟨hs, hb⟩ := ih (by omega) prior q c
      have hrec : (List.range (n + 1)).foldl (sstep q c) ((0 : W), prior)
                = sstep q c ((List.range n).foldl (sstep q c) ((0 : W), prior)) n := by
        rw [List.range_succ, List.foldl_append]
        rfl
      have h1 : (sstep q c ((List.range n).foldl (sstep q c) ((0 : W), prior)) n).1
          = (((List.range n).foldl (sstep q c) ((0 : W), prior)).1
              ||| ((BitVec.ofNat 64
                    (c.getLsbD n && !(((List.range n).foldl (sstep q c) ((0 : W), prior)).2
                        != q.getLsbD n)).toNat) <<< n)) := rfl
      have h2 : (sstep q c ((List.range n).foldl (sstep q c) ((0 : W), prior)) n).2
          = (((List.range n).foldl (sstep q c) ((0 : W), prior)).2 != q.getLsbD n) := rfl
      have hqn : q.getLsbD n = q.toNat.testBit n := rfl
      constructor
      · rw [hrec, h2, hs, Bool.bne_assoc, hqn, ← bxor_succ]
      · intro i
        rw [hrec, h1, BitVec.getLsbD_or, hb, emit_bit (c.getLsbD n && _) n i hlt]
        rw [hs, Bool.bne_assoc, hqn, ← bxor_succ]
        rcases Nat.lt_trichotomy i n with hlt' | heq | hgt
        · rw [decide_eq_true hlt', band_true_left,
            decide_eq_false (Nat.ne_of_lt hlt'), band_false_left, bor_false_right,
            decide_eq_true (Nat.lt_of_lt_of_le hlt' (Nat.le_succ n)), band_true_left]
        · subst heq
          rw [decide_eq_false (Nat.lt_irrefl i), band_false_left,
            decide_eq_true (rfl : (i : Nat) = i), band_true_left, bor_false_left,
            decide_eq_true (Nat.lt_succ_self i), band_true_left]
        · rw [decide_eq_false (Nat.not_lt_of_ge (Nat.le_of_lt hgt)), band_false_left,
            bor_false_left, decide_eq_false (Nat.ne_of_gt hgt), band_false_left,
            decide_eq_false (by omega : ¬ (i < n + 1)), band_false_left]

private theorem scalar_fst_getLsbD (prior : Bool) (len : Nat) (hlen : len ≤ 64)
    (q c : W) (i : Nat) :
    (scalar prior len q c).1.getLsbD i
      = (decide (i < len) && (c.getLsbD i && !(prior != bxor (i + 1) q.toNat))) :=
  (scalar_spec len hlen prior q c).2 i

private theorem scalar_snd (prior : Bool) (len : Nat) (hlen : len ≤ 64) (q c : W) :
    (scalar prior len q c).2 = (prior != bxor len q.toNat) :=
  (scalar_spec len hlen prior q c).1

/-! ## Main theorem -/

theorem sound : TinylambOpt.ResearchGoals.QuotedDelimiterMask.Goal := by
  intro prior len q c hlen _
  refine Prod.ext ?_ ?_
  · apply BitVec.eq_of_toNat_eq
    apply Nat.eq_of_testBit_eq
    intro i
    show (chunk prior len q c).1.getLsbD i = (scalar prior len q c).1.getLsbD i
    rw [chunk_fst_getLsbD prior len hlen q c i, scalar_fst_getLsbD prior len hlen q c i]
  · rw [chunk_snd prior len hlen q c, scalar_snd prior len hlen q c]

end TinylambOpt.ResearchProofs.QuotedDelimiterMask
