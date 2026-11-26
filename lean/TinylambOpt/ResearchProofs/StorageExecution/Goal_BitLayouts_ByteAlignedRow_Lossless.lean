import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `ByteAlignedRow.Lossless` — Read one lane from serialized bytes, including zero
  width.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_Lossless

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

/-- Byte-aligned rows put value `i` in cells `i * align 8 w + b`. Writer cells
are pairwise distinct, all lie inside the serialized frame, and reading the
frame recovers exactly the low `w` bits of `xs[i]`. -/

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

private theorem atByte_little (n k j : Nat) (hj : j < k) :
    atByte (little n k) j = (n / 256 ^ j) % 256 := by
  unfold atByte little
  simp [hj]

private theorem file_testBit (l : Layout) (w : Nat) (xs : List Nat) (j r : Nat)
    (hj : j < byteCount (l.capacity xs.length w)) (hr : r < 8) :
    (atByte (file l w xs) j).testBit r = (image l w xs).testBit (j * 8 + r) := by
  unfold file
  rw [atByte_little]
  · have h256 : (256 : Nat) = 2 ^ 8 := by decide
    have hp : (2 ^ 8) ^ j = 2 ^ (8 * j) := by rw [← Nat.pow_mul]
    rw [h256, hp, Nat.testBit_mod_two_pow, Nat.testBit_div_two_pow]
    simp [hr]
    congr 1
    omega
  · exact hj

private theorem testBit_foldl_row (f : Nat → Nat) (cond : Nat → Bool) (bs : List Nat)
    (acc : Nat) (p : Nat) (hinj : ∀ a ∈ bs, ∀ b ∈ bs, f a = f b → a = b) :
    ((bs.foldl (fun a b => if cond b then a ||| 2 ^ f b else a) acc).testBit p) =
      (acc.testBit p || (bs.any (fun b => cond b && decide (f b = p)))) := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.any_cons]
      have hinj_bs : ∀ a ∈ bs, ∀ c ∈ bs, f a = f c → a = c :=
        fun a ha c hc => hinj a (by simp [ha]) c (by simp [hc])
      by_cases hc : cond b = true
      · rw [ite_eq_left hc]
        rw [ih (acc ||| 2 ^ f b) hinj_bs]
        simp only [Nat.testBit_or, Nat.testBit_two_pow, hc, Bool.true_and, Bool.or_assoc]
      · have hc' : cond b = false := Bool.eq_false_iff.mpr hc
        simp [hc', ih acc hinj_bs]

private theorem testBit_image_fold (l : Layout) (w : Nat) (xs : List Nat) (p : Nat)
    (lxs : List (Nat × Nat)) (acc : Nat)
    (hmem : ∀ x i, (x, i) ∈ lxs → i < xs.length)
    (hinj : ∀ i j a b, (∃ x, (x, i) ∈ lxs) → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) :
    ((lxs.foldl (fun acc (x, i) => (lanes w).foldl
      (fun a b => if x.testBit b then a ||| 2 ^ (l.bit xs.length w i b) else a) acc) acc).testBit p) =
      (acc.testBit p || lxs.any (fun (x, i) =>
        (lanes w).any (fun b => x.testBit b && decide (l.bit xs.length w i b = p)))) := by
  induction lxs generalizing acc with
  | nil => simp
  | cons xi rest ih =>
      obtain ⟨x, i⟩ := xi
      simp only [List.foldl_cons, List.any_cons]
      have hi : i < xs.length := hmem x i (by simp)
      have hmem_rest : ∀ x' i', (x', i') ∈ rest → i' < xs.length :=
        fun x' i' hm => hmem x' i' (by simp [hm])
      have hinj_rest : ∀ i' j a b, (∃ x, (x, i') ∈ rest) → j < xs.length → a < w → b < w →
          l.bit xs.length w i' a = l.bit xs.length w j b → i' = j ∧ a = b :=
        fun i' j a b hex hj ha hb heq => by
          obtain ⟨x', hx'⟩ := hex
          exact hinj i' j a b ⟨x', by simp [hx']⟩ hj ha hb heq
      rw [ih _ hmem_rest hinj_rest]
      rw [testBit_foldl_row (fun b => l.bit xs.length w i b) (fun b => x.testBit b)
        (lanes w) acc p]
      · simp [Bool.or_assoc]
      · intro a ha b hb heq
        have ha' : a < w := (mem_lanes.mp ha)
        have hb' : b < w := (mem_lanes.mp hb)
        exact (hinj i i a b ⟨x, by simp⟩ hi ha' hb' heq).2

private theorem testBit_image (l : Layout) (w : Nat) (xs : List Nat) (p : Nat)
    (hno : ∀ i j a b, i < xs.length → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) :
    (image l w xs).testBit p =
      (xs.zipIdx.any (fun (x, i) => (lanes w).any
        (fun b => x.testBit b && decide (l.bit xs.length w i b = p)))) := by
  unfold image
  have hmem : ∀ x i, (x, i) ∈ xs.zipIdx → i < xs.length := by
    intro x i h
    have hlt := List.snd_lt_of_mem_zipIdx h
    simpa [List.length_zipIdx] using hlt
  have hinj : ∀ i j a b, (∃ x, (x, i) ∈ xs.zipIdx) → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b := by
    intro i j a b hex hj ha hb heq
    obtain ⟨x, hx⟩ := hex
    exact hno i j a b (hmem x i hx) hj ha hb heq
  rw [testBit_image_fold l w xs p xs.zipIdx 0 hmem hinj]
  simp

private theorem testBit_image_eq (l : Layout) (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w)
    (hno : ∀ i j a b, i < xs.length → j < xs.length → a < w → b < w →
      l.bit xs.length w i a = l.bit xs.length w j b → i = j ∧ a = b) :
    (image l w xs).testBit (l.bit xs.length w i b) = (xs[i]?.getD 0).testBit b := by
  rw [testBit_image l w xs (l.bit xs.length w i b) hno]
  rw [Bool.eq_iff_iff]
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨⟨x, j⟩, hjmem, hj⟩
    rw [List.any_eq_true] at hj
    rcases hj with ⟨c, hc, hcond⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    have hsome : xs[j]? = some x := (List.mem_zipIdx_iff_getElem?).mp hjmem
    rcases (List.getElem?_eq_some_iff.mp hsome) with ⟨hjl, hval⟩
    have hc' : c < w := mem_lanes.mp hc
    have heq := hno j i c b hjl hi hc' hb hcond.2
    have hji : j = i := heq.1
    have hcb : c = b := heq.2
    have hsome' : xs[i]? = some x := by
      rw [← hji]
      exact hsome
    have hx : x = xs[i]?.getD 0 := by simp [hsome']
    rw [← hx, ← hcb]
    exact hcond.1
  · intro h
    rw [List.any_eq_true]
    refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
    · rw [List.mem_zipIdx_iff_getElem?]
      simp [hi]
    · rw [List.any_eq_true]
      exact ⟨b, mem_lanes.mpr hb, by simp [h]⟩

private theorem sum_testBit_eq_mod (x w : Nat) :
    ((List.range w).map (fun b => if x.testBit b then 2 ^ b else 0)).sum = x % 2 ^ w := by
  induction w with
  | zero => simp [Nat.mod_one]
  | succ w ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      rw [Nat.mod_pow_succ]
      change x % 2 ^ w + (if x.testBit w then 2 ^ w else 0) =
        x % 2 ^ w + 2 ^ w * (x / 2 ^ w % 2)
      have hbit : x.testBit w = decide (x / 2 ^ w % 2 = 1) := by
        simpa [Nat.testBit_zero] using (Nat.testBit_add x 0 w)
      rw [hbit]
      by_cases h : x / 2 ^ w % 2 = 1
      · simp [h]
      · have hlt : x / 2 ^ w % 2 < 2 := Nat.mod_lt _ (by decide)
        have hz : x / 2 ^ w % 2 = 0 := by omega
        simp [hz]

private theorem lossless_of_nonoverlap (l : Layout)
    (hno : ∀ n w i j a b, i < n → j < n → a < w → b < w →
      l.bit n w i a = l.bit n w j b → i = j ∧ a = b)
    (hbound : ∀ n w i b, i < n → b < w → l.bit n w i b < 8 * byteCount (l.capacity n w)) :
    lossless l := by
  intro w xs i hi
  have hfetch : fetch l xs.length w i (atByte (file l w xs)) =
      ((List.range w).map (fun b => if (xs[i]?.getD 0).testBit b then 2 ^ b else 0)).sum := by
    unfold fetch lanes
    dsimp
    congr 1
    apply List.map_congr_left
    intro b hb
    have hb' : b < w := by simpa [List.mem_range] using hb
    have hbit := hbound xs.length w i b hi hb'
    have hbyte : l.bit xs.length w i b / 8 < byteCount (l.capacity xs.length w) := by omega
    have hrem : l.bit xs.length w i b % 8 < 8 := Nat.mod_lt _ (by decide)
    have hread : (atByte (file l w xs) (l.bit xs.length w i b / 8)).testBit
          (l.bit xs.length w i b % 8) = (image l w xs).testBit (l.bit xs.length w i b) := by
      rw [file_testBit l w xs (l.bit xs.length w i b / 8) (l.bit xs.length w i b % 8) hbyte hrem]
      congr 1
      have := Nat.div_add_mod (l.bit xs.length w i b) 8
      omega
    rw [hread]
    rw [testBit_image_eq l w xs i b hi hb' (fun i j a b hi hj ha hb => hno xs.length w i j a b hi hj ha hb)]
  rw [hfetch, sum_testBit_eq_mod]

private theorem align_ge (w : Nat) : w ≤ align 8 w := by
  unfold align
  omega

private theorem align_eq (w : Nat) : align 8 w = 8 * ((w + 7) / 8) := by
  unfold align
  omega

private theorem byteCount_align (n w : Nat) : byteCount (n * align 8 w) = n * ((w + 7) / 8) := by
  unfold byteCount
  rw [align_eq]
  have hmul : n * (8 * ((w + 7) / 8)) = 8 * (n * ((w + 7) / 8)) := by
    simp [Nat.mul_comm, Nat.mul_left_comm]
  rw [hmul, Nat.mul_add_div (by decide : 0 < 8)]
  simp

private theorem row_nonoverlap : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  by_cases hw : w = 0
  · omega
  have hwpos : 0 < w := Nat.pos_of_ne_zero hw
  have hspos : 0 < align 8 w := Nat.lt_of_lt_of_le hwpos (align_ge w)
  have haS : a < align 8 w := Nat.lt_of_lt_of_le ha (align_ge w)
  have hbS : b < align 8 w := Nat.lt_of_lt_of_le hb (align_ge w)
  have hab : a = b := by
    have hh := congrArg (fun x => x % align 8 w) h
    simpa [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt haS, Nat.mod_eq_of_lt hbS] using hh
  subst b
  have hijm : i * align 8 w = j * align 8 w := Nat.add_right_cancel h
  have hij : i = j := Nat.eq_of_mul_eq_mul_right hspos hijm
  exact ⟨hij, rfl⟩

private theorem row_bound : ∀ n w i b, i < n → b < w →
    layout.bit n w i b < 8 * byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  simp only [layout]
  rw [byteCount_align]
  have hbS : b < align 8 w := Nat.lt_of_lt_of_le hb (align_ge w)
  have h1 : i * align 8 w + b < (i + 1) * align 8 w := by
    rw [Nat.succ_mul]
    omega
  have h2 : (i + 1) * align 8 w ≤ n * align 8 w :=
    Nat.mul_le_mul_right (align 8 w) (by omega)
  have h3 : n * align 8 w = 8 * (n * ((w + 7) / 8)) := by
    rw [align_eq]
    simp [Nat.mul_comm, Nat.mul_left_comm]
  omega

theorem sound_Lossless : ByteAlignedRow.Lossless.Goal := by
  exact lossless_of_nonoverlap layout row_nonoverlap row_bound

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_Lossless
