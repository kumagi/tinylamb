import TinylambOpt.ResearchGoals.Compression.Nullable

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved (17 claims, including the following)

- `AllNullZeroWidth` — `∀ n, striped [] [] 0 n = List.replicate n none`
- `DenseRoundTrip` — `∀ xs, fill (flags xs) (dense xs) = xs`
- `DenseVsSentinel` — `∀ xs, let d := distinct xs let vs := dense xs let w :=
  ordinalWidth d.length ((marks (flags xs)).length + (pack w (vs.map d.idxOf)).length
  < (pack (width d.length) (codes xs)).length ↔ byteCount xs.length + byteCount
  (vs.length*w) < byteCount (xs.length*width d.length))`

Refuted by counterexample

- `RunsFromDistinct` — `∀ (fs gs : List Bool), fs.Perm gs → (flips fs).length = (flips
  gs).length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Nullable

open TinylambOpt.ResearchGoals.Compression.Nullable
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.IntegerFrames
open TinylambOpt.ResearchGoals.EncodedBlocks (dense)

/-!
Proofs and counterexamples for the 18 `Nullable` goals.  Seventeen goals are
sound; `RunsFromDistinct` is false because the number of RLE runs depends on
the order of the flags, not just on their multiset.

The helpers below cover ranks in a validity mask, gap decoding, base-256 byte
streams, bit-packed lane reads, and the width cliffs of `width`/`ubytes`.
-/

/-! ### Generic list helpers -/

private theorem filter_length_countP (l : List α) (p : α → Bool) :
    (l.filter p).length = l.countP p :=
  (List.countP_eq_length_filter (p := p) (l := l)).symm

private theorem count_filter_bool (fs : List Bool) (c : Bool) :
    (fs.filter (fun b => b == c)).length = fs.count c := by
  induction fs with
  | nil => rfl
  | cons b bs ih =>
      by_cases hb : (b == c) = true <;> simp [hb, ih, List.count_cons]

private theorem zipIdx_filter_fst_from (fs : List Bool) (c : Bool) (k : Nat) :
    ((fs.zipIdx k).filter (fun (b, _) => b == c)).map Prod.fst =
      fs.filter (fun b => b == c) := by
  induction fs generalizing k with
  | nil => rfl
  | cons b bs ih =>
      by_cases hb : (b == c) = true <;> simp [hb, ih (k + 1)]

private theorem zipIdx_filter_length (fs : List Bool) (c : Bool) :
    ((fs.zipIdx.filter (fun (b, _) => b == c)).map Prod.snd).length = fs.count c := by
  have hlen : ((fs.zipIdx.filter (fun (b, _) => b == c)).map Prod.snd).length =
      (fs.zipIdx.filter (fun (b, _) => b == c)).length := by simp
  rw [hlen]
  have hfst := congrArg List.length (zipIdx_filter_fst_from fs c 0)
  simpa [count_filter_bool fs c] using hfst

private theorem zipIdx_pairwise_snd (fs : List Bool) (k : Nat) :
    (fs.zipIdx k).Pairwise (fun a b : Bool × Nat => a.2 < b.2) := by
  induction fs generalizing k with
  | nil => simp
  | cons b bs ih =>
      rw [List.zipIdx_cons, List.pairwise_cons]
      constructor
      · intro p hp
        have hz := List.mem_zipIdx hp
        omega
      · simpa [Nat.add_comm] using ih (k + 1)

private theorem zipIdx_positions_contains (bs : List Bool) (mark : Bool) (i : Nat)
    (hi : i < bs.length) :
    (((bs.zipIdx.filter (fun (b, _) => b == mark)).map Prod.snd).contains i = true ↔
      bs[i] = mark) := by
  constructor
  · intro hc
    have hmem : i ∈ ((bs.zipIdx.filter (fun (b, _) => b == mark)).map Prod.snd) :=
      List.elem_iff.mp hc
    rw [List.mem_map] at hmem
    rcases hmem with ⟨pair, hpair, hsnd⟩
    rcases pair with ⟨b, j⟩
    simp only at hsnd
    subst j
    have hf := List.mem_filter.mp hpair
    have hz := List.mem_zipIdx hf.1
    have hb_eq : b = bs[i] := by simpa using hz.2.2
    have hb_mark : b = mark := LawfulBEq.eq_of_beq hf.2
    rw [← hb_eq]
    exact hb_mark
  · intro hbs
    have hzopt : (bs.zipIdx)[i]? = some (bs[i], i) := by
      rw [List.getElem?_zipIdx]
      simp [List.getElem?_eq_getElem hi]
    rw [List.getElem?_eq_some_iff] at hzopt
    rcases hzopt with ⟨hlt, hval⟩
    have hzmem : (bs[i], i) ∈ bs.zipIdx := List.mem_of_getElem hval
    have hfmem : (bs[i], i) ∈ bs.zipIdx.filter (fun (b, _) => b == mark) := by
      rw [List.mem_filter]
      exact ⟨hzmem, by simp [hbs]⟩
    have hmapped : i ∈ ((bs.zipIdx.filter (fun (b, _) => b == mark)).map Prod.snd) :=
      List.mem_map.mpr ⟨(bs[i], i), hfmem, rfl⟩
    exact List.elem_iff.mpr hmapped

/-! ### Gap decoding -/

private theorem undo_gaps (p : Nat) (ids : List Nat) :
    (∀ i ∈ ids, p ≤ i) → ids.Pairwise (fun a b => a < b) →
    undoGaps p (gaps p ids) = ids := by
  induction ids generalizing p with
  | nil => intro _ _; rfl
  | cons i is ih =>
      intro hge hpair
      rw [gaps, undoGaps]
      have hpi : p ≤ i := hge i (by simp)
      have hstep : p + (i - p) = i := Nat.add_sub_of_le hpi
      rw [hstep]
      rw [List.pairwise_cons] at hpair
      have hge' : ∀ j ∈ is, i + 1 ≤ j := by
        intro j hj
        have hij : i < j := hpair.1 j hj
        omega
      rw [ih (i + 1) hge' hpair.2]

private theorem ids_increasing (fs : List Bool) (c : Bool) :
    (ids fs c).Pairwise (fun a b => a < b) := by
  unfold ids
  have hp := (zipIdx_pairwise_snd fs 0).filter (fun (flag, _) => flag == c)
  exact List.Pairwise.map Prod.snd (fun _ _ h => h) hp

private theorem expand_ids (fs : List Bool) (c : Bool) (i : Nat) (hi : i < fs.length) :
    ((undoGaps 0 (gaps 0 (ids fs c))).contains i = true ↔ fs[i] = c) := by
  have hge : ∀ j ∈ ids fs c, 0 ≤ j := by omega
  have hdec := undo_gaps 0 (ids fs c) hge (ids_increasing fs c)
  rw [hdec]
  exact zipIdx_positions_contains fs c i hi

/-! ### Validity ranks and the dense stream -/

private theorem dense_append (as bs : List (Option Int)) :
    dense (as ++ bs) = dense as ++ dense bs := by
  induction as with
  | nil => rfl
  | cons x xs ih =>
      cases x <;> simp [dense]

private theorem dense_length_flags (ys : List (Option Int)) :
    (dense ys).length = (ys.map Option.isSome).count true := by
  induction ys with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simpa [dense, List.count_cons] using ih
      | some v =>
          have h := congrArg (fun n => n + 1) ih
          simpa [dense, List.count_cons] using h

private theorem dense_take_length (xs : List (Option Int)) (i : Nat) :
    (dense (xs.take i)).length = ((flags xs).take i).count true := by
  rw [dense_length_flags]
  simp [flags]

private theorem dense_drop_head (xs : List (Option Int)) (i : Nat) (_hi : i < xs.length)
    (w : Int) (hv : xs[i]? = some (some w)) : (dense (xs.drop i))[0]? = some w := by
  have h0 : (xs.drop i)[0]? = xs[i]? := by
    rw [List.getElem?_drop]
    simp
  have hx : (xs.drop i)[0]? = some (some w) := by rw [h0, hv]
  cases hdrop : xs.drop i with
  | nil => simp [hdrop] at hx
  | cons x rest =>
      have hxv : x = some w := by simpa [hdrop] using hx
      subst x
      simp [dense]

private theorem dense_rank (xs : List (Option Int)) (i : Nat) (hi : i < xs.length)
    (w : Int) (hv : xs[i]? = some (some w)) :
    (dense xs)[((flags xs).take i).count true]? = some w := by
  have happ : dense xs = dense (xs.take i) ++ dense (xs.drop i) := by
    have h := dense_append (xs.take i) (xs.drop i)
    rw [List.take_append_drop] at h
    exact h
  rw [happ]
  rw [← dense_take_length xs i]
  rw [List.getElem?_append]
  simp only [Nat.lt_irrefl, ↓reduceIte, Nat.sub_self]
  exact dense_drop_head xs i hi w hv

private theorem fill_value (xs : List (Option Int)) (i : Nat) (hi : i < xs.length) :
    (fill (flags xs) (dense xs))[i]? = xs[i]? := by
  have hq : (fill (flags xs) (dense xs))[i]? = xs[i]? := by
    unfold fill
    rw [List.getElem?_map, List.getElem?_zipIdx]
    simp only [Nat.zero_add]
    have hflag : (flags xs)[i]? = some xs[i].isSome := by
      rw [List.getElem?_eq_getElem (by simpa [flags] using hi)]
      simp [flags, List.getElem_map]
    cases hxi : xs[i] with
    | none =>
        have hv : xs[i]? = some none := by
          rw [List.getElem?_eq_getElem hi, hxi]
        simp [hxi, hv] at hflag ⊢
        simp [hflag]
    | some w =>
        have hv : xs[i]? = some (some w) := by
          rw [List.getElem?_eq_getElem hi, hxi]
        have hranklt : ((flags xs).take i).count true < (dense xs).length := by
          have hlen := dense_take_length xs i
          have happ : dense xs = dense (xs.take i) ++ dense (xs.drop i) := by
            have h := dense_append (xs.take i) (xs.drop i)
            rw [List.take_append_drop] at h
            exact h
          have hpos : 0 < (dense (xs.drop i)).length := by
            have hd := dense_drop_head xs i hi w hv
            rw [List.getElem?_eq_some_iff] at hd
            rcases hd with ⟨hlt, _⟩
            omega
          rw [← hlen, happ, List.length_append]
          omega
        have hsome : xs[i].isSome = true := by simp [hxi]
        rw [hflag, hsome]
        simp [hranklt, hv]
        have hd := dense_rank xs i hi w hv
        rw [List.getElem?_eq_getElem hranklt] at hd
        exact Option.some.inj hd
  exact hq

/-! ### Base-256 streams and bit-packed lane reads -/

private theorem fromLittle_append_singleton (l : List Nat) (b : Nat) :
    fromLittle (l ++ [b]) = fromLittle l + 256 ^ l.length * b := by
  induction l with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256 ^ xs.length) 256, Nat.mul_assoc]
      omega

private theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
    v % (m * 256) = v % m + m * ((v / m) % 256) := by
  let r := v % m
  let q := v / m
  let s := q % 256
  let t := q / 256
  have hr : r < m := by simpa [r] using Nat.mod_lt v hm
  have hs : s < 256 := by simpa [s] using Nat.mod_lt q (by decide : 0 < 256)
  have hq : q = s + 256 * t := by
    dsimp [s, t]
    exact (Nat.mod_add_div q 256).symm
  have hv : v = r + m * q := by
    dsimp [r, q]
    exact (Nat.mod_add_div v m).symm
  have hs_le : s ≤ 255 := Nat.lt_succ_iff.mp (by simpa using hs)
  have hsum_lt : r + m * s < m * 256 := by
    have hm_le : m * s ≤ m * 255 := Nat.mul_le_mul_left m hs_le
    omega
  have hrewrite : v = (r + m * s) + (m * 256) * t := by
    rw [hv, hq]
    rw [Nat.mul_add, Nat.mul_assoc]
    omega
  have hleft : v % (m * 256) = r + m * s := by
    calc
      v % (m * 256) = ((r + m * s) + (m * 256) * t) % (m * 256) := by rw [hrewrite]
      _ = (r + m * s) % (m * 256) := by rw [Nat.add_mul_mod_self_left]
      _ = r + m * s := Nat.mod_eq_of_lt hsum_lt
  have hright : v % m + m * ((v / m) % 256) = r + m * s := by rfl
  exact hleft.trans hright.symm

private theorem fromLittle_little (v k : Nat) :
    fromLittle ((List.range k).map (fun j => (v / 256 ^ j) % 256)) = v % 256 ^ k := by
  induction k with
  | zero => simp [fromLittle, Nat.mod_one]
  | succ k ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [fromLittle_append_singleton]
      simp only [List.length_map, List.length_range]
      rw [ih, Nat.pow_succ]
      exact (mod_mul_256 v (256 ^ k) (Nat.pow_pos (by decide : 0 < 256))).symm

private theorem little_succ_append (v k : Nat) :
    little v (k + 1) = little v k ++ [(v / 256 ^ k) % 256] := by
  unfold little
  rw [List.range_succ, List.map_append]
  simp

private theorem little_cons (v k : Nat) :
    little v (k + 1) = (v % 256) :: little (v / 256) k := by
  unfold little
  rw [List.range_succ_eq_map, List.map_cons, List.map_map]
  simp only [Nat.pow_zero, Nat.div_one]
  congr 1
  apply List.map_congr_left
  intro i hi
  simp only [Function.comp_apply]
  have hp : 256 ^ Nat.succ i = 256 * 256 ^ i := Nat.pow_succ'
  rw [hp, ← Nat.div_div_eq_div_mul]

private theorem little_drop (v s k : Nat) :
    (little v k).drop s = little (v / 256 ^ s) (k - s) := by
  induction s generalizing v k with
  | zero => simp [little]
  | succ s ih =>
      cases k with
      | zero => simp [little]
      | succ k =>
          rw [little_cons, List.drop_succ_cons, ih]
          have hp : (v / 256) / 256 ^ s = v / 256 ^ (s + 1) := by
            rw [Nat.div_div_eq_div_mul, Nat.pow_succ']
          rw [hp]
          congr 1
          omega

private theorem little_take_le (v k m : Nat) (hm : m ≤ k) :
    (little v k).take m = little v m := by
  induction k generalizing m with
  | zero =>
      have : m = 0 := by omega
      subst m
      simp [little]
  | succ k ih =>
      by_cases hmk : m ≤ k
      · rw [little_succ_append, List.take_append_of_le_length]
        · exact ih m hmk
        · simpa [little] using hmk
      · have hm_eq : m = k + 1 := by omega
        subst m
        simpa [little] using (List.take_length (l := little v (k + 1)))

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hmod : (bits + 7) % 8 < 8 := Nat.mod_lt _ (by decide)
  have hdiv := Nat.mod_add_div (bits + 7) 8
  omega

private theorem byteCount_le_of_le (bits b : Nat) (h : bits ≤ 8 * b) : byteCount bits ≤ b := by
  unfold byteCount
  have hmod : (bits + 7) % 8 < 8 := Nat.mod_lt _ (by decide)
  have hdiv := Nat.mod_add_div (bits + 7) 8
  omega

private theorem word_shift (w : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w) :
    word w xs / 2 ^ w = word w (List.drop 1 xs) := by
  cases xs with
  | nil => simp [word]
  | cons x xs =>
      have hx : x < 2 ^ w := h x (by simp)
      have hxmod : x % 2 ^ w = x := Nat.mod_eq_of_lt hx
      have hxdiv : x / 2 ^ w = 0 := Nat.div_eq_of_lt hx
      rw [word, hxmod]
      rw [Nat.add_comm]
      rw [Nat.mul_add_div (Nat.two_pow_pos w) (word w xs) x, hxdiv, Nat.add_zero]
      simp

private theorem word_digit (w i : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w)
    (hi : i < xs.length) : (word w xs / 2 ^ (i * w)) % 2 ^ w = xs[i] := by
  induction i generalizing xs with
  | zero =>
      simp only [Nat.zero_mul, Nat.pow_zero, Nat.div_one]
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have hx : x < 2 ^ w := h x (by simp)
          simp [word, Nat.mod_eq_of_lt hx]
  | succ i ih =>
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have ht : ∀ y ∈ xs, y < 2 ^ w := by
            intro y hy
            exact h y (by simp [hy])
          have hp : 2 ^ ((i + 1) * w) = 2 ^ w * 2 ^ (i * w) := by
            have hmul : (i + 1) * w = w + i * w := by
              rw [Nat.succ_mul, Nat.add_comm]
            rw [hmul, Nat.pow_add]
          rw [hp, ← Nat.div_div_eq_div_mul, word_shift w (x :: xs) h]
          have hi' : i < xs.length := Nat.lt_of_succ_lt_succ (by simpa using hi)
          exact ih xs ht hi'

private theorem takeAt_pack (w i : Nat) (xs : List Nat) (hbound : ∀ x ∈ xs, x < 2 ^ w)
    (hi : i < xs.length) : takeAt (pack w xs) w i = xs[i] := by
  by_cases hw : w = 0
  · subst w
    have hxi : xs[i] = 0 := by
      have hb := hbound (xs[i]) (List.getElem_mem hi)
      simpa using hb
    simp [takeAt, pack, byteCount, hxi, fromLittle]
  · let v := word w xs
    let k := byteCount (xs.length * w)
    let start := i * w / 8
    let shift := i * w % 8
    let m := byteCount (shift + w)
    have hk_bits : xs.length * w ≤ 8 * k := by
      simpa [k] using bits_le_byteCount (xs.length * w)
    have hiw_le : i * w ≤ xs.length * w := Nat.mul_le_mul_right w (Nat.le_of_lt hi)
    have hiw1_le : (i + 1) * w ≤ xs.length * w :=
      Nat.mul_le_mul_right w (Nat.succ_le_of_lt hi)
    have hstep : (i + 1) * w = i * w + w := by rw [Nat.succ_mul]
    have hstart_le_k : start ≤ k := by
      have hdiv : i * w / 8 ≤ (8 * k) / 8 := Nat.div_le_div_right (by omega)
      have hcancel : (8 * k) / 8 = k := by omega
      simpa [start, hcancel] using hdiv
    have hshiftlt : shift < 8 := by
      dsimp [shift]
      exact Nat.mod_lt _ (by decide)
    have hiweq : i * w = 8 * start + shift := by
      dsimp [start, shift]
      have hmad := Nat.div_add_mod (i * w) 8
      omega
    have hmulsub : 8 * (k - start) = 8 * k - 8 * start := by omega
    have hwindow : shift + w ≤ 8 * (k - start) := by
      rw [hmulsub]
      omega
    have hm_le : m ≤ k - start := by
      dsimp [m]
      exact byteCount_le_of_le (shift + w) (k - start) hwindow
    have hfl : fromLittle ((little v k).drop start |>.take m) =
        (v / 256 ^ start) % 256 ^ m := by
      rw [little_drop, little_take_le _ _ _ hm_le]
      simpa [little] using (fromLittle_little (v / 256 ^ start) m)
    change (fromLittle ((little v k).drop start |>.take m) / 2 ^ shift) % 2 ^ w = xs[i]
    rw [hfl]
    have h8m_ge_shift : shift ≤ 8 * m := by
      have hb := bits_le_byteCount (shift + w)
      dsimp [m] at hb ⊢
      omega
    have h8m_ge_w : w ≤ 8 * m - shift := by
      have hb := bits_le_byteCount (shift + w)
      dsimp [m] at hb ⊢
      omega
    have hpow256 : 256 ^ m = 2 ^ (8 * m) := by
      have h256 : (256 : Nat) = 2 ^ 8 := by decide
      rw [h256, Nat.pow_mul]
    let q := v / 256 ^ start
    let r := q % 256 ^ m
    let t := q / 256 ^ m
    have hq : q = r + 256 ^ m * t := by
      dsimp [r, t, q]
      exact (Nat.mod_add_div (v / 256 ^ start) (256 ^ m)).symm
    have hdivq : q / 2 ^ shift = r / 2 ^ shift + 2 ^ (8 * m - shift) * t := by
      rw [hq, hpow256]
      have hfactor : 2 ^ (8 * m) = 2 ^ shift * 2 ^ (8 * m - shift) := by
        rw [← Nat.pow_add]
        congr 1
        omega
      rw [hfactor, Nat.mul_assoc]
      exact Nat.add_mul_div_left r (2 ^ (8 * m - shift) * t) (Nat.two_pow_pos shift)
    have hmod_eq : (q / 2 ^ shift) % 2 ^ w = (r / 2 ^ shift) % 2 ^ w := by
      rw [hdivq]
      have hfactor : 2 ^ (8 * m - shift) * t =
          2 ^ w * (2 ^ (8 * m - shift - w) * t) := by
        have hexp : 8 * m - shift = w + (8 * m - shift - w) := by omega
        rw [hexp, Nat.pow_add, Nat.mul_assoc]
        simp
      rw [hfactor]
      exact Nat.add_mul_mod_self_left (r / 2 ^ shift) (2 ^ w)
        (2 ^ (8 * m - shift - w) * t)
    have h256start : 256 ^ start = 2 ^ (8 * start) := by
      have h256 : (256 : Nat) = 2 ^ 8 := by decide
      rw [h256, Nat.pow_mul]
    have hqv : q / 2 ^ shift = v / 2 ^ (i * w) := by
      dsimp [q]
      rw [h256start, Nat.div_div_eq_div_mul]
      have hp : 2 ^ (8 * start) * 2 ^ shift = 2 ^ (i * w) := by
        rw [← Nat.pow_add]
        congr 1
        omega
      rw [hp]
    rw [← hmod_eq, hqv]
    exact word_digit w i xs hbound hi

/-! ### Variable-length charges and flip positions -/

private theorem ubytes_le_iff (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
  constructor
  · intro h
    by_cases hn : n = 0
    · subst hn
      exact Nat.pow_pos (by decide : 0 < 128)
    · have hw : n < 2 ^ width n := by
        unfold width
        simp [hn]
        exact Nat.lt_log2_self
      have h1 : (width n + 6) / 7 ≤ k := Nat.le_trans (Nat.le_max_right 1 _) h
      have h3 : width n ≤ 7 * k := by omega
      calc
        n < 2 ^ width n := hw
        _ ≤ 2 ^ (7 * k) := Nat.pow_le_pow_right (by omega) h3
        _ = (2 ^ 7) ^ k := by rw [Nat.pow_mul]
        _ = 128 ^ k := by
          have h : (2 ^ 7 : Nat) = 128 := by decide
          rw [h]
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · rw [Nat.div_le_iff_le_mul_add_pred (by omega)]
      by_cases hn : n = 0
      · subst n
        simp [width]
      · have h128 : 128 ^ k = 2 ^ (7 * k) := by
          have : (128 : Nat) = 2 ^ 7 := by decide
          rw [this, Nat.pow_mul]
        have hlt : n < 2 ^ (7 * k) := by rwa [h128] at h
        have hw : width n ≤ 7 * k := by
          unfold width
          simp [hn]
          have hlog : Nat.log2 n < 7 * k := (Nat.log2_lt hn).mpr hlt
          omega
        omega

private theorem uleb_bounds (n : Nat) : n < 128 ^ (uleb n).length := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
      · rw [uleb]
        simp [hlt]
        have hdivlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        have hd := ih (n / 128) hdivlt
        have hmod : n % 128 < 128 := Nat.mod_lt _ (by decide)
        have hmul : 128 * (n / 128) ≤ n := Nat.mul_div_le n 128
        rw [Nat.pow_succ']
        have hn : n = n % 128 + 128 * (n / 128) := (Nat.mod_add_div n 128).symm
        have hsum : n % 128 + 128 * (n / 128) < 128 * 128 ^ List.length (uleb (n / 128)) :=
          by omega
        omega

private theorem uleb_minimal (n k : Nat) (hk : 0 < k) (hbound : n < 128 ^ k) :
    (uleb n).length ≤ k := by
  induction n using Nat.strongRecOn generalizing k with
  | ind n ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
        omega
      · rw [uleb]
        simp [hlt]
        have hkge : 2 ≤ k := by
          by_cases hk1 : k = 1
          · subst k
            simp at hbound
            omega
          · omega
        have hdivlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        have hmul : 128 * (n / 128) ≤ n := Nat.mul_div_le n 128
        have hpow : 128 ^ k = 128 * 128 ^ (k - 1) := by
          have hs : (k - 1) + 1 = k := by omega
          have hp' : 128 ^ ((k - 1) + 1) = 128 * 128 ^ (k - 1) := Nat.pow_succ'
          rw [hs] at hp'
          exact hp'
        have hdivbound : n / 128 < 128 ^ (k - 1) := by
          apply Nat.lt_of_mul_lt_mul_left
          rw [← hpow]
          omega
        have hd := ih (n / 128) hdivlt (k - 1) (by omega) hdivbound
        omega

private theorem uleb_length (n : Nat) : (uleb n).length = ubytes n := by
  have hpos : 0 < ubytes n := by unfold ubytes; omega
  have hulpos : 0 < (uleb n).length := by
    rw [uleb]
    split <;> simp
  apply Nat.le_antisymm
  · exact uleb_minimal n (ubytes n) hpos
      ((ubytes_le_iff n (ubytes n) hpos).mp (by omega))
  · exact (ubytes_le_iff n (uleb n).length hulpos).mpr (uleb_bounds n)

private theorem flatMap_uleb_length (gs : List Nat) : (gs.flatMap uleb).length = streamBytes gs := by
  induction gs with
  | nil => rfl
  | cons g gs ih => simp [streamBytes, ih, uleb_length]

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem gaps_length (p : Nat) (ids : List Nat) : (gaps p ids).length = ids.length := by
  induction ids generalizing p with
  | nil => rfl
  | cons i is ih => rw [gaps, List.length_cons, ih (i + 1), List.length_cons]

private theorem flips_contains (fs : List Bool) (j : Nat) :
    (flips fs).contains j = true ↔ 0 < j ∧ j < fs.length ∧ fs[j - 1]? != fs[j]? := by
  unfold flips
  constructor
  · intro h
    have hmem : j ∈ ((fs.zipIdx.filter
        (fun (f, i) => 0 < i && fs[i - 1]? != some f)).map Prod.snd) := List.elem_iff.mp h
    rw [List.mem_map] at hmem
    rcases hmem with ⟨pair, hpair, hsnd⟩
    rcases pair with ⟨b, k⟩
    simp only at hsnd
    subst k
    have hf := List.mem_filter.mp hpair
    have hz := List.mem_zipIdx hf.1
    have hltj : j < fs.length := by omega
    have hbopt : some b = fs[j]? := by
      rw [List.getElem?_eq_getElem hltj]
      simp only [Option.some.injEq]
      simpa using hz.2.2
    have hP := hf.2
    dsimp at hP
    have h0 : 0 < j := by
      have ha := hP
      by_cases hz0 : decide (0 < j) = true <;>
        by_cases hzn : fs[j - 1]? != some b <;> simp_all
    have hne : fs[j - 1]? != some b := by
      have ha := hP
      by_cases hz0 : decide (0 < j) = true <;>
        by_cases hzn : fs[j - 1]? != some b <;> simp_all
    rw [hbopt] at hne
    exact ⟨h0, hltj, hne⟩
  · intro h
    have hjopt : fs[j]? = some (fs[j]?.getD false) := by
      rw [List.getElem?_eq_getElem h.2.1]
      simp
    have hzopt : (fs.zipIdx)[j]? = some (fs[j]?.getD false, j) := by
      rw [List.getElem?_zipIdx]
      rw [hjopt]
      simp
    rw [List.getElem?_eq_some_iff] at hzopt
    rcases hzopt with ⟨hlt, hval⟩
    have hzmem : (fs[j]?.getD false, j) ∈ fs.zipIdx := List.mem_of_getElem hval
    have hfmem : (fs[j]?.getD false, j) ∈ fs.zipIdx.filter
        (fun (f, i) => 0 < i && fs[i - 1]? != some f) := by
      rw [List.mem_filter]
      constructor
      · exact hzmem
      · change (decide (0 < j) && fs[j - 1]? != some (fs[j]?.getD false)) = true
        rw [← hjopt]
        simp [h.1, h.2.2]
    have hmapped : j ∈ ((fs.zipIdx.filter
        (fun (f, i) => 0 < i && fs[i - 1]? != some f)).map Prod.snd) :=
      List.mem_map.mpr ⟨(fs[j]?.getD false, j), hfmem, rfl⟩
    exact List.elem_iff.mpr hmapped

private theorem zipIdx_nodup_snd : ∀ (fs : List Bool) (k : Nat),
    (fs.zipIdx k).Pairwise (fun a b : Bool × Nat => a.2 ≠ b.2) := by
  intro fs
  induction fs with
  | nil => intro k; simp
  | cons b bs ih =>
      intro k
      rw [List.zipIdx_cons, List.pairwise_cons]
      constructor
      · intro p hp'
        have hz := List.mem_zipIdx hp'
        omega
      · simpa [Nat.add_comm] using ih (k + 1)

private theorem flips_nodup (fs : List Bool) : (flips fs).Nodup := by
  unfold flips
  have hf := (zipIdx_nodup_snd fs 0).filter
    (fun (f, i) => 0 < i && fs[i - 1]? != some f)
  exact List.Pairwise.map Prod.snd (fun _ _ h => h) hf

private theorem countP_le_succ (l : List Nat) (hn : l.Nodup) (i : Nat) :
    l.countP (fun j => j ≤ i + 1) =
      l.countP (fun j => j ≤ i) + if l.contains (i + 1) then 1 else 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons]
      rw [List.nodup_cons] at hn
      have ih' := ih hn.2
      by_cases hle : a ≤ i
      · have hle1 : a ≤ i + 1 := by omega
        have hane : a ≠ i + 1 := by omega
        have hane' : i + 1 ≠ a := by omega
        rw [ih', decide_eq_true_eq.mpr hle, decide_eq_true_eq.mpr hle1]
        have hc : (a :: as).contains (i + 1) = as.contains (i + 1) := by simp [hane']
        rw [hc]
        omega
      · by_cases hle1 : a ≤ i + 1
        · have haeq : a = i + 1 := by omega
          have hnotmem : i + 1 ∉ as := by
            intro hm
            exact hn.1 (by simpa [haeq] using hm)
          have hc_as : as.contains (i + 1) = false := by
            by_cases ht : as.contains (i + 1) = true
            · exact False.elim (hnotmem (List.elem_iff.mp ht))
            · cases h2 : as.contains (i + 1) <;> simp_all
          rw [ih', decide_eq_true_eq.mpr hle1, decide_eq_false_iff_not.mpr hle]
          have hc_cons : (a :: as).contains (i + 1) = true := by simp [haeq]
          rw [hc_cons, hc_as]
        · have hane : a ≠ i + 1 := by omega
          have hane' : i + 1 ≠ a := by omega
          rw [ih', decide_eq_false_iff_not.mpr hle1, decide_eq_false_iff_not.mpr hle]
          have hc : (a :: as).contains (i + 1) = as.contains (i + 1) := by simp [hane']
          rw [hc]
          omega

private theorem flips_count_zero (fs : List Bool) :
    (flips fs).countP (fun j => j ≤ 0) = 0 := by
  apply List.countP_eq_zero.mpr
  intro j hj
  have hc : (flips fs).contains j = true := List.elem_iff.mpr hj
  rw [flips_contains] at hc
  intro ht
  have hjle : j ≤ 0 := decide_eq_true_eq.mp ht
  omega

/-! ### Soundness proofs -/

theorem sound_DenseRoundTrip : DenseRoundTrip.Goal := by
  intro xs
  apply List.ext_getElem (by simp [fill, flags])
  intro i h1 h2
  have hq := fill_value xs i h2
  rw [List.getElem?_eq_getElem h1, List.getElem?_eq_getElem h2] at hq
  exact Option.some.inj hq

theorem sound_ValidityBits : ValidityBits.Goal := by
  intro fs i hi
  change takeAt (pack 1 (fs.map Bool.toNat)) 1 i = (fs[i]?.getD false).toNat
  rw [takeAt_pack 1 i (fs.map Bool.toNat)]
  · rw [List.getElem_map]
    simp [List.getElem?_eq_getElem hi]
  · intro x hx
    simp at hx
    omega
  · simpa using hi

theorem sound_MinorityRoundTrip : MinorityRoundTrip.Goal := by
  intro fs
  unfold expandMask sparse
  apply List.ext_getElem (by simp)
  intro i h1 h2
  rw [List.getElem_map, List.getElem_range (by simpa using h2)]
  have hc := expand_ids fs (minority fs) i h2
  by_cases hmem : ((undoGaps 0 (gaps 0 (ids fs (minority fs)))).contains i) = true
  · rw [ite_eq_left hmem]
    exact (hc.mp hmem).symm
  · rw [ite_eq_right hmem]
    have hneq : fs[i] ≠ minority fs := by
      intro hh
      exact hmem (hc.mpr hh)
    cases hm : minority fs <;> cases hf : fs[i] <;> simp_all

theorem sound_ExceptionCardinality : ExceptionCardinality.Goal := by
  intro fs
  unfold sparse ids
  rw [gaps_length]
  rw [zipIdx_filter_length]
  by_cases hle : fs.count true ≤ fs.count false
  · have hm : minority fs = true := by
      unfold minority
      simp [hle]
    rw [hm]
    exact (Nat.min_eq_left hle).symm
  · have hm : minority fs = false := by
      unfold minority
      simp [hle]
    rw [hm]
    exact (Nat.min_eq_right (Nat.le_of_not_ge hle)).symm

theorem sound_SparseWins : SparseWins.Goal := by
  intro fs
  let gs := sparse fs
  change (1 + (uleb gs.length ++ gs.flatMap uleb).length < (marks fs).length ↔
    1 + ubytes gs.length + streamBytes gs < byteCount fs.length)
  have hleft : (uleb gs.length ++ gs.flatMap uleb).length =
      ubytes gs.length + streamBytes gs := by
    rw [List.length_append, uleb_length, flatMap_uleb_length]
  have hmarks : (marks fs).length = byteCount fs.length := by
    unfold marks
    rw [pack_length]
    simp
  rw [hleft, hmarks]
  omega

private theorem parity_succ (c : Nat) : ((c + 1) % 2 == 1) = !((c % 2) == 1) := by
  rw [Nat.add_mod]
  simp only [Nat.one_mod]
  have hm : c % 2 < 2 := Nat.mod_lt _ (by decide)
  have hc : c % 2 = 0 ∨ c % 2 = 1 := by omega
  rcases hc with h0 | h1
  · simp [h0]
  · simp [h1]

private theorem xor_not (a b : Bool) : xor a (!b) = !(xor a b) := by
  cases a <;> cases b <;> rfl

theorem sound_ToggleMask : ToggleMask.Goal := by
  intro fs i hi
  have hpar : ∀ j : Nat, j < fs.length →
      xor (fs.head?.getD false) (((flips fs).countP (fun k => k ≤ j)) % 2 == 1) =
        (fs[j]?.getD false) := by
    intro j
    induction j with
    | zero =>
        intro hj
        cases fs with
        | nil => simp at hj
        | cons f rest =>
            change xor f (((flips (f :: rest)).countP (fun k => k ≤ 0)) % 2 == 1) = f
            rw [flips_count_zero]
            simp
    | succ j ih =>
        intro hj
        have hj' : j < fs.length := by omega
        have ihj := ih hj'
        have hcount := countP_le_succ (flips fs) (flips_nodup fs) j
        have htrans : (flips fs).contains (j + 1) = true ↔ fs[j]? != fs[j + 1]? := by
          rw [flips_contains]
          simp only [Nat.add_sub_cancel]
          constructor
          · intro h
            exact h.2.2
          · intro h
            exact ⟨by omega, hj, h⟩
        by_cases hc : (flips fs).contains (j + 1) = true
        · rw [ite_eq_left hc] at hcount
          rw [hcount, parity_succ, xor_not, ihj]
          have hneq : fs[j] != fs[j + 1] := by
            have hn := (htrans.mp hc)
            rw [List.getElem?_eq_getElem hj', List.getElem?_eq_getElem hj] at hn
            simpa using hn
          cases hfj : fs[j] <;> cases hfj1 : fs[j + 1] <;> simp_all
        · rw [ite_eq_right hc] at hcount
          have heq : fs[j] = fs[j + 1] := by
            have hn : ¬ fs[j]? != fs[j + 1]? := by
              intro hnn
              exact hc (htrans.mpr hnn)
            rw [List.getElem?_eq_getElem hj', List.getElem?_eq_getElem hj] at hn
            cases hfj : fs[j] <;> cases hfj1 : fs[j + 1] <;> simp_all
          have heqopt : fs[j]?.getD false = fs[j + 1]?.getD false := by
            rw [List.getElem?_eq_getElem hj', List.getElem?_eq_getElem hj]
            simp [heq]
          rw [hcount, ← heqopt]
          exact ihj
  rw [List.getElem?_eq_getElem hi]
  have hget : fs[i]?.getD false = fs[i] := by
    rw [List.getElem?_eq_getElem hi]
    simp
  rw [← hget]
  exact (congrArg some (hpar i hi)).symm

theorem sound_SharedMask : SharedMask.Goal := by
  intro a b hflags
  rw [hflags]
  exact sound_DenseRoundTrip b

private theorem expand_mask_ids (fs : List Bool) (c : Bool) :
    expandMask fs.length c (gaps 0 (ids fs c)) = fs := by
  unfold expandMask
  apply List.ext_getElem (by simp)
  intro i h1 h2
  rw [List.getElem_map, List.getElem_range (by simpa using h2)]
  have hc := expand_ids fs c i h2
  by_cases hmem : ((undoGaps 0 (gaps 0 (ids fs c))).contains i) = true
  · rw [ite_eq_left hmem]
    exact (hc.mp hmem).symm
  · rw [ite_eq_right hmem]
    have hneq : fs[i] ≠ c := by
      intro hh
      exact hmem (hc.mpr hh)
    cases hcval : c <;> cases hfval : fs[i] <;> simp_all

private theorem xor_self (a b : Bool) : xor a (xor a b) = b := by
  cases a <;> cases b <;> rfl

theorem sound_MaskXor : MaskXor.Goal := by
  intro a b hlen
  let d := a.zipWith xor b
  let mask := expandMask a.length true (gaps 0 (ids d true))
  have hlen' : a.length = d.length := by
    simp [d, List.length_zipWith, hlen]
  have hexp : mask = d := by
    dsimp [mask]
    rw [hlen']
    exact expand_mask_ids d true
  have hlen_mask : mask.length = a.length := by simp [mask, expandMask]
  apply List.ext_getElem?
  intro i
  by_cases hia : i < a.length
  · have hib : i < b.length := by omega
    have hi' : i < d.length := by simpa [hlen'] using hia
    have himask : i < mask.length := by simpa [hlen_mask] using hia
    have hd : d[i] = xor a[i] b[i] := by
      dsimp [d]
      rw [List.getElem_zipWith]
    have hx : mask[i] = d[i] := by
      have hq := congrArg (fun l : List Bool => l[i]?) hexp
      rw [List.getElem?_eq_getElem himask, List.getElem?_eq_getElem hi'] at hq
      exact Option.some.inj hq
    rw [List.getElem?_zipWith, List.getElem?_eq_getElem hia,
      List.getElem?_eq_getElem himask, List.getElem?_eq_getElem hib]
    simp only [Option.some.injEq]
    rw [hx, hd, xor_self]
  · have ha : a[i]? = none := List.getElem?_eq_none (by omega)
    have hb : b[i]? = none := List.getElem?_eq_none (by omega)
    rw [List.getElem?_zipWith, ha]
    simp [hb]

/-! ### Dictionary coding with a NULL sentinel -/

private theorem width_pow_bound (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · unfold width
    simp [hn]
    exact Nat.lt_log2_self

private theorem mem_distinct_of_dense (xs : Col) (v : Int) (hv : v ∈ dense xs) :
    v ∈ distinct xs := by
  simp [distinct, List.mem_eraseDups, hv]

private theorem codes_bound (xs : Col) :
    ∀ x ∈ codes xs, x < 2 ^ width (distinct xs).length := by
  intro x hx
  rcases List.getElem_of_mem hx with ⟨i, hi, hci⟩
  rw [← hci]
  have hil : i < xs.length := by simpa [codes] using hi
  cases hval : xs[i] with
  | none =>
      have hq : (codes xs)[i]? = some 0 := by
        dsimp [codes]
        rw [List.getElem?_map, List.getElem?_eq_getElem hil]
        simp [hval]
      rw [List.getElem?_eq_getElem hi] at hq
      rw [Option.some.inj hq]
      exact Nat.two_pow_pos _
  | some v =>
      have hq : (codes xs)[i]? = some ((distinct xs).idxOf v + 1) := by
        dsimp [codes]
        rw [List.getElem?_map, List.getElem?_eq_getElem hil]
        simp [hval]
      rw [List.getElem?_eq_getElem hi] at hq
      have hsomev : some v ∈ xs := by simpa [hval] using List.getElem_mem hil
      have hvdense : v ∈ dense xs := by simpa [dense] using hsomev
      have hvd : v ∈ distinct xs := mem_distinct_of_dense xs v hvdense
      have hlt : (distinct xs).idxOf v < (distinct xs).length :=
        List.idxOf_lt_length_of_mem hvd
      have hb := width_pow_bound (distinct xs).length
      rw [Option.some.inj hq]
      omega

private theorem codes_len (xs : Col) : (codes xs).length = xs.length := by
  simp [codes]

private theorem codes_at_none (xs : Col) (i : Nat) (hi : i < xs.length)
    (hv : xs[i] = none) : (codes xs)[i]? = some 0 := by
  unfold codes
  rw [List.getElem?_map, List.getElem?_eq_getElem hi]
  simp [hv]

private theorem codes_at_some (xs : Col) (i : Nat) (hi : i < xs.length) (v : Int)
    (hv : xs[i] = some v) :
    (codes xs)[i]? = some ((distinct xs).idxOf v + 1) := by
  unfold codes
  rw [List.getElem?_map, List.getElem?_eq_getElem hi]
  simp [hv]

theorem sound_SentinelRoundTrip : SentinelRoundTrip.Goal := by
  intro xs
  let d := distinct xs
  let w := width d.length
  apply List.ext_getElem (by simp [striped])
  intro i h1 hi
  have hic : i < (codes xs).length := by simpa [codes_len xs] using hi
  have hc := takeAt_pack w i (codes xs) (by
    intro x hx
    simpa [w, d] using codes_bound xs x hx) hic
  have hq : (striped d (pack w (codes xs)) w xs.length)[i]? = xs[i]? := by
    unfold striped
    rw [List.getElem?_map]
    simp [hi]
    rw [hc]
    cases hval : xs[i] with
    | none =>
        have hc0 : (codes xs)[i] = 0 := by
          have hh := codes_at_none xs i hi hval
          rw [List.getElem?_eq_getElem hic] at hh
          exact Option.some.inj hh
        rw [hc0]
        simp
    | some v =>
        have hcode : (codes xs)[i] = (distinct xs).idxOf v + 1 := by
          have hh := codes_at_some xs i hi v hval
          rw [List.getElem?_eq_getElem hic] at hh
          exact Option.some.inj hh
        have hcode' : (codes xs)[i] = d.idxOf v + 1 := by simpa [d] using hcode
        rw [hcode']
        have hsomev : some v ∈ xs := by simpa [hval] using List.getElem_mem hi
        have hvdense : v ∈ dense xs := by simpa [dense] using hsomev
        have hvd : v ∈ d := by simpa [d] using mem_distinct_of_dense xs v hvdense
        have hlt : d.idxOf v < d.length := List.idxOf_lt_length_of_mem hvd
        have hget : d[d.idxOf v]? = some v := by
          rw [List.getElem?_eq_getElem hlt, List.getElem_idxOf hlt]
        simp [hget]
  have h1' := List.getElem?_eq_getElem h1
  have hi' := List.getElem?_eq_getElem hi
  rw [h1', hi'] at hq
  exact Option.some.inj hq

theorem sound_DenseVsSentinel : DenseVsSentinel.Goal := by
  intro xs
  let d := distinct xs
  let vs := dense xs
  let w := ordinalWidth d.length
  change ((marks (flags xs)).length + (pack w (vs.map d.idxOf)).length <
      (pack (width d.length) (codes xs)).length ↔
    byteCount xs.length + byteCount (vs.length * w) <
      byteCount (xs.length * width d.length))
  have hmarks : (marks (flags xs)).length = byteCount xs.length := by
    unfold marks
    rw [pack_length]
    simp [flags]
  rw [hmarks, pack_length, pack_length]
  rw [codes_len]
  simp [vs]

private theorem replicate_append {α : Type} (n : Nat) (a : α) :
    List.replicate n a ++ [a] = List.replicate (n + 1) a := by
  induction n with
  | zero => rfl
  | succ n ih =>
      simpa [List.replicate_succ] using congrArg (fun l => a :: l) ih

private theorem range_map_none (n : Nat) :
    (List.range n).map (fun _ : Nat => (none : Option Int)) = List.replicate n none := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih]
      exact replicate_append n none

theorem sound_AllNullZeroWidth : AllNullZeroWidth.Goal := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [striped, List.range_succ, List.map_append]
      simp [takeAt, range_map_none, replicate_append]

theorem sound_NullableConstant : NullableConstant.Goal := by
  intro xs v hconst
  apply List.ext_getElem?
  intro i
  by_cases hi : i < xs.length
  · rw [List.getElem?_map]
    have hf : (flags xs)[i]? = some xs[i].isSome := by
      rw [List.getElem?_eq_getElem (by simpa [flags] using hi)]
      simp [flags, List.getElem_map]
    cases hxi : xs[i] with
    | none =>
        rw [hf, List.getElem?_eq_getElem hi]
        simp [hxi]
    | some w =>
        have hsomew : some w ∈ xs := by simpa [hxi] using List.getElem_mem hi
        have hmem : w ∈ dense xs := by simpa [dense] using hsomew
        have hvw : v = w := (hconst w hmem).symm
        rw [hf, List.getElem?_eq_getElem hi]
        simp [hxi, hvw]
  · have hxle : xs.length ≤ i := Nat.le_of_not_gt hi
    have hnone : xs[i]? = none := List.getElem?_eq_none hxle
    have hfle : (flags xs).length ≤ i := by simpa [flags] using hxle
    have hfnone : ((flags xs).map (fun good => if good then some v else none))[i]? = none :=
      List.getElem?_eq_none (by simpa using hfle)
    rw [hfnone, hnone]

theorem sound_SentinelCliff : SentinelCliff.Goal := by
  intro d hd
  by_cases hd1 : d = 1
  · subst d
    constructor
    · intro _
      exact ⟨0, by decide⟩
    · intro _
      simp [width]
  · have hd2 : 1 < d := by omega
    have hdne : d ≠ 0 := by omega
    have hdm1ne : d - 1 ≠ 0 := by omega
    constructor
    · intro hlt
      unfold width at hlt
      simp [hdne, hdm1ne] at hlt
      have hloglt : Nat.log2 (d - 1) < Nat.log2 d := by omega
      let k := Nat.log2 d
      have hlow : 2 ^ k ≤ d := (Nat.le_log2 hdne).mp (by omega)
      have hhigh : d < 2 ^ (k + 1) := (Nat.log2_lt hdne).mp (Nat.lt_succ_self _)
      have hltpow : d - 1 < 2 ^ k := (Nat.log2_lt hdm1ne).mp hloglt
      have hdk : d = 2 ^ k := by omega
      exact ⟨k, hdk⟩
    · rintro ⟨k, rfl⟩
      by_cases hk : k = 0
      · subst k
        simp [width]
      · have hpowgt : 1 < 2 ^ k := Nat.one_lt_two_pow hk
        have hm1ne : 2 ^ k - 1 ≠ 0 := by omega
        simp [width, hm1ne, Nat.log2_two_pow]
        have hloglt : Nat.log2 (2 ^ k - 1) < k :=
          (Nat.log2_lt hm1ne).mpr (by omega)
        omega

/-! ### Hot/cold escape tags -/

private theorem countP_le_of_imp (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, q x = true → p x = true) : l.countP q ≤ l.countP p := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons]
      have htail : ∀ x ∈ as, q x = true → p x = true := by
        intro x hx
        exact h x (by simp [hx])
      have hi := ih htail
      by_cases hq : q a = true
      · have hp : p a = true := h a (by simp) hq
        simp [hq, hp]
        omega
      · simp [hq]
        omega

private theorem escape_len_diff (hot : List Int) (x : Int) (hx : x ∉ hot) :
    ∀ ds : List Int,
      ds.countP (fun y => !hot.contains y) -
        ds.countP (fun y => !(x :: hot).contains y) = ds.count x := by
  intro ds
  induction ds with
  | nil => simp
  | cons y ys ih =>
      have hle : ys.countP (fun z => !(x :: hot).contains z) ≤
          ys.countP (fun z => !hot.contains z) := by
        apply countP_le_of_imp
        intro z hz hcold
        by_cases hzx : z == x
        · have hEq : z = x := beq_iff_eq.mp hzx
          subst z
          have hc : (x :: hot).contains x = true := List.elem_iff.mpr (by simp)
          rw [hc] at hcold
          contradiction
        · have heq : (x :: hot).contains z = hot.contains z := by
            cases hc : (x :: hot).contains z with
            | true =>
                have hm : z ∈ x :: hot := List.elem_iff.mp hc
                have hzmem : z ∈ hot := by
                  simp only [List.mem_cons] at hm
                  rcases hm with hzEq | hzmem
                  · exfalso
                    exact hzx (beq_iff_eq.mpr hzEq)
                  · exact hzmem
                exact (List.elem_iff.mpr hzmem).symm
            | false =>
                have hnot : z ∉ hot := by
                  intro hm
                  have hct : (x :: hot).contains z = true :=
                    List.elem_iff.mpr (by simp [hm])
                  rw [hc] at hct
                  contradiction
                have hc2 : hot.contains z = false :=
                  Bool.eq_false_of_ne_true (fun ht => hnot (List.elem_iff.mp ht))
                exact hc2.symm
          rw [← heq]
          exact hcold
      by_cases hyx : y = x
      · subst y
        have hct : hot.contains x = false := by
          cases h : hot.contains x with
          | true => exact absurd (List.elem_iff.mp h) hx
          | false => rfl
        have hp : (!hot.contains x) = true := by rw [hct]; rfl
        have hc : (x :: hot).contains x = true := List.elem_iff.mpr (by simp)
        have hq : (!((x :: hot).contains x)) = false := by rw [hc]; rfl
        have hbeq : (x == x) = true := by simp
        rw [List.countP_cons, List.countP_cons, List.count_cons]
        rw [hp, hq, hbeq]
        rw [ite_eq_left rfl, ite_eq_right (by decide : ¬ (false = true))]
        omega
      · have hbeq : (y == x) = false := by
          by_cases h : y == x
          · exact absurd (beq_iff_eq.mp h) hyx
          · exact Bool.eq_false_of_ne_true h
        by_cases hyhot : y ∈ hot
        · have hct : hot.contains y = true := List.elem_iff.mpr hyhot
          have hctx : (x :: hot).contains y = true :=
            List.elem_iff.mpr (by simp [hyhot])
          have hp : (!hot.contains y) = false := by rw [hct]; rfl
          have hq : (!((x :: hot).contains y)) = false := by rw [hctx]; rfl
          rw [List.countP_cons, List.countP_cons, List.count_cons]
          rw [hp, hq, hbeq]
          rw [ite_eq_right (by decide : ¬ (false = true))]
          exact ih
        · have hct : hot.contains y = false := by
            cases h : hot.contains y with
            | true => exact absurd (List.elem_iff.mp h) hyhot
            | false => rfl
          have hctx : (x :: hot).contains y = false := by
            have hnot : y ∉ x :: hot := by simp [hyx, hyhot]
            exact Bool.eq_false_of_ne_true (fun ht => hnot (List.elem_iff.mp ht))
          have hp : (!hot.contains y) = true := by rw [hct]; rfl
          have hq : (!((x :: hot).contains y)) = true := by rw [hctx]; rfl
          rw [List.countP_cons, List.countP_cons, List.count_cons]
          rw [hp, hq, hbeq]
          rw [ite_eq_left rfl, ite_eq_right (by decide : ¬ (false = true))]
          omega

private theorem dense_count (xs : Col) (v : Int) : (dense xs).count v = xs.count (some v) := by
  induction xs with
  | nil => rfl
  | cons y ys ih =>
      cases y with
      | none =>
          change List.count v (dense ys) = List.count (some v) ys
          exact ih
      | some w =>
          by_cases hwv : w = v
          · subst w
            change List.count v (v :: dense ys) = List.count (some v) (some v :: ys)
            rw [List.count_cons, List.count_cons]
            simp [ih]
          · change List.count v (w :: dense ys) = List.count (some v) (some w :: ys)
            rw [List.count_cons, List.count_cons]
            simp [ih, hwv]

theorem sound_PromoteKey : PromoteKey.Goal := by
  intro hot xs x hx
  change (escapes hot xs).length - (escapes (x :: hot) xs).length =
    xs.count (some x)
  simp only [escapes, filter_length_countP]
  rw [escape_len_diff hot x hx (dense xs)]
  exact dense_count xs x

private theorem hotCodes_len (hot : List Int) (xs : Col) :
    (hotCodes hot xs).length = xs.length := by
  simp [hotCodes]

private theorem hotCodes_at_none (hot : List Int) (xs : Col) (i : Nat)
    (hi : i < xs.length) (hv : xs[i] = none) : (hotCodes hot xs)[i]? = some 0 := by
  unfold hotCodes
  rw [List.getElem?_map, List.getElem?_eq_getElem hi]
  simp [hv]

private theorem hotCodes_at_some_hot (hot : List Int) (xs : Col) (i : Nat)
    (hi : i < xs.length) (v : Int) (hv : xs[i] = some v) (hmem : v ∈ hot) :
    (hotCodes hot xs)[i]? = some (2 + hot.idxOf v) := by
  unfold hotCodes
  rw [List.getElem?_map, List.getElem?_eq_getElem hi]
  simp [hv, hmem]

private theorem hotCodes_at_some_cold (hot : List Int) (xs : Col) (i : Nat)
    (hi : i < xs.length) (v : Int) (hv : xs[i] = some v) (hmem : v ∉ hot) :
    (hotCodes hot xs)[i]? = some 1 := by
  unfold hotCodes
  rw [List.getElem?_map, List.getElem?_eq_getElem hi]
  simp [hv, hmem]

private theorem escape_rank (hot : List Int) : ∀ (xs : Col) (i : Nat), i < xs.length →
    ∀ v : Int, xs[i]? = some (some v) → v ∉ hot →
      (escapes hot xs)[((hotCodes hot xs).take i).count 1]? = some v := by
  intro xs
  induction xs with
  | nil =>
      intro i hi
      simp at hi
  | cons x xs ih =>
      intro i hi v hvi hcv
      cases i with
      | zero =>
          have hxv : x = some v := by
            cases hx : x with
            | none => simp [hx] at hvi
            | some w =>
                have hwv : w = v := by simpa [hx] using hvi
                simp [hwv]
          subst x
          have hdesc : escapes hot (some v :: xs) = v :: escapes hot xs := by
            simp [escapes, dense, hcv]
          rw [hdesc]
          simp [hotCodes, hcv]
      | succ i =>
          have hi'' : i + 1 < xs.length + 1 := by simpa using hi
          have hi' : i < xs.length := Nat.lt_of_succ_lt_succ hi''
          have hvi' : xs[i]? = some (some v) := by
            rw [← hvi]
            simp [hi']
          cases x with
          | none =>
              have hrec := ih i hi' v hvi' hcv
              simp [escapes, dense, hotCodes] at hrec ⊢
              exact hrec
          | some w =>
              by_cases hw : w ∈ hot
              · have hrec := ih i hi' v hvi' hcv
                have hneq : (2 + hot.idxOf w) ≠ 1 := by omega
                simp [escapes, dense, hotCodes, hw, hneq] at hrec ⊢
                exact hrec
              · have hrec := ih i hi' v hvi' hcv
                simp [escapes, dense, hotCodes, hw] at hrec ⊢
                exact hrec

theorem sound_HotEscape : HotEscape.Goal := by
  intro hot xs
  apply List.ext_getElem?
  intro i
  by_cases hi : i < xs.length
  · rw [List.getElem?_map, List.getElem?_zipIdx]
    simp only [Nat.zero_add]
    cases hval : xs[i] with
    | none =>
        rw [hotCodes_at_none hot xs i hi hval]
        rw [List.getElem?_eq_getElem hi, hval]
        simp
    | some v =>
        by_cases hv : v ∈ hot
        · rw [hotCodes_at_some_hot hot xs i hi v hval hv]
          have hlt : hot.idxOf v < hot.length := List.idxOf_lt_length_of_mem hv
          have hget : hot[hot.idxOf v]? = some v := by
            rw [List.getElem?_eq_getElem hlt, List.getElem_idxOf hlt]
          have hne1 : 2 + hot.idxOf v ≠ 1 := by omega
          rw [List.getElem?_eq_getElem hi, hval]
          simp [hget, hne1]
        · rw [hotCodes_at_some_cold hot xs i hi v hval hv]
          have hvopt : xs[i]? = some (some v) := by
            rw [List.getElem?_eq_getElem hi, hval]
          have hesc := escape_rank hot xs i hi v hvopt hv
          rw [List.getElem?_eq_getElem hi, hval]
          simp [hesc]
  · have htagle : (hotCodes hot xs).length ≤ i := by
      simpa [hotCodes_len] using (Nat.le_of_not_gt hi)
    have hztnone : ((hotCodes hot xs).zipIdx)[i]? = none :=
      List.getElem?_eq_none (by simpa using htagle)
    have hxnone : xs[i]? = none := List.getElem?_eq_none (Nat.le_of_not_gt hi)
    rw [List.getElem?_map, hztnone]
    simp [hxnone]

theorem sound_TranslateLocal : TranslateLocal.Goal := by
  intro xs global hglobal
  dsimp only
  apply List.ext_getElem?
  intro i
  by_cases hi : i < xs.length
  · rw [List.getElem?_map]
    cases hval : xs[i] with
    | none =>
        rw [codes_at_none xs i hi hval]
        rw [List.getElem?_eq_getElem hi, hval]
        simp
    | some v =>
        rw [codes_at_some xs i hi v hval]
        rw [List.getElem?_eq_getElem hi, hval]
        have hsomev : some v ∈ xs := by simpa [hval] using List.getElem_mem hi
        have hvdense : v ∈ dense xs := by simpa [dense] using hsomev
        have hvd : v ∈ distinct xs := mem_distinct_of_dense xs v hvdense
        have hlt : (distinct xs).idxOf v < (distinct xs).length :=
          List.idxOf_lt_length_of_mem hvd
        have hdget : (distinct xs)[(distinct xs).idxOf v]? = some v := by
          rw [List.getElem?_eq_getElem hlt, List.getElem_idxOf hlt]
        have htrans : ((distinct xs).map global.idxOf)[(distinct xs).idxOf v]? =
            some (global.idxOf v) := by
          rw [List.getElem?_map, hdget]
          simp
        have hgmem : v ∈ global := hglobal v hvdense
        have hglt : global.idxOf v < global.length := List.idxOf_lt_length_of_mem hgmem
        have hgget : global[global.idxOf v]? = some v := by
          rw [List.getElem?_eq_getElem hglt, List.getElem_idxOf hglt]
        simp [hdget, hgget]
  · have hcle : (codes xs).length ≤ i := by
      simpa [codes_len] using (Nat.le_of_not_gt hi)
    have hcnone : (codes xs)[i]? = none := List.getElem?_eq_none hcle
    have hxnone : xs[i]? = none := List.getElem?_eq_none (Nat.le_of_not_gt hi)
    rw [List.getElem?_map, hcnone]
    simp [hxnone]

/-! ### Boundary between two flip tables -/

private theorem exists_decomp_of_mem {α : Type} (l : List α) (a : α) (h : a ∈ l) :
    ∃ u v, l = u ++ a :: v := by
  induction l with
  | nil => exact absurd h (by simp)
  | cons x l ih =>
      rcases List.mem_cons.mp h with rfl | h'
      · exact ⟨[], l, rfl⟩
      · obtain ⟨u, v, huv⟩ := ih h'
        exact ⟨x :: u, v, congrArg (fun t => x :: t) huv⟩

private theorem perm_move_front {α : Type} (u v : List α) (x : α) :
    List.Perm (u ++ x :: v) (x :: u ++ v) := by
  induction u with
  | nil => exact List.Perm.refl _
  | cons y u ih => exact (List.Perm.cons y ih).trans (List.Perm.swap x y (u ++ v))

private theorem nodup_mid_not_mem {α : Type} {u : List α} {x : α} {v : List α}
    (h : (u ++ x :: v).Nodup) : x ∉ u ++ v := by
  induction u with
  | nil =>
      intro hm
      exact (List.nodup_cons.mp h).1 hm
  | cons y u' ih =>
      intro hm
      cases List.mem_cons.mp hm with
      | inl hxy =>
          subst hxy
          exact (List.nodup_cons.mp h).1 (by simp)
      | inr hm' => exact ih (List.nodup_cons.mp h).2 hm'

private theorem perm_of_nodup_mem {α : Type} {l₁ : List α} (n₁ : l₁.Nodup) :
    ∀ l₂ : List α, l₂.Nodup → (∀ a, a ∈ l₁ ↔ a ∈ l₂) → List.Perm l₁ l₂ := by
  induction l₁ with
  | nil =>
      intro l₂ _ h
      cases l₂ with
      | nil => exact List.Perm.refl _
      | cons y ys =>
          have hy : y ∈ ([] : List α) := (h y).mpr (by simp)
          exact absurd hy (by simp)
  | cons x t ih =>
      intro l₂ n₂ h
      have hx : x ∈ l₂ := (h x).mp (by simp)
      obtain ⟨u, v, huv⟩ := exists_decomp_of_mem l₂ x hx
      have hn' : (u ++ x :: v).Nodup := by rw [← huv]; exact n₂
      have hp : List.Perm (u ++ x :: v) (x :: u ++ v) := perm_move_front u v x
      have h2n : (x :: (u ++ v)).Nodup := hp.nodup_iff.mp hn'
      have nxu : x ∉ u ++ v := nodup_mid_not_mem hn'
      have hxt : x ∉ t := (List.nodup_cons.mp n₁).1
      have hmem : ∀ a, a ∈ t ↔ a ∈ u ++ v := by
        intro a
        constructor
        · intro ha
          have h2 : a ∈ l₂ := (h a).mp (List.mem_cons_of_mem x ha)
          rw [huv, List.mem_append, List.mem_cons] at h2
          rcases h2 with hu | hax | hv
          · exact List.mem_append.mpr (Or.inl hu)
          · subst hax
            exact absurd ha hxt
          · exact List.mem_append.mpr (Or.inr hv)
        · intro h2
          have h3 : a ∈ l₂ := by
            rw [huv, List.mem_append]
            exact Or.imp (fun hu => hu) (fun hv => List.mem_cons_of_mem x hv)
              (List.mem_append.mp h2)
          have h5 : a ∈ x :: t := (h a).mpr h3
          cases List.mem_cons.mp h5 with
          | inl hax => subst hax; exact absurd h2 nxu
          | inr ht => exact ht
      have hperm := ih (List.nodup_cons.mp n₁).2 (u ++ v) (List.nodup_cons.mp h2n).2 hmem
      have hback : List.Perm (x :: (u ++ v)) l₂ := by rw [huv]; exact hp.symm
      exact (List.Perm.cons x hperm).trans hback

private theorem getElem?_pred_length {α : Type} (l : List α) (hl : l ≠ []) :
    l[l.length - 1]? = l.getLast? := by
  induction l with
  | nil => contradiction
  | cons a as ih =>
      cases as with
      | nil => simp
      | cons b bs =>
          simp only [List.length_cons, Nat.add_sub_cancel]
          rw [List.getElem?_cons_succ]
          simpa using ih (by simp : b :: bs ≠ [])

private theorem flips_bound (fs : List Bool) (i : Nat) (hi : i ∈ flips fs) : i < fs.length := by
  have hc : (flips fs).contains i = true := List.elem_iff.mpr hi
  rw [flips_contains] at hc
  exact hc.2.1

private theorem flips_append_singleton (l : List Bool) (x : Bool) :
    (flips (l ++ [x])).Perm
      (flips l ++ if l ≠ [] && l.getLast? != some x then [l.length] else []) := by
  apply perm_of_nodup_mem (flips_nodup _)
  · by_cases hcond : l ≠ [] && l.getLast? != some x
    · rw [ite_eq_left hcond, List.nodup_append]
      refine ⟨flips_nodup l, by simp, ?_⟩
      intro a ha b hb
      simp only [List.mem_singleton] at hb
      subst b
      have hlt := flips_bound l a ha
      omega
    · rw [ite_eq_right hcond]
      simpa using flips_nodup l
  · intro i
    constructor
    · intro hi
      have hc : (flips (l ++ [x])).contains i = true := List.elem_iff.mpr hi
      rw [flips_contains] at hc
      by_cases hil : i < l.length
      · have hca : (flips l).contains i = true := by
          rw [flips_contains]
          refine ⟨hc.1, hil, ?_⟩
          have h1 : (l ++ [x])[i - 1]? = l[i - 1]? := by
            rw [List.getElem?_append]
            simp [show i - 1 < l.length by omega]
          have h2 : (l ++ [x])[i]? = l[i]? := by
            rw [List.getElem?_append]
            simp [hil]
          rw [← h1, ← h2]
          exact hc.2.2
        rw [List.mem_append]
        exact Or.inl (List.elem_iff.mp hca)
      · have hlen : i < l.length + 1 := by
          have := hc.2.1
          simpa using this
        have hieq : i = l.length := by omega
        subst i
        have hnonempty : l ≠ [] := by
          intro hl
          subst hl
          simp at hc
        have hpred : (l ++ [x])[l.length - 1]? = l.getLast? := by
          rw [List.getElem?_append]
          have hle : l.length - 1 < l.length := by omega
          simp only [hle, ↓reduceIte]
          exact getElem?_pred_length l hnonempty
        have hsucc : (l ++ [x])[l.length]? = some x := by
          rw [List.getElem?_append]
          simp
        have hne : (l.getLast? != some x) = true := by
          rw [← hpred, ← hsucc]
          exact hc.2.2
        have hcond : l ≠ [] && l.getLast? != some x := by simp [hnonempty, hne]
        rw [ite_eq_left hcond]
        rw [List.mem_append]
        exact Or.inr (by simp)
    · intro hi
      by_cases hcond : l ≠ [] && l.getLast? != some x
      · rw [ite_eq_left hcond] at hi
        rw [List.mem_append] at hi
        rcases hi with hfl | hri
        · have hc : (flips l).contains i = true := List.elem_iff.mpr hfl
          rw [flips_contains] at hc
          have hc' : (flips (l ++ [x])).contains i = true := by
            rw [flips_contains]
            refine ⟨hc.1, Nat.lt_of_lt_of_le hc.2.1 (by simp), ?_⟩
            have h1 : (l ++ [x])[i - 1]? = l[i - 1]? := by
              rw [List.getElem?_append]
              simp [show i - 1 < l.length by omega]
            have h2 : (l ++ [x])[i]? = l[i]? := by
              rw [List.getElem?_append]
              simp [show i < l.length by omega]
            rw [h1, h2]
            exact hc.2.2
          exact List.elem_iff.mp hc'
        · have hieq : i = l.length := by simpa using List.mem_singleton.mp hri
          subst i
          have hnonempty : l ≠ [] := by
            intro hl
            subst hl
            simp at hcond
          have hposlen : 0 < l.length := List.length_pos_iff.mpr hnonempty
          have hc : (flips (l ++ [x])).contains l.length = true := by
            rw [flips_contains]
            refine ⟨hposlen, by simp, ?_⟩
            have hpred : (l ++ [x])[l.length - 1]? = l.getLast? := by
              rw [List.getElem?_append]
              have hle : l.length - 1 < l.length := by omega
              simp only [hle, ↓reduceIte]
              exact getElem?_pred_length l hnonempty
            have hsucc : (l ++ [x])[l.length]? = some x := by
              rw [List.getElem?_append]
              simp
            rw [hpred, hsucc]
            have hh := hcond
            have hnotEq : ¬ l.getLast? = some x := by simpa [hnonempty] using hh
            by_cases hgl : l.getLast? = some x
            · exact absurd hgl hnotEq
            · simp [hgl]
          exact List.elem_iff.mp hc
      · rw [ite_eq_right hcond] at hi
        have hfl : i ∈ flips l := by simpa using hi
        have hc : (flips l).contains i = true := List.elem_iff.mpr hfl
        rw [flips_contains] at hc
        have hc' : (flips (l ++ [x])).contains i = true := by
          rw [flips_contains]
          refine ⟨hc.1, Nat.lt_of_lt_of_le hc.2.1 (by simp), ?_⟩
          have h1 : (l ++ [x])[i - 1]? = l[i - 1]? := by
            rw [List.getElem?_append]
            simp [show i - 1 < l.length by omega]
          have h2 : (l ++ [x])[i]? = l[i]? := by
            rw [List.getElem?_append]
            simp [show i < l.length by omega]
          rw [h1, h2]
          exact hc.2.2
        exact List.elem_iff.mp hc'

private theorem flips_append_singleton_length (l : List Bool) (x : Bool) :
    (flips (l ++ [x])).length =
      (flips l).length + if l ≠ [] && l.getLast? != some x then 1 else 0 := by
  have hp := flips_append_singleton l x
  rw [hp.length_eq]
  by_cases hbcond : (decide (l ≠ []) && l.getLast? != some x) = true
  · rw [ite_eq_left hbcond]
    have hleft : l ≠ [] := by
      intro hl
      subst hl
      simp at hbcond
    have hright : (l.getLast? != some x) = true := by
      by_cases hgl : l.getLast? = some x
      · have hb : (decide (l ≠ []) && l.getLast? != some x) = false := by
          simp [hleft, hgl]
        rw [hb] at hbcond
        contradiction
      · simp [hgl]
    simp [hleft, hright]
  · rw [ite_eq_right hbcond]
    by_cases hleft : l = []
    · simp [hleft]
    · have hright : ¬ (l.getLast? != some x) = true := by
        intro hne
        have hb : (decide (l ≠ []) && l.getLast? != some x) = true := by
          simp [hleft, hne]
        exact hbcond hb
      have hgl : l.getLast? = some x := by
        by_cases h : l.getLast? = some x
        · exact h
        · exact absurd (by simp [h] : (l.getLast? != some x) = true) hright
      simp [hleft, hgl]

private theorem last_append_nonempty {α : Type} (a b : List α) (hb : b ≠ []) :
    (a ++ b).getLast? = b.getLast? := by
  rw [List.getLast?_append]
  cases hlast : b.getLast? with
  | none =>
      exfalso
      cases b with
      | nil => exact hb rfl
      | cons y ys =>
          cases ys with
          | nil => simp at hlast
          | cons z zs => simp [List.getLast?_cons] at hlast
  | some v => simp

private theorem head_append_singleton {α : Type} (b : List α) (x : α) :
    (b ++ [x]).head? = if b = [] then some x else b.head? := by
  cases b with
  | nil => simp
  | cons y ys => simp

private theorem flips_append_length_aux : ∀ n : Nat, ∀ (a b : List Bool), b.length = n →
    (flips (a ++ b)).length =
      (flips a).length + (flips b).length +
        if a ≠ [] && b ≠ [] && a.getLast? != b.head? then 1 else 0 := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro a b hblen
      cases b with
      | nil => simp [flips]
      | cons y bs =>
          cases hlast : (y :: bs).getLast? with
          | none => simp at hlast
          | some z =>
              obtain ⟨bs', hdecomp⟩ := List.getLast?_eq_some_iff.mp hlast
              have hbslen : bs'.length = bs.length := by
                have h := congrArg List.length hdecomp
                simp at h
                exact h.symm
              have hn : bs.length + 1 = n := by simpa using hblen
              have hlt : bs'.length < n := by omega
              have ih1 := ih bs'.length hlt a bs' rfl
              rw [hdecomp, ← List.append_assoc]
              rw [flips_append_singleton_length (a ++ bs') z, ih1,
                flips_append_singleton_length bs' z]
              by_cases hbsnil : bs' = []
              · subst bs'
                simp
              · have hlastEq : (a ++ bs').getLast? = bs'.getLast? :=
                  last_append_nonempty a bs' hbsnil
                have hhead : (bs' ++ [z]).head? = bs'.head? := by
                  simpa [hbsnil] using head_append_singleton bs' z
                simp [hbsnil, hlastEq, hhead]
                omega

theorem sound_ToggleBoundary : ToggleBoundary.Goal := by
  intro a b
  exact flips_append_length_aux b.length a b rfl

theorem refutation_RunsFromDistinct : ¬ RunsFromDistinct.Goal := by
  -- Counterexample: [true,false,false,false] changes value once (at position 1),
  -- while its permutation [false,true,false,false] changes twice (positions 1, 2).
  intro h
  have hc := h [true, false, false, false] [false, true, false, false] (by decide)
  revert hc
  native_decide

end TinylambOpt.ResearchProofs.Compression.Nullable

-- C++ references to this model or its proof.
-- table/tlcs_compression.hpp
-- table/tlcs_file.cpp
