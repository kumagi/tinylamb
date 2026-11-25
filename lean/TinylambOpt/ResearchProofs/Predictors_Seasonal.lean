import TinylambOpt.ResearchGoals.Compression.Predictors

namespace TinylambOpt.ResearchProofs.Compression.Predictors.Seasonal

open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.Seasonal
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0

-- Wire-level facts (local, since the goal modules only declare propositions).

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have hdiv : (2*n+1)/2 = n := by omega
      simp [zig, unzig, hdiv]
      omega

private theorem log2_base128 (q r : Nat) (hq : 0 < q) (hr : r < 128) :
    Nat.log2 (128*q + r) = 7 + Nat.log2 q := by
  have h2eight : 2^8 = 256 := by native_decide
  have h2seven : 2^7 = 128 := by native_decide
  apply Nat.le_antisymm
  · have hn : 128*q + r ≠ 0 := by omega
    have hqpow : q < 2^(Nat.log2 q + 1) := (Nat.log2_lt (by omega : q ≠ 0)).mp (by omega)
    have hlt : 128*q + r < 2^(8 + Nat.log2 q) := by
      rw [Nat.pow_add, h2eight]
      have hq1 : q + 1 ≤ 2^(Nat.log2 q + 1) := by omega
      have hint : 128*(q+1) ≤ 128 * 2^(Nat.log2 q+1) := by omega
      have h2 : 128 * 2^(Nat.log2 q+1) = 256 * 2^(Nat.log2 q) := by
        rw [show Nat.log2 q + 1 = 1 + Nat.log2 q by omega, Nat.pow_add]
        have h2one : 2^1 = 2 := by native_decide
        rw [h2one]
        omega
      rw [h2] at hint
      omega
    have hlog : Nat.log2 (128*q+r) < 8+Nat.log2 q := (Nat.log2_lt hn).mpr hlt
    omega
  · have hn : 128*q + r ≠ 0 := by omega
    apply (Nat.le_log2 hn).mpr
    have hqpow : 2^(Nat.log2 q) ≤ q := (Nat.le_log2 (by omega : q ≠ 0)).mp (by omega)
    rw [Nat.pow_add, h2seven]
    omega

private theorem div7_add14 (a : Nat) : (a + 14) / 7 = 1 + (a + 7) / 7 := by
  rw [show a + 14 = (a+7) + 1*7 by omega]
  rw [Nat.add_mul_div_right (a+7) 1 (by decide : 0 < 7)]
  omega

private theorem width_iff (n w : Nat) : width n ≤ w ↔ n < 2^w := by
  unfold width
  by_cases h : n == 0
  · have hn0 : n = 0 := by simpa using h
    subst n
    simp [Nat.pow_pos (a:=2) (n:=w) (by decide)]
  · have hnz : n ≠ 0 := by simpa using h
    simp [h]
    constructor
    · intro hh
      have hlog : n.log2 < w := by omega
      exact (Nat.log2_lt hnz).mp hlog
    · intro hh
      have hlog : n.log2 < w := (Nat.log2_lt hnz).mpr hh
      omega

private theorem ubytes_rec (n : Nat) : ubytes n = if n < 128 then 1 else 1 + ubytes (n/128) := by
  by_cases hn : n < 128
  · simp [hn]
    unfold ubytes
    have h127 : 2^7 = 128 := by native_decide
    have hwle : width n ≤ 7 := (width_iff n 7).mpr (by simpa [h127] using hn)
    exact Nat.max_eq_left (by omega)
  · simp [hn]
    unfold ubytes
    have hq : 0 < n/128 := Nat.div_pos (by omega : 128 ≤ n) (by decide)
    have hdecomp : 128 * (n/128) + n%128 = n := Nat.div_add_mod n 128
    have hlogn : Nat.log2 n = 7 + Nat.log2 (n/128) := by
      have htmp := log2_base128 (n/128) (n%128) hq (Nat.mod_lt n (by decide))
      rw [hdecomp] at htmp
      exact htmp
    have hnz : n ≠ 0 := by omega
    have hw : width n = 8 + Nat.log2 (n/128) := by
      unfold width
      have h0f : (n == 0) = false := by simp [hnz]
      rw [h0f]
      simp
      omega
    have hqne : n/128 ≠ 0 := by omega
    have hwq : width (n/128) = 1 + Nat.log2 (n/128) := by
      unfold width
      have h0f : ((n/128) == 0) = false := by simp [hqne]
      rw [h0f]
      simp
      omega
    have hbig : 1 ≤ (width n + 6)/7 := by omega
    have hbigq : 1 ≤ (width (n/128) + 6)/7 := by omega
    rw [Nat.max_eq_right hbig]
    rw [Nat.max_eq_right hbigq]
    rw [hw, hwq]
    rw [show 8 + Nat.log2 (n/128) + 6 = Nat.log2 (n/128) + 14 by omega]
    rw [show 1 + Nat.log2 (n/128) + 6 = Nat.log2 (n/128) + 7 by omega]
    exact div7_add14 (Nat.log2 (n/128))

private theorem ulength (n : Nat) : (uleb n).length = ubytes n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      rw [uleb, ubytes_rec]
      by_cases hn : n < 128
      · simp [hn]
      · simp [hn]
        have hlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        rw [ih (n/128) hlt]
        omega

private theorem packed_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length*w) := by
  simp [pack, little]

private theorem peak_lt_of_all_lt (xs : List Nat) (w : Nat) (h : ∀ x ∈ xs, x < 2^w) :
    peak xs < 2^w := by
  cases xs with
  | nil => simp [peak, Nat.pow_pos (a:=2) (n:=w) (by decide)]
  | cons x xs =>
      have hm : (x::xs).max? = some ((x::xs).max?.getD 0) := by
        cases hmax : (x::xs).max? with
        | none => simp [List.max?_cons] at hmax
        | some m =>
            simp
      have hmem : (x::xs).max?.getD 0 ∈ x::xs := (List.max?_eq_some_iff.mp hm).1
      dsimp [peak]
      rw [hm]
      exact h _ hmem

private theorem width_peak_le (xs : List Nat) (w : Nat) (h : ∀ x ∈ xs, x < 2^w) :
    width (peak xs) ≤ w :=
  (width_iff (peak xs) w).mpr (peak_lt_of_all_lt xs w h)

private theorem width_mono {n m : Nat} (h : n ≤ m) : width n ≤ width m := by
  have hm : m < 2 ^ width m := (width_iff m (width m)).mp (by omega)
  exact (width_iff n (width m)).mpr (by omega)

private theorem ubytes_mono {n m : Nat} (h : n ≤ m) : ubytes n ≤ ubytes m := by
  unfold ubytes
  have hw := width_mono h
  omega

private theorem byteCount_mono {n m : Nat} (h : n ≤ m) : byteCount n ≤ byteCount m := by
  unfold byteCount
  omega

private theorem frame_length (es : List Nat) :
    (frame es).length = 1 + ubytes es.length + ubytes (width (peak es)) +
      byteCount (es.length * width (peak es)) := by
  unfold frame
  simp [ulength, packed_length]
  omega

-- Encoder facts, valid for every predictor.

private theorem encode_length (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) :
    (encode p side i h xs).length = xs.length := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

private theorem encode_append (p : Predict) (side : Nat → Int) (i : Nat) (h xs ys : List Int) :
    encode p side i h (xs ++ ys) =
      encode p side i h xs ++ encode p side (i + xs.length) (xs.reverse ++ h) ys := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      change zig (x - p i h side) :: encode p side (i+1) (x::h) (xs++ys) =
        (zig (x - p i h side) :: encode p side (i+1) (x::h) xs) ++
          encode p side (i+(x::xs).length) ((x::xs).reverse++h) ys
      rw [ih (i+1) (x::h)]
      simp [List.reverse_cons]
      congr 1
      omega

private theorem encode_get (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) (j : Nat) :
    (encode p side i h xs)[j]?.getD 0 =
      if j < xs.length then zig ((xs[j]?.getD 0) - p (i+j) ((xs.take j).reverse ++ h) side) else 0 := by
  induction xs generalizing i h j with
  | nil => simp [encode]
  | cons x xs ih =>
      cases j with
      | zero => simp [encode]
      | succ j =>
          simp [encode, ih, List.reverse_cons, Nat.add_assoc]
          rw [show 1+j = j+1 by omega]

private theorem restore_encode (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) :
    restore p side i h (encode p side i h xs) = xs := by
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      dsimp [encode, restore]
      rw [unzig_zig]
      have hfirst : p i h side + (x - p i h side) = x := by omega
      rw [hfirst]
      rw [ih]

private theorem encode_zeros_of_eq (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int)
    (hc : ∀ j, j < xs.length → p (i+j) ((xs.take j).reverse ++ h) side = (xs[j]?.getD 0)) :
    encode p side i h xs = List.replicate xs.length 0 := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      have h0 : p i h side = x := by
        have := hc 0 (by simp)
        simpa using this
      have htail : ∀ j, j < xs.length →
          p ((i+1)+j) ((xs.take j).reverse ++ (x::h)) side = (xs[j]?.getD 0) := by
        intro j hj
        have := hc (j+1) (by simp [hj])
        have ht : (x::xs).take (j+1) = x :: xs.take j := by simp
        rw [ht, List.reverse_cons] at this
        rw [show (i+1)+j = i+(j+1) by omega]
        simpa [Nat.add_assoc, List.append_assoc] using this
      simp [encode, h0, zig]
      rw [ih (i+1) (x::h) htail]
      rw [List.replicate_succ]

-- Seasonal restart: agreement on the first period+1 history entries suffices.

private def Agree (period : Nat) (h1 h2 : List Int) : Prop := ∀ k, k ≤ period →
  h1[k]?.getD 0 = h2[k]?.getD 0

private theorem agree_of_take (period : Nat) (h : List Int) :
    Agree period (h.take (period+1)) h := by
  intro k hk
  rw [List.getElem?_take]
  simp [show k < period+1 by omega]

private theorem agree_cons {period : Nat} {h1 h2 : List Int} (ha : Agree period h1 h2) (x : Int) :
    Agree period (x::h1) (x::h2) := by
  intro k hk
  cases k with
  | zero => simp
  | succ k =>
      simp
      exact ha k (by omega)

private theorem encode_agree (period : Nat) (side : Nat → Int) (i : Nat) (h1 h2 : List Int)
    (ha : Agree period h1 h2) (xs : List Int) :
    encode (predict period) side i h1 xs = encode (predict period) side i h2 xs := by
  induction xs generalizing i h1 h2 with
  | nil => simp [encode]
  | cons x xs ih =>
      have hpred : predict period i h1 side = predict period i h2 side := by
        dsimp [predict, lag]
        exact ha period (by omega)
      simp [encode, hpred]
      rw [ih (i+1) (x::h1) (x::h2) (agree_cons ha x)]

-- Repeated dictionary pages.

private theorem seed_ok (block h : List Int) (_hb : block ≠ [])
    (hpref : ∃ c, h = block.reverse ++ c) :
    ∀ j, j < block.length →
      lag ((block.take j).reverse ++ h) (block.length - 1) = (block[j]?.getD 0) := by
  intro j hj
  dsimp [lag]
  rcases hpref with ⟨c, rfl⟩
  have hmin : min j block.length = j := by omega
  have hrevlen : (List.take j block).reverse.length = j := by simp [hmin]
  have hpge : ¬ block.length - 1 < j := by omega
  rw [List.getElem?_append]
  rw [hrevlen]
  rw [ite_eq_right hpge]
  rw [List.getElem?_append]
  have hidxlt : block.length - 1 - j < block.reverse.length := by simp; omega
  rw [ite_eq_left hidxlt]
  rw [List.getElem?_reverse (by omega)]
  have harith : block.length - 1 - (block.length - 1 - j) = j := by omega
  rw [harith]

private theorem encode_block_zeros (block h : List Int) (hb : block ≠ [])
    (hpref : ∃ c, h = block.reverse ++ c) (side : Nat → Int) (i : Nat) :
    encode (predict (block.length-1)) side i h block = List.replicate block.length 0 := by
  apply encode_zeros_of_eq
  intro j hj
  dsimp [predict]
  exact seed_ok block h hb hpref j hj

private theorem replicate_append {α : Type} (a : α) (m n : Nat) :
    List.replicate m a ++ List.replicate n a = List.replicate (m+n) a := by
  induction m with
  | zero => simp
  | succ m ih => simp [List.replicate_succ, ih, Nat.succ_add]

private theorem encode_repeat_blocks (block : List Int) (hb : block ≠ []) (n : Nat)
    (h : List Int) (hpref : ∃ c, h = block.reverse ++ c) (side : Nat → Int) (i : Nat) :
    encode (predict (block.length-1)) side i h ((List.replicate n block).flatten) =
      List.replicate (n*block.length) 0 := by
  induction n generalizing i h with
  | zero => simp [encode]
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      rw [encode_append]
      rw [encode_block_zeros block h hb hpref side i]
      have hpref' : ∃ c, block.reverse ++ h = block.reverse ++ c := ⟨h, rfl⟩
      rw [ih (block.reverse++h) hpref' (i+block.length)]
      rw [replicate_append (0 : Nat) block.length (n*block.length)]
      congr 1
      rw [show n+1 = Nat.succ n by omega, Nat.succ_mul]
      omega

-- The four goals.

theorem sound_ResidualRoundTrip :
    TinylambOpt.ResearchGoals.Compression.Predictors.Seasonal.ResidualRoundTrip.Goal := by
  intro period side i h xs
  exact restore_encode (predict period) side i h xs

theorem sound_ByteBudget :
    TinylambOpt.ResearchGoals.Compression.Predictors.Seasonal.ByteBudget.Goal := by
  intro period side i h xs w extra oldBytes hcert hlt
  let p : Predict := predict period
  let es : List Nat := encode p side i h xs
  have henclen : es.length = xs.length := by
    dsimp [es]
    exact encode_length p side i h xs
  have hall : ∀ e ∈ es, e < 2^w := by
    intro e he
    rcases List.mem_iff_getElem.mp he with ⟨j, hj, hval⟩
    have hjxs : j < xs.length := by
      rw [henclen] at hj
      exact hj
    have hget := encode_get p side i h xs j
    have hget' : es[j]?.getD 0 =
        zig ((xs[j]?.getD 0) - p (i+j) ((xs.take j).reverse ++ h) side) := by
      dsimp [es]
      rw [ite_eq_left hjxs] at hget
      exact hget
    have hgetval : es[j]?.getD 0 = e := by
      rw [List.getElem?_eq_getElem hj]
      simp [hval]
    have hcert' : ∀ j < xs.length,
        zig ((xs[j]?.getD 0) - p (i+j) ((xs.take j).reverse ++ h) side) < 2^w := by
      dsimp [certificate] at hcert
      exact hcert
    have hc := hcert' j hjxs
    omega
  have hwle : width (peak es) ≤ w := width_peak_le es w hall
  have hfr := frame_length es
  have hle : (frame es).length ≤ quote xs.length w := by
    rw [hfr]
    unfold quote
    rw [← henclen]
    have h1 : ubytes (width (peak es)) ≤ ubytes w := ubytes_mono hwle
    have h2 : byteCount (es.length * width (peak es)) ≤ byteCount (es.length * w) := by
      apply byteCount_mono
      exact Nat.mul_le_mul_left es.length hwle
    omega
  change extra + (frame es).length < oldBytes
  omega

theorem sound_RestartSeed :
    TinylambOpt.ResearchGoals.Compression.Predictors.Seasonal.RestartSeed.Goal := by
  intro period side i h xs
  exact encode_agree period side i (h.take (period+1)) h (agree_of_take period h) xs

theorem sound_RepeatCycle :
    TinylambOpt.ResearchGoals.Compression.Predictors.Seasonal.RepeatCycle.Goal := by
  intro block n side i hb
  exact encode_repeat_blocks block hb n block.reverse ⟨[], by simp⟩ side i

end TinylambOpt.ResearchProofs.Compression.Predictors.Seasonal
