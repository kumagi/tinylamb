import TinylambOpt.ResearchGoals.Compression.Directories
/-!
Proofs and disproofs for the 16 `Directories` research goals.

Fifteen goals are TRUE: the high/low streams reconstruct every end offset
(including repeated ends for empty strings), the physical byte charge and
break-even formulae hold, sparse select anchors restart at the right bit,
and the concatenation lemmas hold.  `AverageWidthOptimal` is FALSE: for
`sizes = [1]` the mean-based width 1 costs 2 bytes while width 0 costs 1 byte
(witness verified by `native_decide`).
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Directories
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Directories

private theorem word_div_mod (w : Nat) : ∀ (xs : List Nat) i,
    (word w xs / 2^(i*w)) % 2^w = xs[i]?.getD 0 % 2^w := by
  intro xs
  induction xs with
  | nil => intro i; simp [word]
  | cons x xs ih =>
      intro i
      simp only [word]
      cases i with
      | zero => simp
      | succ j =>
          have hr : x % 2^w < 2^w := Nat.mod_lt _ (Nat.two_pow_pos w)
          have hdiv : (x % 2^w + 2^w * word w xs) / 2^w = word w xs := by
            rw [Nat.add_mul_div_left (x % 2^w) (word w xs) (Nat.two_pow_pos w)]
            rw [Nat.div_eq_of_lt hr]
            omega
          rw [Nat.succ_mul, Nat.pow_add]
          rw [Nat.mul_comm (2^(j*w)) (2^w)]
          rw [← Nat.div_div_eq_div_mul, hdiv]
          simpa using ih j

private theorem fromLittle_append_singleton (l : List Nat) (b : Nat) :
    fromLittle (l ++ [b]) = fromLittle l + 256^l.length * b := by
  induction l with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256^xs.length) 256, Nat.mul_assoc]
      omega

private theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
    v % (m*256) = v % m + m * ((v/m) % 256) := by
  let r := v % m
  let q := v / m
  let s := q % 256
  let t := q / 256
  have hr : r < m := by simpa [r] using Nat.mod_lt v hm
  have hs : s < 256 := by simpa [s] using Nat.mod_lt q (by decide : 0 < 256)
  have hq : q = s + 256*t := by
    dsimp [s, t]
    exact (Nat.mod_add_div q 256).symm
  have hv : v = r + m*q := by
    dsimp [r, q]
    exact (Nat.mod_add_div v m).symm
  have hs_le : s ≤ 255 := Nat.lt_succ_iff.mp (by simpa using hs)
  have hmul_le : m*s ≤ m*255 := Nat.mul_le_mul_left m hs_le
  have hsum_lt : r + m*s < m*256 := by omega
  have hrewrite : v = (r + m*s) + (m*256)*t := by
    rw [hv, hq]
    rw [Nat.mul_add, Nat.mul_assoc]
    omega
  have hleft : v % (m*256) = r + m*s := by
    calc
      v % (m*256) = ((r+m*s) + (m*256)*t) % (m*256) := by rw [hrewrite]
      _ = (r+m*s) % (m*256) := by rw [Nat.add_mul_mod_self_left]
      _ = r+m*s := Nat.mod_eq_of_lt hsum_lt
  have hright : v % m + m * ((v/m)%256) = r + m*s := by rfl
  exact hleft.trans hright.symm

private theorem fromLittle_little (v k : Nat) :
    fromLittle ((List.range k).map (fun j => (v/256^j)%256)) = v % 256^k := by
  induction k with
  | zero => simp [fromLittle, Nat.mod_one]
  | succ k ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [fromLittle_append_singleton]
      simp only [List.length_map, List.length_range]
      rw [ih, Nat.pow_succ]
      exact (mod_mul_256 v (256^k) (Nat.pow_pos (by decide : 0 < 256))).symm

private theorem mod_pow_div_mod (n B w A : Nat) (hA : B + w ≤ A) :
    ((n % 2^A) / 2^B) % 2^w = (n / 2^B) % 2^w := by
  have hBA : B ≤ A := by omega
  let q := n / 2^A
  let r := n % 2^A
  have hn : n = q * 2^A + r := by
    have h : 2^A * q + r = n := by
      simpa [q, r] using Nat.div_add_mod n (2^A)
    rw [← h, Nat.mul_comm]
  have hpowA : 2^A = 2^B * 2^(A-B) := by
    rw [← Nat.pow_add, Nat.add_sub_of_le hBA]
  have hdivn : n / 2^B = r / 2^B + q * 2^(A-B) := by
    rw [hn, hpowA]
    rw [← Nat.mul_assoc q (2^B) (2^(A-B)), Nat.mul_comm q (2^B), Nat.mul_assoc]
    rw [Nat.add_comm (2^B * (q * 2^(A-B))) r]
    rw [Nat.add_mul_div_left r (q * 2^(A-B)) (Nat.two_pow_pos B)]
  have hdivr : (n % 2^A) / 2^B = r / 2^B := by rfl
  have hwle : w ≤ A - B := by omega
  have hpowdiff : 2^(A-B) = 2^w * 2^(A-B-w) := by
    rw [← Nat.pow_add, Nat.add_sub_of_le hwle]
  rw [hdivr, hdivn, hpowdiff]
  rw [← Nat.mul_assoc q (2^w) (2^(A-B-w)), Nat.mul_comm q (2^w), Nat.mul_assoc]
  rw [Nat.add_mul_mod_self_left]

private theorem get_pack_bits (bits : List Nat) (hbits : ∀ x ∈ bits, x < 2) (j : Nat)
    (hj : j < 8*(pack 1 bits).length) :
    get (pack 1 bits) 1 j = bits[j]?.getD 0 := by
  have hlen : (pack 1 bits).length = byteCount (bits.length*1) := by
    simp [pack, little]
  have hK : byteCount (bits.length*1) = byteCount bits.length := by simp
  have hA : j + 1 ≤ 8 * byteCount bits.length := by omega
  unfold TinylambOpt.ResearchGoals.Compression.Wire.get TinylambOpt.ResearchGoals.Compression.Wire.pack TinylambOpt.ResearchGoals.Compression.Wire.little
  rw [fromLittle_little, hK]
  have h256 : (256:Nat) ^ byteCount bits.length = 2 ^ (8 * byteCount bits.length) := by
    rw [show (256:Nat) = 2^8 by decide, ← Nat.pow_mul]
  rw [h256]
  simp only [Nat.mul_one, Nat.pow_one]
  rw [mod_pow_div_mod (word 1 bits) j 1 (8*byteCount bits.length) hA]
  have hw := word_div_mod 1 bits j
  simp only [Nat.mul_one, Nat.pow_one] at hw
  rw [hw]
  have hmod : bits[j]?.getD 0 % 2 = bits[j]?.getD 0 := by
    by_cases hjlen : j < bits.length
    · rw [List.getElem?_eq_getElem hjlen]
      simp only [Option.getD_some]
      exact Nat.mod_eq_of_lt (hbits _ (List.getElem_mem hjlen))
    · rw [List.getElem?_eq_none (by omega)]
      simp
  rw [hmod]

private theorem foldl_add (a : Nat) (l : List Nat) : List.foldl (·+·) a l = a + l.sum := by
  calc
    List.foldl (·+·) a l = List.foldl (·+·) (a+0) l := by simp
    _ = a + List.foldl (·+·) 0 l := List.foldl_assoc
    _ = a + l.sum := by rw [← List.sum_eq_foldl]

private theorem ends_get? (s : List Nat) (i : Nat) :
    (ends s)[i]? = if i < s.length then some ((s.take (i+1)).sum) else none := by
  unfold ends
  rw [List.getElem?_drop, List.getElem?_scanl]
  by_cases hi : i < s.length
  · have hcond : 1+i ≤ s.length := by omega
    rw [ite_eq_left hcond, ite_eq_left hi]
    simp only [Option.some.injEq]
    rw [show 1+i = i+1 by omega, foldl_add]
    simp
  · have hcond : ¬ 1+i ≤ s.length := by omega
    rw [ite_eq_right hcond, ite_eq_right hi]

private theorem ends_length (s : List Nat) : (ends s).length = s.length := by
  simp [ends]

private theorem take_sum_mono (s : List Nat) : ∀ i j, i ≤ j → (s.take i).sum ≤ (s.take j).sum := by
  intro i j hij
  induction j with
  | zero =>
      have hi0 : i = 0 := by omega
      subst i
      exact Nat.le_refl _
  | succ j ih =>
      by_cases h : i ≤ j
      · have hih := ih h
        rw [List.take_add_one, List.sum_append]
        have hnonneg : 0 ≤ (s[j]?.toList.sum) := Nat.zero_le _
        omega
      · have : i = j+1 := by omega
        subst i
        exact Nat.le_refl _

private theorem ends_pairwise_le (s : List Nat) : (ends s).Pairwise (·≤·) := by
  rw [List.pairwise_iff_getElem]
  intro i j hi hj hij
  have hi' : i < s.length := by simpa [ends_length] using hi
  have hj' : j < s.length := by simpa [ends_length] using hj
  have h1 := ends_get? s i
  have h2 := ends_get? s j
  rw [ite_eq_left hi'] at h1
  rw [ite_eq_left hj'] at h2
  rw [List.getElem?_eq_getElem hi] at h1
  rw [List.getElem?_eq_getElem hj] at h2
  have hv1 : (ends s)[i] = (s.take (i+1)).sum := by simpa using h1
  have hv2 : (ends s)[j] = (s.take (j+1)).sum := by simpa using h2
  rw [hv1, hv2]
  exact take_sum_mono s (i+1) (j+1) (by omega)

private theorem le_peak (xs : List Nat) (x : Nat) (hx : x ∈ xs) : x ≤ peak xs := by
  unfold peak
  cases hm : xs.max? with
  | none =>
      have : xs = [] := by
        cases xs with
        | nil => rfl
        | cons a as => simp at hm
      simp [this] at hx
  | some m =>
      have hmem_m : m ∈ xs := (List.max?_eq_some_iff.mp hm).1
      have hle : ∀ b ∈ xs, b ≤ m := (List.max?_eq_some_iff.mp hm).2
      simpa [hm] using hle x hx

private theorem peak_ends (s : List Nat) : peak (ends s) = s.sum := by
  unfold peak
  by_cases hs : s = []
  · simp [hs, ends]
  · have hlen : 0 < s.length := by
      cases s with
      | nil => simp at hs
      | cons a as => simp
    have hi : s.length - 1 < s.length := Nat.sub_lt hlen (by decide)
    have hmem : s.sum ∈ ends s := by
      rw [List.mem_iff_getElem?]
      refine ⟨s.length-1, ?_⟩
      rw [ends_get?]
      rw [ite_eq_left hi]
      have ht : s.take (s.length-1+1) = s := by
        rw [Nat.sub_add_cancel (Nat.succ_le_of_lt hlen)]
        simp
      rw [ht]
    have hle : ∀ b ∈ ends s, b ≤ s.sum := by
      intro b hb
      rw [List.mem_iff_getElem?] at hb
      obtain ⟨i, hi'⟩ := hb
      rw [ends_get?] at hi'
      have hlt : i < s.length := by
        by_cases hh : i < s.length
        · exact hh
        · rw [ite_eq_right hh] at hi'
          simp at hi'
      rw [ite_eq_left hlt] at hi'
      simp only [Option.some.injEq] at hi'
      have hsum := take_sum_mono s (i+1) s.length (by omega)
      have htake : s.take s.length = s := by simp
      rw [htake] at hsum
      omega
    have hmax : (ends s).max? = some s.sum := (List.max?_eq_some_iff).2 ⟨hmem, hle⟩
    rw [hmax]
    rfl

private theorem positions_get? (w : Nat) (xs : List Nat) (i : Nat) :
    (positions w xs)[i]? = xs[i]?.map (fun x => x/2^w + i) := by
  unfold positions
  rw [List.getElem?_map, List.getElem?_zipIdx]
  simp [Function.comp_def]

private theorem positions_pairwise_of_mono (w : Nat) (xs : List Nat)
    (hxs : xs.Pairwise (·≤·)) : (positions w xs).Pairwise (·<·) := by
  rw [List.pairwise_iff_getElem] at hxs ⊢
  intro i j hi hj hij
  have hi' : i < xs.length := by simpa [positions] using hi
  have hj' : j < xs.length := by simpa [positions] using hj
  have hxle := hxs i j hi' hj' hij
  have hdivle : xs[i]/2^w ≤ xs[j]/2^w := Nat.div_le_div_right hxle
  simpa [positions, List.getElem_map, List.getElem_zipIdx] using
    (Nat.add_lt_add_of_le_of_lt hdivle hij)

example : (positions 2 (ends [0,7,0])).Pairwise (·<·) := by
  apply positions_pairwise_of_mono
  exact ends_pairwise_le _

example : peak (ends [3,4,0]) = 7 := by
  exact peak_ends _

private theorem range'_split (start p end_ : Nat) (hlo : start ≤ p) (hhi : p < end_) :
    List.range' start (end_ - start) =
      List.range' start (p-start) ++ [p] ++ List.range' (p+1) (end_-(p+1)) := by
  have hlen : (p-start) + 1 + (end_-(p+1)) = end_ - start := by omega
  rw [← hlen]
  rw [← List.range'_append (step := 1)]
  rw [← List.range'_append (step := 1)]
  have hsp : start + (p - start + 1) = p + 1 := by
    have h := Nat.add_sub_of_le hlo
    omega
  simpa [hsp] using (Nat.add_sub_of_le hlo)

private theorem filter_eq_nil_of_all_false {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ a ∈ l, p a = false) : l.filter p = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.filter_cons]
      rw [h a (by simp)]
      rw [ih]
      · rfl
      · intro b hb; exact h b (by simp [hb])

private theorem filter_range'_sorted (ps : List Nat) (hs : ps.Pairwise (·<·))
    (start end_ : Nat) (hlo : ∀ p ∈ ps, start ≤ p) (hhi : ∀ p ∈ ps, p < end_) :
    (List.range' start (end_ - start)).filter (fun j => ps.contains j) = ps := by
  induction ps generalizing start with
  | nil => simp
  | cons p rest ih =>
      have hhead : ∀ q ∈ rest, p < q := (List.pairwise_cons.mp hs).1
      have hrest : rest.Pairwise (·<·) := (List.pairwise_cons.mp hs).2
      have hpl : start ≤ p := hlo p (by simp)
      have hph : p < end_ := hhi p (by simp)
      rw [range'_split start p end_ hpl hph]
      simp only [List.filter_append, List.filter_cons]
      have hbefore : (List.range' start (p-start)).filter (fun j => (p::rest).contains j) = [] := by
        apply filter_eq_nil_of_all_false
        intro j hj
        have hjlt : j < p := by
          have hmem := List.mem_range'.mp hj
          omega
        have hp_ne : (p == j) = false := by
          have : p ≠ j := by omega
          simpa [Bool.eq_false_iff] using this
        have hjrest : j ∉ rest := by
          intro hjmem
          have := hhead j hjmem
          omega
        have hjp : j ≠ p := by omega
        simp [hjp, hjrest]
      rw [hbefore]
      simp only [List.nil_append]
      have hptrue : ((p :: rest).contains p) = true := by simp
      rw [ite_eq_left hptrue]
      have htail_agree : (List.range' (p+1) (end_-(p+1))).filter (fun j => (p::rest).contains j) =
          (List.range' (p+1) (end_-(p+1))).filter (fun j => rest.contains j) := by
        apply List.filter_congr
        intro j hj
        have hjne : j ≠ p := by
          have hmem := List.mem_range'.mp hj
          omega
        simp [hjne]
      rw [htail_agree]
      have hrestlo : ∀ q ∈ rest, p+1 ≤ q := by
        intro q hq
        have := hhead q hq
        omega
      have hresthi : ∀ q ∈ rest, q < end_ := by
        intro q hq; exact hhi q (by simp [hq])
      rw [ih hrest (p+1) hrestlo hresthi]
      simp

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length*w) := by
  simp [pack, little]

private theorem scanl_zeros (n : Nat) :
    List.scanl (·+·) 0 (List.replicate n 0) = List.replicate (n+1) 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.scanl_cons]
      simp only [Nat.zero_add]
      rw [ih]
      have hrep : List.replicate (n+1+1) 0 = 0 :: 0 :: List.replicate n 0 := by
        rw [show n+1+1 = (n+2) by omega]
        rw [show n+2 = (n+1)+1 by omega, List.replicate_succ, List.replicate_succ]
      rw [hrep]
      rw [List.replicate_succ]

private theorem ends_replicate_zero (n : Nat) : ends (List.replicate n 0) = List.replicate n 0 := by
  unfold ends
  rw [scanl_zeros]
  simp

private theorem peak_replicate_zero (n : Nat) : peak (List.replicate n 0) = 0 := by
  cases n <;> simp [peak]

private theorem extents_scanl_aux (sizes : List Nat) (acc : Nat) :
    ((sizes.scanl (· + ·) acc).drop 1).zipWith (· - ·) (acc :: (sizes.scanl (· + ·) acc).drop 1) = sizes := by
  induction sizes generalizing acc with
  | nil => simp
  | cons x xs ih =>
      cases xs with
      | nil =>
          simp only [List.scanl_cons, List.drop_succ_cons, List.drop_zero]
          simp
      | cons y ys =>
          simp only [List.scanl_cons, List.drop_succ_cons, List.drop_zero]
          simpa [Nat.add_sub_cancel_left] using ih (acc + x)

private theorem byteCount_cover_aux (m : Nat) : m ≤ 8 * byteCount m := by
  unfold byteCount
  have h := Nat.div_add_mod (m+7) 8
  have hm : (m+7)%8 < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  omega

private theorem fromLittle_append_d (a b : Bytes) :
    fromLittle (a ++ b) = fromLittle a + 256 ^ a.length * fromLittle b := by
  induction a with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle, List.length_cons]
      rw [ih]
      rw [Nat.pow_succ]
      rw [Nat.mul_add]
      rw [show 256 * (256 ^ xs.length * fromLittle b) = 256 ^ xs.length * 256 * fromLittle b by ac_rfl]
      omega

private theorem fromLittle_lt_d (bs : Bytes) (h : ∀ b ∈ bs, b < 256) :
    fromLittle bs < 256 ^ bs.length := by
  induction bs with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [fromLittle, List.length_cons]
      rw [Nat.pow_succ]
      have hx : x < 256 := h x (by simp)
      have hi := ih (by intro b hb; exact h b (by simp [hb]))
      have hs := Nat.mul_le_mul_left 256 (Nat.succ_le_of_lt hi)
      rw [show 256 ^ xs.length * 256 = 256 * 256 ^ xs.length by ac_rfl]
      omega

private theorem fromLittle_take_mod_d (bs : Bytes) (m : Nat) (hbs : ∀ b ∈ bs, b < 256) :
    fromLittle (bs.take m) = fromLittle bs % 256 ^ m := by
  induction bs generalizing m with
  | nil => cases m <;> simp [fromLittle, Nat.zero_mod]
  | cons b bs ih =>
      cases m with
      | zero => simp [fromLittle]; rw [Nat.mod_one]
      | succ m =>
          simp only [List.take_succ_cons, fromLittle]
          rw [ih m (by intro x hx; exact hbs x (by simp [hx]))]
          let M := 256 ^ m
          have hM : 0 < M := Nat.pow_pos (by decide)
          have hdecomp : 256 * fromLittle bs =
              256 * (fromLittle bs % M) + 256 ^ (m+1) * (fromLittle bs / M) := by
            conv => lhs; rw [show fromLittle bs = fromLittle bs % M + M * (fromLittle bs / M) from (Nat.mod_add_div (fromLittle bs) M).symm]
            rw [Nat.mul_add, Nat.pow_succ]
            dsimp [M]
            congr 1
            ac_rfl
          have hbound : b + 256 * (fromLittle bs % M) < 256 ^ (m+1) := by
            have hb : b < 256 := hbs b (by simp)
            have hlt : fromLittle bs % M < M := Nat.mod_lt _ hM
            have hs := Nat.mul_le_mul_left 256 (Nat.succ_le_of_lt hlt)
            rw [Nat.pow_succ]
            dsimp [M] at *
            omega
          rw [hdecomp, ← Nat.add_assoc, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hbound]

private theorem fromLittle_drop_div_all (bs : Bytes) (s : Nat) (hbs : ∀ b ∈ bs, b < 256) :
    fromLittle (bs.drop s) = fromLittle bs / 256 ^ s := by
  by_cases hsl : s ≤ bs.length
  · have happ := fromLittle_append_d (bs.take s) (bs.drop s)
    rw [List.take_append_drop] at happ
    have hlen : (bs.take s).length = s := by
      rw [List.length_take, Nat.min_eq_left hsl]
    rw [hlen] at happ
    have hlt : fromLittle (bs.take s) < 256 ^ s := by
      simpa [hlen] using fromLittle_lt_d (bs.take s) (by intro b hb; exact hbs b (List.mem_of_mem_take hb))
    rw [happ]
    rw [Nat.add_mul_div_left _ _ (Nat.pow_pos (by decide : 0 < 256))]
    rw [Nat.div_eq_of_lt hlt]
    omega
  · have hgt : bs.length < s := by omega
    have hdrop : bs.drop s = [] := List.drop_eq_nil_of_le (by omega)
    have hlt := fromLittle_lt_d bs hbs
    have hpowle : 256^bs.length ≤ 256^s := Nat.pow_le_pow_right (by decide : 0 < 256) (by omega)
    have hflt : fromLittle bs < 256^s := Nat.lt_of_lt_of_le hlt hpowle
    rw [hdrop]
    simp [fromLittle, Nat.div_eq_of_lt hflt]

private theorem word_bound (w : Nat) : ∀ xs, (∀ x ∈ xs, x < 2^w) → word w xs < 2^(w*xs.length)
  | [], _ => by simp [word, Nat.two_pow_pos]
  | x::xs, h => by
      simp only [word, List.length_cons, Nat.mul_succ]
      have hxmod : x % 2^w < 2^w := Nat.mod_lt _ (Nat.two_pow_pos w)
      have hi := word_bound w xs (by intro y hy; exact h y (by simp [hy]))
      have hs := Nat.mul_le_mul_left (2^w) (Nat.succ_le_of_lt hi)
      rw [Nat.pow_add]
      rw [show 2 ^ (w * xs.length) * 2^w = 2^w * 2 ^ (w * xs.length) by ac_rfl]
      simp only [Nat.succ_eq_add_one, Nat.mul_add, Nat.mul_one] at hs
      omega

private theorem word_map_mod (w : Nat) : ∀ xs : List Nat,
    word w (xs.map (fun x => x % 2^w)) = word w xs
  | [] => rfl
  | x::xs => by simp [word, word_map_mod w xs]

private theorem fromLittle_pack (w : Nat) (xs : List Nat) : fromLittle (pack w xs) = word w xs := by
  unfold pack little
  rw [fromLittle_little]
  have hb : word w xs < 2 ^ (8*byteCount (xs.length*w)) := by
    have hw := word_bound w (xs.map (fun x => x % 2^w)) (by
      intro y hy
      rw [List.mem_map] at hy
      obtain ⟨x, hx, rfl⟩ := hy
      exact Nat.mod_lt _ (Nat.two_pow_pos w))
    rw [← word_map_mod w xs]
    have hle : w * (xs.map (fun x => x % 2^w)).length ≤ 8*byteCount (xs.length*w) := by
      rw [List.length_map, Nat.mul_comm w xs.length]
      exact byteCount_cover_aux _
    exact Nat.lt_of_lt_of_le hw (Nat.pow_le_pow_right (by decide : 0 < 2) hle)
  have h256 : (256:Nat) ^ byteCount (xs.length*w) = 2^(8*byteCount (xs.length*w)) := by
    rw [show (256:Nat)=2^8 by decide, ← Nat.pow_mul]
  rw [h256, Nat.mod_eq_of_lt hb]

private theorem takeAt_pack_low (w : Nat) (xs : List Nat) (i : Nat) :
    takeAt (pack w xs) w i = xs[i]?.getD 0 % 2^w := by
  by_cases hw : w = 0
  · subst w
    simp [TinylambOpt.ResearchGoals.Compression.Wire.takeAt, Nat.mod_one]
  · let start := i*w/8
    let shift := i*w%8
    let m := byteCount (shift+w)
    have hcan : ∀ b ∈ (pack w xs), b < 256 := by
      unfold pack little
      intro b hb
      simp at hb
      obtain ⟨j, hj, rfl⟩ := hb
      exact Nat.mod_lt _ (by decide : 0 < 256)
    have hfl : fromLittle (((pack w xs).drop start).take m) =
        (fromLittle (pack w xs) / 256^start) % 256^m := by
      rw [fromLittle_take_mod_d _ _ (by intro b hb; exact hcan b (List.mem_of_mem_drop hb))]
      rw [fromLittle_drop_div_all _ _ hcan]
    unfold TinylambOpt.ResearchGoals.Compression.Wire.takeAt
    dsimp only
    change (fromLittle (((pack w xs).drop start).take m) / 2^shift) % 2^w = xs[i]?.getD 0 % 2^w
    rw [hfl]
    have h256m : (256:Nat)^m = 2^(8*m) := by
      rw [show (256:Nat)=2^8 by decide, ← Nat.pow_mul]
    rw [h256m]
    have hwin : shift + w ≤ 8*m := byteCount_cover_aux _
    rw [mod_pow_div_mod (fromLittle (pack w xs) / 256^start) shift w (8*m) hwin]
    have h256s : (256:Nat)^start = 2^(8*start) := by
      rw [show (256:Nat)=2^8 by decide, ← Nat.pow_mul]
    rw [h256s, fromLittle_pack]
    rw [Nat.div_div_eq_div_mul]
    rw [show 2^(8*start) * 2^shift = 2^(8*start+shift) by rw [← Nat.pow_add]]
    have hbits : 8*start + shift = i*w := by
      dsimp [start, shift]
      rw [Nat.div_add_mod]
    rw [hbits]
    exact word_div_mod w xs i

private theorem byteCount_cover (m : Nat) : m ≤ 8 * byteCount m := by
  unfold byteCount
  have h := Nat.div_add_mod (m+7) 8
  have hm : (m+7)%8 < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  omega

private theorem range'_zero (n : Nat) : List.range' 0 n = List.range n := by
  apply List.ext_getElem?
  intro i
  by_cases hi : i < n
  · rw [List.getElem?_range' hi, List.getElem?_range hi]
    simp
  · rw [List.getElem?_eq_none (by simpa using hi), List.getElem?_eq_none (by simpa using hi)]

private theorem positions_bound (w : Nat) (xs : List Nat) :
    ∀ p ∈ positions w xs, p < highBits w xs := by
  intro p hp
  rw [List.mem_iff_getElem?] at hp
  obtain ⟨i, hi⟩ := hp
  have hposlen : (positions w xs).length = xs.length := by simp [positions]
  have hxi : i < xs.length := by
    by_cases hh : i < xs.length
    · exact hh
    · have hge : xs.length ≤ i := by omega
      have hge' : (positions w xs).length ≤ i := by simpa [hposlen] using hge
      rw [List.getElem?_eq_none hge'] at hi
      simp at hi
  have hipos : i < (positions w xs).length := by simpa [hposlen] using hxi
  have hv : (positions w xs)[i] = xs[i]/2^w + i := by
    simp [positions, List.getElem_map, List.getElem_zipIdx]
  rw [List.getElem?_eq_getElem hipos] at hi
  have hpval : p = xs[i]/2^w + i := by simpa [hv] using hi.symm
  have hpk : xs[i] ≤ peak xs := le_peak xs _ (List.getElem_mem (l:=xs) hxi)
  have hd : xs[i]/2^w ≤ peak xs/2^w := Nat.div_le_div_right hpk
  unfold highBits
  omega

private theorem get_high_eq (w : Nat) (xs : List Nat) (j : Nat)
    (hj : j < 8*(high w xs).length) :
    get (high w xs) 1 j = (if (positions w xs).contains j then 1 else 0) := by
  unfold high
  have hb : ∀ x ∈ ((List.range (highBits w xs)).map
      (fun j => if (positions w xs).contains j then 1 else 0)), x < 2 := by
    intro x hx
    rw [List.mem_map] at hx
    obtain ⟨j, hjrange, rfl⟩ := hx
    split <;> omega
  have hg := get_pack_bits ((List.range (highBits w xs)).map
      (fun j => if (positions w xs).contains j then 1 else 0)) hb j hj
  rw [hg]
  by_cases hjh : j < highBits w xs
  · have hget : (((List.range (highBits w xs)).map
        (fun j => if (positions w xs).contains j then 1 else 0))[j]?) =
        some (if (positions w xs).contains j then 1 else 0) := by
      rw [List.getElem?_map, List.getElem?_range hjh]
      rfl
    rw [hget]
    simp
  · have hlenbits : (((List.range (highBits w xs)).map
        (fun j => if (positions w xs).contains j then 1 else 0)).length ≤ j) := by
      simp
      omega
    rw [List.getElem?_eq_none hlenbits]
    simp only [Option.getD_none]
    have hpfalse : (positions w xs).contains j = false := by
      rw [Bool.eq_false_iff]
      intro ht
      have hmem : j ∈ positions w xs := List.contains_iff_mem.mp ht
      have := positions_bound w xs j hmem
      omega
    rw [hpfalse]
    simp

private theorem ones_high_eq (w : Nat) (sizes : List Nat) :
    ones (high w (ends sizes)) = positions w (ends sizes) := by
  let xs := ends sizes
  let ps := positions w xs
  have hps : ps.Pairwise (·<·) := by
    dsimp [ps, xs]
    exact positions_pairwise_of_mono w (ends sizes) (ends_pairwise_le sizes)
  have hb : ∀ p ∈ ps, p < highBits w xs := by
    dsimp [ps, xs]
    exact positions_bound w (ends sizes)
  have hlen : (high w xs).length = byteCount (highBits w xs) := by
    unfold high
    rw [pack_length]
    simp
  unfold ones
  change (List.range (8*(high w xs).length)).filter
      (fun j => get (high w xs) 1 j == 1) = ps
  rw [hlen]
  have hcover : highBits w xs ≤ 8*byteCount (highBits w xs) := byteCount_cover _
  have hfilt : (List.range (8*byteCount (highBits w xs))).filter
      (fun j => get (high w xs) 1 j == 1) =
      (List.range (8*byteCount (highBits w xs))).filter (fun j => ps.contains j) := by
    apply List.filter_congr
    intro j hjmem
    have hj : j < 8*byteCount (highBits w xs) := by simpa using (List.mem_range.mp hjmem)
    have hjlen : j < 8*(high w xs).length := by simpa [hlen] using hj
    rw [get_high_eq w xs j hjlen]
    have hbool : (((if (positions w xs).contains j then 1 else 0) == 1) = ps.contains j) := by
      by_cases hc' : j ∈ positions w xs
      · have hc'' : j ∈ ps := by simpa [ps] using hc'
        simp [hc', hc'']
      · have hc'' : j ∉ ps := by simpa [ps] using hc'
        simp [hc', hc'']
    exact hbool
  rw [hfilt]
  rw [← range'_zero]
  simpa using filter_range'_sorted ps hps 0 (8*byteCount (highBits w xs))
      (by intro p hp; omega)
      (by intro p hp; exact Nat.lt_of_lt_of_le (hb p hp) hcover)

private theorem flatten_slice (rows : List (List Nat)) : ∀ i, i < rows.length →
    (rows.flatten.drop ((rows.take i).map List.length).sum).take ((rows[i]?.getD []).length) = rows[i]?.getD [] := by
  induction rows with
  | nil => intro i hi; simp at hi
  | cons r rs ih =>
      intro i hi
      cases i with
      | zero => simp
      | succ j =>
          have hj : j < rs.length := by simpa using hi
          have hih := ih j hj
          simp only [List.take_succ_cons, List.map_cons, List.sum_cons]
          change (List.drop (r.length + ((rs.take j).map List.length).sum) (r ++ rs.flatten)).take ((rs[j]?.getD []).length) = rs[j]?.getD []
          rw [List.drop_append]
          rw [List.drop_eq_nil_of_le (by omega)]
          simp
          simpa [List.map_take] using hih

private theorem add_div_of_dvd_left (d a y : Nat) (hd : 0 < d) (hdiv : d ∣ a) :
    (y + a) / d = y / d + a / d := by
  obtain ⟨k, hk⟩ := hdiv
  subst a
  rw [Nat.add_mul_div_left y k hd]
  rw [Nat.mul_div_right k hd]

private theorem ends_append_prefix (a b : List Nat) :
    (ends (a++b)).take a.length = ends a := by
  apply List.ext_getElem?
  intro j
  rw [List.getElem?_take]
  by_cases hj : j < a.length
  · rw [ite_eq_left hj]
    have h1 := ends_get? (a++b) j
    have h2 := ends_get? a j
    have hj1 : j < (a++b).length := by
      simpa [List.length_append] using (Nat.lt_of_lt_of_le hj (Nat.le_add_right a.length b.length))
    rw [ite_eq_left hj1] at h1
    rw [ite_eq_left hj] at h2
    have ht : (a++b).take (j+1) = a.take (j+1) := by
      rw [List.take_append]
      have : j+1 - a.length = 0 := by omega
      rw [this]
      simp
    rw [ht] at h1
    rw [h1, h2]
  · rw [ite_eq_right hj]
    have hn : (ends a)[j]? = none :=
      List.getElem?_eq_none (by simpa [ends_length] using (show a.length ≤ j by omega))
    rw [hn]

private theorem ends_append_tail (a b : List Nat) :
    (ends (a++b)).drop a.length = (ends b).map (fun y => y + a.sum) := by
  apply List.ext_getElem?
  intro j
  rw [List.getElem?_drop, List.getElem?_map]
  by_cases hj : j < b.length
  · have hi : a.length + j < (a++b).length := by simp; omega
    have h1 := ends_get? (a++b) (a.length+j)
    have h2 := ends_get? b j
    rw [ite_eq_left hi] at h1
    rw [ite_eq_left hj] at h2
    have ht : (a++b).take (a.length+j+1) = a ++ b.take (j+1) := by
      rw [List.take_append]
      rw [List.take_of_length_le (by omega)]
      have : a.length+j+1 - a.length = j+1 := by omega
      rw [this]
    rw [ht] at h1
    rw [h2]
    simp only [Option.map_some, Option.some.injEq]
    rw [h1, List.sum_append]
    simp only [Option.some.injEq]
    omega
  · have hn1 : (ends (a++b))[a.length+j]? = none := by
      apply List.getElem?_eq_none
      simp [ends_length]
      omega
    have hn2 : (ends b)[j]? = none :=
      List.getElem?_eq_none (by simpa [ends_length] using (show b.length ≤ j by omega))
    rw [hn1, hn2]
    simp

private theorem positions_take (w n : Nat) (xs : List Nat) :
    (positions w xs).take n = positions w (xs.take n) := by
  apply List.ext_getElem?
  intro j
  rw [List.getElem?_take]
  by_cases hj : j < n
  · rw [ite_eq_left hj]
    have h1 := positions_get? w xs j
    have h2 := positions_get? w (xs.take n) j
    have ht : (xs.take n)[j]? = xs[j]? := by rw [List.getElem?_take, ite_eq_left hj]
    rw [ht] at h2
    rw [h1, h2]
  · rw [ite_eq_right hj]
    have hn : (positions w (xs.take n))[j]? = none := by
      apply List.getElem?_eq_none
      simp [positions]
      have : min n xs.length ≤ j := by omega
      exact this
    rw [hn]

private theorem byteCount_split (x y : Nat) :
    byteCount x + byteCount y ≤ byteCount (x+y) + 1 := by
  unfold byteCount
  have hx := Nat.div_add_mod (x+7) 8
  have hy := Nat.div_add_mod (y+7) 8
  have hxy := Nat.div_add_mod (x+y+7) 8
  have hmx : (x+7)%8 < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  have hmy : (y+7)%8 < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  have hmxy : (x+y+7)%8 < 8 := Nat.mod_lt _ (by decide : 0 < 8)
  omega

private theorem byteCount_mono {x y : Nat} (h : x ≤ y) : byteCount x ≤ byteCount y := by
  unfold byteCount
  omega

private theorem div_add_le (d a b : Nat) (hd : 0 < d) : a/d + b/d ≤ (a+b)/d := by
  rw [Nat.add_div hd]
  split <;> omega

private theorem prefix_le_cap : ∀ (sizes : List Nat) (k cap : Nat),
    (∀ x ∈ sizes, x ≤ cap) → (sizes.take k).sum ≤ k*cap
  | [], k, cap, h => by simp
  | x::xs, 0, cap, h => by simp
  | x::xs, k+1, cap, h => by
      have hx : x ≤ cap := h x (by simp)
      have ht : ∀ y ∈ xs, y ≤ cap := by intro y hy; exact h y (by simp [hy])
      have hi := prefix_le_cap xs k cap ht
      simp only [List.take_succ_cons, List.sum_cons, List.length_cons, Nat.succ_mul]
      omega

private theorem prefix_gap (sizes : List Nat) (a i cap : Nat)
    (hai : a ≤ i) (hi : i < sizes.length) (hcap : ∀ x ∈ sizes, x ≤ cap) :
    (sizes.take (i+1)).sum ≤ (sizes.take (a+1)).sum + (i-a)*cap := by
  have hla : a+1 ≤ sizes.length := by omega
  have hlen_a : (sizes.take (a+1)).length = a+1 := by
    rw [List.length_take, Nat.min_eq_left hla]
  have hdec : sizes.take (i+1) = sizes.take (a+1) ++ (sizes.drop (a+1)).take (i-a) := by
    conv => lhs; rw [← List.take_append_drop (a+1) sizes]
    rw [List.take_append, hlen_a, List.take_take, Nat.min_eq_right (by omega : a+1 ≤ i+1)]
    have : i+1-(a+1) = i-a := by omega
    rw [this]
  rw [hdec, List.sum_append]
  have htailcap : ∀ y ∈ sizes.drop (a+1), y ≤ cap := by
    intro y hy
    exact hcap y (List.mem_of_mem_drop hy)
  have ht := prefix_le_cap (sizes.drop (a+1)) (i-a) cap htailcap
  omega

private theorem ends_gap (sizes : List Nat) (a i cap : Nat)
    (hai : a ≤ i) (hi : i < sizes.length) (hcap : ∀ x ∈ sizes, x ≤ cap) :
    (ends sizes)[i]?.getD 0 - (ends sizes)[a]?.getD 0 ≤ (i-a)*cap := by
  have hpi := ends_get? sizes i
  have hpa := ends_get? sizes a
  rw [ite_eq_left hi] at hpi
  rw [ite_eq_left (by omega : a < sizes.length)] at hpa
  have hEi : (ends sizes)[i]?.getD 0 = (sizes.take (i+1)).sum := by
    rw [hpi]
    simp
  have hEa : (ends sizes)[a]?.getD 0 = (sizes.take (a+1)).sum := by
    rw [hpa]
    simp
  rw [hEi, hEa]
  have h := prefix_gap sizes a i cap hai hi hcap
  exact Nat.sub_le_iff_le_add.mpr (by omega)

private theorem div_gap (d x t : Nat) (hd : 0 < d) :
    (x+t)/d - x/d ≤ (t+d-1)/d := by
  have hdec : d*(x/d) + x%d = x := Nat.div_add_mod x d
  have hr : x%d < d := Nat.mod_lt x hd
  have hx : x + t = d*(x/d) + (x%d + t) := by
    calc
      x + t = (d*(x/d) + x%d) + t := by rw [hdec]
      _ = d*(x/d) + (x%d + t) := by rw [Nat.add_assoc]
  rw [hx]
  have hadd : (d*(x/d) + (x%d+t))/d = (x%d+t)/d + x/d := by
    rw [Nat.add_comm (d*(x/d)) (x%d+t)]
    rw [Nat.add_mul_div_left (x%d+t) (x/d) hd]
  rw [hadd]
  have hrle : x%d ≤ d-1 := by omega
  have harg : x%d + t ≤ t + d - 1 := by omega
  have hle : (x%d+t)/d ≤ (t+d-1)/d := Nat.div_le_div_right harg
  rw [Nat.add_sub_cancel]
  exact hle

theorem anchorScanBound_sound : AnchorScanBound.Goal := by
  intro w sizes a i cap hai hi hcap
  dsimp
  let xs := ends sizes
  let ps := positions w xs
  have hi' : i < xs.length := by dsimp [xs]; simpa [ends_length] using hi
  have ha' : a < xs.length := by omega
  have hgap := ends_gap sizes a i cap hai hi hcap
  have hEi := ends_get? sizes i
  have hEa := ends_get? sizes a
  rw [ite_eq_left hi] at hEi
  rw [ite_eq_left (by omega : a < sizes.length)] at hEa
  have hmono : (sizes.take (a+1)).sum ≤ (sizes.take (i+1)).sum :=
    take_sum_mono sizes (a+1) (i+1) (by omega)
  let Ea := (ends sizes)[a]?.getD 0
  let Ei := (ends sizes)[i]?.getD 0
  have hleE : Ea ≤ Ei := by
    dsimp [Ea, Ei]
    rw [hEa]
    simp only [Option.getD_some]
    rw [hEi]
    simp only [Option.getD_some]
    exact hmono
  have hgeta : xs[a] = Ea := by
    dsimp [xs, Ea]
    rw [List.getElem?_eq_getElem (by simpa [ends_length] using (show a < sizes.length by omega))]
    simp
  have hgeti : xs[i] = Ei := by
    dsimp [xs, Ei]
    rw [List.getElem?_eq_getElem (by simpa [ends_length] using hi)]
    simp
  have hAi : ps[a]?.getD 0 = Ea/2^w + a := by
    have hg := positions_get? w xs a
    rw [List.getElem?_eq_getElem ha'] at hg
    rw [hgeta] at hg
    simpa [ps] using congrArg (fun o : Option Nat => o.getD 0) hg
  have hAi' : ps[i]?.getD 0 = Ei/2^w + i := by
    have hg := positions_get? w xs i
    rw [List.getElem?_eq_getElem hi'] at hg
    rw [hgeti] at hg
    simpa [ps] using congrArg (fun o : Option Nat => o.getD 0) hg
  rw [hAi', hAi]
  have hdive : Ea/2^w ≤ Ei/2^w := Nat.div_le_div_right hleE
  have hgap' : Ei-Ea ≤ (i-a)*cap := by
    dsimp [Ea, Ei]
    exact hgap
  have hmonodiv : ((Ei-Ea + 2^w-1)/2^w) ≤ ((i-a)*cap + 2^w-1)/2^w :=
    Nat.div_le_div_right (by omega)
  have hq := div_gap (2^w) Ea (Ei-Ea) (Nat.two_pow_pos w)
  have hsum : Ea + (Ei-Ea) = Ei := by omega
  rw [hsum] at hq
  have hq' : Ei/2^w - Ea/2^w ≤ ((i-a)*cap + 2^w-1)/2^w := by omega
  have hsub : Ei/2^w + i - (Ea/2^w + a) = (Ei/2^w - Ea/2^w) + (i-a) := by omega
  rw [hsub]
  omega

private theorem zipIdx_drop (ps : List Nat) (k : Nat) :
    (ps.zipIdx).drop k = (ps.drop k).zipIdx k := by
  apply List.ext_getElem?
  intro j
  rw [List.getElem?_drop, List.getElem?_zipIdx, List.getElem?_zipIdx]
  rw [List.getElem?_drop]
  simp

private theorem take_filter_first (M k : Nat) (hM : 0 < M) (hkpos : 0 < k) (hkM : k ≤ M) :
    ∀ (ps : List Nat), ((ps.zipIdx).take k).filterMap
      (fun x => if x.2 % M == 0 then some x.1 else none) = ps[0]?.toList := by
  induction k with
  | zero => intro ps; omega
  | succ k ih =>
      intro ps
      have hk' : k ≤ M := by omega
      rw [List.take_add_one, List.filterMap_append]
      by_cases hk0 : k = 0
      · subst k
        rw [List.getElem?_zipIdx]
        have hm : (0 : Nat) % M == 0 := by simp
        cases hopt : ps[0]? with
        | none => simp [hopt]
        | some a => simp [hopt, hm]
      · have hkpos' : 0 < k := Nat.pos_of_ne_zero hk0
        have ih' := ih hkpos' hk' ps
        rw [ih']
        have hklt : k < M := by omega
        have hmod : (k % M == 0) = false := by
          rw [Nat.mod_eq_of_lt hklt]
          simp [hk0]
        have hpred : (k % M == 0) = false := hmod
        simp [List.getElem?_zipIdx, hpred]
        intro a ha hk
        rw [hk] at hpred
        contradiction

private theorem filterMap_shift (M : Nat) (l : List (Nat × Nat)) :
    l.filterMap (fun x => if (x.2 + M) % M == 0 then some x.1 else none) =
    l.filterMap (fun x => if x.2 % M == 0 then some x.1 else none) := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have hmod : (a.2 + M) % M = a.2 % M := Nat.add_mod_right a.2 M
      simp only [List.filterMap_cons]
      rw [hmod, ih]

private theorem zipIdx_filter_add (stride s t : Nat) : ∀ ps : List Nat,
    (ps.zipIdx t).filterMap (fun x => if (x.2+s) % stride == 0 then some x.1 else none) =
    (ps.zipIdx).filterMap (fun x => if (x.2+s+t) % stride == 0 then some x.1 else none)
  | [] => rfl
  | x::xs => by
      rw [show (x::xs).zipIdx t = (x,t)::(xs.zipIdx (t+1)) by rfl]
      rw [show (x::xs).zipIdx = (x,0)::(xs.zipIdx 1) by rfl]
      simp only [List.filterMap_cons]
      have hih := zipIdx_filter_add stride s (t+1) xs
      have htail := zipIdx_filter_add stride (s+t) 1 xs
      by_cases h : (t+s)%stride == 0
      · have h' : (0+s+t)%stride == 0 := by simpa [Nat.add_assoc, Nat.add_comm] using h
        have hs_t : (s+t)%stride == 0 := by simpa [Nat.add_comm] using h
        simp [h, h', hs_t]
        have hih' : List.filterMap (fun y : Nat × Nat => if (y.2+s)%stride = 0 then some y.1 else none) (xs.zipIdx (t+1)) =
            List.filterMap (fun y => if (y.2+s+(t+1))%stride = 0 then some y.1 else none) xs.zipIdx := by
          simpa [beq_iff_eq] using hih
        have htail' : List.filterMap (fun y : Nat × Nat => if (y.2+s+t)%stride = 0 then some y.1 else none) (xs.zipIdx 1) =
            List.filterMap (fun y => if (y.2+s+t+1)%stride = 0 then some y.1 else none) xs.zipIdx := by
          simpa [beq_iff_eq, Nat.add_assoc] using htail
        exact hih'.trans (by simpa [Nat.add_assoc] using htail'.symm)
      · have h' : ((0+s+t)%stride == 0) = false := by
          simp only [Bool.eq_false_iff]
          intro hh
          apply h
          simpa [Nat.add_assoc, Nat.add_comm] using hh
        have hf : ((t+s)%stride == 0) = false := Bool.eq_false_of_ne_true h
        have hs_t : ((s+t)%stride == 0) = false := by
          simp only [Bool.eq_false_iff]
          intro hh
          apply h
          simpa [Nat.add_comm] using hh
        simp [hf, h', hs_t]
        have hih' : List.filterMap (fun y : Nat × Nat => if (y.2+s)%stride = 0 then some y.1 else none) (xs.zipIdx (t+1)) =
            List.filterMap (fun y => if (y.2+s+(t+1))%stride = 0 then some y.1 else none) xs.zipIdx := by
          simpa [beq_iff_eq] using hih
        have htail' : List.filterMap (fun y : Nat × Nat => if (y.2+s+t)%stride = 0 then some y.1 else none) (xs.zipIdx 1) =
            List.filterMap (fun y => if (y.2+s+t+1)%stride = 0 then some y.1 else none) xs.zipIdx := by
          simpa [beq_iff_eq, Nat.add_assoc] using htail
        exact hih'.trans (by simpa [Nat.add_assoc] using htail'.symm)

private theorem anchors_start (stride s : Nat) (hs : 0 < stride) : ∀ ps : List Nat,
    (ps.zipIdx s).filterMap (fun x => if x.2 % stride == 0 then some x.1 else none) =
    (ps.zipIdx).filterMap (fun x => if (x.2 + s) % stride == 0 then some x.1 else none)
  | [] => rfl
  | x::xs => by
      rw [show (x::xs).zipIdx s = (x,s)::(xs.zipIdx (s+1)) by rfl]
      rw [show (x::xs).zipIdx = (x,0)::(xs.zipIdx 1) by rfl]
      simp only [List.filterMap_cons]
      have hih := anchors_start stride (s+1) hs xs
      have htail := zipIdx_filter_add stride s 1 xs
      by_cases hs0 : s % stride == 0
      · have h0s : ((0+s)%stride == 0) = true := by simpa using hs0
        have ht := hih.trans (by simpa [Nat.add_assoc] using htail.symm)
        simp [hs0, h0s]
        simpa [beq_iff_eq] using ht
      · have h0s0 : ((0+s)%stride == 0) = false := by simpa using hs0
        have hf : ((s%stride == 0)) = false := Bool.eq_false_of_ne_true hs0
        have ht := hih.trans (by simpa [Nat.add_assoc] using htail.symm)
        simp [hf, h0s0]
        simpa [beq_iff_eq] using ht

private theorem anchors_cons (stride : Nat) (hs : 0 < stride) (ps : List Nat) (hps : ps ≠ []) :
    anchors stride ps = ps[0]?.getD 0 :: anchors stride (ps.drop stride) := by
  have hlen : 0 < ps.length := by
    cases ps with
    | nil => simp at hps
    | cons p ps => simp
  unfold anchors
  have hsplit := List.take_append_drop stride (ps.zipIdx)
  conv => lhs; rw [← hsplit]
  rw [List.filterMap_append]
  have hfirst : ((ps.zipIdx).take stride).filterMap
      (fun x => if x.2 % stride == 0 then some x.1 else none) = ps[0]?.toList :=
    take_filter_first stride stride hs hs (Nat.le_refl stride) ps
  rw [hfirst]
  have hps0 : ps[0]?.toList = [ps[0]?.getD 0] := by
    have hg : ps[0]? = some ps[0] := List.getElem?_eq_getElem hlen
    rw [hg]
    simp
  rw [hps0]
  congr 1
  rw [zipIdx_drop]
  rw [anchors_start stride stride hs]
  rw [filterMap_shift]
  rfl

private theorem anchors_get (stride : Nat) (hs : 0 < stride) (ps : List Nat) (j : Nat)
    (hj : j*stride < ps.length) :
    (anchors stride ps)[j]? = some ps[j*stride] := by
  induction j generalizing ps with
  | zero =>
      have hps : ps ≠ [] := by
        intro hp
        subst hp
        simp at hj
      rw [anchors_cons stride hs ps hps]
      simp
      have hlen : 0 < ps.length := by
        cases ps with
        | nil => simp at hps
        | cons p ps => simp
      rw [List.getElem?_eq_getElem hlen]
      simp
  | succ j ih =>
      have hps : ps ≠ [] := by
        intro hp
        subst hp
        simp at hj
      rw [anchors_cons stride hs ps hps]
      simp only [List.getElem?_cons_succ]
      have hlt : j*stride + stride < ps.length := by
        simpa [Nat.succ_mul] using hj
      have hle : j*stride + stride ≤ ps.length := Nat.le_of_lt hlt
      have hdrop : j*stride < (ps.drop stride).length := by
        simp
        omega
      have hih := ih (ps.drop stride) hdrop
      rw [hih]
      have hd : (ps.drop stride)[j*stride] = ps[stride + j*stride] := by
        rw [List.getElem_drop]
      rw [hd]
      have hidx : stride + j*stride = (j+1)*stride := by rw [Nat.succ_mul, Nat.add_comm]
      simpa [hidx]

private theorem range_map_eq_range' (start N : Nat) :
    (List.range (N-start)).map (fun k => start+k) = List.range' start (N-start) := by
  apply List.ext_getElem?
  intro j
  by_cases hj : j < N-start
  · rw [List.getElem?_map, List.getElem?_range hj, List.getElem?_range' hj]
    simp
  · have h1 : ((List.range (N-start)).map (fun k => start+k)).length = N-start := by simp
    have h2 : (List.range' start (N-start)).length = N-start := by simp
    rw [List.getElem?_eq_none (by omega), List.getElem?_eq_none (by omega)]

theorem uniquePositions_sound : UniquePositions.Goal := by
  intro w sizes
  exact positions_pairwise_of_mono w (ends sizes) (ends_pairwise_le sizes)

theorem unarySelect_sound : UnarySelect.Goal := by
  intro w sizes
  exact ones_high_eq w sizes

theorem endSelect_sound : EndSelect.Goal := by
  intro w sizes i
  let xs := ends sizes
  change lookup w i (high w xs) (pack w xs) = xs[i]?
  have hone : ones (high w xs) = positions w xs := by
    dsimp [xs]
    exact ones_high_eq w sizes
  unfold lookup
  rw [hone]
  have ht := takeAt_pack_low w xs i
  rw [ht]
  by_cases hi : i < xs.length
  · have hpget : (positions w xs)[i]? = some (xs[i]/2^w + i) := by
      have hg := positions_get? w xs i
      rw [List.getElem?_eq_getElem hi] at hg
      simpa using hg
    rw [hpget]
    simp only [Option.map_some]
    have hgetd : xs[i]?.getD 0 = xs[i] := by
      rw [List.getElem?_eq_getElem hi]
      simp
    rw [hgetd]
    rw [List.getElem?_eq_getElem hi]
    simp only [Option.some.injEq]
    have hdm : (xs[i]/2^w)*2^w + xs[i]%2^w = xs[i] := by
      rw [Nat.mul_comm]
      exact Nat.div_add_mod (xs[i]) (2^w)
    have hsub : xs[i]/2^w + i - i = xs[i]/2^w := Nat.add_sub_cancel _ _
    rw [hsub, hdm]
  · have hge : xs.length ≤ i := by omega
    have hpsnone : (positions w xs)[i]? = none := by
      apply List.getElem?_eq_none
      simpa [positions] using hge
    have hxnone : xs[i]? = none := List.getElem?_eq_none hge
    rw [hpsnone, hxnone]
    simp

theorem sliceBySelect_sound : SliceBySelect.Goal := by
  intro w rows i hi
  let sizes := rows.map List.length
  let xs := ends sizes
  let atEnd := fun j => (lookup w j (high w xs) (pack w xs)).getD 0
  have hES : ∀ w sizes j, let ys := ends sizes
      lookup w j (high w ys) (pack w ys) = ys[j]? := endSelect_sound
  have hat : ∀ j, atEnd j = xs[j]?.getD 0 := by
    intro j
    dsimp [atEnd, xs]
    exact congrArg (fun o : Option Nat => o.getD 0) (hES w sizes j)
  have hxsi : xs[i]?.getD 0 = ((rows.take (i+1)).map List.length).sum := by
    have hg := ends_get? sizes i
    have hil : i < sizes.length := by simpa [sizes] using hi
    rw [ite_eq_left hil] at hg
    have htake : sizes.take (i+1) = (rows.take (i+1)).map List.length := by
      simp [sizes, List.map_take]
    rw [htake] at hg
    simpa [xs] using congrArg (fun o : Option Nat => o.getD 0) hg
  have hprev : (if i == 0 then 0 else atEnd (i-1)) = ((rows.take i).map List.length).sum := by
    by_cases h0 : i = 0
    · subst i
      simp
    · rw [ite_eq_right (by simpa using h0)]
      rw [hat (i-1)]
      have him1 : i-1 < sizes.length := by simpa [sizes] using (show i-1 < rows.length by omega)
      have hg := ends_get? sizes (i-1)
      rw [ite_eq_left him1] at hg
      have htake : sizes.take (i-1+1) = (rows.take i).map List.length := by
        have : i-1+1 = i := by omega
        rw [this]
        simp [sizes, List.map_take]
      rw [htake] at hg
      simpa [xs] using congrArg (fun o : Option Nat => o.getD 0) hg
  have hdiff : xs[i]?.getD 0 - ((rows.take i).map List.length).sum = (rows[i]?.getD []).length := by
    rw [hxsi]
    have happ : rows.take (i+1) = rows.take i ++ [rows[i]] := by
      rw [List.take_add_one, List.getElem?_eq_getElem hi]
      simp
    rw [happ, List.map_append]
    simp only [List.map_cons, List.map_nil, List.sum_append, List.sum_cons, List.sum_nil, Nat.add_zero]
    rw [List.getElem?_eq_getElem hi]
    simp
  change (rows.flatten.drop (if i == 0 then 0 else atEnd (i-1))).take (atEnd i - (if i == 0 then 0 else atEnd (i-1))) = rows[i]?.getD []
  rw [hprev, hat i, hdiff]
  simpa [List.map_take] using flatten_slice rows i hi

theorem highPlaneBudget_sound : HighPlaneBudget.Goal := by
  intro sizes
  dsimp
  let xs := ends sizes
  let n := sizes.length
  let p := peak xs
  by_cases hn : n = 0
  · have hs0 : sizes = [] := by
      dsimp [n] at hn
      exact List.eq_nil_of_length_eq_zero hn
    subst sizes
    native_decide
  · have hnpos : 0 < n := Nat.pos_of_ne_zero hn
    let q := p / n
    let w := width q
    have hq1 : q + 1 ≤ 2^w := by
      have hlt : q < 2^w := by
        dsimp [w, q]
        unfold width
        by_cases hq0 : (p / n) == 0
        · have hzero : p / n = 0 := by simpa using hq0
          have hp_lt : p < n := by
            have hd := Nat.div_eq_zero_iff.mp hzero
            omega
          simp [hq0, hp_lt]
        · have hq0' : p / n ≠ 0 := by simpa using hq0
          simp [width, hq0']
          exact Nat.lt_log2_self
      omega
    have hdec : n*q + p%n = p := by
      dsimp [q]
      exact Nat.div_add_mod p n
    have hmod : p % n < n := Nat.mod_lt p hnpos
    have hmul : n*q + n ≤ n*2^w := by
      have htmp := Nat.mul_le_mul_left n hq1
      dsimp [w] at htmp
      rw [Nat.mul_succ] at htmp
      exact htmp
    have hple : p ≤ n*2^w := by omega
    have hdiv : p / 2^w ≤ n := by
      rw [Nat.div_le_iff_le_mul_add_pred (Nat.two_pow_pos w)]
      rw [Nat.mul_comm (2^w) n]
      omega
    have hlen : (ends sizes).length = n := by dsimp [n]; exact ends_length sizes
    dsimp [highBits, suggested]
    rw [hlen]
    have hfin : peak (ends sizes) / 2 ^ width (peak (ends sizes) / n) ≤ n := by
      simpa [p, w, q, xs] using hdiv
    omega

theorem appendPrefix_sound : AppendPrefix.Goal := by
  intro w a b
  rw [← ends_append_prefix a b]
  exact positions_take w a.length (ends (a++b))

theorem alignedShift_sound : AlignedShift.Goal := by
  intro w a b hdiv
  apply List.ext_getElem?
  intro j
  rw [List.getElem?_drop, List.getElem?_map]
  by_cases hj : j < b.length
  · have hiL : a.length+j < (ends (a++b)).length := by simp [ends_length]; omega
    have hiB : j < (ends b).length := by simpa [ends_length] using hj
    have hpL := positions_get? w (ends (a++b)) (a.length+j)
    have hpB := positions_get? w (ends b) j
    rw [List.getElem?_eq_getElem hiL] at hpL
    rw [List.getElem?_eq_getElem hiB] at hpB
    have hvopt := congrArg (fun l => l[j]?) (ends_append_tail a b)
    rw [List.getElem?_drop, List.getElem?_map] at hvopt
    have hbg : (ends b)[j]? = some (ends b)[j] := List.getElem?_eq_getElem hiB
    rw [hbg] at hvopt
    simp only [Option.map_some] at hvopt
    rw [List.getElem?_eq_getElem hiL] at hvopt
    have hval : (ends (a++b))[a.length+j] = (ends b)[j] + a.sum := by
      injection hvopt with h
    rw [hval] at hpL
    rw [hpL, hpB]
    simp only [Option.map_some, Option.some.injEq]
    rw [add_div_of_dvd_left (2^w) a.sum ((ends b)[j]) (Nat.two_pow_pos w) hdiv]
    omega
  · have hn1 : (positions w (ends (a++b)))[a.length+j]? = none := by
      apply List.getElem?_eq_none
      simp [positions, ends_length]
      omega
    have hn2 : (positions w (ends b))[j]? = none := by
      apply List.getElem?_eq_none
      simp [positions, ends_length]
      omega
    rw [hn1, hn2]
    simp

private theorem ceil_rec (L stride : Nat) (hs : 0 < stride) :
    (L+1+stride-1)/stride = (L+stride-1)/stride + (if L%stride == 0 then 1 else 0) := by
  let k := L / stride
  let r := L % stride
  have hdec : k*stride + r = L := by
    dsimp [k, r]
    have h := Nat.div_add_mod L stride
    rw [Nat.mul_comm stride (L/stride)] at h
    exact h
  have hr : r < stride := by dsimp [r]; exact Nat.mod_lt L hs
  by_cases hr0 : r = 0
  · have hif : (L % stride == 0) = true := by simp [r, hr0]
    rw [ite_eq_left hif]
    have hL : L = k*stride := by omega
    rw [hL]
    have h1 : (k*stride+stride-1)/stride = k := by
      apply Nat.div_eq_of_lt_le
      · omega
      · rw [Nat.succ_mul]
        omega
    have h2 : (k*stride+1+stride-1)/stride = k+1 := by
      have heq : k*stride + 1 + stride - 1 = (k+1)*stride := by
        rw [Nat.succ_mul]
        omega
      rw [heq]
      exact Nat.mul_div_left (k+1) hs
    rw [h1, h2]
  · have hif : (L % stride == 0) = false := by simp [r, hr0]
    rw [ite_eq_right (Bool.eq_false_iff.mp hif)]
    have hrpos : 0 < r := Nat.pos_of_ne_zero hr0
    have h1 : (L+stride-1)/stride = k+1 := by
      rw [← hdec]
      apply Nat.div_eq_of_lt_le
      · rw [Nat.succ_mul]
        omega
      · rw [show k+1+1 = (k+1)+1 by omega, Nat.succ_mul, Nat.succ_mul]
        omega
    have h2 : (L+1+stride-1)/stride = k+1 := by
      rw [← hdec]
      apply Nat.div_eq_of_lt_le
      · rw [Nat.succ_mul]
        omega
      · rw [show k+1+1 = (k+1)+1 by omega, Nat.succ_mul, Nat.succ_mul]
        omega
    rw [h1, h2]

private theorem count_multiples (stride : Nat) (hs : 0 < stride) : ∀ L,
    ((List.range L).filter (fun i => i%stride == 0)).length = (L+stride-1)/stride
  | 0 => by
      have h0 : (stride-1)/stride = 0 := Nat.div_eq_of_lt (by omega)
      simpa using h0.symm
  | L+1 => by
      rw [List.range_succ, List.filter_append, List.length_append]
      have htail : (List.filter (fun i => i%stride == 0) [L]).length = (if L%stride == 0 then 1 else 0) := by
        by_cases h : L%stride == 0
        · simp [h]
        · simp [h]
      rw [htail, count_multiples stride hs L]
      have hc := ceil_rec L stride hs
      by_cases h0 : L%stride == 0
      · rw [ite_eq_left h0] at hc ⊢
        omega
      · rw [ite_eq_right h0] at hc ⊢
        omega

private theorem zipIdx_snd (ps : List Nat) (k : Nat) :
    (ps.zipIdx k).map Prod.snd = List.range' k ps.length := by
  induction ps generalizing k with
  | nil => simp
  | cons p ps ih =>
      simp [ih, List.range'_succ]

private theorem anchors_length (stride : Nat) (hs : 0 < stride) (ps : List Nat) :
    (anchors stride ps).length = (ps.length+stride-1)/stride := by
  unfold anchors
  rw [List.length_filterMap_eq_countP]
  have hfun : (fun x : Nat × Nat => (if x.2 % stride == 0 then some x.1 else none).isSome) =
      ((fun i : Nat => i%stride == 0) ∘ Prod.snd) := by
    funext x
    by_cases h : x.2 % stride == 0 <;> simp [h, Function.comp_def]
  rw [hfun]
  have hcnt : (ps.zipIdx).countP ((fun i : Nat => i%stride == 0) ∘ Prod.snd) =
      ((ps.zipIdx).map Prod.snd).countP (fun i => i%stride == 0) := by
    induction ps.zipIdx with
    | nil => rfl
    | cons a as ih =>
        simp only [List.countP_cons, List.map_cons]
        by_cases h : a.2 % stride == 0 <;> simp [h, ih, Function.comp_def]
  rw [hcnt, zipIdx_snd, List.countP_eq_length_filter]
  rw [range'_zero]
  exact count_multiples stride hs ps.length

theorem physicalCharge_sound : PhysicalCharge.Goal := by
  intro w sizes
  dsimp only [directoryBytes]
  have hhi : (high w (ends sizes)).length = byteCount (sizes.length + sizes.sum/2^w) := by
    unfold high
    rw [pack_length]
    simp [highBits, peak_ends, ends_length]
  have hlo : (pack w (ends sizes)).length = byteCount (sizes.length*w) := by
    rw [pack_length]
    simp [ends_length]
  rw [hhi, hlo]
  omega

theorem resetHighBase_sound : ResetHighBase.Goal := by
  intro w a b
  have hA := physicalCharge_sound w a
  have hB := physicalCharge_sound w b
  have hAB := physicalCharge_sound w (a++b)
  have hs : (a++b).sum = a.sum + b.sum := by simp
  have hlen : (a++b).length = a.length + b.length := by simp
  rw [hlen, hs] at hAB
  rw [hA, hB, hAB]
  have h1 := byteCount_split (a.length*w) (b.length*w)
  have hlenw : (a.length+b.length)*w = a.length*w + b.length*w := by rw [Nat.add_mul]
  have h2 := byteCount_split (a.length + a.sum/2^w) (b.length + b.sum/2^w)
  have hdiv := div_add_le (2^w) a.sum b.sum (Nat.two_pow_pos w)
  have hmono : byteCount (a.length + a.sum/2^w + (b.length + b.sum/2^w)) ≤
      byteCount (a.length + b.length + (a.sum+b.sum)/2^w) := by
    apply byteCount_mono
    omega
  have h2' : byteCount (a.length + a.sum/2^w) + byteCount (b.length + b.sum/2^w) ≤
      byteCount (a.length + b.length + (a.sum+b.sum)/2^w) + 1 := by
    have h2a := byteCount_split (a.length + a.sum/2^w) (b.length + b.sum/2^w)
    omega
  rw [hlenw]
  omega

theorem restartSelect_sound : RestartSelect.Goal := by
  intro w sizes stride i hstride hi
  dsimp
  let xs := ends sizes
  let ps := positions w xs
  have hps : ps.Pairwise (·<·) := by
    dsimp [ps, xs]
    exact positions_pairwise_of_mono w (ends sizes) (ends_pairwise_le sizes)
  have hpslen : ps.length = sizes.length := by simp [ps, positions, xs, ends_length]
  have hbase_mul : (i/stride)*stride ≤ i := Nat.div_mul_le_self i stride
  have hbase_lt : (i/stride)*stride < sizes.length := Nat.lt_of_le_of_lt hbase_mul hi
  have hqle : (i/stride+1)*stride ≤ sizes.length+stride-1 := by
    rw [Nat.succ_mul]
    omega
  have hqceil : i/stride+1 ≤ (sizes.length+stride-1)/stride :=
    (Nat.le_div_iff_mul_le hstride).2 hqle
  have hcnt : i/stride < (anchors stride ps).length := by
    rw [anchors_length stride hstride ps, hpslen]
    omega
  have haget := anchors_get stride hstride ps (i/stride) (by simpa [hpslen] using hbase_lt)
  have hanchor : ((anchors stride ps)[i/stride]?).getD 0 = ps[(i/stride)*stride] := by
    rw [haget]
    simp
  let anchor := ps[(i/stride)*stride]
  have hbaseps : (i/stride)*stride < ps.length := by simpa [hpslen] using hbase_lt
  have hanchor_pos : anchor ∈ ps := by
    rw [← show ps[(i/stride)*stride] = anchor by rfl]
    exact List.getElem_mem hbaseps
  have hanchor_bound : anchor < highBits w xs := by
    dsimp [anchor, ps, xs]
    apply positions_bound
    exact hanchor_pos
  have hlenhi : (high w xs).length = byteCount (highBits w xs) := by
    unfold high
    rw [pack_length]
    simp
  have hcover : highBits w xs ≤ 8*(high w xs).length := by
    rw [hlenhi]
    exact byteCount_cover _
  have hastart : anchor ≤ 8*(high w xs).length := Nat.le_trans (Nat.le_of_lt hanchor_bound) hcover
  have hmem_eq : ∀ j, anchor ≤ j → ((ps.contains j) = true ↔ ((ps.drop ((i/stride)*stride)).contains j) = true) := by
    intro j hj
    constructor
    · intro hc
      have hm : j ∈ ps := List.contains_iff_mem.mp hc
      rw [List.mem_iff_getElem?] at hm
      obtain ⟨k, hk⟩ := hm
      have hklt : k < ps.length := by
        by_cases hk' : k < ps.length
        · exact hk'
        · have hge : ps.length ≤ k := by omega
          rw [List.getElem?_eq_none hge] at hk
          simp at hk
      have hpk : ps[k] = j := by
        rw [List.getElem?_eq_getElem hklt] at hk
        simpa using hk
      have hkge : (i/stride)*stride ≤ k := by
        by_cases hkge' : (i/stride)*stride ≤ k
        · exact hkge'
        · have hkltb : k < (i/stride)*stride := by omega
          have hkb := (List.pairwise_iff_getElem.mp hps) k ((i/stride)*stride) hklt hbaseps hkltb
          have hltanchor : j < anchor := by
            simpa [hpk, show ps[(i/stride)*stride] = anchor by rfl] using hkb
          omega
      apply List.contains_iff_mem.mpr
      rw [List.mem_iff_getElem?]
      refine ⟨k-(i/stride)*stride, ?_⟩
      rw [List.getElem?_drop]
      have heq : (i/stride)*stride + (k-(i/stride)*stride) = k := Nat.add_sub_of_le hkge
      rw [heq, hk]
    · intro hc
      apply List.contains_iff_mem.mpr
      have hm : j ∈ ps.drop ((i/stride)*stride) := List.contains_iff_mem.mp hc
      rw [List.mem_iff_getElem?] at hm
      obtain ⟨t, ht⟩ := hm
      rw [List.getElem?_drop] at ht
      have hm2 : ps[(i/stride)*stride+t]? = some j := ht
      have hpos : (i/stride)*stride+t < ps.length := by
        by_cases hh : (i/stride)*stride+t < ps.length
        · exact hh
        · have hge : ps.length ≤ (i/stride)*stride+t := by omega
          rw [List.getElem?_eq_none hge] at hm2
          simp at hm2
      rw [List.mem_iff_getElem?]
      exact ⟨(i/stride)*stride+t, hm2⟩
  have hdrop_ps : (ps.drop ((i/stride)*stride)).Pairwise (·<·) := hps.drop
  have hdroplo : ∀ p ∈ ps.drop ((i/stride)*stride), anchor ≤ p := by
    intro p hp
    rw [List.mem_iff_getElem?] at hp
    obtain ⟨t, ht⟩ := hp
    rw [List.getElem?_drop] at ht
    have hpos : (i/stride)*stride+t < ps.length := by
      by_cases hh : (i/stride)*stride+t < ps.length
      · exact hh
      · have hge : ps.length ≤ (i/stride)*stride+t := by omega
        rw [List.getElem?_eq_none hge] at ht
        simp at ht
    have hval : ps[(i/stride)*stride+t] = p := by
      rw [List.getElem?_eq_getElem hpos] at ht
      simpa using ht
    have hle : ps[(i/stride)*stride] ≤ ps[(i/stride)*stride+t] := by
      by_cases heq : (i/stride)*stride = (i/stride)*stride+t
      · have ht0 : t = 0 := by omega
        subst t
        exact Nat.le_refl _
      · have hlt : (i/stride)*stride < (i/stride)*stride+t := by omega
        exact Nat.le_of_lt ((List.pairwise_iff_getElem.mp hps) ((i/stride)*stride) ((i/stride)*stride+t) hbaseps hpos hlt)
    change ps[(i/stride)*stride] ≤ p
    rw [← hval]
    exact hle
  have hdrop_hi : ∀ p ∈ ps.drop ((i/stride)*stride), p < 8*(high w xs).length := by
    intro p hp
    have hpps : p ∈ ps := by
      have hm := hp
      rw [List.mem_iff_getElem?] at hm
      obtain ⟨t, ht⟩ := hm
      rw [List.getElem?_drop] at ht
      have hpos : (i/stride)*stride+t < ps.length := by
        by_cases hh : (i/stride)*stride+t < ps.length
        · exact hh
        · have hge : ps.length ≤ (i/stride)*stride+t := by omega
          rw [List.getElem?_eq_none hge] at ht
          simp at ht
      rw [List.mem_iff_getElem?]
      exact ⟨(i/stride)*stride+t, ht⟩
    exact Nat.lt_of_lt_of_le (positions_bound w xs p hpps) hcover
  unfold seekFrom
  rw [range_map_eq_range']
  have hfilt1 : (List.range' anchor (8*(high w xs).length-anchor)).filter
      (fun j => get (high w xs) 1 j == 1) =
      (List.range' anchor (8*(high w xs).length-anchor)).filter (fun j => ps.contains j) := by
    apply List.filter_congr
    intro j hj
    have hjlt : j < 8*(high w xs).length := by
      have hm := List.mem_range'.mp hj
      omega
    rw [get_high_eq w xs j hjlt]
    have hbool : (((if (positions w xs).contains j then 1 else 0) == 1) = ps.contains j) := by
      by_cases hm : j ∈ positions w xs
      · have hm' : j ∈ ps := by simpa [ps] using hm
        have hc1 : (positions w xs).contains j = true := List.contains_iff_mem.mpr hm
        rw [ite_eq_left hc1]
        have hc2 : ps.contains j = true := List.contains_iff_mem.mpr hm'
        rw [hc2]
        decide
      · have hm' : j ∉ ps := by simpa [ps] using hm
        have hc1 : ¬ (positions w xs).contains j = true := fun ht => hm (List.contains_iff_mem.mp ht)
        rw [ite_eq_right hc1]
        have hc2 : ps.contains j = false := Bool.eq_false_of_ne_true (fun ht => hm' (List.contains_iff_mem.mp ht))
        rw [hc2]
        decide
    have ha : anchor ≤ j := by
      have hm := List.mem_range'.mp hj
      omega
    by_cases hc : ps.contains j = true
    · have hc' : (ps.drop ((i/stride)*stride)).contains j = true := (hmem_eq j ha).mp hc
      simpa [ps] using hbool
    · have hcf : ps.contains j = false := Bool.eq_false_of_ne_true hc
      have hc' : (ps.drop ((i/stride)*stride)).contains j = false := by
        apply Bool.eq_false_of_ne_true
        intro ht
        have hh := (hmem_eq j ha).mpr ht
        rw [hcf] at hh
        contradiction
      simpa [ps] using hbool
  change ((List.range' ((anchors stride ps)[i/stride]?.getD 0)
      (8*(high w xs).length - ((anchors stride ps)[i/stride]?.getD 0))).filter
      (fun j => get (high w xs) 1 j == 1))[i%stride]? = ps[i]?
  rw [hanchor]
  rw [hfilt1]
  have hfilt0 : (List.range' anchor (8*(high w xs).length-anchor)).filter (fun j => ps.contains j) =
      (List.range' anchor (8*(high w xs).length-anchor)).filter
        (fun j => (ps.drop ((i/stride)*stride)).contains j) := by
    apply List.filter_congr
    intro j hj
    have ha : anchor ≤ j := by
      have hm := List.mem_range'.mp hj
      omega
    by_cases hc : ps.contains j = true
    · have hc2 := (hmem_eq j ha).mp hc
      rw [hc, hc2]
    · have hcf : ps.contains j = false := Bool.eq_false_of_ne_true hc
      have hcf2 : (ps.drop ((i/stride)*stride)).contains j = false := by
        apply Bool.eq_false_of_ne_true
        intro ht
        exact hc ((hmem_eq j ha).mpr ht)
      rw [hcf, hcf2]
  rw [hfilt0]
  have hfilt2 := filter_range'_sorted (ps.drop ((i/stride)*stride)) hdrop_ps anchor
      (8*(high w xs).length) hdroplo hdrop_hi
  rw [hfilt2]
  rw [List.getElem?_drop]
  have hidx : (i/stride)*stride + i%(stride) = i := by
    have hdm := Nat.div_add_mod i stride
    rw [Nat.mul_comm stride (i/stride)] at hdm
    exact hdm
  rw [hidx]

theorem indexedBreakEven_sound : IndexedBreakEven.Goal := by
  intro w sizes stride header hstride
  dsimp
  let xs := ends sizes
  have hdir : directoryBytes w xs = byteCount (sizes.length*w) + byteCount (sizes.length+sizes.sum/2^w) := by
    dsimp [xs]
    exact physicalCharge_sound w sizes
  have hanch : (anchors stride (positions w xs)).length = (sizes.length+stride-1)/stride := by
    have hposlen : (positions w xs).length = sizes.length := by simp [positions, xs, ends_length]
    rw [← hposlen]
    exact anchors_length stride hstride _
  rw [hdir, hanch]
  rw [← Nat.add_assoc header (byteCount (sizes.length*w))
      (byteCount (sizes.length + sizes.sum/2^w))]

theorem deltaLengths_sound : DeltaLengths.Goal := by
  intro sizes
  unfold extents ends
  exact extents_scanl_aux sizes 0

theorem emptyRun_sound : EmptyRun.Goal := by
  intro n
  dsimp only [directoryBytes]
  have hhi : (high 0 (ends (List.replicate n 0))).length = byteCount n := by
    unfold high
    rw [pack_length]
    simp [highBits, ends_replicate_zero, peak_replicate_zero]
  have hlo : (pack 0 (ends (List.replicate n 0))).length = 0 := by
    rw [pack_length]
    change (0 + 7) / 8 = 0
    decide
  rw [hhi, hlo]
  omega

theorem increaseLowWidth_sound : IncreaseLowWidth.Goal := by
  intro w sizes
  dsimp
  simp only [directoryBytes, high]
  repeat rw [pack_length]
  simp only [List.length_map, List.length_range, Nat.mul_one, ends_length]
  constructor <;> intro h <;> omega

theorem averageWidthOptimal_disproof : ¬ AverageWidthOptimal.Goal := by
  intro h
  have h' : ∀ w sizes, let xs := ends sizes
      directoryBytes (suggested xs) xs ≤ directoryBytes w xs := h
  have hh := h' 0 [1]
  revert hh
  native_decide

end TinylambOpt.ResearchProofs.Compression.Directories
