import TinylambOpt.ResearchGoals.Compression.IntegerFrames

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.IntegerFrames
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.IntegerFrames
open TinylambOpt.ResearchGoals.Compression.IntegerFrames

set_option maxHeartbeats 0

private theorem fromLittle_append (a b : Bytes) :
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

private theorem little_bytes (n k : Nat) : ∀ b ∈ little n k, b < 256 := by
  intro b hb
  simp [little] at hb
  obtain ⟨i, hi, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide : 0 < 256)

private theorem fromLittle_lt (bs : Bytes) (h : ∀ b ∈ bs, b < 256) :
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

private theorem fromLittle_take (bs : Bytes) (k : Nat) (h : ∀ b ∈ bs, b < 256) :
    fromLittle (bs.take k) = fromLittle bs % 256 ^ k := by
  induction bs generalizing k with
  | nil => cases k <;> simp [fromLittle]
  | cons x xs ih =>
      cases k with
      | zero =>
          simp [fromLittle]
          have hh := Nat.mod_lt (x + 256 * fromLittle xs) (by decide : 0 < 1)
          omega
      | succ k =>
          simp only [List.take_succ_cons, fromLittle]
          rw [ih (k) (by intro b hb; exact h b (by simp [hb]))]
          let M := 256 ^ k
          have hM : 0 < M := Nat.pow_pos (by decide)
          have hm := Nat.mod_add_div (fromLittle xs) M
          have hdecomp : 256 * fromLittle xs = 256 * (fromLittle xs % M) + 256 ^ (k+1) * (fromLittle xs / M) := by
            conv => lhs; rw [show fromLittle xs = fromLittle xs % M + M * (fromLittle xs / M) from hm.symm]
            rw [Nat.mul_add]
            rw [Nat.pow_succ]
            dsimp [M]
            congr 1
            ac_rfl
          have hbound : x + 256 * (fromLittle xs % M) < 256 ^ (k+1) := by
            have hx : x < 256 := h x (by simp)
            have hlt : fromLittle xs % M < M := Nat.mod_lt _ hM
            have hs := Nat.mul_le_mul_left 256 (Nat.succ_le_of_lt hlt)
            rw [Nat.pow_succ]
            dsimp [M] at *
            omega
          rw [hdecomp]
          rw [← Nat.add_assoc]
          rw [Nat.add_mul_mod_self_left]
          rw [Nat.mod_eq_of_lt hbound]

private theorem fromLittle_drop (bs : Bytes) (k : Nat) (hkl : k ≤ bs.length)
    (h : ∀ b ∈ bs, b < 256) :
    fromLittle (bs.drop k) = fromLittle bs / 256 ^ k := by
  have happ := fromLittle_append (bs.take k) (bs.drop k)
  rw [List.take_append_drop] at happ
  have hlen : (bs.take k).length = k := by
    rw [List.length_take]
    omega
  rw [hlen] at happ
  have hlt : fromLittle (bs.take k) < 256 ^ k := by
    simpa [hlen] using fromLittle_lt (bs.take k) (by intro b hb; exact h b (List.mem_of_mem_take hb))
  rw [happ]
  rw [Nat.add_mul_div_left _ _ (Nat.pow_pos (by decide : 0 < 256))]
  rw [Nat.div_eq_of_lt hlt]
  omega

private theorem fromLittle_little (n k : Nat) : fromLittle (little n k) = n % 256 ^ k := by
  induction k generalizing n with
  | zero =>
      simp [little, fromLittle]
      have hh := Nat.mod_lt n (by decide : 0 < 1)
      omega
  | succ k ih =>
      have hlit : little n (k+1) = little n k ++ [(n / 256^k) % 256] := by
        unfold little
        rw [List.range_succ]
        simp only [List.map_append, List.map_cons, List.map_nil]
      let M := 256 ^ k
      have hm := Nat.mod_add_div n M
      have hdecomp : n = n % M + M * (n / M) := by omega
      have hm2 := Nat.mod_add_div (n / M) 256
      have hdecomp2 : n / M = (n / M) % 256 + 256 * ((n / M) / 256) := by omega
      have hEq : n = n % M + M * ((n / M) % 256 + 256 * ((n / M) / 256)) := by
        calc
          n = n % M + M * (n / M) := hdecomp
          _ = n % M + M * ((n / M) % 256 + 256 * ((n / M) / 256)) :=
              congrArg (fun q => n % M + M * q) hdecomp2
      have hb : n % M + M * ((n / M) % 256) < M * 256 := by
        have hx : (n / M) % 256 < 256 := Nat.mod_lt _ (by decide : 0 < 256)
        have hlt : n % M < M := Nat.mod_lt _ (Nat.pow_pos (by decide : 0 < 256))
        have hs := Nat.mul_le_mul_left M (Nat.succ_le_of_lt hx)
        simp only [Nat.succ_eq_add_one, Nat.mul_add, Nat.mul_one] at hs
        omega
      calc
        fromLittle (little n (k+1))
            = n % 256^k + 256^k * ((n / 256^k) % 256) := by
                rw [hlit, fromLittle_append, ih]
                simp only [fromLittle]
                have hlenlit : (little n k).length = k := by simp [little]
                rw [hlenlit]
                rw [Nat.mul_zero, Nat.add_zero]
        _ = n % M + M * ((n / M) % 256) := by rfl
        _ = (n % M + M * ((n / M) % 256)) % (M * 256) := (Nat.mod_eq_of_lt hb).symm
        _ = (n % M + M * ((n / M) % 256 + 256 * ((n / M) / 256))) % (M * 256) := by
              rw [Nat.mul_add]
              rw [← Nat.mul_assoc]
              rw [← Nat.add_assoc]
              rw [Nat.add_mul_mod_self_left]
        _ = n % (M * 256) := by rw [← hEq]
        _ = n % 256 ^ (k+1) := by rw [Nat.pow_succ]

private theorem window_bytes (bs : Bytes) (start L : Nat) (h : ∀ b ∈ bs, b < 256)
    (hwin : start + L ≤ bs.length) :
    fromLittle ((bs.drop start).take L) = (fromLittle bs / 256 ^ start) % 256 ^ L := by
  have htake := fromLittle_take (bs.drop start) L
    (by intro b hb; exact h b (List.mem_of_mem_drop hb))
  rw [htake]
  have happ := fromLittle_append (bs.take start) (bs.drop start)
  rw [List.take_append_drop] at happ
  have hlen : (bs.take start).length = start := by
    rw [List.length_take]
    omega
  rw [hlen] at happ
  have hlt : fromLittle (bs.take start) < 256 ^ start := by
    simpa [hlen] using fromLittle_lt (bs.take start) (by intro b hb; exact h b (List.mem_of_mem_take hb))
  rw [happ]
  rw [Nat.add_mul_div_left _ _ (Nat.pow_pos (by decide : 0 < 256))]
  rw [Nat.div_eq_of_lt hlt]
  simp

private theorem word_bound (w : Nat) : ∀ xs, (∀ x ∈ xs, x < 2^w) → word w xs < 2^(w*xs.length)
  | [], _ => by simp [word]
  | x::xs, h => by
      simp only [word, List.length_cons, Nat.mul_succ]
      have hxmod : x % 2^w < 2^w := Nat.mod_lt _ (Nat.two_pow_pos w)
      have hi := word_bound w xs (by intro y hy; exact h y (by simp [hy]))
      have hs := Nat.mul_le_mul_left (2^w) (Nat.succ_le_of_lt hi)
      rw [Nat.pow_add]
      rw [show 2 ^ (w * xs.length) * 2^w = 2^w * 2 ^ (w * xs.length) by ac_rfl]
      simp only [Nat.succ_eq_add_one, Nat.mul_add, Nat.mul_one] at hs
      omega

private theorem word_bit (w : Nat) : ∀ (xs : List Nat) (i : Nat), i < xs.length →
    (∀ x ∈ xs, x < 2^w) → word w xs / 2^(i*w) % 2^w = xs.getD i 0
  | [], i, hi, h => by simp at hi
  | x::xs, 0, hi, h => by
      simp only [word]
      rw [show 0*w = 0 by omega, Nat.pow_zero, Nat.div_one]
      rw [Nat.add_mul_mod_self_left]
      rw [Nat.mod_eq_of_lt (h x (by simp))]
      rw [Nat.mod_eq_of_lt (h x (by simp))]
      simp
  | x::xs, i+1, hi, h => by
      have hi' : i < xs.length := by simpa using hi
      have ih := word_bit w xs i hi' (by intro y hy; exact h y (by simp [hy]))
      simp only [word, List.getD_cons_succ]
      rw [Nat.succ_mul]
      rw [Nat.pow_add]
      let P := 2^(i*w)
      let B := 2^w
      have hP : 0 < P := Nat.pow_pos (by omega)
      have hB : 0 < B := Nat.pow_pos (by omega)
      have hr : x % B < B := Nat.mod_lt _ hB
      have hnum : x % 2^w + 2^w * word w xs =
          (x % B + B * (word w xs % P)) + (P * B) * (word w xs / P) := by
        calc
          x % 2^w + 2^w * word w xs = x % B + B * word w xs := by rfl
          _ = x % B + B * (word w xs % P + P * (word w xs / P)) :=
              congrArg (fun q => x % B + B * q) (Nat.mod_add_div (word w xs) P).symm
          _ = (x % B + B * (word w xs % P)) + (P * B) * (word w xs / P) := by
              rw [Nat.mul_add]
              ac_rfl
      have hsmall : x % B + B * (word w xs % P) < P * B := by
        have hlt : word w xs % P < P := Nat.mod_lt _ hP
        have hs := Nat.mul_le_mul_left B (Nat.succ_le_of_lt hlt)
        simp only [Nat.succ_eq_add_one, Nat.mul_add, Nat.mul_one] at hs
        rw [show P * B = B * P by ac_rfl]
        omega
      rw [hnum]
      rw [Nat.add_mul_div_left _ _ (Nat.mul_pos hP hB)]
      rw [Nat.div_eq_of_lt hsmall]
      simpa [P, B] using ih

private theorem bit_window (W s sh L w : Nat) (hwin : sh + w ≤ 8 * L) :
    ((W / 2^(8*s) % 2^(8*L)) / 2^sh) % 2^w = (W / 2^(8*s+sh)) % 2^w := by
  let A := W / 2^(8*s)
  let M := 8*L
  have hshM : sh ≤ M := by dsimp [M] at *; omega
  have hpow : 2^M = 2^sh * 2^(M-sh) := by
    rw [← Nat.pow_add]
    congr 1
    omega
  have hdecomp : A = A % 2^M + 2^M * (A / 2^M) := (Nat.mod_add_div A (2^M)).symm
  have hdiv : A / 2^sh = (A % 2^M) / 2^sh + 2^(M-sh) * (A / 2^M) := by
    calc
      A / 2^sh = (A % 2^M + 2^M * (A / 2^M)) / 2^sh := congrArg (fun q => q / 2^sh) hdecomp
      _ = (A % 2^M + 2^sh * (2^(M-sh) * (A / 2^M))) / 2^sh := by
            rw [hpow]
            congr 1
            ac_rfl
      _ = (A % 2^M) / 2^sh + 2^(M-sh) * (A / 2^M) :=
            Nat.add_mul_div_left _ _ (Nat.pow_pos (by omega))
  have hwsub : w ≤ M - sh := by dsimp [M] at *; omega
  have hpow2 : 2^(M-sh) = 2^w * 2^((M-sh)-w) := by
    rw [← Nat.pow_add]
    congr 1
    omega
  have hWdiv : W / 2^(8*s+sh) = A / 2^sh := by
    unfold A
    rw [Nat.pow_add]
    rw [Nat.div_div_eq_div_mul]
  change ((A % 2^(8*L)) / 2^sh) % 2^w = (W / 2^(8*s+sh)) % 2^w
  rw [hWdiv, hdiv]
  rw [hpow2]
  rw [Nat.mul_assoc]
  rw [Nat.add_mul_mod_self_left]

private theorem byteCount_bound (n : Nat) : n ≤ 8 * byteCount n := by
  unfold byteCount
  have hm := Nat.mod_add_div (n+7) 8
  have hmod : (n+7) % 8 ≤ 7 := by
    have h := Nat.mod_lt (n+7) (by decide : 0 < 8)
    omega
  omega

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  omega

private theorem byteCount_add_mul (q r : Nat) :
    byteCount (8*q + r) = q + byteCount r := by
  unfold byteCount
  calc
    (8*q + r + 7) / 8 = (r + 7 + 8*q) / 8 := by congr 1 <;> omega
    _ = (r+7)/8 + q := Nat.add_mul_div_left _ _ (by decide)
    _ = q + (r+7)/8 := by omega

private theorem pow256 (n : Nat) : 256^n = 2^(8*n) := by
  have h : (256 : Nat) = 2^8 := by decide
  simpa [h] using (Nat.pow_mul 2 8 n).symm

private theorem takeAt_pack (w : Nat) (ys : List Nat) (hall : ∀ y ∈ ys, y < 2^w)
    (i : Nat) (hi : i < ys.length) :
    takeAt (pack w ys) w i = ys.getD i 0 := by
  unfold takeAt
  dsimp only
  let b := i*w
  let st := b/8
  let sh := b%8
  let L := byteCount (sh+w)
  change (fromLittle (((pack w ys).drop st).take L)) / 2^sh % 2^w = ys.getD i 0
  have hb : b = 8*st + sh := by
    dsimp [st, sh]
    rw [Nat.div_add_mod]
  have hbyte : byteCount (b+w) = st + L := by
    rw [hb]
    rw [Nat.add_assoc]
    rw [byteCount_add_mul]
  have hwindow : sh + w ≤ 8*L := byteCount_bound _
  have hfit : st + L ≤ (pack w ys).length := by
    rw [← hbyte]
    have hlenpack : (pack w ys).length = byteCount (ys.length*w) := by simp [pack, little]
    rw [hlenpack]
    apply byteCount_mono
    have hx : i*w + w ≤ ys.length*w := by
      have hs : i+1 ≤ ys.length := Nat.succ_le_of_lt hi
      simpa [Nat.succ_mul] using Nat.mul_le_mul_right w hs
    dsimp [b]
    omega
  have hcan : ∀ y ∈ pack w ys, y < 256 := by
    unfold pack
    exact little_bytes _ _
  have hwin := window_bytes (pack w ys) st L hcan hfit
  have hwl : word w ys < 2^(8*byteCount (ys.length*w)) := by
    have hbnd := word_bound w ys hall
    have hle : w*ys.length ≤ 8*byteCount (ys.length*w) := by
      rw [Nat.mul_comm w ys.length]
      exact byteCount_bound _
    exact Nat.lt_of_lt_of_le hbnd (Nat.pow_le_pow_right (by omega) hle)
  have hfrom : fromLittle (pack w ys) = word w ys := by
    unfold pack
    rw [fromLittle_little]
    rw [pow256]
    rw [Nat.mod_eq_of_lt hwl]
  rw [hwin, hfrom]
  rw [pow256 st, pow256 L]
  rw [bit_window]
  · rw [← hb]
    rw [word_bit w ys i hi hall]
  · exact hwindow

private theorem zipIdx_cons (x : Nat) (xs : List Nat) (n : Nat) :
    (x::xs).zipIdx n = (x,n)::(xs.zipIdx (n+1)) := by
  simp [List.zipIdx]

private theorem zipIdx_index_ge (l : List Nat) (n : Nat) : ∀ p ∈ l.zipIdx n, n ≤ p.2 := by
  induction l generalizing n with
  | nil => simp [List.zipIdx]
  | cons x xs ih =>
      intro p hp
      rw [zipIdx_cons] at hp
      simp only [List.mem_cons] at hp
      rcases hp with hp | hp
      · subst p
        omega
      · exact Nat.le_trans (by omega : n ≤ n+1) (ih (n+1) p hp)

private theorem find?_eq_none_of_ne (ps : List (Nat × Nat)) (i : Nat)
    (h : ∀ p ∈ ps, p.1 ≠ i) : ps.find? (fun e => e.1 == i) = none := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      rw [List.find?_cons]
      by_cases hb : (p.1 == i) = true
      · have heq : p.1 = i := beq_iff_eq.mp hb
        exact False.elim ((h p (by simp)) heq)
      · have h' := ih (by intro q hq; exact h q (by simp [hq]))
        simpa [hb] using h'

private theorem high_zip (w n : Nat) : ∀ (xs : List Nat) (i : Nat),
    high (((xs.zipIdx n).filter (fun (v,_) => 2^w ≤ v)).map (fun (v,j) => (j, v/2^w))) (n+i) =
      xs.getD i 0 / 2^w
  | [], i => by simp [high, List.zipIdx]
  | x::xs, 0 => by
      rw [zipIdx_cons]
      by_cases hx : 2^w ≤ x
      · simp [hx, high]
      · have hxdiv : x / 2^w = 0 := Nat.div_eq_of_lt (by omega)
        have hne : ∀ p ∈ ((xs.zipIdx (n+1)).filter (fun (v,_) => 2^w ≤ v)).map (fun (v,j) => (j, v/2^w)), p.1 ≠ n := by
          intro p hp
          obtain ⟨q, hq, rfl⟩ := List.mem_map.mp hp
          have hqin := (List.mem_filter.mp hq).1
          have hge := zipIdx_index_ge xs (n+1) q hqin
          change q.snd ≠ n
          omega
        have hfind := find?_eq_none_of_ne _ _ hne
        simp [hx, hxdiv, high, hfind]
  | x::xs, i+1 => by
      rw [zipIdx_cons]
      by_cases hx : 2^w ≤ x
      · simp [hx]
        have hh := high_zip w (n+1) xs i
        rw [show n+1+i = n+(1+i) by omega] at hh
        rw [show n + (i+1) = n + (1+i) by omega]
        simpa [high] using hh
      · simp [hx]
        have hh := high_zip w (n+1) xs i
        rw [show n+1+i = n+(1+i) by omega] at hh
        rw [show n + (i+1) = n + (1+i) by omega]
        simpa [high] using hh

private theorem high_patches (w : Nat) (xs : List Nat) (i : Nat) :
    high (patches w xs) i = xs.getD i 0 / 2^w := by
  unfold patches
  simpa using high_zip w 0 xs i

theorem sound_PatchedInverse : PatchedInverse.Goal := by
  intro w xs
  apply List.ext_getElem
  · simp
  · intro i hi1 hi2
    have hi : i < xs.length := by simpa using hi1
    have ht := takeAt_pack w (xs.map (fun x => x % 2^w)) (by
        intro y hy
        rw [List.mem_map] at hy
        obtain ⟨x, hx, rfl⟩ := hy
        exact Nat.mod_lt _ (Nat.two_pow_pos w)) i (by simpa using hi)
    have ht' : takeAt (pack w (xs.map (fun x => x % 2^w))) w i = xs[i] % 2^w := by
      rw [ht]
      simp [hi]
    simp [patched, lows, ht', high_patches]
    have hget : xs[i]?.getD 0 = xs[i] := by simp [hi]
    rw [hget]
    have h := Nat.mod_add_div (xs[i]) (2^w)
    omega


/-! ## Width, byte charge and ULEB length helpers -/

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length*w) := by
  simp [pack, little]

private theorem width_spec (n : Nat) (hn : n ≠ 0) :
    2 ^ (width n - 1) ≤ n ∧ n < 2 ^ width n := by
  unfold width
  simp [hn]
  have h := (Nat.log2_eq_iff hn).mp rfl
  constructor <;> omega

private theorem log2_mono {a b : Nat} (h : a ≤ b) : a.log2 ≤ b.log2 := by
  by_cases ha : a = 0
  · subst a
    simp
  · have hb : b ≠ 0 := by omega
    by_cases hle : a.log2 ≤ b.log2
    · exact hle
    · exfalso
      have hlt' : b.log2 < a.log2 := by omega
      have hb_lt : b < 2 ^ a.log2 := (Nat.log2_lt hb).mp hlt'
      have ha_le : 2 ^ a.log2 ≤ a := (Nat.log2_eq_iff ha).mp rfl |>.1
      omega

private theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  by_cases ha : a = 0
  · subst a
    simp [width]
  · simp [width, ha]
    by_cases hb : b = 0
    · subst b
      omega
    · simp [hb]
      have := log2_mono h
      omega

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · omega
  · have hw := width_mono h
    apply Nat.le_trans (Nat.div_le_div_right (by omega : width a + 6 ≤ width b + 6))
    exact Nat.le_max_right _ _

private theorem ubytes_pos (n : Nat) : 0 < ubytes n := by
  unfold ubytes
  omega

private theorem ubytes_le_iff (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128^k := by
  constructor
  · intro h
    by_cases hn : n = 0
    · subst n
      exact Nat.pow_pos (by decide : 0 < 128)
    · have hw : n < 2 ^ width n := (width_spec n hn).2
      have h1 : (width n + 6) / 7 ≤ k := Nat.le_trans (Nat.le_max_right 1 _) h
      have h3 : width n ≤ 7*k := by omega
      calc
        n < 2 ^ width n := hw
        _ ≤ 2 ^ (7*k) := Nat.pow_le_pow_right (by omega) h3
        _ = (2^7)^k := by rw [← Nat.pow_mul]
        _ ≤ 128^k := Nat.pow_le_pow_left (by omega) _
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · rw [Nat.div_le_iff_le_mul_add_pred (by omega)]
      by_cases hn : n = 0
      · simp [width, hn]
      · have h128 : (128:Nat)^k = 2^(7*k) := by
          have : (128:Nat) = 2^7 := by decide
          rw [this, ← Nat.pow_mul]
        have hlt : n < 2^(7*k) := by omega
        unfold width
        simp [hn]
        have hlog : Nat.log2 n < 7*k := (Nat.log2_lt hn).mpr hlt
        omega

private theorem uleb_len_le_pow (n k : Nat) (hk : 0 < k) (h : n < 128^k) :
    (uleb n).length ≤ k := by
  induction k generalizing n with
  | zero => omega
  | succ k ih =>
      by_cases hn : n < 128
      · rw [uleb]
        simp only [hn, ite_true, List.length_singleton]
        omega
      · by_cases hk0 : k = 0
        · subst k
          simp at h
          omega
        · rw [uleb]
          simp only [hn, ite_false, List.length_cons]
          have hkpos : 0 < k := Nat.pos_of_ne_zero hk0
          have hdiv : n / 128 < 128^k := by
            apply (Nat.div_lt_iff_lt_mul (by omega : 0 < 128)).mpr
            rw [Nat.pow_succ] at h
            simpa [Nat.mul_comm] using h
          have ih' := ih (n/128) hkpos hdiv
          omega

private theorem uleb_len_bound (n : Nat) : n < 128 ^ (uleb n).length := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n < 128
      · simp [uleb, hn]
      · rw [uleb]
        simp only [hn, ite_false, List.length_cons]
        have hlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih (n/128) hlt
        have hstep : n / 128 + 1 ≤ 128 ^ (uleb (n/128)).length := Nat.succ_le_of_lt ih'
        have hmul := Nat.mul_le_mul_left 128 hstep
        rw [Nat.mul_add] at hmul
        simp only [Nat.mul_one] at hmul
        have hn' : n < 128 * (n/128 + 1) := by
          have hm := Nat.mod_add_div n 128
          omega
        rw [Nat.pow_succ]
        omega

private theorem uleb_length (n : Nat) : (uleb n).length = ubytes n := by
  apply Nat.le_antisymm
  · exact uleb_len_le_pow n (ubytes n) (ubytes_pos n) ((ubytes_le_iff n (ubytes n) (ubytes_pos n)).mp (by omega))
  · have hpos : 0 < (uleb n).length := by
      by_cases hn : n < 128
      · rw [uleb]
        simp [hn]
      · rw [uleb]
        simp [hn]
    exact (ubytes_le_iff n (uleb n).length hpos).mpr (uleb_len_bound n)

private theorem patchStream_length (ps : List (Nat × Nat)) :
    (patchStream ps).length = (ps.map (fun (i,h) => ubytes i + ubytes h)).sum := by
  unfold patchStream
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.flatMap_cons, List.length_append, List.map_cons, List.sum_cons]
      rw [ih]
      simp [uleb_length]

theorem sound_ExactQuote : ExactQuote.Goal := by
  intro base w xs
  unfold body charge
  simp only [List.length_append, List.length_cons]
  rw [patchStream_length]
  simp [pack_length, uleb_length, sbytes, lows]
  rw [Nat.mul_comm w xs.length]

theorem sound_HighStreamBudget : HighStreamBudget.Goal := by
  intro w xs
  simp [pack_length]
  rw [Nat.mul_comm (width (peak ((patches w xs).map Prod.snd))) (patches w xs).length]

-- Concrete counterexample: xs = 65535 :: replicate 127 0, base = 0, w = 0.
-- width (peak xs) = 16, so charge 0 16 xs = 262, while charge 0 0 xs = 10
-- (one patch at ordinal 0 with high part 65535).  Choosing the maximum value's
-- width forces 256 payload bytes; storing the single outlier as a patch is
-- strictly cheaper.  Hence width (peak xs) is not a universally minimal width.
theorem refutation_FewestExceptions : ¬ FewestExceptions.Goal := by
  intro h
  have hh := h 0 (65535 :: List.replicate 127 0) 0
  have hnot : ¬ charge 0 (width (peak (65535 :: List.replicate 127 0))) (65535 :: List.replicate 127 0) ≤
      charge 0 0 (65535 :: List.replicate 127 0) := by native_decide
  exact hnot hh


/-! ## Rice charge, stride, signed FOR, envelope -/

private theorem rice_length (k v : Nat) : (rice k v).length = v / 2^k + 1 + k := by
  simp [rice]
  omega

private theorem rice_flat_length (k : Nat) (xs : List Nat) :
    (xs.flatMap (rice k)).length = xs.length*(k+1) + (xs.map (fun v => v/2^k)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.flatMap_cons, List.length_append, rice_length, List.map_cons,
        List.sum_cons, List.length_cons]
      rw [ih]
      have hmul : (xs.length+1)*(k+1) = xs.length*(k+1) + (k+1) := by rw [Nat.succ_mul]
      rw [hmul]
      omega

theorem sound_RiceCharge : RiceCharge.Goal := by
  intro k xs
  exact rice_flat_length k xs

private theorem foldl_gcd_dvd_acc (acc : Nat) : ∀ xs : List Nat, xs.foldl Nat.gcd acc ∣ acc
  | [] => by change acc ∣ acc; exact ⟨1, by omega⟩
  | a::xs => by
      change xs.foldl Nat.gcd (Nat.gcd acc a) ∣ acc
      exact Nat.dvd_trans (foldl_gcd_dvd_acc (Nat.gcd acc a) xs) (Nat.gcd_dvd_left acc a)

private theorem foldl_gcd_dvd_elem (acc : Nat) : ∀ (xs : List Nat) (x : Nat), x ∈ xs →
    xs.foldl Nat.gcd acc ∣ x
  | [], x, hx => by simp at hx
  | a::xs, x, hx => by
      change xs.foldl Nat.gcd (Nat.gcd acc a) ∣ x
      rcases List.mem_cons.mp hx with h | h
      · subst x
        exact Nat.dvd_trans (foldl_gcd_dvd_acc (Nat.gcd acc a) xs) (Nat.gcd_dvd_right acc a)
      · exact foldl_gcd_dvd_elem (Nat.gcd acc a) xs x h

private theorem divisor_dvd (xs : List Nat) (x : Nat) (hx : x ∈ xs) : divisor xs ∣ x := by
  unfold divisor
  exact foldl_gcd_dvd_elem 0 xs x hx

theorem sound_StrideFactor : StrideFactor.Goal := by
  intro xs
  apply List.ext_getElem
  · simp
  · intro i hi1 hi2
    have hi : i < xs.length := by simpa using hi1
    have hd : divisor xs ∣ xs[i] := divisor_dvd xs (xs[i]) (List.getElem_mem hi)
    rw [List.getElem_map]
    rw [Nat.div_mul_cancel hd]

private theorem min_le_of_mem (xs : List Int) (x : Int) (hx : x ∈ xs) : xs.min?.getD 0 ≤ x := by
  cases hm : xs.min? with
  | none =>
      have hnil : xs = [] := List.min?_eq_none_iff.mp hm
      simp [hnil] at hx
  | some m =>
      have hm' := (List.min?_eq_some_iff.mp hm)
      simp
      exact hm'.2 x hx

theorem sound_SignedFor : SignedFor.Goal := by
  intro xs
  apply List.ext_getElem
  · simp
  · intro i hi1 hi2
    have hi : i < xs.length := by simpa using hi1
    let lo := xs.min?.getD 0
    have hle : lo ≤ xs[i] := min_le_of_mem xs (xs[i]) (List.getElem_mem hi)
    rw [List.getElem_map]
    rw [Int.toNat_of_nonneg (by omega : 0 ≤ xs[i] - lo)]
    omega

private theorem byteCount_mul_8 (n : Nat) : byteCount (8*n) = n := by
  unfold byteCount
  calc
    (8*n+7)/8 = (7+8*n)/8 := by congr 1 <;> omega
    _ = 7/8 + n := Nat.add_mul_div_left _ _ (by decide)
    _ = n := by have h : 7/8 = 0 := Nat.div_eq_of_lt (by decide); simp [h]

private theorem width_le_bits (n bits : Nat) (h : n < 2^bits) : width n ≤ bits := by
  by_cases hn : n = 0
  · simp [width, hn]
  · unfold width
    simp [hn]
    have hlog := (Nat.log2_lt hn).mpr h
    omega

theorem sound_ForEnvelope : ForEnvelope.Goal := by
  intro xs hlt
  let p := peak xs
  have hbnd : width p ≤ 8 * tlWidth p := by
    unfold tlWidth
    by_cases h0 : p == 0
    · have hp0 : p = 0 := beq_iff_eq.mp h0
      simp [hp0, width]
    · by_cases h256 : p < 256
      · simp [h0, h256]
        have hw := width_le_bits p 8 (by omega)
        omega
      · by_cases h65536 : p < 65536
        · simp [h0, h256, h65536]
          have hw := width_le_bits p 16 (by omega)
          omega
        · by_cases h32 : p < 2^32
          · simp [h0, h256, h65536, h32]
            have hw := width_le_bits p 32 (by omega)
            omega
          · simp [h0, h256, h65536, h32]
            have hw := width_le_bits p 64 (by omega)
            omega
  have hbc := byteCount_mono (Nat.mul_le_mul_right xs.length hbnd)
  rw [show 8 * tlWidth p * xs.length = 8 * (tlWidth p * xs.length) by ac_rfl] at hbc
  rw [byteCount_mul_8] at hbc
  unfold packedSize
  rw [Nat.mul_comm xs.length (width (peak xs))]
  simpa [p] using hbc


/-! ## Exception counts and gap ordinals -/

private theorem filter_zipIdx_length (w n : Nat) (xs : List Nat) :
    ((xs.zipIdx n).filter (fun (v,_) => 2^w ≤ v)).length = xs.countP (fun v => 2^w ≤ v) := by
  induction xs generalizing n with
  | nil => simp [List.zipIdx]
  | cons x xs ih =>
      rw [zipIdx_cons]
      by_cases hx : 2^w ≤ x
      · simp [hx, ih]
      · simp [hx, ih]

private theorem patches_length (w : Nat) (xs : List Nat) :
    (patches w xs).length = xs.countP (fun v => 2^w ≤ v) := by
  unfold patches
  simpa using filter_zipIdx_length w 0 xs

private theorem countP_le_of_imp (xs : List Nat) (p q : Nat → Bool)
    (himp : ∀ v, q v = true → p v = true) : xs.countP q ≤ xs.countP p := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      by_cases hq : q a = true
      · have hp : p a = true := himp a hq
        simp [hq, hp]
        omega
      · simp [List.countP_cons, hq]
        omega

private theorem countP_ge_sub (xs : List Nat) (p q : Nat → Bool)
    (himp : ∀ v, q v = true → p v = true) :
    xs.countP p - xs.countP q = xs.countP (fun v => p v && !q v) := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      by_cases hp : p a = true
      · by_cases hq : q a = true
        · simp [hp, hq, ih]
        · have hle := countP_le_of_imp as p q himp
          have hqf : q a = false := Bool.eq_false_of_ne_true hq
          simp [hp, hqf]
          omega
      · have hqf : q a = false := by
          by_cases hq : q a = true
          · exact False.elim (hp (himp a hq))
          · exact Bool.eq_false_of_ne_true hq
        simp [hp, hqf, ih]

theorem sound_ExceptionDrop : ExceptionDrop.Goal := by
  intro w xs
  rw [patches_length, patches_length]
  have hmono : ∀ v, decide (2^(w+1) ≤ v) = true → decide (2^w ≤ v) = true := by
    intro v hv
    by_cases h2 : 2^(w+1) ≤ v
    · have h1 : 2^w ≤ v := by
        rw [Nat.pow_succ] at h2
        omega
      simp [h1]
    · simp [h2] at hv
  rw [countP_ge_sub xs (fun v => 2^w ≤ v) (fun v => 2^(w+1) ≤ v) hmono]
  congr 1
  funext v
  by_cases h1 : 2^w ≤ v <;> by_cases h2 : 2^(w+1) ≤ v <;> simp [h1, h2]
  · rw [Nat.pow_succ] at h2 ⊢
    omega

private theorem filter_zipIdx_indices_ge (w n : Nat) (xs : List Nat) :
    ∀ i ∈ (((xs.zipIdx n).filter (fun (v,_) => 2^w ≤ v)).map (fun p => p.2)), n ≤ i := by
  intro i hi
  obtain ⟨p, hp, hpi⟩ := List.mem_map.mp hi
  subst hpi
  exact zipIdx_index_ge xs n p (List.mem_filter.mp hp).1

private theorem patch_indices_strict (w n : Nat) : ∀ xs : List Nat,
    (List.Pairwise (fun a b => a < b))
      (((xs.zipIdx n).filter (fun (v,_) => 2^w ≤ v)).map (fun p => p.2))
  | [] => by simp [List.zipIdx]
  | x::xs => by
      rw [zipIdx_cons]
      by_cases hx : 2^w ≤ x
      · have hxb : decide (2^w ≤ x) = true := decide_eq_true_eq.mpr hx
        simpa [hxb] using
          (List.Pairwise.cons (fun i hi => by
            have hge := filter_zipIdx_indices_ge w (n+1) xs i hi
            omega) (patch_indices_strict w (n+1) xs))
      · have hxb : decide (2^w ≤ x) = false := Bool.eq_false_of_ne_true (fun h => hx (decide_eq_true_eq.mp h))
        simpa [hxb] using patch_indices_strict w (n+1) xs

private theorem undo_gaps_of_gaps (p : Nat) : ∀ (ids : List Nat),
    (∀ i ∈ ids, p ≤ i) → (List.Pairwise (fun a b => a < b) ids) →
    undoGaps p (gaps p ids) = ids
  | [], h, hpair => rfl
  | i::is, h, hpair => by
      have hi : p ≤ i := h i (by simp)
      have hpair' := (List.pairwise_cons.mp hpair).2
      have htail : ∀ j ∈ is, i + 1 ≤ j := by
        intro j hj
        have hij : i < j := (List.pairwise_cons.mp hpair).1 j hj
        omega
      simp only [gaps, undoGaps]
      rw [show p + (i - p) = i by omega]
      rw [undo_gaps_of_gaps (i+1) is htail hpair']

private theorem patch_indices_ge (w : Nat) (xs : List Nat) :
    ∀ i ∈ (patches w xs).map Prod.fst, 0 ≤ i := by
  intro i hi
  omega

private theorem patch_indices_strict' (w : Nat) (xs : List Nat) :
    List.Pairwise (fun a b => a < b) ((patches w xs).map Prod.fst) := by
  unfold patches
  simpa [List.map_map, Function.comp_def] using patch_indices_strict w 0 xs

theorem sound_GapOrdinals : GapOrdinals.Goal := by
  intro w xs
  dsimp only
  exact undo_gaps_of_gaps 0 _ (patch_indices_ge w xs) (patch_indices_strict' w xs)

private theorem gaps_stream_le (p : Nat) : ∀ (ids : List Nat),
    (∀ i ∈ ids, p ≤ i) → (List.Pairwise (fun a b => a < b) ids) →
    streamBytes (gaps p ids) ≤ streamBytes ids
  | [], h, hpair => by simp [gaps, streamBytes]
  | i::is, h, hpair => by
      have hi : p ≤ i := h i (by simp)
      have hpair' := (List.pairwise_cons.mp hpair).2
      have htail : ∀ j ∈ is, i + 1 ≤ j := by
        intro j hj
        have hij : i < j := (List.pairwise_cons.mp hpair).1 j hj
        omega
      change ubytes (i-p) + streamBytes (gaps (i+1) is) ≤ ubytes i + streamBytes is
      have hfirst : ubytes (i-p) ≤ ubytes i := ubytes_mono (by omega)
      have hrest := gaps_stream_le (i+1) is htail hpair'
      omega

theorem sound_GapDominance : GapDominance.Goal := by
  intro w xs
  dsimp only
  exact gaps_stream_le 0 _ (patch_indices_ge w xs) (patch_indices_strict' w xs)


/-! ## Finite width search and GCD budget -/

private theorem peak_le_of_mem (xs : List Nat) (x : Nat) (hx : x ∈ xs) : x ≤ peak xs := by
  cases hm : xs.max? with
  | none =>
      have hnil : xs = [] := List.max?_eq_none_iff.mp hm
      simp [hnil] at hx
  | some m =>
      simpa [peak, hm] using (List.max?_eq_some_iff.mp hm).2 x hx

private theorem peak_lt_width_pow (xs : List Nat) : peak xs < 2 ^ width (peak xs) := by
  by_cases h : peak xs = 0
  · simp [h, width]
  · exact (width_spec (peak xs) h).2

private theorem patches_empty_above (xs : List Nat) (w : Nat)
    (hw : width (peak xs) ≤ w) : patches w xs = [] := by
  unfold patches
  apply List.eq_nil_of_length_eq_zero
  rw [List.length_map]
  rw [filter_zipIdx_length]
  apply List.countP_eq_zero.mpr
  intro v hv
  have hvle : v ≤ peak xs := peak_le_of_mem xs v hv
  have hlt : peak xs < 2^w := Nat.lt_of_lt_of_le (peak_lt_width_pow xs)
    (Nat.pow_le_pow_right (by omega) hw)
  simp [show ¬ 2^w ≤ v by omega]

private theorem charge_at_width_le (base : Int) (xs : List Nat) (w : Nat)
    (hw : width (peak xs) ≤ w) : charge base (width (peak xs)) xs ≤ charge base w xs := by
  have h1 := patches_empty_above xs w hw
  have h2 := patches_empty_above xs (width (peak xs)) (by omega)
  unfold charge
  rw [h1, h2]
  simp
  have hu := ubytes_mono hw
  have hb := byteCount_mono (Nat.mul_le_mul_right xs.length hw)
  omega

private theorem min?_getD_le_of_mem (l : List Nat) (k : Nat) (hk : k ∈ l) :
    l.min?.getD 0 ≤ k := by
  cases hm : l.min? with
  | none =>
      have hnil : l = [] := List.min?_eq_none_iff.mp hm
      simp [hnil] at hk
  | some m =>
      simpa [hm] using (List.min?_eq_some_iff.mp hm).2 k hk

theorem sound_FiniteSearch : FiniteSearch.Goal := by
  intro base xs w
  dsimp only
  let W := width (peak xs)
  let l := (List.range (W+1)).map (fun k => charge base k xs)
  have hWmem : W ∈ List.range (W+1) := List.mem_range.mpr (by omega)
  have hminW : l.min?.getD 0 ≤ charge base W xs :=
    min?_getD_le_of_mem l (charge base W xs) (List.mem_map.mpr ⟨W, hWmem, rfl⟩)
  by_cases hw : w ≤ W
  · have hmin := min?_getD_le_of_mem l (charge base w xs)
      (List.mem_map.mpr ⟨w, List.mem_range.mpr (by omega), rfl⟩)
    exact hmin
  · have hc := charge_at_width_le base xs w (by omega : W ≤ w)
    exact Nat.le_trans hminW hc

private theorem peak_map_div (xs : List Nat) (d : Nat) :
    peak (xs.map (fun x => x / d)) = peak xs / d := by
  cases hm : xs.max? with
  | none =>
      have hnil : xs = [] := List.max?_eq_none_iff.mp hm
      simp [hnil, peak]
  | some p =>
      have hp := List.max?_eq_some_iff.mp hm
      have hmap : (xs.map (fun x => x / d)).max? = some (p / d) := by
        apply List.max?_eq_some_iff.mpr
        constructor
        · exact List.mem_map.mpr ⟨p, hp.1, rfl⟩
        · intro y hy
          obtain ⟨z, hz, rfl⟩ := List.mem_map.mp hy
          exact Nat.div_le_div_right (hp.2 z hz)
      simp [peak, hm, hmap]

theorem sound_GcdBudget : GcdBudget.Goal := by
  intro xs
  dsimp only
  unfold packedSize
  rw [List.length_map]
  rw [peak_map_div]

/-! ## Rice parameter steps -/

private theorem sub_add_sub (a b c d : Nat) (hb : b ≤ a) (hd : d ≤ c) :
    (a + c) - (b + d) = (a - b) + (c - d) := by omega

private theorem sum_div_le (k : Nat) (xs : List Nat) :
    (xs.map (fun x => x/2^(k+1))).sum ≤ (xs.map (fun x => x/2^k)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hle : x/2^(k+1) ≤ x/2^k := by
        rw [Nat.pow_succ]
        apply Nat.div_le_div_left
        · omega
        · exact Nat.two_pow_pos k
      simp only [List.map_cons, List.sum_cons]
      omega

private theorem sum_div_diff (k : Nat) (xs : List Nat) :
    (xs.map (fun x => x/2^k)).sum - (xs.map (fun x => x/2^(k+1))).sum =
      (xs.map (fun x => x/2^k - x/2^(k+1))).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hle : x/2^(k+1) ≤ x/2^k := by
        rw [Nat.pow_succ]
        apply Nat.div_le_div_left
        · omega
        · exact Nat.two_pow_pos k
      have hleS := sum_div_le k xs
      simp only [List.map_cons, List.sum_cons]
      rw [sub_add_sub _ _ _ _ hle hleS]
      rw [ih]

private theorem rice_step_iff (k : Nat) (xs : List Nat) :
    ((xs.flatMap (rice (k+1))).length < (xs.flatMap (rice k)).length ↔
      xs.length < (xs.map (fun x => x/2^k - x/2^(k+1))).sum) := by
  rw [rice_flat_length, rice_flat_length]
  have hsum := sum_div_diff k xs
  have hle := sum_div_le k xs
  have hmul : xs.length*(k+2) = xs.length*(k+1)+xs.length := by
    rw [show k+2 = (k+1)+1 by omega, Nat.mul_succ]
  rw [hmul]
  omega

theorem sound_RiceStep : RiceStep.Goal := by
  exact rice_step_iff

private theorem div2_div4 (A : Nat) : A/2 - A/4 ≤ A - A/2 := by
  have hm := Nat.mod_add_div A 4
  have hr : A % 4 < 4 := Nat.mod_lt _ (by decide : 0 < 4)
  have hA : A = 4*(A/4) + A%4 := by omega
  have hdiv2 : A/2 = 2*(A/4) + (A%4)/2 := by
    rw [hA]
    rw [show 4*(A/4) + A%4 = A%4 + 2*(2*(A/4)) by omega]
    rw [Nat.add_mul_div_left _ _ (by decide : 0 < 2)]
    omega
  rw [hdiv2]
  omega

private theorem rice_step_elem (k x : Nat) :
    x/2^(k+1) - x/2^(k+2) ≤ x/2^k - x/2^(k+1) := by
  have h1 : x/2^(k+1) = (x/2^k)/2 := by
    rw [Nat.div_div_eq_div_mul, Nat.pow_succ]
  have h2 : x/2^(k+2) = (x/2^k)/4 := by
    rw [Nat.div_div_eq_div_mul, Nat.pow_add]
  rw [h1, h2]
  exact div2_div4 (x/2^k)

private theorem rice_step_sum_mono (k : Nat) (xs : List Nat) :
    (xs.map (fun x => x/2^(k+1) - x/2^(k+2))).sum ≤
      (xs.map (fun x => x/2^k - x/2^(k+1))).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have h := rice_step_elem k x
      omega

private theorem rice_step_sum_mono_ge (k d : Nat) (xs : List Nat) :
    (xs.map (fun x => x/2^(k+d) - x/2^(k+d+1))).sum ≤
      (xs.map (fun x => x/2^k - x/2^(k+1))).sum := by
  induction d with
  | zero => simp
  | succ d ih =>
      have hmid := rice_step_sum_mono (k+d) xs
      exact Nat.le_trans hmid ih

private theorem rice_len_mono_at (k : Nat) (xs : List Nat)
    (hstep : (xs.map (fun x => x/2^k - x/2^(k+1))).sum ≤ xs.length) :
    (xs.flatMap (rice k)).length ≤ (xs.flatMap (rice (k+1))).length := by
  rw [rice_flat_length, rice_flat_length]
  have hsum := sum_div_diff k xs
  have hmul : xs.length*(k+2) = xs.length*(k+1)+xs.length := by
    rw [show k+2 = (k+1)+1 by omega, Nat.mul_succ]
  rw [hmul]
  omega

private theorem unimodal_aux (k d : Nat) (xs : List Nat)
    (hstep0 : (xs.map (fun x => x/2^k - x/2^(k+1))).sum ≤ xs.length) :
    (xs.flatMap (rice k)).length ≤ (xs.flatMap (rice (k+d))).length := by
  induction d with
  | zero => simp
  | succ d ih =>
      have hmid := rice_step_sum_mono_ge k d xs
      have hstep : (xs.map (fun x => x/2^(k+d) - x/2^(k+d+1))).sum ≤ xs.length :=
        Nat.le_trans hmid hstep0
      have hmono := rice_len_mono_at (k+d) xs hstep
      rw [show k + Nat.succ d = k+d+1 by omega]
      exact Nat.le_trans ih hmono

theorem sound_RiceUnimodal : RiceUnimodal.Goal := by
  intro k xs hstart j hj
  have hstep0 : (xs.map (fun x => x/2^k - x/2^(k+1))).sum ≤ xs.length := by
    have hnot : ¬ (xs.flatMap (rice (k+1))).length < (xs.flatMap (rice k)).length := by omega
    have hnlt : ¬ xs.length < (xs.map (fun x => x/2^k - x/2^(k+1))).sum := by
      intro hn
      apply hnot
      exact (rice_step_iff k xs).mpr hn
    omega
  have h := unimodal_aux k (j-k) xs hstep0
  rwa [show k+(j-k) = j by omega] at h


/-! ## Two stride segments -/

private theorem foldl_gcd_dvd_of (d acc : Nat) (xs : List Nat) (hacc : d ∣ acc)
    (hxs : ∀ x ∈ xs, d ∣ x) : d ∣ xs.foldl Nat.gcd acc := by
  induction xs generalizing acc with
  | nil => exact hacc
  | cons x xs ih =>
      apply ih (Nat.gcd acc x)
      · exact Nat.dvd_gcd hacc (hxs x (by simp))
      · intro y hy
        exact hxs y (by simp [hy])

private theorem peak_cons (x : Nat) (xs : List Nat) : peak (x::xs) = max x (peak xs) := by
  unfold peak
  rw [List.max?_cons]
  cases h : xs.max? with
  | none => simp
  | some m => simp

private theorem peak_map_le_aux (f g : Nat → Nat) (hfg : ∀ x, f x ≤ g x) :
    ∀ xs : List Nat, peak (xs.map f) ≤ peak (xs.map g)
  | [] => by simp [peak]
  | x::xs => by
      rw [List.map_cons, List.map_cons, peak_cons, peak_cons]
      exact Nat.max_le.mpr ⟨Nat.le_trans (hfg x) (Nat.le_max_left _ _), Nat.le_trans (peak_map_le_aux f g hfg xs) (Nat.le_max_right _ _)⟩

private theorem div_le_div_of_dvd (d da x : Nat) (hda : 0 < da) (hdvd : da ∣ d) :
    x / d ≤ x / da := by
  by_cases hd0 : d = 0
  · subst d
    simp
  · have hle : da ≤ d := Nat.le_of_dvd (by omega) hdvd
    exact Nat.div_le_div_left hle hda

private theorem packedSize_map_div_le (xs : List Nat) (d da : Nat)
    (hda : 0 < da) (hdvd : da ∣ d) :
    packedSize (xs.map (fun x => x/d)) ≤ packedSize (xs.map (fun x => x/da)) := by
  have hp : peak (xs.map (fun x => x/d)) ≤ peak (xs.map (fun x => x/da)) :=
    peak_map_le_aux _ _ (fun x => div_le_div_of_dvd d da x hda hdvd) xs
  unfold packedSize
  rw [List.length_map, List.length_map]
  exact byteCount_mono (Nat.mul_le_mul_left xs.length (width_mono hp))

theorem sound_TwoStrideSegments : TwoStrideSegments.Goal := by
  intro a b da db hda hdb hdiva hdivb
  have hdaD : da ∣ divisor a := by
    unfold divisor
    exact foldl_gcd_dvd_of da 0 a ⟨0, by omega⟩ hdiva
  have hdbD : db ∣ divisor b := by
    unfold divisor
    exact foldl_gcd_dvd_of db 0 b ⟨0, by omega⟩ hdivb
  have h1 := packedSize_map_div_le a (divisor a) da hda hdaD
  have h2 := packedSize_map_div_le b (divisor b) db hdb hdbD
  omega

end TinylambOpt.ResearchProofs.Compression.IntegerFrames
