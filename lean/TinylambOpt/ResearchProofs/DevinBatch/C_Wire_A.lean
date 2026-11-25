import TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.C_Wire_A

open TinylambOpt.ResearchGoals.Compression.Wire

private theorem two_pow_pos (w : Nat) : 0 < 2 ^ w :=
  (Nat.pow_pos_iff).mpr (Or.inl (by decide))
private theorem pow256 (k : Nat) : 256 ^ k = 2 ^ (8 * k) := by
  rw [show (256 : Nat) = 2 ^ 8 from rfl, ← Nat.pow_mul]
private theorem byteCount_mul8 (n : Nat) : n ≤ 8 * byteCount n := by
  show n ≤ 8 * ((n + 7) / 8); omega

private theorem byteCount_add_le (a b : Nat) :
    byteCount a + byteCount b ≤ byteCount (a + b) + 1 := by
  show (a + 7) / 8 + (b + 7) / 8 ≤ (a + b + 7) / 8 + 1
  omega
private theorem fromLittle_append (xs ys : Bytes) :
    fromLittle (xs ++ ys) = fromLittle xs + 256 ^ xs.length * fromLittle ys := by
  induction xs with
  | nil => simp [fromLittle]
  | cons x xs ih =>
    simp only [List.cons_append, fromLittle, List.length_cons]
    rw [ih]
    have hpow : 256 ^ (xs.length + 1) = 256 * 256 ^ xs.length := Nat.pow_add_one'
    rw [hpow, Nat.mul_add, ← Nat.mul_assoc]
    ac_rfl
private theorem fromLittle_lt {bs : Bytes} (h : ∀ b ∈ bs, b < 256) :
    fromLittle bs < 256 ^ bs.length := by
  induction bs with
  | nil => simp [fromLittle]
  | cons b bs ih =>
    have hb : b < 256 := h b (List.mem_cons_self)
    have hbs : ∀ c ∈ bs, c < 256 := fun c hc => h c (List.mem_cons_of_mem _ hc)
    have ih' := ih hbs
    show b + 256 * fromLittle bs < 256 ^ (bs.length + 1)
    calc b + 256 * fromLittle bs
        < 256 * fromLittle bs + 256 := by omega
      _ = 256 * (fromLittle bs + 1) := by rw [Nat.mul_add, Nat.mul_one]
      _ ≤ 256 * 256 ^ bs.length := Nat.mul_le_mul_left _ (by omega)
      _ = 256 ^ (bs.length + 1) := (Nat.pow_add_one').symm
private theorem fromLittle_little (n k : Nat) :
    fromLittle (little n k) = n % 256 ^ k := by
  induction k with
  | zero => simp [little, fromLittle, Nat.mod_one]
  | succ k ih =>
    have hsplit : little n (k + 1) = little n k ++ [(n / 256 ^ k) % 256] := by
      simp [little, List.range_succ]
    rw [hsplit, fromLittle_append, ih]
    have hlen : (little n k).length = k := by simp [little]
    rw [hlen]
    show n % 256 ^ k + 256 ^ k * fromLittle [(n / 256 ^ k) % 256] = n % 256 ^ (k + 1)
    rw [show fromLittle [(n / 256 ^ k) % 256] = (n / 256 ^ k) % 256 from rfl]
    rw [Nat.pow_add_one]
    exact (Nat.mod_mul).symm
private theorem fromLittle_take (bs : Bytes) (s : Nat) :
    fromLittle bs = fromLittle (bs.take s) + 256 ^ s * fromLittle (bs.drop s) := by
  have h2 : fromLittle (bs.take s ++ bs.drop s) = fromLittle bs := by
    rw [List.take_append_drop]
  rw [← h2, fromLittle_append]
  congr 1
  by_cases hH : fromLittle (bs.drop s) = 0
  · rw [hH, Nat.mul_zero, Nat.mul_zero]
  · have hs : s ≤ bs.length := by
      by_cases hle : s ≤ bs.length
      · exact hle
      · have hge : bs.length ≤ s := Nat.le_of_not_ge hle
        have hd : bs.drop s = [] := List.drop_eq_nil_of_le hge
        rw [hd] at hH
        simp [fromLittle] at hH
    have hlen : (bs.take s).length = s := List.length_take_of_le hs
    rw [hlen]
private theorem div_linear (A H r c : Nat) (_hA : 0 < A) (hc : 0 < c) (hr : r < A) :
    (A * H + r) / (A * c) = H / c := by
  apply Nat.div_eq_of_lt_le
  · have h1 : (H / c) * (A * c) = A * (c * (H / c)) := by ac_rfl
    have h2 : A * (c * (H / c)) ≤ A * H := Nat.mul_le_mul_left A (Nat.mul_div_le H c)
    have h3 : A * H ≤ A * H + r := Nat.le_add_right _ _
    rw [h1]
    exact Nat.le_trans h2 h3
  · have hH : c * (H / c) + H % c = H := Nat.div_add_mod H c
    have hmod : H % c < c := Nat.mod_lt H hc
    have hsmall : A * (H % c) + r < A * c := by
      have e1 : A * (H % c) + r < A * (H % c) + A := by
        apply Nat.add_lt_add_left hr
      have e2 : A * (H % c) + A = A * (H % c + 1) := by
        rw [Nat.mul_add, Nat.mul_one]
      have e3 : A * (H % c + 1) ≤ A * c :=
        Nat.mul_le_mul_left A (Nat.succ_le_of_lt hmod)
      have e4 : A * (H % c) + r < A * (H % c + 1) := by rwa [e2] at e1
      exact Nat.lt_of_lt_of_le e4 e3
    have hAH : A * H = A * (c * (H / c)) + A * (H % c) := by
      rw [show A * H = A * (c * (H / c) + H % c) from congrArg (fun t => A * t) hH.symm, Nat.mul_add]
    have hkey : (H / c) * (A * c) = A * (c * (H / c)) := by ac_rfl
    have hstep : A * H + r < (H / c) * (A * c) + A * c := by
      rw [hAH, hkey, Nat.add_assoc]
      apply Nat.add_lt_add_left hsmall
    have hfin : (H / c) * (A * c) + A * c = (H / c + 1) * (A * c) := by
      rw [Nat.add_mul, Nat.one_mul]
    rw [hfin] at hstep
    exact hstep
private theorem word_lt (w : Nat) (xs : List Nat) :
    word w xs < 2 ^ (w * xs.length) := by
  induction xs with
  | nil => simp [word]
  | cons x xs ih =>
    have hx : x % 2 ^ w < 2 ^ w := Nat.mod_lt _ (two_pow_pos w)
    have hW : word w xs < 2 ^ (w * xs.length) := ih
    have h2 : 2 ^ w * (word w xs + 1) ≤ 2 ^ w * 2 ^ (w * xs.length) := by
      apply Nat.mul_le_mul_left
      omega
    have h3 : 2 ^ w * (word w xs + 1) = 2 ^ w * word w xs + 2 ^ w := by
      rw [Nat.mul_add, Nat.mul_one]
    have h4 : 2 ^ (w * xs.length) * 2 ^ w = 2 ^ w * 2 ^ (w * xs.length) := Nat.mul_comm _ _
    show x % 2 ^ w + 2 ^ w * word w xs < 2 ^ (w * (xs.length + 1))
    rw [show w * (xs.length + 1) = w * xs.length + w by rw [Nat.mul_add, Nat.mul_one], Nat.pow_add, h4]
    omega
private theorem word_append (w : Nat) (a b : List Nat) :
    word w (a ++ b) = word w a + 2 ^ (w * a.length) * word w b := by
  induction a with
  | nil => simp [word]
  | cons x a ih =>
    show x % 2 ^ w + 2 ^ w * word w (a ++ b) =
      (x % 2 ^ w + 2 ^ w * word w a) + 2 ^ (w * (a.length + 1)) * word w b
    rw [ih]
    rw [show w * (a.length + 1) = w * a.length + w by rw [Nat.mul_add, Nat.mul_one], Nat.pow_add]
    rw [Nat.mul_add]
    ac_rfl
private theorem pack_fromLittle (w : Nat) (xs : List Nat) :
    fromLittle (pack w xs) = word w xs := by
  show fromLittle (little (word w xs) (byteCount (xs.length * w))) = word w xs
  rw [fromLittle_little]
  apply Nat.mod_eq_of_lt
  have h1 := word_lt w xs
  have h2 : xs.length * w ≤ 8 * byteCount (xs.length * w) := byteCount_mul8 _
  have h3 : w * xs.length ≤ 8 * byteCount (xs.length * w) := by
    rw [Nat.mul_comm w xs.length]; exact h2
  calc word w xs < 2 ^ (w * xs.length) := h1
    _ ≤ 2 ^ (8 * byteCount (xs.length * w)) := Nat.pow_le_pow_right (by decide) h3
    _ = 256 ^ byteCount (xs.length * w) := (pow256 _).symm
private theorem shift_get (w x : Nat) (xs : List Nat) (j : Nat) :
    get (pack w (x :: xs)) w (j + 1) = get (pack w xs) w j := by
  have hpow : 0 < 2 ^ w := two_pow_pos w
  have hpowj : 0 < 2 ^ (j * w) := two_pow_pos _
  show (fromLittle (pack w (x :: xs)) / 2 ^ ((j + 1) * w)) % 2 ^ w =
       (fromLittle (pack w xs) / 2 ^ (j * w)) % 2 ^ w
  rw [pack_fromLittle, pack_fromLittle]
  show ((x % 2 ^ w + 2 ^ w * word w xs) / 2 ^ ((j + 1) * w)) % 2 ^ w =
       (word w xs / 2 ^ (j * w)) % 2 ^ w
  rw [show (j + 1) * w = j * w + w by rw [Nat.add_mul, Nat.one_mul], Nat.pow_add,
      Nat.add_comm (x % 2 ^ w) (2 ^ w * word w xs), Nat.mul_comm (2 ^ (j * w)) (2 ^ w)]
  have hdiv := div_linear (2 ^ w) (word w xs) (x % 2 ^ w) (2 ^ (j * w)) hpow hpowj
    (Nat.mod_lt _ hpow)
  rw [hdiv]
private theorem head_get (w x : Nat) (xs : List Nat) (hx : x < 2 ^ w) :
    get (pack w (x :: xs)) w 0 = x := by
  have hpow : 0 < 2 ^ w := two_pow_pos w
  show (fromLittle (pack w (x :: xs)) / 2 ^ (0 * w)) % 2 ^ w = x
  rw [pack_fromLittle]
  show ((x % 2 ^ w + 2 ^ w * word w xs) / 2 ^ (0 * w)) % 2 ^ w = x
  rw [Nat.zero_mul, Nat.pow_zero, Nat.div_one]
  rw [Nat.add_mul_mod_self_left]
  have hlt : x % 2 ^ w < 2 ^ w := Nat.mod_lt _ hpow
  rw [Nat.mod_eq_of_lt hlt, Nat.mod_eq_of_lt hx]

theorem LocalRead_sound : LocalRead.Goal := by
  intro bs w i hb
  show (fromLittle ((bs.drop (i * w / 8)).take (byteCount ((i * w % 8) + w))) / 2 ^ (i * w % 8)) % 2 ^ w =
       (fromLittle bs / 2 ^ (i * w)) % 2 ^ w
  generalize hs : i * w / 8 = s
  generalize hsh : i * w % 8 = sh
  generalize hk : byteCount (sh + w) = k
  have hdecomp : i * w = 8 * s + sh := by
    have h := Nat.mod_add_div (i * w) 8
    rw [hsh, hs] at h
    omega
  have hLlt : fromLittle (bs.take s) < 256 ^ s := by
    have hmem : ∀ b ∈ bs.take s, b < 256 := fun b hb' => hb b (List.take_subset s bs hb')
    have h1 := fromLittle_lt hmem
    have hlen : (bs.take s).length ≤ s := by
      rw [List.length_take]; exact Nat.min_le_left _ _
    exact Nat.lt_of_lt_of_le h1 (Nat.pow_le_pow_right (by decide) hlen)
  rw [pow256] at hLlt
  -- hLlt : fromLittle (bs.take s) < 2 ^ (8 * s)
  have hdiv : fromLittle bs / 2 ^ (i * w) = fromLittle (bs.drop s) / 2 ^ sh := by
    have hN := fromLittle_take bs s
    rw [hN, pow256, hdecomp, Nat.pow_add]
    rw [Nat.add_comm (fromLittle (bs.take s)) (2 ^ (8 * s) * fromLittle (bs.drop s))]
    exact div_linear _ _ _ _ (two_pow_pos _) (two_pow_pos _) hLlt
  -- window: sh + w ≤ 8k
  have hswk : sh + w ≤ 8 * k := by
    have h1 := byteCount_mul8 (sh + w)
    rw [hk] at h1
    exact h1
  have hk8 : sh ≤ 8 * k := by omega
  have hwk : w ≤ 8 * k - sh := by omega
  have hpowk : 2 ^ (8 * k) = 2 ^ sh * 2 ^ (8 * k - sh) := by
    rw [← Nat.pow_add]
    congr 1
    omega
  have hH2 : fromLittle (bs.drop s) =
      fromLittle ((bs.drop s).take k) +
        2 ^ sh * (2 ^ (8 * k - sh) * fromLittle ((bs.drop s).drop k)) := by
    have hH := fromLittle_take (bs.drop s) k
    rw [hH, pow256, hpowk, Nat.mul_assoc]
  have hHdiv : fromLittle (bs.drop s) / 2 ^ sh =
      fromLittle ((bs.drop s).take k) / 2 ^ sh +
        2 ^ (8 * k - sh) * fromLittle ((bs.drop s).drop k) := by
    rw [hH2]
    exact Nat.add_mul_div_left _ _ (two_pow_pos _)
  have hTmod : (2 ^ (8 * k - sh) * fromLittle ((bs.drop s).drop k)) % 2 ^ w = 0 := by
    have hexp : 8 * k - sh = w + (8 * k - sh - w) := by omega
    rw [hexp, Nat.pow_add, Nat.mul_assoc]
    exact Nat.mul_mod_right _ _
  have hHdivmod : (fromLittle (bs.drop s) / 2 ^ sh) % 2 ^ w =
      (fromLittle ((bs.drop s).take k) / 2 ^ sh) % 2 ^ w := by
    rw [hHdiv, Nat.add_mod, hTmod, Nat.add_zero, Nat.mod_mod]
  rw [hdiv, hHdivmod]


theorem PackedInverse_sound : PackedInverse.Goal := by
  intro w xs
  induction xs with
  | nil => intro hxs; rfl
  | cons x xs ih =>
    intro hxs
    have hx : x < 2 ^ w := hxs x (List.mem_cons_self)
    have hxs' : ∀ y ∈ xs, y < 2 ^ w := fun y hy => hxs y (List.mem_cons_of_mem _ hy)
    rw [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map]
    congr 1
    · exact head_get w x xs hx
    · have hmap : (List.range xs.length).map
          ((fun i => get (pack w (x :: xs)) w i) ∘ Nat.succ) =
        (List.range xs.length).map (get (pack w xs) w) := by
        apply List.map_congr_left
        intro a ha
        exact shift_get w x xs a
      rw [hmap, ih hxs']

theorem BitSplice_sound : BitSplice.Goal := by
  intro w a b
  show little (word w (a ++ b)) (byteCount ((a ++ b).length * w)) =
    little (fromLittle (pack w a) + 2 ^ (w * a.length) * fromLittle (pack w b))
      (byteCount ((a.length + b.length) * w))
  rw [List.length_append, pack_fromLittle, pack_fromLittle, word_append]

theorem PaddingDebt_sound : PaddingDebt.Goal := by
  intro sizes
  rw [Nat.sub_le_iff_le_add']
  induction sizes with
  | nil => simp [byteCount]
  | cons s ss ih =>
    cases ss with
    | nil => simp [byteCount]
    | cons t ss' =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.add_sub_cancel] at ih ⊢
      have hpair : byteCount s + byteCount (t + List.sum ss') ≤
          byteCount (s + (t + List.sum ss')) + 1 := byteCount_add_le _ _
      omega

theorem MinimumWidth_sound : MinimumWidth.Goal := by
  intro xs w
  constructor
  · intro h
    have hpeak : peak xs < 2 ^ w := by
      unfold peak
      by_cases hne : xs = []
      · rw [hne]; simp; exact two_pow_pos w
      · have hsome : xs.max?.isSome := List.isSome_max?_of_ne_nil hne
        obtain ⟨m, hm⟩ := Option.isSome_iff_exists.mp hsome
        have hmem : m ∈ xs := List.max?_mem hm
        have hm2 : m < 2 ^ w := h m hmem
        simp [hm]
        exact hm2
    by_cases hp0 : peak xs = 0
    · rw [hp0]; simp [width]
    · have hw : width (peak xs) = (peak xs).log2 + 1 := by
        unfold width
        simp [hp0]
      rw [hw]
      have hlt : (peak xs).log2 < w := (Nat.log2_lt hp0).mpr hpeak
      omega
  · intro h x hxmem
    have hxle : x ≤ peak xs := by
      unfold peak
      by_cases hne : xs = []
      · simp [hne] at hxmem
      · have hsome : xs.max?.isSome := List.isSome_max?_of_ne_nil hne
        obtain ⟨m, hm⟩ := Option.isSome_iff_exists.mp hsome
        rw [hm]
        exact (List.max?_le_iff hm).1 (Nat.le_refl m) x hxmem
    by_cases hp0 : peak xs = 0
    · have : x = 0 := by omega
      rw [this]
      exact two_pow_pos w
    · have hw : width (peak xs) = (peak xs).log2 + 1 := by
        unfold width
        simp [hp0]
      have hwle : (peak xs).log2 + 1 ≤ w := by rwa [hw] at h
      have hplt : peak xs < 2 ^ w := (Nat.log2_lt hp0).mp (by omega)
      exact Nat.lt_of_le_of_lt hxle hplt

end TinylambOpt.ResearchProofs.DevinBatch.C_Wire_A
