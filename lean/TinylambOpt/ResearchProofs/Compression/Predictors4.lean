import TinylambOpt.ResearchGoals.Compression.Predictors

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Predictors4

open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Proofs for the 16 goals in the `SideDelta`, `SideQuotient`, `TwoPhase` and
`SideResidue` predictor namespaces. All 16 goals turn out to be true: the three
shared shapes (round-trip, byte budget, restart seed) hold generically, and the
four predictor-specific goals follow from the lag/support of each predictor. -/

/-! ## Generic helpers -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem roundtrip_general (p : Predict) (side : Nat → Int) :
    ∀ i h xs, restore p side i h (encode p side i h xs) = xs := by
  intro i h xs
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih =>
      simp only [encode, restore]
      rw [unzig_zig]
      have hy : p i h side + (x - p i h side) = x := by omega
      rw [hy]
      simp [ih]

private theorem encode_eq_of_take_eq (p q : Predict) (s1 s2 : Nat → Int) (i k : Nat) :
    ∀ (h1 h2 : List Int) (xs : List Int),
      h1.take k = h2.take k →
      (∀ (j : Nat) (u v : List Int), u.take k = v.take k → p (i+j) u s1 = q (i+j) v s2) →
      encode p s1 i h1 xs = encode q s2 i h2 xs := by
  intro h1 h2 xs
  induction xs generalizing i h1 h2 with
  | nil => intro _ _; rfl
  | cons x xs ih =>
      intro htake hp
      have hpred : p i h1 s1 = q i h2 s2 := by
        simpa using hp 0 h1 h2 htake
      have htake' : (x :: h1).take k = (x :: h2).take k := by
        cases k with
        | zero => simp
        | succ k =>
            have htakek : h1.take k = h2.take k := by
              have hh := congrArg (fun l : List Int => l.take k) htake
              rw [List.take_take, List.take_take] at hh
              have hmin : min k (k+1) = k := by omega
              simpa [hmin] using hh
            simpa using congrArg (List.cons x) htakek
      have hrec : encode p s1 (i+1) (x :: h1) xs = encode q s2 (i+1) (x :: h2) xs :=
        ih (i+1) (x :: h1) (x :: h2) htake' (by
          intro j u v huv
          have hidx : i + 1 + j = i + (j + 1) := by omega
          simpa [hidx] using hp (j+1) u v huv)
      simp [encode, hpred, hrec]

private theorem lag_eq_of_take_succ {u v : List Int} {n : Nat}
    (h : u.take (n+1) = v.take (n+1)) : lag u n = lag v n := by
  have hn : n < n + 1 := by omega
  have huv : u[n]? = v[n]? := by
    have hh := congrArg (fun l : List Int => l[n]?) h
    simpa [List.getElem?_take, hn] using hh
  simp [lag, huv]

private theorem unzig_zero : unzig 0 = 0 := by
  simp [unzig]

private theorem restore_eq_of_predict_eq (p q : Predict) (s1 s2 : Nat → Int) (i : Nat)
    (h1 h2 : List Int)
    (hpred : ∀ (j : Nat) (u v : List Int), p (i+j) u s1 = q (i+j) v s2) :
    ∀ es, restore p s1 i h1 es = restore q s2 i h2 es := by
  intro es
  induction es generalizing i h1 h2 with
  | nil => rfl
  | cons e es ih =>
      have h0 := hpred 0 h1 h2
      have hrec := ih (i+1) ((p i h1 s1 + unzig e) :: h1) ((q i h2 s2 + unzig e) :: h2) (by
        intro j u v
        have hidx : i + 1 + j = i + (j + 1) := by omega
        simpa [hidx] using hpred (j+1) u v)
      have h0' : p i h1 s1 = q i h2 s2 := by simpa using h0
      rw [h0'] at hrec
      simp [restore, h0', hrec]

private theorem encode_length (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int) :
    ∀ xs, (encode p side i h xs).length = xs.length := by
  intro xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

/-! ## Wire cost helpers -/

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  simp [width, hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem width_le_iff (n W : Nat) (hn : n ≠ 0) : width n ≤ W ↔ n < 2 ^ W := by
  rw [width_eq n hn]
  constructor
  · intro h
    exact (Nat.log2_lt hn).mp (by omega)
  · intro h
    have hh := (Nat.log2_lt hn).mpr h
    omega

private theorem ubytes_le_iff (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
  unfold ubytes
  constructor
  · intro h
    have hw7 : (width n + 6) / 7 ≤ k := (Nat.max_le.mp h).2
    have hw : width n ≤ 7 * k := by
      have hh := (Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)).mp hw7
      omega
    calc n < 2 ^ width n := lt_two_pow_width n
      _ ≤ 2 ^ (7 * k) := Nat.pow_le_pow_right (by decide : 0 < 2) hw
      _ = 128 ^ k := by
        have h128 : (128 : Nat) = 2 ^ 7 := by decide
        rw [h128, ← Nat.pow_mul]
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · have hw : width n ≤ 7 * k := by
        by_cases hn : n = 0
        · subst hn
          simp [width]
        · have hlt : n < 2 ^ (7 * k) := by
            have h128 : (128 : Nat) = 2 ^ 7 := by decide
            rw [h128, ← Nat.pow_mul] at h
            exact h
          exact (width_le_iff n (7 * k) hn).mpr hlt
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_len_succ (n : Nat) (h : ¬ n < 128) :
    (uleb n).length = 1 + (uleb (n / 128)).length := by
  rw [uleb]
  simp [h]
  omega

private theorem uleb_len_le_succ (n k : Nat) :
    (uleb n).length ≤ k + 1 ↔ n < 128 ^ (k + 1) := by
  induction k generalizing n with
  | zero =>
      constructor
      · intro h
        by_cases hn : n < 128
        · exact hn
        · have hlen : 2 ≤ (uleb n).length := by
            rw [uleb_len_succ n hn]
            have := uleb_len_pos (n / 128)
            omega
          omega
      · intro hn
        rw [uleb]
        simp [hn]
  | succ k ih =>
      by_cases hn : n < 128
      · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
        have hlen1 : (uleb n).length = 1 := by rw [h1]; simp
        constructor
        · intro _
          calc n < 128 := hn
            _ ≤ 128 ^ (k + 1 + 1) := by
              have hk1 : 1 ≤ k + 1 + 1 := by omega
              simpa using Nat.pow_le_pow_right (by decide : 0 < 128) hk1
        · intro _
          rw [hlen1]
          omega
      · rw [uleb_len_succ n hn]
        have hih := ih (n / 128)
        constructor
        · intro h
          have hd : (uleb (n / 128)).length ≤ k + 1 := by omega
          have hdiv : n / 128 < 128 ^ (k + 1) := hih.mp hd
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)] at hdiv
          calc n < 128 ^ (k + 1) * 128 := hdiv
            _ = 128 ^ (k + 1 + 1) := (Nat.pow_succ 128 (k + 1)).symm
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            calc n < 128 ^ (k + 1 + 1) := h
              _ = 128 ^ (k + 1) * 128 := by rw [Nat.pow_succ]
          have hd := hih.mpr hdiv
          omega

private theorem uleb_length_eq_ubytes (n : Nat) : (uleb n).length = ubytes n := by
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · obtain ⟨j, hj⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hubpos)
    have hub_le : ubytes n ≤ j + 1 := by rw [hj]; omega
    have hlt : n < 128 ^ (j+1) := (ubytes_le_iff n (j+1) (by omega)).mp hub_le
    have hlen_le : (uleb n).length ≤ j + 1 := (uleb_len_le_succ n j).mpr hlt
    rw [hj]
    simpa using hlen_le
  · obtain ⟨j, hj⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hlenpos)
    have hlen_le : (uleb n).length ≤ j + 1 := by rw [hj]; omega
    have hlt : n < 128 ^ (j+1) := (uleb_len_le_succ n j).mp hlen_le
    have hub_le : ubytes n ≤ j + 1 := (ubytes_le_iff n (j+1) (by omega)).mpr hlt
    rw [hj]
    simpa using hub_le

private theorem pack_length (w : Nat) (xs : List Nat) : (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem width_mono : ∀ a b : Nat, a ≤ b → width a ≤ width b := by
  intro a b hab
  by_cases ha : a = 0
  · simp [width, ha]
  · have hb : b ≠ 0 := by omega
    simp [width, ha, hb]
    have hpow : 2 ^ a.log2 ≤ a := (Nat.le_log2 ha).mp (Nat.le_refl _)
    have hpowb : 2 ^ a.log2 ≤ b := Nat.le_trans hpow hab
    have hlog : a.log2 ≤ b.log2 := (Nat.le_log2 hb).mpr hpowb
    omega

private theorem width_of_lt_pow {n w : Nat} (h : n < 2 ^ w) : width n ≤ w := by
  by_cases hn : n = 0
  · simp [width, hn]
  · rw [width_eq n hn]
    have hh := (Nat.log2_lt hn).mpr h
    omega

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  have hw := width_mono a b h
  omega

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem peak_cons_lt (a : Nat) (l : List Nat) (w : Nat)
    (ha : a < 2 ^ w) (hl : peak l < 2 ^ w) : peak (a :: l) < 2 ^ w := by
  unfold peak
  rw [List.max?_cons]
  cases hm : l.max? with
  | none =>
      have lnil : l = [] := List.max?_eq_none_iff.mp hm
      subst l
      simp at ha ⊢
      exact ha
  | some m =>
      have hmle : m ≤ peak l := by simp [peak, hm]
      simp
      exact Nat.max_lt.mpr ⟨ha, Nat.lt_of_le_of_lt hmle hl⟩

private theorem peak_encode_lt (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) (w : Nat)
    (hc : certificate p side i h xs w) : peak (encode p side i h xs) < 2 ^ w := by
  induction xs generalizing i h with
  | nil =>
      have hp : 0 < 2 ^ w := Nat.pow_pos (by decide)
      simpa [encode, peak] using hp
  | cons x xs ih =>
      have h0 := hc 0 (by simp)
      have hhead : zig (x - p i h side) < 2 ^ w := by simpa using h0
      have htail : certificate p side (i+1) (x::h) xs w := by
        intro j hj
        have hj' : j + 1 < (x :: xs).length := by simpa using Nat.succ_lt_succ hj
        have hh := hc (j+1) hj'
        simpa [List.take_succ_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hh
      have hrec := ih (i+1) (x::h) htail
      simpa [encode] using peak_cons_lt (zig (x - p i h side)) (encode p side (i+1) (x::h) xs) w hhead hrec

private theorem byte_budget_general (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int)
    (w extra oldBytes : Nat) (hc : certificate p side i h xs w)
    (hsmall : extra + quote xs.length w < oldBytes) :
    extra + (frame (encode p side i h xs)).length < oldBytes := by
  let es := encode p side i h xs
  have hpeak : peak es < 2 ^ w := by dsimp [es]; exact peak_encode_lt p side i h xs w hc
  have hwle : width (peak es) ≤ w := width_of_lt_pow hpeak
  have hes : es.length = xs.length := by dsimp [es]; exact encode_length p side i h xs
  have hlen : (frame es).length =
      1 + ubytes es.length + ubytes (width (peak es)) + byteCount (es.length * width (peak es)) := by
    simp [frame, uleb_length_eq_ubytes, pack_length]
    omega
  have hframe_le : (frame es).length ≤ quote xs.length w := by
    rw [hlen, hes]
    unfold quote
    have hu := ubytes_mono hwle
    have hbc := byteCount_mono (Nat.mul_le_mul_left xs.length hwle)
    omega
  dsimp [es] at hframe_le ⊢
  omega

/-! ## ResidualRoundTrip (all four predictors) -/

theorem SideDelta_ResidualRoundTrip_sound : SideDelta.ResidualRoundTrip.Goal := by
  intro side i h xs
  exact roundtrip_general SideDelta.predict side i h xs

theorem SideQuotient_ResidualRoundTrip_sound : SideQuotient.ResidualRoundTrip.Goal := by
  intro period side i h xs
  exact roundtrip_general (SideQuotient.predict period) side i h xs

theorem TwoPhase_ResidualRoundTrip_sound : TwoPhase.ResidualRoundTrip.Goal := by
  intro evenStep oddStep side i h xs
  exact roundtrip_general (TwoPhase.predict evenStep oddStep) side i h xs

theorem SideResidue_ResidualRoundTrip_sound : SideResidue.ResidualRoundTrip.Goal := by
  intro base period side i h xs
  exact roundtrip_general (SideResidue.predict base period) side i h xs

/-! ## ByteBudget (all four predictors) -/

theorem SideDelta_ByteBudget_sound : SideDelta.ByteBudget.Goal := by
  intro side i h xs w extra oldBytes hc hsmall
  exact byte_budget_general SideDelta.predict side i h xs w extra oldBytes hc hsmall

theorem SideQuotient_ByteBudget_sound : SideQuotient.ByteBudget.Goal := by
  intro period side i h xs w extra oldBytes hc hsmall
  exact byte_budget_general (SideQuotient.predict period) side i h xs w extra oldBytes hc hsmall

theorem TwoPhase_ByteBudget_sound : TwoPhase.ByteBudget.Goal := by
  intro evenStep oddStep side i h xs w extra oldBytes hc hsmall
  exact byte_budget_general (TwoPhase.predict evenStep oddStep) side i h xs w extra oldBytes hc hsmall

theorem SideResidue_ByteBudget_sound : SideResidue.ByteBudget.Goal := by
  intro base period side i h xs w extra oldBytes hc hsmall
  exact byte_budget_general (SideResidue.predict base period) side i h xs w extra oldBytes hc hsmall

/-! ## RestartSeed -/

theorem SideDelta_RestartSeed_sound : SideDelta.RestartSeed.Goal := by
  intro side i h xs
  apply encode_eq_of_take_eq SideDelta.predict SideDelta.predict side side i 1 (h.take 1) h xs (by simp [List.take_take])
  intro j u v huv
  simp [SideDelta.predict, lag_eq_of_take_succ huv]

theorem SideQuotient_RestartSeed_sound : SideQuotient.RestartSeed.Goal := by
  intro period side i h xs
  apply encode_eq_of_take_eq (SideQuotient.predict period) (SideQuotient.predict period) side side i 0 (h.take 0) h xs rfl
  intro j u v huv
  rfl

theorem TwoPhase_RestartSeed_sound : TwoPhase.RestartSeed.Goal := by
  intro evenStep oddStep side i h xs
  apply encode_eq_of_take_eq (TwoPhase.predict evenStep oddStep) (TwoPhase.predict evenStep oddStep)
    side side i 2 (h.take 2) h xs (by simp [List.take_take])
  intro j u v huv
  simp [TwoPhase.predict, lag_eq_of_take_succ (n := 1) huv]

theorem SideResidue_RestartSeed_sound : SideResidue.RestartSeed.Goal := by
  intro base period side i h xs
  apply encode_eq_of_take_eq (SideResidue.predict base period) (SideResidue.predict base period)
    side side i 0 (h.take 0) h xs rfl
  intro j u v huv
  rfl

/-! ## SideResidue.PeriodShift -/

theorem SideResidue_PeriodShift_sound : SideResidue.PeriodShift.Goal := by
  intro base period side i h xs k
  let m : Nat := period + 1
  apply encode_eq_of_take_eq (SideResidue.predict base period) (SideResidue.predict base period)
    (fun j => side j + k * (m : Nat)) side i 0 h h xs rfl
  intro j u v huv
  simp [SideResidue.predict, m]

/-! ## SideDelta.ClockOffset -/

private theorem SideDelta_encode_eq_of_lag0 {side : Nat → Int} {i : Nat} {h1 h2 : List Int}
    (hlag : lag h1 0 = lag h2 0) :
    ∀ xs, encode SideDelta.predict side i h1 xs = encode SideDelta.predict side i h2 xs := by
  intro xs
  induction xs generalizing i h1 h2 with
  | nil => rfl
  | cons x xs ih =>
      have hp : SideDelta.predict i h1 side = SideDelta.predict i h2 side := by
        simp [SideDelta.predict, hlag]
      have hlag' : lag (x :: h1) 0 = lag (x :: h2) 0 := by simp [lag]
      simp [encode, hp, ih (i := i+1) (h1 := x :: h1) (h2 := x :: h2) hlag']

theorem SideDelta_ClockOffset_sound : SideDelta.ClockOffset.Goal := by
  intro offset side i n hi
  have haux : ∀ (n : Nat) (i : Nat), 0 < i →
      encode SideDelta.predict side i [side (i-1) + offset]
        ((List.range n).map (fun j => side (i+j) + offset)) = List.replicate n 0 := by
    intro n
    induction n with
    | zero => simp [encode]
    | succ n ih =>
        intro i hi
        rw [List.range_succ_eq_map, List.map_cons]
        simp only [encode]
        have hpred : SideDelta.predict i [side (i-1) + offset] side = side i + offset := by
          simp [SideDelta.predict, lag]
          omega
        rw [hpred]
        simp [zig]
        have hlag : lag ((side i + offset) :: [side (i-1) + offset]) 0 =
            lag [side i + offset] 0 := by simp [lag]
        have heq := SideDelta_encode_eq_of_lag0 (side := side) (i := i+1)
          (h1 := (side i + offset) :: [side (i-1) + offset])
          (h2 := [side i + offset]) hlag
          ((List.range n).map (fun j => side (i+1+j) + offset))
        rw [List.replicate_succ]
        simp only [List.cons.injEq]
        constructor
        · trivial
        · have hmap : (List.range n).map ((fun j => side (i+j) + offset) ∘ Nat.succ) =
              (List.range n).map (fun j => side (i+1+j) + offset) := by
            apply List.map_congr_left
            intro j hj
            simp [Nat.succ_eq_add_one, Nat.add_comm, Nat.add_left_comm]
          rw [hmap, heq]
          simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (i+1) (by omega)
  exact haux n i hi

/-! ## SideQuotient.DateProjection -/

theorem SideQuotient_DateProjection_sound : SideQuotient.DateProjection.Goal := by
  intro period side i n
  induction n generalizing i with
  | zero => rfl
  | succ n ih =>
      rw [List.range_succ_eq_map, List.map_cons, List.replicate_succ]
      simp only [restore, unzig_zero]
      have hrest (h : List Int) : restore (SideQuotient.predict period) side (i+1) h
          (List.replicate n 0) =
          restore (SideQuotient.predict period) side (i+1) [] (List.replicate n 0) := by
        apply restore_eq_of_predict_eq
        intro j u v
        rfl
      rw [hrest]
      simpa [SideQuotient.predict, Function.comp_def, Nat.succ_eq_add_one,
        Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (i+1)

/-! ## TwoPhase.TwoLanes -/

theorem TwoPhase_TwoLanes_sound : TwoPhase.TwoLanes.Goal := by
  intro a b d e side n
  let f : Nat → Int := fun j =>
    if j % 2 == 0 then a + d * ((j / 2 : Nat) : Int) else b + e * ((j / 2 : Nat) : Int)
  let step : Nat → Int := fun j => if j % 2 == 0 then d else e
  have hstep2 : ∀ j : Nat, f j + step (j+2) = f (j+2) := by
    intro j
    have hdivI : ((↑j : Int) + 2) / 2 = (↑j : Int) / 2 + 1 := by
      rw [Int.add_ediv_of_dvd_right (by decide : (2 : Int) ∣ 2)]
      simp
    by_cases hj : j % 2 = 0
    · have hj2 : (j+2) % 2 = 0 := by omega
      simp [f, step, hj, hj2]
      rw [hdivI, Int.mul_add, Int.mul_one]
      ac_rfl
    · have hj2 : (j+2) % 2 ≠ 0 := by omega
      simp [f, step, hj]
      rw [hdivI, Int.mul_add, Int.mul_one]
      ac_rfl
  have haux : ∀ (n : Nat) (j : Nat) (h : List Int),
      h[0]?.getD 0 + step (j+1) = f (j+1) →
      h[1]?.getD 0 + step j = f j →
      restore (TwoPhase.predict d e) side j h (List.replicate n 0) =
        (List.range n).map (fun t => f (j+t)) := by
    intro n
    induction n with
    | zero => intro j h h0 h1; rfl
    | succ n ih =>
        intro j h h0 h1
        have hp : TwoPhase.predict d e j h side = f j := by
          simp [TwoPhase.predict, lag, step] at h1 ⊢
          exact h1
        rw [List.range_succ_eq_map, List.map_cons, List.replicate_succ]
        simp only [restore, unzig_zero]
        rw [hp]
        simp
        congr 1
        have h0' : ((f j) :: h)[0]?.getD 0 + step (j+2) = f (j+2) := by
          simpa using hstep2 j
        have h1' : ((f j) :: h)[1]?.getD 0 + step (j+1) = f (j+1) := by
          simpa using h0
        have hrec := ih (j+1) ((f j) :: h) h0' h1'
        have hmap : (List.range n).map ((fun t => f (j+t)) ∘ Nat.succ) =
            (List.range n).map (fun t => f ((j+1)+t)) := by
          apply List.map_congr_left
          intro t ht
          have heq : j + 1 + t = j + Nat.succ t := by omega
          rw [heq]
          rfl
        rw [hmap]
        exact hrec
  have hseed0 : [b - e, a - d][0]?.getD 0 + step (0+1) = f (0+1) := by
    simp [step, f]
  have hseed1 : [b - e, a - d][1]?.getD 0 + step 0 = f 0 := by
    simp [step, f]
  simpa [f, step] using haux n 0 [b - e, a - d] hseed0 hseed1

end TinylambOpt.ResearchProofs.Compression.Predictors4
