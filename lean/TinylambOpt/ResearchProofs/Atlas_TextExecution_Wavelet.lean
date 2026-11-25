import TinylambOpt.ResearchGoals.Atlas.TextExecution
namespace TinylambOpt.ResearchProofs.Atlas.TextExecution.Wavelet
open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet
set_option maxHeartbeats 0

private theorem filter_take_length (p : Nat → Bool) (w : Nat) (xs : List Nat) :
    (xs.take w).filter p = (xs.filter p).take ((xs.take w).filter p).length := by
  induction xs generalizing w with
  | nil => simp
  | cons x xs ih =>
      cases w with
      | zero => simp
      | succ w =>
          cases hp : p x
          · simpa [hp, List.take_succ_cons] using ih w
          · simpa [hp, List.take_succ_cons] using congrArg (fun t => x :: t) (ih w)

private theorem filter_take_drop (p : Nat → Bool) (l w : Nat) (xs : List Nat) :
    ((xs.drop l).take w).filter p =
      ((xs.filter p).drop (((xs.take l).filter p).length)).take
        (((xs.drop l).take w).filter p).length := by
  induction xs generalizing l w with
  | nil => simp
  | cons x xs ih =>
      cases l with
      | zero => simpa using filter_take_length p w (x::xs)
      | succ l =>
          cases hp : p x
          · simpa [hp, List.take_succ_cons, List.drop_succ_cons] using ih l w
          · simpa [hp, List.take_succ_cons, List.drop_succ_cons] using ih l w

theorem sound_RangeMap : RangeMap.Goal := by
  intro bit l w xs high
  dsimp only
  dsimp [remap]
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  exact filter_take_drop (fun x : Nat => x.testBit bit == high) l w xs

private theorem filter_eq_self_of_all {α} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = true) : l.filter p = l := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = true := h x (by simp)
      have ht : ∀ y ∈ xs, p y = true := by intro y hy; exact h y (by simp [hy])
      simp [hx, ih ht]

private theorem filter_eq_nil_of_all {α} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = false) : l.filter p = [] := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = false := h x (by simp)
      have ht : ∀ y ∈ xs, p y = false := by intro y hy; exact h y (by simp [hy])
      simp [hx, ih ht]

private theorem two_pow_pos (n : Nat) : 0 < 2^n := Nat.pow_pos (by decide)

private theorem lt_two_pow_of_testBit_false {d x : Nat} (h : x < 2^(d+1))
    (hb : x.testBit d = false) : x < 2^d := by
  have hdiv2 : x / 2^d < 2 := by
    rw [Nat.div_lt_iff_lt_mul (two_pow_pos d)]
    have hp : 2^(d+1) = 2*2^d := by rw [Nat.pow_succ, Nat.mul_comm]
    rw [hp] at h
    exact h
  have hb' : (x / 2^d).testBit 0 = false := by
    rw [← Nat.testBit_add]
    simpa using hb
  have hqle : x / 2^d ≤ 1 := Nat.le_of_lt_succ hdiv2
  have hq0 : x / 2^d = 0 := by
    rcases Nat.le_one_iff_eq_zero_or_eq_one.mp hqle with hq0 | hq1
    · exact hq0
    · have htb : (x / 2^d).testBit 0 = true := by simp [Nat.testBit_zero, hq1]
      rw [hb'] at htb
      cases htb
  have hlt1 : x / 2^d < 1 := by rw [hq0]; decide
  simpa using (Nat.div_lt_iff_lt_mul (two_pow_pos d)).mp hlt1

private theorem testBit_true_of_upper {d y : Nat} (hlo : 2^d ≤ y) (hhi : y < 2^(d+1)) :
    y.testBit d = true := by
  by_cases ht : y.testBit d = true
  · exact ht
  · have hf : y.testBit d = false := Bool.eq_false_of_ne_true ht
    have hylt : y < 2^d := lt_two_pow_of_testBit_false (by simpa [Nat.pow_succ] using hhi) hf
    exact False.elim (by omega)

theorem sound_DictionaryGrowth : DictionaryGrowth.Goal := by
  intro d old more k hold hmore hk
  have hlower : lower d (old ++ more) = old := by
    simp [lower, List.filter_append,
      filter_eq_self_of_all (fun x => !x.testBit d) old (by
        intro x hx
        have hb := Nat.testBit_lt_two_pow (hold x hx)
        simp [hb]),
      filter_eq_nil_of_all (fun x => !x.testBit d) more (by
        intro y hy
        have hyb := testBit_true_of_upper (hmore y hy).1 (hmore y hy).2
        simp [hyb])]
  have hupper : upper d (old ++ more) = more := by
    simp [upper, List.filter_append,
      filter_eq_nil_of_all (fun x => x.testBit d) old (by
        intro x hx
        exact Nat.testBit_lt_two_pow (hold x hx)),
      filter_eq_self_of_all (fun x => x.testBit d) more (by
        intro y hy
        exact testBit_true_of_upper (hmore y hy).1 (hmore y hy).2)]
  simp [kth, hlower, hupper]


theorem sound_BetweenCodes : BetweenCodes.Goal := by
  intro xs level lo hi hlohi
  have hp : 0 < 2^level := two_pow_pos level
  apply List.countP_congr
  intro x hx
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · intro h
    exact ⟨(Nat.le_div_iff_mul_le hp).mpr h.1, (Nat.div_lt_iff_lt_mul hp).mpr h.2⟩
  · intro h
    exact ⟨(Nat.le_div_iff_mul_le hp).mp h.1, (Nat.div_lt_iff_lt_mul hp).mp h.2⟩

private theorem count_filter_eq_zero_of_not {α} [BEq α] [LawfulBEq α]
    (p : α → Bool) (l : List α) (a : α) (ha : p a = false) :
    (l.filter p).count a = 0 := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      cases hya : y == a
      · by_cases hpy : p y = true
        · rw [show List.filter p (y :: ys) = y :: List.filter p ys by simp [hpy]]
          rw [List.count_cons]
          simp [hya, ih]
        · simp [hpy, ih]
      · have hyeq : y = a := beq_iff_eq.mp hya
        subst y
        simp [ha, ih]

private theorem count_lower_eq (bit x : Nat) (b : Word) (hx : x.testBit bit = false) :
    (lower bit b).count x = b.count x := by
  dsimp [lower]
  exact List.count_filter (by simp [hx] : (fun y : Nat => !y.testBit bit) x = true)

private theorem count_upper_eq (bit x : Nat) (b : Word) (hx : x.testBit bit = true) :
    (upper bit b).count x = b.count x := by
  dsimp [upper]
  exact List.count_filter (by simp [hx] : (fun y : Nat => y.testBit bit) x = true)

private theorem count_lower_zero (bit x : Nat) (b : Word) (hx : x.testBit bit = true) :
    (lower bit b).count x = 0 := by
  exact count_filter_eq_zero_of_not (fun y : Nat => !y.testBit bit) b x (by simp [hx])

private theorem count_upper_zero (bit x : Nat) (b : Word) (hx : x.testBit bit = false) :
    (upper bit b).count x = 0 := by
  exact count_filter_eq_zero_of_not (fun y : Nat => y.testBit bit) b x hx

private theorem pairs_filter_split (bit : Nat) (a b : Word) :
    (a.map (fun x => b.count x)).sum =
      ((a.filter (fun x => !x.testBit bit)).map
        (fun x => (b.filter (fun y => !y.testBit bit)).count x)).sum +
      ((a.filter (fun x => x.testBit bit)).map
        (fun x => (b.filter (fun y => y.testBit bit)).count x)).sum := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      cases hx : x.testBit bit
      · have hlo : (b.filter (fun y : Nat => !y.testBit bit)).count x = b.count x := by
          exact List.count_filter (by simp [hx] : (fun y : Nat => !y.testBit bit) x = true)
        have hu : (b.filter (fun y : Nat => y.testBit bit)).count x = 0 := by
          exact count_filter_eq_zero_of_not (fun y : Nat => y.testBit bit) b x hx
        simp [hx, ih]
        omega
      · have hlo : (b.filter (fun y : Nat => !y.testBit bit)).count x = 0 := by
          exact count_filter_eq_zero_of_not (fun y : Nat => !y.testBit bit) b x (by simp [hx])
        have hu : (b.filter (fun y : Nat => y.testBit bit)).count x = b.count x := by
          exact List.count_filter (by simp [hx] : (fun y : Nat => y.testBit bit) x = true)
        simp [hx, ih]
        omega

theorem sound_SameBranch : SameBranch.Goal := by
  intro a b bit
  simpa [pairs, lower, upper] using pairs_filter_split bit a b


private theorem two_pow_le_of_testBit_true {d y : Nat} (hbit : y.testBit d = true) :
    2^d ≤ y := by
  by_cases hle : 2^d ≤ y
  · exact hle
  · have hylt : y < 2^d := Nat.lt_of_not_ge hle
    have hf := Nat.testBit_lt_two_pow hylt
    rw [hbit] at hf
    cases hf

private theorem mod_add_two_pow_of_upper {d y : Nat} (hbit : y.testBit d = true)
    (hy : y < 2^(d+1)) : y % 2^d + 2^d = y := by
  have hled : 2^d ≤ y := two_pow_le_of_testBit_true hbit
  have hlt2 : y - 2^d < 2^d := by
    have hp : 2^(d+1) = 2^d + 2^d := by rw [Nat.two_pow_succ]
    omega
  rw [Nat.mod_eq_sub_mod hled]
  rw [Nat.mod_eq_of_lt hlt2]
  omega

private theorem map_mod_add_eq_self (d : Nat) (us : Word)
    (h : ∀ y ∈ us, 2^d ≤ y ∧ y < 2^(d+1)) :
    us.map (fun y => y % 2^d + 2^d) = us := by
  induction us with
  | nil => simp
  | cons y ys ih =>
      have hy := h y (by simp)
      have hybit : y.testBit d = true := testBit_true_of_upper hy.1 hy.2
      have ht : ∀ z ∈ ys, 2^d ≤ z ∧ z < 2^(d+1) := by
        intro z hz
        exact h z (by simp [hz])
      simp [mod_add_two_pow_of_upper hybit hy.2, ih ht]

private theorem leBool_trans (a b c : Nat) (hab : decide (a ≤ b) = true)
    (hbc : decide (b ≤ c) = true) : decide (a ≤ c) = true := by
  simp at hab hbc ⊢
  omega

private theorem leBool_total (a b : Nat) : decide (a ≤ b) || decide (b ≤ a) = true := by
  simp
  omega

private theorem sortedMerge (xs : List Nat) : (xs.mergeSort).Pairwise (fun a b => a ≤ b) := by
  simpa using (List.pairwise_mergeSort leBool_trans leBool_total xs)

private theorem sorted_eq_of_perm (l₁ l₂ : List Nat)
    (h1 : l₁.Pairwise (fun a b => a ≤ b)) (h2 : l₂.Pairwise (fun a b => a ≤ b))
    (hp : l₁.Perm l₂) : l₁ = l₂ := by
  exact List.Perm.eq_of_pairwise (by intro a b ha hb hab₁ hab₂; omega) h1 h2 hp

private theorem append_cons_perm {α} (a : α) (l₁ l₂ : List α) :
    (l₁ ++ a :: l₂).Perm (a :: (l₁ ++ l₂)) := by
  induction l₁ with
  | nil => simp
  | cons y ys ih =>
      exact (List.Perm.cons y ih).trans (List.Perm.swap' a y (List.Perm.refl _))

private theorem lower_upper_perm (bit : Nat) (xs : Word) :
    (lower bit xs ++ upper bit xs).Perm xs := by
  induction xs with
  | nil => simp [lower, upper]
  | cons x xs ih =>
      cases hx : x.testBit bit
      · simpa [lower, upper, hx] using List.Perm.cons x ih
      · have hperm : (lower bit xs ++ x :: upper bit xs).Perm
            (x :: (lower bit xs ++ upper bit xs)) :=
          append_cons_perm x (lower bit xs) (upper bit xs)
        exact (by simpa [lower, upper, hx] using hperm.trans (List.Perm.cons x ih))

private theorem mergeSort_split (d : Nat) (xs : Word)
    (h : ∀ x ∈ xs, x < 2^(d+1)) :
    xs.mergeSort = (lower d xs).mergeSort ++ (upper d xs).mergeSort := by
  apply sorted_eq_of_perm
  · exact sortedMerge xs
  · apply (List.pairwise_append).mpr
    refine ⟨sortedMerge (lower d xs), sortedMerge (upper d xs), ?_⟩
    intro a ha b hb
    have ha_mem : a ∈ lower d xs :=
      (List.mergeSort_perm (lower d xs) (fun a b : Nat => a ≤ b)).mem_iff.mp ha
    have hb_mem : b ∈ upper d xs :=
      (List.mergeSort_perm (upper d xs) (fun a b : Nat => a ≤ b)).mem_iff.mp hb
    have hap := List.mem_filter.mp ha_mem
    have hbp := List.mem_filter.mp hb_mem
    have habit : a.testBit d = false := by
      have ht := hap.2
      simp at ht
      exact ht
    have halt : a < 2^d := lt_two_pow_of_testBit_false (h a hap.1) habit
    have hbge : 2^d ≤ b := two_pow_le_of_testBit_true hbp.2
    omega
  · have p₁ : xs.mergeSort.Perm xs := List.mergeSort_perm xs (fun a b : Nat => a ≤ b)
    have p₂ : xs.Perm (lower d xs ++ upper d xs) := (lower_upper_perm d xs).symm
    have p₃ : (lower d xs ++ upper d xs).Perm
        ((lower d xs).mergeSort ++ (upper d xs).mergeSort) :=
      (List.Perm.append
        (List.mergeSort_perm (lower d xs) (fun a b : Nat => a ≤ b))
        (List.mergeSort_perm (upper d xs) (fun a b : Nat => a ≤ b))).symm
    exact p₁.trans (p₂.trans p₃)

private theorem testBit_mod_two_pow_self (d x : Nat) :
    (x % 2^(d+1)).testBit d = x.testBit d := by
  rw [Nat.testBit_mod_two_pow]
  simp

private theorem kth_mod (d : Nat) (xs : Word) (k : Nat) :
    kth d xs k = kth d (xs.map (fun x => x % 2^d)) k := by
  induction d generalizing xs k with
  | zero => simp [kth]
  | succ d ih =>
      rw [kth, kth]
      have hdiv : 2^d ∣ 2^(d+1) := by
        rw [Nat.pow_succ]
        exact ⟨2, rfl⟩
      have hmodmod : ∀ x : Nat, (x % 2^(d+1)) % 2^d = x % 2^d := by
        intro x
        exact Nat.mod_mod_of_dvd x hdiv
      have hplow : ((fun x : Nat => !x.testBit d) ∘ fun x => x % 2^(d+1)) =
          (fun x : Nat => !x.testBit d) := by
        funext x
        simp [testBit_mod_two_pow_self d x]
      have hphigh : ((fun x : Nat => x.testBit d) ∘ fun x => x % 2^(d+1)) =
          (fun x : Nat => x.testBit d) := by
        funext x
        simp [testBit_mod_two_pow_self d x]
      have hlower : lower d (xs.map (fun x => x % 2^(d+1))) =
          (lower d xs).map (fun x => x % 2^(d+1)) := by
        dsimp [lower]
        rw [List.filter_map, hplow]
      have hupper : upper d (xs.map (fun x => x % 2^(d+1))) =
          (upper d xs).map (fun x => x % 2^(d+1)) := by
        dsimp [upper]
        rw [List.filter_map, hphigh]
      rw [hlower, hupper]
      rw [ih ((lower d xs).map (fun x => x % 2^(d+1))) k]
      rw [ih ((upper d xs).map (fun x => x % 2^(d+1)))
          (k - (List.map (fun x => x % 2^(d+1)) (lower d xs)).length)]
      rw [List.map_map]
      rw [List.map_map]
      rw [show (List.map ((fun x : Nat => x % 2^d) ∘ fun x => x % 2^(d+1)) (lower d xs)) =
              (lower d xs).map (fun x => x % 2^d) by
            apply List.map_congr_left
            intro x hx
            exact hmodmod x]
      rw [show (List.map ((fun x : Nat => x % 2^d) ∘ fun x => x % 2^(d+1)) (upper d xs)) =
              (upper d xs).map (fun x => x % 2^d) by
            apply List.map_congr_left
            intro x hx
            exact hmodmod x]
      simp only [List.length_map]
      rw [← ih (lower d xs) k]
      rw [← ih (upper d xs) (k - (lower d xs).length)]

private theorem upper_sorted_kth (d : Nat) (xs : Word)
    (h : ∀ x ∈ xs, x < 2^(d+1))
    (ihK : ∀ (ys : Word), (∀ y ∈ ys, y < 2^d) → ∀ (j : Nat), j < ys.length →
      ys.mergeSort[j]? = some (kth d ys j))
    (j : Nat) (hj : j < (upper d xs).length) :
    (upper d xs).mergeSort[j]? = some (2^d + kth d (upper d xs) j) := by
  let us := upper d xs
  let ys := us.map (fun y => y % 2^d)
  have husbound : ∀ y ∈ us, 2^d ≤ y ∧ y < 2^(d+1) := by
    intro y hy
    have hyin : y ∈ xs := (List.mem_filter.mp hy).1
    have hybit : y.testBit d = true := (List.mem_filter.mp hy).2
    exact ⟨two_pow_le_of_testBit_true hybit, h y hyin⟩
  have hysbound : ∀ y ∈ ys, y < 2^d := by
    intro y hy
    rcases List.mem_map.mp hy with ⟨z, hz, rfl⟩
    exact Nat.mod_lt _ (two_pow_pos d)
  have hlenys : ys.length = us.length := by simp [ys]
  have hmapid : us.map (fun y => y % 2^d + 2^d) = us := map_mod_add_eq_self d us husbound
  have hmap_eq : us.mergeSort = (ys.mergeSort).map (fun z => z + 2^d) := by
    apply sorted_eq_of_perm
    · exact sortedMerge us
    · exact List.Pairwise.map (fun z : Nat => z + 2^d)
        (by intro a b hab; omega) (sortedMerge ys)
    · have p0 : us.mergeSort.Perm us := List.mergeSort_perm us (fun a b : Nat => a ≤ b)
      have p1 : us.Perm (us.map (fun y => y % 2^d + 2^d)) := List.Perm.of_eq hmapid.symm
      have p2 : (us.map (fun y => y % 2^d + 2^d)).Perm (ys.map (fun z => z + 2^d)) := by
        apply List.Perm.of_eq
        dsimp [ys]
        rw [List.map_map]
        rfl
      have p3 : (ys.map (fun z => z + 2^d)).Perm ((ys.mergeSort).map (fun z => z + 2^d)) :=
        (List.Perm.map (fun z : Nat => z + 2^d)
          (List.mergeSort_perm ys (fun a b : Nat => a ≤ b))).symm
      exact p0.trans (p1.trans (p2.trans p3))
  have hkmod : kth d us j = kth d ys j := by
    simpa [ys] using kth_mod d us j
  have hjys : j < ys.length := by simpa [hlenys] using hj
  have ihres := ihK ys hysbound j hjys
  rw [hmap_eq, List.getElem?_map, ihres]
  simp only [Option.map_some]
  rw [← hkmod]
  change some (kth d us j + 2 ^ d) = some (2 ^ d + kth d us j)
  rw [Nat.add_comm]

private theorem kth_sorted (d : Nat) (xs : Word)
    (h : ∀ x ∈ xs, x < 2^d) (k : Nat) (hk : k < xs.length) :
    xs.mergeSort[k]? = some (kth d xs k) := by
  induction d generalizing xs k with
  | zero =>
      have hzero : ∀ x ∈ xs, x = 0 := by
        intro x hx
        have hxlt := h x hx
        simpa using hxlt
      have hlen : k < xs.mergeSort.length := by simpa [List.length_mergeSort] using hk
      rw [List.getElem?_eq_getElem hlen]
      have hm : xs.mergeSort[k] ∈ xs.mergeSort := List.getElem_mem hlen
      have hm' : xs.mergeSort[k] ∈ xs :=
        (List.mergeSort_perm xs (fun a b : Nat => a ≤ b)).mem_iff.mp hm
      simp [kth, hzero _ hm']
  | succ d ih =>
      have hsplit : xs.mergeSort =
          (lower d xs).mergeSort ++ (upper d xs).mergeSort :=
        mergeSort_split d xs (by
          intro x hx
          simpa [Nat.pow_succ] using h x hx)
      have hlenperm : (lower d xs).length + (upper d xs).length = xs.length := by
        simpa using (lower_upper_perm d xs).length_eq
      rw [kth]
      rw [hsplit, List.getElem?_append]
      by_cases hkz : k < (lower d xs).length
      · have hkz' : k < ((lower d xs).mergeSort).length := by
          simpa [List.length_mergeSort] using hkz
        rw [ite_eq_left hkz', ite_eq_left hkz]
        exact ih (lower d xs) (by
          intro x hx
          have hxmem : x ∈ xs := (List.mem_filter.mp hx).1
          have hxbit : x.testBit d = false := by
            have ht := (List.mem_filter.mp hx).2
            simp at ht
            exact ht
          exact lt_two_pow_of_testBit_false (h x hxmem) hxbit) k hkz
      · have hkz' : ¬ k < ((lower d xs).mergeSort).length := by
          simpa [List.length_mergeSort] using hkz
        rw [ite_eq_right hkz', ite_eq_right hkz]
        have hj : k - (lower d xs).length < (upper d xs).length := by omega
        simpa [List.length_mergeSort] using
          upper_sorted_kth d xs (by intro x hx; exact h x hx) ih
            (k - (lower d xs).length) hj

theorem sound_Quantile : Quantile.Goal := by
  intro d xs k hbound hk
  exact kth_sorted d xs hbound k hk

end TinylambOpt.ResearchProofs.Atlas.TextExecution.Wavelet
