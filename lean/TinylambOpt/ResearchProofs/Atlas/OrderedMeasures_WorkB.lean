import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.OrderedMeasuresPeakCount

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures_WorkB

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! ## ZeroFrames

The `TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_ZeroFrames_Remaining`
module is present in the sources but not part of the built library, so its
proofs are inlined below (helpers renamed with the `om_zf_` prefix). -/

-- BEGIN om-helpers
/-! #### Generic list helpers -/

private theorem om_zf_countP_map_comp {α β : Type} (p : β → Bool) (f : α → β) (l : List α) :
    (l.map f).countP p = l.countP (fun x => p (f x)) := by
  simp [Function.comp_def]

private theorem om_zf_countP_congr_left {α : Type} {l : List α} {p q : α → Bool}
    (h : ∀ a ∈ l, p a = q a) : l.countP p = l.countP q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.countP_cons]
      rw [h a (by simp)]
      rw [ih (by intro b hb; exact h b (by simp [hb]))]
private theorem om_zf_countP_succ_range (p : Nat → Bool) (n : Nat) :
    (List.range (n + 1)).countP p =
      (List.range n).countP (fun i => p (i + 1)) + (if p 0 = true then 1 else 0) := by
  rw [List.range_succ_eq_map]
  simp only [List.countP_cons, List.countP_map, Function.comp_def]

private theorem om_zf_flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem om_zf_flatMap_congr_left {α β : Type} {l : List α} {f g : α → List β}
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.flatMap_cons]
      rw [h a (by simp)]
      rw [ih (by intro b hb; exact h b (by simp [hb]))]

private theorem om_zf_zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem om_zf_take_of_length_le {α : Type} (xs : List α) (n : Nat) (h : xs.length ≤ n) :
    xs.take n = xs := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      cases n with
      | zero => simp at h
      | succ n =>
          rw [List.take_succ_cons]
          congr 1
          apply ih n
          simp only [List.length_cons] at h
          omega

private theorem om_zf_range_split (a b : Nat) :
    List.range (a + b + 1) = List.range (a + 1) ++ (List.range b).map (fun k => a + 1 + k) := by
  induction b with
  | zero => simp
  | succ b ih =>
      rw [show a + (b + 1) + 1 = (a + b + 1) + 1 by omega]
      rw [List.range_succ]
      rw [ih]
      rw [show List.range (b + 1) = List.range b ++ [b] by rw [List.range_succ]]
      simp only [List.map_append, List.map_cons, List.map_nil]
      rw [List.append_assoc]
      rw [show a + b + 1 = a + 1 + b by omega]

private theorem om_zf_beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor <;> intro h <;> exact beq_iff_eq.mpr (beq_iff_eq.mp h).symm

private theorem om_zf_beq_add_right (a b s : Int) : (a + b == s + b) = (a == s) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; omega)
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; rw [this])

private theorem om_zf_beq_add_left (a b s : Int) : (b + a == b + s) = (a == s) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; omega)
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; rw [this])

private theorem om_zf_beq_self_add (b s : Int) : (b == b + s) = (s == 0) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; omega)
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; rw [this]; simp)

private theorem om_zf_map_congr_left {α β : Type} {l : List α} {f g : α → β}
    (h : ∀ a ∈ l, f a = g a) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons]
      rw [h a (by simp)]
      rw [ih (by intro b hb; exact h b (by simp [hb]))]
/-! #### Pair/tie helpers -/

private def om_zf_tieCount (l : List Int) : Nat :=
  (pairs l).countP (fun q : Int × Int => q.1 == q.2)

private theorem om_zf_pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, om_zf_zipIdx_add]
  rw [om_zf_flatMap_map]

private theorem om_zf_tieCount_cons (a : Int) (l : List Int) :
    om_zf_tieCount (a :: l) = om_zf_tieCount l + l.countP (fun b : Int => a == b) := by
  unfold om_zf_tieCount
  rw [om_zf_pairs_cons]
  simp only [List.countP_append, List.countP_map, Function.comp_def]
  omega

private theorem om_zf_tieCount_shift (b : Int) (l : List Int) :
    om_zf_tieCount (l.map (fun s => s + b)) = om_zf_tieCount l := by
  induction l with
  | nil => simp [om_zf_tieCount, pairs]
  | cons a as ih =>
      rw [List.map_cons]
      rw [om_zf_tieCount_cons, om_zf_tieCount_cons, ih]
      rw [om_zf_countP_map_comp]
      apply congrArg (fun n => om_zf_tieCount as + n)
      apply om_zf_countP_congr_left
      intro s hs
      exact om_zf_beq_add_right a b s

/-! #### Windows / prefixes / suffixes structure -/

private theorem om_zf_prefs_cons (b : Int) (z : Series) :
    prefs (b :: z) = b :: (prefs z).map (fun s => b + s) := by
  unfold prefs
  simp only [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map,
    Function.comp_def, List.take_succ_cons, List.sum_cons]
  congr 1 <;> simp

private theorem om_zf_suffs_cons (b : Int) (z : Series) :
    suffs (b :: z) = (b + z.sum) :: suffs z := by
  unfold suffs
  simp only [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map,
    Function.comp_def, List.drop_succ_cons]
  simp

private theorem om_zf_windows_cons (b : Int) (z : Series) :
    windows (b :: z) = (List.range (z.length + 1)).map (fun j => b :: z.take j) ++ windows z := by
  unfold windows
  simp only [List.length_cons]
  conv => lhs; rw [List.range_succ_eq_map]
  simp only [List.flatMap_cons]
  have hzero : z.length + 1 - 0 = z.length + 1 := by omega
  rw [hzero]
  have hfirst : (List.range (z.length + 1)).map
        (fun j => ((b :: z).drop 0).take (j + 1)) =
      (List.range (z.length + 1)).map (fun j => b :: z.take j) := by
    apply om_zf_map_congr_left
    intro j hj
    simp [List.take_succ_cons]
  rw [hfirst]
  apply congrArg (fun l => (List.range (z.length + 1)).map (fun j => b :: z.take j) ++ l)
  rw [om_zf_flatMap_map]
  apply om_zf_flatMap_congr_left
  intro i hi
  have hlt : i < z.length := List.mem_range.mp hi
  have hsub : z.length + 1 - (i + 1) = z.length - i := by omega
  rw [hsub]
  congr 1

private theorem om_zf_measure_cons (b : Int) (z : Series) :
    ZeroFrames.measure (b :: z) = ZeroFrames.measure z +
      ((List.range (z.length + 1)).countP (fun j => b + (z.take j).sum == 0) : Int) := by
  unfold ZeroFrames.measure
  rw [om_zf_windows_cons]
  simp only [List.countP_append, List.countP_map, Function.comp_def, List.sum_cons]
  omega

private theorem om_zf_mem_windows_mem {x w : Series} (hw : w ∈ windows x) :
    ∀ a ∈ w, a ∈ x := by
  induction x with
  | nil => simp [windows] at hw
  | cons b z ih =>
      rw [om_zf_windows_cons] at hw
      rcases List.mem_append.mp hw with hw | hw
      · rcases List.mem_map.mp hw with ⟨j, hj, rfl⟩
        intro a ha
        simp only [List.mem_cons] at ha
        rcases ha with rfl | ha
        · simp
        · exact List.mem_cons_of_mem b (List.mem_of_mem_take ha)
      · intro a ha
        exact List.mem_cons_of_mem b (ih hw a ha)

private theorem om_zf_sum_take_append_right (x y : Series) (k : Nat) :
    ((x ++ y).take (x.length + k + 1)).sum = x.sum + (y.take (k + 1)).sum := by
  rw [List.take_append]
  have hsub : x.length + k + 1 - x.length = k + 1 := by omega
  rw [hsub, List.sum_append]
  rw [om_zf_take_of_length_le x (x.length + k + 1) (by omega)]

/-! #### Nonnegative sum helper -/

private theorem om_zf_sum_nonneg_of_nonneg {w : List Int} (hw : ∀ a ∈ w, 0 ≤ a) : 0 ≤ w.sum := by
  induction w with
  | nil => simp
  | cons a as ih =>
      have ha : 0 ≤ a := hw a (by simp)
      have has : ∀ v ∈ as, 0 ≤ v := by intro v hv; exact hw v (by simp [hv])
      have hsum : 0 ≤ as.sum := ih has
      simp only [List.sum_cons]
      omega

private theorem om_zf_all_zero_of_nonneg_sum_zero {w : List Int} (hw : ∀ a ∈ w, 0 ≤ a)
    (hsum : w.sum = 0) : ∀ a ∈ w, a = 0 := by
  induction w with
  | nil => intro a ha; simp at ha
  | cons b bs ih =>
      have hb : 0 ≤ b := hw b (by simp)
      have hbs : ∀ a ∈ bs, 0 ≤ a := by intro a ha; exact hw a (by simp [ha])
      have htail : 0 ≤ bs.sum := om_zf_sum_nonneg_of_nonneg hbs
      have hb0 : b = 0 := by
        simp only [List.sum_cons] at hsum
        omega
      have htail0 : bs.sum = 0 := by
        simp only [List.sum_cons] at hsum
        omega
      intro a ha
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · exact hb0
      · exact ih hbs htail0 a ha

private theorem om_zf_all_zero_sum_zero {w : List Int} (h : ∀ a ∈ w, a = 0) : w.sum = 0 := by
  induction w with
  | nil => simp
  | cons b bs ih =>
      have hb : b = 0 := h b (by simp)
      have hbs : ∀ a ∈ bs, a = 0 := by intro a ha; exact h a (by simp [ha])
      simp only [List.sum_cons]
      rw [hb, ih hbs]
      simp

/-! #### Prefix-list tie counting -/

private def om_zf_prefixList (x : Series) : List Int := 0 :: prefs x

private theorem om_zf_tieCount_prefixList_cons (b : Int) (z : Series) :
    om_zf_tieCount (om_zf_prefixList (b :: z)) = om_zf_tieCount (om_zf_prefixList z) +
      (List.range (z.length + 1)).countP (fun j => b + (z.take j).sum == 0) := by
  have hpre : om_zf_prefixList (b :: z) = [0, b] ++ (prefs z).map (fun s => b + s) := by
    unfold om_zf_prefixList prefs
    simp only [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map,
      Function.comp_def, List.take_succ_cons, List.sum_cons]
    congr 1 <;> simp
  rw [hpre]
  change om_zf_tieCount (0 :: b :: (prefs z).map (fun s => b + s)) = _
  rw [om_zf_tieCount_cons, om_zf_tieCount_cons, List.countP_cons]
  have hshift : om_zf_tieCount ((prefs z).map (fun s => b + s)) = om_zf_tieCount (prefs z) := by
    simpa [Int.add_comm] using om_zf_tieCount_shift b (prefs z)
  have hc1 : ((prefs z).map (fun s => b + s)).countP (fun s => b == s) =
      (prefs z).countP (fun s => s == 0) := by
    rw [om_zf_countP_map_comp]
    apply om_zf_countP_congr_left
    intro s hs
    exact om_zf_beq_self_add b s
  have hc2 : ((prefs z).map (fun s => b + s)).countP (fun s => 0 == s) =
      (List.range z.length).countP (fun i => b + (z.take (i + 1)).sum == 0) := by
    rw [om_zf_countP_map_comp]
    unfold prefs
    rw [om_zf_countP_map_comp]
    apply om_zf_countP_congr_left
    intro i hi
    exact om_zf_beq_comm_int 0 (b + (z.take (i + 1)).sum)
  have htp : om_zf_tieCount (om_zf_prefixList z) = om_zf_tieCount (prefs z) + (prefs z).countP (fun s => 0 == s) := by
    unfold om_zf_prefixList
    rw [om_zf_tieCount_cons]
  have hcomm : (prefs z).countP (fun s => s == 0) = (prefs z).countP (fun s => 0 == s) := by
    apply om_zf_countP_congr_left
    intro s hs
    exact om_zf_beq_comm_int s 0
  have hrange := om_zf_countP_succ_range (fun j => b + (z.take j).sum == 0) z.length
  have hzero : z.take 0 = ([] : Series) := by rfl
  rw [hshift, hc1, hcomm, hc2, htp, hrange]
  simp only [hzero, List.sum_nil]
  have hb0 : (0 == b) = (b == 0) := om_zf_beq_comm_int 0 b
  have hb00 : (b + 0 == 0) = (b == 0) := by simp
  rw [hb0, hb00]

private theorem om_zf_measure_eq_tieCount_prefixList (x : Series) :
    ZeroFrames.measure x = (om_zf_tieCount (om_zf_prefixList x) : Int) := by
  induction x with
  | nil => simp [ZeroFrames.measure, windows, om_zf_tieCount, om_zf_prefixList, prefs, pairs]
  | cons b z ih =>
      rw [om_zf_measure_cons, ih, om_zf_tieCount_prefixList_cons]
      omega
private theorem om_zf_tieCount_nodup_zero_iff (l : List Int) : om_zf_tieCount l = 0 ↔ l.Nodup := by
  induction l with
  | nil => simp [om_zf_tieCount, pairs]
  | cons a as ih =>
      rw [om_zf_tieCount_cons]
      have hcount : as.countP (fun b : Int => a == b) = 0 ↔ a ∉ as := by
        rw [List.countP_eq_zero]
        constructor
        · intro h ha
          have hb := h a ha
          simp at hb
        · intro h b hb
          intro heq
          exact h (by
            have : a = b := beq_iff_eq.mp heq
            simpa [this] using hb)
      constructor
      · intro h
        rw [List.nodup_cons]
        constructor
        · exact hcount.mp (by omega)
        · exact ih.mp (by omega)
      · intro h
        rw [List.nodup_cons] at h
        have h1 : as.countP (fun b : Int => a == b) = 0 := hcount.mpr h.1
        have h2 : om_zf_tieCount as = 0 := ih.mpr h.2
        omega
-- END om-helpers

theorem sound_ZeroFrames_PrefixCollision : ZeroFrames.PrefixCollision.Goal := by
  intro x
  unfold TiePairs.measure
  simpa [om_zf_prefixList, om_zf_tieCount] using om_zf_measure_eq_tieCount_prefixList x

theorem sound_ZeroFrames_BoundaryBalances : ZeroFrames.BoundaryBalances.Goal := by
  intro x
  induction x with
  | nil =>
      intro y
      simp [ZeroFrames.measure, windows, suffs]
  | cons b xs ih =>
      intro y
      change ZeroFrames.measure (b :: (xs ++ y)) =
        ZeroFrames.measure (b :: xs) + ZeroFrames.measure y +
          ((suffs (b :: xs)).map (fun s => (prefs y).count (-s))).sum
      conv => lhs; rw [om_zf_measure_cons]
      rw [ih y]
      rw [om_zf_measure_cons]
      rw [om_zf_suffs_cons]
      simp only [List.map_cons, List.sum_cons, List.count_eq_countP]
      have hsplit :
          (List.range (xs.length + y.length + 1)).countP
              (fun j => b + ((xs ++ y).take j).sum == 0) =
            (List.range (xs.length + 1)).countP (fun j => b + (xs.take j).sum == 0) +
              (prefs y).countP (fun s => s == -(b + xs.sum)) := by
        rw [om_zf_range_split]
        simp only [List.countP_append, List.countP_map, Function.comp_def]
        have hfirst : (List.range (xs.length + 1)).countP
            (fun j => b + ((xs ++ y).take j).sum == 0) =
            (List.range (xs.length + 1)).countP (fun j => b + (xs.take j).sum == 0) := by
          apply om_zf_countP_congr_left
          intro j hj
          have hjle : j ≤ xs.length := by
            have := List.mem_range.mp hj
            omega
          rw [List.take_append_of_le_length hjle]
        rw [hfirst]
        apply congrArg (fun n => (List.range (xs.length + 1)).countP
          (fun j => b + (xs.take j).sum == 0) + n)
        unfold prefs
        rw [om_zf_countP_map_comp]
        apply om_zf_countP_congr_left
        intro k hk
        have hklt : k < y.length := List.mem_range.mp hk
        rw [show xs.length + 1 + k = xs.length + k + 1 by omega]
        rw [om_zf_sum_take_append_right]
        apply Bool.eq_iff_iff.mpr
        constructor
        · intro hleft
          have hz : b + (xs.sum + (y.take (k + 1)).sum) = 0 := beq_iff_eq.mp hleft
          have hsval : (y.take (k + 1)).sum = -(b + xs.sum) := by omega
          exact beq_iff_eq.mpr hsval
        · intro hright
          have hsval : (y.take (k + 1)).sum = -(b + xs.sum) := beq_iff_eq.mp hright
          apply beq_iff_eq.mpr
          omega
      rw [show (xs ++ y).length = xs.length + y.length by simp]
      rw [hsplit]
      omega

theorem sound_ZeroFrames_Nonnegative : ZeroFrames.Nonnegative.Goal := by
  intro x hx
  unfold ZeroFrames.measure
  apply congrArg (fun n : Nat => (n : Int))
  apply om_zf_countP_congr_left
  intro w hw
  have hwmem : ∀ a ∈ w, a ∈ x := om_zf_mem_windows_mem hw
  have hnonneg : ∀ a ∈ w, 0 ≤ a := by
    intro a ha
    exact hx a (hwmem a ha)
  by_cases hs : w.sum == 0
  · have hs' : w.sum = 0 := beq_iff_eq.mp hs
    have hall : ∀ a ∈ w, a = 0 := om_zf_all_zero_of_nonneg_sum_zero hnonneg hs'
    have hallb : w.all (fun a => a == 0) = true := by
      exact List.all_eq_true.mpr (by intro a ha; exact beq_iff_eq.mpr (hall a ha))
    simp [hs, hallb]
  · have hallnot : w.all (fun a => a == 0) = false := by
      cases hb : w.all (fun a => a == 0)
      · rfl
      · have hall' := List.all_eq_true.mp hb
        have hz : w.sum = 0 := om_zf_all_zero_sum_zero (by
          intro a ha
          exact beq_iff_eq.mp (hall' a ha))
        exact False.elim (hs (by simp [hz]))
    simp [hs, hallnot]

theorem sound_ZeroFrames_EarliestPrune : ZeroFrames.EarliestPrune.Goal := by
  intro x
  constructor
  · intro hzero
    have hm := om_zf_measure_eq_tieCount_prefixList x
    rw [hzero] at hm
    have ht : om_zf_tieCount (om_zf_prefixList x) = 0 := by omega
    simpa [om_zf_prefixList] using (om_zf_tieCount_nodup_zero_iff (om_zf_prefixList x)).mp ht
  · intro hnodup
    have ht : om_zf_tieCount (om_zf_prefixList x) = 0 :=
      (om_zf_tieCount_nodup_zero_iff (om_zf_prefixList x)).mpr (by simpa [om_zf_prefixList] using hnodup)
    have hm := om_zf_measure_eq_tieCount_prefixList x
    rw [ht] at hm
    omega

/-! ## EdgeCurvature

Ported from the (unbuilt) `Atlas_OrderedMeasures_EdgeCurvature_Remaining`
development; helpers renamed with the `om_ec_` prefix. -/

-- BEGIN om-helpers
private theorem om_ec_adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem om_ec_delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, om_ec_adj_cons₂]

private def om_ec_d2 (x : Series) : Series := delta (delta x)

private theorem om_ec_d2_nil : om_ec_d2 [] = [] := by simp [om_ec_d2, delta, adj]
private theorem om_ec_d2_singleton (_a : Int) : om_ec_d2 [_a] = [] := by simp [om_ec_d2, delta, adj]
private theorem om_ec_d2_pair (a b : Int) : om_ec_d2 [a, b] = [] := by simp [om_ec_d2, delta, adj]
private theorem om_ec_d2_take2 (t : Series) : om_ec_d2 (t.take 2) = [] := by
  cases t with
  | nil => simp [om_ec_d2_nil]
  | cons a xs =>
      cases xs with
      | nil => simp [om_ec_d2_singleton]
      | cons b ys => simp [om_ec_d2_pair]

private theorem om_ec_d2_cons₃ (a b c : Int) (xs : Series) :
    om_ec_d2 (a :: b :: c :: xs) = (c - 2*b + a) :: om_ec_d2 (b :: c :: xs) := by
  unfold om_ec_d2
  rw [om_ec_delta_cons₂ a b (c :: xs)]
  rw [om_ec_delta_cons₂ b c xs]
  rw [om_ec_delta_cons₂ (b - a) (c - b) (delta (c :: xs))]
  simp
  omega

private theorem om_ec_d2_triple (a b c : Int) : om_ec_d2 [a, b, c] = [c - 2*b + a] := by
  rw [om_ec_d2_cons₃ a b c []]
  simp [om_ec_d2_pair]

private theorem om_ec_delta_length (x : Series) : (delta x).length = x.length - 1 := by
  induction x with
  | nil => simp [delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b bs =>
          rw [om_ec_delta_cons₂]
          simp only [List.length_cons]
          have hlen : (b :: bs).length = bs.length + 1 := by simp
          rw [hlen] at ih
          simp only [Nat.add_sub_cancel] at ih
          omega

private theorem om_ec_d2_length (x : Series) : (om_ec_d2 x).length = x.length - 2 := by
  unfold om_ec_d2
  rw [om_ec_delta_length]
  rw [om_ec_delta_length]
  omega

private theorem om_ec_zipIdx_add (xs : Series) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private def om_ec_shiftAffine (a c : Int) (x : Series) : Series :=
  x.zipIdx.map (fun (v, i) => v + a * (i : Int) + c)

private theorem om_ec_shiftAffine_cons (a c : Int) (v : Int) (xs : Series) :
    om_ec_shiftAffine a c (v :: xs) = (v + c) :: om_ec_shiftAffine a (c + a) xs := by
  unfold om_ec_shiftAffine
  simp only [List.zipIdx_cons, List.map_cons]
  have hzip := om_ec_zipIdx_add xs 0 1
  simp only [Nat.zero_add] at hzip
  rw [hzip]
  simp only [List.map_map]
  congr 1
  · simp
  · apply List.map_congr_left
    intro p hp
    rcases p with ⟨w, i⟩
    dsimp
    rw [Int.mul_add]
    simp
    omega

private theorem om_ec_d2_shiftAffine (a c : Int) (x : Series) :
    om_ec_d2 (om_ec_shiftAffine a c x) = om_ec_d2 x := by
  induction x generalizing c with
  | nil => simp [om_ec_d2, om_ec_shiftAffine, delta, adj]
  | cons v xs ih =>
      cases xs with
      | nil => simp [om_ec_d2, om_ec_shiftAffine, delta, adj]
      | cons w xs =>
          cases xs with
          | nil => simp [om_ec_d2, om_ec_shiftAffine, delta, adj]
          | cons u xs =>
              rw [om_ec_shiftAffine_cons a c v (w :: u :: xs)]
              rw [om_ec_shiftAffine_cons a (c + a) w (u :: xs)]
              rw [om_ec_shiftAffine_cons a (c + a + a) u xs]
              rw [om_ec_d2_cons₃]
              have htail := ih (c + a)
              rw [om_ec_shiftAffine_cons a (c + a) w (u :: xs)] at htail
              rw [om_ec_shiftAffine_cons a (c + a + a) u xs] at htail
              rw [htail]
              congr 1
              dsimp
              omega

private def om_ec_dBridge (x y : Series) : Series :=
  (x.getLast?.bind (fun a => y.head?.map (fun b => b - a))).toList

private theorem om_ec_delta_append (x y : Series) :
    delta (x ++ y) = delta x ++ om_ec_dBridge x y ++ delta y := by
  induction x with
  | nil => simp [delta, adj, om_ec_dBridge]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil => simp [delta, adj, om_ec_dBridge]
          | cons b ys => simp [delta, adj, om_ec_dBridge]
      | cons b bs =>
          simp only [List.cons_append]
          rw [om_ec_delta_cons₂ a b (bs ++ y)]
          simp only [List.cons_append] at ih
          rw [ih]
          rw [om_ec_delta_cons₂ a b bs]
          simp [om_ec_dBridge]

private theorem om_ec_delta_reverse (x : Series) :
    delta x.reverse = (delta x).reverse.map (fun v => -v) := by
  induction x with
  | nil => simp [delta, adj]
  | cons v xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons w ws =>
          rw [List.reverse_cons]
          rw [om_ec_delta_append]
          simp [om_ec_dBridge]
          rw [List.reverse_cons] at ih
          rw [ih]
          rw [om_ec_delta_cons₂ v w ws]
          simp [delta, adj]
          omega

private theorem om_ec_delta_map_neg (x : Series) :
    delta (x.map (fun v => -v)) = (delta x).map (fun v => -v) := by
  induction x with
  | nil => simp [delta, adj]
  | cons v xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons w ws =>
          simp only [List.map_cons] at ih ⊢
          rw [om_ec_delta_cons₂ (-v) (-w) (ws.map (fun v => -v))]
          rw [om_ec_delta_cons₂ v w ws]
          rw [ih]
          simp
          omega

private theorem om_ec_rev_neg_rev_neg (l : Series) :
    (l.reverse.map (fun v => -v)).reverse.map (fun v => -v) = l := by
  rw [← List.map_reverse, List.reverse_reverse]
  simp [List.map_map, Function.comp_def]

private theorem om_ec_d2_reverse (x : Series) : om_ec_d2 x.reverse = (om_ec_d2 x).reverse := by
  unfold om_ec_d2
  rw [om_ec_delta_reverse]
  rw [om_ec_delta_map_neg]
  have h := om_ec_delta_reverse (delta x).reverse
  simp only [List.reverse_reverse] at h
  have h' := congrArg (fun l : Series => l.reverse.map (fun v => -v)) h.symm
  have hA : delta ((delta x).reverse) = (delta (delta x)).reverse.map (fun v => -v) := by
    simpa [List.map_reverse, List.map_map, Function.comp_def] using h'
  rw [hA]
  simp [List.map_map, Function.comp_def]

private def om_ec_suffix2 : Series → Series
  | [] => []
  | [a] => [a]
  | a :: b :: [] => [a, b]
  | _a :: b :: c :: xs => om_ec_suffix2 (b :: c :: xs)

private theorem om_ec_suffix2_eq : ∀ x : Series, (x.reverse.take 2).reverse = om_ec_suffix2 x
  | [] => by simp [om_ec_suffix2]
  | [a] => by simp [om_ec_suffix2]
  | a :: b :: [] => by simp [om_ec_suffix2]
  | a :: b :: c :: xs => by
      have ih := om_ec_suffix2_eq (b :: c :: xs)
      simp only [om_ec_suffix2]
      rw [List.reverse_cons]
      have hlen : 2 ≤ (b :: c :: xs).reverse.length := by simp
      rw [List.take_append_of_le_length hlen]
      exact ih

private theorem om_ec_d2_append_take2 : ∀ (p t : Series),
    om_ec_d2 (p ++ t) = om_ec_d2 (p ++ t.take 2) ++ om_ec_d2 t
  | [], t => by simp [om_ec_d2_take2]
  | [a], t => by
      cases t with
      | nil => simp [om_ec_d2_nil]
      | cons b bs =>
          cases bs with
          | nil => simp [om_ec_d2_singleton]
          | cons c cs =>
              change om_ec_d2 (a :: b :: c :: cs) = om_ec_d2 [a, b, c] ++ om_ec_d2 (b :: c :: cs)
              rw [om_ec_d2_cons₃, om_ec_d2_triple]
              simp
  | a :: b :: [], t => by
      cases t with
      | nil => simp [om_ec_d2_nil, om_ec_d2_pair]
      | cons c cs =>
          cases cs with
          | nil => simp [om_ec_d2_triple, om_ec_d2_singleton]
          | cons d ds =>
              change om_ec_d2 (a :: b :: c :: d :: ds) = om_ec_d2 [a, b, c, d] ++ om_ec_d2 (c :: d :: ds)
              rw [om_ec_d2_cons₃ a b c (d :: ds), om_ec_d2_cons₃ a b c [d],
                om_ec_d2_cons₃ b c d ds, om_ec_d2_cons₃ b c d []]
              simp [om_ec_d2_pair]
  | a :: b :: c :: xs, t => by
      have ih := om_ec_d2_append_take2 (b :: c :: xs) t
      change om_ec_d2 (b :: c :: (xs ++ t)) = om_ec_d2 (b :: c :: (xs ++ t.take 2)) ++ om_ec_d2 t at ih
      change om_ec_d2 (a :: b :: c :: (xs ++ t)) =
        om_ec_d2 (a :: b :: c :: (xs ++ t.take 2)) ++ om_ec_d2 t
      rw [om_ec_d2_cons₃, om_ec_d2_cons₃, ih]
      simp

private theorem om_ec_d2_append_suffix2 : ∀ (x t : Series), t.length ≤ 2 →
    om_ec_d2 (x ++ t) = om_ec_d2 x ++ om_ec_d2 (om_ec_suffix2 x ++ t)
  | [], t, ht => by simp [om_ec_suffix2, om_ec_d2_nil]
  | [a], t, ht => by simp [om_ec_suffix2, om_ec_d2_singleton]
  | a :: b :: [], t, ht => by simp [om_ec_suffix2, om_ec_d2_pair]
  | a :: b :: c :: xs, t, ht => by
      cases xs with
      | nil =>
          change om_ec_d2 (a :: b :: c :: t) = om_ec_d2 [a, b, c] ++ om_ec_d2 ([b, c] ++ t)
          rw [om_ec_d2_cons₃, om_ec_d2_triple]
          simp
      | cons d ys =>
          have ih := om_ec_d2_append_suffix2 (b :: c :: d :: ys) t ht
          change om_ec_d2 (b :: c :: d :: (ys ++ t)) =
            om_ec_d2 (b :: c :: d :: ys) ++ om_ec_d2 (om_ec_suffix2 (b :: c :: d :: ys) ++ t) at ih
          change om_ec_d2 (a :: b :: c :: d :: (ys ++ t)) =
            om_ec_d2 (a :: b :: c :: d :: ys) ++ om_ec_d2 (om_ec_suffix2 (b :: c :: d :: ys) ++ t)
          rw [om_ec_d2_cons₃ a b c (d :: (ys ++ t)), om_ec_d2_cons₃ a b c (d :: ys), ih]
          simp

private theorem om_ec_d2_append4 (x y : Series) :
    om_ec_d2 (x ++ y) = om_ec_d2 x ++ om_ec_d2 (om_ec_suffix2 x ++ y.take 2) ++ om_ec_d2 y := by
  rw [om_ec_d2_append_take2]
  rw [om_ec_d2_append_suffix2 x (y.take 2) (by simpa using (Nat.min_le_left 2 y.length))]

private theorem om_ec_sum_sq_eq_of_all_eq (l : Series) (a : Int)
    (h : ∀ v ∈ l, v = a) :
    (l.map (fun v => v ^ 2)).sum = (l.length : Int) * a ^ 2 := by
  induction l with
  | nil => simp
  | cons v vs ih =>
      have hv : v = a := h v (by simp)
      have ht : ∀ w ∈ vs, w = a := by intro w hw; exact h w (by simp [hw])
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [hv, ih ht]
      have hcast : ((vs.length + 1 : Nat) : Int) = (vs.length : Int) + 1 := by omega
      rw [hcast, Int.add_mul]
      simp
      omega

private theorem om_ec_pow_two_int (a : Int) : a ^ 2 = a * a := by
  rw [Int.pow_succ, Int.pow_one]

private theorem om_ec_int_mul_self_nonneg (a : Int) : 0 ≤ a * a := by
  rw [← Int.natAbs_mul_self (a := a)]
  exact Int.natCast_nonneg _

private theorem om_ec_sq_nonneg_int (a : Int) : 0 ≤ a ^ 2 := by
  rw [om_ec_pow_two_int]
  exact om_ec_int_mul_self_nonneg a

private theorem om_ec_sq_eq_zero_int (a : Int) (h : a ^ 2 = 0) : a = 0 := by
  have hmul : a * a = 0 := by simpa [om_ec_pow_two_int] using h
  rcases (Int.mul_eq_zero.mp hmul) with hz | hz <;> exact hz

private theorem om_ec_sum_sq_nonneg (l : Series) : 0 ≤ (l.map (fun v => v ^ 2)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := om_ec_sq_nonneg_int a
      omega

private theorem om_ec_sq_le_sum_sq {l : Series} {v : Int} (hv : v ∈ l) :
    v ^ 2 ≤ (l.map (fun w => w ^ 2)).sum := by
  induction l with
  | nil => simp at hv
  | cons a as ih =>
      simp only [List.mem_cons] at hv
      simp only [List.map_cons, List.sum_cons]
      rcases hv with rfl | hv
      · have ht := om_ec_sum_sq_nonneg as
        omega
      · have ht := ih hv
        have ha := om_ec_sq_nonneg_int a
        omega

private theorem om_ec_sum_sq_eq_zero_iff_all_zero (l : Series) :
    (l.map (fun v => v ^ 2)).sum = 0 ↔ ∀ v ∈ l, v = 0 := by
  constructor
  · intro hs v hv
    have hle := om_ec_sq_le_sum_sq hv
    have hvn := om_ec_sq_nonneg_int v
    have hsq : v ^ 2 = 0 := by omega
    exact om_ec_sq_eq_zero_int v hsq
  · intro h
    induction l with
    | nil => simp
    | cons a as ih =>
        simp only [List.map_cons, List.sum_cons]
        have ha : a = 0 := h a (by simp)
        have ht : ∀ w ∈ as, w = 0 := by intro w hw; exact h w (by simp [hw])
        rw [ha, ih ht]
        simp

private def om_ec_arith (a d : Int) (n : Nat) : Series :=
  (List.range n).map (fun i : Nat => a + d * (i : Int))

private theorem om_ec_arith_length (a d : Int) (n : Nat) : (om_ec_arith a d n).length = n := by
  simp [om_ec_arith]

private theorem om_ec_arith_succ (a d : Int) (n : Nat) :
    om_ec_arith a d (n + 1) = a :: om_ec_arith (a + d) d n := by
  unfold om_ec_arith
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih]
      rw [List.range_succ, List.map_append]
      simp only [List.cons_append]
      congr 1
      simp only [List.map_cons, List.map_nil]
      have hcast : ((n + 1 : Nat) : Int) = (n : Int) + 1 := by omega
      rw [hcast, Int.mul_add]
      simp
      omega

private theorem om_ec_d2_zero_arith_canon : ∀ (a b : Int) (xs : Series),
    (∀ v ∈ om_ec_d2 (a :: b :: xs), v = 0) →
    a :: b :: xs = om_ec_arith a (b - a) (a :: b :: xs).length
  | a, b, [], h => by
      change [a, b] = [a + (b - a) * (0 : Int), a + (b - a) * (1 : Int)]
      simp
      omega
  | a, b, c :: ys, h => by
      have hhead : c - 2*b + a = 0 := by
        apply h
        rw [om_ec_d2_cons₃]
        simp
      have htail : ∀ v ∈ om_ec_d2 (b :: c :: ys), v = 0 := by
        intro v hv
        apply h
        rw [om_ec_d2_cons₃]
        simp [hv]
      have ihres := om_ec_d2_zero_arith_canon b c ys htail
      rw [ihres]
      have hd : c - b = b - a := by omega
      rw [hd]
      simp only [List.length_cons]
      rw [om_ec_arith_length]
      have hadd : a + (b - a) = b := by omega
      change a :: om_ec_arith b (b - a) (ys.length + 2) = om_ec_arith a (b - a) (ys.length + 3)
      rw [show ys.length + 3 = (ys.length + 2) + 1 by omega]
      rw [om_ec_arith_succ a (b - a) (ys.length + 2)]
      rw [hadd]

private theorem om_ec_d2_zero_arith (x : Series) (h : ∀ v ∈ om_ec_d2 x, v = 0) :
    ∃ a d : Int, x = om_ec_arith a d x.length := by
  cases x with
  | nil => exact ⟨0, 0, by simp [om_ec_arith]⟩
  | cons a xs =>
      cases xs with
      | nil => exact ⟨a, 0, by simp [om_ec_arith]⟩
      | cons b ys =>
          exact ⟨a, b - a, om_ec_d2_zero_arith_canon a b ys h⟩

private theorem om_ec_d2_replicate (a : Int) : ∀ n : Nat,
    om_ec_d2 (List.replicate n a) = List.replicate (n - 2) 0
  | 0 => by simp [om_ec_d2, delta, adj]
  | 1 => by simp [om_ec_d2, delta, adj]
  | 2 => by simp [om_ec_d2, delta, adj]
  | n + 3 => by
      change om_ec_d2 (a :: a :: a :: List.replicate n a) = List.replicate (n + 1) 0
      rw [om_ec_d2_cons₃]
      have ih := om_ec_d2_replicate a (n + 2)
      change om_ec_d2 (a :: a :: List.replicate n a) = List.replicate n 0 at ih
      rw [ih, List.replicate_succ]
      congr 1
      omega

private theorem om_ec_range_map_succ (f : Nat → Int × Nat) (n : Nat) :
    (List.range (n + 1)).map f = f 0 :: (List.range n).map (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih]
      rw [List.range_succ, List.map_append]
      simp

private theorem om_ec_zipIdx_replicate (n : Nat) :
    (List.replicate n (0 : Int)).zipIdx = (List.range n).map (fun i : Nat => (0, i)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.zipIdx_cons]
      rw [show (List.replicate n (0 : Int)).zipIdx 1 =
          ((List.replicate n (0 : Int)).zipIdx).map (fun p => (p.1, p.2 + 1)) by
            simpa using om_ec_zipIdx_add (List.replicate n (0 : Int)) 0 1]
      rw [ih, List.map_map]
      change (0, 0) :: (List.range n).map (fun i : Nat => (0, i + 1)) =
        (List.range (n + 1)).map (fun i : Nat => (0, i))
      rw [← om_ec_range_map_succ (fun i : Nat => (0, i)) n]

private theorem om_ec_arith_eq_shift (a d : Int) (n : Nat) :
    om_ec_arith a d n = om_ec_shiftAffine d a (List.replicate n 0) := by
  unfold om_ec_arith om_ec_shiftAffine
  rw [om_ec_zipIdx_replicate, List.map_map]
  apply List.map_congr_left
  intro i hi
  dsimp
  change a + d * (i : Int) = (0 : Int) + d * (i : Int) + a
  omega

private theorem om_ec_d2_arith (a d : Int) (n : Nat) :
    om_ec_d2 (om_ec_arith a d n) = List.replicate (n - 2) 0 := by
  rw [om_ec_arith_eq_shift]
  rw [om_ec_d2_shiftAffine]
  exact om_ec_d2_replicate 0 n
-- END om-helpers

theorem sound_EdgeCurvature_AffineTrend : EdgeCurvature.AffineTrend.Goal := by
  intro x a b
  change ((om_ec_d2 (x.zipIdx.map (fun (v, i) => v + a * (i : Int) + b))).map (fun v => v ^ 2)).sum =
    ((om_ec_d2 x).map (fun v => v ^ 2)).sum
  rw [show (x.zipIdx.map (fun (v, i) => v + a * (i : Int) + b)) = om_ec_shiftAffine a b x by rfl]
  rw [om_ec_d2_shiftAffine]

theorem sound_EdgeCurvature_Backward : EdgeCurvature.Backward.Goal := by
  intro x
  change ((om_ec_d2 x.reverse).map (fun v => v ^ 2)).sum = ((om_ec_d2 x).map (fun v => v ^ 2)).sum
  rw [om_ec_d2_reverse]
  simp

theorem sound_EdgeCurvature_EndpointRepair : EdgeCurvature.EndpointRepair.Goal := by
  intro x y
  change ((om_ec_d2 (x ++ y)).map (fun v => v ^ 2)).sum =
    ((om_ec_d2 x).map (fun v => v ^ 2)).sum + ((om_ec_d2 y).map (fun v => v ^ 2)).sum +
      (((om_ec_d2 ((x.reverse.take 2).reverse ++ y.take 2)).map (fun v => v ^ 2)).sum)
  rw [om_ec_d2_append4]
  rw [om_ec_suffix2_eq]
  simp
  omega

theorem sound_EdgeCurvature_FlatAcceleration : EdgeCurvature.FlatAcceleration.Goal := by
  intro x a h
  change ((om_ec_d2 x).map (fun v => v ^ 2)).sum = ((x.length - 2 : Nat) : Int) * a ^ 2
  rw [om_ec_sum_sq_eq_of_all_eq (om_ec_d2 x) a h]
  rw [om_ec_d2_length]

theorem sound_EdgeCurvature_TrendCertificate : EdgeCurvature.TrendCertificate.Goal := by
  intro x
  constructor
  · intro h
    change ((om_ec_d2 x).map (fun v => v ^ 2)).sum = 0 at h
    have hall : ∀ v ∈ om_ec_d2 x, v = 0 := (om_ec_sum_sq_eq_zero_iff_all_zero (om_ec_d2 x)).mp h
    rcases om_ec_d2_zero_arith x hall with ⟨a, d, hx⟩
    exact ⟨a, d, by simpa [om_ec_arith] using hx⟩
  · rintro ⟨a, d, hx⟩
    rw [hx]
    change ((om_ec_d2 (om_ec_arith a d x.length)).map (fun v => v ^ 2)).sum = 0
    rw [om_ec_d2_arith]
    simp

/-! ## PeakCount

Ported from the (unbuilt) `Atlas_OrderedMeasures_PeakCount_Remaining`
development; helpers renamed with the `om_pc_` prefix. `DuplicateLoss`
is a known-false goal, disproven in the built `OrderedMeasuresPeakCount`
module and re-exported at the bottom. -/

-- BEGIN om-helpers
/-! A peak of a series is a position `i` with `0 < i`, `i + 1 < length`,
`x[i-1] < x[i]` and `x[i+1] < x[i]`.  The following predicates and lemmas
restate `PeakCount.measure` as a count over indices, which makes locality
arguments (concatenation, order-preserving maps, single-point edits) direct. -/

private theorem om_pc_zipIdx_eq_range (x : Series) :
    x.zipIdx = (List.range x.length).map (fun i => (x[i]?.getD 0, i)) := by
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_zipIdx, List.getElem?_map]
  by_cases hi : i < x.length
  · have hr : (List.range x.length)[i]? = some i := by simp [hi]
    rw [hr]
    simp [hi, List.getElem?_eq_getElem hi]
  · have hr : (List.range x.length)[i]? = none := by simp [hi]
    rw [hr]
    simp [List.getElem?_eq_none (by omega : x.length ≤ i)]

private def om_pc_pk (x : Series) (i : Nat) : Bool :=
  decide (0 < i) && x[i - 1]?.any (fun a => a < x[i]?.getD 0) &&
    x[i + 1]?.any (fun b => b < x[i]?.getD 0)

private theorem om_pc_measure_eq_count_pk (x : Series) :
    PeakCount.measure x = ((List.range x.length).countP (om_pc_pk x) : Int) := by
  unfold PeakCount.measure
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  congr 1
  rw [om_pc_zipIdx_eq_range]
  rw [List.filter_map]
  simp only [List.length_map]
  congr 1

private theorem om_pc_pk_eq_of_bounds (x : Series) (i : Nat) (hi0 : 0 < i)
    (hi1 : i + 1 < x.length) :
    om_pc_pk x i = true ↔ x[i - 1] < x[i] ∧ x[i + 1] < x[i] := by
  have hi : i < x.length := by omega
  have him : i - 1 < x.length := by omega
  dsimp [om_pc_pk]
  rw [List.getElem?_eq_getElem hi, List.getElem?_eq_getElem him,
    List.getElem?_eq_getElem hi1]
  simp [hi0]

private theorem om_pc_pk_false_zero (x : Series) : om_pc_pk x 0 = false := by
  simp [om_pc_pk]

private theorem om_pc_pk_false_last (x : Series) : om_pc_pk x (x.length - 1) = false := by
  by_cases h : x.length = 0
  · simp [h, om_pc_pk]
  · have hnext : x.length - 1 + 1 = x.length := by omega
    dsimp [om_pc_pk]
    rw [hnext, List.getElem?_eq_none (by omega : x.length ≤ x.length)]
    simp

private theorem om_pc_pk_true_bounds (x : Series) {i : Nat} (h : om_pc_pk x i = true) :
    0 < i ∧ i + 1 < x.length := by
  constructor
  · by_cases hi : 0 < i
    · exact hi
    · dsimp [om_pc_pk] at h
      have hf : (decide (0 < i) : Bool) = false := by simp [hi]
      rw [hf] at h
      simp at h
  · by_cases hi : i + 1 < x.length
    · exact hi
    · have hge : x.length ≤ i + 1 := by omega
      dsimp [om_pc_pk] at h
      rw [List.getElem?_eq_none hge] at h
      simp at h

private theorem om_pc_pk_no_adjacent (x : Series) (i : Nat) :
    ¬ (om_pc_pk x i = true ∧ om_pc_pk x (i + 1) = true) := by
  intro h
  have hb1 := om_pc_pk_true_bounds x h.1
  have hb2 := om_pc_pk_true_bounds x h.2
  have h1 := (om_pc_pk_eq_of_bounds x i hb1.1 (by omega)).mp h.1
  have h2 := (om_pc_pk_eq_of_bounds x (i + 1) hb2.1 hb2.2).mp h.2
  have h2left : x[i] < x[i + 1] := by simpa using h2.1
  omega

private theorem om_pc_countP_congr {α : Type} (p q : α → Bool) (l : List α)
    (h : ∀ a ∈ l, p a = q a) : l.countP p = l.countP q := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  congr 1
  apply List.filter_congr
  intro a ha
  exact h a ha

private theorem om_pc_countP_range_succ (p : Nat → Bool) (n : Nat) :
    (List.range (n + 1)).countP p = (List.range n).countP p + (if p n then 1 else 0) := by
  rw [List.range_succ, List.countP_append]
  simp

private theorem om_pc_measure_eq_count_prefix (x : Series) :
    PeakCount.measure x = ((List.range (x.length - 1)).countP (om_pc_pk x) : Int) := by
  rw [om_pc_measure_eq_count_pk]
  by_cases h : x.length = 0
  · simp [h]
  · have hs : x.length = (x.length - 1) + 1 := by omega
    rw [hs, om_pc_countP_range_succ]
    have hf : om_pc_pk x (x.length - 1) = false := om_pc_pk_false_last x
    simp [hf]

private theorem om_pc_range_add (m n : Nat) :
    List.range (m + n) = List.range m ++ (List.range n).map (fun j => m + j) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [Nat.add_succ, List.range_succ, ih]
      simp [List.range_succ, Nat.add_assoc]

private theorem om_pc_measure_nil : PeakCount.measure ([] : Series) = 0 := by
  unfold PeakCount.measure
  simp

private theorem om_pc_measure_singleton (a : Int) : PeakCount.measure [a] = 0 := by
  unfold PeakCount.measure
  simp

private theorem om_pc_measure_two (a b : Int) : PeakCount.measure [a, b] = 0 := by
  unfold PeakCount.measure
  simp

private theorem om_pc_measure_three (a b c : Int) :
    PeakCount.measure [a, b, c] = (if a < b ∧ c < b then 1 else 0) := by
  by_cases h : a < b ∧ c < b <;>
    unfold PeakCount.measure <;> simp [List.countP_cons, h] <;> omega

private theorem om_pc_measure_four (a b c d : Int) :
    PeakCount.measure [a, b, c, d] =
      (if a < b ∧ c < b then 1 else 0) + (if b < c ∧ d < c then 1 else 0) := by
  by_cases h1 : a < b ∧ c < b <;> by_cases h2 : b < c ∧ d < c <;>
    unfold PeakCount.measure <;> simp [List.countP_cons, h1, h2] <;> omega

private theorem om_pc_measure_of_length_le_two (z : Series) (h : z.length ≤ 2) :
    PeakCount.measure z = 0 := by
  rw [om_pc_measure_eq_count_prefix]
  by_cases h0 : z.length = 0
  · simp [h0]
  · by_cases h1 : z.length = 1
    · simp [h1]
    · have h2 : z.length = 2 := by omega
      rw [h2]
      simp [om_pc_pk_false_zero]

private theorem om_pc_range_succ_from_zero (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun j => j + 1) := by
  rw [List.range_succ_eq_map]

private theorem om_pc_measure_eq_count_pk_shift (x : Series) :
    PeakCount.measure x =
      ((List.range (x.length - 2)).countP (fun j => om_pc_pk x (j + 1)) : Int) := by
  rw [om_pc_measure_eq_count_pk]
  by_cases h0 : x.length = 0
  · simp [h0]
  · by_cases h1 : x.length = 1
    · simp [h1, om_pc_pk_false_zero]
    · have hs : x.length = (x.length - 1) + 1 := by omega
      rw [hs, om_pc_range_succ_from_zero]
      simp only [List.countP_append, List.countP_cons, List.countP_nil, List.map_map,
        Function.comp_def, Nat.zero_add]
      rw [List.countP_map]
      have hz : om_pc_pk x 0 = false := om_pc_pk_false_zero x
      rw [hz]
      change ((List.range (x.length - 1)).countP (fun j => om_pc_pk x (j + 1)) : Int) =
        ((List.range (x.length - 2)).countP (fun j => om_pc_pk x (j + 1)) : Int)
      congr 1
      have hs2 : x.length - 1 = (x.length - 2) + 1 := by omega
      rw [hs2, List.range_succ, List.countP_append]
      simp only [List.countP_cons, List.countP_nil, Nat.add_zero]
      have hlast : om_pc_pk x (x.length - 2 + 1) = false := by
        have : x.length - 2 + 1 = x.length - 1 := by omega
        rw [this]
        exact om_pc_pk_false_last x
      rw [hlast]
      simp

private theorem om_pc_range_concat_split (m n : Nat) (hm : 0 < m) (hn : 0 < n) :
    List.range (m + n - 1) =
      List.range (m - 1) ++ ([m - 1] ++ (List.range (n - 1)).map (fun j => m + j)) := by
  have hsum : m + n - 1 = (m - 1) + n := by omega
  rw [hsum, om_pc_range_add]
  congr 1
  have hn' : n = (n - 1) + 1 := by omega
  rw [hn', om_pc_range_succ_from_zero]
  simp only [List.map_append, List.map_cons, List.map_nil, List.append_nil, List.map_map,
    Function.comp_def, Nat.add_zero]
  congr 1
  apply List.map_congr_left
  intro j hj
  omega

private theorem om_pc_getElem?_append_of_lt (x y : Series) {i : Nat} (h : i < x.length) :
    (x ++ y)[i]? = x[i]? := by
  rw [List.getElem?_append]
  split
  · rfl
  · omega

private theorem om_pc_getElem?_append_of_ge (x y : Series) {i : Nat} (h : x.length ≤ i) :
    (x ++ y)[i]? = y[i - x.length]? := by
  rw [List.getElem?_append]
  split
  · omega
  · rfl

private theorem om_pc_pk_false_of_ge (x : Series) (i : Nat) (h : x.length ≤ i) :
    om_pc_pk x i = false := by
  dsimp [om_pc_pk]
  have h1 : x.length ≤ i + 1 := by omega
  rw [List.getElem?_eq_none h1]
  simp

private theorem om_pc_pk_append_left (x y : Series) (i : Nat) (hi : i + 1 < x.length) :
    om_pc_pk (x ++ y) i = om_pc_pk x i := by
  have h0 : i < x.length := by omega
  have hm : i - 1 < x.length := by omega
  dsimp [om_pc_pk]
  rw [om_pc_getElem?_append_of_lt x y h0, om_pc_getElem?_append_of_lt x y hm,
    om_pc_getElem?_append_of_lt x y hi]

private theorem om_pc_pk_append_right (x y : Series) (j : Nat) (hj : 0 < j) :
    om_pc_pk (x ++ y) (x.length + j) = om_pc_pk y j := by
  have h1 : x.length ≤ x.length + j - 1 := by omega
  have h2 : x.length ≤ x.length + j := by omega
  have h3 : x.length ≤ x.length + j + 1 := by omega
  dsimp [om_pc_pk]
  rw [om_pc_getElem?_append_of_ge x y h1, om_pc_getElem?_append_of_ge x y h2,
    om_pc_getElem?_append_of_ge x y h3]
  have e1 : x.length + j - 1 - x.length = j - 1 := by omega
  have e2 : x.length + j - x.length = j := by omega
  have e3 : x.length + j + 1 - x.length = j + 1 := by omega
  rw [e1, e2, e3]
  have hi0 : 0 < x.length + j := by omega
  simp [hj, hi0]

private theorem om_pc_count_pk_shifted (x y : Series) :
    ((List.range (y.length - 1)).map (fun j => x.length + j)).countP (om_pc_pk (x ++ y)) =
      (if om_pc_pk (x ++ y) x.length then 1 else 0) +
        ((List.range (y.length - 2)).countP (fun j => om_pc_pk y (j + 1)) : Nat) := by
  by_cases hy : y.length = 0
  · have hyl : y = [] := (List.length_eq_zero_iff).mp hy
    subst y
    have hf : om_pc_pk x x.length = false := om_pc_pk_false_of_ge x x.length (by omega)
    simp [hf]
  · by_cases hy1 : y.length = 1
    · have hf : om_pc_pk (x ++ y) x.length = false := by
        dsimp [om_pc_pk]
        have hge : (x ++ y).length ≤ x.length + 1 := by simp [hy1]
        rw [List.getElem?_eq_none hge]
        simp
      simp [hf, hy1]
    · have hs : y.length - 1 = (y.length - 2) + 1 := by omega
      rw [hs, om_pc_range_succ_from_zero]
      simp only [List.map_append, List.map_cons, List.map_nil, List.append_nil,
        List.countP_append, List.countP_cons, List.countP_nil, Nat.add_zero]
      simp only [List.map_map, Function.comp_def]
      rw [List.countP_map]
      change ((List.range (y.length - 2)).countP (fun j => om_pc_pk (x ++ y) (x.length + (j + 1))) +
          (if om_pc_pk (x ++ y) x.length then 1 else 0)) =
        ((if om_pc_pk (x ++ y) x.length then 1 else 0) +
          (List.range (y.length - 2)).countP (fun j => om_pc_pk y (j + 1)))
      have hcnt : (List.range (y.length - 2)).countP (fun j => om_pc_pk (x ++ y) (x.length + (j + 1))) =
          (List.range (y.length - 2)).countP (fun j => om_pc_pk y (j + 1)) := by
        apply om_pc_countP_congr
        intro j hj
        have h := om_pc_pk_append_right x y (j + 1) (by omega)
        simpa using h
      omega

private theorem om_pc_measure_append_split (x y : Series) :
    PeakCount.measure (x ++ y) = PeakCount.measure x + PeakCount.measure y +
      (if om_pc_pk (x ++ y) (x.length - 1) then 1 else 0) +
      (if om_pc_pk (x ++ y) x.length then 1 else 0) := by
  by_cases hx : x.length = 0
  · have hxl : x = [] := (List.length_eq_zero_iff).mp hx
    subst x
    simp [om_pc_pk_false_zero, om_pc_measure_nil]
  · by_cases hy : y.length = 0
    · have hyl : y = [] := (List.length_eq_zero_iff).mp hy
      subst y
      simp [om_pc_pk_false_last, om_pc_pk_false_of_ge, om_pc_measure_nil]
    · rw [om_pc_measure_eq_count_prefix (x ++ y)]
      have hlen : (x ++ y).length = x.length + y.length := by simp
      rw [hlen, om_pc_range_concat_split x.length y.length (by omega) (by omega)]
      rw [List.countP_append, List.countP_append]
      simp only [List.countP_cons, List.countP_nil, Nat.add_zero]
      have hleft : (List.range (x.length - 1)).countP (om_pc_pk (x ++ y)) =
          (List.range (x.length - 1)).countP (om_pc_pk x) := by
        apply om_pc_countP_congr
        intro i hi
        apply om_pc_pk_append_left
        have hi' : i < x.length - 1 := by simpa using hi
        omega
      rw [hleft]
      rw [om_pc_count_pk_shifted]
      simp only [Int.natCast_add, Int.natCast_zero, Nat.zero_add]
      rw [← om_pc_measure_eq_count_prefix x, ← om_pc_measure_eq_count_pk_shift y]
      by_cases hA : om_pc_pk (x ++ y) (x.length - 1) = true <;>
        by_cases hB : om_pc_pk (x ++ y) x.length = true <;>
          simp [hA, hB] <;> ac_rfl

private theorem om_pc_measure_cons (a : Int) (y : Series) :
    PeakCount.measure (a :: y) = PeakCount.measure y + (if om_pc_pk (a :: y) 1 then 1 else 0) := by
  have h := om_pc_measure_append_split [a] y
  simpa [om_pc_measure_singleton, om_pc_pk_false_zero] using h

private theorem om_pc_pk_map_true_iff (x : Series) (f : Int → Int)
    (hf : ∀ a ∈ x, ∀ b ∈ x, (a < b ↔ f a < f b)) (i : Nat) :
    om_pc_pk (x.map f) i = true ↔ om_pc_pk x i = true := by
  constructor
  · intro h
    have hb := om_pc_pk_true_bounds (x.map f) h
    have hi0 : 0 < i := hb.1
    have hi1 : i + 1 < x.length := by simpa using hb.2
    have hi : i < x.length := by omega
    have him : i - 1 < x.length := by omega
    have hbmap : i + 1 < (x.map f).length := by simpa using hi1
    have hlt := (om_pc_pk_eq_of_bounds (x.map f) i hb.1 hbmap).mp h
    have hleft : f (x[i - 1]) < f (x[i]) := by
      simpa [List.getElem?_map, List.getElem?_eq_getElem him,
        List.getElem?_eq_getElem hi] using hlt.1
    have hright : f (x[i + 1]) < f (x[i]) := by
      simpa [List.getElem?_map, List.getElem?_eq_getElem hi1,
        List.getElem?_eq_getElem hi] using hlt.2
    have hm0 : x[i - 1] ∈ x := List.getElem_mem him
    have hmi : x[i] ∈ x := List.getElem_mem hi
    have hm1 : x[i + 1] ∈ x := List.getElem_mem hi1
    exact (om_pc_pk_eq_of_bounds x i hi0 hi1).mpr
      ⟨(hf (x[i - 1]) hm0 (x[i]) hmi).mpr hleft,
       (hf (x[i + 1]) hm1 (x[i]) hmi).mpr hright⟩
  · intro h
    have hb := om_pc_pk_true_bounds x h
    have hi0 : 0 < i := hb.1
    have hi1 : i + 1 < x.length := hb.2
    have hbmap : i + 1 < (x.map f).length := by simpa using hi1
    have hlt := (om_pc_pk_eq_of_bounds x i hi0 hi1).mp h
    have hi : i < x.length := by omega
    have him : i - 1 < x.length := by omega
    have hm0 : x[i - 1] ∈ x := List.getElem_mem him
    have hmi : x[i] ∈ x := List.getElem_mem hi
    have hm1 : x[i + 1] ∈ x := List.getElem_mem hi1
    have hleft : f (x[i - 1]) < f (x[i]) := (hf (x[i - 1]) hm0 (x[i]) hmi).mp hlt.1
    have hright : f (x[i + 1]) < f (x[i]) := (hf (x[i + 1]) hm1 (x[i]) hmi).mp hlt.2
    apply (om_pc_pk_eq_of_bounds (x.map f) i hb.1 hbmap).mpr
    constructor
    · simpa [List.getElem?_map, List.getElem?_eq_getElem him,
        List.getElem?_eq_getElem hi] using hleft
    · simpa [List.getElem?_map, List.getElem?_eq_getElem hi1,
        List.getElem?_eq_getElem hi] using hright

private theorem om_pc_pk_map (x : Series) (f : Int → Int)
    (hf : ∀ a ∈ x, ∀ b ∈ x, (a < b ↔ f a < f b)) (i : Nat) :
    om_pc_pk (x.map f) i = om_pc_pk x i := by
  by_cases h : om_pc_pk x i = true
  · have h' : om_pc_pk (x.map f) i = true := (om_pc_pk_map_true_iff x f hf i).mpr h
    rw [h, h']
  · have hz : om_pc_pk x i = false := by
      cases p : om_pc_pk x i <;> simp_all
    have h' : om_pc_pk (x.map f) i = false := by
      have hn : om_pc_pk (x.map f) i ≠ true := by
        intro hh
        exact h ((om_pc_pk_map_true_iff x f hf i).mp hh)
      cases p : om_pc_pk (x.map f) i <;> simp_all
    rw [hz, h']

private theorem om_pc_last_two_cons_of_length (a : Int) (xs : Series) (h : 2 ≤ xs.length) :
    ((a :: xs).reverse.take 2).reverse = (xs.reverse.take 2).reverse := by
  rw [List.reverse_cons]
  have hle : 2 ≤ xs.reverse.length := by simpa using h
  rw [List.take_append_of_le_length (l₁ := xs.reverse) (l₂ := [a]) hle]

private theorem om_pc_boundary_two (a b : Int) (y : Series) :
    PeakCount.measure ([a, b] ++ y) = PeakCount.measure [a, b] + PeakCount.measure y +
      PeakCount.measure (([a, b].reverse.take 2).reverse ++ y.take 2) := by
  induction y with
  | nil =>
      simp [om_pc_measure_nil, om_pc_measure_two]
  | cons c ys =>
      cases ys with
      | nil =>
          have hbd : (([a, b].reverse.take 2).reverse ++ [c].take 2) = [a, b, c] := by simp
          rw [hbd]
          simp [om_pc_measure_three, om_pc_measure_two, om_pc_measure_singleton, om_pc_measure_nil]
      | cons d zs =>
          have hbd : (([a, b].reverse.take 2).reverse ++ (c :: d :: zs).take 2) = [a, b, c, d] := by simp
          have hpk1 : om_pc_pk (a :: b :: c :: d :: zs) 1 = true ↔ a < b ∧ c < b := by
            apply om_pc_pk_eq_of_bounds <;> simp
          have hpk2 : om_pc_pk (b :: c :: d :: zs) 1 = true ↔ b < c ∧ d < c := by
            apply om_pc_pk_eq_of_bounds <;> simp
          rw [hbd]
          change PeakCount.measure (a :: b :: c :: d :: zs) = _
          rw [om_pc_measure_cons a (b :: c :: d :: zs), om_pc_measure_cons b (c :: d :: zs),
            om_pc_measure_four, om_pc_measure_two]
          by_cases h1 : a < b ∧ c < b <;> by_cases h2 : b < c ∧ d < c <;>
            simp [h1, h2, hpk1, hpk2] <;> omega

private theorem om_pc_boundary_peaks_all (x : Series) : ∀ y,
    PeakCount.measure (x ++ y) = PeakCount.measure x + PeakCount.measure y +
      PeakCount.measure ((x.reverse.take 2).reverse ++ y.take 2) := by
  induction x with
  | nil =>
      intro y
      have htake : PeakCount.measure (y.take 2) = 0 :=
        om_pc_measure_of_length_le_two (y.take 2) (by simp [Nat.min_le_left])
      simp [om_pc_measure_nil, om_pc_pk_false_zero, htake]
  | cons a xs ih =>
      intro y
      cases xs with
      | nil =>
          cases y with
          | nil =>
              simp [om_pc_measure_singleton, om_pc_measure_nil]
          | cons b ys =>
              cases ys with
              | nil =>
                  have hbd : (([a].reverse.take 2).reverse ++ [b].take 2) = [a, b] := by simp
                  rw [hbd]
                  simp [om_pc_measure_two, om_pc_measure_singleton, om_pc_measure_nil]
              | cons c zs =>
                  have hbd : (([a].reverse.take 2).reverse ++ (b :: c :: zs).take 2) = [a, b, c] := by simp
                  have hpk : om_pc_pk (a :: b :: c :: zs) 1 = true ↔ a < b ∧ c < b := by
                    apply om_pc_pk_eq_of_bounds <;> simp
                  rw [hbd]
                  change PeakCount.measure (a :: b :: c :: zs) = _
                  rw [om_pc_measure_cons a (b :: c :: zs), om_pc_measure_three, om_pc_measure_singleton]
                  by_cases h : a < b ∧ c < b <;> simp [h, hpk] <;> omega
      | cons b xs2 =>
          cases xs2 with
          | nil =>
              exact om_pc_boundary_two a b y
          | cons c zs =>
              have hxs2 : 2 ≤ (b :: c :: zs).length := by simp
              have hlast := om_pc_last_two_cons_of_length a (b :: c :: zs) hxs2
              have ihy := ih y
              rw [← hlast] at ihy
              have hbit : om_pc_pk (a :: ((b :: c :: zs) ++ y)) 1 = om_pc_pk (a :: b :: c :: zs) 1 := by
                change om_pc_pk ((a :: b :: c :: zs) ++ y) 1 = om_pc_pk (a :: b :: c :: zs) 1
                apply om_pc_pk_append_left
                simp
              change PeakCount.measure (a :: ((b :: c :: zs) ++ y)) = _
              rw [om_pc_measure_cons a ((b :: c :: zs) ++ y), ihy, om_pc_measure_cons a (b :: c :: zs)]
              rw [hbit]
              ac_rfl
private theorem om_pc_adjacent_cap_pair (x : Series) :
    PeakCount.measure x ≤ (((x.length - 1) / 2 : Nat) : Int) ∧
      (om_pc_pk x 1 = false → PeakCount.measure x ≤ (((x.length - 2) / 2 : Nat) : Int)) := by
  induction x with
  | nil =>
      constructor <;> simp [om_pc_measure_nil, om_pc_pk]
  | cons a xs ih =>
      cases xs with
      | nil =>
          constructor <;> simp [om_pc_measure_singleton, om_pc_pk]
      | cons b ys =>
          rcases ih with ⟨ihP, ihQ⟩
          constructor
          · by_cases h1 : om_pc_pk (a :: b :: ys) 1 = true
            · have hrel : om_pc_pk (a :: b :: ys) 2 = om_pc_pk (b :: ys) 1 := by
                simpa using om_pc_pk_append_right [a] (b :: ys) 1 (by omega)
              have hn2not : ¬ om_pc_pk (b :: ys) 1 = true := by
                intro hb
                apply om_pc_pk_no_adjacent (a :: b :: ys) 1
                constructor
                · exact h1
                · rw [hrel]
                  exact hb
              have hn2 : om_pc_pk (b :: ys) 1 = false := by
                cases hbb : om_pc_pk (b :: ys) 1 <;> simp_all
              rw [om_pc_measure_cons a (b :: ys), if_pos h1]
              have hq := ihQ hn2
              have hnlen : 1 ≤ ys.length := by
                have hb := om_pc_pk_true_bounds (a :: b :: ys) h1
                simp at hb
                omega
              have hcapq : ((b :: ys).length - 2) / 2 = (ys.length - 1) / 2 := by simp
              have hcapx : ((a :: b :: ys).length - 1) / 2 = (ys.length + 1) / 2 := by simp
              rw [hcapq] at hq
              rw [hcapx]
              have harith : (ys.length - 1) / 2 + 1 ≤ (ys.length + 1) / 2 := by omega
              calc
                PeakCount.measure (b :: ys) + 1 ≤
                    ((((ys.length - 1) / 2 : Nat) : Int) + 1) := by omega
                _ = (((ys.length - 1) / 2 + 1 : Nat) : Int) := by
                  rw [Int.natCast_add, Int.natCast_one]
                _ ≤ (((ys.length + 1) / 2 : Nat) : Int) := (Int.ofNat_le).mpr harith
            · rw [om_pc_measure_cons a (b :: ys), if_neg h1]
              have hcapx : ((a :: b :: ys).length - 1) / 2 = (ys.length + 1) / 2 := by simp
              have hacap : ((b :: ys).length - 1) / 2 = ys.length / 2 := by simp
              rw [hcapx]
              rw [hacap] at ihP
              have hcastle := (Int.ofNat_le).mpr (by omega : ys.length / 2 ≤ (ys.length + 1) / 2)
              omega
          · intro hfalse
            have hfnot : ¬ om_pc_pk (a :: b :: ys) 1 = true := by simpa [hfalse]
            rw [om_pc_measure_cons a (b :: ys), if_neg hfnot]
            change PeakCount.measure (b :: ys) ≤ ((((a :: b :: ys).length - 2) / 2 : Nat) : Int)
            have hcapx : ((a :: b :: ys).length - 2) / 2 = ys.length / 2 := by simp
            have hacap : ((b :: ys).length - 1) / 2 = ys.length / 2 := by simp
            rw [hcapx]
            rw [hacap] at ihP
            exact ihP
private theorem om_pc_measure_edit (x : Series) (a : Int) (y : Series) :
    PeakCount.measure (x ++ a :: y) = PeakCount.measure x + PeakCount.measure y +
      (if om_pc_pk (x ++ a :: y) (x.length - 1) then 1 else 0) +
      (if om_pc_pk (x ++ a :: y) x.length then 1 else 0) +
      (if om_pc_pk (a :: y) 1 then 1 else 0) := by
  rw [om_pc_measure_append_split x (a :: y)]
  rw [om_pc_measure_cons a y]
  omega

private theorem om_pc_edit_bits_le_two (x : Series) (a : Int) (y : Series) :
    (if om_pc_pk (x ++ a :: y) (x.length - 1) then 1 else 0) +
      (if om_pc_pk (x ++ a :: y) x.length then 1 else 0) +
      (if om_pc_pk (a :: y) 1 then 1 else 0) ≤ (2 : Int) := by
  by_cases h0 : om_pc_pk (x ++ a :: y) (x.length - 1) = true <;>
    by_cases h1 : om_pc_pk (x ++ a :: y) x.length = true <;>
    by_cases h2 : om_pc_pk (a :: y) 1 = true
  · have hb := om_pc_pk_true_bounds (x ++ a :: y) h0
    have heq : x.length - 1 + 1 = x.length := by omega
    exact False.elim (om_pc_pk_no_adjacent (x ++ a :: y) (x.length - 1)
      ⟨h0, by simpa [heq] using h1⟩)
  · simp [h0, h1, h2]
  · simp [h0, h1, h2]
  · simp [h0, h1, h2]
  · have hrel : om_pc_pk (x ++ a :: y) (x.length + 1) = om_pc_pk (a :: y) 1 := by
      simpa using om_pc_pk_append_right x (a :: y) 1 (by omega)
    have h2z : om_pc_pk (x ++ a :: y) (x.length + 1) = true := by simpa [hrel] using h2
    exact False.elim (om_pc_pk_no_adjacent (x ++ a :: y) x.length ⟨h1, h2z⟩)
  · simp [h0, h1, h2]
  · simp [h0, h1, h2]
  · simp [h0, h1, h2]

private theorem om_pc_natAbs_le_of_bounds {d : Int} (hlo : -2 ≤ d) (hhi : d ≤ 2) : d.natAbs ≤ 2 := by
  apply (Int.ofNat_le).mp
  by_cases h0 : 0 ≤ d
  · rw [Int.natAbs_of_nonneg h0]
    exact hhi
  · have hd : d ≤ 0 := by omega
    have hn : 0 ≤ -d := by omega
    have hcast : (↑(d.natAbs) : Int) = (↑((-d).natAbs) : Int) := by
      rw [Int.natAbs_neg]
    rw [hcast, Int.natAbs_of_nonneg hn]
    omega
-- END om-helpers

theorem sound_PeakCount_BoundaryPeaks : PeakCount.BoundaryPeaks.Goal := by
  exact om_pc_boundary_peaks_all

theorem sound_PeakCount_OrderDictionary : PeakCount.OrderDictionary.Goal := by
  intro x f hf
  rw [om_pc_measure_eq_count_pk, om_pc_measure_eq_count_pk]
  simp only [List.length_map]
  congr 1
  apply om_pc_countP_congr
  intro i hi
  exact om_pc_pk_map x f hf i

theorem sound_PeakCount_AdjacentCap : PeakCount.AdjacentCap.Goal := by
  intro x
  exact (om_pc_adjacent_cap_pair x).1

theorem sound_PeakCount_BoundedRepair : PeakCount.BoundedRepair.Goal := by
  intro x y a b
  rw [om_pc_measure_edit x a y, om_pc_measure_edit x b y]
  have hA := om_pc_edit_bits_le_two x a y
  have hB := om_pc_edit_bits_le_two x b y
  have hA0 : 0 ≤ (if om_pc_pk (x ++ a :: y) (x.length - 1) then 1 else 0) +
        (if om_pc_pk (x ++ a :: y) x.length then 1 else 0) + (if om_pc_pk (a :: y) 1 then 1 else 0) := by
    by_cases h0 : om_pc_pk (x ++ a :: y) (x.length - 1) = true <;>
      by_cases h1 : om_pc_pk (x ++ a :: y) x.length = true <;>
      by_cases h2 : om_pc_pk (a :: y) 1 = true <;> simp [h0, h1, h2]
  have hB0 : 0 ≤ (if om_pc_pk (x ++ b :: y) (x.length - 1) then 1 else 0) +
        (if om_pc_pk (x ++ b :: y) x.length then 1 else 0) + (if om_pc_pk (b :: y) 1 then 1 else 0) := by
    by_cases h0 : om_pc_pk (x ++ b :: y) (x.length - 1) = true <;>
      by_cases h1 : om_pc_pk (x ++ b :: y) x.length = true <;>
      by_cases h2 : om_pc_pk (b :: y) 1 = true <;> simp [h0, h1, h2]
  have hle :
      (((PeakCount.measure x + PeakCount.measure y +
        (if om_pc_pk (x ++ a :: y) (x.length - 1) then 1 else 0)) +
        (if om_pc_pk (x ++ a :: y) x.length then 1 else 0)) +
        (if om_pc_pk (a :: y) 1 then 1 else 0)) -
      (((PeakCount.measure x + PeakCount.measure y +
        (if om_pc_pk (x ++ b :: y) (x.length - 1) then 1 else 0)) +
        (if om_pc_pk (x ++ b :: y) x.length then 1 else 0)) +
        (if om_pc_pk (b :: y) 1 then 1 else 0)) ≤ 2 := by omega
  have hge2 : -2 ≤
      (((PeakCount.measure x + PeakCount.measure y +
        (if om_pc_pk (x ++ a :: y) (x.length - 1) then 1 else 0)) +
        (if om_pc_pk (x ++ a :: y) x.length then 1 else 0)) +
        (if om_pc_pk (a :: y) 1 then 1 else 0)) -
      (((PeakCount.measure x + PeakCount.measure y +
        (if om_pc_pk (x ++ b :: y) (x.length - 1) then 1 else 0)) +
        (if om_pc_pk (x ++ b :: y) x.length then 1 else 0)) +
        (if om_pc_pk (b :: y) 1 then 1 else 0)) := by omega
  exact om_pc_natAbs_le_of_bounds hge2 hle

theorem disproof_PeakCount_DuplicateLoss : ¬ PeakCount.DuplicateLoss.Goal :=
  TinylambOpt.ResearchProofs.Atlas.OrderedMeasuresPeakCount.DuplicateLoss_disproof

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures_WorkB
