import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `ZeroFrames.BoundaryBalances` — `∀ x y, measure (x++y) = measure x + measure y +
  ((suffs x).map (fun s => (prefs y).count (-s))).sum`
- `ZeroFrames.EarliestPrune` — `∀ x, measure x = 0 ↔ (0::prefs x).Nodup`
- `ZeroFrames.Nonnegative` — `∀ x, (∀ v ∈ x, 0 ≤ v) → measure x = (windows x).countP
  (fun w => w.all (· == 0))`
- `ZeroFrames.PrefixCollision` — `∀ x, measure x = TiePairs.measure (0::prefs x)`
- `ZeroFrames.Uniform` — `∀ n a, measure (repeated n a) = if a == 0 then (choose2
  (n+1):Int) else 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.ZeroFrames

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.ZeroFrames
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! #### Generic list helpers -/

private theorem countP_map_comp {α β : Type} (p : β → Bool) (f : α → β) (l : List α) :
    (l.map f).countP p = l.countP (fun x => p (f x)) := by
  simp [Function.comp_def]

private theorem countP_congr_left {α : Type} {l : List α} {p q : α → Bool}
    (h : ∀ a ∈ l, p a = q a) : l.countP p = l.countP q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.countP_cons]
      rw [h a (by simp)]
      rw [ih (by intro b hb; exact h b (by simp [hb]))]

private theorem countP_const {α : Type} (l : List α) (b : Bool) :
    l.countP (fun _ => b) = if b then l.length else 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.countP_cons, List.length_cons]
      cases b <;> simp [ih]

private theorem countP_succ_range (p : Nat → Bool) (n : Nat) :
    (List.range (n + 1)).countP p =
      (List.range n).countP (fun i => p (i + 1)) + (if p 0 = true then 1 else 0) := by
  rw [List.range_succ_eq_map]
  simp only [List.countP_cons, List.countP_map, Function.comp_def]

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem flatMap_congr_left {α β : Type} {l : List α} {f g : α → List β}
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.flatMap_cons]
      rw [h a (by simp)]
      rw [ih (by intro b hb; exact h b (by simp [hb]))]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem take_of_length_le {α : Type} (xs : List α) (n : Nat) (h : xs.length ≤ n) :
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

private theorem range_split (a b : Nat) :
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

private theorem choose2_succ (n : Nat) : choose2 (n + 2) = choose2 (n + 1) + (n + 1) := by
  unfold choose2
  have h1 : n + 2 - 1 = n + 1 := by omega
  have h2 : n + 1 - 1 = n := by omega
  rw [h1, h2]
  rw [show (n + 2) * (n + 1) = (n + 1) * n + 2 * (n + 1) by grind]
  rw [Nat.add_mul_div_left _ _ (by decide : 0 < 2)]

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor <;> intro h <;> exact beq_iff_eq.mpr (beq_iff_eq.mp h).symm

private theorem beq_add_right (a b s : Int) : (a + b == s + b) = (a == s) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; omega)
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; rw [this])

private theorem beq_add_left (a b s : Int) : (b + a == b + s) = (a == s) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; omega)
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; rw [this])

private theorem beq_self_add (b s : Int) : (b == b + s) = (s == 0) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; omega)
  · intro h
    exact beq_iff_eq.mpr (by have := beq_iff_eq.mp h; rw [this]; simp)

private theorem map_congr_left {α β : Type} {l : List α} {f g : α → β}
    (h : ∀ a ∈ l, f a = g a) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons]
      rw [h a (by simp)]
      rw [ih (by intro b hb; exact h b (by simp [hb]))]

private theorem sum_replicate_int (a : Int) (n : Nat) :
    (List.replicate n a).sum = a * (n : Int) := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate, List.sum_cons]
      rw [ih]
      grind

/-! #### Pair/tie helpers -/

private def tieCount (l : List Int) : Nat :=
  (pairs l).countP (fun q : Int × Int => q.1 == q.2)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem tieCount_cons (a : Int) (l : List Int) :
    tieCount (a :: l) = tieCount l + l.countP (fun b : Int => a == b) := by
  unfold tieCount
  rw [pairs_cons]
  simp only [List.countP_append, List.countP_map, Function.comp_def]
  omega

private theorem tieCount_shift (b : Int) (l : List Int) :
    tieCount (l.map (fun s => s + b)) = tieCount l := by
  induction l with
  | nil => simp [tieCount, pairs]
  | cons a as ih =>
      rw [List.map_cons]
      rw [tieCount_cons, tieCount_cons, ih]
      rw [countP_map_comp]
      apply congrArg (fun n => tieCount as + n)
      apply countP_congr_left
      intro s hs
      exact beq_add_right a b s

/-! #### Windows / prefixes / suffixes structure -/

private theorem prefs_cons (b : Int) (z : Series) :
    prefs (b :: z) = b :: (prefs z).map (fun s => b + s) := by
  unfold prefs
  simp only [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map,
    Function.comp_def, List.take_succ_cons, List.sum_cons]
  congr 1 <;> simp

private theorem suffs_cons (b : Int) (z : Series) :
    suffs (b :: z) = (b + z.sum) :: suffs z := by
  unfold suffs
  simp only [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map,
    Function.comp_def, List.drop_succ_cons]
  simp

private theorem windows_cons (b : Int) (z : Series) :
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
    apply map_congr_left
    intro j hj
    simp [List.take_succ_cons]
  rw [hfirst]
  apply congrArg (fun l => (List.range (z.length + 1)).map (fun j => b :: z.take j) ++ l)
  rw [flatMap_map]
  apply flatMap_congr_left
  intro i hi
  have hlt : i < z.length := List.mem_range.mp hi
  have hsub : z.length + 1 - (i + 1) = z.length - i := by omega
  rw [hsub]
  congr 1

private theorem measure_cons (b : Int) (z : Series) :
    ZeroFrames.measure (b :: z) = ZeroFrames.measure z +
      ((List.range (z.length + 1)).countP (fun j => b + (z.take j).sum == 0) : Int) := by
  unfold ZeroFrames.measure
  rw [windows_cons]
  simp only [List.countP_append, List.countP_map, Function.comp_def, List.sum_cons]
  omega

private theorem mem_windows_mem {x w : Series} (hw : w ∈ windows x) :
    ∀ a ∈ w, a ∈ x := by
  induction x with
  | nil => simp [windows] at hw
  | cons b z ih =>
      rw [windows_cons] at hw
      rcases List.mem_append.mp hw with hw | hw
      · rcases List.mem_map.mp hw with ⟨j, hj, rfl⟩
        intro a ha
        simp only [List.mem_cons] at ha
        rcases ha with rfl | ha
        · simp
        · exact List.mem_cons_of_mem b (List.mem_of_mem_take ha)
      · intro a ha
        exact List.mem_cons_of_mem b (ih hw a ha)

private theorem sum_take_append_right (x y : Series) (k : Nat) :
    ((x ++ y).take (x.length + k + 1)).sum = x.sum + (y.take (k + 1)).sum := by
  rw [List.take_append]
  have hsub : x.length + k + 1 - x.length = k + 1 := by omega
  rw [hsub, List.sum_append]
  rw [take_of_length_le x (x.length + k + 1) (by omega)]

/-! #### Nonnegative sum helper -/

private theorem sum_nonneg_of_nonneg {w : List Int} (hw : ∀ a ∈ w, 0 ≤ a) : 0 ≤ w.sum := by
  induction w with
  | nil => simp
  | cons a as ih =>
      have ha : 0 ≤ a := hw a (by simp)
      have has : ∀ v ∈ as, 0 ≤ v := by intro v hv; exact hw v (by simp [hv])
      have hsum : 0 ≤ as.sum := ih has
      simp only [List.sum_cons]
      omega

private theorem all_zero_of_nonneg_sum_zero {w : List Int} (hw : ∀ a ∈ w, 0 ≤ a)
    (hsum : w.sum = 0) : ∀ a ∈ w, a = 0 := by
  induction w with
  | nil => intro a ha; simp at ha
  | cons b bs ih =>
      have hb : 0 ≤ b := hw b (by simp)
      have hbs : ∀ a ∈ bs, 0 ≤ a := by intro a ha; exact hw a (by simp [ha])
      have htail : 0 ≤ bs.sum := sum_nonneg_of_nonneg hbs
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

private theorem all_zero_sum_zero {w : List Int} (h : ∀ a ∈ w, a = 0) : w.sum = 0 := by
  induction w with
  | nil => simp
  | cons b bs ih =>
      have hb : b = 0 := h b (by simp)
      have hbs : ∀ a ∈ bs, a = 0 := by intro a ha; exact h a (by simp [ha])
      simp only [List.sum_cons]
      rw [hb, ih hbs]
      simp

/-! #### Prefix-list tie counting -/

private def prefixList (x : Series) : List Int := 0 :: prefs x

private theorem tieCount_prefixList_cons (b : Int) (z : Series) :
    tieCount (prefixList (b :: z)) = tieCount (prefixList z) +
      (List.range (z.length + 1)).countP (fun j => b + (z.take j).sum == 0) := by
  have hpre : prefixList (b :: z) = [0, b] ++ (prefs z).map (fun s => b + s) := by
    unfold prefixList prefs
    simp only [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map,
      Function.comp_def, List.take_succ_cons, List.sum_cons]
    congr 1 <;> simp
  rw [hpre]
  change tieCount (0 :: b :: (prefs z).map (fun s => b + s)) = _
  rw [tieCount_cons, tieCount_cons, List.countP_cons]
  have hshift : tieCount ((prefs z).map (fun s => b + s)) = tieCount (prefs z) := by
    simpa [Int.add_comm] using tieCount_shift b (prefs z)
  have hc1 : ((prefs z).map (fun s => b + s)).countP (fun s => b == s) =
      (prefs z).countP (fun s => s == 0) := by
    rw [countP_map_comp]
    apply countP_congr_left
    intro s hs
    exact beq_self_add b s
  have hc2 : ((prefs z).map (fun s => b + s)).countP (fun s => 0 == s) =
      (List.range z.length).countP (fun i => b + (z.take (i + 1)).sum == 0) := by
    rw [countP_map_comp]
    unfold prefs
    rw [countP_map_comp]
    apply countP_congr_left
    intro i hi
    exact beq_comm_int 0 (b + (z.take (i + 1)).sum)
  have htp : tieCount (prefixList z) = tieCount (prefs z) + (prefs z).countP (fun s => 0 == s) := by
    unfold prefixList
    rw [tieCount_cons]
  have hcomm : (prefs z).countP (fun s => s == 0) = (prefs z).countP (fun s => 0 == s) := by
    apply countP_congr_left
    intro s hs
    exact beq_comm_int s 0
  have hrange := countP_succ_range (fun j => b + (z.take j).sum == 0) z.length
  have hzero : z.take 0 = ([] : Series) := by rfl
  rw [hshift, hc1, hcomm, hc2, htp, hrange]
  simp only [hzero, List.sum_nil]
  have hb0 : (0 == b) = (b == 0) := beq_comm_int 0 b
  have hb00 : (b + 0 == 0) = (b == 0) := by simp
  rw [hb0, hb00]

private theorem measure_eq_tieCount_prefixList (x : Series) :
    ZeroFrames.measure x = (tieCount (prefixList x) : Int) := by
  induction x with
  | nil => simp [ZeroFrames.measure, windows, tieCount, prefixList, prefs, pairs]
  | cons b z ih =>
      rw [measure_cons, ih, tieCount_prefixList_cons]
      omega

theorem sound_Uniform : ZeroFrames.Uniform.Goal := by
  intro n a
  simp only [repeated]
  induction n with
  | zero => simp [ZeroFrames.measure, windows, choose2]
  | succ n ih =>
      change ZeroFrames.measure (a :: List.replicate n a) = _
      rw [measure_cons, ih]
      have hpred : ∀ j ∈ List.range (n + 1),
          (fun j => a + ((List.replicate n a).take j).sum == 0) j = (a == 0) := by
        intro j hj
        have hjle : j ≤ n := by
          have := List.mem_range.mp hj
          omega
        have htake : (List.replicate n a).take j = List.replicate j a := by
          rw [List.take_replicate, Nat.min_eq_left hjle]
        change (a + ((List.replicate n a).take j).sum == 0) = (a == 0)
        rw [htake, sum_replicate_int]
        by_cases ha : a = 0
        · simp [ha]
        · have hne : a + a * (j : Int) ≠ 0 := by
            intro h
            have hm : a * ((j : Int) + 1) = 0 := by
              have hm' : a * ((j : Int) + 1) = a + a * (j : Int) := by grind
              rw [hm']
              exact h
            rcases Int.mul_eq_zero.mp hm with ha0 | hj0
            · exact ha ha0
            · omega
          apply Bool.eq_iff_iff.mpr
          constructor
          · intro h
            exact False.elim (hne (beq_iff_eq.mp h))
          · intro h
            exact False.elim (ha (beq_iff_eq.mp h))
      have hcnt : (List.range (n + 1)).countP
          (fun j => a + ((List.replicate n a).take j).sum == 0) = if a == 0 then n + 1 else 0 := by
        rw [countP_congr_left hpred, countP_const]
        by_cases ha : a == 0 <;> simp [ha]
      rw [show (List.replicate n a).length = n by simp]
      rw [hcnt]
      by_cases ha : a == 0
      · simp [ha]
        rw [show n + 1 + 1 = n + 2 by omega, choose2_succ]
        omega
      · simp [ha]

theorem sound_Nonnegative : ZeroFrames.Nonnegative.Goal := by
  intro x hx
  unfold ZeroFrames.measure
  apply congrArg (fun n : Nat => (n : Int))
  apply countP_congr_left
  intro w hw
  have hwmem : ∀ a ∈ w, a ∈ x := mem_windows_mem hw
  have hnonneg : ∀ a ∈ w, 0 ≤ a := by
    intro a ha
    exact hx a (hwmem a ha)
  by_cases hs : w.sum == 0
  · have hs' : w.sum = 0 := beq_iff_eq.mp hs
    have hall : ∀ a ∈ w, a = 0 := all_zero_of_nonneg_sum_zero hnonneg hs'
    have hallb : w.all (fun a => a == 0) = true := by
      exact List.all_eq_true.mpr (by intro a ha; exact beq_iff_eq.mpr (hall a ha))
    simp [hs, hallb]
  · have hallnot : w.all (fun a => a == 0) = false := by
      cases hb : w.all (fun a => a == 0)
      · rfl
      · have hall' := List.all_eq_true.mp hb
        have hz : w.sum = 0 := all_zero_sum_zero (by
          intro a ha
          exact beq_iff_eq.mp (hall' a ha))
        exact False.elim (hs (by simp [hz]))
    simp [hs, hallnot]

theorem sound_PrefixCollision : ZeroFrames.PrefixCollision.Goal := by
  intro x
  unfold TiePairs.measure
  simpa [prefixList, tieCount] using measure_eq_tieCount_prefixList x

private theorem tieCount_nodup_zero_iff (l : List Int) : tieCount l = 0 ↔ l.Nodup := by
  induction l with
  | nil => simp [tieCount, pairs]
  | cons a as ih =>
      rw [tieCount_cons]
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
        have h2 : tieCount as = 0 := ih.mpr h.2
        omega

theorem sound_EarliestPrune : ZeroFrames.EarliestPrune.Goal := by
  intro x
  constructor
  · intro hzero
    have hm := measure_eq_tieCount_prefixList x
    rw [hzero] at hm
    have ht : tieCount (prefixList x) = 0 := by omega
    simpa [prefixList] using (tieCount_nodup_zero_iff (prefixList x)).mp ht
  · intro hnodup
    have ht : tieCount (prefixList x) = 0 :=
      (tieCount_nodup_zero_iff (prefixList x)).mpr (by simpa [prefixList] using hnodup)
    have hm := measure_eq_tieCount_prefixList x
    rw [ht] at hm
    omega

theorem sound_BoundaryBalances : ZeroFrames.BoundaryBalances.Goal := by
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
      conv => lhs; rw [measure_cons]
      rw [ih y]
      rw [measure_cons]
      rw [suffs_cons]
      simp only [List.map_cons, List.sum_cons, List.count_eq_countP]
      have hsplit :
          (List.range (xs.length + y.length + 1)).countP
              (fun j => b + ((xs ++ y).take j).sum == 0) =
            (List.range (xs.length + 1)).countP (fun j => b + (xs.take j).sum == 0) +
              (prefs y).countP (fun s => s == -(b + xs.sum)) := by
        rw [range_split]
        simp only [List.countP_append, List.countP_map, Function.comp_def]
        have hfirst : (List.range (xs.length + 1)).countP
            (fun j => b + ((xs ++ y).take j).sum == 0) =
            (List.range (xs.length + 1)).countP (fun j => b + (xs.take j).sum == 0) := by
          apply countP_congr_left
          intro j hj
          have hjle : j ≤ xs.length := by
            have := List.mem_range.mp hj
            omega
          rw [List.take_append_of_le_length hjle]
        rw [hfirst]
        apply congrArg (fun n => (List.range (xs.length + 1)).countP
          (fun j => b + (xs.take j).sum == 0) + n)
        unfold prefs
        rw [countP_map_comp]
        apply countP_congr_left
        intro k hk
        have hklt : k < y.length := List.mem_range.mp hk
        rw [show xs.length + 1 + k = xs.length + k + 1 by omega]
        rw [sum_take_append_right]
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

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.ZeroFrames
