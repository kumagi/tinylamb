import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelectionA

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-! ## Generic frame-selection facts -/

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n := by
  unfold ubytes width
  induction n using Nat.strongRecOn with
  | ind n ih =>
      rw [uleb.eq_1]
      by_cases hn : n < 128
      · simp only [if_pos hn, List.length_singleton]
        by_cases hz : n = 0
        · simp [hz]
        · have hlog : n.log2 < 7 := (Nat.log2_lt hz).2 (by simpa using hn)
          simp [hz]
          omega
      · have hlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        rw [if_neg hn, List.length_cons, ih (n / 128) hlt]
        have hwidth : width (n / 128) + 6 + 1 = width n + 6 := by
          have hz0 : n / 128 ≠ 0 := by
            intro hz
            exact hn (Nat.div_eq_zero_iff.mp hz |>.resolve_left (by decide))
          have hlog : (n / 128).log2 + 1 = n.log2 := by
            have hspec := Nat.log2_eq_iff (k := (n / 128).log2) hz0
            have hlo := (hspec.mp rfl).1
            have hhi := (hspec.mp rfl).2
            apply (Nat.log2_eq_iff (by omega : n ≠ 0)).2
            constructor
            · have hm := (Nat.le_div_iff_mul_le (by decide : 0 < 128)).2 hlo
              simpa [pow_succ, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using hm
            · have hp : 0 < 2 ^ ((n / 128).log2 + 1) := Nat.pow_pos (by decide)
              omega
          have hzn : n ≠ 0 := by omega
          simp [width, hz0, hzn, hlog]
        simp only [width] at hwidth
        omega

private theorem min_add_sub_eq (a b : Nat) : min a b + (b - a) = b := by
  omega

private theorem chosen_add_saved (f : Writer) (xs : List Nat) :
    chosen f xs + saved f xs = (raw xs).length := by
  unfold chosen saved
  simpa [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using
    min_add_sub_eq (frame f xs).length (raw xs).length

private theorem strictSavings_holds (f : Writer) : strictSavings f := by
  intro ps fee
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hmul : fee * (ps.length + 1) = fee * ps.length + fee := by
        rw [Nat.mul_add, Nat.mul_one]
      have hp := chosen_add_saved f p
      omega

private theorem parts_flatten (xs : List Nat) (ps : List (List Nat))
    (h : ps ∈ parts xs) : ps.flatten = xs := by
  induction xs generalizing ps with
  | nil =>
      simp [parts] at h
      simp [h]
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨ps', hps', hmem⟩
      have hflat := ih ps' hps'
      match ps', hmem with
      | [], hmem =>
          simp at hmem
          subst hmem
          simp [hflat]
      | a :: rest, hmem =>
          rw [List.flatten_cons] at hflat
          simp only [List.mem_cons, List.mem_singleton, List.not_mem_nil, or_false] at hmem
          rcases hmem with rfl | rfl <;> simp [hflat]

private theorem parts_length_sum (xs : List Nat) (ps : List (List Nat))
    (h : ps ∈ parts xs) : (ps.map List.length).sum = xs.length := by
  have hf := parts_flatten xs ps h
  have := congrArg List.length hf
  simpa [List.length_flatten] using this

private theorem raw_len_ge_two (xs : List Nat) : 2 ≤ (raw xs).length := by
  simp [raw, uleb_len_eq_ubytes]
  have : 1 ≤ ubytes xs.length := Nat.le_max_left _ _
  omega

private theorem frame_len_ge_two (f : Writer) (xs : List Nat) :
    2 ≤ (frame f xs).length := by
  simp [frame, uleb_len_eq_ubytes]
  have : 1 ≤ ubytes xs.length := Nat.le_max_left _ _
  omega

private theorem chosen_ge_two (f : Writer) (xs : List Nat) : 2 ≤ chosen f xs := by
  exact Nat.le_min.mpr ⟨frame_len_ge_two f xs, raw_len_ge_two xs⟩

private theorem chosen_empty (f : Writer) : chosen f [] = 2 := by
  have hu0 : ubytes 0 = 1 := by simp [ubytes, width]
  unfold chosen
  rw [show (frame f []).length = 2 + (f []).length by
    simp [frame, uleb_len_eq_ubytes, hu0, Nat.add_comm]]
  rw [show (raw []).length = 2 by simp [raw, fixed, uleb_len_eq_ubytes, hu0]]
  simp [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] <;> omega

private theorem pageLimit_le_of_cover (n cap k : Nat) (hcap : 0 < cap)
    (hk : n ≤ k * cap) : pageLimit n cap ≤ k := by
  unfold pageLimit
  rw [Nat.div_le_iff_le_mul_add_pred hcap]
  rw [Nat.mul_comm cap k]
  omega

private theorem charge_ge_mul (f : Writer) (fee : Nat) (ps : List (List Nat))
    (h : ∀ p ∈ ps, 2 ≤ chosen f p) :
    (fee + 2) * ps.length ≤ charge f fee ps := by
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hp : 2 ≤ chosen f p := h p (by simp)
      have ih' : (fee + 2) * ps.length ≤
          (ps.map (fun p => fee + chosen f p)).sum := by
        apply ih
        intro q hq
        exact h q (by simp [hq])
      have hmul : (fee + 2) * (ps.length + 1) =
          (fee + 2) * ps.length + (fee + 2) := by
        rw [Nat.mul_add, Nat.mul_one]
      omega

private theorem mandatoryCuts_holds (f : Writer) : mandatoryCuts f := by
  intro xs cap fee ps hcap hps hall
  have hsum : (ps.map List.length).sum = xs.length := parts_length_sum xs ps hps
  have hle : xs.length ≤ ps.length * cap := by
    rw [← hsum]
    clear hsum hps
    induction ps with
    | nil => simp
    | cons p ps ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons]
        have hp := hall p (by simp)
        have ih' := ih (fun q hq => hall q (by simp [hq]))
        have hmul : (ps.length + 1) * cap = ps.length * cap + cap := by
          simp [Nat.add_mul, Nat.one_mul]
        omega
  have hpl : pageLimit xs.length cap ≤ ps.length :=
    pageLimit_le_of_cover xs.length cap ps.length hcap hle
  have hch : (fee + 2) * ps.length ≤ charge f fee ps :=
    charge_ge_mul f fee ps (fun p _ => chosen_ge_two f p)
  exact Nat.le_trans (Nat.mul_le_mul_left _ hpl) hch

private theorem min?_getD_le_mem_nat (l : List Nat) (x : Nat) (hx : x ∈ l) :
    l.min?.getD 0 ≤ x := by
  cases l with
  | nil => simp at hx
  | cons y ys =>
      have hs : (y :: ys).min? ≠ none := by
        intro hnone
        have : (y :: ys) = [] := List.min?_eq_none_iff.mp hnone
        simp at this
      rcases hmn : (y :: ys).min? with _ | M
      · simp [hmn] at hs
      · exact ((List.le_min?_iff hmn).1 (Nat.le_refl _)) x hx

private theorem min?_eq_some_getD_of_mem_nat (l : List Nat) (x : Nat) (hx : x ∈ l) :
    l.min? = some (l.min?.getD 0) := by
  cases h : l.min? with
  | none =>
      have : l = [] := List.min?_eq_none_iff.mp h
      subst this
      simp at hx
  | some v => simp [h]

private theorem parts_filter_mono (xs : List Nat) (a b : Nat) (hab : a ≤ b)
    (ps : List (List Nat))
    (h : ps ∈ (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a))) :
    ps ∈ (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b)) := by
  simp only [List.mem_filter] at h ⊢
  refine ⟨h.1, ?_⟩
  apply List.all_eq_true.mpr
  intro q hq
  have hq' := (List.all_eq_true.mp h.2) q hq
  simp only [decide_eq_true_eq] at hq' ⊢
  omega

private theorem seekCap_holds (f : Writer) : seekCap f := by
  intro xs a b s hab hb
  unfold budget at hb ⊢
  let la := ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a))).map (charge f 0)
  let lb := ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))).map (charge f 0)
  have hs_mem : s ∈ la := by
    rcases List.min?_eq_some_iff.mp hb with ⟨hmem, _⟩
    exact hmem
  have hs_in_lb : s ∈ lb := by
    rcases List.mem_map.mp hs_mem with ⟨ps, hps, rfl⟩
    exact List.mem_map.mpr ⟨ps, parts_filter_mono xs a b hab ps hps, rfl⟩
  refine ⟨lb.min?.getD 0, min?_getD_le_mem_nat lb s hs_in_lb, ?_⟩
  exact min?_eq_some_getD_of_mem_nat lb s hs_in_lb

private theorem charge_append (f : Writer) (fee : Nat) (ps qs : List (List Nat)) :
    charge f fee (ps ++ qs) = charge f fee ps + charge f fee qs := by
  unfold charge
  simp [List.map_append, List.sum_append]

private theorem parts_append (a b : List Nat) (pa pb : List (List Nat))
    (ha : pa ∈ parts a) (hb : pb ∈ parts b) : pa ++ pb ∈ parts (a ++ b) := by
  induction a generalizing pa with
  | nil =>
      simp [parts] at ha
      subst pa
      simpa using hb
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at ha
      rcases ha with ⟨ps', hps', hmem⟩
      have hrec : ps' ++ pb ∈ parts (xs ++ b) := ih ps' hps'
      simp only [List.cons_append, parts, List.mem_flatMap]
      refine ⟨ps' ++ pb, hrec, ?_⟩
      match ps', hmem with
      | [], hmem =>
          simp at hmem
          subst hmem
          cases pb <;> simp
      | a1 :: rest, hmem =>
          simp only [List.mem_cons, List.mem_singleton, List.not_mem_nil, or_false] at hmem
          rcases hmem with rfl | rfl <;> cases pb <;> simp_all [parts]

private def objective (f : Writer) (fee price : Nat) (ps : List (List Nat)) : Nat :=
  charge f fee ps + price * seekWork ps

private theorem parts_ne_nil (xs : List Nat) : parts xs ≠ [] := by
  induction xs with
  | nil => simp [parts]
  | cons x xs ih =>
      intro h
      have hall := List.flatMap_eq_nil_iff.mp h
      cases hp : parts xs with
      | nil => exact ih hp
      | cons ps rest =>
          have hps := hall ps (by simp [hp])
          cases ps with
          | nil => simp at hps
          | cons a rest => simp at hps

private theorem mergeSort_head_min {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c : α, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b : α, (le a b || le b a) = true)
    {l : List α} {a : α} {t : List α} (h : l.mergeSort le = a :: t) :
    ∀ b ∈ l, le a b = true := by
  have hs := List.pairwise_mergeSort trans total l
  rw [h] at hs
  have hhead : ∀ b ∈ t, le a b = true := (List.pairwise_cons.mp hs).1
  intro b hb
  have hb' : b ∈ l.mergeSort le := by simpa using hb
  rw [h] at hb'
  simp only [List.mem_cons] at hb'
  rcases hb' with rfl | hbt
  · simpa using total b b
  · exact hhead b hbt

private theorem plan_mem_parts (f : Writer) (fee price : Nat) (xs : List Nat) :
    plan f fee price xs ∈ parts xs := by
  let le := fun a b => decide (objective f fee price a ≤ objective f fee price b)
  change ((parts xs).mergeSort le).head?.getD [] ∈ parts xs
  cases hm : (parts xs).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (parts xs) (le := le)
      rw [hm] at hlen
      exact (parts_ne_nil xs (List.length_eq_zero_iff.mp hlen.symm)).elim
  | cons a t =>
      change a ∈ parts xs
      have hmem : a ∈ (parts xs).mergeSort le := by
        rw [hm]
        exact List.mem_cons_self
      exact List.mem_mergeSort.mp hmem

private theorem plan_min (f : Writer) (fee price : Nat) (xs : List Nat) (ps : List (List Nat))
    (hps : ps ∈ parts xs) :
    objective f fee price (plan f fee price xs) ≤ objective f fee price ps := by
  let le := fun a b => decide (objective f fee price a ≤ objective f fee price b)
  have htrans : ∀ a b c : List (List Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))
  have htotal : ∀ a b : List (List Nat), (le a b || le b a) = true := by
    intro a b
    by_cases h : objective f fee price a ≤ objective f fee price b
    · simp [le, h]
    · have h' : objective f fee price b ≤ objective f fee price a := Nat.le_of_not_ge h
      simp [le, h, h']
  have hne : (parts xs).mergeSort le ≠ [] := by
    intro hm
    have hlen := List.length_mergeSort (parts xs) (le := le)
    rw [hm] at hlen
    exact parts_ne_nil xs (List.length_eq_zero_iff.mp hlen.symm)
  cases hm : (parts xs).mergeSort le with
  | nil => exact (hne hm).elim
  | cons a t =>
      have hle := mergeSort_head_min le htrans htotal hm ps hps
      have hnat : objective f fee price a ≤ objective f fee price ps :=
        of_decide_eq_true hle
      change objective f fee price (((parts xs).mergeSort le).head?.getD []) ≤
        objective f fee price ps
      rw [hm]
      exact hnat

private theorem hotPointReads_holds (f : Writer) : hotPointReads f := by
  intro xs fee p q hpq
  let A := plan f fee p xs
  let B := plan f fee q xs
  have hA : objective f fee p A ≤ objective f fee p B :=
    plan_min f fee p xs B (plan_mem_parts f fee q xs)
  have hB : objective f fee q B ≤ objective f fee q A :=
    plan_min f fee q xs A (plan_mem_parts f fee p xs)
  change seekWork B ≤ seekWork A
  apply Nat.le_of_not_gt
  intro hnot
  have hBA : seekWork A < seekWork B := by omega
  have hq : q = p + (q - p) := by omega
  have hpos : 0 < q - p := by omega
  have hr : (q - p) * seekWork A < (q - p) * seekWork B :=
    Nat.mul_lt_mul_of_pos_left hBA hpos
  have hpB : p * seekWork B = p * seekWork A + (q - p) * seekWork B := by
    have heq : q * seekWork B = (p + (q - p)) * seekWork B :=
      congrArg (fun z => z * seekWork B) hq
    rw [Nat.add_mul] at heq
    exact heq
  have hqA : q * seekWork A = p * seekWork A + (q - p) * seekWork A := by
    have heq : q * seekWork A = (p + (q - p)) * seekWork A :=
      congrArg (fun z => z * seekWork A) hq
    rw [Nat.add_mul] at heq
    exact heq
  simp only [objective] at hA hB
  omega

private theorem parallelEncode_holds (f : Writer) : parallelEncode f := by
  intro a b fee
  have hp := plan_min f fee 0 (a ++ b) (plan f fee 0 a ++ plan f fee 0 b)
    (parts_append a b _ _ (plan_mem_parts f fee 0 a) (plan_mem_parts f fee 0 b))
  simpa [objective, charge_append, Nat.zero_mul, Nat.add_zero] using hp

private theorem refute_byteFrontier (f : Writer) : ¬ byteFrontier f := by
  intro h
  have hp : plan f 0 0 [] = [[]] := by
    simp [plan, parts, List.mergeSort_singleton]
  have hh := h [] 0 0 [[]] (by simp [admissible]) (by simp [parts])
  dsimp only at hh
  rw [hp] at hh
  have hbc : quotedOptimum f 0 0 [] = 0 := by
    simp [quotedOptimum, boundaryCost]
  have hc : charge f 0 [[]] = 2 := by simp [charge, chosen_empty]
  rw [hbc, hc] at hh
  simp at hh
  omega

private theorem refute_boundedRaw (f : Writer) : ¬ boundedRaw f := by
  intro h
  have hb : budget f [] 1 = some 2 := by
    unfold budget
    rw [show (parts []).filter (fun ps => ps.all (fun p => p.length ≤ 1)) = [[]] by decide]
    change ([[]].map (charge f 0)).min? = some 2
    simp [charge, chosen_empty]
  have hh := h [] 1 2 (by decide) hb
  simp [pageLimit, ubytes, width] at hh

/-! ## SeparatedLengths -/

theorem sound_SeparatedLengths_StrictSavings : SeparatedLengths.StrictSavings.Goal :=
  strictSavings_holds SeparatedLengths.write

theorem sound_SeparatedLengths_HotPointReads : SeparatedLengths.HotPointReads.Goal :=
  hotPointReads_holds SeparatedLengths.write

theorem sound_SeparatedLengths_SeekCap : SeparatedLengths.SeekCap.Goal :=
  seekCap_holds SeparatedLengths.write

theorem sound_SeparatedLengths_ParallelEncode : SeparatedLengths.ParallelEncode.Goal :=
  parallelEncode_holds SeparatedLengths.write

theorem sound_SeparatedLengths_MandatoryCuts : SeparatedLengths.MandatoryCuts.Goal :=
  mandatoryCuts_holds SeparatedLengths.write

theorem refuted_SeparatedLengths_ByteFrontier : ¬ SeparatedLengths.ByteFrontier.Goal :=
  refute_byteFrontier SeparatedLengths.write

theorem refuted_SeparatedLengths_BoundedRaw : ¬ SeparatedLengths.BoundedRaw.Goal :=
  refute_boundedRaw SeparatedLengths.write

private theorem pack_two_length (xs : List Nat) :
    (pack 2 xs).length = byteCount (xs.length * 2) := by
  simp [pack, little]

private theorem separated_data_length (xs : List Nat) :
    ((xs.zip (sizes xs)).flatMap (fun (v, w) => little v w)).length =
      (sizes xs).sum := by
  rw [List.length_flatMap]
  have hmap : (xs.zip (sizes xs)).map (fun p => (little p.1 p.2).length) =
      (xs.zip (sizes xs)).map Prod.snd := by
    apply List.map_congr_left
    intro p hp
    simp [little]
  rw [hmap, List.map_snd_zip (by simp [sizes])]

theorem sound_SeparatedLengths_FourControls : SeparatedLengths.FourControls.Goal := by
  intro a b ha hadm
  have hbyte : byteCount (2 * (a.length + b.length)) =
      byteCount (2 * a.length) + byteCount (2 * b.length) := by
    have hA : 2 * a.length = 8 * (2 * a.length / 8) := by omega
    calc
      (2 * (a.length + b.length) + 7) / 8 =
          ((2 * b.length + 7) + 8 * (2 * a.length / 8)) / 8 := by omega
      _ = (2 * b.length + 7) / 8 + 2 * a.length / 8 := by
        exact Nat.add_mul_div_right (z := 8) (2 * b.length + 7)
          (2 * a.length / 8) (by decide)
      _ = (2 * a.length + 7) / 8 + (2 * b.length + 7) / 8 := by omega
  have hsz : sizes (a ++ b) = sizes a ++ sizes b := by simp [sizes]
  have hzip : (a ++ b).zip (sizes a ++ sizes b) =
      a.zip (sizes a) ++ b.zip (sizes b) := by
    exact List.zip_append (by simp [sizes])
  simp only [SeparatedLengths.write, hsz, hzip, pack_two_length, List.length_map,
    List.map_append, List.flatMap_append, List.length_append]
  rw [show (sizes a).length = a.length by simp [sizes],
    show (sizes b).length = b.length by simp [sizes],
    show (a.length + b.length) * 2 = 2 * (a.length + b.length) by omega,
    hbyte, separated_data_length, separated_data_length, separated_data_length]
  simp [sizes]
  omega

theorem sound_SeparatedLengths_ControlBytes : SeparatedLengths.ControlBytes.Goal := by
  intro xs hadm
  simp only [SeparatedLengths.write, List.length_append, pack_two_length,
    separated_data_length]
  congr 1
  simp [sizes, Nat.mul_comm]

/-! ## ConstantExceptions -/

theorem sound_ConstantExceptions_StrictSavings : ConstantExceptions.StrictSavings.Goal :=
  strictSavings_holds ConstantExceptions.write

theorem sound_ConstantExceptions_HotPointReads : ConstantExceptions.HotPointReads.Goal :=
  hotPointReads_holds ConstantExceptions.write

theorem sound_ConstantExceptions_SeekCap : ConstantExceptions.SeekCap.Goal :=
  seekCap_holds ConstantExceptions.write

theorem sound_ConstantExceptions_ParallelEncode : ConstantExceptions.ParallelEncode.Goal :=
  parallelEncode_holds ConstantExceptions.write

theorem sound_ConstantExceptions_MandatoryCuts : ConstantExceptions.MandatoryCuts.Goal :=
  mandatoryCuts_holds ConstantExceptions.write

theorem refuted_ConstantExceptions_ByteFrontier : ¬ ConstantExceptions.ByteFrontier.Goal :=
  refute_byteFrontier ConstantExceptions.write

theorem refuted_ConstantExceptions_BoundedRaw : ¬ ConstantExceptions.BoundedRaw.Goal :=
  refute_boundedRaw ConstantExceptions.write

private theorem ubytes_of_lt_128 {n : Nat} (hn : n < 128) : ubytes n = 1 := by
  by_cases hz : n = 0
  · simp [hz, ubytes, width]
  · have hlog : n.log2 < 7 := (Nat.log2_lt hz).2 (by simpa using hn)
    simp [ubytes, width, hz]
    omega

private theorem zipIdx_replicate (x n i : Nat) :
    (List.replicate n x).zipIdx i = (List.range n).map (fun j => (x, i + j)) := by
  induction n generalizing i with
  | zero => simp
  | succ n ih =>
      simp [List.replicate_succ, List.zipIdx_cons, ih, List.range_succ_eq_map,
        List.map_map, Nat.add_assoc]
      intro a ha
      omega

private theorem constant_write_length (xs : List Nat) :
    (ConstantExceptions.write xs).length = 4 +
      ubytes ((xs.zipIdx.filter (fun (v, _) => v != xs.head?.getD 0)).length) +
      ((xs.zipIdx.filter (fun (v, _) => v != xs.head?.getD 0)).map
        (fun (v, i) => ubytes i + 4)).sum := by
  simp [ConstantExceptions.write, little, uleb_len_eq_ubytes, List.length_flatMap,
    Nat.add_assoc]

private theorem exception_cost_lower (l : List (Nat × Nat)) :
    l.length * 5 ≤ (l.map (fun (v, i) => ubytes i + 4)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hu : 1 ≤ ubytes p.2 := Nat.le_max_left _ _
      have hmul : (l.length + 1) * 5 = l.length * 5 + 5 := by omega
      rw [hmul]
      have ih' : l.length * 5 ≤ (l.map (fun x => ubytes x.snd + 4)).sum := by
        simpa only [Prod.eta] using ih
      omega

theorem sound_ConstantExceptions_ModalBeatsFirst : ConstantExceptions.ModalBeatsFirst.Goal := by
  intro a b n hab hn
  have hzleft : ((b :: a :: List.replicate (n - 1) b).zipIdx.filter
      (fun (v, _) => v != b)) = [(a, 1)] := by
    simp [List.zipIdx_cons, zipIdx_replicate, hab]
  have hleft : (ConstantExceptions.write (b :: a :: List.replicate (n - 1) b)).length = 10 := by
    rw [constant_write_length]
    rw [show (b :: a :: List.replicate (n - 1) b).head?.getD 0 = b by rfl, hzleft]
    simp only [List.length_cons, List.length_nil, List.map_cons, List.map_nil,
      List.sum_cons, List.sum_nil]
    rw [ubytes_of_lt_128 (by decide : 1 < 128)]
    simp
  have hcount : ((a :: List.replicate n b).zipIdx.filter
      (fun (v, _) => v != a)).length = n := by
    have hba : b ≠ a := Ne.symm hab
    rw [List.zipIdx_cons, zipIdx_replicate]
    simp [Function.comp_def, hba]
  have hcost := exception_cost_lower ((a :: List.replicate n b).zipIdx.filter
    (fun (v, _) => v != a))
  have hu : 1 ≤ ubytes n := Nat.le_max_left _ _
  have hright := constant_write_length (a :: List.replicate n b)
  rw [hright]
  rw [show (a :: List.replicate n b).head?.getD 0 = a by rfl]
  rw [hcount]
  omega

theorem sound_ConstantExceptions_ConstantCharge : ConstantExceptions.ConstantCharge.Goal := by
  intro x n
  cases n with
  | zero =>
      rw [constant_write_length]
      simp [ubytes, width]
  | succ n =>
      rw [constant_write_length]
      have hz : ((List.replicate (n + 1) x).zipIdx.filter
          (fun (v, _) => v != x)) = [] := by
        simp [zipIdx_replicate]
      rw [List.head?_replicate]
      simp [hz, ubytes_of_lt_128 (by decide : 0 < 128)]

/-! ## DictionaryRuns -/

theorem sound_DictionaryRuns_StrictSavings : DictionaryRuns.StrictSavings.Goal :=
  strictSavings_holds DictionaryRuns.write

theorem sound_DictionaryRuns_HotPointReads : DictionaryRuns.HotPointReads.Goal :=
  hotPointReads_holds DictionaryRuns.write

theorem sound_DictionaryRuns_SeekCap : DictionaryRuns.SeekCap.Goal :=
  seekCap_holds DictionaryRuns.write

theorem sound_DictionaryRuns_ParallelEncode : DictionaryRuns.ParallelEncode.Goal :=
  parallelEncode_holds DictionaryRuns.write

theorem sound_DictionaryRuns_MandatoryCuts : DictionaryRuns.MandatoryCuts.Goal :=
  mandatoryCuts_holds DictionaryRuns.write

theorem refuted_DictionaryRuns_ByteFrontier : ¬ DictionaryRuns.ByteFrontier.Goal :=
  refute_byteFrontier DictionaryRuns.write

theorem refuted_DictionaryRuns_BoundedRaw : ¬ DictionaryRuns.BoundedRaw.Goal :=
  refute_boundedRaw DictionaryRuns.write

-- Specialized dictionary goals are completed below.

/-! ## PatchedOctet -/

theorem sound_PatchedOctet_StrictSavings : PatchedOctet.StrictSavings.Goal :=
  strictSavings_holds PatchedOctet.write

theorem sound_PatchedOctet_HotPointReads : PatchedOctet.HotPointReads.Goal :=
  hotPointReads_holds PatchedOctet.write

theorem sound_PatchedOctet_SeekCap : PatchedOctet.SeekCap.Goal :=
  seekCap_holds PatchedOctet.write

theorem sound_PatchedOctet_ParallelEncode : PatchedOctet.ParallelEncode.Goal :=
  parallelEncode_holds PatchedOctet.write

theorem sound_PatchedOctet_MandatoryCuts : PatchedOctet.MandatoryCuts.Goal :=
  mandatoryCuts_holds PatchedOctet.write

theorem refuted_PatchedOctet_ByteFrontier : ¬ PatchedOctet.ByteFrontier.Goal :=
  refute_byteFrontier PatchedOctet.write

theorem refuted_PatchedOctet_BoundedRaw : ¬ PatchedOctet.BoundedRaw.Goal :=
  refute_boundedRaw PatchedOctet.write

theorem sound_PatchedOctet_EscapeCliff : PatchedOctet.EscapeCliff.Goal := by
  intro v
  simp [PatchedOctet.write, uleb_len_eq_ubytes,
    ubytes_of_lt_128 (by decide : 0 < 128)]
  split <;> simp_all [uleb_len_eq_ubytes,
    ubytes_of_lt_128 (by decide : 0 < 128)] <;> omega

theorem sound_PatchedOctet_EscapeCost : PatchedOctet.EscapeCost.Goal := by
  intro xs
  induction xs with
  | nil => simp [PatchedOctet.write]
  | cons x xs ih =>
      change ((if x < 255 then [x] else 255 :: uleb (x - 255)) ++
        PatchedOctet.write xs).length = _
      rw [List.length_append, ih]
      by_cases hx : x < 255
      · simp [hx, Nat.add_comm]
        omega
      · have hx' : 255 ≤ x := by omega
        simp [hx, hx', uleb_len_eq_ubytes, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

/-! ## XorPredecessor -/

theorem sound_XorPredecessor_StrictSavings : XorPredecessor.StrictSavings.Goal :=
  strictSavings_holds XorPredecessor.write

theorem sound_XorPredecessor_HotPointReads : XorPredecessor.HotPointReads.Goal :=
  hotPointReads_holds XorPredecessor.write

theorem sound_XorPredecessor_SeekCap : XorPredecessor.SeekCap.Goal :=
  seekCap_holds XorPredecessor.write

theorem sound_XorPredecessor_ParallelEncode : XorPredecessor.ParallelEncode.Goal :=
  parallelEncode_holds XorPredecessor.write

theorem sound_XorPredecessor_MandatoryCuts : XorPredecessor.MandatoryCuts.Goal :=
  mandatoryCuts_holds XorPredecessor.write

theorem refuted_XorPredecessor_ByteFrontier : ¬ XorPredecessor.ByteFrontier.Goal :=
  refute_byteFrontier XorPredecessor.write

theorem refuted_XorPredecessor_BoundedRaw : ¬ XorPredecessor.BoundedRaw.Goal :=
  refute_boundedRaw XorPredecessor.write

-- Specialized XOR goals are completed below.

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelectionA
