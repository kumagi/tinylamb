import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelectionB

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-! Generic frame-selection facts and the five requested codec families. -/

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

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
      have hp := chosen_add_saved f p
      simp only [Nat.mul_add, Nat.mul_one] at ih ⊢
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
      cases ps' with
      | nil =>
          dsimp only at hmem
          have hsing : ps = [[x]] := by simpa using hmem
          subst ps
          simp [hflat]
      | cons a rest =>
          dsimp only at hmem
          have hcases : ps = [x] :: a :: rest ∨ ps = (x :: a) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hcases with rfl | rfl <;>
            simpa [List.flatten_cons] using congrArg (List.cons x) hflat

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
  have hu0 : ubytes 0 = 1 := by decide
  unfold chosen
  rw [show (frame f []).length = 2 + (f []).length by
    simp [frame, uleb_len_eq_ubytes, hu0, Nat.add_comm]; omega]
  rw [show (raw []).length = 2 by
    simp [raw, fixed, uleb_len_eq_ubytes, hu0, Nat.add_comm]]
  omega

private theorem pageLimit_le_of_cover (n cap k : Nat) (hcap : 0 < cap)
    (hk : n ≤ k * cap) : pageLimit n cap ≤ k := by
  unfold pageLimit
  rw [Nat.div_le_iff_le_mul_add_pred hcap]
  have hm : cap * k = k * cap := Nat.mul_comm _ _
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
  have h2 := h.2
  rw [List.all_eq_true] at h2 ⊢
  intro p hp
  have hp' := h2 p hp
  simp only [decide_eq_true_eq] at hp' ⊢
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
      rw [List.cons_append, parts]
      cases ps' with
      | nil =>
          dsimp only at hmem
          have hsing : pa = [[x]] := by simpa using hmem
          subst pa
          simp only [List.nil_append, List.cons_append]
          cases pb with
          | nil =>
              exact List.mem_flatMap.mpr ⟨[], by simpa using hrec, by simp⟩
          | cons b1 brest =>
              exact List.mem_flatMap.mpr ⟨b1 :: brest, by simpa using hrec, by simp⟩
      | cons a1 rest =>
          have hmem' : pa = [x] :: a1 :: rest ∨ pa = (x :: a1) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hmem' with rfl | rfl
          · cases pb with
            | nil =>
                simp only [List.append_nil, List.cons_append]
                exact List.mem_flatMap.mpr ⟨a1 :: rest, by simpa using hrec, by simp⟩
            | cons b1 brest =>
                simp only [List.cons_append]
                exact List.mem_flatMap.mpr ⟨a1 :: (rest ++ b1 :: brest), hrec, by simp⟩
          · cases pb with
            | nil =>
                simp only [List.append_nil, List.cons_append]
                exact List.mem_flatMap.mpr ⟨a1 :: rest, by simpa using hrec, by simp⟩
            | cons b1 brest =>
                simp only [List.cons_append]
                exact List.mem_flatMap.mpr ⟨a1 :: (rest ++ b1 :: brest), hrec, by simp⟩

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
      simp only [List.head?_cons, Option.getD_some]
      apply List.mem_mergeSort.mp
      rw [hm]
      exact List.mem_cons_self

private theorem plan_min (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
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
      simpa [le] using hnat

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
  intro hBA
  change seekWork A < seekWork B at hBA
  let r := q - p
  have hq : q = p + r := by dsimp [r]; omega
  have hpos : 0 < r := by dsimp [r]; omega
  have hr : r * seekWork A < r * seekWork B :=
    Nat.mul_lt_mul_of_pos_left hBA hpos
  have hqB : q * seekWork B = p * seekWork B + r * seekWork B := by
    rw [hq, Nat.add_mul]
  have hqA : q * seekWork A = p * seekWork A + r * seekWork A := by
    rw [hq, Nat.add_mul]
  simp only [objective] at hA hB
  omega

private theorem parallelEncode_holds (f : Writer) : parallelEncode f := by
  intro a b fee
  have hp := plan_min f fee 0 (a ++ b) (plan f fee 0 a ++ plan f fee 0 b)
    (parts_append a b _ _ (plan_mem_parts f fee 0 a) (plan_mem_parts f fee 0 b))
  simpa [objective, charge_append, Nat.zero_mul, Nat.add_zero] using hp

private def seg (n off : Nat) (edge : Nat → Nat → Nat) : Nat :=
  match n with
  | 0 => 0
  | m + 1 =>
      ((List.range (m + 1)).map (fun d =>
        edge off (off + d + 1) + seg (m - d) (off + d + 1) edge)).min?.getD 0

private def dpStep (edge : Nat → Nat → Nat) (n : Nat)
    (suffix : Array Nat) (done : Nat) : Array Nat :=
  suffix.push (((List.range (done + 1)).map (fun d =>
    edge (n - done - 1) (n - done - 1 + d + 1) +
      suffix[done - d]?.getD 0)).min?.getD 0)

private def suffixAt (edge : Nat → Nat → Nat) (n : Nat) : Nat → Array Nat
  | 0 => #[0]
  | done + 1 => dpStep edge n (suffixAt edge n done) done

private theorem forIn_yield_eq_foldl {α β : Type} (xs : List α) (init : β)
    (step : β → α → β) :
    Id.run (forIn xs init fun x s => pure (ForInStep.yield (step s x))) =
      xs.foldl step init := by
  induction xs generalizing init with
  | nil => rfl
  | cons x xs ih => simp [List.foldl_cons, ih]

private theorem foldl_range_suffixAt (edge : Nat → Nat → Nat) (n done : Nat) :
    (List.range done).foldl (dpStep edge n) #[0] = suffixAt edge n done := by
  induction done with
  | zero => rfl
  | succ done ih =>
      rw [List.range_succ, List.foldl_append, ih]
      simp [suffixAt]

private theorem suffixAt_size (edge : Nat → Nat → Nat) (n done : Nat) :
    (suffixAt edge n done).size = done + 1 := by
  induction done with
  | zero => rfl
  | succ done ih => simp [suffixAt, dpStep, Array.size_push, ih]

private theorem suffixAt_get (edge : Nat → Nat → Nat) (n done : Nat) (hdone : done ≤ n) :
    ∀ j, j ≤ done → (suffixAt edge n done)[j]? = some (seg j (n - j) edge) := by
  induction done with
  | zero =>
      intro j hj
      have : j = 0 := by omega
      subst j
      simp [suffixAt, seg]
  | succ done ih =>
      intro j hj
      rw [suffixAt, dpStep, Array.getElem?_push, suffixAt_size]
      by_cases hjeq : j = done + 1
      · subst j
        rw [if_pos rfl]
        have hopt :
            ((List.range (done + 1)).map (fun d =>
              edge (n - done - 1) (n - done - 1 + d + 1) +
                (suffixAt edge n done)[done - d]?.getD 0)) =
            ((List.range (done + 1)).map (fun d =>
              edge (n - done - 1) (n - done - 1 + d + 1) +
                seg (done - d) (n - done - 1 + d + 1) edge)) := by
          apply List.map_congr_left
          intro d hd
          have hd' : d ≤ done := by
            have hmem := hd
            simp [List.mem_range] at hmem
            omega
          rw [ih (by omega) (done - d) (by omega)]
          have harg : n - (done - d) = n - done - 1 + d + 1 := by omega
          rw [harg]
          simp
        apply congrArg some
        change (((List.range (done + 1)).map (fun d =>
          edge (n - done - 1) (n - done - 1 + d + 1) +
            (suffixAt edge n done)[done - d]?.getD 0)).min?.getD 0) = _
        rw [hopt, show n - (done + 1) = n - done - 1 by omega]
        simp [seg]
      · rw [if_neg hjeq]
        exact ih (by omega) j (by omega)

private theorem boundaryCost_eq_seg (n : Nat) (edge : Nat → Nat → Nat) :
    boundaryCost n edge = seg n 0 edge := by
  unfold boundaryCost
  change (Id.run (forIn (List.range n) #[0] fun done suffix =>
    pure (ForInStep.yield (dpStep edge n suffix done)))).back?.getD 0 = seg n 0 edge
  rw [forIn_yield_eq_foldl, foldl_range_suffixAt]
  rw [Array.back?_eq_getElem?, suffixAt_size, Nat.add_sub_cancel]
  rw [suffixAt_get edge n n (Nat.le_refl n) n (Nat.le_refl n)]
  simp

private theorem parts_decompose (xs : List Nat) (hxs : xs ≠ []) (ps : List (List Nat))
    (h : ps ∈ parts xs) :
    ∃ k, 0 < k ∧ k ≤ xs.length ∧ ∃ tail,
      ps = xs.take k :: tail ∧ tail ∈ parts (xs.drop k) := by
  induction xs generalizing ps with
  | nil => exact (hxs rfl).elim
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨ps', hps', hmem⟩
      cases ps' with
      | nil =>
          have hxsnil : xs = [] := by simpa using parts_flatten xs [] hps'
          subst xs
          dsimp only at hmem
          have hsing : ps = [[x]] := by simpa using hmem
          subst ps
          exact ⟨1, by decide, by simp, [], by simp,
            by change ([] : List (List Nat)) ∈ [[]]; simp⟩
      | cons a rest =>
          have hxsne : xs ≠ [] := by
            intro hnil
            subst xs
            simp [parts] at hps'
          have hcases : ps = [x] :: a :: rest ∨ ps = (x :: a) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hcases with rfl | rfl
          · exact ⟨1, by decide, by simp, a :: rest, by simp, hps'⟩
          · obtain ⟨k, hk, hklen, tail, hps, htail⟩ := ih hxsne (a :: rest) hps'
            have hcons : a :: rest = xs.take k :: tail := hps
            have ha : a = xs.take k := (List.cons.inj hcons).1
            have hrt : rest = tail := (List.cons.inj hcons).2
            subst a
            subst tail
            refine ⟨k + 1, by omega, by simp only [List.length_cons]; omega,
              rest, ?_, htail⟩
            simp [List.take_succ_cons, List.drop_succ_cons]

private theorem parts_prepend (xs : List Nat) (k : Nat) (tail : List (List Nat))
    (hk : 0 < k) (hklen : k ≤ xs.length) (htail : tail ∈ parts (xs.drop k)) :
    xs.take k :: tail ∈ parts xs := by
  induction k generalizing xs with
  | zero => omega
  | succ k ih =>
      cases xs with
      | nil => simp at hklen
      | cons x xs =>
          simp only [List.length_cons] at hklen
          cases k with
          | zero =>
              cases tail with
              | nil =>
                  have hnil : xs = [] := by simpa using parts_flatten xs [] htail
                  subst xs
                  simp [parts]
              | cons a rest =>
                  rw [parts]
                  exact List.mem_flatMap.mpr ⟨a :: rest, htail, by simp⟩
          | succ k' =>
              rw [List.take_succ_cons]
              rw [List.drop_succ_cons] at htail
              rw [parts]
              exact List.mem_flatMap.mpr ⟨xs.take (k' + 1) :: tail,
                ih xs (by omega) (by omega) htail, by simp⟩

private theorem objective_cons (f : Writer) (fee price : Nat) (xs : List Nat)
    (k : Nat) (tail : List (List Nat)) (hk : k ≤ xs.length) :
    objective f fee price (xs.take k :: tail) =
      fee + chosen f (xs.take k) + price * k ^ 2 + objective f fee price tail := by
  unfold objective charge seekWork
  simp only [List.map_cons, List.sum_cons, List.length_take, Nat.min_eq_left hk,
    Nat.mul_add]
  omega

private def intervalEdge (f : Writer) (fee price : Nat) (xs : List Nat)
    (a b : Nat) : Nat :=
  fee + chosen f ((xs.drop a).take (b - a)) + price * (b - a) ^ 2

private theorem seg_shift (edge : Nat → Nat → Nat) (n off : Nat) :
    seg n off edge = seg n 0 (fun a b => edge (off + a) (off + b)) := by
  revert edge off
  induction n using Nat.strongRecOn with
  | ind n ih =>
    intro edge off
    cases n with
    | zero => simp [seg]
    | succ m =>
      simp only [seg]
      apply congrArg (fun l : List Nat => l.min?.getD 0)
      apply List.map_congr_left
      intro d hd
      have hd' : d ≤ m := by
        have hmem := hd
        simp [List.mem_range] at hmem
        omega
      rw [ih (m - d) (by omega) edge (off + d + 1)]
      simp only [Nat.zero_add]
      rw [ih (m - d) (by omega) (fun a b => edge (off + a) (off + b)) (d + 1)]
      rw [show off + d + 1 = off + (d + 1) by omega]
      congr 1
      apply congrArg (fun e => seg (m - d) 0 e)
      funext a b
      exact congrArg₂ edge (by omega) (by omega)

private theorem intervalEdge_shift (f : Writer) (fee price : Nat) (xs : List Nat)
    (k : Nat) :
    (fun a b => intervalEdge f fee price xs (k + a) (k + b)) =
      intervalEdge f fee price (xs.drop k) := by
  funext a b
  have hsub : k + b - (k + a) = b - a := by omega
  simp [intervalEdge, List.drop_drop, Nat.add_comm, hsub]

private theorem seg_interval_drop (f : Writer) (fee price : Nat) (xs : List Nat)
    (n k : Nat) :
    seg n k (intervalEdge f fee price xs) =
      seg n 0 (intervalEdge f fee price (xs.drop k)) := by
  rw [seg_shift, intervalEdge_shift]

private theorem refute_byteFrontier (f : Writer) : ¬ byteFrontier f := by
  intro h
  have hp : plan f 0 0 [] = [[]] := by
    change ((parts []).mergeSort
      (fun a b => decide (objective f 0 0 a ≤ objective f 0 0 b))).head?.getD [] = [[]]
    simp [parts]
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
    change (List.min? [chosen f []]) = some 2
    rw [chosen_empty]
    rfl
  have hh := h [] 1 2 (by decide) hb
  simp [pageLimit] at hh

/-! ## OrdinalResidual -/

theorem sound_OrdinalResidual_StrictSavings : OrdinalResidual.StrictSavings.Goal :=
  strictSavings_holds OrdinalResidual.write

theorem sound_OrdinalResidual_HotPointReads : OrdinalResidual.HotPointReads.Goal :=
  hotPointReads_holds OrdinalResidual.write

theorem sound_OrdinalResidual_SeekCap : OrdinalResidual.SeekCap.Goal :=
  seekCap_holds OrdinalResidual.write

theorem sound_OrdinalResidual_ParallelEncode : OrdinalResidual.ParallelEncode.Goal :=
  parallelEncode_holds OrdinalResidual.write

theorem sound_OrdinalResidual_MandatoryCuts : OrdinalResidual.MandatoryCuts.Goal :=
  mandatoryCuts_holds OrdinalResidual.write

theorem refuted_OrdinalResidual_ByteFrontier : ¬ OrdinalResidual.ByteFrontier.Goal :=
  refute_byteFrontier OrdinalResidual.write

theorem refuted_OrdinalResidual_BoundedRaw : ¬ OrdinalResidual.BoundedRaw.Goal :=
  refute_boundedRaw OrdinalResidual.write

/-! ## SplitHigh -/

theorem sound_SplitHigh_StrictSavings : SplitHigh.StrictSavings.Goal :=
  strictSavings_holds SplitHigh.write

theorem sound_SplitHigh_HotPointReads : SplitHigh.HotPointReads.Goal :=
  hotPointReads_holds SplitHigh.write

theorem sound_SplitHigh_SeekCap : SplitHigh.SeekCap.Goal :=
  seekCap_holds SplitHigh.write

theorem sound_SplitHigh_ParallelEncode : SplitHigh.ParallelEncode.Goal :=
  parallelEncode_holds SplitHigh.write

theorem sound_SplitHigh_MandatoryCuts : SplitHigh.MandatoryCuts.Goal :=
  mandatoryCuts_holds SplitHigh.write

theorem refuted_SplitHigh_ByteFrontier : ¬ SplitHigh.ByteFrontier.Goal :=
  refute_byteFrontier SplitHigh.write

theorem refuted_SplitHigh_BoundedRaw : ¬ SplitHigh.BoundedRaw.Goal :=
  refute_boundedRaw SplitHigh.write

/-! ## MinimumDelta -/

theorem sound_MinimumDelta_StrictSavings : MinimumDelta.StrictSavings.Goal :=
  strictSavings_holds MinimumDelta.write

theorem sound_MinimumDelta_HotPointReads : MinimumDelta.HotPointReads.Goal :=
  hotPointReads_holds MinimumDelta.write

theorem sound_MinimumDelta_SeekCap : MinimumDelta.SeekCap.Goal :=
  seekCap_holds MinimumDelta.write

theorem sound_MinimumDelta_ParallelEncode : MinimumDelta.ParallelEncode.Goal :=
  parallelEncode_holds MinimumDelta.write

theorem sound_MinimumDelta_MandatoryCuts : MinimumDelta.MandatoryCuts.Goal :=
  mandatoryCuts_holds MinimumDelta.write

theorem refuted_MinimumDelta_ByteFrontier : ¬ MinimumDelta.ByteFrontier.Goal :=
  refute_byteFrontier MinimumDelta.write

theorem refuted_MinimumDelta_BoundedRaw : ¬ MinimumDelta.BoundedRaw.Goal :=
  refute_boundedRaw MinimumDelta.write

/-! ## SparsePlanes -/

theorem sound_SparsePlanes_StrictSavings : SparsePlanes.StrictSavings.Goal :=
  strictSavings_holds SparsePlanes.write

theorem sound_SparsePlanes_HotPointReads : SparsePlanes.HotPointReads.Goal :=
  hotPointReads_holds SparsePlanes.write

theorem sound_SparsePlanes_SeekCap : SparsePlanes.SeekCap.Goal :=
  seekCap_holds SparsePlanes.write

theorem sound_SparsePlanes_ParallelEncode : SparsePlanes.ParallelEncode.Goal :=
  parallelEncode_holds SparsePlanes.write

theorem sound_SparsePlanes_MandatoryCuts : SparsePlanes.MandatoryCuts.Goal :=
  mandatoryCuts_holds SparsePlanes.write

theorem refuted_SparsePlanes_ByteFrontier : ¬ SparsePlanes.ByteFrontier.Goal :=
  refute_byteFrontier SparsePlanes.write

theorem refuted_SparsePlanes_BoundedRaw : ¬ SparsePlanes.BoundedRaw.Goal :=
  refute_boundedRaw SparsePlanes.write

/-! ## SharedDivisor -/

theorem sound_SharedDivisor_StrictSavings : SharedDivisor.StrictSavings.Goal :=
  strictSavings_holds SharedDivisor.write

theorem sound_SharedDivisor_HotPointReads : SharedDivisor.HotPointReads.Goal :=
  hotPointReads_holds SharedDivisor.write

theorem sound_SharedDivisor_SeekCap : SharedDivisor.SeekCap.Goal :=
  seekCap_holds SharedDivisor.write

theorem sound_SharedDivisor_ParallelEncode : SharedDivisor.ParallelEncode.Goal :=
  parallelEncode_holds SharedDivisor.write

theorem sound_SharedDivisor_MandatoryCuts : SharedDivisor.MandatoryCuts.Goal :=
  mandatoryCuts_holds SharedDivisor.write

theorem refuted_SharedDivisor_ByteFrontier : ¬ SharedDivisor.ByteFrontier.Goal :=
  refute_byteFrontier SharedDivisor.write

theorem refuted_SharedDivisor_BoundedRaw : ¬ SharedDivisor.BoundedRaw.Goal :=
  refute_boundedRaw SharedDivisor.write

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelectionB
