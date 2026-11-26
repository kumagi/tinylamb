import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SharedDivisor.ByteFrontier` — Use the actual codec and raw fallback in the size/seek
  optimum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_ByteFrontier

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SharedDivisor

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private def objective (f : Writer) (fee price : Nat) (ps : List (List Nat)) : Nat :=
  charge f fee ps + price * seekWork ps

private theorem parts_flatten (xs : List Nat) (ps : List (List Nat))
    (h : ps ∈ parts xs) : ps.flatten = xs := by
  induction xs generalizing ps with
  | nil =>
      simp [parts] at h
      simp [h]
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨ps', hps', hmem⟩
      cases ps' with
      | nil =>
          dsimp only at hmem
          have hsing : ps = [[x]] := by simpa using hmem
          subst ps
          have hflat := ih [] hps'
          simp at hflat
          subst xs
          simp
      | cons a rest =>
          dsimp only at hmem
          have hcases : ps = [x] :: a :: rest ∨ ps = (x :: a) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hcases with rfl | rfl <;>
            simpa [List.flatten_cons] using congrArg (List.cons x) (ih (a :: rest) hps')

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

private def intervalEdge (f : Writer) (fee price : Nat) (xs : List Nat)
    (a b : Nat) : Nat :=
  fee + chosen f ((xs.drop a).take (b - a)) + price * (b - a) ^ 2

private theorem seg_interval_drop (f : Writer) (fee price : Nat) :
    ∀ (xs : List Nat) n k,
      seg n k (intervalEdge f fee price xs) =
        seg n 0 (intervalEdge f fee price (xs.drop k)) := by
  intro xs n
  induction n using Nat.strongRecOn generalizing xs with
  | ind n ih =>
      intro k
      cases n with
      | zero => simp [seg]
      | succ m =>
          simp only [seg, Nat.zero_add]
          apply congrArg (fun l : List Nat => l.min?.getD 0)
          apply List.map_congr_left
          intro d hd
          have hd' : d ≤ m := by
            have hmem := hd
            simp [List.mem_range] at hmem
            omega
          have hedge : intervalEdge f fee price xs k (k + d + 1) =
              intervalEdge f fee price (xs.drop k) 0 (d + 1) := by
            have hsub : k + d + 1 - k = d + 1 := by omega
            simp [intervalEdge, List.drop_drop, hsub]
          rw [hedge]
          rw [show k + d + 1 = k + (d + 1) by omega]
          rw [ih (m - d) (by omega) xs (k + (d + 1))]
          rw [← List.drop_drop]
          rw [← ih (m - d) (by omega) (xs.drop k) (d + 1)]

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

private theorem min?_getD_mem (l : List Nat) (hne : l ≠ []) : l.min?.getD 0 ∈ l := by
  cases l with
  | nil => contradiction
  | cons y ys =>
      rcases hmn : (y :: ys).min? with _ | M
      · have hnil : (y :: ys) = [] := List.min?_eq_none_iff.mp hmn
        simp at hnil
      · simp only [Option.getD_some]
        exact (List.min?_eq_some_iff.mp hmn).1

/-- The DP optimum is a lower bound for every contiguous partition. -/
private theorem seg_le_objective (f : Writer) (fee price : Nat) :
    ∀ (xs : List Nat) (ps : List (List Nat)), ps ∈ parts xs →
      seg xs.length 0 (intervalEdge f fee price xs) ≤ objective f fee price ps := by
  let P : Nat → Prop := fun n => ∀ (ys : List Nat), ys.length = n →
      ∀ (ps : List (List Nat)), ps ∈ parts ys →
        seg ys.length 0 (intervalEdge f fee price ys) ≤ objective f fee price ps
  have hP : ∀ n, P n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro ys hylen ps hps
        by_cases hnil : ys = []
        · subst ys
          simp [parts] at hps
          subst ps
          simp [seg, objective]
        · obtain ⟨k, hkpos, hklen, tail, hps_eq, htail⟩ :=
            parts_decompose ys hnil ps hps
          subst ps
          have hlt : (ys.drop k).length < n := by
            rw [← hylen]
            simp only [List.length_drop]
            omega
          have ih' : seg (ys.length - k) 0 (intervalEdge f fee price (ys.drop k)) ≤
              objective f fee price tail := by
            simpa only [List.length_drop] using
              (ih (ys.drop k).length hlt (ys.drop k) rfl tail htail)
          have hedge0 : intervalEdge f fee price ys 0 k =
              fee + chosen f (ys.take k) + price * k ^ 2 := by
            simp [intervalEdge]
          have hobj : objective f fee price (ys.take k :: tail) =
              intervalEdge f fee price ys 0 k + objective f fee price tail := by
            rw [objective_cons f fee price ys k tail hklen]
            rw [hedge0]
          have hsegstep : seg ys.length 0 (intervalEdge f fee price ys) ≤
              intervalEdge f fee price ys 0 k +
                seg (ys.length - k) k (intervalEdge f fee price ys) := by
            have hlenpos : 0 < ys.length := List.length_pos_iff.mpr hnil
            rcases Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hlenpos) with ⟨m, hm⟩
            rw [hm]
            simp only [seg, Nat.zero_add]
            apply min?_getD_le_mem_nat
            apply List.mem_map.mpr
            refine ⟨k - 1, ?_, ?_⟩
            · simp [List.mem_range]
              omega
            · have hk1 : k - 1 + 1 = k := by omega
              have hmk : m - (k - 1) = m + 1 - k := by omega
              rw [hk1, hmk]
          have hdrop := seg_interval_drop f fee price ys (ys.length - k) k
          rw [hdrop] at hsegstep
          calc
            seg ys.length 0 (intervalEdge f fee price ys)
                ≤ intervalEdge f fee price ys 0 k +
                    seg (ys.length - k) 0 (intervalEdge f fee price (ys.drop k)) := hsegstep
            _ ≤ intervalEdge f fee price ys 0 k + objective f fee price tail :=
                  Nat.add_le_add_left ih' _
            _ = objective f fee price (ys.take k :: tail) := hobj.symm
  intro xs
  exact hP xs.length xs rfl

/-- The DP optimum is attained by some contiguous partition. -/
private theorem exists_seg_objective (f : Writer) (fee price : Nat) (xs : List Nat) :
    ∃ ps ∈ parts xs, objective f fee price ps ≤
      seg xs.length 0 (intervalEdge f fee price xs) := by
  let P : Nat → Prop := fun n => ∀ (ys : List Nat), ys.length = n →
      ∃ ps ∈ parts ys, objective f fee price ps ≤
        seg ys.length 0 (intervalEdge f fee price ys)
  have hP : ∀ n, P n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro ys hylen
        by_cases hnil : ys = []
        · subst ys
          refine ⟨[], by simp [parts], ?_⟩
          simp [seg, objective, charge, seekWork]
        · have hnpos : 0 < n := by
            rw [← hylen]
            exact List.length_pos_iff.mpr hnil
          let edge := intervalEdge f fee price ys
          let opts : List Nat := (List.range n).map (fun d =>
            edge 0 (d + 1) + seg (n - 1 - d) (d + 1) edge)
          have hopts_ne : opts ≠ [] := by
            simp [opts]
            omega
          have hminmem : opts.min?.getD 0 ∈ opts := min?_getD_mem opts hopts_ne
          obtain ⟨d, hdmem, hdval⟩ := List.mem_map.mp hminmem
          have hdlt : d < n := by simpa [List.mem_range] using hdmem
          let k := d + 1
          have hkpos : 0 < k := by dsimp [k]; omega
          have hklen : k ≤ ys.length := by
            rw [hylen]
            dsimp [k]
            omega
          have hseg_eq : seg ys.length 0 edge = opts.min?.getD 0 := by
            rw [hylen]
            cases n with
            | zero => omega
            | succ m => simp [seg, opts]
          have hlt : (ys.drop k).length < n := by
            rw [← hylen]
            simp only [List.length_drop]
            dsimp [k]
            omega
          obtain ⟨tail, htail, htail_le⟩ :=
            ih (ys.drop k).length hlt (ys.drop k) rfl
          have hdrop := seg_interval_drop f fee price ys (ys.length - k) k
          have htail_le0 : objective f fee price tail ≤
              seg (ys.length - k) 0 (intervalEdge f fee price (ys.drop k)) := by
            simpa only [List.length_drop] using htail_le
          rw [← hdrop] at htail_le0
          have hlen_eq : ys.length - k = n - 1 - d := by
            rw [hylen]
            dsimp [k]
            omega
          have hk_eq : k = d + 1 := by rfl
          rw [hlen_eq, hk_eq] at htail_le0
          have hedge0 : edge 0 k = fee + chosen f (ys.take k) + price * k ^ 2 := by
            simp [edge, intervalEdge]
          have hobj : objective f fee price (ys.take k :: tail) =
              edge 0 k + objective f fee price tail := by
            rw [objective_cons f fee price ys k tail hklen]
            rw [hedge0]
          refine ⟨ys.take k :: tail, parts_prepend ys k tail hkpos hklen htail, ?_⟩
          rw [hobj, hseg_eq]
          rw [← hdval]
          apply Nat.add_le_add_left
          simpa [k] using htail_le0
  exact hP xs.length xs rfl

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

theorem sound_ByteFrontier : SharedDivisor.ByteFrontier.Goal := by
  change byteFrontier write
  intro xs fee price ps hadm hps
  change quotedOptimum write fee price xs =
      charge write fee (plan write fee price xs) + price * seekWork (plan write fee price xs) ∧
    quotedOptimum write fee price xs ≤ charge write fee ps + price * seekWork ps
  let q := quotedOptimum write fee price xs
  let pick := plan write fee price xs
  have hqseg : q = seg xs.length 0 (intervalEdge write fee price xs) := by
    simp only [q, quotedOptimum]
    rw [boundaryCost_eq_seg]
    congr 1
  have hA : ∀ p ∈ parts xs, q ≤ objective write fee price p := by
    intro p hp
    rw [hqseg]
    exact seg_le_objective write fee price xs p hp
  have hB : ∃ p ∈ parts xs, objective write fee price p ≤ q := by
    obtain ⟨p, hp, hle⟩ := exists_seg_objective write fee price xs
    exact ⟨p, hp, by rw [hqseg]; exact hle⟩
  have hpick : pick ∈ parts xs := by
    dsimp [pick]
    exact plan_mem_parts write fee price xs
  have hq_le_pick : q ≤ objective write fee price pick := hA pick hpick
  have hpick_le_q : objective write fee price pick ≤ q := by
    obtain ⟨p, hp, hle⟩ := hB
    exact Nat.le_trans (by simpa [pick] using (plan_min write fee price xs p hp)) hle
  have heq_obj : q = objective write fee price pick :=
    Nat.le_antisymm hq_le_pick hpick_le_q
  constructor
  · simpa [q, pick, objective] using heq_obj
  · simpa [q, objective] using hA ps hps

/-! Truth was first confirmed by evaluation on small inputs: the quoted optimum
equals the chosen plan's cost and lower-bounds every candidate partition. -/
private def checkSmall (xs : List Nat) (fee price : Nat) : Bool :=
  let pick := plan write fee price xs
  let q := quotedOptimum write fee price xs
  q == charge write fee pick + price * seekWork pick &&
    (parts xs).all (fun ps => q ≤ charge write fee ps + price * seekWork ps)

#eval checkSmall [] 0 0
#eval checkSmall [] 3 5
#eval checkSmall [0, 1] 3 5
#eval checkSmall [2, 2, 2] 1 9

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_ByteFrontier
