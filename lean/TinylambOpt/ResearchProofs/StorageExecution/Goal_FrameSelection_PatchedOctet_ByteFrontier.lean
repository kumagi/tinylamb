import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `PatchedOctet.ByteFrontier` — Use the actual codec and raw fallback in the size/seek
  optimum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_ByteFrontier

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.PatchedOctet

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
  | cons x xs ih => simp [List.foldl_cons]

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
        rw [ite_eq_left rfl]
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
      · rw [ite_eq_right hjeq]
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
            simp [List.take_succ_cons]

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

private theorem seg_succ (m off : Nat) (edge : Nat → Nat → Nat) :
    seg (m + 1) off edge =
      ((List.range (m + 1)).map (fun d =>
        edge off (off + d + 1) + seg (m - d) (off + d + 1) edge)).min?.getD 0 := by
  simp [seg]

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
      simp [Nat.add_assoc]

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

private theorem seg_le_objective_aux (f : Writer) (fee price : Nat) :
    ∀ n : Nat, ∀ ys : List Nat, ys.length = n →
      ∀ ps ∈ parts ys,
        seg ys.length 0 (intervalEdge f fee price ys) ≤ objective f fee price ps := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ys hlen ps hps
      cases ys with
      | nil =>
          simp [parts] at hps
          subst ps
          simp [seg, objective, charge, seekWork]
      | cons x xs =>
          have hne : (x :: xs) ≠ [] := by simp
          obtain ⟨k, hkpos, hklen, tail, hps_eq, htail⟩ :=
            parts_decompose (x :: xs) hne ps hps
          subst ps
          let edge := intervalEdge f fee price (x :: xs)
          have hklen' : k ≤ xs.length + 1 := by simpa [edge] using hklen
          have hkle : k ≤ xs.length + 1 := hklen'
          have hdmem : k - 1 ∈ List.range (xs.length + 1) := by
            simp [List.mem_range]
            omega
          have hmem :
              edge 0 ((k - 1) + 1) + seg (xs.length - (k - 1)) ((k - 1) + 1) edge ∈
                (List.range (xs.length + 1)).map (fun d =>
                  edge 0 (d + 1) + seg (xs.length - d) (d + 1) edge) := by
            exact List.mem_map.mpr ⟨k - 1, hdmem, by
              congr 2 <;> omega⟩
          have hseg_min :
              seg (xs.length + 1) 0 edge ≤
                edge 0 ((k - 1) + 1) + seg (xs.length - (k - 1)) ((k - 1) + 1) edge := by
            rw [seg_succ]
            simpa [Nat.zero_add] using (min?_getD_le_mem_nat _ _ hmem)
          have hdrop : (x :: xs).drop k = xs.drop (k - 1) := by
            cases k with
            | zero => omega
            | succ k' => simp
          have hdrop_len : (xs.drop (k - 1)).length = xs.length + 1 - k := by
            simp [List.length_drop]
            omega
          have hseg_shift :
              seg (xs.length + 1 - k) k edge =
                seg ((x :: xs).drop k).length 0
                  (intervalEdge f fee price ((x :: xs).drop k)) := by
            rw [hdrop, hdrop_len, ← hdrop]
            exact seg_interval_drop f fee price (x :: xs) (xs.length + 1 - k) k
          have hlt : ((x :: xs).drop k).length < n := by
            rw [hdrop, hdrop_len, ← hlen]
            simp only [List.length_cons]
            omega
          have hih := ih (((x :: xs).drop k).length) hlt ((x :: xs).drop k) rfl tail htail
          have hopt_edge :
              edge 0 k = fee + chosen f ((x :: xs).take k) + price * k ^ 2 := by
            simp [edge, intervalEdge]
          calc
            seg (x :: xs).length 0 (intervalEdge f fee price (x :: xs))
                = seg (xs.length + 1) 0 edge := by rfl
            _ ≤ edge 0 ((k - 1) + 1) + seg (xs.length - (k - 1)) ((k - 1) + 1) edge := hseg_min
            _ = edge 0 k + seg (xs.length + 1 - k) k edge := by
              congr 2 <;> omega
            _ = edge 0 k + seg ((x :: xs).drop k).length 0
                  (intervalEdge f fee price ((x :: xs).drop k)) := by rw [hseg_shift]
            _ ≤ edge 0 k + objective f fee price tail := Nat.add_le_add_left hih _
            _ = fee + chosen f ((x :: xs).take k) + price * k ^ 2 +
                  objective f fee price tail := by rw [hopt_edge]
            _ = objective f fee price ((x :: xs).take k :: tail) := by
              rw [objective_cons f fee price (x :: xs) k tail hklen']

private theorem seg_le_objective (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    seg xs.length 0 (intervalEdge f fee price xs) ≤ objective f fee price ps :=
  seg_le_objective_aux f fee price xs.length xs rfl ps hps

private theorem exists_part_eq_seg_aux (f : Writer) (fee price : Nat) :
    ∀ n : Nat, ∀ ys : List Nat, ys.length = n →
      ∃ ps ∈ parts ys,
        objective f fee price ps = seg ys.length 0 (intervalEdge f fee price ys) := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ys hlen
      cases ys with
      | nil =>
          refine ⟨[], by simp [parts], ?_⟩
          simp [seg, objective, charge, seekWork]
      | cons x xs =>
          let edge := intervalEdge f fee price (x :: xs)
          let opts := (List.range (xs.length + 1)).map (fun d =>
            edge 0 (d + 1) + seg (xs.length - d) (d + 1) edge)
          have hne : opts ≠ [] := by
            dsimp [opts]
            simp
          have hmin_ne : opts.min? ≠ none := by
            intro hnone
            have heq : opts = [] := List.min?_eq_none_iff.mp hnone
            exact hne heq
          rcases hmn : opts.min? with _ | M
          · exact (hmin_ne hmn).elim
          · have hmemM : M ∈ opts := (List.min?_eq_some_iff.mp hmn).1
            rcases List.mem_map.mp hmemM with ⟨d, hd, hdM⟩
            have hdle : d ≤ xs.length := by
              have hmem := hd
              simp [List.mem_range] at hmem
              omega
            let k := d + 1
            have hkpos : 0 < k := by dsimp [k]; omega
            have hklen : k ≤ (x :: xs).length := by dsimp [k]; simp; omega
            have hdrop : (x :: xs).drop k = xs.drop (k - 1) := by
              dsimp [k]
              rw [show d + 1 = Nat.succ d by omega]
              simp
            have hdrop_len : (xs.drop (k - 1)).length = xs.length - d := by
              simp [List.length_drop, k]
            have hrest_len : ((x :: xs).drop k).length < n := by
              rw [hdrop, hdrop_len]
              have hlen' : xs.length + 1 = n := by
                rw [← hlen]
                simp only [List.length_cons]
              omega
            obtain ⟨tail, htail, htail_eq⟩ :=
              ih (((x :: xs).drop k).length) hrest_len ((x :: xs).drop k) rfl
            have hprepend : (x :: xs).take k :: tail ∈ parts (x :: xs) :=
              parts_prepend (x :: xs) k tail hkpos hklen htail
            refine ⟨(x :: xs).take k :: tail, hprepend, ?_⟩
            have hobj : objective f fee price ((x :: xs).take k :: tail) =
                edge 0 k + objective f fee price tail := by
              rw [objective_cons f fee price (x :: xs) k tail hklen]
              simp [edge, intervalEdge, k]
            have hshift :
                seg (xs.length - d) (d + 1) edge =
                  seg ((x :: xs).drop k).length 0
                    (intervalEdge f fee price ((x :: xs).drop k)) := by
              rw [hdrop, hdrop_len, ← hdrop]
              exact seg_interval_drop f fee price (x :: xs) (xs.length - d) k
            rw [hshift] at hdM
            rw [← htail_eq] at hdM
            have hseg_eq : seg (x :: xs).length 0 (intervalEdge f fee price (x :: xs)) = M := by
              change seg (xs.length + 1) 0 edge = M
              rw [seg_succ]
              have hmn' :
                  ((List.range (xs.length + 1)).map (fun d =>
                    edge 0 (0 + d + 1) + seg (xs.length - d) (0 + d + 1) edge)).min? = some M := by
                simpa [opts, Nat.zero_add] using hmn
              rw [hmn']
              rfl
            rw [hobj, hseg_eq]
            simpa [k] using hdM

private theorem exists_part_eq_seg (f : Writer) (fee price : Nat) (xs : List Nat) :
    ∃ ps ∈ parts xs,
      objective f fee price ps = seg xs.length 0 (intervalEdge f fee price xs) :=
  exists_part_eq_seg_aux f fee price xs.length xs rfl

theorem sound_ByteFrontier : PatchedOctet.ByteFrontier.Goal := by
  intro xs fee price ps _ hps
  let pick := plan write fee price xs
  let edge := intervalEdge write fee price xs
  have hfun :
      (fun a b => fee + chosen write ((xs.drop a).take (b - a)) + price * (b - a) ^ 2) = edge := by
    funext a b
    rfl
  have hq : quotedOptimum write fee price xs = seg xs.length 0 edge := by
    rw [quotedOptimum, boundaryCost_eq_seg, hfun]
  have hle : ∀ qs ∈ parts xs, seg xs.length 0 edge ≤ objective write fee price qs := by
    intro qs hqs
    simpa [edge] using seg_le_objective write fee price xs qs hqs
  have hmin : ∀ qs ∈ parts xs, objective write fee price pick ≤ objective write fee price qs := by
    intro qs hqs
    simpa [pick] using plan_min write fee price xs qs hqs
  have hpick_mem : pick ∈ parts xs := by
    simpa [pick] using plan_mem_parts write fee price xs
  obtain ⟨qs, hqs_mem, hqs_eq⟩ := exists_part_eq_seg write fee price xs
  have hpick_le : objective write fee price pick ≤ seg xs.length 0 edge := by
    rw [← hqs_eq]
    exact hmin qs hqs_mem
  have hseg_le : seg xs.length 0 edge ≤ objective write fee price pick := hle pick hpick_mem
  have heq : seg xs.length 0 edge = objective write fee price pick :=
    Nat.le_antisymm hseg_le hpick_le
  constructor
  · simpa [objective, hq] using heq
  · simpa [objective, hq] using hle ps hps

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_ByteFrontier
