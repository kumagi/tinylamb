import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SeparatedLengths.ByteFrontier` — Use the actual codec and raw fallback in the
  size/seek optimum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_ByteFrontier

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SeparatedLengths

/-! The plan is a minimum of the very objective priced by the dynamic program,
so the byte frontier identity holds for every writer (in particular the
StreamVByte writer). -/

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
  | some v => simp

private theorem exists_eq_min_getD (l : List Nat) (hne : l ≠ []) :
    ∃ x ∈ l, x = l.min?.getD 0 := by
  cases l with
  | nil => contradiction
  | cons a t =>
      refine ⟨(a :: t).min?.getD 0, ?_, rfl⟩
      exact (List.min?_eq_some_iff.mp
        (min?_eq_some_getD_of_mem_nat (a :: t) a (by simp))).1

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

private theorem charge_append (f : Writer) (fee : Nat) (ps qs : List (List Nat)) :
    charge f fee (ps ++ qs) = charge f fee ps + charge f fee qs := by
  unfold charge
  simp [List.map_append, List.sum_append]

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

/-! Dynamic-programming facade: `seg n off edge` is the optimal suffix cost. -/

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
          congr 1 <;> omega

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

/-! The dynamic program equals the minimum of the plan objective over `parts`. -/

private theorem seg_lower (f : Writer) (fee price : Nat) :
    ∀ n, ∀ xs : List Nat, xs.length = n →
      ∀ ps ∈ parts xs,
        seg n 0 (intervalEdge f fee price xs) ≤ objective f fee price ps := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hlen ps hps
      cases n with
      | zero =>
          have hnil : xs = [] := by
            apply List.length_eq_zero_iff.mp
            simpa using hlen
          subst xs
          simp [parts] at hps
          subst ps
          simp [seg, objective]
      | succ m =>
          cases xs with
          | nil => simp at hlen
          | cons x xs' =>
              obtain ⟨k, hkpos, hklen, tail, hrfl, htail⟩ :=
                parts_decompose (x :: xs') (by simp) ps hps
              subst ps
              let ys : List Nat := (x :: xs').drop k
              have hyslen : ys.length = m + 1 - k := by
                dsimp [ys]
                rw [List.length_drop, hlen]
              have hlt : m + 1 - k < m + 1 := by omega
              have ihys : seg (m + 1 - k) 0 (intervalEdge f fee price ys) ≤
                  objective f fee price tail := ih (m + 1 - k) hlt ys hyslen tail htail
              let edge := intervalEdge f fee price (x :: xs')
              have hkmem : k - 1 ∈ List.range (m + 1) := by
                simp [List.mem_range]
                omega
              have hoptmem : edge 0 ((k - 1) + 1) + seg (m - (k - 1)) ((k - 1) + 1) edge ∈
                  (List.range (m + 1)).map (fun d =>
                    edge 0 (d + 1) + seg (m - d) (d + 1) edge) := by
                exact List.mem_map.mpr ⟨k - 1, hkmem, rfl⟩
              have hunfold : seg (m + 1) 0 edge =
                  ((List.range (m + 1)).map (fun d =>
                    edge 0 (d + 1) + seg (m - d) (d + 1) edge)).min?.getD 0 := by
                simp [seg]
              have hsegle : seg (m + 1) 0 edge ≤
                  edge 0 ((k - 1) + 1) + seg (m - (k - 1)) ((k - 1) + 1) edge := by
                rw [hunfold]
                exact min?_getD_le_mem_nat _ _ hoptmem
              have heqk : (k - 1) + 1 = k := by omega
              have hrest : m - (k - 1) = m + 1 - k := by omega
              have hsegle' : seg (m + 1) 0 edge ≤ edge 0 k + seg (m + 1 - k) k edge := by
                rw [heqk, hrest] at hsegle
                exact hsegle
              have hsegdrop : seg (m + 1 - k) k edge =
                  seg (m + 1 - k) 0 (intervalEdge f fee price ys) := by
                dsimp [edge, ys]
                exact seg_interval_drop f fee price (x :: xs') (m + 1 - k) k
              have hedge0 : edge 0 k =
                  fee + chosen f ((x :: xs').take k) + price * k ^ 2 := by
                simp [edge, intervalEdge]
              have hobj : objective f fee price ((x :: xs').take k :: tail) =
                  fee + chosen f ((x :: xs').take k) + price * k ^ 2 +
                    objective f fee price tail :=
                objective_cons f fee price (x :: xs') k tail hklen
              change seg (m + 1) 0 edge ≤ objective f fee price ((x :: xs').take k :: tail)
              rw [hsegdrop, hedge0] at hsegle'
              rw [hobj]
              omega

private theorem seg_achieved (f : Writer) (fee price : Nat) :
    ∀ n, ∀ xs : List Nat, xs.length = n →
      ∃ ps ∈ parts xs, objective f fee price ps = seg n 0 (intervalEdge f fee price xs) := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hlen
      cases n with
      | zero =>
          have hnil : xs = [] := by
            apply List.length_eq_zero_iff.mp
            simpa using hlen
          subst xs
          refine ⟨[], by simp [parts], ?_⟩
          simp [seg, objective, charge, seekWork]
      | succ m =>
          cases xs with
          | nil => simp at hlen
          | cons x xs' =>
              let edge := intervalEdge f fee price (x :: xs')
              let opts := (List.range (m + 1)).map (fun d =>
                edge 0 (d + 1) + seg (m - d) (d + 1) edge)
              have hopts_ne : opts ≠ [] := by simp [opts]
              obtain ⟨v, hv_mem, hv_eq⟩ := exists_eq_min_getD opts hopts_ne
              rcases List.mem_map.mp hv_mem with ⟨d, hdmem, hv_val⟩
              have hdlt : d < m + 1 := by simpa [List.mem_range] using hdmem
              let k := d + 1
              have hkpos : 0 < k := by dsimp [k]; omega
              have hklen : k ≤ (x :: xs').length := by
                dsimp [k]
                simp only [List.length_cons] at hlen
                omega
              let ys : List Nat := (x :: xs').drop k
              have hyslen : ys.length = m + 1 - k := by
                dsimp [ys]
                rw [List.length_drop, hlen]
              obtain ⟨tail, htail, htail_eq⟩ := ih (m + 1 - k) (by omega) ys hyslen
              have hps_mem : (x :: xs').take k :: tail ∈ parts (x :: xs') :=
                parts_prepend (x :: xs') k tail hkpos hklen htail
              refine ⟨(x :: xs').take k :: tail, hps_mem, ?_⟩
              have hobj : objective f fee price ((x :: xs').take k :: tail) =
                  fee + chosen f ((x :: xs').take k) + price * k ^ 2 +
                    objective f fee price tail :=
                objective_cons f fee price (x :: xs') k tail hklen
              have hedge0 : edge 0 k =
                  fee + chosen f ((x :: xs').take k) + price * k ^ 2 := by
                simp [edge, intervalEdge]
              have hsegdrop : seg (m + 1 - k) k edge =
                  seg (m + 1 - k) 0 (intervalEdge f fee price ys) := by
                dsimp [edge, ys]
                exact seg_interval_drop f fee price (x :: xs') (m + 1 - k) k
              have hunfold : seg (m + 1) 0 edge = opts.min?.getD 0 := by
                simp [seg, opts]
              rw [hunfold, ← hv_eq, ← hv_val]
              have hdk : d + 1 = k := rfl
              have hmd : m - d = m + 1 - k := by
                dsimp [k]
                omega
              rw [hdk, hmd]
              rw [hobj, ← hedge0, hsegdrop, ← htail_eq]

theorem sound_ByteFrontier : SeparatedLengths.ByteFrontier.Goal := by
  intro xs fee price ps _hadm hps
  let f : Writer := SeparatedLengths.write
  let pick := plan f fee price xs
  let obj := objective f fee price
  change quotedOptimum f fee price xs = obj pick ∧
    quotedOptimum f fee price xs ≤ obj ps
  have hq0 : quotedOptimum f fee price xs =
      boundaryCost xs.length (intervalEdge f fee price xs) := by
    rfl
  have hq1 : boundaryCost xs.length (intervalEdge f fee price xs) =
      seg xs.length 0 (intervalEdge f fee price xs) :=
    boundaryCost_eq_seg xs.length (intervalEdge f fee price xs)
  have hq : quotedOptimum f fee price xs =
      seg xs.length 0 (intervalEdge f fee price xs) := hq0.trans hq1
  have hlower_pick : seg xs.length 0 (intervalEdge f fee price xs) ≤ obj pick :=
    seg_lower f fee price xs.length xs rfl pick (plan_mem_parts f fee price xs)
  obtain ⟨p0, hp0mem, hp0eq⟩ := seg_achieved f fee price xs.length xs rfl
  have hp0eq' : obj p0 = seg xs.length 0 (intervalEdge f fee price xs) := by
    simpa [obj] using hp0eq
  have hmin : obj pick ≤ obj p0 := plan_min f fee price xs p0 hp0mem
  have hupper : obj pick ≤ seg xs.length 0 (intervalEdge f fee price xs) := by
    rw [hp0eq'] at hmin
    exact hmin
  have heq : quotedOptimum f fee price xs = obj pick := by
    rw [hq]
    omega
  have hlower_ps : quotedOptimum f fee price xs ≤ obj ps := by
    rw [hq]
    exact seg_lower f fee price xs.length xs rfl ps hps
  exact ⟨heq, hlower_ps⟩

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_ByteFrontier
