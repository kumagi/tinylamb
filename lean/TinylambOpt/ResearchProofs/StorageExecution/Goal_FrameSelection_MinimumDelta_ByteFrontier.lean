import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `MinimumDelta.ByteFrontier` — Use the actual codec and raw fallback in the size/seek
  optimum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_ByteFrontier

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.MinimumDelta

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

private theorem min?_some_of_ne_nil (l : List Nat) (hne : l ≠ []) :
    l.min? = some (l.min?.getD 0) := by
  cases l with
  | nil => contradiction
  | cons a t =>
      cases hm : (a :: t).min? with
      | none =>
          have : (a :: t) = [] := List.min?_eq_none_iff.mp hm
          simp at this
      | some v => simp

private theorem min?_le_of_mem (l : List Nat) (x : Nat) (hx : x ∈ l) :
    l.min?.getD 0 ≤ x := by
  have hne : l ≠ [] := by
    intro hnil
    subst l
    simp at hx
  have hm := min?_some_of_ne_nil l hne
  exact (List.min?_eq_some_iff.mp hm).2 x hx

private theorem le_min?_of_all (l : List Nat) (c : Nat) (hne : l ≠ [])
    (hall : ∀ x ∈ l, c ≤ x) : c ≤ l.min?.getD 0 := by
  have hm := min?_some_of_ne_nil l hne
  exact (List.le_min?_iff hm).2 hall

private theorem min?_witness (l : List Nat) (hne : l ≠ []) :
    ∃ x ∈ l, l.min?.getD 0 = x := by
  have hm := min?_some_of_ne_nil l hne
  exact ⟨l.min?.getD 0, (List.min?_eq_some_iff.mp hm).1, rfl⟩

private theorem map_ne_nil {α β : Type} (g : α → β) (l : List α) (h : l ≠ []) :
    l.map g ≠ [] := by
  cases l with
  | nil => contradiction
  | cons a t => simp

private def minCost (f : Writer) (fee price : Nat) (xs : List Nat) : Nat :=
  ((parts xs).map (objective f fee price)).min?.getD 0

private theorem minCost_le_of_mem (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    minCost f fee price xs ≤ objective f fee price ps := by
  unfold minCost
  apply min?_le_of_mem
  exact List.mem_map.mpr ⟨ps, hps, rfl⟩

private theorem le_minCost_of_all (f : Writer) (fee price : Nat) (xs : List Nat)
    (c : Nat) (hall : ∀ ps ∈ parts xs, c ≤ objective f fee price ps) :
    c ≤ minCost f fee price xs := by
  unfold minCost
  apply le_min?_of_all
  · exact map_ne_nil _ _ (parts_ne_nil xs)
  · intro x hx
    rcases List.mem_map.mp hx with ⟨ps, hps, rfl⟩
    exact hall ps hps

private theorem intervalEdge_zero (f : Writer) (fee price : Nat) (ys : List Nat) (k : Nat) :
    intervalEdge f fee price ys 0 k = fee + chosen f (ys.take k) + price * k ^ 2 := by
  simp [intervalEdge]

private theorem objective_singleton (f : Writer) (fee price : Nat) (p : List Nat) :
    objective f fee price [p] = fee + chosen f p + price * p.length ^ 2 := by
  simp [objective, charge, seekWork]

private theorem minCost_cons (f : Writer) (fee price : Nat) (x : Nat) (xs : List Nat) :
    minCost f fee price (x :: xs) =
      ((List.range (xs.length + 1)).map (fun d =>
        intervalEdge f fee price (x :: xs) 0 (d + 1) +
          minCost f fee price (xs.drop d))).min?.getD 0 := by
  let ys := x :: xs
  let opts := (List.range (xs.length + 1)).map (fun d =>
        intervalEdge f fee price ys 0 (d + 1) + minCost f fee price (xs.drop d))
  have hopts_ne : opts ≠ [] := by
    simp [opts]
  apply Nat.le_antisymm
  · apply le_min?_of_all opts (minCost f fee price ys) hopts_ne
    intro o ho
    rcases List.mem_map.mp ho with ⟨d, hdmem, rfl⟩
    have hdle : d ≤ xs.length := by
      simp [List.mem_range] at hdmem
      omega
    let L := (parts (xs.drop d)).map (objective f fee price)
    have hLne : L ≠ [] := by
      intro hnil
      have hpn := parts_ne_nil (xs.drop d)
      rcases hparts : parts (xs.drop d) with _ | ⟨p, ps⟩
      · contradiction
      · simp [L, hparts] at hnil
    obtain ⟨t, htmem, htmin⟩ := min?_witness L hLne
    rcases List.mem_map.mp htmem with ⟨tail, htailparts, htval⟩
    have hmc : minCost f fee price (xs.drop d) = t := by
      change L.min?.getD 0 = t
      exact htmin
    rw [← htval] at hmc
    have htail_drop : tail ∈ parts (ys.drop (d + 1)) := by
      simpa [ys, List.drop_succ_cons] using htailparts
    have hps : ys.take (d + 1) :: tail ∈ parts ys :=
      parts_prepend ys (d + 1) tail (by omega) (by simp [ys]; omega) htail_drop
    have hle := minCost_le_of_mem f fee price ys (ys.take (d + 1) :: tail) hps
    rw [objective_cons f fee price ys (d + 1) tail (by simp [ys]; omega)] at hle
    rw [← hmc] at hle
    have hedge := intervalEdge_zero f fee price ys (d + 1)
    omega
  · apply le_min?_of_all ((parts ys).map (objective f fee price)) (opts.min?.getD 0)
      (map_ne_nil _ _ (parts_ne_nil ys))
    intro c hc
    rcases List.mem_map.mp hc with ⟨ps, hps, rfl⟩
    rcases parts_decompose ys (by simp [ys]) ps hps with
      ⟨k, hkpos, hklen, tail, hps_eq, htail⟩
    subst ps
    let d := k - 1
    have hdle : d ≤ xs.length := by
      dsimp [d]
      simp [ys] at hklen
      omega
    have hdmem : d ∈ List.range (xs.length + 1) := by
      simp [List.mem_range]
      omega
    have hdopt := min?_le_of_mem opts
      (intervalEdge f fee price ys 0 (d + 1) + minCost f fee price (xs.drop d))
      (List.mem_map.mpr ⟨d, hdmem, rfl⟩)
    have hopt_le : opts.min?.getD 0 ≤
        intervalEdge f fee price ys 0 k + minCost f fee price (ys.drop k) := by
      dsimp [d] at hdopt
      have hsucc : k - 1 + 1 = k := Nat.sub_add_cancel hkpos
      have hdrop : xs.drop (k - 1) = ys.drop k := by
        cases k with
        | zero => omega
        | succ k' => simp [ys, List.drop_succ_cons]
      rw [hsucc, hdrop] at hdopt
      exact hdopt
    have hmc_le : minCost f fee price (ys.drop k) ≤ objective f fee price tail :=
      minCost_le_of_mem f fee price (ys.drop k) tail htail
    have hedge := intervalEdge_zero f fee price ys k
    have hobj := objective_cons f fee price ys k tail hklen
    omega

private theorem seg_eq_minCost (f : Writer) (fee price : Nat) :
    ∀ (xs : List Nat) (off : Nat), off ≤ xs.length →
      seg (xs.length - off) off (intervalEdge f fee price xs) =
        minCost f fee price (xs.drop off) := by
  intro xs
  induction xs with
  | nil =>
      intro off hoff
      have hoff0 : off ≤ 0 := by simpa using hoff
      have : off = 0 := Nat.eq_zero_of_le_zero hoff0
      subst off
      simp [seg, minCost, parts, objective, charge, seekWork]
  | cons x xs ih =>
      intro off hoff
      cases off with
      | zero =>
          calc
            seg (xs.length + 1) 0 (intervalEdge f fee price (x :: xs))
                = ((List.range (xs.length + 1)).map (fun d =>
                    intervalEdge f fee price (x :: xs) 0 (d + 1) +
                      minCost f fee price (xs.drop d))).min?.getD 0 := by
                    unfold seg
                    simp only [Nat.zero_add]
                    congr 2
                    apply List.map_congr_left
                    intro d hd
                    have hdle : d ≤ xs.length := by
                      simp [List.mem_range] at hd
                      omega
                    have h1 : seg (xs.length - d) (d + 1)
                          (intervalEdge f fee price (x :: xs)) =
                        seg (xs.length - d) 0 (intervalEdge f fee price (xs.drop d)) := by
                      have hs := seg_interval_drop f fee price (x :: xs) (xs.length - d) (d + 1)
                      simpa [List.drop_succ_cons] using hs
                    have h2 : seg (xs.length - d) d (intervalEdge f fee price xs) =
                        seg (xs.length - d) 0 (intervalEdge f fee price (xs.drop d)) :=
                      seg_interval_drop f fee price xs (xs.length - d) d
                    rw [h1, ← h2, ih d hdle]
            _ = minCost f fee price (x :: xs) := (minCost_cons f fee price x xs).symm
      | succ off' =>
          simp only [List.length_cons] at hoff ⊢
          have hoff' : off' ≤ xs.length := by omega
          have hsub : xs.length + 1 - (off' + 1) = xs.length - off' := by omega
          rw [hsub]
          have h1 : seg (xs.length - off') (off' + 1)
                (intervalEdge f fee price (x :: xs)) =
              seg (xs.length - off') 0 (intervalEdge f fee price (xs.drop off')) := by
            have hs := seg_interval_drop f fee price (x :: xs) (xs.length - off') (off' + 1)
            simpa [List.drop_succ_cons] using hs
          have h2 : seg (xs.length - off') off' (intervalEdge f fee price xs) =
              seg (xs.length - off') 0 (intervalEdge f fee price (xs.drop off')) :=
            seg_interval_drop f fee price xs (xs.length - off') off'
          rw [h1, ← h2]
          simpa [List.drop_succ_cons] using ih off' hoff'

theorem sound_ByteFrontier : MinimumDelta.ByteFrontier.Goal := by
  intro xs fee price ps hadm hps
  by_cases hxnil : xs = []
  · subst xs
    have hp : plan write fee price [] = [] := by
      simp [plan, parts, List.mergeSort_singleton]
    simp [quotedOptimum, boundaryCost, hp, charge, seekWork] at hps ⊢
  · have hq : quotedOptimum write fee price xs = minCost write fee price xs := by
      unfold quotedOptimum
      change boundaryCost xs.length (intervalEdge write fee price xs) =
        minCost write fee price xs
      rw [boundaryCost_eq_seg]
      simpa using seg_eq_minCost write fee price xs 0 (by omega)
    have hpmem : plan write fee price xs ∈ parts xs := plan_mem_parts write fee price xs
    have hmin_le_pick : minCost write fee price xs ≤
        objective write fee price (plan write fee price xs) :=
      minCost_le_of_mem write fee price xs (plan write fee price xs) hpmem
    have hpick_le_min : objective write fee price (plan write fee price xs) ≤
        minCost write fee price xs :=
      le_minCost_of_all write fee price xs _ (fun q hq' =>
        plan_min write fee price xs q hq')
    constructor
    · change quotedOptimum write fee price xs =
        charge write fee (plan write fee price xs) + price * seekWork (plan write fee price xs)
      rw [hq]
      change minCost write fee price xs = objective write fee price (plan write fee price xs)
      exact Nat.le_antisymm hmin_le_pick hpick_le_min
    · change quotedOptimum write fee price xs ≤
        charge write fee ps + price * seekWork ps
      rw [hq]
      change minCost write fee price xs ≤ objective write fee price ps
      exact minCost_le_of_mem write fee price xs ps hps

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_ByteFrontier
