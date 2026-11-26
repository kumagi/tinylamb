import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_ByteFrontier

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

/-! Sanity checks: no counterexample among short inputs with small fees/prices. -/

private def check (xs : List Nat) (fee price : Nat) : Bool :=
  let pick := plan write fee price xs
  let q := quotedOptimum write fee price xs
  q == charge write fee pick + price * seekWork pick &&
    (parts xs).all (fun ps => q ≤ charge write fee ps + price * seekWork ps)

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => (gen n).flatMap (fun l => [0 :: l, 1 :: l, 2 :: l])

#eval (List.range 6).any (fun n =>
  (gen n).any (fun xs =>
    (List.range 4).any (fun fee =>
      (List.range 4).any (fun price => !(check xs fee price)))))
#eval check [] 0 0
#eval check [0,1] 3 5
#eval check [2,2,2] 1 9

/-! ## Generic objective -/

private def objective (f : Writer) (fee price : Nat) (ps : List (List Nat)) : Nat :=
  charge f fee ps + price * seekWork ps

private theorem min?_getD_le_of_mem {l : List Nat} {a : Nat} (h : a ∈ l) :
    l.min?.getD 0 ≤ a := by
  have hs : l.min? = some (l.min?.getD 0) := by
    cases hm : l.min? with
    | none =>
        rw [List.min?_eq_none_iff] at hm
        simp [hm] at h
    | some m => simp [hm]
  exact (List.min?_eq_some_iff.mp hs).2 a h

private theorem min?_is_min (l : List Nat) (hne : l ≠ []) :
    l.min?.getD 0 ∈ l ∧ ∀ b ∈ l, l.min?.getD 0 ≤ b := by
  have hs : l.min? = some (l.min?.getD 0) := by
    cases hm : l.min? with
    | none =>
        rw [List.min?_eq_none_iff] at hm
        exact (hne hm).elim
    | some m => simp [hm]
  exact List.min?_eq_some_iff.mp hs

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

private theorem parts_mem_exists (xs : List Nat) : ∃ ps : List (List Nat), ps ∈ parts xs := by
  induction xs with
  | nil => exact ⟨[], by simp [parts]⟩
  | cons x xs ih =>
      rcases ih with ⟨ps', hps'⟩
      refine ⟨match ps' with | [] => [[x]] | a :: rest => [x] :: a :: rest, ?_⟩
      rw [parts]
      apply List.mem_flatMap.mpr
      exact ⟨ps', hps', by cases ps' <;> simp⟩

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

/-! ## The boundary DP equals the quoted optimum -/

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

/-! ## The DP is a lower bound on every partition, and is attained -/

private theorem seg_def_pos (n : Nat) (hn : 0 < n) (e : Nat → Nat → Nat) :
    seg n 0 e =
      ((List.range n).map (fun d => e 0 (d + 1) + seg (n - 1 - d) (d + 1) e)).min?.getD 0 := by
  rw [show n = (n - 1) + 1 by omega]
  simp [seg, Nat.zero_add]

private theorem seg_le_objective (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    seg xs.length 0 (intervalEdge f fee price xs) ≤ objective f fee price ps := by
  let P : Nat → Prop := fun n => ∀ ys : List Nat, ys.length = n →
    ∀ qs ∈ parts ys,
      seg ys.length 0 (intervalEdge f fee price ys) ≤ objective f fee price qs
  have hP : ∀ n, P n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro ys hlen qs hqs
        cases ys with
        | nil =>
            simp [parts] at hqs
            subst qs
            simp [seg, objective, charge, seekWork]
        | cons x ys =>
            obtain ⟨k, hkpos, hkle, tail, hqs_eq, htail⟩ :=
              parts_decompose (x :: ys) (by simp) qs hqs
            subst qs
            rw [objective_cons f fee price (x :: ys) k tail hkle]
            let m := (x :: ys).length
            let e := intervalEdge f fee price (x :: ys)
            have hmpos : 0 < m := by simp [m]
            have hsegdef := seg_def_pos m hmpos e
            have hdlt : k - 1 < m := by simp [m] at hkle ⊢; omega
            have hmem : e 0 (k - 1 + 1) + seg (m - 1 - (k - 1)) (k - 1 + 1) e ∈
                (List.range m).map (fun d => e 0 (d + 1) + seg (m - 1 - d) (d + 1) e) := by
              apply List.mem_map.mpr
              refine ⟨k - 1, ?_, rfl⟩
              simp [hdlt]
            have hmin := min?_getD_le_of_mem hmem
            have harith1 : k - 1 + 1 = k := by omega
            have harith2 : m - 1 - (k - 1) = m - k := by omega
            rw [harith1, harith2] at hmin
            have hshift := seg_interval_drop f fee price (x :: ys) (m - k) k
            rw [hshift] at hmin
            have hlenlt : ((x :: ys).drop k).length < n := by
              have hdrop : ((x :: ys).drop k).length = (x :: ys).length - k := by simp
              rw [hdrop]
              omega
            have hih := ih ((x :: ys).drop k).length hlenlt ((x :: ys).drop k) rfl tail htail
            have hdrop_len : ((x :: ys).drop k).length = m - k := by simp [m]
            rw [hdrop_len] at hih
            change seg m 0 e ≤ intervalEdge f fee price (x :: ys) 0 k +
              objective f fee price tail
            rw [hsegdef]
            dsimp [e] at hmin hih ⊢
            omega
  exact hP xs.length xs rfl ps hps

private theorem exists_seg_witness (f : Writer) (fee price : Nat) (xs : List Nat) :
    ∃ ps ∈ parts xs,
      objective f fee price ps ≤ seg xs.length 0 (intervalEdge f fee price xs) := by
  let P : Nat → Prop := fun n => ∀ ys : List Nat, ys.length = n →
    ∃ ps ∈ parts ys,
      objective f fee price ps ≤ seg ys.length 0 (intervalEdge f fee price ys)
  have hP : ∀ n, P n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro ys hlen
        cases ys with
        | nil =>
            refine ⟨[], by simp [parts], ?_⟩
            simp [seg, objective, charge, seekWork]
        | cons x ys =>
            let m := (x :: ys).length
            let e := intervalEdge f fee price (x :: ys)
            let g : Nat → Nat := fun d => e 0 (d + 1) + seg (m - 1 - d) (d + 1) e
            have hmpos : 0 < m := by simp [m]
            have hsegdef : seg m 0 e = ((List.range m).map g).min?.getD 0 := by
              simpa [g] using seg_def_pos m hmpos e
            have hne : (List.range m).map g ≠ [] := by
              simp [Nat.ne_of_gt hmpos]
            obtain ⟨hminmem, _⟩ := min?_is_min ((List.range m).map g) hne
            obtain ⟨d, hdmem, hdval⟩ := List.mem_map.mp hminmem
            have hdlt : d < m := by simpa using hdmem
            let k := d + 1
            have hkpos : 0 < k := by simp [k]
            have hkle : k ≤ (x :: ys).length := by simp [m, k] at hdlt ⊢; omega
            have hlenlt : ((x :: ys).drop k).length < n := by
              have hdrop : ((x :: ys).drop k).length = (x :: ys).length - k := by simp
              rw [hdrop]
              omega
            obtain ⟨tail, htail, htaille⟩ :=
              ih ((x :: ys).drop k).length hlenlt ((x :: ys).drop k) rfl
            refine ⟨(x :: ys).take k :: tail,
              parts_prepend (x :: ys) k tail hkpos hkle htail, ?_⟩
            rw [objective_cons f fee price (x :: ys) k tail hkle]
            have hg : g d = seg m 0 e := by
              rw [hsegdef]
              exact hdval
            have harith : m - 1 - d = (x :: ys).length - k := by simp [m, k]
            have hg' : e 0 k + seg ((x :: ys).length - k) k e = seg m 0 e := by
              simpa [g, k, harith] using hg
            have hshift' : seg ((x :: ys).length - k) k e =
                seg ((x :: ys).length - k) 0 (intervalEdge f fee price ((x :: ys).drop k)) := by
              simpa [e] using
                seg_interval_drop f fee price (x :: ys) ((x :: ys).length - k) k
            rw [hshift'] at hg'
            have hdrop_len : ((x :: ys).drop k).length = (x :: ys).length - k := by simp
            rw [hdrop_len] at htaille
            change e 0 k + objective f fee price tail ≤ seg m 0 e
            dsimp [e] at hg' ⊢
            have hlen_eq : ys.length + 1 = (x :: ys).length := by simp
            rw [hlen_eq] at hg'
            omega
  exact hP xs.length xs rfl

/-! ## `plan` picks a minimum of the objective -/

private theorem plan_mem_parts (f : Writer) (fee price : Nat) (xs : List Nat) :
    plan f fee price xs ∈ parts xs := by
  let le : List (List Nat) → List (List Nat) → Bool :=
    fun a b => decide (objective f fee price a ≤ objective f fee price b)
  have hperm := List.mergeSort_perm (parts xs) le
  have hpos : 0 < ((parts xs).mergeSort le).length := by
    rw [hperm.length_eq]
    exact List.length_pos_iff_exists_mem.mpr (parts_mem_exists xs)
  change ((parts xs).mergeSort le).head?.getD [] ∈ parts xs
  cases hsort : (parts xs).mergeSort le with
  | nil => simp [hsort] at hpos
  | cons b bl =>
      change b ∈ parts xs
      apply (List.Perm.mem_iff hperm).mp
      rw [hsort]
      simp

private theorem plan_min (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    objective f fee price (plan f fee price xs) ≤ objective f fee price ps := by
  let le : List (List Nat) → List (List Nat) → Bool :=
    fun a b => decide (objective f fee price a ≤ objective f fee price b)
  have htrans : ∀ a b c : List (List Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    simp [le] at hab hbc ⊢
    omega
  have htotal : ∀ a b : List (List Nat), (le a b || le b a) = true := by
    intro a b
    simp [le]
    omega
  have hperm := List.mergeSort_perm (parts xs) le
  have hpair := List.pairwise_mergeSort htrans htotal (parts xs)
  have hpos : 0 < ((parts xs).mergeSort le).length := by
    rw [hperm.length_eq]
    exact List.length_pos_iff_exists_mem.mpr (parts_mem_exists xs)
  cases hsort : (parts xs).mergeSort le with
  | nil => simp [hsort] at hpos
  | cons a ys =>
      have hmem_sort : ps ∈ a :: ys := by
        rw [← hsort]
        exact (List.Perm.mem_iff hperm).mpr hps
      rw [hsort] at hpair
      cases hpair with
      | cons hhead htail =>
          have hleap : le a ps = true := by
            by_cases hEq : ps = a
            · rw [← hEq]
              simp [le]
            · have hmemys : ps ∈ ys := (List.mem_cons.mp hmem_sort).resolve_left hEq
              exact hhead ps hmemys
          have hineq : objective f fee price a ≤ objective f fee price ps := by
            simpa [le] using hleap
          change objective f fee price (((parts xs).mergeSort le).head?.getD []) ≤
            objective f fee price ps
          rw [hsort]
          simp only [List.head?_cons, Option.getD_some]
          exact hineq

/-! ## Goal -/

theorem sound_ByteFrontier : DictionaryRuns.ByteFrontier.Goal := by
  intro xs fee price ps _hadm hps
  have hq : quotedOptimum write fee price xs =
      seg xs.length 0 (intervalEdge write fee price xs) := by
    unfold quotedOptimum
    rw [boundaryCost_eq_seg]
    rfl
  have hlower_ps := seg_le_objective write fee price xs ps hps
  have hlower_pick := seg_le_objective write fee price xs (plan write fee price xs)
    (plan_mem_parts write fee price xs)
  obtain ⟨w, hwmem, hwle⟩ := exists_seg_witness write fee price xs
  have hpick_le_w := plan_min write fee price xs w hwmem
  have hpick_le_seg : objective write fee price (plan write fee price xs) ≤
      seg xs.length 0 (intervalEdge write fee price xs) := by omega
  constructor
  · change quotedOptimum write fee price xs = objective write fee price (plan write fee price xs)
    rw [hq]
    omega
  · change quotedOptimum write fee price xs ≤ objective write fee price ps
    rw [hq]
    exact hlower_ps

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_ByteFrontier
