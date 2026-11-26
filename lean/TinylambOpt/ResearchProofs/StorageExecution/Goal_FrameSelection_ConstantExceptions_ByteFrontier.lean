import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_ByteFrontier

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions

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

private theorem min?_attained (l : List Nat) (x : Nat) (hx : x ∈ l) :
    ∃ v ∈ l, v = l.min?.getD 0 := by
  cases h : l.min? with
  | none =>
      have : l = [] := List.min?_eq_none_iff.mp h
      subst this
      simp at hx
  | some v =>
      exact ⟨v, (List.min?_eq_some_iff.mp h).1, by simp⟩

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
          cases ps <;> simp at hps

private theorem parts_exists (xs : List Nat) : ∃ ps, ps ∈ parts xs := by
  cases h : parts xs with
  | nil => exact (parts_ne_nil xs h).elim
  | cons ps rest => exact ⟨ps, by simp⟩

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
          simp only [List.mem_cons, List.not_mem_nil, or_false] at hmem
          rcases hmem with rfl | rfl <;> simp [hflat]

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
          simp only [List.mem_cons, List.not_mem_nil, or_false] at hmem
          rcases hmem with rfl | rfl <;> cases pb <;> simp_all

private theorem parts_single (xs : List Nat) (hxs : xs ≠ []) : [xs] ∈ parts xs := by
  induction xs with
  | nil => simp at hxs
  | cons x xs ih =>
      by_cases hxsnil : xs = []
      · subst xs
        simp [parts]
      · simp only [parts, List.mem_flatMap]
        refine ⟨[xs], ih hxsnil, ?_⟩
        simp

private theorem nil_mem_parts_nil {xs : List Nat} (h : [] ∈ parts xs) : xs = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨qs, hqs, hmem⟩
      cases qs with
      | nil =>
          simp at hmem
      | cons a rest => simp at hmem

private theorem parts_tail {xs : List Nat} {a : List Nat} {rest : List (List Nat)}
    (h : a :: rest ∈ parts xs) : rest ∈ parts (xs.drop a.length) := by
  induction xs generalizing a rest with
  | nil => simp [parts] at h
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨qs, hqs, hmem⟩
      cases qs with
      | nil =>
          have hxsnil : xs = [] := nil_mem_parts_nil hqs
          simp at hmem
          rcases hmem with ⟨rfl, rfl⟩
          subst xs
          simp [parts]
      | cons b bs =>
          simp only [List.mem_cons, List.not_mem_nil, or_false] at hmem
          rcases hmem with hmem | hmem
          · cases hmem
            simpa using hqs
          · have hb := ih (a := b) (rest := bs) hqs
            cases hmem
            simpa using hb

private def seg (n off : Nat) (edge : Nat → Nat → Nat) : Nat :=
  match n with
  | 0 => 0
  | m + 1 =>
    ((List.range (m + 1)).map (fun d =>
      edge off (off + d + 1) + seg (m - d) (off + d + 1) edge)).min?.getD 0

private def bcStep (n : Nat) (edge : Nat → Nat → Nat) (done : Nat)
    (suffix : Array Nat) : Array Nat :=
  let start := n - done - 1
  suffix.push (((List.range (done + 1)).map (fun d =>
    edge start (start + d + 1) + suffix[done - d]?.getD 0)).min?.getD 0)

private def bcBody (n : Nat) (edge : Nat → Nat → Nat) (done : Nat)
    (suffix : Array Nat) : Id (ForInStep (Array Nat)) :=
  pure (.yield (bcStep n edge done suffix))

private def bcRun (n : Nat) (edge : Nat → Nat → Nat) : Nat → Nat → Array Nat → Array Nat
  | _, 0, suffix => suffix
  | done, r + 1, suffix => bcRun n edge (done + 1) r (bcStep n edge done suffix)

private theorem bc_loop (n : Nat) (edge : Nat → Nat → Nat) (done r : Nat)
    (suffix : Array Nat) (hsum : done + r = n) :
    forIn (List.range' done r) suffix (bcBody n edge) =
      pure (bcRun n edge done r suffix) := by
  induction r generalizing done suffix with
  | zero => simp [bcRun]
  | succ r ih =>
      simp only [List.range'_succ, List.forIn_cons, bcBody]
      change forIn (List.range' (done + 1) r) (bcStep n edge done suffix)
        (bcBody n edge) = pure (bcRun n edge done (r + 1) suffix)
      simpa [bcRun] using ih (done + 1) (bcStep n edge done suffix) (by omega)

private theorem bcStep_val (n : Nat) (edge : Nat → Nat → Nat) (done : Nat)
    (suffix : Array Nat) (hdone : done < n)
    (hval : ∀ i, i ≤ done → suffix[i]? = some (seg i (n - i) edge)) :
    bcStep n edge done suffix =
      suffix.push (seg (done + 1) (n - (done + 1)) edge) := by
  unfold bcStep
  rw [seg]
  apply congrArg (Array.push suffix)
  apply congrArg (fun xs : List Nat => xs.min?.getD 0)
  apply List.map_congr_left
  intro d hd
  have hd' : d ≤ done := by
    have := List.mem_range.mp hd
    omega
  rw [hval (done - d) (by omega)]
  simp only [Option.getD_some]
  have hoff : n - (done + 1) + d + 1 = n - (done - d) := by omega
  rw [hoff]
  have hstart : n - (done + 1) = n - done - 1 := by omega
  have hend : n - done - 1 + d + 1 = n - (done - d) := by omega
  rw [hstart, hend]

private theorem bcRun_spec (n : Nat) (edge : Nat → Nat → Nat) (done r : Nat)
    (suffix : Array Nat) (hsum : done + r = n)
    (hsize : suffix.size = done + 1)
    (hval : ∀ i, i ≤ done → suffix[i]? = some (seg i (n - i) edge)) :
    (bcRun n edge done r suffix).size = n + 1 ∧
      ∀ i, i ≤ n → (bcRun n edge done r suffix)[i]? = some (seg i (n - i) edge) := by
  induction r generalizing done suffix with
  | zero =>
      have hdn : done = n := by omega
      subst done
      simpa only [bcRun] using
        (show suffix.size = n + 1 ∧ ∀ i, i ≤ n →
          suffix[i]? = some (seg i (n - i) edge) from ⟨hsize, fun i hi => hval i hi⟩)
  | succ r ih =>
      have hs' : (bcStep n edge done suffix).size = done + 2 := by
        simp [bcStep, Array.size_push, hsize]
      have hv' : ∀ i, i ≤ done + 1 →
          (bcStep n edge done suffix)[i]? = some (seg i (n - i) edge) := by
        intro i hi
        rw [bcStep_val n edge done suffix (by omega) hval, Array.getElem?_push]
        by_cases heq : i = suffix.size
        · simp [heq, hsize]
        · simp [heq]
          exact hval i (by omega)
      simpa only [bcRun] using
        ih (done + 1) (bcStep n edge done suffix) (by omega) hs' hv'

private theorem boundaryCost_eq_seg (n : Nat) (edge : Nat → Nat → Nat) :
    boundaryCost n edge = seg n 0 edge := by
  unfold boundaryCost
  rw [List.range_eq_range']
  change (Id.run (do
    let s ← forIn (List.range' 0 n) #[0] (bcBody n edge)
    pure (s.back?.getD 0))) = seg n 0 edge
  rw [bc_loop n edge 0 n #[0] (by omega)]
  change (bcRun n edge 0 n #[0]).back?.getD 0 = seg n 0 edge
  have hspec := bcRun_spec n edge 0 n #[0] (by omega) (by simp)
    (by intro i hi; simp [Nat.le_zero.mp hi, seg])
  obtain ⟨hsize, hval⟩ := hspec
  have hb : (bcRun n edge 0 n #[0]).back? = some (seg n (n - n) edge) := by
    rw [Array.back?_eq_getElem?, hsize]
    simpa using hval n (by omega)
  simp [hb]

private def blockCost (edge : Nat → Nat → Nat) (start : Nat) (bs : List (List Nat)) : Nat :=
  match bs with
  | [] => 0
  | b :: rest => edge start (start + b.length) + blockCost edge (start + b.length) rest

private theorem dp_le_obj (edge : Nat → Nat → Nat) (start : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    seg xs.length start edge ≤ blockCost edge start ps := by
  let P : Nat → Prop := fun n => ∀ (xs : List Nat) (start : Nat) (ps : List (List Nat)),
    xs.length = n → ps ∈ parts xs → seg n start edge ≤ blockCost edge start ps
  have hP : ∀ n, P n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs start ps hlen hps
        cases xs with
        | nil =>
            simp at hlen
            subst n
            simp [parts] at hps
            subst ps
            simp [seg, blockCost]
        | cons x ys =>
            simp at hlen
            subst n
            simp only [parts, List.mem_flatMap] at hps
            rcases hps with ⟨qs, hqs, hmem⟩
            cases hq : qs with
            | nil =>
                have hxsnil : ys = [] := nil_mem_parts_nil (by simpa [hq] using hqs)
                subst ys
                rw [hq] at hmem
                simp at hmem
                rcases hmem with rfl
                simp [seg, blockCost]
            | cons a rest =>
                rw [hq] at hmem
                simp only [List.mem_cons, List.not_mem_nil, or_false] at hmem
                rcases hmem with rfl | rfl
                · rw [seg]
                  have hd0 : 0 ∈ List.range (ys.length + 1) := by simp
                  have hm := min?_getD_le_mem_nat
                    ((List.range (ys.length + 1)).map (fun d =>
                      edge start (start + d + 1) + seg (ys.length - d) (start + d + 1) edge))
                    (edge start (start + 1) + seg ys.length (start + 1) edge)
                    (List.mem_map.mpr ⟨0, hd0, by simp⟩)
                  have hih := ih ys.length (by omega) ys (start + 1) (a :: rest) rfl
                    (by simpa [hq] using hqs)
                  simp [blockCost] at hih ⊢
                  omega
                · have harest := parts_tail (xs := ys) (a := a) (rest := rest)
                    (by simpa [hq] using hqs)
                  have hflat := parts_flatten ys (a :: rest) (by simpa [hq] using hqs)
                  have halen : a.length ≤ ys.length := by
                    have := congrArg List.length hflat
                    simp at this
                    omega
                  rw [seg]
                  have hdm : a.length ∈ List.range (ys.length + 1) := by simp; omega
                  have hm := min?_getD_le_mem_nat
                    ((List.range (ys.length + 1)).map (fun d =>
                      edge start (start + d + 1) + seg (ys.length - d) (start + d + 1) edge))
                    (edge start (start + a.length + 1) +
                      seg (ys.length - a.length) (start + a.length + 1) edge)
                    (List.mem_map.mpr ⟨a.length, hdm, rfl⟩)
                  have hlenlt : (ys.drop a.length).length < ys.length + 1 := by
                    simp
                    omega
                  have hih := ih (ys.drop a.length).length hlenlt (ys.drop a.length)
                    (start + a.length + 1) rest rfl harest
                  simp [blockCost] at hih ⊢
                  rw [show start + (a.length + 1) = start + a.length + 1 by omega]
                  omega
  exact hP xs.length xs start ps rfl hps

private theorem dp_attained (edge : Nat → Nat → Nat) (start : Nat) (xs : List Nat) :
    ∃ ps ∈ parts xs, blockCost edge start ps = seg xs.length start edge := by
  let P : Nat → Prop := fun n => ∀ (xs : List Nat) (start : Nat),
    xs.length = n → ∃ ps ∈ parts xs, blockCost edge start ps = seg n start edge
  have hP : ∀ n, P n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs start hlen
        cases xs with
        | nil =>
            simp at hlen
            subst n
            refine ⟨[], by simp [parts], ?_⟩
            simp [blockCost, seg]
        | cons x ys =>
            simp at hlen
            subst n
            rw [seg]
            let opts := (List.range (ys.length + 1)).map (fun d =>
              edge start (start + d + 1) + seg (ys.length - d) (start + d + 1) edge)
            change ∃ ps ∈ parts (x :: ys), blockCost edge start ps = opts.min?.getD 0
            have hwit : ∃ x, x ∈ opts := by
              dsimp [opts]
              exact ⟨edge start (start + 0 + 1) + seg (ys.length - 0) (start + 0 + 1) edge,
                List.mem_map.mpr ⟨0, by simp, rfl⟩⟩
            obtain ⟨w, hw⟩ := hwit
            obtain ⟨v, hvopts, hvmin⟩ := min?_attained opts w hw
            simp only [opts, List.mem_map] at hvopts
            rcases hvopts with ⟨d, hd, rfl⟩
            have hdle : d ≤ ys.length := by
              have := List.mem_range.mp hd
              omega
            have hlenlt : (ys.drop d).length < ys.length + 1 := by
              simp
              omega
            obtain ⟨ps, hps, hpscost⟩ := ih (ys.drop d).length hlenlt (ys.drop d)
              (start + d + 1) rfl
            rw [← hvmin]
            by_cases hd0 : d = 0
            · subst d
              refine ⟨[x] :: ps, ?_, ?_⟩
              · simp only [parts, List.mem_flatMap]
                refine ⟨ps, hps, ?_⟩
                cases ps <;> simp
              · simp [blockCost, hpscost]
            · let pre := ys.take d
              have hprelen : pre.length = d := by simp [pre, hdle]
              have hdecomp : x :: ys = (x :: pre) ++ ys.drop d := by
                simp [pre, List.take_append_drop]
              refine ⟨(x :: pre) :: ps, ?_, ?_⟩
              · rw [hdecomp]
                exact parts_append (x :: pre) (ys.drop d) [x :: pre] ps
                  (parts_single (x :: pre) (by simp)) hps
              · simp only [blockCost, List.length_cons, hprelen]
                rw [show start + (d + 1) = start + d + 1 by omega]
                rw [hpscost]
                simp
  exact hP xs.length xs start rfl

private def segCost (f : Writer) (fee price : Nat) (xs : List Nat) (a b : Nat) : Nat :=
  fee + chosen f ((xs.drop a).take (b - a)) + price * (b - a) ^ 2

private theorem quoted_eq_seg (f : Writer) (fee price : Nat) (xs : List Nat) :
    quotedOptimum f fee price xs = seg xs.length 0 (segCost f fee price xs) := by
  unfold quotedOptimum
  rw [boundaryCost_eq_seg]
  rfl

private def obj (f : Writer) (fee price : Nat) (ps : List (List Nat)) : Nat :=
  charge f fee ps + price * seekWork ps

private theorem sum_block_eq (f : Writer) (fee price : Nat) (ps : List (List Nat)) :
    (ps.map (fun p => fee + chosen f p + price * p.length ^ 2)).sum =
      obj f fee price ps := by
  induction ps with
  | nil => simp [obj, charge, seekWork]
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons]
      simp [obj, charge, seekWork] at ih ⊢
      rw [ih, Nat.mul_add]
      ac_rfl

private theorem blockCost_segCost_of_flatten (f : Writer) (fee price : Nat)
    (xs : List Nat) (start : Nat) (ps : List (List Nat))
    (hflat : ps.flatten = xs.drop start) :
    blockCost (segCost f fee price xs) start ps =
      (ps.map (fun p => fee + chosen f p + price * p.length ^ 2)).sum := by
  induction ps generalizing start xs with
  | nil => simp [blockCost]
  | cons p rest ih =>
      have hfl : p ++ rest.flatten = xs.drop start := by simpa using hflat
      have hptake : (xs.drop start).take p.length = p := by
        rw [← hfl]
        simp
      have hdrop1 : (xs.drop start).drop p.length = rest.flatten := by
        rw [← hfl]
        simp
      have hdrop : xs.drop (start + p.length) = rest.flatten := by
        simpa [List.drop_drop] using hdrop1
      simp only [blockCost, segCost, hptake, Nat.add_sub_cancel_left]
      rw [ih (start := start + p.length) (xs := xs) hdrop.symm]
      simp

private theorem obj_eq_blockCost (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    obj f fee price ps = blockCost (segCost f fee price xs) 0 ps := by
  have hflatten : ps.flatten = xs := parts_flatten xs ps hps
  rw [blockCost_segCost_of_flatten f fee price xs 0 ps (by simpa using hflatten)]
  exact (sum_block_eq f fee price ps).symm

private theorem minObj_le (f : Writer) (fee price : Nat) (xs : List Nat) (ps : List (List Nat))
    (hps : ps ∈ parts xs) : ((parts xs).map (obj f fee price)).min?.getD 0 ≤ obj f fee price ps := by
  exact min?_getD_le_mem_nat _ _ (List.mem_map.mpr ⟨ps, hps, rfl⟩)

private theorem minObj_le_quoted (f : Writer) (fee price : Nat) (xs : List Nat) :
    ((parts xs).map (obj f fee price)).min?.getD 0 ≤ quotedOptimum f fee price xs := by
  rw [quoted_eq_seg]
  obtain ⟨ps, hps, hpscost⟩ := dp_attained (segCost f fee price xs) 0 xs
  have hobj := obj_eq_blockCost f fee price xs ps hps
  rw [← hobj] at hpscost
  rw [← hpscost]
  exact minObj_le f fee price xs ps hps

private theorem quoted_le_minObj (f : Writer) (fee price : Nat) (xs : List Nat) :
    quotedOptimum f fee price xs ≤ ((parts xs).map (obj f fee price)).min?.getD 0 := by
  rw [quoted_eq_seg]
  obtain ⟨ps0, hps0⟩ := parts_exists xs
  obtain ⟨v, hv, hveq⟩ := min?_attained ((parts xs).map (obj f fee price))
    (obj f fee price ps0) (List.mem_map.mpr ⟨ps0, hps0, rfl⟩)
  rcases List.mem_map.mp hv with ⟨ps, hps, rfl⟩
  have hdple := dp_le_obj (segCost f fee price xs) 0 xs ps hps
  have hobj := obj_eq_blockCost f fee price xs ps hps
  rw [← hobj] at hdple
  rw [← hveq]
  exact hdple

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
  let le := fun a b => decide (obj f fee price a ≤ obj f fee price b)
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
    obj f fee price (plan f fee price xs) ≤ obj f fee price ps := by
  let le := fun a b => decide (obj f fee price a ≤ obj f fee price b)
  have htrans : ∀ a b c : List (List Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))
  have htotal : ∀ a b : List (List Nat), (le a b || le b a) = true := by
    intro a b
    by_cases h : obj f fee price a ≤ obj f fee price b
    · simp [le, h]
    · have h' : obj f fee price b ≤ obj f fee price a := Nat.le_of_not_ge h
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
      have hnat : obj f fee price a ≤ obj f fee price ps := of_decide_eq_true hle
      change obj f fee price (((parts xs).mergeSort le).head?.getD []) ≤ obj f fee price ps
      rw [hm]
      exact hnat

theorem sound_ByteFrontier : ByteFrontier.Goal := by
  intro xs fee price ps hadm hps
  dsimp only
  have hpmem := plan_mem_parts write fee price xs
  have hplan_eq_min : obj write fee price (plan write fee price xs) =
      ((parts xs).map (obj write fee price)).min?.getD 0 := by
    apply Nat.le_antisymm
    · obtain ⟨ps0, hps0⟩ := parts_exists xs
      obtain ⟨v, hv, hveq⟩ := min?_attained ((parts xs).map (obj write fee price))
        (obj write fee price ps0) (List.mem_map.mpr ⟨ps0, hps0, rfl⟩)
      rcases List.mem_map.mp hv with ⟨ps, hps, rfl⟩
      rw [← hveq]
      exact plan_min write fee price xs ps hps
    · exact minObj_le write fee price xs (plan write fee price xs) hpmem
  have hmin_eq_q : ((parts xs).map (obj write fee price)).min?.getD 0 =
      quotedOptimum write fee price xs := by
    apply Nat.le_antisymm
    · exact minObj_le_quoted write fee price xs
    · exact quoted_le_minObj write fee price xs
  constructor
  · change quotedOptimum write fee price xs = obj write fee price (plan write fee price xs)
    rw [← hmin_eq_q, ← hplan_eq_min]
  · change quotedOptimum write fee price xs ≤ obj write fee price ps
    rw [← hmin_eq_q]
    exact minObj_le write fee price xs ps hps

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_ByteFrontier
