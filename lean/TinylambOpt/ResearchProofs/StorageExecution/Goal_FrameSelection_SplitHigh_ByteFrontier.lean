import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_ByteFrontier

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SplitHigh

-- Small-input truth check: no counterexample among short lists, fees and prices.
private def check (xs : List Nat) (fee price : Nat) : Bool :=
  let pick := plan write fee price xs
  let q := quotedOptimum write fee price xs
  q == charge write fee pick + price * seekWork pick &&
    (parts xs).all (fun ps => q ≤ charge write fee ps + price * seekWork ps)

#eval check [] 0 0
#eval check [0, 1] 3 5
#eval check [256, 1, 256] 1 9

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

private theorem min?_attained (l : List Nat) (x : Nat) (hx : x ∈ l) :
    ∃ v ∈ l, v = l.min?.getD 0 := by
  cases h : l.min? with
  | none =>
      have : l = [] := List.min?_eq_none_iff.mp h
      subst this
      simp at hx
  | some v =>
      exact ⟨v, (List.min?_eq_some_iff.mp h).1, by simp [h]⟩

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
  | cons ps rest => exact ⟨ps, by simp [h]⟩

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

private theorem nil_mem_parts_nil {xs : List Nat} (h : [] ∈ parts xs) : xs = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨qs, hqs, hmem⟩
      cases qs with
      | nil => simp at hmem
      | cons a rest => simp at hmem

private theorem parts_single {xs : List Nat} (hxs : xs ≠ []) : [xs] ∈ parts xs := by
  induction xs with
  | nil => exact (hxs rfl).elim
  | cons x xs ih =>
      rw [parts]
      by_cases hnil : xs = []
      · subst xs
        simp [parts]
      · exact List.mem_flatMap.mpr ⟨[xs], ih hnil, by simp⟩

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
  apply congrArg (fun l : List Nat => l.min?.getD 0)
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
  induction hlen : xs.length using Nat.strongRecOn generalizing xs start ps with
  | ind n ih =>
      subst n
      cases xs with
      | nil =>
          simp [parts] at hps
          subst ps
          simp [seg, blockCost]
      | cons x ys =>
          obtain ⟨k, hkpos, hkle, tail, hps_eq, htail⟩ :=
            parts_decompose (x :: ys) (by simp) ps hps
          subst ps
          change seg (ys.length + 1) start edge ≤ blockCost edge start ((x :: ys).take k :: tail)
          simp only [seg]
          have hkle' : k ≤ ys.length + 1 := by simpa using hkle
          have hdm : k - 1 ∈ List.range (ys.length + 1) := by
            simp
            omega
          have hm := min?_getD_le_mem_nat
            ((List.range (ys.length + 1)).map (fun d =>
              edge start (start + d + 1) + seg (ys.length - d) (start + d + 1) edge))
            (edge start (start + (k - 1) + 1) +
              seg (ys.length - (k - 1)) (start + (k - 1) + 1) edge)
            (List.mem_map.mpr ⟨k - 1, hdm, rfl⟩)
          have hdrop_eq : (x :: ys).drop k = ys.drop (k - 1) := by
            cases k with
            | zero => omega
            | succ k' => simp
          have hlen_eq : ys.length - (k - 1) = ((x :: ys).drop k).length := by
            rw [hdrop_eq]
            simp
          have hoff_eq : start + (k - 1) + 1 = start + k := by omega
          rw [hlen_eq, hoff_eq] at hm
          have hih := ih ((x :: ys).drop k).length (by simp; omega)
            (start + k) ((x :: ys).drop k) tail htail rfl
          have hblock : blockCost edge start ((x :: ys).take k :: tail) =
              edge start (start + k) + blockCost edge (start + k) tail := by
            simp [blockCost, List.length_take, Nat.min_eq_left hkle']
          rw [hblock]
          omega

private theorem dp_attained (edge : Nat → Nat → Nat) (start : Nat) (xs : List Nat) :
    ∃ ps ∈ parts xs, blockCost edge start ps = seg xs.length start edge := by
  induction hlen : xs.length using Nat.strongRecOn generalizing xs start with
  | ind n ih =>
      subst n
      cases xs with
      | nil =>
          refine ⟨[], by simp [parts], ?_⟩
          simp [blockCost, seg]
      | cons x ys =>
          change ∃ ps ∈ parts (x :: ys),
            blockCost edge start ps = seg (ys.length + 1) start edge
          simp only [seg]
          let opts := (List.range (ys.length + 1)).map (fun d =>
            edge start (start + d + 1) + seg (ys.length - d) (start + d + 1) edge)
          have hmem0 : opts[0]?.getD 0 ∈ opts := by
            simp [opts]
            exact ⟨0, by simp, by simp⟩
          obtain ⟨v, hvopts, hvmin⟩ := min?_attained opts (opts[0]?.getD 0) hmem0
          simp only [opts, List.mem_map] at hvopts
          rcases hvopts with ⟨d, hd, rfl⟩
          have hdle : d ≤ ys.length := by
            have := List.mem_range.mp hd
            omega
          have hlenlt : (ys.drop d).length < (x :: ys).length := by
            simp
            omega
          obtain ⟨ps, hps, hpscost⟩ := ih (ys.drop d).length hlenlt
            (start + d + 1) (ys.drop d) rfl
          let pre := x :: ys.take d
          have hprelen : pre.length = d + 1 := by simp [pre, hdle]
          have hdecomp : x :: ys = pre ++ ys.drop d := by
            simp [pre]
          refine ⟨pre :: ps, ?_, ?_⟩
          · rw [hdecomp]
            exact parts_append pre (ys.drop d) [pre] ps
              (parts_single (by simp [pre])) hps
          · simp only [blockCost, hprelen]
            have hadd : start + (d + 1) = start + d + 1 := by omega
            rw [hadd]
            rw [hpscost]
            have hlen_drop : (ys.drop d).length = ys.length - d := by simp
            rw [hlen_drop]
            exact hvmin

private def segCost (f : Writer) (fee price : Nat) (xs : List Nat) (a b : Nat) : Nat :=
  fee + chosen f ((xs.drop a).take (b - a)) + price * (b - a) ^ 2

private theorem quoted_eq_seg (f : Writer) (fee price : Nat) (xs : List Nat) :
    quotedOptimum f fee price xs = seg xs.length 0 (segCost f fee price xs) := by
  unfold quotedOptimum
  rw [boundaryCost_eq_seg]
  rfl

private def obj (f : Writer) (fee price : Nat) (ps : List (List Nat)) : Nat :=
  charge f fee ps + price * seekWork ps

private theorem sum_price_length (price : Nat) (ps : List (List Nat)) :
    price * (ps.map (fun p => p.length ^ 2)).sum =
      (ps.map (fun p => price * p.length ^ 2)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, Nat.mul_add, ih]

private theorem obj_eq_sum (f : Writer) (fee price : Nat) (ps : List (List Nat)) :
    obj f fee price ps =
      (ps.map (fun p => fee + chosen f p + price * p.length ^ 2)).sum := by
  unfold obj charge seekWork
  rw [sum_price_length]
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons]
      omega

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
  exact obj_eq_sum f fee price ps

private theorem minObj_le (f : Writer) (fee price : Nat) (xs : List Nat) (ps : List (List Nat))
    (hps : ps ∈ parts xs) : ((parts xs).map (obj f fee price)).min?.getD 0 ≤ obj f fee price ps := by
  exact min?_getD_le_mem_nat _ _ (List.mem_map.mpr ⟨ps, hps, rfl⟩)

private theorem minObj_attained (f : Writer) (fee price : Nat) (xs : List Nat) :
    ∃ ps ∈ parts xs, obj f fee price ps = ((parts xs).map (obj f fee price)).min?.getD 0 := by
  obtain ⟨ps0, hps0⟩ := parts_exists xs
  obtain ⟨v, hv, hveq⟩ := min?_attained ((parts xs).map (obj f fee price))
    (obj f fee price ps0) (List.mem_map.mpr ⟨ps0, hps0, rfl⟩)
  rcases List.mem_map.mp hv with ⟨ps, hps, rfl⟩
  exact ⟨ps, hps, hveq⟩

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

theorem sound_ByteFrontier : SplitHigh.ByteFrontier.Goal := by
  intro xs fee price ps hadm hps
  dsimp only
  have hpmem := plan_mem_parts write fee price xs
  have hplan_eq_min : obj write fee price (plan write fee price xs) =
      ((parts xs).map (obj write fee price)).min?.getD 0 := by
    apply Nat.le_antisymm
    · obtain ⟨ps, hps, hpsmin⟩ := minObj_attained write fee price xs
      rw [← hpsmin]
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_ByteFrontier
