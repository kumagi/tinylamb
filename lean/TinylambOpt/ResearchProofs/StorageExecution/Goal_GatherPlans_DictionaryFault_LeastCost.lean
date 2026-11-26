import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_DictionaryFault_LeastCost

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.DictionaryFault

/-! Both `dp` and `choose` compute the minimum cost of partitioning the
normalized points into consecutive covering extents.  The argument does not
use any property of the price function, so it applies to the dictionary-fault
model in particular. -/

private theorem covers_nonempty (xs : List Nat) : ∃ ps, ps ∈ covers xs := by
  induction xs with
  | nil => exact ⟨[], by simp [covers]⟩
  | cons x xs ih =>
    obtain ⟨q, hq⟩ := ih
    cases q with
    | nil =>
      exact ⟨[(x, x)], by
        rw [covers]
        exact List.mem_flatMap.mpr ⟨[], hq, by simp⟩⟩
    | cons e rest =>
      exact ⟨(x, x) :: e :: rest, by
        rw [covers]
        exact List.mem_flatMap.mpr ⟨e :: rest, hq, by simp⟩⟩

private theorem covers_ne_nil (xs : List Nat) : covers xs ≠ [] := by
  obtain ⟨ps, hps⟩ := covers_nonempty xs
  intro h
  simp [h] at hps

private inductive Parts : List Nat → List Extent → Prop
  | nil : Parts [] []
  | singleton : (x : Nat) → (xs : List Nat) → (rest : List Extent) →
      Parts xs rest → Parts (x :: xs) ((x, x) :: rest)
  | bridge : (x : Nat) → (xs : List Nat) → (d : Nat) → (b : Nat) →
      (rest : List Extent) → xs[d]? = some b →
      Parts (xs.drop (d + 1)) rest → Parts (x :: xs) ((x, b) :: rest)

private theorem parts_first_split {xs : List Nat} {a b : Nat} {rest : List Extent}
    (h : Parts xs ((a, b) :: rest)) :
    ∃ d, xs[d]? = some b ∧ Parts (xs.drop (d + 1)) rest := by
  cases h with
  | singleton x xs rest hrest => exact ⟨0, by simp, hrest⟩
  | bridge x xs d b rest hget hrest =>
    exact ⟨d + 1, by simpa using hget, by simpa using hrest⟩

private theorem covers_parts {xs : List Nat} {ps : List Extent}
    (h : ps ∈ covers xs) : Parts xs ps := by
  induction xs generalizing ps with
  | nil =>
    simp only [covers, List.mem_singleton] at h
    subst ps
    exact .nil
  | cons x xs ih =>
    change ps ∈ (covers xs).flatMap (fun q => match q with
      | [] => [[(x, x)]]
      | (_, b) :: rest => [(x, x) :: q, (x, b) :: rest]) at h
    rw [List.mem_flatMap] at h
    obtain ⟨q, hq, hps⟩ := h
    cases q with
    | nil =>
      simp only [List.mem_singleton] at hps
      subst ps
      have hxs : xs = [] := by
        cases xs with
        | nil => rfl
        | cons y ys =>
          rw [covers, List.mem_flatMap] at hq
          obtain ⟨q', hq', hnil⟩ := hq
          cases q' <;> simp_all
      subst xs
      exact .singleton x [] [] .nil
    | cons e rest =>
      simp only [covers, List.mem_singleton] at hps
      rw [List.mem_cons] at hps
      rcases hps with hps | hps
      · subst ps
        exact .singleton x xs (e :: rest) (ih hq)
      · rw [List.mem_singleton] at hps
        subst ps
        obtain ⟨d, hd, hrest⟩ := parts_first_split (ih hq)
        exact .bridge x xs d e.2 rest hd hrest

private theorem parts_covers_aux (n : Nat) (xs : List Nat) (ps : List Extent)
    (hlen : xs.length = n) (h : Parts xs ps) : ps ∈ covers xs := by
  induction n generalizing xs ps with
  | zero =>
    cases xs with
    | nil =>
      cases h
      simp [covers]
    | cons x xs => simp at hlen
  | succ n ih =>
    cases h with
    | nil => simp [covers]
    | singleton x xs rest hrest =>
      have hr : rest ∈ covers xs := ih xs rest (by simp at hlen; omega) hrest
      rw [covers]
      exact List.mem_flatMap.mpr ⟨rest, hr, by cases rest <;> simp⟩
    | bridge x xs d b rest hget hrest =>
      cases xs with
      | nil => simp at hget
      | cons a tail =>
        have hp : Parts (a :: tail) ((a, b) :: rest) := by
          cases d with
          | zero =>
            simp only [List.getElem?_cons_zero, Option.some.injEq] at hget
            subst b
            exact .singleton a tail rest hrest
          | succ d =>
            exact .bridge a tail d b rest (by simpa using hget) hrest
        have hq : (a, b) :: rest ∈ covers (a :: tail) :=
          ih (a :: tail) ((a, b) :: rest) (by simp at hlen; omega) hp
        rw [covers]
        exact List.mem_flatMap.mpr ⟨(a, b) :: rest, hq, by simp⟩

private theorem parts_covers {xs : List Nat} {ps : List Extent}
    (h : Parts xs ps) : ps ∈ covers xs :=
  parts_covers_aux xs.length xs ps rfl h

private def coverMin (f : Price) (xs : List Nat) : Nat :=
  ((covers xs).map (cost f)).min?.getD 0

private theorem coverMin_le {f : Price} {xs : List Nat} {ps : List Extent}
    (h : ps ∈ covers xs) : coverMin f xs ≤ cost f ps := by
  obtain ⟨q, hq⟩ := covers_nonempty xs
  have hne : (covers xs).map (cost f) ≠ [] := by
    intro he
    have : cost f q ∈ (covers xs).map (cost f) := List.mem_map.mpr ⟨q, hq, rfl⟩
    simp [he] at this
  unfold coverMin
  cases hm : ((covers xs).map (cost f)).min? with
  | none => exact False.elim (hne (List.min?_eq_none_iff.mp hm))
  | some n =>
    exact (List.min?_eq_some_iff.mp hm).2 (cost f ps)
      (List.mem_map.mpr ⟨ps, h, rfl⟩)

private theorem coverMin_attained {f : Price} {xs : List Nat} :
    ∃ ps, ps ∈ covers xs ∧ cost f ps = coverMin f xs := by
  obtain ⟨q, hq⟩ := covers_nonempty xs
  have hne : (covers xs).map (cost f) ≠ [] := by
    intro he
    have : cost f q ∈ (covers xs).map (cost f) := List.mem_map.mpr ⟨q, hq, rfl⟩
    simp [he] at this
  unfold coverMin
  cases hm : ((covers xs).map (cost f)).min? with
  | none => exact False.elim (hne (List.min?_eq_none_iff.mp hm))
  | some n =>
    obtain ⟨hn, _⟩ := List.min?_eq_some_iff.mp hm
    obtain ⟨ps, hps, rfl⟩ := List.mem_map.mp hn
    exact ⟨ps, hps, rfl⟩

private theorem pairwise_head?_le {α : Type} {le : α → α → Bool}
    (hrefl : ∀ x, le x x = true)
    {l : List α} {a b : α} (hpw : l.Pairwise (fun x y => le x y = true))
    (hhead : l.head? = some a) (hmem : b ∈ l) : le a b = true := by
  cases l with
  | nil => simp at hmem
  | cons x xs =>
    simp at hhead
    subst hhead
    rw [List.pairwise_cons] at hpw
    rcases List.mem_cons.mp hmem with rfl | hmem
    · exact hrefl _
    · exact hpw.1 b hmem

private theorem choose_mem_covers (f : Price) (xs : List Nat) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hperm := List.mergeSort_perm (covers (points xs)) le
  have hne' : (covers (points xs)).mergeSort le ≠ [] := by
    intro h
    have := hperm.symm.length_eq
    rw [h] at this
    simp at this
    exact covers_ne_nil (points xs) this
  cases h : (covers (points xs)).mergeSort le with
  | nil => exact absurd h hne'
  | cons hd tl =>
    have hmem : hd ∈ (covers (points xs)).mergeSort le := by simp [h]
    rw [List.mem_mergeSort] at hmem
    simpa [le, h] using hmem

private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) :
    cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hpair : List.Pairwise (fun a b => le a b = true)
      ((covers (points xs)).mergeSort le) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total (cost f a) (cost f b)
  have hmem' : ps ∈ (covers (points xs)).mergeSort le := List.mem_mergeSort.mpr hmem
  cases h : (covers (points xs)).mergeSort le with
  | nil => simp [h] at hmem'
  | cons hd tl =>
    simp only [h, List.head?_cons, Option.getD_some]
    have hle := pairwise_head?_le (fun x => by simp [le]) hpair
      (show ((covers (points xs)).mergeSort le).head? = some hd by simp [h]) hmem'
    simpa [le] using hle

private theorem choose_cost_eq_coverMin (f : Price) (xs : List Nat) :
    cost f (choose f xs) = coverMin f (points xs) := by
  apply Nat.le_antisymm
  · obtain ⟨ps, hps, hcost⟩ := coverMin_attained (f := f) (xs := points xs)
    rw [← hcost]
    exact choose_le f xs ps hps
  · exact coverMin_le (choose_mem_covers f xs)

private def coverCost (f : Price) : List Nat → Nat
  | [] => 0
  | x :: rest =>
      ((List.range (rest.length + 1)).map (fun d =>
        f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d))).min?.getD 0
termination_by xs => xs.length
decreasing_by
  simp
  omega

private theorem coverCost_eq_coverMin (f : Price) (xs : List Nat) :
    coverCost f xs = coverMin f xs := by
  induction hlen : xs.length using Nat.strongRecOn generalizing xs with
  | ind n ih =>
    cases xs with
    | nil => simp [coverCost, coverMin, covers, cost]
    | cons x rest =>
      let L := (List.range (rest.length + 1)).map (fun d =>
        f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d))
      have hn : n = rest.length + 1 := by simpa using hlen.symm
      have ih_rest : coverCost f rest = coverMin f rest :=
        ih rest.length (by omega) rest rfl
      have ih_drop (d : Nat) :
          coverCost f (rest.drop d) = coverMin f (rest.drop d) :=
        ih (rest.drop d).length (by
          simp [List.length_drop]
          omega) (rest.drop d) rfl
      have hcover : coverCost f (x :: rest) = (L.min?).getD 0 := by
        rw [coverCost]
      have hLne : L ≠ [] := by simp [L]
      have hmemL (d : Nat) (hd : d < rest.length + 1) :
          f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d) ∈ L := by
        simp only [L, List.mem_map]
        exact ⟨d, List.mem_range.mpr hd, rfl⟩
      have hmin_le (d : Nat) (hd : d < rest.length + 1) :
          coverCost f (x :: rest) ≤
            f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d) := by
        rw [hcover]
        cases hm : L.min? with
        | none => exact False.elim (hLne (List.min?_eq_none_iff.mp hm))
        | some n =>
          exact (List.min?_eq_some_iff.mp hm).2 _ (hmemL d hd)
      have hmin_attained :
          ∃ d, d < rest.length + 1 ∧
            f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d) =
              coverCost f (x :: rest) := by
        rw [hcover]
        cases hm : L.min? with
        | none => exact False.elim (hLne (List.min?_eq_none_iff.mp hm))
        | some n =>
          have hnmem : n ∈ L := (List.min?_eq_some_iff.mp hm).1
          simp only [Option.getD_some]
          rw [List.mem_map] at hnmem
          obtain ⟨d, hd, hdeq⟩ := hnmem
          exact ⟨d, List.mem_range.mp hd, hdeq⟩
      apply Nat.le_antisymm
      · obtain ⟨ps, hps, hcost⟩ := coverMin_attained (f := f) (xs := x :: rest)
        have hparts := covers_parts hps
        cases hparts with
        | singleton _ _ qs hqs =>
          have hcost' : f x x + cost f qs = coverMin f (x :: rest) := by
            simpa [cost] using hcost
          have hle1 := hmin_le 0 (by simp)
          simp only [List.getElem?_cons_zero, Option.getD_some, List.drop_zero] at hle1
          rw [ih_rest] at hle1
          have hle2 : f x x + coverMin f rest ≤ f x x + cost f qs :=
            Nat.add_le_add_left (coverMin_le (parts_covers hqs)) _
          omega
        | bridge _ _ d b qs hget hqs =>
          obtain ⟨hdlt, _⟩ := List.getElem?_eq_some_iff.mp hget
          have hcost' : f x b + cost f qs = coverMin f (x :: rest) := by
            simpa [cost] using hcost
          have hxd : (x :: rest)[d + 1]? = some b := by simp [hget]
          have hle1 := hmin_le (d + 1) (by omega)
          rw [hxd] at hle1
          simp only [Option.getD_some] at hle1
          rw [ih_drop (d + 1)] at hle1
          have hle2 : f x b + coverMin f (rest.drop (d + 1)) ≤
              f x b + cost f qs :=
            Nat.add_le_add_left (coverMin_le (parts_covers hqs)) _
          omega
      · obtain ⟨d, hdlt, hdeq⟩ := hmin_attained
        cases d with
        | zero =>
          simp only [List.getElem?_cons_zero, Option.getD_some, List.drop_zero] at hdeq
          obtain ⟨qs, hqs, hqcost⟩ := coverMin_attained (f := f) (xs := rest)
          have hmem : (x, x) :: qs ∈ covers (x :: rest) :=
            parts_covers (Parts.singleton x rest qs (covers_parts hqs))
          have hcostcov : cost f ((x, x) :: qs) = coverCost f (x :: rest) := by
            change f x x + cost f qs = coverCost f (x :: rest)
            rw [hqcost, ← ih_rest]
            exact hdeq
          rw [← hcostcov]
          exact coverMin_le hmem
        | succ e =>
          have helt : e < rest.length := by omega
          have hget : rest[e]? = some (rest[e]?.getD x) := by
            have hopt : rest[e]? = some (rest[e]) := by
              rw [List.getElem?_eq_some_iff]
              exact ⟨helt, rfl⟩
            rw [hopt]
            simp
          have hxd : (x :: rest)[e + 1]? = rest[e]? := by simp
          have hdeq' : f x (rest[e]?.getD x) + coverCost f (rest.drop (e + 1)) =
              coverCost f (x :: rest) := by
            rw [hxd] at hdeq
            exact hdeq
          obtain ⟨qs, hqs, hqcost⟩ :=
            coverMin_attained (f := f) (xs := rest.drop (e + 1))
          have hmem : (x, rest[e]?.getD x) :: qs ∈ covers (x :: rest) :=
            parts_covers
              (Parts.bridge x rest e (rest[e]?.getD x) qs hget (covers_parts hqs))
          have hcostcov : cost f ((x, rest[e]?.getD x) :: qs) =
              coverCost f (x :: rest) := by
            change f x (rest[e]?.getD x) + cost f qs = coverCost f (x :: rest)
            rw [hqcost, ← ih_drop (e + 1)]
            exact hdeq'
          rw [← hcostcov]
          exact coverMin_le hmem

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
      · rw [ite_eq_right heq]
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

private def edgeAt (f : Price) (ks : List Nat) (a b : Nat) : Nat :=
  f (ks[a]?.getD 0) (ks[b - 1]?.getD 0)

private theorem seg_eq_coverCost (f : Price) (ks : List Nat) (off : Nat) :
    seg (ks.drop off).length off (edgeAt f ks) = coverCost f (ks.drop off) := by
  induction hlen : (ks.drop off).length using Nat.strongRecOn
      generalizing ks off with
  | ind n ih =>
    cases hs : ks.drop off with
    | nil =>
      have hn0 : n = 0 := by simpa [hs] using hlen.symm
      subst n
      simp [seg, coverCost, hs]
    | cons x rest =>
      have hn : n = rest.length + 1 := by
        rw [← hlen]
        simp [hs]
      subst n
      have h0 : (ks.drop off)[0]? = some x := by simp [hs]
      have hxget : ks[off]? = some x := by simpa using h0
      rw [show (ks.drop off).length = rest.length + 1 by simp [hs]]
      rw [seg]
      rw [coverCost]
      apply congrArg (fun xs : List Nat => xs.min?.getD 0)
      apply List.map_congr_left
      intro d hdmem
      have hdlt : d < rest.length + 1 := List.mem_range.mp hdmem
      have hgeteq : ks[off + d]? = (x :: rest)[d]? := by
        rw [← List.getElem?_drop (xs := ks) (i := off) (j := d)]
        simp [hs]
      have hcase : (x :: rest)[d]? = some ((x :: rest)[d]?.getD x) := by
        have hvalid : d < (x :: rest).length := by simpa using hdlt
        have hopt : (x :: rest)[d]? = some ((x :: rest)[d]) := by
          rw [List.getElem?_eq_some_iff]
          exact ⟨hvalid, rfl⟩
        rw [hopt]
        simp
      have hlast : ks[off + d]?.getD 0 = (x :: rest)[d]?.getD x := by
        rw [hgeteq, hcase]
        simp
      have hdrop : ks.drop (off + d + 1) = rest.drop d := by
        calc
          ks.drop (off + d + 1) = ks.drop (off + (d + 1)) := by rw [Nat.add_assoc]
          _ = (ks.drop off).drop (d + 1) := by rw [List.drop_drop]
          _ = rest.drop d := by simp [hs]
      have hlen_suf : (ks.drop (off + d + 1)).length = (rest.drop d).length := by
        rw [hdrop]
      have ih' : seg (ks.drop (off + d + 1)).length (off + d + 1) (edgeAt f ks) =
          coverCost f (ks.drop (off + d + 1)) :=
        ih (ks.drop (off + d + 1)).length (by
          rw [hlen_suf]
          rw [hn]
          simp [List.length_drop]
          omega) ks (off + d + 1) rfl
      have hkslen : ks.length - off = rest.length + 1 := by
        have := congrArg List.length hs
        simpa [List.length_drop] using this
      have hlen_eq : ks.length - (off + d + 1) = rest.length - d := by omega
      have hseg' : seg (rest.length - d) (off + d + 1) (edgeAt f ks) =
          coverCost f (rest.drop d) := by
        have ih'' : seg (ks.length - (off + d + 1)) (off + d + 1) (edgeAt f ks) =
            coverCost f (ks.drop (off + d + 1)) := by
          simpa [List.length_drop] using ih'
        rw [hlen_eq] at ih''
        rw [hdrop] at ih''
        exact ih''
      have hb : off + d + 1 - 1 = off + d := by omega
      have hfirst : edgeAt f ks off (off + d + 1) =
          f x ((x :: rest)[d]?.getD x) := by
        simp only [edgeAt, hxget, Option.getD_some, hb, hlast]
      rw [hseg', hfirst]

private theorem dp_eq_coverMin (f : Price) (ks : List Nat) : dp f ks = coverMin f ks := by
  unfold dp
  rw [boundaryCost_eq_seg]
  have hsize : ks.toArray.size = ks.length := by simp
  have hedge : (fun a b => f (ks.toArray[a]?.getD 0) (ks.toArray[b - 1]?.getD 0)) =
      edgeAt f ks := by
    funext a b
    simp [edgeAt, List.getElem?_toArray]
  rw [hsize, hedge]
  have hseg : seg ks.length 0 (edgeAt f ks) = coverCost f ks := by
    simpa using (seg_eq_coverCost f ks 0)
  rw [hseg]
  exact coverCost_eq_coverMin f ks

private theorem leastCost_any (f : Price) : leastCost f := by
  intro xs
  change dp f (points xs) = cost f (choose f xs)
  rw [dp_eq_coverMin, choose_cost_eq_coverMin]

theorem sound_LeastCost : DictionaryFault.LeastCost.Goal := by
  intro group fault
  exact leastCost_any (price group fault)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_DictionaryFault_LeastCost
