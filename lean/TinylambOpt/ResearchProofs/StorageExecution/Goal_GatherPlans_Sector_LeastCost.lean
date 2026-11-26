import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `Sector.LeastCost` — `∀ (shift startup : Nat), leastCost (price shift startup)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_LeastCost

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Sector

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  cases xs with
  | nil => simp
  | cons x xs =>
    rw [List.eraseDups_cons, List.nodup_cons]
    constructor
    · intro hx
      have hxf : x ∈ List.filter (fun b => !b == x) xs := List.mem_eraseDups.mp hx
      have hx' := List.mem_filter.mp hxf
      have hxx : x ∈ xs.eraseDups := List.mem_eraseDups.mpr hx'.1
      have hbeq : (x == x) = true := by simp
      simp [hbeq] at hx'
    · exact eraseDups_nodup (List.filter (fun b => !b == x) xs)
termination_by xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b => !b == x) xs)

private theorem points_mem (xs : List Nat) (x : Nat) :
    x ∈ points xs ↔ x ∈ xs := by
  simp [points]

private theorem points_nodup (xs : List Nat) : (points xs).Nodup := by
  unfold points
  exact (List.mergeSort_perm xs.eraseDups (fun a b : Nat => a ≤ b)).nodup_iff.mpr
    (eraseDups_nodup xs)

private theorem pairwise_lt_of_le_nodup {xs : List Nat}
    (hle : xs.Pairwise (fun a b => a ≤ b)) (hnd : xs.Nodup) :
    xs.Pairwise (fun a b => a < b) := by
  induction xs with
  | nil => exact .nil
  | cons x xs ih =>
    rw [List.pairwise_cons] at hle ⊢
    constructor
    · intro b hb
      have hne : x ≠ b := by
        intro heq
        subst b
        exact (List.nodup_cons.mp hnd).1 hb
      have hle' := hle.1 b hb
      omega
    · exact ih hle.2 (List.nodup_cons.mp hnd).2

private theorem points_sorted (xs : List Nat) :
    (points xs).Pairwise (fun a b => a < b) := by
  unfold points
  refine pairwise_lt_of_le_nodup ?_ ?_
  · have hs := List.pairwise_mergeSort
      (le := fun a b : Nat => decide (a ≤ b))
      (trans := fun a b c hab hbc => by
        simpa only [decide_eq_true_eq] using Nat.le_trans
          (of_decide_eq_true hab) (of_decide_eq_true hbc))
      (total := fun a b => by
        rcases Nat.le_total a b with h | h
        · simp [h]
        · simp [h])
      xs.eraseDups
    simpa only [decide_eq_true_eq] using hs
  · exact (List.mergeSort_perm xs.eraseDups (fun a b : Nat => a ≤ b)).nodup_iff.mpr
      (eraseDups_nodup xs)

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
      simp only [covers, List.mem_singleton] at hps
      have hxs : xs = [] := by
        cases xs with
        | nil => rfl
        | cons y ys =>
          rw [covers, List.mem_flatMap] at hq
          obtain ⟨q, hq, hnil⟩ := hq
          cases q <;> simp_all
      subst xs
      subst ps
      exact .singleton x [] [] .nil
    | cons e rest =>
      simp only [covers] at hps
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

private theorem covers_iff_parts {xs : List Nat} {ps : List Extent} :
    ps ∈ covers xs ↔ Parts xs ps :=
  ⟨covers_parts, parts_covers⟩

private theorem parts_endpoints {xs : List Nat} {ps : List Extent}
    (h : Parts xs ps) : ∀ e ∈ ps, e.1 ∈ xs ∧ e.2 ∈ xs := by
  cases h with
  | nil => simp
  | singleton x xs rest hrest =>
    intro e he
    simp only [List.mem_cons, Prod.mk.injEq] at he
    rcases he with rfl | he
    · exact ⟨by simp, by simp⟩
    · obtain ⟨h1, h2⟩ := parts_endpoints hrest e he
      exact ⟨List.mem_cons_of_mem x h1, List.mem_cons_of_mem x h2⟩
  | bridge x xs d b rest hget hrest =>
    intro e he
    simp only [List.mem_cons, Prod.mk.injEq] at he
    rcases he with rfl | he
    · exact ⟨by simp, List.mem_cons_of_mem x (List.mem_iff_getElem?.mpr ⟨d, hget⟩)⟩
    · obtain ⟨h1, h2⟩ := parts_endpoints hrest e he
      exact ⟨List.mem_cons_of_mem x (List.mem_of_mem_drop h1),
        List.mem_cons_of_mem x (List.mem_of_mem_drop h2)⟩

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
      · rw [if_neg heq]
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
    have hmin := (List.min?_eq_some_iff.mp hm).2 (cost f ps)
      (List.mem_map.mpr ⟨ps, h, rfl⟩)
    exact hmin

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

private theorem choose_mem_covers (f : Price) (xs : List Nat)
    (hne : covers (points xs) ≠ []) : choose f xs ∈ covers (points xs) := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hperm := List.mergeSort_perm (covers (points xs)) le
  have hne' : (covers (points xs)).mergeSort le ≠ [] := by
    intro h
    have := hperm.symm.length_eq
    rw [h] at this
    simp at this
    exact hne this
  cases h : (covers (points xs)).mergeSort le with
  | nil => exact absurd h hne'
  | cons hd tl =>
    have hmem : hd ∈ (covers (points xs)).mergeSort le := by simp [h]
    rw [List.mem_mergeSort] at hmem
    simpa [le, h] using hmem

private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) : cost f (choose f xs) ≤ cost f ps := by
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
    simpa only [le, decide_eq_true_eq] using hle

private theorem choose_cost_eq_coverMin (f : Price) (xs : List Nat) :
    cost f (choose f xs) = coverMin f (points xs) := by
  apply Nat.le_antisymm
  · obtain ⟨ps, hps, hcost⟩ := coverMin_attained (f := f) (xs := points xs)
    rw [← hcost]
    exact choose_le f xs ps hps
  · have hne : covers (points xs) ≠ [] := by
      obtain ⟨ps, hps⟩ := covers_nonempty (points xs)
      intro h
      simp [h] at hps
    exact coverMin_le (choose_mem_covers f xs hne)

private def coverCost (f : Price) : List Nat → Nat
  | [] => 0
  | x :: rest =>
      ((List.range (rest.length + 1)).map (fun d =>
        f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d))).min?.getD 0
termination_by xs => xs.length
decreasing_by
  simp [List.length_drop]
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
      have hlt_rest : rest.length < (x :: rest).length := by simp
      rw [hlen] at hlt_rest
      have ih_rest : coverCost f rest = coverMin f rest :=
        ih rest.length hlt_rest rest rfl
      have ih_drop (d : Nat) : coverCost f (rest.drop d) = coverMin f (rest.drop d) := by
        have hlt : (rest.drop d).length < (x :: rest).length := by
          simp [List.length_drop]
          omega
        rw [hlen] at hlt
        exact ih (rest.drop d).length hlt (rest.drop d) rfl
      have hLne : L ≠ [] := by simp [L]
      have hmemL (d : Nat) (hd : d < rest.length + 1) :
          f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d) ∈ L := by
        simp only [L, List.mem_map]
        exact ⟨d, List.mem_range.mpr hd, rfl⟩
      have hdef : coverCost f (x :: rest) = L.min?.getD 0 := by simp [coverCost, L]
      have hmin_le (d : Nat) (hd : d < rest.length + 1) :
          coverCost f (x :: rest) ≤
            f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d) := by
        rw [hdef]
        cases hm : L.min? with
        | none => exact False.elim (hLne (List.min?_eq_none_iff.mp hm))
        | some m => exact (List.min?_eq_some_iff.mp hm).2 _ (hmemL d hd)
      have hmin_attained :
          ∃ d, d < rest.length + 1 ∧
            f x ((x :: rest)[d]?.getD x) + coverCost f (rest.drop d) =
              coverCost f (x :: rest) := by
        rw [hdef]
        cases hm : L.min? with
        | none => exact False.elim (hLne (List.min?_eq_none_iff.mp hm))
        | some m =>
          have hm_mem : m ∈ L := (List.min?_eq_some_iff.mp hm).1
          rw [List.mem_map] at hm_mem
          obtain ⟨d, hd, hdeq⟩ := hm_mem
          exact ⟨d, List.mem_range.mp hd, by simpa using hdeq⟩
      apply Nat.le_antisymm
      · obtain ⟨ps, hps, hcost⟩ := coverMin_attained (f := f) (xs := x :: rest)
        have hparts := covers_parts hps
        cases hparts with
        | singleton x' xs' qs hqs =>
          have hle1 := hmin_le 0 (by simp)
          simp only [List.getElem?_cons_zero, Option.getD_some, List.drop_zero] at hle1
          rw [ih_rest] at hle1
          calc
            coverCost f (x :: rest) ≤ f x x + coverMin f rest := hle1
            _ ≤ f x x + cost f qs := Nat.add_le_add_left (coverMin_le (parts_covers hqs)) _
            _ = cost f ((x, x) :: qs) := by simp [cost]
            _ = coverMin f (x :: rest) := hcost
        | bridge x' xs' d b qs hget hqs =>
          have hdlt : d < rest.length := by
            obtain ⟨hdlt, _⟩ := List.getElem?_eq_some_iff.mp hget
            exact hdlt
          have hxd : (x :: rest)[d + 1]? = some b := by simp [hget]
          have hle1 := hmin_le (d + 1) (by omega)
          rw [hxd] at hle1
          simp only [Option.getD_some] at hle1
          rw [ih_drop (d + 1)] at hle1
          calc
            coverCost f (x :: rest) ≤ f x b + coverMin f (rest.drop (d + 1)) := hle1
            _ ≤ f x b + cost f qs := Nat.add_le_add_left (coverMin_le (parts_covers hqs)) _
            _ = cost f ((x, b) :: qs) := by simp [cost]
            _ = coverMin f (x :: rest) := hcost
      · obtain ⟨d, hdlt, hdeq⟩ := hmin_attained
        cases d with
        | zero =>
          simp only [List.getElem?_cons_zero, Option.getD_some, List.drop_zero] at hdeq
          obtain ⟨qs, hqs, hqcost⟩ := coverMin_attained (f := f) (xs := rest)
          have hmem : (x, x) :: qs ∈ covers (x :: rest) :=
            parts_covers (Parts.singleton x rest qs (covers_parts hqs))
          have hcostcov : cost f ((x, x) :: qs) = coverCost f (x :: rest) := by
            rw [show cost f ((x, x) :: qs) = f x x + cost f qs by simp [cost]]
            rw [hqcost, ← ih_rest]
            exact hdeq
          rw [← hcostcov]
          exact coverMin_le hmem
        | succ e =>
          have helt : e < rest.length := by omega
          have hget : rest[e]? = some (rest[e]?.getD x) := by
            rw [List.getElem?_eq_some_iff]
            refine ⟨helt, ?_⟩
            simp [List.getElem?_eq_some_iff, helt]
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
            rw [show cost f ((x, rest[e]?.getD x) :: qs) =
                f x (rest[e]?.getD x) + cost f qs by simp [cost]]
            rw [hqcost, ← ih_drop (e + 1)]
            exact hdeq'
          rw [← hcostcov]
          exact coverMin_le hmem

private def edgeAt (f : Price) (ks : List Nat) (a b : Nat) : Nat :=
  f (ks[a]?.getD 0) (ks[b - 1]?.getD 0)

private theorem seg_eq_coverCost (f : Price) (ks : List Nat) (off : Nat) :
    seg (ks.drop off).length off (edgeAt f ks) = coverCost f (ks.drop off) := by
  induction hlen : (ks.drop off).length using Nat.strongRecOn generalizing off with
  | ind n ih =>
    cases hdrop0 : ks.drop off with
    | nil =>
      have hn0 : n = 0 := by rw [← hlen, hdrop0]; simp
      rw [hn0]
      simp [seg, coverCost]
    | cons x rest =>
      have ih_drop (d : Nat) :
          seg (ks.drop (off + d + 1)).length (off + d + 1) (edgeAt f ks) =
            coverCost f (ks.drop (off + d + 1)) := by
        apply ih (ks.drop (off + d + 1)).length
        · have hdd : ks.drop (off + d + 1) = (ks.drop off).drop (d + 1) := by
            simpa [Nat.add_assoc] using
              (List.drop_drop (xs := ks) (i := off) (j := d + 1)).symm
          have hle : (ks.drop (off + d + 1)).length ≤ (ks.drop off).length - (d + 1) := by
            rw [hdd, List.length_drop]
            omega
          have hpos : 1 ≤ (ks.drop off).length := by rw [hdrop0]; simp
          omega
        · rfl
      have hxget : ks[off]? = some x := by simpa using (show (ks.drop off)[0]? = some x by simp [hdrop0])
      rw [← hlen, hdrop0]
      change seg (rest.length + 1) off (edgeAt f ks) = coverCost f (x :: rest)
      rw [seg, coverCost]
      apply congrArg (fun ys : List Nat => ys.min?.getD 0)
      apply List.map_congr_left
      intro d hdmem
      have hdlt : d < rest.length + 1 := List.mem_range.mp hdmem
      have hgeteq : ks[off + d]? = (x :: rest)[d]? := by
        have ht := List.getElem?_drop (xs := ks) (i := off) (j := d)
        simpa [hdrop0] using ht.symm
      have hcase : (x :: rest)[d]? = some ((x :: rest)[d]?.getD x) := by
        rw [List.getElem?_eq_some_iff]
        refine ⟨?_, ?_⟩
        · simpa [hdrop0] using hdlt
        · have hdlt' : d < (x :: rest).length := by simpa [hdrop0] using hdlt
          rw [List.getElem?_eq_getElem hdlt']
          simp
      have hlast : ks[off + d]?.getD 0 = (x :: rest)[d]?.getD x := by
        rw [hgeteq, hcase]
        simp
      have hdd' : ks.drop (off + d + 1) = (ks.drop off).drop (d + 1) := by
        simpa [Nat.add_assoc] using
          (List.drop_drop (i := d + 1) (j := off) (l := ks)).symm
      have hdrop : ks.drop (off + d + 1) = rest.drop d := by
        rw [hdd', hdrop0]
        simp
      have hseg := ih_drop d
      rw [hdrop] at hseg
      rw [List.length_drop] at hseg
      simp only [edgeAt, hxget, Option.getD_some]
      have hidx : off + d + 1 - 1 = off + d := by omega
      rw [hidx, hlast, hseg]

private theorem dp_eq_coverMin (f : Price) (ks : List Nat) : dp f ks = coverMin f ks := by
  unfold dp
  rw [boundaryCost_eq_seg]
  rw [List.size_toArray]
  have hedge : (fun a b => f (ks.toArray[a]?.getD 0) (ks.toArray[b - 1]?.getD 0)) =
      edgeAt f ks := by
    funext a b
    simp [edgeAt, List.getElem?_toArray]
  rw [hedge]
  simpa [coverCost_eq_coverMin] using (seg_eq_coverCost f ks 0)

private theorem leastCost_any (f : Price) : leastCost f := by
  intro xs
  change dp f (points xs) = cost f (choose f xs)
  rw [dp_eq_coverMin, choose_cost_eq_coverMin]

theorem sound_LeastCost : Sector.LeastCost.Goal := by
  intro shift startup
  exact leastCost_any (price shift startup)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_LeastCost
