import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

namespace TinylambOpt.ResearchProofs.StorageExecution.GatherPlansB

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

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

end TinylambOpt.ResearchProofs.StorageExecution.GatherPlansB
