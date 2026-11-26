import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q02
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

private theorem sum_perm (f : Int → Int) : ∀ {l₁ l₂ : List Int}, l₁.Perm l₂ →
    (l₁.map f).sum = (l₂.map f).sum
  | _, _, List.Perm.nil => rfl
  | _, _, List.Perm.cons x h => by simpa using sum_perm f h
  | _, _, List.Perm.swap x y l => by simp [Int.add_comm, Int.add_left_comm]
  | _, _, List.Perm.trans h1 h2 => by rw [sum_perm f h1, sum_perm f h2]

private theorem nsum_view (Z : List (Option Int)) :
    nsum Z = if (Z.filterMap id).isEmpty = true then none else some (Z.filterMap id).sum := rfl

private theorem nsum_perm {l₁ l₂ : List (Option Int)} (h : l₁.Perm l₂) : nsum l₁ = nsum l₂ := by
  have hp : (l₁.filterMap id).Perm (l₂.filterMap id) := h.filterMap id
  have hlen := hp.length_eq
  have hemp : (l₁.filterMap id).isEmpty = true ↔ (l₂.filterMap id).isEmpty = true := by
    rw [List.isEmpty_iff, List.isEmpty_iff]
    constructor
    · intro hn
      apply List.length_eq_zero_iff.mp
      rw [← hlen, List.length_eq_zero_iff.mpr hn]
    · intro hn
      apply List.length_eq_zero_iff.mp
      rw [hlen, List.length_eq_zero_iff.mpr hn]
  have hsum : (l₁.filterMap id).sum = (l₂.filterMap id).sum := by
    simpa using sum_perm (fun z : Int => z) hp
  rw [nsum_view, nsum_view]
  by_cases hl : (l₁.filterMap id).isEmpty = true
  · have hr := hemp.mp hl
    rw [hl, hr, hsum]
  · have hr : ¬ ((l₂.filterMap id).isEmpty = true) := by
      intro hr
      exact hl (hemp.mpr hr)
    have hl' : ((l₁.filterMap id).isEmpty = true) = False := by simpa using hl
    have hr' : ((l₂.filterMap id).isEmpty = true) = False := by simpa using hr
    simp only [hl', hr', hsum, ite_false]

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => .nil
  | a :: l => by
      rw [List.eraseDups_cons]
      constructor
      · intro h hmem
        rw [List.mem_eraseDups, List.mem_filter] at hmem
        intro heq
        rw [← heq] at hmem
        simp at hmem
      · exact nodup_eraseDups (l.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by
  have := List.length_filter_le (fun b => !b == a) l
  simp
  omega

private theorem perm_filter_append_not {α : Type} (p : α → Bool) :
    ∀ r : List α, r.Perm (r.filter p ++ r.filter (fun x => !p x))
  | [] => List.Perm.nil
  | x :: xs => by
      have ih := perm_filter_append_not p xs
      cases hp : p x
      · simp only [List.filter, hp, Bool.not_false]
        exact (ih.cons x).trans List.perm_middle.symm
      · simp only [List.filter, hp, Bool.not_true, List.cons_append]
        exact ih.cons x

private theorem perm_buckets_of_nodup (ks : List Nat) (r : List (Nat × Option Int)) :
    ks.Nodup →
    (∀ row ∈ r, row.1 ∈ ks) →
    r.Perm (ks.flatMap (fun k => r.filter (fun row => row.1 == k))) := by
  intro hNodup hCover
  induction ks generalizing r with
  | nil =>
      cases r with
      | nil => exact List.Perm.nil
      | cons x _ =>
          have := hCover x (List.mem_cons_self ..)
          cases this
  | cons k ks ih =>
      rw [List.nodup_cons] at hNodup
      obtain ⟨hkNotMem, hNodupKs⟩ := hNodup
      let rRest := r.filter (fun row => !(row.1 == k))
      have hSplit : r.Perm (r.filter (fun row => row.1 == k) ++ rRest) :=
        perm_filter_append_not (fun row => row.1 == k) r
      have hCoverRest : ∀ row ∈ rRest, row.1 ∈ ks := by
        intro row hMem
        rw [List.mem_filter] at hMem
        have hIn := hCover row hMem.1
        rw [List.mem_cons] at hIn
        cases hIn with
        | inl hEq =>
            have : (row.1 == k) = true := beq_iff_eq.mpr hEq
            rw [this] at hMem
            cases hMem.2
        | inr hInKs => exact hInKs
      have ih' := ih rRest hNodupKs hCoverRest
      have hSameBuckets :
          ks.flatMap (fun k' => rRest.filter (fun row => row.1 == k')) =
          ks.flatMap (fun k' => r.filter (fun row => row.1 == k')) := by
        have hElem : ∀ k' ∈ ks,
            rRest.filter (fun row => row.1 == k') = r.filter (fun row => row.1 == k') := by
          intro k' hk'
          have hne : k' ≠ k := fun hEq => hkNotMem (hEq ▸ hk')
          simp only [rRest, List.filter_filter]
          apply List.filter_congr
          intro row _
          by_cases hEq : row.1 = k'
          · simp [hEq, hne]
          · simp [beq_iff_eq, hEq]
        clear ih ih' hCoverRest hCover hNodupKs hkNotMem hSplit
        induction ks with
        | nil => rfl
        | cons k1 ks1 ihk =>
            have hk1 := hElem k1 (List.mem_cons_self ..)
            have hks1 := ihk (fun z hz => hElem z (List.mem_cons_of_mem k1 hz))
            simp [List.flatMap_cons, hk1, hks1]
      rw [hSameBuckets] at ih'
      simp only [List.flatMap_cons]
      exact hSplit.trans (ih'.append_left _)

private theorem perm_eraseDups_buckets (r : List (Nat × Option Int)) :
    r.Perm (((r.map Prod.fst).eraseDups).flatMap
      (fun k => r.filter (fun row => row.1 == k))) := by
  apply perm_buckets_of_nodup
  · exact nodup_eraseDups (r.map Prod.fst)
  · intro row hrow
    exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨row, hrow, rfl⟩)

private theorem filter_flatMap_buckets_of_key_pred (keys : List Nat)
    (r : List (Nat × Option Int)) (p : Nat → Bool) :
    (keys.flatMap (fun k => r.filter (fun row => row.1 == k))).filter (fun row => p row.1) =
      (keys.filter p).flatMap (fun k => r.filter (fun row => row.1 == k)) := by
  induction keys with
  | nil => rfl
  | cons k ks ih =>
      rw [List.flatMap_cons, List.filter_append, ih]
      by_cases hp : p k = true
      · rw [List.filter_cons_of_pos hp, List.flatMap_cons]
        congr 1
        apply List.filter_eq_self.mpr
        intro row hrow
        have hrow' := List.mem_filter.mp hrow
        have hkeq : row.1 = k := beq_iff_eq.mp hrow'.2
        simpa [hkeq, hp]
      · have hp' : ¬ p k = true := by simpa using hp
        rw [List.filter_cons_of_neg hp']
        have hbucket : (r.filter (fun row => row.1 == k)).filter (fun row => p row.1) = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro row hrow
          have hrow' := List.mem_filter.mp hrow
          have hrow'' := List.mem_filter.mp hrow'.1
          have hkeq : row.1 = k := beq_iff_eq.mp hrow''.2
          have hpfalse : ¬ p row.1 = true := by simpa [hkeq] using hp'
          exact hpfalse hrow'.2
        simp [hbucket]

private def comb (a b : Option Int) : Option Int :=
  match a, b with
  | some x, some y => some (x + y)
  | some x, none => some x
  | none, some y => some y
  | none, none => none

private theorem nsum_append (X Y : List (Option Int)) :
    nsum (X ++ Y) = comb (nsum X) (nsum Y) := by
  rw [nsum_view, nsum_view, nsum_view, List.filterMap_append]
  by_cases hX : (X.filterMap id).isEmpty = true
  · have hXnil : X.filterMap id = [] := List.isEmpty_iff.mp hX
    by_cases hY : (Y.filterMap id).isEmpty = true
    · have hYnil : Y.filterMap id = [] := List.isEmpty_iff.mp hY
      have hXY : (X.filterMap id ++ Y.filterMap id).isEmpty = true := by
        rw [List.isEmpty_iff, List.append_eq_nil_iff]
        exact ⟨hXnil, hYnil⟩
      simp [hX, hY, hXY, hXnil, hYnil, comb]
    · have hXY : ¬ ((X.filterMap id ++ Y.filterMap id).isEmpty = true) := by
        intro h
        have hnil : X.filterMap id ++ Y.filterMap id = [] := List.isEmpty_iff.mp h
        exact hY (List.isEmpty_iff.mpr (List.append_eq_nil_iff.mp hnil).2)
      simp [hX, hY, hXY, hXnil, comb, List.sum_append]
  · by_cases hY : (Y.filterMap id).isEmpty = true
    · have hYnil : Y.filterMap id = [] := List.isEmpty_iff.mp hY
      have hXY : ¬ ((X.filterMap id ++ Y.filterMap id).isEmpty = true) := by
        intro h
        have hnil : X.filterMap id ++ Y.filterMap id = [] := List.isEmpty_iff.mp h
        exact hX (List.isEmpty_iff.mpr (List.append_eq_nil_iff.mp hnil).1)
      simp [hX, hY, hXY, hYnil, comb, List.sum_append]
    · have hXY : ¬ ((X.filterMap id ++ Y.filterMap id).isEmpty = true) := by
        intro h
        have hnil : X.filterMap id ++ Y.filterMap id = [] := List.isEmpty_iff.mp h
        exact hX (List.isEmpty_iff.mpr (List.append_eq_nil_iff.mp hnil).1)
      have hsum : (X.filterMap id ++ Y.filterMap id).sum =
          (X.filterMap id).sum + (Y.filterMap id).sum := List.sum_append
      simp [hX, hY, hXY, comb, hsum]

private theorem nsum_singleton (z : Option Int) : nsum [z] = z := by
  rw [nsum_view]
  cases z <;> simp

private theorem nsum_map_nsum :
    ∀ (xs : List (List (Option Int))), nsum (xs.map nsum) = nsum xs.flatten := by
  intro xs
  induction xs with
  | nil => rfl
  | cons g gs ih =>
      show nsum (nsum g :: gs.map nsum) = nsum (g ++ gs.flatten)
      rw [← List.singleton_append, nsum_append, nsum_singleton, ih, ← nsum_append]

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q02.Goal := by
  intro sales weekDay cell
  let p : Nat → Bool := fun d => weekDay d == cell
  let keys := (sales.map Prod.fst).eraseDups
  let groups := (keys.filter p).map (fun d => forKey sales d)
  have hpermRows :
      (sales.filter (fun row => p row.1)).Perm
        (keys.flatMap (fun k => sales.filter (fun row => row.1 == k)) |>.filter (fun row => p row.1)) := by
    have h := (perm_eraseDups_buckets sales).filter (fun row => p row.1)
    simpa [keys] using h
  have hpermRows' :
      (sales.filter (fun row => p row.1)).Perm
        ((keys.filter p).flatMap (fun k => sales.filter (fun row => row.1 == k))) := by
    exact hpermRows.trans (by
      rw [filter_flatMap_buckets_of_key_pred])
  have hpermVals :
      ((sales.filter (fun row => p row.1)).map Prod.snd).Perm
        ((keys.filter p).flatMap (fun k => forKey sales k)) := by
    have h := hpermRows'.map Prod.snd
    rw [List.map_flatMap] at h
    simpa [forKey] using h
  have hflat : nsum (((sales.filter (fun row => p row.1)).map Prod.snd)) =
      nsum ((keys.filter p).flatMap (fun k => forKey sales k)) := nsum_perm hpermVals
  have hgroups : nsum ((keys.filter p).flatMap (fun k => forKey sales k)) =
      nsum (groups.map nsum) := by
    dsimp [groups]
    exact (nsum_map_nsum ((keys.filter p).map (fun k => forKey sales k))).symm
  rw [hflat, hgroups]
  dsimp [p, keys, groups]
  rw [List.map_map]
  rfl
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q02
