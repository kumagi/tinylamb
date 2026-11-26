import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

/-!
# RollupParentTotalsFromLeafPartitions
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.RollupParentTotalsFromLeafPartitions
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.RollupParentTotalsFromLeafPartitions

private theorem perm_sum_eq {l1 l2 : List Int} (h : l1.Perm l2) : l1.sum = l2.sum := by
  induction h with
  | nil => rfl
  | cons x h ih => simp [List.sum_cons, ih]
  | swap x y l => simp [List.sum_cons]; omega
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem nsum_eq_of_filterMap_perm {xs ys : List (Option Int)}
    (h : (xs.filterMap id).Perm (ys.filterMap id)) : nsum xs = nsum ys := by
  unfold nsum
  cases hx : xs.filterMap id with
  | nil =>
      have hy : ys.filterMap id = [] := by rw [hx] at h; exact h.symm.eq_nil
      simp [hx, hy]
  | cons a as =>
      cases hy2 : ys.filterMap id with
      | nil =>
          rw [hy2] at h; rw [hx] at h
          exact (List.not_perm_nil_cons _ _ h.symm).elim
      | cons b bs =>
          simp [hx, hy2]
          have hp : (a :: as).Perm (b :: bs) := by simpa [hx, hy2] using h
          exact perm_sum_eq hp

private theorem nsum_append (xs ys : List (Option Int)) :
    nsum (xs ++ ys) = match nsum xs, nsum ys with
      | none, t => t
      | t, none => t
      | some a, some b => some (a + b) := by
  unfold nsum
  simp only [List.filterMap_append]
  cases hx : xs.filterMap id <;> cases hy : ys.filterMap id <;> simp [hx, hy, List.sum_append, Int.add_assoc]

private theorem nsum_singleton (xs : List (Option Int)) : nsum [nsum xs] = nsum xs := by
  unfold nsum
  cases h : xs.filterMap id <;> simp [h, List.filterMap]

private theorem nsum_map_nsum (gs : List (List (Option Int))) :
    nsum (gs.map nsum) = nsum gs.flatten := by
  induction gs with
  | nil => simp [nsum]
  | cons g gs ih =>
      simp only [List.map_cons, List.flatten_cons]
      have h1 : nsum (nsum g :: gs.map nsum) = match nsum g, nsum (gs.map nsum) with
          | none, t => t | t, none => t | some a, some b => some (a + b) := by
        have := nsum_append [nsum g] (gs.map nsum)
        simp only [List.singleton_append] at this
        rw [this, nsum_singleton]
      rw [h1, ih]
      have h2 : nsum (g ++ gs.flatten) = match nsum g, nsum gs.flatten with
          | none, t => t | t, none => t | some a, some b => some (a + b) := nsum_append g gs.flatten
      rw [h2]

private theorem filterMap_forKey_flatten_perm {keys : List (Option Nat)} {L : List (Option Nat × Option Int)}
    (hnd : keys.Nodup)
    (hcover : ∀ r ∈ L, r.1 ∈ keys) :
    ((keys.flatMap (fun c => forKey L c)).filterMap id).Perm ((L.map Prod.snd).filterMap id) := by
  induction keys generalizing L with
  | nil =>
      have hL : L = [] := by
        cases L with
        | nil => rfl
        | cons r rs => have := hcover r (by simp); simp at this
      subst hL; simp [forKey]
  | cons k ks ih =>
      have hnd_ks : ks.Nodup := (List.nodup_cons.mp hnd).2
      let Lk := L.filter (fun r => r.1 == k)
      let Lr := L.filter (fun r => !(r.1 == k))
      have hpermL : L.Perm (Lk ++ Lr) := (List.filter_append_perm (fun r : Option Nat × Option Int => r.1 == k) L).symm
      have hcover_r : ∀ r ∈ Lr, r.1 ∈ ks := by
        intro r hr
        have hrL : r ∈ L := (List.mem_filter.mp hr).1
        have hne : r.1 ≠ k := by
          have hb := (List.mem_filter.mp hr).2
          simp at hb; exact fun h => by simp [h] at hb
        have := hcover r hrL
        simp at this
        rcases this with rfl | hks
        · exact (hne rfl).elim
        · exact hks
      have ih' := ih hnd_ks hcover_r
      simp only [List.flatMap_cons, List.filterMap_append]
      have hfk : forKey L k = Lk.map Prod.snd := by simp [forKey, Lk]
      have hfk_other : ∀ c ∈ ks, forKey L c = forKey Lr c := by
        intro c hc
        have hne : c ≠ k := fun he => (List.nodup_cons.mp hnd).1 (he ▸ hc)
        simp only [forKey, Lr, List.filter_filter]
        congr 1
        apply List.filter_congr
        intro r hr
        by_cases hk : r.1 == k
        · have hrk : r.1 = k := eq_of_beq hk
          have hck : (r.1 == c) = false := by
            apply beq_eq_false_iff_ne.mpr
            intro hrc
            apply hne
            rw [← hrk, hrc]
          simp [hk, hck]
        · simp [hk]
      have hflat : (ks.flatMap (fun c => forKey L c)).filterMap id = (ks.flatMap (fun c => forKey Lr c)).filterMap id := by
        congr 1
        simp only [List.flatMap]
        rw [List.map_congr_left (fun c hc => hfk_other c hc)]
      rw [hfk, hflat]
      have hright : ((L.map Prod.snd).filterMap id).Perm (((Lk ++ Lr).map Prod.snd).filterMap id) := by
        apply List.Perm.filterMap
        exact hpermL.map Prod.snd
      have hsplit : (((Lk ++ Lr).map Prod.snd).filterMap id) = ((Lk.map Prod.snd).filterMap id ++ ((Lr.map Prod.snd).filterMap id)) := by
        simp [List.filterMap_append]
      rw [hsplit] at hright
      exact (List.Perm.append_left _ ih').trans hright.symm

private theorem nodup_eraseDups_natopt (l : List (Option Nat)) : l.eraseDups.Nodup := by
  match l with
  | [] => simp [List.eraseDups_nil]
  | a :: as =>
      rw [List.eraseDups_cons]
      simp only [List.nodup_cons]
      refine ⟨?_, nodup_eraseDups_natopt _⟩
      intro ha
      have hmem : a ∈ List.filter (fun b => !b == a) as := List.mem_eraseDups.mp ha
      have hb := (List.mem_filter.mp hmem).2
      simp at hb
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

theorem sound : Goal := by
  intro leaves
  have h1 : nsum ((leaves.map Prod.fst).eraseDups.map (fun c => nsum (forKey leaves c))) =
      nsum (((leaves.map Prod.fst).eraseDups.map (fun c => forKey leaves c)).flatten) := by
    rw [show (leaves.map Prod.fst).eraseDups.map (fun c => nsum (forKey leaves c)) =
        ((leaves.map Prod.fst).eraseDups.map (fun c => forKey leaves c)).map nsum by simp [List.map_map]]
    rw [nsum_map_nsum]
  rw [h1]
  have hnd : ((leaves.map Prod.fst).eraseDups).Nodup := nodup_eraseDups_natopt _
  have hcover : ∀ r ∈ leaves, r.1 ∈ (leaves.map Prod.fst).eraseDups := by
    intro r hr
    rw [List.mem_eraseDups]
    exact List.mem_map.mpr ⟨r, hr, rfl⟩
  exact nsum_eq_of_filterMap_perm (filterMap_forKey_flatten_perm hnd hcover)

#print axioms sound
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.RollupParentTotalsFromLeafPartitions
