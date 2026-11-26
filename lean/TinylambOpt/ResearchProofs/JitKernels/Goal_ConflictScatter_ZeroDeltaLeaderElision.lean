import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-! Dropping leaders whose reduced delta is zero preserves all group sums.
    The compiled scatter still visits every distinct slot with a nonzero
    contribution, and skips the arithmetic store for the zero groups. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_ZeroDeltaLeaderElision

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.ZeroDeltaLeaderElision.Goal := by
  intro us k
  have hnodupFM : ∀ (l : List Nat) (f : Nat → Option Nat), (l.filterMap f).eraseDups.Nodup := by
    intro l
    induction l with
    | nil => intro f; simp [List.filterMap]
    | cons z t ih =>
        intro f
        simp only [List.filterMap_cons]
        split
        · exact ih f
        · rename_i b hb
          rw [List.eraseDups_cons]
          refine (List.nodup_cons).mpr ⟨?_, ?_⟩
          · intro hx
            rw [List.mem_eraseDups, List.mem_filter] at hx
            obtain ⟨hp, _⟩ := hx
            have hbb : (b == b) = true := by simp
            simp_all
          · have hstep : (t.filterMap f).filter (fun c => !(c == b)) =
                t.filterMap (fun y => (f y).filter fun c => !(c == b)) := List.filter_filterMap
            have := ih (fun y => (f y).filter fun c => !(c == b))
            simpa [hstep] using this
  have hnodupErase : ∀ l : List Nat, l.eraseDups.Nodup := by
    intro l
    have key : l.filterMap some = l := by
      induction l with
      | nil => rfl
      | cons z t ih => simp [List.filterMap_cons, ih]
    have := hnodupFM l some
    rwa [key] at this
  have hpairLeaders : (leaders us).Pairwise (fun a b => a ≠ b) := by
    simpa [leaders] using (List.nodup_iff_pairwise_ne.mp (hnodupErase (us.map Prod.fst)))
  have hfilterKey : ∀ (l : List Nat), l.Pairwise (fun a b => a ≠ b) →
      l.filter (fun b => b == k) = if k ∈ l then [k] else [] := by
    intro l hp
    induction l with
    | nil => simp
    | cons x xs ih =>
        have hp' := List.pairwise_cons.mp hp
        by_cases hx : x == k
        · have hxe : x = k := by simpa using hx
          subst x
          have hno : xs.filter (fun b => b == k) = [] := by
            apply (List.filter_eq_nil_iff).2
            intro y hy hyk
            apply hp'.1 y hy
            have hy' : y = k := by simpa using hyk
            simpa [hy']
          simp [hno]
        · have hmem : k ∈ x :: xs ↔ k ∈ xs := by
            constructor
            · intro hm
              rcases List.mem_cons.mp hm with hEq | hm
              · subst k
                have hxx : (x == x) = true := by simp
                exact (hx hxx).elim
              · exact hm
            · intro hm; exact List.mem_cons.mpr (Or.inr hm)
          simp [hx, hmem, ih hp'.2]
  have hkeyFilter : (combine us).filter (fun u => u.1 == k) =
      if k ∈ leaders us then [(k, delta us k)] else [] := by
    have hf := hfilterKey (leaders us) hpairLeaders
    rw [combine, List.filter_map]
    change List.map (fun b => (b, delta us b)) ((leaders us).filter (fun b => b == k)) =
      if k ∈ leaders us then [(k, delta us k)] else []
    rw [hf]
    by_cases hk' : k ∈ leaders us <;> simp [hk']
  have hswap2 : (combine us).filter (fun u => u.1 == k && u.2 != 0) =
      ((combine us).filter (fun u => u.1 == k)).filter (fun u => u.2 != 0) := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro u hu
    simp [Bool.and_comm]
  by_cases hk : k ∈ leaders us
  · by_cases hd0 : delta us k = 0
    · unfold delta at *
      rw [List.filter_filter, hswap2, hkeyFilter]
      simp [hk, hd0]
    · have hd : (delta us k != 0) = true := by simp [hd0]
      unfold delta at *
      rw [List.filter_filter, hswap2, hkeyFilter]
      simp [hk, hd]
  · have hnone : ∀ u ∈ us, u.1 ≠ k := by
      intro u hu huk
      apply hk
      rw [leaders]
      exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨u, hu, huk⟩)
    have hfilter : us.filter (fun u => u.1 == k) = [] := by
      apply (List.filter_eq_nil_iff).2
      intro u hu huk
      have hu' : u.1 = k := by simpa using huk
      exact hnone u hu hu'
    have hd0 : delta us k = 0 := by simp [delta, hfilter]
    unfold delta at *
    rw [List.filter_filter, hswap2, hkeyFilter]
    simp [hk, hd0]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_ZeroDeltaLeaderElision
