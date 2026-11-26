import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
Reducing every colliding SIMD lane to a single signed delta per distinct slot
and issuing one scatter per leader slot preserves the sequential counter: for
every slot `k` and every initial accumulator, `sequential init (combine us) k`
equals `sequential init us k`. SQL grouped execution may therefore coalesce all
matching lanes before publishing each accumulator without changing results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_LeaderReducedScatter
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

private theorem eraseDups_nodup (l : List Nat) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons, List.nodup_cons]
      exact ⟨by simp, eraseDups_nodup (as.filter fun b => !b == a)⟩
termination_by l.length
decreasing_by
  simp only [List.length_cons]
  exact Nat.lt_of_lt_of_le (Nat.lt_succ_self _) (Nat.succ_le_succ (List.length_filter_le _ _))

private theorem filtered_map_single (ks : List Nat) (hf : ks.Nodup) (f : Nat → Int) (k : Nat) :
    ((ks.map (fun x => (x, f x))).filter (fun u : Nat × Int => u.1 == k)).map Prod.snd =
      if k ∈ ks then [f k] else [] := by
  induction ks with
  | nil => simp
  | cons a t ih =>
      obtain ⟨hat, htt⟩ := List.nodup_cons.mp hf
      by_cases hak : a = k
      · subst hak
        have hnil : List.filter (fun u : Nat × Int => u.1 == a) (t.map fun x => (x, f x)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro u hu h
          rcases List.mem_map.mp hu with ⟨y, hy, hyu⟩
          apply hat
          have hxy : y = u.1 := congrArg Prod.fst hyu
          have ha : u.1 = a := by simpa using h
          rw [← ha, ← hxy]
          exact hy
        rw [List.map_cons,
          List.filter_cons_of_pos (p := fun u : Nat × Int => u.1 == a) (by simp), hnil]
        simp
      · rw [List.map_cons,
          List.filter_cons_of_neg (p := fun u : Nat × Int => u.1 == k)
            (fun h => hak (by simpa using h)),
          ih htt]
        by_cases hkt : k ∈ t
        · rw [if_pos hkt, if_pos (by simp [hkt])]
        · have h2 : ¬ k ∈ a :: t := by
            intro hmem
            rcases List.mem_cons.mp hmem with h | h
            · exact hak h.symm
            · exact hkt h
          rw [if_neg hkt, if_neg h2]

private theorem delta_combine (us : Updates) (k : Nat) : delta (combine us) k = delta us k := by
  have hsum : List.map Prod.snd (List.filter (fun u : Nat × Int => u.1 == k) (combine us))
      = if k ∈ leaders us then [delta us k] else [] := by
    show List.map Prod.snd
          (List.filter (fun u : Nat × Int => u.1 == k)
            (List.map (fun x => (x, delta us x)) ((us.map Prod.fst).eraseDups)))
        = if k ∈ (us.map Prod.fst).eraseDups then [delta us k] else []
    exact filtered_map_single _ (eraseDups_nodup _) _ _
  unfold delta
  rw [hsum]
  by_cases hk : k ∈ leaders us
  · rw [if_pos hk, List.sum_cons, List.sum_nil, Int.add_zero]
    exact rfl
  · rw [if_neg hk, List.sum_nil]
    refine Eq.symm ?_
    have hf : us.filter (fun u => u.1 == k) = [] := by
      rw [List.filter_eq_nil_iff]
      intro u hu h
      have h1 : u.1 = k := by simpa using h
      exact hk (List.mem_eraseDups.mpr (h1 ▸ List.mem_map_of_mem (f := Prod.fst) hu))
    rw [hf, List.map_nil, List.sum_nil]

theorem sound : LeaderReducedScatter.Goal := by
  intro us init k
  show init k + delta (combine us) k = init k + delta us k
  rw [delta_combine]

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_LeaderReducedScatter
