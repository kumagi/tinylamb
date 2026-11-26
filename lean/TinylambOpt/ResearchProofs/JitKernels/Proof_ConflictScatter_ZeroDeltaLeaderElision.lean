import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
Eliding the arithmetic store for leader entries whose reduced delta is zero
keeps every slot's numeric sum: filtering `(combine us)` on `u.2 != 0` only
drops lanes that add `0`, so
`delta ((combine us).filter (·.2 != 0)) k = delta us k`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_ZeroDeltaLeaderElision
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

private theorem filter_swap {α : Type} (l : List α) (p q : α → Bool) :
    (l.filter p).filter q = (l.filter q).filter p := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      by_cases hpa : p a = true <;> by_cases hqa : q a = true <;> simp [hpa, hqa, ih]

private theorem sum_filter_neZero (l : List (Nat × Int)) :
    ((l.filter (fun u => u.2 != 0)).map Prod.snd).sum = (l.map Prod.snd).sum := by
  induction l with
  | nil => rfl
  | cons p t ih =>
      obtain ⟨v, n⟩ := p
      by_cases hn : n = 0
      · subst hn
        rw [List.filter_cons_of_neg (p := fun u : Nat × Int => u.2 != 0) (by simpa),
          ih, List.map_cons, List.sum_cons]
        rw [show (v, (0 : Int)).snd = (0 : Int) from rfl, Int.zero_add]
      · rw [List.filter_cons_of_pos (p := fun u : Nat × Int => u.2 != 0) (by simpa using hn),
          List.map_cons, List.sum_cons, ih, List.map_cons, List.sum_cons]

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

private theorem delta_combine_sum (us : Updates) (k : Nat) :
    List.sum (List.map Prod.snd (List.filter (fun u : Nat × Int => u.1 == k) (combine us)))
      = delta us k := by
  have hsum : List.map Prod.snd (List.filter (fun u : Nat × Int => u.1 == k) (combine us))
      = if k ∈ leaders us then [delta us k] else [] := by
    show List.map Prod.snd
          (List.filter (fun u : Nat × Int => u.1 == k)
            (List.map (fun x => (x, delta us x)) ((us.map Prod.fst).eraseDups)))
        = if k ∈ (us.map Prod.fst).eraseDups then [delta us k] else []
    exact filtered_map_single _ (eraseDups_nodup _) _ _
  rw [hsum]
  by_cases hk : k ∈ leaders us
  · rw [if_pos hk, List.sum_cons, List.sum_nil, Int.add_zero]
  · rw [if_neg hk, List.sum_nil]
    refine Eq.symm ?_
    unfold delta
    have hf : us.filter (fun u => u.1 == k) = [] := by
      rw [List.filter_eq_nil_iff]
      intro u hu h
      have h1 : u.1 = k := by simpa using h
      exact hk (List.mem_eraseDups.mpr (h1 ▸ List.mem_map_of_mem (f := Prod.fst) hu))
    rw [hf, List.map_nil, List.sum_nil]

theorem sound : ZeroDeltaLeaderElision.Goal := by
  intro us k
  show List.sum (List.map Prod.snd (List.filter (fun u : Nat × Int => u.1 == k)
      (List.filter (fun u : Nat × Int => u.2 != 0) (combine us)))) = delta us k
  rw [filter_swap (combine us) (fun u : Nat × Int => u.2 != 0)
    (fun u : Nat × Int => u.1 == k), sum_filter_neZero, delta_combine_sum]

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_ZeroDeltaLeaderElision
