import TinylambOpt.ResearchGoals.November26.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q06
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpch
open TinylambOpt.ResearchGoals.BenchmarkRematch


theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: l => by
      rw [List.eraseDups_cons]
      refine List.pairwise_cons.mpr ⟨?_, ?_⟩
      · intro b hb
        have hb' : b ∈ l.filter (fun b => !b == a) := by simpa using hb
        simp only [List.mem_filter] at hb'
        intro hba
        subst b
        simp at hb'
      · exact nodup_eraseDups (l.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by
  simp only [List.length_cons]
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b => !b == a) l)

theorem sum_filter_partition (xs : List (Int × Int)) (p : (Int × Int) → Bool)
    (f : (Int × Int) → Int) :
    ((xs.filter p).map f).sum + ((xs.filter (fun x => !p x)).map f).sum = (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h] at *
        omega
      · have hf : (!p x) = true := by simp [h]
        simp [h, hf] at *
        omega

theorem mul_sum (d : Int) (ps : List Int) : d * ps.sum = (ps.map fun p => d * p).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih => simp [ih, Int.mul_add]

theorem sum_yes (xs : List (Int × Int)) (d : Int) :
    ((xs.filter (fun r => r.1 == d)).map fun r => r.1 * r.2).sum =
      d * (forKey xs d).sum := by
  have hmap :
      ((xs.filter (fun r => r.1 == d)).map (fun r => r.1 * r.2)) =
        ((forKey xs d).map (fun p => d * p)) := by
    unfold forKey
    rw [List.map_map]
    apply List.map_congr_left
    intro r hr
    have heq : (r.1 == d) = true := (List.mem_filter.mp hr).2
    have hrd : r.1 = d := by simpa using heq
    rw [hrd]
    rfl
  rw [hmap]
  exact (mul_sum d (forKey xs d)).symm

theorem forKey_filter_not (xs : List (Int × Int)) (d d' : Int) (hneq : d ≠ d') :
    forKey (xs.filter (fun r => !(r.1 == d))) d' = forKey xs d' := by
  unfold forKey
  rw [List.filter_filter]
  apply congrArg (fun l : List (Int × Int) => l.map Prod.snd)
  apply List.filter_congr
  intro r hr
  by_cases he : r.1 == d'
  · have hr1 : r.1 = d' := by simpa using he
    have hrd : r.1 ≠ d := by
      intro hrd
      apply hneq
      exact hrd.symm.trans hr1
    simp [he, hrd]
  · simp [he]

theorem sum_by_keys (ks : List Int) (xs : List (Int × Int))
    (hcover : ∀ r ∈ xs, r.1 ∈ ks) (hnd : ks.Nodup) :
    (xs.map fun r => r.1 * r.2).sum = (ks.map fun d => d * (forKey xs d).sum).sum := by
  induction ks generalizing xs with
  | nil =>
      have hxnil : xs = [] := by
        cases xs with
        | nil => rfl
        | cons r rs =>
            have := hcover r (by simp)
            simp at this
      simp [hxnil, forKey]
  | cons d ks ih =>
      simp only [List.Nodup, List.pairwise_cons] at hnd
      have hdnks : d ∉ ks := by
        intro h
        exact hnd.1 d h rfl
      let yes := xs.filter (fun r => r.1 == d)
      let no := xs.filter (fun r => !(r.1 == d))
      have hnocover : ∀ r ∈ no, r.1 ∈ ks := by
        intro r hr
        have hmem : r ∈ xs := (List.mem_filter.mp hr).1
        have hkeq : !(r.1 == d) = true := (List.mem_filter.mp hr).2
        have hc := hcover r hmem
        rcases List.mem_cons.mp hc with hd | hk
        · have hr1 : r.1 = d := by simpa using hd
          simp [hr1] at hkeq
        · exact hk
      have ih' := ih no hnocover hnd.2
      have hpartition := sum_filter_partition xs (fun r => r.1 == d) (fun r => r.1 * r.2)
      have hyes := sum_yes xs d
      have hno : (ks.map (fun d' : Int => d' * (forKey no d').sum)).sum =
          (ks.map (fun d' : Int => d' * (forKey xs d').sum)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro d' hd'
        have hdn : d ≠ d' := by
          intro h
          subst d'
          exact hdnks hd'
        rw [forKey_filter_not xs d d' hdn]
      rw [← hpartition, hyes, ih', hno]
      simp [Int.add_assoc]


theorem sound : TinylambOpt.ResearchGoals.November26.Tpch.Q06.Goal := by
  intro rows lo hi
  let eligible := rows.filter (fun (d, _) => lo ≤ d && d ≤ hi)
  have hcover : ∀ r ∈ eligible, r.1 ∈ (eligible.map Prod.fst).eraseDups := by
    intro r hr
    rcases r with ⟨d, p⟩
    have hm : (d, p) ∈ eligible := by simpa using hr
    simpa [List.mem_eraseDups] using (List.mem_map.mpr ⟨(d, p), hm, rfl⟩ : d ∈ eligible.map Prod.fst)
  have h := TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q06.sum_by_keys (eligible.map Prod.fst).eraseDups eligible hcover
      (TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q06.nodup_eraseDups _)
  simpa [total, eligible] using h
end TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q06
