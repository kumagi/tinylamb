import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_HistogramFold

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

private theorem mass_cons (x v : Int) (w : Nat) (ys : Costs) :
    mass ((x,w)::ys) (fun u => u == v) =
      (if (x == v) then w else 0) + mass ys (fun u => u == v) := by
  unfold mass
  by_cases h : (x == v) = true
  · simp [h]
  · have hf : (x == v) = false := Bool.eq_false_of_ne_true h
    simp [hf]

private theorem eraseDups_nodup : ∀ xs : List Int, xs.eraseDups.Nodup
  | [] => by simp
  | x :: xs => by
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hx
        have hmem : x ∈ (List.filter (fun b : Int => ! b == x) xs).eraseDups := hx
        have hmem' : x ∈ List.filter (fun b : Int => ! b == x) xs := (List.mem_eraseDups).mp hmem
        have hb := (List.mem_filter.mp hmem').2
        simp at hb
      · exact eraseDups_nodup (List.filter (fun b : Int => ! b == x) xs)
termination_by xs => xs.length
decreasing_by
  simp_wf
  have hle := List.length_filter_le (fun b : Int => ! b == x) xs
  omega

private theorem sum_add_single (x : Int) (h₁ h₂ : Int → Nat) :
    ∀ vs : List Int, vs.Nodup → x ∈ vs →
      (vs.map (fun v => (if x = v then h₁ v else 0) + h₂ v)).sum =
        h₁ x + (vs.map h₂).sum := by
  intro vs
  induction vs with
  | nil => intro hnodup hx; simp at hx
  | cons v vs ih =>
      intro hnodup hx
      have hnv : v ∉ vs := (List.nodup_cons.mp hnodup).1
      have hnodup' : vs.Nodup := (List.nodup_cons.mp hnodup).2
      by_cases hv : x = v
      · subst v
        have hx_notin : x ∉ vs := hnv
        have hmap : vs.map (fun v' => (if x = v' then h₁ v' else 0) + h₂ v') = vs.map h₂ := by
          apply List.map_congr_left
          intro v' hv'
          have hne : x ≠ v' := by
            intro heq
            exact hx_notin (by simpa [heq] using hv')
          simp [hne]
        simp [List.sum_cons, hmap]
        omega
      · have hx_in_tail : x ∈ vs := by
          rcases List.mem_cons.mp hx with h | h
          · exact False.elim (hv h)
          · exact h
        have hih := ih hnodup' hx_in_tail
        simp [List.sum_cons, hv, hih]
        omega

/-- For any per-value cost `g`, the weighted sum over the raw list equals the
weighted sum over the histogram whose keys are the deduplicated values and
whose weights are the aggregated masses. -/
private theorem histogram_aux (g : Int → Nat) :
    ∀ (xs : Costs) (vs : List Int),
      vs.Nodup →
      (∀ p ∈ xs, p.1 ∈ vs) →
      (vs.map (fun v => mass xs (fun u => u == v) * g v)).sum =
        (xs.map (fun p => p.2 * g p.1)).sum := by
  intro xs
  induction xs with
  | nil =>
      intro vs hnodup hall
      clear hnodup hall
      induction vs with
      | nil => simp [mass]
      | cons v vs ih =>
          simp [mass] at ih ⊢
          exact ih
  | cons p ys ih =>
      rcases p with ⟨x,w⟩
      intro vs hnodup hall
      have hxvs : x ∈ vs := hall (x,w) (by simp)
      have hyall : ∀ p ∈ ys, p.1 ∈ vs := by
        intro p hp
        exact hall p (by simp [hp])
      have hih := ih vs hnodup hyall
      have hmap1 : (vs.map (fun v => mass ((x,w)::ys) (fun u => u == v) * g v)) =
          (vs.map (fun v => ((if x = v then w else 0) + mass ys (fun u => u == v)) * g v)) := by
        apply List.map_congr_left
        intro v hv
        rw [mass_cons]
        simp
      rw [hmap1]
      have hmap2 : (vs.map (fun v => ((if x = v then w else 0) + mass ys (fun u => u == v)) * g v)) =
          (vs.map (fun v => (if x = v then w * g v else 0) + (mass ys (fun u => u == v) * g v))) := by
        apply List.map_congr_left
        intro v hv
        by_cases h : x = v
        · subst v; simp [Nat.add_mul]
        · simp [h]
      rw [hmap2]
      rw [sum_add_single x (fun v => w * g v) (fun v => mass ys (fun u => u == v) * g v) vs hnodup hxvs]
      rw [hih]
      simp [List.sum_cons]

private theorem weighted_map_sum (g : Int → Nat) (vs : List Int) (h : Int → Nat) :
    ((vs.map (fun v => (v, h v))).map (fun p => p.2 * g p.1)).sum =
      (vs.map (fun v => h v * g v)).sum := by
  rw [List.map_map]
  simp [Function.comp_def]

theorem sound_HistogramFold : RiskEnvelope.HistogramFold.Goal := by
  intro xs z a b
  unfold loss
  let g : Int → Nat := fun v => a * (v - z).toNat + b * (z - v).toNat
  change (xs.map (fun p => p.2 * g p.1)).sum =
    (((xs.map Prod.fst).eraseDups.map (fun v => (v, mass xs (fun u => u == v)))).map
      (fun p => p.2 * g p.1)).sum
  rw [weighted_map_sum g ((xs.map Prod.fst).eraseDups) (fun v => mass xs (fun u => u == v))]
  symm
  exact histogram_aux g xs ((xs.map Prod.fst).eraseDups) (eraseDups_nodup _) (by
    intro p hp
    have hp' : p.1 ∈ xs.map Prod.fst := List.mem_map_of_mem hp
    exact (List.mem_eraseDups).mpr hp')

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_HistogramFold
