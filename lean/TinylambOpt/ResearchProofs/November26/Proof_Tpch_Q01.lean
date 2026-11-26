import TinylambOpt.ResearchGoals.November26.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q01
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpch
open TinylambOpt.ResearchGoals.BenchmarkRematch


def addOpt (a b : Option Int) : Option Int :=
  match a, b with
  | none, b => b
  | a, none => a
  | some x, some y => some (x + y)

theorem nsum_eq_filterMap (xs : List (Option Int)) :
    nsum xs = nsum ((xs.filterMap id).map some) := by
  cases xs <;> simp [nsum]

theorem nsum_append (xs ys : List (Option Int)) :
    nsum (xs ++ ys) = addOpt (nsum xs) (nsum ys) := by
  rw [nsum_eq_filterMap, nsum_eq_filterMap (xs := xs), nsum_eq_filterMap (xs := ys)]
  rw [List.filterMap_append]
  cases xs.filterMap id with
  | nil => simp [nsum, addOpt]
  | cons x xs' =>
      cases ys.filterMap id with
      | nil => simp [nsum, addOpt]
      | cons y ys' => simp [nsum, addOpt, Int.add_assoc]

theorem nsum_cons (o : Option Int) (xs : List (Option Int)) :
    nsum (o :: xs) = addOpt o (nsum xs) := by
  cases o with
  | none => simp [nsum, addOpt]
  | some a =>
      cases hx : xs.filterMap id with
      | nil => simp [nsum, addOpt, hx]
      | cons b bs => simp [nsum, addOpt, hx, Int.add_assoc]

def scan (cutoff : Int) (p : List (Int × Option Int)) : List (Option Int) :=
  (p.filter (fun r => r.1 ≤ cutoff)).map Prod.snd

theorem scan_append (cutoff : Int) (p q : List (Int × Option Int)) :
    nsum (scan cutoff (p ++ q)) = addOpt (nsum (scan cutoff p)) (nsum (scan cutoff q)) := by
  simp [scan, List.filter_append, List.map_append, nsum_append]

theorem all_le_of_max (p : List (Int × Option Int)) (cutoff : Int)
    (h : (p.map Prod.fst).max?.any (· ≤ cutoff) = true) :
    ∀ r ∈ p, r.1 ≤ cutoff := by
  rw [Option.any_eq_true] at h
  rcases h with ⟨m, hm, hmle⟩
  have hmax := List.max?_eq_some_iff.mp hm
  intro r hr
  have hrmem : r.1 ∈ p.map Prod.fst := by
    exact List.mem_map.mpr ⟨(r.1, r.2), by simpa using hr, rfl⟩
  exact Int.le_trans (hmax.2 r.1 hrmem) (by simpa using hmle)

theorem state_eq_scan (p : List (Int × Option Int)) (cutoff : Int) :
    (if (p.map Prod.fst).max?.any (· ≤ cutoff) then nsum (p.map Prod.snd)
      else nsum (scan cutoff p)) = nsum (scan cutoff p) := by
  by_cases h : (p.map Prod.fst).max?.any (· ≤ cutoff) = true
  · have hall := all_le_of_max p cutoff h
    have hf : p.filter (fun r => r.1 ≤ cutoff) = p := by
      apply List.filter_eq_self.mpr
      intro r hr
      simpa using hall r hr
    simp [h, scan, hf]
  · simp [h]

theorem scan_pages (pages : List (List (Int × Option Int))) (cutoff : Int) :
    nsum (pages.map (fun p => nsum (scan cutoff p))) = nsum (scan cutoff pages.flatten) := by
  induction pages with
  | nil => simp [scan, nsum]
  | cons p ps ih =>
      rw [List.map_cons, nsum_cons, ih]
      simp only [List.flatten_cons]
      rw [scan_append]


theorem sound : TinylambOpt.ResearchGoals.November26.Tpch.Q01.Goal := by
  intro pages cutoff
  dsimp
  have hstate : pages.map (fun p =>
      if (p.map Prod.fst).max?.any (· ≤ cutoff) then nsum (p.map Prod.snd)
      else nsum ((p.filter (fun r => r.1 ≤ cutoff)).map Prod.snd)) =
      pages.map (fun p => nsum (TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q01.scan cutoff p)) := by
    apply List.map_congr_left
    intro p hp
    simpa [TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q01.scan] using TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q01.state_eq_scan p cutoff
  rw [hstate]
  exact TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q01.scan_pages pages cutoff
end TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q01
