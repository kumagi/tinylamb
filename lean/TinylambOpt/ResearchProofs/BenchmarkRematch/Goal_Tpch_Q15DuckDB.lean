import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q15DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

abbrev maxima (rs : List (Nat × Int)) : Option Int := (rs.map Prod.snd).max?

abbrev candidates (shards : List (List (Nat × Int))) : List (Nat × Int) :=
  shards.flatMap fun rs => rs.filter (fun r => some r.2 == maxima rs)

theorem local_max_of_global {rs : List (Nat × Int)} {m : Int}
    (hmem : m ∈ rs.map Prod.snd) (hub : ∀ y ∈ rs.map Prod.snd, y ≤ m) :
    maxima rs = some m := by
  exact List.max?_eq_some_iff.mpr ⟨hmem, hub⟩

theorem candidates_filter (m : Int) : ∀ shards : List (List (Nat × Int)),
    (∀ y ∈ (shards.flatten.map Prod.snd), y ≤ m) →
    (candidates shards).filter (fun r => r.2 == m) =
      shards.flatten.filter (fun r => r.2 == m) := by
  intro shards
  induction shards with
  | nil => intro hub; simp [candidates]
  | cons rs shards ih =>
    intro hub
    have hrs_mem : ∀ y ∈ rs.map Prod.snd, y ∈ (rs ++ shards.flatten).map Prod.snd := by
      intro y hy
      rcases List.mem_map.mp hy with ⟨r, hr, rfl⟩
      exact List.mem_map_of_mem (List.mem_append_left _ hr)
    have htail_mem : ∀ y ∈ (shards.flatten.map Prod.snd), y ∈ (rs ++ shards.flatten).map Prod.snd := by
      intro y hy
      rcases List.mem_map.mp hy with ⟨r, hr, rfl⟩
      exact List.mem_map_of_mem (List.mem_append_right _ hr)
    have hub_rs : ∀ y ∈ rs.map Prod.snd, y ≤ m := fun y hy => hub y (hrs_mem y hy)
    have hub_tail : ∀ y ∈ (shards.flatten.map Prod.snd), y ≤ m := fun y hy => hub y (htail_mem y hy)
    have hlocal : ∀ r ∈ rs, r.2 == m → some r.2 == maxima rs := by
      intro r hr hrm
      have hrm' : r.2 = m := by simpa using hrm
      have hmem : m ∈ rs.map Prod.snd := by
        simpa [hrm'] using (List.mem_map_of_mem (f := Prod.snd) hr)
      have hmax := local_max_of_global hmem hub_rs
      simp [hmax, hrm']
    simp only [candidates, List.flatMap_cons, List.flatten_cons, List.filter_append]
    rw [ih hub_tail]
    congr 1
    rw [List.filter_filter]
    apply List.filter_congr
    intro r hr
    by_cases hp : r.2 == m
    · simp [hp, hlocal r hr hp]
    · simp [hp]

theorem mem_candidates_of_global (shards : List (List (Nat × Int))) {r : Nat × Int} {m : Int}
    (hrm : r.2 = m) (hrF : r ∈ shards.flatten)
    (hub : ∀ y ∈ (shards.flatten.map Prod.snd), y ≤ m) :
    r ∈ candidates shards := by
  rw [List.mem_flatten] at hrF
  rcases hrF with ⟨rs, hrs, hr⟩
  rw [List.mem_flatMap]
  refine ⟨rs, hrs, ?_⟩
  simp only [List.mem_filter, hr, true_and]
  have hrs_mem : ∀ y ∈ rs.map Prod.snd, y ∈ (shards.flatten.map Prod.snd) := by
    intro y hy
    rcases List.mem_map.mp hy with ⟨s, hs, rfl⟩
    exact List.mem_map_of_mem (List.mem_flatten.mpr ⟨rs, hrs, hs⟩)
  have hub_rs : ∀ y ∈ rs.map Prod.snd, y ≤ m := fun y hy => hub y (hrs_mem y hy)
  have hmem : m ∈ rs.map Prod.snd := by
    simpa [hrm] using (List.mem_map_of_mem (f := Prod.snd) hr)
  have hmax := local_max_of_global hmem hub_rs
  simp [hmax, hrm]

theorem max_candidates (shards : List (List (Nat × Int))) {m : Int}
    (hFmax : (shards.flatten.map Prod.snd).max? = some m) :
    ((candidates shards).map Prod.snd).max? = some m := by
  have hub : ∀ y ∈ (shards.flatten.map Prod.snd), y ≤ m :=
    (List.max?_le_iff hFmax).1 (by omega : m ≤ m)
  apply List.max?_eq_some_iff.mpr
  constructor
  · have hmemF : m ∈ shards.flatten.map Prod.snd := List.max?_mem hFmax
    rcases List.mem_map.mp hmemF with ⟨r, hrF, hrm⟩
    have hrc : r ∈ candidates shards := mem_candidates_of_global shards hrm hrF hub
    simpa [hrm] using (List.mem_map_of_mem (f := Prod.snd) hrc)
  · intro y hy
    rcases List.mem_map.mp hy with ⟨r, hrc, hry⟩
    have hrF : r ∈ shards.flatten := by
      rw [List.mem_flatMap] at hrc
      rcases hrc with ⟨rs, hrs, hrf⟩
      exact List.mem_flatten.mpr ⟨rs, hrs, (List.mem_filter.mp hrf).1⟩
    have hyF : y ∈ shards.flatten.map Prod.snd := by
      simpa [hry] using (List.mem_map_of_mem (f := Prod.snd) hrF)
    exact hub y hyF

theorem candidates_empty_of_flatten_empty : ∀ shards : List (List (Nat × Int)),
    shards.flatten = [] → candidates shards = [] := by
  intro shards
  induction shards with
  | nil => intro h; simp [candidates]
  | cons rs shards ih =>
    intro h
    have hn : rs ++ shards.flatten = [] := by simpa only [List.flatten_cons] using h
    have hparts : rs = [] ∧ shards.flatten = [] := by
      cases rs with
      | nil =>
        have ht : shards.flatten = [] := by simpa using hn
        exact ⟨rfl, ht⟩
      | cons x xs => simp at hn
    simp [candidates, hparts.1, ih hparts.2]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch.Q15DuckDB.Goal := by
  intro shards
  dsimp only
  by_cases hF : shards.flatten = []
  · have hC := candidates_empty_of_flatten_empty shards hF
    simp [hC, hF]
  · have hV : shards.flatten.map Prod.snd ≠ [] := by
      intro hv
      have : shards.flatten = [] := by simpa using hv
      exact hF this
    let m : Int := (shards.flatten.map Prod.snd).max hV
    have hFmax : (shards.flatten.map Prod.snd).max? = some m := by
      simpa [m] using (List.max?_eq_some_max (l := shards.flatten.map Prod.snd) hV)
    have hCmax : ((candidates shards).map Prod.snd).max? = some m := max_candidates shards hFmax
    have hfilter : (candidates shards).filter (fun r => r.2 == m) =
        shards.flatten.filter (fun r => r.2 == m) :=
      candidates_filter m shards ((List.max?_le_iff hFmax).1 (by omega : m ≤ m))
    rw [hCmax, hFmax]
    simpa using hfilter

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q15DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q15DuckDB.sound
