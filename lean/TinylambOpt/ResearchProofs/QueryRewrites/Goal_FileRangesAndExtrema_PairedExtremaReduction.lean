import TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema

/-!
# PairedExtremaReduction

Comparing non-NULL values in pairs and taking minima of the lower values and maxima of
the higher values preserves both extrema and satisfies the modeled comparison bound.

A scan computing MIN and MAX together can share the comparison within each pair and
handle an unmatched last value.
-/

set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema


theorem min?_eq_of_cover {xs ys : List Int}
    (hsub : ∀ y ∈ ys, y ∈ xs)
    (hcov : ∀ x ∈ xs, ∃ y ∈ ys, y ≤ x) :
    ys.min? = xs.min? := by
  cases xs with
  | nil =>
      have hys : ys = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro y hy
        simpa using hsub y hy
      simp [hys]
  | cons x xs =>
      cases ys with
      | nil =>
          obtain ⟨y, hy, _⟩ := hcov x (by simp)
          simp at hy
      | cons y ys =>
          let M := (xs.min?).elim x (min x)
          let N := (ys.min?).elim y (min y)
          have hM : (x :: xs).min? = some M := by simp [M, List.min?_cons]
          have hN : (y :: ys).min? = some N := by simp [N, List.min?_cons]
          have hMs := List.min?_eq_some_iff.mp hM
          have hNs := List.min?_eq_some_iff.mp hN
          simp only [hM, hN]
          apply congrArg some
          apply Int.le_antisymm
          · obtain ⟨z, hz, hzle⟩ := hcov M hMs.1
            exact Int.le_trans (hNs.2 z hz) hzle
          · exact hMs.2 N (hsub N hNs.1)

theorem max?_eq_of_cover {xs ys : List Int}
    (hsub : ∀ y ∈ ys, y ∈ xs)
    (hcov : ∀ x ∈ xs, ∃ y ∈ ys, x ≤ y) :
    ys.max? = xs.max? := by
  cases xs with
  | nil =>
      have hys : ys = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro y hy
        simpa using hsub y hy
      simp [hys]
  | cons x xs =>
      cases ys with
      | nil =>
          obtain ⟨y, hy, _⟩ := hcov x (by simp)
          simp at hy
      | cons y ys =>
          let M := (xs.max?).elim x (max x)
          let N := (ys.max?).elim y (max y)
          have hM : (x :: xs).max? = some M := by simp [M, List.max?_cons]
          have hN : (y :: ys).max? = some N := by simp [N, List.max?_cons]
          have hMs := List.max?_eq_some_iff.mp hM
          have hNs := List.max?_eq_some_iff.mp hN
          simp only [hM, hN]
          apply congrArg some
          apply Int.le_antisymm
          · exact hMs.2 N (hsub N hNs.1)
          · obtain ⟨z, hz, hzle⟩ := hcov M hMs.1
            exact Int.le_trans hzle (hNs.2 z hz)

theorem sub_lo : ∀ vs : List Int, ∀ y ∈ (extremes vs).1, y ∈ vs
  | [], y, hy => by simp [extremes] at hy
  | [x], y, hy => by simpa [extremes] using hy
  | a :: b :: rest, y, hy => by
      by_cases hab : a ≤ b
      · simp [extremes, hab] at hy
        rcases hy with rfl | hy
        · simp
        · exact List.mem_cons_of_mem a (List.mem_cons_of_mem b (sub_lo rest y hy))
      · simp [extremes, hab] at hy
        rcases hy with rfl | hy
        · simp
        · exact List.mem_cons_of_mem a (List.mem_cons_of_mem b (sub_lo rest y hy))

theorem cov_lo : ∀ vs : List Int, ∀ z ∈ vs, ∃ y ∈ (extremes vs).1, y ≤ z
  | [], z, hz => by simp at hz
  | [u], z, hz => by
      simp at hz
      subst z
      exact ⟨u, by simp [extremes], by omega⟩
  | a :: b :: rest, z, hz => by
      simp only [List.mem_cons] at hz
      by_cases hab : a ≤ b
      · rcases hz with hza | hzb | hzrest
        · subst z
          exact ⟨a, by simp [extremes, hab], by omega⟩
        · subst z
          exact ⟨a, by simp [extremes, hab], hab⟩
        · obtain ⟨y, hy, hyz⟩ := cov_lo rest z hzrest
          exact ⟨y, by simp [extremes, hab, hy], hyz⟩
      · rcases hz with hza | hzb | hzrest
        · subst z
          have hba : b ≤ a := by omega
          exact ⟨b, by simp [extremes, hab], hba⟩
        · subst z
          exact ⟨b, by simp [extremes, hab], by omega⟩
        · obtain ⟨y, hy, hyz⟩ := cov_lo rest z hzrest
          exact ⟨y, by simp [extremes, hab, hy], hyz⟩

theorem sub_hi : ∀ vs : List Int, ∀ y ∈ (extremes vs).2, y ∈ vs
  | [], y, hy => by simp [extremes] at hy
  | [x], y, hy => by simpa [extremes] using hy
  | a :: b :: rest, y, hy => by
      by_cases hab : a ≤ b
      · simp [extremes, hab] at hy
        rcases hy with rfl | hy
        · simp
        · exact List.mem_cons_of_mem a (List.mem_cons_of_mem b (sub_hi rest y hy))
      · simp [extremes, hab] at hy
        rcases hy with rfl | hy
        · simp
        · exact List.mem_cons_of_mem a (List.mem_cons_of_mem b (sub_hi rest y hy))

theorem cov_hi : ∀ xs : List Int, ∀ z ∈ xs, ∃ y ∈ (extremes xs).2, z ≤ y
  | [], z, hz => by simp at hz
  | [u], z, hz => by
      simp at hz
      subst z
      exact ⟨u, by simp [extremes], by omega⟩
  | a :: b :: rest, z, hz => by
      simp only [List.mem_cons] at hz
      by_cases hab : a ≤ b
      · rcases hz with hza | hzb | hzrest
        · subst z
          exact ⟨b, by simp [extremes, hab], hab⟩
        · subst z
          exact ⟨b, by simp [extremes, hab], by omega⟩
        · obtain ⟨y, hy, hzy⟩ := cov_hi rest z hzrest
          exact ⟨y, by simp [extremes, hab, hy], hzy⟩
      · have hba : b ≤ a := by omega
        rcases hz with hza | hzb | hzrest
        · subst z
          exact ⟨a, by simp [extremes, hab], by omega⟩
        · subst z
          exact ⟨a, by simp [extremes, hab], hba⟩
        · obtain ⟨y, hy, hzy⟩ := cov_hi rest z hzrest
          exact ⟨y, by simp [extremes, hab, hy], hzy⟩

theorem length_lo : ∀ vs : List Int, (extremes vs).1.length = (vs.length + 1) / 2
  | [] => by simp [extremes]
  | [_] => by simp [extremes]
  | a :: b :: rest => by
      have ih := length_lo rest
      by_cases hab : a ≤ b
      · simp [extremes, hab, ih]
        omega
      · simp [extremes, hab, ih]
        omega

theorem length_hi : ∀ vs : List Int, (extremes vs).2.length = (vs.length + 1) / 2
  | [] => by simp [extremes]
  | [_] => by simp [extremes]
  | a :: b :: rest => by
      have ih := length_hi rest
      by_cases hab : a ≤ b
      · simp [extremes, hab, ih]
        omega
      · simp [extremes, hab, ih]
        omega

theorem length_bound (n : Nat) :
    n / 2 + (((n+1)/2) - 1) + (((n+1)/2) - 1) ≤ (3*n+1)/2 - 2 := by
  have h1 : (n+1)/2 = n/2 + n%2 := by omega
  have h2 : n%2 < 2 := Nat.mod_lt n (by decide)
  have h3 : (3*n+1)/2 = n + (n+1)/2 := by omega
  omega


theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema.PairedExtremaReduction.Goal := by
  intro rows col
  dsimp
  let vs := rows.filterMap (fun r => r col)
  have hmin : (extremes vs).1.min? = vs.min? := by
    exact TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.min?_eq_of_cover (TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.sub_lo vs)
      (TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.cov_lo vs)
  have hmax : (extremes vs).2.max? = vs.max? := by
    exact TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.max?_eq_of_cover (TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.sub_hi vs)
      (TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.cov_hi vs)
  constructor
  · exact Prod.ext hmin hmax
  · rw [TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.length_lo vs, TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.length_hi vs]
    exact TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction.length_bound vs.length
end TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_PairedExtremaReduction
