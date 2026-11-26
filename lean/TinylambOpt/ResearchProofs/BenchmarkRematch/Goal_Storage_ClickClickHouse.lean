import TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
import TinylambOpt.ResearchProofs.BenchmarkRematch.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_ClickClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem getElem?_some {α : Type} (l : List α) (i : Nat) (h : i < l.length) :
    ∃ v, l[i]? = some v := by
  induction l generalizing i with
  | nil => simp at h
  | cons a as ih =>
    cases i with
    | zero => exact ⟨a, by simp⟩
    | succ j =>
      have h' : j < as.length := by
        simp only [List.length_cons] at h
        omega
      exact ih j h'

private theorem zip_getElem? {α β : Type} : ∀ (xs : List α) (ys : List β) (i : Nat) {x y},
    xs[i]? = some x → ys[i]? = some y → (xs.zip ys)[i]? = some (x, y) := by
  intro xs
  induction xs with
  | nil => intro ys i x y hx _; cases hx
  | cons a as ih =>
    intro ys i x y hx hy
    cases ys with
    | nil => cases hy
    | cons b bs =>
      cases i with
      | zero =>
        simp only [List.getElem?_cons_zero] at hx hy
        have eax := Option.some.inj hx
        have eby := Option.some.inj hy
        subst eax; subst eby
        simp
      | succ j =>
        simp only [List.getElem?_cons_succ] at hx hy ⊢
        exact ih bs j hx hy

theorem sound : ClickClickHouse.Goal := by
  intro a b c w i hab hbc hi
  intro anchors
  have hlenbc : (b.zip c).length = a.length := by
    have hmin : min b.length c.length = b.length := by omega
    rw [List.length_zip, hmin, hab]
  have hanch : anchors.length = a.length := by
    dsimp only [anchors]
    rw [List.length_map, List.length_zip, hlenbc]
    omega
  have hi' : i < anchors.length := by omega
  have key : ∀ col ∈ [a, b, c], col.length = a.length →
      (∀ d ∈ (col.zip anchors).map (fun (v, k) => zig (v - k)), d < 2 ^ w) →
      (anchors[i]?.getD 0)
          + unzig (takeAt (pack w ((col.zip anchors).map (fun (v, k) => zig (v - k)))) w i)
        = col[i]?.getD 0 := by
    intro col hcol hlen hres
    have hicol : i < col.length := by omega
    obtain ⟨v, hv⟩ := getElem?_some col i hicol
    obtain ⟨k, hk⟩ := getElem?_some anchors i hi'
    have hzip : (col.zip anchors)[i]? = some (v, k) := zip_getElem? _ _ _ hv hk
    have hri : ((col.zip anchors).map (fun (t : Int × Int) => zig (t.1 - t.2)))[i]?
        = some (zig (v - k)) := by
      rw [List.getElem?_map, hzip]
      simp
    have ht : takeAt (pack w ((col.zip anchors).map (fun (t : Int × Int) =>
        zig (t.1 - t.2)))) w i = zig (v - k) :=
      Common.takeAt_pack w _ hres i _ hri
    have hgoal : (k : Int) + unzig (takeAt (pack w ((col.zip anchors).map
        (fun (t : Int × Int) => zig (t.1 - t.2)))) w i) = v := by
      rw [ht, TinylambOpt.ResearchProofs.Compression.Wire.sound_SignedInverse]
      omega
    simpa [hv, hk] using hgoal
  intro col hcol residual hres
  have hc : col = a ∨ col = b ∨ col = c := by simpa using hcol
  rcases hc with h | h | h
  · exact key col hcol (by rw [h]) hres
  · exact key col hcol (by rw [h]; exact hab.symm) hres
  · exact key col hcol (by rw [h]; exact (hbc.symm.trans hab.symm)) hres

end TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_ClickClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_ClickClickHouse.sound
