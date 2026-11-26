import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# UnionOfRanksWithTies

The union of the two qualifying rank sets retains every requested row, including ties at
rank ten. The executor can defer loading details until the union has been selected.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.UnionOfRanksWithTies
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sorted_lt_of_lt_count : ∀ (L : List Nat), L.Pairwise (fun a b : Nat => a ≤ b) →
    ∀ (x i : Nat) (hilen : i < L.length), i < L.countP (fun y => y < x) → L[i] < x := by
  intro L
  induction L with
  | nil => intros; simp at *
  | cons a t ih =>
    intro hs x i hilen hi
    have hs' : (∀ b ∈ t, a ≤ b) ∧ t.Pairwise (fun a b : Nat => a ≤ b) := by simpa using hs
    by_cases hax : a < x
    · cases i with
      | zero => simpa [hax]
      | succ j =>
        have hjlen : j < t.length := by simpa using hilen
        simp [List.countP_cons, hax] at hi
        simpa using ih hs'.2 x j hjlen hi
    · have htz : t.countP (fun y => y < x) = 0 := by
        apply List.countP_eq_zero.mpr
        intro b hb hlt
        have hbx : b < x := by simpa using hlt
        exact hax (Nat.lt_of_le_of_lt (hs'.1 b hb) hbx)
      cases i with
      | zero => simp [hax, htz] at hi
      | succ j => simp [hax, htz] at hi

theorem sorted_le_of_count_le : ∀ (L : List Nat), L.Pairwise (fun a b : Nat => a ≤ b) →
    ∀ (x i : Nat) (hi : L.countP (fun y => y < x) ≤ i) (hilen : i < L.length), x ≤ L[i] := by
  intro L
  induction L with
  | nil => intro hs x i hi hilen; exact False.elim (Nat.not_lt_zero i hilen)
  | cons a t ih =>
    intro hs x i hi hilen
    have hs' : (∀ b ∈ t, a ≤ b) ∧ t.Pairwise (fun a b : Nat => a ≤ b) := by simpa using hs
    by_cases hax : a < x
    · cases i with
      | zero => simp [List.countP_cons, hax] at hi
      | succ j =>
        simp [List.countP_cons, hax] at hi
        have hjlen : j < t.length := by simpa using hilen
        exact ih hs'.2 x j hi hjlen
    · have htz : t.countP (fun y => y < x) = 0 := by
        apply List.countP_eq_zero.mpr
        intro b hb hlt
        have hbx : b < x := by simpa using hlt
        exact hax (Nat.lt_of_le_of_lt (hs'.1 b hb) hbx)
      cases i with
      | zero => simpa using (Nat.le_of_not_gt hax)
      | succ j =>
        have hjlen : j < t.length := by simpa using hilen
        exact ih hs'.2 x j (by simp [htz]) hjlen

theorem sorted_within (L : List Nat) (hs : L.Pairwise (fun a b : Nat => a ≤ b)) (x : Nat) :
    ((L[9]?).all (fun v => x ≤ v)) = decide (L.countP (fun y => y < x) < 10) := by
  by_cases hlen : L.length ≤ 9
  · have hget : L[9]? = none := by
      cases h9 : L[9]? with
      | none => rfl
      | some v =>
        have : 9 < L.length := (List.getElem?_eq_some_iff.mp h9).1
        omega
    have hc : L.countP (fun y => y < x) < 10 := by
      have hle := List.countP_le_length (p := fun y : Nat => y < x) (l := L)
      omega
    simp [hget, hc]
  · have hlen' : 9 < L.length := by omega
    have hget : L[9]? = some L[9] := List.getElem?_eq_getElem hlen'
    by_cases hc : L.countP (fun y => y < x) < 10
    · have hxlev : x ≤ L[9] := sorted_le_of_count_le L hs x 9 (by omega) hlen'
      simp [hget, hc, hxlev]
    · have hvlt : L[9] < x := sorted_lt_of_lt_count L hs x 9 hlen' (by omega)
      have hnot : ¬ x ≤ L[9] := by omega
      simp [hget, hc, hnot]

theorem withinTen (keys : List Nat) (x : Nat) :
    (keys.mergeSort[9]?).all (fun v => x ≤ v) = decide (countWhere keys (fun y => y < x) < 10) := by
  let L := keys.mergeSort
  have hsBool0 := List.pairwise_mergeSort
    (le := fun a b : Nat => decide (a ≤ b))
    (by intro a b c hab hbc; simp at hab hbc ⊢; omega)
    (by intro a b; simp; omega) keys
  have hs : L.Pairwise (fun a b : Nat => a ≤ b) := by
    dsimp only [L]
    simpa using hsBool0
  have hperm : L.Perm keys := by
    dsimp only [L]
    exact List.mergeSort_perm keys (fun a b : Nat => decide (a ≤ b))
  have hckeys : countWhere keys (fun y => y < x) = keys.countP (fun y => y < x) := by
    rw [countWhere, ← List.countP_eq_length_filter]
  have hcperm : keys.countP (fun y => y < x) = L.countP (fun y => y < x) :=
    hperm.symm.countP_eq (fun y => y < x)
  change ((L[9]?).all (fun v => x ≤ v)) = decide (countWhere keys (fun y => y < x) < 10)
  rw [hckeys, hcperm]
  exact sorted_within L hs x

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.UnionOfRanksWithTies.Goal := by
  intro rows
  apply List.filter_congr
  intro r hr
  rcases r with ⟨x,y⟩
  dsimp only
  rw [withinTen (rows.map Prod.fst) x, withinTen (rows.map Prod.snd) y]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.UnionOfRanksWithTies

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.UnionOfRanksWithTies.sound
