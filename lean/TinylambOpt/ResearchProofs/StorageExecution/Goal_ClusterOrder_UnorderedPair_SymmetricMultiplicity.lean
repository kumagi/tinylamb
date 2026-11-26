import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `UnorderedPair.SymmetricMultiplicity` — `∀ rs a b, a<b → ((physical key rs).map
  key).count (a,b) = rs.count (a,b) + rs.count (b,a)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_UnorderedPair_SymmetricMultiplicity

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.UnorderedPair

private theorem perm_length (key : Key) (rs : List Row) : (perm key rs).length = rs.length := by
  unfold perm
  simp

private theorem perm_perm_range (key : Key) (rs : List Row) :
    (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1; let b := key y.1
    a.1 < b.1 || (a.1==b.1 && (a.2 < b.2 || (a.2==b.2 && x.2≤y.2)))
  have hsort : (rs.zipIdx.mergeSort cmp).Perm rs.zipIdx := List.mergeSort_perm _ _
  have hmap : ((rs.zipIdx.mergeSort cmp).map Prod.snd).Perm (rs.zipIdx.map Prod.snd) :=
    hsort.map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    simp [List.zipIdx_map_snd, ← List.range_eq_range']
  simpa [hz] using hmap

private theorem list_eq_map_range_getD {α : Type} (l : List α) (d : α) :
    l = (List.range l.length).map (fun i => l[i]?.getD d) := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp only [List.length_map, List.length_range] at h2
    simp [List.getElem?_eq_getElem h2]

private theorem physical_perm (key : Key) (rs : List Row) : (physical key rs).Perm rs := by
  unfold physical
  have hp : (perm key rs).Perm (List.range rs.length) := perm_perm_range key rs
  have h1 : ((perm key rs).map (fun i => rs[i]?.getD (0,0))).Perm
      ((List.range rs.length).map (fun i => rs[i]?.getD (0,0))) := hp.map _
  have h2 : ((List.range rs.length).map (fun i => rs[i]?.getD (0,0))) = rs := by
    symm
    exact list_eq_map_range_getD rs (0,0)
  simpa [h2] using h1

private theorem count_map_of_perm {α β : Type} [DecidableEq β] (f : α → β)
    {l1 l2 : List α} (hp : l1.Perm l2) (b : β) :
    (l1.map f).count b = (l2.map f).count b := by
  induction hp with
  | nil => rfl
  | cons x h ih => simp [List.count_cons, ih]
  | swap x y l => simp [List.count_cons]; omega
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem unordered_key_eq {x y a b : Nat} (hab : a < b) :
    UnorderedPair.key (x,y) = (a,b) ↔ (x,y) = (a,b) ∨ (x,y) = (b,a) := by
  constructor
  · intro h
    unfold UnorderedPair.key at h
    simp only [Prod.mk.injEq] at h
    by_cases hxy : x ≤ y
    · have hmin : min x y = x := Nat.min_eq_left hxy
      have hmax : max x y = y := Nat.max_eq_right hxy
      simp [hmin, hmax] at h
      exact Or.inl (Prod.ext h.1 h.2)
    · have hyx : y ≤ x := Nat.le_of_lt (Nat.lt_of_not_ge hxy)
      have hmin : min x y = y := Nat.min_eq_right hyx
      have hmax : max x y = x := Nat.max_eq_left hyx
      simp [hmin, hmax] at h
      exact Or.inr (Prod.ext h.2 h.1)
  · intro h
    rcases h with h | h
    · have hx : x = a := congrArg Prod.fst h
      have hy : y = b := congrArg Prod.snd h
      subst x
      subst y
      unfold UnorderedPair.key
      exact Prod.ext (Nat.min_eq_left (Nat.le_of_lt hab)) (Nat.max_eq_right (Nat.le_of_lt hab))
    · have hx : x = b := congrArg Prod.fst h
      have hy : y = a := congrArg Prod.snd h
      subst x
      subst y
      unfold UnorderedPair.key
      exact Prod.ext (Nat.min_eq_right (Nat.le_of_lt hab)) (Nat.max_eq_left (Nat.le_of_lt hab))

private theorem unordered_count_map {a b : Nat} (hab : a < b) (rs : List Row) :
    (rs.map UnorderedPair.key).count (a,b) = rs.count (a,b) + rs.count (b,a) := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨x,y⟩
      have hneab : a ≠ b := by omega
      have hneba : b ≠ a := by omega
      by_cases hk : UnorderedPair.key (x,y) = (a,b)
      · rcases (unordered_key_eq hab).mp hk with h | h
        · have hxa : x = a := congrArg Prod.fst h
          have hyb : y = b := congrArg Prod.snd h
          subst x
          subst y
          simp [hk, ih, hneab, hneba]
          omega
        · have hxb' : x = b := congrArg Prod.fst h
          have hya : y = a := congrArg Prod.snd h
          subst x
          subst y
          have hk' : UnorderedPair.key (b,a) = (a,b) := (unordered_key_eq hab).mpr (Or.inr rfl)
          simp [hk', ih, hneab, hneba]
          omega
      · have hxa : (x,y) ≠ (a,b) := by
          intro h
          exact hk ((unordered_key_eq hab).mpr (Or.inl h))
        have hxb : (x,y) ≠ (b,a) := by
          intro h
          exact hk ((unordered_key_eq hab).mpr (Or.inr h))
        simp [hk, hxa, hxb, ih]

theorem sound_SymmetricMultiplicity : UnorderedPair.SymmetricMultiplicity.Goal := by
  intro rs a b hab
  have hcount : ((physical UnorderedPair.key rs).map UnorderedPair.key).count (a,b) =
      (rs.map UnorderedPair.key).count (a,b) :=
    count_map_of_perm UnorderedPair.key (physical_perm UnorderedPair.key rs) (a,b)
  rw [hcount]
  exact unordered_count_map hab rs

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_UnorderedPair_SymmetricMultiplicity
