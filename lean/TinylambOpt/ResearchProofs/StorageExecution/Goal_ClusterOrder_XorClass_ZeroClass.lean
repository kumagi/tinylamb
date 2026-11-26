import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `XorClass.ZeroClass` — `∀ rs, ((physical key rs).takeWhile (fun p => (key
  p).1==0)).length = rs.countP (fun (x,y) => x==y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_XorClass_ZeroClass

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.XorClass

private theorem xor_eq_zero {a b : Nat} (h : a ^^^ b = 0) : a = b := by
  have hh := congrArg (fun x => x ^^^ b) h
  simpa [Nat.xor_assoc] using hh

private theorem beq_xor_zero (x y : Nat) : ((x ^^^ y) == 0) = (x == y) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact beq_iff_eq.mpr (xor_eq_zero (beq_iff_eq.mp h))
  · intro h
    have hxy : x = y := beq_iff_eq.mp h
    subst y
    simp

private theorem point_eq (p : Row) : (p.1 == p.2) = ((key p).1 == 0) := by
  rcases p with ⟨x, y⟩
  change (x == y) = ((x ^^^ y) == 0)
  exact (beq_xor_zero x y).symm

private theorem beq_zero_lt_one (n : Nat) : (n == 0) = decide (n < 1) := by
  apply Bool.eq_iff_iff.mpr
  simp [Nat.lt_one_iff]

private theorem lex_trans (a b c p q r i j k : Nat)
    (h1 : a < b || (a == b && (p < q || (p == q && i ≤ j))))
    (h2 : b < c || (b == c && (q < r || (q == r && j ≤ k)))) :
    a < c || (a == c && (p < r || (p == r && i ≤ k))) := by
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢
  rcases h1 with hab | ⟨hab, hrest1⟩
  · rcases h2 with hbc | ⟨hbc, hrest2⟩
    · exact Or.inl (by omega)
    · subst c
      exact Or.inl hab
  · rcases h2 with hbc | ⟨hbc, hrest2⟩
    · subst b
      exact Or.inl hbc
    · subst c
      subst b
      right
      constructor
      · rfl
      · rcases hrest1 with hpq | ⟨hpq, hij⟩
        · rcases hrest2 with hqr | ⟨hqr, hjk⟩
          · exact Or.inl (by omega)
          · subst r
            exact Or.inl hpq
        · subst q
          rcases hrest2 with hqr | ⟨hqr, hjk⟩
          · exact Or.inl (by omega)
          · subst r
            exact Or.inr ⟨rfl, by omega⟩

private theorem lex_total (a b p q i j : Nat) :
    (a < b || (a == b && (p < q || (p == q && i ≤ j)))) ||
      (b < a || (b == a && (q < p || (q == p && j ≤ i)))) = true := by
  simp only [Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]
  by_cases hab : a < b
  · exact Or.inl (Or.inl hab)
  · have hba_le : b ≤ a := Nat.le_of_not_gt hab
    by_cases heq : a = b
    · subst b
      by_cases hpq : p < q
      · exact Or.inl (Or.inr ⟨rfl, Or.inl hpq⟩)
      · have hqp_le : q ≤ p := Nat.le_of_not_gt hpq
        by_cases hpqeq : p = q
        · subst q
          by_cases hij : i ≤ j
          · exact Or.inl (Or.inr ⟨rfl, Or.inr ⟨rfl, hij⟩⟩)
          · have hji : j ≤ i := by omega
            exact Or.inr (Or.inr ⟨rfl, Or.inr ⟨rfl, hji⟩⟩)
        · have hqplt : q < p := Nat.lt_of_le_of_ne hqp_le (Ne.symm hpqeq)
          exact Or.inr (Or.inr ⟨rfl, Or.inl hqplt⟩)
    · have hbalt : b < a := Nat.lt_of_le_of_ne hba_le (Ne.symm heq)
      exact Or.inr (Or.inl hbalt)

private def tagLe (x y : Row × Nat) : Bool :=
  let a := key x.1
  let b := key y.1
  a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2)))

private theorem tagLe_trans (x y z : Row × Nat) :
    tagLe x y = true → tagLe y z = true → tagLe x z = true := by
  intro h1 h2
  dsimp [tagLe] at h1 h2 ⊢
  exact lex_trans (key x.1).1 (key y.1).1 (key z.1).1
    (key x.1).2 (key y.1).2 (key z.1).2 x.2 y.2 z.2 h1 h2

private theorem tagLe_total (x y : Row × Nat) : (tagLe x y || tagLe y x) = true := by
  dsimp [tagLe]
  exact lex_total (key x.1).1 (key y.1).1 (key x.1).2 (key y.1).2 x.2 y.2

private theorem rowAt_of_mem_zipIdx {rs : List Row} {x : Row × Nat} (hx : x ∈ rs.zipIdx) :
    rs[x.2]?.getD (0, 0) = x.1 := by
  have hget : rs[x.2]? = some x.1 := List.mem_zipIdx_iff_getElem?.mp hx
  simp [hget]

private theorem list_eq_map_range_getD {α : Type} (l : List α) (d : α) :
    l = (List.range l.length).map (fun i => l[i]?.getD d) := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp only [List.length_map, List.length_range] at h2
    simp [List.getElem?_eq_getElem h2]

private theorem range'_eq_range (n : Nat) : List.range' 0 n = List.range n := by
  rw [List.range'_eq_map_range]
  simp

private theorem perm_indices (rs : List Row) : (perm key rs).Perm (List.range rs.length) := by
  have hperm0 :
      (rs.zipIdx.mergeSort (fun (x : Row × Nat) (y : Row × Nat) =>
        let a := key x.1
        let b := key y.1
        a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2))))).Perm rs.zipIdx :=
    List.mergeSort_perm _ _
  have hperm1 := List.Perm.map Prod.snd hperm0
  unfold perm
  simpa [List.zipIdx_map_snd, range'_eq_range] using hperm1

private theorem physical_perm (rs : List Row) : (physical key rs).Perm rs := by
  unfold physical
  have hmap := (perm_indices rs).map (fun i => rs[i]?.getD (0, 0))
  rw [(list_eq_map_range_getD rs (0, 0)).symm] at hmap
  exact hmap

private theorem sorted_merged_xor (rs : List Row) :
    ((rs.zipIdx.mergeSort tagLe).map (fun p : Row × Nat => (key (rs[p.2]?.getD (0, 0))).1)).Pairwise
      (fun a b : Nat => a ≤ b) := by
  rw [List.pairwise_map]
  have hs := List.pairwise_mergeSort tagLe_trans tagLe_total rs.zipIdx
  apply hs.imp_of_mem
  intro a b ha hb hab
  have ha' : a ∈ rs.zipIdx := by simpa using ha
  have hb' : b ∈ rs.zipIdx := by simpa using hb
  have hrowa := rowAt_of_mem_zipIdx ha'
  have hrowb := rowAt_of_mem_zipIdx hb'
  simp only [tagLe, key, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at hab
  rw [hrowa, hrowb]
  simp only [key]
  rcases hab with hlt | ⟨heq, hrest⟩
  · have hlt' : a.1.fst ^^^ a.1.snd < b.1.fst ^^^ b.1.snd := decide_eq_true_eq.mp hlt
    exact Nat.le_of_lt hlt'
  · exact Nat.le_of_eq heq

private theorem sorted_xor (rs : List Row) :
    ((physical key rs).map (fun p : Row => (key p).1)).Pairwise (fun a b : Nat => a ≤ b) := by
  unfold physical perm
  change (((rs.zipIdx.mergeSort tagLe).map Prod.snd).map (fun i => rs[i]?.getD (0, 0))).map
      (fun p : Row => (key p).1) |>.Pairwise (fun a b : Nat => a ≤ b)
  have heq :
      (((rs.zipIdx.mergeSort tagLe).map Prod.snd).map (fun i => rs[i]?.getD (0, 0))).map
          (fun p : Row => (key p).1) =
        ((rs.zipIdx.mergeSort tagLe).map (fun p : Row × Nat => (key (rs[p.2]?.getD (0, 0))).1)) := by
    simp only [List.map_map]
    congr 1
  rw [heq]
  exact sorted_merged_xor rs

private theorem takeWhile_count_xor (l : List Row)
    (hs : (l.map (fun p : Row => (key p).1)).Pairwise (fun a b : Nat => a ≤ b)) (t : Nat) :
    (l.takeWhile (fun p : Row => decide ((key p).1 < t))).length =
      l.countP (fun p : Row => decide ((key p).1 < t)) := by
  induction l with
  | nil => simp
  | cons a xs ih =>
      have hhead : ∀ x ∈ xs.map (fun p : Row => (key p).1), (key a).1 ≤ x :=
        (List.pairwise_cons.mp hs).1
      have hstail : (xs.map (fun p : Row => (key p).1)).Pairwise (fun a b : Nat => a ≤ b) :=
        (List.pairwise_cons.mp hs).2
      have ihtail := ih hstail
      by_cases ha : (key a).1 < t
      · simp [List.takeWhile_cons, ha, List.countP_cons, ihtail]
      · have hcnt : xs.countP (fun p : Row => decide ((key p).1 < t)) = 0 := by
          rw [List.countP_eq_zero]
          intro b hb hbt
          have hbmem : (key b).1 ∈ xs.map (fun p : Row => (key p).1) :=
            List.mem_map_of_mem hb
          have hle := hhead (key b).1 hbmem
          simp only [decide_eq_true_eq] at hbt
          omega
        simp [List.takeWhile_cons, ha, List.countP_cons, hcnt]

theorem sound_ZeroClass : ZeroClass.Goal := by
  intro rs
  dsimp only
  simp only [point_eq]
  rw [← (physical_perm rs).countP_eq (fun p : Row => (key p).1 == 0)]
  have htake := takeWhile_count_xor (physical key rs) (sorted_xor rs) 1
  simpa only [beq_zero_lt_one] using htake

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_XorClass_ZeroClass
