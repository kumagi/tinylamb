import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `PowerBucket.WidthRunCount` — `∀ rs, (rle ((physical key rs).map (fun p => width
  p.1))).length = (rs.map (fun (p : Row) => width p.1)).eraseDups.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_PowerBucket_WidthRunCount

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.PowerBucket
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## Permutation helpers -/

private theorem perm_perm_range (key : Key) (rs : List Row) :
    (perm key rs).Perm (List.range rs.length) := by
  unfold perm
  let cmp := fun (x : Row × Nat) (y : Row × Nat) =>
    let a := key x.1
    let b := key y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2)))
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
  have h1 : ((perm key rs).map (fun i => rs[i]?.getD (0, 0))).Perm
      ((List.range rs.length).map (fun i => rs[i]?.getD (0, 0))) := hp.map _
  have h2 : ((List.range rs.length).map (fun i => rs[i]?.getD (0, 0))) = rs := by
    symm
    exact list_eq_map_range_getD rs (0, 0)
  simpa [h2] using h1

/-! ## RLE of a sorted list -/

private theorem rle_cons (y : Nat) (ys : List Nat) :
    rle (y :: ys) = match rle ys with
      | [] => [(y, 1)]
      | (q, k) :: qs => if y == q then (y, k + 1) :: qs else (y, 1) :: rle ys := by
  rfl

private theorem rle_cons_eq (y : Nat) (ys : List Nat) :
    ∃ k t, rle (y :: ys) = (y, k) :: t := by
  rw [rle_cons]
  cases h : rle ys with
  | nil =>
      exact ⟨1, [], by simp [h]⟩
  | cons q qs =>
      by_cases hyq : y == q.1
      · exact ⟨q.2 + 1, qs, by simp [rle_cons, h, hyq]⟩
      · exact ⟨1, q :: qs, by simp [rle_cons, h, hyq]⟩

private theorem filter_ne_eq_self {x : Nat} {l : List Nat} (h : x ∉ l) :
    l.filter (fun b => !b == x) = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hane : a ≠ x := by intro hax; exact h (by simp [hax])
      have has : x ∉ as := by intro hx; exact h (by simp [hx])
      simp [hane, ih has]

private theorem rle_sorted_length (l : List Nat) (hl : l.Pairwise (· ≤ ·)) :
    (rle l).length = l.eraseDups.length := by
  induction l with
  | nil => simp [rle]
  | cons x xs ih =>
    rw [List.pairwise_cons] at hl
    cases hxs : xs with
    | nil =>
      subst xs
      simp [rle, List.eraseDups_cons]
    | cons y ys =>
      subst xs
      obtain ⟨k, t, hrle⟩ := rle_cons_eq y ys
      have ih' := ih hl.2
      rw [hrle] at ih'
      rw [rle_cons, hrle]
      simp only
      by_cases hxy : x == y
      · have hyx : y = x := by
          have hxy' : x = y := by simpa using hxy
          exact hxy'.symm
        subst y
        simp [List.eraseDups_cons] at ih'
        simpa [hxy, List.eraseDups_cons] using ih'
      · have hxnot : x ∉ y :: ys := by
          intro hxmem
          simp only [List.mem_cons] at hxmem
          rcases hxmem with hxy_eq | hxys
          · exact hxy (by simp [hxy_eq])
          · have hxy_le : x ≤ y := hl.1 y (by simp)
            have hyx_le : y ≤ x := (List.pairwise_cons.mp hl.2).1 x hxys
            exact hxy (by simp [Nat.le_antisymm hxy_le hyx_le])
        have hfilt : (y :: ys).filter (fun b => !b == x) = y :: ys := filter_ne_eq_self hxnot
        simp [hxy, ih', List.eraseDups_cons, hfilt]

/-! ## EraseDups is invariant under permutation -/

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List Nat, l.length = n → l.eraseDups.Nodup
  have H : motive l.length := by
    refine Nat.strongRecOn (motive := motive) l.length ?_
    intro n ih
    intro l hlen
    cases l with
    | nil => simp
    | cons a as =>
        simp at hlen
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro ha
          have hm : a ∈ as.filter (fun b => !b == a) := List.mem_eraseDups.mp ha
          simp at hm
        · have hlt : (as.filter (fun b => !b == a)).length < n := by
            have hle := List.length_filter_le (fun b : Nat => !b == a) as
            omega
          exact ih _ hlt _ rfl
  exact H l rfl

private theorem perm_eraseDups_length {l₁ l₂ : List Nat} (p : l₁.Perm l₂) :
    l₁.eraseDups.length = l₂.eraseDups.length := by
  have hp : l₁.eraseDups.Perm l₂.eraseDups :=
    (List.perm_ext_iff_of_nodup (nodup_eraseDups l₁) (nodup_eraseDups l₂)).mpr (by
      intro a
      rw [List.mem_eraseDups, List.mem_eraseDups]
      exact p.mem_iff)
  exact hp.length_eq

theorem sound_WidthRunCount : PowerBucket.WidthRunCount.Goal := by
  intro rs
  let col := (physical PowerBucket.key rs).map (fun p => width p.1)
  have hperm : col.Perm (rs.map (fun (p : Row) => width p.1)) :=
    (physical_perm PowerBucket.key rs).map _
  let cmp : Row × Nat → Row × Nat → Bool := fun x y =>
    width x.1.1 < width y.1.1 ||
      (width x.1.1 == width y.1.1 &&
        (x.1.2 < y.1.2 || (x.1.2 == y.1.2 && x.2 ≤ y.2)))
  have htrans : ∀ x y z : Row × Nat, cmp x y = true → cmp y z = true → cmp x z = true := by
    intro x y z h1 h2
    simp only [cmp, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h1 h2 ⊢
    omega
  have htotal : ∀ x y : Row × Nat, (cmp x y || cmp y x) = true := by
    intro x y
    simp only [cmp, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]
    omega
  have hsort := List.pairwise_mergeSort htrans htotal rs.zipIdx
  have hsorted : col.Pairwise (· ≤ ·) := by
    dsimp [col]
    unfold physical perm
    change (((((rs.zipIdx.mergeSort cmp).map Prod.snd).map
      (fun i => rs[i]?.getD (0, 0))).map (fun p => width p.1)).Pairwise (fun a b => a ≤ b))
    rw [List.pairwise_map, List.pairwise_map, List.pairwise_map]
    refine List.Pairwise.imp_of_mem ?_ hsort
    intro a b ha hb hab
    have ha_mem : a ∈ rs.zipIdx := (List.mergeSort_perm rs.zipIdx cmp).mem_iff.mp ha
    have hb_mem : b ∈ rs.zipIdx := (List.mergeSort_perm rs.zipIdx cmp).mem_iff.mp hb
    have hrowa : rs[a.2]?.getD (0, 0) = a.1 := by
      have hg := List.mem_zipIdx_iff_getElem?.mp ha_mem
      simp [hg]
    have hrowb : rs[b.2]?.getD (0, 0) = b.1 := by
      have hg := List.mem_zipIdx_iff_getElem?.mp hb_mem
      simp [hg]
    rw [hrowa, hrowb]
    simp only [cmp, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at hab ⊢
    omega
  rw [rle_sorted_length col hsorted]
  exact perm_eraseDups_length hperm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_PowerBucket_WidthRunCount
