import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum
import TinylambOpt.ResearchProofs.CloudBI.Common

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_QuarterlyOutliersFromPresentSlots

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Projecting four unique quarter slots preserves the nullable average state:
absent quarters contribute no witness at all, while present quarters contribute
exactly the non-null values of the source rows. The proof groups source rows by
key and shows the grouped values are a permutation of the projected values. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.CloudBI.Helpers

/-- A permutation of Int lists preserves their sum. -/
theorem perm_sum_int {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons x p ih => simpa using ih
  | swap x y xs => simp [Int.add_comm, Int.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

/-- Mapping then filtering non-null values is the same as filtering while mapping. -/
theorem filterMap_id_map {α β : Type} (f : α → Option β) :
    ∀ xs : List α, (xs.map f).filterMap id = xs.filterMap f
  | [] => rfl
  | a :: xs => by
      cases h : f a <;> simp [h, filterMap_id_map f xs]

/-- Filtering with the constantly true predicate is the identity. -/
theorem filter_true_eq_self {α : Type} : ∀ l : List α, l.filter (fun _ => true) = l
  | [] => rfl
  | a :: l => by
      rw [List.filter_cons_of_pos (p := fun _ : α => true) rfl]
      rw [filter_true_eq_self l]

/-- Congruence for `flatMap` pointwise on the elements of the key list. -/
theorem flatMap_congr_left {α β : Type} (f g : α → List β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.flatMap_cons, List.flatMap_cons, h a (by simp)]
      have hih := ih (fun x hx => h x (by simp [hx]))
      rw [hih]

/-- With distinct keys, each key bucket has at most one row. -/
theorem forKey_length_le_one {α β : Type} [BEq α] [LawfulBEq α]
    (rows : List (α × β)) (h : (rows.map Prod.fst).Nodup) (k : α) :
    (forKey rows k).length ≤ 1 := by
  induction rows with
  | nil => simp [forKey]
  | cons r rs ih =>
      cases h with
      | cons hhead htail =>
          have ih' := ih htail
          by_cases hk : r.1 == k
          · have hnot : ∀ x ∈ rs, ¬ x.1 == k := by
              intro x hx hxk
              have hxEq : x.1 = k := beq_iff_eq.mp hxk
              have hrEq : r.1 = k := beq_iff_eq.mp hk
              exact hhead x.1 (List.mem_map.mpr ⟨x, hx, rfl⟩) (hrEq.trans hxEq.symm)
            have hnil : rs.filter (fun x => x.1 == k) = [] := by
              apply List.eq_nil_iff_forall_not_mem.mpr
              intro x hx
              exact hnot x (List.mem_filter.mp hx).1 (List.mem_filter.mp hx).2
            simp [forKey, hk, hnil]
          · have hfilt : (r :: rs).filter (fun x => x.1 == k) = rs.filter (fun x => x.1 == k) := by
              simp [hk]
            simpa [forKey, hfilt] using ih'

/-- For a bucket of length at most one, the head witness has the same non-null
values as the bucket itself. -/
theorem head_join_filterMap {xs : List (Option Int)} (h : xs.length ≤ 1) :
    [((xs.head?).join)].filterMap id = xs.filterMap id := by
  cases xs with
  | nil => rfl
  | cons x xs =>
      cases xs with
      | nil => cases x <;> rfl
      | cons y ys => simp at h

/-- Filtering a key list through an Option witness is the same as flat-mapping
the singleton witness lists. -/
theorem filterMap_eq_flatMap_singleton {α β : Type} (ks : List α) (f : α → Option β) :
    ks.filterMap f = ks.flatMap (fun k => [f k].filterMap id) := by
  induction ks with
  | nil => rfl
  | cons k ks ih =>
      rw [List.filterMap_cons, List.flatMap_cons]
      cases h : f k <;> simp [h, ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.QuarterlyOutliersFromPresentSlots.Goal := by
  intro quarters hnod
  dsimp
  let ks := (quarters.map Prod.fst).eraseDups
  let f : Fin 4 → Option Int := fun k => ((forKey quarters k).head?).join
  let present := (List.finRange 4).filter (fun k => k ∈ ks)
  let absent := (List.finRange 4).filter (fun k => k ∉ ks)
  have hpart : (List.finRange 4).Perm (present ++ absent) := by
    dsimp [present, absent]
    exact (filter_not_partition (fun k : Fin 4 => k ∈ ks) (List.finRange 4)).symm
  have habsNil : absent.filterMap f = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro v hv
    rcases List.mem_filterMap.mp hv with ⟨k, hk, hvk⟩
    have hknot : k ∉ ks := by
      simpa using (List.mem_filter.mp hk).2
    have hnil : forKey quarters k = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro v hv'
      rcases List.mem_map.mp hv' with ⟨r, hr, hrval⟩
      have hrkey : r.1 = k := beq_iff_eq.mp (List.mem_filter.mp hr).2
      have hmemmap : k ∈ quarters.map Prod.fst :=
        List.mem_map.mpr ⟨r, (List.mem_filter.mp hr).1, hrkey⟩
      have hknot' : k ∉ quarters.map Prod.fst := by simpa [ks] using hknot
      exact hknot' hmemmap
    dsimp [f] at hvk
    simp [hnil] at hvk
  have hslotsPresent : ((List.finRange 4).filterMap f).Perm (present.filterMap f) := by
    have hp := hpart.filterMap f
    rw [List.filterMap_append, habsNil, List.append_nil] at hp
    exact hp
  have hpresentPerm : present.Perm ks := by
    apply (List.perm_ext_iff_of_nodup
      (List.Sublist.nodup (List.filter_sublist (p := fun k : Fin 4 => k ∈ ks)) (List.nodup_finRange 4))
      (TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping.nodup_eraseDups (quarters.map Prod.fst))).2
    intro k
    constructor
    · intro hk
      simpa [ks] using (List.mem_filter.mp hk).2
    · intro hk
      have hkks : k ∈ ks := by simpa [ks] using hk
      exact List.mem_filter.mpr ⟨List.mem_finRange k, by simpa using hkks⟩
  have hslotsKs : ((List.finRange 4).filterMap f).Perm (ks.filterMap f) :=
    hslotsPresent.trans (hpresentPerm.filterMap f)
  have hgroup : ((ks.flatMap (forKey quarters)).filterMap id).Perm (ks.filterMap f) := by
    rw [List.filterMap_flatMap]
    apply List.Perm.of_eq
    rw [filterMap_eq_flatMap_singleton]
    apply flatMap_congr_left
    intro k hk
    exact (head_join_filterMap (forKey_length_le_one quarters hnod k)).symm
  have hvalues : (quarters.filterMap Prod.snd).Perm ((ks.flatMap (forKey quarters)).filterMap id) := by
    have hp := TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping.forKey_filter_perm quarters (fun _ : Fin 4 => true)
    simpa only [filter_true_eq_self, filterMap_id_map, ks] using hp.filterMap id
  have hperm : (quarters.filterMap Prod.snd).Perm ((List.finRange 4).filterMap f) :=
    (hvalues.trans hgroup).trans hslotsKs.symm
  exact Prod.ext (perm_sum_int hperm.symm) hperm.length_eq.symm
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_QuarterlyOutliersFromPresentSlots
