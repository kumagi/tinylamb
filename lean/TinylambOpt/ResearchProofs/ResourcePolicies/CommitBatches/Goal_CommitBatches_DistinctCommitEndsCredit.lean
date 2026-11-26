import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for DistinctCommitEndsCredit. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_DistinctCommitEndsCredit

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem eraseDups_nodup : ∀ l : List Nat, l.eraseDups.Nodup := by
  intro l
  induction hlen : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons x xs =>
          rw [List.eraseDups_cons, List.nodup_cons]
          constructor
          · simp
          · have hle : (List.filter (fun b => !b == x) xs).length ≤ xs.length :=
              List.Sublist.length_le List.filter_sublist
            simp at hlen hle
            exact ih (List.filter (fun b => !b == x) xs).length (by omega)
              (List.filter (fun b => !b == x) xs) rfl

theorem partition_length (xs : List Nat) (x : Nat) (hxnot : x ∉ xs) :
    (xs.filter (fun y => y < x)).length + (xs.filter (fun y => x < y)).length = xs.length := by
  induction xs with
  | nil => simp
  | cons y ys ih =>
      have hxnot_ys : x ∉ ys := by intro h; exact hxnot (by simp [h])
      have ih' := ih hxnot_ys
      by_cases hylt : y < x
      · have hygt : ¬ x < y := by omega
        simp [hylt, hygt]
        omega
      · have hyneq : y ≠ x := by intro h; exact hxnot (by simp [h])
        have hygt : x < y := by omega
        simp [hylt, hygt]
        omega

theorem nodup_map_sub (l : List Nat) (x : Nat) (hnd : l.Nodup) (hgt : ∀ y ∈ l, x < y) :
    (l.map (fun y => y - (x+1))).Nodup := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        obtain ⟨z, hzmem, hz⟩ := List.mem_map.mp hmem
        have hzgt := hgt z (by simp [hzmem])
        have hygt := hgt y (by simp)
        have hzdecomp : z = (z - (x+1)) + (x+1) := by omega
        have hydecomp : y = (y - (x+1)) + (x+1) := by omega
        have hzy : z = y := by omega
        have hymem : y ∈ ys := by
          subst z
          exact hzmem
        exact (List.nodup_cons.mp hnd).1 (by simp [hymem])
      · exact ih (List.nodup_cons.mp hnd).2 (fun z hz => hgt z (by simp [hz]))

theorem nodup_lt_length : ∀ N, ∀ (l : List Nat), l.Nodup → (∀ x ∈ l, x < N) → l.length ≤ N := by
  intro N
  induction N using Nat.strongRecOn with
  | ind N ih =>
      intro l hnd hb
      cases l with
      | nil => simp
      | cons x xs =>
          have hxN : x < N := hb x (by simp)
          have hndxs : xs.Nodup := (List.nodup_cons.mp hnd).2
          have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
          have hbxs : ∀ y ∈ xs, y < N := by intro y hy; exact hb y (by simp [hy])
          let lo := xs.filter (fun y => y < x)
          let hi := xs.filter (fun y => x < y)
          have hndlo : lo.Nodup := List.Nodup.sublist List.filter_sublist hndxs
          have hndhi : hi.Nodup := List.Nodup.sublist List.filter_sublist hndxs
          have hblo : ∀ y ∈ lo, y < x := by intro y hy; simp [lo] at hy; exact hy.2
          have hhi_gt : ∀ y ∈ hi, x < y := by intro y hy; simp [hi] at hy; exact hy.2
          have hhi_lt : ∀ y ∈ hi, y < N := by
            intro y hy
            have hyx : y ∈ xs ∧ x < y := by simpa [hi] using hy
            exact hbxs y hyx.1
          have hlo_le : lo.length ≤ x := ih x hxN lo hndlo hblo
          have hmapnd : (hi.map (fun y => y - (x+1))).Nodup := nodup_map_sub hi x hndhi hhi_gt
          have hmapb : ∀ z ∈ hi.map (fun y => y - (x+1)), z < N - (x+1) := by
            intro z hz
            obtain ⟨y, hy, hyz⟩ := List.mem_map.mp hz
            have hylt := hhi_lt y hy
            have hygt := hhi_gt y hy
            omega
          have hhi_le : (hi.map (fun y => y - (x+1))).length ≤ N - (x+1) :=
            ih (N - (x+1)) (by omega) (hi.map (fun y => y - (x+1))) hmapnd hmapb
          have hpart : lo.length + hi.length = xs.length := by
            simpa [lo, hi] using partition_length xs x hxnot
          simp only [List.length_map] at hhi_le
          simp only [List.length_cons]
          omega


theorem sound : DistinctCommitEndsCredit.Goal := by
  intro ws durable tip hb
  have hbd : ∀ x ∈ (ws.map Waiter.stop).eraseDups, durable < x ∧ x ≤ tip := by
    intro x hx
    obtain ⟨w, hw, hwx⟩ := List.mem_map.mp (List.mem_eraseDups.mp hx)
    have := hb w hw
    simpa [hwx] using this
  have hmapnd : ((ws.map Waiter.stop).eraseDups.map (fun x => x - (durable+1))).Nodup :=
    nodup_map_sub (ws.map Waiter.stop).eraseDups durable (eraseDups_nodup _) (fun y hy => (hbd y hy).1)
  have hmapb : ∀ z ∈ (ws.map Waiter.stop).eraseDups.map (fun x => x - (durable+1)),
      z < tip - durable := by
    intro z hz
    obtain ⟨y, hy, hyz⟩ := List.mem_map.mp hz
    have hylo := (hbd y hy).1
    have hyhi := (hbd y hy).2
    omega
  have hle := nodup_lt_length (tip-durable)
    ((ws.map Waiter.stop).eraseDups.map (fun x => x - (durable+1))) hmapnd hmapb
  simpa using hle

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_DistinctCommitEndsCredit
