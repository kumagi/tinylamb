import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `AlphabeticMerge.OptimalTree` — `∀ ws, ws ≠ [] → (∃ t, weights t = ws ∧ work t = best
  ws) ∧ ∀ t, weights t = ws → best ws ≤ work t`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_OptimalTree

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

private theorem weights_ne_nil (t : Tree) : weights t ≠ [] := by
  induction t with
  | leaf w => simp [weights]
  | fork l r ihl ihr => simp [weights, ihl, ihr]

/-- With enough splitting fuel, `dp` never overestimates any real tree. -/
private theorem dp_le_work : ∀ (t : Tree) (f : Nat) (ws : List Nat),
    weights t = ws → ws.length ≤ f → dp f ws ≤ work t := by
  intro t
  induction t with
  | leaf w =>
      intro f ws hwt hle
      subst ws
      cases f <;> simp [dp, weights, work]
  | fork l r ihl ihr =>
      intro f ws hwt hle
      subst ws
      simp [weights, work] at hle ⊢
      have hln : 0 < (weights l).length := List.length_pos_iff.mpr (weights_ne_nil l)
      have hrn : 0 < (weights r).length := List.length_pos_iff.mpr (weights_ne_nil r)
      have hlen2 : 2 ≤ (weights l ++ weights r).length := by
        simp [List.length_append]
        omega
      have hf2 : 2 ≤ f := by omega
      cases f with
      | zero => omega
      | succ f' =>
          have hf'_l : (weights l).length ≤ f' := by omega
          have hf'_r : (weights r).length ≤ f' := by omega
          have hnot : ¬ (weights l).length + (weights r).length ≤ 1 := by omega
          rw [dp]
          simp [hnot]
          have h1le : 1 ≤ (weights l).length := by omega
          have ht : (weights l ++ weights r).take ((weights l).length - 1 + 1) = weights l := by
            rw [Nat.sub_add_cancel h1le]
            exact List.take_left
          have hd : (weights l ++ weights r).drop ((weights l).length - 1 + 1) = weights r := by
            rw [Nat.sub_add_cancel h1le]
            exact List.drop_left
          have hk_mem : dp f' (weights l) + dp f' (weights r) ∈
              (List.range ((weights l ++ weights r).length - 1)).map
                (fun i => dp f' ((weights l ++ weights r).take (i + 1)) +
                          dp f' ((weights l ++ weights r).drop (i + 1))) := by
            apply List.mem_map.mpr
            refine ⟨(weights l).length - 1, ?_, ?_⟩
            · rw [List.mem_range]
              simp [List.length_append] at hle ⊢
              omega
            · rw [ht, hd]
          have hmin :
              ((List.range ((weights l ++ weights r).length - 1)).map
                (fun i => dp f' ((weights l ++ weights r).take (i + 1)) +
                          dp f' ((weights l ++ weights r).drop (i + 1)))).min?.getD 0
                ≤ dp f' (weights l) + dp f' (weights r) :=
            List.min?_getD_le_of_mem hk_mem
          have hl_bound := ihl f' (weights l) rfl hf'_l
          have hr_bound := ihr f' (weights r) rfl hf'_r
          simp [List.length_append] at hmin
          omega

/-- With enough fuel the DP value is attained by an actual tree. -/
private theorem exists_optimal_tree_aux : ∀ n, ∀ (ws : List Nat), ws.length = n → ws ≠ [] →
    ∀ f, n ≤ f → ∃ t, weights t = ws ∧ work t = dp f ws := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ws hlen hne f hf
      by_cases hsmall : ws.length ≤ 1
      · rcases ws with _ | ⟨w, rest⟩
        · contradiction
        · cases rest with
          | nil =>
              refine ⟨Tree.leaf w, ?_, ?_⟩
              · simp [weights]
              · cases f <;> simp [dp, work]
          | cons w2 rest2 =>
              simp at hsmall
      · have hlen2 : 2 ≤ ws.length := by omega
        have hf2 : 2 ≤ f := by omega
        cases f with
        | zero => omega
        | succ f' =>
            let cands := (List.range (ws.length - 1)).map
              (fun i => dp f' (ws.take (i + 1)) + dp f' (ws.drop (i + 1)))
            have hcands_ne : cands ≠ [] := by
              dsimp [cands]
              rw [List.map_eq_nil_iff]
              rw [List.range_eq_nil]
              omega
            have hmin_some : ∃ m, cands.min? = some m := by
              have hs : cands.min?.isSome = true := List.isSome_min?_of_ne_nil hcands_ne
              exact Option.isSome_iff_exists.mp hs
            rcases hmin_some with ⟨m, hm⟩
            have hm_mem : m ∈ cands := List.min?_mem hm
            rcases List.mem_map.mp hm_mem with ⟨i, hi_range, hi_eq⟩
            have hi_lt : i < ws.length - 1 := List.mem_range.mp hi_range
            have hi1_le : i + 1 ≤ ws.length := by omega
            have htake_len : (ws.take (i + 1)).length = i + 1 := by
              simp [List.length_take, Nat.min_eq_left hi1_le]
            have hdrop_len : (ws.drop (i + 1)).length = ws.length - (i + 1) := by
              simp [List.length_drop]
            have htake_ne : ws.take (i + 1) ≠ [] := by
              rw [← List.length_pos_iff, htake_len]
              omega
            have hdrop_ne : ws.drop (i + 1) ≠ [] := by
              rw [← List.length_pos_iff, hdrop_len]
              omega
            have htake_length_lt : (ws.take (i + 1)).length < n := by
              rw [← hlen, htake_len]
              omega
            have hdrop_length_lt : (ws.drop (i + 1)).length < n := by
              rw [← hlen, hdrop_len]
              omega
            have hf'_take : (ws.take (i + 1)).length ≤ f' := by
              rw [← hlen] at hf
              omega
            have hf'_drop : (ws.drop (i + 1)).length ≤ f' := by
              rw [← hlen] at hf
              omega
            obtain ⟨tl, htlw, htlwork⟩ :=
              ih (ws.take (i + 1)).length htake_length_lt (ws.take (i + 1)) rfl htake_ne f' hf'_take
            obtain ⟨tr, htrw, htrwork⟩ :=
              ih (ws.drop (i + 1)).length hdrop_length_lt (ws.drop (i + 1)) rfl hdrop_ne f' hf'_drop
            refine ⟨Tree.fork tl tr, ?_, ?_⟩
            · simp [weights, htlw, htrw, List.take_append_drop]
            · have hsum : ws.sum = (ws.take (i + 1)).sum + (ws.drop (i + 1)).sum := by
                rw [← List.sum_append, List.take_append_drop]
              have hdp : dp (Nat.succ f') ws = ws.sum + m := by
                rw [dp]
                simp [hsmall]
                dsimp [cands] at hm
                rw [hm]
                rfl
              rw [hdp, ← hi_eq, hsum]
              simp [work, htlw, htrw, htlwork, htrwork]
              omega

theorem sound_OptimalTree : AlphabeticMerge.OptimalTree.Goal := by
  intro ws hne
  constructor
  · exact exists_optimal_tree_aux ws.length ws rfl hne ws.length (Nat.le_refl _)
  · intro t htw
    exact dp_le_work t ws.length ws htw (Nat.le_refl _)

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_OptimalTree
