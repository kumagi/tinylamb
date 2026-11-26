import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Product.FallbackNecessary` — A rejected deletion has indistinguishable old summaries
  requiring different results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_FallbackNecessary

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem prod_const_map (l : List Nat) (c : Nat) : (l.map (fun _ => c)).prod = c ^ l.length := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih, Nat.pow_succ, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc]

theorem sound : Product.FallbackNecessary.Goal := by
  intro xs x hx hd
  have hlen : 1 < xs.length := by
    by_cases h1 : xs.length = 1
    · have hxs : xs = [x] := by
        cases xs with
        | nil => simp at h1
        | cons y ys =>
            simp at h1
            subst ys
            have hxy : x = y := by simpa using hx
            subst y
            rfl
      subst xs
      simp [dropOne] at hd
    · have hp : 0 < xs.length := List.length_pos_of_mem hx
      omega
  have hxzero : x = 0 := by
    by_cases hz : x == 0
    · simpa using hz
    · have hxne : x ≠ 0 := by intro h; subst x; simp at hz
      have hlenb : (xs.length == 1) = false := by
        cases hb : xs.length == 1
        · rfl
        · have hn : xs.length = 1 := by simpa using hb
          omega
      simp [dropOne, Product.model, hlenb, hxne] at hd
  subst x
  let l := xs.erase 0
  have hllen : l.length = xs.length - 1 := by dsimp [l]; rw [List.length_erase_of_mem hx]
  have hlnonempty : l ≠ [] := by
    intro h
    have : l.length = 0 := by simp [h]
    omega
  let c := if l.prod = 1 then 2 else 1
  let ys := 0 :: l.map (fun _ => c)
  refine ⟨ys, ?_, ?_, ?_, ?_⟩
  · simp [ys]
  · simp [ys, hllen]
    omega
  · have hne_xs : xs ≠ [] := by intro h; subst xs; simp at hx
    have hprod_xs0 : xs.prod = 0 := by
      have hp := prod_eq_mul_prod_erase (xs := xs) (x := 0) hx
      rw [hp]
      simp
    have hvxs : value Product.model xs = some 0 := by
      simpa [value, Product.model, hne_xs, List.prod_eq_foldl] using congrArg some hprod_xs0
    have hne_ys : ys ≠ [] := by simp [ys]
    have hprod_ys0 : ys.foldl (fun a b => a * b) 1 = 0 := by
      simp only [ys, List.foldl]
      rw [foldl_mul_acc]
      simp
    have hvys : value Product.model ys = some 0 := by
      simp [value, Product.model, hne_ys, hprod_ys0]
    rw [hvxs, hvys]
  · have hne_lmap : l.map (fun _ => c) ≠ [] := by simp [hlnonempty]
    have hval_l : value Product.model l = some (l.prod) := by
      have : l ≠ [] := hlnonempty
      simp [value, Product.model, this, List.prod_eq_foldl]
    have hval_map : value Product.model (l.map (fun _ => c)) = some (c ^ l.length) := by
      have hm := prod_const_map l c
      have hf : (l.map (fun _ => c)).foldl (fun a b => a * b) 1 = c ^ l.length := by
        simpa [List.prod_eq_foldl] using hm
      have his := isEmpty_false_of_ne_nil hne_lmap
      simp [value, Product.model, his, hf]
    have he : ys.erase 0 = l.map (fun _ => c) := by simp [ys]
    have hel : xs.erase 0 = l := rfl
    have hcne : c ^ l.length ≠ l.prod := by
      by_cases hp1 : l.prod = 1
      · have hc_eq : c = 2 := by simp [c, hp1]
        rw [hc_eq, hp1]
        have hpos : 0 < l.length := List.length_pos_iff.mpr hlnonempty
        have htwo : 2 ≤ 2 ^ l.length := Nat.le_self_pow (Nat.ne_of_gt hpos) 2
        omega
      · have hc_eq : c = 1 := by simp [c, hp1]
        rw [hc_eq]
        simpa [eq_comm] using hp1
    rw [he, hel, hval_l, hval_map]
    simp [hcne]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_FallbackNecessary
