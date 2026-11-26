import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `Exchanges.FrameScale` — `∀ cap xs n, 0 < cap → 0 < n → traffic (cap*n) xs ≤ traffic
  cap xs ∧ traffic cap xs ≤ n*traffic (cap*n) xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_FrameScale

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges

/-!
  Small exhaustive check (destinations 0..2, bytes 0..3, cap 1..4, n 1..4)
  confirms both inequalities hold.
-/
#eval
  let bks : List (List Bucket) := [[]] ++
    (List.range 3).flatMap (fun d =>
      (List.range 4).map (fun w => [(d, w)]))
  bks.all (fun xs =>
    (List.range 4).all (fun ci =>
      (List.range 4).all (fun ni =>
        let cap := ci + 1
        let n := ni + 1
        traffic (cap * n) xs ≤ traffic cap xs &&
          traffic cap xs ≤ n * traffic (cap * n) xs)))

private theorem packets_le_iff (cap v c : Nat) (hcap : 0 < cap) :
    packets cap v ≤ c ↔ v ≤ cap * c := by
  unfold packets
  rw [Nat.div_le_iff_le_mul_add_pred hcap]
  omega

private theorem packets_coarser (cap n v : Nat) (hcap : 0 < cap) (hn : 0 < n) :
    packets (cap * n) v ≤ packets cap v := by
  apply (packets_le_iff (cap * n) v (packets cap v) (Nat.mul_pos hcap hn)).2
  have hv : v ≤ cap * packets cap v :=
    (packets_le_iff cap v (packets cap v) hcap).1 (by omega)
  calc
    v ≤ cap * packets cap v := hv
    _ = (cap * packets cap v) * 1 := by omega
    _ ≤ (cap * packets cap v) * n := Nat.mul_le_mul_left _ hn
    _ = (cap * n) * packets cap v := by ac_rfl

private theorem packets_finer (cap n v : Nat) (hcap : 0 < cap) (hn : 0 < n) :
    packets cap v ≤ n * packets (cap * n) v := by
  apply (packets_le_iff cap v (n * packets (cap * n) v) hcap).2
  have hv : v ≤ (cap * n) * packets (cap * n) v :=
    (packets_le_iff (cap * n) v (packets (cap * n) v) (Nat.mul_pos hcap hn)).1
      (by omega)
  simpa [Nat.mul_assoc] using hv

private theorem sum_map_le_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      exact Nat.add_le_add (h x (by simp)) (ih (fun y hy => h y (by simp [hy])))

private theorem sum_map_mul_nat {α : Type} (l : List α) (c : Nat) (f : α → Nat) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [Nat.mul_add, ih]

theorem sound_Exchanges_FrameScale : Exchanges.FrameScale.Goal := by
  intro cap xs n hcap hn
  let ids := (xs.map Prod.fst).eraseDups
  constructor
  · unfold traffic
    apply sum_map_le_nat
    intro d hd
    exact packets_coarser cap n (volume xs d) hcap hn
  · unfold traffic
    change (ids.map (fun d => packets cap (volume xs d))).sum ≤
      n * (ids.map (fun d => packets (cap * n) (volume xs d))).sum
    rw [← sum_map_mul_nat]
    apply sum_map_le_nat
    intro d hd
    exact packets_finer cap n (volume xs d) hcap hn

end TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_FrameScale
