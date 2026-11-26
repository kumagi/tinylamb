import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

/-!
A query can use a synopsis to avoid inspecting some input only when the claimed bounds
hold for the underlying data.

Proved

- `Samples.ShardSampling` — `∀ (a b : List Int) k, (sums (a++b) k).Perm ((List.range
  (k+1)).flatMap (fun i => (sums a i).flatMap (fun x => (sums b (k-i)).map (x+·))))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Samples_ShardSampling

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples

/-! ### Generic permutation helpers -/

private theorem perm_append_middle {α : Type} (a b c : List α) :
    (a ++ b ++ c).Perm (a ++ c ++ b) := by
  simpa only [List.append_assoc] using
    (List.perm_append_comm (l₁ := b) (l₂ := c)).append_left a

private theorem perm_swap_middle {α : Type} (a b c d : List α) :
    (a ++ b ++ c ++ d).Perm (a ++ c ++ b ++ d) := by
  exact (perm_append_middle a b c).append_right d

private theorem perm_flatMap_fun {α β : Type} {l : List α} (f g : α → List β)
    (h : ∀ a ∈ l, (f a).Perm (g a)) : (l.flatMap f).Perm (l.flatMap g) := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.flatMap_cons]
      have ha := h a (by simp)
      have ht : ∀ z ∈ l, (f z).Perm (g z) := by intro z hz; exact h z (by simp [hz])
      exact (ha.append_right (l.flatMap f)).trans ((ih ht).append_left (g a))

private theorem flatMap_append_parts_perm {α β γ : Type} (l : List α) (f g : α → List β)
    (h : α → β → List γ) :
    (l.flatMap (fun a => (f a ++ g a).flatMap (h a))).Perm
      ((l.flatMap fun a => (f a).flatMap (h a)) ++
        (l.flatMap fun a => (g a).flatMap (h a))) := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.flatMap_cons, List.flatMap_append]
      have ih' : (l.flatMap (fun a => (f a).flatMap (h a) ++ (g a).flatMap (h a))).Perm
          ((l.flatMap fun a => (f a).flatMap (h a)) ++
            (l.flatMap fun a => (g a).flatMap (h a))) := by
        simpa only [List.flatMap_append] using ih
      have h1 : ((f a).flatMap (h a) ++ (g a).flatMap (h a) ++
            l.flatMap (fun a => (f a).flatMap (h a) ++ (g a).flatMap (h a))).Perm
            ((f a).flatMap (h a) ++ (g a).flatMap (h a) ++
              ((l.flatMap fun a => (f a).flatMap (h a)) ++
                (l.flatMap fun a => (g a).flatMap (h a)))) := by
        simpa only [List.append_assoc] using ih'.append_left
          ((f a).flatMap (h a) ++ (g a).flatMap (h a))
      have h2 : ((f a).flatMap (h a) ++ (g a).flatMap (h a) ++
              ((l.flatMap fun a => (f a).flatMap (h a)) ++
                (l.flatMap fun a => (g a).flatMap (h a)))).Perm
            ((f a).flatMap (h a) ++ (l.flatMap fun a => (f a).flatMap (h a)) ++
              (g a).flatMap (h a) ++
                (l.flatMap fun a => (g a).flatMap (h a))) := by
        simpa only [List.append_assoc] using
          perm_swap_middle ((f a).flatMap (h a)) ((g a).flatMap (h a))
            (l.flatMap fun a => (f a).flatMap (h a))
            (l.flatMap fun a => (g a).flatMap (h a))
      simpa only [List.append_assoc] using h1.trans h2

private theorem flatMap_const_nil {α β : Type} (l : List α) :
    l.flatMap (fun _ => ([] : List β)) = [] := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

/-! ### Samples-specific helpers -/

private theorem sums_cons (x : Int) (xs : List Int) (k : Nat) :
    sums (x :: xs) (k + 1) = sums xs (k + 1) ++ (sums xs k).map (x + ·) := by
  simp [sums, samples, List.map_append, List.map_map, Function.comp_def]

private theorem perm_prefix_sums (xs bs : List Int) (x : Int) :
    ((xs.map (x + ·)).flatMap fun v => bs.map (v + ·)).Perm
      ((xs.flatMap fun s => bs.map (s + ·)).map (x + ·)) := by
  induction xs with
  | nil => simp
  | cons s ss ih =>
      simp only [List.map_cons, List.flatMap_cons, List.map_append, List.map_map]
      have hhead : (bs.map fun t => (x + s) + t) = bs.map fun t => x + (s + t) := by
        apply List.map_congr_left
        intro t ht
        grind
      rw [hhead]
      exact (List.Perm.refl _).append ih

/-! ### Main decomposition -/

private theorem sums_append_perm (a b : List Int) : ∀ (k : Nat),
    (sums (a ++ b) k).Perm ((List.range (k + 1)).flatMap (fun i =>
      (sums a i).flatMap (fun x => (sums b (k - i)).map (x + ·)))) := by
  induction a with
  | nil =>
      intro k
      cases k with
      | zero => simp [sums, samples]
      | succ k =>
          have hr : List.range (k + 1 + 1) = 0 :: List.map Nat.succ (List.range (k + 1)) :=
            List.range_succ_eq_map
          rw [hr]
          simp only [List.flatMap_cons, List.flatMap_map]
          have hz : sums ([] : List Int) 0 = [0] := rfl
          have hs : ∀ i : Nat, sums ([] : List Int) (i + 1) = [] := by
            intro i
            rfl
          simp [hz, hs]
          rw [flatMap_const_nil]
          simp
  | cons x xs ih =>
      intro k
      cases k with
      | zero => simp [sums, samples]
      | succ k =>
          change (sums (x :: (xs ++ b)) (k + 1)).Perm
            ((List.range (k + 1 + 1)).flatMap (fun i =>
              (sums (x :: xs) i).flatMap (fun y =>
                (sums b (k + 1 - i)).map (y + ·))))
          rw [sums_cons x (xs ++ b) k]
          have hr : List.range (k + 1 + 1) = 0 :: List.map Nat.succ (List.range (k + 1)) :=
            List.range_succ_eq_map
          rw [hr]
          simp only [List.flatMap_cons, List.flatMap_map]
          have hzero : (sums (x :: xs) 0).flatMap
              (fun v => (sums b (k + 1 - 0)).map (v + ·)) = sums b (k + 1) := by
            simp [sums, samples]
          rw [hzero]
          have hsplit : ((List.range (k + 1)).flatMap (fun i =>
              (sums (x :: xs) (i + 1)).flatMap
                (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))).Perm
              (((List.range (k + 1)).flatMap (fun i =>
                (sums xs (i + 1)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))) ++
              ((List.range (k + 1)).flatMap (fun i =>
                ((sums xs i).map (x + ·)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·))))) := by
            simpa only [sums_cons] using
              flatMap_append_parts_perm (List.range (k + 1))
                (fun i => sums xs (i + 1)) (fun i => (sums xs i).map (x + ·))
                (fun (i : Nat) (v : Int) =>
                  (sums b ((k : Nat) + 1 - (i + 1))).map (fun t => v + t))
          have hrange : (List.range (k + 1 + 1)).flatMap (fun i =>
              (sums xs i).flatMap
                (fun v => (sums b (k + 1 - i)).map (v + ·))) =
              sums b (k + 1) ++ (List.range (k + 1)).flatMap (fun i =>
                (sums xs (i + 1)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·))) := by
            rw [List.range_succ_eq_map, List.flatMap_cons, List.flatMap_map]
            simp [sums, samples, Nat.sub_zero]
          have hp1 : (sums b (k + 1) ++ (List.range (k + 1)).flatMap (fun i =>
              (sums xs (i + 1)).flatMap
                (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))).Perm
              (sums (xs ++ b) (k + 1)) := by
            have hp := (ih (k + 1)).symm
            rw [hrange] at hp
            exact hp
          have hsub : ∀ i : Nat, k + 1 - (i + 1) = k - i := by
            intro i
            omega
          have hmap : ((List.range (k + 1)).flatMap (fun i =>
              ((sums xs i).flatMap fun s =>
                (sums b (k + 1 - (i + 1))).map (s + ·)).map (x + ·))).Perm
              ((sums (xs ++ b) k).map (x + ·)) := by
            rw [← List.map_flatMap]
            simpa only [hsub] using (ih k).symm.map (x + ·)
          have hlocal : ((List.range (k + 1)).flatMap (fun i =>
              ((sums xs i).map (x + ·)).flatMap
                (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))).Perm
              ((List.range (k + 1)).flatMap (fun i =>
                ((sums xs i).flatMap fun s =>
                  (sums b (k + 1 - (i + 1))).map (s + ·)).map (x + ·))) := by
            apply perm_flatMap_fun (l := List.range (k + 1))
            intro i hi
            exact perm_prefix_sums (sums xs i) (sums b (k + 1 - (i + 1))) x
          have hp2 : ((List.range (k + 1)).flatMap (fun i =>
              ((sums xs i).map (x + ·)).flatMap
                (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))).Perm
              ((sums (xs ++ b) k).map (x + ·)) :=
            hlocal.trans hmap
          have hstep2 : ((sums b (k + 1) ++ (List.range (k + 1)).flatMap (fun i =>
              (sums xs (i + 1)).flatMap
                (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))) ++
              ((List.range (k + 1)).flatMap (fun i =>
                ((sums xs i).map (x + ·)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·))))).Perm
              (sums b (k + 1) ++ (List.range (k + 1)).flatMap (fun i =>
                (sums (x :: xs) (i + 1)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))) := by
            simpa only [List.append_assoc] using
              (hsplit.append_left (sums b (k + 1))).symm
          exact (hp1.symm.append hp2.symm).trans hstep2

theorem sound_Samples_ShardSampling : Samples.ShardSampling.Goal := by
  intro a b k
  exact sums_append_perm a b k

end TinylambOpt.ResearchProofs.Atlas.Goal_Samples_ShardSampling
