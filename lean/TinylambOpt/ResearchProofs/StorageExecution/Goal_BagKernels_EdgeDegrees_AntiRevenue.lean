import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_AntiRevenue

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

-- Small-input sanity checks (the universally quantified goal is not Decidable).
#eval weighted bag ([(0, 1), (1, 1), (2, 2)] : List (Nat × Nat))
    (fun x => if bag.mass [(2, 3), (0, 9)] x == 0 then x else 0)
#eval ((bag.expand ([(0, 1), (1, 1), (2, 2)] : List (Nat × Nat))).filter
    (fun x => !(bag.expand [(2, 3), (0, 9)]).contains x)).sum
#eval weighted bag ([(0, 1), (1, 1), (2, 2)] : List (Nat × Nat))
    (fun x => if bag.mass [(0, 9)] x == 0 then x else 0)
#eval ((bag.expand ([(0, 1), (1, 1), (2, 2)] : List (Nat × Nat))).filter
    (fun x => !(bag.expand [(0, 9)]).contains x)).sum

private theorem sum_eq_of_perm {l₁ l₂ : List Nat} (p : l₁.Perm l₂) (f : Nat → Nat) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction p with
  | nil => rfl
  | cons x p ih => simpa using ih
  | swap x y l => simp only [List.map_cons, List.sum_cons]; omega
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem sum_eq_zero_of_pointwise {α : Type} (l : List α) (g : α → Nat)
    (h : ∀ x ∈ l, g x = 0) : (l.map g).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have h : motive l.length := Nat.strongRecOn l.length (fun n ih l hl => by
    cases l with
    | nil => simp [List.eraseDups_nil]
    | cons b bs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro hb
          rw [List.mem_eraseDups] at hb
          have hb' := (List.mem_filter.mp hb).2
          simp at hb'
        · apply ih (bs.filter (fun x => !x == b)).length
          · have hle := List.length_filter_le (fun x => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

private theorem sum_ite_eq_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (hn : l.Nodup) (a : α) (n : Nat) :
    (l.map (fun k => if k == a then n else 0)).sum = if a ∈ l then n else 0 := by
  induction l with
  | nil => simp
  | cons k ks ih =>
      have hk : k ∉ ks := (List.nodup_cons.mp hn).1
      have hks : ks.Nodup := (List.nodup_cons.mp hn).2
      have ih' := ih hks
      by_cases hka : k = a
      · subst a
        have hzero : (ks.map (fun k₁ : α => if k₁ == k then n else 0)).sum = 0 :=
          sum_eq_zero_of_pointwise ks (fun k₁ : α => if k₁ == k then n else 0) (by
            intro k₁ hk₁
            have hne : k₁ ≠ k := by
              intro heq
              apply hk
              simpa [heq] using hk₁
            simp [hne])
        simp [hzero]
      · have hneq : (k == a) = false := by simp [hka]
        have hne : a ≠ k := fun h => hka h.symm
        simpa [hneq, hne] using ih'

private theorem hist_perm (l : List Nat) :
    l.Perm (l.eraseDups.flatMap (fun k => List.replicate (l.count k) k)) := by
  apply List.perm_iff_count.mpr
  intro a
  symm
  rw [List.count_flatMap]
  calc
    (List.map (List.count a ∘ (fun k : Nat => List.replicate (l.count k) k)) l.eraseDups).sum
        = (l.eraseDups.map (fun k => if k == a then l.count k else 0)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro k hk
            simp [List.count_replicate]
    _ = (l.eraseDups.map (fun k => if k == a then l.count a else 0)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro k hk
            by_cases hka : k == a
            · have heq : k = a := by simpa using hka
              subst k
              simp
            · simp [hka]
    _ = if a ∈ l.eraseDups then l.count a else 0 :=
            sum_ite_eq_of_nodup l.eraseDups (nodup_eraseDups l) a (l.count a)
    _ = l.count a := by
            by_cases ha : a ∈ l.eraseDups
            · simp [ha]
            · have hna : a ∉ l := by simpa [List.mem_eraseDups] using ha
              simp [ha, List.count_eq_zero_of_not_mem hna]

private theorem sum_flatMap_replicate_map (d : List Nat) (c : Nat → Nat) (f : Nat → Nat) :
    ((d.flatMap (fun k => List.replicate (c k) k)).map f).sum =
      (d.map (fun k => c k * f k)).sum := by
  induction d with
  | nil => simp
  | cons k ks ih =>
      simp [List.map_replicate, List.sum_replicate_nat, ih]

private theorem sum_map_eq_eraseDups_count (l : List Nat) (f : Nat → Nat) :
    (l.map f).sum = (l.eraseDups.map (fun k => l.count k * f k)).sum := by
  rw [sum_eq_of_perm (hist_perm l) f]
  exact sum_flatMap_replicate_map l.eraseDups (fun k => l.count k) f

private theorem count_flatMap_pairs (es : List (Nat × Nat)) (k : Nat) :
    List.count k (es.flatMap (fun (a, b) => [a, b])) =
      List.count k (es.map Prod.fst) + List.count k (es.map Prod.snd) := by
  induction es with
  | nil => simp
  | cons e es ih =>
      obtain ⟨a, b⟩ := e
      change List.count k ([a, b] ++ es.flatMap (fun (a, b) => [a, b])) =
        List.count k (a :: es.map Prod.fst) + List.count k (b :: es.map Prod.snd)
      rw [List.count_append, List.count_cons, List.count_cons, ih]
      by_cases hak : a == k <;> by_cases hbk : b == k <;>
        simp [hak, hbk, List.count_cons] <;> omega

private theorem count_expand_eq_mass (es : List (Nat × Nat)) (k : Nat) :
    (bag.expand es).count k = bag.mass es k := by
  change List.count k (es.flatMap (fun (a, b) => [a, b])) = bag.mass es k
  simpa [bag] using count_flatMap_pairs es k

private theorem keys_count_eq_mass (es : List (Nat × Nat)) (k : Nat) :
    (bag.keys es).count k = bag.mass es k := by
  simp [bag, List.count_append]

private theorem keys_perm_expand (es : List (Nat × Nat)) :
    (bag.keys es).Perm (bag.expand es) := by
  apply List.perm_iff_count.mpr
  intro k
  rw [keys_count_eq_mass, ← count_expand_eq_mass]

private theorem weighted_eq_expand_sum (es : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag es f = ((bag.expand es).map f).sum := by
  unfold weighted
  calc
    ((bag.keys es).eraseDups.map (fun k => bag.mass es k * f k)).sum
        = ((bag.keys es).eraseDups.map (fun k => (bag.keys es).count k * f k)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro k hk
            rw [keys_count_eq_mass]
    _ = ((bag.keys es).map f).sum := (sum_map_eq_eraseDups_count (bag.keys es) f).symm
    _ = ((bag.expand es).map f).sum := sum_eq_of_perm (keys_perm_expand es) f

private theorem filter_sum_eq_map_ite (l : List Nat) (p : Nat → Bool) :
    (l.filter p).sum = (l.map (fun x => if p x then x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

theorem sound_AntiRevenue : EdgeDegrees.AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_expand_sum]
  have hbool : ∀ x : Nat, (bag.mass c x == 0) = (!(bag.expand c).contains x) := by
    intro x
    rw [← count_expand_eq_mass c x]
    by_cases hmem : x ∈ bag.expand c
    · have hcnt : (bag.expand c).count x ≠ 0 := by
        intro hz
        have hpos : 0 < (bag.expand c).count x := List.count_pos_iff.mpr hmem
        omega
      simp [hcnt, hmem]
    · have hcnt : (bag.expand c).count x = 0 := List.count_eq_zero_of_not_mem hmem
      simp [hcnt, hmem]
  rw [filter_sum_eq_map_ite]
  simp [hbool]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_AntiRevenue
