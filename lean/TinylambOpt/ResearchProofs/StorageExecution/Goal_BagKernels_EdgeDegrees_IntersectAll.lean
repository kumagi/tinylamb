import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `EdgeDegrees.IntersectAll` — Intersect two bags by the minimum multiplicity at each
  key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

/-! ## Generic list lemmas -/

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
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
          · have hle := List.length_filter_le (fun x : α => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, List.count_replicate, beq_iff_eq]

private theorem sum_eq_single_of_nodup (xs : List Nat) (hn : xs.Nodup) (k : Nat) (f : Nat → Nat) :
    (xs.map (fun x => if x = k then f x else 0)).sum = if k ∈ xs then f k else 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.map_cons, List.sum_cons]
      by_cases hxk : x = k
      · subst x
        have hnot : k ∉ xs := hn.1
        simpa [hnot] using (ih hn.2)
      · have hi := ih hn.2
        have hkx : k ≠ x := fun h => hxk h.symm
        by_cases hk : k ∈ xs
        · simp [hxk, hkx, hk, hi]
        · simp [hxk, hkx, hk, hi]

/-! ## EdgeDegrees bag facts -/

private theorem count_expand_eq_mass (es : List (Nat × Nat)) (k : Nat) :
    (bag.expand es).count k = bag.mass es k := by
  induction es with
  | nil => simp [bag]
  | cons e es ih =>
      rcases e with ⟨a, b⟩
      change List.count k (a :: b :: bag.expand es) =
        (List.count k (a :: es.map Prod.fst)) + (List.count k (b :: es.map Prod.snd))
      simp [List.count_cons, ih, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]
      simp [bag]

private theorem mem_keys_of_mass_pos (es : List (Nat × Nat)) (k : Nat)
    (h : 0 < bag.mass es k) : k ∈ bag.keys es := by
  have hmass : 0 < (es.map Prod.fst).count k + (es.map Prod.snd).count k := by
    simpa [bag] using h
  have hdisj : 0 < (es.map Prod.fst).count k ∨ 0 < (es.map Prod.snd).count k := by omega
  rw [show bag.keys es = es.map Prod.fst ++ es.map Prod.snd by rfl]
  cases hdisj with
  | inl hf => simp [List.count_pos_iff.mp hf]
  | inr hs => simp [List.count_pos_iff.mp hs]

private theorem nodup_live (es : List (Nat × Nat)) : (live bag es).Nodup := by
  unfold live
  exact List.Sublist.nodup List.filter_sublist (eraseDups_nodup (bag.keys es))

private theorem mem_live_of_mass_pos (es : List (Nat × Nat)) (k : Nat)
    (h : 0 < bag.mass es k) : k ∈ live bag es := by
  have hkeys : k ∈ bag.keys es := mem_keys_of_mass_pos es k h
  unfold live
  rw [List.mem_filter]
  constructor
  · rw [List.mem_eraseDups]
    exact hkeys
  · simp [h]

/-! ## Goal -/

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  rw [count_flatMap_replicate]
  rw [sum_eq_single_of_nodup (live bag a) (nodup_live a)]
  rw [count_expand_eq_mass, count_expand_eq_mass]
  by_cases hk : k ∈ live bag a
  · simp [hk]
  · have hm0 : bag.mass a k = 0 := by
      by_cases hz : bag.mass a k = 0
      · exact hz
      · exact False.elim (hk (mem_live_of_mass_pos a k (Nat.pos_of_ne_zero hz)))
    simp [hk, hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_IntersectAll
