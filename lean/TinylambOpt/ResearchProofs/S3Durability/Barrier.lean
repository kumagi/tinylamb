import TinylambOpt.ResearchGoals.S3Durability.Barrier

/-!
Proofs for the reply barrier goals. The target is a maximum over the objects a
statement read of per-object maxima over raised commits; every visible commit
that touched such an object is one of the terms. This lets a SQL reply observe
absent rows through an index without exposing a commit that a crash could
still lose.
-/

namespace TinylambOpt.ResearchProofs.S3Durability.Barrier

open TinylambOpt.ResearchGoals.S3Durability.Barrier

/-- A left fold of `max` never goes below its initial value. -/
theorem init_le_foldl_max {α : Type} (f : α → Nat) :
    ∀ (l : List α) (init : Nat), init ≤ l.foldl (fun m y => max m (f y)) init
  | [], _ => Nat.le_refl _
  | y :: l, init =>
    Nat.le_trans (Nat.le_max_left init (f y)) (init_le_foldl_max f l (max init (f y)))

/-- Every term of the fold is below its result. -/
theorem le_foldl_max {α : Type} (f : α → Nat) :
    ∀ (l : List α) (init : Nat) (x : α), x ∈ l → f x ≤ l.foldl (fun m y => max m (f y)) init
  | y :: l, init, x, h => by
    cases h with
    | head =>
      exact Nat.le_trans (Nat.le_max_right init (f y)) (init_le_foldl_max f l _)
    | tail _ h' => exact le_foldl_max f l (max init (f y)) x h'

/-- The fold stays below any bound of its initial value and all its terms. -/
theorem foldl_max_le {α : Type} (f : α → Nat) (B : Nat) :
    ∀ (l : List α) (init : Nat), init ≤ B → (∀ y ∈ l, f y ≤ B) →
      l.foldl (fun m y => max m (f y)) init ≤ B
  | [], _, hi, _ => hi
  | y :: l, init, hi, hy =>
    foldl_max_le f B l (max init (f y))
      (Nat.max_le.mpr ⟨hi, hy y (List.mem_cons_self ..)⟩)
      (fun z hz => hy z (List.mem_cons_of_mem y hz))

theorem lsn_le_modLsn (raised : List Commit) (c : Commit) (o : Nat)
    (hc : c ∈ raised) (ho : o ∈ c.objs) : c.lsn ≤ modLsn raised o := by
  have := le_foldl_max (fun c => if o ∈ c.objs then c.lsn else 0) raised 0 c hc
  simpa [modLsn, ho] using this

theorem modLsn_le_target (raised : List Commit) (read : List Nat) (o : Nat)
    (ho : o ∈ read) : modLsn raised o ≤ target raised read :=
  le_foldl_max (modLsn raised) read 0 o ho

theorem mod_lsn_complete : ModLsnComplete.Goal := by
  intro raised vis read hraised c hc o ho hobj
  exact Nat.le_trans (lsn_le_modLsn raised c o (hraised c hc) hobj)
    (modLsn_le_target raised read o ho)

theorem reply_reveals_only_durable : ReplyRevealsOnlyDurable.Goal := by
  intro raised vis read durable hraised hdur c hc o ho hobj
  exact Nat.le_trans (mod_lsn_complete raised vis read hraised c hc o ho hobj) hdur

theorem modLsn_le_maxLsn (raised : List Commit) (o : Nat) :
    modLsn raised o ≤ maxLsn raised := by
  apply foldl_max_le _ _ raised 0 (Nat.zero_le _)
  intro c hc
  have hc' : c.lsn ≤ maxLsn raised := le_foldl_max (fun c => c.lsn) raised 0 c hc
  by_cases ho : o ∈ c.objs
  · simpa [ho] using hc'
  · simp [ho]

theorem target_within_published : TargetWithinPublished.Goal := by
  intro raised read
  exact foldl_max_le _ _ read 0 (Nat.zero_le _) (fun o _ => modLsn_le_maxLsn raised o)

end TinylambOpt.ResearchProofs.S3Durability.Barrier
