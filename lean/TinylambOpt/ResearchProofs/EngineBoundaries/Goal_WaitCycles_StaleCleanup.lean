import TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

/-!
Concurrency control must account for the modeled wait dependencies of SQL transactions.

Proved

- `StaleCleanup` — A late cleanup cannot cancel a newer wait from the same transaction
  with a fresh ticket.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_StaleCleanup

open TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

private theorem mem_zip_fst {α β} {xs : List α} {ys : List β} {a : α} {b : β}
    (h : (a,b) ∈ xs.zip ys) : a ∈ xs := by
  induction xs generalizing ys with
  | nil => simp at h
  | cons x xs ih =>
      cases ys with
      | nil => simp at h
      | cons y ys =>
          simp at h
          rcases h with h | h
          · rcases h with ⟨_, _⟩
            subst a
            simp
          · exact List.mem_cons_of_mem x (ih h)

private theorem mem_zip_snd {α β} {xs : List α} {ys : List β} {a : α} {b : β}
    (h : (a,b) ∈ xs.zip ys) : b ∈ ys := by
  induction ys generalizing xs with
  | nil => simp at h
  | cons y ys ih =>
      cases xs with
      | nil => simp at h
      | cons x xs =>
          simp at h
          rcases h with h | h
          · rcases h with ⟨_, _⟩
            subst b
            simp
          · exact List.mem_cons_of_mem y (ih h)

private theorem rotate_perm (x : Nat) (rest : List Nat) : (rest ++ [x]).Perm (x :: rest) := by
  induction rest with
  | nil => simp
  | cons y ys ih =>
      have h1 : (y :: (ys ++ [x])).Perm (y :: (x :: ys)) := ih.cons y
      have h2 : (y :: (x :: ys)).Perm (x :: (y :: ys)) := List.Perm.swap x y ys
      exact h1.trans h2

private theorem mem_zip_of_mem_left {α β} {xs : List α} {ys : List β} {w : α}
    (hlen : xs.length = ys.length) (hw : w ∈ xs) : ∃ b ∈ ys, (w,b) ∈ xs.zip ys := by
  induction xs generalizing ys with
  | nil => simp at hw
  | cons x xs ih =>
      cases ys with
      | nil => simp at hlen
      | cons y ys =>
          simp at hw
          rcases hw with rfl | hw
          · exact ⟨y, by simp, by simp⟩
          · have hlen' : xs.length = ys.length := by simpa using hlen
            rcases ih hlen' hw with ⟨b, hb, hpair⟩
            exact ⟨b, by simp [hb], by simp [hpair]⟩

private theorem chain_succ (g : Graph) (xs : List Nat) (w : Nat)
    (hchain : chain g xs = true) (hw : w ∈ xs) :
    ∃ b ∈ xs, (edge g w).any (fun e => e.holder == b) = true := by
  unfold chain at hchain
  have hzip := (List.all_eq_true.mp hchain)
  have hlen : xs.length = (xs.drop 1 ++ xs.take 1).length := by
    cases xs with
    | nil => simp
    | cons x xs => simp
  rcases mem_zip_of_mem_left hlen hw with ⟨b, hbrot, hpair⟩
  have hperm : (xs.drop 1 ++ xs.take 1).Perm xs := by
    cases xs with
    | nil => simp
    | cons x xs => simpa using rotate_perm x xs
  have hb : b ∈ xs := hperm.mem_iff.mp hbrot
  have hbtrue := hzip (w,b) hpair
  exact ⟨b, hb, by simpa using hbtrue⟩

theorem sound : StaleCleanup.Goal := by
  intro g e old hne
  unfold install clearTicket edge
  simp
  exact Or.inl (fun h : e.ticket = old => hne h.symm)

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_StaleCleanup
