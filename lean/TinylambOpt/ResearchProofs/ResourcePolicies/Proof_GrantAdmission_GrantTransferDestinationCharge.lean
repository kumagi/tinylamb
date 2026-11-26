import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Moving an owner's grants from one pool to another preserves the total
process occupancy (each ticket keeps its byte count), while the destination
pool's charge grows exactly by the moved bytes — so the destination cap test
against `cap` is equivalent to charging the old destination occupancy plus the
transfer size. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_GrantTransferDestinationCharge
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

/-- A byte-preserving rewrite of every ticket leaves the live byte total
unchanged. -/
private theorem liveBytes_map_id (f : Ticket → Ticket) (hf : ∀ t, (f t).bytes = t.bytes) :
    ∀ ts : List Ticket, liveBytes (ts.map f) = liveBytes ts := by
  intro ts
  induction ts with
  | nil => rfl
  | cons a t ih =>
    dsimp only [liveBytes] at ih ⊢
    simp only [List.map_cons, List.sum_cons, hf, ih]

/-- The destination pool after the transfer holds exactly its original bytes
plus the moved bytes (a moved ticket cannot have been in the destination
already because the pools differ). -/
private theorem liveBytes_filter_transfer (fromPool toPool owner : Nat) (hne : fromPool ≠ toPool) :
    ∀ ts : List Ticket,
      liveBytes (List.filter (fun t => t.pool == toPool)
        (List.map (fun t =>
          if t.owner == owner && t.pool == fromPool then { t with pool := toPool } else t) ts))
        = liveBytes (ts.filter (fun t => t.pool == toPool))
          + liveBytes (ts.filter (fun t => t.owner == owner && t.pool == fromPool)) := by
  intro ts
  induction ts with
  | nil => rfl
  | cons a t ih =>
    dsimp only [liveBytes] at ih ⊢
    cases hq : (a.owner == owner && a.pool == fromPool) with
    | true =>
      -- the head moves: it joins the destination, and it was not there before
      have hpf : (a.pool == fromPool) = true := by
        cases hA : (a.owner == owner) <;> cases hB : (a.pool == fromPool) <;>
          simp [hA, hB] at hq ⊢
      have hmove : ((fun u : Ticket => u.pool == toPool)
          (if a.owner == owner && a.pool == fromPool then {a with pool := toPool} else a)) = true := by
        rw [if_pos hq]
        simp
      have hkeep : ¬ ((fun u : Ticket => u.pool == toPool) a = true) := by
        intro hh
        have h1 : a.pool = toPool := by simpa [beq_iff_eq] using hh
        have h2 : a.pool = fromPool := by simpa [beq_iff_eq] using hpf
        exact hne (h2.symm.trans h1)
      have hba : Ticket.bytes (if a.owner == owner && a.pool == fromPool
          then {a with pool := toPool} else a) = a.bytes := by
        rw [if_pos hq]
      simp only [List.map_cons,
        List.filter_cons_of_pos (p := fun u : Ticket => u.pool == toPool) hmove,
        List.filter_cons_of_neg (p := fun u : Ticket => u.pool == toPool) hkeep,
        List.filter_cons_of_pos (p := fun t : Ticket => t.owner == owner && t.pool == fromPool) hq,
        List.sum_cons, hba]
      omega
    | false =>
      -- the head keeps its pool; the byte bookkeeping is unchanged
      have hfa : (if a.owner == owner && a.pool == fromPool
          then {a with pool := toPool} else a) = a := by
        rw [if_neg (fun hh => absurd hh (by simpa using hq))]
      rw [List.map_cons, hfa]
      cases hq2 : (a.pool == toPool) with
      | true =>
        simp only [List.filter_cons_of_pos (p := fun u : Ticket => u.pool == toPool) hq2,
          List.filter_cons_of_neg (p := fun t : Ticket => t.owner == owner && t.pool == fromPool)
            (by simpa using hq),
          List.map_cons, List.sum_cons]
        omega
      | false =>
        simp only [List.filter_cons_of_neg (p := fun u : Ticket => u.pool == toPool)
            (by simpa using hq2),
          List.filter_cons_of_neg (p := fun t : Ticket => t.owner == owner && t.pool == fromPool)
            (by simpa using hq)]
        exact ih

/-- Process occupancy is invariant under the pool rewrite, and the destination
cap test shifts by exactly the transferred bytes. -/
theorem sound : GrantTransferDestinationCharge.Goal := by
  intro ts fromPool toPool owner cap hne
  dsimp only
  have hf : ∀ t : Ticket, (if t.owner == owner && t.pool == fromPool
      then {t with pool := toPool} else t).bytes = t.bytes := by
    intro t
    split <;> rfl
  have hc := liveBytes_map_id (fun t => if t.owner == owner && t.pool == fromPool
    then {t with pool := toPool} else t) hf ts
  have hfil := liveBytes_filter_transfer fromPool toPool owner hne ts
  constructor
  · exact hc
  · rw [hfil]

end TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_GrantTransferDestinationCharge
