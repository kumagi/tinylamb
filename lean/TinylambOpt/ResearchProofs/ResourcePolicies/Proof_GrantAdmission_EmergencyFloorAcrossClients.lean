import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! With at most `maxClients` independently charged emergency buffers, each at
most `floor`, the simultaneous peak `buffers.sum` is bounded by
`maxClients * floor`. Induction on the client list peels one client per step;
a nonempty list forces `maxClients = m + 1`, the tail satisfies the bound with
`m`, and `Nat.add_mul` expands the successor product, which `omega` finishes. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_EmergencyFloorAcrossClients
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem sound : EmergencyFloorAcrossClients.Goal := by
  intro buffers
  induction buffers with
  | nil => intro floor maxClients _ _; simp
  | cons b rest ih =>
    intro floor maxClients hlen hall
    rw [List.length_cons] at hlen
    obtain ⟨m, rfl⟩ : ∃ m, maxClients = m + 1 := ⟨maxClients - 1, by omega⟩
    have hb : b ≤ floor := hall b List.mem_cons_self
    have hrest := ih floor m (by omega) (fun x hx => hall x (List.mem_cons_of_mem b hx))
    rw [List.sum_cons, Nat.add_mul, Nat.one_mul]
    omega

end TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_EmergencyFloorAcrossClients
