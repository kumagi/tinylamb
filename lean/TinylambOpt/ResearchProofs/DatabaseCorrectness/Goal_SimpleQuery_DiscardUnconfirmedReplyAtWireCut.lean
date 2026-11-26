import TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

/-!
After a durability failure the withheld reply is dropped while bytes already
accepted by the socket stay on the wire. The earlier pending byte count given
by the frame model is exactly the remainder of the old reply past the sent
cursor, so the visible prefix is unchanged.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_DiscardUnconfirmedReplyAtWireCut

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow (Frames remaining)
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

theorem sound : DiscardUnconfirmedReplyAtWireCut.Goal := by
  intro old hidden n hn
  have hbuf : n ≤ ((old ++ hidden).flatten).length := by
    simp only [List.flatten_append, List.length_append]
    omega
  have hmin : min n ((old ++ hidden).flatten).length = n := Nat.min_eq_left hbuf
  have hearlier : (remaining n old).flatten.length = old.flatten.length - n := by
    rw [remaining_flatten]
    simp
  have hlen : ((old ++ hidden).flatten).length - n =
      (old.flatten.length - n) + hidden.flatten.length := by
    simp only [List.flatten_append, List.length_append]
    omega
  have hm : min (((old ++ hidden).flatten).length - n) ((remaining n old).flatten.length) =
      old.flatten.length - n := by
    rw [hearlier, hlen]
    omega
  simp only [TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery.discard, hmin, hm]
  have hsum : n + (old.flatten.length - n) = old.flatten.length := by omega
  rw [hsum]
  simp [List.flatten_append, List.take_append_of_le_length (Nat.le_refl old.flatten.length)]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_DiscardUnconfirmedReplyAtWireCut
