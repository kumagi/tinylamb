import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LexSuccessor_ReverseCursor

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.LexSuccessor

private def lexLe (a b : Point) : Prop := a.x < b.x ∨ (a.x = b.x ∧ a.y ≤ b.y)
private def lexLt (a b : Point) : Prop := a.x < b.x ∨ (a.x = b.x ∧ a.y < b.y)

private theorem qualifies_iff (p v : Point) :
    shape.qualifies p v = true ↔ lexLt p v := by
  simp [shape, lexLt]

private theorem lexLt_of_lexLt_lexLe {p r s : Point} (hpr : lexLt p r) (hrs : lexLe r s) : lexLt p s := by
  unfold lexLt lexLe at *
  rcases hpr with hpr | ⟨hpx, hpy⟩ <;> rcases hrs with hrs | ⟨hrx, hry⟩ <;> omega

theorem sound_LexSuccessor_ReverseCursor : LexSuccessor.ReverseCursor.Goal := by
  intro p rs hpair
  induction rs with
  | nil => simp [scan]
  | cons r rs ih =>
      have hhead : ∀ s ∈ rs, lexLe r.1 s.1 := (List.pairwise_cons.mp hpair).1
      have hpair_rs : rs.Pairwise (fun a b => lexLe a.1 b.1) := (List.pairwise_cons.mp hpair).2
      by_cases hq : shape.qualifies p r.1 = true
      · have hall : ∀ s ∈ rs, shape.qualifies p s.1 = true := by
          intro s hs
          exact (qualifies_iff p s.1).2 (lexLt_of_lexLt_lexLe ((qualifies_iff p r.1).1 hq) (hhead s hs))
        have hfilter : rs.filter (fun s => shape.qualifies p s.1) = rs := by
          apply List.filter_eq_self.2
          intro s hs
          exact hall s hs
        simp [scan, hq, List.dropWhile, hfilter]
      · have hfalse : shape.qualifies p r.1 = false := by
          cases h : shape.qualifies p r.1 <;> simp_all
        simpa [scan, hq, hfalse] using ih hpair_rs

end TinylambOpt.ResearchProofs.Atlas.Goal_LexSuccessor_ReverseCursor
