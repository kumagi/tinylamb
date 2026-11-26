import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q11
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q11.Goal := by
  intro a b c d scale hb hd hs hdiv1 hdiv2
  let q1 : Int := a * scale / b
  let q2 : Int := c * scale / d
  have h1 : q1 * b = a * scale := by
    dsimp [q1]
    exact Int.ediv_mul_cancel hdiv1
  have h2 : q2 * d = c * scale := by
    dsimp [q2]
    exact Int.ediv_mul_cancel hdiv2
  constructor
  · intro h
    have hqb : q2 * b < q1 * b := Int.mul_lt_mul_of_pos_right h hb
    have hqbd : (q2 * b) * d < (q1 * b) * d := Int.mul_lt_mul_of_pos_right hqb hd
    have hleft : (q2 * b) * d = (c * b) * scale := by
      rw [show (q2 * b) * d = (q2 * d) * b by ac_rfl, h2]
      ac_rfl
    have hright : (q1 * b) * d = (a * d) * scale := by
      rw [h1]
      ac_rfl
    rw [hleft, hright] at hqbd
    exact (Int.mul_lt_mul_right hs).mp hqbd
  · intro h
    have hscl : (c * b) * scale < (a * d) * scale := Int.mul_lt_mul_of_pos_right h hs
    have hleft : (c * b) * scale = (q2 * b) * d := by
      rw [show (q2 * b) * d = (q2 * d) * b by ac_rfl, h2]
      ac_rfl
    have hright : (a * d) * scale = (q1 * b) * d := by
      rw [h1]
      ac_rfl
    rw [hleft, hright] at hscl
    have hqb : q2 * b < q1 * b := (Int.mul_lt_mul_right hd).mp hscl
    exact (Int.mul_lt_mul_right hb).mp hqb
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q11
