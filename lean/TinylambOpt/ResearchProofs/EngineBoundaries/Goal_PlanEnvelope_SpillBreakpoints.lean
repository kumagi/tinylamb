import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

/-!
A cached or specialized plan needs the modeled validity conditions before it can serve a
SQL invocation.

Proved

- `SpillBreakpoints` — Include both working-set breakpoints, clamped to the grant
  interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_SpillBreakpoints

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_add_rows (p : Plan) (lo n g : Nat) (hlo : lo ≤ n) :
    cost p n g = cost p lo g + p.perRow * (n - lo) := by
  unfold cost
  have hmul : p.perRow * lo + p.perRow * (n - lo) = p.perRow * n := by
    rw [← Nat.mul_add, Nat.add_sub_of_le hlo]
  omega

private theorem cheap_rows (p q : Plan) (l h n g : Nat) (hl : l ≤ n) (hn : n ≤ h)
    (cl : cheaper p q l g = true) (ch : cheaper p q h g = true) : cheaper p q n g := by
  unfold cheaper at cl ch ⊢
  have cl' : cost p l g ≤ cost q l g := by simpa using cl
  have ch' : cost p h g ≤ cost q h g := by simpa using ch
  have hpn := cost_add_rows p l n g hl
  have hqn := cost_add_rows q l n g hl
  have hph := cost_add_rows p l h g (Nat.le_trans hl hn)
  have hqh := cost_add_rows q l h g (Nat.le_trans hl hn)
  rw [hph, hqh] at ch'
  apply decide_eq_true_eq.mpr
  rw [hpn, hqn]
  by_cases hs : p.perRow ≤ q.perRow
  · exact Nat.add_le_add cl' (Nat.mul_le_mul_right (n - l) hs)
  · have hqp : q.perRow < p.perRow := Nat.lt_of_not_ge hs
    rcases Nat.exists_eq_add_of_le (Nat.le_of_lt hqp) with ⟨k, hk⟩
    have hkn : k * (n - l) ≤ k * (h - l) := Nat.mul_le_mul_left k (by omega)
    have hh' : cost p l g + k * (h - l) ≤ cost q l g := by
      rw [hk, Nat.add_mul] at ch'
      omega
    have hg' : cost p l g + k * (n - l) ≤ cost q l g := Nat.le_trans (Nat.add_le_add_left hkn _) hh'
    rw [hk, Nat.add_mul]
    omega

private theorem cost_spill_low (p : Plan) (n u g : Nat) (hug : u ≤ g) (hgw : g ≤ p.workingSet) :
    cost p n u = cost p n g + p.spillRate * (g - u) := by
  have huw : u ≤ p.workingSet := Nat.le_trans hug hgw
  have hsub : p.workingSet - u = (p.workingSet - g) + (g - u) := by omega
  unfold cost
  rw [hsub, Nat.add_mul, Nat.mul_comm (g - u) p.spillRate]
  omega

private theorem cost_spill_high (p : Plan) (n u g : Nat) (hpu : p.workingSet ≤ u) (hug : u ≤ g) :
    cost p n g = cost p n u := by
  have hpg : p.workingSet ≤ g := Nat.le_trans hpu hug
  unfold cost
  simp [Nat.sub_eq_zero_of_le hpu, Nat.sub_eq_zero_of_le hpg]

private theorem both_high (p q : Plan) (n u v g : Nat) (hu : u ≤ g) (_hv : g ≤ v)
    (hpu : p.workingSet ≤ u) (hqu : q.workingSet ≤ u)
    (cu : cheaper p q n u = true) : cheaper p q n g := by
  unfold cheaper at cu
  apply decide_eq_true_eq.mpr
  have hpg : cost p n g = cost p n u := cost_spill_high p n u g hpu hu
  have hqg : cost q n g = cost q n u := cost_spill_high q n u g hqu hu
  simpa [hpg, hqg] using cu

private theorem q_high_p_low (p q : Plan) (n u v g : Nat) (hu : u ≤ g) (hv : g ≤ v)
    (hqu : q.workingSet ≤ u) (hvp : v ≤ p.workingSet)
    (cu : cheaper p q n u = true) : cheaper p q n g := by
  unfold cheaper at cu
  apply decide_eq_true_eq.mpr
  have hqg : cost q n g = cost q n u := cost_spill_high q n u g hqu hu
  have hple : cost p n g ≤ cost p n u := by
    have hpu := cost_spill_low p n u g hu (Nat.le_trans hv hvp)
    rw [hpu]
    exact Nat.le_add_right _ _
  rw [hqg]
  exact Nat.le_trans hple (by simpa using cu)

private theorem p_high_q_low (p q : Plan) (n u v g : Nat) (hu : u ≤ g) (hv : g ≤ v)
    (hpu : p.workingSet ≤ u) (hvq : v ≤ q.workingSet)
    (cv : cheaper p q n v = true) : cheaper p q n g := by
  unfold cheaper at cv
  apply decide_eq_true_eq.mpr
  have hpg : cost p n g = cost p n v := (cost_spill_high p n g v (Nat.le_trans hpu hu) hv).symm
  have hqg := cost_spill_low q n g v hv hvq
  have hqle : cost q n v ≤ cost q n g := by rw [hqg]; exact Nat.le_add_right _ _
  rw [hpg]
  exact Nat.le_trans (by simpa using cv) hqle

private theorem both_low (p q : Plan) (n u v g : Nat) (hu : u ≤ g) (hv : g ≤ v)
    (hvp : v ≤ p.workingSet) (hvq : v ≤ q.workingSet)
    (cu : cheaper p q n u = true) (cv : cheaper p q n v = true) : cheaper p q n g := by
  unfold cheaper at cu cv
  apply decide_eq_true_eq.mpr
  by_cases hs : p.spillRate ≤ q.spillRate
  · have hpg := cost_spill_low p n g v hv hvp
    have hqg := cost_spill_low q n g v hv hvq
    have hmul : p.spillRate * (v - g) ≤ q.spillRate * (v - g) :=
      Nat.mul_le_mul_right (v - g) hs
    rw [hpg, hqg]
    exact Nat.add_le_add (by simpa using cv) hmul
  · have hsq : q.spillRate ≤ p.spillRate := Nat.le_of_not_ge hs
    have hpu := cost_spill_low p n u g hu (Nat.le_trans hv hvp)
    have hqu := cost_spill_low q n u g hu (Nat.le_trans hv hvq)
    have hmul : q.spillRate * (g - u) ≤ p.spillRate * (g - u) :=
      Nat.mul_le_mul_right (g - u) hsq
    have htmp : cost p n g + q.spillRate * (g - u) ≤
        cost q n g + q.spillRate * (g - u) := by
      apply Nat.le_trans (Nat.add_le_add_left hmul _)
      rw [hpu, hqu] at cu
      simpa using cu
    exact Nat.add_le_add_iff_right.mp htmp

private theorem clamp_le_of_ge (a b w : Nat) (haw : a ≤ w) :
    min b (max a w) ≤ w := by
  by_cases hwb : w ≤ b
  · have hmax : max a w = w := Nat.max_eq_right haw
    simp [hwb, hmax]
  · exact Nat.le_trans (Nat.min_le_left b (max a w)) (Nat.le_of_not_ge hwb)

private theorem le_clamp (a b w g : Nat) (hgb : g ≤ b) (hgw : g ≤ w) :
    g ≤ min b (max a w) := by
  exact Nat.le_min_of_le_of_le hgb (Nat.le_trans hgw (Nat.le_max_right a w))

private theorem le_clamp_of_w_le_b (a b w : Nat) (hwb : w ≤ b) :
    w ≤ min b (max a w) := by
  exact Nat.le_min_of_le_of_le hwb (Nat.le_max_right a w)

private theorem clamp_eq_max (a b w : Nat) (hab : a ≤ b) (hwb : w ≤ b) :
    min b (max a w) = max a w := by
  exact Nat.min_eq_right (Nat.max_le_of_le_of_le hab hwb)

theorem sound : SpillBreakpoints.Goal := by
  intro p q l h a b n g hln hnh hag hgb hpts
  have hab : a ≤ b := Nat.le_trans hag hgb
  let ca := min b (max a p.workingSet)
  let cb := min b (max a q.workingSet)
  have hall := List.all_eq_true.mp hpts
  have hlrow : (clamps a b [a, b, p.workingSet, q.workingSet]).all (cheaper p q l) = true := by
    exact hall l (by simp)
  have hhrow : (clamps a b [a, b, p.workingSet, q.workingSet]).all (cheaper p q h) = true := by
    exact hall h (by simp)
  have hmem_a : a ∈ clamps a b [a, b, p.workingSet, q.workingSet] := by simp [clamps, hab]
  have hmem_b : b ∈ clamps a b [a, b, p.workingSet, q.workingSet] := by simp [clamps, hab]
  have hmem_ca : ca ∈ clamps a b [a, b, p.workingSet, q.workingSet] := by simp [clamps, ca]
  have hmem_cb : cb ∈ clamps a b [a, b, p.workingSet, q.workingSet] := by simp [clamps, cb]
  have cheap_l_a := (List.all_eq_true.mp hlrow) a hmem_a
  have cheap_l_b := (List.all_eq_true.mp hlrow) b hmem_b
  have cheap_l_ca := (List.all_eq_true.mp hlrow) ca hmem_ca
  have cheap_l_cb := (List.all_eq_true.mp hlrow) cb hmem_cb
  have cheap_h_a := (List.all_eq_true.mp hhrow) a hmem_a
  have cheap_h_b := (List.all_eq_true.mp hhrow) b hmem_b
  have cheap_h_ca := (List.all_eq_true.mp hhrow) ca hmem_ca
  have cheap_h_cb := (List.all_eq_true.mp hhrow) cb hmem_cb
  have cheap_n_a := cheap_rows p q l h n a hln hnh cheap_l_a cheap_h_a
  have cheap_n_b := cheap_rows p q l h n b hln hnh cheap_l_b cheap_h_b
  have cheap_n_ca := cheap_rows p q l h n ca hln hnh cheap_l_ca cheap_h_ca
  have cheap_n_cb := cheap_rows p q l h n cb hln hnh cheap_l_cb cheap_h_cb
  by_cases hgp : g ≤ p.workingSet
  · by_cases hgq : g ≤ q.workingSet
    · have hmin_p : min ca cb ≤ p.workingSet :=
        Nat.le_trans (Nat.min_le_left ca cb) (clamp_le_of_ge a b p.workingSet (Nat.le_trans hag hgp))
      have hmin_q : min ca cb ≤ q.workingSet :=
        Nat.le_trans (Nat.min_le_right ca cb) (clamp_le_of_ge a b q.workingSet (Nat.le_trans hag hgq))
      have hgmin : g ≤ min ca cb :=
        Nat.le_min_of_le_of_le (le_clamp a b p.workingSet g hgb hgp) (le_clamp a b q.workingSet g hgb hgq)
      have cheap_n_min : cheaper p q n (min ca cb) = true := by
        by_cases hle : ca ≤ cb
        · simpa [Nat.min_eq_left hle] using cheap_n_ca
        · have hge : cb ≤ ca := Nat.le_of_not_ge hle
          simpa [Nat.min_eq_right hge] using cheap_n_cb
      exact both_low p q n a (min ca cb) g hag hgmin hmin_p hmin_q cheap_n_a cheap_n_min
    · have hwqg : q.workingSet < g := Nat.lt_of_not_ge hgq
      have hwqb : q.workingSet ≤ b := Nat.le_trans (Nat.le_of_lt hwqg) hgb
      have hcb_eq : cb = max a q.workingSet := by
        simpa [cb] using clamp_eq_max a b q.workingSet hab hwqb
      have hcb_le_g : cb ≤ g := by
        rw [hcb_eq]
        exact Nat.max_le_of_le_of_le hag (Nat.le_of_lt hwqg)
      have hwp_cb : q.workingSet ≤ cb := by rw [hcb_eq]; exact Nat.le_max_right a q.workingSet
      have hca_le_wp : ca ≤ p.workingSet := clamp_le_of_ge a b p.workingSet (Nat.le_trans hag hgp)
      exact q_high_p_low p q n cb ca g hcb_le_g (le_clamp a b p.workingSet g hgb hgp)
        hwp_cb hca_le_wp cheap_n_cb
  · have hwp_lt : p.workingSet < g := Nat.lt_of_not_ge hgp
    have hwpb : p.workingSet ≤ b := Nat.le_trans (Nat.le_of_lt hwp_lt) hgb
    have hca_eq : ca = max a p.workingSet := by
      simpa [ca] using clamp_eq_max a b p.workingSet hab hwpb
    have hca_le_g : ca ≤ g := by
      rw [hca_eq]
      exact Nat.max_le_of_le_of_le hag (Nat.le_of_lt hwp_lt)
    have hwp_ca : p.workingSet ≤ ca := by rw [hca_eq]; exact Nat.le_max_right a p.workingSet
    by_cases hgq : g ≤ q.workingSet
    · have hg_cb : g ≤ cb := le_clamp a b q.workingSet g hgb hgq
      have hcb_le_wq : cb ≤ q.workingSet := clamp_le_of_ge a b q.workingSet (Nat.le_trans hag hgq)
      exact p_high_q_low p q n ca cb g hca_le_g hg_cb hwp_ca hcb_le_wq cheap_n_cb
    · have hwq_lt : q.workingSet < g := Nat.lt_of_not_ge hgq
      have hwqb : q.workingSet ≤ b := Nat.le_trans (Nat.le_of_lt hwq_lt) hgb
      have hcb_eq : cb = max a q.workingSet := by
        simpa [cb] using clamp_eq_max a b q.workingSet hab hwqb
      have hcb_le_g : cb ≤ g := by
        rw [hcb_eq]
        exact Nat.max_le_of_le_of_le hag (Nat.le_of_lt hwq_lt)
      have hwq_cb : q.workingSet ≤ cb := by rw [hcb_eq]; exact Nat.le_max_right a q.workingSet
      have hmax_le_g : max ca cb ≤ g := Nat.max_le_of_le_of_le hca_le_g hcb_le_g
      have hwp_max : p.workingSet ≤ max ca cb := Nat.le_trans hwp_ca (Nat.le_max_left ca cb)
      have hwq_max : q.workingSet ≤ max ca cb := Nat.le_trans hwq_cb (Nat.le_max_right ca cb)
      have cheap_n_max : cheaper p q n (max ca cb) = true := by
        by_cases hle : ca ≤ cb
        · simpa [Nat.max_eq_right hle] using cheap_n_cb
        · have hge : cb ≤ ca := Nat.le_of_not_ge hle
          simpa [Nat.max_eq_left hge] using cheap_n_ca
      exact both_high p q n (max ca cb) b g hmax_le_g hgb hwp_max hwq_max cheap_n_max

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_SpillBreakpoints
