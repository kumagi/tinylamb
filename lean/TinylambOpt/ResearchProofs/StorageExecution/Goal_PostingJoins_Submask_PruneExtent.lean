import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_PruneExtent

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem sub_pow_decomp (P q r : Nat) (hq : 1 ≤ q) : (P*q + r) - P = P*(q-1) + r := by
  have hP : P ≤ P*q := by
    simpa using Nat.mul_le_mul_left P hq
  rw [Nat.sub_add_comm hP]
  rw [Nat.mul_sub_left_distrib, Nat.mul_one]

private theorem testBit_sub_two_pow_self {z b : Nat} (hz : z.testBit b = true) :
    (z - 2^b).testBit b = false := by
  let q := z / 2^b
  let r := z % 2^b
  have hr : r < 2^b := by exact Nat.mod_lt _ (Nat.two_pow_pos b)
  have hzdecomp : z = 2^b * q + r := by
    dsimp [q, r]
    exact (Nat.div_add_mod z (2^b)).symm
  have hqmod : q % 2 = 1 := by
    have h := (Nat.testBit_eq_decide_div_mod_eq (x := z) (i := b))
    rw [hz] at h
    simp at h
    simpa [q] using h
  have hqm := Nat.div_add_mod q 2
  have hqpos : 1 ≤ q := by omega
  have hsub : z - 2^b = 2^b * (q - 1) + r := by
    rw [hzdecomp]
    exact sub_pow_decomp (2^b) q r hqpos
  rw [hsub]
  rw [Nat.testBit_two_pow_mul_add (a := q-1) (b := r) hr]
  simp only [Nat.sub_self]
  rw [Nat.testBit_zero]
  have hq1mod : (q-1) % 2 = 0 := by omega
  simp [hq1mod]

private theorem testBit_sub_two_pow_of_ne {z b i : Nat} (hz : z.testBit b = true) (hne : i ≠ b) :
    (z - 2^b).testBit i = z.testBit i := by
  let q := z / 2^b
  let r := z % 2^b
  have hr : r < 2^b := by exact Nat.mod_lt _ (Nat.two_pow_pos b)
  have hzdecomp : z = 2^b * q + r := by
    dsimp [q, r]
    exact (Nat.div_add_mod z (2^b)).symm
  have hqmod : q % 2 = 1 := by
    have h := (Nat.testBit_eq_decide_div_mod_eq (x := z) (i := b))
    rw [hz] at h
    simp at h
    simpa [q] using h
  have hqm := Nat.div_add_mod q 2
  have hqpos : 1 ≤ q := by omega
  have hsub : z - 2^b = 2^b * (q - 1) + r := by
    rw [hzdecomp]
    exact sub_pow_decomp (2^b) q r hqpos
  rw [hsub, hzdecomp]
  rw [Nat.testBit_two_pow_mul_add (a := q-1) (b := r) hr]
  rw [Nat.testBit_two_pow_mul_add (a := q) (b := r) hr]
  by_cases hib : i < b
  · simp [hib]
  · simp [hib]
    have hbi : b < i := by omega
    let d := i - b
    have hdpos : 0 < d := by dsimp [d]; omega
    have hi_eq : i = b + d := by dsimp [d]; omega
    have hcore : (q - 1).testBit (i - b) = q.testBit (i - b) := by
      have hq1 : q - 1 = 2 * (q / 2) := by omega
      obtain ⟨e, he⟩ := Nat.exists_eq_succ_of_ne_zero (by omega : i - b ≠ 0)
      rw [he]
      rw [Nat.testBit_add_one, Nat.testBit_add_one]
      have hdiv1 : (q - 1) / 2 = q / 2 := by
        rw [hq1]
        simp
      rw [hdiv1]
    simpa [hi_eq, Nat.add_sub_cancel_left] using hcore

private theorem bit_lt_width {x b : Nat} (hbx : x.testBit b = true) : b < width x := by
  by_cases hc : b < width x
  · exact hc
  · exfalso
    have hc' : width x ≤ b := by omega
    have hxb' : x < 2^b := by
      unfold width at hc'
      by_cases hx0 : x = 0
      · simp [hx0] at hbx
      · simp [hx0] at hc'
        exact Nat.lt_of_lt_of_le Nat.lt_log2_self (Nat.pow_le_pow_right (by decide) hc')
    have hbfalse : x.testBit b = false := Nat.testBit_lt_two_pow hxb'
    rw [hbfalse] at hbx
    cases hbx

private theorem mem_submasks_of_submask {x y : Nat} (hy : (y &&& x) == y) : y ∈ submasks x := by
  unfold submasks
  let bits := (List.range (width x)).filter x.testBit
  have hbits_all : ∀ b ∈ bits, x.testBit b = true := fun b hb => (List.mem_filter.mp hb).2
  suffices h : ∀ (bs : List Nat) (acc : List Nat),
      (∀ b ∈ bs, x.testBit b = true) →
      (∀ z, (z &&& x) == z → (∀ b, z.testBit b = true → b ∉ bs) → z ∈ acc) →
      ∀ z, (z &&& x) == z → z ∈ bs.foldl (fun acc bit => acc ++ acc.map (· + 2^bit)) acc from
    h bits [0] hbits_all (by
      intro z hz hno
      have hz0 : z = 0 := by
        apply Nat.eq_of_testBit_eq
        intro i
        by_cases hzi : z.testBit i = true
        · have hxi : x.testBit i = true := by
            have hbit := congrArg (fun n => Nat.testBit n i) (eq_of_beq hz)
            simp only [Nat.testBit_and] at hbit
            rw [hzi] at hbit
            simpa using hbit
          have hilt : i < width x := bit_lt_width hxi
          have hmem : i ∈ bits := by simp [bits, List.mem_filter, hilt, hxi]
          exact False.elim (hno i hzi hmem)
        · cases hb : z.testBit i with
          | false => simp
          | true => exact False.elim (hzi hb)
      simp [hz0]) y hy
  intro bs
  induction bs with
  | nil =>
      intro acc _ hacc z hz
      exact hacc z hz (by intro b hb; simp)
  | cons b bs ih =>
      intro acc hbits hacc z hz
      simp only [List.foldl_cons]
      exact ih (acc ++ acc.map (fun v => v + 2^b))
        (fun c hc => hbits c (List.mem_cons_of_mem _ hc))
        (by
          intro w hw hnow
          by_cases hwb : w.testBit b = true
          · have hb2 : 2^b ≤ w := Nat.ge_two_pow_of_testBit hwb
            have hw' : ((w - 2^b) &&& x) == w - 2^b := by
              simp only [beq_iff_eq] at hw ⊢
              apply Nat.eq_of_testBit_eq
              intro i
              simp only [Nat.testBit_and]
              by_cases hib : i = b
              · subst i
                rw [testBit_sub_two_pow_self hwb]
                have hbx : x.testBit b = true := hbits b List.mem_cons_self
                simp [hbx]
              · rw [testBit_sub_two_pow_of_ne hwb hib]
                have hbit := congrArg (fun n => Nat.testBit n i) hw
                simp only [Nat.testBit_and] at hbit
                exact hbit
            have hnow' : ∀ c, (w - 2^b).testBit c = true → c ∉ bs := by
              intro c hc hcbs
              by_cases hcb : c = b
              · subst c
                have hcfalse := testBit_sub_two_pow_self hwb
                rw [hcfalse] at hc
                cases hc
              · have hwc : w.testBit c = true := by
                  rw [← testBit_sub_two_pow_of_ne hwb hcb]
                  exact hc
                exact hnow c hwc hcbs
            have hmem' := hacc (w - 2^b) hw' (by
              intro c hc hcin
              simp only [List.mem_cons] at hcin
              rcases hcin with hcb | hcbs
              · subst c
                have hcfalse := testBit_sub_two_pow_self hwb
                rw [hcfalse] at hc
                cases hc
              · exact hnow' c hc hcbs)
            simp only [List.mem_append, List.mem_map]
            exact Or.inr ⟨w - 2^b, hmem', Nat.sub_add_cancel hb2⟩
          · have hmem' := hacc w hw (by
              intro c hc hcin
              simp only [List.mem_cons] at hcin
              rcases hcin with rfl | hcbs
              · exact hwb hc
              · exact hnow c hc hcbs)
            simp only [List.mem_append]
            exact Or.inl hmem')
        z hz


theorem sound_PruneExtent : Submask.PruneExtent.Goal := by
  intro u t x ys lo hi hv hbounds hmiss
  rw [List.eq_nil_iff_forall_not_mem]
  intro y hy
  rcases List.mem_filter.mp hy with ⟨hyin, hytest⟩
  have hyu : y < u := hv.2 y hyin
  simp only [search, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at hytest
  have hymem : y ∈ submasks x := mem_submasks_of_submask (beq_of_eq hytest.1)
  have hp : (y, y + 1) ∈ search.spans u t x := by
    simp only [search, List.mem_map, List.mem_filter]
    exact ⟨y, ⟨hymem, by simp [hyu, hytest.2]⟩, rfl⟩
  have hm := hmiss (y, y + 1) hp
  have hb := hbounds y hyin
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_PruneExtent
