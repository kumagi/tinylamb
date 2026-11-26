import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `EdgeCurvature.TrendCertificate` — `∀ x, measure x = 0 ↔ ∃ a d : Int, x = (List.range
  x.length).map (fun (i : Nat) => a+d*i)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_TrendCertificate

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private def d2 (x : Series) : Series := delta (delta x)

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj_cons₂]

private theorem d2_cons₃ (a b c : Int) (xs : Series) :
    d2 (a :: b :: c :: xs) = (c - 2 * b + a) :: d2 (b :: c :: xs) := by
  unfold d2
  rw [delta_cons₂ a b (c :: xs)]
  rw [delta_cons₂ b c xs]
  rw [delta_cons₂ (b - a) (c - b) (delta (c :: xs))]
  simp
  omega

private theorem int_sq_nonneg (v : Int) : 0 ≤ v ^ 2 := by
  have hsq : v ^ 2 = v * v := by
    rw [Int.pow_succ]
    simp [Int.pow_succ]
  rw [hsq]
  by_cases hv : 0 ≤ v
  · exact Int.mul_nonneg hv hv
  · have hv' : 0 ≤ -v := by omega
    rw [← Int.neg_mul_neg v v]
    exact Int.mul_nonneg hv' hv'

private theorem int_sq_eq_zero (v : Int) : v ^ 2 = 0 ↔ v = 0 := by
  constructor
  · intro h
    have hn : (v ^ 2).natAbs = 0 := by rw [h]; rfl
    rw [Int.natAbs_pow] at hn
    have hv0 : v.natAbs = 0 := by simpa using hn
    exact Int.natAbs_eq_zero.mp hv0
  · intro h
    subst v
    simp

private theorem sum_sq_nonneg (l : Series) : 0 ≤ (l.map (fun v => v ^ 2)).sum := by
  induction l with
  | nil => simp
  | cons v vs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hv := int_sq_nonneg v
      omega

private theorem sum_sq_eq_zero_iff (l : Series) :
    (l.map (fun v => v ^ 2)).sum = 0 ↔ ∀ v ∈ l, v = 0 := by
  induction l with
  | nil => simp
  | cons v vs ih =>
      simp only [List.map_cons, List.sum_cons, List.mem_cons]
      constructor
      · intro h
        have hv2 : 0 ≤ v ^ 2 := int_sq_nonneg v
        have hs2 : 0 ≤ (vs.map (fun v => v ^ 2)).sum := sum_sq_nonneg vs
        have hv0 : v ^ 2 = 0 := by omega
        have hs0 : (vs.map (fun v => v ^ 2)).sum = 0 := by omega
        have hv := (int_sq_eq_zero v).mp hv0
        have hall := ih.mp hs0
        intro w hw
        rcases hw with rfl | hw
        · exact hv
        · exact hall w hw
      · intro h
        have hv0 : v ^ 2 = 0 := (int_sq_eq_zero v).mpr (h v (by simp))
        have hs0 : (vs.map (fun v => v ^ 2)).sum = 0 := ih.mpr (by
          intro w hw
          exact h w (by simp [hw]))
        omega

private theorem affine_cons (a d : Int) (n : Nat) :
    (a - d) :: ((List.range n).map (fun i : Nat => a + d * (i : Int))) =
    (List.range (n + 1)).map (fun i : Nat => (a - d) + d * (i : Int)) := by
  apply List.ext_getElem
  · simp
  · intro i hi hi'
    cases i with
    | zero => simp
    | succ j =>
        simp only [List.getElem_cons_succ, List.getElem_map, List.getElem_range]
        have hcast : ((j + 1 : Nat) : Int) = (j : Int) + 1 := by omega
        rw [hcast]
        rw [Int.mul_add]
        simp
        omega

private theorem zero_d2_affine : ∀ x : Series,
    (∀ v ∈ d2 x, v = 0) →
    ∃ a d : Int, x = (List.range x.length).map (fun i : Nat => a + d * (i : Int)) := by
  intro x
  induction x with
  | nil =>
      intro h
      exact ⟨0, 0, by simp⟩
  | cons a xs ih =>
      cases xs with
      | nil =>
          intro h
          exact ⟨a, 0, by simp⟩
      | cons b ys =>
          cases ys with
          | nil =>
              intro h
              refine ⟨a, b - a, ?_⟩
              change [a, b] = [a + (b - a) * (0 : Int), a + (b - a) * (1 : Int)]
              simp
              omega
          | cons c zs =>
              intro h
              have hc : c - 2 * b + a = 0 := by
                have hmem : c - 2 * b + a ∈ d2 (a :: b :: c :: zs) := by
                  rw [d2_cons₃]
                  simp
                exact h (c - 2 * b + a) hmem
              have htail : ∀ v ∈ d2 (b :: c :: zs), v = 0 := by
                intro v hv
                apply h v
                rw [d2_cons₃]
                exact List.mem_cons_of_mem _ hv
              rcases ih htail with ⟨a', d', haff⟩
              have hb : b = a' := by
                have hg := congrArg (fun l : Series => l[0]?) haff
                simpa using hg
              have hc' : c = a' + d' := by
                have hg := congrArg (fun l : Series => l[1]?) haff
                simpa using hg
              have ha : a = a' - d' := by omega
              refine ⟨a' - d', d', ?_⟩
              have hmain := affine_cons a' d' (b :: c :: zs).length
              rw [← haff] at hmain
              simpa [ha] using hmain

private theorem zipIdx_add (xs : Series) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private def shiftAffine (a c : Int) (x : Series) : Series :=
  x.zipIdx.map (fun (v, i) => v + a * (i : Int) + c)

private theorem shiftAffine_cons (a c : Int) (v : Int) (xs : Series) :
    shiftAffine a c (v :: xs) = (v + c) :: shiftAffine a (c + a) xs := by
  unfold shiftAffine
  simp only [List.zipIdx_cons, List.map_cons]
  have hzip := zipIdx_add xs 0 1
  simp only [Nat.zero_add] at hzip
  rw [hzip]
  simp only [List.map_map]
  congr 1
  · simp
  · apply List.map_congr_left
    intro p hp
    rcases p with ⟨w, i⟩
    dsimp
    rw [Int.mul_add]
    simp
    omega

private theorem d2_shiftAffine (a c : Int) (x : Series) :
    d2 (shiftAffine a c x) = d2 x := by
  induction x generalizing c with
  | nil => simp [d2, shiftAffine, delta, adj]
  | cons v xs ih =>
      cases xs with
      | nil => simp [d2, shiftAffine, delta, adj]
      | cons w xs =>
          cases xs with
          | nil => simp [d2, shiftAffine, delta, adj]
          | cons u xs =>
              rw [shiftAffine_cons a c v (w :: u :: xs)]
              rw [shiftAffine_cons a (c + a) w (u :: xs)]
              rw [shiftAffine_cons a (c + a + a) u xs]
              rw [d2_cons₃]
              have htail := ih (c + a)
              rw [shiftAffine_cons a (c + a) w (u :: xs)] at htail
              rw [shiftAffine_cons a (c + a + a) u xs] at htail
              rw [htail]
              congr 1
              dsimp
              omega

private theorem shiftAffine_replicate (a d : Int) (n : Nat) :
    shiftAffine d a (List.replicate n 0) =
      (List.range n).map (fun i : Nat => a + d * (i : Int)) := by
  induction n generalizing a with
  | zero => simp [shiftAffine]
  | succ n ih =>
      simp only [List.replicate_succ, shiftAffine_cons]
      rw [ih (a + d)]
      simpa using (affine_cons (a + d) d n)

private theorem delta_replicate (n : Nat) (d : Int) :
    delta (List.replicate (n + 1) d) = List.replicate n 0 := by
  induction n with
  | zero => simp [delta, adj]
  | succ n ih =>
      rw [show List.replicate (n + 1 + 1) d = d :: List.replicate (n + 1) d by
        simp [List.replicate_succ]]
      rw [show List.replicate (n + 1) d = d :: List.replicate n d by
        simp [List.replicate_succ]]
      rw [delta_cons₂]
      rw [show d :: List.replicate n d = List.replicate (n + 1) d by
        simp [List.replicate_succ]]
      rw [ih]
      rw [List.replicate_succ]
      simp

private theorem d2_replicate_zero (n : Nat) :
    ∀ v ∈ d2 (List.replicate n (0 : Int)), v = 0 := by
  cases n with
  | zero => simp [d2, delta, adj]
  | succ n =>
      cases n with
      | zero => simp [d2, delta, adj]
      | succ m =>
          change ∀ v ∈ d2 (List.replicate (m + 1 + 1) (0 : Int)), v = 0
          rw [show d2 (List.replicate (m + 1 + 1) (0 : Int)) = List.replicate m 0 by
            unfold d2
            rw [delta_replicate (m + 1) (0 : Int)]
            rw [delta_replicate m (0 : Int)]]
          simp

private theorem affine_zero_measure (a d : Int) (n : Nat) :
    ((d2 ((List.range n).map (fun i : Nat => a + d * (i : Int)))).map (fun v => v ^ 2)).sum = 0 := by
  have hshape := shiftAffine_replicate a d n
  have hd2eq : d2 ((List.range n).map (fun i : Nat => a + d * (i : Int))) =
      d2 (List.replicate n 0) := by
    rw [← hshape]
    exact d2_shiftAffine d a (List.replicate n 0)
  rw [hd2eq]
  exact (sum_sq_eq_zero_iff (d2 (List.replicate n 0))).mpr (d2_replicate_zero n)

theorem sound_EdgeCurvature_TrendCertificate : EdgeCurvature.TrendCertificate.Goal := by
  intro x
  change ((d2 x).map (fun v => v ^ 2)).sum = 0 ↔
    ∃ a d : Int, x = (List.range x.length).map (fun i : Nat => a + d * (i : Int))
  constructor
  · intro h
    exact zero_d2_affine x ((sum_sq_eq_zero_iff (d2 x)).mp h)
  · rintro ⟨a, d, hx⟩
    rw [hx]
    simpa using affine_zero_measure a d x.length

end TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_TrendCertificate
