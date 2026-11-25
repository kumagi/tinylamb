import TinylambOpt.ResearchGoals.Frontiers.SparseRecovery
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Frontiers.SparseRecovery

open TinylambOpt.ResearchGoals.Frontiers.SparseRecovery

/-! Soundness and disproof theorems for the SparseRecovery research goals. -/

private theorem int_pow_two (a : Int) : a^2 = a*a := by
  rw [Int.pow_succ]
  simp [Int.pow_succ]


private def D : Nat → (Int → Int) → Int → Int
  | 0, f => f
  | k+1, f => fun z => D k f (z+1) - D k f z

private theorem D_const_zero (k : Nat) : D k (fun _ : Int => (0:Int)) = fun _ => 0 := by
  induction k with
  | zero => rfl
  | succ k ih =>
      funext z
      simp only [D]
      rw [show D k (fun _ : Int => (0:Int)) (z+1) = 0 by simpa using congrFun ih (z+1)]
      rw [show D k (fun _ : Int => (0:Int)) z = 0 by simpa using congrFun ih z]
      omega

private theorem D_sub (k : Nat) (f g : Int → Int) :
    D k (fun z => f z - g z) = fun z => D k f z - D k g z := by
  induction k with
  | zero => rfl
  | succ k ih =>
      funext z
      simp only [D]
      rw [show D k (fun z => f z - g z) (z+1) = D k f (z+1) - D k g (z+1) by simpa using congrFun ih (z+1)]
      rw [show D k (fun z => f z - g z) z = D k f z - D k g z by simpa using congrFun ih z]
      omega

private theorem D_of_zero (k : Nat) (f : Int → Int) (h : ∀ z, D k f z = 0) :
    ∀ z, D (k+1) f z = 0 := by
  intro z
  simp only [D]
  rw [h (z+1), h z]
  omega

private theorem D_mono (k : Nat) (f : Int → Int) (h : ∀ z, D k f z = 0) :
    ∀ m, k ≤ m → ∀ z, D m f z = 0 := by
  intro m hm
  induction hm with
  | refl => exact h
  | @step m hkm ih =>
      intro z
      exact D_of_zero m f ih z

private theorem mul_lin_delta (c : Int) (g : Int → Int) (z : Int) :
    ((z+1) - c) * g (z+1) - (z-c) * g z = ((z+1)-c) * (g (z+1) - g z) + g z := by
  simp only [Int.sub_eq_add_neg, Int.add_mul, Int.mul_add, Int.neg_mul, Int.mul_neg]
  omega

private theorem D_mul_linear (k : Nat) (c : Int) (g : Int → Int) (z : Int) :
    D (k+1) (fun z => (z - c) * g z) z =
      (z + (k:Int) + 1 - c) * D (k+1) g z + ((k:Int)+1) * D k g z := by
  induction k generalizing z with
  | zero =>
      change (z+1-c)*g (z+1) - (z-c)*g z =
        (z + (0:Int) + 1 - c) * (g (z+1) - g z) + ((0:Int)+1) * g z
      rw [mul_lin_delta]
      rw [show (z + (0:Int) + 1 - c) = z + 1 - c by omega]
      omega
  | succ k ih =>
      change D (k+1) (fun z => (z-c)*g z) (z+1) - D (k+1) (fun z => (z-c)*g z) z =
        (z + ((k+1:Nat):Int) + 1 - c) * D (k+2) g z + (((k+1:Nat):Int)+1) * D (k+1) g z
      rw [show ((k+1:Nat):Int) = (k:Int)+1 by simp]
      rw [show D (k+2) g z = D (k+1) g (z+1) - D (k+1) g z by rfl]
      rw [ih (z+1), ih]
      have hksub : ((k:Int)+1) * D k g (z+1) - ((k:Int)+1) * D k g z =
          ((k:Int)+1) * (D k g (z+1) - D k g z) := by rw [Int.mul_sub]
      have hD1 : D k g (z+1) - D k g z = D (k+1) g z := by rfl
      have hb := mul_lin_delta (c - (k:Int) - 1) (D (k+1) g) z
      rw [show (z+1) - (c - (k:Int) - 1) = (z+1) + (k:Int) + 1 - c by omega] at hb
      rw [show z - (c - (k:Int) - 1) = z + (k:Int) + 1 - c by omega] at hb
      rw [show (z+1) + (k:Int) + 1 - c = z + ((k:Int)+1) + 1 - c by omega] at hb
      have hD1 : D k g (z+1) - D k g z = D (k+1) g z := by rfl
      rw [hD1] at hksub
      rw [show
        (z + 1 + (k:Int) + 1 - c) * D (k+1) g (z+1) + ((k:Int)+1) * D k g (z+1) -
          ((z + (k:Int) + 1 - c) * D (k+1) g z + ((k:Int)+1) * D k g z) =
        ((z + 1 + (k:Int) + 1 - c) * D (k+1) g (z+1) - (z+(k:Int)+1-c)*D (k+1) g z) +
          (((k:Int)+1)*D k g (z+1) - ((k:Int)+1)*D k g z) by omega]
      rw [show z + 1 + (k:Int) + 1 - c = z + ((k:Int)+1) + 1 - c by omega]
      rw [hb, hksub]
      have hcoef : (z + ((k:Int)+1) + 1 - c) * (D (k+1) g (z+1) - D (k+1) g z) =
          (z + ((k:Int)+1) + 1 - c) * D (k+1) g (z+1) -
            (z + ((k:Int)+1) + 1 - c) * D (k+1) g z := by rw [Int.mul_sub]
      rw [hcoef]
      have hcoef2 : (z + ((k:Int)+1) + 1 - c) * D (k+1) g z =
          (z + (k:Int) + 1 - c) * D (k+1) g z + D (k+1) g z := by
        rw [show (z + ((k:Int)+1) + 1 - c) = (z + (k:Int) + 1 - c) + (1:Int) by omega]
        rw [Int.add_mul, Int.one_mul]
      rw [hcoef2]
      rw [show ((k:Int)+1+1) * D (k+1) g z = ((k:Int)+1) * D (k+1) g z + D (k+1) g z by
        rw [show ((k:Int)+1+1) = ((k:Int)+1)+1 by omega, Int.add_mul, Int.one_mul]]
      omega


private def polyI : List Int → Int → Int
  | [], _ => 1
  | x::xs, z => (z-x) * polyI xs z

private theorem D_polyI_zero (xs : List Int) : ∀ z, D (xs.length+1) (polyI xs) z = 0 := by
  induction xs with
  | nil =>
      intro z
      simp [D, polyI]
  | cons x xs ih =>
      intro z
      change D (xs.length + 2) (fun z => (z-x)*polyI xs z) z = 0
      have h := D_mul_linear (xs.length+1) x (polyI xs) z
      change D ((xs.length+1)+1) (fun z => (z-x)*polyI xs z) z =
        (z + (xs.length+1:Int) + 1 - x) * D ((xs.length+1)+1) (polyI xs) z +
          ((xs.length+1:Int)+1) * D (xs.length+1) (polyI xs) z at h
      rw [h]
      have h0 : D ((xs.length+1)+1) (polyI xs) z = 0 := D_of_zero (xs.length+1) (polyI xs) ih z
      rw [h0]
      have h1 : D (xs.length+1) (polyI xs) z = 0 := ih z
      rw [h1]
      omega

private def dIter : Nat → (Nat → Int) → Nat → Int
  | 0, f => f
  | k+1, f => fun t => dIter k f (t+1) - dIter k f t

private theorem dIter_delta (k : Nat) (f : Nat → Int) :
    dIter k (fun t => f (t+1) - f t) = fun t => dIter (k+1) f t := by
  induction k with
  | zero => rfl
  | succ k ih =>
      funext t
      simp only [dIter]
      rw [show dIter k (fun t => f (t+1) - f t) (t+1) = dIter (k+1) f (t+1) by
        simpa using congrFun ih (t+1)]
      rw [show dIter k (fun t => f (t+1) - f t) t = dIter (k+1) f t by
        simpa using congrFun ih t]
      change dIter (k+1) f (t+1) - dIter (k+1) f t =
        dIter (k+1) f (t+1) - dIter (k+1) f t
      rfl

private theorem dIter_eq_D (f : Int → Int) (a : Int) (k t : Nat) :
    dIter k (fun t : Nat => f (a + (t:Int))) t = D k f (a + (t:Int)) := by
  induction k generalizing t with
  | zero => rfl
  | succ k ih =>
      simp only [dIter]
      rw [ih (t+1), ih t]
      rw [show ((t+1:Nat):Int) = (t:Int)+1 by simp]
      rw [show a + ((t:Int)+1) = (a+(t:Int)) + 1 by omega]
      rfl

private theorem dIter_zero_of_zeros (n : Nat) (f : Nat → Int)
    (hD : ∀ t, dIter (n+1) f t = 0)
    (hz : ∀ i, i ≤ n → f i = 0) : ∀ t, f t = 0 := by
  induction n generalizing f with
  | zero =>
      intro t
      induction t with
      | zero => exact hz 0 (by omega)
      | succ t iht =>
          have hdt := hD t
          simp only [dIter] at hdt
          omega
  | succ n ih =>
      let g : Nat → Int := fun t => f (t+1) - f t
      have hDg : ∀ t, dIter (n+1) g t = 0 := by
        intro t
        have ht := hD t
        change dIter (n+1) g t = 0
        rw [dIter_delta]
        simpa using ht
      have hzg : ∀ i, i ≤ n → g i = 0 := by
        intro i hi
        dsimp [g]
        have h1 := hz (i+1) (by omega)
        have h2 := hz i (by omega)
        omega
      have hg := ih g hDg hzg
      intro t
      induction t with
      | zero => exact hz 0 (by omega)
      | succ t iht =>
          have hgt := hg t
          dsimp [g] at hgt
          omega

private theorem polyI_ext_of_zeros (p q : List Int) (a : Int) (d : Nat)
    (hp : p.length ≤ d) (hq : q.length ≤ d)
    (hz : ∀ t : Nat, t ≤ d → polyI p (a + (t:Int)) = polyI q (a + (t:Int))) :
    ∀ t : Nat, polyI p (a + (t:Int)) = polyI q (a + (t:Int)) := by
  let f : Int → Int := fun z => polyI p z - polyI q z
  let g : Nat → Int := fun t => f (a + (t:Int))
  have hDp : ∀ z, D (d+1) (polyI p) z = 0 := by
    intro z
    exact D_mono (p.length+1) (polyI p) (D_polyI_zero p) (d+1) (by omega) z
  have hDq : ∀ z, D (d+1) (polyI q) z = 0 := by
    intro z
    exact D_mono (q.length+1) (polyI q) (D_polyI_zero q) (d+1) (by omega) z
  have hDf : ∀ z, D (d+1) f z = 0 := by
    intro z
    dsimp [f]
    rw [D_sub]
    simp only [hDp z, hDq z]
    omega
  have hDg : ∀ t, dIter (d+1) g t = 0 := by
    intro t
    dsimp [g]
    rw [dIter_eq_D]
    exact hDf (a+(t:Int))
  have hzg : ∀ i, i ≤ d → g i = 0 := by
    intro i hi
    dsimp [g, f]
    rw [hz i hi]
    omega
  have hall := dIter_zero_of_zeros d g hDg hzg
  intro t
  have ht := hall t
  dsimp [g, f] at ht
  omega



private theorem D_zero_of_zeros_int (m : Nat) (f : Int → Int) (z : Int)
    (hz : ∀ i : Nat, i ≤ m → f (z + (i:Int)) = 0) : D m f z = 0 := by
  induction m generalizing z with
  | zero =>
      simpa [D] using hz 0 (by omega)
  | succ m ih =>
      simp only [D]
      have h1 : D m f (z+1) = 0 := by
        apply ih (z+1)
        intro i hi
        simpa [Int.add_assoc, Int.add_comm, Int.add_left_comm] using hz (i+1) (by omega)
      have h2 : D m f z = 0 := by
        apply ih z
        intro i hi
        exact hz i (by omega)
      omega

private theorem D_backward (k : Nat) (f : Int → Int) (z : Int)
    (hD : D k f z = 0) (hz : ∀ i : Nat, 1 ≤ i → i ≤ k → f (z + (i:Int)) = 0) :
    f z = 0 := by
  induction k generalizing z with
  | zero =>
      simpa [D] using hD
  | succ k ih =>
      have hnext : D k f (z+1) = 0 := by
        apply D_zero_of_zeros_int k f (z+1)
        intro i hi
        simpa [Int.add_assoc, Int.add_comm, Int.add_left_comm] using hz (i+1) (by omega) (by omega)
      have hDk : D k f z = 0 := by
        simp only [D] at hD
        omega
      apply ih z hDk
      intro i hi1 hik
      exact hz i hi1 (by omega)

private theorem D_all_zero_of_zeros_nat (k : Nat) (f : Int → Int)
    (hD : ∀ z, D k f z = 0) (hz : ∀ t : Nat, t < k → f (t:Int) = 0) : ∀ z : Int, f z = 0 := by
  by_cases hk : k = 0
  · subst k
    intro z
    exact hD z
  · have hkpos : 0 < k := Nat.pos_of_ne_zero hk
    have hpos : ∀ t : Nat, f (t:Int) = 0 := by
      have hDg : ∀ s : Nat, dIter k (fun s : Nat => f ((0:Int)+(s:Int))) s = 0 := by
        intro s
        rw [dIter_eq_D]
        simpa using hD ((0:Int)+(s:Int))
      have hzeros : ∀ s : Nat, s ≤ k-1 → (fun s : Nat => f ((0:Int)+(s:Int))) s = 0 := by
        intro s hs
        simpa using hz s (by omega)
      have hall := dIter_zero_of_zeros (k-1) (fun s : Nat => f ((0:Int)+(s:Int)))
        (by simpa [Nat.sub_add_cancel hkpos] using hDg) hzeros
      intro t
      simpa using hall t
    have hneg : ∀ t : Nat, f (-((t:Int)+1)) = 0 := by
      intro t
      exact Nat.strongRecOn t (motive := fun t => f (-((t:Int)+1)) = 0) (fun t ih => by
          apply D_backward k f (-((t:Int)+1)) (hD (-((t:Int)+1)))
          intro i hi1 hik
          let w : Int := -((t:Int)+1) + (i:Int)
          have hwdef : f (-((t:Int)+1) + (i:Int)) = 0 := by
            by_cases hw : 0 ≤ w
            · rcases Int.eq_ofNat_of_zero_le hw with ⟨m, hm⟩
              have hmk : m < k := by
                have hwle : w ≤ (i:Int) - 1 := by dsimp [w]; omega
                omega
              have hmzero := hz m hmk
              simpa [w, hm] using hmzero
            · have hit : i ≤ t := by
                dsimp [w] at hw
                omega
              let s : Nat := t - i
              have hst : s < t := by dsimp [s]; omega
              have hcast : (s:Int) = (t:Int) - (i:Int) := by dsimp [s]; omega
              have hweq : w = -((s:Int)+1) := by dsimp [w]; rw [hcast]; omega
              rw [show -((t:Int)+1) + (i:Int) = w by rfl, hweq]
              exact ih s hst
          exact hwdef)
    intro z
    by_cases hz0 : 0 ≤ z
    · rcases Int.eq_ofNat_of_zero_le hz0 with ⟨t, rfl⟩
      exact hpos t
    · obtain ⟨t, ht⟩ : ∃ t : Nat, z = -((t:Int)+1) := by
        refine ⟨Int.toNat (-z-1), ?_⟩
        have hp : 0 ≤ -z-1 := by omega
        rw [Int.toNat_of_nonneg hp]
        omega
      rw [ht]
      exact hneg t


private theorem D_shift (k : Nat) (f : Int → Int) (a z : Int) :
    D k (fun z => f (a+z)) z = D k f (a+z) := by
  induction k generalizing z with
  | zero => rfl
  | succ k ih =>
      simp only [D]
      rw [ih (z+1), ih z]
      rw [show a + (z+1) = (a+z)+1 by omega]

private theorem D_all_zero_of_zeros_from (k : Nat) (f : Int → Int) (a : Int)
    (hD : ∀ z, D k f z = 0) (hz : ∀ t : Nat, t < k → f (a+(t:Int)) = 0) :
    ∀ z : Int, f z = 0 := by
  let g : Int → Int := fun z => f (a+z)
  have hDg : ∀ z, D k g z = 0 := by
    intro z
    dsimp [g]
    rw [D_shift]
    exact hD (a+z)
  have hzg : ∀ t : Nat, t < k → g (t:Int) = 0 := by
    intro t ht
    dsimp [g]
    simpa using hz t ht
  have hall := D_all_zero_of_zeros_nat k g hDg hzg
  intro z
  have hz' := hall (z-a)
  dsimp [g] at hz'
  rw [show a + (z-a) = z by omega] at hz'
  exact hz'


private theorem polyI_map (xs : List Nat) (z : Int) :
    polyI (xs.map (fun (x : Nat) => (x:Int))) z = poly xs z := by
  induction xs with
  | nil => simp [polyI, poly]
  | cons x xs ih => simp [polyI, poly, ih]

private theorem D_add_f (k : Nat) (f g : Int → Int) :
    D k (fun z => f z + g z) = fun z => D k f z + D k g z := by
  induction k with
  | zero => rfl
  | succ k ih =>
      funext z
      simp only [D]
      rw [show D k (fun z => f z + g z) (z+1) = D k f (z+1) + D k g (z+1) by simpa using congrFun ih (z+1)]
      rw [show D k (fun z => f z + g z) z = D k f z + D k g z by simpa using congrFun ih z]
      omega

private theorem D_monic_diff_zero (p q : List Int) (hlen : p.length = q.length) :
    ∀ z, D (p.length) (fun z => polyI p z - polyI q z) z = 0 := by
  induction p generalizing q with
  | nil =>
      cases q with
      | nil =>
          intro z
          simp [D, polyI]
      | cons y ys =>
          simp at hlen
  | cons x ps ih =>
      cases q with
      | nil => simp at hlen
      | cons y qs =>
          simp only [List.length_cons] at hlen
          have hlen' : ps.length = qs.length := by omega
          intro z
          change D (ps.length + 1) (fun z => (z-x)*polyI ps z - (z-y)*polyI qs z) z = 0
          have hdecomp : (fun z => (z-x)*polyI ps z - (z-y)*polyI qs z) =
              (fun z => (z-x)*(polyI ps z - polyI qs z) + (y-x)*polyI qs z) := by
            funext w
            simp only [Int.sub_eq_add_neg, Int.add_mul, Int.mul_add, Int.neg_mul, Int.mul_neg]
            omega
          rw [hdecomp]
          have hadd := congrFun (D_add_f (ps.length+1)
            (fun w => (w-x)*(polyI ps w - polyI qs w))
            (fun w => (y-x)*polyI qs w)) z
          rw [hadd]
          change D (ps.length+1) (fun w => (w-x)*(polyI ps w - polyI qs w)) z +
            D (ps.length+1) (fun w => (y-x)*polyI qs w) z = 0
          have hmul := D_mul_linear ps.length x (fun w => polyI ps w - polyI qs w) z
          change D (ps.length+1) (fun w => (w-x)*(polyI ps w - polyI qs w)) z =
            (z + (ps.length:Int) + 1 - x) * D (ps.length+1) (fun w => polyI ps w - polyI qs w) z +
              ((ps.length:Int)+1) * D (ps.length) (fun w => polyI ps w - polyI qs w) z at hmul
          rw [hmul]
          have h0 : D (ps.length+1) (fun w => polyI ps w - polyI qs w) z = 0 := by
            have hi := ih qs hlen' z
            exact D_of_zero (ps.length) (fun w => polyI ps w - polyI qs w) (ih qs hlen') z
          rw [h0]
          have h1 : D (ps.length) (fun w => polyI ps w - polyI qs w) z = 0 := ih qs hlen' z
          rw [h1]
          have hq0 : D (ps.length+1) (polyI qs) z = 0 := by
            simpa [hlen'] using (D_polyI_zero qs z)
          have hlin : ∀ k (c : Int) (g : Int → Int) (w : Int), D k (fun v => c * g v) w = c * D k g w := by
            intro k c g w
            induction k generalizing w with
            | zero => rfl
            | succ k ihk =>
                simp only [D]
                rw [ihk (w+1), ihk w]
                rw [Int.mul_sub]
          have hconst : D (ps.length+1) (fun w => (y-x)*polyI qs w) z = 0 := by
            rw [hlin]
            rw [hq0]
            omega
          rw [hconst]
          omega

private theorem poly_eq_all_of_nat_monic (p q : List Nat)
    (hlen : p.length = q.length)
    (hz : ∀ t : Nat, t < p.length → poly p (t:Int) = poly q (t:Int)) :
    ∀ z : Int, poly p z = poly q z := by
  let f : Int → Int := fun z => polyI (p.map (fun (x : Nat) => (x:Int))) z - polyI (q.map (fun (x : Nat) => (x:Int))) z
  have hDf : ∀ z, D (p.length) f z = 0 := by
    intro z
    dsimp [f]
    simpa using (D_monic_diff_zero (p.map (fun (x : Nat) => (x:Int))) (q.map (fun (x : Nat) => (x:Int))) (by simpa using hlen) z)
  have hzf : ∀ t : Nat, t < p.length → f (t:Int) = 0 := by
    intro t ht
    dsimp [f]
    rw [polyI_map, polyI_map, hz t ht]
    omega
  have hall := D_all_zero_of_zeros_nat (p.length) f hDf hzf
  intro z
  have hz' := hall z
  dsimp [f] at hz'
  rw [polyI_map, polyI_map] at hz'
  omega

private theorem poly_eq_all_of_nat (p q : List Nat) (d : Nat)
    (hp : p.length ≤ d) (hq : q.length ≤ d)
    (hz : ∀ t : Nat, t ≤ d → poly p (t:Int) = poly q (t:Int)) :
    ∀ z : Int, poly p z = poly q z := by
  let f : Int → Int := fun z => polyI (p.map (fun (x : Nat) => (x:Int))) z - polyI (q.map (fun (x : Nat) => (x:Int))) z
  have hDp : ∀ z, D (d+1) (polyI (p.map (fun (x : Nat) => (x:Int)))) z = 0 := by
    intro z
    exact D_mono ((p.map (fun (x : Nat) => (x:Int))).length+1) _ (D_polyI_zero _) (d+1) (by simpa using hp) z
  have hDq : ∀ z, D (d+1) (polyI (q.map (fun (x : Nat) => (x:Int)))) z = 0 := by
    intro z
    exact D_mono ((q.map (fun (x : Nat) => (x:Int))).length+1) _ (D_polyI_zero _) (d+1) (by simpa using hq) z
  have hDf : ∀ z, D (d+1) f z = 0 := by
    intro z
    dsimp [f]
    rw [D_sub]
    change D (d+1) (polyI (List.map (fun (x : Nat) => (x:Int)) p)) z - D (d+1) (polyI (List.map (fun (x : Nat) => (x:Int)) q)) z = 0
    rw [hDp z, hDq z]
    omega
  have hzf : ∀ t : Nat, t < d+1 → f (t:Int) = 0 := by
    intro t ht
    dsimp [f]
    rw [polyI_map, polyI_map, hz t (by omega)]
    omega
  have hall := D_all_zero_of_zeros_nat (d+1) f hDf hzf
  intro z
  have hz' := hall z
  dsimp [f] at hz'
  rw [polyI_map, polyI_map] at hz'
  omega

private theorem poly_zero_iff_mem (xs : List Nat) (z : Int) :
    poly xs z = 0 ↔ ∃ x ∈ xs, (x:Int) = z := by
  induction xs with
  | nil => simp [poly]
  | cons x xs ih =>
      simp only [poly]
      rw [List.map_cons, List.prod_cons]
      rw [Int.mul_eq_zero]
      constructor
      · rintro (h | h)
        · refine ⟨x, by simp, (Int.sub_eq_zero.mp h).symm⟩
        · rcases ih.mp h with ⟨y, hy, hyz⟩
          exact ⟨y, by simp [hy], hyz⟩
      · rintro ⟨y, hy, hyz⟩
        rcases List.mem_cons.mp hy with rfl | hy
        · left; rw [hyz]; simp
        · right; exact ih.mpr ⟨y, hy, hyz⟩

private theorem poly_perm_eq (xs ys : List Nat) (h : xs.Perm ys) (z : Int) :
    poly xs z = poly ys z := by
  induction h with
  | nil => rfl
  | cons a h ih =>
      simp only [poly, List.map_cons, List.prod_cons]
      have ih' := ih
      simp only [poly] at ih'
      rw [ih']
  | trans h1 h2 ih1 ih2 => rw [ih1, ih2]
  | swap a b l =>
      simp only [poly, List.map_cons, List.prod_cons]
      calc
        (z - (b:Int)) * ((z - (a:Int)) * (List.map (fun x : Nat => z - (x:Int)) l).prod)
            = ((z - (a:Int)) * (List.map (fun x : Nat => z - (x:Int)) l).prod) * (z - (b:Int)) := Int.mul_comm _ _
        _ = (z - (a:Int)) * ((List.map (fun x : Nat => z - (x:Int)) l).prod * (z - (b:Int))) := by rw [Int.mul_assoc]
        _ = (z - (a:Int)) * ((z - (b:Int)) * (List.map (fun x : Nat => z - (x:Int)) l).prod) := by
            rw [Int.mul_comm ((List.map (fun x : Nat => z - (x:Int)) l).prod) ((z:Int)-(b:Int))]

private theorem perm_of_poly_eq_all (p q : List Nat)
    (h : ∀ z : Int, poly p z = poly q z) : p.Perm q := by
  induction p generalizing q with
  | nil =>
      cases q with
      | nil => exact List.Perm.nil
      | cons y ys =>
          have hy : poly (y::ys) (y:Int) = 0 := by simp [poly]
          have h0 := h (y:Int)
          rw [hy] at h0
          simp [poly] at h0
  | cons x xs ih =>
      have hx0 : poly q (x:Int) = 0 := by rw [← h (x:Int)]; simp [poly]
      rcases (poly_zero_iff_mem q (x:Int)).mp hx0 with ⟨y, hyq, hyx⟩
      have hxy : y = x := Int.ofNat.inj hyx
      subst y
      let q' : List Nat := q.erase x
      let d_tail : Nat := xs.length + q'.length
      let a_tail : Int := (x:Int) - (d_tail:Int) - 1
      have hpq' : q.Perm (x :: q') := List.perm_cons_erase hyq
      have htail : ∀ z : Int, poly xs z = poly q' z := by
        intro z
        by_cases hzx : z = (x:Int)
        · have hzneq : ∀ w : Int, w ≠ (x:Int) → polyI (xs.map (fun (v : Nat) => (v:Int))) w - polyI (q'.map (fun (v : Nat) => (v:Int))) w = 0 := by
            intro w hw
            have hpw : polyI (xs.map (fun (v : Nat) => (v:Int))) w = poly xs w := polyI_map xs w
            have hqw : polyI (q'.map (fun (v : Nat) => (v:Int))) w = poly q' w := polyI_map q' w
            rw [hpw, hqw]
            have hw1 : poly (x::xs) w = (w-(x:Int))*poly xs w := by simp [poly]
            have hw2 : poly (x::q') w = (w-(x:Int))*poly q' w := by simp [poly]
            have heq : poly (x::xs) w = poly (x::q') w := by
              rw [h w]
              exact poly_perm_eq q (x::q') hpq' w
            rw [hw1, hw2] at heq
            have hwne : w - (x:Int) ≠ 0 := by omega
            have hc := Int.eq_of_mul_eq_mul_left hwne heq
            omega
          let f : Int → Int := fun w => polyI (xs.map (fun (v : Nat) => (v:Int))) w - polyI (q'.map (fun (v : Nat) => (v:Int))) w
          have hDp : ∀ w, D (d_tail+1) (polyI (xs.map (fun (v : Nat) => (v:Int)))) w = 0 := by
            intro w
            exact D_mono ((xs.map (fun (v : Nat) => (v:Int))).length+1) _ (D_polyI_zero _) (d_tail+1) (by dsimp [d_tail]; simp) w
          have hDq : ∀ w, D (d_tail+1) (polyI (q'.map (fun (v : Nat) => (v:Int)))) w = 0 := by
            intro w
            exact D_mono ((q'.map (fun (v : Nat) => (v:Int))).length+1) _ (D_polyI_zero _) (d_tail+1) (by dsimp [d_tail]; simp) w
          have hDf : ∀ w, D (d_tail+1) f w = 0 := by
            intro w
            dsimp [f]
            rw [D_sub]
            change D (d_tail+1) (polyI (xs.map (fun (v : Nat) => (v:Int)))) w -
              D (d_tail+1) (polyI (q'.map (fun (v : Nat) => (v:Int)))) w = 0
            rw [hDp w, hDq w]
            omega
          have hzf : ∀ t : Nat, t < d_tail+1 → f (a_tail+(t:Int)) = 0 := by
            intro t ht
            dsimp [f]
            apply hzneq (a_tail+(t:Int))
            dsimp [a_tail]
            omega
          have hall := D_all_zero_of_zeros_from (d_tail+1) f a_tail hDf hzf
          have atz := hall z
          dsimp [f] at atz
          rw [polyI_map, polyI_map] at atz
          omega
        · have hw1 : poly (x::xs) z = (z-(x:Int))*poly xs z := by simp [poly]
          have hw2 : poly (x::q') z = (z-(x:Int))*poly q' z := by simp [poly]
          have heq : poly (x::xs) z = poly (x::q') z := by
            rw [h z]
            exact poly_perm_eq q (x::q') hpq' z
          rw [hw1, hw2] at heq
          have hzneq0 : z - (x:Int) ≠ 0 := by omega
          exact Int.eq_of_mul_eq_mul_left hzneq0 heq
      have ihres := ih q' htail
      exact (List.Perm.cons x ihres).trans hpq'.symm


theorem sound_MonicFingerprint : MonicFingerprint.Goal := by
  intro a b hlen hprobes
  by_cases ha0 : a.length = 0
  · have hb0 : b.length = 0 := by omega
    have hae : a = [] := List.eq_nil_of_length_eq_zero ha0
    have hbe : b = [] := List.eq_nil_of_length_eq_zero hb0
    rw [hae, hbe]
  apply perm_of_poly_eq_all
  apply poly_eq_all_of_nat_monic a b hlen
  intro t ht
  have hg := congrArg (fun l : List Int => l[t]?) hprobes
  simpa [probes, List.getElem?_map, ht] using hg

private theorem poly_append (xs ys : List Nat) (z : Int) :
    poly (xs ++ ys) z = poly xs z * poly ys z := by
  induction xs with
  | nil => simp [poly]
  | cons x xs ih =>
      simp only [poly, List.map_cons, List.prod_cons, List.cons_append]
      change (z - (x:Int)) * poly (xs ++ ys) z = (z - (x:Int)) * poly xs z * poly ys z
      rw [ih]
      rw [Int.mul_assoc]

theorem sound_RationalDelta : RationalDelta.Goal := by
  intro ins del altIns altDel d hins hdel hz x
  have heq : ∀ t : Nat, t ≤ d → poly (ins ++ altDel) (t:Int) = poly (del ++ altIns) (t:Int) := by
    intro t ht
    rw [poly_append, poly_append]
    exact hz t ht
  have hperm : (ins ++ altDel).Perm (del ++ altIns) := by
    apply perm_of_poly_eq_all
    apply poly_eq_all_of_nat (ins ++ altDel) (del ++ altIns) d
    · simpa using hins
    · simpa using hdel
    · exact heq
  have hcount := hperm.count x
  rw [List.count_append, List.count_append] at hcount
  omega


private theorem range'_shift (s n : Nat) : List.range' (s+1) n 1 = (List.range' s n 1).map (fun i => i+1) := by
  induction n generalizing s with
  | zero => simp
  | succ n ih =>
      rw [List.range'_succ, List.range'_succ, ih s, ih (s+1)]
      rw [ih s]
      simp

private def sumRange (n : Nat) (f : Nat → Int) : Int := ((List.range' 0 n 1).map f).sum

private theorem sumRange_succ (n : Nat) (f : Nat → Int) :
    sumRange (n+1) f = f 0 + sumRange n (fun i => f (i+1)) := by
  unfold sumRange
  rw [List.range'_succ]
  rw [range'_shift]
  rw [List.map_cons, List.sum_cons]
  rw [List.map_map]
  rfl

private theorem sumRange_last (n : Nat) (f : Nat → Int) :
    sumRange (n+1) f = sumRange n f + f n := by
  induction n generalizing f with
  | zero => simp [sumRange]
  | succ n ih =>
      rw [sumRange_succ]
      have h := ih (fun i => f (i+1))
      rw [h]
      rw [sumRange_succ]
      omega

private theorem sumRange_sub (n : Nat) (f g : Nat → Int) :
    sumRange n (fun i => f i - g i) = sumRange n f - sumRange n g := by
  induction n generalizing f g with
  | zero => simp [sumRange]
  | succ n ih =>
      rw [sumRange_succ, sumRange_succ, sumRange_succ]
      rw [ih (fun i => f (i+1)) (fun i => g (i+1))]
      omega

private theorem sumRange_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    sumRange n (fun i => c * f i) = c * sumRange n f := by
  induction n generalizing f with
  | zero => simp [sumRange]
  | succ n ih =>
      rw [sumRange_succ, sumRange_succ]
      rw [ih (fun i => f (i+1))]
      rw [Int.mul_add]

private theorem sumRange_mul_const_right (n : Nat) (f : Nat → Int) (c : Int) :
    sumRange n (fun i => f i * c) = sumRange n f * c := by
  induction n generalizing f with
  | zero => simp [sumRange]
  | succ n ih =>
      rw [sumRange_succ, sumRange_succ]
      rw [ih (fun i => f (i+1))]
      rw [Int.add_mul]

private theorem sumRange_zpow (n : Nat) (a : Nat → Int) (z : Int) :
    sumRange n (fun i => a i * z^(i+1)) = z * sumRange n (fun i => a i * z^i) := by
  have hcongr : (fun i : Nat => a i * z^(i+1)) = (fun i => (a i * z^i) * z) := by
    funext i
    rw [Int.pow_succ]
    rw [Int.mul_assoc]
  rw [hcongr]
  rw [sumRange_mul_const_right]
  rw [Int.mul_comm]

private theorem sumRange_shift (n : Nat) (f : Nat → Int) :
    sumRange n (fun i => f (i+1)) = sumRange (n+1) f - f 0 := by
  rw [sumRange_succ]
  omega

private def coeff : List Nat → Nat → Int
| [], j => if j = 0 then 1 else 0
| x::xs, 0 => - (x:Int) * coeff xs 0
| x::xs, j+1 => coeff xs j - (x:Int) * coeff xs (j+1)

private theorem coeff_above (xs : List Nat) {j : Nat} (hj : xs.length < j) : coeff xs j = 0 := by
  induction xs generalizing j with
  | nil =>
      cases j with
      | zero => simp at hj
      | succ j => simp [coeff]
  | cons x xs ih =>
      cases j with
      | zero => simp at hj
      | succ j =>
          simp only [coeff, List.length_cons] at hj ⊢
          have hlt : xs.length < j ∨ xs.length = j := by omega
          rcases hlt with hlt | heq
          · rw [ih hlt, ih (by omega : xs.length < j+1)]
            simp
          · omega

private def csum : List Nat → Int → Int
| [], _ => 1
| x::xs, z => (z-(x:Int)) * csum xs z

private theorem csum_eq_sumRange (xs : List Nat) (z : Int) :
    sumRange (xs.length+1) (fun j => coeff xs j * z^j) = csum xs z := by
  induction xs with
  | nil => simp [sumRange, coeff, csum]
  | cons x xs ih =>
      rw [sumRange_succ]
      simp only [coeff, Int.pow_zero, Int.one_mul]
      have hcongr : (fun i : Nat => (coeff xs i - (x:Int)*coeff xs (i+1)) * z^(i+1)) =
          (fun i => coeff xs i * z^(i+1) - ((x:Int)*coeff xs (i+1)) * z^(i+1)) := by
        funext i
        rw [Int.sub_mul]
      rw [hcongr]
      rw [sumRange_sub]
      have hcongr2 : (fun i : Nat => ((x:Int)*coeff xs (i+1)) * z^(i+1)) =
          (fun i => (x:Int) * (coeff xs (i+1) * z^(i+1))) := by
        funext i
        rw [Int.mul_assoc]
      rw [hcongr2]
      rw [sumRange_mul_const]
      rw [sumRange_zpow]
      simp only [List.length_cons]
      have hshift : sumRange (xs.length+1) (fun i => coeff xs (i+1) * z^(i+1)) =
          sumRange (xs.length+2) (fun j => coeff xs j * z^j) - coeff xs 0 * z^0 := by
        have hC := sumRange_succ (xs.length+1) (fun j => coeff xs j * z^j)
        rw [hC]
        simp
        omega
      rw [hshift]
      rw [show sumRange (xs.length+2) (fun j => coeff xs j * z^j) =
          sumRange (xs.length+1) (fun j => coeff xs j * z^j) +
            coeff xs (xs.length+1) * z^(xs.length+1) from sumRange_last _ _]
      rw [show coeff xs (xs.length+1) = 0 from coeff_above xs (by omega)]
      simp
      have hih := ih
      rw [sumRange_succ] at hih
      simp at hih
      rw [ih]
      simp only [csum]
      rw [Int.sub_mul]
      rw [Int.mul_sub]
      rw [Int.neg_mul]
      omega


private theorem sumRange_add (n : Nat) (f g : Nat → Int) :
    sumRange n (fun i => f i + g i) = sumRange n f + sumRange n g := by
  induction n generalizing f g with
  | zero => simp [sumRange]
  | succ n ih =>
      rw [sumRange_succ, sumRange_succ, sumRange_succ]
      rw [ih (fun i => f (i+1)) (fun i => g (i+1))]
      omega

private theorem sum_xpow_shift (c : Nat → Int) (x : Int) (s m : Nat) :
    sumRange (m+1) (fun i => c (s+i) * x^i) - x * sumRange (m+1) (fun i => c (s+1+i) * x^i) =
      c s - c (s+m+1) * x^(m+1) := by
  induction m generalizing s with
  | zero =>
      simp [sumRange]
      rw [Int.pow_succ]
      simp
      rw [Int.mul_comm]
  | succ m ih =>
      have h1 : sumRange (m+1) (fun i => c (s+1+i) * x^(i+1)) =
          x * sumRange (m+1) (fun i => c (s+1+i) * x^i) :=
        sumRange_zpow (m+1) (fun i => c (s+1+i)) x
      have h2 : sumRange (m+1) (fun i => c (s+2+i) * x^(i+1)) =
          x * sumRange (m+1) (fun i => c (s+2+i) * x^i) :=
        sumRange_zpow (m+1) (fun i => c (s+2+i)) x
      have hs1 := sumRange_succ (m+1) (fun i => c (s+i) * x^i)
      have hs2 := sumRange_succ (m+1) (fun i => c (s+1+i) * x^i)
      rw [show sumRange (m+2) (fun i => c (s+i) * x^i) =
          c s + sumRange (m+1) (fun i => c (s+1+i) * x^(i+1)) by
            simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hs1]
      rw [show sumRange (m+2) (fun i => c (s+1+i) * x^i) =
          c (s+1) + sumRange (m+1) (fun i => c (s+2+i) * x^(i+1)) by
            simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hs2]
      rw [h1, h2]
      have hi := ih (s+1)
      simp only [Nat.add_assoc] at hi
      have hgroup : (c s + x * sumRange (m+1) (fun i => c (s+1+i) * x^i)) -
          x * (c (s+1) + x * sumRange (m+1) (fun i => c (s+2+i) * x^i)) =
          c s - x * c (s+1) + x * (sumRange (m+1) (fun i => c (s+1+i) * x^i) -
            x * sumRange (m+1) (fun i => c (s+2+i) * x^i)) := by
        rw [Int.mul_add, Int.mul_sub]
        omega
      rw [hgroup]
      have hi' : sumRange (m+1) (fun i => c (s+1+i) * x^i) -
          x * sumRange (m+1) (fun i => c (s+2+i) * x^i) =
          c (s+1) - c (s+m+2) * x^(m+1) := by
        simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hi
      rw [hi']
      rw [Int.mul_sub]
      rw [show s + (m+1) + 1 = s + m + 2 by omega]
      rw [show x^(m+1+1) = (x^m * x) * x by rw [Int.pow_succ, Int.pow_succ]]
      have hmul : x * (c (s+m+2) * x^(m+1)) = c (s+m+2) * (x^(m+1) * x) := by
        rw [Int.mul_comm x (c (s+m+2) * x^(m+1))]
        rw [Int.mul_assoc]
      rw [hmul]
      rw [show x^(m+1) * x = (x^m * x) * x by rw [Int.pow_succ]]
      omega

private theorem sumRange_zero (n : Nat) : sumRange n (fun _ => (0:Int)) = 0 := by
  induction n with
  | zero => simp [sumRange]
  | succ n ih => rw [sumRange_succ]; rw [ih]; simp

private theorem moment_sumRange_swap (xs : List Nat) (n : Nat) (c : Nat → Int) :
    sumRange (n+1) (fun i => c i * (moment xs i : Int)) =
      (xs.map (fun (x : Nat) => sumRange (n+1) (fun i => c i * (x:Int)^i))).sum := by
  induction xs with
  | nil =>
      simp [moment]
      apply sumRange_zero
  | cons y ys ih =>
      simp only [moment, List.map_cons, List.sum_cons]
      change sumRange (n+1) (fun i => c i * ((y:Int)^i + (moment ys i : Int))) = _
      have hcongr : (fun i : Nat => c i * ((y:Int)^i + (moment ys i : Int))) =
          (fun i => c i * (y:Int)^i + c i * (moment ys i : Int)) := by
        funext i
        rw [Int.mul_add]
      rw [hcongr]
      rw [sumRange_add]
      rw [ih]


private theorem polyI_map_eq_csum (xs : List Nat) (z : Int) :
    polyI (xs.map (fun (x : Nat) => (x:Int))) z = csum xs z := by
  induction xs with
  | nil => simp [polyI, csum]
  | cons x xs ih => simp [polyI, csum, ih]

private theorem polyI_map_zero_of_mem (xs : List Nat) (y : Nat) (hy : y ∈ xs) :
    polyI (xs.map (fun (x : Nat) => (x:Int))) (y:Int) = 0 := by
  induction xs with
  | nil => simp at hy
  | cons x xs ih =>
      simp only [List.map_cons, polyI, List.prod_cons]
      rcases List.mem_cons.mp hy with rfl | hy
      · simp
      · rw [ih hy]
        simp

private theorem sum_eq_zero_of_all_zero (l : List Int) (h : ∀ a ∈ l, a = 0) : l.sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons]
      have ha := h a (by simp)
      have ht := ih (fun b hb => h b (by simp [hb]))
      omega

private theorem moment_coeff_sum (xs : List Nat) :
    sumRange (xs.length+1) (fun i => coeff xs i * (moment xs i : Int)) = 0 := by
  rw [moment_sumRange_swap]
  apply sum_eq_zero_of_all_zero
  intro a ha
  rcases List.mem_map.mp ha with ⟨x, hx, rfl⟩
  rw [csum_eq_sumRange, ← polyI_map_eq_csum]
  exact polyI_map_zero_of_mem xs x hx


private theorem coeff_length (xs : List Nat) : coeff xs xs.length = 1 := by
  induction xs with
  | nil => simp [coeff]
  | cons x xs ih =>
    simp [coeff, ih, show coeff xs (xs.length+1) = 0 from coeff_above xs (by omega)]

private theorem sum_ones (xs : List Nat) : (xs.map (fun _ => (1:Int))).sum = xs.length := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_ones_nat (xs : List Nat) : (xs.map (fun _ : Nat => (1:Nat))).sum = xs.length := by
  induction xs with
  | nil => simp
  | cons y ys ih => simp [ih]; omega

private theorem newton_len (xs : List Nat) (r : Nat) (hr : r ≤ xs.length) :
    (r:Int)*coeff xs r = sumRange (xs.length - r + 1) (fun i => coeff xs (r+i) * (moment xs i : Int)) := by
  induction xs generalizing r with
  | nil =>
      cases r with
      | zero => simp [sumRange, coeff, moment]
      | succ r => simp at hr
  | cons x xs ih =>
      cases r with
      | zero =>
          simp only [Nat.sub_zero, List.length_cons]
          rw [show ((0:Nat):Int) = 0 by rfl]
          simp
          exact (moment_coeff_sum (x::xs)).symm
      | succ r =>
          simp only [List.length_cons, Nat.succ_sub_succ_eq_sub] at hr
          have hrle : r ≤ xs.length := by omega
          by_cases hle : r+1 ≤ xs.length
          · have ih_new := ih (r+1) hle
            have ih_prev := ih r hrle
            have hle2 : r ≤ xs.length := hrle
            have ih_prev' : (r:Int)*coeff xs r =
                sumRange (xs.length - r + 1) (fun i => coeff xs (r+i) * (moment xs i : Int)) := ih_prev
            let n := xs.length
            let m := n - (r+1)
            have hm1 : n - (r+1) + 1 = m + 1 := by rfl
            have hm2 : n - r + 1 = m + 2 := by dsimp [m, n]; omega
            have hm3 : (n+1) - (r+1) + 1 = m + 2 := by dsimp [m, n]; omega
            simp only [List.length_cons]
            rw [hm3]
            -- expand moment of cons
            have hpcons : ∀ i, (moment (x::xs) i : Int) = (x:Int)^i + (moment xs i : Int) := by
              intro i
              simp [moment]
            have hdcons : ∀ j, coeff (x::xs) (r+1+j) = coeff xs (r+j) - (x:Int)*coeff xs (r+1+j) := by
              intro j
              rw [show r + 1 + j = (r+j)+1 by omega]
              simp [coeff]
            have hsplit : sumRange (m+2) (fun i => coeff (x::xs) (r+1+i) * (moment (x::xs) i : Int)) =
                (sumRange (m+2) (fun i => coeff xs (r+i) * (x:Int)^i) -
                  x * sumRange (m+2) (fun i => coeff xs (r+1+i) * (x:Int)^i)) +
                (sumRange (m+2) (fun i => coeff xs (r+i) * (moment xs i : Int)) -
                  x * sumRange (m+2) (fun i => coeff xs (r+1+i) * (moment xs i : Int))) := by
              have hcongr : (fun i : Nat => coeff (x::xs) (r+1+i) * (moment (x::xs) i : Int)) =
                  (fun i => (coeff xs (r+i) * (x:Int)^i - x * (coeff xs (r+1+i) * (x:Int)^i)) +
                    (coeff xs (r+i) * (moment xs i : Int) - x * (coeff xs (r+1+i) * (moment xs i : Int)))) := by
                funext i
                rw [hpcons, hdcons]
                simp only [Int.sub_mul, Int.mul_add, Int.mul_assoc]
              rw [hcongr]
              rw [sumRange_add, sumRange_sub, sumRange_sub]
              have hm1' := sumRange_mul_const (m+2) (fun i => coeff xs (r+1+i) * (x:Int)^i) (x:Int)
              have hm2' := sumRange_mul_const (m+2) (fun i => coeff xs (r+1+i) * (moment xs i : Int)) (x:Int)
              omega
            rw [hsplit]
            have hxpow := sum_xpow_shift (fun j => coeff xs j) (x:Int) r (m+1)
            have hxpow' : sumRange (m+2) (fun i => coeff xs (r+i) * (x:Int)^i) -
                (x:Int) * sumRange (m+2) (fun i => coeff xs (r+1+i) * (x:Int)^i) =
                coeff xs r := by
              have hz : coeff xs (r + (m+1) + 1) = 0 := by
                have : xs.length < r + (m+1) + 1 := by dsimp [m, n] at *; omega
                exact coeff_above xs this
              have hs := hxpow
              rw [show m+1+1 = m+2 by omega] at hs
              rw [hz] at hs
              simp at hs
              have hs' : (sumRange (m+2) (fun i => coeff xs (r+i) * (x:Int)^i) -
                  (x:Int) * sumRange (m+2) (fun i => coeff xs (r+1+i) * (x:Int)^i)) = coeff xs r := by
                simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hs
              rw [hs']
            rw [hxpow']
            have hmom2 : sumRange (m+2) (fun i => coeff xs (r+i) * (moment xs i : Int)) =
                (r:Int)*coeff xs r := by
              rw [← hm2]
              exact ih_prev'.symm
            rw [hmom2]
            have hmom1 : sumRange (m+2) (fun i => coeff xs (r+1+i) * (moment xs i : Int)) =
                ((r:Int)+1)*coeff xs (r+1) := by
              have hz : coeff xs (r+1 + (m+1)) = 0 := by
                have : xs.length < r+1+(m+1) := by dsimp [m, n] at *; omega
                exact coeff_above xs this
              rw [sumRange_last]
              rw [hz]
              simp
              simpa [hm1, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih_new.symm
            rw [hmom1]
            rw [show coeff (x::xs) (r+1) = coeff xs r - (x:Int)*coeff xs (r+1) by rfl]
            rw [show ((r+1:Nat):Int) = (r:Int)+1 by simp]
            rw [show ((r:Int)+1)*coeff xs (r+1) = (r:Int)*coeff xs (r+1) + coeff xs (r+1) by rw [Int.add_mul, Int.one_mul]]
            rw [Int.mul_sub]
            rw [show ((r:Int)+1)*coeff xs r = (r:Int)*coeff xs r + coeff xs r by rw [Int.add_mul, Int.one_mul]]
            rw [show ((r:Int)+1)*((x:Int)*coeff xs (r+1)) = (x:Int)*(((r:Int)+1)*coeff xs (r+1)) by
              rw [Int.mul_comm ((r:Int)+1) ((x:Int)*coeff xs (r+1))]
              rw [Int.mul_assoc]
              rw [Int.mul_comm (coeff xs (r+1)) ((r:Int)+1)]]
            rw [show ((r:Int)+1)*coeff xs (r+1) = (r:Int)*coeff xs (r+1) + coeff xs (r+1) by rw [Int.add_mul, Int.one_mul]]
            omega
          · have hr_eq : r = xs.length := by omega
            subst r
            rw [show (x::xs).length - (xs.length+1) + 1 = 1 by simp]
            rw [show coeff (x::xs) (xs.length+1) = 1 from coeff_length (x::xs)]
            simp [sumRange, moment]
            rw [sum_ones_nat]
            rw [show coeff (x::xs) (xs.length+1) = 1 from coeff_length (x::xs)]
            omega


private theorem sumRange_eq_zero_of_all_zero (n : Nat) (f : Nat → Int)
    (h : ∀ i, i ≤ n → f i = 0) : sumRange n f = 0 := by
  let l := (List.range' 0 n 1).map f
  have hl : l.sum = 0 := by
    apply sum_eq_zero_of_all_zero
    intro a ha
    rcases List.mem_map.mp ha with ⟨i, hi, rfl⟩
    apply h i
    have hir : i ∈ List.range' 0 n 1 := hi
    have : i < n := by simpa using hir
    omega
  simpa [sumRange, l] using hl

private theorem newton_padded (xs : List Nat) (d : Nat) (hd : xs.length ≤ d) (r : Nat) (_hr : r ≤ d) :
    (r:Int)*coeff xs r = sumRange (d-r+1) (fun i => coeff xs (r+i) * (moment xs i : Int)) := by
  by_cases hrlen : r ≤ xs.length
  · have h := newton_len xs r hrlen
    have hext_core : ∀ k, sumRange (xs.length+k-r+1) (fun i => coeff xs (r+i) * (moment xs i : Int)) =
        sumRange (xs.length-r+1) (fun i => coeff xs (r+i) * (moment xs i : Int)) := by
      intro k
      induction k with
      | zero => simp
      | succ k ih =>
          rw [show xs.length+(k+1)-r+1 = (xs.length+k-r+1)+1 by omega]
          rw [sumRange_last]
          rw [show coeff xs (r+(xs.length+k-r+1)) = 0 by
            have : xs.length < r+(xs.length+k-r+1) := by omega
            exact coeff_above xs this]
          simp
          exact ih
    have hk : xs.length + (d - xs.length) = d := Nat.add_sub_of_le hd
    have hext : sumRange (d-r+1) (fun i => coeff xs (r+i) * (moment xs i : Int)) =
        sumRange (xs.length-r+1) (fun i => coeff xs (r+i) * (moment xs i : Int)) := by
      rw [← hk]
      exact hext_core (d - xs.length)
    rw [hext]
    exact h
  · have hgt : xs.length < r := by omega
    have hz : coeff xs r = 0 := coeff_above xs hgt
    rw [hz]
    simp
    exact (sumRange_eq_zero_of_all_zero (d-r+1) (fun i => coeff xs (r+i) * (moment xs i : Int)) (by
    intro i hi
    rw [show coeff xs (r+i) = 0 from coeff_above xs (by omega)]
    simp)).symm

private theorem sumRange_congr (n : Nat) (f g : Nat → Int)
    (h : ∀ i, i ≤ n → f i = g i) : sumRange n f = sumRange n g := by
  induction n generalizing f g with
  | zero => simp [sumRange]
  | succ n ih =>
      rw [sumRange_succ, sumRange_succ]
      rw [h 0 (by omega)]
      rw [ih (fun i => f (i+1)) (fun i => g (i+1)) (by intro i hi; exact h (i+1) (by omega))]

private theorem coeff_eq_of_moments (a b : List Nat) (d : Nat)
    (ha : a.length ≤ d) (hb : b.length ≤ d)
    (hm : ∀ k, k ≤ d → moment a k = moment b k) :
    ∀ j, coeff a j = coeff b j := by
  have hlen : a.length = b.length := by
    have h0 := hm 0 (by omega)
    simp [moment] at h0
    rw [sum_ones_nat, sum_ones_nat] at h0
    exact h0
  by_cases hn0 : a.length = 0
  · intro j
    have hb0 : b.length = 0 := by omega
    rw [show a = [] from List.eq_nil_of_length_eq_zero hn0,
        show b = [] from List.eq_nil_of_length_eq_zero hb0]
  · have hnpos : 0 < a.length := Nat.pos_of_ne_zero hn0
    let n := a.length
    have hdesc : ∀ m, m < n → coeff a (n-m) = coeff b (n-m) := by
      intro m
      exact Nat.strongRecOn m (motive := fun m => m < n → coeff a (n-m) = coeff b (n-m)) (fun m ih => by
          intro hm_lt
          by_cases hm0 : m = 0
          · subst m
            rw [Nat.sub_zero, coeff_length a]
            have hb_len : b.length = n := by omega
            simpa [hb_len] using (coeff_length b).symm
          · have hmp : 0 < m := Nat.pos_of_ne_zero hm0
            let r := n - m
            have hrle : r ≤ d := by dsimp [r, n]; omega
            have hrn : r < n := by dsimp [r]; omega
            have hna := newton_padded a d ha r hrle
            have hnb := newton_padded b d hb r hrle
            have hrpos : 0 < r := by dsimp [r]; omega
            have hshift : sumRange (d-r) (fun i => coeff a (r+(i+1)) * (moment a (i+1) : Int)) =
                sumRange (d-r) (fun i => coeff b (r+(i+1)) * (moment b (i+1) : Int)) := by
              apply sumRange_congr
              intro i hi
              by_cases hi_m : i < m
              · have hci : coeff a (r+(i+1)) = coeff b (r+(i+1)) := by
                  have hidx : r+(i+1) = n - (m-(i+1)) := by dsimp [r, n]; omega
                  rw [hidx]
                  exact ih (m-(i+1)) (by omega) (by omega)
                rw [hci, hm (i+1) (by omega)]
              · rw [coeff_above a (by dsimp [r, n] at *; omega),
                    coeff_above b (by dsimp [r, n] at *; omega)]
                simp
            rw [sumRange_succ] at hna hnb
            rw [hshift] at hna
            have hp0 : (moment a 0 : Int) = (n:Int) := by simp [moment, n, sum_ones_nat]
            rw [hp0] at hna
            simp at hna hnb
            have hm0b : (moment b 0 : Int) = (moment a 0 : Int) := by
              rw [hm 0 (by omega)]
            rw [hm0b] at hnb
            rw [hp0] at hnb
            have hmul : ((r:Int) - (n:Int)) * coeff a r = ((r:Int)-(n:Int)) * coeff b r := by
              rw [Int.sub_mul, Int.sub_mul]
              rw [Int.mul_comm (n:Int) (coeff a r), Int.mul_comm (n:Int) (coeff b r)]
              omega
            have hne : (r:Int) - (n:Int) ≠ 0 := by omega
            exact Int.eq_of_mul_eq_mul_left hne hmul)
    intro j
    by_cases hjle : j ≤ d
    · by_cases hjn : j = 0
      · subst j
        have hza := moment_coeff_sum a
        have hzb := moment_coeff_sum b
        rw [← hlen] at hzb
        dsimp [n] at hza hzb hdesc
        have hshift : sumRange n (fun i => coeff a (i+1) * (moment a (i+1) : Int)) =
            sumRange n (fun i => coeff b (i+1) * (moment b (i+1) : Int)) := by
          apply sumRange_congr
          intro i hi
          by_cases hi' : i+1 ≤ n
          · have hci : coeff a (i+1) = coeff b (i+1) := by
              have h := hdesc (n-(i+1)) (by omega)
              dsimp [n] at h
              have hidx : a.length - (a.length - (i+1)) = i+1 := by omega
              simpa [hidx] using h
            rw [hci, hm (i+1) (by omega)]
          · rw [coeff_above a (by omega), coeff_above b (by omega)]
            simp
        rw [sumRange_succ] at hza hzb
        change (coeff b 0 * (moment b 0 : Int) + sumRange n (fun i => coeff b (i+1) * (moment b (i+1) : Int))) = 0 at hzb
        rw [hshift] at hza
        have hp0 : (moment a 0 : Int) = (n:Int) := by simp [moment, n, sum_ones_nat]
        have hp0b : (moment b 0 : Int) = (n:Int) := by simp [moment, n, hlen, sum_ones_nat]
        rw [hp0] at hza
        rw [hp0b] at hzb
        have hmul : coeff a 0 * (n:Int) = coeff b 0 * (n:Int) := by omega
        exact Int.eq_of_mul_eq_mul_right (by omega : (n:Int) ≠ 0) hmul
      · by_cases hjn : j ≤ n
        · by_cases hjeq : j = n
          · subst j
            rw [coeff_length a]
            have hb_len : b.length = n := by omega
            simpa [hb_len] using (coeff_length b).symm
          · have hjlt : j < n := by omega
            have hidx : n - (n-j) = j := by omega
            rw [← hidx]
            exact hdesc (n-j) (by omega)
        · rw [coeff_above a (by omega), coeff_above b (by omega)]
    · rw [coeff_above a (by omega), coeff_above b (by omega)]


private theorem csum_eq_sumRange_padded (xs : List Nat) (d : Nat) (hd : xs.length ≤ d) (z : Int) :
    sumRange (d+1) (fun j => coeff xs j * z^j) = csum xs z := by
  have hcore : ∀ k, sumRange (xs.length+k+1) (fun j => coeff xs j * z^j) = csum xs z := by
    intro k
    induction k with
    | zero => simpa using csum_eq_sumRange xs z
    | succ k ih =>
        rw [show xs.length+(k+1)+1 = (xs.length+k+1)+1 by omega]
        rw [sumRange_last]
        rw [show coeff xs (xs.length+k+1) = 0 by
          exact coeff_above xs (by omega)]
        simp
        exact ih
  have hk : xs.length + (d - xs.length) = d := Nat.add_sub_of_le hd
  rw [← hk]
  simpa [Nat.add_assoc] using hcore (d - xs.length)

private theorem polyI_eq_of_moments (a b : List Nat) (d : Nat)
    (ha : a.length ≤ d) (hb : b.length ≤ d)
    (hm : ∀ k, k ≤ d → moment a k = moment b k) :
    ∀ z : Int, polyI (a.map (fun (x : Nat) => (x:Int))) z =
      polyI (b.map (fun (x : Nat) => (x:Int))) z := by
  have hcoeff := coeff_eq_of_moments a b d ha hb hm
  intro z
  rw [polyI_map_eq_csum, polyI_map_eq_csum]
  rw [← csum_eq_sumRange_padded a d ha z, ← csum_eq_sumRange_padded b d hb z]
  apply sumRange_congr
  intro j hj
  rw [hcoeff j]



private theorem two_root_eq_int (a b p : Int) :
    2*p*p + (a+b)^2 = 2*p*(a+b) + (a*a + b*b) ↔ (p-a)*(p-b) = 0 := by
  constructor
  · intro h
    have h2 : 2 * ((p-a)*(p-b)) = 0 := by
      simp only [int_pow_two, Int.add_mul, Int.mul_add] at h
      simp only [Int.mul_assoc] at h
      simp only [Int.mul_comm p a, Int.mul_comm p b, Int.mul_comm b a] at h
      simp only [Int.sub_eq_add_neg, Int.neg_add, Int.neg_mul, Int.mul_neg,
        Int.add_mul, Int.mul_add] at ⊢
      simp only [Int.mul_comm p a, Int.mul_comm p b, Int.mul_comm b a] at ⊢
      omega
    omega
  · intro h
    simp only [Int.sub_eq_add_neg, Int.neg_add, Int.neg_mul, Int.mul_neg,
      Int.add_mul, Int.mul_add] at h
    simp only [Int.mul_comm p a, Int.mul_comm p b, Int.mul_comm b a] at h
    simp only [int_pow_two, Int.add_mul, Int.mul_add]
    simp only [Int.mul_assoc]
    simp only [Int.mul_comm p a, Int.mul_comm p b, Int.mul_comm b a]
    omega

private theorem two_root_nat (a b p : Nat) :
    2*p*p + (a+b)^2 = 2*p*(a+b) + (a^2+b^2) ↔ p = a ∨ p = b := by
  constructor
  · intro h
    have hI : 2*(p:Int)*(p:Int) + ((a:Int)+(b:Int))^2 =
        2*(p:Int)*((a:Int)+(b:Int)) + ((a:Int)*(a:Int)+(b:Int)*(b:Int)) := by
      have hc := congrArg (fun n : Nat => (n : Int)) h
      simpa [int_pow_two] using hc
    have hz := (two_root_eq_int (a:Int) (b:Int) (p:Int)).mp hI
    rcases Int.mul_eq_zero.mp hz with hz | hz
    · left; exact Int.ofNat.inj (Int.sub_eq_zero.mp hz)
    · right; exact Int.ofNat.inj (Int.sub_eq_zero.mp hz)
  · intro hp
    have hz : ((p:Int) - (a:Int)) * ((p:Int) - (b:Int)) = 0 := by
      rcases hp with rfl | rfl <;> simp
    have hI := (two_root_eq_int (a:Int) (b:Int) (p:Int)).mpr hz
    apply Int.ofNat.inj
    simpa [int_pow_two] using hI

theorem sound_TwoRootMask : TwoRootMask.Goal := by
  intro xs p hlen
  match xs with
  | [] => simp at hlen
  | [a] => simp at hlen
  | a :: b :: [] =>
      simp only [List.mem_cons, List.mem_singleton]
      simp [moment]
      exact (two_root_nat a b p).symm
  | a :: b :: c :: tl => simp at hlen


private theorem moment_cons (x : Nat) (xs : List Nat) (k : Nat) :
    moment (x::xs) k = x^k + moment xs k := by simp [moment]

theorem sound_SwapDecoder : SwapDecoder.Goal := by
  intro base removed added hne
  dsimp only
  let d : Int := (moment (added::base) 1 : Int) - moment (removed::base) 1
  let q : Int := (moment (added::base) 2 : Int) - moment (removed::base) 2
  have hd : d = (added:Int) - (removed:Int) := by
    dsimp [d]
    simp [moment_cons]
    omega
  have hq : q = (added:Int)*(added:Int) - (removed:Int)*(removed:Int) := by
    dsimp [q]
    simp [moment_cons, int_pow_two]
    omega
  have hdne : d ≠ 0 := by
    intro h
    have : (added:Int) = (removed:Int) := by omega
    exact hne (Int.ofNat.inj this).symm
  have hqfac : q = d * ((added:Int) + (removed:Int)) := by
    rw [hq, hd]
    simp only [Int.sub_eq_add_neg, Int.add_mul, Int.mul_add, Int.neg_mul]
    rw [Int.mul_comm (removed:Int) (added:Int)]
    omega
  have hdiv : q / d = (added:Int) + (removed:Int) :=
    Int.ediv_eq_of_eq_mul_right hdne hqfac
  have h1 : (q / d - d) / 2 = (removed:Int) := by
    rw [hdiv, hd]
    have heq : (added:Int) + (removed:Int) - ((added:Int) - (removed:Int)) = 2 * (removed:Int) := by omega
    rw [heq]
    exact Int.ediv_eq_of_eq_mul_right (by omega : (2:Int) ≠ 0) rfl
  have h2 : (q / d + d) / 2 = (added:Int) := by
    rw [hdiv, hd]
    have heq : (added:Int) + (removed:Int) + ((added:Int) - (removed:Int)) = 2 * (added:Int) := by omega
    rw [heq]
    exact Int.ediv_eq_of_eq_mul_right (by omega : (2:Int) ≠ 0) rfl
  rw [h1, h2]

private theorem poly_cons_int (x : Nat) (xs : List Nat) (z : Int) :
    poly (x::xs) z = (z - (x:Int)) * poly xs z := by
  simp [poly]


theorem sound_DisjointResultant : DisjointResultant.Goal := by
  intro a b
  constructor
  · intro h
    induction b with
    | nil => simp [poly] at h
    | cons x xs ih =>
        simp only [List.map_cons, List.prod_cons] at h
        rcases Int.mul_eq_zero.mp h with hx | hrest
        · rcases (poly_zero_iff_mem a (x:Int)).mp hx with ⟨y, hya, hyx⟩
          refine ⟨y, hya, ?_⟩
          rw [Int.ofNat.inj hyx]
          simp
        · rcases ih hrest with ⟨y, hya, hyxs⟩
          exact ⟨y, hya, by simp [hyxs]⟩
  · rintro ⟨x, hxa, hxb⟩
    have hx0 : poly a (x:Int) = 0 := (poly_zero_iff_mem a (x:Int)).mpr ⟨x, hxa, rfl⟩
    induction b with
    | nil => simp at hxb
    | cons y ys ih =>
        rcases List.mem_cons.mp hxb with rfl | hy
        · simp only [List.map_cons, List.prod_cons]
          simp [hx0]
        · simp only [List.map_cons, List.prod_cons]
          rw [ih hy]
          simp

private theorem countP_congr {α} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.countP p = l.countP q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.countP_cons]
      rw [ih (fun x hx => h x (by simp [hx])), h a (by simp)]

private theorem countP_le_of_imp {α} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = true → q x = true) : l.countP p ≤ l.countP q := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.countP_cons]
      by_cases hp : p a = true
      · have hq : q a = true := h a (by simp) hp
        simp [hp, hq]
        exact ih (fun x hx => h x (by simp [hx]))
      · have ih' := ih (fun x hx => h x (by simp [hx]))
        simp [hp]
        omega

private theorem buckets_ge_count (xs : List Nat) (h : Nat → Nat) (key : Nat) :
    xs.count key ≤ buckets xs h key := by
  unfold buckets
  rw [List.count_eq_countP]
  apply countP_le_of_imp
  intro x hx hxeq
  have hxk : x = key := beq_iff_eq.mp hxeq
  subst x
  simp

private theorem buckets_eq_count_of_isolated (xs : List Nat) (h : Nat → Nat) (key : Nat)
    (his : ∀ x ∈ xs, h x = h key → x = key) : buckets xs h key = xs.count key := by
  unfold buckets
  rw [List.count_eq_countP]
  apply countP_congr
  intro x hx
  by_cases hxk : x = key
  · subst x
    simp
  · have hne : h x ≠ h key := by
      intro hh
      exact hxk (his x hx hh)
    have hb1 : (h x == h key) = false := Bool.eq_false_of_ne_true (by
      intro htrue
      exact hne (beq_iff_eq.mp htrue))
    have hb2 : (x == key) = false := Bool.eq_false_of_ne_true (by
      intro htrue
      exact hxk (beq_iff_eq.mp htrue))
    rw [hb1, hb2]

theorem sound_IsolatedHash : IsolatedHash.Goal := by
  intro xs hashes key his
  rw [List.min?_eq_some_iff]
  constructor
  · rcases his with ⟨h, hh, hisolate⟩
    refine List.mem_map.mpr ⟨h, hh, ?_⟩
    exact buckets_eq_count_of_isolated xs h key hisolate
  · intro b hb
    rcases List.mem_map.mp hb with ⟨h, hh, rfl⟩
    exact buckets_ge_count xs h key


private theorem int_sq_add (a b : Int) : (a+b)^2 = a*a + 2*a*b + b*b := by
  simp only [int_pow_two]
  rw [Int.add_mul, Int.mul_add, Int.mul_add]
  simp only [Int.mul_comm b a]
  rw [Int.mul_assoc]
  rw [Int.two_mul]
  omega
private theorem int_sq_sub (a b : Int) : (a-b)^2 = a*a - 2*a*b + b*b := by
  simp only [int_pow_two]
  rw [Int.sub_eq_add_neg, Int.add_mul, Int.mul_add, Int.mul_add]
  simp only [Int.neg_mul, Int.mul_neg, Int.mul_comm b a]
  rw [Int.sub_eq_add_neg]
  rw [Int.mul_assoc]
  rw [Int.two_mul]
  omega

private def pairSq : List Nat → Int
  | [] => 0
  | x :: xs => ((xs.map (fun y : Nat => ((x:Int) - (y:Int))^2)).sum) + pairSq xs

private theorem sum_nonneg_int (l : List Int) (h : ∀ a ∈ l, 0 ≤ a) : 0 ≤ l.sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons]
      have h1 := h a (by simp)
      have h2 := ih (fun b hb => h b (by simp [hb]))
      omega

private theorem pairSq_nonneg (xs : List Nat) : 0 ≤ pairSq xs := by
  induction xs with
  | nil => simp [pairSq]
  | cons x xs ih =>
      simp only [pairSq]
      have hA : 0 ≤ (xs.map (fun y : Nat => ((x:Int) - (y:Int))^2)).sum := by
        apply sum_nonneg_int
        intro a ha
        rcases List.mem_map.mp ha with ⟨y, hy, rfl⟩
        exact Int.sq_nonneg _
      omega

private theorem sum_sq_aux (x : Nat) (ys : List Nat) :
    (ys.map (fun y : Nat => (x:Int)^2 - 2*(x:Int)*(y:Int) + (y:Int)^2)).sum =
      (ys.length : Int) * (x:Int)^2 - 2*(x:Int)*(moment ys 1 : Int) + (moment ys 2 : Int) := by
  induction ys with
  | nil => simp [moment]
  | cons y ys ih =>
      rw [List.map_cons, List.sum_cons, ih]
      simp [moment, int_pow_two]
      rw [Int.mul_add]
      rw [Int.add_mul]
      simp only [Int.one_mul]
      omega

private theorem sum_sq_diff (x : Nat) (xs : List Nat) :
    (xs.map (fun y : Nat => ((x:Int) - (y:Int))^2)).sum =
      (xs.length : Int) * (x:Int)^2 - 2*(x:Int)*(moment xs 1 : Int) + (moment xs 2 : Int) := by
  have hmap : (xs.map (fun y : Nat => ((x:Int) - (y:Int))^2)) =
      (xs.map (fun y : Nat => (x:Int)^2 - 2*(x:Int)*(y:Int) + (y:Int)^2)) := by
    induction xs with
    | nil => rfl
    | cons y ys ih =>
        simp only [List.map_cons]
        rw [int_sq_sub, ih]
        simp only [int_pow_two]
  rw [hmap]
  exact sum_sq_aux x xs

private theorem cs_identity (xs : List Nat) :
    (moment xs 1 : Int)^2 + pairSq xs = (xs.length : Int) * (moment xs 2 : Int) := by
  induction xs with
  | nil => simp [moment, pairSq]
  | cons x xs ih =>
      have hlen : ((x::xs).length : Int) = (xs.length : Int) + 1 := by simp
      rw [pairSq]
      rw [sum_sq_diff]
      have h1 : (moment (x::xs) 1 : Int) = (x:Int) + (moment xs 1 : Int) := by simp [moment]
      have h2 : (moment (x::xs) 2 : Int) = (x:Int)^2 + (moment xs 2 : Int) := by simp [moment, int_pow_two]
      rw [h1, h2]
      rw [hlen]
      rw [int_sq_add]
      simp only [int_pow_two] at ih ⊢
      rw [Int.add_mul, Int.mul_add]
      rw [Int.sub_eq_add_neg, Int.mul_add]
      omega

private theorem sum_sq_eq_zero_of_sum_eq_zero (l : List Int)
    (hnon : ∀ a ∈ l, 0 ≤ a) (hsum : l.sum = 0) : ∀ a ∈ l, a = 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons] at hsum
      have ha := hnon a (by simp)
      have has_non : 0 ≤ as.sum := sum_nonneg_int as (fun b hb => hnon b (by simp [hb]))
      have ha0 : a = 0 := by omega
      have has0 : as.sum = 0 := by omega
      intro b hb
      rcases List.mem_cons.mp hb with rfl | hb
      · exact ha0
      · exact ih (fun c hc => hnon c (by simp [hc])) has0 b hb

private theorem replicate_of_all_eq (ys : List Nat) (c : Nat)
    (hall : ∀ y ∈ ys, y = c) : ys = List.replicate ys.length c := by
  induction ys with
  | nil => rfl
  | cons y ys ih =>
      have hy : y = c := hall y (by simp)
      have ht : ∀ z ∈ ys, z = c := by intro z hz; exact hall z (by simp [hz])
      rw [ih ht, hy]
      simp only [List.length_cons, List.length_replicate]
      rw [List.replicate_succ]

private def headNat : List Nat → Nat
  | [] => 0
  | x :: _ => x

private theorem all_eq_replicate (xs : List Nat) (h : pairSq xs = 0) :
    xs = List.replicate xs.length (headNat xs) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [pairSq] at h
      have hA_non : 0 ≤ (xs.map (fun y : Nat => ((x:Int) - (y:Int))^2)).sum :=
        sum_nonneg_int _ (by intro a ha; rcases List.mem_map.mp ha with ⟨y,hy,rfl⟩; exact Int.sq_nonneg _)
      have hT_non : 0 ≤ pairSq xs := pairSq_nonneg xs
      have hA0 : (xs.map (fun y : Nat => ((x:Int) - (y:Int))^2)).sum = 0 := by omega
      have hT0 : pairSq xs = 0 := by omega
      have hz := sum_sq_eq_zero_of_sum_eq_zero (xs.map (fun y : Nat => ((x:Int)-(y:Int))^2))
        (by intro a ha; rcases List.mem_map.mp ha with ⟨y,hy,rfl⟩; exact Int.sq_nonneg _) hA0
      have hall : ∀ y ∈ xs, y = x := by
        intro y hy
        have hsq : ((x:Int)-(y:Int))^2 = 0 := hz _ (List.mem_map.mpr ⟨y, hy, rfl⟩)
        have hmul : ((x:Int)-(y:Int))*((x:Int)-(y:Int)) = 0 := by simpa [int_pow_two] using hsq
        rcases Int.mul_eq_zero.mp hmul with h0 | h0
        · exact Int.ofNat.inj (Int.sub_eq_zero.mp h0).symm
        · exact Int.ofNat.inj (Int.sub_eq_zero.mp h0).symm
      have hrep : xs = List.replicate xs.length x := replicate_of_all_eq xs x hall
      simp only [List.length_cons, headNat]
      rw [hrep]
      simp only [List.length_replicate]
      rw [List.replicate_succ]

theorem sound_UniformBucket : UniformBucket.Goal := by
  intro xs hne hcond
  have hInt := congrArg (fun n : Nat => (n : Int)) hcond
  simp at hInt
  have hpair : pairSq xs = 0 := by
    have hi := cs_identity xs
    omega
  let c := headNat xs
  have hrep : xs = List.replicate xs.length c := by
    simpa [c] using all_eq_replicate xs hpair
  have hnpos : 0 < xs.length := by
    cases xs with
    | nil => contradiction
    | cons x xs => simp
  have hm : moment xs 1 = xs.length * c := by
    rw [hrep]
    simp [moment]
  rw [hm]
  rw [hrep]
  simp only [List.length_replicate]
  rw [Nat.mul_div_right c hnpos]
private theorem moment_append (a b : List Nat) (k : Nat) :
    moment (a ++ b) k = moment a k + moment b k := by
  induction a with
  | nil => simp [moment]
  | cons x xs ih =>
      simp [moment, ih]
      omega

theorem sound_PeelingVariance : PeelingVariance.Goal := by
  intro known live hlive
  dsimp only
  intro hvar
  have hcond : live.length * moment live 2 = moment live 1 ^ 2 := by
    simpa [Nat.pow_two, moment_append] using hvar
  have hub := sound_UniformBucket live hlive hcond
  intro x hx
  have hx' : x ∈ List.replicate live.length (moment live 1 / live.length) := by
    rw [hub] at hx
    exact hx
  have hmem := List.mem_replicate.mp hx'
  simpa [moment_append] using hmem.2


/-- Counterexample: a = [0,4,5], b = [1,2,6]; both have length 3, sum 9 and sum of squares 41,
    but `a.contains 4 = true` while `b.contains 4 = false`. -/
theorem refutation_ThreeFromTwo : ¬ ThreeFromTwo.Goal := by
  intro h
  have hh := h [0,4,5] [1,2,6] (by decide) (by decide) (by decide) (by decide) 4
  have h4 : ([0,4,5].contains 4) = true := by decide
  have h4' : ([1,2,6].contains 4) = false := by decide
  rw [h4, h4'] at hh
  contradiction


theorem sound_BoundedMoments : BoundedMoments.Goal := by
  intro a b d ha hb hm
  apply perm_of_poly_eq_all
  intro z
  have h := polyI_eq_of_moments a b d ha hb hm z
  rw [polyI_map, polyI_map] at h
  exact h

end TinylambOpt.ResearchProofs.Frontiers.SparseRecovery
