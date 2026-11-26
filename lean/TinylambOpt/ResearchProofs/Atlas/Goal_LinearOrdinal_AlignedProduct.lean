import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `LinearOrdinal.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map
  (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Bezout coefficient for a stride-1 lane -/

private theorem bezout_spec (a b : Nat) :
    (a.gcd b : Int) = (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 := by
  induction b using Nat.strongRecOn generalizing a with
  | ind b ih =>
      by_cases hb : b = 0
      · subst b
        simp [bezout]
      · have hmod : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
        have hrec := ih (a % b) hmod b
        have hfst : (bezout a b).1 = (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        have hsnd : (bezout a b).2 =
            (bezout b (a % b)).1 - ((a / b : Nat) : Int) * (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        rw [hfst, hsnd]
        rcases hbez : bezout b (a % b) with ⟨x, y⟩
        have hgcd : a.gcd b = b.gcd (a % b) := by
          rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
        rw [hgcd, hrec]
        have hd' : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
          exact_mod_cast (Nat.div_add_mod a b).symm
        rw [hd']
        grind

private theorem bezout_one_coeff (d : Nat) :
    (1 : Int) = (bezout 1 d).1 + (d : Int) * (bezout 1 d).2 := by
  have h := bezout_spec 1 d
  simpa using h

private theorem unit_root_sub (s d : Nat) :
    (((s : Int) * (bezout 1 d).1 - (s : Int)) % (d : Int)) = 0 := by
  have h := bezout_one_coeff d
  apply Int.emod_eq_zero_of_dvd
  refine ⟨-(s : Int) * (bezout 1 d).2, ?_⟩
  grind

private theorem intersection_linear (n s d m : Nat) (ab as bb bs : Int)
    (hn : n ≠ 0) (hm : m ≠ 0) (hd : 0 < d) :
    intersection (positions ⟨0, 1, n, ab, as⟩) (positions ⟨s, d, m, bb, bs⟩) =
      let high := min ((n - 1 : Nat) : Int) ((s : Int) + ((m - 1 : Nat) : Int) * (d : Int))
      ⟨(s : Int), d, if (s : Int) ≤ high then ((high - (s : Int)) / (d : Int)).toNat + 1 else 0⟩ := by
  simp [positions, intersection, hn, hm, Nat.ne_of_gt hd, Nat.gcd_one_left, unit_root_sub, lastVal]

/-! ## List sum helpers -/

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem rows_linear (c : Config) :
    rows LinearOrdinal.layout c = (List.range c.n).map (fun (i : Nat) => (i, c.base + c.step * (i : Int))) := by
  unfold rows
  rw [show (List.range c.n).filterMap (fun (i : Nat) =>
        (LinearOrdinal.layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun (i : Nat) => some (i, c.base + c.step * (i : Int))) from by
    apply filterMap_congr'
    intro i hi
    simp [LinearOrdinal.layout]]
  exact filterMap_some_eq_map _ _

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_range_succ (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_map_mul_left {α : Type} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_map_const {α : Type} (l : List α) (v : Int) :
    (l.map (fun _ => v)).sum = (l.length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul]; omega

private theorem sum_map_sum_comm {α β : Type} (la : List α) (lb : List β) (f : α → β → Int) :
    (la.map (fun a => (lb.map (fun b => f a b)).sum)).sum =
      (lb.map (fun b => (la.map (fun a => f a b)).sum)).sum := by
  induction la with
  | nil => simp [sum_map_const]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

private theorem sum_range_single (n target : Nat) (v : Int) :
    ((List.range n).map (fun i => if i = target then v else 0)).sum =
      if target < n then v else 0 := by
  induction n generalizing target with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_singleton]
      rw [ih]
      by_cases ht : target = n
      · subst target
        simp
      · have hnt : n ≠ target := by omega
        have htlt : target < n ∨ n < target := by omega
        rcases htlt with hlt | hgt
        · have htn' : target < n + 1 := by omega
          simp [hlt, htn', hnt]
        · have htn' : ¬target < n + 1 := by omega
          simp [show ¬target < n by omega, htn', hnt]

private theorem sum_range_if_le (Q m : Nat) (f : Nat → Int) :
    ((List.range m).map (fun j => if j ≤ Q then f j else 0)).sum =
      ((List.range (min m (Q + 1))).map f).sum := by
  induction m generalizing Q with
  | zero => simp
  | succ m ih =>
      rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_singleton]
      by_cases hQm : Q < m
      · have hmin : min (m + 1) (Q + 1) = min m (Q + 1) := by omega
        rw [hmin, ih]
        simp [show ¬m ≤ Q by omega]
      · have hQge : m ≤ Q := by omega
        have hmin : min (m + 1) (Q + 1) = m + 1 := by omega
        rw [hmin, ih]
        have hmin2 : min m (Q + 1) = m := by omega
        rw [hmin2]
        rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append, List.sum_append]
        simp [hQge]

/-! ## Closed form of a product of two aligned arithmetic progressions -/

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

private theorem squares_succ (k : Nat) :
    squares (k + 1) = squares k + (k : Nat) * k := by
  unfold squares
  have hkm1 : k + 1 - 1 = k := by omega
  rw [hkm1]
  have h2 : 2 * (k + 1) - 1 = 2 * k + 1 := by omega
  rw [h2]
  have hp : (k + 1) * k * (2 * k + 1) = k * (k - 1) * (2 * k - 1) + 6 * (k * k) := by
    by_cases hk : k = 0
    · simp [hk]
    · obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hk
      have hm : m + 1 - 1 = m := by omega
      have h2m : 2 * (m + 1) - 1 = 2 * m + 1 := by omega
      rw [hm, h2m]
      change (m + 1 + 1) * (m + 1) * (2 * (m + 1) + 1) =
        (m + 1) * m * (2 * m + 1) + 6 * ((m + 1) * (m + 1))
      grind
  rw [hp, Nat.add_mul_div_left _ _ (by decide : 0 < 6)]
  simp [Int.natCast_add, Int.natCast_mul]

private theorem sum_prod (cnt : Nat) (x dx y dy : Int) :
    ((List.range cnt).map (fun (k : Nat) => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (cnt : Int) * x * y + tri cnt * (x * dy + y * dx) + squares cnt * dx * dy := by
  induction cnt with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ k ih =>
      rw [sum_range_succ, ih]
      rw [show ((k + 1 : Nat) : Int) = (k : Int) + 1 by omega]
      rw [tri_succ, squares_succ]
      grind

private theorem aligned_linear (n s d m : Nat) (hn : n ≠ 0) (hm : m ≠ 0) (hd : 0 < d)
    (ab as bb bs : Int) :
    aligned ⟨0, 1, n, ab, as⟩ ⟨s, d, m, bb, bs⟩ =
      let high := min ((n - 1 : Nat) : Int) ((s : Int) + ((m - 1 : Nat) : Int) * (d : Int))
      let cnt := if (s : Int) ≤ high then ((high - (s : Int)) / (d : Int)).toNat + 1 else 0
      ((List.range cnt).map (fun k =>
        (ab + as * ((s + k * d : Nat) : Int)) * (bb + bs * (k : Int)))).sum := by
  unfold aligned
  rw [intersection_linear n s d m ab as bb bs hn hm hd]
  dsimp only
  simp [firstAt, slopeAt, Nat.div_self hd]
  rw [← sum_prod]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  grind

/-! ## Left-hand side as a single sum over the right lane -/

private theorem lhs_sum (n s d m : Nat) (ab as bb bs : Int) :
    ((List.range n).map (fun (i : Nat) => ((List.range m).map (fun (j : Nat) =>
      if i = s + j * d then (ab + as * (i : Int)) * (bb + bs * (j : Int)) else 0)).sum)).sum =
    ((List.range m).map (fun (j : Nat) => if s + j * d < n then
      (ab + as * ((s + j * d : Nat) : Int)) * (bb + bs * (j : Int)) else 0)).sum := by
  rw [sum_map_sum_comm]
  apply congrArg List.sum
  apply List.map_congr_left
  intro j hj
  have hfun : (fun (a : Nat) => if a = s + j * d then
        (ab + as * (a : Int)) * (bb + bs * (j : Int)) else 0) =
      (fun (a : Nat) => if a = s + j * d then
        (ab + as * ((s + j * d : Nat) : Int)) * (bb + bs * (j : Int)) else 0) := by
    funext a
    by_cases h : a = s + j * d
    · subst a
      simp
    · simp [h]
  rw [hfun, sum_range_single]

/-! ## Counting the matching ordinals -/

private theorem sum_map_eq_zero {α : Type} (l : List α) (f : α → Int)
    (h : ∀ a ∈ l, f a = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun b hb => h b (by simp [hb]))]
      simp

private theorem sum_aligned_eq (n s d m : Nat) (hn : n ≠ 0) (hm : m ≠ 0) (hd : 0 < d)
    (ab as bb bs : Int) :
    ((List.range m).map (fun (j : Nat) => if s + j * d < n then
      (ab + as * ((s + j * d : Nat) : Int)) * (bb + bs * (j : Int)) else 0)).sum =
    let high := min ((n - 1 : Nat) : Int) ((s : Int) + ((m - 1 : Nat) : Int) * (d : Int))
    let cnt := if (s : Int) ≤ high then ((high - (s : Int)) / (d : Int)).toNat + 1 else 0
    ((List.range cnt).map (fun k =>
      (ab + as * ((s + k * d : Nat) : Int)) * (bb + bs * (k : Int)))).sum := by
  have hn0 : 0 < n := Nat.pos_of_ne_zero hn
  have hm0 : 0 < m := Nat.pos_of_ne_zero hm
  have hdI : (0 : Int) < (d : Int) := by exact_mod_cast hd
  let high := min ((n - 1 : Nat) : Int) ((s : Int) + ((m - 1 : Nat) : Int) * (d : Int))
  let q := (high - (s : Int)) / (d : Int)
  change ((List.range m).map (fun (j : Nat) => if s + j * d < n then
      (ab + as * ((s + j * d : Nat) : Int)) * (bb + bs * (j : Int)) else 0)).sum =
    ((List.range (if (s : Int) ≤ high then q.toNat + 1 else 0)).map (fun k =>
      (ab + as * ((s + k * d : Nat) : Int)) * (bb + bs * (k : Int)))).sum
  by_cases hs : s < n
  ·
    have hhigh_s : high - (s : Int) = ((min (n - 1 - s) ((m - 1) * d) : Nat) : Int) := by
      dsimp [high]
      have hnsub : (n - 1 : Nat) = s + (n - 1 - s) := by omega
      rw [hnsub]
      push_cast
      omega
    have hqnon : 0 ≤ q := by
      dsimp [q]
      apply Int.ediv_nonneg
      · rw [hhigh_s]
        exact_mod_cast (Nat.zero_le (min (n - 1 - s) ((m - 1) * d)))
      · exact Int.le_of_lt hdI
    have hqcast : (q.toNat : Int) = q := Int.toNat_of_nonneg hqnon
    have hjiff : ∀ j, j < m → (s + j * d < n ↔ (j : Int) ≤ q) := by
      intro j hj
      constructor
      · intro hlt
        have hjn : j * d ≤ n - 1 - s := by omega
        have hjm : j * d ≤ (m - 1) * d := Nat.mul_le_mul_right d (by omega)
        have hminle : j * d ≤ min (n - 1 - s) ((m - 1) * d) := by omega
        have hminleI : ((j * d : Nat) : Int) ≤ ((min (n - 1 - s) ((m - 1) * d) : Nat) : Int) := by
          exact_mod_cast hminle
        rw [← hhigh_s] at hminleI
        have hmul : (j : Int) * (d : Int) ≤ high - (s : Int) := by
          push_cast at hminleI
          exact hminleI
        dsimp [q]
        exact (Int.le_ediv_iff_mul_le hdI).mpr hmul
      · intro hle
        have hmul : (j : Int) * (d : Int) ≤ high - (s : Int) := by
          dsimp [q] at hle
          exact (Int.le_ediv_iff_mul_le hdI).mp hle
        have hmul' : j * d ≤ min (n - 1 - s) ((m - 1) * d) := by
          rw [hhigh_s] at hmul
          push_cast at hmul
          exact_mod_cast hmul
        have hjn : j * d ≤ n - 1 - s := by omega
        omega
    have hqle : q.toNat + 1 ≤ m := by
      have hminle : min (n - 1 - s) ((m - 1) * d) ≤ (m - 1) * d := Nat.min_le_right _ _
      have hleInt : ((min (n - 1 - s) ((m - 1) * d) : Nat) : Int) ≤ ((m - 1 : Nat) : Int) * (d : Int) := by
        exact_mod_cast hminle
      rw [← hhigh_s] at hleInt
      have hedive : q ≤ ((m - 1 : Nat) : Int) := by
        dsimp [q]
        rw [Int.ediv_le_iff_le_mul hdI]
        omega
      rw [← hqcast] at hedive
      omega
    have hshigh : (s : Int) ≤ high := by
      dsimp [high]
      have hsle : s ≤ n - 1 := by omega
      have hmle : 0 ≤ ((m - 1 : Nat) : Int) * (d : Int) := by omega
      omega
    have hcnt : (if (s : Int) ≤ high then ((high - (s : Int)) / (d : Int)).toNat + 1 else 0) = q.toNat + 1 := by
      simp [hshigh, q]
    -- rewrite the left sum using the congruence test
    have hleft : ((List.range m).map (fun (j : Nat) => if s + j * d < n then
        (ab + as * ((s + j * d : Nat) : Int)) * (bb + bs * (j : Int)) else 0)).sum =
      ((List.range m).map (fun (j : Nat) => if j ≤ q.toNat then
        (ab + as * ((s + j * d : Nat) : Int)) * (bb + bs * (j : Int)) else 0)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro j hjmem
      have hj : j < m := List.mem_range.mp hjmem
      have hiff := hjiff j hj
      by_cases hcond : s + j * d < n
      · have hjle : (j : Int) ≤ q := hiff.mp hcond
        have hjle' : j ≤ q.toNat := by omega
        simp [hcond, hjle']
      · have hjle : ¬ (j : Int) ≤ q := by
          intro hle
          exact hcond (hiff.mpr hle)
        have hjle' : ¬ j ≤ q.toNat := by omega
        simp [hcond, hjle']
    rw [hleft, sum_range_if_le]
    have hmincnt : min m (q.toNat + 1) = q.toNat + 1 := by omega
    rw [hmincnt, hcnt]
  · have hge : n ≤ s := by omega
    have hnot : ¬ (s : Int) ≤ min ((n - 1 : Nat) : Int) ((s : Int) + ((m - 1 : Nat) : Int) * (d : Int)) := by
      have hnle : ((n - 1 : Nat) : Int) ≤ (s : Int) := by omega
      have hle2 : (s : Int) ≤ (s : Int) + ((m - 1 : Nat) : Int) * (d : Int) := by
        have : 0 ≤ ((m - 1 : Nat) : Int) * (d : Int) :=
          Int.mul_nonneg (by exact_mod_cast (Nat.zero_le (m - 1))) (Int.le_of_lt hdI)
        omega
      omega
    rw [ite_eq_right hnot]
    simp only [List.range_zero, List.map_nil, List.sum_nil]
    apply sum_map_eq_zero
    intro j hj
    have hj' := List.mem_range.mp hj
    have : ¬ s + j * d < n := by omega
    simp [this]

/-! ## Main theorem -/

theorem sound_LinearOrdinal_AlignedProduct : LinearOrdinal.AlignedProduct.Goal := by
  rintro ⟨n, split, parameter, ab, as, bias, dict⟩ ⟨s, d, m, bb, bs⟩ hd
  simp only at hd
  rw [rows_linear]
  rw [List.map_map]
  rw [Function.comp_def]
  simp only [beq_iff_eq]
  simp [LinearOrdinal.layout, lane]
  by_cases hn : n = 0
  · subst n
    simp [aligned, intersection, positions, tri, squares,
      TinylambOpt.ResearchGoals.PairAggregates.choose2]
  by_cases hm : m = 0
  · subst m
    simp [aligned, intersection, positions, tri, squares,
      TinylambOpt.ResearchGoals.PairAggregates.choose2, sum_map_eq_zero]
  rw [lhs_sum n s d m ab as bb bs]
  rw [aligned_linear n s d m hn hm hd ab as bb bs]
  exact sum_aligned_eq n s d m hn hm hd ab as bb bs

end TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_AlignedProduct
