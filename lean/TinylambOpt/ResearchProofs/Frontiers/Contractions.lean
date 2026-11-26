import TinylambOpt.ResearchGoals.Frontiers.Contractions

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved (9 claims, including the following)

- `ExceptionJoin` — `∀ n m l r u v (edits : List (Nat × Nat × Int)), (∀ e ∈ edits, e.1
  < n ∧ e.2.1 < m) → run n m (fun i j => l i*r j + ((edits.filter (fun (x,y,_) => x
  == i && y == j)).map (fun e => e.2.2)).sum) u v = dot n u l * dot m r v +
  (edits.map (fun (i,j,w) => u i*w*v j)).sum`
- `InvisibleDelta` — `∀ n m d (l r : Mat) (delta : Vec), (∀ h < d, dot n delta (fun i
  => l i h) = 0) → ∀ v, run n m (fun i j => total d (fun h => l i h*r h j)) delta v =
  0`
- `LatentBridge` — `∀ n h k (l r s t : Mat) i j, total n (fun x => dot h (l i) (fun p
  => r p x) * dot k (s x) (fun q => t q j)) = total h (fun p => total k (fun q => l i
  p * dot n (r p) (fun x => s x q) * t q j))`

Refuted by counterexample

- `QuotientCycles` — `∀ n (a b : Mat) (cls : Nat → Nat), (∀ i < n, ∀ c, total n (fun j
  => if cls j == c then a i j else 0) = total n (fun j => if cls j == c then b i j
  else 0)) → cycle n a a a = cycle n b b b`
-/

namespace TinylambOpt.ResearchProofs.Frontiers.Contractions

open TinylambOpt.ResearchGoals.Frontiers.Contractions

set_option maxHeartbeats 0

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map fun x => f x + g x).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map fun x => f x - g x).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_neg {α : Type} (xs : List α) (f : α → Int) :
    (xs.map fun x => - f x).sum = - (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_mul_left {α : Type} (xs : List α) (c : Int) (f : α → Int) :
    (xs.map fun x => c * f x).sum = c * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [Int.mul_add]

private theorem sum_map_mul_right {α : Type} (xs : List α) (f : α → Int) (c : Int) :
    (xs.map fun x => f x * c).sum = (xs.map f).sum * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [Int.add_mul]

private theorem sum_map_congr {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x = g x) : (xs.map f).sum = (xs.map g).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (by intro y hy; exact h y (by simp [hy]))]

private theorem sum_map_zero {α : Type} (xs : List α) : (xs.map fun _ => (0 : Int)).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem sum_map_nonneg {α : Type} (xs : List α) (f : α → Int)
    (h : ∀ x ∈ xs, 0 ≤ f x) : 0 ≤ (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht := ih (by intro y hy; exact h y (by simp [hy]))
      omega

private theorem total_congr (n : Nat) (f g : Nat → Int)
    (h : ∀ i, i < n → f i = g i) : total n f = total n g := by
  unfold total
  apply sum_map_congr
  intro x hx
  exact h x (List.mem_range.mp hx)

private theorem total_add (n : Nat) (f g : Nat → Int) :
    total n (fun i => f i + g i) = total n f + total n g := by
  unfold total
  exact sum_map_add (List.range n) f g

private theorem total_sub (n : Nat) (f g : Nat → Int) :
    total n (fun i => f i - g i) = total n f - total n g := by
  unfold total
  exact sum_map_sub (List.range n) f g

private theorem total_neg (n : Nat) (f : Nat → Int) :
    total n (fun i => - f i) = - total n f := by
  unfold total
  exact sum_map_neg (List.range n) f

private theorem total_mul_left (n : Nat) (c : Int) (f : Nat → Int) :
    total n (fun i => c * f i) = c * total n f := by
  unfold total
  exact sum_map_mul_left (List.range n) c f

private theorem total_mul_right (n : Nat) (f : Nat → Int) (c : Int) :
    total n (fun i => f i * c) = total n f * c := by
  unfold total
  exact sum_map_mul_right (List.range n) f c

private theorem total_le (n : Nat) (f g : Nat → Int)
    (h : ∀ i, i < n → f i ≤ g i) : total n f ≤ total n g := by
  induction n with
  | zero => simp [total]
  | succ n ih =>
      rw [show total (n+1) f = total n f + f n by
            simp [total, List.range_succ, List.map_append, List.sum_append],
          show total (n+1) g = total n g + g n by
            simp [total, List.range_succ, List.map_append, List.sum_append]]
      have h1 := ih (by intro i hi; exact h i (by omega))
      have h2 := h n (by omega)
      omega

private theorem total_bounds (n : Nat) (f g : Nat → Int)
    (hlo : ∀ i, i < n → -g i ≤ f i) (hup : ∀ i, i < n → f i ≤ g i) :
    - total n g ≤ total n f ∧ total n f ≤ total n g := by
  constructor
  · rw [← total_neg]
    apply total_le
    intro i hi
    exact hlo i hi
  · exact total_le n f g hup

private theorem total_zero (n : Nat) : total n (fun _ => (0 : Int)) = 0 := by
  unfold total
  exact sum_map_zero (List.range n)

private theorem sum_swap {α β : Type} (xs : List α) (ys : List β) (f : α → β → Int) :
    (xs.map fun x => (ys.map fun y => f x y).sum).sum =
      (ys.map fun y => (xs.map fun x => f x y).sum).sum := by
  induction xs with
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      exact (sum_map_zero ys).symm
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [sum_map_add]
      rw [ih]

private theorem total_swap (n m : Nat) (f : Nat → Nat → Int) :
    total n (fun i => total m (fun j => f i j)) =
      total m (fun j => total n (fun i => f i j)) := by
  unfold total
  exact sum_swap (List.range n) (List.range m) f

private theorem beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    subst b
    simp
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    subst a
    simp

private theorem total_indicator (n k : Nat) (hk : k < n) (f : Nat → Int) :
    total n (fun c => if c == k then f c else 0) = f k := by
  induction n with
  | zero => omega
  | succ n ih =>
      by_cases hkn : n = k
      · subst k
        have hz : total n (fun c => if c == n then f c else 0) = 0 := by
          calc
            total n (fun c => if c == n then f c else 0)
                = total n (fun _ => (0 : Int)) := by
                    apply total_congr
                    intro c hc
                    have hcne : c ≠ n := by omega
                    simp [hcne]
            _ = 0 := by
                    unfold total
                    exact sum_map_zero (List.range n)
        calc
          total (n+1) (fun c => if c == n then f c else 0)
              = total n (fun c => if c == n then f c else 0) + f n := by
                  simp [total, List.range_succ, List.map_append, List.sum_append]
          _ = 0 + f n := by rw [hz]
          _ = f n := by simp
      · have hklt : k < n := by omega
        calc
          total (n+1) (fun c => if c == k then f c else 0)
              = total n (fun c => if c == k then f c else 0) + 0 := by
                  simp [total, List.range_succ, List.map_append, List.sum_append, hkn]
          _ = f k := by
                  rw [ih hklt]
                  simp

private theorem neg_natAbs_le (x : Int) : -((x.natAbs : Nat) : Int) ≤ x := by
  rcases Int.natAbs_eq x with h | h
  · rw [h]
    omega
  · rw [h]
    simp

private theorem run_outer (n m : Nat) (l r u v : Vec) :
    run n m (outer l r) u v = dot n u l * dot m r v := by
  unfold run dot outer
  calc
    total n (fun i => u i * total m (fun j => l i * r j * v j))
        = total n (fun i => u i * (l i * total m (fun j => r j * v j))) := by
            apply total_congr
            intro i hi
            congr 1
            calc
              total m (fun j => l i * r j * v j)
                  = total m (fun j => l i * (r j * v j)) := by
                      apply total_congr
                      intro j hj
                      rw [Int.mul_assoc]
              _ = l i * total m (fun j => r j * v j) := by
                      rw [total_mul_left]
    _ = total n (fun i => (u i * l i) * total m (fun j => r j * v j)) := by
            apply total_congr
            intro i hi
            rw [← Int.mul_assoc]
    _ = total n (fun i => total m (fun j => (u i * l i) * (r j * v j))) := by
            apply total_congr
            intro i hi
            rw [← total_mul_left]
    _ = total m (fun j => total n (fun i => (u i * l i) * (r j * v j))) := by
            exact total_swap n m (fun i j => (u i * l i) * (r j * v j))
    _ = total m (fun j => (total n (fun i => u i * l i)) * (r j * v j)) := by
            apply total_congr
            intro j hj
            rw [total_mul_right]
    _ = total n (fun i => u i * l i) * total m (fun j => r j * v j) := by
            rw [total_mul_left]

private theorem run_add (n m : Nat) (a b : Mat) (u v : Vec) :
    run n m (plus a b) u v = run n m a u v + run n m b u v := by
  unfold run plus
  calc
    total n (fun i => u i * total m (fun j => (a i j + b i j) * v j))
        = total n (fun i => u i * (total m (fun j => a i j * v j) +
            total m (fun j => b i j * v j))) := by
            apply total_congr
            intro i hi
            congr 1
            calc
              total m (fun j => (a i j + b i j) * v j)
                  = total m (fun j => a i j * v j + b i j * v j) := by
                      apply total_congr
                      intro j hj
                      rw [Int.add_mul]
              _ = total m (fun j => a i j * v j) + total m (fun j => b i j * v j) := by
                      rw [total_add]
    _ = total n (fun i => u i * total m (fun j => a i j * v j) +
        u i * total m (fun j => b i j * v j)) := by
            apply total_congr
            intro i hi
            rw [Int.mul_add]
    _ = total n (fun i => u i * total m (fun j => a i j * v j)) +
        total n (fun i => u i * total m (fun j => b i j * v j)) := by
            rw [total_add]

private theorem cycle_plus_outer (n : Nat) (a b c : Mat) (u v : Vec) :
    cycle n (plus a (outer u v)) b c = cycle n a b c + cycle n (outer u v) b c := by
  unfold cycle plus outer
  calc
    total n (fun i => total n (fun j => total n (fun k =>
        (a i j + u i * v j) * b j k * c k i)))
        = total n (fun i => total n (fun j =>
            total n (fun k => a i j * b j k * c k i) +
            total n (fun k => u i * v j * b j k * c k i))) := by
            apply total_congr
            intro i hi
            apply total_congr
            intro j hj
            calc
              total n (fun k => (a i j + u i * v j) * b j k * c k i)
                  = total n (fun k => a i j * b j k * c k i +
                      u i * v j * b j k * c k i) := by
                      apply total_congr
                      intro k hk
                      simp only [Int.add_mul]
              _ = total n (fun k => a i j * b j k * c k i) +
                    total n (fun k => u i * v j * b j k * c k i) := by
                      rw [total_add]
    _ = total n (fun i => total n (fun j => total n (fun k => a i j * b j k * c k i)) +
          total n (fun j => total n (fun k => u i * v j * b j k * c k i))) := by
          apply total_congr
          intro i hi
          rw [total_add]
    _ = total n (fun i => total n (fun j => total n (fun k => a i j * b j k * c k i))) +
        total n (fun i => total n (fun j => total n (fun k => u i * v j * b j k * c k i))) := by
          rw [total_add]

private theorem cycle_outer (n : Nat) (u v : Vec) (b c : Mat) :
    cycle n (outer u v) b c =
      total n (fun k => dot n v (fun j => b j k) * dot n (c k) u) := by
  unfold cycle outer dot
  calc
    total n (fun i => total n (fun j => total n (fun k =>
        u i * v j * b j k * c k i)))
        = total n (fun i => total n (fun k => total n (fun j =>
            u i * v j * b j k * c k i))) := by
            apply total_congr
            intro i hi
            exact total_swap n n (fun j k => u i * v j * b j k * c k i)
    _ = total n (fun k => total n (fun i => total n (fun j =>
          u i * v j * b j k * c k i))) := by
          exact total_swap n n (fun i k => total n (fun j => u i * v j * b j k * c k i))
    _ = total n (fun k => total n (fun j => total n (fun i =>
          u i * v j * b j k * c k i))) := by
          apply total_congr
          intro k hk
          exact total_swap n n (fun i j => u i * v j * b j k * c k i)
    _ = total n (fun k => total n (fun j =>
          (v j * b j k) * total n (fun i => u i * c k i))) := by
          apply total_congr
          intro k hk
          apply total_congr
          intro j hj
          calc
            total n (fun i => u i * v j * b j k * c k i)
                = total n (fun i => (v j * b j k) * (u i * c k i)) := by
                    apply total_congr
                    intro i hi
                    simp only [Int.mul_comm, Int.mul_left_comm]
            _ = (v j * b j k) * total n (fun i => u i * c k i) := by
                    rw [total_mul_left]
    _ = total n (fun k => total n (fun j => v j * b j k) *
          total n (fun i => u i * c k i)) := by
          apply total_congr
          intro k hk
          rw [total_mul_right]
    _ = total n (fun k => total n (fun j => v j * b j k) *
          total n (fun i => c k i * u i)) := by
          apply total_congr
          intro k hk
          congr 1
          apply total_congr
          intro i hi
          rw [Int.mul_comm]

private theorem group_by_class (n m : Nat) (a reduced : Mat) (cls : Nat → Nat)
    (i : Nat) (_hi : i < n)
    (hcls : ∀ j, j < n → cls j < m)
    (hrow : ∀ c, c < m → total n (fun j => if cls j == c then a i j else 0) =
      reduced (cls i) c)
    (g : Nat → Int) :
    total n (fun j => a i j * g (cls j)) =
      total m (fun c => reduced (cls i) c * g c) := by
  symm
  calc
    total m (fun c => reduced (cls i) c * g c)
        = total m (fun c => total n (fun j => if cls j == c then a i j else 0) * g c) := by
            apply total_congr
            intro c hc
            rw [hrow c hc]
    _ = total m (fun c => total n (fun j => (if cls j == c then a i j else 0) * g c)) := by
            apply total_congr
            intro c hc
            rw [total_mul_right]
    _ = total n (fun j => total m (fun c => (if cls j == c then a i j else 0) * g c)) := by
            exact total_swap m n (fun c j => (if cls j == c then a i j else 0) * g c)
    _ = total n (fun j => a i j * g (cls j)) := by
            apply total_congr
            intro j hj
            have hswap : (fun c => if cls j == c then a i j * g c else 0) =
                (fun c => if c == cls j then (fun c => a i j * g c) c else 0) := by
              funext c
              rw [beq_comm (cls j) c]
            rw [show (fun c => (if cls j == c then a i j else 0) * g c) =
                (fun c => if cls j == c then a i j * g c else 0) by
                  funext c
                  by_cases h : cls j == c <;> simp [h]]
            rw [hswap]
            exact total_indicator m (cls j) (hcls j hj) (fun c => a i j * g c)

private theorem sum_filter_eq_map_ite {α : Type} (L : List α) (p : α → Bool) (g : α → Int) :
    ((L.filter p).map g).sum = (L.map fun x => if p x then g x else 0).sum := by
  induction L with
  | nil => rfl
  | cons x xs ih =>
      by_cases hp : p x <;> simp [hp, ih]

private theorem run_edits (n m : Nat) (edits : List (Nat × Nat × Int)) (u v : Vec)
    (hedits : ∀ e ∈ edits, e.1 < n ∧ e.2.1 < m) :
    run n m (fun i j => ((edits.filter (fun e => e.1 == i && e.2.1 == j)).map
      (fun e => e.2.2)).sum) u v =
      (edits.map (fun e => u e.1 * e.2.2 * v e.2.1)).sum := by
  unfold run
  calc
    total n (fun i => u i * total m (fun j =>
        ((edits.filter (fun e => e.1 == i && e.2.1 == j)).map (fun e => e.2.2)).sum * v j))
        = total n (fun i => u i * total m (fun j =>
            (edits.map (fun e => if e.1 == i && e.2.1 == j then e.2.2 * v j else 0)).sum)) := by
            apply total_congr
            intro i hi
            congr 1
            apply total_congr
            intro j hj
            rw [sum_filter_eq_map_ite]
            rw [← sum_map_mul_right]
            apply sum_map_congr
            intro e he
            by_cases hp : e.1 == i && e.2.1 == j <;> simp [hp]
    _ = total n (fun i => u i * ((edits.map (fun e =>
          total m (fun j => if e.1 == i && e.2.1 == j then e.2.2 * v j else 0))).sum)) := by
            apply total_congr
            intro i hi
            congr 1
            unfold total
            exact sum_swap (List.range m) edits
              (fun j e => if e.1 == i && e.2.1 == j then e.2.2 * v j else 0)
    _ = total n (fun i => u i * (edits.map (fun e =>
          if e.1 == i then e.2.2 * v e.2.1 else 0)).sum) := by
            apply total_congr
            intro i hi
            congr 1
            apply sum_map_congr
            intro e he
            by_cases hei : e.1 == i
            · have hf : (fun j => if e.1 == i && e.2.1 == j then e.2.2 * v j else 0) =
                  (fun j => if e.2.1 == j then e.2.2 * v j else 0) := by
                funext j
                simp [hei]
              rw [hf]
              have hswap : (fun j => if e.2.1 == j then e.2.2 * v j else 0) =
                  (fun j => if j == e.2.1 then (fun j => e.2.2 * v j) j else 0) := by
                funext j
                rw [beq_comm (e.2.1) j]
              rw [hswap]
              simpa [hei] using
                (total_indicator m (e.2.1) (hedits e he).2 (fun j => e.2.2 * v j))
            · calc
                total m (fun j => if e.1 == i && e.2.1 == j then e.2.2 * v j else 0)
                    = total m (fun _ => (0 : Int)) := by
                        apply total_congr
                        intro j hj
                        simp [hei]
                _ = 0 := total_zero m
                _ = (if e.1 == i then e.2.2 * v e.2.1 else 0) := by simp [hei]
    _ = total n (fun i => (edits.map (fun e =>
          u i * (if e.1 == i then e.2.2 * v e.2.1 else 0))).sum) := by
            apply total_congr
            intro i hi
            rw [← sum_map_mul_left]
    _ = (edits.map (fun e => total n (fun i =>
          u i * (if e.1 == i then e.2.2 * v e.2.1 else 0)))).sum := by
            unfold total
            exact sum_swap (List.range n) edits
              (fun i e => u i * (if e.1 == i then e.2.2 * v e.2.1 else 0))
    _ = (edits.map (fun e => u e.1 * e.2.2 * v e.2.1)).sum := by
            apply sum_map_congr
            intro e he
            have hf : (fun i => u i * (if e.1 == i then e.2.2 * v e.2.1 else 0)) =
                (fun i => if i == e.1 then (fun i => u i * (e.2.2 * v e.2.1)) i else 0) := by
              funext i
              rw [beq_comm e.1 i]
              by_cases h : i == e.1 <;> simp [h]
            rw [hf]
            rw [total_indicator n e.1 (hedits e he).1 (fun i => u i * (e.2.2 * v e.2.1))]
            rw [← Int.mul_assoc]

theorem sound_LatentJoin :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.LatentJoin.Goal := by
  intro n m rank l r u v
  unfold run dot
  calc
    total n (fun i => u i * total m (fun j =>
        (total rank (fun h => l i h * r h j)) * v j))
        = total n (fun i => u i * total m (fun j =>
            total rank (fun h => (l i h * r h j) * v j))) := by
            apply total_congr
            intro i hi
            congr 1
            apply total_congr
            intro j hj
            rw [total_mul_right]
    _ = total n (fun i => u i * total rank (fun h =>
          total m (fun j => (l i h * r h j) * v j))) := by
          apply total_congr
          intro i hi
          congr 1
          exact total_swap m rank (fun j h => (l i h * r h j) * v j)
    _ = total n (fun i => u i * total rank (fun h =>
          l i h * total m (fun j => r h j * v j))) := by
          apply total_congr
          intro i hi
          congr 1
          apply total_congr
          intro h hh
          calc
            total m (fun j => (l i h * r h j) * v j)
                = total m (fun j => l i h * (r h j * v j)) := by
                    apply total_congr
                    intro j hj
                    rw [Int.mul_assoc]
            _ = l i h * total m (fun j => r h j * v j) := by
                    rw [total_mul_left]
    _ = total n (fun i => total rank (fun h =>
          u i * (l i h * total m (fun j => r h j * v j)))) := by
          apply total_congr
          intro i hi
          rw [total_mul_left]
    _ = total rank (fun h => total n (fun i =>
          u i * (l i h * total m (fun j => r h j * v j)))) := by
          exact total_swap n rank (fun i h => u i * (l i h * total m (fun j => r h j * v j)))
    _ = total rank (fun h => total n (fun i =>
          (u i * l i h) * total m (fun j => r h j * v j))) := by
          apply total_congr
          intro h hh
          apply total_congr
          intro i hi
          rw [Int.mul_assoc]
    _ = total rank (fun h => total n (fun i => u i * l i h) *
          total m (fun j => r h j * v j)) := by
          apply total_congr
          intro h hh
          rw [total_mul_right]

theorem sound_PivotCertificate :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.PivotCertificate.Goal := by
  intro n m a p q u v hp hq hapq hfac
  unfold run dot
  have hmain :
      (total n (fun i => u i * a i q)) * (total m (fun j => a p j * v j)) =
        a p q * total n (fun i => u i * total m (fun j => a i j * v j)) := by
    calc
      (total n (fun i => u i * a i q)) * (total m (fun j => a p j * v j))
          = total n (fun i => (u i * a i q) *
              total m (fun j => a p j * v j)) := by
              rw [total_mul_right]
      _ = total n (fun i => total m (fun j =>
            (u i * a i q) * (a p j * v j))) := by
              apply total_congr
              intro i hi
              rw [total_mul_left]
      _ = total n (fun i => total m (fun j =>
            (u i * v j) * (a i q * a p j))) := by
              apply total_congr
              intro i hi
              apply total_congr
              intro j hj
              simp only [Int.mul_comm, Int.mul_left_comm]
      _ = total n (fun i => total m (fun j =>
            (u i * v j) * (a p q * a i j))) := by
              apply total_congr
              intro i hi
              apply total_congr
              intro j hj
              rw [hfac i hi j hj]
      _ = total n (fun i => total m (fun j =>
            a p q * (u i * (a i j * v j)))) := by
              apply total_congr
              intro i hi
              apply total_congr
              intro j hj
              simp only [Int.mul_comm, Int.mul_left_comm]
      _ = total n (fun i => a p q * total m (fun j => u i * (a i j * v j))) := by
              apply total_congr
              intro i hi
              rw [total_mul_left]
      _ = a p q * total n (fun i => total m (fun j => u i * (a i j * v j))) := by
              rw [total_mul_left]
      _ = a p q * total n (fun i => u i * total m (fun j => a i j * v j)) := by
              congr 1
              apply total_congr
              intro i hi
              rw [total_mul_left]
  rw [hmain, Int.mul_ediv_cancel_left _ hapq]

theorem sound_ResidualHaving :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.ResidualHaving.Goal := by
  intro n m a l r u v eps threshold hb hlt
  let E : Mat := fun i j => a i j - l i * r j
  let V : Int := total m (fun j => (v j).natAbs)
  let U : Int := total n (fun i => (u i).natAbs)
  have hVnonneg : 0 ≤ V := by
    dsimp [V]
    unfold total
    apply sum_map_nonneg
    intro j hj
    omega
  have hUnonneg : 0 ≤ U := by
    dsimp [U]
    unfold total
    apply sum_map_nonneg
    intro i hi
    omega
  have hinner : ∀ i, i < n →
      - (eps : Int) * V ≤ total m (fun j => E i j * v j) ∧
        total m (fun j => E i j * v j) ≤ (eps : Int) * V := by
    intro i hi
    have hupj : ∀ j, j < m → E i j * v j ≤ (eps : Int) * (v j).natAbs := by
      intro j hj
      calc
        E i j * v j ≤ ((E i j * v j).natAbs : Int) := Int.le_natAbs
        _ = (E i j).natAbs * (v j).natAbs := by simp [Int.natAbs_mul]
        _ ≤ (eps : Int) * (v j).natAbs := by
              have hm := Nat.mul_le_mul_right (v j).natAbs (hb i hi j hj)
              exact_mod_cast hm
    have hloj : ∀ j, j < m → -((eps : Int) * (v j).natAbs) ≤ E i j * v j := by
      intro j hj
      calc
        -((eps : Int) * (v j).natAbs) ≤ -(((E i j * v j).natAbs : Nat) : Int) := by
              have hm := Nat.mul_le_mul_right (v j).natAbs (hb i hi j hj)
              have hm' : ((E i j).natAbs : Int) * (v j).natAbs ≤ (eps : Int) * (v j).natAbs := by
                exact_mod_cast hm
              simp [Int.natAbs_mul] at hm' ⊢
              omega
        _ ≤ E i j * v j := neg_natAbs_le _
    have hbnds := total_bounds m (fun j => E i j * v j)
        (fun j => (eps : Int) * (v j).natAbs) hloj hupj
    have hsum : total m (fun j => (eps : Int) * (v j).natAbs) = (eps : Int) * V := by
      dsimp [V]
      rw [total_mul_left]
    constructor
    · have h := hbnds.1
      rw [hsum] at h
      calc
        -(eps : Int) * V = -((eps : Int) * V) := by rw [Int.neg_mul]
        _ ≤ total m (fun j => E i j * v j) := h
    · rw [hsum] at hbnds
      exact hbnds.2
  have hterm : ∀ i, i < n →
      u i * total m (fun j => E i j * v j) ≤ (u i).natAbs * ((eps : Int) * V) := by
    intro i hi
    let inner : Int := total m (fun j => E i j * v j)
    have hu := (hinner i hi).2
    have hl := (hinner i hi).1
    change inner ≤ (eps : Int) * V at hu
    change -(eps : Int) * V ≤ inner at hl
    change u i * inner ≤ (u i).natAbs * ((eps : Int) * V)
    have habs : (inner.natAbs : Int) ≤ (eps : Int) * V := by
      rcases Int.natAbs_eq inner with h1 | h1
      · rw [h1] at hu ⊢
        simpa using hu
      · rw [h1] at hl ⊢
        rw [Int.neg_mul] at hl
        simp at hl ⊢
        omega
    calc
      u i * inner ≤ ((u i * inner).natAbs : Int) := Int.le_natAbs
      _ = (u i).natAbs * (inner.natAbs : Int) := by simp [Int.natAbs_mul]
      _ ≤ (u i).natAbs * ((eps : Int) * V) := by
            exact Int.mul_le_mul_of_nonneg_left habs (by omega)
  have hrunE :
      total n (fun i => u i * total m (fun j => E i j * v j)) ≤
        total n (fun i => (u i).natAbs * ((eps : Int) * V)) := by
      apply total_le
      intro i hi
      exact hterm i hi
  have hRHS : total n (fun i => (u i).natAbs * ((eps : Int) * V)) =
      (eps : Int) * U * V := by
    dsimp [U]
    rw [total_mul_right]
    simp only [Int.mul_assoc, Int.mul_left_comm]
  have hdecomp : run n m a u v =
      dot n u l * dot m r v +
        total n (fun i => u i * dot m (E i) v) := by
    have ha : a = plus (outer l r) E := by
      funext i j
      dsimp [plus, outer, E]
      omega
    rw [ha, run_add, run_outer]
    unfold run
    rfl
  rw [hdecomp]
  have hle : total n (fun i => u i * dot m (E i) v) ≤
      (eps : Int) * U * V := by
    change total n (fun i => u i * total m (fun j => E i j * v j)) ≤
      (eps : Int) * U * V
    rw [← hRHS]
    exact hrunE
  change dot n u l * dot m r v + (eps : Int) * U * V < threshold at hlt
  omega

theorem sound_TriangleFactors :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.TriangleFactors.Goal := by
  intro n a b c d e f
  unfold cycle dot
  let B : Int := total n (fun j => b j * c j)
  let D : Int := total n (fun k => d k * e k)
  let A : Int := total n (fun i => f i * a i)
  calc
    total n (fun i => total n (fun j => total n (fun k =>
        (a i * b j) * (c j * d k) * (e k * f i))))
        = total n (fun i => total n (fun j =>
            (a i * b j * c j * f i) * D)) := by
            apply total_congr
            intro i hi
            apply total_congr
            intro j hj
            calc
              total n (fun k => (a i * b j) * (c j * d k) * (e k * f i))
                  = total n (fun k => (a i * b j * c j * f i) * (d k * e k)) := by
                      apply total_congr
                      intro k hk
                      simp only [Int.mul_comm, Int.mul_left_comm]
              _ = (a i * b j * c j * f i) * D := by
                      rw [total_mul_left]
    _ = total n (fun i => (a i * f i * D) * B) := by
            apply total_congr
            intro i hi
            calc
              total n (fun j => (a i * b j * c j * f i) * D)
                  = total n (fun j => (a i * f i * D) * (b j * c j)) := by
                      apply total_congr
                      intro j hj
                      simp only [Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
              _ = (a i * f i * D) * B := by
                      rw [total_mul_left]
    _ = total n (fun i => (f i * a i) * (D * B)) := by
            apply total_congr
            intro i hi
            simp only [Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
    _ = A * (D * B) := by
            rw [total_mul_right]
    _ = dot n b c * dot n d e * dot n f a := by
            simp only [dot, A, B, D]
            rw [Int.mul_comm D B, Int.mul_comm A (B * D)]

theorem sound_RankUpdate :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.RankUpdate.Goal := by
  intro n a b c u v
  rw [cycle_plus_outer, cycle_outer]
  omega

theorem sound_InvisibleDelta :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.InvisibleDelta.Goal := by
  intro n m d l r delta hdelta v
  unfold dot at hdelta
  unfold run dot
  calc
    total n (fun i => delta i * total m (fun j =>
        total d (fun h => l i h * r h j) * v j))
        = total n (fun i => delta i * total m (fun j =>
            total d (fun h => (l i h * r h j) * v j))) := by
            apply total_congr
            intro i hi
            congr 1
            apply total_congr
            intro j hj
            rw [total_mul_right]
    _ = total n (fun i => delta i * total d (fun h =>
          total m (fun j => (l i h * r h j) * v j))) := by
          apply total_congr
          intro i hi
          congr 1
          exact total_swap m d (fun j h => (l i h * r h j) * v j)
    _ = total n (fun i => delta i * total d (fun h =>
          l i h * total m (fun j => r h j * v j))) := by
          apply total_congr
          intro i hi
          congr 1
          apply total_congr
          intro h hh
          calc
            total m (fun j => (l i h * r h j) * v j)
                = total m (fun j => l i h * (r h j * v j)) := by
                    apply total_congr
                    intro j hj
                    rw [Int.mul_assoc]
            _ = l i h * total m (fun j => r h j * v j) := by
                    rw [total_mul_left]
    _ = total n (fun i => total d (fun h =>
          delta i * (l i h * total m (fun j => r h j * v j)))) := by
          apply total_congr
          intro i hi
          rw [total_mul_left]
    _ = total d (fun h => total n (fun i =>
          delta i * (l i h * total m (fun j => r h j * v j)))) := by
          exact total_swap n d (fun i h => delta i * (l i h * total m (fun j => r h j * v j)))
    _ = total d (fun h => total n (fun i =>
          (delta i * l i h) * total m (fun j => r h j * v j))) := by
          apply total_congr
          intro h hh
          apply total_congr
          intro i hi
          rw [Int.mul_assoc]
    _ = total d (fun h => (total n (fun i => delta i * l i h)) *
          total m (fun j => r h j * v j)) := by
          apply total_congr
          intro h hh
          rw [total_mul_right]
    _ = total d (fun h => (0 : Int) * total m (fun j => r h j * v j)) := by
          apply total_congr
          intro h hh
          rw [hdelta h hh]
    _ = 0 := by
          simp only [Int.zero_mul]
          unfold total
          exact sum_map_zero (List.range d)

theorem sound_QuotientWalk :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.QuotientWalk.Goal := by
  intro n m a reduced cls endWeight hcls hrow depth
  induction depth with
  | zero =>
      intro i hi
      rfl
  | succ depth ih =>
      intro i hi
      unfold walk step
      calc
        total n (fun j => a i j * walk n a (fun j => endWeight (cls j)) depth j)
            = total n (fun j => a i j * walk m reduced endWeight depth (cls j)) := by
                apply total_congr
                intro j hj
                rw [ih j hj]
        _ = total m (fun c => reduced (cls i) c * walk m reduced endWeight depth c) := by
                exact group_by_class n m a reduced cls i hi hcls
                  (fun c hc => hrow i hi c hc) (walk m reduced endWeight depth)

/-- Counterexample: n = 6, cls = fun _ => 0 (one class),
  tri i j = 1 iff i != j and i/3 = j/3 (two disjoint triangles),
  ring i j = 1 iff (i+1)%6 = j or (j+1)%6 = i (the 6-cycle).
  Every vertex has degree 2, so the class-row sums agree, but
  cycle 6 tri tri tri = 12 while cycle 6 ring ring ring = 0. -/
theorem refutation_QuotientCycles : ¬ TinylambOpt.ResearchGoals.Frontiers.Contractions.QuotientCycles.Goal := by
  intro h
  let tri : Mat := fun i j => if i != j && i / 3 == j / 3 then 1 else 0
  let ring : Mat := fun i j => if (i + 1) % 6 == j || (j + 1) % 6 == i then 1 else 0
  let cls : Nat → Nat := fun _ => 0
  have hprem : ∀ i < 6, ∀ c,
      total 6 (fun j => if cls j == c then tri i j else 0) =
      total 6 (fun j => if cls j == c then ring i j else 0) := by
    intro i hi c
    by_cases hc : c = 0
    · subst c
      have hrowall : ∀ i ∈ List.range 6,
          total 6 (fun j => if cls j == 0 then tri i j else 0) =
          total 6 (fun j => if cls j == 0 then ring i j else 0) := by
            native_decide
      exact hrowall i (List.mem_range.mpr hi)
    · have h0c : ((0 : Nat) == c) = false := by
        by_cases h' : (0 == c) = true
        · have hz : (0 : Nat) = c := beq_iff_eq.mp h'
          exact False.elim (hc hz.symm)
        · exact Bool.eq_false_of_ne_true h'
      simp [cls, h0c]
  have heq := h 6 tri ring cls hprem
  have htri : cycle 6 tri tri tri = 12 := by native_decide
  have hring : cycle 6 ring ring ring = 0 := by native_decide
  rw [htri, hring] at heq
  omega

theorem sound_ExceptionJoin :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.ExceptionJoin.Goal := by
  intro n m l r u v edits hedits
  let E : Mat := fun i j => ((edits.filter (fun e => e.1 == i && e.2.1 == j)).map
    (fun e => e.2.2)).sum
  have ha : (fun i j => l i * r j + E i j) = plus (outer l r) E := by
    funext i j
    rfl
  change run n m (fun i j => l i * r j + E i j) u v =
    dot n u l * dot m r v + (edits.map (fun e => u e.1 * e.2.2 * v e.2.1)).sum
  rw [ha, run_add, run_outer, run_edits n m edits u v hedits]

theorem sound_LatentBridge :
    TinylambOpt.ResearchGoals.Frontiers.Contractions.LatentBridge.Goal := by
  intro n h k l r s t i j
  unfold dot
  calc
    total n (fun x => total h (fun p => l i p * r p x) *
        total k (fun q => s x q * t q j))
        = total n (fun x => total h (fun p =>
            (l i p * r p x) * total k (fun q => s x q * t q j))) := by
            apply total_congr
            intro x hx
            rw [total_mul_right]
    _ = total n (fun x => total h (fun p => total k (fun q =>
          (l i p * r p x) * (s x q * t q j)))) := by
          apply total_congr
          intro x hx
          apply total_congr
          intro p hp
          rw [total_mul_left]
    _ = total h (fun p => total n (fun x => total k (fun q =>
          (l i p * r p x) * (s x q * t q j)))) := by
          exact total_swap n h (fun x p => total k (fun q => (l i p * r p x) * (s x q * t q j)))
    _ = total h (fun p => total k (fun q => total n (fun x =>
          (l i p * r p x) * (s x q * t q j)))) := by
          apply total_congr
          intro p hp
          exact total_swap n k (fun x q => (l i p * r p x) * (s x q * t q j))
    _ = total h (fun p => total k (fun q =>
          (l i p * t q j) * total n (fun x => r p x * s x q))) := by
          apply total_congr
          intro p hp
          apply total_congr
          intro q hq
          calc
            total n (fun x => (l i p * r p x) * (s x q * t q j))
                = total n (fun x => (l i p * t q j) * (r p x * s x q)) := by
                    apply total_congr
                    intro x hx
                    simp only [Int.mul_comm, Int.mul_left_comm]
            _ = (l i p * t q j) * total n (fun x => r p x * s x q) := by
                    rw [total_mul_left]
    _ = total h (fun p => total k (fun q =>
          l i p * total n (fun x => r p x * s x q) * t q j)) := by
          apply total_congr
          intro p hp
          apply total_congr
          intro q hq
          simp only [Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]

end TinylambOpt.ResearchProofs.Frontiers.Contractions
