import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `LagProduct.RunStretch` — `∀ x n, 0 < n → measure (x.flatMap (repeated n)) = measure
  x + ((n-1:Nat):Int)*(x.map (·^2)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LagProduct_RunStretch

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-- Sum of the products over adjacent entries (the LagProduct measure). -/
private def edgeSum (x : Series) : Int :=
  ((adj x).map (fun p : Int × Int => p.1 * p.2)).sum

private theorem edgeSum_cons (a : Int) (xs : Series) :
    edgeSum (a :: xs) = (xs.head?.map (a * ·)).getD 0 + edgeSum xs := by
  unfold edgeSum
  cases xs with
  | nil => simp [adj]
  | cons b bs => rfl

private theorem edgeSum_cons_cons (a b : Int) (xs : Series) :
    edgeSum (a :: b :: xs) = a * b + edgeSum (b :: xs) := by
  rfl

private theorem edgeSum_append (x y : Series) :
    edgeSum (x ++ y) = edgeSum x + edgeSum y + seam x y (· * ·) := by
  induction x generalizing y with
  | nil => simp [edgeSum, adj, seam]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil => simp [edgeSum, adj, seam]
          | cons b bs =>
              change edgeSum (a :: b :: bs) =
                edgeSum [a] + edgeSum (b :: bs) + seam [a] (b :: bs) (· * ·)
              rw [edgeSum_cons_cons]
              simp [edgeSum, adj, seam]
              ac_rfl
      | cons b bs =>
          change edgeSum (a :: b :: (bs ++ y)) =
            edgeSum (a :: b :: bs) + edgeSum y + seam (a :: b :: bs) y (· * ·)
          rw [edgeSum_cons_cons, edgeSum_cons_cons]
          change a * b + edgeSum ((b :: bs) ++ y) =
            a * b + edgeSum (b :: bs) + edgeSum y + seam (b :: bs) y (· * ·)
          rw [ih]
          ac_rfl

private theorem sq_eq_mul (a : Int) : a ^ 2 = a * a := by grind

private theorem cast_nat_sub_one (n : Nat) (hn : 0 < n) :
    ((n - 1 : Nat) : Int) = (n : Int) - 1 := by
  cases n with
  | zero => omega
  | succ n => simp

private theorem edgeHead_replicate (a : Int) (n : Nat) :
    ((repeated n a).head?.map (a * ·)).getD 0 = if n = 0 then 0 else a * a := by
  cases n <;> simp [repeated, List.replicate_succ]

private theorem edgeSum_replicate (a : Int) (n : Nat) :
    edgeSum (repeated n a) = ((n - 1 : Nat) : Int) * a ^ 2 := by
  induction n with
  | zero => simp [edgeSum, repeated, adj]
  | succ n ih =>
      rw [show repeated (n + 1) a = a :: repeated n a by
            simp [repeated, List.replicate_succ], edgeSum_cons, ih,
        edgeHead_replicate]
      cases n with
      | zero => simp [sq_eq_mul]
      | succ m =>
          simp only [ite_eq_right (Nat.succ_ne_zero m), Nat.add_sub_cancel,
            Int.natCast_add, Int.natCast_one, sq_eq_mul]
          rw [Int.add_mul, Int.one_mul]
          ac_rfl

private theorem seam_replicate_append (a b : Int) (zs : Series) (n : Nat) (hn : 0 < n) :
    seam (repeated n a) (repeated n b ++ zs) (· * ·) = a * b := by
  cases n with
  | zero => omega
  | succ n =>
      change seam (List.replicate (n + 1) a)
        (List.replicate (n + 1) b ++ zs) (· * ·) = a * b
      unfold seam
      have hlast : (List.replicate (n + 1) a).getLast? = some a := by
        rw [List.getLast?_replicate]
        simp
      rw [hlast]
      simp only [Option.bind_some, Option.map_some, Option.getD_some,
        List.replicate_succ, List.cons_append, List.head?_cons]

private theorem edgeSum_expand_runs (x : Series) (n : Nat) (hn : 0 < n) :
    edgeSum (x.flatMap (repeated n)) =
      edgeSum x + ((n - 1 : Nat) : Int) * (x.map (· ^ 2)).sum := by
  induction x with
  | nil => simp [edgeSum, adj]
  | cons a xs ih =>
      simp only [List.flatMap_cons]
      rw [edgeSum_append, edgeSum_replicate, ih]
      cases xs with
      | nil => simp [edgeSum, adj, seam]
      | cons b bs =>
          rw [show (b :: bs).flatMap (repeated n) =
            repeated n b ++ bs.flatMap (repeated n) by rfl]
          rw [seam_replicate_append a b (bs.flatMap (repeated n)) n hn,
            edgeSum_cons_cons, edgeSum_cons]
          simp only [List.map_cons, List.sum_cons]
          have hn' := cast_nat_sub_one n hn
          rw [hn']
          simp only [sq_eq_mul, Int.sub_mul, Int.mul_add, Int.one_mul]
          ac_rfl

theorem sound_LagProduct_RunStretch : LagProduct.RunStretch.Goal := by
  intro x n hn
  simpa [LagProduct.measure, edgeSum] using edgeSum_expand_runs x n hn

end TinylambOpt.ResearchProofs.Atlas.Goal_LagProduct_RunStretch
