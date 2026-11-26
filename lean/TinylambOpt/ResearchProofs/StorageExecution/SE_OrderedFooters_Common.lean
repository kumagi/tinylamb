import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

`edges` of an appended non-empty pair, with the bridging edge spelled out.

Named results include `foldl_map_reference`, `combine_ref`, `treeReduce_of_stitch`.
-/
set_option maxHeartbeats 0

/-! Generic machinery: every Fold contract follows from `stitch` alone. -/
namespace TinylambOpt.ResearchProofs.StorageExecution.SE_ORF
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.Compression.Wire

/-- Linear-arithmetic-with-atoms normalizer for Nat (no Mathlib `ring`). -/
syntax "nring" : tactic
macro_rules | `(tactic| nring) => `(tactic| simp only [Nat.add_mul, Nat.mul_add, Nat.mul_one,
  Nat.mul_zero, Nat.one_mul, Nat.zero_mul, Nat.add_zero, Nat.zero_add, Nat.add_comm,
  Nat.add_left_comm, Nat.add_assoc, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc])

theorem foldl_map_reference {f : Fold σ} (hst : stitch f) (acc : List Nat)
    (ps : List (List Nat)) :
    (ps.map f.reference).foldl f.join (f.reference acc) = f.reference (acc ++ ps.flatten) := by
  induction ps generalizing acc with
  | nil => simp
  | cons p ps ih =>
    rw [List.map_cons, List.foldl_cons, hst, ih, List.flatten_cons, ← List.append_assoc]

theorem combine_ref {f : Fold σ} (hst : stitch f) (ps : List (List Nat)) :
    combine f ps = f.reference ps.flatten := by
  unfold combine
  exact foldl_map_reference hst [] ps

theorem treeReduce_of_stitch {f : Fold σ} (hst : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
    show f.join (rootState f a) (rootState f b) = f.reference (plain a ++ plain b)
    rw [iha, ihb]
    exact hst _ _

theorem replicate_add_gen (xs : List Nat) : ∀ a b,
    List.replicate (a + b) xs = List.replicate a xs ++ List.replicate b xs := by
  intro a
  induction a with
  | zero => intro b; simp
  | succ a ih => intro b; rw [Nat.succ_add, List.replicate_succ, ih b, List.replicate_succ, List.cons_append]

theorem flatten_replicate_even (xs : List Nat) (k : Nat) :
    (List.replicate (k + k) xs).flatten =
      (List.replicate k xs).flatten ++ (List.replicate k xs).flatten := by
  rw [replicate_add_gen, List.flatten_append]

theorem flatten_replicate_odd (xs : List Nat) (k : Nat) :
    (List.replicate k xs).flatten ++ xs = (List.replicate (k + 1) xs).flatten := by
  induction k with
  | zero => simp
  | succ k ih => simp [List.replicate_succ, ih]

theorem repeatState_zero {f : Fold σ} (s : σ) : repeatState f s 0 = f.reference [] := by
  unfold repeatState; rfl

theorem repeatPower_of_stitch {f : Fold σ} (hst : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | ind n ih =>
    by_cases hz : n = 0
    · subst hz
      rw [repeatState_zero]
      simp
    · have hex : ∃ q, n = q + q ∨ n = q + q + 1 := ⟨n / 2, by omega⟩
      obtain ⟨q, hq⟩ := hex
      cases hq with
      | inl hq =>
        subst hq
        by_cases hq0 : q = 0
        · subst hq0
          rw [repeatState_zero]
          simp
        · have key : repeatState f (f.reference xs) (q + q) =
                f.join (repeatState f (f.reference xs) q) (repeatState f (f.reference xs) q) := by
            conv => lhs; unfold repeatState
            simp [hq0, (by omega : (q + q) % 2 = 0), (by omega : (q + q) / 2 = q)]
          rw [key, ih q (by omega), hst, flatten_replicate_even]
      | inr hq =>
        subst hq
        have key : repeatState f (f.reference xs) (q + q + 1) =
            f.join (f.join (repeatState f (f.reference xs) q)
              (repeatState f (f.reference xs) q)) (f.reference xs) := by
          conv => lhs; unfold repeatState
          simp [(by omega : (q + q + 1) % 2 = 1), (by omega : (q + q + 1) / 2 = q)]
        rw [key, ih q (by omega), hst, hst,
          ← flatten_replicate_odd, ← flatten_replicate_even]

theorem drop_take_frame (a m z : List Nat) (lo hi : Nat) (h : lo ≤ a.length) :
    ((a ++ m ++ z).drop lo).take (a.length - lo + m.length + hi) = a.drop lo ++ m ++ z.take hi := by
  rw [List.append_assoc]
  have h1 : (a ++ (m ++ z)).drop lo = a.drop lo ++ (m ++ z) := List.drop_append_of_le_length h
  have h2 : a.length - lo + m.length + hi = (a.drop lo).length + (m.length + hi) := by
    rw [List.length_drop]; omega
  rw [h1, h2, List.take_length_add_append (l₂ := m ++ z) (i := m.length + hi)]
  rw [List.take_length_add_append (l₁ := m) (l₂ := z) (i := hi)]
  rw [List.append_assoc]

theorem map_range_succ' {α : Type} (g : Nat → α) (n : Nat) :
    (List.range (n + 1)).map g = g 0 :: (List.range n).map (fun i => g (i + 1)) := by
  rw [List.range_succ_eq_map, List.map_cons, List.map_map]
  rw [show (g ∘ Nat.succ) = (fun i => g (i + 1))
      from funext (fun i => congrArg g (Nat.add_one i).symm)]

theorem flatten_set {ps : List (List Nat)} (i : Nat) (xs : List Nat) (h : i < ps.length) :
    (ps.set i xs).flatten = (ps.take i).flatten ++ xs ++ (ps.drop (i + 1)).flatten := by
  induction ps generalizing i with
  | nil => simp at h
  | cons p ps ih =>
    cases i with
    | zero =>
      rcases ps with _ | q
      · simp [List.set]
      · simp [List.take, List.drop]
    | succ k =>
      rw [List.set_cons_succ, List.flatten_cons, List.take_succ_cons, List.drop_succ_cons]
      rw [ih k (by simpa using h)]
      simp [List.flatten_cons, List.append_assoc]

theorem prefixCheckpoint_of_stitch {f : Fold σ} (hst : stitch f) : prefixCheckpoint f := by
  intro init ps
  induction ps generalizing init with
  | nil => simp
  | cons p ps ih =>
    rw [List.map_cons, List.scanl_cons, List.length_cons]
    have h1 : f.join (f.reference init) (f.reference p) = f.reference (init ++ p) := hst _ _
    rw [h1, ih (init ++ p)]
    conv => rhs; rw [map_range_succ']
    simp only [List.take_zero, List.flatten_nil, List.append_nil, List.cons.injEq, and_self,
      and_true, true_and]
    congr 1
    funext i
    refine congrArg f.reference ?_
    rw [List.take_succ_cons, List.flatten_cons, ← List.append_assoc]

theorem amendLeaf_of_stitch {f : Fold σ} (hst : stitch f) : amendLeaf f := by
  intro ps i xs hi
  rw [combine_ref hst, combine_ref hst, combine_ref hst,
    hst xs, hst (List.flatten (ps.take i)), flatten_set i xs hi, List.append_assoc]

theorem flatten_replicate_single (n v : Nat) : (List.replicate n [v]).flatten = List.replicate n v := by
  induction n with
  | zero => rfl
  | succ n ih => simp [List.replicate_succ, ih, List.flatten_cons]

theorem runInput_of_stitch {f : Fold σ} (hst : stitch f) (rp : repeatPower f) : runInput f := by
  intro rs
  have key : ∀ (v n : Nat), repeatState f (f.reference [v]) n =
      f.reference (List.replicate n v) := by
    intro v n
    rw [rp, flatten_replicate_single]
  have : ((rs.map (fun vn => repeatState f (f.reference [vn.1]) vn.2)).foldl f.join
      (f.reference []) : σ)
      = f.reference (rs.flatMap (fun vn => List.replicate vn.2 vn.1)) := by
    simp only [key]
    have hc : combine f (rs.map (fun vn => List.replicate vn.2 vn.1)) =
        ((rs.map (fun vn => f.reference (List.replicate vn.2 vn.1))).foldl
          f.join (f.reference [])) := by
      unfold combine
      rw [List.map_map]
      rw [show (f.reference ∘ (fun vn : Nat × Nat => List.replicate vn.2 vn.1)) =
            (fun vn : Nat × Nat => f.reference (List.replicate vn.2 vn.1)) from rfl]
    rw [← hc, combine_ref hst]
    have hf : (rs.map (fun vn => List.replicate vn.2 vn.1)).flatten
        = rs.flatMap (fun vn => List.replicate vn.2 vn.1) := by
      induction rs with
      | nil => rfl
      | cons x xs ih => simp [List.map_cons, ih, List.flatMap]
    rw [hf]
  exact this

theorem join_assoc_reachable {f : Fold σ} (hst : stitch f) (a b c : List Nat) :
    f.join (f.join (f.reference a) (f.reference b)) (f.reference c)
      = f.join (f.reference a) (f.join (f.reference b) (f.reference c)) := by
  rw [hst, hst, List.append_assoc, ← hst, ← hst]

theorem reorderNeighbors_of_stitch {f : Fold σ} (hst : stitch f) : reorderNeighbors f := by
  intro pre a b suf h
  have h1 : f.reference (a ++ b) = f.reference (b ++ a) := by rw [← hst, ← hst, h]
  have H1 : f.reference (((pre ++ a) ++ b) ++ suf)
      = f.join (f.reference pre) (f.join (f.reference (a ++ b)) (f.reference suf)) := by
    rw [List.append_assoc, List.append_assoc,
      ← List.append_assoc (as := a) (bs := b) (cs := suf), ← hst, ← hst]
  have H2 : f.reference (((pre ++ b) ++ a) ++ suf)
      = f.join (f.reference pre) (f.join (f.reference (b ++ a)) (f.reference suf)) := by
    rw [List.append_assoc, List.append_assoc,
      ← List.append_assoc (as := b) (bs := a) (cs := suf), ← hst, ← hst]
  rw [H1, H2, h1]

theorem frameSlice_of_stitch {f : Fold σ} (hst : stitch f) : frameSlice f := by
  intro a ps z lo hi hle
  rw [combine_ref hst, ← join_assoc_reachable hst, hst, hst,
    drop_take_frame a ps.flatten z lo hi hle]

theorem peak_cons (x : Nat) (xs : List Nat) : peak (x :: xs) = max x (peak xs) := by
  cases xs with
  | nil => simp [peak]
  | cons y ys =>
    unfold peak
    rw [List.max?_cons, List.max?_cons]
    rcases (y :: ys).max? with _ | m
    · simp
    · simp

theorem le_peak_of_mem {xs : List Nat} (h : x ∈ xs) : x ≤ peak xs := by
  induction xs with
  | nil => simp at h
  | cons y ys ih =>
    rw [List.mem_cons] at h
    rcases h with rfl | h
    · rw [peak_cons]; exact Nat.le_max_left _ _
    · rw [peak_cons]; exact Nat.le_trans (ih h) (Nat.le_max_right _ _)

theorem peak_le_of_le {xs : List Nat} (h : ∀ x ∈ xs, x ≤ n) : peak xs ≤ n := by
  induction xs with
  | nil => simp [peak]
  | cons y ys ih =>
    rw [peak_cons]
    refine Nat.max_le.mpr ⟨h y (by simp), ih fun x hx => h x (by simp [hx])⟩

/-- `edges` of an appended non-empty pair, with the bridging edge spelled out. -/
theorem edges_cons_three (y w : Nat) (ws : List Nat) :
    edges (y :: w :: ws) = (y, w) :: edges (w :: ws) := rfl

theorem edges_cons_nonempty (y : Nat) (l : List Nat) (hl : l ≠ []) :
    edges (y :: l) = (y, l.head?.getD 0) :: edges l := by
  rcases l with _ | ⟨w, ws⟩
  · exact absurd hl (by simp)
  · rfl

theorem edges_append (u v : List Nat) (hu : u ≠ []) (hv : v ≠ []) :
    edges (u ++ v) = edges u ++ ((u.getLast?.getD 0, v.head?.getD 0) :: edges v) := by
  induction u with
  | nil => exact absurd hu (by simp)
  | cons y t ih =>
    cases t with
    | nil =>
      rcases v with _ | ⟨w, ws⟩
      · exact absurd hv (by simp)
      · rfl
    | cons z zs =>
      rw [List.cons_append]
      have hl : ((z :: zs) ++ v) ≠ [] := by rw [List.cons_append]; simp
      have hhead : ((z :: zs) ++ v).head?.getD 0 = z := by rw [List.cons_append]; rfl
      rw [edges_cons_nonempty _ _ hl, hhead, ih (by simp)]
      rfl

theorem exists_append_last (xs : List Nat) (h : xs ≠ []) :
    ∃ as a, xs = as ++ [a] := by
  induction xs with
  | nil => exact absurd h (by simp)
  | cons y t ih =>
    cases t with
    | nil => exact ⟨[], y, by simp⟩
    | cons z zs =>
      obtain ⟨as, a, h⟩ := ih (by simp)
      exact ⟨y :: as, a, by rw [h, List.cons_append]⟩

theorem sum_le_sum_of_le {α : Type} (f g : α → Nat) (l : List α)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons y ys ih =>
    rw [List.map_cons, List.map_cons, List.sum_cons, List.sum_cons]
    exact Nat.add_le_add (h y (by simp)) (ih fun x hx => h x (by simp [hx]))

theorem getD_last_cons (y : Nat) (ya : List Nat) :
    (y :: ya).getLast?.getD 0 = (y :: ya).getLast?.getD y := by
  cases ya with
  | nil => rfl
  | cons w ws => rfl

theorem last_getD_append_cons (y : Nat) (ya zb : List Nat) (z : Nat) :
    ((y :: (ya ++ z :: zb)).getLast?.getD y) = ((z :: zb).getLast?.getD z) := by
  rcases h : (z :: zb).getLast? with _ | v
  · have : (z :: zb).getLast? ≠ none := by
      rcases zb with _ | ⟨w, ws⟩
      · simp
      · rw [List.getLast?_cons]; simp
    exact absurd h this
  · rw [show (y :: (ya ++ z :: zb) : List Nat) = ((y :: ya) ++ (z :: zb)) from rfl,
      List.getLast?_append, h]
    have h2 : (((some v).or ((y :: ya).getLast?)) : Option Nat) = some v := rfl
    rw [h2, Option.getD_some, Option.getD_some]
