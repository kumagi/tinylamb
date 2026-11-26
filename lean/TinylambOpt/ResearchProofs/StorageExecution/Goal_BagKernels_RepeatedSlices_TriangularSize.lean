import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_TriangularSize

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

-- Sanity checks on small inputs (empty list, all-equal keys, mixed/repeated keys).
#eval weighted bag ([] : List Nat) (fun _ => 1)
#eval weighted bag ([1, 2, 1] : List Nat) (fun _ => 1)
#eval weighted bag ([3, 1, 3, 2, 1] : List Nat) (fun _ => 1)
example : weighted bag ([] : List Nat) (fun _ => 1) = 0 := by decide
example : weighted bag ([1, 2, 1] : List Nat) (fun _ => 1) = 6 := by decide
example : weighted bag ([3, 1, 3, 2, 1] : List Nat) (fun _ => 1) = 15 := by decide

/-- `eraseDups` is duplicate-free (for a lawful BEq type). -/
private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have h : motive l.length := Nat.strongRecOn l.length (fun n ih l hl => by
    cases l with
    | nil => simp [List.eraseDups_nil]
    | cons b bs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro hb
          rw [List.mem_eraseDups] at hb
          have hb' := (List.mem_filter.mp hb).2
          simp at hb'
        · apply ih (bs.filter (fun x => !x == b)).length
          · have hle := List.length_filter_le (fun x => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

/-- Elementary closed form `n + n*(n-1)/2 = n*(n+1)/2`. -/
private theorem sum_range_closed (n : Nat) : n + n*(n-1)/2 = n*(n+1)/2 := by
  cases n with
  | zero => simp
  | succ k =>
      change (k+1) + (k+1)*k/2 = (k+1)*(k+2)/2
      rw [show (k+1)*(k+2) = (k+1)*k + (k+1)*2 by rw [Nat.mul_add]]
      rw [Nat.add_mul_div_right _ _ (by omega : 0 < 2)]
      omega

/-- Sum of `1 .. n` in closed form. -/
private theorem sum_range_map (n : Nat) :
    ((List.range n).map (fun i => i+1)).sum = n*(n+1)/2 := by
  rw [show ((List.range n).map (fun i => i+1)) = List.range' 1 n by
    rw [List.range_eq_range']
    simpa [Nat.add_comm] using (List.map_add_range' 0 n 1)]
  rw [List.sum_range']
  simpa using sum_range_closed n

/-- Total weight of `xs.zipIdx`, with ordinals starting at `i`, is `sum (i+1 .. i+n)`. -/
private theorem zipIdx_sum (i : Nat) (xs : List Nat) :
    ((xs.zipIdx i).map (fun e => e.2 + 1)).sum =
      ((List.range' i xs.length).map (fun j => j + 1)).sum := by
  induction xs generalizing i with
  | nil => simp
  | cons x xs ih =>
      simp [List.zipIdx_cons, List.range'_succ, ih]

/-- Sum of per-key weights, grouped by a list of keys. -/
private def gsumW (ps : List (Nat × Nat)) (ks : List Nat) (w : Nat → Nat) : Nat :=
  (ks.map (fun k => ((ps.filter (fun e => e.1 == k)).map (fun e => w e.2)).sum)).sum

/-- Splitting the total weight by a predicate `p` and its complement. -/
private theorem sum_filter_compl (ps : List (Nat × Nat)) (w : Nat → Nat) (p : Nat × Nat → Bool) :
    ((ps.filter p).map (fun e => w e.2)).sum + ((ps.filter (fun e => !p e)).map (fun e => w e.2)).sum =
      (ps.map (fun e => w e.2)).sum := by
  induction ps with
  | nil => simp
  | cons e ps ih =>
      by_cases h : p e = true
      · simp [h]
        omega
      · have hf : p e = false := by cases hpe : p e <;> simp_all
        simp [hf]
        omega

/-- Filtering pairs by key `k` commutes with discarding key `v`, when `v ≠ k`. -/
private theorem filter_key_ne (ps : List (Nat × Nat)) {v k : Nat} (h : v ≠ k) :
    ps.filter (fun e => e.1 == k) =
      (ps.filter (fun e => !(e.1 == v))).filter (fun e => e.1 == k) := by
  induction ps with
  | nil => simp
  | cons e ps ih =>
      by_cases hv : (e.1 == v) = true
      · have heq : e.1 = v := beq_iff_eq.mp hv
        have hk : (e.1 == k) = false := by simp [heq, h]
        simp [hv, hk, ih]
      · have hv' : (e.1 == v) = false := by cases hpe : e.1 == v <;> simp_all
        by_cases hk : (e.1 == k) = true <;> simp [hv', hk, ih]

/-- For a duplicate-free key list covering every key in `ps`, grouping by those
    keys recovers the total weight. -/
private theorem gsumW_of_nodup (ps : List (Nat × Nat)) (ks : List Nat) (w : Nat → Nat)
    (hnd : ks.Nodup) (hcov : ∀ e ∈ ps, e.1 ∈ ks) :
    gsumW ps ks w = (ps.map (fun e => w e.2)).sum := by
  induction ks generalizing ps with
  | nil =>
      have hempty : ps = [] := by
        cases ps with
        | nil => rfl
        | cons e es =>
            have hbad := hcov e (by simp)
            simp at hbad
      subst ps
      simp [gsumW]
  | cons v ks' ih =>
      rw [List.nodup_cons] at hnd
      let psO := ps.filter (fun e => !(e.1 == v))
      have hne : ∀ k ∈ ks', v ≠ k := by
        intro k hk hv
        exact hnd.1 (by simpa [hv] using hk)
      have hg_tail : gsumW ps ks' w = gsumW psO ks' w := by
        unfold gsumW
        have hmaps :
            ks'.map (fun k => ((ps.filter (fun e => e.1 == k)).map (fun e => w e.2)).sum) =
            ks'.map (fun k => ((psO.filter (fun e => e.1 == k)).map (fun e => w e.2)).sum) := by
          apply List.map_congr_left
          intro k hk
          rw [filter_key_ne ps (hne k hk)]
        exact congrArg List.sum hmaps
      have hcovO : ∀ e ∈ psO, e.1 ∈ ks' := by
        intro e he
        have heps : e ∈ ps := (List.mem_filter.mp he).1
        have hh := hcov e heps
        simp only [List.mem_cons] at hh
        cases hh with
        | inl hv_eq =>
            have hbeq : (e.1 == v) = true := by simp [hv_eq]
            have hnot : (e.1 == v) = false := by
              have ht := (List.mem_filter.mp he).2
              simpa using ht
            rw [hbeq] at hnot
            cases hnot
        | inr hk => exact hk
      have hIH := ih psO hnd.2 hcovO
      have hsplit : gsumW ps (v :: ks') w = gsumW ps [v] w + gsumW ps ks' w := by simp [gsumW]
      rw [hsplit, hg_tail, hIH]
      have hgv : gsumW ps [v] w = ((ps.filter (fun e => e.1 == v)).map (fun e => w e.2)).sum := by
        simp [gsumW]
      rw [hgv]
      exact sum_filter_compl ps w (fun e => e.1 == v)

/-- Grouping by the distinct keys of `ps` recovers the total weight. -/
private theorem sum_weights (ps : List (Nat × Nat)) (w : Nat → Nat) :
    gsumW ps (ps.map Prod.fst).eraseDups w = (ps.map (fun e => w e.2)).sum := by
  apply gsumW_of_nodup
  · exact nodup_eraseDups _
  · intro e he
    rw [List.mem_eraseDups]
    exact List.mem_map.mpr ⟨e, he, rfl⟩

theorem sound_TriangularSize : RepeatedSlices.TriangularSize.Goal := by
  intro xs
  unfold weighted bag
  dsimp only
  simp only [Nat.mul_one, id_eq]
  let ps : List (Nat × Nat) := xs.zipIdx
  have hfst : ps.map Prod.fst = xs := by
    simp [ps]
  have hwt : (ps.map (fun e => (fun i => i+1) e.2)).sum =
      ((List.range xs.length).map (fun i => i+1)).sum := by
    simpa [ps, List.range_eq_range'] using (zipIdx_sum 0 xs)
  have hmain := sum_weights ps (fun i => i+1)
  rw [hfst] at hmain
  change gsumW ps xs.eraseDups (fun i => i+1) = xs.length*(xs.length+1)/2
  rw [hmain, hwt, sum_range_map]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_TriangularSize
