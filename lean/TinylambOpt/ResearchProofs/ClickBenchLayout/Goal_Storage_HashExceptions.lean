import TinylambOpt.ResearchGoals.ClickBenchLayout.Storage

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Storage_HashExceptions

open TinylambOpt.ResearchGoals.ClickBenchLayout.Storage
open TinylambOpt.ResearchGoals.ClickBenchLayout.Storage.HashExceptions
open HashExceptions

def hs (rows : List Row) (k : Nat) : List UInt64 :=
  rows.filterMap (fun r => if r.1 == k then some r.2 else none)

def total (f : Nat → UInt64) (R : List Row) : Nat := R.countP (fun r => f r.1 == r.2)

def rm (k0 : Nat) (R : List Row) : List Row := R.filter (fun r => r.1 != k0)

theorem predicted_eq (rows : List Row) (code : Nat) : predicted rows code = mode (hs rows code) := by
  simp only [predicted, hs]

theorem count_hs (R : List Row) (k : Nat) (y : UInt64) :
    (hs R k).count y = R.countP (fun r => r.1 == k && y == r.2) := by
  induction R with
  | nil => simp [hs]
  | cons a R ih =>
    by_cases ha : a.1 = k
    · by_cases hy : y = a.2
      · simp [hs, List.count_cons, List.countP_cons, ha, hy] at ih ⊢
        omega
      · have hy' : ¬ a.2 = y := fun h => hy h.symm
        simp [hs, List.count_cons, List.countP_cons, ha, hy, hy'] at ih ⊢
        omega
    · simp [hs, List.count_cons, List.countP_cons, ha] at ih ⊢
      omega

theorem fold_max (xs : List UInt64) : ∀ (ys : List UInt64) (init : UInt64),
    xs.count init ≤ xs.count (ys.foldl (fun w h => if xs.count h > xs.count w then h else w) init) ∧
    ∀ y ∈ ys, xs.count y ≤ xs.count (ys.foldl (fun w h => if xs.count h > xs.count w then h else w) init) := by
  intro ys
  induction ys with
  | nil => intro init; simp
  | cons a ys ih =>
    intro init
    simp only [List.foldl_cons]
    by_cases h : xs.count a > xs.count init
    · simp only [h, ite_true]
      obtain ⟨i1, i2⟩ := ih a
      refine ⟨by omega, ?_⟩
      intro y hy
      rcases List.mem_cons.mp hy with rfl | hy
      · exact i1
      · exact i2 y hy
    · simp only [h, ite_false]
      obtain ⟨i1, i2⟩ := ih init
      refine ⟨i1, ?_⟩
      intro y hy
      rcases List.mem_cons.mp hy with rfl | hy
      · have : xs.count y ≤ xs.count init := by omega
        omega
      · exact i2 y hy

theorem mode_max (xs : List UInt64) (y : UInt64) : xs.count y ≤ xs.count (mode xs) := by
  by_cases hy : y ∈ xs
  · exact (fold_max xs xs 0).2 y hy
  · rw [List.count_eq_zero_of_not_mem hy]; exact Nat.zero_le _

theorem total_cons (f : Nat → UInt64) (a : Row) (R : List Row) :
    total f (a :: R) = (if f a.1 == a.2 then 1 else 0) + total f R := by
  simp [total, List.countP_cons]; omega

theorem total_split (f : Nat → UInt64) (k0 : Nat) (R : List Row) :
    total f R = R.countP (fun r => r.1 == k0 && f r.1 == r.2) + total f (rm k0 R) := by
  induction R with
  | nil => simp [total, rm]
  | cons a R ih =>
    by_cases ha : a.1 = k0
    · by_cases hf : f a.1 = a.2
      · simp [total, rm, List.filter_cons, List.countP_cons, ha, hf] at ih ⊢
        omega
      · simp [total, rm, List.filter_cons, List.countP_cons, ha, hf] at ih ⊢
        omega
    · by_cases hf : f a.1 = a.2
      · simp [total, rm, List.filter_cons, List.countP_cons, ha, hf] at ih ⊢
        omega
      · simp [total, rm, List.filter_cons, List.countP_cons, ha, hf] at ih ⊢
        omega

theorem hs_rm (k0 : Nat) (R : List Row) (k : Nat) :
    hs (rm k0 R) k = if k = k0 then [] else hs R k := by
  induction R with
  | nil => simp [hs, rm]
  | cons a R ih =>
    by_cases ha : a.1 = k0
    · by_cases hk : k = k0
      · simp_all [rm, hs, List.filter_cons, List.filterMap_cons]
      · have : a.1 ≠ k := fun h => hk (h.symm.trans ha)
        simp_all [rm, hs, List.filter_cons, List.filterMap_cons]
    · by_cases hk : k = k0
      · have : a.1 ≠ k := fun h => ha (h ▸ hk)
        simp_all [rm, hs, List.filter_cons, List.filterMap_cons]
      · simp_all [rm, hs, List.filter_cons, List.filterMap_cons]

theorem total_le (pred f : Nat → UInt64) : ∀ n (R : List Row), R.length ≤ n →
    (∀ k y, (hs R k).count y ≤ (hs R k).count (pred k)) → total f R ≤ total pred R := by
  intro n
  induction n with
  | zero =>
    intro R hR _
    have : R = [] := List.eq_nil_of_length_eq_zero (by omega)
    subst this; simp [total]
  | succ n ih =>
    intro R hR hmax
    cases R with
    | nil => simp [total]
    | cons a rest =>
      have hsplitf := total_split f a.1 (a :: rest)
      have hsplitp := total_split pred a.1 (a :: rest)
      have hg : (a :: rest).countP (fun r => r.1 == a.1 && f r.1 == r.2) ≤
          (a :: rest).countP (fun r => r.1 == a.1 && pred r.1 == r.2) := by
        have e1 : (a :: rest).countP (fun r => r.1 == a.1 && f r.1 == r.2) =
            (a :: rest).countP (fun r => r.1 == a.1 && f a.1 == r.2) := by
          apply List.countP_congr
          intro r _
          by_cases h : r.1 = a.1
          · simp [h]
          · simp [h]
        have e2 : (a :: rest).countP (fun r => r.1 == a.1 && pred r.1 == r.2) =
            (a :: rest).countP (fun r => r.1 == a.1 && pred a.1 == r.2) := by
          apply List.countP_congr
          intro r _
          by_cases h : r.1 = a.1
          · simp [h]
          · simp [h]
        rw [e1, e2, ← count_hs, ← count_hs]
        exact hmax a.1 (f a.1)
      have hlen : (rm a.1 (a :: rest)).length ≤ n := by
        have hna : ¬ ((a.1 != a.1) = true) := by simp
        have e : rm a.1 (a :: rest) = rm a.1 rest := by
          simp only [rm]; exact List.filter_cons_of_neg hna
        rw [e]
        have := List.length_filter_le (fun r : Row => r.1 != a.1) rest
        simp only [rm]
        simp at hR
        omega
      have hm' : ∀ k y, (hs (rm a.1 (a :: rest)) k).count y ≤ (hs (rm a.1 (a :: rest)) k).count (pred k) := by
        intro k y
        rw [hs_rm]
        by_cases hk : k = a.1
        · simp [hk]
        · simp only [hk, ite_false]
          exact hmax k y
      have := ih _ hlen hm'
      omega

def exF (f : Nat → UInt64) (x : Row × Nat) : Option (Nat × UInt64) :=
  if f x.1.1 == x.1.2 then none else some (x.2, x.1.2)

theorem exceptions_eq (rows : List Row) (f : Nat → UInt64) :
    exceptions rows f = (rows.zipIdx).filterMap (exF f) := rfl

theorem exc_len (f : Nat → UInt64) (R : List Row) : ∀ n,
    (((R.zipIdx n).filterMap (exF f)).length + total f R = R.length) := by
  induction R with
  | nil => intro n; simp [total]
  | cons a R ih =>
    intro n
    obtain ⟨k, h⟩ := a
    have := ih (n+1)
    by_cases hf : f k = h
    · have e : exF f ((k,h),n) = none := by simp [exF, hf]
      simp only [List.zipIdx_cons, List.filterMap_cons, e]
      simp [total_cons, hf] at this ⊢
      omega
    · have e : exF f ((k,h),n) = some (n,h) := by simp [exF, hf]
      simp only [List.zipIdx_cons, List.filterMap_cons, e]
      simp [total_cons, hf] at this ⊢
      omega

theorem lk (f : Nat → UInt64) (R : List Row) : ∀ n j,
    ((R.zipIdx n).filterMap (exF f)).lookup j =
      if n ≤ j then (match R[j-n]? with
        | some (k,h) => if f k = h then none else some h
        | none => none) else none := by
  induction R with
  | nil => intro n j; simp
  | cons a R ih =>
    intro n j
    obtain ⟨k, h⟩ := a
    by_cases hf : f k = h
    · have e : exF f ((k,h),n) = none := by simp [exF, hf]
      simp only [List.zipIdx_cons, List.filterMap_cons, e]
      rw [ih]
      by_cases hn : n + 1 ≤ j
      · have e2 : j - n = (j - (n+1)) + 1 := by omega
        simp [hn, e2, show n ≤ j by omega]
      · by_cases hj : n = j
        · subst hj; simp [hf]; intro h; omega
        · have : ¬ n ≤ j := by omega
          simp [hn, this]
    · have e : exF f ((k,h),n) = some (n,h) := by simp [exF, hf]
      simp only [List.zipIdx_cons, List.filterMap_cons, e]
      by_cases hj : j = n
      · subst hj; simp [List.lookup, hf]
      · have hjn : (j == n) = false := by simpa using hj
        simp only [List.lookup, hjn]
        rw [ih]
        by_cases hn : n + 1 ≤ j
        · have e2 : j - n = (j - (n+1)) + 1 := by omega
          simp [hn, e2, show n ≤ j by omega]
        · have : ¬ n ≤ j := by omega
          simp [hn, this]

theorem sel (predict : Nat → UInt64) (patch : List (Nat × UInt64)) (wanted : UInt64 → Bool) :
    ∀ (R : List Row) (n : Nat),
    (∀ j k h, R[j]? = some (k,h) → (patch.lookup (n+j)).getD (predict k) = h) →
    ((R.map Prod.fst).zipIdx n).filterMap (fun (k,i) =>
        let h := (patch.lookup i).getD (predict k)
        if wanted h then some i else none) =
      (R.zipIdx n).filterMap (fun ((_,h),i) => if wanted h then some i else none) := by
  intro R
  induction R with
  | nil => intro n _; simp
  | cons a R ih =>
    intro n hspec
    obtain ⟨k, h⟩ := a
    have h0 := hspec 0 k h (by simp)
    simp only [Nat.add_zero] at h0
    have := ih (n+1) (by
      intro j k' h' hj
      have := hspec (j+1) k' h' (by simpa using hj)
      rwa [show n + (j+1) = n + 1 + j by omega] at this)
    simp only [List.map_cons, List.zipIdx_cons, List.filterMap_cons, h0]
    rw [this]

theorem sound : HashExceptions.Goal := by
  intro rows wanted
  dsimp only
  have hmax : ∀ k y, (hs rows k).count y ≤ (hs rows k).count (predicted rows k) := by
    intro k y
    rw [predicted_eq]; exact mode_max _ _
  refine ⟨?_, ?_⟩
  · intro alt
    have e1 := exc_len (predicted rows) rows 0
    have e2 := exc_len alt rows 0
    have := total_le (predicted rows) alt rows.length rows (Nat.le_refl _) hmax
    rw [exceptions_eq, exceptions_eq]
    omega
  · have hl := lk (predicted rows) rows
    refine sel _ _ wanted rows 0 ?_
    intro j k h hj
    have := hl 0 j
    rw [Nat.zero_add, exceptions_eq, this]
    simp only [Nat.zero_le, ite_true, Nat.sub_zero, hj]
    by_cases hf : predicted rows k = h
    · simp [hf]
    · simp [hf]

end TinylambOpt.ResearchProofs.ClickBenchLayout.Storage_HashExceptions
