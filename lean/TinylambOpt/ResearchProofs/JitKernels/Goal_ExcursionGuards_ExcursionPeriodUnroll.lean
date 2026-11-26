import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! A repeated chunk needs only one period's extrema and the sign of its drift
to certify every unrolled iteration. Positive drift raises the maximum linearly;
negative drift leaves the first period's maximum in charge. -/


namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_ExcursionPeriodUnroll

open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem min?_some_of_mem : ∀ (l : List Int) (a : Int), a ∈ l → ∃ m, l.min? = some m := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      cases xs with
      | nil => simp
      | cons y ys => simp [List.min?_cons]

theorem max?_some_of_mem : ∀ (l : List Int) (a : Int), a ∈ l → ∃ m, l.max? = some m := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      cases xs with
      | nil => simp
      | cons y ys => simp [List.max?_cons]

theorem min_le_mem : ∀ (l : List Int) (a : Int), a ∈ l → l.min?.getD 0 ≤ a := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · cases xs with
        | nil => simp
        | cons y ys =>
            obtain ⟨m, hm⟩ := min?_some_of_mem (y :: ys) y (by simp)
            simp [List.min?_cons, hm]
            exact Int.min_le_left a m
      · cases xs with
        | nil => simp at ha
        | cons y ys =>
            obtain ⟨m, hm⟩ := min?_some_of_mem (y :: ys) y (by simp)
            have hle := ih a ha
            simp [List.min?_cons, hm] at hle ⊢
            omega

theorem le_max_mem : ∀ (l : List Int) (a : Int), a ∈ l → a ≤ l.max?.getD 0 := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · cases xs with
        | nil => simp
        | cons y ys =>
            obtain ⟨m, hm⟩ := max?_some_of_mem (y :: ys) y (by simp)
            simp [List.max?_cons, hm]
            exact Int.le_max_left a m
      · cases xs with
        | nil => simp at ha
        | cons y ys =>
            obtain ⟨m, hm⟩ := max?_some_of_mem (y :: ys) y (by simp)
            have hle := ih a ha
            simp [List.max?_cons, hm] at hle ⊢
            omega


theorem scanl_from : ∀ (s : Int) (xs : List Int), List.scanl (·+·) s xs = (path xs).map (fun v => s + v) := by
  intro s xs
  induction xs generalizing s with
  | nil => simp [path]
  | cons x xs ih =>
      simp only [path, List.scanl_cons, Int.zero_add]
      rw [ih (s + x)]
      rw [ih x]
      simp only [List.map_cons, Int.add_zero]
      rw [List.map_map]
      congr 1
      apply List.map_congr_left
      intro v hv
      simp only [Function.comp_apply]
      omega

theorem min_map_add : ∀ (l : List Int) (c : Int), l ≠ [] → (l.map (fun v => c + v)).min?.getD 0 = c + l.min?.getD 0 := by
  intro l
  induction l with
  | nil => intro c h; simp at h
  | cons x xs ih =>
      intro c hne
      cases xs with
      | nil => simp
      | cons y ys =>
          obtain ⟨m, hm⟩ := min?_some_of_mem (y :: ys) y (by simp)
          have hxs : (y :: ys) ≠ [] := by simp
          have hi := ih c hxs
          simp [List.min?_cons, hm] at hi ⊢
          rw [hi]
          by_cases hxy : x ≤ m
          · simp [Int.min_def, hxy]
          · have hyx : m < x := by omega
            simp [Int.min_def, hxy]

theorem max_map_add : ∀ (l : List Int) (c : Int), l ≠ [] → (l.map (fun v => c + v)).max?.getD 0 = c + l.max?.getD 0 := by
  intro l
  induction l with
  | nil => intro c h; simp at h
  | cons x xs ih =>
      intro c hne
      cases xs with
      | nil => simp
      | cons y ys =>
          obtain ⟨m, hm⟩ := max?_some_of_mem (y :: ys) y (by simp)
          have hxs : (y :: ys) ≠ [] := by simp
          have hi := ih c hxs
          simp [List.max?_cons, hm] at hi ⊢
          rw [hi]
          by_cases hxy : x ≤ m
          · simp [Int.max_def, hxy]
          · have hyx : m < x := by omega
            simp [Int.max_def, hxy]

theorem path_zero_mem : ∀ xs : List Int, (0 : Int) ∈ path xs := by
  intro xs
  induction xs with
  | nil => simp [path]
  | cons x xs ih => simp [path, List.scanl_cons]

theorem path_cons' (x : Int) (xs : List Int) : path (x :: xs) = 0 :: (path xs).map (fun v => x + v) := by
  simp only [path, List.scanl_cons, Int.zero_add]
  rw [scanl_from]
  rfl

theorem low_cons (x : Int) (xs : List Int) : low (x :: xs) = min 0 (x + low xs) := by
  have hne : path xs ≠ [] := List.ne_nil_of_mem (path_zero_mem xs)
  have hmem : x + 0 ∈ (path xs).map (fun v => x + v) := by
    rw [List.mem_map]
    refine ⟨0, path_zero_mem xs, ?_⟩
    omega
  obtain ⟨m, hm⟩ := min?_some_of_mem ((path xs).map (fun v => x + v)) (x + 0) hmem
  have hmma := min_map_add (path xs) x hne
  rw [hm] at hmma
  simp only [low, path_cons', List.min?_cons, hm, Option.elim_some]
  simp at hmma
  rw [hmma]
  rfl

theorem high_cons (x : Int) (xs : List Int) : high (x :: xs) = max 0 (x + high xs) := by
  have hne : path xs ≠ [] := List.ne_nil_of_mem (path_zero_mem xs)
  have hmem : x + 0 ∈ (path xs).map (fun v => x + v) := by
    rw [List.mem_map]
    refine ⟨0, path_zero_mem xs, ?_⟩
    omega
  obtain ⟨m, hm⟩ := max?_some_of_mem ((path xs).map (fun v => x + v)) (x + 0) hmem
  have hmma := max_map_add (path xs) x hne
  rw [hm] at hmma
  simp only [high, path_cons', List.max?_cons, hm, Option.elim_some]
  simp at hmma
  rw [hmma]
  rfl

theorem min_add : ∀ c a b : Int, min (c + a) (c + b) = c + min a b := by
  intro c a b
  by_cases h : a ≤ b
  · simp [Int.min_def, h]
  · have h' : b < a := by omega
    simp [Int.min_def, h]

theorem max_add : ∀ c a b : Int, max (c + a) (c + b) = c + max a b := by
  intro c a b
  by_cases h : a ≤ b
  · simp
  · have h' : b < a := by omega
    simp [Int.max_def, h]

theorem min_min_min (a b c : Int) : min a (min b c) = min (min a b) c := by
  by_cases hab : a ≤ b <;> by_cases hbc : b ≤ c <;> by_cases hac : a ≤ c <;> simp [Int.min_def, hab, hbc, hac] <;> omega

theorem max_max_max (a b c : Int) : max a (max b c) = max (max a b) c := by
  by_cases hab : a ≤ b <;> by_cases hbc : b ≤ c <;> by_cases hac : a ≤ c <;> simp [Int.max_def, hab, hbc, hac] <;> omega

theorem excursion_concat : ExcursionSummaryConcatenation.Goal := by
  intro a b
  have hlowb : low b ≤ 0 := by
    have h := min_le_mem (path b) 0 (path_zero_mem b)
    simpa [low] using h
  have hhighb : 0 ≤ high b := by
    have h := le_max_mem (path b) 0 (path_zero_mem b)
    simpa [high] using h
  induction a with
  | nil =>
      apply Prod.ext
      · simp only [List.nil_append, List.sum_nil, Int.zero_add]
        exact (Int.min_eq_right hlowb).symm
      · simp only [List.nil_append, List.sum_nil, Int.zero_add]
        exact (Int.max_eq_right hhighb).symm
  | cons x xs ih =>
      simp only [low_cons, high_cons]
      simp only [List.cons_append, List.sum_cons]
      rw [low_cons x (xs ++ b), high_cons x (xs ++ b)]
      have ihlow := congrArg Prod.fst ih
      have ihhigh := congrArg Prod.snd ih
      simp at ihlow ihhigh
      rw [ihlow, ihhigh]
      rw [← min_add x (low xs) (xs.sum + low b)]
      rw [← max_add x (high xs) (xs.sum + high b)]
      rw [min_min_min, max_max_max]
      apply Prod.ext <;> omega


theorem path_append_singleton : ∀ (ys : List Int) (x : Int), path (ys ++ [x]) = path ys ++ [ys.sum + x] := by
  intro ys x
  simp [path, List.scanl_append, List.sum_eq_foldl]

theorem min_append_singleton : ∀ (l : List Int) (a : Int), l ≠ [] → (l ++ [a]).min?.getD 0 = min (l.min?.getD 0) a := by
  intro l
  induction l with
  | nil => intro a h; simp at h
  | cons x xs ih =>
      intro a hne
      cases xs with
      | nil => simp
      | cons y ys =>
          obtain ⟨m, hm⟩ := min?_some_of_mem (y :: ys) y (by simp)
          have hxs : (y :: ys) ≠ [] := by simp
          have hi := ih a hxs
          simp [List.min?_cons, hm] at hi ⊢
          rw [hi]

theorem max_append_singleton : ∀ (l : List Int) (a : Int), l ≠ [] → (l ++ [a]).max?.getD 0 = max (l.max?.getD 0) a := by
  intro l
  induction l with
  | nil => intro a h; simp at h
  | cons x xs ih =>
      intro a hne
      cases xs with
      | nil => simp
      | cons y ys =>
          obtain ⟨m, hm⟩ := max?_some_of_mem (y :: ys) y (by simp)
          have hxs : (y :: ys) ≠ [] := by simp
          have hi := ih a hxs
          simp [List.max?_cons, hm] at hi ⊢
          rw [hi]

theorem low_append_singleton (ys : List Int) (x : Int) : low (ys ++ [x]) = min (low ys) (ys.sum + x) := by
  rw [low, path_append_singleton]
  exact min_append_singleton (path ys) (ys.sum + x) (List.ne_nil_of_mem (path_zero_mem ys))

theorem high_append_singleton (ys : List Int) (x : Int) : high (ys ++ [x]) = max (high ys) (ys.sum + x) := by
  rw [high, path_append_singleton]
  exact max_append_singleton (path ys) (ys.sum + x) (List.ne_nil_of_mem (path_zero_mem ys))


theorem replicate_flatten_succ (xs : List Int) (n : Nat) :
    (List.replicate (n + 1) xs).flatten = xs ++ (List.replicate n xs).flatten := by
  cases n <;> simp [List.replicate]

theorem sum_flatten_replicate : ∀ (xs : List Int) (n : Nat), (List.replicate n xs).flatten.sum = (n : Int) * xs.sum := by
  intro xs n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [replicate_flatten_succ]
      simp [List.sum_append, ih]
      rw [Int.add_mul]
      omega


theorem findIdx?_none_of_all_false : ∀ (p : Int → Bool) (l : List Int),
    (∀ v ∈ l, p v = false) → List.findIdx? p l = none := by
  intro p l hfalse
  cases h : List.findIdx? p l with
  | none => rfl
  | some i =>
      have hs : (List.findIdx? p l).isSome = true := by simp [h]
      rw [List.findIdx?_isSome] at hs
      obtain ⟨v, hv, hpv⟩ := List.any_eq_true.mp hs
      have hf : p v = false := hfalse v hv
      rw [hf] at hpv
      cases hpv

theorem sum_mem_path : ∀ xs : List Int, xs.sum ∈ path xs := by
  intro xs
  induction xs with
  | nil => simp [path]
  | cons x xs ih =>
      simp only [List.sum_cons]
      rw [path_cons']
      rw [List.mem_cons]
      right
      rw [List.mem_map]
      exact ⟨xs.sum, ih, by omega⟩

theorem path_eq_zero_cons_tail : ∀ xs : List Int, path xs = 0 :: (path xs).tail := by
  intro xs
  cases xs <;> simp [path]

theorem findIdx?_path_tail : ∀ (p : Int → Bool) (b : List Int), p 0 = false →
    List.findIdx? p (path b) = Option.map (fun i => i + 1) (List.findIdx? p (path b).tail) := by
  intro p b hp0
  rw [path_eq_zero_cons_tail]
  rw [List.findIdx?_cons]
  simp [hp0]

theorem path_append_drop0 : ∀ (a b : List Int),
    path (a ++ b) = path a ++ (path b).tail.map (fun v => a.sum + v) := by
  intro a b
  simp only [path, List.scanl_append, List.sum_eq_foldl]
  rw [scanl_from (List.foldl (fun x1 x2 => x1 + x2) 0 a) b]
  simp only [path]
  rw [← List.map_tail]

theorem path_length : ∀ xs : List Int, (path xs).length = xs.length + 1 := by
  intro xs
  simp [path]

theorem sound : ExcursionPeriodUnroll.Goal := by
  intro xs n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [replicate_flatten_succ]
      have hconc := excursion_concat xs ((List.replicate (n + 1) xs).flatten)
      have hhigh := congrArg Prod.snd hconc
      simp at hhigh
      rw [hhigh]
      rw [ih]
      have hcast : ((n + 1 : Nat) : Int) = (n : Int) + 1 := by omega
      rw [hcast, Int.add_mul]
      simp only [Int.one_mul]
      have hn : 0 ≤ (n : Int) := by omega
      have hhigh_nonneg : 0 ≤ high xs := by
        have h := le_max_mem (path xs) 0 (path_zero_mem xs)
        simpa [high] using h
      by_cases hs : 0 ≤ xs.sum
      · have hns : 0 ≤ (n : Int) * xs.sum := Int.mul_nonneg hn hs
        rw [Int.max_eq_right hns]
        have hcond : high xs ≤ xs.sum + (high xs + (n : Int) * xs.sum) := by omega
        rw [Int.max_eq_right hcond]
        have hns2 : 0 ≤ (n : Int) * xs.sum + xs.sum := by omega
        rw [Int.max_eq_right hns2]
        omega
      · have hslt : xs.sum < 0 := by omega
        have hns : (n : Int) * xs.sum ≤ 0 := Int.mul_nonpos_of_nonneg_of_nonpos hn (Int.le_of_lt hslt)
        rw [Int.max_eq_left hns]
        have hcond : xs.sum + (high xs + 0) ≤ high xs := by omega
        rw [Int.max_eq_left hcond]
        have hns2 : (n : Int) * xs.sum + xs.sum ≤ 0 := by omega
        rw [Int.max_eq_left hns2]
        omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_ExcursionPeriodUnroll
