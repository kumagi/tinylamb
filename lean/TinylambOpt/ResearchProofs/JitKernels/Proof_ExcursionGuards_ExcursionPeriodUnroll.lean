import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
Repeating a vector `n + 1` times makes the prefix path of each period the
previous path shifted by the period's net change, so the global maximum is the
period's own `high` lifted by the positive accumulated drift:
`high ((replicate (n+1) xs).flatten) = high xs + max 0 (n * xs.sum)`. The
proof rewrites the flatten of the successor replication as `F(n) ++ xs`, applies
the concat extremum identity `high (a++b) = max (high a) (a.sum + high b)`,
and finishes the resulting `max` arithmetic by a case split on the sign of the
drift (where nonlinear `omega` steps become exact `Int.mul_nonneg`/
`Int.mul_nonpos` bounds). SQL execution may therefore unroll vector iterations
under one period guard plus a drift check.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_ExcursionPeriodUnroll
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

private theorem foldl_sum (s : Int) (xs : List Int) :
    xs.foldl (·+·) s = s + xs.sum := by
  induction xs generalizing s with
  | nil => simp
  | cons a t ih => rw [List.foldl_cons, ih, List.sum_cons]; omega

private theorem some_max (xs : List Int) : ∃ m, (path xs).max? = some m := by
  cases h : (path xs).max? with
  | some m => exact ⟨m, rfl⟩
  | none => exact absurd (List.max?_eq_none_iff.mp h) (by simp [path])

private theorem add_sum_mem_scanl :
    ∀ (a : List Int) (s : Int), s + a.sum ∈ a.scanl (·+·) s := by
  intro a
  induction a with
  | nil => intro s; rw [List.sum_nil, Int.add_zero]; exact List.mem_singleton_self _
  | cons c t ih =>
    intro s
    rw [List.sum_cons, ← Int.add_assoc, List.scanl_cons]
    exact List.mem_cons.mpr (Or.inr (ih (s + c)))

private theorem path_append_form (a b : List Int) :
    path (a++b) = path a ++ (b.scanl (·+·) a.sum).tail := by
  show (a++b).scanl (·+·) 0 = _
  rw [List.scanl_append, foldl_sum, Int.zero_add, path]

private theorem path_shift (t : Int) :
    ∀ (b : List Int), b.scanl (·+·) t = (path b).map (t + ·) := by
  intro b
  induction b generalizing t with
  | nil => simp [path]
  | cons c u ih =>
    simp only [List.scanl_cons, path, Int.zero_add, Int.add_zero, List.map_cons]
    rw [ih (t + c), ih c, List.map_map, List.cons.injEq]
    refine ⟨rfl, congrArg (fun h => List.map h (path u))
      (funext (fun x => by simp only [Function.comp_apply]; omega))⟩

private theorem mem_path_shift (t : Int) (b : List Int) (w : Int) :
    w ∈ b.scanl (·+·) t ↔ ∃ v, v ∈ path b ∧ w = t + v := by
  rw [path_shift, List.mem_map]
  constructor
  · rintro ⟨v, hv, heq⟩
    exact ⟨v, hv, heq.symm⟩
  · rintro ⟨v, hv, rfl⟩
    exact ⟨v, hv, rfl⟩

private theorem mem_path_append_left (a b : List Int) (v : Int) (h : v ∈ path a) :
    v ∈ path (a++b) := by
  rw [path_append_form]
  exact List.mem_append.mpr (Or.inl h)

private theorem mem_path_append_right (a b : List Int) (v : Int) (hv : v ∈ path b) :
    a.sum + v ∈ path (a++b) := by
  by_cases hv0 : v = 0
  · subst hv0
    rw [Int.add_zero]
    have h : (0 : Int) + a.sum ∈ a.scanl (·+·) 0 := add_sum_mem_scanl a 0
    exact mem_path_append_left a b a.sum (by simpa [path] using h)
  · rw [path_append_form, List.mem_append]
    refine Or.inr ?_
    obtain ⟨rest, hrest⟩ : ∃ rest, b.scanl (·+·) a.sum = a.sum :: rest := by
      cases b with
      | nil => exact ⟨[], by rw [List.scanl_nil]⟩
      | cons c t => exact ⟨t.scanl (·+·) (a.sum + c), by rw [List.scanl_cons]⟩
    rw [hrest, List.tail_cons]
    have hin : a.sum + v ∈ a.sum :: rest := hrest ▸ (mem_path_shift a.sum b _).mpr ⟨v, hv, rfl⟩
    rw [List.mem_cons] at hin
    obtain hxy | hmem := hin
    · exact absurd hxy (by omega)
    · exact hmem

private theorem mem_path_append_split (a b : List Int) (v : Int) (hv : v ∈ path (a++b)) :
    v ∈ path a ∨ v - a.sum ∈ path b := by
  rw [path_append_form, List.mem_append] at hv
  obtain hin | hright := hv
  · exact Or.inl hin
  · refine Or.inr ?_
    have hd : v ∈ (b.scanl (·+·) a.sum).drop 1 := by rw [List.drop_one]; exact hright
    have := List.mem_of_mem_drop (i := 1) hd
    obtain ⟨u, hu, hueq⟩ := (mem_path_shift a.sum b v).mp this
    have he : u = v - a.sum := by omega
    rw [← he]
    exact hu

private theorem high_append (a b : List Int) :
    high (a++b) = max (high a) (a.sum + high b) := by
  obtain ⟨Ma, hMa⟩ := some_max a
  obtain ⟨Mb, hMb⟩ := some_max b
  obtain ⟨haM, haB⟩ := List.max?_eq_some_iff.mp hMa
  obtain ⟨hbM, hbB⟩ := List.max?_eq_some_iff.mp hMb
  have hsome : (path (a++b)).max? = some (max Ma (a.sum + Mb)) := by
    refine List.max?_eq_some_iff.mpr ⟨?_, ?_⟩
    · rcases Int.le_total (a.sum + Mb) Ma with h | h
      · rw [Int.max_eq_left h]
        exact mem_path_append_left a b Ma haM
      · rw [Int.max_eq_right h]
        exact mem_path_append_right a b Mb hbM
    · intro v hv
      obtain h1 | h2 := mem_path_append_split a b v hv
      · have hle := haB v h1
        rcases Int.le_total (a.sum + Mb) Ma with h | h
        · rw [Int.max_eq_left h]; omega
        · rw [Int.max_eq_right h]; omega
      · have hle := hbB (v - a.sum) h2
        rcases Int.le_total (a.sum + Mb) Ma with h | h
        · rw [Int.max_eq_left h]; omega
        · rw [Int.max_eq_right h]; omega
  show (path (a++b)).max?.getD 0 = max (high a) (a.sum + high b)
  rw [hsome]
  have ha : high a = Ma := by rw [high, hMa, Option.getD_some]
  have hb : high b = Mb := by rw [high, hMb, Option.getD_some]
  rw [ha, hb, Option.getD_some]

private theorem sum_flatten (xs : List Int) :
    ∀ n : Nat, ((List.replicate n xs).flatten).sum = (n : Int) * xs.sum := by
  intro n
  induction n with
  | zero => simp
  | succ k ih =>
    rw [List.replicate_succ, List.flatten_cons, List.sum_append, ih]
    simp only [Int.natCast_add_one, Int.add_mul, Int.one_mul]
    omega

theorem sound : ExcursionPeriodUnroll.Goal := by
  intro xs n
  induction n with
  | zero =>
    rw [List.replicate_succ, List.replicate_zero, List.flatten_cons,
      List.flatten_nil, List.append_nil]
    simp
  | succ k ih =>
    have hflat : (List.replicate (k + 1 + 1) xs).flatten =
        xs ++ (List.replicate (k + 1) xs).flatten := by
      rw [List.replicate_succ, List.flatten_cons]
    rw [hflat, high_append, ih]
    rw [show ((k + 1 : Nat) : Int) * xs.sum = (k : Int) * xs.sum + xs.sum from by
      simp only [Int.natCast_add_one, Int.add_mul, Int.one_mul]]
    rcases Int.le_total xs.sum 0 with hS | hS
    · have hp : (k : Int) * xs.sum ≤ 0 :=
        Int.mul_nonpos_of_nonneg_of_nonpos (by omega) hS
      have hpq : (k : Int) * xs.sum + xs.sum ≤ 0 := by omega
      rw [Int.max_eq_left hp, Int.max_eq_left hpq]
      rw [Int.add_zero, Int.max_eq_left (by omega : xs.sum + high xs ≤ high xs)]
    · have hp : 0 ≤ (k : Int) * xs.sum :=
        Int.mul_nonneg (by omega) hS
      have hpq : 0 ≤ (k : Int) * xs.sum + xs.sum := by omega
      rw [Int.max_eq_right hp, Int.max_eq_right hpq]
      rw [Int.max_eq_right (by omega :
        high xs ≤ xs.sum + (high xs + (k : Int) * xs.sum))]
      omega

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_ExcursionPeriodUnroll
