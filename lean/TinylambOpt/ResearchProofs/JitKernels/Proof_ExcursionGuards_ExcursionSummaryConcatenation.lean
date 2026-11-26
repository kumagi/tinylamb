import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
A fused chunk's prefix extrema can be computed from the two segments'
summaries: running the scan of `b` from the accumulator `a.sum` shifts every
`b`-prefix by `a.sum`, so `low (a++b) = min (low a) (a.sum + low b)` and
dually for `high`. The proof characterizes membership in `path (a++b)` via
`List.scanl_append` (each element is either an `a`-prefix or `a.sum` plus a
`b`-prefix) and transfers the extrema through that decomposition with
`min?_eq_some_iff`/`max?_eq_some_iff`. SQL execution may therefore compute
chunk summaries segment-by-segment.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_ExcursionSummaryConcatenation
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

private theorem foldl_sum (s : Int) (xs : List Int) :
    xs.foldl (·+·) s = s + xs.sum := by
  induction xs generalizing s with
  | nil => simp
  | cons a t ih => rw [List.foldl_cons, ih, List.sum_cons]; omega

private theorem some_min (xs : List Int) : ∃ m, (path xs).min? = some m := by
  cases h : (path xs).min? with
  | some m => exact ⟨m, rfl⟩
  | none => exact absurd (List.min?_eq_none_iff.mp h) (by simp [path])

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

private theorem low_append (a b : List Int) :
    low (a++b) = min (low a) (a.sum + low b) := by
  obtain ⟨ma, hma⟩ := some_min a
  obtain ⟨mb, hmb⟩ := some_min b
  obtain ⟨ham, hab⟩ := List.min?_eq_some_iff.mp hma
  obtain ⟨hbm, hbb⟩ := List.min?_eq_some_iff.mp hmb
  have hsome : (path (a++b)).min? = some (min ma (a.sum + mb)) := by
    refine List.min?_eq_some_iff.mpr ⟨?_, ?_⟩
    · rcases Int.le_total ma (a.sum + mb) with h | h
      · rw [Int.min_eq_left h]
        exact mem_path_append_left a b ma ham
      · rw [Int.min_eq_right h]
        exact mem_path_append_right a b mb hbm
    · intro v hv
      obtain h1 | h2 := mem_path_append_split a b v hv
      · have hle := hab v h1
        rcases Int.le_total ma (a.sum + mb) with h | h
        · rw [Int.min_eq_left h]; omega
        · rw [Int.min_eq_right h]; omega
      · have hle := hbb (v - a.sum) h2
        rcases Int.le_total ma (a.sum + mb) with h | h
        · rw [Int.min_eq_left h]; omega
        · rw [Int.min_eq_right h]; omega
  show (path (a++b)).min?.getD 0 = min (low a) (a.sum + low b)
  rw [hsome]
  have ha : low a = ma := by rw [low, hma, Option.getD_some]
  have hb : low b = mb := by rw [low, hmb, Option.getD_some]
  rw [ha, hb, Option.getD_some]

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

theorem sound : ExcursionSummaryConcatenation.Goal := by
  intro a b
  rw [low_append, high_append]

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_ExcursionSummaryConcatenation
