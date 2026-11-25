import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! ### max0 and maximum helpers -/

private theorem max0_nonneg (xs : List Int) : 0 ≤ max0 xs := by
  simpa [max0] using (Int.le_max_left (0 : Int) (xs.max?.getD 0))

private theorem le_max0_of_mem {xs : List Int} {a : Int} (ha : a ∈ xs) : a ≤ max0 xs := by
  unfold max0
  cases hm : xs.max? with
  | none =>
      have hnil : xs = [] := List.max?_eq_none_iff.mp hm
      simp [hnil] at ha
  | some m =>
      have ham : a ≤ m := (List.max?_eq_some_iff.mp hm).2 a ha
      simp only [hm, Option.getD_some]
      omega

private theorem max0_le {xs : List Int} {c : Int} (hc : 0 ≤ c)
    (h : ∀ a ∈ xs, a ≤ c) : max0 xs ≤ c := by
  unfold max0
  apply Int.max_le.mpr
  constructor
  · exact hc
  · cases hm : xs.max? with
    | none => simpa [hm]
    | some m => exact (List.max?_le_iff hm).mpr h

private theorem max0_append (xs ys : List Int) :
    max0 (xs ++ ys) = max (max0 xs) (max0 ys) := by
  apply Int.le_antisymm
  · apply max0_le (c := max (max0 xs) (max0 ys))
      (Int.le_trans (max0_nonneg xs) (Int.le_max_left _ _))
    intro a ha
    rcases List.mem_append.mp ha with ha | ha
    · exact Int.le_trans (le_max0_of_mem ha) (Int.le_max_left _ _)
    · exact Int.le_trans (le_max0_of_mem ha) (Int.le_max_right _ _)
  · apply Int.max_le.mpr
    constructor
    · apply max0_le (max0_nonneg _)
      intro a ha
      exact le_max0_of_mem (List.mem_append_left _ ha)
    · apply max0_le (max0_nonneg _)
      intro a ha
      exact le_max0_of_mem (List.mem_append_right _ ha)

private theorem max0_le_of_subset {xs ys : List Int} (h : ∀ a ∈ xs, a ∈ ys) :
    max0 xs ≤ max0 ys := by
  apply max0_le (max0_nonneg _)
  intro a ha
  exact le_max0_of_mem (h a ha)

private def rawMax (xs : List Int) : Int := xs.max?.getD 0

private theorem rawMax_mem {xs : List Int} (hne : xs ≠ []) : rawMax xs ∈ xs := by
  unfold rawMax
  cases hm : xs.max? with
  | none =>
      have hnil : xs = [] := List.max?_eq_none_iff.mp hm
      contradiction
  | some v => exact List.max?_mem hm

private theorem rawMax_ge {xs : List Int} {a : Int} (ha : a ∈ xs) : a ≤ rawMax xs := by
  unfold rawMax
  cases hm : xs.max? with
  | none =>
      have hnil : xs = [] := List.max?_eq_none_iff.mp hm
      simp [hnil] at ha
  | some v =>
      have : a ≤ v := (List.max?_eq_some_iff.mp hm).2 a ha
      simpa [hm] using this

private theorem max0_eq_rawMax_of_pos {xs : List Int} (h : 0 < rawMax xs) :
    max0 xs = rawMax xs := by
  unfold max0
  change max (0 : Int) (rawMax xs) = rawMax xs
  exact Int.max_eq_right (by omega)

private theorem max0_eq_zero_of_nonpos {xs : List Int} (h : rawMax xs ≤ 0) :
    max0 xs = 0 := by
  simp only [max0, rawMax]
  exact Int.max_eq_left h

private theorem rawMax_pos_of_max0_pos {xs : List Int} (h : 0 < max0 xs) : 0 < rawMax xs := by
  change (0 : Int) < xs.max?.getD 0
  change (0 : Int) < max 0 (xs.max?.getD 0) at h
  omega

private theorem rawMax_map_add {xs : List Int} (hne : xs ≠ []) (c : Int) :
    rawMax (xs.map (fun a => c + a)) = c + rawMax xs := by
  have hm : xs.max? = some (rawMax xs) := by
    unfold rawMax
    cases h : xs.max? with
    | none =>
        have hnil : xs = [] := List.max?_eq_none_iff.mp h
        contradiction
    | some v => simp [h]
  have hmem : rawMax xs ∈ xs := List.max?_mem hm
  have hle : ∀ a ∈ xs, a ≤ rawMax xs := (List.max?_eq_some_iff.mp hm).2
  have hm2 : (xs.map (fun a => c + a)).max? = some (c + rawMax xs) := by
    apply List.max?_eq_some_iff.mpr
    constructor
    · exact List.mem_map.mpr ⟨rawMax xs, hmem, rfl⟩
    · intro b hb
      rcases List.mem_map.mp hb with ⟨a, ha, rfl⟩
      exact Int.add_le_add_left (hle a ha) c
  unfold rawMax
  rw [hm2, hm]
  simp

private theorem max0_map_add {xs : List Int} (hne : xs ≠ []) (c : Int) :
    max0 (xs.map (fun a => c + a)) = max 0 (c + rawMax xs) := by
  change max 0 (rawMax (xs.map (fun a => c + a))) = max 0 (c + rawMax xs)
  rw [rawMax_map_add hne c]

private theorem sum_map_nonneg {α : Type} (xs : List α) (f : α → Int)
    (h : ∀ a ∈ xs, 0 ≤ f a) : 0 ≤ (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := h a (by simp)
      have hi := ih (fun b hb => h b (by simp [hb]))
      omega

private theorem sum_nonpos {xs : List Int} (h : ∀ a ∈ xs, a ≤ 0) : xs.sum ≤ 0 := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons]
      have ha := h a (by simp)
      have hi := ih (fun b hb => h b (by simp [hb]))
      omega

/-! ### Windows, prefixes and suffixes -/

private theorem window_exists {x w : Series} (hw : w ∈ windows x) :
    ∃ i, i < x.length ∧ ∃ j, j < x.length - i ∧ w = (x.drop i).take (j + 1) := by
  unfold windows at hw
  rcases List.mem_flatMap.mp hw with ⟨i, hi, hw⟩
  rw [List.mem_range] at hi
  rcases List.mem_map.mp hw with ⟨j, hj, rfl⟩
  rw [List.mem_range] at hj
  exact ⟨i, hi, j, hj, rfl⟩

private theorem mem_of_window_mem {x w : Series} (hw : w ∈ windows x) :
    ∀ v ∈ w, v ∈ x := by
  rcases window_exists hw with ⟨i, hi, j, hj, rfl⟩
  intro v hv
  exact List.mem_of_mem_drop (List.mem_of_mem_take hv)

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  unfold prefs
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.map_cons, List.flatMap_cons, List.map_map, Function.comp_def,
    List.take_succ_cons, List.take_zero, List.sum_cons, List.sum_nil,
    Nat.add_zero, Int.add_zero]

private theorem suffs_cons (a : Int) (xs : Series) :
    suffs (a :: xs) = (a + xs.sum) :: suffs xs := by
  unfold suffs
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.map_cons, List.map_map, Function.comp_def, List.drop_succ_cons,
    List.drop_zero, List.sum_cons, Nat.add_zero]

private theorem suffs_append (x y : Series) :
    suffs (x ++ y) = (suffs x).map (fun s => s + y.sum) ++ suffs y := by
  induction x with
  | nil => simp [suffs]
  | cons a xs ih =>
      rw [List.cons_append, suffs_cons, suffs_cons, ih]
      simp only [List.map_cons, List.cons_append, List.sum_append]
      congr 1
      omega

private def prefixLists (x : Series) : List Series :=
  (List.range x.length).map (fun i => x.take (i + 1))

private def suffixLists (x : Series) : List Series :=
  (List.range x.length).map (fun i => x.drop i)

private theorem prefs_eq_map_sum (x : Series) : prefs x = (prefixLists x).map List.sum := by
  unfold prefs prefixLists
  rw [List.map_map]
  rfl

private theorem suffs_eq_map_sum (x : Series) : suffs x = (suffixLists x).map List.sum := by
  unfold suffs suffixLists
  rw [List.map_map]
  rfl

private theorem prefixLists_mem_windows {x u : Series} (hu : u ∈ prefixLists x) :
    u ∈ windows x := by
  unfold prefixLists at hu
  rcases List.mem_map.mp hu with ⟨i, hi, rfl⟩
  rw [List.mem_range] at hi
  unfold windows
  apply List.mem_flatMap.mpr
  refine ⟨0, ?_, ?_⟩
  · rw [List.mem_range]
    omega
  apply List.mem_map.mpr
  refine ⟨i, ?_, ?_⟩
  · simpa using hi
  · simp

private theorem suffixLists_mem_windows {x u : Series} (hu : u ∈ suffixLists x) :
    u ∈ windows x := by
  unfold suffixLists at hu
  rcases List.mem_map.mp hu with ⟨i, hi, rfl⟩
  rw [List.mem_range] at hi
  unfold windows
  refine List.mem_flatMap.mpr ⟨i, by simpa using hi, ?_⟩
  refine List.mem_map.mpr ⟨x.length - i - 1, ?_, ?_⟩
  · rw [List.mem_range]
    omega
  · rw [show (x.length - i - 1) + 1 = x.length - i by omega]
    rw [List.take_of_length_le (by simp [List.length_drop])]

private theorem mem_suffs_exists {x : Series} {s : Int} (hs : s ∈ suffs x) :
    ∃ i, i < x.length ∧ (x.drop i).sum = s := by
  unfold suffs at hs
  rcases List.mem_map.mp hs with ⟨i, hi, rfl⟩
  rw [List.mem_range] at hi
  exact ⟨i, hi, rfl⟩

private theorem mem_prefs_exists {x : Series} {p : Int} (hp : p ∈ prefs x) :
    ∃ i, i < x.length ∧ (x.take (i + 1)).sum = p := by
  unfold prefs at hp
  rcases List.mem_map.mp hp with ⟨i, hi, rfl⟩
  rw [List.mem_range] at hi
  exact ⟨i, hi, rfl⟩

private theorem prefs_sum_mem_frames {x : Series} {p : Int} (hp : p ∈ prefs x) :
    p ∈ (windows x).map List.sum := by
  rcases mem_prefs_exists hp with ⟨i, hi, rfl⟩
  apply List.mem_map.mpr
  refine ⟨x.take (i + 1), ?_, rfl⟩
  apply prefixLists_mem_windows
  unfold prefixLists
  exact List.mem_map.mpr ⟨i, by simpa using hi, rfl⟩

private theorem suffs_sum_mem_frames {x : Series} {s : Int} (hs : s ∈ suffs x) :
    s ∈ (windows x).map List.sum := by
  rcases mem_suffs_exists hs with ⟨i, hi, rfl⟩
  apply List.mem_map.mpr
  refine ⟨x.drop i, ?_, rfl⟩
  apply suffixLists_mem_windows
  unfold suffixLists
  exact List.mem_map.mpr ⟨i, by simpa using hi, rfl⟩

private theorem max0_prefs_le_measure (x : Series) : max0 (prefs x) ≤ BestFrame.measure x := by
  unfold BestFrame.measure
  apply max0_le (max0_nonneg _)
  intro p hp
  exact le_max0_of_mem (prefs_sum_mem_frames hp)

private theorem max0_suffs_le_measure (x : Series) : max0 (suffs x) ≤ BestFrame.measure x := by
  unfold BestFrame.measure
  apply max0_le (max0_nonneg _)
  intro s hs
  exact le_max0_of_mem (suffs_sum_mem_frames hs)

private theorem measure_le_of_subset {x y : Series}
    (h : ∀ w ∈ windows x, w ∈ windows y) : BestFrame.measure x ≤ BestFrame.measure y := by
  unfold BestFrame.measure
  apply max0_le (max0_nonneg _)
  intro v hv
  rcases List.mem_map.mp hv with ⟨w, hw, rfl⟩
  exact le_max0_of_mem (List.mem_map.mpr ⟨w, h w hw, rfl⟩)

private theorem window_mem_append_left (x y w : Series) (hw : w ∈ windows x) :
    w ∈ windows (x ++ y) := by
  rcases window_exists hw with ⟨i, hi, j, hj, rfl⟩
  unfold windows
  refine List.mem_flatMap.mpr ⟨i, ?_, ?_⟩
  · simp only [List.mem_range]
    simp only [List.length_append]
    omega
  · refine List.mem_map.mpr ⟨j, ?_, ?_⟩
    · simp only [List.mem_range]
      simp only [List.length_append]
      omega
    · rw [List.drop_append_of_le_length (by omega : i ≤ x.length)]
      rw [List.take_append_of_le_length (by
        rw [List.length_drop]
        omega : j + 1 ≤ (x.drop i).length)]

private theorem window_mem_append_right (x y w : Series) (hw : w ∈ windows y) :
    w ∈ windows (x ++ y) := by
  rcases window_exists hw with ⟨i, hi, j, hj, rfl⟩
  unfold windows
  refine List.mem_flatMap.mpr ⟨x.length + i, ?_, ?_⟩
  · simp only [List.mem_range, List.length_append]
    omega
  · refine List.mem_map.mpr ⟨j, ?_, ?_⟩
    · simp only [List.mem_range]
      simp only [List.length_append] at hj ⊢
      omega
    · rw [List.drop_append]
      simp [List.drop_eq_nil_iff.mpr (by omega : x.length ≤ x.length + i)]

private theorem window_mem_cons_tail {a : Int} {xs w : Series} (hw : w ∈ windows xs) :
    w ∈ windows (a :: xs) := by
  rcases window_exists hw with ⟨i, hi, j, hj, rfl⟩
  unfold windows
  refine List.mem_flatMap.mpr ⟨i + 1, ?_, ?_⟩
  · simp only [List.mem_range, List.length_cons]
    omega
  · refine List.mem_map.mpr ⟨j, ?_, ?_⟩
    · simpa using hj
    · simp only [List.drop_succ_cons]

private theorem cross_lists_mem_windows {x y u v : Series}
    (hu : u ∈ suffixLists x) (hv : v ∈ prefixLists y) : u ++ v ∈ windows (x ++ y) := by
  unfold suffixLists at hu
  unfold prefixLists at hv
  rcases List.mem_map.mp hu with ⟨i, hi, rfl⟩
  rcases List.mem_map.mp hv with ⟨j, hj, rfl⟩
  rw [List.mem_range] at hi hj
  unfold windows
  refine List.mem_flatMap.mpr ⟨i, ?_, ?_⟩
  · simp only [List.mem_range, List.length_append]
    omega
  · refine List.mem_map.mpr ⟨x.length - i + j, ?_, ?_⟩
    · simp only [List.mem_range]
      simp only [List.length_append]
      omega
    · rw [List.drop_append_of_le_length (by omega : i ≤ x.length)]
      rw [List.take_append]
      have hlong : (x.drop i).length ≤ (x.length - i + j) + 1 := by
        rw [List.length_drop]
        omega
      rw [List.take_of_length_le hlong]
      have hdrop : (x.drop i).length = x.length - i := List.length_drop (l := x) (i := i)
      simp only [hdrop]
      have hsub : (x.length - i + j) + 1 - (x.length - i) = j + 1 := by omega
      rw [hsub]

private theorem window_append_classify (x y : Series) {w : Series} (hw : w ∈ windows (x ++ y)) :
    (∃ u ∈ windows x, u = w) ∨
    (∃ u ∈ windows y, u = w) ∨
    (∃ u ∈ suffixLists x, ∃ v ∈ prefixLists y, u ++ v = w) := by
  rcases window_exists hw with ⟨i, hi, j, hj, hw⟩
  simp only [List.length_append] at hi hj
  by_cases hix : i < x.length
  · by_cases hcross : i + j < x.length
    · left
      have heq : (x.drop i).take (j + 1) = w := by
        rw [hw]
        rw [List.drop_append_of_le_length (by omega : i ≤ x.length)]
        rw [List.take_append_of_le_length (by
          rw [List.length_drop]
          omega : j + 1 ≤ (x.drop i).length)]
      refine ⟨(x.drop i).take (j + 1), ?_, heq⟩
      unfold windows
      refine List.mem_flatMap.mpr ⟨i, ?_, ?_⟩
      · rw [List.mem_range]
        omega
      · refine List.mem_map.mpr ⟨j, ?_, rfl⟩
        rw [List.mem_range]
        omega
    · right
      right
      let k := j + 1 - (x.length - i)
      have hkpos : 0 < k := by dsimp [k]; omega
      have hkle : k ≤ y.length := by dsimp [k]; omega
      refine ⟨x.drop i, ?_, y.take k, ?_, ?_⟩
      · unfold suffixLists
        exact List.mem_map.mpr ⟨i, by simpa using hix, rfl⟩
      · unfold prefixLists
        refine List.mem_map.mpr ⟨k - 1, ?_, ?_⟩
        · rw [List.mem_range]
          omega
        · congr 1
          omega
      · rw [hw]
        rw [List.drop_append_of_le_length (by omega : i ≤ x.length)]
        rw [List.take_append, List.length_drop]
        have hlong : (x.drop i).length ≤ j + 1 := by
          rw [List.length_drop]
          omega
        rw [List.take_of_length_le hlong]
  · right
    left
    have heq : (y.drop (i - x.length)).take (j + 1) = w := by
      rw [hw]
      rw [List.drop_append, List.drop_eq_nil_iff.mpr (by omega : x.length ≤ i)]
      simp
    refine ⟨(y.drop (i - x.length)).take (j + 1), ?_, heq⟩
    unfold windows
    refine List.mem_flatMap.mpr ⟨i - x.length, ?_, ?_⟩
    · rw [List.mem_range]
      omega
    · refine List.mem_map.mpr ⟨j, ?_, rfl⟩
      rw [List.mem_range]
      omega

private theorem window_sum_le_bound (x y : Series) {w : Series} (hw : w ∈ windows (x ++ y)) :
    w.sum ≤ max (max (BestFrame.measure x) (BestFrame.measure y))
      (max0 (suffs x) + max0 (prefs y)) := by
  rcases window_append_classify x y hw with h | h | h
  · rcases h with ⟨u, hu, rfl⟩
    exact Int.le_trans (le_max0_of_mem (List.mem_map.mpr ⟨u, hu, rfl⟩))
      (Int.le_trans (Int.le_max_left _ _) (Int.le_max_left _ _))
  · rcases h with ⟨u, hu, rfl⟩
    exact Int.le_trans (le_max0_of_mem (List.mem_map.mpr ⟨u, hu, rfl⟩))
      (Int.le_trans (Int.le_max_right _ _) (Int.le_max_left _ _))
  · rcases h with ⟨u, hu, v, hv, rfl⟩
    have huM : u.sum ≤ max0 (suffs x) := by
      apply le_max0_of_mem
      rw [suffs_eq_map_sum]
      exact List.mem_map.mpr ⟨u, hu, rfl⟩
    have hvP : v.sum ≤ max0 (prefs y) := by
      apply le_max0_of_mem
      rw [prefs_eq_map_sum]
      exact List.mem_map.mpr ⟨v, hv, rfl⟩
    rw [List.sum_append]
    exact Int.le_trans (Int.add_le_add huM hvP) (Int.le_max_right _ _)

private theorem cross_window_exists {x y : Series} (hxne : x ≠ []) (hyne : y ≠ []) :
    ∃ u ∈ suffixLists x, ∃ v ∈ prefixLists y,
      u.sum = rawMax (suffs x) ∧ v.sum = rawMax (prefs y) ∧
      u ++ v ∈ windows (x ++ y) := by
  have hsne : suffs x ≠ [] := by
    unfold suffs
    cases x with
    | nil => contradiction
    | cons a xs => simp
  have hpne : prefs y ≠ [] := by
    unfold prefs
    cases y with
    | nil => contradiction
    | cons a xs => simp
  have hsmem : rawMax (suffs x) ∈ (suffixLists x).map List.sum := by
    rw [← suffs_eq_map_sum]
    exact rawMax_mem hsne
  have hpmem : rawMax (prefs y) ∈ (prefixLists y).map List.sum := by
    rw [← prefs_eq_map_sum]
    exact rawMax_mem hpne
  rcases List.mem_map.mp hsmem with ⟨u, hu, husum⟩
  rcases List.mem_map.mp hpmem with ⟨v, hv, hvsum⟩
  exact ⟨u, hu, v, hv, husum, hvsum, cross_lists_mem_windows hu hv⟩

theorem sound_CrossFrame : BestFrame.CrossFrame.Goal := by
  intro x y
  apply Int.le_antisymm
  · unfold BestFrame.measure
    apply max0_le
    · have hxm := max0_nonneg ((windows x).map List.sum)
      have hym := max0_nonneg ((windows y).map List.sum)
      have hM := max0_nonneg (suffs x)
      have hP := max0_nonneg (prefs y)
      omega
    · intro v hv
      rcases List.mem_map.mp hv with ⟨w, hw, rfl⟩
      exact window_sum_le_bound x y hw
  · apply Int.max_le.mpr
    constructor
    · apply Int.max_le.mpr
      constructor
      · exact measure_le_of_subset (fun w hw => window_mem_append_left x y w hw)
      · exact measure_le_of_subset (fun w hw => window_mem_append_right x y w hw)
    · by_cases hM0 : max0 (suffs x) = 0
      · rw [hM0, Int.zero_add]
        exact Int.le_trans (max0_prefs_le_measure y)
          (measure_le_of_subset (fun w hw => window_mem_append_right x y w hw))
      · by_cases hP0 : max0 (prefs y) = 0
        · rw [hP0, Int.add_zero]
          exact Int.le_trans (max0_suffs_le_measure x)
            (measure_le_of_subset (fun w hw => window_mem_append_left x y w hw))
        · have hMpos : 0 < max0 (suffs x) := by
            have := max0_nonneg (suffs x)
            omega
          have hPpos : 0 < max0 (prefs y) := by
            have := max0_nonneg (prefs y)
            omega
          have hxne : x ≠ [] := by
            intro hx
            subst x
            simp [suffs, max0] at hM0
          have hyne : y ≠ [] := by
            intro hy
            subst y
            simp [prefs, max0] at hP0
          have hMraw : max0 (suffs x) = rawMax (suffs x) :=
            max0_eq_rawMax_of_pos (rawMax_pos_of_max0_pos hMpos)
          have hPraw : max0 (prefs y) = rawMax (prefs y) :=
            max0_eq_rawMax_of_pos (rawMax_pos_of_max0_pos hPpos)
          rcases cross_window_exists hxne hyne with ⟨u, hu, v, hv, husum, hvsum, huw⟩
          rw [hMraw, hPraw, ← husum, ← hvsum, ← List.sum_append]
          exact le_max0_of_mem (List.mem_map.mpr ⟨u ++ v, huw, rfl⟩)

/-! ### Repeated periods -/

private theorem sum_flatten_replicate (n : Nat) (x : Series) :
    ((List.replicate n x).flatten).sum = (n : Int) * x.sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons, List.sum_append, ih]
      push_cast
      grind

private theorem flatten_replicate_succ_right (n : Nat) (x : Series) :
    (List.replicate (Nat.succ n) x).flatten = (List.replicate n x).flatten ++ x := by
  rw [show Nat.succ n = n + 1 by omega, List.replicate_succ', List.flatten_append]
  simp

private theorem suffix_repeat_decomp {x : Series} {n : Nat} {v : Int}
    (hv : v ∈ suffs ((List.replicate n x).flatten)) :
    ∃ s ∈ suffs x, ∃ k : Nat, k < n ∧ v = s + (k : Int) * x.sum := by
  induction n generalizing v with
  | zero => simp [suffs] at hv
  | succ n ih =>
      rw [flatten_replicate_succ_right, suffs_append] at hv
      rcases List.mem_append.mp hv with hv | hv
      · rcases List.mem_map.mp hv with ⟨w, hw, hwsum⟩
        rcases ih (v := w) hw with ⟨s, hs, k, hk, hwval⟩
        refine ⟨s, hs, k + 1, by omega, ?_⟩
        rw [← hwsum, hwval]
        grind
      · exact ⟨v, hv, 0, by omega, by grind⟩

private theorem suffix_repeat_realize {x : Series} {n : Nat} (hn : 1 ≤ n)
    {s : Int} (hs : s ∈ suffs x) (k : Nat) (hk : k < n) :
    ∃ v ∈ suffs ((List.replicate n x).flatten), v = s + (k : Int) * x.sum := by
  induction n generalizing k with
  | zero => omega
  | succ n ih =>
      rw [flatten_replicate_succ_right, suffs_append]
      by_cases hk0 : k = 0
      · subst k
        refine ⟨s, ?_, by grind⟩
        exact List.mem_append_right _ hs
      · have hkpos : 0 < k := Nat.pos_of_ne_zero hk0
        have hk' : k - 1 < n := by omega
        rcases ih (by omega) (k - 1) hk' with ⟨v, hv, hvsum⟩
        refine ⟨v + x.sum, ?_, ?_⟩
        · exact List.mem_append_left _ (List.mem_map.mpr ⟨v, hv, rfl⟩)
        · rw [hvsum]
          have : ((k - 1 : Nat) : Int) + 1 = (k : Int) := by omega
          grind

private theorem max0_suffs_repeat (x : Series) (n : Nat) (hn : 1 ≤ n) :
    max0 (suffs ((List.replicate n x).flatten)) =
      max0 (suffs x) + ((n - 1 : Nat) : Int) * max 0 x.sum := by
  apply Int.le_antisymm
  · refine max0_le ?_ ?_
    · have hMnn := max0_nonneg (suffs x)
      have hTnn : (0 : Int) ≤ max 0 x.sum := Int.le_max_left _ _
      have hmulnn : (0 : Int) ≤ ((n - 1 : Nat) : Int) * max 0 x.sum :=
        Int.mul_nonneg (Int.natCast_nonneg _) hTnn
      omega
    · intro v hv
      rcases suffix_repeat_decomp hv with ⟨s, hs, k, hk, rfl⟩
      have hsM : s ≤ max0 (suffs x) := le_max0_of_mem hs
      have hk_le : k ≤ n - 1 := by omega
      by_cases hnonneg : 0 ≤ x.sum
      · have hmul : (k : Int) * x.sum ≤ ((n - 1 : Nat) : Int) * x.sum := by
          exact Int.mul_le_mul_of_nonneg_right (by exact_mod_cast hk_le) hnonneg
        have hT : max 0 x.sum = x.sum := Int.max_eq_right hnonneg
        rw [hT]
        omega
      · have hsumle : x.sum ≤ 0 := by omega
        have hmul : (k : Int) * x.sum ≤ 0 :=
          Int.mul_nonpos_of_nonneg_of_nonpos (Int.natCast_nonneg k) hsumle
        have hT : max 0 x.sum = 0 := Int.max_eq_left hsumle
        rw [hT, Int.mul_zero]
        omega
  · by_cases hpos : 0 < x.sum
    · have hxne : x ≠ [] := by
        intro hx
        subst x
        simp at hpos
      have hxlen : 0 < x.length := by
        cases x with
        | nil => exact False.elim (hxne rfl)
        | cons a xs => simp
      have hsummem : x.sum ∈ suffs x := by
        unfold suffs
        exact List.mem_map.mpr ⟨0, by rw [List.mem_range]; exact hxlen, by simp⟩
      have hrawge : x.sum ≤ rawMax (suffs x) := rawMax_ge hsummem
      have hrawpos : 0 < rawMax (suffs x) := Int.lt_of_lt_of_le hpos hrawge
      have hM : max0 (suffs x) = rawMax (suffs x) := max0_eq_rawMax_of_pos hrawpos
      have hsne : suffs x ≠ [] := by
        intro he
        have : rawMax (suffs x) = 0 := by simp [rawMax, he]
        omega
      have hrawmem' : rawMax (suffs x) ∈ (suffixLists x).map List.sum := by
        rw [← suffs_eq_map_sum]
        exact rawMax_mem hsne
      rcases List.mem_map.mp hrawmem' with ⟨u, hu, husum⟩
      have hu' : u.sum ∈ suffs x := by
        rw [suffs_eq_map_sum]
        exact List.mem_map.mpr ⟨u, hu, rfl⟩
      rcases suffix_repeat_realize hn hu' (n - 1) (by omega) with ⟨v, hv, hvsum⟩
      have hval : v ≤ max0 (suffs ((List.replicate n x).flatten)) := le_max0_of_mem hv
      rw [hvsum] at hval
      rw [hM]
      have hT : max 0 x.sum = x.sum := Int.max_eq_right (by omega : (0 : Int) ≤ x.sum)
      rw [hT]
      rw [husum] at hval
      exact hval
    · have hsumle : x.sum ≤ 0 := by omega
      have hT : max 0 x.sum = 0 := Int.max_eq_left hsumle
      rw [hT, Int.mul_zero, Int.add_zero]
      by_cases hrawpos : 0 < rawMax (suffs x)
      · have hsne : suffs x ≠ [] := by
          intro he
          have : rawMax (suffs x) = 0 := by simp [rawMax, he]
          omega
        have hM : max0 (suffs x) = rawMax (suffs x) := max0_eq_rawMax_of_pos hrawpos
        have hrawmem' : rawMax (suffs x) ∈ (suffixLists x).map List.sum := by
          rw [← suffs_eq_map_sum]
          exact rawMax_mem hsne
        rcases List.mem_map.mp hrawmem' with ⟨u, hu, husum⟩
        have hu' : u.sum ∈ suffs x := by
          rw [suffs_eq_map_sum]
          exact List.mem_map.mpr ⟨u, hu, rfl⟩
        rcases suffix_repeat_realize hn hu' 0 (by omega) with ⟨v, hv, hvsum⟩
        have hval : v ≤ max0 (suffs ((List.replicate n x).flatten)) := le_max0_of_mem hv
        rw [hvsum] at hval
        rw [husum] at hval
        rw [hM]
        simpa using hval
      · have hM : max0 (suffs x) = 0 := max0_eq_zero_of_nonpos (by omega)
        rw [hM]
        exact max0_nonneg _

theorem sound_RepeatedGain : BestFrame.RepeatedGain.Goal := by
  intro x n hn
  induction n with
  | zero => omega
  | succ n ih =>
      cases n with
      | zero => omega
      | succ m =>
          by_cases hn2 : m = 0
          · subst m
            have hcross := sound_CrossFrame x x
            simpa [List.replicate] using hcross
          · have hn_ge2 : 2 ≤ m + 1 := by omega
            have hprev := ih hn_ge2
            rw [flatten_replicate_succ_right]
            have hcross := sound_CrossFrame ((List.replicate (m + 1) x).flatten) x
            rw [hcross]
            rw [max0_suffs_repeat x (m + 1) (by omega)]
            rw [hprev]
            have hTnonneg : 0 ≤ max 0 x.sum := Int.le_max_left _ _
            have hle : (m + 1 - 2 : Nat) ≤ (m + 1 - 1 : Nat) := by omega
            have hmul : ((m + 1 - 2 : Nat) : Int) * max 0 x.sum ≤
                ((m + 1 - 1 : Nat) : Int) * max 0 x.sum := by
              exact Int.mul_le_mul_of_nonneg_right (by exact_mod_cast hle) hTnonneg
            have hn1 : m + 1 - 1 = m := by omega
            have hn2' : m + 1 + 1 - 2 = m := by omega
            have heq :
                max0 (suffs x) + ((m + 1 - 1 : Nat) : Int) * max 0 x.sum + max0 (prefs x) =
                  max0 (suffs x) + max0 (prefs x) +
                    ((m + 1 + 1 - 2 : Nat) : Int) * max 0 x.sum := by
              rw [hn1, hn2']
              omega
            apply Int.le_antisymm
            · apply Int.max_le.mpr
              constructor
              · apply Int.max_le.mpr
                constructor
                · apply Int.max_le.mpr
                  constructor
                  · exact Int.le_max_left _ _
                  · exact Int.le_trans
                      (Int.add_le_add_left hmul (max0 (suffs x) + max0 (prefs x)))
                      (Int.le_max_right _ _)
                · exact Int.le_max_left _ _
              · rw [heq]
                exact Int.le_max_right _ _
            · apply Int.max_le.mpr
              constructor
              · exact Int.le_trans (Int.le_max_left _ _)
                  (Int.le_trans (Int.le_max_left _ _) (Int.le_max_left _ _))
              · rw [← heq]
                exact Int.le_max_right _ _

/-! ### Nonpositive pages -/

theorem sound_NegativePage : BestFrame.NegativePage.Goal := by
  intro x hx
  unfold BestFrame.measure
  apply Int.le_antisymm
  · apply max0_le (by omega)
    intro v hv
    rcases List.mem_map.mp hv with ⟨w, hw, rfl⟩
    apply sum_nonpos
    intro a ha
    exact hx a (mem_of_window_mem hw a ha)
  · exact max0_nonneg _

/-! ### Prefix drawdown -/

private def pval (x : Series) (a : Nat) : Int := (x.take a).sum

private theorem pval_zero (x : Series) : pval x 0 = 0 := by simp [pval]

private theorem prefs_get {x : Series} {m : Nat} (hm : m < x.length) :
    (prefs x)[m]? = some ((x.take (m + 1)).sum) := by
  unfold prefs
  rw [List.getElem?_map, List.getElem?_range hm]
  simp

private theorem prefs_length (x : Series) : (prefs x).length = x.length := by
  simp [prefs]

private theorem z_length (x : Series) : (0 :: prefs x).length = x.length + 1 := by
  simp [prefs_length]

private theorem z_get {x : Series} {k : Nat} (hk : k ≤ x.length) :
    ((0 :: prefs x).reverse)[k]? = some (pval x (x.length - k)) := by
  have hlen := z_length x
  have hlt : k < (0 :: prefs x).length := by omega
  rw [List.getElem?_reverse hlt]
  rw [hlen]
  have hidx : x.length + 1 - 1 - k = x.length - k := by omega
  rw [hidx]
  by_cases hzero : x.length - k = 0
  · have hklen : k = x.length := by omega
    subst k
    simp [pval]
  · have hpos : 0 < x.length - k := Nat.pos_of_ne_zero hzero
    have hidx2 : x.length - k = (x.length - k - 1) + 1 := by omega
    rw [hidx2, List.getElem?_cons_succ]
    rw [prefs_get (by omega : x.length - k - 1 < x.length)]
    simp [pval]

private theorem z_get_elem {x : Series} {k : Nat} (hk : k ≤ x.length) :
    ((0 :: prefs x).reverse)[k]? = some (pval x (x.length - k)) := z_get hk

private theorem zipIdx_mem {α : Type} (xs : List α) (i n : Nat) (hi : i < xs.length) :
    (xs[i], n + i) ∈ xs.zipIdx n := by
  induction xs generalizing i n with
  | nil => simp at hi
  | cons a as ih =>
      cases i with
      | zero => simp
      | succ i =>
          have hi' : i < as.length := by
            simp only [List.length_cons] at hi
            omega
          simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih i (n + 1) hi'

private theorem sum_take_sub (x : Series) {a b : Nat} (hab : a ≤ b) :
    (x.take b).sum - (x.take a).sum = ((x.drop a).take (b - a)).sum := by
  induction a generalizing x b with
  | zero => simp
  | succ a ih =>
      cases x with
      | nil => simp
      | cons c cs =>
          have hbpos : 0 < b := by omega
          rw [show b = (b - 1) + 1 by omega]
          rw [List.take_succ_cons, List.take_succ_cons]
          simp only [List.sum_cons]
          rw [List.drop_succ_cons]
          have heq3 : b - 1 + 1 - (a + 1) = b - 1 - a := by omega
          rw [heq3]
          rw [← ih (x := cs) (b := b - 1) (by omega)]
          grind

private theorem frame_sum_pval {x : Series} {v : Int} (hv : v ∈ (windows x).map List.sum) :
    ∃ a b : Nat, a < b ∧ b ≤ x.length ∧ v = pval x b - pval x a := by
  rcases List.mem_map.mp hv with ⟨w, hw, rfl⟩
  rcases window_exists hw with ⟨i, hi, j, hj, rfl⟩
  refine ⟨i, i + j + 1, by omega, ?_, ?_⟩
  · omega
  · simp only [pval]
    rw [sum_take_sub x (by omega : i ≤ i + j + 1)]
    rw [show i + j + 1 - i = j + 1 by omega]

private theorem pval_diff_frame_mem (x : Series) {a b : Nat} (hab : a < b)
    (hble : b ≤ x.length) : pval x b - pval x a ∈ (windows x).map List.sum := by
  refine List.mem_map.mpr ⟨(x.drop a).take (b - a), ?_, ?_⟩
  · unfold windows
    refine List.mem_flatMap.mpr ⟨a, ?_, ?_⟩
    · simp only [List.mem_range]
      omega
    · refine List.mem_map.mpr ⟨b - a - 1, ?_, ?_⟩
      · simp only [List.mem_range]
        omega
      · rw [show (b - a - 1) + 1 = b - a by omega]
  · simp only [pval]
    rw [show b - a = b - a by rfl]
    rw [sum_take_sub x (by omega : a ≤ b)]

private theorem pairdiff_of_pval {x : Series} {a b : Nat} (hab : a < b) (hble : b ≤ x.length) :
    pval x b - pval x a ∈
      ((pairs ((0 :: prefs x).reverse)).map (fun p : Int × Int => p.1 - p.2)) := by
  let z := (0 :: prefs x).reverse
  have hzlen : z.length = x.length + 1 := by dsimp [z]; simp [prefs_length]
  have hia : x.length - b < z.length := by omega
  have hib : x.length - a < z.length := by omega
  have hzi : z[x.length - b]'hia = pval x b := by
    have ho := z_get_elem (x := x) (k := x.length - b) (by omega)
    have h2 := List.getElem?_eq_getElem (l := z) (i := x.length - b) hia
    rw [h2] at ho
    have hidx : x.length - (x.length - b) = b := by omega
    simpa [hidx] using Option.some.inj ho
  have hzj : z[x.length - a]'hib = pval x a := by
    have ho := z_get_elem (x := x) (k := x.length - a) (by omega)
    have h2 := List.getElem?_eq_getElem (l := z) (i := x.length - a) hib
    rw [h2] at ho
    have hidx : x.length - (x.length - a) = a := by omega
    simpa [hidx] using Option.some.inj ho
  unfold pairs
  apply List.mem_map.mpr
  refine ⟨(pval x b, pval x a), ?_, ?_⟩
  · apply List.mem_flatMap.mpr
    refine ⟨(z[x.length - b]'hia, x.length - b), ?_, ?_⟩
    · change (z[x.length - b]'hia, x.length - b) ∈ z.zipIdx
      simpa using zipIdx_mem z (x.length - b) 0 hia
    · apply List.mem_map.mpr
      refine ⟨z[x.length - a]'hib, ?_, ?_⟩
      · apply List.mem_drop_iff_getElem.mpr
        have hkb : (x.length - b) + 1 ≤ x.length - a := by omega
        have hadd : (x.length - b) + 1 + ((x.length - a) - ((x.length - b) + 1)) =
            x.length - a := Nat.add_sub_of_le hkb
        refine ⟨(x.length - a) - ((x.length - b) + 1), ?_, ?_⟩
        · rw [show (x.length - a - (x.length - b + 1)) + (x.length - b + 1) =
              (x.length - b + 1) + (x.length - a - (x.length - b + 1)) by omega]
          rw [hadd]
          exact hib
        · congr 1
      · rw [hzi, hzj]
  · simp

private theorem pair_pval_of_mem {x : Series} {p : Int × Int}
    (hp : p ∈ pairs ((0 :: prefs x).reverse)) :
    ∃ a b : Nat, a < b ∧ b ≤ x.length ∧ p.1 - p.2 = pval x b - pval x a := by
  let z := (0 :: prefs x).reverse
  have hzlen : z.length = x.length + 1 := by dsimp [z]; simp [prefs_length]
  unfold pairs at hp
  rcases List.mem_flatMap.mp hp with ⟨q, hq, hp⟩
  have hqinfo := List.mem_zipIdx hq
  rcases List.mem_map.mp hp with ⟨r, hr, hrp⟩
  rw [List.mem_drop_iff_getElem] at hr
  rcases hr with ⟨t, ht, hrt⟩
  let i : Nat := q.2
  let j : Nat := i + 1 + t
  have hi_lt : i < z.length := by
    dsimp [i]
    rw [hzlen]
    simpa [prefs_length] using hqinfo.2.1
  have hj_lt : j < z.length := by
    dsimp [j, i]
    rw [hzlen] at ht ⊢
    omega
  have hi_le : i ≤ x.length := by omega
  have hj_le : j ≤ x.length := by omega
  have hij : i < j := by dsimp [j]; omega
  have hqval : q.1 = z[i]'hi_lt := hqinfo.2.2
  have hrval : r = z[j]'hj_lt := by
    dsimp [j, i] at hrt ⊢
    simpa [z, Nat.add_assoc] using hrt.symm
  refine ⟨x.length - j, x.length - i, ?_, ?_, ?_⟩
  · omega
  · omega
  · rw [← hrp]
    simp only [Prod.fst, Prod.snd]
    rw [hqval, hrval]
    have hzi : z[i]'hi_lt = pval x (x.length - i) := by
      have ho := z_get_elem (x := x) (k := i) hi_le
      have h2 := List.getElem?_eq_getElem (l := z) (i := i) hi_lt
      rw [h2] at ho
      exact Option.some.inj ho
    have hzj : z[j]'hj_lt = pval x (x.length - j) := by
      have ho := z_get_elem (x := x) (k := j) hj_le
      have h2 := List.getElem?_eq_getElem (l := z) (i := j) hj_lt
      rw [h2] at ho
      exact Option.some.inj ho
    rw [hzi, hzj]

theorem sound_PrefixDrawdown : BestFrame.PrefixDrawdown.Goal := by
  intro x
  apply Int.le_antisymm
  · unfold BestFrame.measure
    apply max0_le (max0_nonneg _)
    intro v hv
    rcases frame_sum_pval hv with ⟨a, b, hab, hble, rfl⟩
    exact le_max0_of_mem (pairdiff_of_pval hab hble)
  · unfold Drawdown.measure
    apply max0_le (max0_nonneg _)
    intro d hd
    rcases List.mem_map.mp hd with ⟨p, hp, rfl⟩
    rcases p with ⟨p₁, p₂⟩
    rcases pair_pval_of_mem hp with ⟨a, b, hab, hble, hpval⟩
    change p₁ - p₂ ≤ max0 ((windows x).map List.sum)
    rw [hpval]
    exact le_max0_of_mem (pval_diff_frame_mem x hab hble)

/-! ### Deletion fence -/

private theorem windows_cons_prefix (a : Int) (xs : Series) :
    windows (a :: xs) =
      [[a]] ++ ((List.range xs.length).map (fun i => a :: xs.take (i + 1))) ++ windows xs := by
  unfold windows
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.flatMap_append, List.take_succ_cons, List.take_zero,
    Nat.add_zero, Nat.succ_eq_add_one]
  congr 2
  · simp [List.range_succ_eq_map, List.drop_zero, List.take_succ_cons,
      List.take_zero, Nat.add_assoc]
  · funext i
    simp [List.drop_drop, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private def dropPen (x : Series) (keep : Int → Bool) : Int :=
  ((x.filter (fun v => !keep v)).map (fun v => max 0 (-v))).sum

private theorem dropPen_nonneg (x : Series) (keep : Int → Bool) : 0 ≤ dropPen x keep := by
  unfold dropPen
  apply sum_map_nonneg
  intro v hv
  exact Int.le_max_left _ _

private theorem prefix_filter_window (a : Int) (xs : Series) (keep : Int → Bool)
    (hkeep : keep a = true) (m : Nat) (hm : m < (xs.filter keep).length) :
    ∃ k : Nat, k < (a :: xs).length ∧
      a + ((xs.filter keep).take (m + 1)).sum ≤
        ((a :: xs).take (k + 1)).sum + dropPen xs keep := by
  induction xs generalizing m with
  | nil => simp at hm
  | cons b bs ih =>
      by_cases hb : keep b = true
      · have hfilt : (b :: bs).filter keep = b :: bs.filter keep := by simp [hb]
        have hpen : dropPen (b :: bs) keep = dropPen bs keep := by simp [dropPen, hb]
        rw [hfilt] at hm
        cases m with
        | zero =>
            refine ⟨1, by simp, ?_⟩
            rw [hpen, hfilt]
            simp
            have hnon := dropPen_nonneg bs keep
            omega
        | succ m =>
            have hm' : m < (bs.filter keep).length := by
              simp only [List.length_cons] at hm
              exact Nat.lt_of_succ_lt_succ hm
            rcases ih m hm' with ⟨k, hk, hle⟩
            refine ⟨k + 1, ?_, ?_⟩
            · simp at hk ⊢
              omega
            · rw [hpen, hfilt]
              have ht1 : (List.take (k + 2) (a :: b :: bs)).sum =
                  a + b + (bs.take k).sum := by
                rw [show k + 2 = (k + 1) + 1 by omega, List.take_succ_cons]
                rw [List.take_succ_cons]
                simp
                omega
              have ht2 : (List.take (k + 1) (a :: bs)).sum = a + (bs.take k).sum := by
                rw [List.take_succ_cons]
                simp
              have htF : (List.take (m + 2) (b :: bs.filter keep)).sum =
                  b + (List.take (m + 1) (bs.filter keep)).sum := by
                rw [show m + 2 = (m + 1) + 1 by omega, List.take_succ_cons]
                simp
              rw [ht1, htF]
              rw [ht2] at hle
              omega
      · have hfilt : (b :: bs).filter keep = bs.filter keep := by simp [hb]
        have hpen : dropPen (b :: bs) keep = max 0 (-b) + dropPen bs keep := by
          simp [dropPen, hb]
        rw [hfilt] at hm
        rcases ih m hm with ⟨k, hk, hle⟩
        refine ⟨k + 1, ?_, ?_⟩
        · simp only [List.length_cons] at hk ⊢
          omega
        · rw [hpen, hfilt]
          have ht : (List.take (k + 2) (a :: b :: bs)).sum =
              (List.take (k + 1) (a :: bs)).sum + b := by
            rw [show k + 2 = (k + 1) + 1 by omega, List.take_succ_cons]
            rw [List.take_succ_cons]
            simp
            grind
          rw [ht]
          have hbpen : (0 : Int) ≤ b + max 0 (-b) := by
            by_cases hbneg : b ≤ 0
            · have hm0 : max 0 (-b) = -b := Int.max_eq_right (by omega)
              rw [hm0]
              omega
            · have hm0 : max 0 (-b) = 0 := Int.max_eq_left (by omega)
              rw [hm0]
              omega
          have hnon := dropPen_nonneg bs keep
          omega

private theorem cons_window_classify {a : Int} {xs w : Series} (hw : w ∈ windows (a :: xs)) :
    w = [a] ∨ (∃ i, i < xs.length ∧ w = a :: xs.take (i + 1)) ∨ w ∈ windows xs := by
  rcases window_exists hw with ⟨idx, hidx, j, hj, hw_eq⟩
  by_cases idx0 : idx = 0
  · subst idx
    by_cases hj0 : j = 0
    · subst j
      left
      simpa using hw_eq
    · right
      left
      refine ⟨j - 1, ?_, ?_⟩
      · simp only [List.length_cons] at hj
        omega
      · rw [hw_eq]
        simp only [List.drop_zero]
        cases j with
        | zero => contradiction
        | succ j =>
            simp [List.take_succ_cons]
  · right
    right
    unfold windows
    refine List.mem_flatMap.mpr ⟨idx - 1, ?_, ?_⟩
    · rw [List.mem_range]
      simp only [List.length_cons] at hidx
      omega
    · refine List.mem_map.mpr ⟨j, ?_, ?_⟩
      · rw [List.mem_range]
        simp only [List.length_cons] at hj
        omega
      · rw [hw_eq]
        rw [show idx = (idx - 1) + 1 by omega, List.drop_succ_cons]
        rw [show idx - 1 + 1 - 1 = idx - 1 by omega]

theorem sound_DeletionFence : BestFrame.DeletionFence.Goal := by
  intro x keep
  change BestFrame.measure (x.filter keep) ≤ BestFrame.measure x + dropPen x keep
  induction x with
  | nil => simp [BestFrame.measure, dropPen]
  | cons a xs ih =>
      by_cases hkeep : keep a = true
      · have hf : (a :: xs).filter keep = a :: xs.filter keep := by simp [hkeep]
        have hp : dropPen (a :: xs) keep = dropPen xs keep := by simp [dropPen, hkeep]
        rw [hf, hp]
        unfold BestFrame.measure at ih ⊢
        apply max0_le
        · have hm := max0_nonneg ((windows (a :: xs)).map List.sum)
          have hpen := dropPen_nonneg xs keep
          omega
        · intro v hv
          rcases List.mem_map.mp hv with ⟨w, hw, rfl⟩
          rcases cons_window_classify hw with hw | hw | hw
          · subst w
            simp only [List.sum_cons, List.sum_nil, Int.add_zero]
            have ha_mem : [a] ∈ windows (a :: xs) := by
              unfold windows
              refine List.mem_flatMap.mpr ⟨0, ?_, ?_⟩
              · rw [List.mem_range]
                simp
              · refine List.mem_map.mpr ⟨0, ?_, ?_⟩
                · rw [List.mem_range]
                  simp
                · simp
            have hale : a ≤ max0 ((windows (a :: xs)).map List.sum) := by
              exact le_max0_of_mem (List.mem_map.mpr ⟨[a], ha_mem, by simp⟩)
            have hpen := dropPen_nonneg xs keep
            omega
          · rcases hw with ⟨i, hi, rfl⟩
            simp only [List.sum_cons]
            rcases prefix_filter_window a xs keep hkeep i hi with ⟨k, hk, hle⟩
            have hwin : (a :: xs).take (k + 1) ∈ windows (a :: xs) := by
              apply prefixLists_mem_windows
              unfold prefixLists
              exact List.mem_map.mpr ⟨k, by rw [List.mem_range]; exact hk, rfl⟩
            have hwle : ((a :: xs).take (k + 1)).sum ≤ max0 ((windows (a :: xs)).map List.sum) := by
              exact le_max0_of_mem (List.mem_map.mpr ⟨(a :: xs).take (k + 1), hwin, rfl⟩)
            omega
          · have hwle : w.sum ≤ max0 ((windows (xs.filter keep)).map List.sum) := by
              exact le_max0_of_mem (List.mem_map.mpr ⟨w, hw, rfl⟩)
            have hxs_le : BestFrame.measure xs ≤ BestFrame.measure (a :: xs) :=
              measure_le_of_subset (fun u hu => window_mem_cons_tail hu)
            unfold BestFrame.measure at hxs_le
            omega
      · have hf : (a :: xs).filter keep = xs.filter keep := by simp [hkeep]
        have hp : dropPen (a :: xs) keep = max 0 (-a) + dropPen xs keep := by
          simp [dropPen, hkeep]
        rw [hf, hp]
        have hxs_le : BestFrame.measure xs ≤ BestFrame.measure (a :: xs) :=
          measure_le_of_subset (fun w hw => window_mem_cons_tail hw)
        have hpen := dropPen_nonneg xs keep
        have hbpen : 0 ≤ max 0 (-a) := Int.le_max_left _ _
        omega

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining
