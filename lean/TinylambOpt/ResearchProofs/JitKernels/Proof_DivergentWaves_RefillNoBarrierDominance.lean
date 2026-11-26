import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! SOUND: Eager lane refilling never finishes later than synchronized packet barriers.
Split the task list into consecutive packets of at most `width` tasks. While one packet is
processed, the minimum lane load never exceeds the bound carried over from previous
packets: the packet has at most `width` tasks, and before its `t`-th task at most `t`
lanes have been touched, so some untouched lane still holds its old load, which is at or
below that bound. Every task is added to the minimum lane and is at most the packet's
peak, hence after the packet every lane is bounded by the old bound plus that peak.
Summing packet peaks yields exactly the barrier cost `ticks w steps`, therefore
`refill w steps ≤ ticks w steps`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_RefillNoBarrierDominance
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

private theorem div_step (n w : Nat) (hw : 0 < w) : (n + w) / w = n / w + 1 := by
  rw [Nat.add_comm, Nat.add_div hw]
  rw [Nat.mod_self, Nat.div_self hw]
  have hlt : n % w < w := Nat.mod_lt n hw
  rw [if_neg (by omega)]
  omega

private theorem map_range_split {α : Type} (f : Nat → α) :
    ∀ m : Nat, List.map f (List.range (m + 1)) =
      f 0 :: List.map (fun j => f (j + 1)) (List.range m) := by
  intro m
  induction m with
  | zero => rw [Nat.zero_add, List.range_one]; simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, ih,
        List.range_succ, List.map_append, List.map_cons]
      simp

/-- `map f (range (m+1)) = f 0 :: map g (range m)`. -/
private theorem map_range_eq_cons {α : Type} :
    ∀ (f g : Nat → α) (a : α), f 0 = a → (∀ j, f (j + 1) = g j) →
      ∀ m : Nat, List.map f (List.range (m + 1)) = a :: List.map g (List.range m) := by
  intro f g a h0 h1 m
  rw [map_range_split, h0]
  refine congrArg (a :: ·) (List.map_congr_left (fun j _ => ?_))
  exact h1 j

private theorem waves_nil (w : Nat) (hw : 0 < w) : waves w [] = [] := by
  unfold waves
  rw [List.length_nil, Nat.div_eq_of_lt (by omega), List.range_zero, List.map_nil]

private theorem waves_short (w : Nat) (xs : List Nat) (hw : 0 < w) (h1 : 0 < xs.length)
    (h2 : xs.length ≤ w) : waves w xs = [peak xs] := by
  unfold waves
  have hsub : xs.length + w - 1 = w + (xs.length - 1) := by omega
  rw [hsub, Nat.add_comm, div_step (xs.length - 1) w hw, Nat.div_eq_of_lt (by omega),
    Nat.zero_add, List.range_one]
  simp [List.drop_zero, List.take_of_length_le h2]

private theorem waves_long (w : Nat) (xs : List Nat) (hw : 0 < w) (h : w < xs.length) :
    waves w xs = peak (xs.take w) :: waves w (xs.drop w) := by
  have hlen : (xs.drop w).length = xs.length - w := List.length_drop
  have hdiv : (xs.length + w - 1) / w = (xs.length - w + w - 1) / w + 1 := by
    have h1 : xs.length - w + w - 1 = xs.length - 1 := by omega
    rw [h1, show xs.length + w - 1 = xs.length - 1 + w by omega, div_step _ _ hw]
  unfold waves
  rw [hlen, hdiv, map_range_eq_cons
    (fun j => peak ((xs.drop (j * w)).take w))
    (fun j => peak (((xs.drop w).drop (j * w)).take w))
    (peak (xs.take w)) (by simp [List.drop_zero])
    (fun j => by
      rw [List.drop_drop,
        show (j + 1) * w = w + j * w by rw [Nat.add_mul, Nat.one_mul, Nat.add_comm]])]

/-- The running maximum dominates the accumulator and every element. -/
private theorem le_foldl_max :
    ∀ (as : List Nat) (init : Nat), init ≤ as.foldl max init := by
  intro as
  induction as with
  | nil => intro init; exact Nat.le_refl init
  | cons a as ih =>
      intro init
      show init ≤ as.foldl max (max init a)
      exact Nat.le_trans (Nat.le_max_left init a) (ih (max init a))

private theorem mem_foldl_max :
    ∀ (as : List Nat) (init x : Nat), x ∈ as → x ≤ as.foldl max init := by
  intro as
  induction as with
  | nil => intro init x h; cases h
  | cons a as ih =>
      intro init x h
      show x ≤ as.foldl max (max init a)
      rw [List.mem_cons] at h
      cases h with
      | inl he =>
          rw [he]
          exact Nat.le_trans (Nat.le_max_right init a) (le_foldl_max as (max init a))
      | inr hx => exact ih (max init a) x hx

/-- Every task is at most its packet's peak. -/
private theorem peak_le_mem : ∀ (x : Nat) (xs : List Nat), x ∈ xs → x ≤ peak xs := by
  intro x xs h
  cases xs with
  | nil => cases h
  | cons a as =>
      rw [List.mem_cons] at h
      cases h with
      | inl he => rw [he]; exact le_foldl_max as a
      | inr hx => exact mem_foldl_max as a x hx

private theorem foldlMaxLE :
    ∀ (as : List Nat) (init c : Nat), init ≤ c → (∀ x ∈ as, x ≤ c) → as.foldl max init ≤ c := by
  intro as
  induction as with
  | nil => intro init c h1 _; exact h1
  | cons b as ih =>
      intro init c h1 h2
      show as.foldl max (max init b) ≤ c
      exact ih (max init b) c (Nat.max_le.mpr ⟨h1, h2 b (by simp)⟩)
        (fun x hx => h2 x (by simp [hx]))

/-- If every element is bounded by `c`, so is the peak. -/
private theorem peak_le_bound :
    ∀ (xs : List Nat) (c : Nat), (∀ x ∈ xs, x ≤ c) → peak xs ≤ c := by
  intro xs c h
  cases xs with
  | nil => exact Nat.zero_le c
  | cons a as =>
      show as.foldl max a ≤ c
      exact foldlMaxLE as a c (h a (by simp)) (fun x hx => h x (by simp [hx]))

/-- The running minimum is dominated by the accumulator and every element. -/
private theorem foldl_min_le_init :
    ∀ (as : List Nat) (init : Nat), as.foldl min init ≤ init := by
  intro as
  induction as with
  | nil => intro init; exact Nat.le_refl init
  | cons a as ih =>
      intro init
      show as.foldl min (min init a) ≤ init
      exact Nat.le_trans (ih (min init a)) (Nat.min_le_left init a)

private theorem foldl_min_le_mem :
    ∀ (as : List Nat) (init x : Nat), x ∈ as → as.foldl min init ≤ x := by
  intro as
  induction as with
  | nil => intro init x h; cases h
  | cons a as ih =>
      intro init x h
      show as.foldl min (min init a) ≤ x
      rw [List.mem_cons] at h
      cases h with
      | inl he =>
          rw [he]
          exact Nat.le_trans (foldl_min_le_init as (min init a)) (Nat.min_le_right init a)
      | inr hx => exact ih (min init a) x hx

/-- The extracted minimum is below every element, and is itself a member. -/
private theorem min_getD_le_mem :
    ∀ (l : List Nat) (x : Nat), x ∈ l → l.min?.getD 0 ≤ x := by
  intro l x h
  cases l with
  | nil => cases h
  | cons a as =>
      show List.foldl min a as ≤ x
      rw [List.mem_cons] at h
      cases h with
      | inl he => rw [he]; exact foldl_min_le_init as a
      | inr hx => exact foldl_min_le_mem as a x hx

private theorem foldl_min_mem :
    ∀ (as : List Nat) (init : Nat), as.foldl min init ∈ init :: as := by
  intro as
  induction as with
  | nil => intro init; exact List.Mem.head _
  | cons b as ih =>
      intro init
      have hmem : min init b ∈ init :: b :: as := by
        by_cases hb : init ≤ b
        · rw [Nat.min_eq_left hb]; exact List.Mem.head _
        · rw [Nat.min_eq_right (by omega)]; exact List.Mem.tail _ (List.Mem.head _)
      have h := ih (min init b)
      rw [List.mem_cons] at h
      cases h with
      | inl he => rw [← he] at hmem; exact hmem
      | inr hx => exact List.Mem.tail _ (List.Mem.tail _ hx)

private theorem min_getD_mem : ∀ (l : List Nat), l ≠ [] → l.min?.getD 0 ∈ l := by
  intro l hl
  cases l with
  | nil => exact absurd rfl hl
  | cons a as => exact foldl_min_mem as a

/-- `modify` changes one position; everything else is untouched. -/
private theorem modify_le :
    ∀ (l : List Nat) (i : Nat) (f : Nat → Nat) (V : Nat),
      (∀ x ∈ l, x ≤ V) → ((hi : i < l.length) → f (l[i]'hi) ≤ V) →
        ∀ y ∈ l.modify i f, y ≤ V := by
  intro l
  induction l with
  | nil =>
      intro i f U h1 h2 y hy
      rw [List.modify_nil] at hy
      cases hy
  | cons a as ih =>
      intro i f U h1 h2 y hy
      match i with
      | 0 =>
          have h2' : f a ≤ U := h2 (Nat.succ_pos _)
          have hmem : y ∈ f a :: as := hy
          rw [List.mem_cons] at hmem
          cases hmem with
          | inl he => rw [he]; exact h2'
          | inr hx => exact h1 y (List.Mem.tail _ hx)
      | k + 1 =>
          have hmem : y ∈ a :: as.modify k f := hy
          rw [List.mem_cons] at hmem
          cases hmem with
          | inl he => rw [he]; exact h1 a (List.Mem.head _)
          | inr hx =>
              exact ih k f U (fun x hx' => h1 x (List.Mem.tail _ hx'))
                (fun hi => h2 (Nat.succ_lt_succ hi)) y hx

/-- One `modify` removes at most one element from the below-threshold filter. -/
private theorem filter_len_modify_le :
    ∀ (l : List Nat) (i : Nat) (f : Nat → Nat) (U : Nat),
      (l.filter (fun x => x ≤ U)).length ≤
        ((l.modify i f).filter (fun x => x ≤ U)).length + 1 := by
  intro l
  induction l with
  | nil => intro i f U; simp
  | cons a as ih =>
      intro i f U
      match i with
      | 0 =>
          have he : (a :: as).modify 0 f = f a :: as := rfl
          have ha : ((a :: as).filter (fun x => x ≤ U)).length ≤
              (as.filter (fun x => x ≤ U)).length + 1 := by
            by_cases h : a ≤ U
            · rw [List.filter_cons_of_pos (p := fun x => x ≤ U) (by simpa using h),
                List.length_cons]
              omega
            · rw [List.filter_cons_of_neg (p := fun x => x ≤ U) (by simpa using h)]
              omega
          have hf : (as.filter (fun x => x ≤ U)).length ≤
              ((f a :: as).filter (fun x => x ≤ U)).length := by
            by_cases h : f a ≤ U
            · rw [List.filter_cons_of_pos (p := fun x => x ≤ U) (by simpa using h),
                List.length_cons]
              omega
            · rw [List.filter_cons_of_neg (p := fun x => x ≤ U) (by simpa using h)]
              omega
          rw [he]
          omega
      | k + 1 =>
          have he : (a :: as).modify (k + 1) f = a :: as.modify k f := rfl
          rw [he]
          by_cases h : a ≤ U
          · rw [List.filter_cons_of_pos (p := fun x => x ≤ U) (by simpa using h),
              List.filter_cons_of_pos (p := fun x => x ≤ U) (by simpa using h),
              List.length_cons, List.length_cons]
            have hi : (as.filter (fun x => x ≤ U)).length ≤
                ((as.modify k f).filter (fun x => x ≤ U)).length + 1 := ih k f U
            omega
          · rw [List.filter_cons_of_neg (p := fun x => x ≤ U) (by simpa using h),
              List.filter_cons_of_neg (p := fun x => x ≤ U) (by simpa using h)]
            exact ih k f U

/-- The fold over `assign` preserves the lane count. -/
private theorem foldl_assign_length :
    ∀ (steps : List Nat) (loads : List Nat),
      (steps.foldl assign loads).length = loads.length := by
  intro steps
  induction steps with
  | nil => intro loads; rfl
  | cons s ss ih =>
      intro loads
      have key : (s :: ss).foldl assign loads = ss.foldl assign (assign loads s) := rfl
      rw [key, ih]
      show (loads.modify (loads.idxOf (loads.min?.getD 0)) (fun x => x + s)).length =
        loads.length
      rw [List.length_modify]

/-- One `assign` keeps all loads bounded when the minimum plus the task fits. -/
private theorem assign_le_aux :
    ∀ (l : List Nat) (s U : Nat) (y : Nat), y ∈ assign l s →
      (∀ x ∈ l, x ≤ U) → l.min?.getD 0 + s ≤ U → y ≤ U := by
  intro l s U y hy h1 h2
  have hy' : y ∈ l.modify (l.idxOf (l.min?.getD 0)) (fun x => x + s) := hy
  have hle : ∀ x ∈ l.modify (l.idxOf (l.min?.getD 0)) (fun x => x + s), x ≤ U := by
    refine modify_le l _ _ U h1 ?_
    intro hlt
    rw [List.getElem_idxOf hlt]
    exact h2
  exact hle y hy'

/-- Processing one packet of at most `w` tasks adds at most the packet peak;
`t` counts the lanes of `l` already touched by the current packet. -/
private theorem chunkBounded :
    ∀ (w U P : Nat) (r : List Nat) (t : Nat) (l : List Nat), 0 < w → l.length = w →
      t + r.length ≤ w → (∀ x ∈ l, x ≤ U + P) → (∀ x ∈ r, x ≤ P) →
      w ≤ t + (l.filter (fun x => x ≤ U)).length →
      ∀ y ∈ r.foldl assign l, y ≤ U + P := by
  intro w U P r
  induction r with
  | nil =>
      intro t l hw hlen hsum h1 h2 h3 y hy
      exact h1 y hy
  | cons s r ih =>
      intro t l hw hlen hsum h1 h2 h3 y hy
      have hlenr : (s :: r).length = r.length + 1 := rfl
      rw [hlenr] at hsum
      have hy2 : y ∈ r.foldl assign (assign l s) := hy
      refine ih (t + 1) (assign l s) hw ?_ ?_ ?_ ?_ ?_ y hy2
      · have key : (assign l s).length = l.length := by
            show (l.modify (l.idxOf (l.min?.getD 0)) (fun x => x + s)).length = l.length
            rw [List.length_modify]
        rw [key]
        exact hlen
      · omega
      · have hminU : l.min?.getD 0 ≤ U := by
            have hpos : 0 < (l.filter (fun x => x ≤ U)).length := by omega
            obtain ⟨z, hz⟩ := List.length_pos_iff_exists_mem.mp hpos
            have hzm : z ∈ l := (List.mem_filter.mp hz).1
            have hzv : z ≤ U := decide_eq_true_eq.mp (List.mem_filter.mp hz).2
            exact Nat.le_trans (min_getD_le_mem l z hzm) hzv
        intro x hx
        exact assign_le_aux l s (U + P) x hx h1 (by
          have hs' : s ≤ P := h2 s (by simp)
          omega)
      · intro x hx
        exact h2 x (by simp [hx])
      · have h : (l.filter (fun x => x ≤ U)).length ≤
            ((assign l s).filter (fun x => x ≤ U)).length + 1 := by
          show (l.filter (fun x => x ≤ U)).length ≤
              ((l.modify (l.idxOf (l.min?.getD 0)) (fun x => x + s)).filter (fun x => x ≤ U)).length + 1
          exact filter_len_modify_le l (l.idxOf (l.min?.getD 0)) (fun x => x + s) U
        omega

/-- Processing every leading packet bounds each lane by the carried bound plus the
sum of the wave peaks. -/
private theorem foldsBounded :
    ∀ (w : Nat), 0 < w → ∀ n : Nat, ∀ steps : List Nat, steps.length ≤ n →
      ∀ (V : Nat) (loads : List Nat), loads.length = w → (∀ x ∈ loads, x ≤ V) →
      ∀ y ∈ steps.foldl assign loads, y ≤ V + (waves w steps).sum := by
  intro w hw n
  induction n with
  | zero =>
      intro steps hlen V loads hl h1 y hy
      match steps, hlen with
      | [], _ =>
          rw [waves_nil w hw, List.sum_nil, Nat.add_zero]
          exact h1 y hy
  | succ k ih =>
      intro steps hlen V loads hl h1 y hy
      by_cases hz : steps = []
      · subst hz
        rw [waves_nil w hw, List.sum_nil, Nat.add_zero]
        exact h1 y hy
      · have hpos : 0 < steps.length := List.length_pos_iff.mpr hz
        have hfilt : (loads.filter (fun x => x ≤ V)).length = loads.length := by
          rw [List.length_filter_eq_length_iff]
          intro a ha
          exact decide_eq_true_eq.mpr (h1 a ha)
        by_cases hsmall : steps.length ≤ w
        · have hwv : waves w steps = [peak steps] := waves_short w steps hw hpos hsmall
          have hres : ∀ y ∈ steps.foldl assign loads, y ≤ V + peak steps :=
            chunkBounded w V (peak steps) steps 0 loads hw hl (by omega)
              (fun x hx => Nat.le_trans (h1 x hx) (Nat.le_add_right V (peak steps)))
              (fun x hx => peak_le_mem x steps hx)
              (by rw [hfilt]; omega)
          rw [hwv, List.sum_cons, List.sum_nil, Nat.add_zero]
          exact hres y hy
        · have hge : w < steps.length := by omega
          have hta : (steps.take w).length = w := by
            rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hge)]
          have hdp : (steps.drop w).length ≤ k := by
            rw [List.length_drop]; omega
          have hsplit : steps.foldl assign loads =
              (steps.drop w).foldl assign ((steps.take w).foldl assign loads) := by
            rw [← List.foldl_append, List.take_append_drop]
          have hfirst : ∀ z ∈ (steps.take w).foldl assign loads,
              z ≤ V + peak (steps.take w) :=
            chunkBounded w V (peak (steps.take w)) (steps.take w) 0 loads hw hl
              (by omega)
              (fun x hx => Nat.le_trans (h1 x hx) (Nat.le_add_right V (peak (steps.take w))))
              (fun x hx => peak_le_mem x (steps.take w) hx)
              (by rw [hfilt]; omega)
          have hlenMid : ((steps.take w).foldl assign loads).length = w := by
            rw [foldl_assign_length]; exact hl
          rw [hsplit] at hy
          have hres := ih (steps.drop w) hdp (V + peak (steps.take w)) _ hlenMid hfirst y hy
          rw [waves_long w steps hw hge, List.sum_cons]
          omega

/-- Eager refilling costs no more than barrier-synchronized waves. -/
theorem sound : RefillNoBarrierDominance.Goal := by
  intro steps w hw
  show peak (steps.foldl assign (List.replicate w 0)) ≤ (waves w steps).sum
  have hmem : ∀ y ∈ steps.foldl assign (List.replicate w 0),
      y ≤ 0 + (waves w steps).sum :=
    foldsBounded w hw steps.length steps (Nat.le_refl _) 0 (List.replicate w 0)
      (by rw [List.length_replicate])
      (by intro x hx; rw [List.mem_replicate] at hx; omega)
  rw [Nat.zero_add] at hmem
  exact peak_le_bound _ _ hmem

end TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_RefillNoBarrierDominance
