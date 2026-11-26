import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_SharedBuild

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.SharedBuild

-- general list lemmas
theorem map_filter_sum_eq_map_ite_sum {α} (p : α → Bool) (f : α → Nat) (l : List α) :
  ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.filter_cons, List.map_cons, List.sum_cons]
    cases hp : p x <;> simp [hp, ih]

theorem sum_map_le_sum_map {α} (f g : α → Nat) (l : List α)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp only [List.map_cons, List.sum_cons]
    have hx := h x (by simp)
    have hxs : ∀ y ∈ xs, f y ≤ g y := by intro y hy; exact h y (by simp [hy])
    have := ih hxs
    omega

theorem sum_map_add {α} (f g : α → Nat) (l : List α) :
  (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp only [List.map_cons, List.sum_cons]
    omega

theorem sum_map_zero {α} (l : List α) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

theorem eraseDups_nodup (l : List Nat) : l.eraseDups.Nodup := by
  induction h : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
    cases l with
    | nil => simp
    | cons x xs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · simp [List.mem_eraseDups]
      · apply ih (List.filter (fun b => !b == x) xs).length
        · simp only [List.length_cons] at h
          have hle : (List.filter (fun b => !b == x) xs).length ≤ xs.length := List.length_filter_le _ _
          omega
        · rfl

theorem sum_filter_mem_le_sum (w : Nat → Nat) (l : List Nat) (hnd : l.Nodup) (T : List Nat) :
  ((l.filter (fun k => decide (k ∈ T))).map w).sum ≤ (T.map w).sum := by
  induction l generalizing T with
  | nil => simp
  | cons k ks ih =>
    simp only [List.filter_cons, List.map_cons, List.sum_cons]
    by_cases hkT : k ∈ T
    · simp [hkT]
      obtain ⟨pre, suf, hT⟩ := List.mem_iff_append.mp hkT
      subst T
      have hndks : ks.Nodup := (List.nodup_cons.mp hnd).2
      have hkks : k ∉ ks := (List.nodup_cons.mp hnd).1
      have hfilter_eq : (ks.filter (fun x => decide (x ∈ pre ++ k :: suf))) =
                        (ks.filter (fun x => decide (x ∈ pre ++ suf))) := by
        apply List.filter_congr
        intro x hx
        have hxk : x ≠ k := by intro h; subst x; exact hkks hx
        simp [hxk]
      rw [hfilter_eq]
      have hih := ih hndks (pre ++ suf)
      simp only [List.map_append, List.sum_append, List.map_cons, List.sum_cons] at hih ⊢
      omega
    · simp [hkT]
      exact ih (List.nodup_cons.mp hnd).2 T

theorem extra_eq_w (price : Nat → Nat) (avail req : List Nat) :
  extra price avail req =
    (req.eraseDups.map (fun k => if k ∈ avail then 0 else price k)).sum := by
  unfold extra
  rw [map_filter_sum_eq_map_ite_sum (fun k : Nat => !avail.contains k) price req.eraseDups]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  simp


theorem map_ite_mem_filter_sum {α} [DecidableEq α] (f : α → Nat) (l : List α) (T : List α) :
  ((l.filter (fun k => decide (k ∈ T))).map f).sum =
    (l.map (fun k => if k ∈ T then f k else 0)).sum := by
  rw [map_filter_sum_eq_map_ite_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  by_cases h : k ∈ T <;> simp [h]

theorem extra_le_sum (price : Nat → Nat) (avail aB bB : List Nat) :
  extra price avail aB ≤ extra price bB aB + extra price avail bB := by
  rw [extra_eq_w, extra_eq_w, extra_eq_w]
  let wav : Nat → Nat := fun k => if k ∈ avail then 0 else price k
  let wb : Nat → Nat := fun k => if k ∈ bB then 0 else price k
  change (aB.eraseDups.map wav).sum ≤ (aB.eraseDups.map wb).sum + (bB.eraseDups.map wav).sum
  have hnd : aB.eraseDups.Nodup := eraseDups_nodup aB
  have hpoint : ∀ k ∈ aB.eraseDups, wav k ≤ wb k + (if k ∈ bB then wav k else 0) := by
    intro k hk
    by_cases hkav : k ∈ avail
    · simp [wav, hkav]
    · by_cases hkb : k ∈ bB
      · simp [wav, wb, hkav, hkb]
      · simp [wav, wb, hkav, hkb]
  have hsum : (aB.eraseDups.map wav).sum ≤
      (aB.eraseDups.map wb).sum + (aB.eraseDups.map (fun k => if k ∈ bB then wav k else 0)).sum := by
    calc
      (aB.eraseDups.map wav).sum ≤ (aB.eraseDups.map (fun k => wb k + (if k ∈ bB then wav k else 0))).sum :=
        sum_map_le_sum_map wav (fun k => wb k + (if k ∈ bB then wav k else 0)) aB.eraseDups hpoint
      _ = (aB.eraseDups.map wb).sum + (aB.eraseDups.map (fun k => if k ∈ bB then wav k else 0)).sum :=
        sum_map_add wb (fun k => if k ∈ bB then wav k else 0) aB.eraseDups
  have hpart : (aB.eraseDups.map (fun k => if k ∈ bB then wav k else 0)).sum =
      ((aB.eraseDups.filter (fun k => decide (k ∈ bB))).map wav).sum := by
    symm
    exact map_ite_mem_filter_sum wav aB.eraseDups bB
  rw [hpart] at hsum
  have hsub := sum_filter_mem_le_sum wav aB.eraseDups hnd bB.eraseDups
  -- filter membership in bB.eraseDups vs bB
  have hfilt : (aB.eraseDups.filter (fun k => decide (k ∈ bB.eraseDups))) =
              (aB.eraseDups.filter (fun k => decide (k ∈ bB))) := by
    apply List.filter_congr
    intro x hx
    simp [List.mem_eraseDups]
  rw [← hfilt] at hsum
  omega

theorem sound : Goal := by
  intro price a b
  constructor
  · intro h
    have hb := h b.builds
    dsimp [cost] at hb
    have hzero : extra price b.builds b.builds = 0 := by
      rw [extra_eq_w]
      have hm : (b.builds.eraseDups.map (fun k => if k ∈ b.builds then 0 else price k)) =
                (b.builds.eraseDups.map (fun _ : Nat => 0)) := by
        apply List.map_congr_left
        intro k hk
        have hkb : k ∈ b.builds := List.mem_eraseDups.mp hk
        simp [hkb]
      rw [hm]
      exact sum_map_zero _
    omega
  · intro h available
    dsimp [cost]
    have hle := extra_le_sum price available a.builds b.builds
    omega

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_SharedBuild
