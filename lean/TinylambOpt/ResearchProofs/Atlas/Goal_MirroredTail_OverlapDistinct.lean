import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `MirroredTail.OverlapDistinct` — `∀ c, c.step ≠ 0 → (distinctValues layout c).length
  = max (min c.n c.split) (c.n-c.split)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MirroredTail_OverlapDistinct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail

private def seq (base step : Int) (j : Nat) : Int := base + step * (j : Int)

private theorem seq_inj (base step : Int) (hs : step ≠ 0) {i j : Nat}
    (h : seq base step i = seq base step j) : i = j := by
  unfold seq at h
  have hmul : step * (i : Int) = step * (j : Int) := by omega
  have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_left hs hmul
  exact Int.ofNat_inj.mp hij

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem nodup_filter {α : Type} (p : α → Bool) :
    ∀ {l : List α}, l.Nodup → (l.filter p).Nodup := by
  intro l h
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp h
      by_cases hpx : p x = true
      · simp [hpx]
        exact ⟨fun hxmem => hp.1 x hxmem rfl, ih hp.2⟩
      · simp [hpx]
        exact ih hp.2

private theorem nodup_reverse {α : Type} :
    ∀ {l : List α}, l.Nodup → l.reverse.Nodup := by
  intro l h
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp h
      rw [List.reverse_cons]
      change List.Pairwise (fun a b : α => a ≠ b) (xs.reverse ++ [x])
      rw [List.pairwise_append]
      refine ⟨ih hp.2, List.pairwise_singleton _ _, ?_⟩
      intro a ha b hb
      simp at hb
      subst b
      have haxs : a ∈ xs := by simpa using ha
      exact fun hab => hp.1 a haxs hab.symm

private theorem nodup_eraseDups_eq {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l : List α}, l.Nodup → l.eraseDups = l := by
  intro l h
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp h
      rw [List.eraseDups_cons]
      have hf : (List.filter (fun y => ! y == x) xs) = xs := by
        apply List.filter_eq_self.mpr
        intro y hy
        have hyne : y ≠ x := by
          intro hxy
          exact hp.1 y hy hxy.symm
        simp [hyne]
      rw [hf, ih hp.2]

private theorem countP_ge (a b : Nat) :
    (List.range b).countP (fun k => decide (a ≤ k)) = b - a := by
  induction b with
  | zero => simp
  | succ b ih =>
      rw [List.range_succ, List.countP_append, List.countP_singleton, ih]
      by_cases hab : a ≤ b
      · have hsub : b + 1 - a = b - a + 1 := by
          rw [Nat.add_comm b 1, Nat.add_sub_assoc hab, Nat.add_comm]
        simp [hab, hsub]
      · have hble : b + 1 ≤ a := by omega
        have hsub : b + 1 - a = b - a := by
          rw [Nat.sub_eq_zero_of_le hble, Nat.sub_eq_zero_of_le (by omega)]
        simp [hab, hsub]

private theorem filter_not_elem_range (a b : Nat) :
    (List.range b).reverse.filter (fun x => ! List.elem x (List.range a)) =
    (List.range b).reverse.filter (fun x => decide (a ≤ x)) := by
  apply List.filter_congr
  intro x hx
  by_cases hlt : x < a
  · have hxmem : x ∈ List.range a := List.mem_range.mpr hlt
    have hnle : ¬ a ≤ x := by omega
    simp [hxmem, hnle]
  · have hxmem : ¬ x ∈ List.range a := by simpa using hlt
    have hle : a ≤ x := by omega
    simp [hxmem, hle]

private theorem map_succ_reverse_range (n : Nat) :
    ((List.range n).reverse.map (fun x => x + 1)) ++ [0] = n :: (List.range n).reverse := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.reverse_append, List.reverse_singleton]
      simp only [List.map_append, List.map_cons, List.map_nil]
      change (n + 1) :: (List.map (fun x => x + 1) (List.range n).reverse ++ [0]) =
        (n + 1) :: ([n] ++ (List.range n).reverse)
      rw [ih, List.singleton_append]

private theorem range_reverse_eq_map (n : Nat) :
    (List.range n).reverse = (List.range n).map (fun j => n - 1 - j) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.reverse_append, List.reverse_singleton]
      rw [List.map_append, List.map_singleton]
      rw [ih]
      have hmap : (List.range n).map (fun j => n + 1 - 1 - j) =
          (List.range n).map (fun j => (n - 1 - j) + 1) := by
        apply List.map_congr_left
        intro j hj
        have hjlt : j < n := List.mem_range.mp hj
        omega
      rw [hmap]
      have hmap2 : (List.range n).map (fun j => n - 1 - j + 1) =
          ((List.range n).map (fun j => n - 1 - j)).map (fun x => x + 1) := by
        rw [List.map_map]
        rfl
      rw [hmap2]
      have hzero : n + 1 - 1 - n = 0 := by omega
      rw [hzero]
      simpa [ih] using (map_succ_reverse_range n).symm

private theorem eraseDups_range_append_reverse_range (a b : Nat) :
    (((List.range a) ++ (List.range b).reverse).eraseDups).length = max a b := by
  rw [List.eraseDups_append]
  have hER1 : (List.range a).eraseDups = List.range a := nodup_eraseDups_eq List.nodup_range
  rw [hER1]
  simp only [List.length_append, List.length_range]
  have hnd2 : ((List.range b).reverse).Nodup := nodup_reverse List.nodup_range
  change a + (((List.range b).reverse.filter (fun x => ! List.elem x (List.range a))).eraseDups).length = max a b
  have hndrem : ((List.range b).reverse.filter (fun x => ! List.elem x (List.range a))).Nodup :=
    nodup_filter _ hnd2
  rw [nodup_eraseDups_eq hndrem]
  rw [filter_not_elem_range]
  rw [← List.countP_eq_length_filter]
  rw [List.countP_reverse]
  rw [countP_ge]
  by_cases hab : a ≤ b
  · have hmax : max a b = b := Nat.max_eq_right hab
    rw [hmax]
    omega
  · have hmax : max a b = a := Nat.max_eq_left (Nat.le_of_not_ge hab)
    rw [hmax]
    omega

private theorem eraseDups_map_seq (base step : Int) (hs : step ≠ 0) :
    ∀ l : List Nat, (l.map (seq base step)).eraseDups = (l.eraseDups).map (seq base step) := by
  intro l
  induction hlen : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons x xs =>
          have hlt : xs.length < n := by
            simp at hlen
            omega
          rw [List.map_cons, List.eraseDups_cons, List.eraseDups_cons, List.map_cons]
          congr 1
          rw [List.filter_map]
          have hpred : ((fun b : Int => ! b == seq base step x) ∘ seq base step) =
                       (fun y : Nat => ! y == x) := by
            funext y
            by_cases hyx : y = x
            · subst y
              simp
            · have hne : seq base step y ≠ seq base step x := by
                intro heq
                exact hyx (seq_inj base step hs heq)
              have h1 : (seq base step y == seq base step x) = false := by
                rw [← Bool.not_eq_true]
                intro hb
                exact hne (beq_iff_eq.mp hb)
              have h2 : (y == x) = false := by
                rw [← Bool.not_eq_true]
                intro hb
                exact hyx (beq_iff_eq.mp hb)
              change (!(seq base step y == seq base step x)) = (!(y == x))
              rw [h1, h2]
          rw [hpred]
          have hlt2 : (List.filter (fun y : Nat => ! y == x) xs).length < n := by
            have hle := List.Sublist.length_le (List.filter_sublist (p := fun y : Nat => ! y == x) (l := xs))
            omega
          exact ih (List.filter (fun y : Nat => ! y == x) xs).length hlt2
            (List.filter (fun y : Nat => ! y == x) xs) rfl

private theorem distinct_up_down (base step : Int) (hs : step ≠ 0) (a b : Nat) :
    (((List.range a).map (seq base step)) ++
      ((List.range b).map (fun k => seq base step (b - 1 - k)))).eraseDups.length = max a b := by
  have hsecond : (List.range b).map (fun k => seq base step (b - 1 - k)) =
      ((List.range b).map (fun k => b - 1 - k)).map (seq base step) := by
    simp [List.map_map]
  rw [hsecond, ← List.map_append]
  rw [eraseDups_map_seq base step hs]
  simp only [List.length_map]
  rw [← range_reverse_eq_map b]
  exact eraseDups_range_append_reverse_range a b

private def val (c : Config) (i : Nat) : Int :=
  if i < c.split then seq c.base c.step i
  else seq c.base c.step (c.n - 1 - i)

private theorem read_val (c : Config) (i : Nat) :
    layout.read c i = some (val c i) := by
  by_cases hi : i < c.split <;> simp [layout, val, hi, seq]

private theorem rows_vals (c : Config) :
    (rows layout c).map Prod.snd = (List.range c.n).map (val c) := by
  unfold rows
  rw [show (List.range c.n).filterMap (fun i : Nat => (layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun i : Nat => some (i, val c i)) from by
    apply filterMap_congr'
    intro i hi
    rw [read_val]
    simp]
  rw [filterMap_some_eq_map]
  rw [List.map_map]
  rfl

theorem sound_MirroredTail_OverlapDistinct : MirroredTail.OverlapDistinct.Goal := by
  intro c hs
  change (distinctValues layout c).length = max (min c.n c.split) (c.n - c.split)
  unfold distinctValues
  rw [rows_vals]
  by_cases hsplit : c.split ≤ c.n
  · have hvals : (List.range c.n).map (val c) =
        ((List.range c.split).map (seq c.base c.step)) ++
          ((List.range (c.n - c.split)).map (fun k => seq c.base c.step (c.n - c.split - 1 - k))) := by
      calc
        (List.range c.n).map (val c) =
            (List.range (c.split + (c.n - c.split))).map (val c) := by
              rw [Nat.add_sub_of_le hsplit]
        _ = ((List.range c.split) ++ (List.range (c.n - c.split)).map (fun x => c.split + x)).map (val c) := by
              rw [List.range_add]
        _ = (List.range c.split).map (val c) ++
            ((List.range (c.n - c.split)).map (fun x => c.split + x)).map (val c) := by
              rw [List.map_append]
        _ = (List.range c.split).map (seq c.base c.step) ++
            ((List.range (c.n - c.split)).map (fun k => seq c.base c.step (c.n - c.split - 1 - k))) := by
              congr 1
              · apply List.map_congr_left
                intro i hi
                have hi' : i < c.split := List.mem_range.mp hi
                simp [val, hi']
              · rw [List.map_map]
                apply List.map_congr_left
                intro k hk
                have hk' : k < c.n - c.split := List.mem_range.mp hk
                have hnot : ¬ c.split + k < c.split := by omega
                have harith : c.n - 1 - (c.split + k) = c.n - c.split - 1 - k := by omega
                simp [val, hnot, harith]
    rw [hvals]
    rw [distinct_up_down c.base c.step hs]
    rw [Nat.min_eq_right hsplit]
  · have hnlt : c.n < c.split := by omega
    have hvals : (List.range c.n).map (val c) = (List.range c.n).map (seq c.base c.step) := by
      apply List.map_congr_left
      intro i hi
      have hi' : i < c.n := List.mem_range.mp hi
      have his : i < c.split := by omega
      simp [val, his]
    rw [hvals]
    rw [eraseDups_map_seq c.base c.step hs]
    have hnd : (List.range c.n).eraseDups = List.range c.n := nodup_eraseDups_eq List.nodup_range
    rw [hnd]
    simp only [List.length_map]
    have hmin : min c.n c.split = c.n := Nat.min_eq_left (by omega)
    have hsub : c.n - c.split = 0 := by omega
    rw [hmin, hsub]
    simp

end TinylambOpt.ResearchProofs.Atlas.Goal_MirroredTail_OverlapDistinct
