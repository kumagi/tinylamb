import TinylambOpt.ResearchGoals.BenchmarkRematch.Storage

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_AnalyticsDuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Storage

/-!
`AnalyticsDuckDB.Goal` is TRUE: `byteLane` keeps exactly the bytes whose
*position* is congruent to `lane` modulo `width`, so concatenating the
lanes in position order and offsetting by the sizes of the earlier lanes
lands exactly on `page[i]`.  The core of the proof is `laneStruct`, which
writes each lane as the arithmetic progression `lane, lane + w, …`.
-/

private theorem map_congr' {α β : Type} {f g : α → β} (l : List α) (h : ∀ x, f x = g x) :
    l.map f = l.map g :=
  congrArg (fun k => List.map k l) (funext h)

private theorem get_some {l : List Nat} {i : Nat} (h : i < l.length) : ∃ a, l[i]? = some a := by
  induction l generalizing i with
  | nil => exact absurd h (by simp)
  | cons a l ih =>
    cases i with
    | zero => exact ⟨a, rfl⟩
    | succ i =>
      obtain ⟨b, hb⟩ := ih (Nat.lt_of_succ_lt_succ h)
      exact ⟨b, by rw [List.getElem?_cons_succ, hb]⟩

private theorem modEqIff {w r u lane : Nat} (hw : 0 < w) (hr : r < w) (hu : u < w) :
    (r + u) % w = lane ↔ (lane + w - u) % w = r ∧ lane < w := by
  constructor
  · intro h
    have hl : lane < w := by
      rw [← h]
      exact Nat.mod_lt (r + u) hw
    have h₂ : (r + u) % w = lane := h
    have h₃ : (lane + w - u) % w = r := by
      by_cases hr' : r + u < w
      · rw [Nat.mod_eq_of_lt hr'] at h₂
        have e : lane + w - u = r + w := by omega
        rw [e, show r + w = r + w * 1 from by rw [Nat.mul_one], Nat.add_mul_mod_self_left]
        exact Nat.mod_eq_of_lt hr
      · have hdiv : (r + u) / w = 1 :=
          Nat.div_eq_of_lt_le (by omega) (by omega)
        have h₅ := Nat.mod_add_div (r + u) w
        rw [hdiv, Nat.mul_one] at h₅
        have h₄ : (r + u) % w = r + u - w := by omega
        rw [h₄] at h₂
        have e : lane + w - u = r := by omega
        rw [e]
        exact Nat.mod_eq_of_lt hr
    exact ⟨h₃, hl⟩
  · rintro ⟨h₂, hl⟩
    rw [← h₂, Nat.mod_add_mod]
    have h₃ : lane + w - u + u = lane + w := Nat.sub_add_cancel (by omega : u ≤ lane + w)
    rw [h₃, show lane + w = lane + w * 1 from by rw [Nat.mul_one], Nat.add_mul_mod_self_left]
    exact Nat.mod_eq_of_lt hl

/-- The indices below `n` whose (shifted) position is `≡ lane (mod w)`,
as an arithmetic progression. -/
private theorem laneStruct {w : Nat} (hw : 0 < w) :
    ∀ (n s lane : Nat),
      (List.range n).filter (fun j => (j + s) % w == lane) =
        if lane < w
        then (List.range (n / w + if (lane + w - s % w) % w < n % w then 1 else 0)).map
            (fun k => (lane + w - s % w) % w + w * k)
        else [] := by
  intro n
  induction n with
  | zero =>
    intro s lane
    by_cases hg : lane < w
    · rw [ite_eq_left hg]
      simp
    · rw [ite_eq_right hg]
      simp
  | succ n ih =>
    intro s lane
    rw [List.range_succ, List.filter_append, ih s lane]
    simp only [List.filter_cons, List.filter_nil, Nat.beq_eq_true_eq]
    have hn : n = n / w * w + n % w := (Nat.div_add_mod' n w).symm
    have hr : n % w < w := Nat.mod_lt n hw
    have hdlt : (lane + w - s % w) % w < w := Nat.mod_lt _ hw
    have hpn : ((n + s) % w = lane) ↔ ((lane + w - s % w) % w = n % w ∧ lane < w) := by
      rw [Nat.add_mod]
      exact modEqIff hw hr (Nat.mod_lt s hw)
    by_cases hg : lane < w
    · rw [ite_eq_left hg, ite_eq_left hg]
      by_cases hp : (n + s) % w = lane
      · -- the new index `n` joins the lane: one more element is appended
        have hd : (lane + w - s % w) % w = n % w := (hpn.mp hp).1
        rw [ite_eq_left hp, hd]
        have hfin : n % w + w * (n / w) = n := by
          rw [Nat.mul_comm, Nat.add_comm, Nat.div_add_mod']
        by_cases hrw : n % w + 1 < w
        · have h₁ : (n + 1) / w = n / w :=
            Nat.div_eq_of_lt_le (by omega) (by rw [Nat.add_mul, Nat.one_mul]; omega)
          have h₂ : (n + 1) % w = n % w + 1 := by
            rw [show n + 1 = n % w + 1 + n / w * w from by omega,
              Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrw]
          rw [h₁, h₂, ite_eq_left (Nat.lt_succ_self _), ite_eq_right (Nat.lt_irrefl _),
            Nat.add_zero, List.range_succ, List.map_append, List.map_singleton, hfin]
        · have hrw' : n % w + 1 = w := by omega
          have h₁ : (n + 1) / w = n / w + 1 := by
            refine Nat.div_eq_of_eq_mul_right hw ?_
            rw [Nat.mul_add, Nat.mul_one, Nat.mul_comm w (n / w)]
            omega
          have h₂ : (n + 1) % w = 0 := by
            rw [show n + 1 = (n / w + 1) * w by rw [Nat.add_mul, Nat.one_mul]; omega,
              Nat.mul_mod_left]
          rw [h₁, h₂, ite_eq_right (Nat.not_lt_zero _), ite_eq_right (Nat.lt_irrefl _),
            Nat.add_zero, Nat.add_zero, List.range_succ, List.map_append, List.map_singleton,
            hfin]
      · -- the new index misses the lane: nothing is appended
        rw [ite_eq_right hp, List.append_nil]
        have hdne : ¬ ((lane + w - s % w) % w = n % w) := fun h => hp (hpn.mpr ⟨h, hg⟩)
        by_cases hrw : n % w + 1 < w
        · have h₁ : (n + 1) / w = n / w :=
            Nat.div_eq_of_lt_le (by omega) (by rw [Nat.add_mul, Nat.one_mul]; omega)
          have h₂ : (n + 1) % w = n % w + 1 := by
            rw [show n + 1 = n % w + 1 + n / w * w from by omega,
              Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrw]
          rw [h₁, h₂]
          by_cases hP : (lane + w - s % w) % w < n % w
          · rw [ite_eq_left hP, ite_eq_left (by omega)]
          · rw [ite_eq_right hP, ite_eq_right (by omega)]
        · have hrw' : n % w + 1 = w := by omega
          have h₁ : (n + 1) / w = n / w + 1 := by
            refine Nat.div_eq_of_eq_mul_right hw ?_
            rw [Nat.mul_add, Nat.mul_one, Nat.mul_comm w (n / w)]
            omega
          have h₂ : (n + 1) % w = 0 := by
            rw [show n + 1 = (n / w + 1) * w by rw [Nat.add_mul, Nat.one_mul]; omega,
              Nat.mul_mod_left]
          have hlt : (lane + w - s % w) % w < n % w := by omega
          rw [h₁, h₂, ite_eq_right (Nat.not_lt_zero _), Nat.add_zero, ite_eq_left hlt]
    · rw [ite_eq_right hg, ite_eq_right hg]
      have hp' : ¬ ((n + s) % w = lane) := fun h => hg (hpn.mp h).2
      rw [ite_eq_right hp']
      rfl

/-- Lane membership by position: `j % w = lane` keeps the positions
`lane, lane + w, lane + 2*w, …` below `n`. -/
private theorem laneEq {w : Nat} (hw : 0 < w) (n lane : Nat) :
    (List.range n).filter (fun j => j % w == lane) =
      if lane < w
      then (List.range (n / w + if lane < n % w then 1 else 0)).map (fun k => lane + w * k)
      else [] := by
  have h := laneStruct hw n 0 lane
  have hz : (fun j : Nat => (j + 0) % w == lane) = (fun j : Nat => j % w == lane) := by
    funext j
    rw [Nat.add_zero]
  rw [hz] at h
  by_cases hg : lane < w
  · rw [ite_eq_left hg] at h ⊢
    have hd : (lane + w - 0 % w) % w = lane := by
      rw [Nat.zero_mod, Nat.sub_zero, show lane + w = lane + w * 1 from by rw [Nat.mul_one],
        Nat.add_mul_mod_self_left]
      exact Nat.mod_eq_of_lt hg
    rw [hd] at h
    exact h
  · rw [ite_eq_right hg] at h ⊢
    exact h

private theorem zipIdx_start (page : List Nat) :
    ∀ s, page.zipIdx s = (List.range page.length).map (fun j => (page[j]!, j + s)) := by
  induction page with
  | nil => intro s; rfl
  | cons a l ih =>
    intro s
    rw [List.zipIdx_cons, List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map,
      ih (s + 1)]
    rw [show ((a :: l)[0]!, (0 : Nat) + s) = (a, s) by
      rw [List.getElem!_cons_zero, Nat.zero_add]]
    refine congrArg (fun x => (a, s) :: x) ?_
    refine congrArg (List.map · (List.range l.length)) ?_
    refine funext (fun j => ?_)
    simp only [Function.comp_apply, Nat.succ_eq_add_one]
    rw [List.getElem!_cons_succ]
    exact Prod.ext rfl (by omega)

private theorem byteLane_eq (page : List Nat) (w lane : Nat) :
    byteLane page w lane
      = ((List.range page.length).filter (fun j => j % w == lane)).map (fun j => page[j]!) := by
  unfold byteLane
  rw [zipIdx_start page 0, List.filter_map, List.map_map]
  rfl

private theorem take_map {α β : Type} (f : α → β) :
    ∀ (l : List α) (n : Nat), (l.map f).take n = (l.take n).map f := by
  intro l
  induction l with
  | nil => intro n; simp
  | cons a l ih =>
    intro n
    cases n with
    | zero => simp
    | succ n => simp only [List.take_succ_cons, List.map_cons]; rw [ih n]

private theorem take_range_le : ∀ (w k : Nat), k ≤ w → (List.range w).take k = List.range k := by
  intro w
  induction w with
  | zero => intro k hk; rw [Nat.le_zero.mp hk, List.take_zero, List.range_zero]
  | succ w ih =>
    intro k hk
    cases k with
    | zero => rw [List.take_zero, List.range_zero]
    | succ k =>
      rw [List.range_succ_eq_map, List.take_succ_cons, take_map, ih k (by omega),
        ← List.range_succ_eq_map]

private theorem lt_sz {w n m t : Nat} (hw : 0 < w) (hm : m < w) (h : w * t + m < n) :
    t < n / w + if m < n % w then 1 else 0 := by
  have hr : n % w < w := Nat.mod_lt n hw
  have hn : n = n / w * w + n % w := (Nat.div_add_mod' n w).symm
  have hcontra : ¬ (n / w + 1 ≤ t) := by
    intro h₃
    have h₄ : n / w * w + w ≤ w * t := by
      have h₄' := Nat.mul_le_mul_left w h₃
      rw [Nat.mul_comm w (n / w + 1), Nat.add_mul, Nat.one_mul] at h₄'
      exact h₄'
    have h₅ : w * t ≤ w * t + m := Nat.le_add_right _ _
    have h₆ : n ≤ w * t + m := by rw [hn]; omega
    omega
  by_cases hg : m < n % w
  · rw [ite_eq_left hg]
    omega
  · rw [ite_eq_right hg, Nat.add_zero]
    by_cases h₂ : t = n / w
    · subst h₂
      have h₃ : w * (n / w) + m < n / w * w + n % w := by
        rw [← hn]
        exact h
      rw [Nat.mul_comm w (n / w)] at h₃
      exact absurd (by omega : m < n % w) hg
    · omega

theorem sound : AnalyticsDuckDB.Goal := by
  intro page w i hw hi
  dsimp only
  let m := i % w
  let t := i / w
  let n := page.length
  have hm0 : m = i % w := rfl
  have ht0 : t = i / w := rfl
  have hm : m < w := by rw [hm0]; exact Nat.mod_lt _ hw
  have hmt : m + w * t = i := by
    rw [hm0, ht0, Nat.add_comm, Nat.mul_comm, Nat.div_add_mod']
  have hlt : w * t + m < n := by
    rw [Nat.add_comm, hmt]
    exact hi
  let sz : Nat → Nat := fun ℓ => if ℓ < w then n / w + if ℓ < n % w then 1 else 0 else 0
  let blk : Nat → List Nat := fun ℓ => (List.range (sz ℓ)).map (fun k => page[ℓ + w * k]!)
  have hsz : sz = fun ℓ => if ℓ < w then n / w + if ℓ < n % w then 1 else 0 else 0 := rfl
  have hblk : blk = fun ℓ => (List.range (sz ℓ)).map (fun k => page[ℓ + w * k]!) := rfl
  have hblock : ∀ ℓ : Nat, byteLane page w ℓ = blk ℓ := by
    intro ℓ
    show byteLane page w ℓ =
      List.map (fun k => page[ℓ + w * k]!)
        (List.range (if ℓ < w then page.length / w + if ℓ < page.length % w then 1 else 0 else 0))
    rw [byteLane_eq, laneEq hw page.length ℓ]
    by_cases hg : ℓ < w
    · rw [ite_eq_left hg, ite_eq_left hg]
      exact List.map_map.trans rfl
    · rw [ite_eq_right hg, ite_eq_right hg, List.range_zero, List.map_nil]
      exact rfl
  have hlane : (List.range w).map (fun lane => byteLane page w lane) = (List.range w).map blk :=
    map_congr' (List.range w) (fun ℓ => hblock ℓ)
  rw [hlane]
  have htake : ((List.range w).map blk).take m = (List.range m).map blk := by
    rw [take_map, take_range_le w m (Nat.le_of_lt hm)]
  have hlen : ∀ ℓ, (blk ℓ).length = sz ℓ := by
    intro ℓ
    rw [hblk, List.length_map, List.length_range]
  have hbeg : ((((List.range w).map blk).take m).map List.length).sum
      = List.sum ((List.range m).map sz) := by
    rw [htake, List.map_map]
    refine congrArg List.sum (map_congr' (List.range m) (fun ℓ => ?_))
    show List.length (blk ℓ) = sz ℓ
    exact hlen ℓ
  rw [hbeg]
  have hsplit : ∃ T, (List.range w).map blk = (List.range m).map blk ++ blk m :: T := by
    refine ⟨((List.range w).map blk).drop (m + 1), ?_⟩
    have h₂ : ((List.range w).map blk).take (m + 1) = (List.range m).map blk ++ [blk m] := by
      rw [take_map, take_range_le w (m + 1) (by omega), List.range_succ, List.map_append,
        List.map_singleton]
    have h₁ := List.take_append_drop (m + 1) ((List.range w).map blk)
    rw [h₂] at h₁
    rw [← List.singleton_append, ← List.append_assoc]
    exact h₁.symm
  obtain ⟨T, hT⟩ := hsplit
  rw [hT, List.flatten_append, List.flatten_cons]
  have hfl : ((List.range m).map blk).flatten.length = List.sum ((List.range m).map sz) := by
    rw [List.length_flatten, List.map_map]
    exact congrArg List.sum (map_congr' (List.range m) fun ℓ => hlen ℓ)
  rw [List.getElem?_append_right (by rw [hfl]; exact Nat.le_add_right _ _), hfl,
    Nat.add_sub_cancel_left]
  have hszm : sz m = n / w + if m < n % w then 1 else 0 := by
    show (if m < w then n / w + if m < n % w then 1 else 0 else 0) = _
    exact ite_eq_left hm
  have hbmm : blk m = List.map (fun k => page[m + w * k]!)
      (List.range (n / w + if m < n % w then 1 else 0)) := by
    show List.map (fun k => page[m + w * k]!) (List.range (sz m)) = _
    rw [hszm]
  have htK : t < n / w + if m < n % w then 1 else 0 := lt_sz hw hm hlt
  rw [hbmm]
  rw [List.getElem?_append_left (by rw [List.length_map, List.length_range]; exact htK)]
  rw [List.getElem?_map, List.getElem?_range htK]
  show some (page[m + w * t]!) = page[i]?
  rw [hmt]
  have key : page[i]? = some (page[i]!) := by
    obtain ⟨a, ha⟩ := get_some hi
    have h₂ : page[i]! = a := List.getElem!_of_getElem? ha
    rw [ha, h₂]
  exact key.symm

end TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_AnalyticsDuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_AnalyticsDuckDB.sound
