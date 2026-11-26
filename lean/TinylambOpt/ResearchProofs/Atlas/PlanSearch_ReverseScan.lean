import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `ParallelCuts.ReverseScan` — `∀ cap xs, (∀ w ∈ xs, w ≤ cap) → required cap xs.reverse
  = required cap xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReverseScan

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

/-! Next-fit (`pack`) closing blocks greedily as late as possible gives the
minimum number of consecutive blocks of weight at most `cap`.  The minimum is
invariant under reversing the weight list (reverse every block and the block
list), hence so is `required`. -/

private def step (cap : Nat) (s : Nat × Nat) (w : Nat) : Nat × Nat :=
  if s.2 + w ≤ cap then (s.1, s.2 + w) else (s.1 + 1, w)

private def inv (g a : Nat × Nat) : Prop :=
  g.1 ≤ a.1 ∧ (g.1 = a.1 → g.2 ≤ a.2)

private theorem inv_stay (cap : Nat) (ws : List Nat) (g : Nat × Nat) (a cur : Nat)
    (h : inv g (a, cur)) (hsum : cur + ws.sum ≤ cap) :
    inv (ws.foldl (step cap) g) (a, cur + ws.sum) := by
  dsimp [inv] at h ⊢
  induction ws generalizing g cur with
  | nil => simpa using h
  | cons w ws ih =>
      simp only [List.sum_cons] at hsum
      have hcw : cur + w ≤ cap := by omega
      have hsum' : (cur + w) + ws.sum ≤ cap := by omega
      by_cases hg : g.2 + w ≤ cap
      · have h1 : inv (g.1, g.2 + w) (a, cur + w) := by
          constructor
          · exact h.1
          · intro heq
            have hs := h.2 heq
            omega
        simpa [step, hg, Nat.add_assoc] using ih (g.1, g.2 + w) (cur + w) h1 hsum'
      · have hlt : g.1 < a := by
          by_cases hh : g.1 < a
          · exact hh
          · have heq : g.1 = a := by omega
            have hs := h.2 heq
            omega
        have h1 : inv (g.1 + 1, w) (a, cur + w) := by
          constructor
          · omega
          · intro _
            omega
        simpa [step, hg, Nat.add_assoc] using ih (g.1 + 1, w) (cur + w) h1 hsum'

private theorem inv_close (cap : Nat) (g : Nat × Nat) (a cur w : Nat)
    (h : inv g (a, cur)) (_hw : w ≤ cap) :
    inv (step cap g w) (a + 1, w) := by
  dsimp [inv] at h ⊢
  by_cases hg : g.2 + w ≤ cap
  · rw [show step cap g w = (g.1, g.2 + w) by simp [step, hg]]
    constructor
    · omega
    · intro heq
      omega
  · rw [show step cap g w = (g.1 + 1, w) by simp [step, hg]]
    constructor
    · omega
    · intro heq
      omega

private theorem inv_block (cap : Nat) (b : List Nat) (g : Nat × Nat) (a cur : Nat)
    (h : inv g (a, cur)) (hbw : ∀ w ∈ b, w ≤ cap) (hsum : b.sum ≤ cap) :
    inv (b.foldl (step cap) g) (a + 1, b.sum) := by
  cases b with
  | nil =>
      dsimp [inv] at h ⊢
      simp at hsum
      constructor
      · omega
      · intro heq
        omega
  | cons w ws =>
      have hw : w ≤ cap := hbw w (by simp)
      have h1 : inv (step cap g w) (a + 1, w) := inv_close cap g a cur w h hw
      have hsum' : w + ws.sum ≤ cap := by simpa using hsum
      have h2 : inv (ws.foldl (step cap) (step cap g w)) (a + 1, w + ws.sum) :=
        inv_stay cap ws (step cap g w) (a + 1) w h1 hsum'
      simpa [List.foldl_cons, List.sum_cons] using h2

private theorem inv_blocks_rest (cap : Nat) (bs : List (List Nat)) (g : Nat × Nat) (a cur : Nat)
    (h : inv g (a, cur))
    (hbw : ∀ w ∈ bs.flatten, w ≤ cap) (hsum : ∀ b ∈ bs, b.sum ≤ cap) :
    inv (bs.flatten.foldl (step cap) g)
      (bs.foldl (fun (s : Nat × Nat) b => (s.1 + 1, b.sum)) (a, cur)) := by
  induction bs generalizing g a cur with
  | nil => simpa using h
  | cons b bs ih =>
      have hbw_b : ∀ w ∈ b, w ≤ cap := by
        intro w hw
        exact hbw w (by simp [hw])
      have hbw_bs : ∀ w ∈ bs.flatten, w ≤ cap := by
        intro w hw
        exact hbw w (by simp [hw])
      have hsum_b : b.sum ≤ cap := hsum b (by simp)
      have hsum_bs : ∀ x ∈ bs, x.sum ≤ cap := by
        intro x hx
        exact hsum x (by simp [hx])
      have h1 := inv_block cap b g a cur h hbw_b hsum_b
      have h2 := ih (b.foldl (step cap) g) (a + 1) b.sum h1 hbw_bs hsum_bs
      simpa [List.foldl_append, List.flatten_cons] using h2

private def witnessAfterFirst (bs : List (List Nat)) (a : Nat) : Nat × Nat :=
  match bs with
  | [] => (a, 0)
  | b :: rest => rest.foldl (fun (s : Nat × Nat) b => (s.1 + 1, b.sum)) (a, b.sum)

-- The first block is already open, so it does not close a block; later blocks do.
private theorem inv_blocks (cap : Nat) (bs : List (List Nat)) (g : Nat × Nat) (a : Nat)
    (h : inv g (a, 0))
    (hbw : ∀ w ∈ bs.flatten, w ≤ cap) (hsum : ∀ b ∈ bs, b.sum ≤ cap) :
    inv (bs.flatten.foldl (step cap) g) (witnessAfterFirst bs a) := by
  cases bs with
  | nil => simpa [witnessAfterFirst] using h
  | cons b rest =>
      have hbw_rest : ∀ w ∈ rest.flatten, w ≤ cap := by
        intro w hw
        exact hbw w (by simp [hw])
      have hsum_b : b.sum ≤ cap := hsum b (by simp)
      have hsum_rest : ∀ x ∈ rest, x.sum ≤ cap := by
        intro x hx
        exact hsum x (by simp [hx])
      have h1 : inv (b.foldl (step cap) g) (a, b.sum) := by
        simpa using inv_stay cap b g a 0 h (by simpa using hsum_b)
      have h2 := inv_blocks_rest cap rest (b.foldl (step cap) g) a b.sum h1 hbw_rest hsum_rest
      simpa [witnessAfterFirst, List.foldl_append, List.flatten_cons] using h2

private theorem witness_count (bs : List (List Nat)) (a cur : Nat) :
    (bs.foldl (fun (s : Nat × Nat) b => (s.1 + 1, b.sum)) (a, cur)).1 = a + bs.length := by
  induction bs generalizing a cur with
  | nil => simp
  | cons b bs ih =>
      have h := ih (a + 1) b.sum
      simp only [List.length_cons, List.foldl_cons]
      omega

-- Greedy uses no more blocks than any contiguous partition of the same list.
private theorem optimal (cap : Nat) (xs : List Nat) (bs : List (List Nat))
    (hflat : bs.flatten = xs) (hne : xs ≠ []) (hbw : ∀ w ∈ xs, w ≤ cap)
    (hsum : ∀ b ∈ bs, b.sum ≤ cap) :
    (xs.foldl (step cap) (0, 0)).1 + 1 ≤ bs.length := by
  have hbsne : bs ≠ [] := by
    intro hb
    subst bs
    simp at hflat
    exact hne hflat
  have hbslen : 1 ≤ bs.length := List.ne_nil_iff_length_pos.mp hbsne
  have hbw' : ∀ w ∈ bs.flatten, w ≤ cap := by
    intro w hw
    exact hbw w (by simpa [hflat] using hw)
  have h := inv_blocks cap bs (0, 0) 0 (by simp [inv]) hbw' hsum
  have hlen : (witnessAfterFirst bs 0).1 = bs.length - 1 := by
    cases bs with
    | nil => simp [witnessAfterFirst]
    | cons b rest =>
        simpa [witnessAfterFirst] using witness_count rest 0 b.sum
  rw [hflat] at h
  dsimp [inv] at h
  have hle := h.1
  omega

-- Maximal fitting prefix of a list, together with the remaining suffix.
private def takeFit (cap s : Nat) : List Nat → List Nat × List Nat
  | [] => ([], [])
  | w :: ws => if s + w ≤ cap then
      let (p, q) := takeFit cap (s + w) ws
      (w :: p, q)
    else ([], w :: ws)

private theorem takeFit_sum_le (cap s : Nat) (xs : List Nat) (hs : s ≤ cap) :
    s + (takeFit cap s xs).1.sum ≤ cap := by
  induction xs generalizing s with
  | nil => simpa [takeFit] using hs
  | cons w ws ih =>
      by_cases h : s + w ≤ cap
      · simpa [takeFit, h, Nat.add_assoc] using ih (s + w) h
      · simpa [takeFit, h] using hs

private theorem takeFit_append (cap s : Nat) (xs : List Nat) :
    (takeFit cap s xs).1 ++ (takeFit cap s xs).2 = xs := by
  induction xs generalizing s with
  | nil => simp [takeFit]
  | cons w ws ih =>
      by_cases h : s + w ≤ cap
      · simp [takeFit, h, ih]
      · simp [takeFit, h]

private theorem takeFit_prefix (cap s : Nat) (b ys : List Nat) (h : s + b.sum ≤ cap) :
    ∃ r, (takeFit cap s (b ++ ys)).1 = b ++ r := by
  induction b generalizing s with
  | nil =>
      simp
  | cons w b ih =>
      simp only [List.sum_cons] at h
      have hw : s + w ≤ cap := by omega
      have h' : (s + w) + b.sum ≤ cap := by omega
      obtain ⟨r, hr⟩ := ih (s + w) h'
      refine ⟨r, ?_⟩
      simp [takeFit, hw, hr]

private theorem takeFit_notfit (cap s : Nat) (xs : List Nat) (w : Nat) (ws : List Nat)
    (hq : (takeFit cap s xs).2 = w :: ws) : s + (takeFit cap s xs).1.sum + w > cap := by
  induction xs generalizing s with
  | nil => simp [takeFit] at hq
  | cons x xt ih =>
      by_cases h : s + x ≤ cap
      · have hres := ih (s + x) (by simpa [takeFit, h] using hq)
        simpa [takeFit, h, Nat.add_assoc] using hres
      · have hq' : x :: xt = w :: ws := by simpa [takeFit, h] using hq
        injection hq' with hx hxt
        subst w
        have hgt : cap < s + x := by omega
        simpa [takeFit, h] using hgt

private theorem fold_noreset (cap k s : Nat) (p : List Nat) (h : s + p.sum ≤ cap) :
    p.foldl (step cap) (k, s) = (k, s + p.sum) := by
  induction p generalizing s with
  | nil => simp
  | cons w p ih =>
      simp only [List.sum_cons] at h
      have hw : s + w ≤ cap := by omega
      have h' : (s + w) + p.sum ≤ cap := by omega
      simpa [step, hw, Nat.add_assoc] using ih (s + w) h'

private theorem fold_shift (cap : Nat) (xs : List Nat) (c s : Nat) :
    (xs.foldl (step cap) (c + 1, s)).1 = (xs.foldl (step cap) (c, s)).1 + 1 ∧
    (xs.foldl (step cap) (c + 1, s)).2 = (xs.foldl (step cap) (c, s)).2 := by
  induction xs generalizing c s with
  | nil => simp
  | cons w ws ih =>
      by_cases h : s + w ≤ cap
      · simpa [step, h] using ih c (s + w)
      · simpa [step, h, Nat.add_assoc] using ih (c + 1) w

private theorem pack_eq (cap : Nat) (xs : List Nat) : pack cap xs = xs.foldl (step cap) (0, 0) := by
  rfl

private theorem required_nonempty (cap : Nat) (xs : List Nat) (hne : xs ≠ []) :
    required cap xs = (xs.foldl (step cap) (0, 0)).1 + 1 := by
  simp [required, hne, pack_eq]

private theorem required_cons_takeFit (cap w : Nat) (ws : List Nat)
    (hw : w ≤ cap) (hws : ∀ x ∈ ws, x ≤ cap) :
    required cap (w :: ws) =
      required cap (takeFit cap 0 (w :: ws)).2 + 1 := by
  let p := (takeFit cap 0 (w :: ws)).1
  let q := (takeFit cap 0 (w :: ws)).2
  have hpq : p ++ q = w :: ws := by
    simpa [p, q] using takeFit_append cap 0 (w :: ws)
  have hfit : w ≤ cap := by simpa using hw
  have hp_ne : p ≠ [] := by
    simp [p, takeFit, hfit]
  have hp_sum : p.sum ≤ cap := by
    simpa [p] using takeFit_sum_le cap 0 (w :: ws) (by omega)
  have hallq : ∀ x ∈ q, x ≤ cap := by
    intro x hx
    have hxall : x ∈ w :: ws := by
      rw [← hpq]
      simp [hx]
    simp only [List.mem_cons] at hxall
    rcases hxall with rfl | hxws
    · exact hw
    · exact hws x hxws
  have hfold0 : (w :: ws).foldl (step cap) (0, 0) = q.foldl (step cap) (0, p.sum) := by
    rw [← hpq, List.foldl_append]
    have hnp : p.foldl (step cap) (0, 0) = (0, p.sum) := by
      simpa [Nat.zero_add] using fold_noreset cap 0 0 p (by simpa using hp_sum)
    rw [hnp]
  by_cases hqnil : q = []
  · have hp_eq : p = w :: ws := by
      rw [hqnil] at hpq
      simpa using hpq
    have hf : (w :: ws).foldl (step cap) (0, 0) = (0, (w :: ws).sum) := by
      rw [hfold0, hqnil]
      simp [hp_eq]
    have hr1 : required cap (w :: ws) = 1 := by
      rw [required_nonempty cap (w :: ws) (by simp), hf]
    have hrq : required cap (takeFit cap 0 (w :: ws)).2 = 0 := by
      simp [q, hqnil, required]
    omega
  · obtain ⟨u, us, hqcons⟩ := List.exists_cons_of_ne_nil hqnil
    have hu : u ≤ cap := hallq u (by simp [hqcons])
    have hnot : 0 + p.sum + u > cap := by
      have hnot' := takeFit_notfit cap 0 (w :: ws) u us (by simpa [q] using hqcons)
      simpa [p, Nat.zero_add] using hnot'
    have hnotle : ¬ (p.sum + u ≤ cap) := by omega
    have hstep : step cap (0, p.sum) u = (1, u) := by
      simp [step, hnotle]
    have hfoldq : q.foldl (step cap) (0, 0) = us.foldl (step cap) (0, u) := by
      rw [hqcons]
      simp [step, hu]
    have hfoldw : (w :: ws).foldl (step cap) (0, 0) = us.foldl (step cap) (1, u) := by
      rw [hfold0, hqcons]
      simp [hstep]
    have hs := fold_shift cap us 0 u
    have hc : (us.foldl (step cap) (1, u)).1 = (us.foldl (step cap) (0, u)).1 + 1 := by
      simpa using hs.1
    rw [required_nonempty cap (w :: ws) (by simp), hfoldw]
    rw [show (takeFit cap 0 (w :: ws)).2 = u :: us by simpa [q] using hqcons]
    have hfoldq' : (u :: us).foldl (step cap) (0, 0) = us.foldl (step cap) (0, u) := by
      simp [step, hu]
    rw [required_nonempty cap (u :: us) (by simp), hfoldq']
    omega

-- The greedy partition of a list: one maximal fitting prefix, then recurse.
private theorem exists_part_len (cap : Nat) : ∀ n : Nat, ∀ xs : List Nat,
    xs.length = n → (∀ w ∈ xs, w ≤ cap) →
    ∃ bs : List (List Nat), bs.flatten = xs ∧ bs.length = required cap xs ∧
      (∀ b ∈ bs, b.sum ≤ cap) := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hlen hall
      cases xs with
      | nil =>
          exact ⟨[], by simp [required]⟩
      | cons w ws =>
          have hw : w ≤ cap := hall w (by simp)
          have hws : ∀ x ∈ ws, x ≤ cap := by
            intro x hx
            exact hall x (by simp [hx])
          let p := (takeFit cap 0 (w :: ws)).1
          let q := (takeFit cap 0 (w :: ws)).2
          have hpq : p ++ q = w :: ws := by
            simpa [p, q] using takeFit_append cap 0 (w :: ws)
          have hp_ne : p ≠ [] := by simp [p, takeFit, hw]
          have hlenpq : p.length + q.length = n := by
            rw [← hlen, ← hpq]
            simp
          have hlt : q.length < n := by
            have hp_pos : 0 < p.length := List.ne_nil_iff_length_pos.mp hp_ne
            omega
          have hallq : ∀ x ∈ q, x ≤ cap := by
            intro x hx
            have hxall : x ∈ w :: ws := by
              rw [← hpq]
              simp [hx]
            exact hall x hxall
          obtain ⟨bsq, hflatq, hlenq, hsumq⟩ := ih q.length hlt q rfl hallq
          refine ⟨p :: bsq, ?_, ?_, ?_⟩
          · simp [hpq, hflatq]
          · have hdr := required_cons_takeFit cap w ws hw hws
            simpa [p, q, hlenq] using hdr.symm
          · intro b hb
            simp only [List.mem_cons] at hb
            rcases hb with rfl | hb
            · simpa [p] using takeFit_sum_le cap 0 (w :: ws) (by omega)
            · exact hsumq b hb

private theorem exists_part (cap : Nat) (xs : List Nat) (hall : ∀ w ∈ xs, w ≤ cap) :
    ∃ bs : List (List Nat), bs.flatten = xs ∧ bs.length = required cap xs ∧
      (∀ b ∈ bs, b.sum ≤ cap) :=
  exists_part_len cap xs.length xs rfl hall

private theorem flatten_reverse_blocks (bs : List (List Nat)) :
    (bs.reverse.map List.reverse).flatten = bs.flatten.reverse := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      simpa [List.map_reverse] using ih

theorem sound_ReverseScan : ParallelCuts.ReverseScan.Goal := by
  intro cap xs hall
  by_cases hxne : xs = []
  · subst xs
    simp
  have hallrev : ∀ w ∈ xs.reverse, w ≤ cap := by
    intro w hw
    exact hall w (by simpa using hw)
  obtain ⟨bs, hflat, hlen, hsum⟩ := exists_part cap xs.reverse hallrev
  let bs' := bs.reverse.map List.reverse
  have hflat' : bs'.flatten = xs := by
    rw [show bs'.flatten = bs.flatten.reverse by simpa [bs'] using flatten_reverse_blocks bs, hflat]
    simp
  have hsum' : ∀ b ∈ bs', b.sum ≤ cap := by
    intro b hb
    have hm : b ∈ bs.reverse.map List.reverse := by simpa [bs'] using hb
    rcases List.mem_map.mp hm with ⟨a, ha, haeq⟩
    subst b
    have := hsum a.reverse.reverse (by simpa using ha)
    simpa using this
  have hle1 : required cap xs ≤ required cap xs.reverse := by
    rw [required_nonempty cap xs hxne]
    have h := optimal cap xs bs' hflat' hxne hall hsum'
    simpa [bs', hlen] using h
  obtain ⟨cs, hflatc, hlenc, hsumc⟩ := exists_part cap xs hall
  let cs' := cs.reverse.map List.reverse
  have hflatc' : cs'.flatten = xs.reverse := by
    rw [show cs'.flatten = cs.flatten.reverse by simpa [cs'] using flatten_reverse_blocks cs, hflatc]
  have hsumc' : ∀ b ∈ cs', b.sum ≤ cap := by
    intro b hb
    have hm : b ∈ cs.reverse.map List.reverse := by simpa [cs'] using hb
    rcases List.mem_map.mp hm with ⟨a, ha, haeq⟩
    subst b
    have := hsumc a.reverse.reverse (by simpa using ha)
    simpa using this
  have hrevne : xs.reverse ≠ [] := by simp [hxne]
  have hle2 : required cap xs.reverse ≤ required cap xs := by
    rw [required_nonempty cap xs.reverse hrevne]
    have h := optimal cap xs.reverse cs' hflatc' hrevne hallrev hsumc'
    simpa [cs', hlenc] using h
  omega
end TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReverseScan
