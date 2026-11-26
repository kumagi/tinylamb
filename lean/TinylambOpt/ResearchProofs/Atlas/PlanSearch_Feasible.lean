import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `ParallelCuts.Feasible` — `∀ cap xs k, (∀ w ∈ xs, w ≤ cap) → (required cap xs ≤ k ↔ ∃
  blocks : List (List Nat), blocks.flatten = xs ∧ blocks.length ≤ k ∧ peak blocks ≤
  cap)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_Feasible

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

/-! Next-fit (`pack`) is optimal for contiguous partitioning: greedily closing a
block as late as possible never needs more blocks than any other partition.
The proof compares two scans of the same weight list.  The greedy scan starts
with one already-open block `(1,0)`; the other scan follows the block
boundaries of an arbitrary witness.  The invariant is

    greedy.closed ≤ witness.started  ∧  (equal counts → greedy.load ≤ witness.load).

Both directions of the theorem follow: the forward direction exhibits the
greedy partition, and the backward direction compares it against the witness. -/

private def step (cap : Nat) (s : Nat × Nat) (w : Nat) : Nat × Nat :=
  if s.2 + w ≤ cap then (s.1, s.2 + w) else (s.1 + 1, w)

private def inv (g a : Nat × Nat) : Prop :=
  g.1 ≤ a.1 ∧ (g.1 = a.1 → g.2 ≤ a.2)

-- Starting the greedy scan with count `c+1` keeps the count exactly one larger
-- than starting with `c`, while the load stays identical.
private theorem fold_shift (cap : Nat) (xs : List Nat) (c s : Nat) :
    (xs.foldl (step cap) (c + 1, s)).1 = (xs.foldl (step cap) (c, s)).1 + 1 ∧
    (xs.foldl (step cap) (c + 1, s)).2 = (xs.foldl (step cap) (c, s)).2 := by
  induction xs generalizing c s with
  | nil => simp
  | cons w ws ih =>
      by_cases h : s + w ≤ cap
      · simpa [step, h] using ih c (s + w)
      · simpa [step, h, Nat.add_assoc] using ih (c + 1) w

-- The witness stays inside its current block: its new load is bounded by cap.
private theorem inv_stay (cap : Nat) (ws : List Nat) (g : Nat × Nat) (a cur : Nat)
    (h : inv g (a, cur)) (hsum : cur + ws.sum ≤ cap) :
    inv (ws.foldl (step cap) g) (a, cur + ws.sum) := by
  induction ws generalizing g cur with
  | nil => simpa using h
  | cons w ws ih =>
      simp only [List.sum_cons] at hsum
      have hle : g.1 ≤ a := h.1
      have hcw : cur + w ≤ cap := by omega
      have hsum' : (cur + w) + ws.sum ≤ cap := by omega
      by_cases hg : g.2 + w ≤ cap
      · have h1 : inv (g.1, g.2 + w) (a, cur + w) := by
          constructor
          · exact h.1
          · intro heq
            have hs := h.2 heq
            dsimp at hs ⊢
            omega
        simpa [step, hg, Nat.add_assoc] using ih (g.1, g.2 + w) (cur + w) h1 hsum'
      · have hne : g.1 ≠ a := by
          intro heq
          have hs := h.2 heq
          dsimp at hs
          omega
        have hlt : g.1 < a := by omega
        have h1 : inv (g.1 + 1, w) (a, cur + w) := by
          constructor
          · omega
          · intro _
            omega
        simpa [step, hg, Nat.add_assoc] using ih (g.1 + 1, w) (cur + w) h1 hsum'

-- The witness starts a fresh block: the previous load is discarded.
private theorem inv_close (cap : Nat) (g : Nat × Nat) (a cur w : Nat)
    (h : inv g (a, cur)) (_hw : w ≤ cap) :
    inv (step cap g w) (a + 1, w) := by
  have hle : g.1 ≤ a := h.1
  by_cases hg : g.2 + w ≤ cap
  · have hres : g.1 ≤ a + 1 ∧ (g.1 = a + 1 → g.2 + w ≤ w) := by
      constructor
      · exact Nat.le_trans h.1 (Nat.le_succ a)
      · intro heq
        have hne : g.1 ≠ a := by
          intro heq'
          have hs := h.2 heq'
          dsimp at hs
          omega
        omega
    simpa [step, hg, inv] using hres
  · have hres : g.1 + 1 ≤ a + 1 ∧ (g.1 + 1 = a + 1 → w ≤ w) := by
      constructor
      · exact Nat.add_le_add_right h.1 1
      · intro heq
        omega
    simpa [step, hg, inv] using hres

-- Process one (nonempty) block of the witness other than its first block.
private theorem inv_block (cap : Nat) (b : List Nat) (g : Nat × Nat) (a cur : Nat)
    (h : inv g (a, cur)) (hbne : b ≠ []) (hbw : ∀ w ∈ b, w ≤ cap) (hsum : b.sum ≤ cap) :
    inv (b.foldl (step cap) g) (a + 1, b.sum) := by
  cases b with
  | nil => contradiction
  | cons w ws =>
      have hw : w ≤ cap := hbw w (by simp)
      have h1 : inv (step cap g w) (a + 1, w) := inv_close cap g a cur w h hw
      have hsum' : w + ws.sum ≤ cap := by simpa using hsum
      have h2 : inv (ws.foldl (step cap) (step cap g w)) (a + 1, w + ws.sum) :=
        inv_stay cap ws (step cap g w) (a + 1) w h1 hsum'
      simpa [List.foldl_cons, List.sum_cons] using h2

-- Fold over all blocks after the first; every block starts with a close step.
private theorem inv_rest (cap : Nat) (bs : List (List Nat)) (g : Nat × Nat) (a cur : Nat)
    (h : inv g (a, cur)) (hne : ∀ b ∈ bs, b ≠ [])
    (hbw : ∀ w ∈ bs.flatten, w ≤ cap) (hsum : ∀ b ∈ bs, b.sum ≤ cap) :
    inv (bs.flatten.foldl (step cap) g)
      (bs.foldl (fun (s : Nat × Nat) b => (s.1 + 1, b.sum)) (a, cur)) := by
  induction bs generalizing g a cur with
  | nil => simp; exact h
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
      have hne_bs : ∀ x ∈ bs, x ≠ [] := by
        intro x hx
        exact hne x (by simp [hx])
      have h1 := inv_block cap b g a cur h (hne b (by simp)) hbw_b hsum_b
      have h2 := ih (b.foldl (step cap) g) (a + 1) b.sum h1 hne_bs hbw_bs hsum_bs
      simpa [List.foldl_append, List.flatten_cons] using h2

-- Compare the greedy scan with a partition consisting of nonempty blocks.
private theorem inv_blocks (cap : Nat) (bs : List (List Nat)) (g : Nat × Nat)
    (hbs : bs ≠ []) (hne : ∀ b ∈ bs, b ≠ [])
    (h : inv g (1, 0))
    (hbw : ∀ w ∈ bs.flatten, w ≤ cap) (hsum : ∀ b ∈ bs, b.sum ≤ cap) :
    inv (bs.flatten.foldl (step cap) g)
      (bs.foldl (fun (s : Nat × Nat) b => (s.1 + 1, b.sum)) (0, 0)) := by
  cases bs with
  | nil => contradiction
  | cons b bs =>
      have hbne : b ≠ [] := hne b (by simp)
      have hbw_b : ∀ w ∈ b, w ≤ cap := by
        intro w hw
        exact hbw w (by simp [hw])
      have hsum_b : b.sum ≤ cap := hsum b (by simp)
      have hsum' : (0 : Nat) + b.sum ≤ cap := by simpa using hsum_b
      -- the first block is entered with a stay step from load 0
      have h1 : inv (b.foldl (step cap) g) (1, b.sum) := by
        simpa using (inv_stay cap b g 1 0 h hsum')
      have hbw_bs : ∀ w ∈ bs.flatten, w ≤ cap := by
        intro w hw
        exact hbw w (by simp [hw])
      have hsum_bs : ∀ x ∈ bs, x.sum ≤ cap := by
        intro x hx
        exact hsum x (by simp [hx])
      have hne_bs : ∀ x ∈ bs, x ≠ [] := by
        intro x hx
        exact hne x (by simp [hx])
      have h2 := inv_rest cap bs (b.foldl (step cap) g) 1 b.sum h1 hne_bs hbw_bs hsum_bs
      simpa [List.foldl_append, List.flatten_cons, List.foldl_cons] using h2

/-! The explicit greedy partition.  `done` holds the closed blocks, `cur` the
currently open one; both are built in forward order so that flattening is
easy to reason about. -/

private structure GBS where
  done : List (List Nat)
  cur : List Nat
  sum : Nat

private def gbInit : GBS := ⟨[], [], 0⟩

private def gbStep (cap : Nat) (st : GBS) (w : Nat) : GBS :=
  if st.sum + w ≤ cap then ⟨st.done, st.cur ++ [w], st.sum + w⟩
  else ⟨st.done ++ [st.cur], [w], w⟩

private theorem gb_flatten (cap : Nat) (xs : List Nat)
    (done : List (List Nat)) (cur : List Nat) (sum : Nat) :
    let st := xs.foldl (gbStep cap) ⟨done, cur, sum⟩
    st.done.flatten ++ st.cur = done.flatten ++ cur ++ xs := by
  induction xs generalizing done cur sum with
  | nil => simp
  | cons w ws ih =>
      by_cases h : sum + w ≤ cap
      · simpa [gbStep, h, List.append_assoc] using ih done (cur ++ [w]) (sum + w)
      · simpa [gbStep, h, List.append_assoc] using ih (done ++ [cur]) [w] w

private def GInv (cap : Nat) (st : GBS) (q : Nat × Nat) : Prop :=
  st.sum = q.2 ∧ st.done.length + 1 = q.1 ∧
  st.sum = st.cur.sum ∧ st.sum ≤ cap ∧ (∀ b ∈ st.done, b.sum ≤ cap)

private theorem gb_step_inv (cap : Nat) (st : GBS) (q : Nat × Nat) (w : Nat)
    (h : GInv cap st q) (hw : w ≤ cap) : GInv cap (gbStep cap st w) (step cap q w) := by
  have heq : st.sum = q.2 := h.1
  have hsumle : st.sum ≤ cap := h.2.2.2.1
  have hcursum : st.sum = st.cur.sum := h.2.2.1
  by_cases hs : st.sum + w ≤ cap
  · have hq : q.2 + w ≤ cap := by omega
    simp only [gbStep, step, hs, hq, ite_true]
    unfold GInv
    dsimp
    constructor
    · omega
    constructor
    · exact h.2.1
    constructor
    · simp [List.sum_append, hcursum]
    constructor
    · exact hs
    · exact h.2.2.2.2
  · have hq : ¬q.2 + w ≤ cap := by omega
    simp only [gbStep, step, hs, hq, ite_false]
    unfold GInv
    dsimp
    constructor
    · rfl
    constructor
    · simp [h.2.1]
    constructor
    · simp
    constructor
    · exact hw
    · intro b hb
      simp at hb
      rcases hb with hb | rfl
      · exact h.2.2.2.2 b hb
      · omega

private theorem gb_fold_inv_aux (cap : Nat) (xs : List Nat) (st : GBS) (q : Nat × Nat)
    (h : GInv cap st q) (hcap : ∀ w ∈ xs, w ≤ cap) :
    GInv cap (xs.foldl (gbStep cap) st) (xs.foldl (step cap) q) := by
  induction xs generalizing st q with
  | nil => simpa using h
  | cons w ws ih =>
      have hw : w ≤ cap := hcap w (by simp)
      have hcap' : ∀ u ∈ ws, u ≤ cap := by
        intro u hu
        exact hcap u (by simp [hu])
      exact ih (gbStep cap st w) (step cap q w) (gb_step_inv cap st q w h hw) hcap'

private theorem peak_le_of_all (cap : Nat) {blocks : List (List Nat)}
    (h : ∀ b ∈ blocks, b.sum ≤ cap) : peak blocks ≤ cap := by
  unfold peak
  by_cases hem : blocks.map List.sum = []
  · simp [hem]
  · have hmxne : (blocks.map List.sum).max? ≠ none := by
      simpa [List.max?_eq_none_iff] using hem
    cases hmx : (blocks.map List.sum).max? with
    | none => contradiction
    | some v =>
        have hv_mem : v ∈ blocks.map List.sum := (List.max?_eq_some_iff.mp hmx).1
        rcases List.mem_map.mp hv_mem with ⟨b, hb, hbv⟩
        have hbcap : b.sum ≤ cap := h b hb
        simpa [hmx, hbv] using hbcap

private theorem mem_sum_le_peak {blocks : List (List Nat)} {b : List Nat} (hb : b ∈ blocks) :
    b.sum ≤ peak blocks := by
  unfold peak
  have hbmem : b.sum ∈ blocks.map List.sum := List.mem_map.mpr ⟨b, hb, rfl⟩
  have hne : blocks.map List.sum ≠ [] := by
    intro hnil
    rw [hnil] at hbmem
    simp at hbmem
  have hmxne : (blocks.map List.sum).max? ≠ none := by
    simpa [List.max?_eq_none_iff] using hne
  cases hmx : (blocks.map List.sum).max? with
  | none => contradiction
  | some v =>
      have hall : ∀ x ∈ blocks.map List.sum, x ≤ v :=
        (List.max?_le_iff hmx).1 (Nat.le_refl v)
      simpa [hmx] using hall b.sum hbmem

private def isNEmpty (b : List Nat) : Bool := decide (b ≠ [])

private theorem filter_flatten (blocks : List (List Nat)) :
    (blocks.filter isNEmpty).flatten = blocks.flatten := by
  induction blocks with
  | nil => simp
  | cons b bs ih =>
      by_cases hb : b = [] <;> simp [isNEmpty, hb, ih]

private theorem alt_fold_count_aux (bs : List (List Nat)) (c s : Nat) :
    (bs.foldl (fun (s : Nat × Nat) b => (s.1 + 1, b.sum)) (c, s)).1 = c + bs.length := by
  induction bs generalizing c s with
  | nil => simp
  | cons b bs ih =>
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (c + 1) b.sum

private theorem alt_fold_count (bs : List (List Nat)) :
    (bs.foldl (fun (s : Nat × Nat) b => (s.1 + 1, b.sum)) (0, 0)).1 = bs.length := by
  simpa using alt_fold_count_aux bs 0 0

private theorem pack_eq_step (cap : Nat) (xs : List Nat) :
    pack cap xs = xs.foldl (step cap) (0, 0) := by
  rfl

theorem sound_Feasible : ParallelCuts.Feasible.Goal := by
  intro cap xs k hcap
  have xs_eq_nil_of_empty : xs.isEmpty = true → xs = [] := by
    intro h
    cases xs with
    | nil => rfl
    | cons y ys => simp at h
  constructor
  · intro hreq
    by_cases hempty : xs.isEmpty
    · have hxs : xs = [] := xs_eq_nil_of_empty hempty
      refine ⟨[], ?_, ?_, ?_⟩
      · simp [hxs]
      · simp
      · simp [peak]
    · let st := xs.foldl (gbStep cap) gbInit
      let blocks := st.done ++ [st.cur]
      have hgi : GInv cap st (xs.foldl (step cap) (1, 0)) :=
        gb_fold_inv_aux cap xs gbInit (1, 0) (by simp [GInv, gbInit]) hcap
      have hflatten : st.done.flatten ++ st.cur = xs := by
        simpa [gbInit, st] using (gb_flatten cap xs [] [] 0)
      have hblocks_flat : blocks.flatten = xs := by
        simp [blocks, hflatten]
      have hlen_g : st.done.length + 1 = (xs.foldl (step cap) (1, 0)).1 := hgi.2.1
      have hlen : blocks.length = (xs.foldl (step cap) (1, 0)).1 := by
        simpa [blocks] using hlen_g
      have hshift : (xs.foldl (step cap) (1, 0)).1 = (xs.foldl (step cap) (0, 0)).1 + 1 := by
        simpa using (fold_shift cap xs 0 0).1
      have hreq'' : (xs.foldl (step cap) (0, 0)).1 + 1 ≤ k := by
        unfold required at hreq
        rw [pack_eq_step] at hreq
        simpa [hempty] using hreq
      have hlen_le : blocks.length ≤ k := by omega
      have hpeak : peak blocks ≤ cap := by
        apply peak_le_of_all cap
        intro b hb
        simp [blocks] at hb
        rcases hb with hb | rfl
        · exact hgi.2.2.2.2 b hb
        · have hsumle : st.sum ≤ cap := hgi.2.2.2.1
          have hcursum : st.sum = st.cur.sum := hgi.2.2.1
          omega
      exact ⟨blocks, hblocks_flat, hlen_le, hpeak⟩
  · rintro ⟨blocks, hflat, hlen, hpeak⟩
    by_cases hempty : xs.isEmpty
    · simp [required, hempty]
    · let bs := blocks.filter isNEmpty
      have hbs_flat : bs.flatten = xs := by
        rw [show bs.flatten = blocks.flatten by simp [bs, filter_flatten], hflat]
      have hbs_ne : bs ≠ [] := by
        intro h
        have hxe : xs = [] := by
          rw [← hbs_flat, h]
          simp
        exact hempty (by simp [hxe])
      have hbs_hne : ∀ b ∈ bs, b ≠ [] := by
        intro b hb
        have hp := (List.mem_filter.mp (by simpa [bs] using hb)).2
        exact of_decide_eq_true (by simpa [isNEmpty] using hp)
      have hbs_len : bs.length ≤ k := by
        have hf := List.length_filter_le isNEmpty blocks
        have : bs.length = (blocks.filter isNEmpty).length := rfl
        omega
      have hbs_sum : ∀ b ∈ bs, b.sum ≤ cap := by
        intro b hb
        have hb0 : b ∈ blocks := (List.mem_filter.mp (by simpa [bs] using hb)).1
        have hle := mem_sum_le_peak hb0
        omega
      have hbs_bw : ∀ w ∈ bs.flatten, w ≤ cap := by
        intro w hw
        rw [hbs_flat] at hw
        exact hcap w hw
      have hinv := inv_blocks cap bs (1, 0) hbs_ne hbs_hne (by simp [inv]) hbs_bw hbs_sum
      have hinv' : inv (xs.foldl (step cap) (1, 0))
          (bs.foldl (fun (s : Nat × Nat) b => (s.1 + 1, b.sum)) (0, 0)) := by
        simpa [hbs_flat] using hinv
      have hgcount : (xs.foldl (step cap) (1, 0)).1 ≤ k := by
        have hcnt := alt_fold_count bs
        have hfirst := hinv'.1
        rw [hcnt] at hfirst
        exact Nat.le_trans hfirst hbs_len
      have hreqeq : required cap xs = (xs.foldl (step cap) (1, 0)).1 := by
        unfold required
        simp [hempty]
        rw [pack_eq_step]
        exact (fold_shift cap xs 0 0).1.symm
      omega

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_Feasible
