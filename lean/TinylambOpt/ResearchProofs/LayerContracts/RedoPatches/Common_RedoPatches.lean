import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved (81 claims, including the following)

- `Affine.Difference` — `∀ a x y, x ≤ y → model.patch a y-model.patch a x = a.1*(y-x)`
- `Affine.ExactExchange` — Is the proposed conflict test complete as well as sound?
- `Affine.ExchangeGuard` — When marked independent, patches may be scheduled in either
  order.

Refuted by counterexample (19 claims, including the following)

- `Affine.NewestOnly` — A last-record-per-cell index reconstructs both value and stamp.
- `Affine.NoStamp` — Blindly repeating a patch after recovery is observationally
  idempotent.
- `Affine.RestartInside` — Can a fused record also replay on a snapshot already
  containing its first half?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common

open TinylambOpt.ResearchGoals.LayerContracts.RedoPatches

/-! Generic replay-list facts and reusable per-model contracts. -/

theorem logRun_append (m : Model) (s : Cell) (a b : List Record) :
    logRun m s (a ++ b) = logRun m (logRun m s a) b := by
  simp [logRun, List.foldl_append]

theorem replay_lsn_ge (m : Model) (s : Cell) (r : Record) :
    s.lsn ≤ (replay m s r).lsn := by
  unfold replay
  by_cases h : s.lsn < r.1
  · simp [h]
    omega
  · simp [h]

theorem logRun_lsn_ge_self (m : Model) (s : Cell) (a : List Record) :
    s.lsn ≤ (logRun m s a).lsn := by
  induction a generalizing s with
  | nil => simp [logRun]
  | cons r t ih =>
      change s.lsn ≤ (logRun m (replay m s r) t).lsn
      have h1 := replay_lsn_ge m s r
      have h2 := ih (replay m s r)
      omega

theorem logRun_lsn_ge_records (m : Model) (s : Cell) (a : List Record)
    (hp : (a.map Prod.fst).Pairwise (· < ·)) :
    ∀ r ∈ a, r.1 ≤ (logRun m s a).lsn := by
  induction a generalizing s with
  | nil => simp
  | cons hd tl ih =>
      have hp' : (hd.1 :: tl.map Prod.fst).Pairwise (· < ·) := by simpa using hp
      have hpt : (tl.map Prod.fst).Pairwise (· < ·) := List.Pairwise.of_cons hp'
      change ∀ x ∈ hd :: tl, x.1 ≤ (logRun m (replay m s hd) tl).lsn
      intro x hx
      simp only [List.mem_cons] at hx
      by_cases hxhd : x = hd
      · subst x
        by_cases ht : tl = []
        · rw [ht]
          simp only [logRun, List.foldl_nil]
          unfold replay
          by_cases h : s.lsn < hd.1 <;> simp [h] <;> omega
        · obtain ⟨y, hy⟩ := List.exists_mem_of_ne_nil tl ht
          have hyf : y.1 ∈ tl.map Prod.fst := List.mem_map_of_mem hy
          have hry : hd.1 < y.1 := List.rel_of_pairwise_cons hp' hyf
          have hyfinal := ih (replay m s hd) hpt y hy
          omega
      · have hxt : x ∈ tl := by simpa [hxhd] using hx
        exact ih (replay m s hd) hpt x hxt

theorem logRun_eq_self_of_all_le (m : Model) (s : Cell) (a : List Record)
    (h : ∀ r ∈ a, r.1 ≤ s.lsn) : logRun m s a = s := by
  induction a generalizing s with
  | nil => rfl
  | cons r t ih =>
      have hr : replay m s r = s := by
        unfold replay
        split
        · have := h r (by simp)
          omega
        · rfl
      change logRun m (replay m s r) t = s
      rw [hr]
      exact ih s (fun x hx => h x (by simp [hx]))

theorem suffixRetry_all (m : Model) : suffixRetry m := by
  intro s a b hp
  have hlsn := logRun_lsn_ge_records m s a hp
  have hidem : logRun m (logRun m s a) a = logRun m s a :=
    logRun_eq_self_of_all_le m (logRun m s a) a hlsn
  rw [logRun_append, logRun_append, hidem]

theorem parallelFold_of_fusion (m : Model) (h : fusion m) : parallelFold m := by
  intro a b c l r p q x hfl hlp hbc har
  have hl : ∀ y, m.patch l y = m.patch b (m.patch a y) := fun y => h a b l y hfl
  have hp : ∀ y, m.patch p y = m.patch c (m.patch l y) := fun y => h l c p y hlp
  have hr : ∀ y, m.patch r y = m.patch c (m.patch b y) := fun y => h b c r y hbc
  have hq : ∀ y, m.patch q y = m.patch r (m.patch a y) := fun y => h a r q y har
  calc
    m.patch p x = m.patch c (m.patch l x) := hp x
    _ = m.patch c (m.patch b (m.patch a x)) := by rw [hl]
    _ = m.patch r (m.patch a x) := by rw [hr]
    _ = m.patch q x := (hq x).symm

theorem restartInside_of_fusion_noStamp (m : Model) (hf : fusion m) (hn : noStamp m) :
    restartInside m := by
  intro s i j a b c his hij hfuse
  dsimp only
  let y := m.patch a s.value
  have h1 : m.patch c y = m.patch b (m.patch a y) := hf a b c y hfuse
  have h2 : m.patch a y = y := by simpa [y] using hn a s.value
  rw [h2] at h1
  have hsj : s.lsn < j := by omega
  simp [replay, his, y, h1]

theorem restartInside_of_fuse_eq (m : Model) (h : ∀ a b c, m.fuse a b = some c → ∀ x, m.patch c x = m.patch b x) :
    restartInside m := by
  intro s i j a b c his hij hfuse
  dsimp only
  have h1 := h a b c hfuse (m.patch a s.value)
  have hsj : s.lsn < j := by omega
  simp [replay, his, h1]

theorem newestOnly_of_const (m : Model) (h : ∀ b x y, m.patch b x = m.patch b y) : newestOnly m := by
  intro s i j a b his hij
  have hp : m.patch b s.value = m.patch b (m.patch a s.value) := h b s.value (m.patch a s.value)
  have hsj : s.lsn < j := by omega
  have h1 : replay m s (i, a) = ⟨i, m.patch a s.value⟩ := by
    unfold replay
    simp [his]
  have h2 : replay m ⟨i, m.patch a s.value⟩ (j, b) = ⟨j, m.patch b (m.patch a s.value)⟩ := by
    unfold replay
    simp [hij]
  have h3 : replay m s (j, b) = ⟨j, m.patch b s.value⟩ := by
    unfold replay
    simp [hsj]
  simp only [logRun, List.foldl_cons, List.foldl_nil]
  rw [h1, h2, h3, hp]

/-! Bitwise helpers (no Mathlib in this environment). -/

theorem sub_div_two_of_mod_le {x y : Nat} (hyx : y ≤ x) (hmod : y % 2 ≤ x % 2) :
    (x - y) / 2 = x / 2 - y / 2 := by
  omega

theorem and_mod_le (x M : Nat) : (x &&& M) % 2 ≤ x % 2 := by
  change (x &&& M) % 2^1 ≤ x % 2^1
  rw [Nat.and_mod_two_pow]
  exact Nat.and_le_left

theorem sub_mask_div_two (x M : Nat) :
    (x - (x &&& M)) / 2 = x / 2 - ((x / 2) &&& (M / 2)) := by
  rw [← Nat.and_div_two]
  exact sub_div_two_of_mod_le Nat.and_le_left (and_mod_le x M)

theorem testBit_sub_mask (i x M : Nat) :
    (x - (x &&& M)).testBit i = (x.testBit i && !(M.testBit i)) := by
  induction i generalizing x M with
  | zero =>
      rw [Nat.testBit_zero, Nat.testBit_zero, Nat.testBit_zero]
      by_cases hm : M % 2 = 1
      · have hxm : (x &&& M) % 2 = x % 2 := by
          change (x &&& M) % 2^1 = x % 2^1
          rw [Nat.and_mod_two_pow]
          simp [hm]
        have hd : (x - (x &&& M)) % 2 = 0 := by omega
        simp [hd, hm]
      · have hm0 : M % 2 = 0 := by omega
        have hxm : (x &&& M) % 2 = 0 := by
          change (x &&& M) % 2^1 = (0 : Nat)
          rw [Nat.and_mod_two_pow]
          simp [hm0]
        have hxdecomp := Nat.mod_add_div x 2
        have hydecomp := Nat.mod_add_div (x &&& M) 2
        have hle : x &&& M ≤ x := Nat.and_le_left
        have hd : (x - (x &&& M)) % 2 = x % 2 := by omega
        simp [hd, hm]
  | succ i ih =>
      rw [Nat.testBit_succ, Nat.testBit_succ, Nat.testBit_succ]
      rw [sub_mask_div_two]
      exact ih (x / 2) (M / 2)

theorem testBit_mergeMask (M B x i : Nat) :
    (mergeMask M B x).testBit i = ((x.testBit i && !(M.testBit i)) || (B.testBit i && M.testBit i)) := by
  rw [mergeMask, Nat.testBit_or, Nat.testBit_and, testBit_sub_mask]

theorem sub_mask_disjoint (x M : Nat) : (x - (x &&& M)) &&& M = 0 := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_and, testBit_sub_mask]
  cases hm : M.testBit i <;> simp

/-! ------------------------------ Overwrite ------------------------------ -/

theorem overwrite_fusion : Overwrite.Fusion.Goal := by
  intro a b c x hf
  simp [Overwrite.model] at hf ⊢
  subst c
  rfl

theorem overwrite_restartInside : Overwrite.RestartInside.Goal := by
  exact restartInside_of_fuse_eq Overwrite.model (by
    intro a b c hf x
    simp [Overwrite.model] at hf ⊢
    subst c
    rfl)

theorem overwrite_exchangeGuard : Overwrite.ExchangeGuard.Goal := by
  intro a b x h
  simp [Overwrite.model] at h ⊢
  exact h.symm

theorem overwrite_exactExchange : Overwrite.ExactExchange.Goal := by
  intro a b h
  simp [Overwrite.model] at h ⊢
  exact h.symm

theorem overwrite_newestOnly : Overwrite.NewestOnly.Goal := by
  exact newestOnly_of_const Overwrite.model (by intro b x y; rfl)

theorem overwrite_noStamp : Overwrite.NoStamp.Goal := by
  intro a x
  rfl

theorem overwrite_suffixRetry : Overwrite.SuffixRetry.Goal := suffixRetry_all Overwrite.model

theorem overwrite_parallelFold : Overwrite.ParallelFold.Goal :=
  parallelFold_of_fusion Overwrite.model overwrite_fusion

theorem overwrite_constant : Overwrite.Constant.Goal := by
  intro a x y
  rfl

theorem overwrite_collision : Overwrite.Collision.Goal := by
  intro a b x
  simp [Overwrite.model]

/-! ------------------------------ Increment ------------------------------ -/

theorem increment_fusion : Increment.Fusion.Goal := by
  intro a b c x hf
  simp [Increment.model] at hf ⊢
  subst c
  omega

theorem not_increment_restartInside : ¬ Increment.RestartInside.Goal := by
  intro h
  have hh := h ⟨0, 0⟩ 1 2 (1, 0) (1, 0) (2, 0) (by simp) (by simp) (by simp [Increment.model])
  simp [Increment.model, replay] at hh

theorem increment_exchangeGuard : Increment.ExchangeGuard.Goal := by
  intro a b x h
  simp [Increment.model]
  omega

theorem increment_exactExchange : Increment.ExactExchange.Goal := by
  intro a b h
  simp [Increment.model]

theorem not_increment_newestOnly : ¬ Increment.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 0⟩ 1 2 (1, 0) (1, 0) (by simp) (by simp)
  simp [Increment.model, replay, logRun] at hh

theorem not_increment_noStamp : ¬ Increment.NoStamp.Goal := by
  intro h
  have hh := h (1, 0) 0
  simp [Increment.model] at hh

theorem increment_suffixRetry : Increment.SuffixRetry.Goal := suffixRetry_all Increment.model

theorem increment_parallelFold : Increment.ParallelFold.Goal :=
  parallelFold_of_fusion Increment.model increment_fusion

theorem increment_rebase : Increment.Rebase.Goal := by
  intro a x d
  simp [Increment.model]
  omega

theorem increment_duplicateCost : Increment.DuplicateCost.Goal := by
  intro a x
  simp [Increment.model]

/-! -------------------------------- Affine -------------------------------- -/

theorem affine_fusion : Affine.Fusion.Goal := by
  intro a b c x hf
  simp [Affine.model] at hf ⊢
  subst c
  simp [Nat.mul_add, Nat.mul_comm, Nat.mul_left_comm, Nat.add_comm, Nat.add_left_comm]

theorem not_affine_restartInside : ¬ Affine.RestartInside.Goal := by
  intro h
  have hh := h ⟨0, 1⟩ 1 2 (2, 0) (1, 0) (2, 0) (by simp) (by simp) (by simp [Affine.model])
  simp [Affine.model, replay] at hh

theorem affine_exchangeGuard : Affine.ExchangeGuard.Goal := by
  intro a b x h
  simp [Affine.model] at h ⊢
  calc
    b.1 * (a.1 * x + a.2) + b.2 = (b.1 * a.1) * x + (b.1 * a.2 + b.2) := by
      simp [Nat.mul_add, Nat.mul_assoc, Nat.add_assoc]
    _ = (a.1 * b.1) * x + (a.1 * b.2 + a.2) := by rw [Nat.mul_comm b.1 a.1, h]
    _ = a.1 * (b.1 * x + b.2) + a.2 := by
      simp [Nat.mul_add, Nat.mul_assoc, Nat.add_assoc]

theorem affine_exactExchange : Affine.ExactExchange.Goal := by
  intro a b h
  simp [Affine.model] at h ⊢
  have h0 := h 0
  simpa using h0

theorem not_affine_newestOnly : ¬ Affine.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 1⟩ 1 2 (2, 0) (1, 0) (by simp) (by simp)
  simp [Affine.model, replay, logRun] at hh

theorem not_affine_noStamp : ¬ Affine.NoStamp.Goal := by
  intro h
  have hh := h (2, 0) 1
  simp [Affine.model] at hh

theorem affine_suffixRetry : Affine.SuffixRetry.Goal := suffixRetry_all Affine.model

theorem affine_parallelFold : Affine.ParallelFold.Goal :=
  parallelFold_of_fusion Affine.model affine_fusion

theorem affine_noBase : Affine.NoBase.Goal := by
  intro a
  constructor
  · intro h
    have h1 := h 1
    simpa [Affine.model] using h1
  · intro h x
    simp [Affine.model, h]

theorem affine_difference : Affine.Difference.Goal := by
  intro a x y hxy
  simp [Affine.model]
  rw [Nat.add_sub_add_right]
  rw [← Nat.mul_sub_left_distrib]

/-! ------------------------------ HighWater ------------------------------- -/

theorem highWater_fusion : HighWater.Fusion.Goal := by
  intro a b c x hf
  simp [HighWater.model] at hf ⊢
  subst c
  simp [Nat.max_comm, Nat.max_left_comm]

theorem highWater_exchangeGuard : HighWater.ExchangeGuard.Goal := by
  intro a b x h
  simp [HighWater.model]
  omega

theorem highWater_exactExchange : HighWater.ExactExchange.Goal := by
  intro a b h
  simp [HighWater.model]

theorem not_highWater_newestOnly : ¬ HighWater.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 0⟩ 1 2 (5, 0) (1, 0) (by simp) (by simp)
  simp [HighWater.model, replay, logRun] at hh

theorem highWater_noStamp : HighWater.NoStamp.Goal := by
  intro a x
  simp [HighWater.model]
  omega

theorem highWater_restartInside : HighWater.RestartInside.Goal :=
  restartInside_of_fusion_noStamp HighWater.model highWater_fusion highWater_noStamp

theorem highWater_suffixRetry : HighWater.SuffixRetry.Goal := suffixRetry_all HighWater.model

theorem highWater_parallelFold : HighWater.ParallelFold.Goal :=
  parallelFold_of_fusion HighWater.model highWater_fusion

theorem highWater_neverShrink : HighWater.NeverShrink.Goal := by
  intro a x
  simp [HighWater.model]
  omega

theorem highWater_dominated : HighWater.Dominated.Goal := by
  intro a b x hab
  simp [HighWater.model]
  omega

/-! ------------------------------- Ceiling -------------------------------- -/

theorem ceiling_fusion : Ceiling.Fusion.Goal := by
  intro a b c x hf
  simp [Ceiling.model] at hf ⊢
  subst c
  simp [Nat.min_comm, Nat.min_left_comm]

theorem ceiling_exchangeGuard : Ceiling.ExchangeGuard.Goal := by
  intro a b x h
  simp [Ceiling.model]
  omega

theorem ceiling_exactExchange : Ceiling.ExactExchange.Goal := by
  intro a b h
  simp [Ceiling.model]

theorem not_ceiling_newestOnly : ¬ Ceiling.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 10⟩ 1 2 (5, 0) (7, 0) (by simp) (by simp)
  simp [Ceiling.model, replay, logRun] at hh

theorem ceiling_noStamp : Ceiling.NoStamp.Goal := by
  intro a x
  simp [Ceiling.model]

theorem ceiling_restartInside : Ceiling.RestartInside.Goal :=
  restartInside_of_fusion_noStamp Ceiling.model ceiling_fusion ceiling_noStamp

theorem ceiling_suffixRetry : Ceiling.SuffixRetry.Goal := suffixRetry_all Ceiling.model

theorem ceiling_parallelFold : Ceiling.ParallelFold.Goal :=
  parallelFold_of_fusion Ceiling.model ceiling_fusion

theorem ceiling_neverGrow : Ceiling.NeverGrow.Goal := by
  intro a x
  simp [Ceiling.model]
  omega

theorem ceiling_smallerLimit : Ceiling.SmallerLimit.Goal := by
  intro a b x hba
  simp [Ceiling.model]
  omega

/-! ------------------------------- XorFlags -------------------------------- -/

theorem xorFlags_fusion : XorFlags.Fusion.Goal := by
  intro a b c x hf
  simp [XorFlags.model] at hf ⊢
  subst c
  rw [Nat.xor_assoc]

theorem not_xorFlags_restartInside : ¬ XorFlags.RestartInside.Goal := by
  intro h
  have hh := h ⟨0, 0⟩ 1 2 (1, 0) (1, 0) (0, 0) (by simp) (by simp) (by simp [XorFlags.model])
  simp [XorFlags.model, replay] at hh

theorem xorFlags_exchangeGuard : XorFlags.ExchangeGuard.Goal := by
  intro a b x h
  simp [XorFlags.model]
  calc
    x ^^^ a.1 ^^^ b.1 = x ^^^ (a.1 ^^^ b.1) := Nat.xor_assoc x a.1 b.1
    _ = x ^^^ (b.1 ^^^ a.1) := by rw [Nat.xor_comm a.1 b.1]
    _ = x ^^^ b.1 ^^^ a.1 := (Nat.xor_assoc x b.1 a.1).symm

theorem xorFlags_exactExchange : XorFlags.ExactExchange.Goal := by
  intro a b h
  simp [XorFlags.model]

theorem not_xorFlags_newestOnly : ¬ XorFlags.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 0⟩ 1 2 (1, 0) (1, 0) (by simp) (by simp)
  simp [XorFlags.model, replay, logRun] at hh

theorem not_xorFlags_noStamp : ¬ XorFlags.NoStamp.Goal := by
  intro h
  have hh := h (1, 0) 0
  simp [XorFlags.model] at hh

theorem xorFlags_suffixRetry : XorFlags.SuffixRetry.Goal := suffixRetry_all XorFlags.model

theorem xorFlags_parallelFold : XorFlags.ParallelFold.Goal :=
  parallelFold_of_fusion XorFlags.model xorFlags_fusion

theorem xorFlags_undo : XorFlags.Undo.Goal := by
  intro a x
  simp [XorFlags.model, Nat.xor_assoc]

theorem xorFlags_cancelDelta : XorFlags.CancelDelta.Goal := by
  intro a b x h
  simp [XorFlags.model] at h
  have hh := congrArg (fun y : Nat => x ^^^ y) h
  rw [← Nat.xor_assoc, ← Nat.xor_assoc, Nat.xor_self] at hh
  simpa using hh

theorem newestOnly_of_prefix_absorbed (m : Model)
    (h : ∀ a b x, m.patch b (m.patch a x) = m.patch b x) : newestOnly m := by
  intro s i j a b his hij
  have hp := h a b s.value
  have hsj : s.lsn < j := by omega
  have h1 : replay m s (i, a) = ⟨i, m.patch a s.value⟩ := by
    unfold replay
    simp [his]
  have h2 : replay m ⟨i, m.patch a s.value⟩ (j, b) = ⟨j, m.patch b (m.patch a s.value)⟩ := by
    unfold replay
    simp [hij]
  have h3 : replay m s (j, b) = ⟨j, m.patch b s.value⟩ := by
    unfold replay
    simp [hsj]
  simp only [logRun, List.foldl_cons, List.foldl_nil]
  rw [h1, h2, h3, hp]

/-! ------------------------------ FieldStore ------------------------------- -/

theorem fieldStore_noStamp : FieldStore.NoStamp.Goal := by
  intro a x
  apply Nat.eq_of_testBit_eq
  intro i
  cases hA : a.1.testBit i <;> cases hB : a.2.testBit i <;> cases hx : x.testBit i <;>
    simp [FieldStore.model, testBit_mergeMask, hA, hB, hx]

theorem fieldStore_fusion : FieldStore.Fusion.Goal := by
  intro a b c x hf
  simp [FieldStore.model] at hf
  subst c
  apply Nat.eq_of_testBit_eq
  intro i
  cases hA : a.1.testBit i <;> cases hB : b.1.testBit i <;>
    cases ha2 : a.2.testBit i <;> cases hb2 : b.2.testBit i <;> cases hx : x.testBit i <;>
    simp [FieldStore.model, testBit_mergeMask, Nat.testBit_and, Nat.testBit_or,
      hA, hB, ha2, hb2, hx]

theorem fieldStore_restartInside : FieldStore.RestartInside.Goal :=
  restartInside_of_fusion_noStamp FieldStore.model fieldStore_fusion fieldStore_noStamp

theorem fieldStore_exchangeGuard : FieldStore.ExchangeGuard.Goal := by
  intro a b x h
  simp [FieldStore.model] at h
  apply Nat.eq_of_testBit_eq
  intro i
  have hz := congrArg (fun n : Nat => n.testBit i) h
  simp [Nat.testBit_and] at hz
  cases hA : a.1.testBit i <;> cases hB : b.1.testBit i <;>
    cases ha2 : a.2.testBit i <;> cases hb2 : b.2.testBit i <;> cases hx : x.testBit i <;>
    simp [FieldStore.model, testBit_mergeMask, hA, hB, ha2, hb2, hx] at hz ⊢

theorem not_fieldStore_exactExchange : ¬ FieldStore.ExactExchange.Goal := by
  intro h
  have hg := h (1, 1) (1, 1) (fun x => rfl)
  simp [FieldStore.model] at hg

theorem not_fieldStore_newestOnly : ¬ FieldStore.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 0⟩ 1 2 (1, 1) (2, 0) (by simp) (by simp)
  simp [FieldStore.model, mergeMask, replay, logRun] at hh

theorem fieldStore_suffixRetry : FieldStore.SuffixRetry.Goal := suffixRetry_all FieldStore.model

theorem fieldStore_parallelFold : FieldStore.ParallelFold.Goal :=
  parallelFold_of_fusion FieldStore.model fieldStore_fusion

theorem fieldStore_preserveUnmasked : FieldStore.PreserveUnmasked.Goal := by
  intro a x i hi
  simp [FieldStore.model, testBit_mergeMask, hi]

theorem fieldStore_maskedValue : FieldStore.MaskedValue.Goal := by
  intro a x
  apply Nat.eq_of_testBit_eq
  intro i
  cases hA : a.1.testBit i <;> cases hB : a.2.testBit i <;> cases hx : x.testBit i <;>
    simp [FieldStore.model, testBit_mergeMask, Nat.testBit_and, hA, hB, hx]

/-! -------------------------------- Consume -------------------------------- -/

theorem consume_fusion : Consume.Fusion.Goal := by
  intro a b c x hf
  simp [Consume.model] at hf ⊢
  subst c
  rw [Nat.sub_sub]

theorem not_consume_restartInside : ¬ Consume.RestartInside.Goal := by
  intro h
  have hh := h ⟨0, 3⟩ 1 2 (1, 0) (1, 0) (2, 0) (by simp) (by simp) (by simp [Consume.model])
  simp [Consume.model, replay] at hh

theorem consume_exchangeGuard : Consume.ExchangeGuard.Goal := by
  intro a b x h
  simp [Consume.model, Nat.sub_sub, Nat.add_comm]

theorem consume_exactExchange : Consume.ExactExchange.Goal := by
  intro a b h
  simp [Consume.model]

theorem not_consume_newestOnly : ¬ Consume.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 3⟩ 1 2 (1, 0) (2, 0) (by simp) (by simp)
  simp [Consume.model, replay, logRun] at hh

theorem not_consume_noStamp : ¬ Consume.NoStamp.Goal := by
  intro h
  have hh := h (1, 0) 2
  simp [Consume.model] at hh

theorem consume_suffixRetry : Consume.SuffixRetry.Goal := suffixRetry_all Consume.model

theorem consume_parallelFold : Consume.ParallelFold.Goal :=
  parallelFold_of_fusion Consume.model consume_fusion

theorem consume_underflow : Consume.Underflow.Goal := by
  intro a x
  simp [Consume.model]
  omega

theorem consume_residual : Consume.Residual.Goal := by
  intro a x h
  simp [Consume.model]
  omega

/-! ------------------------------- CompareSwap ------------------------------ -/

theorem compareSwap_fusion : CompareSwap.Fusion.Goal := by
  intro a b c x hf
  by_cases habp : a.1 = b.1
  · have hab : (a.1 == b.1) = true := by simp [habp]
    simp [CompareSwap.model] at hf
    rcases hf with ⟨_, hceq⟩
    subst c
    by_cases hxp : x = a.1
    · have hxb : x = b.1 := by simpa [habp] using hxp
      simp [CompareSwap.model, hxp]
    · have hxb : x ≠ b.1 := by
        intro heq
        exact hxp (by simpa [habp] using heq)
      simp [CompareSwap.model, hxp, hxb]
  · have hab : (a.1 == b.1) = false := by simp [habp]
    simp [CompareSwap.model] at hf
    rcases hf with ⟨heq, _⟩
    exact (habp heq).elim

theorem compareSwap_exchangeGuard : CompareSwap.ExchangeGuard.Goal := by
  intro a b x h
  simp [CompareSwap.model] at h
  rcases h with ⟨⟨h1, h2⟩, h3⟩
  have h1s : b.1 ≠ a.1 := fun he => h1 he.symm
  by_cases hxap : x = a.1
  · by_cases hxbp : x = b.1
    · exfalso
      exact h1 (hxap.symm.trans hxbp)
    · simp [CompareSwap.model, hxap, h1, h2]
  · by_cases hxbp : x = b.1
    · simp [CompareSwap.model, hxbp, h1s, h3]
    · simp [CompareSwap.model, hxap, hxbp]

theorem not_compareSwap_exactExchange : ¬ CompareSwap.ExactExchange.Goal := by
  intro h
  have hg := h (1, 2) (1, 2) (fun x => rfl)
  simp [CompareSwap.model] at hg

theorem not_compareSwap_newestOnly : ¬ CompareSwap.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 1⟩ 1 2 (1, 2) (2, 3) (by simp) (by simp)
  simp [CompareSwap.model, replay, logRun] at hh

theorem compareSwap_noStamp : CompareSwap.NoStamp.Goal := by
  intro a x
  by_cases hx : x == a.1
  · have hxp : x = a.1 := by simpa using hx
    simp [CompareSwap.model, hxp]
  · have hxn : x ≠ a.1 := by intro he; exact hx (by simp [he])
    simp [CompareSwap.model, hxn]

theorem compareSwap_restartInside : CompareSwap.RestartInside.Goal :=
  restartInside_of_fusion_noStamp CompareSwap.model compareSwap_fusion compareSwap_noStamp

theorem compareSwap_suffixRetry : CompareSwap.SuffixRetry.Goal := suffixRetry_all CompareSwap.model

theorem compareSwap_parallelFold : CompareSwap.ParallelFold.Goal :=
  parallelFold_of_fusion CompareSwap.model compareSwap_fusion

theorem compareSwap_skipped : CompareSwap.Skipped.Goal := by
  intro a x h
  simp [CompareSwap.model, h]

theorem compareSwap_source : CompareSwap.Source.Goal := by
  intro a x h
  by_cases hx : x == a.1
  · simpa using hx
  · have hxn : x ≠ a.1 := by intro he; exact hx (by simp [he])
    exfalso
    apply h
    simp [CompareSwap.model, hxn]

/-! -------------------------------- Modulo --------------------------------- -/

theorem modulo_fusion : Modulo.Fusion.Goal := by
  intro a b c x hf
  simp [Modulo.model] at hf
  rcases hf with ⟨hdiv, rfl⟩
  have hdvd : b.1 + 1 ∣ a.1 + 1 := (Nat.dvd_iff_mod_eq_zero).2 hdiv
  change x % (b.1 + 1) = (x % (a.1 + 1)) % (b.1 + 1)
  exact (Nat.mod_mod_of_dvd x hdvd).symm

theorem modulo_restartInside : Modulo.RestartInside.Goal := by
  exact restartInside_of_fuse_eq Modulo.model (by
    intro a b c hf x
    simp [Modulo.model] at hf
    rcases hf with ⟨_, rfl⟩
    rfl)

theorem modulo_exchangeGuard : Modulo.ExchangeGuard.Goal := by
  intro a b x h
  simp [Modulo.model] at h ⊢
  by_cases hab : a.1 ≤ b.1
  · have hmax : max a.1 b.1 = b.1 := Nat.max_eq_right hab
    have hmin : min a.1 b.1 = a.1 := Nat.min_eq_left hab
    rw [hmax, hmin] at h
    have hdvd : a.1 + 1 ∣ b.1 + 1 := (Nat.dvd_iff_mod_eq_zero).2 h
    have hlt : x % (a.1 + 1) < b.1 + 1 := by
      have := Nat.mod_lt x (by omega : 0 < a.1 + 1)
      omega
    rw [Nat.mod_eq_of_lt hlt]
    rw [Nat.mod_mod_of_dvd x hdvd]
  · have hba : b.1 ≤ a.1 := by omega
    have hmax : max a.1 b.1 = a.1 := Nat.max_eq_left hba
    have hmin : min a.1 b.1 = b.1 := Nat.min_eq_right hba
    rw [hmax, hmin] at h
    have hdvd : b.1 + 1 ∣ a.1 + 1 := (Nat.dvd_iff_mod_eq_zero).2 h
    have hlt : x % (b.1 + 1) < a.1 + 1 := by
      have := Nat.mod_lt x (by omega : 0 < b.1 + 1)
      omega
    rw [Nat.mod_mod_of_dvd x hdvd]
    rw [Nat.mod_eq_of_lt hlt]

theorem modulo_exactExchange : Modulo.ExactExchange.Goal := by
  intro a b h
  simp [Modulo.model]
  by_cases hab : a.1 ≤ b.1
  · rw [Nat.max_eq_right hab, Nat.min_eq_left hab]
    have hb := h (b.1 + 1)
    simp [Modulo.model] at hb
    have hlt : (b.1 + 1) % (a.1 + 1) < b.1 + 1 := by
      have := Nat.mod_lt (b.1 + 1) (by omega : 0 < a.1 + 1)
      omega
    rw [Nat.mod_eq_of_lt hlt] at hb
    exact hb
  · have hba : b.1 ≤ a.1 := by omega
    rw [Nat.max_eq_left hba, Nat.min_eq_right hba]
    have ha := h (a.1 + 1)
    simp [Modulo.model] at ha
    have hlt : (a.1 + 1) % (b.1 + 1) < a.1 + 1 := by
      have := Nat.mod_lt (a.1 + 1) (by omega : 0 < b.1 + 1)
      omega
    rw [Nat.mod_eq_of_lt hlt] at ha
    exact ha.symm

theorem not_modulo_newestOnly : ¬ Modulo.NewestOnly.Goal := by
  intro h
  have hh := h ⟨0, 3⟩ 1 2 (2, 0) (1, 0) (by simp) (by simp)
  simp [Modulo.model, replay, logRun] at hh

theorem modulo_noStamp : Modulo.NoStamp.Goal := by
  intro a x
  simp [Modulo.model]

theorem modulo_suffixRetry : Modulo.SuffixRetry.Goal := suffixRetry_all Modulo.model

theorem modulo_parallelFold : Modulo.ParallelFold.Goal :=
  parallelFold_of_fusion Modulo.model modulo_fusion

theorem modulo_fitsExtent : Modulo.FitsExtent.Goal := by
  intro a x
  simp [Modulo.model]
  have := Nat.mod_lt x (by omega : 0 < a.1 + 1)
  omega

theorem modulo_period : Modulo.Period.Goal := by
  intro a x
  simp [Modulo.model, Nat.add_assoc]

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common
