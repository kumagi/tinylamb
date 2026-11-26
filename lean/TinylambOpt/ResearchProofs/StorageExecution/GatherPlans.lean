import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

namespace TinylambOpt.ResearchProofs.StorageExecution.GatherPlans

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

-- ── Contiguous model-specific ──

theorem sound_Contiguous_BridgeGap : Contiguous.BridgeGap.Goal := by
  intro fee x y hxy
  simp only [Contiguous.price, span]
  omega

-- ── MirrorZones model-specific ──

theorem sound_MirrorZones_BoundaryTax : MirrorZones.BoundaryTax.Goal := by
  intro z h a b _hab
  simp only [MirrorZones.price, span]
  omega

theorem sound_MirrorZones_JumpUnprofitable : MirrorZones.JumpUnprofitable.Goal := by
  intro z fee x y hxy hfee hzone
  simp only [MirrorZones.price, span]
  have hk : 1 ≤ y / (z + 1) - x / (z + 1) := by omega
  have hspan : 1 ≤ y - x := by omega
  have hmul : 2 ≤ fee * (y / (z + 1) - x / (z + 1)) := by
    have h2 : 2 ≤ fee := by omega
    calc
      2 = 2 * 1 := by omega
      _ ≤ fee * (y / (z + 1) - x / (z + 1)) := Nat.mul_le_mul h2 hk
  have hx : x / (z + 1) - x / (z + 1) = 0 := by omega
  have hy : y / (z + 1) - y / (z + 1) = 0 := by omega
  simp only [hx, hy]
  omega

-- ── CodecRestarts model-specific ──

theorem sound_CodecRestarts_SharedRestart : CodecRestarts.SharedRestart.Goal := by
  intro stride fee x y _hxy hdiv
  simp only [CodecRestarts.price, hdiv]

theorem sound_CodecRestarts_PointDecode : CodecRestarts.PointDecode.Goal := by
  intro k fee x
  simp only [CodecRestarts.price]
  have hmod := Nat.mod_add_div x (k + 1)
  have hle : x / (k + 1) * (k + 1) ≤ x := Nat.div_mul_le_self x (k + 1)
  have hcomm : x / (k + 1) * (k + 1) = (k + 1) * (x / (k + 1)) := Nat.mul_comm _ _
  omega

-- ── TwoTier model-specific ──

theorem sound_TwoTier_HotTail : TwoTier.HotTail.Goal := by
  intro p c a b hpa hab
  simp only [TwoTier.price, span]
  have h1 : min a p = p := by omega
  have h2 : min (b + 1) p = p := by omega
  simp [h1, h2]

theorem sound_TwoTier_SplitAtTemperature : TwoTier.SplitAtTemperature.Goal := by
  intro pivot cold a b hap hpb
  simp only [TwoTier.price, span]
  have ha : min a pivot = a := by omega
  have hp1 : min (pivot - 1 + 1) pivot = pivot := by omega
  have hp1' : min pivot pivot = pivot := by omega
  have hp2 : min (b + 1) pivot = pivot := by omega
  have hsub : pivot - 1 + 1 - a = pivot - a := by omega
  simp only [ha, hp1, hp1', hp2, hsub, Nat.sub_self, Nat.mul_zero, Nat.add_zero]
  omega

-- ── ObjectRanges model-specific ──

theorem sound_ObjectRanges_MinimumBill : ObjectRanges.MinimumBill.Goal := by
  intro u f a b _hab hspan
  simp only [ObjectRanges.price]
  unfold align
  have h1 : span a b + (u + 1) - 1 = span a b + u := by omega
  rw [h1]
  have hpos : 1 ≤ span a b := by simp [span]; omega
  have hdiv : (span a b + u) / (u + 1) = 1 := by
    apply Nat.le_antisymm
    · have hle : span a b + u < 2 * (u + 1) := by omega
      have hlt := (Nat.div_lt_iff_lt_mul (show 0 < u + 1 by omega)).mpr hle
      omega
    · have hge : u + 1 ≤ span a b + u := by omega
      exact (Nat.le_div_iff_mul_le (show 0 < u + 1 by omega)).mpr (by simpa using hge)
  simp [hdiv]
  omega

theorem sound_ObjectRanges_CoalescePaidTail : ObjectRanges.CoalescePaidTail.Goal := by
  intro u f a b c _hab _hbc hdiv
  simp only [ObjectRanges.price, align]
  have h1 : span a c + (u + 1) - 1 = span a c + u := by omega
  have h2 : span a b + (u + 1) - 1 = span a b + u := by omega
  rw [h1, h2, hdiv]

-- ── StripedWorkers model-specific ──

theorem sound_StripedWorkers_ActivationCeiling : StripedWorkers.ActivationCeiling.Goal := by
  intro stripe workers a b _hab
  simp only [StripedWorkers.price]
  exact Nat.add_le_add_left (Nat.min_le_left _ _) _

theorem sound_StripedWorkers_AllWorkers : StripedWorkers.AllWorkers.Goal := by
  intro k w a b hw
  simp only [StripedWorkers.price]
  have hmin : min (w + 1) (pages (k + 1) a b) = w + 1 := Nat.min_eq_left hw
  omega

-- ── DictionaryFault model-specific ──

theorem sound_DictionaryFault_WithinDictionary : DictionaryFault.WithinDictionary.Goal := by
  intro g f a b _hab hdiv
  simp [DictionaryFault.price, pages, hdiv]

theorem sound_DictionaryFault_ColdBucketGap : DictionaryFault.ColdBucketGap.Goal := by
  intro g f a b c hab hbc hdiv
  have hbc' : b ≤ c := Nat.le_of_lt hbc
  have hpages : pages (g + 1) a c = pages (g + 1) a b := by
    simp [pages, hdiv]
  simp only [DictionaryFault.price, hpages, span]
  have hcancel : (c + 1 - a + f * pages (g + 1) a b) - (b + 1 - a + f * pages (g + 1) a b) =
      (c + 1 - a) - (b + 1 - a) := by
    rw [Nat.add_sub_add_right]
  rw [hcancel]
  omega

-- ── TieredPrefetch model-specific ──

theorem sound_TieredPrefetch_FirstOverflow : TieredPrefetch.FirstOverflow.Goal := by
  intro lim fee x
  simp only [TieredPrefetch.price, span]
  have hs : x + lim + 1 - x = lim + 1 := by omega
  simp only [hs]
  simp only [show lim < lim + 1 from by omega, ite_true]
  omega

theorem sound_TieredPrefetch_AvoidSpill : TieredPrefetch.AvoidSpill.Goal := by
  intro lim fee a n hn hnlim hlim2n
  simp only [TieredPrefetch.price, span]
  have hspan1 : a + n - 1 + 1 - a = n := by omega
  have hspan2 : a + 2 * n - 1 + 1 - (a + n) = n := by omega
  have hspan3 : a + 2 * n - 1 + 1 - a = 2 * n := by omega
  simp only [hspan1, hspan2, hspan3]
  have h1 : ¬ (lim < n) := by omega
  have h2 : lim < 2 * n := hlim2n
  simp [h1, h2]
  omega

-- ── QuadraticDecode model-specific ──

theorem sound_QuadraticDecode_EqualLengthSplit : QuadraticDecode.EqualLengthSplit.Goal := by
  intro fee a n _hn
  simp only [QuadraticDecode.price, span]
  have hs1 : a + 2 * n - 1 + 1 - a = 2 * n := by omega
  have hs2 : a + n - 1 + 1 - a = n := by omega
  have hs3 : a + 2 * n - 1 + 1 - (a + n) = n := by omega
  rw [hs1, hs2, hs3]
  have hsq : (2 * n) ^ 2 = 4 * (n * n) := by
    simp only [Nat.pow_two]; ac_rfl
  have hsq2 : n ^ 2 = n * n := by simp [Nat.pow_two]
  rw [hsq, hsq2]
  constructor <;> intro h <;> omega

theorem sound_QuadraticDecode_MergeNeighbors : QuadraticDecode.MergeNeighbors.Goal := by
  intro f a m b ham hmb
  simp only [QuadraticDecode.price, span]
  have hsab : b + 1 - a = (m + 1 - a) + (b - m) := by omega
  have hsmb : b + 1 - (m + 1) = b - m := by omega
  rw [hsab, hsmb]
  let x := m + 1 - a
  let y := b - m
  show f + (x + y) ^ 2 ≤ f + x ^ 2 + (f + y ^ 2) ↔ 2 * x * y ≤ f
  have hsq : (x + y) ^ 2 = x ^ 2 + y ^ 2 + 2 * (x * y) := by
    simp only [Nat.pow_two, Nat.add_mul, Nat.mul_add]
    rw [Nat.mul_comm y x]
    omega
  simp only [hsq, Nat.pow_two, Nat.mul_assoc]
  constructor <;> intro h <;> omega

-- ── Generic: LatencyCap ──

theorem latencyCap_any (f : Price) : latencyCap f := by
  intro xs a b n hab hbest
  unfold bestCapped at hbest ⊢
  have hsub : ∀ ps, ps ∈ (covers (points xs)).filter (fun ps => ps.all (fun (ab : Nat × Nat) => decide (span ab.1 ab.2 ≤ a))) →
      ps ∈ (covers (points xs)).filter (fun ps => ps.all (fun (ab : Nat × Nat) => decide (span ab.1 ab.2 ≤ b))) := by
    intro ps hps
    simp only [List.mem_filter] at hps ⊢
    refine ⟨hps.1, ?_⟩
    have hall := hps.2
    simp only [List.all_eq_true] at hall ⊢
    intro p hp
    have := hall p hp
    simp only [decide_eq_true_eq] at this ⊢
    exact Nat.le_trans this hab
  obtain ⟨hmem_n, hmin_n⟩ := List.min?_eq_some_iff.mp hbest
  have hmem' : n ∈ ((covers (points xs)).filter (fun ps => ps.all (fun (ab : Nat × Nat) => decide (span ab.1 ab.2 ≤ b)))).map (cost f) := by
    rw [List.mem_map] at hmem_n ⊢
    obtain ⟨ps, hps, rfl⟩ := hmem_n
    exact ⟨ps, hsub ps hps, rfl⟩
  cases hm : (((covers (points xs)).filter (fun ps => ps.all (fun (ab : Nat × Nat) => decide (span ab.1 ab.2 ≤ b)))).map (cost f)).min? with
  | none =>
    rw [List.min?_eq_none_iff] at hm
    simp [hm] at hmem'
  | some m =>
    refine ⟨m, ?_, rfl⟩
    obtain ⟨_, hmin_m⟩ := List.min?_eq_some_iff.mp hm
    exact hmin_m n hmem'

theorem sound_Contiguous_LatencyCap : Contiguous.LatencyCap.Goal := fun fee => latencyCap_any (Contiguous.price fee)
theorem sound_Sector_LatencyCap : Sector.LatencyCap.Goal := fun s f => latencyCap_any (Sector.price s f)
theorem sound_MirrorZones_LatencyCap : MirrorZones.LatencyCap.Goal := fun z p => latencyCap_any (MirrorZones.price z p)
theorem sound_CodecRestarts_LatencyCap : CodecRestarts.LatencyCap.Goal := fun r s => latencyCap_any (CodecRestarts.price r s)
theorem sound_TwoTier_LatencyCap : TwoTier.LatencyCap.Goal := fun p c => latencyCap_any (TwoTier.price p c)
theorem sound_ObjectRanges_LatencyCap : ObjectRanges.LatencyCap.Goal := fun u f => latencyCap_any (ObjectRanges.price u f)
theorem sound_StripedWorkers_LatencyCap : StripedWorkers.LatencyCap.Goal := fun s w => latencyCap_any (StripedWorkers.price s w)
theorem sound_QuadraticDecode_LatencyCap : QuadraticDecode.LatencyCap.Goal := fun f => latencyCap_any (QuadraticDecode.price f)
theorem sound_DictionaryFault_LatencyCap : DictionaryFault.LatencyCap.Goal := fun g f => latencyCap_any (DictionaryFault.price g f)
theorem sound_TieredPrefetch_LatencyCap : TieredPrefetch.LatencyCap.Goal := fun l f => latencyCap_any (TieredPrefetch.price l f)


-- ── Generic helpers for covers/choose ──

private theorem pairwise_head?_le {α : Type} {le : α → α → Bool}
    (hrefl : ∀ x, le x x = true)
    {l : List α} {a b : α} (hpw : l.Pairwise (fun x y => le x y = true))
    (hhead : l.head? = some a) (hmem : b ∈ l) : le a b = true := by
  cases l with
  | nil => simp at hmem
  | cons x xs =>
    simp at hhead
    subst hhead
    rw [List.pairwise_cons] at hpw
    rcases List.mem_cons.mp hmem with rfl | hmem
    · exact hrefl _
    · exact hpw.1 b hmem

private theorem choose_mem_covers (f : Price) (xs : List Nat) (hne : covers (points xs) ≠ []) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  have hperm := List.mergeSort_perm (covers (points xs)) (fun a b => decide (cost f a ≤ cost f b))
  have hne' : (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) ≠ [] := by
    intro h
    have := hperm.symm.length_eq
    rw [h] at this
    simp at this
    exact hne this
  cases h : (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) with
  | nil => exact absurd h hne'
  | cons hd tl =>
    have hmem : hd ∈ (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) := by
      simp [h]
    rw [List.mem_mergeSort] at hmem
    simpa [h] using hmem

private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) :
    cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hpair : List.Pairwise (fun a b => le a b = true)
      ((covers (points xs)).mergeSort le) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total (cost f a) (cost f b)
  have hmem' : ps ∈ (covers (points xs)).mergeSort le := List.mem_mergeSort.mpr hmem
  cases h : (covers (points xs)).mergeSort le with
  | nil =>
    simp [h] at hmem'
  | cons hd tl =>
    simp only [h, List.head?_cons, Option.getD_some]
    have hle := pairwise_head?_le (fun x => by simp [le]) hpair (show ((covers (points xs)).mergeSort le).head? = some hd by simp [h]) hmem'
    simp only [le, decide_eq_true_eq] at hle
    exact hle

private theorem covers_nonempty : (xs : List Nat) → covers xs ≠ []
  | [] => by simp [covers]
  | x :: xs => by
    have ih := covers_nonempty xs
    simp only [covers, ne_eq]
    intro h
    cases hxs : covers xs with
    | nil => exact ih hxs
    | cons p ps =>
      have : (match p with | [] => [[(x,x)]] | (_,b)::rest => [(x,x)::p,(x,b)::rest]) = [] := by
        have hall : ∀ ps' ∈ p :: ps, (match ps' with | [] => [[(x,x)]] | (_,b)::rest => [(x,x)::ps',(x,b)::rest]) = [] := by
          simp only [List.flatMap_eq_nil_iff] at h
          rw [hxs] at h
          exact h
        exact hall p (by simp)
      cases p with
      | nil => simp at this
      | cons hd tl => simp at this

-- ── Generic: covers membership helpers ──

private theorem covers_singleton_mem (xs : List Nat) :
    xs.map (fun k => (k, k)) ∈ covers xs := by
  induction xs with
  | nil => simp [covers]
  | cons x xs ih =>
    simp only [covers, List.mem_flatMap]
    refine ⟨xs.map (fun k => (k, k)), ih, ?_⟩
    cases hxs : xs.map (fun k => (k, k)) with
    | nil =>
      simp [hxs]
    | cons hd tl =>
      simp [hxs]

private theorem covers_full_mem : (xs : List Nat) → xs ≠ [] →
    [(xs.head?.getD 0, xs.getLast?.getD 0)] ∈ covers xs
  | [], h => by cases h rfl
  | [x], _ => by simp [covers]
  | x :: y :: ys, _ => by
    have ih := covers_full_mem (y :: ys) (by simp)
    rw [covers]
    simp only [List.mem_flatMap]
    refine ⟨[((y :: ys).head?.getD 0, (y :: ys).getLast?.getD 0)], ⟨ih, ?_⟩⟩
    simp [List.head?, List.getLast?, List.getLast?_cons_cons]

-- ── Generic: SparseOrDense ──

theorem sparseOrDense_any (f : Price) : sparseOrDense f := by
  intro xs hne
  simp only [sparseOrDense] at *
  let ks := points xs
  have hks_ne : ks ≠ [] := by
    intro h
    apply hne
    have hperm : (List.eraseDups xs).mergeSort.Perm (List.eraseDups xs) := List.mergeSort_perm _ _
    have hempty : List.eraseDups xs = [] := by
      have hlen := hperm.symm.length_eq
      have h' : (List.eraseDups xs).mergeSort = [] := by simpa [ks, points] using h
      rw [h'] at hlen
      exact List.eq_nil_of_length_eq_zero (by simpa using hlen)
    exact List.eq_nil_iff_forall_not_mem.mpr (fun a ha => by
      have := List.mem_eraseDups.mpr ha; simp [hempty] at this)
  change cost f (choose f xs) ≤ min ((ks.map (fun k => f k k)).sum) (f (ks.head?.getD 0) (ks.getLast?.getD 0))
  apply Nat.le_min.mpr
  constructor
  · have hmem : ks.map (fun k => (k, k)) ∈ covers ks := covers_singleton_mem ks
    have hle := choose_le f xs (ks.map (fun k => (k, k))) hmem
    simp only [cost, List.map_map, Function.comp] at hle ⊢
    exact hle
  · have hmem : [(ks.head?.getD 0, ks.getLast?.getD 0)] ∈ covers ks := covers_full_mem ks hks_ne
    have hle := choose_le f xs [(ks.head?.getD 0, ks.getLast?.getD 0)] hmem
    simp only [cost, List.map_map, Function.comp] at hle ⊢
    exact hle

theorem sound_Contiguous_SparseOrDense : Contiguous.SparseOrDense.Goal := fun fee => sparseOrDense_any (Contiguous.price fee)
theorem sound_Sector_SparseOrDense : Sector.SparseOrDense.Goal := fun s f => sparseOrDense_any (Sector.price s f)
theorem sound_MirrorZones_SparseOrDense : MirrorZones.SparseOrDense.Goal := fun z p => sparseOrDense_any (MirrorZones.price z p)
theorem sound_CodecRestarts_SparseOrDense : CodecRestarts.SparseOrDense.Goal := fun r s => sparseOrDense_any (CodecRestarts.price r s)
theorem sound_TwoTier_SparseOrDense : TwoTier.SparseOrDense.Goal := fun p c => sparseOrDense_any (TwoTier.price p c)
theorem sound_ObjectRanges_SparseOrDense : ObjectRanges.SparseOrDense.Goal := fun u f => sparseOrDense_any (ObjectRanges.price u f)
theorem sound_StripedWorkers_SparseOrDense : StripedWorkers.SparseOrDense.Goal := fun s w => sparseOrDense_any (StripedWorkers.price s w)
theorem sound_QuadraticDecode_SparseOrDense : QuadraticDecode.SparseOrDense.Goal := fun f => sparseOrDense_any (QuadraticDecode.price f)
theorem sound_DictionaryFault_SparseOrDense : DictionaryFault.SparseOrDense.Goal := fun g f => sparseOrDense_any (DictionaryFault.price g f)
theorem sound_TieredPrefetch_SparseOrDense : TieredPrefetch.SparseOrDense.Goal := fun l f => sparseOrDense_any (TieredPrefetch.price l f)


-- ── Generic: TwoRequests ──

theorem twoRequests_any (f : Price) : twoRequests f := by
  intro x y hxy
  have hxy' : ¬ y ≤ x := by omega
  have hne : (x == y) = false := beq_eq_false_iff_ne.mpr (by omega)
  have hne' : (y == x) = false := beq_eq_false_iff_ne.mpr (by omega)
  have hpts : points [x, y] = [x, y] := by
    simp [points, List.eraseDups, List.eraseDupsBy, List.eraseDupsBy.loop, hne, hne', List.mergeSort, List.merge, hxy', Nat.le_of_lt hxy]
  have hcov : covers [x, y] = [[(x, x), (y, y)], [(x, y)]] := by
    simp [covers]
  simp only [twoRequests, hpts, hcov, choose, cost, List.map, List.sum_cons, List.sum_nil, Nat.add_zero]
  by_cases hle : f x x + f y y ≤ f x y
  · have h1 : (List.mergeSort [[(x,x),(y,y)], [(x,y)]] (fun a b => decide ((a.map (fun p => f p.1 p.2)).sum ≤ (b.map (fun p => f p.1 p.2)).sum))) =
        [[(x,x),(y,y)], [(x,y)]] := by
      simp [List.mergeSort, hle]
    simp [hle]
  · have hgt : f x y ≤ f x x + f y y := Nat.le_of_lt (Nat.not_le.mp hle)
    have h1 : (List.mergeSort [[(x,x),(y,y)], [(x,y)]] (fun a b => decide ((a.map (fun p => f p.1 p.2)).sum ≤ (b.map (fun p => f p.1 p.2)).sum))) =
        [[(x,y)], [(x,x),(y,y)]] := by
      simp [List.mergeSort, hle]
    simp [hle, Nat.min_eq_right hgt]

theorem sound_Contiguous_TwoRequests : Contiguous.TwoRequests.Goal := fun fee => twoRequests_any (Contiguous.price fee)
theorem sound_Sector_TwoRequests : Sector.TwoRequests.Goal := fun s f => twoRequests_any (Sector.price s f)
theorem sound_MirrorZones_TwoRequests : MirrorZones.TwoRequests.Goal := fun z p => twoRequests_any (MirrorZones.price z p)
theorem sound_CodecRestarts_TwoRequests : CodecRestarts.TwoRequests.Goal := fun r s => twoRequests_any (CodecRestarts.price r s)
theorem sound_TwoTier_TwoRequests : TwoTier.TwoRequests.Goal := fun p c => twoRequests_any (TwoTier.price p c)
theorem sound_ObjectRanges_TwoRequests : ObjectRanges.TwoRequests.Goal := fun u f => twoRequests_any (ObjectRanges.price u f)
theorem sound_StripedWorkers_TwoRequests : StripedWorkers.TwoRequests.Goal := fun s w => twoRequests_any (StripedWorkers.price s w)
theorem sound_QuadraticDecode_TwoRequests : QuadraticDecode.TwoRequests.Goal := fun f => twoRequests_any (QuadraticDecode.price f)
theorem sound_DictionaryFault_TwoRequests : DictionaryFault.TwoRequests.Goal := fun g f => twoRequests_any (DictionaryFault.price g f)
theorem sound_TieredPrefetch_TwoRequests : TieredPrefetch.TwoRequests.Goal := fun l f => twoRequests_any (TieredPrefetch.price l f)


-- ── helpers for Sector.OneSector ──

private theorem covers_no_empty (xs : List Nat) (hxs : xs ≠ []) (ps : List Extent) (hps : ps ∈ covers xs) : ps ≠ [] := by
  induction xs generalizing ps with
  | nil => exact absurd rfl hxs
  | cons y ys ih =>
    simp only [covers, List.mem_flatMap] at hps
    obtain ⟨qs, hqs, hmem⟩ := hps
    cases qs with
    | nil =>
      simp at hmem
      subst hmem
      simp
    | cons hd tl =>
      simp at hmem
      rcases hmem with rfl | rfl <;> simp


private theorem sector_extent_ge (k fee a b : Nat) : fee + 1 ≤ Sector.price k fee a b := by
  simp only [Sector.price, pages]
  omega

private theorem cost_nonempty_ge (f : Price) (bound : Nat) (hbound : ∀ a b, bound ≤ f a b)
    (ps : List Extent) (hne : ps ≠ []) : bound ≤ cost f ps := by
  cases ps with
  | nil => exact absurd rfl hne
  | cons p ps =>
    simp only [cost, List.map, List.sum_cons]
    have := hbound p.1 p.2
    omega

theorem sound_Sector_OneSector : Sector.OneSector.Goal := by
  intro k fee xs hne hall
  let ks := points xs
  have hks_ne : ks ≠ [] := by
    intro h
    apply hne
    have hperm : (List.eraseDups xs).mergeSort.Perm (List.eraseDups xs) := List.mergeSort_perm _ _
    have hempty : List.eraseDups xs = [] := by
      have hlen := hperm.symm.length_eq
      have h' : (List.eraseDups xs).mergeSort = [] := by simpa [ks, points] using h
      rw [h'] at hlen
      exact List.eq_nil_of_length_eq_zero (by simpa using hlen)
    exact List.eq_nil_iff_forall_not_mem.mpr (fun a ha => by
      have := List.mem_eraseDups.mpr ha; simp [hempty] at this)
  -- upper bound
  have hfull_mem : [(ks.head?.getD 0, ks.getLast?.getD 0)] ∈ covers ks := covers_full_mem ks hks_ne
  have hle := choose_le (Sector.price k fee) xs [(ks.head?.getD 0, ks.getLast?.getD 0)] hfull_mem
  have hcost_full : cost (Sector.price k fee) [(ks.head?.getD 0, ks.getLast?.getD 0)] = fee + 1 := by
    simp only [cost, List.map, List.sum_cons, List.sum_nil, Nat.add_zero, Sector.price]
    simp only [pages]
    have hmem_ks_xs : ∀ a, a ∈ ks → a ∈ xs := by
      intro a ha
      have : a ∈ (List.eraseDups xs).mergeSort := by simpa [points, ks] using ha
      exact List.mem_eraseDups.mp (List.mem_mergeSort.mp this)
    have hhead_xs : ks.head?.getD 0 ∈ xs := hmem_ks_xs _ (by
      cases hks : ks with
      | nil => exact absurd hks hks_ne
      | cons a as => simp [hks])
    have hlast_xs : ks.getLast?.getD 0 ∈ xs := hmem_ks_xs _ (by
      cases hks : ks with
      | nil => exact absurd hks hks_ne
      | cons a as =>
        cases as with
        | nil => simp [List.getLast?]
        | cons b bs => simp [List.getLast?, List.getLast?_cons])
    have hsec : (ks.getLast?.getD 0) / 2 ^ k = (ks.head?.getD 0) / 2 ^ k := by
      rw [hall (ks.getLast?.getD 0) hlast_xs, hall (ks.head?.getD 0) hhead_xs]
    simp [hsec]
  -- lower bound
  have hge : fee + 1 ≤ cost (Sector.price k fee) (choose (Sector.price k fee) xs) := by
    have hne_cov : covers ks ≠ [] := covers_nonempty ks
    have hmem := choose_mem_covers (Sector.price k fee) xs hne_cov
    have hne_ps : choose (Sector.price k fee) xs ≠ [] := covers_no_empty ks hks_ne _ hmem
    exact cost_nonempty_ge _ _ (fun a b => sector_extent_ge k fee a b) _ hne_ps
  have hle' : cost (Sector.price k fee) (choose (Sector.price k fee) xs) ≤ fee + 1 := by
    calc cost (Sector.price k fee) (choose (Sector.price k fee) xs)
        ≤ cost (Sector.price k fee) [(ks.head?.getD 0, ks.getLast?.getD 0)] := hle
      _ = fee + 1 := hcost_full
  omega
