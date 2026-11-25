import TinylambOpt.ResearchGoals.Compression.Partition

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.C_Partition

open TinylambOpt.ResearchGoals.Compression.Partition
open TinylambOpt.ResearchGoals.Compression.Wire

/-!
Per-goal notes (all seven goals are TRUE; proofs ported from the sibling
proof development `ResearchProofs.Compression.Partition`, which already
contains covering theorems for every leaf below):

* `ShortestPartition.Goal`: `dp` with fuel = xs.length explores every split
  point; strong induction on length shows each subproblem has enough fuel,
  and the candidate costs over `splits xs` and over first-page prefixes are
  shown to have the same minimum (`best_first`, `best_achieved`,
  `splits_decompose`, `splits_of_prefix`).
* `SeekBudget.Goal`: enlarging the page cap `a ≤ b` only grows the filtered
  candidate list, so the old minimizer `n` remains a member; the new `min?`
  is `≤ n` and still `some`.
* `MixedModes.Goal`: `tagged modes p ≤ 1 + f p` pointwise since `f ∈ modes`,
  so `bytes` is monotone and `best_le_bytes` on the `f`-optimal split gives
  the inequality.
* `HeaderSensitivity.Goal`: both estimates are `≤`-minimal for their own
  cost; writing `high = (high-low)+low` and comparing the two byte sums
  forces `ph.length ≤ pl.length`.
* `EstimationRegret.Goal`: per-page `natAbs` error `≤ e` gives
  `bytes actual sel ≤ bytes approx sel + e·|sel|` and
  `bytes approx opt ≤ bytes actual opt + e·|opt|`; `sel` is approx-minimal,
  so the regret is `≤ e·(|sel|+|opt|)`.
* `DirectoryWidthSearch.Goal`: `commonWidth ps ≤ width xs.length` for every
  split (each page length `≤ xs.length`), so the `w = width xs.length`
  bucket contains every candidate and its value equals `indexed`; values
  for smaller `w` only replace `commonWidth` by a smaller `w`, giving
  equal minima.
* `CappedAppend.Goal`: capped optima `pa`, `pb` concatenate to a capped
  split of `a++b` (`valid_append`, `capped_append`,
  `complete_boundaries`), so `boundedBest` of the concatenation is `≤ ca+cb`
  and is attained (`boundedBest_achieved`).
-/

private theorem bytes_append (cost : Cost) (pa pb : List (List Nat)) :
    bytes cost (pa ++ pb) = bytes cost pa + bytes cost pb := by
  simp [bytes]

private theorem splits_ne_nil : ∀ xs : List Nat, splits xs ≠ [] := by
  intro xs
  induction xs with
  | nil => simp [splits]
  | cons x xs ih =>
      rcases List.exists_mem_of_ne_nil (splits xs) ih with ⟨p, hp⟩
      unfold splits
      apply List.ne_nil_of_mem (a := match p with | [] => [[x]] | f::r => (x::f)::r)
      rw [List.mem_flatMap]
      refine ⟨p, hp, ?_⟩
      cases p <;> simp

private theorem mem_splits_of_valid : ∀ ps xs, valid ps xs → ps ∈ splits xs := by
  intro ps xs
  induction xs generalizing ps with
  | nil =>
      intro h
      unfold valid at h
      have hp : ps = [] := by
        cases ps with
        | nil => rfl
        | cons p rest =>
            have hpne : p ≠ [] := h.2 p (by simp)
            have hall := (List.flatten_eq_nil_iff.mp h.1) p (by simp)
            contradiction
      subst hp
      simp [splits]
  | cons x xs ih =>
      intro h
      unfold valid at h
      have hfl : (ps.flatten) = x :: xs := h.1
      cases ps with
      | nil => simp at hfl
      | cons p rest =>
          have hpne : p ≠ [] := h.2 p (by simp)
          cases p with
          | nil => contradiction
          | cons y ys =>
              have hfl' : (y :: ys) ++ rest.flatten = x :: xs := by simpa [List.flatten_cons] using hfl
              have hy : y = x := (List.cons.inj hfl').1
              subst y
              have htail := (List.cons.inj hfl').2
              cases ys with
              | nil =>
                  have hrest : valid rest xs := by
                    unfold valid
                    constructor
                    · simpa using htail
                    · intro q hq
                      exact h.2 q (by simp [hq])
                  have hm := ih rest hrest
                  simp only [splits, List.mem_flatMap]
                  refine ⟨rest, hm, ?_⟩
                  cases rest <;> simp
              | cons z zs =>
                  have hpages : valid ((z :: zs) :: rest) xs := by
                    unfold valid
                    constructor
                    · simpa using htail
                    · intro q hq
                      simp only [List.mem_cons] at hq
                      rcases hq with rfl | hq
                      · simp
                      · exact h.2 q (by simp [hq])
                  have hm := ih ((z :: zs) :: rest) hpages
                  simp only [splits, List.mem_flatMap]
                  refine ⟨(z :: zs) :: rest, hm, ?_⟩
                  simp

private theorem valid_of_mem_splits : ∀ ps xs, ps ∈ splits xs → valid ps xs := by
  intro ps xs
  induction xs generalizing ps with
  | nil =>
      intro h
      simp [splits] at h
      subst ps
      simp [valid]
  | cons x xs ih =>
      intro h
      simp only [splits, List.mem_flatMap] at h
      rcases h with ⟨pages, hpages, hf⟩
      have hv := ih pages hpages
      unfold valid at hv ⊢
      cases pages with
      | nil =>
          have hxs : xs = [] := by simpa using hv.1.symm
          subst xs
          simp at hf
          subst ps
          simp
      | cons first rest =>
          have hfirst_ne : first ≠ [] := hv.2 first (by simp)
          simp at hf
          rcases hf with hf | hf
          · rw [hf]
            constructor
            · simpa [List.flatten_cons] using congrArg (fun l : List Nat => x :: l) hv.1
            · intro q hq
              simp only [List.mem_cons] at hq
              rcases hq with rfl | hq
              · simp
              · exact hv.2 q (by simp [hq])
          · rw [hf]
            constructor
            · simpa [List.flatten_cons] using congrArg (fun l : List Nat => x :: l) hv.1
            · intro q hq
              simp only [List.mem_cons] at hq
              rcases hq with rfl | hq
              · simp
              · rcases hq with rfl | hq
                · exact hfirst_ne
                · exact hv.2 q (by simp [hq])

private theorem complete_boundaries : ∀ ps xs, ps ∈ splits xs ↔ valid ps xs := by
  intro ps xs
  constructor
  · exact valid_of_mem_splits ps xs
  · exact mem_splits_of_valid ps xs

private theorem best_le_bytes (cost : Cost) (xs : List Nat) (ps : List (List Nat))
    (hp : ps ∈ splits xs) : best cost xs ≤ bytes cost ps := by
  unfold best
  exact List.min?_getD_le_of_mem (List.mem_map.mpr ⟨ps, hp, rfl⟩)

private theorem best_achieved (cost : Cost) (xs : List Nat) :
    ∃ ps ∈ splits xs, bytes cost ps = best cost xs := by
  unfold best
  cases h : ((splits xs).map (bytes cost)).min? with
  | none =>
      have hnil := List.min?_eq_none_iff.mp h
      have hne : (splits xs).map (bytes cost) ≠ [] := by
        rcases List.exists_mem_of_ne_nil (splits xs) (splits_ne_nil xs) with ⟨p, hp⟩
        exact List.ne_nil_of_mem (a := bytes cost p)
          (List.mem_map.mpr ⟨p, hp, rfl⟩)
      contradiction
  | some v =>
      have hmem : v ∈ (splits xs).map (bytes cost) := List.min?_mem h
      rcases List.mem_map.mp hmem with ⟨ps, hps, rfl⟩
      refine ⟨ps, hps, ?_⟩
      simp

private theorem valid_append {pa pb : List (List Nat)} {a b : List Nat}
    (ha : valid pa a) (hb : valid pb b) : valid (pa ++ pb) (a ++ b) := by
  unfold valid at ha hb ⊢
  constructor
  · simp [ha.1, hb.1]
  · intro p hp
    rw [List.mem_append] at hp
    rcases hp with hp | hp
    · exact ha.2 p hp
    · exact hb.2 p hp

private theorem tagged_le_of_mem (modes : List Cost) (f : Cost) (hf : f ∈ modes)
    (p : List Nat) : tagged modes p ≤ 1 + f p := by
  unfold tagged
  exact List.min?_getD_le_of_mem (List.mem_map.mpr ⟨f, hf, rfl⟩)

private theorem head_mergeSort_min {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c : α, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b : α, (le a b || le b a) = true)
    {l : List α} {a : α} {t : List α} (h : l.mergeSort le = a :: t) :
    ∀ b ∈ l, le a b := by
  have hs := List.pairwise_mergeSort trans total l
  rw [h] at hs
  have hhead : ∀ b ∈ t, le a b = true := (List.pairwise_cons.mp hs).1
  intro b hb
  have hb' : b ∈ l.mergeSort le := by simpa using hb
  rw [h] at hb'
  simp only [List.mem_cons] at hb'
  rcases hb' with rfl | hbt
  · simpa using (total b b)
  · exact hhead b hbt

private theorem estimate_min (cost : Cost) (xs : List Nat) (ps : List (List Nat))
    (hps : ps ∈ splits xs) : bytes cost (estimate cost xs) ≤ bytes cost ps := by
  unfold estimate
  let le : List (List Nat) → List (List Nat) → Bool := fun a b => decide (bytes cost a ≤ bytes cost b)
  have htrans : ∀ a b c : List (List Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    unfold le at hab hbc ⊢
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))
  have htotal : ∀ a b : List (List Nat), (le a b || le b a) = true := by
    intro a b
    simp only [le]
    by_cases h : bytes cost a ≤ bytes cost b
    · simp [h]
    · have h' : bytes cost b ≤ bytes cost a := Nat.le_of_not_ge h
      simp [h, h']
  cases hm : (splits xs).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (splits xs) (le := le)
      rw [hm] at hlen
      simp at hlen
      have hpos := splits_ne_nil xs
      apply False.elim
      apply hpos
      apply List.length_eq_zero_iff.mp
      omega
  | cons a t =>
      have hmin := head_mergeSort_min le htrans htotal hm
      have hp_le : le a ps := hmin ps hps
      unfold le at hp_le
      have hp_le' : bytes cost a ≤ bytes cost ps := of_decide_eq_true hp_le
      simpa [hm] using hp_le'


private theorem nil_mem_splits : ∀ ys : List Nat, ([] : List (List Nat)) ∈ splits ys → ys = [] := by
  intro ys h
  have hv := valid_of_mem_splits [] ys h
  unfold valid at hv
  simpa using hv.1.symm

private theorem splits_decompose : ∀ (xs : List Nat) (ps : List (List Nat)), xs ≠ [] → ps ∈ splits xs →
    ∃ k, 1 ≤ k ∧ k ≤ xs.length ∧ ∃ ps', ps = xs.take k :: ps' ∧ ps' ∈ splits (xs.drop k) := by
  intro xs
  induction xs with
  | nil => intro ps hne; contradiction
  | cons x xs ih =>
      intro ps hne hmem
      unfold splits at hmem
      rw [List.mem_flatMap] at hmem
      rcases hmem with ⟨pages, hpages, hf⟩
      cases pages with
      | nil =>
          have hxs : xs = [] := nil_mem_splits xs hpages
          subst xs
          simp at hf
          subst ps
          refine ⟨1, by simp, by simp, [], ?_, ?_⟩
          · simp
          · simp [splits]
      | cons first rest =>
          have hxsne : xs ≠ [] := by
            intro h
            subst xs
            simp [splits] at hpages
          rcases ih (first :: rest) hxsne hpages with ⟨k, hk1, hkle, ps', hpages_eq, hps'mem⟩
          have hfirst_eq : first = xs.take k := (List.cons.inj hpages_eq).1
          have hrest_eq : rest = ps' := (List.cons.inj hpages_eq).2
          subst first
          subst rest
          simp at hf
          rcases hf with hf | hf
          · subst ps
            refine ⟨k+1, by omega, by simpa using Nat.succ_le_succ hkle, ps', ?_, ?_⟩
            · simp
            · simpa using hps'mem
          · subst ps
            refine ⟨1, by simp, by simp, xs.take k :: ps', ?_, ?_⟩
            · simp
            · exact hpages

private theorem splits_of_prefix : ∀ (xs : List Nat) (k : Nat) (ps' : List (List Nat)),
    1 ≤ k → k ≤ xs.length → ps' ∈ splits (xs.drop k) → (xs.take k) :: ps' ∈ splits xs := by
  intro xs
  induction xs with
  | nil => intro k ps' hk1 hkle hps'; simp at hkle; omega
  | cons x xs ih =>
      intro k ps' hk1 hkle hps'
      cases k with
      | zero => omega
      | succ k' =>
          simp at hkle
          by_cases hk0 : k' = 0
          · subst k'
            unfold splits
            rw [List.mem_flatMap]
            refine ⟨ps', ?_, ?_⟩
            · simpa using hps'
            · cases ps' <;> simp
          · have hk'1 : 1 ≤ k' := by omega
            have hk'le : k' ≤ xs.length := by omega
            have hrec : (xs.take k') :: ps' ∈ splits xs := ih k' ps' hk'1 hk'le (by simpa using hps')
            unfold splits
            rw [List.mem_flatMap]
            refine ⟨(xs.take k') :: ps', hrec, ?_⟩
            simp

private theorem best_first (cost : Cost) (xs : List Nat) (hxs : xs ≠ []) :
    best cost xs = ((List.range xs.length).map
      (fun i => cost (xs.take (i+1)) + best cost (xs.drop (i+1)))).min?.getD 0 := by
  let S : List Nat := (splits xs).map (bytes cost)
  let C : List Nat := (List.range xs.length).map
      (fun i => cost (xs.take (i+1)) + best cost (xs.drop (i+1)))
  have hSne : S ≠ [] := by
    dsimp [S]
    rcases List.exists_mem_of_ne_nil (splits xs) (splits_ne_nil xs) with ⟨p, hp⟩
    exact List.ne_nil_of_mem (a := bytes cost p) (List.mem_map.mpr ⟨p, hp, rfl⟩)
  have hlenpos : 0 < xs.length := by
    cases xs with
    | nil => contradiction
    | cons a as => simp
  have hCne : C ≠ [] := by
    dsimp [C]
    exact List.ne_nil_of_mem (a := cost (xs.take (0+1)) + best cost (xs.drop (0+1)))
      (List.mem_map.mpr ⟨0, List.mem_range.mpr hlenpos, rfl⟩)
  have hmS_eq : S.min? = some (S.min?.getD 0) := by
    have hne : S.min? ≠ none := by
      intro hn
      have hnil := List.min?_eq_none_iff.mp hn
      contradiction
    cases hs : S.min? with
    | none => exact (hne hs).elim
    | some m => simp
  have hmC_eq : C.min? = some (C.min?.getD 0) := by
    have hne : C.min? ≠ none := by
      intro hn
      have hnil := List.min?_eq_none_iff.mp hn
      contradiction
    cases hs : C.min? with
    | none => exact (hne hs).elim
    | some m => simp
  have hSsome : ∃ mS, S.min? = some mS := ⟨S.min?.getD 0, hmS_eq⟩
  have hCsome : ∃ mC, C.min? = some mC := ⟨C.min?.getD 0, hmC_eq⟩
  obtain ⟨mS, hmS⟩ := hSsome
  obtain ⟨mC, hmC⟩ := hCsome
  have hmSmem : mS ∈ S := List.min?_mem hmS
  have hmCmem : mC ∈ C := List.min?_mem hmC
  have hCtoS : ∀ c ∈ C, c ∈ S := by
    intro c hc
    dsimp [C] at hc
    rcases List.mem_map.mp hc with ⟨i, hi, rfl⟩
    have hi_lt : i < xs.length := List.mem_range.mp hi
    have hk1 : 1 ≤ i+1 := by omega
    have hkle : i+1 ≤ xs.length := by omega
    rcases best_achieved cost (xs.drop (i+1)) with ⟨ps', hps', hbest⟩
    have hprefix : (xs.take (i+1)) :: ps' ∈ splits xs := splits_of_prefix xs (i+1) ps' hk1 hkle hps'
    dsimp [S]
    refine List.mem_map.mpr ⟨(xs.take (i+1)) :: ps', hprefix, ?_⟩
    simpa [bytes] using hbest
  have hStoC : ∀ s ∈ S, ∃ c ∈ C, c ≤ s := by
    intro s hs
    dsimp [S] at hs
    rcases List.mem_map.mp hs with ⟨ps, hps, rfl⟩
    rcases splits_decompose xs ps hxs hps with ⟨k, hk1, hkle, ps', hps_eq, hps'mem⟩
    subst ps
    let i := k - 1
    have hi_lt : i < xs.length := by dsimp [i]; omega
    have hc_mem : cost (xs.take (i+1)) + best cost (xs.drop (i+1)) ∈ C := by
      dsimp [C]
      exact List.mem_map.mpr ⟨i, List.mem_range.mpr hi_lt, rfl⟩
    have hi1 : i+1 = k := by dsimp [i]; omega
    refine ⟨cost (xs.take (i+1)) + best cost (xs.drop (i+1)), hc_mem, ?_⟩
    have hle := best_le_bytes cost (xs.drop k) ps' hps'mem
    simpa [bytes, hi1] using hle
  have hmCinS : mC ∈ S := hCtoS mC hmCmem
  have hmS_le_mC : mS ≤ mC := by
    have hh := List.min?_getD_le_of_mem (k := 0) hmCinS
    simpa [hmS] using hh
  rcases hStoC mS hmSmem with ⟨c, hcC, hc_le⟩
  have hmC_le_c : mC ≤ c := by
    have hh := List.min?_getD_le_of_mem (k := 0) hcC
    simpa [hmC] using hh
  have hmC_le_mS : mC ≤ mS := Nat.le_trans hmC_le_c hc_le
  have hm_eq : mS = mC := Nat.le_antisymm hmS_le_mC hmC_le_mS
  unfold best
  change S.min?.getD 0 = C.min?.getD 0
  simp [hmS, hmC, hm_eq]

private theorem dp_eq_best_of_le (cost : Cost) :
    ∀ n, ∀ (xs : List Nat) (fuel : Nat), xs.length = n → n ≤ fuel → dp cost fuel xs = best cost xs := by
  intro n
  exact Nat.strongRecOn n (fun n ih => by
      intro xs fuel hxl hfuel
      cases xs with
      | nil => cases fuel <;> simp [dp, best, splits, bytes]
      | cons x xs' =>
          simp at hxl hfuel
          have hnpos : 0 < n := by omega
          have hfuelpos : fuel ≠ 0 := by omega
          rcases Nat.exists_eq_succ_of_ne_zero hfuelpos with ⟨f, hf⟩
          subst fuel
          rw [best_first cost (x :: xs') (by simp)]
          change (if (x :: xs').isEmpty then 0 else
            ((List.range (x :: xs').length).map (fun i =>
              cost ((x :: xs').take (i+1)) + dp cost f ((x :: xs').drop (i+1)))).min?.getD 0) =
            ((List.range (x :: xs').length).map (fun i =>
              cost ((x :: xs').take (i+1)) + best cost ((x :: xs').drop (i+1)))).min?.getD 0
          simp only [List.isEmpty_cons, Bool.false_eq_true, ite_false]
          have hdrop : ∀ i ∈ List.range (x :: xs').length,
              dp cost f ((x :: xs').drop (i+1)) = best cost ((x :: xs').drop (i+1)) := by
            intro i hi
            have hi_lt : i < (x :: xs').length := List.mem_range.mp hi
            have hlen_drop : ((x :: xs').drop (i+1)).length < n := by
              rw [List.length_drop]
              have hlt : (x :: xs').length - (i + 1) < (x :: xs').length :=
                Nat.sub_lt (by simp) (Nat.succ_pos i)
              rw [← hxl]
              exact hlt
            have hle_f : ((x :: xs').drop (i+1)).length ≤ f :=
              Nat.le_of_lt_succ (Nat.lt_of_lt_of_le hlen_drop hfuel)
            exact ih ((x :: xs').drop (i+1)).length hlen_drop ((x :: xs').drop (i+1)) f rfl hle_f
          apply congrArg (fun l : List Nat => l.min?.getD 0)
          apply List.map_congr_left
          intro i hi
          exact congrArg (fun v : Nat => cost ((x :: xs').take (i+1)) + v) (hdrop i hi))

theorem ShortestPartition_sound : TinylambOpt.ResearchGoals.Compression.Partition.ShortestPartition.Goal := by
  intro cost xs
  exact dp_eq_best_of_le cost xs.length xs xs.length rfl (Nat.le_refl _)


private theorem bytes_add_const (cost : Cost) (h : Nat) (ps : List (List Nat)) :
    bytes (fun p => h + cost p) ps = bytes cost ps + h * ps.length := by
  unfold bytes
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih, Nat.mul_succ]
      omega

private theorem natAbs_sub_le_bounds (a b e : Nat)
    (h : ((a : Int) - (b : Int)).natAbs ≤ e) : a ≤ b + e ∧ b ≤ a + e := by
  omega

private theorem bytes_le_sum_err (actual approx : Cost) (e : Nat) (ps : List (List Nat))
    (h : ∀ p ∈ ps, ((actual p : Int) - approx p).natAbs ≤ e) :
    bytes actual ps ≤ bytes approx ps + e * ps.length := by
  unfold bytes
  induction ps with
  | nil => simp
  | cons p ps ih =>
      have hp := h p (by simp)
      have htail := ih (by intro q hq; exact h q (by simp [hq]))
      have hp' : actual p ≤ approx p + e := (natAbs_sub_le_bounds _ _ _ hp).1
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [Nat.mul_succ]
      omega

private theorem bytes_ge_sum_err (actual approx : Cost) (e : Nat) (ps : List (List Nat))
    (h : ∀ p ∈ ps, ((actual p : Int) - approx p).natAbs ≤ e) :
    bytes approx ps ≤ bytes actual ps + e * ps.length := by
  unfold bytes
  induction ps with
  | nil => simp
  | cons p ps ih =>
      have hp := h p (by simp)
      have htail := ih (by intro q hq; exact h q (by simp [hq]))
      have hp' : approx p ≤ actual p + e := (natAbs_sub_le_bounds _ _ _ hp).2
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [Nat.mul_succ]
      omega

private theorem estimate_mem_splits (cost : Cost) (xs : List Nat) :
    estimate cost xs ∈ splits xs := by
  unfold estimate
  let le : List (List Nat) → List (List Nat) → Bool := fun a b => decide (bytes cost a ≤ bytes cost b)
  cases hm : (splits xs).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (splits xs) (le := le)
      rw [hm] at hlen
      simp at hlen
      exact False.elim ((splits_ne_nil xs) (List.length_eq_zero_iff.mp (by omega)))
  | cons a t =>
      have hmem : a ∈ (splits xs).mergeSort le := by rw [hm]; simp
      have hmem0 : a ∈ splits xs := by simpa using hmem
      simpa [hm] using hmem0

private theorem sum_le_sum {α : Type} (f g : α → Nat) :
    ∀ l : List α, (∀ a ∈ l, f a ≤ g a) → (l.map f).sum ≤ (l.map g).sum := by
  intro l
  induction l with
  | nil => intro h; simp
  | cons a as ih =>
      intro h
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h a (by simp)) (ih (by intro b hb; exact h b (by simp [hb])))

private theorem width_mono : ∀ a b : Nat, a ≤ b → width a ≤ width b := by
  intro a b hab
  by_cases ha : a = 0
  · simp [width, ha]
  · have hb : b ≠ 0 := by omega
    simp [width, ha, hb]
    have hpow : 2 ^ a.log2 ≤ a := (Nat.le_log2 ha).mp (Nat.le_refl _)
    have hpowb : 2 ^ a.log2 ≤ b := Nat.le_trans hpow hab
    have hlog : a.log2 ≤ b.log2 := (Nat.le_log2 hb).mpr hpowb
    omega

private theorem byteCount_mono : ∀ a b : Nat, a ≤ b → byteCount a ≤ byteCount b := by
  intro a b h
  unfold byteCount
  exact Nat.div_le_div_right (Nat.add_le_add_right h 7)

private theorem page_len_le_total : ∀ (ps : List (List Nat)) p, p ∈ ps → p.length ≤ ps.flatten.length := by
  intro ps
  induction ps with
  | nil => intro p h; simp at h
  | cons q qs ih =>
      intro p hp
      rw [List.flatten_cons, List.length_append]
      simp only [List.mem_cons] at hp
      rcases hp with rfl | hp
      · omega
      · have hq := ih p hp
        omega

private theorem peak_le : ∀ (ns : List Nat) k, (∀ n ∈ ns, n ≤ k) → peak ns ≤ k := by
  intro ns
  induction ns with
  | nil => intro k h; simp [peak]
  | cons n ns ih =>
      intro k h
      unfold peak
      rw [List.max?_cons]
      cases hmax : ns.max? with
      | none =>
          have hnsnil : ns = [] := List.max?_eq_none_iff.mp hmax
          subst ns
          simp at h ⊢
          exact h
      | some m =>
          have hmle : m ≤ k := h m (by simp [List.max?_mem hmax])
          have hnle : n ≤ k := h n (by simp)
          simp
          exact Nat.max_le.mpr ⟨hnle, hmle⟩

private theorem commonWidth_le_width_length (ps : List (List Nat)) (xs : List Nat) (hps : ps ∈ splits xs) :
    commonWidth ps ≤ width xs.length := by
  have hv := valid_of_mem_splits ps xs hps
  unfold valid at hv
  unfold commonWidth
  apply width_mono
  apply peak_le
  intro n hn
  rcases List.mem_map.mp hn with ⟨p, hp_mem, rfl⟩
  have hlen := page_len_le_total ps p hp_mem
  rw [hv.1] at hlen
  exact hlen

private theorem boundedBest_achieved (cost : Cost) (n : Nat) (xs : List Nat) (v : Nat)
    (h : boundedBest cost n xs = some v) :
    ∃ ps ∈ splits xs, capped n ps ∧ bytes cost ps = v := by
  unfold boundedBest at h
  have hmem : v ∈ ((splits xs).filter (capped n)).map (bytes cost) := List.min?_mem h
  rcases List.mem_map.mp hmem with ⟨ps, hpsF, hbytes⟩
  rw [List.mem_filter] at hpsF
  exact ⟨ps, hpsF.1, hpsF.2, hbytes⟩

private theorem capped_append (n : Nat) (pa pb : List (List Nat)) :
    capped n pa → capped n pb → capped n (pa ++ pb) := by
  unfold capped
  intro ha hb
  rw [List.all_append]
  simp [ha, hb]

theorem SeekBudget_sound : TinylambOpt.ResearchGoals.Compression.Partition.SeekBudget.Goal := by
  intro cost a b xs n hab hbn
  unfold boundedBest at hbn ⊢
  let La := ((splits xs).filter (capped a)).map (bytes cost)
  let Lb := ((splits xs).filter (capped b)).map (bytes cost)
  have hminLa : La.min? = some n := by simpa [La] using hbn
  have hnmemLa : n ∈ La := List.min?_mem hminLa
  have hcap : ∀ ps ∈ splits xs, capped a ps → capped b ps := by
    intro ps hps hc
    unfold capped at hc ⊢
    have hfa : ∀ p ∈ ps, p.length ≤ a := by
      intro p hp
      exact of_decide_eq_true ((List.all_eq_true.mp hc) p hp)
    exact List.all_eq_true.mpr (by
      intro p hp
      exact decide_eq_true (Nat.le_trans (hfa p hp) hab))
  have hnmemLb : n ∈ Lb := by
    have hnmemLa' : n ∈ ((splits xs).filter (capped a)).map (bytes cost) := by
      simpa [La] using hnmemLa
    rcases List.mem_map.mp hnmemLa' with ⟨ps, hpsF, hbytes⟩
    rw [List.mem_filter] at hpsF
    dsimp [Lb]
    apply List.mem_map.mpr
    refine ⟨ps, ?_, hbytes⟩
    rw [List.mem_filter]
    exact ⟨hpsF.1, hcap ps hpsF.1 hpsF.2⟩
  have hmin_eq : Lb.min? = some (Lb.min?.getD 0) := by
    have hne : Lb.min? ≠ none := by
      intro hn
      have hnil := List.min?_eq_none_iff.mp hn
      exact (List.ne_nil_of_mem hnmemLb) hnil
    cases hm : Lb.min? with
    | none => exact (hne hm).elim
    | some m => simp
  refine ⟨Lb.min?.getD 0, ?_, ?_⟩
  · exact List.min?_getD_le_of_mem hnmemLb
  · simpa [Lb] using hmin_eq

theorem MixedModes_sound : TinylambOpt.ResearchGoals.Compression.Partition.MixedModes.Goal := by
  intro modes f xs hf
  have hpoint : ∀ p : List Nat, tagged modes p ≤ 1 + f p := tagged_le_of_mem modes f hf
  rcases best_achieved (fun p => 1 + f p) xs with ⟨ps, hps, hbest⟩
  have hle : bytes (tagged modes) ps ≤ bytes (fun p => 1 + f p) ps := by
    unfold bytes
    apply sum_le_sum
    intro p hp
    exact hpoint p
  rw [← hbest]
  exact Nat.le_trans (best_le_bytes (tagged modes) xs ps hps) hle

theorem HeaderSensitivity_sound : TinylambOpt.ResearchGoals.Compression.Partition.HeaderSensitivity.Goal := by
  intro cost xs low high hlh
  let ch : Cost := fun p => high + cost p
  let cl : Cost := fun p => low + cost p
  let ph := estimate ch xs
  let pl := estimate cl xs
  have hph : ph ∈ splits xs := by dsimp [ph]; exact estimate_mem_splits ch xs
  have hpl : pl ∈ splits xs := by dsimp [pl]; exact estimate_mem_splits cl xs
  have h1 : bytes ch ph ≤ bytes ch pl := by dsimp [ph]; exact estimate_min ch xs pl hpl
  have h2 : bytes cl pl ≤ bytes cl ph := by dsimp [pl]; exact estimate_min cl xs ph hph
  rw [bytes_add_const, bytes_add_const] at h1
  rw [bytes_add_const, bytes_add_const] at h2
  have hmul : high * ph.length + low * pl.length ≤ high * pl.length + low * ph.length := by omega
  have hhigh : high = (high - low) + low := (Nat.sub_add_cancel (Nat.le_of_lt hlh)).symm
  have hsub : high - low + low = high := Nat.sub_add_cancel (Nat.le_of_lt hlh)
  have hmul_high_ph : high * ph.length = (high - low) * ph.length + low * ph.length := by
    calc
      high * ph.length = ((high - low) + low) * ph.length := by rw [hsub]
      _ = (high - low) * ph.length + low * ph.length := Nat.add_mul _ _ _
  have hmul_high_pl : high * pl.length = (high - low) * pl.length + low * pl.length := by
    calc
      high * pl.length = ((high - low) + low) * pl.length := by rw [hsub]
      _ = (high - low) * pl.length + low * pl.length := Nat.add_mul _ _ _
  have heq1 : high * ph.length + low * pl.length =
      (high - low) * ph.length + low * (ph.length + pl.length) := by
    rw [hmul_high_ph, Nat.mul_add]
    omega
  have heq2 : high * pl.length + low * ph.length =
      (high - low) * pl.length + low * (ph.length + pl.length) := by
    rw [hmul_high_pl, Nat.mul_add]
    omega
  have hdiff : (high - low) * ph.length ≤ (high - low) * pl.length := by omega
  have hpos : 0 < high - low := Nat.sub_pos_of_lt hlh
  have hres : ph.length ≤ pl.length := Nat.le_of_mul_le_mul_left hdiff hpos
  simpa [ph, pl] using hres

theorem EstimationRegret_sound : TinylambOpt.ResearchGoals.Compression.Partition.EstimationRegret.Goal := by
  intro actual approx xs e herr
  let sel := estimate approx xs
  let opt := estimate actual xs
  have hsel : sel ∈ splits xs := by dsimp [sel]; exact estimate_mem_splits approx xs
  have hopt : opt ∈ splits xs := by dsimp [opt]; exact estimate_mem_splits actual xs
  have hsel_act : bytes actual sel ≤ bytes approx sel + e * sel.length := by
    dsimp [sel]
    apply bytes_le_sum_err
    intro p hp
    exact herr _ hsel p hp
  have hopt_apx : bytes approx opt ≤ bytes actual opt + e * opt.length := by
    dsimp [opt]
    apply bytes_ge_sum_err
    intro p hp
    exact herr _ hopt p hp
  have hapx : bytes approx sel ≤ bytes approx opt := by
    dsimp [sel, opt]
    exact estimate_min approx xs (estimate actual xs) (by dsimp [opt] at hopt; exact hopt)
  change bytes actual sel ≤ bytes actual opt + e * (sel.length + opt.length)
  rw [Nat.mul_add]
  omega

theorem DirectoryWidthSearch_sound : TinylambOpt.ResearchGoals.Compression.Partition.DirectoryWidthSearch.Goal := by
  intro cost xs
  let L : List Nat := (splits xs).map (indexed cost)
  let R : List Nat := (List.range (width xs.length + 1)).flatMap (fun w =>
      ((splits xs).filter (fun ps => commonWidth ps ≤ w)).map (fun ps =>
        bytes cost ps + byteCount (ps.length*w) + ubytes ps.length))
  have hcw_le : ∀ ps ∈ splits xs, commonWidth ps ≤ width xs.length := by
    intro ps hps
    exact commonWidth_le_width_length ps xs hps
  have hLne : L ≠ [] := by
    dsimp [L]
    rcases List.exists_mem_of_ne_nil (splits xs) (splits_ne_nil xs) with ⟨p, hp⟩
    exact List.ne_nil_of_mem (a := indexed cost p) (List.mem_map.mpr ⟨p, hp, rfl⟩)
  have hRne : R ≠ [] := by
    rcases List.exists_mem_of_ne_nil (splits xs) (splits_ne_nil xs) with ⟨p, hp⟩
    let w := commonWidth p
    have hwle : w ≤ width xs.length := hcw_le p hp
    have hwin : w ∈ List.range (width xs.length + 1) := List.mem_range.mpr (by omega)
    dsimp [R]
    apply List.ne_nil_of_mem (a := bytes cost p + byteCount (p.length*w) + ubytes p.length)
    rw [List.mem_flatMap]
    refine ⟨w, hwin, ?_⟩
    apply List.mem_map.mpr
    refine ⟨p, ?_, rfl⟩
    rw [List.mem_filter]
    exact ⟨hp, decide_eq_true (Nat.le_refl _)⟩
  have hLmin : ∃ v, L.min? = some v := by
    refine ⟨L.min?.getD 0, ?_⟩
    have hne : L.min? ≠ none := by
      intro hn
      exact hLne (List.min?_eq_none_iff.mp hn)
    cases hm : L.min? with
    | none => exact (hne hm).elim
    | some m => simp
  have hRmin : ∃ v, R.min? = some v := by
    refine ⟨R.min?.getD 0, ?_⟩
    have hne : R.min? ≠ none := by
      intro hn
      exact hRne (List.min?_eq_none_iff.mp hn)
    cases hm : R.min? with
    | none => exact (hne hm).elim
    | some m => simp
  obtain ⟨vL, hmL⟩ := hLmin
  obtain ⟨vR, hmR⟩ := hRmin
  have hvLmem : vL ∈ L := List.min?_mem hmL
  have hvRmem : vR ∈ R := List.min?_mem hmR
  have hLR : vL ≤ vR := by
    dsimp [R] at hvRmem
    rw [List.mem_flatMap] at hvRmem
    rcases hvRmem with ⟨w, hwrange, hmemw⟩
    rw [List.mem_map] at hmemw
    rcases hmemw with ⟨ps, hpsF, hval⟩
    rw [List.mem_filter] at hpsF
    rcases hpsF with ⟨hps, hcw⟩
    rw [← hval]
    have hidx_le : indexed cost ps ≤ bytes cost ps + byteCount (ps.length*w) + ubytes ps.length := by
      unfold indexed
      have hcw' : commonWidth ps ≤ w := of_decide_eq_true hcw
      have hmul : ps.length * commonWidth ps ≤ ps.length * w := Nat.mul_le_mul_left _ hcw'
      have hbc := byteCount_mono _ _ hmul
      omega
    have hidx_mem : indexed cost ps ∈ L := by
      dsimp [L]
      exact List.mem_map.mpr ⟨ps, hps, rfl⟩
    have hmin_le_idx : vL ≤ indexed cost ps := by
      have hh := List.min?_getD_le_of_mem (k := 0) hidx_mem
      simpa [hmL] using hh
    exact Nat.le_trans hmin_le_idx hidx_le
  have hRL : vR ≤ vL := by
    dsimp [L] at hvLmem
    rcases List.mem_map.mp hvLmem with ⟨ps, hps, hval⟩
    rw [← hval]
    let w := commonWidth ps
    have hwle : w ≤ width xs.length := hcw_le ps hps
    have hwin : w ∈ List.range (width xs.length + 1) := List.mem_range.mpr (by omega)
    have hcandmem : bytes cost ps + byteCount (ps.length*w) + ubytes ps.length ∈ R := by
      dsimp [R]
      rw [List.mem_flatMap]
      refine ⟨w, hwin, ?_⟩
      apply List.mem_map.mpr
      refine ⟨ps, ?_, rfl⟩
      rw [List.mem_filter]
      exact ⟨hps, decide_eq_true (Nat.le_refl _)⟩
    have hmin_le_cand : vR ≤ bytes cost ps + byteCount (ps.length*w) + ubytes ps.length := by
      have hh := List.min?_getD_le_of_mem (k := 0) hcandmem
      simpa [hmR] using hh
    have heq : indexed cost ps = bytes cost ps + byteCount (ps.length*w) + ubytes ps.length := by
      dsimp [w]
      rfl
    rw [heq]
    exact hmin_le_cand
  have heq : vL = vR := Nat.le_antisymm hLR hRL
  change L.min? = R.min?
  rw [hmL, hmR, heq]

theorem CappedAppend_sound : TinylambOpt.ResearchGoals.Compression.Partition.CappedAppend.Goal := by
  intro cost a b cap ca cb ha hb
  rcases boundedBest_achieved cost cap a ca ha with ⟨pa, hpa, hcapa, hbytesa⟩
  rcases boundedBest_achieved cost cap b cb hb with ⟨pb, hpb, hcapb, hbytesb⟩
  have hv : valid (pa ++ pb) (a ++ b) :=
    valid_append ((complete_boundaries pa a).mp hpa) ((complete_boundaries pb b).mp hpb)
  have hpm : pa ++ pb ∈ splits (a ++ b) := (complete_boundaries (pa ++ pb) (a ++ b)).mpr hv
  have hcapm : capped cap (pa ++ pb) := capped_append cap pa pb hcapa hcapb
  unfold boundedBest
  let L := ((splits (a++b)).filter (capped cap)).map (bytes cost)
  have hmem : ca + cb ∈ L := by
    dsimp [L]
    apply List.mem_map.mpr
    refine ⟨pa ++ pb, ?_, ?_⟩
    · rw [List.mem_filter]
      exact ⟨hpm, hcapm⟩
    · rw [bytes_append, hbytesa, hbytesb]
  have hmin_eq : L.min? = some (L.min?.getD 0) := by
    have hne : L.min? ≠ none := by
      intro hn
      exact (List.ne_nil_of_mem hmem) (List.min?_eq_none_iff.mp hn)
    cases hm : L.min? with
    | none => exact (hne hm).elim
    | some m => simp
  refine ⟨L.min?.getD 0, ?_, ?_⟩
  · exact List.min?_getD_le_of_mem hmem
  · simpa [L] using hmin_eq

end TinylambOpt.ResearchProofs.DevinBatch.C_Partition
