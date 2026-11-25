import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_IntervalDeletes_LiveSelect

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.IntervalDeletes

/-! `knots n rs` is the sorted, deduplicated set of clamped interval endpoints
    together with `0` and `n`, so its adjacent pairs are half-open tiles on which
    `blocked rs` is constant.  The unblocked tiles therefore concatenate to
    exactly `live n rs`, and `seek` is just `get?` on those concatenated tiles.
    Verified exhaustively for `n ≤ 5`, at most two spans with endpoints in `0..6`,
    and all `k ≤ n + 2` before proving. -/

private theorem head!_cons (a : Nat) (l : List Nat) : (a :: l).head! = a :=
  List.head!_of_head? rfl

private def ranges : List Span → List Nat
  | [] => []
  | (a, b) :: ps => List.range' a (b - a) ++ ranges ps

private theorem seek_eq_ranges_get (ps : List Span) (k : Nat) :
    seek ps k = (ranges ps)[k]? := by
  induction ps generalizing k with
  | nil => simp [seek, ranges]
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      rw [show seek ((a, b) :: ps) k =
        if k < b - a then some (a + k) else seek ps (k - (b - a)) by rfl]
      rw [show ranges ((a, b) :: ps) = List.range' a (b - a) ++ ranges ps by rfl]
      rw [List.getElem?_append, List.length_range']
      by_cases h : k < b - a
      · rw [if_pos h, if_pos h, List.getElem?_range' h]
        simp
      · rw [if_neg h, if_neg h]
        rw [ih]

private def coverLive (rs : List Span) : List Nat → List Nat
  | [] => []
  | [_] => []
  | a :: b :: rest =>
      (if !blocked rs a then List.range' a (b - a) else []) ++ coverLive rs (b :: rest)

private theorem ranges_pieces_eq_coverLive (rs : List Span) : ∀ ks : List Nat,
    ranges ((ks.zip (ks.drop 1)).filter (fun r => !blocked rs r.1)) = coverLive rs ks := by
  intro ks
  induction ks with
  | nil => simp [ranges, coverLive]
  | cons a rest ih =>
      cases rest with
      | nil => simp [ranges, coverLive]
      | cons b rest' =>
          have ih' : ranges (List.filter (fun r => !blocked rs r.1) ((b :: rest').zip rest')) =
              coverLive rs (b :: rest') := by
            simpa [List.drop_one] using ih
          rw [show (a :: b :: rest').zip ((a :: b :: rest').drop 1) =
              (a, b) :: ((b :: rest').zip rest') by simp]
          by_cases hb : blocked rs a <;> simp [ranges, coverLive, ih', hb]

private theorem filter_range'_const (p : Nat → Bool) (a len : Nat)
    (h : ∀ i, a ≤ i → i < a + len → p i = p a) :
    (List.range' a len).filter p = if p a then List.range' a len else [] := by
  by_cases hp : p a
  · rw [if_pos hp]
    rw [List.filter_eq_self.mpr]
    intro i hi
    rw [List.mem_range'_1] at hi
    rw [h i hi.1 hi.2, hp]
  · rw [if_neg hp]
    rw [List.filter_eq_nil_iff.mpr]
    intro i hi
    rw [List.mem_range'_1] at hi
    have hia := h i hi.1 hi.2
    intro hpi
    rw [hia] at hpi
    exact hp hpi

private def AdjConst (rs : List Span) : List Nat → Prop
  | [] | [_] => True
  | a :: b :: rest =>
      (∀ i, a ≤ i → i < b → blocked rs i = blocked rs a) ∧ AdjConst rs (b :: rest)

private theorem coverLive_eq_filter_aux (rs : List Span) :
    ∀ (ks : List Nat) (n : Nat),
      ks ≠ [] →
      ks.Pairwise (· ≤ ·) →
      (∀ x ∈ ks, x ≤ n) →
      n ∈ ks →
      AdjConst rs ks →
      coverLive rs ks = (List.range' ks.head! (n - ks.head!)).filter (fun i => !blocked rs i) := by
  intro ks
  induction ks with
  | nil =>
      intro n hne; simp at hne
  | cons a rest ih =>
      intro n hne hsorted hle hnmem hadj
      cases rest with
      | nil =>
          have hna : n = a := by simpa using hnmem
          subst n
          rw [head!_cons]
          simp [coverLive]
      | cons b rest' =>
          have hab : a ≤ b := (List.pairwise_cons.mp hsorted).1 b (by simp)
          have hnmem_tail : n ∈ b :: rest' := by
            rw [List.mem_cons] at hnmem
            rcases hnmem with hna | hnmem
            · subst n
              have hbn : b ≤ a := hle b (by simp)
              have hba : b = a := by omega
              subst b
              simp
            · exact hnmem
          have hle_tail : ∀ x ∈ b :: rest', x ≤ n := by
            intro x hx; exact hle x (by simp [hx])
          have hadj_tail : AdjConst rs (b :: rest') := hadj.2
          have ih' : coverLive rs (b :: rest') =
              (List.range' b (n - b)).filter (fun i => !blocked rs i) := by
            simpa [head!_cons] using ih n (by simp) hsorted.tail hle_tail hnmem_tail hadj_tail
          simp only [coverLive]
          rw [ih', head!_cons]
          have hfirst := hadj.1
          have hfilter_first :
              (List.range' a (b - a)).filter (fun i => !blocked rs i) =
                if !blocked rs a then List.range' a (b - a) else [] := by
            apply filter_range'_const
            intro i hai hib
            have hi_lt : i < b := by omega
            rw [hfirst i hai hi_lt]
          have hbn : b ≤ n := hle b (by simp)
          have hsplit : List.range' a (n - a) =
              List.range' a (b - a) ++ List.range' b (n - b) := by
            have hstart : a + (b - a) = b := by omega
            have hlen : (b - a) + (n - b) = n - a := by omega
            calc
              List.range' a (n - a) = List.range' a ((b - a) + (n - b)) := by rw [hlen]
              _ = List.range' a (b - a) ++ List.range' (a + (b - a)) (n - b) := by
                simpa using (List.range'_append (s := a) (m := b - a) (n := n - b) (step := 1)).symm
              _ = List.range' a (b - a) ++ List.range' b (n - b) := by rw [hstart]
          rw [hsplit, List.filter_append, hfilter_first]

private theorem pairwise_eraseDups_le : ∀ l : List Nat, l.Pairwise (· ≤ ·) → l.eraseDups.Pairwise (· ≤ ·)
  | [], _ => by simp
  | a :: as, h => by
      rw [List.eraseDups_cons]
      rw [List.pairwise_cons] at h ⊢
      constructor
      · intro x hx
        rw [List.mem_eraseDups] at hx
        exact h.1 x (List.mem_filter.mp hx).1
      · exact pairwise_eraseDups_le (as.filter fun b : Nat => !b == a) (h.2.filter _)
termination_by l => l.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b : Nat => !b == a) as)

private theorem knots_sorted (n : Nat) (rs : List Span) :
    (knots n rs).Pairwise (· ≤ ·) := by
  unfold knots
  apply pairwise_eraseDups_le
  have hs := List.pairwise_mergeSort
    (le := fun a b : Nat => decide (a ≤ b))
    (trans := by intro a b c hab hbc; simp at hab hbc ⊢; omega)
    (total := by
      intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have hba : b ≤ a := by omega
        simp [h, hba])
    (0 :: n :: rs.flatMap (fun (a, b) => [min a n, min b n]))
  exact hs.imp (fun {a b} h => of_decide_eq_true h)

private theorem knots_mem_zero (n : Nat) (rs : List Span) : 0 ∈ knots n rs := by
  unfold knots
  simp [List.mem_mergeSort]

private theorem knots_mem_n (n : Nat) (rs : List Span) : n ∈ knots n rs := by
  unfold knots
  simp [List.mem_mergeSort]

private theorem knots_mem_of_mem (n a b : Nat) (rs : List Span) (h : (a,b) ∈ rs) :
    min a n ∈ knots n rs ∧ min b n ∈ knots n rs := by
  unfold knots
  have hflat : min a n ∈ rs.flatMap (fun (r : Span) => [min r.1 n, min r.2 n]) := by
    apply List.mem_flatMap.mpr
    exact ⟨(a,b), h, by simp⟩
  have hflat2 : min b n ∈ rs.flatMap (fun (r : Span) => [min r.1 n, min r.2 n]) := by
    apply List.mem_flatMap.mpr
    exact ⟨(a,b), h, by simp⟩
  constructor <;> simp [List.mem_mergeSort, hflat, hflat2]

private theorem knots_le_n (n : Nat) (rs : List Span) : ∀ x ∈ knots n rs, x ≤ n := by
  unfold knots
  intro x hx
  rw [List.mem_eraseDups, List.mem_mergeSort] at hx
  simp only [List.mem_cons] at hx
  rcases hx with hx | hx | hx
  · subst x; exact Nat.zero_le n
  · subst x; omega
  · rw [List.mem_flatMap] at hx
    rcases hx with ⟨⟨a,b⟩, hr, hx⟩
    have hx' : x = min a n ∨ x = min b n := by simpa using hx
    rcases hx' with rfl | rfl <;> exact Nat.min_le_right _ _

private theorem hit_clamped (n x y i : Nat) (hi : i < n) :
    hit (x, y) i = hit (min x n, min y n) i := by
  by_cases hx : x ≤ n
  · by_cases hy : y ≤ n
    · simp [hit, Nat.min_eq_left hx, Nat.min_eq_left hy]
    · have hny : n ≤ y := by omega
      have hily : i < y := by omega
      simp [hit, Nat.min_eq_left hx, Nat.min_eq_right hny, hi, hily]
  · have hnx : n ≤ x := by omega
    have hxi : ¬ x ≤ i := by omega
    have hni : ¬ n ≤ i := by omega
    by_cases hy : y ≤ n
    · simp [hit, Nat.min_eq_right hnx, Nat.min_eq_left hy, hxi, hni]
    · have hny : n ≤ y := by omega
      simp [hit, Nat.min_eq_right hnx, Nat.min_eq_right hny, hxi, hni]

private theorem endpoint_le_or_ge {s b u : Nat} {rest : List Nat}
    (hb : ∀ u ∈ b :: rest, b ≤ u)
    (h : u ≤ s ∨ u ∈ s :: b :: rest) : u ≤ s ∨ b ≤ u := by
  rcases h with h | h
  · exact Or.inl h
  · rw [List.mem_cons] at h
    rcases h with rfl | h
    · left; omega
    · exact Or.inr (hb u h)

private theorem blocked_eq_at_head (n : Nat) (rs : List Span) (s b i : Nat)
    (hsb : s < b) (hbn : b ≤ n) (hsi : s ≤ i) (hib : i < b)
    (hend : ∀ (x y : Nat), (x, y) ∈ rs →
       (min x n ≤ s ∨ b ≤ min x n) ∧ (min y n ≤ s ∨ b ≤ min y n)) :
    blocked rs i = blocked rs s := by
  have hsn : s < n := by omega
  have hin : i < n := by omega
  revert hend
  induction rs with
  | nil => intro hend; simp [blocked]
  | cons r rs ih =>
      intro hend
      rcases r with ⟨x, y⟩
      simp only [blocked, List.any_cons]
      change (hit (x, y) i || blocked rs i) = (hit (x, y) s || blocked rs s)
      rw [ih (by
        intro x y hxy
        exact hend x y (by simp [hxy]))]
      congr 1
      have he := hend x y (by simp)
      rcases he with ⟨hu, hv⟩
      rw [hit_clamped n x y i hin, hit_clamped n x y s hsn]
      simp only [hit]
      rcases hu with hu | hu
      · have hui : min x n ≤ i := by omega
        rcases hv with hv | hv
        · have hivn : ¬ i < min y n := by omega
          have hsvn : ¬ s < min y n := by omega
          simp [hui, hu, hivn, hsvn]
        · have hivt : i < min y n := by omega
          have hsvt : s < min y n := by omega
          simp [hui, hu, hivt, hsvt]
      · have huin : ¬ min x n ≤ i := by omega
        have husn : ¬ min x n ≤ s := by omega
        simp [huin, husn]

private theorem adjConst_aux (n : Nat) (rs : List Span) :
    ∀ (s : Nat) (rest : List Nat),
      (s :: rest).Pairwise (· ≤ ·) →
      (∀ x ∈ s :: rest, x ≤ n) →
      (∀ (x y : Nat), (x, y) ∈ rs →
        (min x n ≤ s ∨ min x n ∈ s :: rest) ∧
        (min y n ≤ s ∨ min y n ∈ s :: rest)) →
      AdjConst rs (s :: rest) := by
  intro s rest
  induction rest generalizing s with
  | nil => intro hs hle hend; simp [AdjConst]
  | cons b rest' ih =>
      intro hs hle hend
      have hsleb : s ≤ b := (List.pairwise_cons.mp hs).1 b (by simp)
      have htail_sorted : (b :: rest').Pairwise (· ≤ ·) := (List.pairwise_cons.mp hs).2
      have hb_ge : ∀ u ∈ b :: rest', b ≤ u := by
        intro u hu
        rw [List.mem_cons] at hu
        rcases hu with rfl | hu
        · omega
        · exact (List.pairwise_cons.mp htail_sorted).1 u hu
      have hle_tail : ∀ u ∈ b :: rest', u ≤ n := by
        intro u hu; exact hle u (by simp [hu])
      have hfirst : ∀ i, s ≤ i → i < b → blocked rs i = blocked rs s := by
        intro i hsi hib
        by_cases hsb : s = b
        · omega
        · have hslt : s < b := by omega
          have hbn : b ≤ n := hle b (by simp)
          apply blocked_eq_at_head n rs s b i hslt hbn hsi hib
          intro x y hxy
          have he := hend x y hxy
          exact ⟨endpoint_le_or_ge hb_ge he.1, endpoint_le_or_ge hb_ge he.2⟩
      have hend_tail : ∀ (x y : Nat), (x, y) ∈ rs →
          (min x n ≤ b ∨ min x n ∈ b :: rest') ∧
          (min y n ≤ b ∨ min y n ∈ b :: rest') := by
        intro x y hxy
        have he := hend x y hxy
        constructor
        · rcases he.1 with hu | hu
          · left; omega
          · rw [List.mem_cons] at hu
            rcases hu with hseq | hu
            · left; rw [hseq]; exact hsleb
            · right; exact hu
        · rcases he.2 with hv | hv
          · left; omega
          · rw [List.mem_cons] at hv
            rcases hv with hseq | hv
            · left; rw [hseq]; exact hsleb
            · right; exact hv
      exact ⟨hfirst, ih b htail_sorted hle_tail hend_tail⟩

private theorem knots_adjConst (n : Nat) (rs : List Span) : AdjConst rs (knots n rs) := by
  generalize hk : knots n rs = ks
  cases ks with
  | nil =>
      have hz := knots_mem_zero n rs
      rw [hk] at hz
      simp at hz
  | cons s rest =>
      apply adjConst_aux n rs s rest
      · simpa [hk] using knots_sorted n rs
      · intro x hx
        have hx' : x ∈ knots n rs := by simpa [hk] using hx
        exact knots_le_n n rs x hx'
      · intro a b hab
        have hmem := knots_mem_of_mem n a b rs hab
        rw [hk] at hmem
        exact ⟨Or.inr hmem.1, Or.inr hmem.2⟩

private theorem knots_head_zero (n : Nat) (rs : List Span) : (knots n rs).head! = 0 := by
  generalize hk : knots n rs = ks
  cases ks with
  | nil =>
      have hz := knots_mem_zero n rs
      rw [hk] at hz
      simp at hz
  | cons s rest =>
      have hs0 : 0 ∈ s :: rest := by simpa [hk] using knots_mem_zero n rs
      rw [head!_cons]
      rw [List.mem_cons] at hs0
      rcases hs0 with h0 | h0
      · omega
      · have hsle := (List.pairwise_cons.mp (by simpa [hk] using knots_sorted n rs)).1
        have : s ≤ 0 := hsle 0 h0
        omega

private theorem coverLive_knots (n : Nat) (rs : List Span) :
    coverLive rs (knots n rs) = live n rs := by
  have hne : knots n rs ≠ [] := by
    intro h
    have hz := knots_mem_zero n rs
    rw [h] at hz
    simp at hz
  have hs := coverLive_eq_filter_aux rs (knots n rs) n hne
    (knots_sorted n rs) (knots_le_n n rs) (knots_mem_n n rs) (knots_adjConst n rs)
  rw [knots_head_zero] at hs
  simpa [live, List.range_eq_range'] using hs

theorem sound_IntervalDeletes_LiveSelect : LiveSelect.Goal := by
  intro n rs k
  rw [seek_eq_ranges_get]
  rw [show (pieces n rs).filter (fun r => !blocked rs r.1) =
      ((knots n rs).zip ((knots n rs).drop 1)).filter (fun r => !blocked rs r.1) by rfl]
  rw [ranges_pieces_eq_coverLive, coverLive_knots]

end TinylambOpt.ResearchProofs.Atlas.Goal_IntervalDeletes_LiveSelect
