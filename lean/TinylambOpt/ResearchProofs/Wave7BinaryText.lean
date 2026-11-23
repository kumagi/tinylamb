import TinylambOpt.ResearchGoals.ExecutionStrategies.BinaryText

namespace TinylambOpt.ResearchProofs.Wave7BinaryText

namespace InternalPrefixFence

open TinylambOpt.ResearchGoals.BinaryText

private theorem byte_succ (b : UInt8) (h : b ≠ 255) : b < b + 1 := by
  have hpos : ∀ c : UInt8, c ≠ 255 → c < c + 1 := by native_decide
  exact hpos b h

private theorem upperRev_none_iff (r : Bytes) :
    upperRev r = none ↔ ∀ b ∈ r, b == 255 := by
  induction r with
  | nil => simp [upperRev]
  | cons b bs ih =>
      by_cases hb : b == 255
      · have hb255 : b = 255 := beq_iff_eq.mp hb
        simp [upperRev, hb, ih, hb255]
      · have hbne : b ≠ 255 := by intro h; apply hb; simpa [h]
        simp [upperRev, hb, ih, hbne]

private def IsUpper (p hi : Bytes) : Prop :=
  ∃ j c, j < p.length ∧ p[j]? = some c ∧ c ≠ 255 ∧
    hi = p.take j ++ [c + 1] ∧ (∀ k, j < k → k < p.length → p[k]? = some 255)

private theorem upperRev_isUpper (r t : Bytes) (h : upperRev r = some t) :
    IsUpper r.reverse t.reverse := by
  induction r generalizing t with
  | nil => simp [upperRev] at h
  | cons b bs ih =>
      by_cases hb : b == 255
      · simp [upperRev, hb] at h
        rcases ih t h with ⟨j, c, hjlen, hjget, hcne, ht, htail⟩
        have hrev : (b :: bs).reverse = bs.reverse ++ [b] := by simp
        rw [hrev]
        refine ⟨j, c, ?_, ?_, hcne, ?_, ?_⟩
        · have hjlen' : j < bs.length := by simpa using hjlen
          simpa using Nat.lt_succ_of_lt hjlen'
        · rw [List.getElem?_append_left hjlen]
          exact hjget
        · have hjle : j ≤ bs.reverse.length := by omega
          rw [List.take_append_of_le_length hjle]
          exact ht
        · intro k hjk hklen
          have hklen0 : k < bs.length + 1 := by simpa using hklen
          have hklen1 : k ≤ bs.length := by omega
          by_cases hk' : k < bs.length
          · rw [List.getElem?_append_left (by simpa using hk')]
            exact htail k hjk (by simpa using hk')
          · have hkge : bs.length ≤ k := Nat.le_of_not_gt hk'
            have hkeq : k = bs.length := by omega
            have hb255 : b = 255 := beq_iff_eq.mp hb
            subst k
            have hle : bs.reverse.length ≤ bs.length := by simp
            rw [List.getElem?_append_right hle]
            simp [hb255]
      · simp [upperRev, hb] at h
        subst t
        refine ⟨bs.reverse.length, b, by simp, ?_, ?_, ?_, ?_⟩
        · simp
        · intro hb255
          apply hb
          simpa [hb255]
        · simp
        · intro k hk hklen
          simp at hklen
          have hrevlen : bs.reverse.length = bs.length := by simp
          omega

private theorem upper_isUpper (p hi : Bytes) (h : upper p = some hi) : IsUpper p hi := by
  unfold upper at h
  cases hrev : upperRev p.reverse with
  | none => simp [hrev] at h
  | some t =>
      simp [hrev] at h
      have hu := upperRev_isUpper p.reverse t hrev
      subst hi
      simpa using hu

private theorem drop_cons (p : Bytes) (j : Nat) (hj : j < p.length) :
    ∃ t, p.drop j = p[j] :: t := by
  cases h : p.drop j with
  | nil =>
      have hl := List.length_drop (i := j) (l := p)
      simp [h] at hl
      omega
  | cons b t =>
      refine ⟨t, ?_⟩
      have hb0 : (p.drop j)[0]? = some b := by simp [h]
      have hb : p[j]? = some b := by simpa [List.getElem?_drop] using hb0
      rcases List.getElem?_eq_some_iff.mp hb with ⟨_, hbval⟩
      simpa [hbval] using h

private theorem isUpper_gt (p hi x : Bytes) (hu : IsUpper p hi) (hpref : p.isPrefixOf x = true) :
    x < hi := by
  rcases hu with ⟨j, c, hjlen, hjget, hcne, rfl, _⟩
  have hp : p <+: x := List.isPrefixOf_iff_prefix.mp hpref
  rcases hp with ⟨rest, rfl⟩
  have hxeq : p ++ rest = p.take j ++ (p.drop j ++ rest) := by
    have htad := List.take_append_drop j p
    calc
      p ++ rest = (p.take j ++ p.drop j) ++ rest := by rw [htad]
      _ = p.take j ++ (p.drop j ++ rest) := List.append_assoc _ _ _
  rw [hxeq]
  rw [List.append_lt_append_iff_of_length_eq rfl]
  simp only [List.lt_irrefl, false_or, true_and]
  rcases drop_cons p j hjlen with ⟨t, ht⟩
  rw [ht]
  have hcval : p[j] = c := by
    rcases List.getElem?_eq_some_iff.mp hjget with ⟨_, hv⟩
    exact hv
  rw [hcval]
  exact List.cons_lt_cons_of_lt (byte_succ c hcne)

private theorem succ_le_of_lt (c d : UInt8) (h : c < d) : c + 1 ≤ d := by
  have hf : ∀ u v : UInt8, u < v → u + 1 ≤ v := by native_decide
  exact hf c d h

private theorem isUpper_converse (p hi x : Bytes) (hu : IsUpper p hi)
    (hle : p ≤ x) (hxhi : x < hi) : p.isPrefixOf x := by
  rcases hu with ⟨j, c, hjlen, hjget, hcne, rfl, htail⟩
  rw [List.isPrefixOf_iff_prefix]
  rw [List.le_iff_exists] at hle
  rcases hle with hEq | ⟨i, hi1, hi2, hagree, hlt⟩
  · refine ⟨x.drop p.length, ?_⟩
    have ht := List.take_append_drop p.length x
    rw [← hEq] at ht
    exact ht
  · have hjmin : min j p.length = j := Nat.min_eq_left (by omega)
    by_cases hij : i < j
    · have hiLt : p.take j ++ [c + 1] < x := by
        rw [List.lt_iff_exists]
        right
        refine ⟨i, ?_, hi2, ?_, ?_⟩
        · simp
          omega
        · intro k hk
          have hkj : k < j := by omega
          have hklen1 : k < (p.take j ++ [c + 1]).length := by simp; omega
          rw [List.getElem_append_left (by simp [hkj, hjmin]) (h' := hklen1)]
          rw [List.getElem_take]
          exact hagree k hk
        · have hi1' : i < (p.take j).length := by simpa [hjmin] using hij
          have hklen1 : i < (p.take j ++ [c + 1]).length := by simp; omega
          rw [List.getElem_append_left hi1' (h' := hklen1)]
          rw [List.getElem_take]
          exact hlt
      exact False.elim (Std.Asymm.asymm (r := fun a b : Bytes => a < b) (p.take j ++ [c + 1]) x hiLt hxhi)
    · by_cases hji : j < i
      · have hpval : p[i] = 255 := by
          rcases List.getElem?_eq_some_iff.mp (htail i hji hi1) with ⟨_, hv⟩
          exact hv
        have hmax : ∀ (u v : UInt8), u = 255 → ¬ u < v := by native_decide
        exact False.elim (hmax (p[i]) (x[i]) hpval hlt)
      · have hieq : i = j := by omega
        subst i
        rcases List.getElem?_eq_some_iff.mp hjget with ⟨_, hpjval⟩
        have hcx : c < x[j] := by simpa [hpjval] using hlt
        have hcle : c + 1 ≤ x[j] := succ_le_of_lt c x[j] hcx
        by_cases heq : c + 1 = x[j]
        · have htake : p.take j ++ [c + 1] = x.take (j + 1) := by
            apply List.ext_getElem
            · have hxmin : min (j + 1) x.length = j + 1 := Nat.min_eq_left (by omega)
              simp [hjmin, hxmin]
            · intro k hk1 hk2
              by_cases hkj : k < j
              · have hlen1 : (p.take j ++ [c + 1]).length = j + 1 := by simp [hjmin]
                have hklen1 : k < (p.take j ++ [c + 1]).length := by omega
                rw [List.getElem_append_left (by simp [hkj, hjmin]) (h' := hklen1)]
                rw [List.getElem_take]
                rw [List.getElem_take]
                exact hagree k hkj
              · have hlen1 : (p.take j ++ [c + 1]).length = j + 1 := by simp [hjmin]
                have hklt : k < j + 1 := by simpa [hlen1] using hk1
                have hkeq : k = j := by omega
                subst k
                have hklen1 : j < (p.take j ++ [c + 1]).length := by simpa [hlen1]
                rw [List.getElem_append_right (h₁ := by simp [hjmin]) (h₂ := hklen1)]
                simp [hjmin]
                have hxmin1 : min (j + 1) x.length = j + 1 := Nat.min_eq_left (by omega)
                exact heq
          have hlehi : p.take j ++ [c + 1] ≤ x := by
            rw [htake]
            exact (List.take_prefix (j + 1) x).le
          exact False.elim (hlehi hxhi)
        · have hclt : c + 1 < x[j] := by
            have hf : ∀ u v : UInt8, u ≤ v → u ≠ v → u < v := by native_decide
            exact hf (c + 1) (x[j]) hcle heq
          have hiLt : p.take j ++ [c + 1] < x := by
            rw [List.lt_iff_exists]
            right
            refine ⟨j, ?_, hi2, ?_, ?_⟩
            · have hjmin : min j p.length = j := Nat.min_eq_left (by omega)
              simp [hjmin, hjlen]
            · intro k hk
              have hklen1 : k < (p.take j ++ [c + 1]).length := by simp; omega
              rw [List.getElem_append_left (by simp [hk, hjmin]) (h' := hklen1)]
              rw [List.getElem_take]
              exact hagree k hk
            · have hklen1 : j < (p.take j ++ [c + 1]).length := by simp [hjmin]
              rw [List.getElem_append_right (h₁ := by simp [hjmin]) (h₂ := hklen1)]
              simp [hjmin]
              exact hclt
          exact False.elim (Std.Asymm.asymm (r := fun a b : Bytes => a < b) (p.take j ++ [c + 1]) x hiLt hxhi)

private theorem upper_none_converse (p x : Bytes) (hup : upper p = none) (hle : p ≤ x) :
    p.isPrefixOf x := by
  have hall : ∀ b ∈ p, b == 255 := by
    unfold upper at hup
    cases hrev : upperRev p.reverse with
    | some t => simp [hrev] at hup
    | none =>
        have hb := (upperRev_none_iff p.reverse).mp hrev
        intro b hbmem
        apply hb
        simpa using hbmem
  rw [List.isPrefixOf_iff_prefix]
  rw [List.le_iff_exists] at hle
  rcases hle with hEq | ⟨i, hi1, hi2, hagree, hlt⟩
  · refine ⟨x.drop p.length, ?_⟩
    have ht := List.take_append_drop p.length x
    rw [← hEq] at ht
    exact ht
  · have hbget : p[i]? = some (p[i]) := by
      apply List.getElem?_eq_some_iff.mpr
      exact ⟨hi1, rfl⟩
    have hb255 : p[i] == 255 := hall (p[i]) (by simp [hi1])
    have hbval : p[i] = 255 := beq_iff_eq.mp hb255
    have hmax : ∀ (u v : UInt8), u = 255 → ¬ u < v := by native_decide
    exact False.elim (hmax (p[i]) (x[i]) hbval hlt)

theorem sound : TinylambOpt.ResearchGoals.BinaryText.PrefixFence.Goal := by
  intro p x
  by_cases hpref : p.isPrefixOf x = true
  · have hle : p ≤ x := (List.isPrefixOf_iff_prefix.mp hpref).le
    have hub : (upper p).all (fun hi => decide (x < hi)) = true := by
      cases hup : upper p with
      | none => simp
      | some hi =>
          have hlt := isUpper_gt p hi x (upper_isUpper p hi hup) hpref
          simp [hup, hlt]
    simp [hpref, hle, hub]
  · have hrhs : (decide (p ≤ x) && (upper p).all (fun hi => decide (x < hi))) = false := by
      by_cases hle : p ≤ x
      · cases hup : upper p with
        | none =>
            exact False.elim (hpref (upper_none_converse p x hup hle))
        | some hi =>
            have hnlt : ¬ x < hi := by
              intro hxhi
              exact hpref (isUpper_converse p hi x (upper_isUpper p hi hup) hle hxhi)
            simp [hup, hle, hnlt]
      · simp [hle]
    simp [hpref, hrhs]

end InternalPrefixFence

namespace InternalSharedPrefixCompare

open TinylambOpt.ResearchGoals.BinaryText

private theorem opt_none_lt_some (x : UInt8) : compare (none : Option UInt8) (some x) = .lt := by
  have h : ∀ y : UInt8, compare (none : Option UInt8) (some y) = .lt := by native_decide
  exact h x

private theorem opt_some_some (x y : UInt8) : compare (some x) (some y) = compare x y := by
  have h : ∀ u v : UInt8, compare (some u) (some v) = compare u v := by native_decide
  exact h x y

private theorem opt_some_gt_none (x : UInt8) : compare (some x) (none : Option UInt8) = .gt := by
  have h : ∀ y : UInt8, compare (some y) (none : Option UInt8) = .gt := by native_decide
  exact h x

theorem sound : TinylambOpt.ResearchGoals.BinaryText.SharedPrefixCompare.Goal := by
  intro prev
  induction prev with
  | nil =>
      intro curr probe
      simp [nextCmp, lcp]
  | cons x xs ih =>
      intro curr probe
      cases curr with
      | nil =>
          cases probe with
          | nil => simp [nextCmp, lcp]
          | cons z zs =>
              by_cases hxz : x == z
              · have hpos : 0 < 1 + lcp xs zs := by omega
                simp [nextCmp, lcp, hxz, hpos, opt_none_lt_some]
              · simp [nextCmp, lcp, hxz]
      | cons y ys =>
          cases probe with
          | nil =>
              by_cases hxy : x == y
              · have hpos : 0 < 1 + lcp xs ys := by omega
                simp [nextCmp, lcp, hxy, hpos, opt_some_gt_none]
              · simp [nextCmp, lcp, hxy]
          | cons z zs =>
              by_cases hxy : x == y
              · by_cases hxz : x == z
                · have hy : y = x := (beq_iff_eq.mp hxy).symm
                  have hz : z = x := (beq_iff_eq.mp hxz).symm
                  subst y
                  subst z
                  have h1 : 1 + lcp xs ys = lcp xs ys + 1 := by omega
                  have h2 : 1 + lcp xs zs = lcp xs zs + 1 := by omega
                  simp only [lcp, hxy, hxz, if_true]
                  rw [h1, h2]
                  simpa [nextCmp, Nat.succ_lt_succ_iff, ih] using (ih ys zs)
                · have hy : y = x := (beq_iff_eq.mp hxy).symm
                  subst y
                  have hz : z ≠ x := by intro h'; apply hxz; simpa [h']
                  have hpos : 0 < 1 + lcp xs ys := by omega
                  have hxz' : x ≠ z := by intro h'; exact hz h'.symm
                  cases hcmp : compare x z with
                  | eq => exact False.elim (hz (Std.LawfulEqOrd.compare_eq_iff_eq.mp hcmp).symm)
                  | lt => simp [nextCmp, lcp, hpos, hcmp, hxz', opt_some_some]
                  | gt => simp [nextCmp, lcp, hpos, hcmp, hxz', opt_some_some]
              · by_cases hxz : x == z
                · have hz : z = x := (beq_iff_eq.mp hxz).symm
                  subst z
                  have hy : y ≠ x := by intro h'; apply hxy; simpa [h']
                  have hpos : 0 < 1 + lcp xs zs := by omega
                  have hyx' : x ≠ y := by intro h'; exact hy h'.symm
                  cases hcmp : compare y x with
                  | eq => exact False.elim (hy (Std.LawfulEqOrd.compare_eq_iff_eq.mp hcmp))
                  | lt => simp [nextCmp, lcp, hpos, hcmp, hyx', opt_some_some]
                  | gt => simp [nextCmp, lcp, hpos, hcmp, hyx', opt_some_some]
                · simp [nextCmp, lcp, hxy, hxz]

end InternalSharedPrefixCompare

namespace InternalAnchorPositions

open TinylambOpt.ResearchGoals.BinaryText

private theorem match_anchor (s p : Bytes) (j : Nat) (b : UInt8) (hpj : p[j]? = some b)
    (i : Nat) (hi : i ≤ s.length) (hm : matchesAt s p i) :
    i + j < s.length ∧ s[i + j]? = some b := by
  rcases List.getElem?_eq_some_iff.mp hpj with ⟨hjlen, hpjval⟩
  have hp : p <+: s.drop i := List.isPrefixOf_iff_prefix.mp hm
  have hlen := List.IsPrefix.length_le hp
  rw [List.length_drop] at hlen
  have hsum : p.length + i ≤ s.length := Nat.add_le_of_le_sub hi hlen
  constructor
  · omega
  · have hjdrop : j < (s.drop i).length := by
      have hlen0 := List.IsPrefix.length_le hp
      omega
    have hget := hp.getElem hjlen
    have hget? : (s.drop i)[j]? = some b := by
      apply List.getElem?_eq_some_iff.mpr
      exact ⟨hjdrop, by simpa [hpjval] using hget.symm⟩
    simpa [List.getElem?_drop] using hget?

private theorem zipIdx_pairwise_snd (s : Bytes) :
    s.zipIdx.Pairwise (fun e f : UInt8 × Nat => e.2 < f.2) := by
  induction s with
  | nil => simp [List.zipIdx]
  | cons u us ih =>
      rw [List.zipIdx_cons']
      apply List.pairwise_cons.mpr
      constructor
      · intro e he
        rcases List.mem_map.mp he with ⟨f, hf, rfl⟩
        simp
      · have h := ih.map (f := Prod.map id (fun i : Nat => i + 1))
          (S := fun e f : UInt8 × Nat => e.2 < f.2) (by
          intro a c hac
          change a.2 + 1 < c.2 + 1
          omega)
        simpa using h

private def anchorQ (b : UInt8) (j : Nat) (e : UInt8 × Nat) : Bool := e.1 == b && j ≤ e.2

private theorem pairwise_map_sub_of_mem {l : List (UInt8 × Nat)} (j : Nat) (b : UInt8)
    (hrel : l.Pairwise (fun e f => anchorQ b j e = true → anchorQ b j f = true → e.2 < f.2))
    (hQmem : ∀ e ∈ l, anchorQ b j e = true) :
    (l.map (fun e => e.2 - j)).Pairwise (fun a c => a < c) := by
  induction l with
  | nil => simp
  | cons e es ih =>
      have hqe : anchorQ b j e = true := hQmem e (by simp)
      have hQes : ∀ f ∈ es, anchorQ b j f = true := by
        intro f hf
        exact hQmem f (by simp [hf])
      apply List.pairwise_cons.mpr
      constructor
      · intro x hx
        rcases List.mem_map.mp hx with ⟨f, hf, rfl⟩
        have hlt : e.2 < f.2 := (List.pairwise_cons.mp hrel).1 f hf hqe (hQes f hf)
        have hjel : j ≤ e.2 := by
          dsimp [anchorQ] at hqe
          exact of_decide_eq_true ((Bool.and_eq_true_iff.mp hqe).2)
        exact Nat.sub_lt_sub_right hjel hlt
      · exact ih (List.Pairwise.of_cons hrel) hQes

private theorem pairwise_filter_map_sub (l : List (UInt8 × Nat)) (j : Nat) (b : UInt8)
    (hpair : l.Pairwise (fun e f : UInt8 × Nat => e.2 < f.2)) :
    ((l.filter (anchorQ b j)).map (fun e => e.2 - j)).Pairwise (fun a c => a < c) := by
  have hfilt0 : (l.filter (anchorQ b j)).Pairwise (fun e f : UInt8 × Nat => e.2 < f.2) :=
    (List.pairwise_filter (p := anchorQ b j) (R := fun e f : UInt8 × Nat => e.2 < f.2) (l := l)).mpr
      (hpair.imp (by intro e f hef hqe hqf; exact hef))
  have hfilt : (l.filter (anchorQ b j)).Pairwise
      (fun e f => anchorQ b j e = true → anchorQ b j f = true → e.2 < f.2) :=
    hfilt0.imp (by intro e f hef hqe hqf; exact hef)
  have hQmem : ∀ e ∈ l.filter (anchorQ b j), anchorQ b j e = true := by
    intro e he
    exact (List.mem_filter.mp he).2
  exact pairwise_map_sub_of_mem j b hfilt hQmem

private theorem nodup_of_pairwise_lt {l : List Nat}
    (h : l.Pairwise (fun a b => a < b)) : l.Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      apply List.nodup_cons.mpr
      constructor
      · intro hxmem
        have hlt := (List.pairwise_cons.mp h).1 x hxmem
        omega
      · exact ih (List.Pairwise.of_cons h)

theorem sound : TinylambOpt.ResearchGoals.BinaryText.AnchorPositions.Goal := by
  intro s p j b hpj
  let cand : List Nat := (s.zipIdx.filter (anchorQ b j)).map (fun e => e.2 - j)
  let lhs : List Nat := cand.filter (matchesAt s p)
  let rhs : List Nat := (List.range (s.length + 1)).filter (matchesAt s p)
  have hcand_pair : cand.Pairwise (fun a c => a < c) := by
    dsimp [cand]
    exact pairwise_filter_map_sub (s.zipIdx) j b (zipIdx_pairwise_snd s)
  have hlhs_pair : lhs.Pairwise (fun a c => a ≤ c) := by
    dsimp [lhs]
    exact (List.Pairwise.filter (matchesAt s p) (hcand_pair.imp (by
      intro a c hac
      exact Nat.le_of_lt hac)))
  have hrhs_pair : rhs.Pairwise (fun a c => a ≤ c) := by
    dsimp [rhs]
    exact List.Pairwise.filter (matchesAt s p) List.pairwise_le_range
  have hlhs_nodup : lhs.Nodup := nodup_of_pairwise_lt hcand_pair |>.filter _
  have hrhs_nodup : rhs.Nodup := by
    dsimp [rhs]
    exact (List.filter_sublist (p := matchesAt s p) (l := List.range (s.length + 1))).nodup List.nodup_range
  have hmem : ∀ i, i ∈ lhs ↔ i ∈ rhs := by
    intro i
    constructor
    · intro hi
      dsimp [lhs, cand] at hi
      simp only [List.mem_filter, List.mem_map] at hi
      rcases hi with ⟨⟨e, heQ, heq⟩, hRi⟩
      have hemem : e ∈ s.zipIdx := heQ.1
      have hidxlt : e.2 < s.length := by
        have hsnd := (List.mem_zipIdx hemem).2.1
        simpa using hsnd
      have hjle : j ≤ e.2 := by
        exact of_decide_eq_true ((Bool.and_eq_true_iff.mp heQ.2).2)
      have hsum : i + j = e.2 := by omega
      dsimp [rhs]
      rw [List.mem_filter]
      constructor
      · rw [List.mem_range]
        omega
      · simpa [hsum] using hRi
    · intro hi
      dsimp [rhs] at hi
      rw [List.mem_filter] at hi
      rcases hi with ⟨hirange, hRi⟩
      have hile : i ≤ s.length := by
        rw [List.mem_range] at hirange
        omega
      have ha := match_anchor s p j b hpj i hile hRi
      let e : UInt8 × Nat := (b, i + j)
      have hezip : e ∈ s.zipIdx := by
        dsimp [e]
        rw [List.mk_mem_zipIdx_iff_getElem?]
        exact ha.2
      have heQ : anchorQ b j e = true := by
        dsimp [anchorQ, e]
        simp [ha.1]
      dsimp [lhs, cand]
      rw [List.mem_filter]
      constructor
      · rw [List.mem_map]
        refine ⟨e, ?_, ?_⟩
        · rw [List.mem_filter]
          exact ⟨hezip, heQ⟩
        · dsimp [e]
          omega
      · exact hRi
  have hperm : List.Perm lhs rhs :=
    (List.perm_ext_iff_of_nodup hlhs_nodup hrhs_nodup).mpr hmem
  have heq := List.Perm.eq_of_pairwise (le := fun a c : Nat => a ≤ c)
    (by intro a c ha hc hac hca; exact Nat.le_antisymm hac hca)
    hlhs_pair hrhs_pair hperm
  change lhs = rhs
  exact heq

end InternalAnchorPositions

namespace InternalDeleteSignature

open TinylambOpt.ResearchGoals.BinaryText

private theorem mem_deletes_cons_zero (x : UInt8) (xs : Bytes) : xs ∈ deletes (x :: xs) := by
  unfold deletes
  simp only [List.length_cons, List.mem_map]
  refine ⟨0, by simp, ?_⟩
  simp

private theorem mem_deletes_cons_succ (x : UInt8) {xs s : Bytes} (h : s ∈ deletes xs) :
    x :: s ∈ deletes (x :: xs) := by
  unfold deletes at h ⊢
  simp only [List.length_cons, List.mem_map] at h ⊢
  rcases h with ⟨i, hi, rfl⟩
  refine ⟨i + 1, ?_, ?_⟩
  · simp at hi ⊢
    omega
  · simp [List.take_succ_cons, List.drop_succ_cons]

private def shares (a b : Bytes) : Prop :=
  ((a :: deletes a).any fun s => (b :: deletes b).contains s) = true

private theorem shares_self (a : Bytes) : shares a a := by
  simp [shares]

private theorem shares_of_mem {a b s : Bytes} (ha : s ∈ a :: deletes a)
    (hb : s ∈ b :: deletes b) : shares a b := by
  rw [shares, List.any_eq_true]
  exact ⟨s, ha, by simpa using hb⟩

private theorem mem_cons_lift (x : UInt8) (a s : Bytes) (h : s ∈ a :: deletes a) :
    x :: s ∈ (x :: a) :: deletes (x :: a) := by
  cases h with
  | head => simp
  | tail _ hs => exact List.Mem.tail (x :: a) (mem_deletes_cons_succ x hs)

private theorem shares_cons (x : UInt8) {a b : Bytes} (h : shares a b) : shares (x :: a) (x :: b) := by
  rw [shares, List.any_eq_true] at h ⊢
  rcases h with ⟨s, hs, hcontains⟩
  have hc : s ∈ b :: deletes b := by simpa using hcontains
  exact ⟨x :: s, mem_cons_lift x a s hs, by simpa using mem_cons_lift x b s hc⟩

theorem sound : TinylambOpt.ResearchGoals.BinaryText.DeleteSignature.Goal := by
  intro a
  induction a with
  | nil =>
      intro b hb
      cases b with
      | nil => exact shares_self []
      | cons y ys =>
          cases ys with
          | nil => simp [shares, deletes]
          | cons z zs => simp [near] at hb
  | cons x xs ih =>
      intro b hb
      cases b with
      | nil =>
          cases xs with
          | nil => simp [shares, deletes]
          | cons z zs => simp [near] at hb
      | cons y ys =>
          by_cases hxy : x == y
          · have hxy' : x = y := beq_iff_eq.mp hxy
            subst y
            simp [near] at hb
            exact shares_cons x (ih ys hb)
          · simp [near, hxy] at hb
            rcases hb with (hEq | hDel) | hIns
            · subst ys
              exact shares_of_mem (a := x :: xs) (b := y :: xs) (s := xs)
                (List.Mem.tail (x :: xs) (mem_deletes_cons_zero x xs))
                (List.Mem.tail (y :: xs) (mem_deletes_cons_zero y xs))
            · subst xs
              exact shares_of_mem (a := x :: y :: ys) (b := y :: ys) (s := y :: ys)
                (List.Mem.tail (x :: y :: ys) (mem_deletes_cons_zero x (y :: ys)))
                (List.Mem.head (deletes (y :: ys)))
            · subst ys
              exact shares_of_mem (a := x :: xs) (b := y :: x :: xs) (s := x :: xs)
                (List.Mem.head (deletes (x :: xs)))
                (List.Mem.tail (y :: x :: xs) (mem_deletes_cons_zero y (x :: xs)))

end InternalDeleteSignature

namespace InternalStripSort

open TinylambOpt.ResearchGoals.BinaryText

private def leB (a b : Bytes) : Bool := decide (a ≤ b)

private theorem leB_trans : ∀ a b c : Bytes, leB a b = true → leB b c = true → leB a c = true := by
  intro a b c hab hbc
  exact decide_eq_true (List.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))

private theorem leB_total : ∀ a b : Bytes, (leB a b || leB b a) = true := by
  intro a b
  by_cases h : a ≤ b
  · simp [leB, h]
  · have hba : b ≤ a := by
      have hlt : b < a := by simpa using h
      exact Std.Asymm.asymm (r := fun x y : Bytes => x < y) b a hlt
    simp [leB, h, hba]

private theorem le_append_left_iff (p s t : Bytes) : (p ++ s ≤ p ++ t) ↔ s ≤ t := by
  simpa using (List.append_left_le_iff (l₁ := p) (l₂ := s) (l₃ := t))

private theorem prefix_drop_append (p x : Bytes) (h : p.isPrefixOf x = true) :
    p ++ x.drop p.length = x := by
  have hp : p <+: x := List.isPrefixOf_iff_prefix.mp h
  rcases hp with ⟨t, rfl⟩
  simp

private theorem map_drop_append_eq (p : Bytes) : ∀ xs : List Bytes,
    (∀ x ∈ xs, p.isPrefixOf x = true) →
    (xs.map (List.drop p.length)).map (fun s => p ++ s) = xs := by
  intro xs hx
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons]
      have hx1 : p ++ x.drop p.length = x := prefix_drop_append p x (hx x (by simp))
      have htail : ∀ y ∈ xs, p.isPrefixOf y = true := by
        intro y hy
        exact hx y (by simp [hy])
      rw [hx1, ih htail]

private theorem sorted_of_leB {l : List Bytes}
    (h : l.Pairwise (fun a b => leB a b = true)) : l.Pairwise (fun a b => a ≤ b) :=
  h.imp (by intro a b hab; exact of_decide_eq_true hab)

private theorem leB_of_le {a b : Bytes} (h : a ≤ b) : leB a b = true := decide_eq_true h

private theorem antisymB (a b : Bytes) (h1 : a ≤ b) (h2 : b ≤ a) : a = b := by
  exact Std.Trichotomous.trichotomous (r := fun x y : Bytes => x < y) a b h2 h1

theorem sound : TinylambOpt.ResearchGoals.BinaryText.StripSort.Goal := by
  intro p xs hx
  change ((xs.map (List.drop p.length)).mergeSort leB).map (fun s => p ++ s) = xs.mergeSort leB
  let suffixes := xs.map (List.drop p.length)
  let restored := suffixes.mergeSort leB |>.map (fun s => p ++ s)
  have hrestore : ∀ x ∈ xs, p ++ x.drop p.length = x := by
    intro x hxm
    exact prefix_drop_append p x (hx x hxm)
  have hmap : (xs.map (List.drop p.length)).map (fun s => p ++ s) = xs :=
    map_drop_append_eq p xs hx
  have hperm_map : List.Perm ((xs.map (List.drop p.length)).map (fun s => p ++ s)) xs := by
    rw [hmap]
  have hperm1 : List.Perm restored xs := by
    dsimp [restored, suffixes]
    exact ((List.mergeSort_perm (xs.map (List.drop p.length)) leB).map (fun s => p ++ s)).trans hperm_map
  have hperm2 : List.Perm xs.mergeSort xs := by
    change List.Perm (xs.mergeSort leB) xs
    exact List.mergeSort_perm xs leB
  have hsorted1 : restored.Pairwise (fun a b => a ≤ b) := by
    dsimp [restored, suffixes]
    have hpair := List.pairwise_mergeSort leB_trans leB_total (xs.map (List.drop p.length))
    exact hpair.map (fun s => p ++ s) (by
      intro a b hab
      rw [le_append_left_iff]
      exact of_decide_eq_true hab)
  have hsorted2 : (xs.mergeSort).Pairwise (fun a b => a ≤ b) := by
    change (xs.mergeSort leB).Pairwise (fun a b => a ≤ b)
    exact sorted_of_leB (List.pairwise_mergeSort leB_trans leB_total xs)
  -- hperm1 relates restored to xs, while hsorted2 is about xs.mergeSort; use trans
  have hperm1' : List.Perm restored xs.mergeSort := hperm1.trans hperm2.symm
  have heq' := List.Perm.eq_of_pairwise (le := fun a b : Bytes => a ≤ b)
    (by intro a b ha hb hab hba; exact antisymB a b hab hba) hsorted1 hsorted2 hperm1'
  exact heq'

end InternalStripSort

namespace InternalAcrossChunks

open TinylambOpt.ResearchGoals.BinaryText

private theorem match_len_le (s p : Bytes) (hpne : p ≠ []) (i : Nat) (h : matchesAt s p i) :
    i + p.length ≤ s.length := by
  have hp : p <+: s.drop i := List.isPrefixOf_iff_prefix.mp h
  have hl := List.IsPrefix.length_le hp
  rw [List.length_drop] at hl
  have hpos : 0 < p.length := by
    cases p with
    | nil => contradiction
    | cons _ _ => simp
  have hsubpos : 0 < s.length - i := by omega
  have hi : i ≤ s.length := by omega
  omega

private theorem prefix_append_take_bool (p s t : Bytes) (k : Nat)
    (h : p.isPrefixOf (s ++ t) = true) (hk : p.length ≤ s.length + k) :
    p.isPrefixOf (s ++ t.take k) = true := by
  have hprop : p <+: s ++ t := List.isPrefixOf_iff_prefix.mp h
  rw [List.isPrefixOf_iff_prefix]
  have hp_eq : p = (s ++ t).take p.length := by
    rcases hprop with ⟨r, hr⟩
    rw [← hr]
    simp
  rw [hp_eq]
  by_cases hL : p.length ≤ s.length
  · rw [List.take_append_of_le_length hL]
    exact (List.take_prefix p.length s).trans (List.prefix_append s (t.take k))
  · rw [List.take_append]
    rw [List.take_of_length_le (by omega : s.length ≤ p.length)]
    rw [List.prefix_append_right_inj]
    have hsub : p.length - s.length ≤ k := by omega
    exact List.prefix_take_iff.mpr ⟨List.take_prefix (p.length - s.length) t, by
      rw [List.length_take]
      have hmin := Nat.min_le_left (p.length - s.length) t.length
      omega⟩

private theorem matches_append_of_le (a b p : Bytes) (i : Nat)
    (h : i + p.length ≤ a.length) :
    matchesAt (a ++ b) p i ↔ matchesAt a p i := by
  have hi : i ≤ a.length := by omega
  rw [matchesAt, matchesAt, List.drop_append_of_le_length hi]
  constructor
  · intro hm
    have hk : p.length ≤ (a.drop i).length + 0 := by
      simp [List.length_drop]
      omega
    simpa using (prefix_append_take_bool p (a.drop i) b 0 hm hk)
  · intro hm
    have hmprop := List.isPrefixOf_iff_prefix.mp hm
    exact List.isPrefixOf_iff_prefix.mpr (hmprop.trans (List.prefix_append _ _))

private theorem matches_shift (a b p : Bytes) (i : Nat) :
    matchesAt (a ++ b) p (i + a.length) ↔ matchesAt b p i := by
  have hge : a.length ≤ i + a.length := by omega
  have hdrop : (a ++ b).drop (i + a.length) = b.drop i := by
    rw [List.drop_append]
    rw [List.drop_eq_nil_of_le hge]
    simp
  simp [matchesAt, hdrop]

private theorem cross_core (a b p : Bytes) (hp : p ≠ []) (j : Nat)
    (hj : j < (a.drop (a.length - (p.length - 1))).length) :
    matchesAt (a ++ b) p ((a.length - (p.length - 1)) + j) ↔
      matchesAt (a.drop (a.length - (p.length - 1)) ++ b.take (p.length - 1)) p j := by
  have hk : p.length = (p.length - 1) + 1 := by
    have hpos : 0 < p.length := by
      cases p with
      | nil => contradiction
      | cons _ _ => simp
    omega
  have hstartle : a.length - (p.length - 1) ≤ a.length := Nat.sub_le _ _
  have hsum : (a.length - (p.length - 1)) + (a.drop (a.length - (p.length - 1))).length = a.length := by
    rw [List.length_drop]
    omega
  have hdropa : a.drop ((a.length - (p.length - 1)) + j) =
      (a.drop (a.length - (p.length - 1))).drop j := by
    rw [List.drop_drop]
  have hstartj : (a.length - (p.length - 1)) + j ≤ a.length := by omega
  have hdropab : (a ++ b).drop ((a.length - (p.length - 1)) + j) =
      (a.drop (a.length - (p.length - 1))).drop j ++ b := by
    rw [List.drop_append_of_le_length hstartj]
    rw [hdropa]
  have hdropfringe : (a.drop (a.length - (p.length - 1)) ++ b.take (p.length - 1)).drop j =
      (a.drop (a.length - (p.length - 1))).drop j ++ b.take (p.length - 1) := by
    apply List.drop_append_of_le_length
    simpa [List.length_drop] using (Nat.le_of_lt hj)
  simp only [matchesAt]
  rw [hdropab, hdropfringe]
  constructor
  · intro hm
    have hk' : p.length ≤ ((a.drop (a.length - (p.length - 1))).drop j).length + (p.length - 1) := by
      simp only [List.length_drop]
      omega
    exact prefix_append_take_bool p ((a.drop (a.length - (p.length - 1))).drop j) b (p.length - 1) hm hk'
  · intro hm
    have hmprop := List.isPrefixOf_iff_prefix.mp hm
    have htakepref : (a.drop (a.length - (p.length - 1))).drop j ++ b.take (p.length - 1) <+:
        (a.drop (a.length - (p.length - 1))).drop j ++ b := by
      rw [List.prefix_append_right_inj]
      exact List.take_prefix (p.length - 1) b
    exact List.isPrefixOf_iff_prefix.mpr (hmprop.trans htakepref)

private theorem length_filter_add_length_filter_not {α : Type} (l : List α) (Q : α → Bool) :
    (l.filter Q).length + (l.filter (fun x => !Q x)).length = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hq : Q x = true
      · simp [List.filter_cons, hq]
        omega
      · simp [List.filter_cons, hq]
        omega

private theorem nodup_of_pairwise_lt {l : List Nat} (h : l.Pairwise (fun a b => a < b)) : l.Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      apply List.nodup_cons.mpr
      constructor
      · intro hxmem
        have hlt := (List.pairwise_cons.mp h).1 x hxmem
        omega
      · exact ih (List.Pairwise.of_cons h)

private theorem matches_at_n_false (a p : Bytes) (hpne : p ≠ []) :
    matchesAt a p a.length = false := by
  rw [matchesAt]
  have hdrop : a.drop a.length = [] := List.drop_eq_nil_of_le (Nat.le_refl _)
  rw [hdrop]
  cases p with
  | nil => contradiction
  | cons _ _ => simp [List.isPrefixOf]

theorem sound : TinylambOpt.ResearchGoals.BinaryText.AcrossChunks.Goal := by
  intro a b p hpne
  let n := a.length
  let m := p.length
  let k := m - 1
  let start := n - k
  let cut := a.drop start
  let c := cut.length
  let fringe := cut ++ b.take k
  have hmpos : 0 < m := by
    dsimp [m]
    cases p with
    | nil => contradiction
    | cons _ _ => simp
  have hk : k + 1 = m := by dsimp [k]; omega
  have hsum : start + c = n := by
    dsimp [start, c, cut, n, k]
    rw [List.length_drop]
    omega
  have hlenab : (a ++ b).length + 1 = n + (b.length + 1) := by
    simp [n, Nat.add_assoc]
  have hfull : List.range ((a ++ b).length + 1) =
      (List.range n) ++ (List.range (b.length + 1)).map (fun i => i + n) := by
    rw [hlenab]
    simpa [Nat.add_assoc, Nat.add_comm] using (List.range_add (n := n) (m := b.length + 1))
  have hB : (((List.range (b.length + 1)).map (fun i => i + n)).filter
      (matchesAt (a ++ b) p)).length = occurrences b p := by
    rw [List.filter_map]
    have hc : (List.range (b.length + 1)).filter ((matchesAt (a ++ b) p) ∘ fun i => i + n) =
        (List.range (b.length + 1)).filter (matchesAt b p) := by
      apply List.filter_congr
      intro i hi
      dsimp [Function.comp]
      exact Bool.eq_iff_iff.mpr (matches_shift a b p i)
    rw [hc, List.length_map]
    dsimp [occurrences, n]
  let pa := (List.range n).filter (matchesAt (a ++ b) p)
  have hnotfilter : pa.filter (fun i => !decide (i + m ≤ n)) =
      pa.filter (fun i => n < i + m) := by
    apply List.filter_congr
    intro i hi
    by_cases hle : i + m ≤ n
    · have hnlt : ¬ n < i + m := Nat.not_lt_of_ge hle
      simp [hle, hnlt]
    · have hnlt : n < i + m := Nat.lt_of_not_ge hle
      simp [hle, hnlt]
  have hpartlen : pa.length = (pa.filter (fun i => i + m ≤ n)).length +
      (pa.filter (fun i => n < i + m)).length := by
    have h := length_filter_add_length_filter_not pa (fun i => i + m ≤ n)
    rw [hnotfilter] at h
    exact h.symm
  have hA0 : pa.filter (fun i => i + m ≤ n) = (List.range n).filter (matchesAt a p) := by
    dsimp [pa]
    rw [List.filter_filter]
    apply List.filter_congr
    intro i hi
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro h
      rcases Bool.and_eq_true_iff.mp h with ⟨hleB, hmab⟩
      have hle : i + m ≤ n := of_decide_eq_true hleB
      have hm := matches_append_of_le a b p i (by simpa [n, m] using hle)
      exact hm.mp hmab
    · intro h
      have hle := match_len_le a p hpne i h
      have hm := matches_append_of_le a b p i (by simpa [n, m] using hle)
      apply Bool.and_eq_true_iff.mpr
      exact ⟨decide_eq_true hle, hm.mpr h⟩
  have hA0len : ((List.range n).filter (matchesAt a p)).length = occurrences a p := by
    dsimp [occurrences, n]
    rw [List.range_succ, List.filter_append]
    simp [matches_at_n_false a p hpne]
  let crossList := (List.range c).filter (fun j => c < j + m && matchesAt fringe p j)
  let crossing := pa.filter (fun i => n < i + m)
  have hcross : crossing = crossList.map (fun j => start + j) := by
    dsimp [crossing, crossList, pa]
    apply List.Perm.eq_of_pairwise (le := fun x y : Nat => x ≤ y)
      (by intro x y hx hy hxy hyx; exact Nat.le_antisymm hxy hyx)
    · have hp1 : (pa.filter (fun i => n < i + m)).Pairwise (fun x y => x < y) := by
        dsimp [pa]
        exact (List.Pairwise.filter (fun i : Nat => n < i + m)
          (List.Pairwise.filter (matchesAt (a ++ b) p) List.pairwise_lt_range))
      exact hp1.imp (by intro x y h; exact Nat.le_of_lt h)
    · have hp2 : (crossList.map (fun j => start + j)).Pairwise (fun x y => x < y) := by
        have hc1 : crossList.Pairwise (fun x y => x < y) := by
          dsimp [crossList]
          exact List.Pairwise.filter (fun j : Nat => c < j + m && matchesAt fringe p j)
            List.pairwise_lt_range
        exact hc1.map (fun j => start + j) (by intro x y h; omega)
      exact hp2.imp (by intro x y h; exact Nat.le_of_lt h)
    · have hp1 : (pa.filter (fun i => n < i + m)).Pairwise (fun x y => x < y) := by
        dsimp [pa]
        exact List.Pairwise.filter (fun i : Nat => n < i + m)
          (List.Pairwise.filter (matchesAt (a ++ b) p) List.pairwise_lt_range)
      have hp2 : (crossList.map (fun j => start + j)).Pairwise (fun x y => x < y) := by
        have hc1 : crossList.Pairwise (fun x y => x < y) := by
          dsimp [crossList]
          exact List.Pairwise.filter (fun j : Nat => c < j + m && matchesAt fringe p j)
            List.pairwise_lt_range
        exact hc1.map (fun j => start + j) (by intro x y h; omega)
      apply (List.perm_ext_iff_of_nodup (nodup_of_pairwise_lt hp1)
        (nodup_of_pairwise_lt hp2)).mpr
      intro i
      constructor
      · intro hi
        have hi' := List.mem_filter.mp hi
        have hirange := List.mem_range.mp ((List.mem_filter.mp hi'.1).1)
        have hcrossCond : n < i + m := of_decide_eq_true hi'.2
        have histart : start ≤ i := by
          dsimp [start, n, k, m] at *
          omega
        let j := i - start
        have hjlt : j < c := by
          dsimp [j]
          have := hsum
          omega
        have hjl : c < j + m := by
          dsimp [j]
          have := hsum
          omega
        have hmfringe : matchesAt fringe p j = true := (cross_core a b p hpne j (by simpa [cut, c, start] using hjlt)).mp (by
          have hadd : start + j = i := by dsimp [j]; omega
          rw [hadd]
          exact (List.mem_filter.mp hi'.1).2)
        rw [List.mem_map]
        refine ⟨j, ?_, ?_⟩
        · rw [List.mem_filter]
          exact ⟨List.mem_range.mpr hjlt, by simp [hjl, hmfringe]⟩
        · dsimp [j]
          omega
      · intro hi
        rcases List.mem_map.mp hi with ⟨j, hjmem, hji⟩
        have hj' := List.mem_filter.mp hjmem
        have hjrange := List.mem_range.mp hj'.1
        have hcond : c < j + m := of_decide_eq_true ((Bool.and_eq_true_iff.mp hj'.2).1)
        have hmfr : matchesAt fringe p j = true := (Bool.and_eq_true_iff.mp hj'.2).2
        rw [List.mem_filter]
        constructor
        · rw [List.mem_filter]
          constructor
          · rw [List.mem_range]
            omega
          · have hmab := (cross_core a b p hpne j hjrange).mpr hmfr
            rw [← hji]
            exact hmab
        · have : n < (start + j) + m := by omega
          rw [← hji]
          exact decide_eq_true this
  have hcrosslen : crossing.length = crossList.length := by rw [hcross, List.length_map]
  have hA : pa.length = occurrences a p + crossList.length := by
    rw [hpartlen, hA0, hA0len, hcrosslen]
  dsimp [occurrences]
  rw [hfull, List.filter_append, List.length_append]
  rw [show (List.range n).filter (matchesAt (a ++ b) p) = pa by rfl]
  rw [hA, hB]
  dsimp [occurrences]
  simp [fringe, crossList, cut, c, start, k, m, n]
  omega

end InternalAcrossChunks

namespace InternalTupleEncoding

open TinylambOpt.ResearchGoals.BinaryText

private def esc (s : Bytes) : Bytes := s.flatMap fun b => if b == 0 then [0,255] else [b]

private theorem byte_pos_of_ne_zero (x : UInt8) (h : x ≠ 0) : (0 : UInt8) < x := by
  have hpos : ∀ z : UInt8, z ≠ 0 → (0 : UInt8) < z := by native_decide
  exact hpos x h

private theorem byte_cmp (x y : UInt8) :
    compare (if x == 0 then ([0,255] : Bytes) else [x])
      (if y == 0 then ([0,255] : Bytes) else [y]) = compare x y := by
  by_cases hx : x == 0
  · by_cases hy : y == 0
    · have hx0 : x = 0 := beq_iff_eq.mp hx
      have hy0 : y = 0 := beq_iff_eq.mp hy
      subst x; subst y
      simp
    · have hx0 : x = 0 := beq_iff_eq.mp hx
      subst x
      have hy0 : y ≠ 0 := by intro h; apply hy; simpa [h]
      have hylt : (0 : UInt8) < y := byte_pos_of_ne_zero y hy0
      have hcmp : compare (0 : UInt8) y = .lt := Std.compare_eq_lt.mpr hylt
      simp [hy, hcmp]
  · by_cases hy : y == 0
    · have hy0 : y = 0 := beq_iff_eq.mp hy
      subst y
      have hx0 : x ≠ 0 := by intro h; apply hx; simpa [h]
      have hxlt : (0 : UInt8) < x := byte_pos_of_ne_zero x hx0
      have hcmp : compare x (0 : UInt8) = .gt := Std.compare_eq_gt.mpr hxlt
      simp [hx, hcmp]
    · simp [hx, hy]

private theorem esc_prefix_inj (x y : UInt8)
    (h : (if x == 0 then ([0,255] : Bytes) else [x]) <+:
         (if y == 0 then ([0,255] : Bytes) else [y])) : x = y := by
  by_cases hx : x == 0 <;> by_cases hy : y == 0
  · have hx0 : x = 0 := beq_iff_eq.mp hx
    have hy0 : y = 0 := beq_iff_eq.mp hy
    simpa [hx0, hy0]
  · have hx0 : x = 0 := beq_iff_eq.mp hx
    subst x
    have hy0 : y ≠ 0 := by intro h'; apply hy; simpa [h']
    simp [hy0] at h
  · have hy0 : y = 0 := beq_iff_eq.mp hy
    subst y
    have hx0 : x ≠ 0 := by intro h'; apply hx; simpa [h']
    simp [hx0] at h
  · have hx0 : x ≠ 0 := by intro h'; apply hx; simpa [h']
    have hy0 : y ≠ 0 := by intro h'; apply hy; simpa [h']
    simpa [hx0, hy0] using h

private theorem esc_cmp (a c : Bytes) : compare (esc a) (esc c) = compare a c := by
  unfold esc
  apply List.compare_flatMap_flatMap
  · exact byte_cmp
  · exact esc_prefix_inj
  · intro x
    by_cases hx : x == 0 <;> simp [hx]

private theorem compare_append_of_not_prefix {l₁ l₂ l₃ l₄ : Bytes}
    (h₁ : ¬ l₁ <+: l₂) (h₂ : ¬ l₂ <+: l₁) :
    compare (l₁ ++ l₃) (l₂ ++ l₄) = compare l₁ l₂ := by
  induction l₁ generalizing l₂ with
  | nil => exact False.elim (h₁ List.nil_prefix)
  | cons x xs ih =>
      cases l₂ with
      | nil => exact False.elim (h₂ List.nil_prefix)
      | cons y ys =>
          simp only [List.cons_append, List.compareLex_cons_cons]
          cases hxy : compare x y with
          | eq =>
              have hxy' : x = y := Std.LawfulEqOrd.compare_eq_iff_eq.mp hxy
              subst y
              have h₁' : ¬ xs <+: ys := by simpa using h₁
              have h₂' : ¬ ys <+: xs := by simpa using h₂
              simpa [hxy] using (ih h₁' h₂')
          | lt => simp [hxy]
          | gt => simp [hxy]

private theorem field_cmp (a c : Bytes) : compare (field a) (field c) = compare a c := by
  induction a generalizing c with
  | nil =>
      cases c with
      | nil => simp [field, esc]
      | cons y ys =>
          by_cases hy : y == 0
          · have hy0 : y = 0 := beq_iff_eq.mp hy
            subst y
            have h255 : compare (0 : UInt8) 255 = .lt := by native_decide
            simp [field, esc, h255]
          · have hy0 : y ≠ 0 := by intro h; apply hy; simpa [h]
            have hylt : (0 : UInt8) < y := byte_pos_of_ne_zero y hy0
            have hcmp : compare (0 : UInt8) y = .lt := Std.compare_eq_lt.mpr hylt
            simp [field, esc, hy0, hcmp]
  | cons x xs ih =>
      cases c with
      | nil =>
          by_cases hx : x == 0
          · have hx0 : x = 0 := beq_iff_eq.mp hx
            subst x
            have h255 : compare (255 : UInt8) 0 = .gt := by native_decide
            simp [field, esc, h255]
          · have hx0 : x ≠ 0 := by intro h; apply hx; simpa [h]
            have hxlt : (0 : UInt8) < x := byte_pos_of_ne_zero x hx0
            have hcmp : compare x (0 : UInt8) = .gt := Std.compare_eq_gt.mpr hxlt
            simp [field, esc, hx0, hcmp]
      | cons y ys =>
          by_cases hxy : x == y
          · have hxy' : x = y := beq_iff_eq.mp hxy
            subst y
            have happ := List.compare_append_append_of_length_eq
              (l₁ := if x == 0 then ([0,255] : Bytes) else [x])
              (l₂ := if x == 0 then ([0,255] : Bytes) else [x])
              (l₃ := esc xs ++ [0,0]) (l₄ := esc ys ++ [0,0]) rfl
            have ih' : compare (esc xs ++ [0,0]) (esc ys ++ [0,0]) = compare xs ys := by
              simpa [field, esc] using ih ys
            rw [ih'] at happ
            simpa [field, esc] using happ
          · have hxyne : x ≠ y := by intro h; apply hxy; simpa [h]
            have hnot1 : ¬ (if x == 0 then ([0,255] : Bytes) else [x]) <+:
                (if y == 0 then ([0,255] : Bytes) else [y]) := by
              intro h
              exact hxyne (esc_prefix_inj x y h)
            have hnot2 : ¬ (if y == 0 then ([0,255] : Bytes) else [y]) <+:
                (if x == 0 then ([0,255] : Bytes) else [x]) := by
              intro h
              exact hxyne (esc_prefix_inj y x h).symm
            have happ := compare_append_of_not_prefix
              (l₃ := esc xs ++ [0,0]) (l₄ := esc ys ++ [0,0]) hnot1 hnot2
            cases hcxy : compare x y with
            | eq => exact False.elim (hxyne (Std.LawfulEqOrd.compare_eq_iff_eq.mp hcxy))
            | lt =>
                have hb := byte_cmp x y
                rw [hb, hcxy] at happ
                simpa [field, esc, hcxy] using happ
            | gt =>
                have hb := byte_cmp x y
                rw [hb, hcxy] at happ
                simpa [field, esc, hcxy] using happ

private theorem field_prefix_inj : ∀ (a c : Bytes), field a <+: field c → a = c := by
  intro a
  induction a with
  | nil =>
      intro c h
      cases c with
      | nil => rfl
      | cons y ys =>
          by_cases hy : y == 0
          · have hy0 : y = 0 := beq_iff_eq.mp hy
            subst y
            cases ys with
            | nil => simp [field, esc] at h
            | cons z zs =>
                by_cases hz : z == 0 <;> simp [field, esc, hz] at h
          · have hy0 : y ≠ 0 := by intro h'; apply hy; simpa [h']
            simp [field, esc, hy0] at h
            exact False.elim (hy0 h.1.symm)
  | cons x xs ih =>
      intro c h
      cases c with
      | nil =>
          have hlen := List.IsPrefix.length_le h
          simp [field, esc] at hlen
          by_cases hxeq : x = 0
          · subst x
            simp at hlen
            omega
          · have hxbool : ¬ x == 0 := by intro hh; exact hxeq (beq_iff_eq.mp hh)
            simp [hxeq] at hlen
            omega
      | cons y ys =>
          by_cases hx : x == 0
          · have hx0 : x = 0 := beq_iff_eq.mp hx
            subst x
            by_cases hy : y == 0
            · have hy0 : y = 0 := beq_iff_eq.mp hy
              subst y
              simp [field, esc] at h
              have h' : field xs <+: field ys := by simpa [field, esc] using h
              exact congrArg (fun t => 0 :: t) (ih ys h')
            · have hy0 : y ≠ 0 := by intro h'; apply hy; simpa [h']
              simp [field, esc, hy0] at h
              exact False.elim (hy0 h.1.symm)
          · have hx0 : x ≠ 0 := by intro h'; apply hx; simpa [h']
            by_cases hy : y == 0
            · have hy0 : y = 0 := beq_iff_eq.mp hy
              subst y
              simp [field, esc, hx0] at h
            · have hy0 : y ≠ 0 := by intro h'; apply hy; simpa [h']
              by_cases hxy : x == y
              · have hxy' : x = y := beq_iff_eq.mp hxy
                subst y
                simp [field, esc, hx0] at h ⊢
                have h' : field xs <+: field ys := by simpa [field, esc] using h
                exact ih ys h'
              · simp [field, esc, hx0, hy0, hxy] at h
                have hxy_eq : x = y := h.1
                have h2 := h.2
                subst y
                have h' : field xs <+: field ys := by simpa [field, esc] using h2
                exact congrArg (fun t => x :: t) (ih ys h')

theorem sound : TinylambOpt.ResearchGoals.BinaryText.TupleEncoding.Goal := by
  intro a b c d
  cases hcmp : compare a c with
  | eq =>
      have hac : a = c := Std.LawfulEqOrd.compare_eq_iff_eq.mp hcmp
      subst c
      have happ := List.compare_append_append_of_length_eq
        (l₁ := field a) (l₂ := field a) (l₃ := field b) (l₄ := field d) rfl
      have hbd := field_cmp b d
      rw [hbd] at happ
      simpa using happ
  | lt =>
      have hf : compare (field a) (field c) = .lt := by rw [field_cmp, hcmp]
      have hnot1 : ¬ field a <+: field c := by
        intro h
        have hac := field_prefix_inj a c h
        subst c
        simp at hcmp
      have hnot2 : ¬ field c <+: field a := by
        intro h
        have hac := field_prefix_inj c a h
        subst c
        simp at hcmp
      have happ := compare_append_of_not_prefix (l₃ := field b) (l₄ := field d) hnot1 hnot2
      simpa [hf] using happ
  | gt =>
      have hf : compare (field a) (field c) = .gt := by rw [field_cmp, hcmp]
      have hnot1 : ¬ field a <+: field c := by
        intro h
        have hac := field_prefix_inj a c h
        subst c
        simp at hcmp
      have hnot2 : ¬ field c <+: field a := by
        intro h
        have hac := field_prefix_inj c a h
        subst c
        simp at hcmp
      have happ := compare_append_of_not_prefix (l₃ := field b) (l₄ := field d) hnot1 hnot2
      simpa [hf] using happ

end InternalTupleEncoding

-- TODO(PrefixCardinality): the counting identity for sorted dictionaries is still under construction; no compiling proof obtained in this session.

theorem PrefixFence_sound : TinylambOpt.ResearchGoals.BinaryText.PrefixFence.Goal := InternalPrefixFence.sound
theorem SharedPrefixCompare_sound : TinylambOpt.ResearchGoals.BinaryText.SharedPrefixCompare.Goal := InternalSharedPrefixCompare.sound
theorem AnchorPositions_sound : TinylambOpt.ResearchGoals.BinaryText.AnchorPositions.Goal := InternalAnchorPositions.sound
theorem DeleteSignature_sound : TinylambOpt.ResearchGoals.BinaryText.DeleteSignature.Goal := InternalDeleteSignature.sound
theorem StripSort_sound : TinylambOpt.ResearchGoals.BinaryText.StripSort.Goal := InternalStripSort.sound
theorem AcrossChunks_sound : TinylambOpt.ResearchGoals.BinaryText.AcrossChunks.Goal := InternalAcrossChunks.sound
theorem TupleEncoding_sound : TinylambOpt.ResearchGoals.BinaryText.TupleEncoding.Goal := InternalTupleEncoding.sound

end TinylambOpt.ResearchProofs.Wave7BinaryText