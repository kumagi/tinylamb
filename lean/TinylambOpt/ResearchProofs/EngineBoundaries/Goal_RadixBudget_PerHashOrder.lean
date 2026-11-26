import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_PerHashOrder

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

private theorem bucket_filter_comm (bit : Nat) (v : Bool) (h : Nat) (rs : Batch) :
    (bucket bit v rs).filter (fun r => r.hash == h) =
      (rs.filter (fun r => r.hash == h)).filter (fun r => r.hash.testBit bit == v) := by
  induction rs with
  | nil => simp [bucket]
  | cons r rs ih => simp [bucket, Bool.and_comm]

private theorem filter_bit_of_hash (bit : Nat) (v : Bool) (h : Nat) (rs : Batch) :
    (rs.filter (fun r => r.hash == h)).filter (fun r => r.hash.testBit bit == v) =
      if h.testBit bit == v then rs.filter (fun r => r.hash == h) else [] := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases hr : r.hash == h
      · have hreq : r.hash = h := beq_iff_eq.mp hr
        have hsame : r.hash.testBit bit = h.testBit bit := by rw [hreq]
        by_cases ht : h.testBit bit == v
        · have ht' : r.hash.testBit bit == v := by simpa [hsame] using ht
          simp [hr, ht, ht', ih]
        · have ht' : (r.hash.testBit bit == v) = false := by
            by_cases hx : r.hash.testBit bit == v <;> simp_all
          simp [hr, ht, ht', ih]
      · simp [hr, ih]

private theorem bucket_filter_hash (bit : Nat) (v : Bool) (rs : Batch) (h : Nat) :
    (bucket bit v rs).filter (fun r => r.hash == h) =
      if h.testBit bit == v then rs.filter (fun r => r.hash == h) else [] := by
  rw [bucket_filter_comm, filter_bit_of_hash]

private theorem flatMap_filter (xs : List Leaf) (P : Row → Bool) :
    (xs.flatMap Leaf.rows).filter P = xs.flatMap (fun l => l.rows.filter P) := by
  induction xs with
  | nil => simp
  | cons l ls ih => simp [ih, List.filter_append]

private theorem mem_rows_of_any (xs : List Leaf) (h : Nat) (l : Leaf)
    (hl : l ∈ xs) (hany : l.rows.any (fun r => r.hash == h) = true) :
    ∃ r, r ∈ xs.flatMap Leaf.rows ∧ (r.hash == h) = true := by
  rcases (List.any_eq_true.mp hany) with ⟨r, hrl, hrh⟩
  exact ⟨r, List.mem_flatMap_of_mem hl hrl, hrh⟩

private theorem singleton_filter_le_one (l : Leaf) (P : Leaf → Bool) :
    ([l].filter P).length ≤ 1 := by
  simp [List.filter_cons]
  split <;> simp

private theorem no_hash_leaves (xs : List Leaf) (h : Nat)
    (hnone : (xs.flatMap Leaf.rows).filter (fun r => r.hash == h) = []) :
    (xs.filter (fun l => l.rows.any (fun r => r.hash == h))).length = 0 := by
  induction xs with
  | nil => simp
  | cons l ls ih =>
      have hnot : l.rows.any (fun r => r.hash == h) ≠ true := by
        intro hany
        rcases mem_rows_of_any (l :: ls) h l (by simp) hany with ⟨r, hrmem, hrh⟩
        have hrf : r ∈ ((l :: ls).flatMap Leaf.rows).filter (fun r => r.hash == h) :=
          List.mem_filter.mpr ⟨hrmem, hrh⟩
        rw [hnone] at hrf
        simp at hrf
      have hanyfalse : l.rows.any (fun r => r.hash == h) = false := by
        cases hb : l.rows.any (fun r => r.hash == h) <;> simp_all
      have happ : (l.rows.filter (fun r => r.hash == h)) ++
          (ls.flatMap Leaf.rows).filter (fun r => r.hash == h) = [] := by
        simpa [flatMap_filter, List.filter_append] using hnone
      have htail : (ls.flatMap Leaf.rows).filter (fun r => r.hash == h) = [] :=
        (List.eq_nil_of_append_eq_nil happ).2
      simp [hanyfalse, ih htail]

private theorem side_spec (b d : Nat) (rs : Batch) (h : Nat) (side : Bool)
    (ih : ∀ (rs : Batch) (h : Nat),
      ((leaves b d rs).flatMap Leaf.rows).filter (fun r => r.hash == h) =
          rs.filter (fun r => r.hash == h) ∧
      ((leaves b d rs).filter (fun l => l.rows.any (fun r => r.hash == h))).length ≤ 1) :
    let child := bucket d side rs
    let sub := if child.isEmpty then [] else
      if b < size child && stagnant (size child) (size rs) then [⟨child, true⟩]
      else leaves b d child
    ((sub.flatMap Leaf.rows).filter (fun r => r.hash == h) = child.filter (fun r => r.hash == h)) ∧
    ((sub.filter (fun l => l.rows.any (fun r => r.hash == h))).length ≤ 1) := by
  dsimp
  by_cases he : (bucket d side rs).isEmpty = true
  · have hnil : bucket d side rs = [] := by simpa using he
    simp [hnil]
  · by_cases hs : (b < size (bucket d side rs) && stagnant (size (bucket d side rs)) (size rs)) = true
    · constructor
      · simp [he, hs]
      · simp [he, hs, singleton_filter_le_one]
    · simpa [he, hs] using ih (bucket d side rs) h

theorem sound : PerHashOrder.Goal := by
  intro b d
  induction d with
  | zero =>
      intro rs h
      constructor
      · simp [leaves]
      · simp [leaves, singleton_filter_le_one]
  | succ d ih =>
      intro rs h
      by_cases hsmall : size rs ≤ b
      · constructor
        · simp [leaves, hsmall]
        · simp [leaves, hsmall, singleton_filter_le_one]
      · simp only [leaves, hsmall, ite_false, List.flatMap_cons, List.flatMap_nil, List.append_nil]
        let F : Bool → List Leaf := fun side =>
          let child := bucket d side rs
          if child.isEmpty then [] else
          if b < size child && stagnant (size child) (size rs) then [⟨child, true⟩]
          else leaves b d child
        change (((F false ++ F true).flatMap Leaf.rows).filter (fun r => r.hash == h) =
            rs.filter (fun r => r.hash == h)) ∧
          (((F false ++ F true).filter (fun l => l.rows.any (fun r => r.hash == h))).length ≤ 1)
        have hFs := side_spec b d rs h false ih
        have hFt := side_spec b d rs h true ih
        dsimp [F] at hFs hFt
        have hfilt := bucket_filter_hash d false rs h
        have hfiltT := bucket_filter_hash d true rs h
        have hFrows : ((F false).flatMap Leaf.rows).filter (fun r => r.hash == h) =
            (bucket d false rs).filter (fun r => r.hash == h) := by
          simpa [F] using hFs.1
        have hTrows : ((F true).flatMap Leaf.rows).filter (fun r => r.hash == h) =
            (bucket d true rs).filter (fun r => r.hash == h) := by
          simpa [F] using hFt.1
        have hFleaves : ((F false).filter (fun l => l.rows.any (fun r => r.hash == h))).length ≤ 1 := by
          simpa [F] using hFs.2
        have hTleaves : ((F true).filter (fun l => l.rows.any (fun r => r.hash == h))).length ≤ 1 := by
          simpa [F] using hFt.2
        by_cases hb : h.testBit d = false
        · have hf_eq : (bucket d false rs).filter (fun r => r.hash == h) = rs.filter (fun r => r.hash == h) := by
            simpa [hb] using hfilt
          have ht_eq : (bucket d true rs).filter (fun r => r.hash == h) = [] := by
            have htbit : (h.testBit d == true) = false := by simp [hb]
            simpa [htbit] using hfiltT
          constructor
          · calc
              ((F false ++ F true).flatMap Leaf.rows).filter (fun r => r.hash == h)
                  = ((F false).flatMap Leaf.rows).filter (fun r => r.hash == h) ++
                    ((F true).flatMap Leaf.rows).filter (fun r => r.hash == h) := by
                      simp [List.filter_append]
              _ = (bucket d false rs).filter (fun r => r.hash == h) ++
                    (bucket d true rs).filter (fun r => r.hash == h) := by rw [hFrows, hTrows]
              _ = rs.filter (fun r => r.hash == h) := by rw [hf_eq, ht_eq]; simp
          · have htn : ((F true).filter (fun l => l.rows.any (fun r => r.hash == h))).length = 0 :=
              no_hash_leaves (F true) h (by rw [hTrows, ht_eq])
            rw [List.filter_append]
            simp [htn, hFleaves]
        · have hbtrue : h.testBit d = true := by
            cases hx : h.testBit d <;> simp_all
          have hf_eq : (bucket d false rs).filter (fun r => r.hash == h) = [] := by
            have hfbit : (h.testBit d == false) = false := by simp [hbtrue]
            simpa [hfbit] using hfilt
          have ht_eq : (bucket d true rs).filter (fun r => r.hash == h) = rs.filter (fun r => r.hash == h) := by
            simpa [hbtrue] using hfiltT
          constructor
          · calc
              ((F false ++ F true).flatMap Leaf.rows).filter (fun r => r.hash == h)
                  = ((F false).flatMap Leaf.rows).filter (fun r => r.hash == h) ++
                    ((F true).flatMap Leaf.rows).filter (fun r => r.hash == h) := by
                      simp [List.filter_append]
              _ = (bucket d false rs).filter (fun r => r.hash == h) ++
                    (bucket d true rs).filter (fun r => r.hash == h) := by rw [hFrows, hTrows]
              _ = rs.filter (fun r => r.hash == h) := by rw [hf_eq, ht_eq]; simp
          · have hfn : ((F false).filter (fun l => l.rows.any (fun r => r.hash == h))).length = 0 :=
              no_hash_leaves (F false) h (by rw [hFrows, hf_eq])
            rw [List.filter_append]
            simp [hfn, hTleaves]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_PerHashOrder
