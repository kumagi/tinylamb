import TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

/-! Every accepted wire prefix is canonical.

A successful field decode records only zero padding and a length marker that
match the raw bytes; re-encoding the recovered bytes reproduces the consumed
wire exactly. SQL execution may trust the decoder for index-boundary checks
without accepting noncanonical aliases of the same key. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_AcceptedKeyPrefixCanonical

open TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

private theorem groups_short_none (initial : Bool) (l : Bytes) (h : l.length < 9) : groups initial l = none := by
  cases l with
  | nil => simp [groups]
  | cons a l =>
    cases l with
    | nil => simp [groups]
    | cons b l =>
      cases l with
      | nil => simp [groups]
      | cons c l =>
        cases l with
        | nil => simp [groups]
        | cons d l =>
          cases l with
          | nil => simp [groups]
          | cons e l =>
            cases l with
            | nil => simp [groups]
            | cons f l =>
              cases l with
              | nil => simp [groups]
              | cons g l =>
                cases l with
                | nil => simp [groups]
                | cons h8 l =>
                  cases l with
                  | nil => simp [groups]
                  | cons i l =>
                    simp at h
                    omega

private theorem groups_cont (initial : Bool) (a b c d e f g h : UInt8) (rest : Bytes) :
    groups initial ([a,b,c,d,e,f,g,h,9] ++ rest) =
      (groups false rest).bind (fun p => some ([a,b,c,d,e,f,g,h] ++ p.1, p.2)) := by
  simp [groups]



private theorem body_append8 (block more : Bytes) (hblock : block.length = 8) (hmore : more ≠ []) :
    body (block ++ more) = block ++ [9] ++ body more := by
  rcases List.exists_cons_of_length_eq_add_one (by omega : block.length = 7 + 1) with ⟨a, t, rfl⟩
  simp at hblock
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 6 + 1) with ⟨b, t, rfl⟩
  simp at hblock
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 5 + 1) with ⟨c, t, rfl⟩
  simp at hblock
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 4 + 1) with ⟨d, t, rfl⟩
  simp at hblock
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 3 + 1) with ⟨e, t, rfl⟩
  simp at hblock
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 2 + 1) with ⟨f, t, rfl⟩
  simp at hblock
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 1 + 1) with ⟨g, t, rfl⟩
  simp at hblock
  rcases List.exists_cons_of_length_eq_add_one (by omega : t.length = 0 + 1) with ⟨h, t, rfl⟩
  simp at hblock
  have ht : t = [] := by omega
  subst t
  cases more with
  | nil => contradiction
  | cons i rest => simp [body]


private theorem body_short (s : Bytes) (h : s.length ≤ 8) :
    body s = s ++ List.replicate (8 - s.length) 0 ++ [s.length.toUInt8] := by
  cases s with
  | nil => simp [body]
  | cons a s =>
    cases s with
    | nil => simp [body]
    | cons b s =>
      cases s with
      | nil => simp [body]
      | cons c s =>
        cases s with
        | nil => simp [body]
        | cons d s =>
          cases s with
          | nil => simp [body]
          | cons e s =>
            cases s with
            | nil => simp [body]
            | cons f s =>
              cases s with
              | nil => simp [body]
              | cons g s =>
                cases s with
                | nil => simp [body]
                | cons h8 tail0 =>
                  have ht : tail0 = [] := by
                    apply List.length_eq_zero_iff.mp
                    simp only [List.length_cons] at h
                    omega
                  subst tail0
                  simp [body]

private theorem groups_canonical_aux (n : Nat) : ∀ (wire : Bytes), wire.length = n →
    ∀ (initial : Bool) s tail, groups initial wire = some (s, tail) →
      wire = body s ++ tail ∧ (initial = false → s ≠ []) := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro wire hlen initial s tail hget
      cases wire with
      | nil => simp [groups] at hget
      | cons a wire =>
        cases wire with
        | nil => simp [groups] at hget
        | cons b wire =>
          cases wire with
          | nil => simp [groups] at hget
          | cons c wire =>
            cases wire with
            | nil => simp [groups] at hget
            | cons d wire =>
              cases wire with
              | nil => simp [groups] at hget
              | cons e wire =>
                cases wire with
                | nil => simp [groups] at hget
                | cons f wire =>
                  cases wire with
                  | nil => simp [groups] at hget
                  | cons g wire =>
                    cases wire with
                    | nil => simp [groups] at hget
                    | cons h wire =>
                      cases wire with
                      | nil => simp [groups] at hget
                      | cons flag rest =>
                        simp only [List.length_cons] at hlen
                        have hrest_lt : rest.length < n := by omega
                        rw [groups] at hget
                        by_cases hflag9 : (flag == 9) = true
                        · rw [if_pos hflag9] at hget
                          cases hg : groups false rest with
                          | none => simp [hg] at hget
                          | some p =>
                              rcases p with ⟨more, tail2⟩
                              have hrec := ih rest.length hrest_lt rest rfl false more tail2 hg
                              rcases hrec with ⟨hrest_eq, hmorene⟩
                              simp [hg] at hget
                              rcases hget with ⟨hs, ht⟩
                              subst s
                              subst tail
                              constructor
                              · rw [hrest_eq]
                                have hf : flag = 9 := beq_iff_eq.mp hflag9
                                rw [hf]
                                cases more with
                                | nil => exact (hmorene rfl rfl).elim
                                | cons i rest =>
                                    simp [body, List.append_assoc]
                              · intro hi
                                simp
                        · rw [if_neg hflag9] at hget
                          by_cases hcond : flag.toNat > 8 || (flag == 0 && !initial) || !(([a,b,c,d,e,f,g,h] : Bytes).drop flag.toNat).all (· == 0)
                          · rw [if_pos hcond] at hget
                            simp at hget
                          · rw [if_neg hcond] at hget
                            have hle8 : flag.toNat ≤ 8 := by
                              by_cases hle : flag.toNat ≤ 8
                              · exact hle
                              · exfalso
                                apply hcond
                                simp [Nat.lt_of_not_ge hle]
                            have hzero : (flag == 0) → initial = true := by
                              intro hf
                              cases initial <;> simp_all
                            have hpad : (([a,b,c,d,e,f,g,h] : Bytes).drop flag.toNat).all (· == 0) = true := by
                              by_cases hp : (([a,b,c,d,e,f,g,h] : Bytes).drop flag.toNat).all (· == 0) = true
                              · exact hp
                              · exfalso
                                apply hcond
                                simp [hp]
                            simp at hget
                            rcases hget with ⟨hs, ht⟩
                            subst s
                            subst tail
                            let k := flag.toNat
                            let block : Bytes := [a,b,c,d,e,f,g,h]
                            let pad := block.drop k
                            have hpad' : pad.all (· == 0) = true := by simpa [pad, block, k] using hpad
                            have hpadLen : pad.length = 8 - k := by simp [pad, block, List.length_drop, k]
                            have hpadEq : pad = List.replicate (8 - k) 0 := by
                              have hall : ∀ x ∈ pad, x = 0 := by
                                intro x hx
                                have hx' := (List.all_eq_true.mp hpad') x hx
                                exact beq_iff_eq.mp hx'
                              rw [← hpadLen]
                              exact List.eq_replicate_of_mem hall
                            have htakeLen : (block.take k).length = k := by
                              have hblockLen : block.length = 8 := by simp [block]
                              rw [List.length_take, hblockLen, Nat.min_eq_left hle8]
                            have hflagEq : k.toUInt8 = flag := by
                              rw [← UInt8.toNat_inj]
                              simp [k]
                            change a :: b :: c :: d :: e :: f :: g :: h :: flag :: rest = body (block.take k) ++ rest ∧ (initial = false → block.take k ≠ [])
                            constructor
                            · rw [body_short (block.take k) (by omega)]
                              rw [htakeLen, hflagEq]
                              rw [← hpadEq]
                              rw [List.take_append_drop]
                              rfl
                            · intro hi
                              by_cases hk0 : k = 0
                              · have hf0 : flag = 0 := by
                                  rw [← hflagEq]
                                  simp [hk0]
                                have hinitTrue := hzero (by simp [hf0])
                                exfalso
                                cases initial <;> simp_all
                              · intro he
                                have : k = 0 := by
                                  have hh := congrArg List.length he
                                  simpa [htakeLen] using hh
                                exact hk0 this

theorem sound : AcceptedKeyPrefixCanonical.Goal := by
  intro wire s tail h
  cases wire with
  | nil => simp [decode] at h
  | cons tag rest =>
      by_cases ht : (tag == 2) = true
      · have htag : tag = 2 := beq_iff_eq.mp ht
        subst tag
        simp [decode] at h
        have hc := groups_canonical_aux rest.length rest rfl true s tail h
        rcases hc with ⟨hw, _⟩
        simp [encode, hw]
      · have hne : tag ≠ 2 := by intro he; exact ht (by simp [he])
        simp [decode, hne] at h
end TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_AcceptedKeyPrefixCanonical
