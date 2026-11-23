import TinylambOpt.ResearchGoals.UniqueKeyLeftJoinElisionAndMark

namespace TinylambOpt.ResearchProofs.UniqueKeyLeftJoinElisionAndMark

open TinylambOpt.ResearchGoals.UniqueKeyLeftJoinElisionAndMark

private def keyHits (rs : List RRow) (k : Int) : List RRow :=
  rs.filterMap fun r =>
    match r.1 with
    | some rk => if k == rk then some r else none
    | none => none

private def hits (rs : List RRow) (l : LRow) : List RRow :=
  match l.1 with
  | none => []
  | some k => keyHits rs k

private def emit (rs : List RRow) (l : LRow) : List (LRow × Option RRow) :=
  let hs := hits rs l
  if hs.isEmpty then [(l, none)] else hs.map (fun r => (l, some r))

private theorem ljoin_eq (ls : List LRow) (rs : List RRow) :
    ljoin ls rs = ls.flatMap (emit rs) := by
  rfl

private theorem keyHits_cons_none (rs : List RRow) (k v : Int) :
    keyHits (((none, v) : RRow) :: rs) k = keyHits rs k := by
  rfl

private theorem keyHits_cons_eq (rs : List RRow) (k v : Int) :
    keyHits (((some k, v) : RRow) :: rs) k = ((some k, v) : RRow) :: keyHits rs k := by
  simp [keyHits]

private theorem keyHits_cons_ne (rs : List RRow) (k j v : Int) (h : k ≠ j) :
    keyHits (((some j, v) : RRow) :: rs) k = keyHits rs k := by
  simp [keyHits, h]

private theorem keyHits_length (rs : List RRow) (k : Int) :
    (keyHits rs k).length = (rs.filterMap Prod.fst).count k := by
  induction rs with
  | nil => simp [keyHits]
  | cons r rs ih =>
      rcases r with ⟨kr, vr⟩
      cases kr with
      | none =>
          rw [keyHits_cons_none]
          simpa using ih
      | some rk =>
          by_cases h : k == rk
          · have heq : k = rk := by simpa using h
            subst rk
            rw [keyHits_cons_eq]
            simp [ih]
          · have hne : k ≠ rk := by
              simp at h
              exact h
            rw [keyHits_cons_ne rs k rk vr hne]
            have hne' : rk ≠ k := Ne.symm hne
            simp [hne', ih]

private theorem keyHits_length_le_one (rs : List RRow) (k : Int)
    (hnd : NonNullKeysNodup rs) : (keyHits rs k).length ≤ 1 := by
  rw [keyHits_length]
  exact (List.nodup_iff_count.mp (by simpa [NonNullKeysNodup] using hnd)) k

private theorem keyHits_length_eq_zero (rs : List RRow) (k : Int) :
    (keyHits rs k).length = 0 ↔ k ∉ rs.filterMap Prod.fst := by
  rw [keyHits_length, List.count_eq_zero]

private theorem emit_some_left (l : LRow) (hs : List RRow) (hlen : hs.length ≤ 1) :
    (if hs.isEmpty then [(l, none)] else hs.map (fun r => (l, some r))).map Prod.fst = [l] := by
  cases hs with
  | nil => simp
  | cons r tl =>
      simp at hlen
      have htnil : tl = [] := by omega
      subst tl
      simp

private theorem emit_left (rs : List RRow) (l : LRow) (hnd : NonNullKeysNodup rs) :
    (emit rs l).map Prod.fst = [l] := by
  rcases l with ⟨lk, lv⟩
  cases lk with
  | none => simp [emit, hits]
  | some k =>
      simp only [emit, hits]
      exact emit_some_left (some k, lv) (keyHits rs k) (keyHits_length_le_one rs k hnd)

private theorem emit_some_mark (rs : List RRow) (k lv : Int)
    (hs : List RRow) (hlen : hs.length ≤ 1)
    (hzero : hs.length = 0 ↔ k ∉ rs.filterMap Prod.fst) :
    (if hs.isEmpty then [(((some k, lv) : LRow), none)]
       else hs.map (fun r => (((some k, lv) : LRow), some r))).map
        (fun p : LRow × Option RRow => (p.1.2, p.2.isSome)) =
      [(lv, memOptKey (some k) rs)] := by
  cases hs with
  | nil =>
      have hnotmem : k ∉ rs.filterMap Prod.fst := hzero.mp rfl
      simp [hnotmem, memOptKey]
  | cons r tl =>
      simp at hlen
      have htnil : tl = [] := by omega
      subst tl
      have hmem : k ∈ rs.filterMap Prod.fst := by
        by_cases h : k ∈ rs.filterMap Prod.fst
        · exact h
        · have hz : (1 : Nat) = 0 := by simpa using hzero.mpr h
          omega
      simp [hmem, memOptKey]

private theorem emit_mark (rs : List RRow) (l : LRow) (hnd : NonNullKeysNodup rs) :
    (emit rs l).map (fun p : LRow × Option RRow => (p.1.2, p.2.isSome)) =
      [(l.2, memOptKey l.1 rs)] := by
  rcases l with ⟨lk, lv⟩
  cases lk with
  | none => simp [emit, hits, memOptKey]
  | some k =>
      simp only [emit, hits]
      exact emit_some_mark rs k lv (keyHits rs k)
        (keyHits_length_le_one rs k hnd) (keyHits_length_eq_zero rs k)

private def innerOneStep (l : LRow) (r : RRow) : Option (LRow × RRow) :=
  match l.1, r.1 with
  | some lk, some rk => if lk == rk && decide (l.2 < r.2) then some (l, r) else none
  | _, _ => none

private def innerOne (rs : List RRow) (l : LRow) : List (LRow × RRow) :=
  rs.filterMap (innerOneStep l)

private theorem innerJoinLt_cons (l : LRow) (ls : List LRow) (rs : List RRow) :
    innerJoinLt (l :: ls) rs = innerOne rs l ++ innerJoinLt ls rs := by
  rfl

private theorem inner_one_empty (l : LRow) (rs : List RRow) (maxR : Int)
    (hmax : ∀ r ∈ rs, r.2 ≤ maxR) (hnot : ¬ l.2 < maxR) :
    innerOne rs l = [] := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      have hrle : r.2 ≤ maxR := hmax r (by simp)
      have hnlt : ¬ l.2 < r.2 := by omega
      have hdec : decide (l.2 < r.2) = false := by
        rw [decide_eq_false_iff_not]
        exact hnlt
      have hrnone : innerOneStep l r = none := by
        rcases l with ⟨lk, lv⟩
        rcases r with ⟨rk, rv⟩
        cases lk <;> cases rk <;> simp [innerOneStep, hdec]
      have ihTail : rs.filterMap (innerOneStep l) = [] := by
        have ih' := ih (fun r hr => hmax r (by simp [hr]))
        simpa [innerOne] using ih'
      change (r :: rs).filterMap (innerOneStep l) = []
      simp [hrnone, ihTail]

theorem sound : TinylambOpt.ResearchGoals.UniqueKeyLeftJoinElisionAndMark.Goal := by
  constructor
  · intro ls rs hnd
    constructor
    · rw [ljoin_eq]
      change (ls.flatMap (emit rs)).map Prod.fst = ls
      rw [List.map_flatMap]
      induction ls with
      | nil => rfl
      | cons l ls ih => simp [emit_left rs l hnd, ih]
    · rw [ljoin_eq]
      change (ls.flatMap (emit rs)).map (fun p : LRow × Option RRow => (p.1.2, p.2.isSome)) =
        optMarkJoin ls rs
      rw [List.map_flatMap]
      simp only [optMarkJoin]
      induction ls with
      | nil => rfl
      | cons l ls ih => simp [emit_mark rs l hnd, ih]
  constructor
  · intro ls rs maxR hmax
    induction ls with
    | nil => rfl
    | cons l ls ih =>
        by_cases hl : decide (l.2 < maxR) = true
        · have hf : List.filter (fun l => decide (l.2 < maxR)) (l :: ls) =
              l :: List.filter (fun l => decide (l.2 < maxR)) ls := by
            simp [hl]
          rw [hf, innerJoinLt_cons, innerJoinLt_cons, ih]
        · have hlfalse : decide (l.2 < maxR) = false := by
            cases h : decide (l.2 < maxR)
            · rfl
            · exact (hl h).elim
          have hnot : ¬ l.2 < maxR := by
            rw [← decide_eq_false_iff_not]
            exact hlfalse
          have hf : List.filter (fun l => decide (l.2 < maxR)) (l :: ls) =
              List.filter (fun l => decide (l.2 < maxR)) ls := by
            simp [hlfalse]
          rw [hf, innerJoinLt_cons, ih]
          rw [inner_one_empty l rs maxR hmax hnot]
          simp
  · decide

end TinylambOpt.ResearchProofs.UniqueKeyLeftJoinElisionAndMark
