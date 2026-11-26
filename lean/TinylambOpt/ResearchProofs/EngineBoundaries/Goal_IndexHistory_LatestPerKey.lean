import TinylambOpt.ResearchGoals.EngineBoundaries.IndexHistory

/-!
Index reads across version changes must still observe the keys appropriate to the SQL
snapshot.

Proved

- `LatestPerKey` — Keep only the latest mutation for a key when checking a historical
  change, regardless of multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_IndexHistory_LatestPerKey

open TinylambOpt.ResearchGoals.EngineBoundaries.IndexHistory

private def maxList : List Nat → Nat
  | [] => 0
  | x :: xs => max x (maxList xs)

private theorem maxList_eq (l : List Nat) : l.max?.getD 0 = maxList l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hm : xs.max? with
      | none =>
          have hnil : xs = [] := List.max?_eq_none_iff.mp hm
          subst xs
          simp [maxList]
      | some m =>
          rw [hm] at ih
          simp at ih
          rw [List.max?_cons, hm]
          simp [maxList, ih]

private theorem le_maxList_of_mem (l : List Nat) (x : Nat) (hx : x ∈ l) : x ≤ maxList l := by
  induction l with
  | nil => simp at hx
  | cons y ys ih =>
      simp only [maxList]
      rcases List.mem_cons.mp hx with hxy | hx
      · subst x
        exact Nat.le_max_left y (maxList ys)
      · exact Nat.le_trans (ih hx) (Nat.le_max_right y (maxList ys))

private theorem le_max_of_mem (l : List Nat) (x : Nat) (hx : x ∈ l) : x ≤ l.max?.getD 0 := by
  rw [maxList_eq]
  exact le_maxList_of_mem l x hx

private theorem maxList_append (a b : List Nat) : maxList (a ++ b) = max (maxList a) (maxList b) := by
  induction a with
  | nil => simp [maxList]
  | cons x xs ih =>
      simp [maxList, ih, Nat.max_left_comm, Nat.max_comm]

private theorem ts_le_latest (c : Change) (cs : List Change) (hc : c ∈ cs) : c.ts ≤ latest cs := by
  unfold latest
  apply le_max_of_mem (cs.map Change.ts) c.ts
  exact List.mem_map.mpr ⟨c, hc, rfl⟩

private theorem noTouch (q : Query) (d : Nat) (hsnap : d ≤ q.snap)
    (cs : List Change) (hbound : ∀ c ∈ cs, touches q c = true → c.ts ≤ d) :
    cs.any (touches q) = false := by
  induction cs with
  | nil => rfl
  | cons c cs ih =>
      have hc : touches q c = false := by
        by_cases ht : touches q c = true
        · have hts := hbound c (by simp) ht
          simp only [touches, Bool.and_eq_true, decide_eq_true_eq] at ht
          have hnot : ¬ q.snap < c.ts := by omega
          exact False.elim (hnot ht.2)
        · cases h : touches q c <;> simp_all
      have htail : ∀ z ∈ cs, touches q z = true → z.ts ≤ d := by
        intro z hz ht
        exact hbound z (by simp [hz]) ht
      simp [hc, ih htail]

private theorem impossibleTouch (q : Query) (c : Change) (hlo : q.lo ≥ q.hi) : touches q c = false := by
  unfold touches
  by_cases hr : c.root == q.root <;> simp [hr]
  by_cases hl : q.lo ≤ c.key <;> simp [hl]
  by_cases hh : c.key < q.hi <;> simp [hh]
  omega

theorem sound : LatestPerKey.Goal := by
  intro q cs
  let P := touches q
  let Q : Change → Bool := fun c => P c &&
    !(cs.any (fun d => d.root == c.root && d.key == c.key && c.ts < d.ts))
  change cs.any P = cs.any Q
  by_cases hp : cs.any P = true
  · have hcand : (cs.filter P) ≠ [] := by
      intro hnil
      have hfalse : cs.any P = false := by
        apply List.any_eq_false.mpr
        intro x hx hpx
        have hxmem : x ∈ cs.filter P := List.mem_filter.mpr ⟨hx, hpx⟩
        simp [hnil] at hxmem
      simp [hp] at hfalse
    have hmapped : (cs.filter P).map Change.ts ≠ [] := by
      intro hnil
      apply hcand
      cases h : cs.filter P with
      | nil => rfl
      | cons c cl =>
          simp [h] at hnil
    cases hmts : ((cs.filter P).map Change.ts).max? with
    | none =>
        exfalso
        exact hmapped (List.max?_eq_none_iff.mp hmts)
    | some T =>
        have hmemT : T ∈ (cs.filter P).map Change.ts := (List.max?_eq_some_iff.mp hmts).1
        rcases List.mem_map.mp hmemT with ⟨c, hcP, hcT⟩
        have hPc : P c = true := (List.mem_filter.mp hcP).2
        have hno : cs.any (fun d : Change => d.root == c.root && d.key == c.key && c.ts < d.ts) = false := by
          apply List.any_eq_false.mpr
          intro d hdcs hdparts
          simp only [Bool.and_eq_true, decide_eq_true_eq] at hdparts
          have hdroot := hdparts.1.1
          have hdkey := hdparts.1.2
          have hdts := hdparts.2
          have hroot : d.root = c.root := by simpa using hdroot
          have hkey : d.key = c.key := by simpa using hdkey
          have hcParts : (((c.root == q.root) = true ∧ q.lo ≤ c.key) ∧ c.key < q.hi) ∧ q.snap < c.ts := by
            simpa only [P, touches, Bool.and_eq_true, decide_eq_true_eq] using hPc
          have hPd : P d = true := by
            simp only [P, touches, Bool.and_eq_true, decide_eq_true_eq]
            exact ⟨⟨⟨by rw [hroot]; exact hcParts.1.1.1,
                     by rw [hkey]; exact hcParts.1.1.2⟩,
                     by rw [hkey]; exact hcParts.1.2⟩,
                   by omega⟩
          have hdmem : d ∈ cs.filter P := List.mem_filter.mpr ⟨hdcs, hPd⟩
          have hdT : d.ts ≤ T := (List.max?_eq_some_iff.mp hmts).2 d.ts (List.mem_map.mpr ⟨d, hdmem, rfl⟩)
          omega
        have hQc : Q c = true := by simp [Q, hPc, hno]
        have hq : cs.any Q = true := List.any_eq_true.mpr ⟨c, (List.mem_filter.mp hcP).1, hQc⟩
        simp [hp, hq]
  · have hq : cs.any Q = false := by
      by_cases hqt : cs.any Q = true
      · rcases List.any_eq_true.mp hqt with ⟨c, hc, hQc⟩
        have hPc : P c = true := by
          have hh := hQc
          simp only [Q, Bool.and_eq_true] at hh
          exact hh.1
        exact False.elim (hp (List.any_eq_true.mpr ⟨c, hc, hPc⟩))
      · cases hb : cs.any Q <;> simp_all
    simp [hp, hq]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_IndexHistory_LatestPerKey
