import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `AlphabeticMerge.MonotoneCut` — `∀ ws v, 2 ≤ ws.length → cut ws ≤ cut (ws++[v])`
- `AlphabeticMerge.Quadrangle` — `∀ a b c, b ≠ [] → best (a++b) + best (b++c) ≤ best
  (a++b++c) + best b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch.MonotoneCutAndQuadrangle

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

/-!
 The two remaining Atlas/PlanSearch goals: monotone optimal split position and
 the quadrangle (Monge) inequality for the alphabetic-merge DP optimum.

 The helper development below is a local copy of the already verified proofs in
 TinylambOpt/ResearchProofs/Atlas/PlanSearch_MonotoneCut.lean and
 TinylambOpt/ResearchProofs/Atlas/PlanSearch_Quadrangle.lean, adapted to this namespace.
-/

private def cand (xs : List Nat) (i : Nat) : Nat :=
  best (xs.take i) + best (xs.drop i)

-- `dp` stabilizes once the fuel exceeds the list length.
private theorem dp_stable : ∀ (n fuel : Nat) (xs : List Nat),
    xs.length ≤ n → n ≤ fuel → dp fuel xs = dp n xs := by
  intro n
  induction n with
  | zero =>
      intro fuel xs hx hn
      cases xs with
      | nil =>
          induction fuel with
          | zero => simp [dp]
          | succ f => simp [dp]
      | cons a as => simp at hx
  | succ n ih =>
      intro fuel xs hx hn
      by_cases hshort : xs.length ≤ 1
      · cases fuel with
        | zero => omega
        | succ fuel' => simp [dp, hshort]
      · cases fuel with
        | zero => omega
        | succ fuel' =>
            have hnle : n ≤ fuel' := by omega
            have h2 : 2 ≤ xs.length := by omega
            simp only [dp]
            rw [if_neg (by omega : ¬ xs.length ≤ 1)]
            rw [if_neg (by omega : ¬ xs.length ≤ 1)]
            congr 2
            have hmap :
                (List.range (xs.length - 1)).map
                    (fun i => dp fuel' (xs.take (i + 1)) + dp fuel' (xs.drop (i + 1))) =
                  (List.range (xs.length - 1)).map
                    (fun i => dp n (xs.take (i + 1)) + dp n (xs.drop (i + 1))) := by
              apply List.map_congr_left
              intro i hi
              have hi_lt : i < xs.length - 1 := by simpa using hi
              have htake : (xs.take (i + 1)).length ≤ n := by simp; omega
              have hdrop : (xs.drop (i + 1)).length ≤ n := by simp; omega
              rw [ih fuel' (xs.take (i + 1)) htake hnle,
                  ih fuel' (xs.drop (i + 1)) hdrop hnle]
            rw [hmap]

private theorem dp_eq_best (fuel : Nat) (xs : List Nat) (h : xs.length ≤ fuel) :
    dp fuel xs = best xs := by
  simpa [best] using dp_stable xs.length fuel xs (by omega) h

private theorem best_formula (xs : List Nat) (h2 : 2 ≤ xs.length) :
    best xs = xs.sum + ((List.range (xs.length - 1)).map (fun i => cand xs (i + 1))).min?.getD 0 := by
  unfold best
  have hfuel : xs.length = (xs.length - 1) + 1 := by omega
  rw [hfuel, dp]
  rw [if_neg (by omega : ¬ xs.length ≤ 1)]
  have hrange : xs.length - 1 + 1 - 1 = xs.length - 1 := by omega
  rw [hrange]
  congr 2
  have hmap :
      (List.range (xs.length - 1)).map
          (fun i => dp (xs.length - 1) (xs.take (i + 1)) + dp (xs.length - 1) (xs.drop (i + 1))) =
        (List.range (xs.length - 1)).map (fun i => cand xs (i + 1)) := by
    apply List.map_congr_left
    intro i hi
    have hi_lt : i < xs.length - 1 := by simpa using hi
    rw [dp_eq_best (xs.length - 1) (xs.take (i + 1)) (by simp; omega),
        dp_eq_best (xs.length - 1) (xs.drop (i + 1)) (by simp; omega)]
    rfl
  rw [hmap]

private theorem best_le_cand (xs : List Nat) (i : Nat) (hi1 : 1 ≤ i) (hi2 : i < xs.length) :
    best xs ≤ xs.sum + cand xs i := by
  have h2 : 2 ≤ xs.length := by omega
  rw [best_formula xs h2]
  have hmem : cand xs i ∈ (List.range (xs.length - 1)).map (fun j => cand xs (j + 1)) := by
    refine List.mem_map.mpr ?_
    refine ⟨i - 1, ?_, ?_⟩
    · simp [List.mem_range]
      omega
    · have hidx : i - 1 + 1 = i := by omega
      rw [hidx]
  exact Nat.add_le_add_left (List.min?_getD_le_of_mem hmem) xs.sum

private theorem exists_best_split (xs : List Nat) (h2 : 2 ≤ xs.length) :
    ∃ i, 1 ≤ i ∧ i < xs.length ∧ best xs = xs.sum + cand xs i := by
  rw [best_formula xs h2]
  let L := (List.range (xs.length - 1)).map (fun j => cand xs (j + 1))
  have hne : L ≠ [] := by
    simp [L]
    omega
  have hm : L.min hne ∈ L := List.min?_mem (List.min?_eq_some_min hne)
  obtain ⟨j, hjrange, hjval⟩ := List.mem_map.mp hm
  have hjlt : j < xs.length - 1 := by simpa using hjrange
  refine ⟨j + 1, by omega, by omega, ?_⟩
  have hget : L.min?.getD 0 = L.min hne := by simp [List.min?_eq_some_min hne]
  rw [hget]
  exact congrArg (fun x => xs.sum + x) hjval.symm

private theorem best_superadd_aux : ∀ n : Nat, ∀ xs ys : List Nat,
    xs.length + ys.length = n → xs ≠ [] → ys ≠ [] →
    best xs + best ys ≤ best (xs ++ ys) := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs ys hlen hx hy
      by_cases hsmall : xs.length = 1 ∧ ys.length = 1
      · rcases xs with _ | ⟨a, xs'⟩
        · simp at hx
        · rcases ys with _ | ⟨b, ys'⟩
          · simp at hy
          · have hxnil : xs' = [] := by simpa using hsmall.1
            have hynil : ys' = [] := by simpa using hsmall.2
            subst xs'
            subst ys'
            simp [best, dp]
      · have hxlen : 1 ≤ xs.length := List.length_pos_iff.mpr hx
        have hylen : 1 ≤ ys.length := List.length_pos_iff.mpr hy
        have htot : 3 ≤ xs.length + ys.length := by omega
        have h2 : 2 ≤ (xs ++ ys).length := by simpa using (show 2 ≤ xs.length + ys.length by omega)
        obtain ⟨z, hz1, hzlt, hzbest⟩ := exists_best_split (xs ++ ys) h2
        by_cases hzle : z ≤ xs.length
        · have htake : (xs ++ ys).take z = xs.take z := List.take_append_of_le_length hzle
          have hdrop : (xs ++ ys).drop z = xs.drop z ++ ys := List.drop_append_of_le_length hzle
          by_cases hzlen : z = xs.length
          · have hp2nil : xs.drop z = [] := by rw [hzlen, List.drop_length]
            have htake_eq : xs.take z = xs := by rw [hzlen, List.take_length]
            rw [hzbest, cand, htake, hdrop, hp2nil, htake_eq]
            simp
          · have hzltxs : z < xs.length := by omega
            have hbxs := best_le_cand xs z (by omega) hzltxs
            simp [cand] at hbxs
            have hp2ne : xs.drop z ≠ [] := by
              apply List.length_pos_iff.mp
              simp
              omega
            have hp2len : (xs.drop z).length + ys.length < n := by
              simp at hlen ⊢
              omega
            have hrec := ih ((xs.drop z).length + ys.length) hp2len (xs.drop z) ys rfl hp2ne hy
            rw [hzbest, cand, htake, hdrop]
            have hsum : (xs ++ ys).sum = xs.sum + ys.sum := by simp
            rw [hsum]
            omega
        · let q := z - xs.length
          have hq1 : 1 ≤ q := by omega
          have hq2 : q < ys.length := by
            simp at hzlt
            omega
          have hdxs : xs.drop z = [] := List.drop_eq_nil_of_le (by omega)
          have htxs : xs.take z = xs := by
            have h := List.take_append_drop z xs
            rw [hdxs] at h
            simpa using h
          have htake : (xs ++ ys).take z = xs ++ ys.take q := by
            rw [List.take_append, htxs]
          have hdrop : (xs ++ ys).drop z = ys.drop q := by
            rw [List.drop_append, hdxs]
            simp [q]
          have hbys := best_le_cand ys q hq1 hq2
          simp [cand] at hbys
          have hr1ne : ys.take q ≠ [] := by
            apply List.length_pos_iff.mp
            simp
            omega
          have hrlen : xs.length + (ys.take q).length < n := by
            simp at hlen ⊢
            omega
          have hrec := ih (xs.length + (ys.take q).length) hrlen xs (ys.take q) rfl hx hr1ne
          rw [hzbest, cand, htake, hdrop]
          have hsum : (xs ++ ys).sum = xs.sum + ys.sum := by simp
          rw [hsum]
          omega

private theorem best_superadd (xs ys : List Nat) (hx : xs ≠ []) (hy : ys ≠ []) :
    best xs + best ys ≤ best (xs ++ ys) :=
  best_superadd_aux (xs.length + ys.length) xs ys rfl hx hy

private theorem best_qi_singleton_aux : ∀ n : Nat, ∀ p r : List Nat, ∀ w : Nat,
    (p ++ [w] ++ r).length = n →
    best (p ++ [w]) + best ([w] ++ r) ≤ best (p ++ [w] ++ r) := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro p r w hlen
      by_cases hp : p = []
      · subst p
        simp [best, dp]
      by_cases hr : r = []
      · subst r
        simp [best, dp]
      have hpne : p ≠ [] := hp
      have hrne : r ≠ [] := hr
      have hp1 : 1 ≤ p.length := List.length_pos_iff.mpr hpne
      have hr1 : 1 ≤ r.length := List.length_pos_iff.mpr hrne
      have hL2 : 2 ≤ (p ++ [w] ++ r).length := by
        simp at hlen ⊢
        omega
      obtain ⟨z, hz1, hzlt, hzbest⟩ := exists_best_split (p ++ [w] ++ r) hL2
      by_cases hzle : z ≤ p.length
      · have htakeL : (p ++ [w] ++ r).take z = p.take z := by
          rw [List.take_append_of_le_length (by simp; omega)]
          exact List.take_append_of_le_length hzle
        have hdropL : (p ++ [w] ++ r).drop z = (p.drop z ++ [w]) ++ r := by
          rw [List.drop_append_of_le_length (by simp; omega)]
          rw [List.drop_append_of_le_length hzle]
        have hbpw : best (p ++ [w]) ≤ (p ++ [w]).sum + best (p.take z) + best (p.drop z ++ [w]) := by
          have hb := best_le_cand (p ++ [w]) z (by omega) (by simp; omega)
          have ht : (p ++ [w]).take z = p.take z := List.take_append_of_le_length hzle
          have hd : (p ++ [w]).drop z = p.drop z ++ [w] := List.drop_append_of_le_length hzle
          simpa [cand, ht, hd, Nat.add_assoc] using hb
        rw [hzbest, cand, htakeL, hdropL]
        by_cases hzlen : z = p.length
        · have hpdrop : p.drop z = [] := by rw [hzlen, List.drop_length]
          have hptake : p.take z = p := by rw [hzlen, List.take_length]
          rw [hpdrop, hptake] at hbpw ⊢
          simp [best, dp] at hbpw ⊢
          omega
        · have hzltp : z < p.length := by omega
          have hpdropne : p.drop z ≠ [] := by
            apply List.length_pos_iff.mp
            simp
            omega
          have hm : (p.drop z ++ [w] ++ r).length < n := by
            simp at hlen ⊢
            omega
          have hrec := ih (p.drop z ++ [w] ++ r).length hm (p.drop z) r w rfl
          have hsum : (p ++ [w]).sum + r.sum = (p ++ [w] ++ r).sum := by
            simp [List.sum_append]
            omega
          omega
      · let d := z - p.length - 1
        let t := r.take d
        let u := r.drop d
        have hdle : d ≤ r.length := by
          dsimp [d]
          simp at hzlt
          omega
        have htlen : t.length = d := by simp [t, Nat.min_eq_left hdle]
        have hdlt : d < r.length := by
          dsimp [d] at *
          simp at hzlt
          omega
        have hune : u ≠ [] := by
          apply List.length_pos_iff.mp
          simp [u, Nat.sub_pos_of_lt hdlt]
        have hrde : r = t ++ u := by
          dsimp [t, u]
          exact (List.take_append_drop d r).symm
        have hzlen : z = (p ++ [w] ++ t).length := by
          simp [htlen, d]
          omega
        have hLdecomp : p ++ [w] ++ r = (p ++ [w] ++ t) ++ u := by
          rw [hrde]
          simp [List.append_assoc]
        have htakeL : (p ++ [w] ++ r).take z = p ++ [w] ++ t := by
          rw [hLdecomp, hzlen]
          rw [List.take_append_of_le_length (by omega)]
          rw [List.take_length]
        have hdropL : (p ++ [w] ++ r).drop z = u := by
          rw [hLdecomp, hzlen]
          rw [List.drop_append_of_le_length (by omega)]
          rw [List.drop_length]
          simp
        have hulen : 1 ≤ u.length := List.length_pos_iff.mpr hune
        have hbwr : best ([w] ++ r) ≤ ([w] ++ r).sum + best ([w] ++ t) + best u := by
          have hbrdec : [w] ++ r = ([w] ++ t) ++ u := by rw [hrde]; simp [List.append_assoc]
          have hlen_eq : 1 + t.length = ([w] ++ t).length := by simp; omega
          have hb := best_le_cand ([w] ++ r) (1 + t.length) (by omega) (by
            rw [hbrdec]
            simp
            omega)
          have ht' : ([w] ++ r).take (1 + t.length) = [w] ++ t := by
            rw [hbrdec]
            rw [List.take_append_of_le_length (by omega)]
            rw [hlen_eq, List.take_length]
          have hd' : ([w] ++ r).drop (1 + t.length) = u := by
            rw [hbrdec]
            rw [List.drop_append_of_le_length (by omega)]
            rw [hlen_eq, List.drop_length]
            simp
          dsimp [cand] at hb
          have ht'' : List.take (1 + t.length) (w :: r) = w :: t := by simpa using ht'
          have hd'' : List.drop (1 + t.length) (w :: r) = u := by simpa using hd'
          rw [ht'', hd''] at hb
          simpa [Nat.add_assoc] using hb
        rw [hzbest, cand, htakeL, hdropL]
        have hm : (p ++ [w] ++ t).length < n := by
          rw [hLdecomp] at hlen
          simp at hlen
          omega
        have hrec := ih (p ++ [w] ++ t).length hm p t w rfl
        have hs1 : ([w] ++ r).sum = w + r.sum := by simp
        have hs2 : (p ++ [w]).sum = p.sum + w := by simp
        have hs3 : (p ++ [w] ++ r).sum = p.sum + w + r.sum := by
          simp [List.sum_append]
          omega
        omega

private theorem best_qi_singleton (p r : List Nat) (w : Nat) :
    best (p ++ [w]) + best ([w] ++ r) ≤ best (p ++ [w] ++ r) :=
  best_qi_singleton_aux (p ++ [w] ++ r).length p r w rfl

private theorem best_qi_aux : ∀ n : Nat, ∀ p q r : List Nat,
    (p ++ q ++ r).length = n → q ≠ [] →
    best (p ++ q) + best (q ++ r) ≤ best (p ++ q ++ r) + best q := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro p q r hlen hq
      by_cases hp : p = []
      · subst p
        change best q + best (q ++ r) ≤ best (q ++ r) + best q
        omega
      by_cases hr : r = []
      · subst r
        rw [List.append_nil, List.append_nil]
        omega
      have hpne : p ≠ [] := hp
      have hrne : r ≠ [] := hr
      have hrpos : 1 ≤ r.length := List.length_pos_iff.mpr hrne
      by_cases hqone : q.length = 1
      · rcases q with _ | ⟨w, qs⟩
        · simp at hqone
        · have hnil : qs = [] := by simpa using hqone
          subst qs
          simpa [best, dp] using best_qi_singleton p r w
      have hq2 : 2 ≤ q.length := by
        have : 1 ≤ q.length := List.length_pos_iff.mpr hq
        omega
      obtain ⟨k, hk1, hklt, hkbest⟩ := exists_best_split q hq2
      let q1 := q.take k
      let q2 := q.drop k
      have hqsplit : q = q1 ++ q2 := (List.take_append_drop k q).symm
      have hq1len_eq : q1.length = k := by
        dsimp [q1]
        rw [List.length_take, Nat.min_eq_left (by omega)]
      have hqlen_split : q.length = q1.length + q2.length := by
        rw [hqsplit]
        simp
      have hq1ne : q1 ≠ [] := by
        apply List.length_pos_iff.mp
        dsimp [q1]
        simp
        omega
      have hq2ne : q2 ≠ [] := by
        apply List.length_pos_iff.mp
        dsimp [q2]
        simp
        omega
      have hbestq : best q = q.sum + best q1 + best q2 := by
        dsimp [q1, q2]
        simpa [cand, Nat.add_assoc] using hkbest
      have hq2pos : 1 ≤ q2.length := List.length_pos_iff.mpr hq2ne
      have hlen' : p.length + q.length + r.length = n := by
        simpa [List.length_append, Nat.add_assoc] using hlen
      have hL2 : 2 ≤ (p ++ q ++ r).length := by
        have hp1 : 1 ≤ p.length := List.length_pos_iff.mpr hpne
        have hr1 : 1 ≤ r.length := List.length_pos_iff.mpr hrne
        simp at hlen ⊢
        omega
      obtain ⟨z, hz1, hzlt, hzbest⟩ := exists_best_split (p ++ q ++ r) hL2
      by_cases hzle : z ≤ p.length + q1.length
      · let Y' := (p ++ q1).drop z
        let Y := (p ++ q).drop z
        have hzL : z ≤ (p ++ q1).length := by simp [hq1len_eq]; omega
        have hYdecomp : Y = Y' ++ q2 := by
          dsimp [Y, Y']
          rw [hqsplit, ← List.append_assoc]
          exact List.drop_append_of_le_length hzL
        have hXtake : (p ++ q ++ r).take z = (p ++ q).take z :=
          List.take_append_of_le_length (by simp [hq1len_eq]; omega)
        have hYdrop : (p ++ q ++ r).drop z = Y ++ r := by
          dsimp [Y]
          exact List.drop_append_of_le_length (by simp [hq1len_eq]; omega)
        have hbpq : best (p ++ q) ≤ (p ++ q).sum + best ((p ++ q).take z) + best Y := by
          by_cases hzltpq : z < (p ++ q).length
          · have hb := best_le_cand (p ++ q) z (by omega) hzltpq
            simpa [cand, Y, Nat.add_assoc] using hb
          · have hzge : (p ++ q).length ≤ z := by omega
            have hdropnil : (p ++ q).drop z = [] := List.drop_eq_nil_of_le hzge
            have htakeall : (p ++ q).take z = p ++ q := by
              have h := List.take_append_drop z (p ++ q)
              rw [hdropnil] at h
              simpa using h
            rw [htakeall]
            simp [Y, hdropnil, best]
            omega
        have hbqr : best (q ++ r) ≤ (q ++ r).sum + best q1 + best (q2 ++ r) := by
          have hq1len : q1.length ≤ q.length := by simp [hqsplit]
          have ht : (q ++ r).take q1.length = q1 := by
            rw [List.take_append_of_le_length hq1len]
            rw [hqsplit, List.take_append_of_le_length (by omega), List.take_length]
          have hd : (q ++ r).drop q1.length = q2 ++ r := by
            rw [List.drop_append_of_le_length hq1len]
            rw [hqsplit, List.drop_append_of_le_length (by omega), List.drop_length]
            simp
          have hq1ltqr : q1.length < (q ++ r).length := by
            rw [hqsplit]
            simp
            omega
          have hb := best_le_cand (q ++ r) q1.length (List.length_pos_iff.mpr hq1ne) hq1ltqr
          simpa [cand, ht, hd, Nat.add_assoc] using hb

        have hrec : best Y + best (q2 ++ r) ≤ best q2 + best (Y ++ r) := by
          by_cases hYnil : Y' = []
          · have hYeq : Y = q2 := by
              rw [hYdecomp, hYnil]
              simp
            rw [hYeq]
            simp
          · have hYp1 : 1 ≤ Y'.length := List.length_pos_iff.mpr hYnil
            have hYlen : Y'.length = (p.length + q1.length) - z := by
              dsimp [Y']
              simp
            have hm : (Y' ++ q2 ++ r).length < n := by
              rw [List.length_append, List.length_append, hYlen]
              omega
            have hrec' := ih (Y' ++ q2 ++ r).length hm Y' q2 r rfl hq2ne
            rw [← hYdecomp] at hrec'
            simpa [List.append_assoc, Nat.add_comm] using hrec'
        rw [hzbest, cand, hXtake, hYdrop]
        have hsum : (p ++ q).sum + (q ++ r).sum = q.sum + (p ++ q ++ r).sum := by
          simp [List.sum_append]
          omega
        omega
      · let d := z - p.length
        have hdgt : q1.length < d := by dsimp [d]; omega
        by_cases hdlt : d < q.length
        · let Q1 := q.take d
          let Q2 := q.drop d
          have hQ1len : Q1.length = d := by
            dsimp [Q1]
            rw [List.length_take, Nat.min_eq_left (by omega)]
          have hQ2ne : Q2 ≠ [] := by
            apply List.length_pos_iff.mp
            dsimp [Q2]
            simp
            omega
          have hQsplit : Q1 ++ Q2 = q := by
            dsimp [Q1, Q2]
            exact List.take_append_drop d q
          have hzlen : z = (p ++ Q1).length := by
            simp [hQ1len]
            dsimp [d]
            omega
          have hLdecomp : p ++ q ++ r = (p ++ Q1) ++ (Q2 ++ r) := by
            rw [← hQsplit]
            simp [List.append_assoc]
          have htakeL : (p ++ q ++ r).take z = p ++ Q1 := by
            rw [hLdecomp, hzlen]
            rw [List.take_append_of_le_length (by omega)]
            rw [List.take_length]
          have hdropL : (p ++ q ++ r).drop z = Q2 ++ r := by
            rw [hLdecomp, hzlen]
            rw [List.drop_append_of_le_length (by omega)]
            rw [List.drop_length]
            simp
          have hbpq : best (p ++ q) ≤ (p ++ q).sum + best (p ++ q1) + best q2 := by
            have hb := best_le_cand (p ++ q) (p.length + q1.length) (by omega) (by
              simp [hqsplit]
              omega)
            have hz1len : p.length + q1.length = (p ++ q1).length := by simp
            have ht : (p ++ q).take (p.length + q1.length) = p ++ q1 := by
              rw [hz1len, hqsplit, ← List.append_assoc]
              rw [List.take_append_of_le_length (by omega), List.take_length]
            have hd' : (p ++ q).drop (p.length + q1.length) = q2 := by
              rw [hz1len, hqsplit, ← List.append_assoc]
              rw [List.drop_append_of_le_length (by omega), List.drop_length]
              simp
            dsimp [cand] at hb
            rw [ht, hd'] at hb
            simpa [Nat.add_assoc] using hb
          have hbqr : best (q ++ r) ≤ (q ++ r).sum + best Q1 + best (Q2 ++ r) := by
            have ht : (q ++ r).take d = Q1 := by
              dsimp [Q1]
              exact List.take_append_of_le_length (by omega)
            have hd' : (q ++ r).drop d = Q2 ++ r := by
              dsimp [Q2]
              exact List.drop_append_of_le_length (by omega)
            have hb := best_le_cand (q ++ r) d (by omega) (by simp; omega)
            dsimp [cand] at hb
            rw [ht, hd'] at hb
            simpa [Nat.add_assoc] using hb
          have hQ1rest : Q1.drop q1.length ≠ [] := by
            apply List.length_pos_iff.mp
            dsimp [Q1, q1]
            simp
            omega
          have hQ1take : Q1.take q1.length = q1 := by
            rw [hq1len_eq]
            dsimp [Q1, q1]
            rw [List.take_take, Nat.min_eq_left (by omega)]
          have hQ1eq : q1 ++ Q1.drop q1.length = Q1 := by
            have h := List.take_append_drop q1.length Q1
            rw [hQ1take] at h
            exact h
          have hQ1len_split : Q1.length = q1.length + (Q1.drop q1.length).length := by
            rw [← hQ1eq]
            simp
          have hm : (p ++ q1 ++ Q1.drop q1.length).length < n := by
            simp [hlen, Q1, q1]
            omega
          have hrec' := ih (p ++ q1 ++ Q1.drop q1.length).length hm p q1 (Q1.drop q1.length) rfl hq1ne
          have hrec : best (p ++ q1) + best Q1 ≤ best q1 + best (p ++ Q1) := by
            rw [← hQ1eq]
            simpa [List.append_assoc, Nat.add_comm] using hrec'
          rw [hzbest, cand, htakeL, hdropL]
          have hsum : (p ++ q).sum + (q ++ r).sum = q.sum + (p ++ q ++ r).sum := by
            simp [List.sum_append]
            omega
          omega
        · let e := d - q.length
          let t := r.take e
          let u := r.drop e
          have hele : e ≤ r.length := by
            dsimp [e, d] at *
            simp at hzlt ⊢
            omega
          have htlen : t.length = e := by simp [t, Nat.min_eq_left hele]
          have hult : e < r.length := by
            dsimp [e, d] at *
            simp at hzlt ⊢
            omega
          have hune : u ≠ [] := by
            apply List.length_pos_iff.mp
            simp [u, Nat.sub_pos_of_lt hult]
          have hrde : r = t ++ u := by
            dsimp [t, u]
            exact (List.take_append_drop e r).symm
          have hpqle : p.length + q.length ≤ z := by
            dsimp [d] at hdlt
            omega
          have hzlen : z = (p ++ q ++ t).length := by
            simp [htlen]
            dsimp [e]
            omega
          have hLdecomp : p ++ q ++ r = (p ++ q ++ t) ++ u := by
            rw [hrde]
            simp [List.append_assoc]
          have htakeL : (p ++ q ++ r).take z = p ++ q ++ t := by
            rw [hLdecomp, hzlen]
            rw [List.take_append_of_le_length (by omega)]
            rw [List.take_length]
          have hdropL : (p ++ q ++ r).drop z = u := by
            rw [hLdecomp, hzlen]
            rw [List.drop_append_of_le_length (by omega)]
            rw [List.drop_length]
            simp
          have hbqr : best (q ++ r) ≤ (q ++ r).sum + best (q ++ t) + best u := by
            have hrdec : q ++ r = (q ++ t) ++ u := by rw [hrde]; simp [List.append_assoc]
            have hlen_eq : q.length + t.length = (q ++ t).length := by simp
            have hqtlt : q.length + t.length < (q ++ r).length := by
              simp
              omega
            have hb := best_le_cand (q ++ r) (q.length + t.length) (by omega) hqtlt
            have ht' : (q ++ r).take (q.length + t.length) = q ++ t := by
              rw [hrdec]
              rw [List.take_append_of_le_length (by omega)]
              rw [hlen_eq, List.take_length]
            have hd' : (q ++ r).drop (q.length + t.length) = u := by
              rw [hrdec]
              rw [List.drop_append_of_le_length (by omega)]
              rw [hlen_eq, List.drop_length]
              simp
            dsimp [cand] at hb
            rw [ht', hd'] at hb
            simpa [Nat.add_assoc] using hb
          have hbpq : best (p ++ q) ≤ (p ++ q).sum + best (p ++ q1) + best q2 := by
            have hb := best_le_cand (p ++ q) (p.length + q1.length) (by omega) (by
              simp [hqsplit]
              omega)
            have hz1len : p.length + q1.length = (p ++ q1).length := by simp
            have ht : (p ++ q).take (p.length + q1.length) = p ++ q1 := by
              rw [hz1len, hqsplit, ← List.append_assoc]
              rw [List.take_append_of_le_length (by omega), List.take_length]
            have hd' : (p ++ q).drop (p.length + q1.length) = q2 := by
              rw [hz1len, hqsplit, ← List.append_assoc]
              rw [List.drop_append_of_le_length (by omega), List.drop_length]
              simp
            dsimp [cand] at hb
            rw [ht, hd'] at hb
            simpa [Nat.add_assoc] using hb
          rw [hzbest, cand, htakeL, hdropL]
          have hm : (p ++ q1 ++ (q2 ++ t)).length < n := by
            rw [hLdecomp] at hlen
            simp [hqsplit] at hlen ⊢
            omega
          have hrec' := ih (p ++ q1 ++ (q2 ++ t)).length hm p q1 (q2 ++ t) rfl hq1ne
          have hrec : best (p ++ q1) + best (q ++ t) ≤ best q1 + best (p ++ q ++ t) := by
            rw [hqsplit]
            simpa [List.append_assoc, Nat.add_comm] using hrec'
          have hsum : (p ++ q).sum + (q ++ r).sum = q.sum + (p ++ q ++ r).sum := by
            simp [List.sum_append]
            omega
          omega

private theorem best_qi (p q r : List Nat) (hq : q ≠ []) :
    best (p ++ q) + best (q ++ r) ≤ best (p ++ q ++ r) + best q :=
  best_qi_aux (p ++ q ++ r).length p q r rfl hq

private def cutRel (ws : List Nat) (i j : Nat) : Bool := decide (cand ws i ≤ cand ws j)

private def cutCands (ws : List Nat) : List Nat :=
  (List.range (ws.length - 1)).map (fun x => x + 1)

private theorem cut_eq (ws : List Nat) :
    cut ws = ((cutCands ws).mergeSort (cutRel ws)).head?.getD 0 := rfl

private theorem cutCands_nodup (ws : List Nat) : (cutCands ws).Nodup := by
  unfold cutCands
  exact List.Pairwise.map (fun x : Nat => x + 1) (by
    intro a b h
    omega) List.nodup_range

private theorem cut_mem (ws : List Nat) (h2 : 2 ≤ ws.length) :
    cut ws ∈ cutCands ws := by
  unfold cut
  let le := cutRel ws
  have hne : cutCands ws ≠ [] := by
    unfold cutCands
    simp
    omega
  cases hS : (cutCands ws).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (le := le) (cutCands ws)
      rw [hS] at hlen
      simp [cutCands] at hlen
      omega
  | cons a t =>
      have ha : a ∈ (cutCands ws).mergeSort le := by rw [hS]; simp
      have hamem : a ∈ cutCands ws := (List.mergeSort_perm (cutCands ws) le).mem_iff.mp ha
      have haeq : a = cut ws := by
        rw [cut_eq]
        rw [hS]
        simp
      have hexpr :
          (((List.range (ws.length - 1)).map (fun x => x + 1)).mergeSort
              (fun i j => best (ws.take i) + best (ws.drop i) ≤
                best (ws.take j) + best (ws.drop j))).head?.getD 0 = a := by
        change ((cutCands ws).mergeSort (cutRel ws)).head?.getD 0 = a
        rw [hS]
        simp
      rw [hexpr]
      exact hamem

private theorem cut_min (ws : List Nat) (h2 : 2 ≤ ws.length) (i : Nat)
    (hi : i ∈ cutCands ws) : cand ws (cut ws) ≤ cand ws i := by
  let le := cutRel ws
  have htrans : ∀ a b c, le a b → le b c → le a c := by
    intro a b c
    simp [le, cutRel] at *
    omega
  have htotal : ∀ a b, le a b || le b a := by
    intro a b
    simp [le, cutRel] at *
    omega
  have hsorted := List.pairwise_mergeSort htrans htotal (cutCands ws)
  have hi' : i ∈ (cutCands ws).mergeSort le :=
    (List.mergeSort_perm (cutCands ws) le).mem_iff.mpr hi
  cases hS : (cutCands ws).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (le := le) (cutCands ws)
      rw [hS] at hlen
      simp [cutCands] at hlen
      omega
  | cons a t =>
      rw [hS] at hsorted hi'
      have ha : a = cut ws := by
        rw [cut_eq]
        rw [hS]
        simp
      by_cases hia : i = a
      · subst i
        rw [← ha]
        omega
      · have ht : i ∈ t := by
          simpa [hia] using hi'
        have hrel := List.rel_of_pairwise_cons hsorted ht
        have hrel' : cand ws a ≤ cand ws i := by simpa [le, cutRel] using hrel
        rw [ha] at hrel'
        exact hrel'

private theorem sublist_pair_range_succ : ∀ n i j : Nat,
    i ∈ List.range n → j ∈ List.range n → i < j →
    List.Sublist [i + 1, j + 1] ((List.range n).map (fun x => x + 1)) := by
  intro n
  induction n with
  | zero =>
      intro i j hi
      simp at hi
  | succ n ih =>
      intro i j hi hj hij
      have hi' : i < n.succ := by simpa using hi
      have hj' : j < n.succ := by simpa using hj
      by_cases hin : i = n
      · subst i
        omega
      by_cases hjn : j = n
      · subst j
        have hiR : i < n := by omega
        have hs : List.Sublist [i + 1] ((List.range n).map (fun x => x + 1)) := by
          rw [List.singleton_sublist]
          simp [hiR]
        simpa [List.range_succ] using hs.append_right [n + 1]
      · have hiR : i < n := by omega
        have hjR : j < n := by omega
        have hrec := ih i j (by simpa using hiR) (by simpa using hjR) hij
        let M := (List.range n).map (fun x => x + 1)
        have hMsub : List.Sublist M (M ++ [n + 1]) := by
          simpa using List.Sublist.append (List.Sublist.refl M)
            (List.Sublist.cons (n + 1) List.Sublist.slnil)
        have hfinal := hrec.trans hMsub
        simpa [M, List.range_succ] using hfinal

private theorem cut_strict (ws : List Nat) (h2 : 2 ≤ ws.length) (i : Nat)
    (hi : i ∈ cutCands ws) (hlt : i < cut ws)
    (hcost : cand ws i ≤ cand ws (cut ws)) : False := by
  let le := cutRel ws
  have htrans : ∀ a b c, le a b → le b c → le a c := by
    intro a b c
    simp [le, cutRel] at *
    omega
  have htotal : ∀ a b, le a b || le b a := by
    intro a b
    simp [le, cutRel] at *
    omega
  have hcw := cut_mem ws h2
  have hi1 : 1 ≤ i := by
    unfold cutCands at hi
    rcases List.mem_map.mp hi with ⟨j, hj, hji⟩
    omega
  have hc1 : 1 ≤ cut ws := by
    unfold cutCands at hcw
    rcases List.mem_map.mp hcw with ⟨j, hj, hji⟩
    omega
  have hidx : i - 1 < cut ws - 1 := by omega
  have hiR : i - 1 ∈ List.range (ws.length - 1) := by
    unfold cutCands at hi
    rcases List.mem_map.mp hi with ⟨j, hj, hji⟩
    have heq : i - 1 = j := by omega
    rw [heq]
    simpa using hj
  have hcR : cut ws - 1 ∈ List.range (ws.length - 1) := by
    unfold cutCands at hcw
    rcases List.mem_map.mp hcw with ⟨j, hj, hji⟩
    have heq : cut ws - 1 = j := by omega
    rw [heq]
    simpa using hj
  have hi_eq : i - 1 + 1 = i := by omega
  have hc_eq : cut ws - 1 + 1 = cut ws := by omega
  have hsub : List.Sublist [i, cut ws] (cutCands ws) := by
    unfold cutCands
    simpa [hi_eq, hc_eq] using
      sublist_pair_range_succ (ws.length - 1) (i - 1) (cut ws - 1) hiR hcR hidx
  have hleic : le i (cut ws) := by
    simp [le, cutRel]
    exact hcost
  have hpair := List.pair_sublist_mergeSort htrans htotal hleic hsub
  cases hS : (cutCands ws).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (le := le) (cutCands ws)
      rw [hS] at hlen
      simp [cutCands] at hlen
      omega
  | cons a t =>
      rw [hS] at hpair
      have hnodup : ((cutCands ws).mergeSort le).Nodup :=
        (List.mergeSort_perm (cutCands ws) le).symm.nodup (cutCands_nodup ws)
      rw [hS] at hnodup
      have hnot : cut ws ∉ t := by
        have ha : a = cut ws := by
          rw [cut_eq]
          rw [hS]
          simp
        rw [← ha]
        exact (List.nodup_cons.mp hnodup).1
      cases hpair with
      | cons x htail =>
          exact hnot (List.Sublist.subset htail (by simp))
      | cons_cons x htail =>
          exact hnot (List.Sublist.subset htail (by simp))

private theorem cut_least (ws : List Nat) (h2 : 2 ≤ ws.length) (i : Nat)
    (hi : i ∈ cutCands ws) (hcost : cand ws i ≤ cand ws (cut ws)) :
    cut ws ≤ i := by
  by_cases hle : cut ws ≤ i
  · exact hle
  · have hlt : i < cut ws := by omega
    exact (cut_strict ws h2 i hi hlt hcost).elim

private theorem cand_append_mono (ws : List Nat) (v p q : Nat)
    (hp1 : 1 ≤ p) (hp2 : p < ws.length) (hpq : p < q) (hq2 : q < ws.length)
    (hcost : cand (ws ++ [v]) p ≤ cand (ws ++ [v]) q) :
    cand ws p ≤ cand ws q := by
  let a := ws.take p
  let b := (ws.drop p).take (q - p)
  let c := ws.drop q
  have hpqsub : p + (q - p) = q := by omega
  have hbp : p ≤ ws.length := by omega
  have hbq : q ≤ ws.length := by omega
  have htakep_ws : ws.take p = a := rfl
  have hdrop_part : ws.drop p = b ++ c := by
    calc
      ws.drop p = (ws.drop p).take (q - p) ++ (ws.drop p).drop (q - p) :=
        (List.take_append_drop (q - p) (ws.drop p)).symm
      _ = b ++ c := by
        dsimp [b, c]
        rw [List.drop_drop, hpqsub]
  have hlenab : (a ++ b).length = q := by
    dsimp [a, b]
    simp
    omega
  have htakeq_ws : ws.take q = a ++ b := by
    rw [← List.take_append_drop p ws]
    rw [List.take_append, List.take_take, Nat.min_eq_right (by omega)]
    have htp : (ws.take p).length = p := by
      simp [List.length_take, Nat.min_eq_left hbp]
    rw [htp]
  have hdropq_ws : ws.drop q = c := rfl
  have htakep : (ws ++ [v]).take p = a := by
    rw [List.take_append_of_le_length hbp]
  have hdropp : (ws ++ [v]).drop p = b ++ c ++ [v] := by
    rw [List.drop_append_of_le_length hbp, hdrop_part]
  have htakeq : (ws ++ [v]).take q = a ++ b := by
    rw [List.take_append_of_le_length hbq]
    exact htakeq_ws
  have hdropq : (ws ++ [v]).drop q = c ++ [v] := by
    rw [List.drop_append_of_le_length hbq, hdropq_ws]
  have hcne : c ≠ [] := by
    apply List.length_pos_iff.mp
    dsimp [c]
    simp
    omega
  have hqi := best_qi b c [v] hcne
  dsimp [cand] at hcost ⊢
  rw [htakep, hdropp, htakeq, hdropq] at hcost
  rw [htakep_ws, hdrop_part, htakeq_ws, hdropq_ws]
  omega

theorem sound_MonotoneCut :
    TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge.MonotoneCut.Goal := by
  intro ws v h2
  let L := ws ++ [v]
  have hL2 : 2 ≤ L.length := by dsimp [L]; simp; omega
  have hkws := cut_mem ws h2
  have hkL : cut ws ∈ cutCands L := by
    unfold cutCands at hkws ⊢
    dsimp [L]
    simp at hkws ⊢
    rcases hkws with ⟨j, hj, hji⟩
    refine ⟨j, ?_, ?_⟩
    · omega
    · simpa [L] using hji
  have hlmem := cut_mem L hL2
  have hkltws : cut ws < ws.length := by
    unfold cutCands at hkws
    rcases List.mem_map.mp hkws with ⟨j, hj, hji⟩
    simp at hj
    omega
  by_cases hln : cut L = ws.length
  · dsimp [L] at hln
    rw [hln]
    exact Nat.le_of_lt hkltws
  · have hllt : cut L < ws.length := by
      unfold cutCands at hlmem
      dsimp [L] at hlmem
      simp at hlmem
      rcases hlmem with ⟨j, hj, hji⟩
      have hji' : j + 1 = cut L := by simpa [L] using hji
      have hleL : cut L ≤ ws.length := by omega
      exact Nat.lt_of_le_of_ne hleL hln
    by_cases hle : cut ws ≤ cut L
    · exact hle
    · have hpq : cut L < cut ws := by omega
      have hl1 : 1 ≤ cut L := by
        unfold cutCands at hlmem
        rcases List.mem_map.mp hlmem with ⟨j, hj, hji⟩
        omega
      have hcost : cand L (cut L) ≤ cand L (cut ws) := cut_min L hL2 (cut ws) hkL
      have hcw : cand ws (cut L) ≤ cand ws (cut ws) :=
        cand_append_mono ws v (cut L) (cut ws) hl1 hllt hpq hkltws hcost
      have hlmemws : cut L ∈ cutCands ws := by
        unfold cutCands at hlmem ⊢
        dsimp [L] at hlmem
        simp at hlmem ⊢
        rcases hlmem with ⟨j, hj, hji⟩
        have hji' : j + 1 = cut L := by simpa [L] using hji
        have hjlt : j + 1 < ws.length := by rw [hji']; exact hllt
        refine ⟨j, ?_, ?_⟩
        · omega
        · simpa [L] using hji
      exact cut_least ws h2 (cut L) hlmemws hcw

private theorem minGetD_le_of_mem (xs : List Nat) (a : Nat) (ha : a ∈ xs) :
    xs.min?.getD 0 ≤ a := by
  cases h : xs.min? with
  | none =>
      have hnil : xs = [] := List.min?_eq_none_iff.mp h
      simp [hnil] at ha
  | some m =>
      have hm : m ≤ a := (List.min?_eq_some_iff.mp h).2 a ha
      simpa [h] using hm

private theorem minGetD_eq_of_mem (xs : List Nat) (a : Nat) (ha : a ∈ xs)
    (hmin : ∀ b ∈ xs, a ≤ b) : xs.min?.getD 0 = a := by
  have h : xs.min? = some a := (List.min?_eq_some_iff).2 ⟨ha, hmin⟩
  simp [h]

private theorem minGetD_map_mono {α : Type} (xs : List α) (f g : α → Nat)
    (h : ∀ a ∈ xs, f a ≤ g a) :
    (xs.map f).min?.getD 0 ≤ (xs.map g).min?.getD 0 := by
  cases hm : (xs.map g).min? with
  | none =>
      have hnil : xs.map g = [] := List.min?_eq_none_iff.mp hm
      cases xs with
      | nil => simp
      | cons a as => simp at hnil
  | some m =>
      have hmem : m ∈ xs.map g := List.min?_mem hm
      obtain ⟨a, ha, rfl⟩ := List.mem_map.mp hmem
      exact Nat.le_trans (minGetD_le_of_mem _ _ (List.mem_map_of_mem ha)) (by simpa [hm] using h a ha)

/-! The fuel in `dp` never binds once it is at least the list length: each split
reduces both the fuel and the length of every recursive list by the same amount.
Hence `best` is the unconstrained optimum. -/

private theorem dp_fuel_eq (ws : List Nat) (f g : Nat)
    (hf : ws.length ≤ f) (hg : ws.length ≤ g) : dp f ws = dp g ws := by
  induction f generalizing g ws with
  | zero =>
      cases ws with
      | nil =>
          cases g with
          | zero => rfl
          | succ g => simp [dp]
      | cons w ws =>
          simp at hf
  | succ f ih =>
      cases g with
      | zero =>
          cases ws with
          | nil => simp [dp]
          | cons w ws => simp at hg
      | succ g =>
          by_cases hsmall : ws.length ≤ 1
          · simp [dp, hsmall]
          · have hlen : 2 ≤ ws.length := by omega
            simp [dp, hsmall]
            congr 2
            apply List.map_congr_left
            intro i hi
            have hi' : i < ws.length - 1 := by simpa using hi
            have htk_f : (ws.take (i + 1)).length ≤ f := by
              rw [List.length_take]
              omega
            have hdk_f : (ws.drop (i + 1)).length ≤ f := by
              rw [List.length_drop]
              omega
            have htk_g : (ws.take (i + 1)).length ≤ g := by
              rw [List.length_take]
              omega
            have hdk_g : (ws.drop (i + 1)).length ≤ g := by
              rw [List.length_drop]
              omega
            rw [ih (ws.take (i + 1)) g htk_f htk_g]
            rw [ih (ws.drop (i + 1)) g hdk_f hdk_g]

private theorem dp_fuel_mono (ws : List Nat) {f g : Nat} (h : f ≤ g) :
    dp f ws ≤ dp g ws := by
  induction g generalizing f ws with
  | zero =>
      have : f = 0 := by omega
      subst f
      exact Nat.le_refl _
  | succ g ih =>
      cases f with
      | zero => exact Nat.zero_le _
      | succ f =>
          by_cases hs : ws.length ≤ 1
          · simp [dp, hs]
          · simp [dp, hs]
            apply minGetD_map_mono
            intro i hi
            exact Nat.add_le_add (ih (ws.take (i + 1)) (by omega))
              (ih (ws.drop (i + 1)) (by omega))

private theorem dp_step (ws : List Nat) (h : 2 ≤ ws.length) :
    dp ws.length ws = ws.sum +
      ((List.range (ws.length - 1)).map (fun i =>
        dp (ws.length - 1) (ws.take (i + 1)) +
        dp (ws.length - 1) (ws.drop (i + 1)))).min?.getD 0 := by
  have hL : ws.length = (ws.length - 1) + 1 := by omega
  rw [hL]
  simp [dp, show ¬ws.length ≤ 1 by omega]

private theorem best_small (ws : List Nat) (h : ws.length ≤ 1) : best ws = 0 := by
  unfold best
  rw [dp_fuel_eq ws ws.length 1 (by omega) h]
  simp [dp, h]

private theorem best_le_split (u v : List Nat) (hu : u ≠ []) (hv : v ≠ []) :
    best (u ++ v) ≤ (u ++ v).sum + best u + best v := by
  have hlen : 2 ≤ (u ++ v).length := by
    simp only [List.length_append]
    have hu1 : 1 ≤ u.length := Nat.succ_le_of_lt (List.length_pos_iff.mpr hu)
    have hv1 : 1 ≤ v.length := Nat.succ_le_of_lt (List.length_pos_iff.mpr hv)
    omega
  let i := u.length - 1
  have hu1 : 1 ≤ u.length := Nat.succ_le_of_lt (List.length_pos_iff.mpr hu)
  have hv1 : 1 ≤ v.length := Nat.succ_le_of_lt (List.length_pos_iff.mpr hv)
  have hi : i ∈ List.range ((u ++ v).length - 1) := by
    simp only [List.mem_range]
    simp [i]
    omega
  have htake : (u ++ v).take (i + 1) = u := by
    simp [i, Nat.sub_add_cancel hu1]
  have hdrop : (u ++ v).drop (i + 1) = v := by
    simp [i, Nat.sub_add_cancel hu1]
  have hle := minGetD_le_of_mem
      ((List.range ((u ++ v).length - 1)).map
        (fun j => dp ((u ++ v).length - 1) ((u ++ v).take (j + 1)) +
          dp ((u ++ v).length - 1) ((u ++ v).drop (j + 1))))
      (dp ((u ++ v).length - 1) u + dp ((u ++ v).length - 1) v)
      (List.mem_map.mpr ⟨i, hi, by simp [htake, hdrop]⟩)
  have hu_fuel : u.length ≤ (u ++ v).length - 1 := by
    simp only [List.length_append]
    omega
  have hv_fuel : v.length ≤ (u ++ v).length - 1 := by
    simp only [List.length_append]
    omega
  have hdp1 : dp ((u ++ v).length - 1) u = dp u.length u := by
    apply dp_fuel_eq
    · exact hu_fuel
    · exact Nat.le_refl _
  have hdp2 : dp ((u ++ v).length - 1) v = dp v.length v := by
    apply dp_fuel_eq
    · exact hv_fuel
    · exact Nat.le_refl _
  unfold best
  rw [dp_step (u ++ v) hlen]
  rw [← hdp1, ← hdp2]
  omega

private theorem best_opt_split (ws : List Nat) (h : 2 ≤ ws.length) :
    ∃ u v : List Nat, u ≠ [] ∧ v ≠ [] ∧ ws = u ++ v ∧
      best ws = ws.sum + best u + best v := by
  let f := fun j => dp (ws.length - 1) (ws.take (j + 1)) +
    dp (ws.length - 1) (ws.drop (j + 1))
  let J := (List.range (ws.length - 1)).map f
  have hJne : J ≠ [] := by
    simp [J]
    omega
  have hJsome : ∃ m, J.min? = some m := by
    cases hs : J.min? with
    | none =>
        have : J = [] := List.min?_eq_none_iff.mp hs
        exact False.elim (hJne this)
    | some m => exact ⟨m, rfl⟩
  obtain ⟨m, hm⟩ := hJsome
  have hm_mem : m ∈ J := List.min?_mem hm
  obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hm_mem
  have hi' : i < ws.length - 1 := by simpa [J] using hi
  have htk_len : (ws.take (i + 1)).length ≤ ws.length - 1 := by
    rw [List.length_take]
    omega
  have hdk_len : (ws.drop (i + 1)).length ≤ ws.length - 1 := by
    rw [List.length_drop]
    omega
  refine ⟨ws.take (i + 1), ws.drop (i + 1), ?_, ?_, (List.take_append_drop (i + 1) ws).symm, ?_⟩
  · apply List.length_pos_iff.mp
    rw [List.length_take]
    omega
  · apply List.length_pos_iff.mp
    rw [List.length_drop]
    omega
  · unfold best
    rw [dp_step ws h]
    change ws.sum + J.min?.getD 0 = ws.sum +
      dp (ws.take (i + 1)).length (ws.take (i + 1)) +
      dp (ws.drop (i + 1)).length (ws.drop (i + 1))
    have htk_eq : dp (ws.length - 1) (ws.take (i + 1)) =
        dp (ws.take (i + 1)).length (ws.take (i + 1)) := by
      apply dp_fuel_eq
      · exact htk_len
      · exact Nat.le_refl _
    have hdk_eq : dp (ws.length - 1) (ws.drop (i + 1)) =
        dp (ws.drop (i + 1)).length (ws.drop (i + 1)) := by
      apply dp_fuel_eq
      · exact hdk_len
      · exact Nat.le_refl _
    have hmin : J.min?.getD 0 = dp (ws.length - 1) (ws.take (i + 1)) +
        dp (ws.length - 1) (ws.drop (i + 1)) := by
      simp [hm, f]
    rw [hmin, htk_eq, hdk_eq]
    omega

/-- `seg xs lo hi` is the half-open interval `[lo, hi)` of `xs`. -/
private def seg (xs : List Nat) (lo hi : Nat) : List Nat := (xs.drop lo).take (hi - lo)
private def cst (xs : List Nat) (lo hi : Nat) : Nat := best (seg xs lo hi)

private theorem seg_append (xs : List Nat) (lo m hi : Nat) (hlm : lo ≤ m) (hmh : m ≤ hi) :
    seg xs lo m ++ seg xs m hi = seg xs lo hi := by
  unfold seg
  rw [show m = lo + (m - lo) by omega]
  rw [← List.drop_drop]
  rw [show lo + (m - lo) - lo = m - lo by omega]
  rw [show hi - (lo + (m - lo)) = hi - m by omega]
  rw [← List.take_add]
  rw [show (m - lo) + (hi - m) = hi - lo by omega]

private theorem seg_length (xs : List Nat) (lo hi : Nat) (hle : hi ≤ xs.length) :
    (seg xs lo hi).length = hi - lo := by
  unfold seg
  rw [List.length_take, List.length_drop]
  rw [Nat.min_eq_left]
  omega

private theorem seg_ne_nil (xs : List Nat) (lo hi : Nat) (hlt : lo < hi) (hle : hi ≤ xs.length) :
    seg xs lo hi ≠ [] := by
  apply List.length_pos_iff.mp
  rw [seg_length xs lo hi hle]
  omega

private theorem seg_sum_append (xs : List Nat) (lo m hi : Nat) (hlm : lo ≤ m) (hmh : m ≤ hi) :
    (seg xs lo hi).sum = (seg xs lo m).sum + (seg xs m hi).sum := by
  rw [← seg_append xs lo m hi hlm hmh, List.sum_append]

private theorem cst_small (xs : List Nat) (lo hi : Nat) (h : hi ≤ lo + 1) :
    cst xs lo hi = 0 := by
  unfold cst
  apply best_small
  unfold seg
  rw [List.length_take]
  omega

private theorem cst_le_split (xs : List Nat) (lo s hi : Nat)
    (hls : lo < s) (hsh : s < hi) (hle : hi ≤ xs.length) :
    cst xs lo hi ≤ (seg xs lo hi).sum + cst xs lo s + cst xs s hi := by
  have hseg := seg_append xs lo s hi (by omega) (by omega)
  have hne1 := seg_ne_nil xs lo s hls (by omega)
  have hne2 := seg_ne_nil xs s hi hsh hle
  unfold cst
  rw [← hseg, List.sum_append]
  simpa [List.sum_append] using best_le_split (seg xs lo s) (seg xs s hi) hne1 hne2

private theorem cst_opt_split (xs : List Nat) (lo hi : Nat) (h : lo + 2 ≤ hi)
    (hle : hi ≤ xs.length) :
    ∃ s, lo < s ∧ s < hi ∧
      cst xs lo hi = (seg xs lo hi).sum + cst xs lo s + cst xs s hi := by
  have hs2 : 2 ≤ (seg xs lo hi).length := by
    rw [seg_length xs lo hi hle]
    omega
  obtain ⟨u, v, hu, hv, huv, heq⟩ := best_opt_split (seg xs lo hi) hs2
  have hulen : u.length + v.length = hi - lo := by
    have := congrArg List.length huv
    rw [List.length_append, seg_length xs lo hi hle] at this
    exact this.symm
  let s := lo + u.length
  have hsl : lo < s := by
    have : 0 < u.length := List.length_pos_iff.mpr hu
    simp [s]
    omega
  have hsh : s < hi := by
    have : 0 < v.length := List.length_pos_iff.mpr hv
    simp [s]
    omega
  have hlen1 : (seg xs lo s).length = u.length := by
    rw [seg_length xs lo s (by omega)]
    simp [s]
  have happ : seg xs lo s ++ seg xs s hi = u ++ v :=
    (seg_append xs lo s hi (by omega) (by omega)).trans huv
  have hseg1 : seg xs lo s = u := by
    have := congrArg (fun l : List Nat => l.take u.length) happ
    simpa [hlen1, List.take_append_of_le_length] using this
  have happ' : u ++ seg xs s hi = u ++ v := by simpa [hseg1] using happ
  have hseg2 : seg xs s hi = v := List.append_cancel_left happ'
  refine ⟨s, hsl, hsh, ?_⟩
  unfold cst
  rw [hseg1, hseg2]
  exact heq

private theorem weight_qi (xs : List Nat) (lo lo' hi hi' : Nat)
    (hll : lo ≤ lo') (hlh : lo' ≤ hi) (hhh : hi ≤ hi') (_hle : hi' ≤ xs.length) :
    (seg xs lo hi).sum + (seg xs lo' hi').sum ≤
      (seg xs lo' hi).sum + (seg xs lo hi').sum := by
  let A := (seg xs lo lo').sum
  let B := (seg xs lo' hi).sum
  let C := (seg xs hi hi').sum
  have h1 : (seg xs lo hi).sum = A + B := by
    exact seg_sum_append xs lo lo' hi hll hlh
  have h2 : (seg xs lo' hi').sum = B + C := by
    exact seg_sum_append xs lo' hi hi' hlh hhh
  have h3 : (seg xs lo hi').sum = A + B + C := by
    rw [seg_sum_append xs lo hi hi' (by omega) hhh,
        seg_sum_append xs lo lo' hi hll hlh]
  rw [h1, h2, h3]
  omega

private theorem weight_mono_right (xs : List Nat) (lo hi hi' : Nat)
    (hlh : lo ≤ hi) (hhh : hi ≤ hi') (_hle : hi' ≤ xs.length) :
    (seg xs lo hi).sum ≤ (seg xs lo hi').sum := by
  rw [seg_sum_append xs lo hi hi' hlh hhh]
  omega

private theorem weight_mono_left (xs : List Nat) (lo lo' hi : Nat)
    (hll : lo ≤ lo') (hlh : lo' ≤ hi) :
    (seg xs lo' hi).sum ≤ (seg xs lo hi).sum := by
  rw [seg_sum_append xs lo lo' hi hll hlh]
  omega

/-- Yao's lemma.  Induct on the length `hi' - lo` of the long side.  The optimal
split of the long interval and of the two shorter intervals are compared; every
induction hypothesis is applied to a strictly shorter long side. -/
private theorem qi_measure (xs : List Nat) (n : Nat) :
    ∀ lo lo' hi hi' : Nat, lo ≤ lo' → lo' < hi → hi ≤ hi' → hi' ≤ xs.length →
      hi' - lo ≤ n →
      cst xs lo hi + cst xs lo' hi' ≤ cst xs lo' hi + cst xs lo hi' := by
  induction n with
  | zero =>
      intro lo lo' hi hi' hll hlt hhh hle hn
      omega
  | succ n ih =>
      intro lo lo' hi hi' hll hlt hhh hle hn
      by_cases hdec : hi' - lo ≤ n
      · exact ih lo lo' hi hi' hll hlt hhh hle hdec
      · have hmeas : hi' - lo = n + 1 := by omega
        by_cases hlo_eq : lo = lo'
        · subst lo'
          omega
        have hlo_lt : lo < lo' := by omega
        by_cases hA : lo' + 2 ≤ hi
        · -- Case B: the middle interval has at least two elements.
          obtain ⟨y, hly, hyh, hy_eq⟩ := cst_opt_split xs lo' hi hA (by omega)
          have hlo_hi' : lo + 2 ≤ hi' := by omega
          obtain ⟨z, hlz, hzh', hz_eq⟩ := cst_opt_split xs lo hi' hlo_hi' hle
          by_cases hzy : z < y
          · have hub1 := cst_le_split xs lo z hi (by omega) (by omega) (by omega)
            have hub2 := cst_le_split xs lo' y hi' (by omega) (by omega) hle
            have hmeas2 : hi' - z ≤ n := by omega
            have hih := ih z y hi hi' (by omega) (by omega) (by omega) hle hmeas2
            have hw := weight_qi xs lo lo' hi hi' hll (by omega) hhh hle
            omega
          · have hyz : y ≤ z := by omega
            have hub1 := cst_le_split xs lo y hi (by omega) (by omega) (by omega)
            have hub2 := cst_le_split xs lo' z hi' (by omega) (by omega) hle
            have hmeas2 : z - lo ≤ n := by omega
            have hih := ih lo lo' y z hll (by omega) (by omega) (by omega) hmeas2
            have hw := weight_qi xs lo lo' hi hi' hll (by omega) hhh hle
            omega
        · -- Case A: the middle interval is a single element, so hi = lo' + 1.
          have hi_succ : hi = lo' + 1 := by omega
          have hcst_lo_hi : cst xs lo' hi = 0 := cst_small xs lo' hi (by omega)
          have hlo_hi' : lo + 2 ≤ hi' := by omega
          obtain ⟨z, hlz, hzh', hz_eq⟩ := cst_opt_split xs lo hi' hlo_hi' hle
          by_cases hzle : z ≤ lo'
          · by_cases hzeq : z = lo'
            · subst z
              have hub := cst_le_split xs lo lo' hi hlo_lt (by omega) (by omega)
              have hw := weight_mono_right xs lo hi hi' (by omega) (by omega) hle
              omega
            · have hzlt : z < lo' := by omega
              have hub := cst_le_split xs lo z hi (by omega) (by omega) (by omega)
              have hmeas2 : hi' - z ≤ n := by omega
              have hih := ih z lo' hi hi' (by omega) hlt (by omega) hle hmeas2
              have hw := weight_mono_right xs lo hi hi' (by omega) (by omega) hle
              omega
          · have hzge : hi ≤ z := by omega
            have hub2 := cst_le_split xs lo' z hi' (by omega) (by omega) hle
            by_cases hzeq : z = hi
            · subst z
              have hw := weight_mono_left xs lo lo' hi' (by omega) (by omega)
              omega
            · have hzgt : hi < z := by omega
              have hmeas2 : z - lo ≤ n := by omega
              have hih := ih lo lo' hi z hll (by omega) (by omega) (by omega) hmeas2
              have hw := weight_mono_left xs lo lo' hi' (by omega) (by omega)
              omega

theorem sound_Quadrangle :
    TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge.Quadrangle.Goal := by
  intro a b c hb
  by_cases ha : a = []
  · subst a
    simp
    omega
  by_cases hc : c = []
  · subst c
    simp
  let xs := a ++ b ++ c
  let lo := 0
  let lo' := a.length
  let hi := a.length + b.length
  let hi' := xs.length
  have hll : lo ≤ lo' := by simp [lo, lo']
  have hlt : lo' < hi := by
    have hbpos : 0 < b.length := List.length_pos_iff.mpr hb
    simp [lo', hi]
    omega
  have hhh : hi ≤ hi' := by
    simp [hi, hi', xs]
  have hle : hi' ≤ xs.length := by omega
  have hq := qi_measure xs (hi' - lo) lo lo' hi hi' hll hlt hhh hle (by omega)
  have hs1 : seg xs 0 hi = a ++ b := by
    unfold seg
    simp only [List.drop_zero, Nat.sub_zero]
    dsimp [xs, hi]
    change List.take (a.length + b.length) (a ++ b ++ c) = a ++ b
    rw [List.take_add]
    have hdrop : List.drop a.length (a ++ b ++ c) = b ++ c := by
      simp
    rw [hdrop]
    have htakea : List.take a.length (a ++ b ++ c) = a := by simp
    have htakeb : List.take b.length (b ++ c) = b := by simp
    rw [htakea, htakeb]
  have hs2 : seg xs lo' hi = b := by
    unfold seg
    have hsub : hi - lo' = b.length := by simp [hi, lo']
    rw [hsub]
    have hdrop : List.drop lo' xs = b ++ c := by simp [xs, lo']
    rw [hdrop]
    simp
  have hs3 : seg xs lo' hi' = b ++ c := by
    unfold seg
    have hsub : hi' - lo' = (b ++ c).length := by
      simp [hi', lo', xs]
      omega
    rw [hsub]
    have hdrop : List.drop lo' xs = b ++ c := by simp [xs, lo']
    rw [hdrop]
    exact List.take_length
  have hs4 : seg xs 0 hi' = a ++ b ++ c := by
    unfold seg
    simp only [List.drop_zero, Nat.sub_zero]
    change List.take xs.length xs = xs
    exact List.take_length
  unfold cst at hq
  rw [hs1, hs2, hs3, hs4] at hq
  omega

end TinylambOpt.ResearchProofs.Atlas.PlanSearch.MonotoneCutAndQuadrangle
