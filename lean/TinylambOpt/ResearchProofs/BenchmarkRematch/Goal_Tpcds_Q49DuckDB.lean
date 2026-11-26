import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q49DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- On a sorted list every prefix element is dominated by every suffix element. -/
private theorem take_le_drop {l : List Nat}
    (hp : l.Pairwise (fun a b : Nat => decide (a ≤ b) = true)) :
    ∀ (n : Nat), ∀ w ∈ l.take n, ∀ y ∈ l.drop n, decide (w ≤ y) = true := by
  induction l with
  | nil =>
    intro n w hw
    exact absurd hw (by simp)
  | cons a as ih =>
    obtain ⟨h1, h2⟩ := List.pairwise_cons.mp hp
    intro n w hw y hy
    by_cases hn : n = 0
    · subst hn
      exact absurd hw (by simp)
    · have hn0 : 0 < n := Nat.pos_of_ne_zero hn
      rw [List.take_cons hn0] at hw
      rw [List.drop_cons hn0] at hy
      obtain rfl | hwm := List.mem_cons.mp hw
      · exact h1 y (List.mem_of_mem_drop hy)
      · exact ih h2 (n - 1) w hwm y hy

/-- The `RANK() <= 10` guard on the ascending key order agrees with counting
how many keys are strictly below `x`: the 10th sorted key, when it exists,
witnesses the boundary. -/
private theorem pw (keys : List Nat) (x : Nat) :
    (keys.mergeSort[9]?).all (x ≤ ·) = decide (countWhere keys (· < x) < 10) := by
  show (keys.mergeSort[9]?).all (fun z => decide (x ≤ z)) =
      decide ((keys.filter (fun k => decide (k < x))).length < 10)
  have trans : ∀ a b c : Nat, decide (a ≤ b) = true → decide (b ≤ c) = true →
      decide (a ≤ c) = true := by
    intro a b c h1 h2
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2))
  have total : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    rcases Nat.le_total a b with h | h
    · rw [decide_eq_true h]; simp
    · rw [decide_eq_true h]; simp
  have hp : keys.mergeSort.Pairwise (fun a b : Nat => decide (a ≤ b) = true) :=
    List.pairwise_mergeSort trans total keys
  have hperm : (keys.mergeSort.filter (fun k => decide (k < x))).length =
      (keys.filter (fun k => decide (k < x))).length :=
    ((List.mergeSort_perm keys (fun a b => decide (a ≤ b))).filter
      (fun k => decide (k < x))).length_eq
  cases h : keys.mergeSort[9]? with
  | none =>
    rw [Option.all_none]
    symm
    apply decide_eq_true
    rw [List.getElem?_eq_none_iff] at h
    have hle := List.length_filter_le (fun k => decide (k < x)) keys
    have hlen : keys.length = keys.mergeSort.length := (List.length_mergeSort keys).symm
    omega
  | some v =>
    obtain ⟨hl, hvget⟩ := (List.getElem?_eq_some_iff).mp h
    rw [Option.all_some]
    have e10 : (10 : Nat) = 9 + 1 := rfl
    have h10 : keys.mergeSort.take 10 = keys.mergeSort.take 9 ++ [v] := by
      rw [e10, List.take_succ_eq_append_getElem hl, hvget]
    have h9 : keys.mergeSort.drop 9 = v :: keys.mergeSort.drop 10 := by
      rw [List.drop_eq_getElem_cons hl, hvget]
    -- if `v < x`, the whole prefix of ten keys is `< x`: ten rows qualify
    have hA : v < x →
        10 ≤ (keys.mergeSort.filter (fun k => decide (k < x))).length := by
      intro hv
      have hvin : v ∈ keys.mergeSort.take 10 := by rw [h10]; simp
      have hall : ∀ y ∈ keys.mergeSort.take 10, decide (y < x) = true := by
        rw [h10]
        intro y hy
        obtain h19 | hyv := List.mem_append.mp hy
        · have hyv' : decide (y ≤ v) = true :=
            take_le_drop hp 9 y h19 v (by rw [h9]; exact List.mem_cons_self)
          exact decide_eq_true (Nat.lt_of_le_of_lt (of_decide_eq_true hyv') hv)
        · rw [List.mem_singleton.mp hyv]
          exact decide_eq_true hv
      have hfe : (keys.mergeSort.take 10).filter (fun k => decide (k < x)) =
          keys.mergeSort.take 10 := (List.filter_eq_self).mpr hall
      have hlen : (keys.mergeSort.take 10).length = 10 := by
        rw [List.length_take]
        omega
      have hs : List.Sublist ((keys.mergeSort.take 10).filter (fun k => decide (k < x)))
          (keys.mergeSort.filter (fun k => decide (k < x))) :=
        List.Sublist.filter _ (List.take_sublist 10 keys.mergeSort)
      have hle := List.Sublist.length_le hs
      rw [hfe, hlen] at hle
      exact hle
    -- if `x <= v`, no key from position nine onwards qualifies: at most nine rows do
    have hB : ¬(v < x) →
        (keys.mergeSort.filter (fun k => decide (k < x))).length < 10 := by
      intro hvnx
      have hxv : x ≤ v := by omega
      have hvin : v ∈ keys.mergeSort.take 10 := by rw [h10]; simp
      have hd10 : (keys.mergeSort.drop 10).filter (fun k => decide (k < x)) = [] := by
        rw [List.eq_nil_iff_forall_not_mem]
        intro y hmem
        obtain ⟨hydrop, hyp⟩ := List.mem_filter.mp hmem
        have hyv := take_le_drop hp 10 v hvin y hydrop
        have vley : v ≤ y := of_decide_eq_true hyv
        have yltx : y < x := of_decide_eq_true hyp
        omega
      have hvf : ([v] : List Nat).filter (fun k => decide (k < x)) = [] := by
        have hd : decide (v < x) = false := decide_eq_false hvnx
        rw [List.filter_cons, hd]
        simp
      have hsplit : keys.mergeSort.filter (fun k => decide (k < x)) =
          (keys.mergeSort.take 9).filter (fun k => decide (k < x)) := by
        calc keys.mergeSort.filter (fun k => decide (k < x))
            = (keys.mergeSort.take 10 ++ keys.mergeSort.drop 10).filter
                (fun k => decide (k < x)) := by
                rw [List.take_append_drop]
          _ = (keys.mergeSort.take 9 ++ [v] ++ keys.mergeSort.drop 10).filter
                (fun k => decide (k < x)) := by rw [h10]
          _ = (keys.mergeSort.take 9).filter (fun k => decide (k < x)) := by
                rw [List.filter_append, List.filter_append, hvf, hd10,
                  List.append_nil, List.append_nil]
      have h9len : ((keys.mergeSort.take 9).filter (fun k => decide (k < x))).length ≤ 9 := by
        have hle := List.length_filter_le (fun k => decide (k < x)) (keys.mergeSort.take 9)
        have hlt : (keys.mergeSort.take 9).length ≤ 9 := by
          rw [List.length_take]
          omega
        omega
      have hfin : (keys.mergeSort.filter (fun k => decide (k < x))).length ≤ 9 := by
        rw [hsplit]
        exact h9len
      omega
    by_cases e1 : x ≤ v <;>
      by_cases e2 : (keys.filter (fun k => decide (k < x))).length < 10 <;>
        first
          | rw [decide_eq_true e1, decide_eq_true e2]
          | exfalso
            have hct := hB (by omega : ¬(v < x))
            rw [hperm] at hct
            exact absurd hct e2
          | exfalso
            have hct := hA (by omega : v < x)
            rw [hperm] at hct
            omega
          | rw [decide_eq_false e1, decide_eq_false e2]

/-- The two RANK guards select the same detail rows. -/
theorem sound : Q49DuckDB.Goal := by
  intro rows
  show rows.filter (fun (x, y) =>
        ((rows.map Prod.fst).mergeSort[9]?).all (x ≤ ·) ||
          ((rows.map Prod.snd).mergeSort[9]?).all (y ≤ ·)) =
      rows.filter (fun (x, y) =>
        countWhere (rows.map Prod.fst) (· < x) < 10 ||
          countWhere (rows.map Prod.snd) (· < y) < 10)
  refine List.filter_congr (fun xy _ => ?_)
  rcases xy with ⟨x, y⟩
  show (((rows.map Prod.fst).mergeSort[9]?).all (x ≤ ·) ||
        ((rows.map Prod.snd).mergeSort[9]?).all (y ≤ ·)) =
      (countWhere (rows.map Prod.fst) (· < x) < 10 ||
        countWhere (rows.map Prod.snd) (· < y) < 10)
  have h1 := pw (rows.map Prod.fst) x
  have h2 := pw (rows.map Prod.snd) y
  rw [h1, h2]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q49DuckDB.sound
