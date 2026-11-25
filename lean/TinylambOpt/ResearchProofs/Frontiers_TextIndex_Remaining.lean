import TinylambOpt.ResearchGoals.Frontiers.TextIndex
import TinylambOpt.ResearchProofs.Frontiers.TextIndex

namespace TinylambOpt.ResearchProofs.Frontiers.TextIndex.Remaining

open TinylambOpt.ResearchGoals.Frontiers.TextIndex

private theorem sa_perm (s : Word) : (sa s).Perm (List.range (s.length+1)) := by
  unfold sa
  exact List.mergeSort_perm (List.range (s.length+1))
    (fun i j : Nat => decide (s.drop i ≤ s.drop j))

private theorem mem_sa_iff {s : Word} {i : Nat} : i ∈ sa s ↔ i ≤ s.length := by
  unfold sa
  rw [List.mem_mergeSort]
  rw [List.mem_range]
  omega

private theorem sa_nodup (s : Word) : (sa s).Nodup := by
  exact (sa_perm s).symm.nodup List.nodup_range

private theorem sa_pairwise (s : Word) :
    (sa s).Pairwise (fun i j => (decide (s.drop i ≤ s.drop j) : Bool) = true) := by
  unfold sa
  apply List.pairwise_mergeSort
  · intro a b c hab hbc
    have hab' : s.drop a ≤ s.drop b := of_decide_eq_true hab
    have hbc' : s.drop b ≤ s.drop c := of_decide_eq_true hbc
    exact decide_eq_true_eq.mpr (List.le_trans hab' hbc')
  · intro a b
    rcases List.le_total (s.drop a) (s.drop b) with h | h
    · simp [h]
    · simp [h]

private theorem pairwise_drop_lt_of_le_of_nodup (s : Word) {l : List Nat}
    (hle : l.Pairwise (fun i j => (decide (s.drop i ≤ s.drop j) : Bool) = true)) (hnd : l.Nodup)
    (hall : ∀ i ∈ l, i ≤ s.length) :
    l.Pairwise (fun i j => s.drop i < s.drop j) := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons a t ih =>
      rw [List.pairwise_cons] at hle ⊢
      have hnd' := (List.nodup_cons.mp hnd).2
      have hanot : a ∉ t := (List.nodup_cons.mp hnd).1
      have hall' : ∀ i ∈ t, i ≤ s.length := by
        intro i hi
        exact hall i (by simp [hi])
      refine ⟨?_, ih hle.2 hnd' hall'⟩
      intro b hb
      have hleab : (decide (s.drop a ≤ s.drop b) : Bool) = true := hle.1 b hb
      have hle' : s.drop a ≤ s.drop b := of_decide_eq_true hleab
      have hab : a ≠ b := by
        intro heq
        subst b
        exact hanot hb
      have hne : s.drop a ≠ s.drop b := by
        intro hd
        have hleneq := congrArg List.length hd
        simp only [List.length_drop] at hleneq
        have ha := hall a (by simp)
        have hbnd := hall b (by simp [hb])
        omega
      rcases List.le_iff_lt_or_eq.mp hle' with hlt | heq
      · exact hlt
      · exact False.elim (hne heq)

private theorem sa_sorted_lt (s : Word) :
    (sa s).Pairwise (fun i j => s.drop i < s.drop j) := by
  exact pairwise_drop_lt_of_le_of_nodup s (sa_pairwise s) (sa_nodup s)
    (by intro i hi; exact mem_sa_iff.mp hi)

private theorem take_count_eq_filter_aux {α : Type} (p : α → Bool) (less : α → α → Prop)
    : ∀ l : List α,
    l.Pairwise less →
    (∀ a b, a ∈ l → b ∈ l → less a b → p b = true → p a = true) →
    l.take (l.countP p) = l.filter p := by
  intro l hpair hdown
  induction l with
  | nil => simp
  | cons a t ih =>
      rw [List.pairwise_cons] at hpair
      by_cases hp : p a = true
      · have hcnt : (a :: t).countP p = t.countP p + 1 := by simp [hp]
        have hfil : (a :: t).filter p = a :: t.filter p := by simp [hp]
        rw [hcnt, hfil]
        simp
        apply ih hpair.2
        intro b c hb hc hlt hpc
        exact hdown b c (by simp [hb]) (by simp [hc]) hlt hpc
      · have htfalse : ∀ b ∈ t, ¬ p b = true := by
          intro b hb hpb
          have hlt : less a b := hpair.1 b hb
          have hpa := hdown a b (by simp) (by simp [hb]) hlt hpb
          exact hp hpa
        have htzero : t.countP p = 0 := List.countP_eq_zero.mpr htfalse
        have hcnt : (a :: t).countP p = 0 := by simp [hp, htzero]
        have hfil : (a :: t).filter p = [] := by
          simp [hp, List.filter_eq_nil_iff.mpr htfalse]
        rw [hcnt, hfil]
        simp

private theorem take_eq_filter (s p : Word) :
    (sa s).take (lo s p) = (sa s).filter (fun i => decide (s.drop i < p)) := by
  unfold lo
  apply take_count_eq_filter_aux
  · exact sa_sorted_lt s
  · intro a b ha hb hlt hpb
    have hlt' : s.drop a < s.drop b := hlt
    have hpb' : s.drop b < p := of_decide_eq_true hpb
    exact decide_eq_true_eq.mpr (List.lt_trans hlt' hpb')

private theorem count_range_succ (f : Nat → Bool) (n : Nat) :
    (List.range (n+1)).countP f = (if f 0 then 1 else 0) +
      (List.range n).countP (fun i => f (1+i)) := by
  rw [show n + 1 = 1 + n by omega, List.range_add, List.countP_append, List.countP_map]
  rfl

/-! ## counting suffixes -/

private def countSufLt (s p : Word) : Nat :=
  (List.range (s.length + 1)).countP (fun i => decide (s.drop i < p))

private theorem lo_eq_countSufLt (s p : Word) : lo s p = countSufLt s p := by
  unfold lo countSufLt
  exact List.Perm.countP_eq (fun i => decide (s.drop i < p)) (sa_perm s)

private theorem countSufLt_cons (b : Nat) (t p : Word) :
    countSufLt (b :: t) p = (if (b :: t) < p then 1 else 0) + countSufLt t p := by
  unfold countSufLt
  rw [show (b :: t).length + 1 = (t.length + 1) + 1 by simp only [List.length_cons]]
  rw [count_range_succ]
  congr 1
  · simp
  · apply List.countP_congr
    intro i hi
    have hd : (b :: t).drop (1 + i) = t.drop i := by
      simp [Nat.add_comm]
    rw [hd]

private def countPre (s : Word) (a : Nat) (p : Word) : Nat :=
  (List.range s.length).countP (fun j => decide (s[j]? == some a && s.drop (j+1) < p))

private theorem preceding_succ_eq_get (s : Word) (j : Nat) : preceding s (j+1) = s[j]? := by
  unfold preceding
  simp

private theorem countPre_cons (b a : Nat) (t p : Word) :
    countPre (b :: t) a p = (if b == a && t < p then 1 else 0) + countPre t a p := by
  unfold countPre
  rw [show (b :: t).length = t.length + 1 by simp only [List.length_cons],
    count_range_succ]
  simp
  apply List.countP_congr
  intro i hi
  have hg : (b :: t)[1 + i]? = t[i]? := by
    simp [Nat.add_comm]
  rw [hg]
  simp [Nat.add_comm]

private theorem occ_range_eq_countPre (s : Word) (a : Nat) (p : Word) :
    (List.range (s.length+1)).countP
        (fun i => decide (s.drop i < p) && (preceding s i == some a)) =
      countPre s a p := by
  unfold countPre
  rw [count_range_succ]
  simp [preceding]
  apply List.countP_congr
  intro i hi
  have hd : s.drop (1 + i) = s.drop (i + 1) := by rw [Nat.add_comm]
  rw [hd]
  simp [Bool.and_comm]

private theorem occ_lo_eq_countPre (s : Word) (a : Nat) (p : Word) :
    occ s a (lo s p) = countPre s a p := by
  unfold occ
  rw [take_eq_filter]
  have hperm := (sa_perm s).filter (fun i => decide (s.drop i < p))
  rw [List.Perm.countP_eq (fun i => preceding s i == some a) hperm]
  rw [List.countP_filter]
  simpa [Bool.and_comm] using occ_range_eq_countPre s a p

private theorem cons_lt_cons_iff (b a : Nat) (t p : Word) :
    (b :: t) < (a :: p) ↔ b < a ∨ (b = a ∧ t < p) := by
  constructor
  · intro h
    change List.Lex (· < ·) (b :: t) (a :: p) at h
    cases h with
    | rel hlt => exact Or.inl hlt
    | cons hlex => exact Or.inr ⟨rfl, hlex⟩
  · intro h
    change List.Lex (· < ·) (b :: t) (a :: p)
    rcases h with hlt | ⟨rfl, hlt⟩
    · exact List.Lex.rel hlt
    · exact List.Lex.cons hlt

private theorem ind_cons_lt (b a : Nat) (t p : Word) :
    (if b :: t < a :: p then 1 else 0) =
      (if b < a then 1 else 0) + (if b == a && t < p then 1 else 0) := by
  by_cases hba : b < a
  · have hlt : b :: t < a :: p := (cons_lt_cons_iff b a t p).mpr (Or.inl hba)
    have hne : b ≠ a := by omega
    simp [hba, hlt, hne]
  · by_cases heq : b = a
    · subst b
      by_cases htp : t < p
      · have hlt : a :: t < a :: p := (cons_lt_cons_iff a a t p).mpr (Or.inr ⟨rfl, htp⟩)
        simp [htp, hlt]
      · have hnlt : ¬ a :: t < a :: p := by
          intro h
          rcases (cons_lt_cons_iff a a t p).mp h with haa | ⟨_, htp'⟩
          · exact (Nat.lt_irrefl a) haa
          · exact htp htp'
        simp [htp, hnlt]
    · have hnlt : ¬ b :: t < a :: p := by
        intro h
        rcases (cons_lt_cons_iff b a t p).mp h with hba' | ⟨heq', _⟩
        · exact hba hba'
        · exact heq heq'
      have hbeq : (b == a) = false := by simp [heq]
      simp [hba, hnlt, hbeq]

private theorem countSufLt_eq (s : Word) (a : Nat) (p : Word) :
    countSufLt s (a :: p) = first s a + countPre s a p := by
  induction s with
  | nil => simp [countSufLt, first, countPre]
  | cons b t ih =>
      rw [countSufLt_cons, ind_cons_lt, ih]
      unfold first
      simp only [List.countP_cons]
      rw [countPre_cons]
      simp
      omega

/-! ## Soundness of BackwardSearch -/

theorem sound_BackwardSearch :
    TinylambOpt.ResearchGoals.Frontiers.TextIndex.BackwardSearch.Goal := by
  intro s p a
  dsimp
  have hlo : lo s (a :: p) = first s a + occ s a (lo s p) := by
    rw [lo_eq_countSufLt, countSufLt_eq, occ_lo_eq_countPre]
  have hlen : (hits s (a :: p)).length =
      occ s a (lo s p + (hits s p).length) - occ s a (lo s p) := by
    have hcont := Contiguous_sound s p
    have htake : (sa s).take (lo s p + (hits s p).length) =
        (sa s).take (lo s p) ++ hits s p := by
      rw [List.take_add, hcont]
    unfold occ
    rw [htake, List.countP_append]
    have hnext := LocateExtension_sound s p a
    rw [hnext]
    simp [List.countP_eq_length_filter]
  rw [hlo, hlen]

/-! ## Row indices and the LF step -/

private theorem countP_lt_eq_idxOf_aux (s : Word) (i : Nat) : ∀ l : List Nat,
    l.Pairwise (fun a b => s.drop a < s.drop b) →
    l.Nodup → i ∈ l →
    l.countP (fun j => s.drop j < s.drop i) = l.idxOf i := by
  intro l hpair hnd hi
  induction l with
  | nil => simp at hi
  | cons a t ih =>
      rw [List.pairwise_cons] at hpair
      have hnd' := (List.nodup_cons.mp hnd).2
      have hanot : a ∉ t := (List.nodup_cons.mp hnd).1
      by_cases hai : a = i
      · subst i
        have hzero : t.countP (fun j => s.drop j < s.drop a) = 0 := by
          apply List.countP_eq_zero.mpr
          intro j hj hlt
          have haj : s.drop a < s.drop j := hpair.1 j hj
          have hlt' : s.drop j < s.drop a := of_decide_eq_true hlt
          exact (List.not_lt.mpr (List.le_of_lt haj)) hlt'
        simp [hzero]
      · have hit : i ∈ t := by
          rcases List.mem_cons.mp hi with hia | hit
          · exact False.elim (hai hia.symm)
          · exact hit
        have halt : s.drop a < s.drop i := hpair.1 i hit
        have hcnt : (a :: t).countP (fun j => s.drop j < s.drop i) =
            t.countP (fun j => s.drop j < s.drop i) + 1 := by simp [halt]
        rw [hcnt, ih hpair.2 hnd' hit]
        rw [List.idxOf_cons]
        simp [hai]

private theorem lo_drop_eq_idxOf (s : Word) {i : Nat} (hi : i ≤ s.length) :
    lo s (s.drop i) = (sa s).idxOf i := by
  unfold lo
  exact countP_lt_eq_idxOf_aux s i (sa s) (sa_sorted_lt s) (sa_nodup s)
    (mem_sa_iff.mpr hi)

private theorem idxOf_eq_of_getElem? {l : List Nat} (hnd : l.Nodup) {r x : Nat}
    (h : l[r]? = some x) : l.idxOf x = r := by
  rcases List.getElem?_eq_some_iff.mp h with ⟨hr, hget⟩
  have hmem : x ∈ l := List.mem_of_getElem hget
  have hlt := List.idxOf_lt_length_of_mem hmem
  have hget' := List.getElem_idxOf hlt
  exact (hnd.getElem_inj (hi := hlt) (hj := hr)).mp (hget'.trans hget.symm)

private theorem drop_pred_of_preceding {s : Word} {i a : Nat}
    (h : preceding s i = some a) : s.drop (i - 1) = a :: s.drop i := by
  unfold preceding at h
  by_cases hi0 : i == 0
  · simp [hi0] at h
  · have hget : s[i - 1]? = some a := by simpa [hi0] using h
    have hpos : 0 < i := by
      by_cases hi : i = 0
      · simp [hi] at hi0
      · omega
    have htake : s.take i = s.take (i - 1) ++ [a] := by
      rw [← Nat.sub_add_cancel (Nat.succ_le_of_lt hpos), List.take_add_one]
      simp [hget]
    have h1 := List.take_append_drop (i - 1) s
    have h2 := List.take_append_drop i s
    rw [htake] at h2
    have h3 : s.take (i - 1) ++ (a :: s.drop i) = s.take (i - 1) ++ s.drop (i - 1) := by
      simpa [List.append_assoc] using h2.trans h1.symm
    exact List.append_cancel_left h3.symm

private theorem lf_step (s : Word) {rr pos : Nat}
    (hrr : (sa s)[rr]? = some pos) (hpos : 0 < pos) :
    (sa s)[lf s rr]? = some (pos - 1) := by
  have hposmem : pos ∈ sa s := by
    rcases List.getElem?_eq_some_iff.mp hrr with ⟨hr, hget⟩
    exact List.mem_of_getElem hget
  have hposle : pos ≤ s.length := mem_sa_iff.mp hposmem
  have hidx : (sa s).idxOf pos = rr := idxOf_eq_of_getElem? (sa_nodup s) hrr
  cases hp : preceding s pos with
  | none =>
      unfold preceding at hp
      have hp0 : (pos == 0) = false := by simp [Nat.ne_of_gt hpos]
      have hpnone : s[pos - 1]? = none := by simpa [hp0] using hp
      have hltidx : pos - 1 < s.length := by omega
      have hpsome : s[pos - 1]? = some (s[pos - 1]'hltidx) := by simp
      rw [hpsome] at hpnone
      simp at hpnone
  | some a =>
      have hdrop : s.drop (pos - 1) = a :: s.drop pos := drop_pred_of_preceding hp
      have hlf : lf s rr = first s a + occ s a rr := by
        unfold lf
        simp [hrr, hp]
      have hidxpred : (sa s).idxOf (pos - 1) = first s a + occ s a rr := by
        rw [← lo_drop_eq_idxOf s (by omega : pos - 1 ≤ s.length)]
        rw [lo_eq_countSufLt, hdrop, countSufLt_eq]
        have hrrlo : lo s (s.drop pos) = rr := by
          rw [lo_drop_eq_idxOf s hposle, hidx]
        rw [← hrrlo, occ_lo_eq_countPre]
      have hmem : pos - 1 ∈ sa s := mem_sa_iff.mpr (by omega)
      have hlt := List.idxOf_lt_length_of_mem hmem
      have hget := List.getElem_idxOf (xs := sa s) hlt
      have hopt : (sa s)[(sa s).idxOf (pos - 1)]? = some (pos - 1) :=
        List.getElem?_eq_some_iff.mpr ⟨hlt, hget⟩
      rw [hidxpred] at hopt
      rw [hlf]
      exact hopt

private theorem lf_iter_value (s : Word) (pos rr k : Nat)
    (hrr : (sa s)[rr]? = some pos) (hk : k ≤ pos) :
    (sa s)[rewind s k rr]? = some (pos - k) := by
  induction k generalizing rr pos with
  | zero => simpa [rewind] using hrr
  | succ k ih =>
      rw [rewind]
      have hpos : 0 < pos := by omega
      have hlf := lf_step s hrr hpos
      have hik : k ≤ pos - 1 := by omega
      have hres := ih (rr := lf s rr) (pos := pos - 1) hlf hik
      rw [show pos - 1 - k = pos - (k + 1) by omega] at hres
      exact hres

/-! ## Soundness of SampledLocate -/

theorem sound_SampledLocate :
    TinylambOpt.ResearchGoals.Frontiers.TextIndex.SampledLocate.Goal := by
  intro s r stride hr hstride
  dsimp
  have hgr : (sa s)[r]? = some ((sa s)[r]'hr) := by simp
  let pos := (sa s)[r]?.getD 0
  have hpos : pos = (sa s)[r]'hr := by
    dsimp [pos]
    rw [hgr]
    simp
  have hsome : (sa s)[r]? = some pos := by rw [hpos]; exact hgr
  exact lf_iter_value s pos r (pos % stride) hsome (Nat.mod_le _ _)

end TinylambOpt.ResearchProofs.Frontiers.TextIndex.Remaining
