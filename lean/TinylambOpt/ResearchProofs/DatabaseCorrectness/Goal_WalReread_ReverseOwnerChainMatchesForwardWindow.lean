import TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

/-! Reverse WAL traversal agrees with the staged owner window.

A valid owner chain is ordered by start position and each non-head record links
to its predecessor's start. Following those links backwards from the last
window record therefore visits exactly the owner's window records in reverse,
even when other transactions' records fill the gaps. SQL execution may switch
from staged memory writes to WAL re-read without losing read-your-writes order. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_ReverseOwnerChainMatchesForwardWindow

open TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

private theorem find_of_mem (log : List Record) (hpair : log.Pairwise (fun a b => a.start < b.start))
    (owner : Nat) (r : Record) (hr : r ∈ log) (hro : r.owner = owner) :
    log.find? (fun x => x.start == r.start && x.owner == owner) = some r := by
  cases log with
  | nil => simp at hr
  | cons a tl =>
      rw [List.find?_cons]
      rw [List.pairwise_cons] at hpair
      by_cases har : a = r
      · subst r
        simp [hro]
      · have hrt : r ∈ tl := (List.mem_cons.mp hr).resolve_left (by simpa [eq_comm] using har)
        have hlt : a.start < r.start := hpair.1 r hrt
        have hpred : (a.start == r.start && a.owner == owner) = false := by
          have hne : a.start ≠ r.start := Nat.ne_of_lt hlt
          simp [hne]
        rw [hpred]
        exact find_of_mem tl hpair.2 owner r hrt hro

private theorem linked_snoc_prev (pre : List Record) (z y : Record)
    (hgl : pre.getLast? = some y) (h : linked (pre ++ [z])) : z.prev = y.start := by
  induction pre with
  | nil => simp at hgl
  | cons a tl ih =>
      cases tl with
      | nil =>
          have hh : z.prev = a.start ∧ a.start < z.start := by
            simpa [linked] using h
          have hya : y = a := by simpa using hgl.symm
          rw [hya]
          exact hh.1
      | cons b tls =>
          have hh : linked ((b :: tls) ++ [z]) := (by
            have ht : b.prev = a.start ∧ a.start < b.start ∧ linked ((b :: tls) ++ [z]) := by
              simpa [linked] using h
            exact ht.2.2)
          have hgl' : (b :: tls).getLast? = some y := by simpa using hgl
          exact ih hgl' hh

private theorem linked_prefix (l : List Record) (z : Record) : linked (l ++ [z]) → linked l := by
  induction l with
  | nil => simp [linked]
  | cons a tl ih =>
      cases tl with
      | nil => simp [linked]
      | cons b tls =>
          intro h
          have ht : b.prev = a.start ∧ a.start < b.start ∧ linked (b :: (tls ++ [z])) := by
            simpa [linked] using h
          have hbt : linked (b :: tls) := ih ht.2.2
          exact ⟨ht.1, ht.2.1, hbt⟩

private theorem backward_chain (log : List Record) (owner first : Nat) :
    ∀ n (chain : List Record),
      chain.length = n →
      chain.head?.map Record.start = some first →
      (∀ r ∈ chain, r.owner = owner ∧ log.find? (fun x => x.start == r.start && x.owner == owner) = some r) →
      chain.Pairwise (fun a b => a.start < b.start) →
      linked chain →
      backward log owner first n (((chain.getLast?).map Record.start).getD 0) = chain.reverse := by
  intro n
  induction n with
  | zero =>
      intro chain hlen hhead hfind hchainp hlinked
      have hc : chain = [] := List.length_eq_zero_iff.mp hlen
      subst chain
      simp at hhead
  | succ n ih =>
      intro chain hlen hhead hfind hchainp hlinked
      have hcne : chain ≠ [] := by intro hc; subst chain; simp at hhead
      cases hgl : chain.getLast? with
      | none =>
          exfalso
          exact hcne (List.getLast?_eq_none_iff.mp hgl)
      | some z =>
          rcases (List.getLast?_eq_some_iff.mp hgl) with ⟨pre, hchain⟩
          have hprelen : pre.length = n := by
            rw [hchain] at hlen
            simp at hlen
            omega
          have hzmem : z ∈ chain := by rw [hchain]; simp
          have hz := hfind z hzmem
          have hzstart : z.start = (((chain.getLast?).map Record.start).getD 0) := by
            rw [hgl]
            rfl
          change backward log owner first (n + 1) z.start = chain.reverse
          cases hpre : pre with
          | nil =>
              have hn : n = 0 := by simpa [hpre] using hprelen.symm
              subst n
              subst pre
              rw [hchain]
              simp only [List.reverse_singleton, List.cons_append, List.nil_append]
              dsimp [backward]
              rw [hz.2]
              simp [hhead]
          | cons a pre' =>
              have hprehead : (pre.head?).map Record.start = some first := by
                rw [hchain] at hhead
                simpa [hpre] using hhead
              have hafirst : a.start = first := by simpa [hpre] using hprehead
              have hfindpre : ∀ r ∈ pre, r.owner = owner ∧ log.find? (fun x => x.start == r.start && x.owner == owner) = some r := by
                intro r hr
                apply hfind r
                rw [hchain]
                simp [hr]
              have hpapp : (pre ++ [z]).Pairwise (fun a b => a.start < b.start) := by
                simpa [hchain] using hchainp
              have hprepair : pre.Pairwise (fun a b => a.start < b.start) :=
                List.Pairwise.sublist (by simp : pre.Sublist (pre ++ [z])) hpapp
              have hlinkedpre : linked pre :=
                linked_prefix pre z (by simpa [hchain] using hlinked)
              have ihpre := ih pre hprelen hprehead hfindpre hprepair hlinkedpre
              have ha_mem : a ∈ pre := by simp [hpre]
              have haz : a.start < z.start := hpapp.rel_of_mem_append ha_mem (by simp)
              have hz_ne_first : (z.start == first) = false := by
                have : z.start ≠ first := by omega
                simp [this]
              cases hyl : pre.getLast? with
              | none => exact (by simpa [hpre] using hyl)
              | some y =>
                  have hg : pre.getLast? = some (pre.getLast (by simp [hpre])) :=
                    List.getLast?_eq_some_getLast (by simp [hpre])
                  have hy_eq : y = pre.getLast (by simp [hpre]) := by
                    exact Option.some.inj (hyl.symm.trans hg)
                  have hy_mem : y ∈ pre := by
                    rw [hy_eq]
                    exact List.getLast_mem (by simp [hpre])
                  have hzprev : z.prev = y.start := linked_snoc_prev pre z y hyl (by simpa [hchain] using hlinked)
                  have hy_lt_z : y.start < z.start := hpapp.rel_of_mem_append hy_mem (by simp)
                  have hfirst_le_y : first ≤ y.start := by
                    have hy' : y ∈ a :: pre' := by simpa [hpre] using hy_mem
                    rcases List.mem_cons.mp hy' with rfl | hyp
                    · omega
                    · have hpcons : (a :: pre').Pairwise (fun a b => a.start < b.start) := by simpa [hpre] using hprepair
                      rw [List.pairwise_cons] at hpcons
                      have : a.start < y.start := hpcons.1 y hyp
                      omega
                  have hstop : (z.start == first || z.start ≤ z.prev || z.prev < first) = false := by
                    have hnle : ¬ z.start ≤ z.prev := by omega
                    have hnlt : ¬ z.prev < first := by omega
                    simp [hz_ne_first, hnle, hnlt]
                  dsimp [backward]
                  rw [hz.2]
                  simp only [Option.some.injEq, ↓reduceIte]
                  rw [hstop]
                  rw [hzprev]
                  have ihpre' : backward log owner first n y.start = pre.reverse := by
                    simpa [hyl] using ihpre
                  simp only [Bool.false_eq_true, ↓reduceIte]
                  rw [ihpre']
                  rw [hchain]
                  simp

theorem sound : ReverseOwnerChainMatchesForwardWindow.Goal := by
  intro log owner first last endPos hnodup hpair
  dsimp
  intro hhead hlast hlinked
  let own := log.filter (fun r => r.owner == owner && first ≤ r.start && r.start < endPos)
  have hfind : ∀ r ∈ own, r.owner = owner ∧ log.find? (fun x => x.start == r.start && x.owner == owner) = some r := by
    intro r hr
    have hrmem : r ∈ log := (List.mem_filter.mp hr).1
    rw [List.mem_filter] at hr
    simp only [Bool.and_eq_true] at hr
    have hro : r.owner = owner := beq_iff_eq.mp hr.2.1.1
    exact ⟨hro, find_of_mem log hpair owner r hrmem hro⟩
  have hownp : own.Pairwise (fun a b => a.start < b.start) := by
    exact hpair.filter _
  have hback := backward_chain log owner first own.length own rfl hhead hfind hownp hlinked
  have hlast' : (own.getLast?).map Record.start = some last := by simpa [own] using hlast
  have hcur : (((own.getLast?).map Record.start).getD 0) = last := by simpa [hlast']
  rw [hcur] at hback
  rw [hback]
  simp [ownEdits, own, List.filterMap_reverse]
end TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_ReverseOwnerChainMatchesForwardWindow
