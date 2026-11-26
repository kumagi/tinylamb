import TinylambOpt.ResearchGoals.EngineDecisions.MVCC
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.GuardedDelete

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons]
      simp [List.mem_eraseDups, nodup_eraseDups (as.filter fun b => ! b == a)]
termination_by l.length
decreasing_by simp_wf; have hle := List.length_filter_le (fun b => ! b == a) as; omega

theorem filter_eq_count (l : List Nat) (b : Nat) : (l.filter (fun k => k == b)).length = l.count b := by
  rw [← List.countP_eq_length_filter, ← List.count_eq_countP]

theorem nodup_filter_eq_length (l : List Nat) (hl : l.Nodup) (b : Nat) :
    (l.filter (fun k => k == b)).length = if b ∈ l then 1 else 0 := by
  rw [filter_eq_count, hl.count]

theorem filter_or_split (l : List Nat) (p q : Nat → Bool) :
    (l.filter (fun k => p k || q k)).length =
      (l.filter p).length + (l.filter (fun k => ! p k && q k)).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases hp : p a <;> cases hq : q a <;> simp [hp, hq, ih] <;> omega

theorem eraseDups_filter_mem_length (l m : List Nat) (p : Nat → Bool) (hm : m.Nodup) :
  (l.eraseDups.filter fun k => p k && k ∈ m).length =
    (m.filter fun k => p k && k ∈ l).length := by
  induction m with
  | nil => simp
  | cons b bs ih =>
      have hbnot : b ∉ bs := (List.nodup_cons.mp hm).1
      have hbs : bs.Nodup := (List.nodup_cons.mp hm).2
      have hnd := nodup_eraseDups l
      have hsplit := filter_or_split l.eraseDups
          (fun k => p k && k == b)
          (fun k => p k && k ∈ bs && !(k == b))
      have hcongr : (l.eraseDups.filter fun k => p k && k ∈ b :: bs) =
          (l.eraseDups.filter fun k => (p k && k == b) || (p k && k ∈ bs && !(k == b))) := by
        apply List.filter_congr
        intro k hk
        by_cases hkb : k == b
        · have hkeq : k = b := by simpa using hkb
          subst k
          simp
        · have hkne : k ≠ b := by
            intro h
            subst k
            simp at hkb
          by_cases hkbs : k ∈ bs <;> simp [hkb, hkbs, hkne]
      rw [hcongr, hsplit]
      have hfirst : (l.eraseDups.filter fun k => p k && k == b).length =
          if p b && b ∈ l then 1 else 0 := by
        cases hp : p b
        · simp [hp]
          intro a ha hpa hab
          subst a
          simp [hp] at hpa
        · have hc : (l.eraseDups.filter fun k => p k && k == b) =
              (l.eraseDups.filter fun k => k == b) := by
              apply List.filter_congr
              intro k hk
              by_cases hkb : k == b
              · have : k = b := by simpa using hkb
                subst k
                simp [hp]
              · simp [hkb]
          rw [hc, nodup_filter_eq_length l.eraseDups hnd b]
          simp [hp, List.mem_eraseDups]
      rw [hfirst]
      have hsecond : (l.eraseDups.filter fun k => !(p k && k == b) && (p k && k ∈ bs && !(k == b))).length =
          (l.eraseDups.filter fun k => p k && k ∈ bs).length := by
        apply congrArg List.length
        apply List.filter_congr
        intro k hk
        by_cases hkb : k == b
        · have : k = b := by simpa using hkb
          subst k
          simp [hbnot]
        · simp [hkb]
      rw [hsecond, ih hbs]
      by_cases hbp : p b && b ∈ l <;> simp [hbp, List.mem_cons] <;> omega

namespace TinylambOpt.ResearchProofs.EngineDecisions.MVCC_GuardedDelete

theorem filter_filter_length {α : Type} (p q : α → Bool) (l : List α) :
    ((l.filter p).filter q).length = ((l.filter q).filter p).length := by
  rw [List.filter_filter, List.filter_filter]
  apply congrArg List.length
  apply List.filter_congr
  intro x hx
  exact Bool.and_comm (q x) (p x)

theorem filter_not_length {α : Type} (p : α → Bool) (l : List α) :
    (l.filter p).length + (l.filter (fun x => !p x)).length = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hp : p x <;> simp [hp] <;> omega

theorem survivors_append_length (A O I : List Nat) :
    (survivors (A ++ I) O).length = (survivors A O).length + (survivors I O).length := by
  simp [survivors, List.filter_append]

theorem sound : Goal := by
  intro snapshot own other inserted quota hsnap hins hdisj
  unfold quotaOK
  rw [decide_eq_true_eq]
  have hbase : snapshot.length - (own.eraseDups.filter fun k => snapshot.contains k).length =
      (survivors snapshot own).length := by
    have h := eraseDups_filter_mem_length own snapshot (fun _ => true) hsnap
    simp only [Bool.true_and] at h
    have h' : (own.eraseDups.filter fun k => snapshot.contains k).length =
        (snapshot.filter fun k => k ∈ own).length := by simpa using h
    have hf := filter_not_length (fun k => k ∈ own) snapshot
    dsimp [survivors]
    rw [h']
    have hf' : (snapshot.filter fun k => k ∈ own).length +
        (snapshot.filter fun k => ! own.contains k).length = snapshot.length := by
      simpa using hf
    omega
  have hlost : (other.eraseDups.filter fun k => snapshot.contains k && !own.contains k).length =
      ((survivors snapshot own).filter fun k => k ∈ other).length := by
    have h := eraseDups_filter_mem_length other snapshot (fun k => ! own.contains k) hsnap
    rw [show (other.eraseDups.filter fun k => snapshot.contains k && !own.contains k) =
        (other.eraseDups.filter fun k => (! own.contains k) && k ∈ snapshot) by
          apply List.filter_congr
          intro k hk
          simp [Bool.and_comm]]
    rw [h]
    dsimp [survivors]
    simp [List.filter_filter, Bool.and_comm]
  rw [hbase, hlost]
  have hrem : (survivors snapshot own).length -
        ((survivors snapshot own).filter fun k => k ∈ other).length =
      (survivors (survivors snapshot other) own).length := by
    have hf := filter_not_length (fun k => k ∈ other) (survivors snapshot own)
    have hsw : ((survivors snapshot own).filter fun k => ! k ∈ other).length =
        (survivors (survivors snapshot other) own).length := by
      dsimp [survivors]
      rw [filter_filter_length]
      apply congrArg List.length
      apply List.filter_congr
      intro k hk
      simp
    rw [← hsw]
    omega
  rw [survivors_append_length, ← hrem]
  have hle := List.length_filter_le (fun k => k ∈ other) (survivors snapshot own)
  omega

end TinylambOpt.ResearchProofs.EngineDecisions.MVCC_GuardedDelete
