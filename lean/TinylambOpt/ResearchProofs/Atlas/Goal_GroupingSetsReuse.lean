import TinylambOpt.ProposedGoals.GroupingSetsReuse

/-!
Proved

- `Goal` — All four grouping sets agree with a fresh scan, and the reuse path never
  reads the input more than once.

The reference model is defined in `TinylambOpt.ProposedGoals.GroupingSetsReuse`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GroupingSetsReuse

open TinylambOpt.ProposedGoals.GroupingSetsReuse

def step (acc : List (Key × State)) (r : Row) : List (Key × State) :=
  let k := masked true true r
  match acc.find? (fun p => p.1 == k) with
  | none => acc ++ [(k, addRow empty r)]
  | some (_, s) => acc.map (fun p => if p.1 == k then (k, addRow s r) else p)

theorem finest_eq_foldl (rows : List Row) : finest rows = rows.foldl step [] := by
  rfl

def csum (f : Key → Bool) (ps : List (Key × State)) : Nat :=
  ((ps.filter (fun p => f p.1)).map (fun p => p.2.1)).sum

def psum (f : Key → Bool) (ps : List (Key × State)) : Nat :=
  ((ps.filter (fun p => f p.1)).map (fun p => p.2.2)).sum

theorem csum_cons (f : Key → Bool) (p : Key × State) (ps : List (Key × State)) :
    csum f (p :: ps) = (if f p.1 then p.2.1 else 0) + csum f ps := by
  by_cases hf : f p.1 <;> simp [csum, hf]

theorem psum_cons (f : Key → Bool) (p : Key × State) (ps : List (Key × State)) :
    psum f (p :: ps) = (if f p.1 then p.2.2 else 0) + psum f ps := by
  by_cases hf : f p.1 <;> simp [psum, hf]

def rcsum (f : Key → Bool) (rows : List Row) : Nat :=
  ((rows.filter (fun r => f (masked true true r))).map (fun _ => 1)).sum

def rpsum (f : Key → Bool) (rows : List Row) : Nat :=
  ((rows.filter (fun r => f (masked true true r))).map (fun r => r.2.2)).sum

theorem foldG_eq (ps : List (Key × State)) (f : Key → Bool) (s : State) :
    ps.foldl (fun s p => if f p.1 then combine s p.2 else s) s =
      (s.1 + csum f ps, s.2 + psum f ps) := by
  induction ps generalizing s with
  | nil => simp [csum, psum]
  | cons p tl ih =>
      by_cases hf : f p.1
      · rw [List.foldl_cons, ite_eq_left hf, ih (combine s p.2)]
        simp [combine, csum, psum, hf]
        omega
      · rw [List.foldl_cons, ite_eq_right hf, ih s]
        simp [combine, csum, psum, hf]

theorem rowFold_eq (rows : List Row) (f : Key → Bool) (s : State) :
    rows.foldl (fun s r => if f (masked true true r) then addRow s r else s) s =
      (s.1 + rcsum f rows, s.2 + rpsum f rows) := by
  induction rows generalizing s with
  | nil => simp [rcsum, rpsum]
  | cons r tl ih =>
      by_cases hf : f (masked true true r)
      · rw [List.foldl_cons, ite_eq_left hf, ih (addRow s r)]
        simp [addRow, rcsum, rpsum, hf]
        omega
      · rw [List.foldl_cons, ite_eq_right hf, ih s]
        simp [addRow, rcsum, rpsum, hf]

theorem not_mem_of_find_none (acc : List (Key × State)) (r : Row)
    (h : acc.find? (fun p => p.1 == masked true true r) = none) :
    masked true true r ∉ acc.map Prod.fst := by
  intro hm
  rcases List.mem_map.mp hm with ⟨p, hpacc, hp⟩
  have hp1 : p.1 = masked true true r := hp
  have hfalse := (List.find?_eq_none.mp h) p hpacc
  exact hfalse (beq_iff_eq.mpr hp1)

theorem find_none_of_not_mem (acc : List (Key × State)) (r : Row)
    (h : masked true true r ∉ acc.map Prod.fst) :
    acc.find? (fun p => p.1 == masked true true r) = none := by
  apply List.find?_eq_none.mpr
  intro p hpacc hbeq
  exact h (List.mem_map.mpr ⟨p, hpacc, beq_iff_eq.mp hbeq⟩)

theorem map_fst_update (acc : List (Key × State)) (k : Key) (s : State) (r : Row) :
    (acc.map (fun p => if p.1 == k then (k, addRow s r) else p)).map Prod.fst =
      acc.map Prod.fst := by
  rw [List.map_map]
  apply List.map_congr_left
  intro p hp
  by_cases h : p.1 == k
  · have hp1 : p.1 = k := beq_iff_eq.mp h
    simp [hp1]
  · have hpne : p.1 ≠ k := by
      intro he
      exact h (beq_iff_eq.mpr he)
    simp [hpne]

theorem step_nodup (acc : List (Key × State)) (r : Row) :
    (acc.map Prod.fst).Nodup → ((step acc r).map Prod.fst).Nodup := by
  intro hnd
  cases hf : acc.find? (fun p => p.1 == masked true true r) with
  | none =>
      have hnot := not_mem_of_find_none acc r hf
      have hstep : step acc r = acc ++ [(masked true true r, addRow empty r)] := by
        simp [step, hf]
      rw [hstep, List.map_append]
      simp only [List.map_cons, List.map_nil]
      exact List.nodup_append.mpr ⟨hnd, by simp, by
        intro a ha b hb
        simp at hb
        subst b
        intro haeq
        exact hnot (by simpa [haeq] using ha)⟩
  | some q =>
      rcases q with ⟨_, s⟩
      have hstep : step acc r = acc.map (fun p => if p.1 == masked true true r then (masked true true r, addRow s r) else p) := by
        simp [step, hf]
      rw [hstep]
      rw [map_fst_update]
      exact hnd

theorem step_sums_update (acc : List (Key × State)) (r : Row) (f : Key → Bool) (s : State)
    (hnd : (acc.map Prod.fst).Nodup)
    (hfind : acc.find? (fun p => p.1 == masked true true r) = some (masked true true r, s)) :
    csum f (step acc r) = csum f acc + (if f (masked true true r) then 1 else 0) ∧
    psum f (step acc r) = psum f acc + (if f (masked true true r) then r.2.2 else 0) := by
  induction acc with
  | nil => simp at hfind
  | cons p tl ih =>
      by_cases hpk : p.1 == masked true true r
      · have hp1 : p.1 = masked true true r := beq_iff_eq.mp hpk
        have hp_eq : p = (masked true true r, s) := by
          simpa [hpk, hp1] using hfind
        have hp2 : p.2 = s := congrArg Prod.snd hp_eq
        have hnotl : masked true true r ∉ tl.map Prod.fst := by
          have hn := (List.nodup_cons.mp hnd).1
          intro hm
          exact hn (by simpa [hp1] using hm)
        have hmap : tl.map (fun q => if q.1 = masked true true r then (masked true true r, addRow s r) else q) = tl := by
          have hmap0 : tl.map (fun q => if q.1 = masked true true r then (masked true true r, addRow s r) else q) = tl.map id := by
            apply List.map_congr_left
            intro q hq
            have hqne : q.1 ≠ masked true true r := by
              intro heq
              exact hnotl (List.mem_map.mpr ⟨q, hq, heq⟩)
            simp [hqne]
          simpa using hmap0
        have hstep : step (p :: tl) r = (masked true true r, addRow s r) :: tl := by
          simp [step, hfind, hp1, hmap]
        rw [hstep]
        simp only [csum_cons, psum_cons]
        by_cases hfp : f p.1
        · have hfk : f (masked true true r) := by simpa [hp1] using hfp
          simp [addRow, hfp, hfk, hp1, hp2]
          omega
        · have hfk : ¬ f (masked true true r) := by simpa [hp1] using hfp
          simp [addRow, hfp, hfk, hp1, hp2]
      · have hpne : p.1 ≠ masked true true r := by
          intro he
          exact hpk (beq_iff_eq.mpr he)
        have hfindtl : tl.find? (fun q => q.1 == masked true true r) = some (masked true true r, s) := by
          simpa [hpne] using hfind
        have hndtl : (tl.map Prod.fst).Nodup := (List.nodup_cons.mp hnd).2
        have hih := ih hndtl hfindtl
        have hstep : step (p :: tl) r = p :: step tl r := by
          simp [step, hpne, hfindtl]
        rw [hstep]
        simp only [csum_cons, psum_cons]
        rw [hih.1, hih.2]
        by_cases hfp : f p.1 <;> by_cases hfk : f (masked true true r) <;> simp [hfp, hfk] <;> omega

theorem step_sums (acc : List (Key × State)) (r : Row) (f : Key → Bool)
    (hnd : (acc.map Prod.fst).Nodup) :
    csum f (step acc r) = csum f acc + (if f (masked true true r) then 1 else 0) ∧
    psum f (step acc r) = psum f acc + (if f (masked true true r) then r.2.2 else 0) := by
  cases hf : acc.find? (fun p => p.1 == masked true true r) with
  | none =>
      have hstep : step acc r = acc ++ [(masked true true r, addRow empty r)] := by
        simp [step, hf]
      rw [hstep]
      have hcf : csum f (acc ++ [(masked true true r, addRow empty r)]) =
          csum f acc + (if f (masked true true r) then (addRow empty r).1 else 0) := by
        by_cases hfk : f (masked true true r) <;> simp [csum, addRow, empty, hfk]
      have hpf : psum f (acc ++ [(masked true true r, addRow empty r)]) =
          psum f acc + (if f (masked true true r) then (addRow empty r).2 else 0) := by
        by_cases hfk : f (masked true true r) <;> simp [psum, addRow, empty, hfk]
      rw [hcf, hpf]
      by_cases hfk : f (masked true true r) <;> simp [hfk, addRow, empty]
  | some q =>
      rcases q with ⟨kq, s⟩
      have hp : ((fun p : Key × State => p.1 == masked true true r) (kq, s)) = true := by
        exact List.find?_some (p := fun p : Key × State => p.1 == masked true true r) (a := (kq, s)) hf
      have hkq : kq = masked true true r := beq_iff_eq.mp hp
      have hfind' : acc.find? (fun p => p.1 == masked true true r) = some (masked true true r, s) := by
        simpa [hkq] using hf
      exact step_sums_update acc r f s hnd hfind'

theorem foldl_nodup (rows : List Row) (acc : List (Key × State)) :
    (acc.map Prod.fst).Nodup → ((rows.foldl step acc).map Prod.fst).Nodup := by
  induction rows generalizing acc with
  | nil => intro h; exact h
  | cons r tl ih =>
      intro h
      exact ih (step acc r) (step_nodup acc r h)

theorem finest_nodup (rows : List Row) : ((finest rows).map Prod.fst).Nodup := by
  rw [finest_eq_foldl]
  exact foldl_nodup rows [] (by simp)

theorem foldl_sums (rows : List Row) (acc : List (Key × State)) (f : Key → Bool)
    (hnd : (acc.map Prod.fst).Nodup) :
    csum f (rows.foldl step acc) = csum f acc + rcsum f rows ∧
    psum f (rows.foldl step acc) = psum f acc + rpsum f rows := by
  induction rows generalizing acc with
  | nil => simp [rcsum, rpsum]
  | cons r tl ih =>
      rw [List.foldl_cons]
      have hs := step_sums acc r f hnd
      have hnd' := step_nodup acc r hnd
      have ihtl := ih (step acc r) hnd'
      rw [ihtl.1, ihtl.2, hs.1, hs.2]
      by_cases hf : f (masked true true r) <;> simp [addRow, rcsum, rpsum, hf] <;> omega

theorem finest_sums (rows : List Row) (f : Key → Bool) :
    csum f (finest rows) = rcsum f rows ∧
    psum f (finest rows) = rpsum f rows := by
  rw [finest_eq_foldl]
  have h := foldl_sums rows [] f (by simp)
  simpa [csum, psum] using h

theorem finest_fusion (rows : List Row) (f : Key → Bool) (s : State) :
    (finest rows).foldl (fun s p => if f p.1 then combine s p.2 else s) s =
      rows.foldl (fun s r => if f (masked true true r) then addRow s r else s) s := by
  rw [foldG_eq, rowFold_eq]
  have h := finest_sums rows f
  rw [h.1, h.2]

theorem sound_GroupingSetsReuse : TinylambOpt.ProposedGoals.GroupingSetsReuse.Goal := by
  intro rows mask hmask
  constructor
  · funext k
    unfold reuse direct
    rw [finest_fusion rows (fun key => coarsen mask.1 mask.2 key == k) empty]
    simp [masked, coarsen]
  · intro _
    simp [reuseReads, directReads, masks]
    omega

end TinylambOpt.ResearchProofs.Atlas.Goal_GroupingSetsReuse
