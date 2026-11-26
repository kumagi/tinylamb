import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Named results include `scanRevAux_eq_foldl`, `scan_eq_rev`, `scanRevAux_snoc`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts

def scanRevAux (m : Model) (i : Nat) (s : Nat × List Nat) : List Sym → Nat × List Nat
| [] => s
| c::xs =>
  let (t,hit) := m.step s.1 c
  scanRevAux m (i+1) (t, if hit then i::s.2 else s.2) xs

def scanR (m : Model) (q : Nat) (xs : List Sym) : Nat × List Nat := scanRevAux m 0 (q,[]) xs

def foldStep (m : Model) (s : Nat × List Nat) (ci : Sym × Nat) : Nat × List Nat :=
  let (t,hit) := m.step s.1 ci.1
  (t, if hit then ci.2::s.2 else s.2)

theorem scanRevAux_eq_foldl (m : Model) (d : Nat) (s : Nat × List Nat) (xs : List Sym) :
    scanRevAux m d s xs = (xs.zipIdx d).foldl (foldStep m) s := by
  induction xs generalizing d s with
  | nil => simp [scanRevAux]
  | cons c xs ih =>
      rw [List.zipIdx_cons]
      simp [scanRevAux, foldStep, ih]

theorem scan_eq_rev (m : Model) (q : Nat) (xs : List Sym) :
    scan m q xs = (let (z,c) := scanR m q xs; (z,c.reverse)) := by
  unfold scan
  change (let (z,c) := (xs.zipIdx).foldl (foldStep m) (q,[]); (z,c.reverse)) =
    (let (z,c) := scanR m q xs; (z,c.reverse))
  simp only [scanR, scanRevAux_eq_foldl]

theorem scanRevAux_snoc (m : Model) (d : Nat) (q : Nat) (acc : List Nat) (xs : List Sym) :
    scanRevAux m d (q,acc) xs =
      (let (z,c) := scanRevAux m d (q,[]) xs; (z,c++acc)) := by
  induction xs generalizing d q acc with
  | nil => simp [scanRevAux]
  | cons c xs ih =>
      rcases hstep : m.step q c with ⟨t,hit⟩
      by_cases hh : hit = true
      · simp [scanRevAux, hstep, hh]
        rw [ih (d+1) t (d::acc), ih (d+1) t [d]] <;> simp
      · simp [scanRevAux, hstep, hh]
        rw [ih (d+1) t acc, ih (d+1) t []] <;> simp

theorem scanRevAux_shift (m : Model) (d e : Nat) (q : Nat) (acc : List Nat) (xs : List Sym) :
    scanRevAux m (d+e) (q,acc) xs =
      (let (z,c) := scanRevAux m d (q,[]) xs; (z, c.map (e+·) ++ acc)) := by
  induction xs generalizing d q acc with
  | nil => simp [scanRevAux]
  | cons c xs ih =>
      rcases hstep : m.step q c with ⟨t,hit⟩
      by_cases hh : hit = true
      · simp [scanRevAux, hstep, hh]
        rw [show d + e + 1 = (d+1) + e by omega]
        rw [ih (d+1) t ((d+e)::acc)]
        have hsnoc := scanRevAux_snoc m (d+1) t [d] xs
        rw [hsnoc]
        simp [Nat.add_comm]
      · simp [scanRevAux, hstep, hh]
        rw [show d + e + 1 = (d+1) + e by omega]
        rw [ih (d+1) t acc]

theorem scanRevAux_append (m : Model) (d : Nat) (s : Nat × List Nat) (a b : List Sym) :
    scanRevAux m d s (a++b) = scanRevAux m (d+a.length) (scanRevAux m d s a) b := by
  induction a generalizing d s with
  | nil => simp [scanRevAux]
  | cons c xs ih =>
      rcases hstep : m.step s.1 c with ⟨t,hit⟩
      by_cases hh : hit = true
      · simp [scanRevAux, hstep, hh]
        rw [show d + (xs.length + 1) = (d+1) + xs.length by omega]
        rw [ih (d+1) (t, d::s.2)]
      · simp [scanRevAux, hstep, hh]
        rw [show d + (xs.length + 1) = (d+1) + xs.length by omega]
        rw [ih (d+1) (t, s.2)]

theorem scanR_append (m : Model) (q : Nat) (a b : List Sym) :
    scanR m q (a++b) =
      (let (z,c) := scanR m q a
       let (z',c') := scanR m z b
       (z', c'.map (a.length+·) ++ c)) := by
  unfold scanR
  rw [scanRevAux_append]
  generalize hs : scanRevAux m 0 (q,[]) a = sa
  rcases sa with ⟨z,c⟩
  simp only
  rw [scanRevAux_shift]

theorem carry_general (m : Model) : carry m := by
  intro q a b
  simp only [scan_eq_rev]
  change (let (z,c) := scanR m q (a++b); (z,c.reverse)) =
    (let (x,cx) := scanR m q a
     let (y,cy) := scanR m x b
     (y, joinAt a.length cx.reverse cy.reverse))
  rw [scanR_append]
  simp [List.reverse_append, joinAt]

theorem scan_singleton (m : Model) (q : Nat) (c : Sym) :
    scan m q [c] = ((m.step q c).1, if (m.step q c).2 then [0] else []) := by
  rcases hstep : m.step q c with ⟨z,hit⟩
  cases hit <;> simp [scan, List.zipIdx_cons, hstep]

theorem scan_cons (m : Model) (q : Nat) (c : Sym) (xs : List Sym) :
    scan m q (c::xs) =
      (let (z,hit) := m.step q c
       let (z',cs) := scan m z xs
       (z', if hit then 0 :: cs.map (1+·) else cs.map (1+·))) := by
  rw [show c :: xs = [c] ++ xs by rfl]
  rw [carry_general]
  rw [scan_singleton]
  rcases hstep : m.step q c with ⟨z,hit⟩
  cases hit <;> simp [hstep, joinAt]

theorem nextCut_general (m : Model) : nextCut m := by
  intro q xs
  induction xs generalizing q with
  | nil =>
      intro i h
      simp [scan] at h
  | cons c xs ih =>
      intro i h suffix
      rcases hstep : m.step q c with ⟨q1,hit⟩
      rw [scan_cons] at h
      simp [hstep] at h
      by_cases hh : hit = true
      · simp [hh] at h
        subst i
        have htake0 : (c :: xs).take (0 + 1) = [c] := by simp
        rw [htake0]
        change (scan m q (c :: suffix)).2.head? = some 0
        rw [scan_cons]
        simp [hstep, hh]
      · simp [hh] at h
        have hi : ∃ j, (scan m q1 xs).2.head? = some j ∧ i = j + 1 := by
          cases hcuts : (scan m q1 xs).2 with
          | nil => simp [hcuts] at h
          | cons j t =>
              refine ⟨j, ?_, ?_⟩
              · simp [hcuts]
              · rw [hcuts] at h
                simp at h
                omega
        rcases hi with ⟨j,hj,rfl⟩
        have hih := ih q1 j hj suffix
        have htake : (c :: xs).take (j + 1 + 1) = c :: xs.take (j+1) := by
          cases j <;> simp
        rw [htake]
        change (scan m q (c :: (xs.take (j+1) ++ suffix))).2.head? = some (j+1)
        rw [scan_cons]
        simp [hstep, hh, hih]
        omega

theorem discard_of_cutResets (m : Model)
    (hres : ∀ q c, (m.step q c).2 = true → (m.step q c).1 = 0) : discardCutPrefix m := by
  intro q xs
  induction xs generalizing q with
  | nil =>
      intro i h
      simp [scan] at h
  | cons c xs ih =>
      intro i h
      rcases hstep : m.step q c with ⟨q1,hit⟩
      rw [scan_cons] at h
      simp [hstep] at h
      by_cases hh : hit = true
      · have hhit : (m.step q c).2 = true := by rw [hstep]; exact hh
        have hq1z : q1 = 0 := by simpa [hstep] using hres q c hhit
        simp [hh] at h
        subst i
        rw [scan_cons]
        simp [hstep, hh, hq1z]
      · simp [hh] at h
        have hi : ∃ j, (scan m q1 xs).2.head? = some j ∧ i = j + 1 := by
          cases hcuts : (scan m q1 xs).2 with
          | nil => simp [hcuts] at h
          | cons j t =>
              refine ⟨j, ?_, ?_⟩
              · simp [hcuts]
              · rw [hcuts] at h
                simp at h
                omega
        rcases hi with ⟨j,hj,rfl⟩
        have hih := ih q1 j hj
        rw [scan_cons]
        simp [hstep, hh]
        rw [show ((scan m q1 xs).2.map (fun x => 1 + x)).tail =
               ((scan m q1 xs).2.tail.map (fun x => 1 + x)) by
                 cases (scan m q1 xs).2 <;> simp]
        rw [hih]
        simp [List.map_map, List.drop, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

theorem skipPlain_of_stable (m : Model)
    (hstable : ∀ q c, (!(m.special c)) = true → m.step q c = (q,false)) : skipPlain m := by
  intro q xs hreach hno
  induction xs generalizing q with
  | nil => simp [scan]
  | cons c xs ih =>
      have hb := hno
      simp at hb
      have hc : (!(m.special c)) = true := by simpa [hb.1]
      have ht : (!(xs.any m.special)) = true := by
        rw [Bool.not_eq_true']
        rw [Bool.eq_false_iff]
        intro hany
        rcases List.any_eq_true.mp hany with ⟨x, hx, hxs⟩
        have hx' := hb.2 x hx
        simp [hx'] at hxs
      have hstep := hstable q c hc
      rw [scan_cons]
      simp [hstep, ih q hreach ht]

theorem lineRestart_of_lfReset (m : Model)
    (hlf : ∀ q, (m.step q .lf).1 = 0) : lineRestart m := by
  intro a b
  have hc := carry_general m 0 (a ++ [.lf]) b
  rw [hc]
  simp only [joinAt]
  congr 1
  have hstate : (scan m 0 (a ++ [.lf])).1 = 0 := by
    have hc2 := carry_general m 0 a [.lf]
    rw [hc2]
    simp only [joinAt]
    rw [scan_singleton, hlf]
  rw [hstate]
  congr 2
  ext x
  simp

theorem sym_beq_true (a b : Sym) : ((a == b) = true) ↔ a = b := by
  cases a <;> cases b <;> decide

theorem sym_beq_false (a b : Sym) : ((a == b) = false) ↔ a ≠ b := by
  cases a <;> cases b <;> decide

theorem stateKey_of_injective (m : Model) (hinj : Function.Injective m.cacheKey) : stateKey m := by
  intro q r xs hq hr hkey
  have hqr : q = r := hinj hkey
  subst r
  simp

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common
