import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Lcp.CompareSkip` — `∀ a b needle, common a b > common a needle → compare a needle =
  compare b needle`
- `Lcp.DictionarySize` — `∀ ws, frontBytes ws + (lcpEdges ws).sum = (ws.map
  List.length).sum`
- `Lcp.PagePrefix` — `∀ (p a b w : Word), a ≤ w → w ≤ b → p.isPrefixOf a → p.isPrefixOf
  b → p.isPrefixOf w`
- `Lcp.RangeMinimum` — `∀ (ws : List Word) i j, ws.Pairwise (· ≤ ·) → i < j → j <
  ws.length → ((lcpEdges ws).drop i |>.take (j-i)).min? = some (common (ws[i]?.getD
  []) (ws[j]?.getD []))`
- `Lcp.ShardBridge` — `∀ a b, frontBytes (a++b) + common (a.getLast?.getD [])
  (b.head?.getD []) = frontBytes a + frontBytes b`
-/

namespace TinylambOpt.ResearchProofs.Atlas.TextExecution.Lcp

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp

set_option maxHeartbeats 0

private theorem common_le_left (a b : Word) : common a b ≤ a.length := by
  induction a generalizing b with
  | nil => simp [common]
  | cons x xs ih =>
      cases b with
      | nil => simp [common]
      | cons y ys =>
          by_cases h : (x == y) = true
          · simp [common, h]
            have htail := ih ys
            omega
          · simp [common, h]

private theorem common_le_right (a b : Word) : common a b ≤ b.length := by
  induction b generalizing a with
  | nil => simp [common]
  | cons y ys ih =>
      cases a with
      | nil => simp [common]
      | cons x xs =>
          by_cases h : (x == y) = true
          · simp [common, h]
            have htail := ih xs
            omega
          · simp [common, h]

private theorem common_drop_add (a b : Word) : (a.drop (common a b)).length + common a b = a.length := by
  rw [List.length_drop]
  have h := common_le_left a b
  omega

private theorem common_take_eq {a b : Word} {k : Nat} (h : k ≤ common a b) : a.take k = b.take k := by
  induction k generalizing a b with
  | zero => simp
  | succ k ih =>
      cases a with
      | nil => simp [common] at h
      | cons x xs =>
          cases b with
          | nil => simp [common] at h
          | cons y ys =>
              by_cases hxy : (x == y) = true
              · have hxeq : x = y := beq_iff_eq.mp hxy
                have hk : k ≤ common xs ys := by
                  simp [common, hxy] at h
                  omega
                simp [hxeq, ih hk]
              · simp [common, hxy] at h

private theorem common_diff_at (a b : Word) :
    let k := common a b
    k < a.length → k < b.length → a[k]?.getD 0 != b[k]?.getD 0 := by
  dsimp
  induction a generalizing b with
  | nil => simp [common]
  | cons x xs ih =>
      cases b with
      | nil => simp [common]
      | cons y ys =>
          by_cases hxy : (x == y) = true
          · intro hlt1 hlt2
            simp only [common, hxy, ite_true] at hlt1 hlt2 ⊢
            simp only [List.length_cons] at hlt1 hlt2 ⊢
            have h1 : common xs ys < xs.length := by omega
            have h2 : common xs ys < ys.length := by omega
            rw [Nat.add_comm 1 (common xs ys)]
            simp only [List.getElem?_cons_succ]
            exact ih ys h1 h2
          · have hne : x ≠ y := by intro he; apply hxy; simp [he]
            simp [common, hxy, hne]

private theorem common_symm (a b : Word) : common a b = common b a := by
  induction a generalizing b with
  | nil => cases b <;> simp [common]
  | cons x xs ih =>
      cases b with
      | nil => simp [common]
      | cons y ys =>
          by_cases hxy : (x == y) = true
          · have hxy' : x = y := beq_iff_eq.mp hxy
            have hyx : (y == x) = true := by simp [hxy']
            simp [common, hxy', ih ys]
          · have hxyne : x ≠ y := by intro he; apply hxy; simp [he]
            have hyxne : y ≠ x := by omega
            have hyx : (y == x) = false := by simp [hyxne]
            simp [common, hxy, hyx]

private theorem common_drop_add' (a b : Word) : (a.drop (common b a)).length + common b a = a.length := by
  rw [common_symm b a]
  exact common_drop_add a b

private theorem common_self (a : Word) : common a a = a.length := by
  induction a with
  | nil => simp [common]
  | cons x xs ih => simp [common, ih, Nat.add_comm]

private theorem isPrefixOf_true_iff (p a : Word) : p.isPrefixOf a = true ↔ ∃ t, a = p ++ t := by
  constructor
  · intro h
    have hp : List.IsPrefix p a := by simpa using h
    rcases hp with ⟨t, ht⟩
    exact ⟨t, ht.symm⟩
  · rintro ⟨t, rfl⟩
    simp

private theorem common_min_of_le {a b c : Word} (hab : a ≤ b) (hbc : b ≤ c) :
    common a c = min (common a b) (common b c) := by
  induction a generalizing b c with
  | nil => simp [common]
  | cons x xs ih =>
      cases b with
      | nil =>
          exfalso
          exact hab (List.Lex.nil)
      | cons y ys =>
          cases c with
          | nil =>
              exfalso
              exact hbc (List.Lex.nil)
          | cons z zs =>
              by_cases hxy : x = y
              · subst y
                by_cases hxz : x = z
                · subst z
                  have hxsle : xs ≤ ys := by
                    intro hlex
                    exact hab (List.Lex.cons hlex)
                  have hysle : ys ≤ zs := by
                    intro hlex
                    exact hbc (List.Lex.cons hlex)
                  have hrec := ih hxsle hysle
                  simp [common, hrec]
                · have hxzlt : x < z := by
                    have hnzx : ¬ z < x := by
                      intro hzx
                      exact hbc (List.Lex.rel hzx)
                    omega
                  have hxzne : x ≠ z := by omega
                  simp [common, hxz]
              · have hltgt : x < y ∨ y < x := by omega
                rcases hltgt with hxylt | hyxlt
                · have hnzx : ¬ z < y := by
                    intro hzy
                    exact hbc (List.Lex.rel hzy)
                  have hxzlt : x < z := by omega
                  have hxzne : x ≠ z := by omega
                  simp [common, hxy, hxzne]
                · exfalso
                  exact hab (List.Lex.rel hyxlt)

private theorem lcpEdges_cons_cons (w x : Word) (xs : List Word) :
    lcpEdges (w :: x :: xs) = common w x :: lcpEdges (x :: xs) := by
  simp [lcpEdges]

private theorem lcpEdges_drop (ws : List Word) (i : Nat) :
    (lcpEdges ws).drop i = lcpEdges (ws.drop i) := by
  induction ws generalizing i with
  | nil => simp [lcpEdges]
  | cons w ws ih =>
      cases i with
      | zero => simp
      | succ i =>
          cases ws with
          | nil => simp [lcpEdges]
          | cons x xs =>
              change (lcpEdges (w :: x :: xs)).drop (i + 1) = lcpEdges ((w :: x :: xs).drop (i + 1))
              rw [lcpEdges_cons_cons]
              simp only [List.drop_succ_cons]
              exact ih i

private theorem rangeMinEdges (ws : List Word) (hp : ws.Pairwise (· ≤ ·)) :
    ∀ j, 0 < j → j < ws.length →
      ((lcpEdges ws).take j).min? = some (common (ws.head?.getD []) (ws[j]?.getD [])) := by
  induction ws with
  | nil => intro j hjpos hj; simp at hj
  | cons w rest ih =>
      intro j hjpos hj
      cases hp with
      | cons hw_all hp_rest =>
        have hp_rest0 := hp_rest
        cases rest with
        | nil => simp at hj; omega
        | cons x xs =>
          cases hp_rest with
          | cons hx_all hp_xs =>
          cases j with
          | zero => omega
          | succ j' =>
              cases j' with
              | zero =>
                  simp [lcpEdges_cons_cons]
              | succ k =>
                  have hjpos2 : 0 < k + 1 := by omega
                  have hjlt : k + 1 < (x :: xs).length := by simpa using hj
                  have hrec := ih hp_rest0 (k + 1) hjpos2 hjlt
                  have hklt : k < xs.length := by simpa using hjlt
                  have hwle : w ≤ x := hw_all x (by simp)
                  have hxle : x ≤ xs[k] := hx_all _ (List.getElem_mem hklt)
                  have hthree := common_min_of_le hwle hxle
                  rw [lcpEdges_cons_cons]
                  rw [List.take_succ_cons]
                  rw [List.min?_cons]
                  rw [hrec]
                  simp [List.getElem?_cons_succ, List.getElem?_eq_getElem hklt]
                  rw [← hthree]

private theorem pairwise_drop (ws : List Word) (i : Nat) :
    ws.Pairwise (· ≤ ·) → (ws.drop i).Pairwise (· ≤ ·) := by
  induction i generalizing ws with
  | zero => intro hp; simpa using hp
  | succ i ih =>
      intro hp
      cases ws with
      | nil => simp
      | cons w ws =>
          cases hp with
          | cons hw tail => simpa [List.drop_succ_cons] using ih ws tail

private def fb (prev : Word) (ws : List Word) : Nat :=
  ((ws.zip (prev :: ws)).map (fun (w, p) => (w.drop (common p w)).length)).sum

private theorem fb_cons (prev w : Word) (ws : List Word) :
    fb prev (w :: ws) = (w.drop (common prev w)).length + fb w ws := by
  cases ws with
  | nil => simp [fb]
  | cons x xs => simp [fb]

private theorem fb_general (prev : Word) (ws : List Word) :
    fb prev ws + (lcpEdges ws).sum + common prev (ws.head?.getD []) =
      (ws.map List.length).sum := by
  induction ws generalizing prev with
  | nil => simp [fb, lcpEdges, common]
  | cons w ws ih =>
      rw [fb_cons]
      cases ws with
      | nil =>
          have hle := common_le_right prev w
          simp [fb, lcpEdges]
          omega
      | cons x xs =>
          have hdrop := common_drop_add' w prev
          have ih' := ih w
          simp only [List.head?_cons, Option.getD_some, List.map_cons, List.sum_cons] at ih' ⊢
          rw [lcpEdges_cons_cons]
          simp only [List.sum_cons]
          omega

private theorem fb_head (prev : Word) (ws : List Word) :
    fb prev ws + common prev (ws.head?.getD []) = fb [] ws := by
  have h1 := fb_general prev ws
  have h2 := fb_general [] ws
  simp [common] at h2
  omega

private theorem fb_append_aux (prev : Word) (a b : List Word) (ha : a ≠ []) :
    fb prev (a ++ b) + common (a.getLast?.getD []) (b.head?.getD []) = fb prev a + fb [] b := by
  induction a generalizing prev b with
  | nil => contradiction
  | cons w as ih =>
      cases as with
      | nil =>
          rw [List.cons_append]
          rw [fb_cons]
          have hh := fb_head w b
          simp [fb] at hh ⊢
          omega
      | cons x xs =>
          have ires := ih w b (by simp : x :: xs ≠ [])
          rw [List.cons_append]
          rw [fb_cons]
          rw [fb_cons]
          simp only [List.getLast?_cons_cons]
          omega

theorem sound_RangeMinimum : RangeMinimum.Goal := by
  intro ws i j hp hij hj
  have hpos : 0 < j - i := by omega
  have hlt : j - i < (ws.drop i).length := by
    rw [List.length_drop]
    omega
  have hp_drop : (ws.drop i).Pairwise (· ≤ ·) := pairwise_drop ws i hp
  have hres := rangeMinEdges (ws.drop i) hp_drop (j - i) hpos hlt
  rw [← lcpEdges_drop ws i] at hres
  have hijle : i ≤ j := Nat.le_of_lt hij
  simpa [List.getElem?_drop, Nat.add_sub_of_le hijle] using hres

theorem sound_DictionarySize : DictionarySize.Goal := by
  intro ws
  have h := fb_general [] ws
  simpa [frontBytes, fb, common] using h

private theorem compare_cons_of_ne (x z : Nat) (xs zs : List Nat) (h : x ≠ z) :
    compare (x :: xs) (z :: zs) = compare x z := by
  change List.compareLex compare (x :: xs) (z :: zs) = compare x z
  rw [List.compareLex_cons_cons]
  cases hc : compare x z with
  | eq =>
      have hxz : x = z := by
        by_cases hlt : x < z
        · have hh := Nat.compare_eq_lt.mpr hlt
          rw [hc] at hh
          cases hh
        · by_cases hgt : z < x
          · have hh := Nat.compare_eq_gt.mpr hgt
            rw [hc] at hh
            cases hh
          · omega
      exact False.elim (h hxz)
  | lt => simp
  | gt => simp

theorem sound_CompareSkip : CompareSkip.Goal := by
  intro a b needle hab
  induction a generalizing b needle with
  | nil => simp [common] at hab
  | cons x xs ih =>
      cases b with
      | nil => simp [common] at hab
      | cons y ys =>
          cases needle with
          | nil => simp
          | cons z zs =>
              by_cases hxz : x = z
              · subst z
                by_cases hxy : x = y
                · subst y
                  have hprem : common xs ys > common xs zs := by simpa [common] using hab
                  have hrec := ih ys zs hprem
                  simpa using hrec
                · have hxne : x ≠ y := hxy
                  simp [common, hxne] at hab
              · have hxzne : x ≠ z := hxz
                have hcneedle : common (x :: xs) (z :: zs) = 0 := by simp [common, hxzne]
                rw [hcneedle] at hab
                have hxybool : (x == y) = true := by
                  by_cases hbeq : (x == y) = true
                  · exact hbeq
                  · have hc : common (x :: xs) (y :: ys) = 0 := by simp [common, hbeq]
                    rw [hc] at hab
                    simp at hab
                have hxy : x = y := beq_iff_eq.mp hxybool
                subst y
                rw [compare_cons_of_ne x z xs zs hxzne]
                rw [compare_cons_of_ne x z ys zs hxzne]

theorem sound_PagePrefix : PagePrefix.Goal := by
  intro p
  induction p with
  | nil =>
      intro a b w haw hwb hpa hpb
      exact (isPrefixOf_true_iff [] w).mpr ⟨w, by simp⟩
  | cons x ps ih =>
      intro a b w haw hwb hpa hpb
      rcases (isPrefixOf_true_iff (x :: ps) a).mp hpa with ⟨ta, rfl⟩
      rcases (isPrefixOf_true_iff (x :: ps) b).mp hpb with ⟨tb, rfl⟩
      cases w with
      | nil =>
          exact False.elim (haw (List.Lex.nil))
      | cons y ws =>
          by_cases hxy : x = y
          · subst y
            have hpsa : ps.isPrefixOf (ps ++ ta) = true :=
              (isPrefixOf_true_iff ps (ps ++ ta)).mpr ⟨ta, rfl⟩
            have hpsb : ps.isPrefixOf (ps ++ tb) = true :=
              (isPrefixOf_true_iff ps (ps ++ tb)).mpr ⟨tb, rfl⟩
            have htaila : ps ++ ta ≤ ws := by
              intro hlt
              exact haw (List.Lex.cons hlt)
            have htailb : ws ≤ ps ++ tb := by
              intro hlt
              exact hwb (List.Lex.cons hlt)
            have ihres := ih (ps ++ ta) (ps ++ tb) ws htaila htailb hpsa hpsb
            rcases (isPrefixOf_true_iff ps ws).mp ihres with ⟨tw, rfl⟩
            exact (isPrefixOf_true_iff (x :: ps) (x :: ps ++ tw)).mpr ⟨tw, rfl⟩
          · have hltgt : x < y ∨ y < x := by omega
            rcases hltgt with hxylt | hyxlt
            · exact False.elim (hwb (List.Lex.rel hxylt))
            · exact False.elim (haw (List.Lex.rel hyxlt))

theorem sound_ShardBridge : ShardBridge.Goal := by
  intro a b
  change fb [] (a ++ b) + common (a.getLast?.getD []) (b.head?.getD []) = fb [] a + fb [] b
  cases a with
  | nil => simp [fb, common]
  | cons w ws => exact fb_append_aux [] (w :: ws) b (by simp)

end TinylambOpt.ResearchProofs.Atlas.TextExecution.Lcp
