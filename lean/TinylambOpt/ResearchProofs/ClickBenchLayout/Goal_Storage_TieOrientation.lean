import TinylambOpt.ResearchGoals.ClickBenchLayout.Storage

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Storage_TieOrientation

open TinylambOpt.ResearchGoals.ClickBenchLayout.Storage.TieOrientation
open TinylambOpt.ResearchGoals.ClickBenchLayout.Storage

def lastW (w : List Word) : Word := w.getLast?.getD []

def lastOr (prev : Word) (p : List Word) : Word :=
  match p with
  | [] => prev
  | _ => lastW p

theorem lastW_append (p g : List Word) (hg : g ≠ []) : lastW (p ++ g) = lastW g := by
  cases g with
  | nil => contradiction
  | cons a gs =>
    simp only [lastW, List.getLast?_append]
    rw [show (a :: gs).getLast? = some (gs.getLast?.getD a) by rw [List.getLast?_cons]]
    simp

theorem lastOr_nil_eq_lastW (p : List Word) : lastOr [] p = lastW p := by
  cases p <;> simp [lastOr, lastW]

theorem lastOr_cons (b : Word) (p : List Word) : lastOr b p = lastW (b :: p) := by
  cases p <;> simp [lastOr, lastW]

theorem charge_append (prev : Word) (p g : List Word) :
    charge prev (p ++ g) = charge prev p + charge (lastOr prev p) g := by
  induction p generalizing prev with
  | nil => simp [charge, lastOr]
  | cons b p ih =>
    simp only [List.cons_append]
    rw [charge, ih]
    change price prev b + (charge b p + charge (lastOr b p) g) =
      (price prev b + charge b p) + charge (lastW (b :: p)) g
    rw [← lastOr_cons b p]
    omega

theorem orders_exists : ∀ gs : List (List Word), ∃ p, p ∈ orders gs := by
  intro gs
  induction gs with
  | nil => exact ⟨[], by simp [orders]⟩
  | cons g gs ih =>
    obtain ⟨p, hp⟩ := ih
    refine ⟨g ++ p, ?_⟩
    simp only [orders]
    exact List.mem_flatMap.mpr ⟨p, hp, by simp⟩

theorem orders_nonempty (gs : List (List Word)) : orders gs ≠ [] := by
  obtain ⟨p, hp⟩ := orders_exists gs
  intro h
  rw [h] at hp
  simp at hp

theorem orders_append_mem : ∀ (gs : List (List Word)) (g p : List Word),
    p ∈ orders (gs ++ [g]) ↔ ∃ q ∈ orders gs, p = q ++ g ∨ p = q ++ g.reverse := by
  intro gs
  induction gs with
  | nil =>
    intro g p
    simp [orders]
  | cons a gs ih =>
    intro g p
    rw [show (a :: gs) ++ [g] = a :: (gs ++ [g]) by simp]
    simp only [orders, List.mem_flatMap]
    constructor
    · rintro ⟨t, ht, hpmem⟩
      rw [ih g t] at ht
      obtain ⟨q, hq, ht' | ht'⟩ := ht
      · subst t
        simp at hpmem
        rcases hpmem with hp | hp
        · subst p
          refine ⟨a ++ q, ?_, ?_⟩
          · exact ⟨q, hq, by simp⟩
          · left; simp [List.append_assoc]
        · subst p
          refine ⟨a.reverse ++ q, ?_, ?_⟩
          · exact ⟨q, hq, by simp⟩
          · left; simp [List.append_assoc]
      · subst t
        simp at hpmem
        rcases hpmem with hp | hp
        · subst p
          refine ⟨a ++ q, ?_, ?_⟩
          · exact ⟨q, hq, by simp⟩
          · right; simp [List.append_assoc]
        · subst p
          refine ⟨a.reverse ++ q, ?_, ?_⟩
          · exact ⟨q, hq, by simp⟩
          · right; simp [List.append_assoc]
    · rintro ⟨r, hr, hp⟩
      try simp only [List.mem_cons, List.mem_singleton] at hp
      rcases hp with hp | hp
      · subst p
        obtain ⟨t, ht, hrmem⟩ := hr
        simp at hrmem
        rcases hrmem with hr' | hr'
        · subst r
          refine ⟨t ++ g, ?_, ?_⟩
          · rw [ih g (t ++ g)]
            exact ⟨t, ht, Or.inl rfl⟩
          · simp [List.append_assoc]
        · subst r
          refine ⟨t ++ g, ?_, ?_⟩
          · rw [ih g (t ++ g)]
            exact ⟨t, ht, Or.inl rfl⟩
          · simp [List.append_assoc]
      · subst p
        obtain ⟨t, ht, hrmem⟩ := hr
        simp at hrmem
        rcases hrmem with hr' | hr'
        · subst r
          refine ⟨t ++ g.reverse, ?_, ?_⟩
          · rw [ih g (t ++ g.reverse)]
            exact ⟨t, ht, Or.inr rfl⟩
          · simp [List.append_assoc]
        · subst r
          refine ⟨t ++ g.reverse, ?_, ?_⟩
          · rw [ih g (t ++ g.reverse)]
            exact ⟨t, ht, Or.inr rfl⟩
          · simp [List.append_assoc]

def step (S : List (Word × Nat)) (g : List Word) : List (Word × Nat) :=
  if g.isEmpty then S else [g, g.reverse].map fun order =>
    (lastW order, (S.map (fun (w, c) => c + charge w order)).min?.getD 0)

def statesFrom (S : List (Word × Nat)) (gs : List (List Word)) : List (Word × Nat) :=
  gs.foldl step S

theorem statesFrom_append (S : List (Word × Nat)) (gs : List (List Word)) (g : List Word) :
    statesFrom S (gs ++ [g]) = step (statesFrom S gs) g := by
  simp [statesFrom]

def Good (S : List (Word × Nat)) (gs : List (List Word)) : Prop :=
  (∀ st ∈ S, ∃ p ∈ orders gs, lastW p = st.1 ∧ charge [] p = st.2) ∧
  (∀ p ∈ orders gs, ∃ st ∈ S, lastW p = st.1 ∧ st.2 ≤ charge [] p)

theorem min_getD_le (l : List Nat) (x : Nat) (hx : x ∈ l) : l.min?.getD 0 ≤ x := by
  cases hm : l.min? with
  | none =>
    rw [List.min?_eq_none_iff] at hm
    subst hm
    simp at hx
  | some m =>
    have h := (List.min?_eq_some_iff.mp hm).2 x hx
    simpa using h

theorem min_attain (l : List Nat) (hne : l ≠ []) :
    ∃ m ∈ l, l.min?.getD 0 = m ∧ ∀ x ∈ l, m ≤ x := by
  cases hm : l.min? with
  | none =>
    rw [List.min?_eq_none_iff] at hm
    exact absurd hm hne
  | some m =>
    have hmem := (List.min?_eq_some_iff.mp hm).1
    have hmin := (List.min?_eq_some_iff.mp hm).2
    exact ⟨m, hmem, by simpa using hmin, hmin⟩

theorem good_init : Good [([], 0)] [] := by
  constructor
  · intro st hst
    simp only [List.mem_singleton] at hst
    rcases hst with rfl
    refine ⟨[], by simp [orders], ?_, ?_⟩
    · rfl
    · simp [charge]
  · intro p hp
    have hp' : p = [] := by simpa [orders] using hp
    subst p
    exact ⟨([], 0), by simp, by simp [lastW, charge]⟩

theorem good_step (S : List (Word × Nat)) (gs : List (List Word)) (g : List Word)
    (hS : Good S gs) (hSne : S ≠ []) : Good (step S g) (gs ++ [g]) := by
  by_cases hempty : g = []
  · subst g
    have hs : step S [] = S := by simp [step]
    rw [hs]
    constructor
    · intro st hst
      obtain ⟨p, hp, h1, h2⟩ := hS.1 st hst
      refine ⟨p ++ [], ?_, ?_, ?_⟩
      · rw [orders_append_mem]
        exact ⟨p, hp, Or.inl rfl⟩
      · simpa using h1
      · simpa using h2
    · intro p hp
      rw [orders_append_mem] at hp
      obtain ⟨q, hq, hporder⟩ := hp
      rcases hporder with hp | hp
      · subst p
        obtain ⟨st, hst, h1, h2⟩ := hS.2 q hq
        exact ⟨st, hst, by simpa using h1, by simpa using h2⟩
      · subst p
        obtain ⟨st, hst, h1, h2⟩ := hS.2 q hq
        exact ⟨st, hst, by simpa using h1, by simpa using h2⟩
  · have hgne : g.isEmpty = false := by simpa using hempty
    have hstep : step S g = [g, g.reverse].map fun order =>
        (lastW order, (S.map (fun (w, c) => c + charge w order)).min?.getD 0) := by
      simp [step, hgne]
    rw [hstep]
    constructor
    · intro st hst
      simp only [List.mem_map] at hst
      obtain ⟨order, horder, rfl⟩ := hst
      simp at horder
      rcases horder with horder | horder
      · subst order
        let L := S.map (fun (w, c) => c + charge w g)
        have hLne : L ≠ [] := by simp [hSne, L]
        obtain ⟨m, hm_mem, hm_eq, hm_min⟩ := min_attain L hLne
        obtain ⟨s0, hs0, hs0_eq⟩ : ∃ s0 ∈ S, s0.2 + charge s0.1 g = m := by
          dsimp [L] at hm_mem
          obtain ⟨s0, hs0, hs0_eq⟩ := List.mem_map.mp hm_mem
          exact ⟨s0, hs0, hs0_eq⟩
        obtain ⟨q, hq, hq1, hq2⟩ := hS.1 s0 hs0
        refine ⟨q ++ g, ?_, ?_, ?_⟩
        · rw [orders_append_mem]
          exact ⟨q, hq, Or.inl rfl⟩
        · rw [lastW_append q g hempty]
        · rw [charge_append, lastOr_nil_eq_lastW, hq1, hq2, hs0_eq, hm_eq]
      · subst order
        have hrev : g.reverse ≠ [] := by simp [hempty]
        let L := S.map (fun (w, c) => c + charge w g.reverse)
        have hLne : L ≠ [] := by simp [hSne, L]
        obtain ⟨m, hm_mem, hm_eq, hm_min⟩ := min_attain L hLne
        obtain ⟨s0, hs0, hs0_eq⟩ : ∃ s0 ∈ S, s0.2 + charge s0.1 g.reverse = m := by
          dsimp [L] at hm_mem
          obtain ⟨s0, hs0, hs0_eq⟩ := List.mem_map.mp hm_mem
          exact ⟨s0, hs0, hs0_eq⟩
        obtain ⟨q, hq, hq1, hq2⟩ := hS.1 s0 hs0
        refine ⟨q ++ g.reverse, ?_, ?_, ?_⟩
        · rw [orders_append_mem]
          exact ⟨q, hq, Or.inr rfl⟩
        · rw [lastW_append q g.reverse hrev]
        · rw [charge_append, lastOr_nil_eq_lastW, hq1, hq2, hs0_eq, hm_eq]
    · intro p hp
      rw [orders_append_mem] at hp
      obtain ⟨q, hq, hporder⟩ := hp
      rcases hporder with hp | hp
      · subst p
        obtain ⟨s0, hs0, hs01, hs02⟩ := hS.2 q hq
        refine ⟨(lastW g, (S.map (fun (w, c) => c + charge w g)).min?.getD 0), ?_, ?_, ?_⟩
        · simp
        · rw [lastW_append q g hempty]
        · have hle := min_getD_le (S.map (fun (w, c) => c + charge w g)) (s0.2 + charge s0.1 g)
            (List.mem_map.mpr ⟨s0, hs0, rfl⟩)
          rw [charge_append, lastOr_nil_eq_lastW, hs01]
          exact Nat.le_trans hle (Nat.add_le_add_right hs02 _)
      · subst p
        have hrev : g.reverse ≠ [] := by simp [hempty]
        obtain ⟨s0, hs0, hs01, hs02⟩ := hS.2 q hq
        refine ⟨(lastW g.reverse, (S.map (fun (w, c) => c + charge w g.reverse)).min?.getD 0), ?_, ?_, ?_⟩
        · simp
        · rw [lastW_append q g.reverse hrev]
        · have hle := min_getD_le (S.map (fun (w, c) => c + charge w g.reverse)) (s0.2 + charge s0.1 g.reverse)
            (List.mem_map.mpr ⟨s0, hs0, rfl⟩)
          rw [charge_append, lastOr_nil_eq_lastW, hs01]
          exact Nat.le_trans hle (Nat.add_le_add_right hs02 _)

theorem good_states : ∀ gs : List (List Word), Good (statesFrom [([], 0)] gs) gs := by
  intro gs
  have H : ∀ n : Nat, ∀ gs : List (List Word), gs.length = n →
      Good (statesFrom [([], 0)] gs) gs := by
    intro n
    induction n with
    | zero =>
      intro gs hlen
      have : gs = [] := List.eq_nil_of_length_eq_zero hlen
      subst gs
      exact good_init
    | succ n ih =>
      intro gs hlen
      have hgsne : gs ≠ [] := by
        intro h
        subst h
        simp at hlen
      let g : List Word := gs.getLast hgsne
      let gs' : List (List Word) := gs.dropLast
      have hdecomp : gs' ++ [g] = gs := List.dropLast_concat_getLast hgsne
      have hlen' : gs'.length = n := by
        rw [show gs'.length = gs.length - 1 by
          dsimp [gs']
          exact List.length_dropLast]
        omega
      have hih := ih gs' hlen'
      rw [← hdecomp, statesFrom_append]
      apply good_step
      · exact hih
      · intro hnil
        obtain ⟨p, hp⟩ := orders_exists gs'
        obtain ⟨st, hst, _⟩ := hih.2 p hp
        simp [hnil] at hst
  exact H gs.length gs rfl

theorem good_min (S : List (Word × Nat)) (gs : List (List Word)) (hS : Good S gs) :
    (S.map Prod.snd).min?.getD 0 = ((orders gs).map (charge [])).min?.getD 0 := by
  have hneS : S ≠ [] := by
    intro h
    obtain ⟨p, hp⟩ := orders_exists gs
    obtain ⟨st, hst, _⟩ := hS.2 p hp
    simp [h] at hst
  have hneC : (orders gs).map (charge []) ≠ [] := by
    intro h
    have h' : orders gs = [] := List.map_eq_nil_iff.mp h
    exact orders_nonempty gs h'
  apply Nat.le_antisymm
  · obtain ⟨mc, hmc_mem, hmc_eq, hmc_min⟩ := min_attain ((orders gs).map (charge [])) hneC
    obtain ⟨p, hp, hpc⟩ : ∃ p ∈ orders gs, charge [] p = mc := by
      obtain ⟨p, hp, hpc⟩ := List.mem_map.mp hmc_mem
      exact ⟨p, hp, hpc⟩
    obtain ⟨st, hst, hst1, hst2⟩ := hS.2 p hp
    have hle := min_getD_le (S.map Prod.snd) st.2 (List.mem_map.mpr ⟨st, hst, rfl⟩)
    omega
  · obtain ⟨ms, hms_mem, hms_eq, hms_min⟩ := min_attain (S.map Prod.snd) (by simpa using hneS)
    obtain ⟨st, hst, hst_eq⟩ : ∃ st ∈ S, st.2 = ms := by
      obtain ⟨z, hz, hz_eq⟩ := List.mem_map.mp hms_mem
      exact ⟨z, hz, hz_eq⟩
    obtain ⟨p, hp, hp1, hp2⟩ := hS.1 st hst
    have hle := min_getD_le ((orders gs).map (charge [])) (charge [] p)
      (List.mem_map.mpr ⟨p, hp, rfl⟩)
    omega

theorem sound : TieOrientation.Goal := by
  intro groups
  let S := statesFrom [([], 0)] groups
  have hg : Good S groups := good_states groups
  have hmin := good_min S groups hg
  have hneS : S ≠ [] := by
    intro h
    obtain ⟨p, hp⟩ := orders_exists groups
    obtain ⟨st, hst, _⟩ := hg.2 p hp
    simp [h, S] at hst
  have hneCosts : (orders groups).map (charge []) ≠ [] := by
    intro h
    have h' : orders groups = [] := List.map_eq_nil_iff.mp h
    exact orders_nonempty groups h'
  have hcosts : ((orders groups).map (charge [])).min? =
      some (((orders groups).map (charge [])).min?.getD 0) := by
    cases hm : ((orders groups).map (charge [])).min? with
    | none =>
      have : (orders groups).map (charge []) = [] := List.min?_eq_none_iff.mp hm
      exact absurd this hneCosts
    | some m => simp [hm]
  change some ((S.map Prod.snd).min?.getD 0) = ((orders groups).map (charge [])).min?
  rw [hcosts, hmin]

end TinylambOpt.ResearchProofs.ClickBenchLayout.Storage_TieOrientation
