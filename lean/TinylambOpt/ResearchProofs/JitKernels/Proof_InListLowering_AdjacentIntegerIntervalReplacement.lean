import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
If the distinct entries of a non-NULL key list exactly fill the closed integer
interval between its minimum `l` and maximum `h` (cardinality `h - l + 1`),
then `x IN (keys)` is equivalent to the interval test `l ≤ x ≤ h`. The proof
turns `keep` into plain membership, then applies a pigeonhole principle: a
duplicate-free list of integers contained in `[lo, lo + d]` with `d + 1`
elements contains every point of the interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_AdjacentIntegerIntervalReplacement
open TinylambOpt.ResearchGoals.JitKernels.InListLowering

private theorem nodup_filter_eraseDups :
    ∀ (t : List Int) (p : Int → Bool), List.Nodup ((t.filter p).eraseDups) := by
  intro t
  induction t with
  | nil => intro p; simp
  | cons c rest ih =>
    intro p
    cases hpc : p c with
    | true =>
      rw [List.filter_cons_of_pos hpc, List.eraseDups_cons, List.nodup_cons]
      refine ⟨fun hmem => ?_, ?_⟩
      · obtain ⟨_, m2⟩ := List.mem_filter.mp (List.mem_eraseDups.mp hmem)
        simp at m2
      · have key : (List.filter p rest).filter (fun b => !b == c)
            = List.filter (fun a => !a == c && p a) rest := by
          rw [List.filter_filter]
        rw [key]
        exact ih _
    | false =>
      rw [List.filter_cons_of_neg (by simpa using hpc)]
      exact ih p

private theorem nodup_eraseDups (ws : List Int) : List.Nodup (ws.eraseDups) := by
  have keyt : ws.filter (fun _ => true) = ws := by simp
  simpa [keyt] using nodup_filter_eraseDups ws (fun _ => true)

/-- Pigeonhole bound: `d + 2` distinct integers cannot all lie in `[lo, lo + d]`. -/
private theorem bound : ∀ (d : Nat) (lo : Int) (ws : List Int),
    List.Nodup ws → (∀ a ∈ ws, lo ≤ a ∧ a ≤ lo + d) → ws.length ≤ d + 1 := by
  intro d
  induction d with
  | zero =>
    intro lo ws hnd hall
    cases ws with
    | nil => simp
    | cons a t =>
      rw [List.nodup_cons] at hnd
      obtain ⟨hat, _⟩ := hnd
      have ha := hall a (by simp)
      have hlo : a = lo := by omega
      cases t with
      | nil => simp
      | cons b tb =>
        have hb := hall b (by simp)
        have hb0 : b = lo := by omega
        exfalso
        have : a ∈ b :: tb := by rw [hlo, ← hb0]; simp
        exact hat this
  | succ k ih =>
    intro lo ws hnd hall
    cases ws with
    | nil => simp
    | cons a t =>
      rw [List.nodup_cons] at hnd
      obtain ⟨hat, hndt⟩ := hnd
      by_cases haT : a = lo + (k + 1 : Nat)
      · have ht : ∀ b ∈ t, lo ≤ b ∧ b ≤ lo + k := fun b hb =>
          ⟨(hall b (by simp [hb])).1, by
            have h2 := (hall b (by simp [hb])).2
            have hne : ¬ b = lo + (k + 1 : Nat) := by
              intro heq
              exact hat (by rw [haT, ← heq]; exact hb)
            omega⟩
        have := ih lo t hndt ht
        simp only [List.length_cons]
        omega
      · by_cases htop : (lo + (k + 1 : Nat)) ∈ t
        · have hs : ∀ b ∈ a :: t.erase (lo + (k + 1 : Nat)),
              lo ≤ b ∧ b ≤ lo + k := by
            intro b hb
            rcases List.mem_cons.mp hb with hab | hmem
            · subst hab
              have ha := hall b (by simp)
              exact ⟨ha.1, by omega⟩
            · have h2 := hall b (List.Mem.tail a (List.mem_of_mem_erase hmem))
              have hne : ¬ b = lo + (k + 1 : Nat) := by
                intro hbT
                exact hndt.not_mem_erase (hbT ▸ hmem)
              exact ⟨h2.1, by omega⟩
          have hnd2 : List.Nodup (a :: t.erase (lo + (k + 1 : Nat))) := by
            rw [List.nodup_cons]
            exact ⟨fun h => hat (List.mem_of_mem_erase h), List.Nodup.erase _ hndt⟩
          have := ih lo (a :: t.erase (lo + (k + 1 : Nat))) hnd2 hs
          have helen : (t.erase (lo + (k + 1 : Nat))).length = t.length - 1 := by
            rw [List.length_erase, if_pos htop]
          simp only [List.length_cons] at this ⊢
          omega
        · have hall' : ∀ b ∈ a :: t, lo ≤ b ∧ b ≤ lo + k := fun b hb =>
            ⟨(hall b hb).1, by
              have h2 := (hall b hb).2
              have hne : ¬ b = lo + (k + 1 : Nat) := by
                intro hbT
                rcases List.mem_cons.mp hb with hba | hbt
                · exact haT (hba ▸ hbT)
                · exact htop (hbT ▸ hbt)
              omega⟩
          have := ih lo (a :: t) (List.nodup_cons.mpr ⟨hat, hndt⟩) hall'
          simp only [List.length_cons] at this ⊢
          omega

/-- Completeness: `d + 1` distinct integers inside `[lo, lo + d]` cover it. -/
private theorem pigeon : ∀ (d : Nat) (lo : Int) (ws : List Int),
    List.Nodup ws → (∀ a ∈ ws, lo ≤ a ∧ a ≤ lo + d) → ws.length = d + 1 →
    ∀ x, lo ≤ x → x ≤ lo + d → x ∈ ws := by
  intro d
  induction d with
  | zero =>
    intro lo ws hnd hall hlen x hx1 hx2
    cases ws with
    | nil => exact absurd hlen (by decide)
    | cons a t =>
      have ha := hall a (by simp)
      have aeq : a = lo := by omega
      cases t with
      | nil =>
        rw [aeq, List.mem_singleton]
        omega
      | cons b tb => exact absurd hlen (by simp)
  | succ k ih =>
    intro lo ws hnd hall hlen x hx1 hx2
    by_cases htop : (lo + (k + 1 : Nat)) ∈ ws
    · by_cases hxT : x = lo + (k + 1 : Nat)
      · exact hxT ▸ htop
      · have hne : x ≤ lo + k := by omega
        have hws : (ws.erase (lo + (k + 1 : Nat))).length = k + 1 := by
          rw [List.length_erase, if_pos htop]
          have h2 : ws.length = k + 1 + 1 := by omega
          omega
        have hall' : ∀ b ∈ ws.erase (lo + (k + 1 : Nat)), lo ≤ b ∧ b ≤ lo + k := fun b hb =>
          ⟨(hall b (List.mem_of_mem_erase hb)).1, by
            have h2 := (hall b (List.mem_of_mem_erase hb)).2
            have hneb : ¬ b = lo + (k + 1 : Nat) := by
              intro hbT
              exact hnd.not_mem_erase (hbT ▸ hb)
            omega⟩
        have hnd' : List.Nodup (ws.erase (lo + (k + 1 : Nat))) := List.Nodup.erase _ hnd
        have := ih lo (ws.erase (lo + (k + 1 : Nat))) hnd' hall' hws x hx1 hne
        exact List.mem_of_mem_erase this
    · have hall' : ∀ b ∈ ws, lo ≤ b ∧ b ≤ lo + k := fun b hb =>
        ⟨(hall b hb).1, by
          have h2 := (hall b hb).2
          have hne : ¬ b = lo + (k + 1 : Nat) := fun hbT => htop (hbT ▸ hb)
          omega⟩
      have := bound k lo ws hnd hall'
      omega

private theorem testNE (xs : Keys) (h : xs ≠ []) (w : Int) :
    test xs (some w) = if xs.contains (some w) then some true
      else if xs.contains none then none else some false := by
  cases xs with
  | nil => exact absurd rfl h
  | cons y ys =>
    show (if _ then _ else if (some w).isNone then _ else _) = _
    simp

theorem sound : AdjacentIntegerIntervalReplacement.Goal := by
  intro vs l h x hmin hmax hlen
  obtain ⟨hl, hl'⟩ := List.min?_eq_some_iff.mp hmin
  obtain ⟨hh, hh'⟩ := List.max?_eq_some_iff.mp hmax
  have hle : l ≤ h := hl' _ hh
  let d := (h - l).toNat
  have hd : (d : Int) = h - l := Int.toNat_of_nonneg (by omega)
  have hvsne : vs ≠ [] := by
    intro contra
    rw [contra] at hl
    exact absurd hl (by simp)
  have hmapne : vs.map some ≠ [] := by
    intro contra
    cases vs with
    | nil => exact absurd rfl hvsne
    | cons z zs => exact absurd contra (by simp)
  have hlenN : vs.eraseDups.length = d + 1 := by
    rw [← hd] at hlen
    omega
  have hW : List.Nodup (vs.eraseDups) := nodup_eraseDups vs
  have hWelem : ∀ a ∈ vs.eraseDups, l ≤ a ∧ a ≤ l + d := fun a ha =>
    ⟨hl' _ (List.mem_eraseDups.mp ha), by
      have h2 := hh' _ (List.mem_eraseDups.mp ha)
      rw [hd]; omega⟩
  have hinterval : l ≤ x → x ≤ l + d → x ∈ vs := fun h1 h2 =>
    List.mem_eraseDups.mp <| pigeon d l (vs.eraseDups) hW hWelem hlenN x h1 h2
  have hnone : ((vs.map some).contains none) = false := by
    rw [List.contains_eq_mem]
    refine decide_eq_false (by
      intro hm
      obtain ⟨a, _, haeq⟩ := List.mem_map.mp hm
      cases haeq)
  show (test (vs.map some) (some x) == some true) = decide (l ≤ x ∧ x ≤ h)
  rw [testNE (vs.map some) hmapne x]
  by_cases hxm : x ∈ vs
  · have hc : (vs.map some).contains (some x) = true := by
      rw [List.contains_iff_mem, List.mem_map]
      exact ⟨x, hxm, rfl⟩
    rw [if_pos hc, decide_eq_true ⟨hl' _ hxm, hh' _ hxm⟩]
    exact (by decide : (some true == some true) = true)
  · have hc : (vs.map some).contains (some x) = false := by
      rw [List.contains_eq_mem]
      refine decide_eq_false (by
        intro h
        obtain ⟨a, ha, haeq⟩ := List.mem_map.mp h
        exact hxm ((Option.some.inj haeq) ▸ ha))
    rw [if_neg (by simpa using hc), hnone, if_neg (by simpa using hnone)]
    rw [decide_eq_false (by
      intro h2
      exact hxm (hinterval h2.1 (by rw [hd]; omega)))]
    exact (by decide : (some false == some true) = false)

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_AdjacentIntegerIntervalReplacement
