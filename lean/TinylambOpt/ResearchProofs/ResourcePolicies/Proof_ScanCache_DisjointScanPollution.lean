import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_DisjointScanPollution

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache


/-!
  Scanning keys disjoint from later probes cannot increase demand misses:
  evicting a scanned key from the prefix is monotone for all other keys.
  -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

namespace Helpers

def bs (bytes : Nat → Nat) (l : List Nat) : Nat := (l.map bytes).sum

theorem fit_prefix (bytes : Nat → Nat) : ∀ (cap : Nat) (l : List Nat),
    fit bytes cap l <+: l
  | _, [] => by simp [fit]
  | cap, i :: is => by
      by_cases h : bytes i ≤ cap
      · simpa [fit, h] using fit_prefix bytes (cap - bytes i) is
      · simp [fit, h]

theorem fit_sum_le (bytes : Nat → Nat) : ∀ (cap : Nat) (l : List Nat),
    bs bytes (fit bytes cap l) ≤ cap
  | _, [] => by simp [fit, bs]
  | cap, i :: is => by
      by_cases h : bytes i ≤ cap
      · simp only [fit, h, ite_true]
        simp [bs]
        have ih := fit_sum_le bytes (cap - bytes i) is
        simp [bs] at ih
        omega
      · simp [fit, h, bs]

theorem fit_max (bytes : Nat → Nat) : ∀ (cap : Nat) (l p : List Nat),
    p <+: l → bs bytes p ≤ cap → p <+: fit bytes cap l
  | cap, [], p, hp, hs => by
      cases p with
      | nil => simp [fit]
      | cons j p' =>
          rcases hp with ⟨t, ht⟩
          simp at ht
  | cap, i :: is, p, hp, hs => by
      cases p with
      | nil => exact List.nil_prefix
      | cons j p' =>
          rcases hp with ⟨t, ht⟩
          simp only [List.cons_append] at ht
          cases ht
          have hsum_aux : bytes i + (List.map bytes p').sum ≤ cap := by
            simpa [bs] using hs
          have hsum : bs bytes p' ≤ cap - bytes i := by
            simp [bs]
            omega
          by_cases h : bytes i ≤ cap
          · simpa [fit, h] using
              (fit_max bytes (cap - bytes i) (p' ++ t) p' ⟨t, rfl⟩ hsum)
          · omega

theorem fit_mono_cap (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b) (l : List Nat) :
    fit bytes a l <+: fit bytes b l := by
  exact fit_max bytes b l (fit bytes a l) (fit_prefix bytes a l)
    (Nat.le_trans (fit_sum_le bytes a l) hab)

theorem fit_mono_list (bytes : Nat → Nat) (cap : Nat) {l₁ l₂ : List Nat}
    (h : l₁ <+: l₂) : fit bytes cap l₁ <+: fit bytes cap l₂ := by
  exact fit_max bytes cap l₂ (fit bytes cap l₁)
    ((fit_prefix bytes cap l₁).trans h) (fit_sum_le bytes cap l₁)

theorem fit_fit (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b) (l : List Nat) :
    fit bytes a (fit bytes b l) = fit bytes a l := by
  apply List.IsPrefix.eq_of_length_le (fit_mono_list bytes a (fit_prefix bytes b l))
  exact (fit_max bytes a (fit bytes b l) (fit bytes a l)
    (fit_mono_cap bytes hab l) (fit_sum_le bytes a l)).length_le

theorem shift (bytes : Nat → Nat) (k : Nat) : ∀ (d : Nat) (xs : List Nat),
    fit bytes d (xs.erase k) = fit bytes d ((fit bytes (d + bytes k) xs).erase k)
  | d, [] => by simp [fit]
  | d, x :: xs => by
      by_cases hxk : x = k
      · subst x
        simp [fit, Nat.add_sub_cancel_left]
        exact (fit_fit bytes (Nat.le_refl d) xs).symm
      · have hne : (x == k) = false := by simp [hxk]
        simp only [List.erase_cons, hne, ite_false]
        by_cases hxd : bytes x ≤ d
        · have hxbig : bytes x ≤ d + bytes k := by omega
          simp [fit, hxd, hxbig, hne]
          have hs := shift bytes k (d - bytes x) xs
          have hcap : d - bytes x + bytes k = d + bytes k - bytes x := by omega
          simpa [hcap] using hs
        · by_cases hxbig : bytes x ≤ d + bytes k
          · simp [fit, hxd, hxbig, hne]
          · simp [fit, hxd, hxbig, hne]

theorem fit_cons_erase_fit (bytes : Nat → Nat) (cap k : Nat) (c : List Nat) :
    fit bytes cap (k :: c.erase k) = fit bytes cap (k :: (fit bytes cap c).erase k) := by
  by_cases hbk : bytes k ≤ cap
  · have hcap : cap - bytes k + bytes k = cap := by omega
    simpa [fit, hbk, hcap] using shift bytes k (cap - bytes k) c
  · simp [fit, hbk]

theorem touch_commute (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b)
    (c : List Nat) (k : Nat) :
    fit bytes a (touch bytes b c k) = touch bytes a (fit bytes a c) k := by
  unfold touch
  rw [fit_fit bytes hab]
  exact fit_cons_erase_fit bytes a k c

theorem run_shrink (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b) :
    ∀ (c : List Nat) (trace : List Nat),
      fit bytes a (run bytes b c trace) = run bytes a (fit bytes a c) trace
  | c, [] => by simp [run]
  | c, k :: ks => by
      change fit bytes a (run bytes b (touch bytes b c k) ks) =
        run bytes a (touch bytes a (fit bytes a c) k) ks
      rw [run_shrink bytes hab (touch bytes b c k) ks]
      rw [touch_commute bytes hab c k]



def keep (s x : Nat) : Bool := x != s

theorem takeWhile_fit_of_takeWhile_prefix (bytes : Nat → Nat) (cap s : Nat) :
    ∀ {l₁ l₂ : List Nat}, l₁.takeWhile (keep s) <+: l₂.takeWhile (keep s) →
      (fit bytes cap l₁).takeWhile (keep s) <+: (fit bytes cap l₂).takeWhile (keep s)
  | [], l₂, h => by simp [fit]
  | x :: xs, [], h => by
      have hx : x = s := by
        by_cases hx' : x = s
        · exact hx'
        · have hxkeep : keep s x = true := by simp [keep, hx']
          have hp : (x :: xs).takeWhile (keep s) = x :: xs.takeWhile (keep s) := by simp [hxkeep]
          rw [hp] at h
          rcases h with ⟨t, ht⟩
          simp at ht
      subst x
      by_cases hbs : bytes s ≤ cap
      · simp [fit, hbs, keep]
      · simp [fit, hbs]
  | x :: xs, y :: ys, h => by
      by_cases hx : x = s
      · subst x
        by_cases hbs : bytes s ≤ cap
        · simp [fit, hbs, keep]
        · simp [fit, hbs]
      · have hxkeep : keep s x = true := by simp [keep, hx]
        have htwx : (x :: xs).takeWhile (keep s) = x :: xs.takeWhile (keep s) := by simp [hxkeep]
        rw [htwx] at h
        by_cases hy : y = s
        · subst y
          simp [keep] at h
        · have hykeep : keep s y = true := by simp [keep, hy]
          have htwy : (y :: ys).takeWhile (keep s) = y :: ys.takeWhile (keep s) := by simp [hykeep]
          rw [htwy] at h
          rcases (List.cons_prefix_cons.mp h) with ⟨hxy, htail⟩
          subst y
          by_cases hxfit : bytes x ≤ cap
          · have ih := takeWhile_fit_of_takeWhile_prefix bytes (cap - bytes x) s htail
            have hfitL : fit bytes cap (x :: xs) = x :: fit bytes (cap - bytes x) xs := by simp [fit, hxfit]
            have hfitR : fit bytes cap (x :: ys) = x :: fit bytes (cap - bytes x) ys := by simp [fit, hxfit]
            rw [hfitL, hfitR]
            have hp' : x :: (fit bytes (cap - bytes x) xs).takeWhile (keep s) <+:
                x :: (fit bytes (cap - bytes x) ys).takeWhile (keep s) :=
              List.cons_prefix_cons.mpr ⟨rfl, ih⟩
            simpa [hxkeep] using hp'
          · simp [fit, hxfit]



theorem filter_prefix (p : Nat → Bool) : ∀ {l₁ l₂ : List Nat},
    l₁ <+: l₂ → l₁.filter p <+: l₂.filter p
  | [], l₂, h => by simp
  | x :: xs, [], h => by
      rcases h with ⟨t, ht⟩
      simp at ht
  | x :: xs, y :: ys, h => by
      rcases h with ⟨t, ht⟩
      simp only [List.cons_append] at ht
      injection ht with hy htail
      subst y
      by_cases hpx : p x = true
      · have hp := filter_prefix p (⟨t, htail⟩ : xs <+: ys)
        have hp' : x :: xs.filter p <+: x :: ys.filter p := List.cons_prefix_cons.mpr ⟨rfl, hp⟩
        simpa [hpx] using hp'
      · simpa [hpx] using filter_prefix p (⟨t, htail⟩ : xs <+: ys)

theorem prefix_nil_eq {l : List Nat} (h : l <+: []) : l = [] := by
  rcases h with ⟨t, ht⟩
  cases t
  · simpa using ht
  · simp at ht

theorem filter_keep_eq_self {s : Nat} {l : List Nat} (h : s ∉ l) :
    l.filter (keep s) = l := by
  apply List.filter_eq_self.mpr
  intro a ha
  simp [keep]
  intro has
  subst a
  exact h ha

theorem takeWhile_keep_eq_self {s : Nat} : ∀ {l : List Nat}, s ∉ l →
    l.takeWhile (keep s) = l
  | [], h => by simp
  | x :: xs, h => by
      have hx : x ≠ s := by
        intro hx
        subst x
        exact h (by simp)
      have hxs : s ∉ xs := by
        intro hs
        exact h (by simp [hs])
      have hxkeep : keep s x = true := by simp [keep, hx]
      simpa [hxkeep] using congrArg (List.cons x) (takeWhile_keep_eq_self hxs)

theorem filter_fit_after_s (bytes : Nat → Nat) (cap s : Nat) (hbs : bytes s ≤ cap) :
    ∀ {xs l₂ : List Nat}, xs.Nodup → s ∉ xs → l₂.Nodup →
      xs <+: l₂.filter (keep s) →
      (fit bytes (cap - bytes s) xs).filter (keep s) <+: (fit bytes cap l₂).filter (keep s)
  | xs, [], hnd, hsx, hnd2, h => by
      have hxnil : xs = [] := prefix_nil_eq h
      subst xs
      simp [fit]
  | xs, y :: ys, hnd, hsx, hnd2, h => by
      by_cases hy : y = s
      · subst y
        have hs_not_ys : s ∉ ys := (List.nodup_cons.mp hnd2).1
        have hysfilter : ys.filter (keep s) = ys := filter_keep_eq_self hs_not_ys
        have h' : xs <+: ys := by simpa [keep, hysfilter] using h
        have hfitprefix := fit_mono_list bytes (cap - bytes s) h'
        have hfilter := filter_prefix (keep s) hfitprefix
        have hfits : fit bytes cap (s :: ys) = s :: fit bytes (cap - bytes s) ys := by simp [fit, hbs]
        rw [hfits]
        simpa [keep] using hfilter
      · have hykeep : keep s y = true := by simp [keep, hy]
        have hfilter_cons : (y :: ys).filter (keep s) = y :: ys.filter (keep s) := by simp [hykeep]
        rw [hfilter_cons] at h
        cases xs with
        | nil => simp [fit]
        | cons x xs' =>
          have hxprefix : x = y := (List.cons_prefix_cons.mp h).1
          subst x
          have htail : xs' <+: ys.filter (keep s) := (List.cons_prefix_cons.mp h).2
          have hxs'nd : xs'.Nodup := (List.nodup_cons.mp hnd).2
          have hsx' : s ∉ xs' := by
            intro hs
            exact hsx (by simp [hs])
          have hysnd : ys.Nodup := (List.nodup_cons.mp hnd2).2
          by_cases hyfit : bytes y ≤ cap - bytes s
          · have hbs' : bytes s ≤ cap - bytes y := by omega
            have hcapEq : cap - bytes s - bytes y = cap - bytes y - bytes s := by omega
            have ih := filter_fit_after_s bytes (cap - bytes y) s hbs' hxs'nd hsx' hysnd htail
            rw [← hcapEq] at ih
            have hcapfit : bytes y ≤ cap := by omega
            have hfitL : fit bytes (cap - bytes s) (y :: xs') = y :: fit bytes (cap - bytes s - bytes y) xs' := by simp [fit, hyfit]
            have hfitR : fit bytes cap (y :: ys) = y :: fit bytes (cap - bytes y) ys := by simp [fit, hcapfit]
            rw [hfitL, hfitR]
            have hp' : y :: (fit bytes (cap - bytes s - bytes y) xs').filter (keep s) <+:
                y :: (fit bytes (cap - bytes y) ys).filter (keep s) :=
              List.cons_prefix_cons.mpr ⟨rfl, ih⟩
            simpa [hykeep] using hp'
          · have hfitLnil : fit bytes (cap - bytes s) (y :: xs') = [] := by simp [fit, hyfit]
            rw [hfitLnil]
            exact List.nil_prefix



def rel (s : Nat) (l₁ l₂ : List Nat) : Prop :=
  l₁.filter (keep s) <+: l₂.filter (keep s) ∧
  l₁.takeWhile (keep s) <+: l₂.takeWhile (keep s)

theorem fit_rel (bytes : Nat → Nat) (cap s : Nat) :
    ∀ {l₁ l₂ : List Nat}, l₁.Nodup → l₂.Nodup → rel s l₁ l₂ →
      rel s (fit bytes cap l₁) (fit bytes cap l₂)
  | [], l₂, hnd1, hnd2, h => by
      simp [fit, rel]
  | x :: xs, l₂, hnd1, hnd2, h => by
      have hndxs : xs.Nodup := (List.nodup_cons.mp hnd1).2
      cases l₂ with
      | nil =>
          have hfilterEq : (x :: xs).filter (keep s) = [] := prefix_nil_eq h.1
          have htakeEq : (x :: xs).takeWhile (keep s) = [] := prefix_nil_eq h.2
          have hx : x = s := by
            by_cases hx' : x = s
            · exact hx'
            · have hxkeep : keep s x = true := by simp [keep, hx']
              have htw : (x :: xs).takeWhile (keep s) = x :: xs.takeWhile (keep s) := by simp [hxkeep]
              rw [htw] at htakeEq
              simp at htakeEq
          subst x
          constructor
          · have hp := filter_prefix (keep s) (fit_prefix bytes cap (s :: xs))
            rw [hfilterEq] at hp
            exact hp
          · by_cases hbs : bytes s ≤ cap
            · simp [fit, hbs, keep]
            · simp [fit, hbs]
      | cons y ys =>
          have hndys : ys.Nodup := (List.nodup_cons.mp hnd2).2
          by_cases hx : x = s
          · subst x
            have hs_not_xs : s ∉ xs := (List.nodup_cons.mp hnd1).1
            by_cases hbs : bytes s ≤ cap
            · constructor
              · by_cases hy : y = s
                · subst y
                  have hs_not_ys : s ∉ ys := (List.nodup_cons.mp hnd2).1
                  have htail : xs <+: ys := by
                    simpa [keep, filter_keep_eq_self hs_not_xs,
                      filter_keep_eq_self hs_not_ys] using h.1
                  have hfit := fit_mono_list bytes (cap - bytes s) htail
                  have hf := filter_prefix (keep s) hfit
                  have hfitsL : fit bytes cap (s :: xs) = s :: fit bytes (cap - bytes s) xs := by
                    simp [fit, hbs]
                  have hfitsR : fit bytes cap (s :: ys) = s :: fit bytes (cap - bytes s) ys := by
                    simp [fit, hbs]
                  rw [hfitsL, hfitsR]
                  simpa [keep] using hf
                · have hfilterL : xs.filter (keep s) = xs := filter_keep_eq_self hs_not_xs
                  have htail : xs <+: (y :: ys).filter (keep s) := by
                    simpa [keep, hfilterL] using h.1
                  have hf := filter_fit_after_s bytes cap s hbs hndxs hs_not_xs hnd2 (l₂ := y :: ys) htail
                  have hfitsL : fit bytes cap (s :: xs) = s :: fit bytes (cap - bytes s) xs := by
                    simp [fit, hbs]
                  rw [hfitsL]
                  simpa [keep] using hf
              · simp [fit, hbs, keep]
            · simp [rel, fit, hbs]
          · have hxkeep : keep s x = true := by simp [keep, hx]
            have htakeL : (x :: xs).takeWhile (keep s) = x :: xs.takeWhile (keep s) := by simp [hxkeep]
            have htake := h.2
            rw [htakeL] at htake
            by_cases hy : y = s
            · subst y
              simp [keep] at htake
            · have hykeep : keep s y = true := by simp [keep, hy]
              have htakeR : (y :: ys).takeWhile (keep s) = y :: ys.takeWhile (keep s) := by simp [hykeep]
              rw [htakeR] at htake
              have hpT := List.cons_prefix_cons.mp htake
              have hxy : x = y := hpT.1
              subst y
              have htakeTail : xs.takeWhile (keep s) <+: ys.takeWhile (keep s) := hpT.2
              have hfilterL : (x :: xs).filter (keep s) = x :: xs.filter (keep s) := by simp [hxkeep]
              have hfilterR : (x :: ys).filter (keep s) = x :: ys.filter (keep s) := by simp [hxkeep]
              have hfilter := h.1
              rw [hfilterL, hfilterR] at hfilter
              have hpF := List.cons_prefix_cons.mp hfilter
              have hfilterTail : xs.filter (keep s) <+: ys.filter (keep s) := hpF.2
              by_cases hxfit : bytes x ≤ cap
              · have ih := fit_rel bytes (cap - bytes x) s hndxs hndys ⟨hfilterTail, htakeTail⟩
                have hfitL : fit bytes cap (x :: xs) = x :: fit bytes (cap - bytes x) xs := by simp [fit, hxfit]
                have hfitR : fit bytes cap (x :: ys) = x :: fit bytes (cap - bytes x) ys := by simp [fit, hxfit]
                rw [hfitL, hfitR]
                constructor
                · have hp' : x :: (fit bytes (cap - bytes x) xs).filter (keep s) <+:
                      x :: (fit bytes (cap - bytes x) ys).filter (keep s) :=
                    List.cons_prefix_cons.mpr ⟨rfl, ih.1⟩
                  simpa [hxkeep] using hp'
                · have hp' : x :: (fit bytes (cap - bytes x) xs).takeWhile (keep s) <+:
                      x :: (fit bytes (cap - bytes x) ys).takeWhile (keep s) :=
                    List.cons_prefix_cons.mpr ⟨rfl, ih.2⟩
                  simpa [hxkeep] using hp'
              · simp [rel, fit, hxfit]



theorem mem_of_prefix {a : Nat} {l p : List Nat} (h : p <+: l) (ha : a ∈ p) : a ∈ l := by
  rcases h with ⟨t, ht⟩
  rw [← ht]
  simp [ha]

theorem mem_fit_bytes_le (bytes : Nat → Nat) : ∀ (cap : Nat) (l : List Nat) {x : Nat},
    x ∈ fit bytes cap l → bytes x ≤ cap
  | _, [], x, h => by simp [fit] at h
  | cap, y :: ys, x, h => by
      by_cases hy : bytes y ≤ cap
      · simp [fit, hy] at h
        rcases h with hxy | hxmem
        · subst x
          exact hy
        · have ih := mem_fit_bytes_le bytes (cap - bytes y) ys hxmem
          omega
      · simp [fit, hy] at h

theorem not_mem_erase_self_of_nodup : ∀ {c : List Nat}, c.Nodup → ∀ k, k ∉ c.erase k
  | [], hc, k => by simp
  | x :: xs, hc, k => by
      have hxnot : x ∉ xs := (List.nodup_cons.mp hc).1
      have hxsnd : xs.Nodup := (List.nodup_cons.mp hc).2
      by_cases hxk : x = k
      · subst x
        simpa using hxnot
      · have hbeq : (x == k) = false := by simp [hxk]
        rw [List.erase_cons, hbeq]
        simp
        exact ⟨by intro hkx; exact hxk hkx.symm, not_mem_erase_self_of_nodup hxsnd k⟩

theorem nodup_of_prefix : ∀ {p l : List Nat}, p <+: l → l.Nodup → p.Nodup
  | [], l, h, hl => by simp
  | x :: xs, [], h, hl => by
      rcases h with ⟨t, ht⟩
      simp at ht
  | x :: xs, y :: ys, h, hl => by
      rcases h with ⟨t, ht⟩
      simp only [List.cons_append] at ht
      injection ht with hxy htailEq
      subst y
      subst ys
      have hxnotAll : x ∉ xs ++ t := (List.nodup_cons.mp hl).1
      have htailnd : (xs ++ t).Nodup := (List.nodup_cons.mp hl).2
      have hxsnd : xs.Nodup := nodup_of_prefix (⟨t, rfl⟩ : xs <+: xs ++ t) htailnd
      exact List.nodup_cons.mpr ⟨by
        intro hxmem
        exact hxnotAll (by simp [hxmem]), hxsnd⟩

theorem touch_nodup (bytes : Nat → Nat) (cap : Nat) {c : List Nat} (hc : c.Nodup) (k : Nat) :
    (touch bytes cap c k).Nodup := by
  unfold touch
  apply nodup_of_prefix (fit_prefix bytes cap (k :: c.erase k))
  exact List.nodup_cons.mpr ⟨not_mem_erase_self_of_nodup hc k, List.Nodup.erase k hc⟩

theorem fit_touch_self (bytes : Nat → Nat) (cap : Nat) (c : List Nat) (k : Nat) :
    fit bytes cap (touch bytes cap c k) = touch bytes cap c k := by
  unfold touch
  exact fit_fit bytes (Nat.le_refl cap) (k :: c.erase k)

theorem fit_run_self (bytes : Nat → Nat) (cap : Nat) :
    ∀ {c : List Nat} (trace : List Nat), fit bytes cap c = c →
      fit bytes cap (run bytes cap c trace) = run bytes cap c trace
  | c, [], h => h
  | c, k :: ks, h => by
      change fit bytes cap (run bytes cap (touch bytes cap c k) ks) =
        run bytes cap (touch bytes cap c k) ks
      exact fit_run_self bytes cap ks (fit_touch_self bytes cap c k)

theorem run_nodup (bytes : Nat → Nat) (cap : Nat) :
    ∀ (c : List Nat) (trace : List Nat), c.Nodup → (run bytes cap c trace).Nodup
  | c, [], hc => hc
  | c, k :: ks, hc => by
      change (run bytes cap (touch bytes cap c k) ks).Nodup
      exact run_nodup bytes cap (touch bytes cap c k) ks (touch_nodup bytes cap hc k)

theorem takeWhile_erase (s k : Nat) (hsk : k ≠ s) : ∀ (l : List Nat),
    (l.erase k).takeWhile (keep s) = (l.takeWhile (keep s)).erase k
  | [] => by simp
  | x :: xs => by
      by_cases hxk : x = k
      · subst x
        have hkeep : keep s k = true := by simp [keep, hsk]
        have htw : List.takeWhile (keep s) (k :: xs) = k :: List.takeWhile (keep s) xs := by simp [hkeep]
        rw [htw]
        simp
      · by_cases hxs : x = s
        · subst x
          simp [List.erase_cons, hxk, keep]
        · have hxkeep : keep s x = true := by simp [keep, hxs]
          have hbeq : (x == k) = false := by simp [hxk]
          simp [List.erase_cons, hbeq, hxkeep, takeWhile_erase s k hsk xs]

theorem takeWhile_erase_prefix {s k : Nat} (hsk : k ≠ s) {A B : List Nat}
    (h : A.takeWhile (keep s) <+: B.takeWhile (keep s)) :
    (A.erase k).takeWhile (keep s) <+: (B.erase k).takeWhile (keep s) := by
  rw [takeWhile_erase s k hsk A, takeWhile_erase s k hsk B]
  exact List.IsPrefix.erase k h

def dom (s : Nat) (A B : List Nat) : Prop :=
  B.filter (keep s) <+: A.filter (keep s) ∧
  B.takeWhile (keep s) <+: A.takeWhile (keep s) ∧
  (s ∈ A → s ∈ B)

theorem dom_refl (s : Nat) (A : List Nat) : dom s A A := by
  simp [dom]

theorem dom_trans {s : Nat} {A B C : List Nat} (hAB : dom s A B) (hBC : dom s B C) : dom s A C := by
  refine ⟨?_, ?_, ?_⟩
  · exact hBC.1.trans hAB.1
  · exact hBC.2.1.trans hAB.2.1
  · intro hs
    exact hBC.2.2 (hAB.2.2 hs)

theorem dom_scan (bytes : Nat → Nat) (cap s : Nat) {c : List Nat}
    (_hc : c.Nodup) (hcfit : fit bytes cap c = c) : dom s c (touch bytes cap c s) := by
  unfold touch
  by_cases hbs : bytes s ≤ cap
  · have hraw : (c.erase s).filter (keep s) = c.filter (keep s) := by
      rw [← List.erase_filter]
      apply List.erase_of_not_mem
      simp [keep]
    have hfitprefix := filter_prefix (keep s) (fit_prefix bytes (cap - bytes s) (c.erase s))
    have hfilter : (fit bytes cap (s :: c.erase s)).filter (keep s) <+: c.filter (keep s) := by
      rw [show fit bytes cap (s :: c.erase s) = s :: fit bytes (cap - bytes s) (c.erase s) by simp [fit, hbs]]
      simpa [keep, hraw] using hfitprefix
    refine ⟨hfilter, ?_, ?_⟩
    · simp [fit, hbs, keep]
    · intro hsC
      have hsFit : s ∈ fit bytes cap c := by simpa [hcfit] using hsC
      have hle := mem_fit_bytes_le bytes cap c hsFit
      rw [show fit bytes cap (s :: c.erase s) = s :: fit bytes (cap - bytes s) (c.erase s) by simp [fit, hbs]]
      simp
  · have hsnot : s ∉ c := by
      intro hsC
      have hsFit : s ∈ fit bytes cap c := by simpa [hcfit] using hsC
      have hle := mem_fit_bytes_le bytes cap c hsFit
      omega
    simp [fit, hbs, dom, hsnot]

theorem mem_fit_of_dom (bytes : Nat → Nat) (cap s : Nat) :
    ∀ {A B : List Nat}, A.Nodup → B.Nodup → dom s A B →
      s ∈ fit bytes cap A → s ∈ fit bytes cap B
  | [], B, hA, hB, h, hs => by simp [fit] at hs
  | x :: xs, B, hA, hB, h, hs => by
      have hndxs : xs.Nodup := (List.nodup_cons.mp hA).2
      cases B with
      | nil =>
          have hsA : s ∈ x :: xs := mem_of_prefix (fit_prefix bytes cap (x :: xs)) hs
          have hsB : s ∈ ([] : List Nat) := h.2.2 hsA
          simp at hsB
      | cons y ys =>
          have hndys : ys.Nodup := (List.nodup_cons.mp hB).2
          by_cases hx : x = s
          · subst x
            have hsB : s ∈ y :: ys := h.2.2 (by simp)
            have hyNil : (y :: ys).takeWhile (keep s) = [] :=
              prefix_nil_eq (by simpa [keep] using h.2.1)
            have hy : y = s := by
              by_cases hy' : y = s
              · exact hy'
              · have hykeep : keep s y = true := by simp [keep, hy']
                have htw : (y :: ys).takeWhile (keep s) = y :: ys.takeWhile (keep s) := by simp [hykeep]
                rw [htw] at hyNil
                simp at hyNil
            subst y
            by_cases hbs : bytes s ≤ cap
            · simp [fit, hbs]
            · simp [fit, hbs] at hs
          · have hxkeep : keep s x = true := by simp [keep, hx]
            have htwL : (x :: xs).takeWhile (keep s) = x :: xs.takeWhile (keep s) := by simp [hxkeep]
            have hfilterL : (x :: xs).filter (keep s) = x :: xs.filter (keep s) := by simp [hxkeep]
            have htake := h.2.1
            rw [htwL] at htake
            by_cases hy : y = s
            · subst y
              by_cases hxfit : bytes x ≤ cap
              · have hfitL : fit bytes cap (x :: xs) = x :: fit bytes (cap - bytes x) xs := by simp [fit, hxfit]
                rw [hfitL] at hs
                have hsne : s ≠ x := by
                  intro hx'
                  exact hx hx'.symm
                simp [hsne] at hs
                have hle := mem_fit_bytes_le bytes (cap - bytes x) xs hs
                have hlecap : bytes s ≤ cap := by omega
                simp [fit, hlecap]
              · simp [fit, hxfit] at hs
            · have hykeep : keep s y = true := by simp [keep, hy]
              have htwR : (y :: ys).takeWhile (keep s) = y :: ys.takeWhile (keep s) := by simp [hykeep]
              rw [htwR] at htake
              have hpT := List.cons_prefix_cons.mp htake
              have hxy : x = y := hpT.1.symm
              subst y
              have htakeTail : ys.takeWhile (keep s) <+: xs.takeWhile (keep s) := hpT.2
              have hfilterR : (x :: ys).filter (keep s) = x :: ys.filter (keep s) := by simp [hxkeep]
              have hfilter := h.1
              rw [hfilterL, hfilterR] at hfilter
              have hpF := List.cons_prefix_cons.mp hfilter
              have hfilterTail : ys.filter (keep s) <+: xs.filter (keep s) := hpF.2
              have hmemTail : dom s xs ys := ⟨hfilterTail, htakeTail, by
                intro hsx
                have hsA : s ∈ x :: xs := by simp [hsx]
                have hsB : s ∈ x :: ys := h.2.2 hsA
                have hsne : s ≠ x := by
                  intro hx'
                  exact hx hx'.symm
                simpa [hsne] using hsB⟩
              by_cases hxfit : bytes x ≤ cap
              · have hfitL : fit bytes cap (x :: xs) = x :: fit bytes (cap - bytes x) xs := by simp [fit, hxfit]
                have hfitR : fit bytes cap (x :: ys) = x :: fit bytes (cap - bytes x) ys := by simp [fit, hxfit]
                rw [hfitL] at hs
                have hsne : s ≠ x := by
                  intro hx'
                  exact hx hx'.symm
                simp [hsne] at hs
                rw [hfitR]
                simp [hsne]
                exact mem_fit_of_dom bytes (cap - bytes x) s hndxs hndys hmemTail hs
              · simp [fit, hxfit] at hs

theorem dom_touch (bytes : Nat → Nat) (cap s k : Nat) (hsk : k ≠ s)
    {A B : List Nat} (hA : A.Nodup) (hB : B.Nodup) (h : dom s A B) :
    dom s (touch bytes cap A k) (touch bytes cap B k) := by
  have hkkeep : keep s k = true := by simp [keep, hsk]
  have hrawA : (k :: A.erase k).Nodup :=
    List.nodup_cons.mpr ⟨not_mem_erase_self_of_nodup hA k, List.Nodup.erase k hA⟩
  have hrawB : (k :: B.erase k).Nodup :=
    List.nodup_cons.mpr ⟨not_mem_erase_self_of_nodup hB k, List.Nodup.erase k hB⟩
  have hfilterErase : (B.erase k).filter (keep s) <+: (A.erase k).filter (keep s) := by
    rw [← List.erase_filter, ← List.erase_filter]
    exact List.IsPrefix.erase k h.1
  have htakeErase : (B.erase k).takeWhile (keep s) <+: (A.erase k).takeWhile (keep s) :=
    takeWhile_erase_prefix hsk h.2.1
  have hrawFilter : (k :: B.erase k).filter (keep s) <+: (k :: A.erase k).filter (keep s) := by
    have hp : k :: (B.erase k).filter (keep s) <+: k :: (A.erase k).filter (keep s) :=
      List.cons_prefix_cons.mpr ⟨rfl, hfilterErase⟩
    simpa [hkkeep] using hp
  have hrawTake : (k :: B.erase k).takeWhile (keep s) <+: (k :: A.erase k).takeWhile (keep s) := by
    have hp : k :: (B.erase k).takeWhile (keep s) <+: k :: (A.erase k).takeWhile (keep s) :=
      List.cons_prefix_cons.mpr ⟨rfl, htakeErase⟩
    simpa [hkkeep] using hp
  have hrelRaw := fit_rel bytes cap s hrawB hrawA ⟨hrawFilter, hrawTake⟩
  have hmemRaw : dom s (k :: A.erase k) (k :: B.erase k) := by
    refine ⟨hrawFilter, hrawTake, ?_⟩
    intro hsAraw
    have hsks : s ≠ k := by
      intro h
      exact hsk h.symm
    have hsA : s ∈ A := by
      simp only [List.mem_cons] at hsAraw
      rcases hsAraw with hsEq | hsErase
      · exact False.elim (hsks hsEq)
      · exact (List.mem_erase_of_ne hsks).mp hsErase
    have hsB : s ∈ B := h.2.2 hsA
    simp only [List.mem_cons]
    right
    exact (List.mem_erase_of_ne hsks).mpr hsB
  have hmemFit := mem_fit_of_dom bytes cap s hrawA hrawB hmemRaw
  unfold touch
  exact ⟨hrelRaw.1, hrelRaw.2, hmemFit⟩

theorem misses_dom (bytes : Nat → Nat) (cap s : Nat) :
    ∀ {A B : List Nat} (probes : List Nat), A.Nodup → B.Nodup → dom s A B →
      (∀ k ∈ probes, k ≠ s) →
      demandMisses bytes cap A probes ≤ demandMisses bytes cap B probes
  | A, B, [], hA, hB, h, hk => by simp [demandMisses]
  | A, B, k :: ks, hA, hB, h, hk => by
      have hkne : k ≠ s := hk k (by simp)
      have htail : ∀ x ∈ ks, x ≠ s := by
        intro x hx
        exact hk x (by simp [hx])
      have hA' := touch_nodup bytes cap hA k
      have hB' := touch_nodup bytes cap hB k
      have h' := dom_touch bytes cap s k hkne hA hB h
      have ih := misses_dom bytes cap s (A := touch bytes cap A k) (B := touch bytes cap B k)
        ks hA' hB' h' htail
      have hBhit_to_Ahit : k ∈ B → k ∈ A := by
        intro hkB
        have hkFB : k ∈ B.filter (keep s) := by simpa [keep, hkne] using hkB
        have hkFA : k ∈ A.filter (keep s) := mem_of_prefix h.1 hkFB
        exact (List.mem_filter.mp hkFA).1
      by_cases hkB : k ∈ B
      · have hkA := hBhit_to_Ahit hkB
        simp [demandMisses, hkA, hkB]
        exact ih
      · by_cases hkA : k ∈ A
        · simp [demandMisses, hkA, hkB]
          omega
        · simp [demandMisses, hkA, hkB]
          exact ih

theorem miss_scan_step (bytes : Nat → Nat) (cap s : Nat) {c : List Nat} (probes : List Nat)
    (hc : c.Nodup) (hcfit : fit bytes cap c = c)
    (hs : ∀ k ∈ probes, k ≠ s) :
    demandMisses bytes cap c probes ≤ demandMisses bytes cap (touch bytes cap c s) probes := by
  exact misses_dom bytes cap s probes hc (touch_nodup bytes cap hc s)
    (dom_scan bytes cap s hc hcfit) hs

theorem misses_scan_list (bytes : Nat → Nat) (cap : Nat) :
    ∀ (A : List Nat) (scan probes : List Nat), A.Nodup → fit bytes cap A = A →
      (∀ k ∈ scan, ¬ k ∈ probes) →
      demandMisses bytes cap A probes ≤ demandMisses bytes cap (run bytes cap A scan) probes
  | A, [], probes, hA, hAfit, hdisj => by simp [run]
  | A, s :: ss, probes, hA, hAfit, hdisj => by
      have hs : ∀ k ∈ probes, k ≠ s := by
        intro k hk hks
        subst k
        exact hdisj s (by simp) hk
      have hstep := miss_scan_step bytes cap s probes hA hAfit hs
      have htaildisj : ∀ k ∈ ss, ¬ k ∈ probes := by
        intro k hk
        exact hdisj k (by simp [hk])
      have hA' : (touch bytes cap A s).Nodup := touch_nodup bytes cap hA s
      have hA'fit : fit bytes cap (touch bytes cap A s) = touch bytes cap A s :=
        fit_touch_self bytes cap A s
      have htail := misses_scan_list bytes cap (touch bytes cap A s) ss probes hA' hA'fit htaildisj
      change demandMisses bytes cap A probes ≤ demandMisses bytes cap (run bytes cap (touch bytes cap A s) ss) probes
      exact Nat.le_trans hstep htail

end Helpers


theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.DisjointScanPollution.Goal := by
  intro bytes cap warm scan probes hdisj
  let A := run bytes cap [] warm
  have hA : A.Nodup := Helpers.run_nodup bytes cap [] warm (by simp)
  have hAfit : fit bytes cap A = A := Helpers.fit_run_self bytes cap warm (by simp [fit])
  simpa [A] using Helpers.misses_scan_list bytes cap A scan probes hA hAfit hdisj
end TinylambOpt.ResearchProofs.ResourcePolicies.ScanCache.Goal_DisjointScanPollution
