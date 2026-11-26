import TinylambOpt.ResearchGoals.ExecutionStrategies.OrderedStreams

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.

Proved (8 claims, including the following)

- `TinylambOpt.ResearchGoals.OrderedStreams.MergeDiagonal` — `∀ a b n, sorted a →
  sorted b → n ≤ a.length+b.length → let (i,j) := mergeCut a b n i+j = n ∧ cutOK a b
  i j ∧ (∀ i' j', i'+j' = n → cutOK a b i' j' → i' = i ∧ j' = j) ∧ (List.merge
  (a.drop i) (b.drop j) leKey) = (List.merge a b leKey).drop n`
- `TinylambOpt.ResearchGoals.OrderedStreams.MonotoneProbes` — `∀ ks rows, ks.Pairwise
  (· ≤ ·) → sorted rows → seekMany ks rows = ks.map (fun k => rows.find? fun r => r.1
  == k)`
- `TinylambOpt.ResearchGoals.OrderedStreams.RangeCoalescing` — `∀ (keys : List Nat) (g
  : Nat) (rows : List Nat), let spans := islands keys.mergeSort g (spans.flatMap fun
  (lo,hi) => rows.filter fun x => lo ≤ x && x ≤ hi && keys.contains x).Perm
  (rows.filter keys.contains) ∧ spans.Pairwise (fun a b => a.2+g+1 < b.1)`
-/

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.OrderedStreams

open TinylambOpt.ResearchGoals.OrderedStreams


private theorem find_head_dropWhile (k : Int) (xs : List Row) (hs : sorted xs) :
    (xs.dropWhile (fun r => r.1 < k)).head?.filter (fun r => r.1 == k) =
      xs.find? (fun r => r.1 == k) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hsx : sorted xs := List.Pairwise.of_cons hs
      by_cases hx : x.1 < k
      · have hxne : x.1 ≠ k := by omega
        have hxeq : (x.1 == k) = false := by simp [hxne]
        simp [List.dropWhile_cons, hx, hxeq, ih hsx, List.find?_cons, hxeq]
      · by_cases hxk : x.1 = k
        · have hxkeq : (x.1 == k) = true := by simp [hxk]
          simp [List.dropWhile_cons, hx, hxkeq, List.find?_cons, hxkeq]
        · have hxgt : k < x.1 := by omega
          have hxkeq : (x.1 == k) = false := by simp [hxk]
          have hnone : xs.find? (fun r => r.1 == k) = none := by
            rw [List.find?_eq_none]
            intro y hy
            have hge : x.1 ≤ y.1 := (List.pairwise_cons.mp hs).1 y hy
            simp [show y.1 ≠ k by omega]
          simp [hx, hxk, hxkeq, hnone]

private theorem dropWhile_chain (k k' : Int) (hkk : k ≤ k') (xs : List Row) :
    (xs.dropWhile (fun r => r.1 < k)).dropWhile (fun r => r.1 < k') =
      xs.dropWhile (fun r => r.1 < k') := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : x.1 < k
      · have hx' : x.1 < k' := by omega
        simp [List.dropWhile_cons, hx, hx', ih]
      · by_cases hx' : x.1 < k'
        · simp [List.dropWhile_cons, hx, hx', ih]
        · simp [List.dropWhile_cons, hx, hx']

private theorem sorted_dropWhile (p : Row → Bool) (xs : List Row) (hs : sorted xs) :
    sorted (xs.dropWhile p) := by
  induction xs with
  | nil => simp [sorted]
  | cons x xs ih =>
      by_cases hp : p x = true
      · simpa [hp] using ih (List.Pairwise.of_cons hs)
      · simpa [hp] using hs

private theorem seekMany_aux : ∀ (ks : List Int) (rows : List Row),
    ks.Pairwise (· ≤ ·) → sorted rows →
    seekMany ks rows = ks.map (fun k => rows.find? fun r => r.1 == k)
  | [], _, _, _ => by simp [seekMany]
  | k :: ks, rows, hks, hs => by
      have htail : ∀ k' ∈ ks, k ≤ k' := (List.pairwise_cons.mp hks).1
      have hks' : ks.Pairwise (· ≤ ·) := List.Pairwise.of_cons hks
      let rest := rows.dropWhile (fun r => r.1 < k)
      have hsrest : sorted rest := by simpa [rest] using sorted_dropWhile (fun r : Row => r.1 < k) rows hs
      have hfirst : rest.head?.filter (fun r => r.1 == k) = rows.find? (fun r => r.1 == k) := by
        simpa [rest] using find_head_dropWhile k rows hs
      have hmap : ks.map (fun k' => rest.find? (fun r => r.1 == k')) =
          ks.map (fun k' => rows.find? (fun r => r.1 == k')) := by
        apply List.map_congr_left
        intro k' hk'
        have hle : k ≤ k' := htail k' hk'
        have h1 := find_head_dropWhile k' rest hsrest
        have h2 := find_head_dropWhile k' rows hs
        rw [← h1, ← h2]
        have hchain : rest.dropWhile (fun r => r.1 < k') = rows.dropWhile (fun r => r.1 < k') := by
          simpa [rest] using dropWhile_chain k k' hle rows
        rw [hchain]
      simp [seekMany, rest, hfirst, hmap, seekMany_aux ks rest hks' hsrest]

theorem MonotoneProbes_sound : TinylambOpt.ResearchGoals.OrderedStreams.MonotoneProbes.Goal := by
  intro ks rows hks hrows
  exact seekMany_aux ks rows hks hrows



private def flatPair (a b : List Nat) : List (Nat × Nat) :=
  a.flatMap fun x => b.map fun y => (x, y)

private def flatRuns (rs : Runs) : List (Nat × Nat) :=
  rs.flatMap fun (a, b) => flatPair a b

private theorem flatPair_length (a b : List Nat) : (flatPair a b).length = a.length * b.length := by
  induction a with
  | nil => simp [flatPair]
  | cons x xs ih =>
      change (b.map (fun y => (x, y)) ++ flatPair xs b).length = (x :: xs).length * b.length
      rw [List.length_append, ih]
      simp [Nat.succ_mul]
      omega

private theorem flatPair_nil (a : List Nat) : flatPair a [] = [] := by
  induction a with
  | nil => rfl
  | cons x xs ih => simp [flatPair, ih]

private theorem flatProd_get : ∀ (a b : List Nat) (k : Nat), b.length > 0 →
    (flatPair a b)[k]? =
      (do let x ← a[k / b.length]?
          let y ← b[k % b.length]?
          some (x, y))
  | [], b, k, hb => by simp [flatPair]
  | x :: xs, b, k, hb => by
      simp only [flatPair, List.flatMap_cons]
      by_cases hk : k < b.length
      · rw [List.getElem?_append_left (by simpa using hk)]
        have hdiv : k / b.length = 0 := Nat.div_eq_of_lt hk
        have hmod : k % b.length = k := Nat.mod_eq_of_lt hk
        rw [hdiv, hmod]
        simp
        cases h : b[k]? <;> simp [h]
      · rw [List.getElem?_append_right (by simpa using hk)]
        have hkdecomp : k = (k - b.length) + b.length * 1 := by omega
        have hdiv : k / b.length = (k - b.length) / b.length + 1 := by
          calc
            k / b.length = ((k - b.length) + b.length * 1) / b.length := by rw [← hkdecomp]
            _ = (k - b.length) / b.length + 1 := by simpa using (Nat.add_mul_div_left (k - b.length) 1 hb)
        have hmod : k % b.length = (k - b.length) % b.length := by
          calc
            k % b.length = ((k - b.length) + b.length * 1) % b.length := by rw [← hkdecomp]
            _ = (k - b.length) % b.length := Nat.add_mul_mod_self_left (k - b.length) b.length 1
        have hgetx : (x :: xs)[k / b.length]? = xs[(k - b.length) / b.length]? := by
          rw [hdiv]
          simp
        have hgetb : b[k % b.length]? = b[(k - b.length) % b.length]? := by rw [hmod]
        rw [hgetx, hgetb]
        simpa [flatPair] using flatProd_get xs b (k - b.length) hb

private theorem seekPair_eq_flat : ∀ (rs : Runs) (k : Nat),
    seekPair rs k = (flatRuns rs)[k]?
  | [], k => by simp [seekPair, flatRuns]
  | (a, b) :: rs, k => by
      cases b with
      | nil =>
          simp [seekPair, flatRuns, flatPair_nil, seekPair_eq_flat rs k]
      | cons b bs =>
          change seekPair ((a, b :: bs) :: rs) k = (flatPair a (b :: bs) ++ flatRuns rs)[k]?
          have hb : (b :: bs).length > 0 := by simp
          by_cases hk : k < a.length * (b :: bs).length
          · rw [seekPair, if_pos hk]
            rw [List.getElem?_append_left]
            · simpa using (flatProd_get a (b :: bs) k hb).symm
            · simpa [flatPair_length] using hk
          · rw [seekPair, if_neg hk]
            rw [List.getElem?_append_right]
            · rw [seekPair_eq_flat rs (k - a.length * (b :: bs).length)]
              congr 1
              rw [flatPair_length]
            · simpa [flatPair_length] using hk

theorem WeightedOffset_sound : TinylambOpt.ResearchGoals.OrderedStreams.WeightedOffset.Goal := by
  intro rs k
  simpa [flatRuns, flatPair] using seekPair_eq_flat rs k



private theorem le_of_mem_getLast {l : List Row} (hs : sorted l) :
    ∀ x ∈ l, ∀ z ∈ l.getLast?, x.1 ≤ z.1 := by
  induction l with
  | nil => simp
  | cons a l ih =>
      cases l with
      | nil => simp
      | cons b l =>
          have hs' : sorted (b :: l) := List.Pairwise.of_cons hs
          have hab : a.1 ≤ b.1 := (List.pairwise_cons.mp hs).1 b (by simp)
          intro x hx z hz
          simp at hx
          rcases hx with rfl | hx
          · have hbz : b.1 ≤ z.1 := by
              simpa using ih hs' b (by simp) z hz
            omega
          · exact ih hs' x (by simp [hx]) z hz

private theorem splice_aux : ∀ (block tail right : List Row),
    sorted block → sorted right →
    (∀ x ∈ block.getLast?, ∀ y ∈ right.head?, x.1 ≤ y.1) →
    List.merge (block ++ tail) right leKey = block ++ List.merge tail right leKey
  | [], tail, right, _, _, _ => by simp [List.merge]
  | x :: xs, tail, [], hs, _, _ => by simp [List.merge]
  | x :: xs, tail, y :: ys, hs, hr, hcond => by
      have hxs : sorted xs := List.Pairwise.of_cons hs
      have hyhead : y ∈ (y :: ys).head? := by simp [List.head?]
      have hxy : x.1 ≤ y.1 := by
        cases xs with
        | nil =>
            exact hcond x (by simp) y hyhead
        | cons x' xs' =>
            have hs' : sorted (x' :: xs') := List.Pairwise.of_cons hs
            have hle_x_last : ∀ z ∈ (x' :: xs').getLast?, x.1 ≤ z.1 := by
              intro z hz
              have hab : x.1 ≤ x'.1 := (List.pairwise_cons.mp hs).1 x' (by simp)
              have hxz : x'.1 ≤ z.1 := le_of_mem_getLast hs' x' (by simp) z hz
              omega
            rcases hlast : (x' :: xs').getLast? with _ | z
            · simp at hlast
            · have hzmem : z ∈ (x :: x' :: xs').getLast? := by simpa [hlast]
              have h1 := hle_x_last z (by simpa using hzmem)
              have h2 := hcond z hzmem y hyhead
              omega
      have hcond' : ∀ z ∈ xs.getLast?, ∀ w ∈ (y :: ys).head?, z.1 ≤ w.1 := by
        intro z hz w hw
        have hz' : z ∈ (x :: xs).getLast? := by
          cases xs with
          | nil => simp at hz
          | cons x' xs' => simpa using hz
        exact hcond z hz' w hw
      simp [List.merge, leKey, hxy, splice_aux xs tail (y :: ys) hxs hr hcond']

theorem SpliceBlock_sound : TinylambOpt.ResearchGoals.OrderedStreams.SpliceBlock.Goal := by
  intro block tail right hs hr h
  exact splice_aux block tail right hs hr h



private theorem expand_cons (key n : Nat) (ps : Rle) :
    expand ((key, n) :: ps) = List.replicate n key ++ expand ps := rfl

private theorem not_mem_expand_of_lt : ∀ {xs : Rle} {x : Nat},
    (∀ p ∈ xs, x < p.1) → x ∉ expand xs
  | [], x, hx, hmem => by simp [expand] at hmem
  | (key, n) :: ps, x, hx, hmem => by
      have hxkey : x < key := hx (key, n) (by simp)
      have htail : ∀ p ∈ ps, x < p.1 := by intro p hp; exact hx p (by simp [hp])
      rw [expand_cons, List.mem_append] at hmem
      rcases hmem with hrep | hmem
      · have hxk : x = key := (List.mem_replicate.mp hrep).2
        omega
      · exact not_mem_expand_of_lt htail hmem

private theorem intersect_count_aux : ∀ (fuel : Nat) (a b : Rle),
    (a.map Prod.fst).Pairwise (· < ·) → (b.map Prod.fst).Pairwise (· < ·) →
    a.length + b.length ≤ fuel →
    ∀ k, (expand (intersectRuns fuel a b)).count k =
      min ((expand a).count k) ((expand b).count k)
  | 0, a, b, ha, hb, hfuel, k => by
      have hla : a.length = 0 := by omega
      have hlb : b.length = 0 := by omega
      have hae : a = [] := List.length_eq_zero_iff.mp hla
      have hbe : b = [] := List.length_eq_zero_iff.mp hlb
      subst a; subst b
      simp [intersectRuns, expand]
  | fuel + 1, [], b, ha, hb, hfuel, k => by
      cases b <;> simp [intersectRuns, expand]
  | fuel + 1, a, [], ha, hb, hfuel, k => by
      cases a <;> simp [intersectRuns, expand, Nat.min_eq_left]
  | fuel + 1, (x, n) :: xs, (y, m) :: ys, ha, hb, hfuel, k => by
      have ha' : (xs.map Prod.fst).Pairwise (· < ·) := List.Pairwise.of_cons ha
      have hb' : (ys.map Prod.fst).Pairwise (· < ·) := List.Pairwise.of_cons hb
      have hax_gt : ∀ p ∈ xs, x < p.1 := by
        intro p hp
        exact (List.pairwise_cons.mp ha).1 p.1 (List.mem_map.mpr ⟨p, hp, rfl⟩)
      have hby_gt : ∀ p ∈ ys, y < p.1 := by
        intro p hp
        exact (List.pairwise_cons.mp hb).1 p.1 (List.mem_map.mpr ⟨p, hp, rfl⟩)
      simp at hfuel
      by_cases hxy : x < y
      · have hb_notx : x ∉ expand ((y, m) :: ys) := by
          apply not_mem_expand_of_lt
          intro p hp
          simp at hp
          rcases hp with rfl | hp
          · exact hxy
          · have := hby_gt p hp
            omega
        have hb_countx : (expand ((y, m) :: ys)).count x = 0 := List.count_eq_zero_of_not_mem hb_notx
        have hxs_countx : (expand xs).count x = 0 := List.count_eq_zero_of_not_mem (not_mem_expand_of_lt hax_gt)
        have hfuel' : xs.length + ((y, m) :: ys).length ≤ fuel := by simp; omega
        have ih := intersect_count_aux fuel xs ((y, m) :: ys) ha' hb hfuel'
        by_cases hk : k = x
        · subst k
          have houtx : (expand (intersectRuns fuel xs ((y, m) :: ys))).count x = 0 := by
            rw [ih, hxs_countx, hb_countx]
            simp
          have hyx_ne : y ≠ x := by omega
          rw [expand_cons, List.count_append] at hb_countx
          simp [intersectRuns, hxy, expand_cons, List.count_append, houtx, hxs_countx, hb_countx]
        · have ha_count : (expand ((x, n) :: xs)).count k = (expand xs).count k := by
            rw [expand_cons, List.count_append]
            have hxk : x ≠ k := by omega
            have hbeq : (x == k) = false := by simp [hxk]
            simp [List.count_replicate, hbeq]
          simp [intersectRuns, hxy]
          rw [ih, ha_count]
      · by_cases hyx : y < x
        · have ha_noty : y ∉ expand ((x, n) :: xs) := by
            apply not_mem_expand_of_lt
            intro p hp
            simp at hp
            rcases hp with rfl | hp
            · exact hyx
            · have := hax_gt p hp
              omega
          have ha_county : (expand ((x, n) :: xs)).count y = 0 := List.count_eq_zero_of_not_mem ha_noty
          have hys_county : (expand ys).count y = 0 := List.count_eq_zero_of_not_mem (not_mem_expand_of_lt hby_gt)
          have hfuel' : ((x, n) :: xs).length + ys.length ≤ fuel := by simp; omega
          have ih := intersect_count_aux fuel ((x, n) :: xs) ys ha hb' hfuel'
          by_cases hk : k = y
          · subst k
            have houty : (expand (intersectRuns fuel ((x, n) :: xs) ys)).count y = 0 := by
              rw [ih, ha_county, hys_county]
              simp
            have hxy_ne : x ≠ y := by omega
            rw [expand_cons, List.count_append] at ha_county
            simp [intersectRuns, hxy, hyx, expand_cons, List.count_append, houty, ha_county, hys_county]
          · have hb_count : (expand ((y, m) :: ys)).count k = (expand ys).count k := by
              rw [expand_cons, List.count_append]
              have hyk : y ≠ k := by omega
              have hbeq : (y == k) = false := by simp [hyk]
              simp [List.count_replicate, hbeq]
            simp [intersectRuns, hxy, hyx]
            rw [ih, hb_count]
        · have hxy_eq : x = y := by omega
          subst y
          have hxs_countx : (expand xs).count x = 0 := List.count_eq_zero_of_not_mem (not_mem_expand_of_lt hax_gt)
          have hys_countx : (expand ys).count x = 0 := List.count_eq_zero_of_not_mem (not_mem_expand_of_lt hby_gt)
          have hfuel' : xs.length + ys.length ≤ fuel := by omega
          have ih := intersect_count_aux fuel xs ys ha' hb' hfuel'
          by_cases hk : k = x
          · subst k
            have houtx : (expand (intersectRuns fuel xs ys)).count x = 0 := by
              rw [ih, hxs_countx, hys_countx]
              simp
            have hir : intersectRuns (fuel + 1) ((x, n) :: xs) ((x, m) :: ys) =
                (x, min n m) :: intersectRuns fuel xs ys := by
              simp [intersectRuns, hxy, hyx]
            rw [hir]
            change (expand ((x, min n m) :: intersectRuns fuel xs ys)).count x =
              min ((expand ((x, n) :: xs)).count x) ((expand ((x, m) :: ys)).count x)
            simp [expand_cons, List.count_append, houtx, hxs_countx, hys_countx]
          · have hxk : x ≠ k := by omega
            have hbeq : (x == k) = false := by simp [hxk]
            have ha_count : (expand ((x, n) :: xs)).count k = (expand xs).count k := by
              rw [expand_cons, List.count_append]
              simp [List.count_replicate, hbeq]
            have hb_count : (expand ((x, m) :: ys)).count k = (expand ys).count k := by
              rw [expand_cons, List.count_append]
              simp [List.count_replicate, hbeq]
            have hir : intersectRuns (fuel + 1) ((x, n) :: xs) ((x, m) :: ys) =
                (x, min n m) :: intersectRuns fuel xs ys := by
              simp [intersectRuns, hxy, hyx]
            rw [hir]
            change (expand ((x, min n m) :: intersectRuns fuel xs ys)).count k =
              min ((expand ((x, n) :: xs)).count k) ((expand ((x, m) :: ys)).count k)
            rw [expand_cons, List.count_append, ih, ha_count, hb_count]
            simp [List.count_replicate, hbeq]

theorem RunIntersection_sound : TinylambOpt.ResearchGoals.OrderedStreams.RunIntersection.Goal := by
  intro a b ha hb k
  exact intersect_count_aux (a.length + b.length) a b ha hb (by omega) k



private def stepIsland (g : Nat) (acc : List (Nat × Nat)) (x : Nat) : List (Nat × Nat) :=
  match acc with
  | [] => [(x, x)]
  | (l, r) :: rest => if x ≤ r + g + 1 then (l, x) :: rest else (x, x) :: acc

private def AccOK (g bound : Nat) (acc : List (Nat × Nat)) : Prop :=
  acc.Pairwise (fun a b => b.2 + g + 1 < a.1) ∧
  ∀ s ∈ acc, s.1 ≤ s.2 ∧ s.2 ≤ bound

private theorem accOK_nil (g bound : Nat) : AccOK g bound [] := by simp [AccOK]

private theorem getD_getLast_eq : ∀ (l : List Nat) (a b : Nat), l ≠ [] →
    l.getLast?.getD a = l.getLast?.getD b
  | [], a, b, h => by contradiction
  | [x], a, b, h => by simp
  | x :: y :: ys, a, b, h => by
      simpa using getD_getLast_eq (y :: ys) a b (by simp)

private theorem accOK_step (g : Nat) (xs : List Nat) (bound : Nat)
    (hxs : xs.Pairwise (· ≤ ·)) (hge : ∀ x ∈ xs, bound ≤ x)
    (acc : List (Nat × Nat)) (hacc : AccOK g bound acc) :
    AccOK g (xs.getLast?.getD bound) (xs.foldl (stepIsland g) acc) := by
  induction xs generalizing acc bound with
  | nil => simpa using hacc
  | cons x xs ih =>
      have hxge : bound ≤ x := hge x (by simp)
      have hxs' : xs.Pairwise (· ≤ ·) := List.Pairwise.of_cons hxs
      have hge' : ∀ y ∈ xs, x ≤ y := by
        intro y hy
        exact (List.pairwise_cons.mp hxs).1 y hy
      have hstep : AccOK g x (stepIsland g acc x) := by
        unfold AccOK at hacc ⊢
        cases acc with
        | nil => simp [stepIsland]
        | cons s rest =>
            rcases s with ⟨l, r⟩
            have hlrs : l ≤ r := (hacc.2 (l, r) (by simp)).1
            have hrsb : r ≤ bound := (hacc.2 (l, r) (by simp)).2
            have hrlx : r ≤ x := by omega
            have hrestcross : ∀ t ∈ rest, t.2 + g + 1 < l := (List.pairwise_cons.mp hacc.1).1
            by_cases hcond : x ≤ r + g + 1
            · rw [stepIsland, if_pos hcond]
              constructor
              · exact List.Pairwise.cons (by intro t ht; exact hrestcross t ht) hacc.1.tail
              · intro s hs
                simp only [List.mem_cons] at hs
                rcases hs with rfl | hs
                · exact ⟨by omega, by simp⟩
                · have h := hacc.2 s (by simp [hs])
                  exact ⟨h.1, by omega⟩
            · rw [stepIsland, if_neg hcond]
              constructor
              · apply List.Pairwise.cons
                · intro t ht
                  simp only [List.mem_cons] at ht
                  rcases ht with rfl | ht
                  · omega
                  · have := hrestcross t ht
                    omega
                · exact hacc.1
              · intro s hs
                simp only [List.mem_cons] at hs
                rcases hs with rfl | rfl | hs
                · simp
                · exact ⟨hlrs, by omega⟩
                · have h := hacc.2 s (by simp [hs])
                  exact ⟨h.1, by omega⟩
      have hres := ih x hxs' hge' (stepIsland g acc x) hstep
      cases xs with
      | nil => simpa using hres
      | cons y ys =>
          cases ys with
          | nil => simpa using hres
          | cons z zs =>
              have hb : (z :: zs).getLast?.getD bound = (z :: zs).getLast?.getD x :=
                getD_getLast_eq (z :: zs) bound x (by simp)
              simpa [hb] using hres

private theorem islands_pairwise (xs : List Nat) (g : Nat) (hxs : xs.Pairwise (· ≤ ·)) :
    (islands xs g).Pairwise (fun a b => a.2 + g + 1 < b.1) := by
  have hacc := accOK_step g xs 0 hxs (by simp) [] (accOK_nil g 0)
  change ((xs.foldl (stepIsland g) []).reverse).Pairwise (fun a b => a.2 + g + 1 < b.1)
  rw [List.pairwise_reverse]
  simpa using hacc.1

private theorem islands_ok (xs : List Nat) (g : Nat) (hxs : xs.Pairwise (· ≤ ·)) :
    (islands xs g).Pairwise (fun a b => a.2 + g + 1 < b.1) ∧
    ∀ s ∈ islands xs g, s.1 ≤ s.2 := by
  have hacc := accOK_step g xs 0 hxs (by simp) [] (accOK_nil g 0)
  constructor
  · change ((xs.foldl (stepIsland g) []).reverse).Pairwise (fun a b => a.2 + g + 1 < b.1)
    rw [List.pairwise_reverse]
    simpa using hacc.1
  · intro s hs
    have hs' : s ∈ xs.foldl (stepIsland g) [] := by
      change s ∈ (xs.foldl (stepIsland g) []).reverse at hs
      simpa using hs
    exact (hacc.2 s hs').1

private theorem mem_step_nil (g x : Nat) : (x, x) ∈ stepIsland g [] x := by simp [stepIsland]

private theorem mem_step_extend (g l r x : Nat) (rest : List (Nat × Nat))
    (hcond : x ≤ r + g + 1) : (l, x) ∈ stepIsland g ((l, r) :: rest) x := by simp [stepIsland, hcond]

private theorem mem_step_keep (g l r x : Nat) (rest : List (Nat × Nat))
    (hcond : ¬ x ≤ r + g + 1) : (l, r) ∈ stepIsland g ((l, r) :: rest) x := by simp [stepIsland, hcond]

private theorem mem_step_new (g l r x : Nat) (rest : List (Nat × Nat))
    (hcond : ¬ x ≤ r + g + 1) : (x, x) ∈ stepIsland g ((l, r) :: rest) x := by simp [stepIsland, hcond]

private theorem mem_step_rest (g l r x : Nat) (rest : List (Nat × Nat)) (s : Nat × Nat)
    (hs : s ∈ rest) : s ∈ stepIsland g ((l, r) :: rest) x := by
  by_cases hcond : x ≤ r + g + 1
  · simp [stepIsland, hcond, hs]
  · simp [stepIsland, hcond, hs]

private theorem cover_persist (g : Nat) : ∀ (xs : List Nat) (bound : Nat) (acc : List (Nat × Nat)),
    xs.Pairwise (· ≤ ·) → (∀ x ∈ xs, bound ≤ x) → AccOK g bound acc →
    ∀ s ∈ acc, ∀ z, s.1 ≤ z ∧ z ≤ s.2 →
      ∃ t ∈ xs.foldl (stepIsland g) acc, t.1 ≤ z ∧ z ≤ t.2
  | [], bound, acc, hxs, hge, hacc, s, hs, z, hz => ⟨s, by simpa using hs, hz⟩
  | y :: ys, bound, acc, hxs, hge, hacc, s, hs, z, hz => by
      have hyge : bound ≤ y := hge y (by simp)
      have hge' : ∀ w ∈ ys, y ≤ w := by intro w hw; exact (List.pairwise_cons.mp hxs).1 w hw
      have hstep : AccOK g y (stepIsland g acc y) := by
        have hres := accOK_step g [y] bound (by simp)
          (fun w hw => hge w (by simp [show w = y from by simpa using hw])) acc hacc
        simpa using hres
      have hs' : ∃ t ∈ stepIsland g acc y, t.1 ≤ z ∧ z ≤ t.2 := by
        cases acc with
        | nil => simp at hs
        | cons q rest =>
            rcases q with ⟨l, r⟩
            have hlrs : l ≤ r := (hacc.2 (l, r) (by simp)).1
            have hrsb : r ≤ bound := (hacc.2 (l, r) (by simp)).2
            simp only [List.mem_cons] at hs
            rcases hs with hsr | hsrest
            · cases hsr
              by_cases hcond : y ≤ r + g + 1
              · exact ⟨(l, y), mem_step_extend g l r y rest hcond, by omega⟩
              · exact ⟨(l, r), mem_step_keep g l r y rest hcond, hz⟩
            · exact ⟨s, mem_step_rest g l r y rest s hsrest, hz⟩
      rcases hs' with ⟨t, ht, htz⟩
      rcases cover_persist g ys y (stepIsland g acc y) (List.Pairwise.of_cons hxs) hge' hstep t ht z htz with ⟨u, hu, huz⟩
      exact ⟨u, by simpa using hu, huz⟩

private theorem cover_foldl (g : Nat) : ∀ (xs : List Nat) (bound : Nat) (acc : List (Nat × Nat)),
    xs.Pairwise (· ≤ ·) → (∀ x ∈ xs, bound ≤ x) → AccOK g bound acc →
    ∀ x ∈ xs, ∃ s ∈ xs.foldl (stepIsland g) acc, s.1 ≤ x ∧ x ≤ s.2
  | [], bound, acc, hxs, hge, hacc, x, hx => by simp at hx
  | y :: ys, bound, acc, hxs, hge, hacc, x, hx => by
      have hyge : bound ≤ y := hge y (by simp)
      have hge' : ∀ z ∈ ys, y ≤ z := by intro z hz; exact (List.pairwise_cons.mp hxs).1 z hz
      have hstep : AccOK g y (stepIsland g acc y) := by
        have hres := accOK_step g [y] bound (by simp)
          (fun z hz => hge z (by simp [show z = y from by simpa using hz])) acc hacc
        simpa using hres
      have ihres := cover_foldl g ys y (stepIsland g acc y) (List.Pairwise.of_cons hxs) hge' hstep
      simp only [List.mem_cons] at hx
      rcases hx with hxy | hx
      · subst x
        have first : ∃ t ∈ stepIsland g acc y, t.1 ≤ y ∧ y ≤ t.2 := by
          cases acc with
          | nil => exact ⟨(y, y), mem_step_nil g y, by simp⟩
          | cons q rest =>
              rcases q with ⟨l, r⟩
              have hlrs : l ≤ r := (hacc.2 (l, r) (by simp)).1
              have hrsb : r ≤ bound := (hacc.2 (l, r) (by simp)).2
              by_cases hcond : y ≤ r + g + 1
              · exact ⟨(l, y), mem_step_extend g l r y rest hcond, by omega⟩
              · exact ⟨(y, y), mem_step_new g l r y rest hcond, by simp⟩
        rcases first with ⟨t, ht, htz⟩
        rcases cover_persist g ys y (stepIsland g acc y) (List.Pairwise.of_cons hxs) hge' hstep t ht y htz with ⟨u, hu, huz⟩
        exact ⟨u, by simpa using hu, huz⟩
      · rcases ihres x hx with ⟨s, hs, hsle⟩
        exact ⟨s, by simpa using hs, hsle⟩

private theorem cover_islands (xs : List Nat) (g : Nat) (hxs : xs.Pairwise (· ≤ ·)) :
    ∀ x ∈ xs, ∃ s ∈ islands xs g, s.1 ≤ x ∧ x ≤ s.2 := by
  intro x hx
  rcases cover_foldl g xs 0 [] hxs (by simp) (accOK_nil g 0) x hx with ⟨s, hs, hsle⟩
  exact ⟨s, by change s ∈ (xs.foldl (stepIsland g) []).reverse; simpa using hs, hsle⟩

private theorem disjoint_pairwise (g : Nat) (spans : List (Nat × Nat))
    (hp : spans.Pairwise (fun a b => a.2 + g + 1 < b.1)) :
    ∀ s ∈ spans, ∀ t ∈ spans, s ≠ t → s.2 < t.1 ∨ t.2 < s.1 := by
  induction spans with
  | nil => simp
  | cons u us ih =>
      intro s hs t ht hne
      have hp_tail : us.Pairwise (fun a b => a.2 + g + 1 < b.1) := hp.tail
      have hcross : ∀ v ∈ us, u.2 + g + 1 < v.1 := (List.pairwise_cons.mp hp).1
      simp only [List.mem_cons] at hs ht
      rcases hs with rfl | hs
      · rcases ht with rfl | ht
        · contradiction
        · left
          have := hcross t ht
          omega
      · rcases ht with rfl | ht
        · right
          have := hcross s hs
          omega
        · exact ih hp_tail s hs t ht hne

private theorem count_filter_eq (v : Nat) (p : Nat → Bool) (rows : List Nat) :
    (rows.filter p).count v = if p v = true then rows.count v else 0 := by
  by_cases hp : p v = true
  · simp [hp, List.count_filter hp]
  · have hnot : v ∉ rows.filter p := by simp [List.mem_filter, hp]
    simp [hp, List.count_eq_zero_of_not_mem hnot]

private theorem sum_zero_map {α : Type} (xs : List α) : (xs.map fun _ => (0 : Nat)).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simpa using ih

private theorem sum_unique_span (spans : List (Nat × Nat)) (v c : Nat)
    (hnd : spans.Nodup)
    (huniq : ∃ s ∈ spans, s.1 ≤ v ∧ v ≤ s.2 ∧
      ∀ t ∈ spans, t.1 ≤ v ∧ v ≤ t.2 → t = s) :
    (spans.map fun s => if (s.1 ≤ v && v ≤ s.2) = true then c else 0).sum = c := by
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  induction spans with
  | nil => rcases huniq with ⟨s, hs, _⟩; simp at hs
  | cons s rest ih =>
      have hnd_rest : rest.Nodup := (List.nodup_cons.mp hnd).2
      have hs_not_rest : s ∉ rest := (List.nodup_cons.mp hnd).1
      by_cases hs : s.1 ≤ v ∧ v ≤ s.2
      · rcases huniq with ⟨u, hu, hule, huv, hu_unique⟩
        have hus : u = s := (hu_unique s (by simp) hs).symm
        have hzero : ∀ t ∈ rest, (if t.1 ≤ v ∧ v ≤ t.2 then c else 0) = 0 := by
          intro t ht
          have htcond : ¬ (t.1 ≤ v ∧ v ≤ t.2) := by
            intro htc
            have htu : t = u := hu_unique t (by simp [ht]) htc
            have hts : t ≠ s := by
              intro h
              have hsrest : s ∈ rest := by simpa [h] using ht
              exact hs_not_rest hsrest
            exact hts (htu.trans hus)
          simp [htcond]
        have hmap : (rest.map fun t => if t.1 ≤ v ∧ v ≤ t.2 then c else 0) =
            rest.map fun _ => (0 : Nat) := by
          apply List.map_congr_left
          intro t ht
          exact hzero t ht
        rw [List.map_cons, List.sum_cons, hmap, sum_zero_map]
        simp [hs]
      · rcases huniq with ⟨u, hu, hule, huv, hu_unique⟩
        have hune : u ≠ s := by
          intro h
          subst u
          exact hs ⟨hule, huv⟩
        have hurest : u ∈ rest := by
          cases hu with
          | head => exact (hune rfl).elim
          | tail _ ht => exact ht
        have hu' : ∃ u ∈ rest, u.1 ≤ v ∧ v ≤ u.2 ∧
            ∀ t ∈ rest, t.1 ≤ v ∧ v ≤ t.2 → t = u := by
          refine ⟨u, hurest, hule, huv, ?_⟩
          intro t ht htc
          exact hu_unique t (by simp [ht]) htc
        have hrec := ih hnd_rest hu'
        rw [List.map_cons, List.sum_cons]
        simp [hs, hrec]

private theorem perm_contains_of_perm {l₁ l₂ : List Nat} (h : l₁.Perm l₂) (x : Nat) :
    l₁.contains x = l₂.contains x := h.contains_eq

private theorem sorted_mergeSort_nat (xs : List Nat) : xs.mergeSort.Pairwise (· ≤ ·) := by
  have h := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
    (by
      intro a b c hab hbc
      apply decide_eq_true
      have hab' : a ≤ b := of_decide_eq_true hab
      have hbc' : b ≤ c := of_decide_eq_true hbc
      omega)
    (by
      intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have h' : b ≤ a := by omega
        simp [h, h'])
    xs
  exact h.imp (by intro a b hab; exact of_decide_eq_true hab)

theorem RangeCoalescing_sound : TinylambOpt.ResearchGoals.OrderedStreams.RangeCoalescing.Goal := by
  intro keys g rows
  let ks := keys.mergeSort
  let spans := islands ks g
  have hks : ks.Pairwise (· ≤ ·) := sorted_mergeSort_nat keys
  have hok := islands_ok ks g hks
  have hpairs : spans.Pairwise (fun a b => a.2 + g + 1 < b.1) := by
    simpa [spans] using hok.1
  have hintervals : ∀ s ∈ spans, s.1 ≤ s.2 := by
    intro s hs
    exact hok.2 s (by simpa [spans] using hs)
  have hcover := cover_islands ks g hks
  have hdisj := disjoint_pairwise g spans hpairs
  have hnd_aux : ∀ (sp : List (Nat × Nat)),
      sp.Pairwise (fun a b => a.2 + g + 1 < b.1) →
      (∀ s ∈ sp, s.1 ≤ s.2) → sp.Nodup := by
    intro sp hpair hint
    induction sp with
    | nil => simp
    | cons u us ih =>
        have hu := hint u (by simp)
        have hcross : ∀ t ∈ us, u.2 + g + 1 < t.1 := (List.pairwise_cons.mp hpair).1
        have hint_tail : ∀ s ∈ us, s.1 ≤ s.2 := by intro s hs; exact hint s (by simp [hs])
        have hus : us.Nodup := ih (List.Pairwise.of_cons hpair) hint_tail
        rw [List.nodup_cons]
        constructor
        · intro huin
          have := hcross u huin
          omega
        · exact hus
  have hnd : spans.Nodup := hnd_aux spans hpairs hintervals
  constructor
  · rw [List.perm_iff_count]
    intro v
    by_cases hv : keys.contains v = true
    · have hvMem : v ∈ keys := by simpa using hv
      have hvks : ks.contains v = true := by
        have hp : ks.Perm keys := by simpa [ks] using List.mergeSort_perm keys (fun a b : Nat => a ≤ b)
        rw [perm_contains_of_perm hp v]
        exact hv
      have hvksmem : v ∈ ks := by simpa using hvks
      rcases hcover v hvksmem with ⟨s, hs, hsle, hsle2⟩
      have huniq : ∃ s ∈ spans, s.1 ≤ v ∧ v ≤ s.2 ∧
          ∀ t ∈ spans, t.1 ≤ v ∧ v ≤ t.2 → t = s := by
        refine ⟨s, hs, hsle, hsle2, ?_⟩
        intro t ht htle
        by_cases hts : t = s
        · exact hts
        · rcases hdisj s hs t ht (fun h => hts h.symm) with hlt | hgt
          · have hv1 : v ≤ s.2 := hsle2
            have hv2 : t.1 ≤ v := htle.1
            omega
          · have hv1 : v ≤ t.2 := htle.2
            have hv2 : s.1 ≤ v := hsle
            omega
      have hsum := sum_unique_span spans v (rows.count v) hnd huniq
      have hflat : (spans.flatMap fun (lo, hi) => rows.filter fun x => lo ≤ x && x ≤ hi && keys.contains x).count v
          = (spans.map fun t => (rows.filter fun x => t.1 ≤ x && x ≤ t.2 && keys.contains x).count v).sum := by
        exact List.count_flatMap
      rw [hflat]
      have hmap : (spans.map fun t => (rows.filter fun x => t.1 ≤ x && x ≤ t.2 && keys.contains x).count v) =
          spans.map fun t => if (t.1 ≤ v && v ≤ t.2 && keys.contains v) = true then rows.count v else 0 := by
        apply List.map_congr_left
        intro t ht
        exact count_filter_eq v (fun x => t.1 ≤ x && x ≤ t.2 && keys.contains x) rows
      rw [hmap]
      have hright : (rows.filter keys.contains).count v = rows.count v := by
        simpa [hvMem] using count_filter_eq v keys.contains rows
      rw [hright]
      simpa [hvMem] using hsum
    · have hv' : keys.contains v = false := by cases h : keys.contains v <;> simp_all
      have hvNotMem : v ∉ keys := by simpa using hv'
      have hflat : (spans.flatMap fun (lo, hi) => rows.filter fun x => lo ≤ x && x ≤ hi && keys.contains x).count v
          = (spans.map fun t => (rows.filter fun x => t.1 ≤ x && x ≤ t.2 && keys.contains x).count v).sum := by
        exact List.count_flatMap
      rw [hflat]
      have hmap : (spans.map fun t => (rows.filter fun x => t.1 ≤ x && x ≤ t.2 && keys.contains x).count v) =
          spans.map fun _ => (0 : Nat) := by
        apply List.map_congr_left
        intro t ht
        have hc := count_filter_eq v (fun x => t.1 ≤ x && x ≤ t.2 && keys.contains x) rows
        simpa [hvNotMem] using hc
      rw [hmap, sum_zero_map]
      have hright : (rows.filter keys.contains).count v = 0 := by
        simpa [hvNotMem] using count_filter_eq v keys.contains rows
      rw [hright]
  · simpa [spans] using hpairs



private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) {xs : List α}
    (h : ∀ x ∈ xs, p x = false) : xs.filter p = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ z ∈ xs, p z = false := by intro z hz; exact h z (by simp [hz])
      simp [List.filter_cons, hx, ih ht]

private theorem takeWhile_len_eq_filter_len (xs : List Nat) (x : Nat)
    (hs : xs.Pairwise (· ≤ ·)) :
    (xs.takeWhile (fun y : Nat => y < x)).length = (xs.filter (fun y : Nat => y < x)).length := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      by_cases ha : a < x
      · simpa [ha] using ih (List.Pairwise.of_cons hs)
      · have hnot : ∀ b ∈ xs, ¬ b < x := by
          intro b hb
          have hab : a ≤ b := (List.pairwise_cons.mp hs).1 b hb
          omega
        have hfilter : xs.filter (fun y : Nat => y < x) = [] := by
          apply filter_eq_nil_of_all_false
          intro b hb
          exact decide_eq_false (hnot b hb)
        simp [ha, hfilter]

private theorem sum_filter_lt (runs : List (List Nat)) (x : Nat) :
    (runs.map fun s => (s.filter fun y : Nat => y < x).length).sum =
      (runs.flatten.filter fun y : Nat => y < x).length := by
  induction runs with
  | nil => rfl
  | cons r rs ih =>
      simp [List.filter_append, ih]

private theorem rank_eq (runs : List (List Nat)) (x : Nat)
    (hr : ∀ r ∈ runs, r.Pairwise (· ≤ ·)) :
    (runs.map fun s => (s.takeWhile fun y : Nat => y < x).length).sum =
      (runs.flatten.filter fun y : Nat => y < x).length := by
  have hall : ∀ r ∈ runs, (r.takeWhile (fun y : Nat => y < x)).length =
      (r.filter (fun y : Nat => y < x)).length := by
    intro r hrp
    exact takeWhile_len_eq_filter_len r x (hr r hrp)
  have hmap : (runs.map fun s => (s.takeWhile fun y : Nat => y < x).length) =
      runs.map fun s => (s.filter fun y : Nat => y < x).length := by
    apply List.map_congr_left
    intro r hrp
    exact hall r hrp
  rw [hmap]
  exact sum_filter_lt runs x

private theorem sorted_filter_range (S : List Nat) (hS : S.Pairwise (· ≤ ·)) (hnd : S.Nodup) :
    ∀ lo size,
    S.filter (fun x => decide (let r := (S.filter fun y : Nat => y < x).length;
      lo ≤ r ∧ r < lo + size)) = (S.drop lo).take size := by
  induction S with
  | nil => intro lo size; simp
  | cons a s ih =>
      have hS' : s.Pairwise (· ≤ ·) := List.Pairwise.of_cons hS
      have hnd' : s.Nodup := List.Pairwise.of_cons hnd
      have ha_lt : ∀ b ∈ s, a < b := by
        intro b hb
        have hale : a ≤ b := (List.pairwise_cons.mp hS).1 b hb
        have hane : a ≠ b := by
          intro h
          subst b
          exact (List.nodup_cons.mp hnd).1 hb
        omega
      have hcount_a : (s.filter fun y : Nat => y < a) = [] := by
        apply filter_eq_nil_of_all_false
        intro b hb
        have hge : a ≤ b := (List.pairwise_cons.mp hS).1 b hb
        exact decide_eq_false (by omega)
      have hcount_head : ((a :: s).filter fun y : Nat => y < a).length = 0 := by simp [hcount_a]
      intro lo size
      cases lo with
      | zero =>
          cases size with
          | zero => simp
          | succ k =>
              have hPa : decide (let r := ((a :: s).filter fun y : Nat => y < a).length;
                  0 ≤ r ∧ r < 0 + (k + 1)) = true := by simp [hcount_head, hcount_a]
              have htailpred : ∀ b ∈ s,
                  decide (let r := ((a :: s).filter fun y : Nat => y < b).length;
                    0 ≤ r ∧ r < 0 + (k + 1)) =
                  decide (let r := (s.filter fun y : Nat => y < b).length;
                    0 ≤ r ∧ r < 0 + k) := by
                intro b hb
                have hbcount : ((a :: s).filter fun y : Nat => y < b).length =
                    (s.filter fun y : Nat => y < b).length + 1 := by
                  simp [List.filter_cons, decide_eq_true (ha_lt b hb)]
                rw [hbcount]
                apply Bool.eq_iff_iff.mpr
                constructor
                · intro h
                  have hp := of_decide_eq_true h
                  simp at hp
                  apply decide_eq_true
                  simp
                  omega
                · intro h
                  have hp := of_decide_eq_true h
                  simp at hp
                  apply decide_eq_true
                  simp
                  omega
              have hfilter_tail : s.filter (fun x =>
                    decide (let r := ((a :: s).filter fun y : Nat => y < x).length;
                      0 ≤ r ∧ r < 0 + (k + 1))) =
                  s.filter (fun x =>
                    decide (let r := (s.filter fun y : Nat => y < x).length;
                      0 ≤ r ∧ r < 0 + k)) := by
                apply List.filter_congr
                intro x hx
                exact htailpred x hx
              rw [List.filter_cons, hPa, hfilter_tail]
              have hrec := ih hS' hnd' 0 k
              change a :: s.filter (fun x =>
                decide (let r := (s.filter fun y : Nat => y < x).length; 0 ≤ r ∧ r < 0 + k)) = a :: s.take k
              exact congrArg (fun l => a :: l) hrec
      | succ lo =>
          have hPa : decide (let r := ((a :: s).filter fun y : Nat => y < a).length;
              (lo + 1) ≤ r ∧ r < (lo + 1) + size) = false := by simp [hcount_head, hcount_a]
          have htailpred : ∀ b ∈ s,
              decide (let r := ((a :: s).filter fun y : Nat => y < b).length;
                (lo + 1) ≤ r ∧ r < (lo + 1) + size) =
              decide (let r := (s.filter fun y : Nat => y < b).length;
                lo ≤ r ∧ r < lo + size) := by
            intro b hb
            have hbcount : ((a :: s).filter fun y : Nat => y < b).length =
                (s.filter fun y : Nat => y < b).length + 1 := by
              simp [List.filter_cons, decide_eq_true (ha_lt b hb)]
            rw [hbcount]
            apply Bool.eq_iff_iff.mpr
            constructor
            · intro h
              have hp := of_decide_eq_true h
              simp at hp
              apply decide_eq_true
              simp
              omega
            · intro h
              have hp := of_decide_eq_true h
              simp at hp
              apply decide_eq_true
              simp
              omega
          have hfilter_tail : s.filter (fun x =>
                decide (let r := ((a :: s).filter fun y : Nat => y < x).length;
                  (lo + 1) ≤ r ∧ r < (lo + 1) + size)) =
              s.filter (fun x =>
                decide (let r := (s.filter fun y : Nat => y < x).length;
                  lo ≤ r ∧ r < lo + size)) := by
            apply List.filter_congr
            intro x hx
            exact htailpred x hx
          rw [List.filter_cons, hPa, hfilter_tail]
          have hrec := ih hS' hnd' lo size
          simpa [List.drop_succ_cons] using hrec

private theorem flatMap_filter_eq_flatten_filter (runs : List (List Nat)) (P : Nat → Bool) :
    runs.flatMap (fun r => r.filter P) = runs.flatten.filter P := by
  induction runs with
  | nil => rfl
  | cons r rs ih => simp [List.filter_append, ih]

private theorem count_lt_of_perm {l₁ l₂ : List Nat} (h : l₁.Perm l₂) (x : Nat) :
    (l₁.filter fun y : Nat => y < x).length = (l₂.filter fun y : Nat => y < x).length := by
  have hc := List.Perm.countP_eq (fun y : Nat => y < x) h
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter] at hc
  exact hc

theorem RankSlice_sound : TinylambOpt.ResearchGoals.OrderedStreams.RankSlice.Goal := by
  intro runs lo size hnd hr
  let flat := runs.flatten
  let S := flat.mergeSort
  have hSsortedP : S.Pairwise (fun a b : Nat => decide (a ≤ b) = true) := by
    have h := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
      (by
        intro a b c hab hbc
        apply decide_eq_true
        have h1 : a ≤ b := of_decide_eq_true hab
        have h2 : b ≤ c := of_decide_eq_true hbc
        omega)
      (by
        intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have h' : b ≤ a := by omega
          simp [h, h'])
      flat
    simpa [S] using h
  have hSsorted : S.Pairwise (· ≤ ·) := hSsortedP.imp (by intro a b h; exact of_decide_eq_true h)
  have hSnd : S.Nodup := by
    have hp := List.mergeSort_perm flat (fun a b : Nat => a ≤ b)
    have hnd' := List.Nodup.perm hnd (by simpa [S] using hp.symm)
    simpa [S, flat] using hnd'
  have hrange := sorted_filter_range S hSsorted hSnd lo size
  have hpermS : S.Perm flat := by simpa [S, flat] using List.mergeSort_perm flat (fun a b : Nat => a ≤ b)
  let selected := runs.flatMap fun r => r.filter fun x =>
    let rank := (runs.map fun s => (s.takeWhile (· < x)).length).sum
    lo ≤ rank && rank < lo + size
  let P : Nat → Bool := fun x => decide (lo ≤ (flat.filter fun y : Nat => y < x).length ∧
    (flat.filter fun y : Nat => y < x).length < lo + size)
  let Q : Nat → Bool := fun x => decide (let r := (S.filter fun y : Nat => y < x).length;
    lo ≤ r ∧ r < lo + size)
  have hsel_eq : selected = flat.filter P := by
    unfold selected P flat
    rw [flatMap_filter_eq_flatten_filter]
    apply List.filter_congr
    intro x hx
    rw [rank_eq runs x hr]
    apply Bool.eq_iff_iff.mpr
    simp [Bool.and_eq_true]
  have hQP : ∀ x, Q x = P x := by
    intro x
    unfold Q P
    have hc := count_lt_of_perm hpermS x
    rw [hc]
  have hpermSel : selected.Perm (S.filter Q) := by
    rw [hsel_eq]
    have h1 : (flat.filter P).Perm (S.filter P) := hpermS.symm.filter P
    have h2 : S.filter Q = S.filter P := by
      apply List.filter_congr
      intro x hx
      exact hQP x
    rw [h2]
    exact h1
  have hpermFinal : selected.mergeSort.Perm (S.filter Q) := by
    exact (List.mergeSort_perm selected (fun a b : Nat => a ≤ b)).trans hpermSel
  have hselSorted : selected.mergeSort.Pairwise (· ≤ ·) := by
    have h := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
      (by
        intro a b c hab hbc
        apply decide_eq_true
        have h1 : a ≤ b := of_decide_eq_true hab
        have h2 : b ≤ c := of_decide_eq_true hbc
        omega)
      (by
        intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have h' : b ≤ a := by omega
          simp [h, h'])
      selected
    exact h.imp (by intro a b hab; exact of_decide_eq_true hab)
  have hTSorted : (S.filter Q).Pairwise (· ≤ ·) := hSsorted.filter Q
  have heq : selected.mergeSort = S.filter Q := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
    · intro a b ha hb hab hba
      omega
    · exact hselSorted
    · exact hTSorted
    · exact hpermFinal
  change selected.mergeSort = (S.drop lo).take size
  rw [heq]
  simpa [Q] using hrange

private def firstP (P : Nat → Bool) : Nat → Nat → Nat → Nat
  | 0, lo, _ => lo
  | fuel + 1, lo, hi =>
      if lo ≥ hi then lo else
        let mid := (lo + hi) / 2
        if P mid then firstP P fuel lo mid else firstP P fuel (mid + 1) hi

private theorem pow_succ_two (n : Nat) : 2 ^ (n + 1) = 2 * 2 ^ n := by
  rw [show n + 1 = Nat.succ n by omega]
  rw [Nat.pow_succ]
  omega

private theorem firstP_result (P : Nat → Bool) :
    ∀ fuel lo hi, lo ≤ hi → hi < lo + 2 ^ fuel → P hi = true →
    (∀ i, lo ≤ i → i < hi → P i = true → P (i + 1) = true) →
    P (firstP P fuel lo hi) = true ∧
      (∀ i, lo ≤ i → i < firstP P fuel lo hi → P i = false) := by
  intro fuel
  induction fuel with
  | zero =>
      intro lo hi hle hwidth hhi hmono
      have hEq : hi = lo := by
        have : hi < lo + 1 := by simpa using hwidth
        omega
      subst hi
      constructor
      · simpa [firstP] using hhi
      · intro i hi hlt
        simp [firstP] at hlt
        omega
  | succ fuel ih =>
      intro lo hi hle hwidth hhi hmono
      by_cases hlohi : lo = hi
      · subst hi
        constructor
        · simpa [firstP] using hhi
        · intro i hi hlt
          simp [firstP] at hlt
          omega
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hmid_lo : lo ≤ mid := by dsimp [mid]; omega
        have hmid_hi : mid < hi := by dsimp [mid]; omega
        have hpown : 0 < 2 ^ fuel := Nat.pow_pos (by decide : 0 < 2)
        have hmidmul : 2 * mid ≤ lo + hi := by
          simpa [mid, Nat.mul_comm] using Nat.mul_div_le (lo + hi) 2
        have hA : lo + hi < (mid + 1) * 2 := by
          have hdivlt : (lo + hi) / 2 < mid + 1 := by simpa [mid] using Nat.lt_succ_self mid
          exact (Nat.div_lt_iff_lt_mul (by decide : 0 < 2)).mp hdivlt
        rw [pow_succ_two] at hwidth
        have htwo : 2 * 2 ^ fuel = 2 ^ fuel + 2 ^ fuel := by omega
        rw [htwo] at hwidth
        have hnle : ¬ hi ≤ lo := by omega
        by_cases hPmid : P mid = true
        · have hw : mid < lo + 2 ^ fuel := by
            omega
          have hres := ih lo mid hmid_lo hw hPmid (by
            intro i hil him hip
            exact hmono i hil (by omega) hip)
          have hfirst : firstP P (fuel + 1) lo hi = firstP P fuel lo mid := by
            simp [firstP, mid, hPmid, hnle]
          rw [hfirst]
          exact hres
        · have hmono_le : ∀ {i j : Nat}, lo ≤ i → i ≤ j → j ≤ hi → P i = true → P j = true := by
            intro i j hli hij hjh hPi
            induction j with
            | zero =>
                have : i = 0 := by omega
                subst i
                exact hPi
            | succ j ihj =>
                by_cases h : i ≤ j
                · exact hmono j (by omega) (by omega) (ihj h (by omega))
                · have hji : i = j + 1 := by omega
                  subst i
                  exact hPi
          have hw : hi < (mid + 1) + 2 ^ fuel := by
            omega
          have hres := ih (mid + 1) hi (by omega) hw hhi (by
            intro i hi1 hih1 hip
            exact hmono i (by omega) hih1 hip)
          have hfirst : firstP P (fuel + 1) lo hi = firstP P fuel (mid + 1) hi := by
            simp [firstP, mid, hPmid, hnle]
          rw [hfirst]
          constructor
          · exact hres.1
          · intro i hi hlt
            by_cases him : i ≤ mid
            · have hnot : ¬ P i = true := by
                intro hPi
                exact hPmid (hmono_le (by omega) him (by omega) hPi)
              cases hpi : P i with
              | true => exact (hnot hpi).elim
              | false => rfl
            · exact hres.2 i (by omega) hlt
private theorem getLast_take (l : List Row) (k : Nat) (hk : 0 < k) (hkl : k ≤ l.length) :
    (l.take k).getLast? = l[k - 1]? := by
  rw [List.getLast?_eq_getElem?]
  rw [List.length_take_of_le hkl]
  exact List.getElem?_take_of_lt (by omega)

private theorem sorted_get_le (l : List Row) (hs : sorted l) {i j : Nat}
    (hi : i < l.length) (hj : j < l.length) (hij : i ≤ j) : l[i].1 ≤ l[j].1 := by
  by_cases h : i = j
  · subst j
    omega
  · exact (List.pairwise_iff_getElem.mp hs) i j hi hj (by omega)

private theorem lt_length_of_getElem?_some {α : Type} {l : List α} {i : Nat} {x : α}
    (h : l[i]? = some x) : i < l.length := by
  by_cases hlt : i < l.length
  · exact hlt
  · have hle : l.length ≤ i := Nat.le_of_not_gt hlt
    have hnone : l[i]? = none := List.getElem?_eq_none_iff.mpr hle
    rw [hnone] at h
    cases h

private theorem cutOK_cons_left (x : Row) (xs b : List Row) (i j : Nat)
    (hi : i + 1 ≤ (x :: xs).length) (hj : j ≤ b.length)
    (hcut : cutOK (x :: xs) b (i + 1) j) : cutOK xs b i j := by
  rcases hcut with ⟨_, _, hleft, hright⟩
  have hi' : i ≤ xs.length := by simpa using hi
  refine ⟨hi', hj, ?_, ?_⟩
  · cases i with
    | zero => simp
    | succ i' =>
        intro p hp q hq
        have hpeq1 : (xs.take (i' + 1)).getLast? = xs[i']? :=
          getLast_take xs (i' + 1) (by omega) (by omega)
        have hpeq2 : ((x :: xs).take (i' + 2)).getLast? = (x :: xs)[i' + 1]? :=
          getLast_take (x :: xs) (i' + 2) (by omega) (by omega)
        have hp1 : p ∈ xs[i']? := by simpa [hpeq1] using hp
        have hp' : p ∈ ((x :: xs).take (i' + 2)).getLast? := by
          rw [hpeq2]
          simpa using hp1
        exact hleft p hp' q hq
  · intro q hq p hp
    have hp' : p ∈ (x :: xs)[i + 1]? := by simpa using hp
    exact hright q hq p hp'

private theorem cutOK_cons_right (a : List Row) (y : Row) (ys : List Row) (i j : Nat)
    (hi : i ≤ a.length) (hj : j + 1 ≤ (y :: ys).length)
    (hcut : cutOK a (y :: ys) i (j + 1)) : cutOK a ys i j := by
  rcases hcut with ⟨_, _, hleft, hright⟩
  have hj' : j ≤ ys.length := by simpa using hj
  refine ⟨hi, hj', ?_, ?_⟩
  · intro p hp q hq
    have hq' : q ∈ (y :: ys)[j + 1]? := by simpa using hq
    exact hleft p hp q hq'
  · cases j with
    | zero => simp
    | succ j' =>
        intro q hq p hp
        have hqeq1 : (ys.take (j' + 1)).getLast? = ys[j']? :=
          getLast_take ys (j' + 1) (by omega) (by omega)
        have hqeq2 : ((y :: ys).take (j' + 2)).getLast? = (y :: ys)[j' + 1]? :=
          getLast_take (y :: ys) (j' + 2) (by omega) (by omega)
        have hq1 : q ∈ ys[j']? := by simpa [hqeq1] using hq
        have hq' : q ∈ ((y :: ys).take (j' + 2)).getLast? := by
          rw [hqeq2]
          simpa using hq1
        exact hright q hq' p hp

private theorem merge_append_left : ∀ (a l₁ l₂ : List Row),
    sorted a → sorted (l₁ ++ l₂) →
    (∀ x ∈ a, ∀ y ∈ l₁, y.1 < x.1) →
    List.merge a (l₁ ++ l₂) leKey = l₁ ++ List.merge a l₂ leKey
  | a, [], l₂, ha, hl, hcond => by simp
  | [], l₁, l₂, ha, hl, hcond => by simp
  | x :: xs, y :: ys, l₂, ha, hl, hcond => by
      have hxy : y.1 < x.1 := hcond x (by simp) y (by simp)
      have hxyle : leKey x y = false := by
        unfold leKey
        apply decide_eq_false
        omega
      have hcond' : ∀ x' ∈ x :: xs, ∀ y' ∈ ys, y'.1 < x'.1 := by
        intro x' hx' y' hy'
        exact hcond x' hx' y' (by simp [hy'])
      have hl' : sorted (ys ++ l₂) := List.Pairwise.of_cons hl
      simp [List.merge, hxyle, merge_append_left (x :: xs) ys l₂ ha hl' hcond']

private theorem merge_append_right : ∀ (l₁ l₂ b : List Row),
    sorted (l₁ ++ l₂) → sorted b →
    (∀ x ∈ l₁, ∀ y ∈ b, x.1 ≤ y.1) →
    List.merge (l₁ ++ l₂) b leKey = l₁ ++ List.merge l₂ b leKey
  | [], l₂, b, hl, hb, hcond => by simp
  | x :: xs, l₂, [], hl, hb, hcond => by simp
  | x :: xs, l₂, y :: ys, hl, hb, hcond => by
      have hxy : x.1 ≤ y.1 := hcond x (by simp) y (by simp)
      have hxyle : leKey x y = true := by
        unfold leKey
        exact decide_eq_true hxy
      have hl' : sorted (xs ++ l₂) := List.Pairwise.of_cons hl
      have hcond' : ∀ x' ∈ xs, ∀ y' ∈ y :: ys, x'.1 ≤ y'.1 := by
        intro x' hx' y' hy'
        exact hcond x' (by simp [hx']) y' hy'
      simp [List.merge, hxyle, merge_append_right xs l₂ (y :: ys) hl' hb hcond']

private theorem merge_split_aux : ∀ (a b : List Row) (i j : Nat),
    sorted a → sorted b → i ≤ a.length → j ≤ b.length → cutOK a b i j →
    List.merge a b leKey =
      List.merge (a.take i) (b.take j) leKey ++ List.merge (a.drop i) (b.drop j) leKey := by
  intro a
  induction a with
  | nil =>
      intro b i j ha hb hi hj hcut
      have hi0 : i = 0 := by simp at hi; omega
      subst i
      simp
  | cons x xs iha =>
      intro b
      induction b with
      | nil =>
          intro i j ha hb hi hj hcut
          have hj0 : j = 0 := by simp at hj; omega
          subst j
          simp
      | cons y ys ihb =>
          intro i j ha hb hi hj hcut
          cases i with
          | zero =>
              have hcond : ∀ z ∈ x :: xs, ∀ w ∈ (y :: ys).take j, w.1 < z.1 := by
                intro z hz w hw
                have hzge : x.1 ≤ z.1 := by
                  rcases List.mem_cons.mp hz with rfl | hz
                  · omega
                  · exact (List.pairwise_cons.mp ha).1 z hz
                rcases hget : (y :: ys).take j |>.getLast? with _ | last
                · have hnil : (y :: ys).take j = [] := List.getLast?_eq_none_iff.mp hget
                  simp [hnil] at hw
                · have hsortedtake : sorted ((y :: ys).take j) := hb.take
                  have hlastmem : last ∈ ((y :: ys).take j).getLast? := by
                    rw [hget]
                    simp
                  have hwle : w.1 ≤ last.1 :=
                    le_of_mem_getLast hsortedtake w hw last hlastmem
                  have hxe : x ∈ (x :: xs)[0]? := by simp
                  have hlt : last.1 < x.1 := hcut.2.2.2 last hlastmem x hxe
                  omega
              have hm := merge_append_left (x :: xs) ((y :: ys).take j) ((y :: ys).drop j)
                ha (by simpa using hb) hcond
              simpa using hm
          | succ i' =>
              cases j with
              | zero =>
                  have hcond : ∀ z ∈ (x :: xs).take (i' + 1), ∀ w ∈ y :: ys, z.1 ≤ w.1 := by
                    intro z hz w hw
                    have hwge : y.1 ≤ w.1 := by
                      rcases List.mem_cons.mp hw with rfl | hw
                      · omega
                      · exact (List.pairwise_cons.mp hb).1 w hw
                    rcases hget : (x :: xs).take (i' + 1) |>.getLast? with _ | last
                    · have hnil : (x :: xs).take (i' + 1) = [] := List.getLast?_eq_none_iff.mp hget
                      simp [hnil] at hz
                    · have hsortedtake : sorted ((x :: xs).take (i' + 1)) := ha.take
                      have hlastmem : last ∈ ((x :: xs).take (i' + 1)).getLast? := by
                        rw [hget]
                        simp
                      have hzle : z.1 ≤ last.1 :=
                        le_of_mem_getLast hsortedtake z hz last hlastmem
                      have hye : y ∈ (y :: ys)[0]? := by simp
                      have hle : last.1 ≤ y.1 := hcut.2.2.1 last hlastmem y hye
                      omega
                  have hm := merge_append_right ((x :: xs).take (i' + 1))
                    ((x :: xs).drop (i' + 1)) (y :: ys) (by simpa using ha) hb hcond
                  simpa using hm
              | succ j' =>
                  by_cases hxy : leKey x y = true
                  · have hcut' : cutOK xs (y :: ys) i' (j' + 1) :=
                      cutOK_cons_left x xs (y :: ys) i' (j' + 1) (by simpa using hi) (by simpa using hj) hcut
                    have hih := iha (y :: ys) i' (j' + 1)
                      (List.Pairwise.of_cons ha) hb (by simpa using hi) (by simpa using hj) hcut'
                    have hm : List.merge (x :: xs) (y :: ys) leKey =
                        x :: List.merge xs (y :: ys) leKey := by
                      simp [hxy]
                    rw [hm, hih]
                    simp [List.take, List.drop, hxy]
                  · have hyxlt : y.1 < x.1 := by
                      have hnot : ¬ x.1 ≤ y.1 := by
                        unfold leKey at hxy
                        intro hle
                        have ht : decide (x.1 ≤ y.1) = true := decide_eq_true hle
                        rw [ht] at hxy
                        have hf : (true : Bool) = false := by simpa using hxy
                        cases hf
                      omega
                    have hyxle : leKey y x = true := by
                      unfold leKey
                      apply decide_eq_true
                      omega
                    have hcut' : cutOK (x :: xs) ys (i' + 1) j' :=
                      cutOK_cons_right (x :: xs) y ys (i' + 1) j' (by simpa using hi) (by simpa using hj) hcut
                    have hih := ihb (i' + 1) j'
                      ha (List.Pairwise.of_cons hb) (by simpa using hi) (by simpa using hj) hcut'
                    have hm : List.merge (x :: xs) (y :: ys) leKey =
                        y :: List.merge (x :: xs) ys leKey := by
                      simp [hxy]
                    rw [hm, hih]
                    simp [List.take, List.drop, hxy, hyxle]

private theorem cutOK_unique (a b : List Row) (ha : sorted a) (hb : sorted b) (n i j i' j' : Nat)
    (hsum : i + j = n) (hsum' : i' + j' = n)
    (hcut : cutOK a b i j) (hcut' : cutOK a b i' j') : i = i' ∧ j = j' := by
  rcases hcut with ⟨hi_le, hj_le, hleft, hright⟩
  rcases hcut' with ⟨hi'_le, hj'_le, hleft', hright'⟩
  by_cases hii : i = i'
  · subst i'
    constructor
    · rfl
    · omega
  · have hor : i < i' ∨ i' < i := by omega
    rcases hor with hlt | hlt
    · have hjj : j' < j := by omega
      rcases hbg : b[j - 1]? with _ | y
      · have hbnone : b.length ≤ j - 1 := List.getElem?_eq_none_iff.mp hbg
        omega
      · rcases hag : a[i]? with _ | x
        · have hanone : a.length ≤ i := List.getElem?_eq_none_iff.mp hag
          omega
        · have hgetB : (b.take j).getLast? = b[j - 1]? := getLast_take b j (by omega) hj_le
          have hymem : y ∈ (b.take j).getLast? := by rw [hgetB, hbg]; simp
          have hxmem : x ∈ a[i]? := by rw [hag]; simp
          have hyx : y.1 < x.1 := hright y hymem x hxmem
          have hidxa : i < a.length := lt_length_of_getElem?_some hag
          have hidxa' : i' - 1 < a.length := by omega
          rcases hag' : (a.take i').getLast? with _ | x'
          · have hnil : a.take i' = [] := List.getLast?_eq_none_iff.mp hag'
            have hlen : (a.take i').length = 0 := by rw [hnil]; simp
            have hmin0 : min i' a.length = 0 := by simpa using hlen
            have hmin : min i' a.length = i' := Nat.min_eq_left hi'_le
            rw [hmin] at hmin0
            omega
          · have hlast : (a.take i').getLast? = a[i' - 1]? := getLast_take a i' (by omega) hi'_le
            have hag'' : a[i' - 1] = x' := by
              have ht := hag'
              rw [hlast] at ht
              rw [List.getElem?_eq_getElem (by omega)] at ht
              exact Option.some.inj ht
            rcases hbg' : b[j']? with _ | y'
            · have hbnone : b.length ≤ j' := List.getElem?_eq_none_iff.mp hbg'
              omega
            · have hx'mem : x' ∈ (a.take i').getLast? := by rw [hag']; simp
              have hy'mem : y' ∈ b[j']? := by rw [hbg']; simp
              have hxx : x'.1 ≤ y'.1 := hleft' x' hx'mem y' hy'mem
              have hidxj' : j' < b.length := lt_length_of_getElem?_some hbg'
              have hidxj : j - 1 < b.length := lt_length_of_getElem?_some hbg
              have hxa : a[i].1 ≤ x'.1 := by
                rw [← hag'']
                exact sorted_get_le a ha hidxa hidxa' (by omega)
              have hyb : y'.1 ≤ b[j - 1].1 := by
                have hy'eq : b[j'] = y' := by
                  have ht := hbg'
                  rw [List.getElem?_eq_getElem (by omega)] at ht
                  exact Option.some.inj ht
                rw [← hy'eq]
                exact sorted_get_le b hb hidxj' hidxj (by omega)
              have hxeq : a[i] = x := by
                have ht := hag
                rw [List.getElem?_eq_getElem (by omega)] at ht
                exact Option.some.inj ht
              have hyeq : b[j - 1] = y := by
                have ht := hbg
                rw [List.getElem?_eq_getElem (by omega)] at ht
                exact Option.some.inj ht
              rw [← hxeq] at hyx
              rw [← hyeq] at hyx
              omega
    · have hjj : j < j' := by omega
      rcases hbg : b[j' - 1]? with _ | y
      · have hbnone : b.length ≤ j' - 1 := List.getElem?_eq_none_iff.mp hbg
        omega
      · rcases hag : a[i']? with _ | x
        · have hanone : a.length ≤ i' := List.getElem?_eq_none_iff.mp hag
          omega
        · have hgetB : (b.take j').getLast? = b[j' - 1]? := getLast_take b j' (by omega) hj'_le
          have hymem : y ∈ (b.take j').getLast? := by rw [hgetB, hbg]; simp
          have hxmem : x ∈ a[i']? := by rw [hag]; simp
          have hyx : y.1 < x.1 := hright' y hymem x hxmem
          have hidxa : i' < a.length := lt_length_of_getElem?_some hag
          have hidxa0 : i - 1 < a.length := by omega
          rcases hag0 : (a.take i).getLast? with _ | x'
          · have hnil : a.take i = [] := List.getLast?_eq_none_iff.mp hag0
            have hlen : (a.take i).length = 0 := by rw [hnil]; simp
            have hmin0 : min i a.length = 0 := by simpa using hlen
            have hmin : min i a.length = i := Nat.min_eq_left hi_le
            rw [hmin] at hmin0
            omega
          · have hlast : (a.take i).getLast? = a[i - 1]? := getLast_take a i (by omega) hi_le
            have hag'' : a[i - 1] = x' := by
              have ht := hag0
              rw [hlast] at ht
              rw [List.getElem?_eq_getElem (by omega)] at ht
              exact Option.some.inj ht
            rcases hbg0 : b[j]? with _ | y'
            · have hbnone : b.length ≤ j := List.getElem?_eq_none_iff.mp hbg0
              omega
            · have hx'mem : x' ∈ (a.take i).getLast? := by rw [hag0]; simp
              have hy'mem : y' ∈ b[j]? := by rw [hbg0]; simp
              have hxx : x'.1 ≤ y'.1 := hleft x' hx'mem y' hy'mem
              have hidxj : j < b.length := lt_length_of_getElem?_some hbg0
              have hidxj' : j' - 1 < b.length := lt_length_of_getElem?_some hbg
              have hxa : a[i'].1 ≤ x'.1 := by
                rw [← hag'']
                exact sorted_get_le a ha hidxa hidxa0 (by omega)
              have hyb : y'.1 ≤ b[j' - 1].1 := by
                have hy'eq : b[j] = y' := by
                  have ht := hbg0
                  rw [List.getElem?_eq_getElem (by omega)] at ht
                  exact Option.some.inj ht
                rw [← hy'eq]
                exact sorted_get_le b hb hidxj hidxj' (by omega)
              have hxeq : a[i'] = x := by
                have ht := hag
                rw [List.getElem?_eq_getElem (by omega)] at ht
                exact Option.some.inj ht
              have hyeq : b[j' - 1] = y := by
                have ht := hbg
                rw [List.getElem?_eq_getElem (by omega)] at ht
                exact Option.some.inj ht
              rw [← hxeq] at hyx
              rw [← hyeq] at hyx
              omega

private def moreLeft (a b : List Row) (n i : Nat) : Bool :=
  let j := n - i
  match b[j - 1]?, a[i]? with
  | some y, some x => j != 0 && decide (x.1 ≤ y.1)
  | _, _ => false

private def mlProp (a b : List Row) (n i : Nat) : Prop :=
  n - i ≠ 0 ∧ ∃ x, a[i]? = some x ∧ ∃ y, b[n - i - 1]? = some y ∧ x.1 ≤ y.1

private theorem moreLeft_iff_ml (a b : List Row) (n i : Nat) :
    moreLeft a b n i = true ↔ mlProp a b n i := by
  by_cases hj : n - i = 0
  · rcases hb0 : b[0]? with _ | y
    · rcases ha0 : a[i]? with _ | x
      · simp [moreLeft, mlProp, hj, hb0, ha0]
      · simp [moreLeft, mlProp, hj, hb0, ha0]
    · rcases ha0 : a[i]? with _ | x
      · simp [moreLeft, mlProp, hj, hb0, ha0]
      · simp [moreLeft, mlProp, hj, hb0, ha0]
  · have hjb : (n - i != 0) = true := by simp [hj]
    rcases hbget : b[n - i - 1]? with _ | y
    · simp [moreLeft, mlProp, hj, hjb, hbget]
    · rcases haget : a[i]? with _ | x
      · simp [moreLeft, mlProp, hj, hjb, hbget, haget]
      · simp [moreLeft, mlProp, hj, hjb, hbget, haget]

private theorem ml_mono (a b : List Row) (ha : sorted a) (hb : sorted b)
    (n lo i : Nat) (hlo : lo = n - b.length) (hli : lo ≤ i) :
    mlProp a b n (i + 1) → mlProp a b n i := by
  intro h
  rcases h with ⟨hjne_succ, x', hx', y', hy', hxy⟩
  have hidxa_succ : i + 1 < a.length := lt_length_of_getElem?_some hx'
  have hidxb_succ : n - (i + 1) - 1 < b.length := lt_length_of_getElem?_some hy'
  have hidxa : i < a.length := by omega
  have hidxb : n - i - 1 < b.length := by
    rw [hlo] at hli
    omega
  have hji_ne : n - i ≠ 0 := by omega
  have haget : a[i]? = some a[i] := List.getElem?_eq_getElem (by omega)
  have hbget : b[n - i - 1]? = some b[n - i - 1] :=
    List.getElem?_eq_getElem (by omega)
  have hxeq : a[i + 1] = x' := by
    have ht := hx'
    rw [List.getElem?_eq_getElem (by omega)] at ht
    exact Option.some.inj ht
  have hyeq : b[n - (i + 1) - 1] = y' := by
    have ht := hy'
    rw [List.getElem?_eq_getElem (by omega)] at ht
    exact Option.some.inj ht
  have hxle : a[i].1 ≤ x'.1 := by
    rw [← hxeq]
    exact sorted_get_le a ha hidxa hidxa_succ (by omega)
  have hyle : y'.1 ≤ b[n - i - 1].1 := by
    rw [← hyeq]
    exact sorted_get_le b hb hidxb_succ hidxb (by omega)
  exact ⟨hji_ne, a[i], haget, b[n - i - 1], hbget, by omega⟩

private theorem not_ml_hi (a b : List Row) (n : Nat) :
    ¬ mlProp a b n (min n a.length) := by
  intro h
  rcases h with ⟨hjne, x, hx, y, hy, hxy⟩
  have hidxa : min n a.length < a.length := lt_length_of_getElem?_some hx
  by_cases hle : a.length ≤ n
  · have hmin : min n a.length = a.length := Nat.min_eq_right hle
    omega
  · have hnle : n ≤ a.length := by omega
    have hmin : min n a.length = n := Nat.min_eq_left hnle
    omega

private theorem firstP_bounds (P : Nat → Bool) : ∀ (fuel lo hi : Nat), lo ≤ hi →
    lo ≤ firstP P fuel lo hi ∧ firstP P fuel lo hi ≤ hi
  | 0, lo, hi, hle => by simp [firstP, hle]
  | fuel + 1, lo, hi, hle => by
      by_cases hh : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp [firstP]
      · let mid := (lo + hi) / 2
        have hmidlo : lo ≤ mid := by dsimp [mid]; omega
        have hmidhi : mid < hi := by dsimp [mid]; omega
        by_cases hP : P mid = true
        · have hres := firstP_bounds P fuel lo mid hmidlo
          have hfirst : firstP P (fuel + 1) lo hi = firstP P fuel lo mid := by
            simp [firstP, mid, hP, hh]
          rw [hfirst]
          exact ⟨hres.1, by omega⟩
        · have hres := firstP_bounds P fuel (mid + 1) hi (by omega)
          have hfirst : firstP P (fuel + 1) lo hi = firstP P fuel (mid + 1) hi := by
            simp [firstP, mid, hP, hh]
          rw [hfirst]
          exact ⟨by omega, hres.2⟩

private theorem diagonal_eq_firstP (a b : List Row) (n fuel lo hi : Nat) :
    diagonal a.toArray b.toArray n fuel lo hi =
      firstP (fun i => !(moreLeft a b n i)) fuel lo hi := by
  induction fuel generalizing lo hi with
  | zero => simp [diagonal, firstP]
  | succ fuel ih =>
      by_cases hh : lo ≥ hi
      · simp [diagonal, firstP, hh]
      · by_cases hm : moreLeft a b n ((lo + hi) / 2) = true
        · have hc : (match b[n - (lo + hi) / 2 - 1]?, a[(lo + hi) / 2]? with
              | some y, some x => n - (lo + hi) / 2 != 0 && decide (x.1 ≤ y.1)
              | _, _ => false) = true := by simpa [moreLeft] using hm
          simp [diagonal, firstP, List.getElem?_toArray, hh, hm, ih]
          intro hfalse
          have hcontra : (true : Bool) = false := by
            rw [← hc]
            exact hfalse
          cases hcontra
        · have hmf : moreLeft a b n ((lo + hi) / 2) = false := by
            cases h' : moreLeft a b n ((lo + hi) / 2) <;> simp_all
          have hc : (match b[n - (lo + hi) / 2 - 1]?, a[(lo + hi) / 2]? with
              | some y, some x => n - (lo + hi) / 2 != 0 && decide (x.1 ≤ y.1)
              | _, _ => false) = false := by simpa [moreLeft] using hmf
          simp [diagonal, firstP, List.getElem?_toArray, hh, hmf, ih]
          intro htrue
          have hcontra : (false : Bool) = true := by
            rw [← hc]
            exact htrue
          cases hcontra

private theorem log2_fuel (d : Nat) : d < 2 ^ (d.log2 + 2) := by
  induction d using Nat.strongRecOn with
  | ind d ih =>
      by_cases hd : d < 2
      · have h01 : d = 0 ∨ d = 1 := by omega
        rcases h01 with rfl | rfl <;> native_decide
      · have hd2 : 2 ≤ d := by omega
        rw [Nat.log2_def]
        simp [hd2]
        have hhalf : d / 2 < d := Nat.log2_terminates d hd2
        have ihh := ih (d / 2) hhalf
        have hp : 0 < 2 ^ ((d / 2).log2 + 2) := Nat.pow_pos (by decide)
        have hdp : d / 2 + 1 ≤ 2 ^ ((d / 2).log2 + 2) := by omega
        have hdm := Nat.div_add_mod d 2
        have hmod : d % 2 < 2 := Nat.mod_lt d (by decide)
        have hdle : d ≤ 2 * (d / 2) + 1 := by omega
        rw [show (d / 2).log2 + 1 + 2 = ((d / 2).log2 + 2) + 1 by omega, pow_succ_two]
        omega

theorem MergeDiagonal_sound : TinylambOpt.ResearchGoals.OrderedStreams.MergeDiagonal.Goal := by
  intro a b n ha hb hn
  let lo := n - b.length
  let hi := min n a.length
  let fuel := (hi - lo).log2 + 2
  let P : Nat → Bool := fun i => !(moreLeft a b n i)
  have hlohi : lo ≤ hi := by
    dsimp [lo, hi]
    omega
  have hwidth : hi < lo + 2 ^ fuel := by
    dsimp [fuel]
    have hd := log2_fuel (hi - lo)
    omega
  have hPhi : P hi = true := by
    dsimp [P]
    cases hm : moreLeft a b n hi
    · rfl
    · have hmlp := (moreLeft_iff_ml a b n hi).mp hm
      have hmlp' : mlProp a b n (min n a.length) := by simpa [hi] using hmlp
      exact False.elim ((not_ml_hi a b n) hmlp')
  have hmono : ∀ i, lo ≤ i → i < hi → P i = true → P (i + 1) = true := by
    intro i hli hih hPi
    dsimp [P] at hPi ⊢
    have hml_i_false : moreLeft a b n i = false := by
      cases hm : moreLeft a b n i
      · rfl
      · have hf : (!(moreLeft a b n i)) = false := by simp [hm]
        rw [hf] at hPi
        cases hPi
    cases hml_succ : moreLeft a b n (i + 1)
    · rfl
    · have hp_succ := (moreLeft_iff_ml a b n (i + 1)).mp hml_succ
      have hp_i := ml_mono a b ha hb n lo i (by rfl) hli hp_succ
      have hml_i_true := (moreLeft_iff_ml a b n i).mpr hp_i
      rw [hml_i_false] at hml_i_true
      cases hml_i_true
  have hres := firstP_result P fuel lo hi hlohi hwidth hPhi hmono
  let i := firstP P fuel lo hi
  have hdiag : diagonal a.toArray b.toArray n fuel lo hi = i := by
    dsimp [i, P]
    exact diagonal_eq_firstP a b n fuel lo hi
  have hPi : moreLeft a b n i = false := by
    have hp := hres.1
    dsimp [P] at hp
    cases hm : moreLeft a b n i
    · rfl
    · have hf : (!(moreLeft a b n i)) = false := by simp [hm]
      rw [hf] at hp
      cases hp
  have hprev : ∀ k, lo ≤ k → k < i → moreLeft a b n k = true := by
    intro k hlk hki
    have hp := hres.2 k hlk hki
    dsimp [P] at hp
    cases hm : moreLeft a b n k
    · have hf : (!(moreLeft a b n k)) = true := by simp [hm]
      rw [hf] at hp
      cases hp
    · rfl
  have hibounds := firstP_bounds P fuel lo hi hlohi
  have hi_le_a : i ≤ a.length := by
    have hbnd := hibounds.2
    dsimp [i, hi] at hbnd ⊢
    omega
  have hi_le_n : i ≤ n := by
    have hbnd := hibounds.2
    dsimp [i, hi] at hbnd ⊢
    omega
  let j := n - i
  have hj_le_b : j ≤ b.length := by
    have hbnd := hibounds.1
    dsimp [i, j, lo] at hbnd ⊢
    omega
  have hj_eq : i + j = n := by dsimp [j]; omega
  have hcut : cutOK a b i j := by
    refine ⟨hi_le_a, hj_le_b, ?_, ?_⟩
    · intro x hx y hy
      by_cases hi0 : i = 0
      · have hax : (a.take i).getLast? = none := by rw [hi0]; simp
        simp [hax] at hx
      · by_cases hil : i = lo
        · have hjl : n - lo = b.length ∨ lo = 0 := by dsimp [lo]; omega
          rcases hjl with hjl | hlo0
          · have hbj : b[j]? = none := by dsimp [j]; rw [hil, hjl]; simp
            simp [hbj] at hy
          · have hax : (a.take i).getLast? = none := by rw [hil, hlo0]; simp
            simp [hax] at hx
        · have hilt : lo ≤ i - 1 := by omega
          have hilt' : i - 1 < i := by omega
          have hmprev := hprev (i - 1) hilt hilt'
          have hmlp : mlProp a b n (i - 1) := (moreLeft_iff_ml a b n (i - 1)).mp hmprev
          rcases hmlp with ⟨_, x', hx', y', hy', hlexy⟩
          have hxeq' : a[i - 1] = x' := by
            have ht := hx'
            rw [List.getElem?_eq_getElem (by omega)] at ht
            exact Option.some.inj ht
          have hyeq' : b[n - (i - 1) - 1] = y' := by
            have ht := hy'
            rw [List.getElem?_eq_getElem (by omega)] at ht
            exact Option.some.inj ht
          have hgeta : (a.take i).getLast? = a[i - 1]? := getLast_take a i (by omega) hi_le_a
          have hxeq : x = a[i - 1] := by
            have ht := hx
            rw [hgeta] at ht
            simp at ht
            rw [List.getElem?_eq_getElem (by omega)] at ht
            exact (Option.some.inj ht).symm
          have hidxeq : n - (i - 1) - 1 = j := by dsimp [j]; omega
          have hyeq : y = b[n - (i - 1) - 1] := by
            have ht := hy
            rw [← hidxeq] at ht
            simp at ht
            rw [List.getElem?_eq_getElem (by omega)] at ht
            exact (Option.some.inj ht).symm
          rw [hxeq, hyeq, hxeq', hyeq']
          exact hlexy
    · by_cases hj0 : j = 0
      · simp [hj0]
      · by_cases hia : i = a.length
        · simp [hia]
        · have hidxa : i < a.length := by omega
          have hidxb : j - 1 < b.length := by omega
          dsimp [j] at hidxb
          have hbgi : b[n - i - 1]? = some b[n - i - 1] := List.getElem?_eq_getElem (by omega)
          have hagi : a[i]? = some a[i] := List.getElem?_eq_getElem (by omega)
          have hmlf : moreLeft a b n i = false := hPi
          dsimp [moreLeft] at hmlf
          rw [hbgi, hagi] at hmlf
          simp at hmlf
          have hlt : b[n - i - 1].1 < a[i].1 := by omega
          have hgetb : (b.take j).getLast? = b[j - 1]? := getLast_take b j (by omega) hj_le_b
          intro y hy x hx
          have hyeq : y = b[n - i - 1] := by
            have ht := hy
            rw [hgetb] at ht
            have hidxeq2 : j - 1 = n - i - 1 := rfl
            rw [hidxeq2] at ht
            simp at ht
            rw [List.getElem?_eq_getElem (by omega)] at ht
            exact (Option.some.inj ht).symm
          have hxeq : x = a[i] := by
            have ht := hx
            simp at ht
            rw [List.getElem?_eq_getElem (by omega)] at ht
            exact (Option.some.inj ht).symm
          rw [hyeq, hxeq]
          exact hlt
  have hcut_eq : mergeCut a b n = (i, j) := by
    dsimp [mergeCut, lo, hi, fuel, i, j]
    rw [hdiag]
  simp [hcut_eq]
  refine ⟨hj_eq, hcut, ?_, ?_⟩
  · intro i' j' hsum hcut'
    have hu := cutOK_unique a b ha hb n i j i' j' hj_eq hsum hcut hcut'
    exact ⟨hu.1.symm, hu.2.symm⟩
  · have hm := merge_split_aux a b i j ha hb hi_le_a hj_le_b hcut
    rw [hm]
    have hlen : (List.merge (a.take i) (b.take j) leKey).length = n := by
      simp [Nat.min_eq_left hi_le_a, Nat.min_eq_left hj_le_b, hj_eq]
    rw [← hlen]
    simp

private abbrev Tagged := Row × Nat

private abbrev tagLE (a b : Tagged) : Prop := List.zipIdxLE leKey a b = true

private def expRuns (rs : List (Int × List Nat)) : List Row :=
  rs.flatMap fun (k, vs) => vs.map fun v => (k, v)

private def tagBlocks : List (Int × List Nat) → Nat → List (List Tagged)
  | [], _ => []
  | (k, vs) :: rs, off =>
      (vs.map fun v => (k, v)).zipIdx off :: tagBlocks rs (off + vs.length)

private theorem expRuns_append (rs₁ rs₂ : List (Int × List Nat)) :
    expRuns (rs₁ ++ rs₂) = expRuns rs₁ ++ expRuns rs₂ := by
  induction rs₁ <;> simp [expRuns, *]

private theorem tagBlocks_flatten : ∀ (rs : List (Int × List Nat)) (off : Nat),
    (tagBlocks rs off).flatten = (expRuns rs).zipIdx off
  | [], off => by simp [tagBlocks, expRuns]
  | (k, vs) :: rs, off => by
      simp [tagBlocks, expRuns, List.zipIdx_append, tagBlocks_flatten rs (off + vs.length)]

private theorem zipIdx_map_fst {α : Type} : ∀ (l : List α) (off : Nat),
    (l.zipIdx off).map (fun z => z.1) = l
  | [], _ => by simp
  | x :: xs, off => by simp [zipIdx_map_fst xs (off + 1)]

private theorem tagBlocks_fst (rs : List (Int × List Nat)) (off : Nat) :
    ((tagBlocks rs off).flatten.map fun z => z.1) = expRuns rs := by
  rw [tagBlocks_flatten]
  exact zipIdx_map_fst (expRuns rs) off

private theorem tagBlocks_rev_fst : ∀ (rs : List (Int × List Nat)) (off : Nat),
    ((tagBlocks rs off).reverse.flatten.map fun z => z.1) = expRuns rs.reverse
  | [], off => by simp [tagBlocks, expRuns]
  | (k, vs) :: rs, off => by
      have htail := tagBlocks_rev_fst rs (off + vs.length)
      simp [tagBlocks, expRuns, List.reverse_cons, List.flatten_append,
        List.map_append, htail]

private theorem tagBlocks_rev_perm : ∀ (rs : List (Int × List Nat)) (off : Nat),
    ((tagBlocks rs off).reverse.flatten).Perm (tagBlocks rs off).flatten
  | [], off => by simp [tagBlocks]
  | (k, vs) :: rs, off => by
      have htail := tagBlocks_rev_perm rs (off + vs.length)
      have h1 := htail.append (List.Perm.refl ((vs.map fun v => (k, v)).zipIdx off))
      simpa [tagBlocks] using h1.trans (List.perm_append_comm
        (l₁ := (tagBlocks rs (off + vs.length)).flatten)
        (l₂ := (vs.map fun v => (k, v)).zipIdx off))

private theorem mem_block_fst_key {k : Int} {vs : List Nat} {off : Nat} {z : Tagged} :
    z ∈ (vs.map fun v => (k, v)).zipIdx off → z.1.1 = k := by
  intro hz
  have hfz : z.1 ∈ vs.map (fun v => (k, v)) := List.fst_mem_of_mem_zipIdx hz
  rcases List.mem_map.mp hfz with ⟨v, hv, hvEq⟩
  rw [← hvEq]

private theorem block_pairwise (k : Int) (vs : List Nat) (off : Nat) :
    ((vs.map fun v => (k, v)).zipIdx off).Pairwise tagLE := by
  induction vs generalizing off with
  | nil => simp
  | cons v vs ih =>
      simp only [List.map_cons, List.zipIdx_cons]
      rw [List.pairwise_cons]
      constructor
      · intro z hz
        have hkey : z.1.1 = k := mem_block_fst_key hz
        have hidx0 : off + 1 ≤ z.2 := List.le_snd_of_mem_zipIdx hz
        have hidx : off ≤ z.2 := by omega
        have hle1 : leKey (k, v) z.1 = true := by
          unfold leKey
          apply decide_eq_true
          rw [hkey]
          omega
        have hle2 : leKey z.1 (k, v) = true := by
          unfold leKey
          apply decide_eq_true
          rw [hkey]
          omega
        simp [tagLE, List.zipIdxLE, hle1, hle2, hidx]
      · simpa using ih (off + 1)

private theorem mem_tagBlocks_key_lt : ∀ (rs : List (Int × List Nat)) (off : Nat)
    (bound : Int) (z : Tagged),
    (∀ r ∈ rs, r.1 < bound) → z ∈ (tagBlocks rs off).flatten → z.1.1 < bound
  | [], off, bound, z, h, hz => by simp [tagBlocks] at hz
  | (k, vs) :: rs, off, bound, z, h, hz => by
      have hk : k < bound := h (k, vs) (by simp)
      have htail : ∀ r ∈ rs, r.1 < bound := by intro r hr; exact h r (by simp [hr])
      change z ∈ (vs.map fun v => (k, v)).zipIdx off ++
        (tagBlocks rs (off + vs.length)).flatten at hz
      rw [List.mem_append] at hz
      rcases hz with hz | hz
      · rw [mem_block_fst_key hz]
        exact hk
      · exact mem_tagBlocks_key_lt rs (off + vs.length) bound z htail hz

private theorem tagBlocks_rev_pairwise : ∀ (rs : List (Int × List Nat)) (off : Nat),
    (rs.map Prod.fst).Pairwise (· > ·) →
    ((tagBlocks rs off).reverse.flatten).Pairwise tagLE
  | [], off, h => by simp [tagBlocks]
  | (k, vs) :: rs, off, h => by
      have hkrs : ∀ r ∈ rs, r.1 < k := by
        intro r hr
        exact (List.pairwise_cons.mp h).1 r.1 (List.mem_map.mpr ⟨r, hr, rfl⟩)
      have hpair_tail := tagBlocks_rev_pairwise rs (off + vs.length) (List.Pairwise.of_cons h)
      have hpair_block := block_pairwise k vs off
      have hcross : ∀ x ∈ (tagBlocks rs (off + vs.length)).reverse.flatten,
          ∀ y ∈ (vs.map fun v => (k, v)).zipIdx off, tagLE x y := by
        intro x hx y hy
        have hxkey : x.1.1 < k :=
          mem_tagBlocks_key_lt rs (off + vs.length) k x hkrs (by simpa using hx)
        have hykey : y.1.1 = k := mem_block_fst_key hy
        have hle1 : leKey x.1 y.1 = true := by
          unfold leKey
          apply decide_eq_true
          rw [hykey]
          omega
        have hle2 : leKey y.1 x.1 = false := by
          unfold leKey
          apply decide_eq_false
          rw [hykey]
          omega
        simp [tagLE, List.zipIdxLE, hle1, hle2]
      have happ := List.pairwise_append.mpr ⟨hpair_tail, hpair_block, hcross⟩
      simpa [tagBlocks, List.reverse_cons, List.flatten_append] using happ

theorem ReversePeerRuns_sound : TinylambOpt.ResearchGoals.OrderedStreams.ReversePeerRuns.Goal := by
  intro runs hruns
  let L := expRuns runs
  let T := (tagBlocks runs 0).reverse.flatten
  have hpairT : T.Pairwise tagLE := by
    simpa [T, L] using tagBlocks_rev_pairwise runs 0 hruns
  have hpermT : T.Perm L.zipIdx := by
    have hp := tagBlocks_rev_perm runs 0
    rw [tagBlocks_flatten] at hp
    simpa [T, L] using hp
  have hle_trans : ∀ (a b c : Row), leKey a b → leKey b c → leKey a c := by
    intro a b c hab hbc
    unfold leKey at hab hbc ⊢
    apply decide_eq_true
    have h1 : a.1 ≤ b.1 := of_decide_eq_true hab
    have h2 : b.1 ≤ c.1 := of_decide_eq_true hbc
    omega
  have hle_total : ∀ (a b : Row), leKey a b || leKey b a := by
    intro a b
    unfold leKey
    by_cases h : a.1 ≤ b.1
    · simp [h]
    · have h' : b.1 ≤ a.1 := by omega
      simp [h, h']
  have hpairM : ((L.zipIdx).mergeSort (List.zipIdxLE leKey)).Pairwise tagLE := by
    exact List.pairwise_mergeSort (le := List.zipIdxLE leKey)
      (List.zipIdxLE_trans hle_trans) (List.zipIdxLE_total hle_total) L.zipIdx
  have hpermM : ((L.zipIdx).mergeSort (List.zipIdxLE leKey)).Perm T := by
    exact (List.mergeSort_perm L.zipIdx (List.zipIdxLE leKey)).trans hpermT.symm
  have hEq : (L.zipIdx).mergeSort (List.zipIdxLE leKey) = T := by
    apply List.Perm.eq_of_pairwise (le := tagLE)
    · intro a b ha hb hab hba
      have hle_ab : leKey a.1 b.1 = true := by
        by_cases h : leKey a.1 b.1 = true
        · exact h
        · have hf : leKey a.1 b.1 = false := by cases hle : leKey a.1 b.1 <;> simp_all
          simp [tagLE, List.zipIdxLE, hf] at hab
      have hle_ba : leKey b.1 a.1 = true := by
        by_cases h : leKey b.1 a.1 = true
        · exact h
        · have hf : leKey b.1 a.1 = false := by cases hle : leKey b.1 a.1 <;> simp_all
          simp [tagLE, List.zipIdxLE, hf] at hba
      have hind1 : a.2 ≤ b.2 := by simpa [tagLE, List.zipIdxLE, hle_ab, hle_ba] using hab
      have hind2 : b.2 ≤ a.2 := by simpa [tagLE, List.zipIdxLE, hle_ab, hle_ba] using hba
      have hind : a.2 = b.2 := by omega
      have hbmem : b ∈ L.zipIdx := hpermT.mem_iff.mp hb
      have hamem : a ∈ L.zipIdx := (List.mergeSort_perm L.zipIdx (List.zipIdxLE leKey)).mem_iff.mp ha
      have had : L[a.2]? = some a.1 := List.mk_mem_zipIdx_iff_getElem?.mp hamem
      have hbd : L[b.2]? = some b.1 := List.mk_mem_zipIdx_iff_getElem?.mp hbmem
      rw [hind] at had
      have hrow : a.1 = b.1 := by
        have hd : some a.1 = some b.1 := by rw [← had, hbd]
        exact Option.some.inj hd
      exact Prod.ext hrow hind
    · exact hpairM
    · exact hpairT
    · exact hpermM
  have hst := List.mergeSort_zipIdx (l := L) (le := leKey)
  have hfst : T.map (fun z => z.1) = expRuns runs.reverse := by
    simpa [T] using tagBlocks_rev_fst runs 0
  change expRuns runs.reverse = (expRuns runs).mergeSort leKey
  rw [← hfst, ← hEq, hst]

-- C++ references to this model or its proof.
-- executor/detail/execution_strategies.hpp
-- executor/detail/execution_strategies_test.cpp
