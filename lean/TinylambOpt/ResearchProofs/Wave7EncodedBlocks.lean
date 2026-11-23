import TinylambOpt.ResearchGoals.ExecutionStrategies.EncodedBlocks

namespace TinylambOpt.ResearchProofs.Wave7EncodedBlocks

open TinylambOpt.ResearchGoals.EncodedBlocks

private theorem filter_congr {α : Type} (p q : α → Bool) (xs : List α)
    (h : ∀ x, p x = q x) : xs.filter p = xs.filter q := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.filter_cons, h x, ih]

theorem PatchedRange_sound : TinylambOpt.ResearchGoals.EncodedBlocks.PatchedRange.Goal := by
  intro base lo hi deltas patch live
  dsimp only
  apply filter_congr
  intro i
  cases hpatch : patch i with
  | none =>
      let d : Int := ((deltas[i]?.getD 0).toNat : Int)
      change (live i && (decide (lo - base ≤ d) && decide (d < hi - base))) =
        (live i && decide (lo ≤ base + d) && decide (base + d < hi))
      by_cases hl : live i
      · by_cases h1 : lo - base ≤ d
        · have h1' : lo ≤ base + d := by omega
          by_cases h2 : d < hi - base
          · have h2' : base + d < hi := by omega
            simp [hl, h1, h2, h1', h2']
          · have h2' : ¬ base + d < hi := by omega
            simp [hl, h1, h2, h1', h2']
        · have h1' : ¬ lo ≤ base + d := by omega
          simp [hl, h1, h1']
      · simp [hl]
  | some x =>
      by_cases hl : live i <;> simp [hpatch, hl]

private theorem int_mul_two (x : Int) : x * 2 = x + x := by
  rw [Int.mul_comm, Int.two_mul]

private theorem drop_range_add (lo m : Nat) :
    (List.range m).drop lo = (List.range (m - lo)).map (fun i => lo + i) := by
  by_cases h : lo ≤ m
  · rw [← Nat.add_sub_of_le h, List.range_add]
    have hd : List.drop lo (List.range lo ++ (List.range (m - lo)).map (fun i => lo + i)) =
        (List.range (m - lo)).map (fun i => lo + i) := by
      simpa using (List.drop_append_length (l₁ := List.range lo)
        (l₂ := (List.range (m - lo)).map (fun i => lo + i)))
    rw [hd]
    simp
  · have hle : m ≤ lo := by omega
    rw [List.drop_eq_nil_of_le (by simpa : (List.range m).length ≤ lo)]
    have : m - lo = 0 := by omega
    simp [this]

private theorem stride_alg (A d : Int) (k : Nat) :
    (k:Int)*(2*A+((k:Int)-1)*d) + 2*(A+(k:Int)*d) =
      ((k+1:Nat):Int)*(2*A+(k:Int)*d) := by
  simp [Int.natCast_add, Int.mul_add, Int.mul_sub,
        Int.mul_assoc, Int.mul_comm, Int.mul_left_comm, int_mul_two,
        Int.add_assoc, Int.add_comm, Int.add_left_comm]
  omega

private theorem stride_sum (a d : Int) (lo k : Nat) :
    2 * (((List.range k).map (fun i => a + ((lo + i : Nat) : Int) * d)).sum) =
      (k:Int)*(2*(a+(lo:Int)*d)+((k:Int)-1)*d) := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.sum_append, Int.mul_add, ih]
      simpa [Int.natCast_add, Int.add_mul, Int.add_assoc, Int.add_comm, Int.add_left_comm]
        using stride_alg (a+(lo:Int)*d) d k

theorem StrideSlice_sound : TinylambOpt.ResearchGoals.EncodedBlocks.StrideSlice.Goal := by
  intro a d n lo hi
  dsimp only
  let m := min n hi
  let len := m - lo
  change 2 * ((((List.range n).take hi).drop lo).map (fun (i : Nat) => a + (i:Int)*d)).sum =
    (len:Int)*(2*(a+(lo:Int)*d)+((len:Int)-1)*d)
  have htake : (List.range n).take hi = List.range m := by
    rw [List.take_range]
    simp [m, Nat.min_comm]
  rw [htake]
  by_cases hlo : lo ≤ m
  · have hdrop : (List.range m).drop lo = (List.range len).map (fun i => lo + i) := by
      simpa [len] using drop_range_add lo m
    rw [hdrop]
    rw [show ((List.range len).map (fun i => lo + i)).map (fun (i : Nat) => a + (i:Int)*d) =
        (List.range len).map (fun i => a + ((lo + i : Nat) : Int) * d) by
          simp [List.map_map]]
    exact stride_sum a d lo len
  · have hlen : len = 0 := by simp [len]; omega
    have hle : m ≤ lo := by omega
    have hnil : (List.range m).drop lo = [] := by
      exact List.drop_eq_nil_of_le (by simpa : (List.range m).length ≤ lo)
    simp [hlen, hnil]

private theorem moment_shift (ds : List Int) (total start : Nat) :
    ((ds.zipIdx (start + 1)).map (fun x => ((total : Int) - (x.2 : Int)) * x.1)).sum =
      ((ds.zipIdx start).map (fun x => (((total : Int) - 1) - (x.2 : Int)) * x.1)).sum := by
  induction ds generalizing start with
  | nil => simp
  | cons x xs ih =>
      have hcoef : (total : Int) - ((start : Int) + 1) = ((total : Int) - 1) - (start : Int) := by
        omega
      simp [List.zipIdx_cons, Int.natCast_add, hcoef, ih (start + 1)]

private theorem moment_cons (d : Int) (ds : List Int) :
    (((d :: ds).zipIdx.map (fun x => (((ds.length + 1 : Nat) : Int) - (x.2 : Int)) * x.1)).sum) =
      ((ds.length + 1 : Nat) : Int) * d +
        ((ds.zipIdx.map (fun x => ((ds.length : Int) - (x.2 : Int)) * x.1)).sum) := by
  simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
  rw [moment_shift ds (ds.length + 1) 0]
  simp

theorem SecondDifferences_sound : TinylambOpt.ResearchGoals.EncodedBlocks.SecondDifferences.Goal := by
  intro p v ds
  induction ds generalizing p v with
  | nil => simp [integrate]
  | cons d ds ih =>
      change integrate (p + v + d) (v + d) ds = _
      rw [ih]
      simp only [List.length_cons, List.sum_cons]
      rw [moment_cons]
      apply Prod.ext
      · simp [Int.natCast_add, Int.mul_add, Int.add_mul, Int.mul_comm,
              Int.add_assoc, Int.add_comm, Int.add_left_comm]
      · omega

private def editStep (h : Std.HashMap Nat Int) (e : Edit) : Std.HashMap Nat Int :=
  let (l, r, d) := e
  if l < r then
    let h := h.insert l (h.getD l 0 + d)
    h.insert r (h.getD r 0 - d)
  else h

private def impulseAt (e : Edit) (i : Nat) : Int :=
  let (l, r, d) := e
  if l < r then (if i == l then d else if i == r then -d else 0) else 0

private theorem impulses_eq_foldl : impulses = List.foldl editStep {} := by
  funext es; rfl

private theorem editStep_getD (h : Std.HashMap Nat Int) (e : Edit) (i : Nat) :
    (editStep h e).getD i 0 = h.getD i 0 + impulseAt e i := by
  rcases e with ⟨l, r, d⟩
  by_cases hlr : l < r
  · have hlr' : l ≠ r := by omega
    have hrl' : r ≠ l := by omega
    by_cases hil : i = l
    · have hir : i ≠ r := by omega
      have hri : r ≠ i := by omega
      simp [editStep, impulseAt, hlr, Std.HashMap.getD_insert, Int.sub_eq_add_neg, hil, hrl']
    · by_cases hir : i = r
      · have hil' : i ≠ l := by omega
        have hli : l ≠ i := by omega
        simp [editStep, impulseAt, hlr, Std.HashMap.getD_insert, Int.sub_eq_add_neg, hir, hli, hlr', hrl']
      · have hri : r ≠ i := by omega
        have hli : l ≠ i := by omega
        simp [editStep, impulseAt, hlr, Std.HashMap.getD_insert, Int.sub_eq_add_neg, hil, hir, hri, hli, hlr', hrl']
  · simp [editStep, impulseAt, hlr]

private theorem impulses_foldl_getD (es : List Edit) (h : Std.HashMap Nat Int) (i : Nat) :
    (es.foldl editStep h).getD i 0 = h.getD i 0 + (es.map fun e => impulseAt e i).sum := by
  induction es generalizing h with
  | nil => simp
  | cons e es ih =>
      rw [show (e :: es).foldl editStep h = es.foldl editStep (editStep h e) by rfl]
      rw [ih, editStep_getD]
      simp only [List.map_cons, List.sum_cons]
      omega

private theorem impulses_getD (es : List Edit) (i : Nat) :
    (impulses es).getD i 0 = (es.map fun e => impulseAt e i).sum := by
  rw [impulses_eq_foldl]
  simpa using impulses_foldl_getD es ({} : Std.HashMap Nat Int) i

private def sumRange (L : Nat) (f : Nat → Int) : Int := ((List.range L).map f).sum

private theorem sumRange_zero (L : Nat) : sumRange L (fun _ => 0) = 0 := by
  unfold sumRange
  induction (List.range L) with
  | nil => rfl
  | cons x xs ih => simp [List.map_cons, ih]

private theorem sumRange_succ (L : Nat) (f : Nat → Int) :
    sumRange (L+1) f = sumRange L f + f L := by
  simp [sumRange, List.range_succ, List.map_append, List.sum_append]

private theorem sumRange_add (L : Nat) (f g : Nat → Int) :
    sumRange L (fun j => f j + g j) = sumRange L f + sumRange L g := by
  induction L with
  | zero => simp [sumRange]
  | succ L ih =>
      rw [sumRange_succ, sumRange_succ, sumRange_succ, ih]
      omega

private theorem sumRange_map_sum (es : List Edit) (L : Nat) :
    sumRange L (fun j => (es.map fun e => impulseAt e j).sum) =
      (es.map fun e => sumRange L (fun j => impulseAt e j)).sum := by
  induction es with
  | nil => simpa [sumRange] using sumRange_zero L
  | cons e es ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [sumRange_add, ih]

private theorem impulses_sumRange (es : List Edit) (L : Nat) :
    sumRange L (fun j => (impulses es).getD j 0) =
      (es.map fun e => sumRange L (fun j => impulseAt e j)).sum := by
  unfold sumRange
  simp only [impulses_getD]
  exact sumRange_map_sum es L

private theorem impulseAt_sumRange (l r : Nat) (d : Int) (L : Nat) (hlr : l < r) :
    sumRange L (fun j => impulseAt (l, r, d) j) =
      if l < L ∧ L ≤ r then d else 0 := by
  induction L with
  | zero => simp [sumRange]
  | succ L ih =>
      rw [sumRange_succ]
      by_cases hLl : L < l
      · have h1 : impulseAt (l, r, d) L = 0 := by
          simp [impulseAt, hlr, hLl, show L ≠ r by omega, show L ≠ l by omega]
        have hc0 : ¬ (l < L ∧ L ≤ r) := by omega
        have hc1 : ¬ (l < L + 1 ∧ L + 1 ≤ r) := by omega
        rw [ih, h1]
        simp [hc0, hc1]
      · have hLge : l ≤ L := by omega
        by_cases hLr : L < r
        · by_cases hLeq : L = l
          · subst L
            have h1 : impulseAt (l, r, d) l = d := by simp [impulseAt, hlr]
            have hc0 : ¬ (l < l ∧ l ≤ r) := by omega
            have hc1 : (l < l + 1 ∧ l + 1 ≤ r) := by omega
            rw [ih, h1]
            simp [hc0, hc1]
          · have hLgt : l < L := by omega
            have h1 : impulseAt (l, r, d) L = 0 := by
              simp [impulseAt, hlr, show L ≠ l by omega, show L ≠ r by omega]
            have hc0 : (l < L ∧ L ≤ r) := by omega
            have hc1 : (l < L + 1 ∧ L + 1 ≤ r) := by omega
            rw [ih, h1]
            simp [hc0, hc1]
        · have hLrge : r ≤ L := by omega
          by_cases hLeq : L = r
          · subst L
            have h1 : impulseAt (l, r, d) r = -d := by
              simp [impulseAt, hlr, show r ≠ l by omega]
            have hc0 : (l < r ∧ r ≤ r) := by omega
            have hc1 : ¬ (l < r + 1 ∧ r + 1 ≤ r) := by omega
            rw [ih, h1]
            simp [hc0, hc1]
            omega
          · have h1 : impulseAt (l, r, d) L = 0 := by
              simp [impulseAt, hlr, show L ≠ l by omega, show L ≠ r by omega]
            have hc0 : ¬ (l < L ∧ L ≤ r) := by omega
            have hc1 : ¬ (l < L + 1 ∧ L + 1 ≤ r) := by omega
            rw [ih, h1]
            simp [hc0, hc1]

private theorem foldl_add_eq (f : Nat → Int) (l : List Nat) (a : Int) :
    l.foldl (fun s j => s + f j) a = a + (l.map f).sum := by
  induction l generalizing a with
  | nil => simp
  | cons x xs ih => simp [ih, Int.add_assoc]

theorem RangeEdits_sound : TinylambOpt.ResearchGoals.EncodedBlocks.RangeEdits.Goal := by
  intro xs es
  dsimp only
  let h := impulses es
  let adjustments := ((List.range xs.length).scanl (fun s i => s + h.getD i 0) 0).drop 1
  have hadj (i : Nat) (hi : i < xs.length) :
      adjustments[i]? = some (sumRange (i+1) (fun j => h.getD j 0)) := by
    dsimp [adjustments]
    rw [List.getElem?_drop]
    rw [List.getElem?_scanl]
    rw [if_pos (by simp; omega)]
    have htake : (List.range xs.length).take (1 + i) = List.range (i + 1) := by
      rw [List.take_range]
      have : min (1 + i) xs.length = i + 1 := by omega
      rw [this]
    rw [htake, foldl_add_eq]
    simp [sumRange]
  apply List.ext_getElem?
  intro i
  by_cases hi : i < xs.length
  · rcases hxi : xs[i]? with _ | x
    · have : xs.length ≤ i := List.getElem?_eq_none_iff.mp hxi
      omega
    · rw [List.getElem?_zipWith, hxi, hadj i hi]
      simp
      rw [hxi]
      simp
      rw [impulses_sumRange]
      have hsingle : ∀ e : Edit,
          sumRange (i+1) (fun j => impulseAt e j) =
            (let (l,r,d) := e; if l ≤ i ∧ i < r then d else 0) := by
        intro e
        rcases e with ⟨l, r, d⟩
        by_cases hlr : l < r
        · rw [impulseAt_sumRange l r d (i+1) hlr]
          by_cases hcond : l ≤ i ∧ i < r
          · have hc : l < i+1 ∧ i+1 ≤ r := by omega
            simp [hcond, hc]
          · have hc : ¬ (l < i+1 ∧ i+1 ≤ r) := by omega
            simp [hcond, hc]
        · have hn : ¬ (l ≤ i ∧ i < r) := by omega
          simpa [impulseAt, hlr, hn] using sumRange_zero (i+1)
      simp [hsingle]
  · have hge : xs.length ≤ i := by omega
    have hxs : xs[i]? = none := List.getElem?_eq_none hge
    rw [List.getElem?_zipWith, hxs]
    simp
    rw [hxs]
    simp

theorem SparseGather_sound : TinylambOpt.ResearchGoals.EncodedBlocks.SparseGather.Goal := by
  intro xs
  induction xs with
  | nil => intro i; simp [dense]
  | cons a as ih =>
      intro i
      cases i with
      | zero =>
          cases a <;> simp [dense]
      | succ j =>
          cases a with
          | none =>
              simpa [dense] using ih j
          | some x =>
              simpa [dense, List.getElem?_map] using ih j

private def spansFrom (off : Nat) : List Run → List (Nat × Nat × Int)
  | [] => []
  | (n,x)::rs => (off, off+n, x) :: spansFrom (off+n) rs

private def spansAcc (rs : List Run) (off : Nat) (acc : List (Nat × Nat × Int)) :
    Nat × List (Nat × Nat × Int) :=
  rs.foldl (fun (offset,acc) (n,x) => (offset+n,(offset,offset+n,x)::acc)) (off,acc)

private theorem spansAcc_append (rs : List Run) (off : Nat) (acc : List (Nat × Nat × Int)) :
    (spansAcc rs off acc).2 = (spansAcc rs off []).2 ++ acc := by
  induction rs generalizing off acc with
  | nil => rfl
  | cons q rs ih =>
      rcases q with ⟨n,x⟩
      simp only [spansAcc, List.foldl_cons]
      change (spansAcc rs (off+n) ((off,off+n,x)::acc)).2 =
        (spansAcc rs (off+n) [(off,off+n,x)]).2 ++ acc
      rw [ih, ih]
      rw [ih (off+n) [(off,off+n,x)]]
      simp


private theorem spans_eq_spansFrom (rs : List Run) : spans rs = spansFrom 0 rs := by
  unfold spans
  change (spansAcc rs 0 []).2.reverse = spansFrom 0 rs
  induction rs with
  | nil => rfl
  | cons q rs ih =>
      rcases q with ⟨n,x⟩
      simp only [spansAcc, List.foldl_cons]
      change ((spansAcc rs (0+n) [(0,0+n,x)]).2).reverse = _
      rw [spansAcc_append]
      simp only [List.reverse_append, List.reverse_cons, List.reverse_nil, List.nil_append]
      -- need spans from off n
      have hgen : ∀ (rss : List Run) (off : Nat), (spansAcc rss off []).2.reverse = spansFrom off rss := by
        intro rss off
        induction rss generalizing off with
        | nil => rfl
        | cons q rs ih2 =>
            rcases q with ⟨m,y⟩
            simp only [spansAcc, List.foldl_cons]
            change ((spansAcc rs (off+m) [(off,off+m,y)]).2).reverse = _
            rw [spansAcc_append]
            simp only [List.reverse_append, List.reverse_cons, List.reverse_nil, List.nil_append]
            rw [ih2 (off+m)]
            rfl
      rw [hgen rs (0+n)]
      rfl

private theorem spansFrom_add (m : Nat) (rs : List Run) :
    spansFrom m rs = (spansFrom 0 rs).map (fun (l,r,x) => (l+m, r+m, x)) := by
  induction rs generalizing m with
  | nil => rfl
  | cons q rs ih =>
      rcases q with ⟨n,x⟩
      simp only [spansFrom]
      simp only [Nat.zero_add]
      rw [ih (m+n), ih n]
      simp only [List.map_cons, List.map_map]
      congr 1
      · simp [Nat.add_comm, Nat.add_left_comm]
      · apply congrArg (fun f => List.map f (spansFrom 0 rs))
        funext t
        rcases t with ⟨u,v,z⟩
        simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem int_sum_sub {α : Type} (l : List α) (f g : α → Int) :
    (l.map fun x => f x - g x).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [ih]
      omega

private theorem int_sum_mul_left (l : List Int) (c : Int) :
    (l.map fun x => c * x).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem prefix_replicate (m i : Nat) (y : Int) :
    ((List.replicate m y).take i).sum = ((min i m : Nat) : Int) * y := by
  rw [List.take_replicate]
  induction (min i m) with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih]
      simp [Int.natCast_add, Int.add_mul]
      omega

private theorem prefix_append (xs ys : List Int) (i : Nat) :
    ((xs ++ ys).take i).sum = (xs.take i).sum + (ys.take (i - xs.length)).sum := by
  rw [List.take_append, List.sum_append]

private theorem take_drop_slice (xs : List Int) (l r : Nat) (hlr : l ≤ r) (hr : r ≤ xs.length) :
    ((xs.take r).drop l).sum = (xs.take r).sum - (xs.take l).sum := by
  have ht : (xs.take r).take l = xs.take l := by
    rw [List.take_take, Nat.min_eq_left hlr]
  have happ : xs.take r = (xs.take l) ++ ((xs.take r).drop l) := by
    conv => lhs; rw [← List.take_append_drop l (xs.take r)]
    rw [ht]
  have hsum := congrArg List.sum happ
  rw [List.sum_append] at hsum
  omega

private theorem prefix_unpack_spans (b : List Run) : ∀ i : Nat,
    ((unpack b).take i).sum =
      ((spansFrom 0 b).map fun (u,v,y) => ((min i v - u : Nat) : Int) * y).sum := by
  induction b with
  | nil => simp [unpack, spansFrom]
  | cons q b ih =>
      intro i
      rcases q with ⟨m,y⟩
      simp only [unpack, List.flatMap_cons, spansFrom]
      rw [prefix_append]
      rw [prefix_replicate]
      rw [show (List.replicate m y).length = m by simp]
      change ((min i m : Nat) : Int) * y + ((unpack b).take (i - m)).sum = _
      rw [ih (i - m)]
      simp only [Nat.zero_add]
      rw [spansFrom_add m b]
      simp only [List.map_cons, List.sum_cons, Nat.sub_zero]
      have hshift : ∀ (u v : Nat), min (i - m) v - u = min i (m+v) - (m+u) := by
        intro u v
        by_cases him : m ≤ i
        · omega
        · have : i - m = 0 := by omega
          rw [this]
          have hmin : min i (m+v) = i := by omega
          rw [hmin]
          omega
      congr 1
      rw [List.map_map]
      apply congrArg List.sum
      apply congrArg (fun f => List.map f (spansFrom 0 b))
      funext t
      rcases t with ⟨u,v,z⟩
      simp [hshift u v, Nat.add_comm, Nat.add_assoc, Nat.add_left_comm]

private theorem slice_eq_prefix (xs : List Int) (l r : Nat) (hlr : l ≤ r) :
    ((xs.take r).drop l).sum = (xs.take r).sum - (xs.take l).sum := by
  induction xs generalizing l r with
  | nil => simp
  | cons x xs ih =>
      cases l with
      | zero =>
          simp
      | succ l' =>
          cases r with
          | zero => omega
          | succ r' =>
              have hlr' : l' ≤ r' := by omega
              simp [ih l' r' hlr']
              omega

private theorem span_overlap_eq (l r u v : Nat) :
    (min r v - max l u : Nat) = (min r v - u) - (min l v - u) := by
  omega

private theorem slice_sum_spans (b : List Run) (l r : Nat) (hlr : l ≤ r) :
    (((unpack b).take r).drop l).sum =
      ((spansFrom 0 b).map fun (u,v,y) => ((min r v - max l u : Nat) : Int) * y).sum := by
  rw [slice_eq_prefix (unpack b) l r hlr]
  rw [prefix_unpack_spans b r, prefix_unpack_spans b l]
  rw [← int_sum_sub]
  apply congrArg List.sum
  apply congrArg (fun f => List.map f (spansFrom 0 b))
  funext t
  rcases t with ⟨u,v,y⟩
  simp only
  have hnat := span_overlap_eq l r u v
  have hle : min l v - u ≤ min r v - u := by omega
  rw [hnat, Int.ofNat_sub hle, Int.sub_mul]

private theorem zip_append_drop {α β : Type} (xs ys : List α) (zs : List β) :
    (xs ++ ys).zip zs = xs.zip zs ++ ys.zip (zs.drop xs.length) := by
  induction xs generalizing zs with
  | nil => simp
  | cons x xs ih =>
      cases zs with
      | nil => simp
      | cons z zs =>
          simp [List.zip_cons_cons, ih]


private theorem dot_replicate (n : Nat) (x : Int) (ys : List Int) :
    (((List.replicate n x).zip ys).map fun (p,q) => p*q).sum = x * (ys.take n).sum := by
  induction n generalizing ys with
  | zero => simp
  | succ n ih =>
      cases ys with
      | nil => simp
      | cons y ys =>
          rw [List.replicate_succ, List.zip_cons_cons]
          simp only [List.map_cons, List.sum_cons]
          rw [ih]
          rw [List.take_succ_cons]
          simp only [List.sum_cons]
          rw [Int.mul_add]

private theorem take_drop_shift (ys : List Int) (l u n : Nat) :
    ((ys.take (u+n)).drop (l+n)).sum = (((ys.drop n).take u).drop l).sum := by
  rw [List.drop_take]
  have hsub : u + n - (l + n) = u - l := by omega
  rw [hsub]
  rw [List.drop_take]
  rw [List.drop_drop]
  rw [Nat.add_comm l n]


private theorem dot_unpack_spans (a : List Run) (ys : List Int) :
    (((unpack a).zip ys).map fun (p,q) => p*q).sum =
      ((spansFrom 0 a).map fun (l,r,x) => x * (((ys.take r).drop l).sum)).sum := by
  induction a generalizing ys with
  | nil => simp [unpack, spansFrom]
  | cons q a ih =>
      rcases q with ⟨n,x⟩
      simp only [unpack, List.flatMap_cons, spansFrom]
      rw [zip_append_drop]
      rw [List.map_append, List.sum_append]
      rw [dot_replicate]
      rw [show (List.replicate n x).length = n by simp]
      change x * (ys.take n).sum +
          ((((unpack a).zip (ys.drop n)).map fun (p,q) => p*q).sum) = _
      rw [ih (ys.drop n)]
      simp only [List.map_cons, List.sum_cons, Nat.zero_add, Nat.sub_zero]
      rw [spansFrom_add n a]
      rw [List.map_map]
      congr 1
      apply congrArg List.sum
      apply congrArg (fun f => List.map f (spansFrom 0 a))
      funext t
      rcases t with ⟨l,r,z⟩
      simp only [Function.comp_apply]
      rw [take_drop_shift]

private theorem spansFrom_le {a : List Run} {l r : Nat} {x : Int}
    (h : (l,r,x) ∈ spansFrom 0 a) : l ≤ r := by
  induction a generalizing l r x with
  | nil => simp [spansFrom] at h
  | cons q a ih =>
      rcases q with ⟨n,z⟩
      simp only [spansFrom, Nat.zero_add, List.mem_cons] at h
      rcases h with heq | h
      · cases heq
        omega
      · have hadd := spansFrom_add n a
        rw [hadd] at h
        rcases List.mem_map.mp h with ⟨p, hp, heq⟩
        rcases p with ⟨l',r',x'⟩
        have hle := ih hp
        simp at heq
        omega

theorem OverlapDot_sound : TinylambOpt.ResearchGoals.EncodedBlocks.OverlapDot.Goal := by
  intro a b
  dsimp only
  rw [spans_eq_spansFrom a, spans_eq_spansFrom b]
  let inner := fun (l r : Nat) (x : Int) =>
    ((spansFrom 0 b).map fun (u,v,y) => ((min r v - max l u : Nat) : Int) * x * y).sum
  have hinner : ∀ (l r : Nat) (x : Int), l ≤ r →
      inner l r x = x * (((unpack b).take r).drop l).sum := by
    intro l r x hlr
    dsimp [inner]
    have hs := slice_sum_spans b l r hlr
    rw [hs, ← int_sum_mul_left]
    rw [List.map_map]
    apply congrArg List.sum
    apply congrArg (fun f => List.map f (spansFrom 0 b))
    funext t
    rcases t with ⟨u,v,y⟩
    simp only
    simp [Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
  change ((spansFrom 0 a).map fun (l,r,x) => inner l r x).sum =
    (((unpack a).zip (unpack b)).map fun (x,y) => x*y).sum
  have hL : ((spansFrom 0 a).map fun (l,r,x) => inner l r x).sum =
      ((spansFrom 0 a).map fun (l,r,x) => x * (((unpack b).take r).drop l).sum).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro t ht
    rcases t with ⟨l,r,x⟩
    simp only
    exact hinner l r x (spansFrom_le ht)
  rw [hL]
  exact (dot_unpack_spans a (unpack b)).symm

private def sel (mask : List Bool) : List Nat := (mask.zipIdx.filter Prod.fst).map Prod.snd

private theorem zipIdx_snd_ge (mask : List Bool) (start : Nat) {p : Bool × Nat}
    (hp : p ∈ mask.zipIdx start) : start ≤ p.2 := by
  induction mask generalizing start with
  | nil => simp at hp
  | cons b bs ih =>
      simp [List.zipIdx_cons] at hp
      rcases hp with rfl | hp
      · omega
      · have h := ih (start+1) hp
        omega

private theorem sel_sorted_aux (mask : List Bool) (start : Nat) :
    (((mask.zipIdx start).filter Prod.fst).map Prod.snd).Pairwise (· < ·) := by
  induction mask generalizing start with
  | nil => simp
  | cons b bs ih =>
      cases b
      · simpa [List.zipIdx_cons] using ih (start+1)
      · rw [List.zipIdx_cons]
        simp only [List.filter_cons, decide_true, Bool.true_eq, ↓reduceIte]
        rw [List.map_cons, List.pairwise_cons]
        constructor
        · intro q hq
          rcases List.mem_map.mp hq with ⟨p, hp, rfl⟩
          have hp' := (List.mem_filter.mp hp).1
          have hge := zipIdx_snd_ge bs (start+1) hp'
          omega
        · exact ih (start+1)

private theorem sel_sorted (mask : List Bool) : (sel mask).Pairwise (· < ·) := by
  simpa [sel] using sel_sorted_aux mask 0

private def cnt (mask : List Bool) (j : Nat) : Nat := (mask.take j).count true

private theorem cnt_succ (mask : List Bool) (j : Nat) :
    cnt mask (j+1) = cnt mask j + (if mask[j]?.getD false then 1 else 0) := by
  unfold cnt
  rw [List.take_succ, List.count_append]
  cases h : mask[j]? with
  | none => simp [h]
  | some b => cases b <;> simp [h]

private theorem cnt_mono (mask : List Bool) : ∀ a b : Nat, a ≤ b → cnt mask a ≤ cnt mask b := by
  intro a b h
  obtain ⟨c, rfl⟩ := Nat.exists_eq_add_of_le h
  unfold cnt
  rw [List.take_add, List.count_append]
  omega

private theorem bit_mem_sel (mask : List Bool) (j : Nat) :
    mask[j]?.getD false = true ↔ j ∈ sel mask := by
  unfold sel
  rw [List.mem_map]
  constructor
  · intro h
    refine ⟨(true, j), ?_, rfl⟩
    rw [List.mem_filter]
    constructor
    · rw [List.mem_zipIdx_iff_getElem?]
      cases hget : mask[j]? with
      | none => simp [hget] at h
      | some b => cases b <;> simp [hget] at h ⊢
    · simp
  · rintro ⟨p, hp, hpj⟩
    have hpf := (List.mem_filter.mp hp).1
    rw [List.mem_zipIdx_iff_getElem?] at hpf
    have hpt := (List.mem_filter.mp hp).2
    rw [hpj] at hpf
    rw [hpf]
    simp [hpt]

private theorem filter_eq_nil_of_not {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, ¬ p x = true) : l.filter p = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = false := by
        by_cases hpt : p x = true
        · exact False.elim ((h x (by simp)) hpt)
        · cases hpx : p x <;> simp_all
      have ht : ∀ y ∈ xs, ¬ p y = true := by intro y hy; exact h y (by simp [hy])
      rw [List.filter_cons]
      simp [hx, ih ht]

private theorem count_lt_succ_sorted {l : List Nat} (hs : l.Pairwise (· < ·)) (j : Nat) :
    (l.filter fun s => s < j+1).length =
      (l.filter fun s => s < j).length + (if j ∈ l then 1 else 0) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      obtain ⟨hagt, hstail⟩ := List.pairwise_cons.mp hs
      by_cases hlt : a < j
      · have hne : j ≠ a := by omega
        have hf1 : ((a :: as).filter fun s => s < j+1).length =
            1 + (as.filter fun s => s < j+1).length := by
          simp [show a < j+1 by omega]
          omega
        have hf2 : ((a :: as).filter fun s => s < j).length =
            1 + (as.filter fun s => s < j).length := by
          simp [hlt]
          omega
        rw [hf1, hf2, ih hstail]
        by_cases hm : j ∈ as
        · have hm' : j ∈ a :: as := by simp [hm]
          simp [hm, hm']
          omega
        · have hm' : ¬ j ∈ a :: as := by simp [hm, hne]
          simp [hm, hm']
      · have hge : j ≤ a := by omega
        by_cases heq : a = j
        · subst a
          have hfn1 : (as.filter fun s => s < j+1) = [] := by
            apply filter_eq_nil_of_not
            intro x hx
            have hxgt := hagt x hx
            simp [show ¬ x < j+1 by omega]
          have hfn2 : (as.filter fun s => s < j) = [] := by
            apply filter_eq_nil_of_not
            intro x hx
            have hxgt := hagt x hx
            simp [show ¬ x < j by omega]
          have hnotmem : ¬ j ∈ as := by
            intro hj
            have := hagt j hj
            omega
          simp [hfn1, hfn2, hnotmem]
        · have hjlt : j < a := by omega
          have hnot1 : ¬ a < j+1 := by omega
          have hnot2 : ¬ a < j := by omega
          by_cases hm : j ∈ as
          · have hm' : j ∈ a :: as := by simp [hm]
            simp [hnot1, hnot2, hm, hm', ih hstail]
          · have hm' : ¬ j ∈ a :: as := by simp [hm, show j ≠ a by omega]
            simp [hnot1, hnot2, hm, hm', ih hstail]

private theorem cnt_eq_filter_sel (mask : List Bool) (j : Nat) :
    cnt mask j = ((sel mask).filter fun s => s < j).length := by
  induction j with
  | zero =>
      simp [cnt]
      rw [show (sel mask).filter (fun _ => false) = [] by
        apply filter_eq_nil_of_not
        intro x hx
        simp]
      rfl
  | succ j ih =>
      rw [cnt_succ, ih, count_lt_succ_sorted (sel_sorted mask) j]
      have hiff := bit_mem_sel mask j
      by_cases hb : mask[j]?.getD false <;> by_cases hm : j ∈ sel mask <;> simp [hb, hm] at hiff ⊢

private theorem sel_lt_length (mask : List Bool) {s : Nat} (hs : s ∈ sel mask) : s < mask.length := by
  unfold sel at hs
  rcases List.mem_map.mp hs with ⟨p, hp, rfl⟩
  have hpf := (List.mem_filter.mp hp).1
  have hh := List.mem_zipIdx hpf
  simpa using hh.2.1

private theorem cnt_length_eq_sel (mask : List Bool) :
    cnt mask mask.length = (sel mask).length := by
  rw [cnt_eq_filter_sel]
  have hf : (sel mask).filter (fun s => s < mask.length) = sel mask := by
    apply List.filter_eq_self.mpr
    intro s hs
    exact decide_eq_true (by exact sel_lt_length mask hs)
  rw [hf]

private theorem count_lt_get {l : List Nat} (hs : l.Pairwise (· < ·)) :
    ∀ k (hk : k < l.length), (l.filter fun s => s < l[k]).length = k := by
  induction l with
  | nil => intro k hk; simp at hk
  | cons a as ih =>
      intro k hk
      cases k with
      | zero =>
          rw [List.getElem_cons_zero a as (by simp)]
          have hfn : (a :: as).filter (fun s => s < a) = [] := by
            rw [List.filter_cons]
            have hnot : ∀ x ∈ as, ¬ x < a := by
              intro x hx
              have h := (List.pairwise_cons.mp hs).1 x hx
              omega
            simp [show ¬ a < a by omega]
            intro x hx
            have hxnot := hnot x hx
            omega
          rw [hfn]
          rfl
      | succ k =>
          rw [List.getElem_cons_succ a as k (by simpa using hk)]
          obtain ⟨hagt, hstail⟩ := List.pairwise_cons.mp hs
          have hk' : k < as.length := by simpa using hk
          have hak : a < as[k] := by
            exact hagt (as[k]) (List.getElem_mem hk')
          have hf : (a :: as).filter (fun s => s < as[k]) =
              a :: (as.filter fun s => s < as[k]) := by
            rw [List.filter_cons]
            simp [hak]
          rw [hf, List.length_cons, ih hstail k hk']

private theorem count_le_get {l : List Nat} (hs : l.Pairwise (· < ·)) :
    ∀ k (hk : k < l.length), (l.filter fun s => s ≤ l[k]).length = k + 1 := by
  induction l with
  | nil => intro k hk; simp at hk
  | cons a as ih =>
      intro k hk
      cases k with
      | zero =>
          rw [List.getElem_cons_zero a as (by simp)]
          have hf : (a :: as).filter (fun s => s ≤ a) = [a] := by
            rw [List.filter_cons]
            have htail : (as.filter fun s => s ≤ a) = [] := by
              apply filter_eq_nil_of_not
              intro x hx
              have h := (List.pairwise_cons.mp hs).1 x hx
              intro hxt
              have hxa : x ≤ a := of_decide_eq_true hxt
              omega
            simp [htail]
          rw [hf]
          rfl
      | succ k =>
          rw [List.getElem_cons_succ a as k (by simpa using hk)]
          obtain ⟨hagt, hstail⟩ := List.pairwise_cons.mp hs
          have hk' : k < as.length := by simpa using hk
          have hak : a ≤ as[k] := by
            have := hagt (as[k]) (List.getElem_mem hk')
            omega
          have hf : (a :: as).filter (fun s => s ≤ as[k]) =
              a :: (as.filter fun s => s ≤ as[k]) := by
            rw [List.filter_cons]
            simp [hak]
          rw [hf, List.length_cons, ih hstail k hk']

private theorem count_lt_mono (l : List Nat) (t t' : Nat) (h : t ≤ t') :
    (l.filter fun s => s < t).length ≤ (l.filter fun s => s < t').length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x < t
      · have hx' : x < t' := by omega
        simp [hx, hx', ih]
      · by_cases hx' : x < t'
        · simp [hx, hx', ih]
          omega
        · simp [hx, hx', ih]

private theorem filter_lt_succ_eq_le (l : List Nat) (i : Nat) :
    (l.filter fun s => s < i+1) = (l.filter fun s => s ≤ i) := by
  apply filter_congr
  intro s
  by_cases h : s ≤ i <;> simp [h] <;> omega

private def IsAns (mask : List Bool) (k i : Nat) : Prop :=
  (∀ u, u < i → cnt mask (u+1) ≤ k) ∧
    (i = mask.length ∨ (i < mask.length ∧ k < cnt mask (i+1)))

private theorem sel_get_spec (mask : List Bool) (k : Nat) (hk : k < (sel mask).length) :
    IsAns mask k (sel mask)[k] := by
  have hs := sel_sorted mask
  have hi_lt : (sel mask)[k] < mask.length :=
    sel_lt_length mask (List.getElem_mem hk)
  constructor
  · intro u hu
    rw [cnt_eq_filter_sel]
    have hut : u + 1 ≤ (sel mask)[k] := by omega
    have hle := count_lt_mono (sel mask) (u+1) ((sel mask)[k]) hut
    rw [count_lt_get hs k hk] at hle
    exact hle
  · right
    constructor
    · exact hi_lt
    · rw [cnt_eq_filter_sel, filter_lt_succ_eq_le, count_le_get hs k hk]
      omega

private theorem foldl_count (l : List Bool) (a : Nat) :
    l.foldl (fun n bit => n + bit.toNat) a = a + l.count true := by
  induction l generalizing a with
  | nil => simp
  | cons b bs ih =>
      cases b <;> simp [ih, Nat.add_assoc] <;> omega

private theorem ranks_get (mask : List Bool) (j : Nat) (hj : j ≤ mask.length) :
    ((((mask.scanl (fun n bit => n + bit.toNat) 0).toArray)[j]?).getD 0) = cnt mask j := by
  rw [List.getElem?_toArray, List.getElem?_scanl]
  rw [if_pos hj]
  simp [foldl_count, cnt]

private theorem pow_succ_two (n : Nat) : 2 ^ (n + 1) = 2 * 2 ^ n := by
  rw [show n + 1 = Nat.succ n by omega, Nat.pow_succ]
  omega

private theorem isAns_unique (mask : List Bool) (k t₁ t₂ : Nat)
    (h₁ : IsAns mask k t₁) (h₂ : IsAns mask k t₂) : t₁ = t₂ := by
  unfold IsAns at h₁ h₂
  by_cases hlt : t₁ < t₂
  · rcases h₁.2 with ht1 | ⟨ht1n, hkc⟩
    · subst t₁
      rcases h₂.2 with ht2 | ⟨ht2n, _⟩
      · subst t₂; rfl
      · omega
    · have := h₂.1 t₁ hlt
      omega
  · by_cases hgt : t₂ < t₁
    · rcases h₂.2 with ht2 | ⟨ht2n, hkc⟩
      · subst t₂
        rcases h₁.2 with ht1 | ⟨ht1n, _⟩
        · subst t₁; rfl
        · omega
      · have := h₁.1 t₂ hgt
        omega
    · omega

private theorem locate_is_answer (mask : List Bool) (k : Nat) :
    let ranks := (mask.scanl (fun n bit => n + bit.toNat) 0).toArray
    ∀ fuel lo hi : Nat,
      lo ≤ hi → hi ≤ mask.length → hi < lo + 2 ^ fuel →
      (∀ u, u < lo → cnt mask (u+1) ≤ k) →
      (hi = mask.length ∨ k < cnt mask (hi+1)) →
      IsAns mask k (locate ranks k fuel lo hi) := by
  dsimp only
  intro fuel
  induction fuel with
  | zero =>
      intro lo hi hle hbound hw hbefore hhi
      have heq : hi = lo := by
        simp at hw
        omega
      subst hi
      simp only [locate]
      constructor
      · exact hbefore
      · rcases hhi with rfl | hhi
        · exact Or.inl rfl
        · by_cases hln : lo = mask.length
          · exact Or.inl hln
          · exact Or.inr ⟨by omega, hhi⟩
  | succ fuel ih =>
      intro lo hi hle hbound hw hbefore hhi
      by_cases hge : lo ≥ hi
      · have heq : hi = lo := by omega
        subst hi
        simp only [locate, if_pos hge]
        constructor
        · exact hbefore
        · rcases hhi with rfl | hhi
          · exact Or.inl rfl
          · by_cases hln : lo = mask.length
            · exact Or.inl hln
            · exact Or.inr ⟨by omega, hhi⟩
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hmid_lo : lo ≤ mid := by dsimp [mid]; omega
        have hmid_hi : mid < hi := by dsimp [mid]; omega
        have hmid1 : mid + 1 ≤ mask.length := by omega
        have hrk : ((mask.scanl (fun n bit => n + bit.toNat) 0)[mid+1]?).getD 0 =
            cnt mask (mid+1) := by
          simpa [List.getElem?_toArray] using ranks_get mask (mid+1) hmid1
        by_cases hbranch : ((mask.scanl (fun n bit => n + bit.toNat) 0)[mid+1]?).getD 0 ≤ k
        · have hcnt : cnt mask (mid+1) ≤ k := by simpa [hrk] using hbranch
          have hbefore' : ∀ u, u < mid+1 → cnt mask (u+1) ≤ k := by
            intro u hu
            have hum : u+1 ≤ mid+1 := by omega
            exact Nat.le_trans (cnt_mono mask (u+1) (mid+1) hum) hcnt
          rw [pow_succ_two] at hw
          have hw' : hi < (mid+1) + 2^fuel := by
            dsimp [mid] at *
            omega
          have hres := ih (mid+1) hi (by omega) hbound hw' hbefore' hhi
          simpa [locate, List.getElem?_toArray, hge, mid, hbranch] using hres
        · have hcntgt : k < cnt mask (mid+1) := by
            have : ¬ cnt mask (mid+1) ≤ k := by simpa [hrk] using hbranch
            omega
          rw [pow_succ_two] at hw
          have hw' : mid < lo + 2^fuel := by
            dsimp [mid] at *
            omega
          have hres := ih lo mid (by omega) (by omega) hw' hbefore (Or.inr hcntgt)
          simpa [locate, List.getElem?_toArray, hge, mid, hbranch] using hres

theorem SelectDirectory_sound : TinylambOpt.ResearchGoals.EncodedBlocks.SelectDirectory.Goal := by
  intro mask k
  dsimp only
  change (if (locate (mask.scanl (fun n bit => n + bit.toNat) 0).toArray k
        (mask.length.log2+2) 0 mask.length) < mask.length then
      some (locate (mask.scanl (fun n bit => n + bit.toNat) 0).toArray k
        (mask.length.log2+2) 0 mask.length) else none) = (sel mask)[k]?
  have hltpow : mask.length < 2 ^ (mask.length.log2 + 2) := by
    have hlt : mask.length < 2 ^ (mask.length.log2 + 1) := Nat.lt_log2_self
    have hle : 2 ^ (mask.length.log2 + 1) ≤ 2 ^ (mask.length.log2 + 2) := by
      rw [show mask.length.log2 + 2 = (mask.length.log2 + 1) + 1 by omega, pow_succ_two]
      omega
    omega
  have hans : IsAns mask k
      (locate (mask.scanl (fun n bit => n + bit.toNat) 0).toArray k
        (mask.length.log2+2) 0 mask.length) := by
    apply locate_is_answer
    · omega
    · omega
    · simpa using hltpow
    · intro u hu
      omega
    · exact Or.inl rfl
  rcases hans.2 with hiEq | ⟨hiLt, hkcnt⟩
  · have hkge : (sel mask).length ≤ k := by
      rw [← cnt_length_eq_sel]
      cases hn : mask.length with
      | zero => simp [cnt, hn]
      | succ n =>
          have h := hans.1 n (by omega)
          simpa [hn, Nat.succ_eq_add_one] using h
    rw [if_neg (by omega)]
    exact (List.getElem?_eq_none hkge).symm
  · have hklt : k < (sel mask).length := by
      rw [← cnt_length_eq_sel]
      have hle := cnt_mono mask
        (locate (mask.scanl (fun n bit => n + bit.toNat) 0).toArray k
          (mask.length.log2+2) 0 mask.length + 1) mask.length (by omega)
      omega
    have hansSel := sel_get_spec mask k hklt
    have heq : (sel mask)[k] =
        locate (mask.scanl (fun n bit => n + bit.toNat) 0).toArray k
          (mask.length.log2+2) 0 mask.length := isAns_unique mask k _ _ hansSel hans
    rw [if_pos hiLt]
    have hsomespec : (sel mask)[k]? = some (sel mask)[k] :=
      List.getElem?_eq_some_iff.mpr ⟨hklt, rfl⟩
    rw [hsomespec, heq]

private theorem nodup_eraseDups (l : List Int) : l.eraseDups.Nodup := by
  let P : Nat → Prop := fun n => ∀ l : List Int, l.length = n → l.eraseDups.Nodup
  have h : ∀ n, P n := fun n => Nat.strongRecOn n (fun n ih l hl => by
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        refine List.nodup_cons.mpr ⟨?_, ?_⟩
        · intro hm
          have hma : a ∈ as.filter (fun b => ! b == a) := (List.mem_eraseDups).mp hm
          have hf := (List.mem_filter.mp hma).2
          simp at hf
        · apply ih (as.filter (fun b => ! b == a)).length
          · have hl' : as.length + 1 = n := by simpa using hl
            have : as.length < n := by omega
            exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) this
          · rfl)
  exact h l.length l rfl

private theorem nodup_length_eq_of_mem_iff {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l₁ : List α}, l₁.Nodup → ∀ l₂ : List α, l₂.Nodup →
      (∀ a, a ∈ l₁ ↔ a ∈ l₂) → l₁.length = l₂.length := by
  intro l₁ d₁
  induction l₁ with
  | nil =>
      intro l₂ d₂ h
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ d₂ h
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp d₁
      have ha : a ∈ l₂ := (h a).mp (@List.mem_cons_self _ a r)
      have hd : (l₂.erase a).Nodup := d₂.erase a
      have hiff : ∀ c, c ∈ r ↔ c ∈ l₂.erase a := by
        intro c
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hr
          exact ⟨fun hca => hna (hca ▸ hr), (h c).mp (List.mem_cons_of_mem a hr)⟩
        · rintro ⟨hca, hc⟩
          rcases List.mem_cons.mp ((h c).mpr hc) with rfl | hr
          · exact absurd rfl hca
          · exact hr
      have hpos : 0 < l₂.length := by
        cases l₂ with
        | nil => exact absurd ha List.not_mem_nil
        | cons b bs => exact Nat.succ_pos _
      have hlen : (l₂.erase a).length + 1 = l₂.length := by
        rw [List.length_erase_of_mem ha]
        exact Nat.sub_add_cancel (Nat.succ_le_of_lt hpos)
      rw [List.length_cons, ih hnd (l₂.erase a) hd hiff, hlen]

private theorem increasing_eq_of_mem_iff {l₁ l₂ : List Nat}
    (h₁ : l₁.Pairwise (· < ·)) (h₂ : l₂.Pairwise (· < ·))
    (hmem : ∀ q, q ∈ l₁ ↔ q ∈ l₂) : l₁ = l₂ := by
  revert l₂
  induction l₁ with
  | nil =>
      intro l₂ h₂ hmem
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((hmem b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ h₂ hmem
      obtain ⟨haft, hrinc⟩ := List.pairwise_cons.mp h₁
      cases l₂ with
      | nil => exact absurd ((hmem a).mp List.mem_cons_self) List.not_mem_nil
      | cons b bs =>
          obtain ⟨hbft, hbinc⟩ := List.pairwise_cons.mp h₂
          have hba : b = a := by
            by_cases hba' : b = a
            · exact hba'
            · exfalso
              rcases Nat.lt_trichotomy b a with hlt | heq | hgt
              · have hbmem : b ∈ a :: r := (hmem b).mpr List.mem_cons_self
                rcases List.mem_cons.mp hbmem with hba'' | hbr
                · exact hba' hba''
                · have hab := haft b hbr
                  omega
              · exact hba' heq
              · have hamem : a ∈ b :: bs := (hmem a).mp List.mem_cons_self
                rcases List.mem_cons.mp hamem with hab' | hbs
                · exact hba' hab'.symm
                · have hbb := hbft a hbs
                  omega
          subst b
          have hmem' : ∀ q, q ∈ r ↔ q ∈ bs := by
            intro q
            have hq : (q = a ∨ q ∈ r) ↔ (q = a ∨ q ∈ bs) := by
              simpa [List.mem_cons] using hmem q
            constructor
            · intro hqr
              have hne : q ≠ a := by
                intro hqa
                subst q
                exact Nat.lt_irrefl a (haft a hqr)
              rcases (hq.mp (Or.inr hqr)) with hqa | hqs
              · exact absurd hqa hne
              · exact hqs
            · intro hqs
              have hne : q ≠ a := by
                intro hqa
                subst q
                exact Nat.lt_irrefl a (hbft a hqs)
              rcases (hq.mpr (Or.inr hqs)) with hqa | hqr
              · exact absurd hqa hne
              · exact hqr
          have htail : r = bs := ih hrinc hbinc hmem'
          rw [htail]

private theorem eraseDups_cons_length (ys : List Int) (x : Int) :
    (x :: ys).eraseDups.length = ys.eraseDups.length + (if x ∈ ys then 0 else 1) := by
  by_cases hxy : x ∈ ys
  · have hlen := nodup_length_eq_of_mem_iff (nodup_eraseDups (x :: ys)) ys.eraseDups
      (nodup_eraseDups ys) (fun z => by
        constructor
        · intro hz
          rcases List.mem_cons.mp ((List.mem_eraseDups).mp hz) with rfl | hzys
          · exact (List.mem_eraseDups).mpr hxy
          · exact (List.mem_eraseDups).mpr hzys
        · intro hz
          apply (List.mem_eraseDups).mpr
          exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hz)))
    rw [hlen, if_pos hxy, Nat.add_zero]
  · have hnody : (x :: ys.eraseDups).Nodup := by
      refine List.nodup_cons.mpr ⟨?_, nodup_eraseDups ys⟩
      intro hm
      exact hxy ((List.mem_eraseDups).mp hm)
    have hlen := nodup_length_eq_of_mem_iff (nodup_eraseDups (x :: ys)) (x :: ys.eraseDups) hnody
      (fun z => by
        constructor
        · intro hz
          have hz' : z = x ∨ z ∈ ys := List.mem_cons.mp ((List.mem_eraseDups).mp hz)
          rcases hz' with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mpr hzys))
        · intro hz
          apply (List.mem_eraseDups).mpr
          rcases List.mem_cons.mp hz with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hzys)))
    rw [hlen, List.length_cons, if_neg hxy]


private def adjEq (l : List Int) : Nat := (l.zip (l.drop 1)).countP (fun p => p.1 == p.2)

private theorem adjEq_cons (x : Int) (ys : List Int) :
    adjEq (x :: ys) = adjEq ys + (if ys.head? == some x then 1 else 0) := by
  cases ys with
  | nil => simp [adjEq]
  | cons y zs =>
      by_cases hxy : x = y
      · subst y
        simp [adjEq, List.countP_cons]
      · have h1 : (x == y) = false := by simp [hxy]
        have hyx : y ≠ x := by intro h; exact hxy h.symm
        have h2 : (y == x) = false := by simpa using hyx
        simp [adjEq, List.countP_cons, h1, h2]

private theorem adjEq_le_length (l : List Int) : adjEq l ≤ l.length := by
  unfold adjEq
  have h := List.countP_le_length (p := fun p : Int × Int => p.1 == p.2)
    (l := l.zip (l.drop 1))
  simp only [List.length_zip, List.length_drop] at h
  omega

private theorem head_le_of_mem_sorted {x y : Int} {ys : List Int}
    (h : (x :: ys).Pairwise (· ≤ ·)) (hy : y ∈ x :: ys) : x ≤ y := by
  obtain ⟨hall, _⟩ := List.pairwise_cons.mp h
  rcases List.mem_cons.mp hy with rfl | hy
  · omega
  · exact hall y hy

private theorem eraseDups_length_sorted (l : List Int) (h : l.Pairwise (· ≤ ·)) :
    l.eraseDups.length = l.length - adjEq l := by
  induction l with
  | nil => simp [adjEq]
  | cons x ys ih =>
      have hys : ys.Pairwise (· ≤ ·) := h.of_cons
      have hxle : ∀ y ∈ ys, x ≤ y := (List.pairwise_cons.mp h).1
      rw [eraseDups_cons_length]
      rw [ih hys]
      rw [adjEq_cons]
      by_cases hxmem : x ∈ ys
      · have hind : (if ys.head? == some x then 1 else 0) = 1 := by
          cases ys with
          | nil => simp at hxmem
          | cons y zs =>
              have hyx : y ≤ x := head_le_of_mem_sorted h.of_cons hxmem
              have hxy : x ≤ y := hxle y (by simp)
              have hyeq : y = x := by omega
              simp [hyeq]
        rw [if_pos hxmem, hind]
        have hb := adjEq_le_length ys
        simp only [List.length_cons]
        omega
      · have hind : (if ys.head? == some x then 1 else 0) = 0 := by
          cases ys with
          | nil => simp
          | cons y zs =>
              by_cases hyx : y == x
              · have hyeq : y = x := by simpa using hyx
                have : x ∈ y :: zs := by simp [hyeq]
                exact absurd this hxmem
              · simp [hyx]
        rw [if_neg hxmem, hind]
        have hb := adjEq_le_length ys
        simp only [List.length_cons]
        omega

private def seam (l1 l2 : List Int) : Nat :=
  match l1.getLast?, l2.head? with
  | some a, some b => if a == b then 1 else 0
  | _, _ => 0

private theorem adjEq_append (l1 l2 : List Int) :
    adjEq (l1 ++ l2) = adjEq l1 + adjEq l2 + seam l1 l2 := by
  induction l1 with
  | nil =>
      cases l2 with
      | nil => simp [adjEq, seam]
      | cons z zs => simp [adjEq, seam]
  | cons x xs ih =>
      cases xs with
      | nil =>
          cases l2 with
          | nil => simp [adjEq, adjEq_cons, seam]
          | cons z zs =>
              rw [show (x :: []) ++ (z :: zs) = x :: z :: zs by rfl]
              rw [adjEq_cons]
              by_cases h : x = z
              · subst z
                simp [adjEq, seam]
              · have hz : z ≠ x := by intro hz; exact h hz.symm
                simp [adjEq, seam, h, hz]
      | cons y ys =>
          rw [show (x :: y :: ys) ++ l2 = x :: ((y :: ys) ++ l2) by rfl]
          rw [adjEq_cons x ((y :: ys) ++ l2)]
          rw [ih]
          rw [adjEq_cons x (y :: ys)]
          have hhead : (((y :: ys) ++ l2).head?) = (y :: ys).head? := by
            simp
          rw [hhead]
          have hlast : (x :: y :: ys).getLast? = (y :: ys).getLast? := by
            rw [List.getLast?_cons]
            cases h : (y :: ys).getLast? with
            | none =>
                exfalso
                exact absurd (List.getLast?_eq_none_iff.mp h) (by simp)
            | some z => simp [h]
          simp [seam, hlast]
          omega

private theorem sublist_flatten_of_mem {p : List Int} {pages : List (List Int)}
    (hp : p ∈ pages) : p.Sublist pages.flatten := by
  induction pages with
  | nil => simp at hp
  | cons q qs ih =>
      simp at hp
      rcases hp with rfl | hp
      · simpa [List.flatten] using (List.Sublist.refl p).append_right qs.flatten
      · simpa [List.flatten] using (List.Sublist.append (List.nil_sublist q) (ih hp))

private theorem cross_le_of_flatten_pairwise {p q : List Int} {qs : List (List Int)}
    (h : (p :: q :: qs).flatten.Pairwise (· ≤ ·)) :
    ∀ a ∈ p, ∀ b ∈ q, a ≤ b := by
  have hpq : List.Pairwise (· ≤ ·) (p ++ (q :: qs).flatten) := by
    simpa [List.flatten] using h
  have hparts := (List.pairwise_append.mp hpq).2.2
  intro a ha b hb
  exact hparts a ha b (by simp [hb])

private theorem seam_flatten {p q : List Int} {ps : List (List Int)}
    (hp : p ≠ []) (hq : q ≠ []) :
    seam p (q :: ps).flatten = if p.getLast? == q.head? then 1 else 0 := by
  cases q with
  | nil => contradiction
  | cons z zs =>
      cases hlast : p.getLast? <;> simp [seam, hlast]

private theorem adjEq_flatten_pages (ps : List (List Int))
    (hall : ∀ p ∈ ps, p ≠ [])
    (hsorted : ps.flatten.Pairwise (· ≤ ·)) :
    adjEq ps.flatten = (ps.map adjEq).sum +
      (ps.zip (ps.drop 1)).countP (fun (a,b) => a.getLast? == b.head?) := by
  induction ps with
  | nil => simp [adjEq]
  | cons p ps ih =>
      cases ps with
      | nil => simp [adjEq, seam]
      | cons q ps =>
          have hparts := List.pairwise_append.mp (by
            simpa [List.flatten] using hsorted :
              List.Pairwise (fun a b : Int => a ≤ b) (p ++ (q :: ps).flatten))
          have hpp : p.Pairwise (· ≤ ·) := hparts.1
          have hrest : (q :: ps).flatten.Pairwise (· ≤ ·) := hparts.2.1
          have ih' := ih (by intro r hr; exact hall r (by simp [hr])) hrest
          have hcr := cross_le_of_flatten_pairwise hsorted
          have hnonp : p ≠ [] := hall p (by simp)
          have hnonq : q ≠ [] := hall q (by simp)
          rw [show (p :: q :: ps).flatten = p ++ (q :: ps).flatten by rfl]
          rw [adjEq_append]
          rw [ih']
          rw [seam_flatten hnonp hnonq]
          simp only [List.drop_cons, List.zip_cons_cons, List.countP_cons]
          by_cases hs : p.getLast? == q.head? <;> simp [hs] <;> omega

private theorem flatten_filter_nonempty (pages : List (List Int)) :
    (pages.filter (fun p => !p.isEmpty)).flatten = pages.flatten := by
  induction pages with
  | nil => rfl
  | cons p ps ih =>
      cases p <;> simp [ih]

private theorem sum_erase_filter_nonempty (pages : List (List Int)) :
    ((pages.filter (fun p => !p.isEmpty)).map fun p => p.eraseDups.length).sum =
      (pages.map fun p => p.eraseDups.length).sum := by
  induction pages with
  | nil => rfl
  | cons p ps ih =>
      cases p <;> simp [ih]

private theorem length_sum_pages (ps : List (List Int)) :
    ps.flatten.length = ((ps.map fun p => p.length).sum) := by
  induction ps with
  | nil => rfl
  | cons p ps ih => simp [ih]

private theorem erase_sum_pages (ps : List (List Int))
    (hall : ∀ p ∈ ps, p ≠ []) (hsorted : ps.flatten.Pairwise (· ≤ ·)) :
    ((ps.map fun p => p.length).sum) =
      ((ps.map fun p => p.eraseDups.length).sum) + (ps.map adjEq).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      have hpne : p ≠ [] := hall p (by simp)
      have hparts := List.pairwise_append.mp (by
        simpa [List.flatten] using hsorted :
          List.Pairwise (fun a b : Int => a ≤ b) (p ++ ps.flatten))
      have hpp : p.Pairwise (· ≤ ·) := hparts.1
      have hrest : ps.flatten.Pairwise (· ≤ ·) := hparts.2.1
      have ih' := ih (by intro r hr; exact hall r (by simp [hr])) hrest
      have hpErase := eraseDups_length_sorted p hpp
      have hb := adjEq_le_length p
      simp only [List.map_cons, List.sum_cons]
      omega

theorem DistinctSeams_sound : TinylambOpt.ResearchGoals.EncodedBlocks.DistinctSeams.Goal := by
  intro pages hpair
  dsimp only
  let ps := pages.filter fun p => !p.isEmpty
  have hfl : ps.flatten = pages.flatten := by
    simpa [ps] using flatten_filter_nonempty pages
  have hsum : (ps.map fun p => p.eraseDups.length).sum =
      (pages.map fun p => p.eraseDups.length).sum := by
    simpa [ps] using sum_erase_filter_nonempty pages
  have hpsSorted : ps.flatten.Pairwise (· ≤ ·) := by
    rw [hfl]
    exact hpair
  have hnon : ∀ p ∈ ps, p ≠ [] := by
    intro p hp
    have hp' := (List.mem_filter.mp hp).2
    intro hnil
    subst p
    simp at hp'
  rw [← hsum, ← hfl]
  change ((ps.map fun p => p.eraseDups.length).sum -
      (ps.zip (ps.drop 1)).countP (fun (a,b) => a.getLast? == b.head?)) =
    ps.flatten.eraseDups.length
  have hAdj := adjEq_flatten_pages ps hnon hpsSorted
  have hGlob := eraseDups_length_sorted ps.flatten hpsSorted
  have hLen := length_sum_pages ps
  have hErase := erase_sum_pages ps hnon hpsSorted
  rw [hGlob, hLen, hErase, hAdj]
  have hb := adjEq_le_length ps.flatten
  omega

end TinylambOpt.ResearchProofs.Wave7EncodedBlocks
