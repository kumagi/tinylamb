import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection


/-! ## Generic helpers -/

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

/-! ## SeparatedLengths specific -/

namespace SeparatedLengths

private theorem little_length (n k : Nat) : (little n k).length = k := by
  simp [little]

private theorem sizes_length (xs : List Nat) : (sizes xs).length = xs.length := by
  simp [sizes]

private theorem pack_length (w : Nat) (xs : List Nat) : (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little_length]

theorem sound_ControlBytes : ControlBytes.Goal := by
  intro xs _hadm
  simp [SeparatedLengths.write, List.length_append, pack_length, sizes_length, List.length_map, List.length_flatMap]
  congr 1
  · rw [Nat.mul_comm]
  · -- sum of lengths in flatMap
    have hlen : (sizes xs).length = xs.length := sizes_length xs
    have hmap : ((xs.zip (sizes xs)).map (fun x => (little x.1 x.2).length)).sum = (sizes xs).sum := by
      have h2 : (xs.zip (sizes xs)).map (fun x => x.2) = sizes xs := by
        rw [List.map_zip_right (fun _ x => x)]
        · simp
        · rw [hlen]
      simp [little_length, h2]
    simpa [Function.comp, little_length] using hmap

theorem sound_FourControls : FourControls.Goal := by
  intro a b ha4 _hadm
  -- write(a++b).length = write(a).length + write(b).length
  -- control part: pack 2 over (sizes(a++b)).map(·-1) = pack 2 over (sizes a ... ++ sizes b ...)
  -- data part: flatMap over zip splits
  -- key: a.length % 4 = 0 => control bytes align
  simp only [SeparatedLengths.write, List.length_append]
  have hsa : (sizes (a ++ b)) = sizes a ++ sizes b := by simp [sizes, List.map_append]
  have hlenA : (sizes a).length = a.length := sizes_length a
  have hlenB : (sizes b).length = b.length := sizes_length b
  -- data part splits
  have hdata : ((a ++ b).zip (sizes (a ++ b))).flatMap (fun (v,w) => little v w) =
      (a.zip (sizes a)).flatMap (fun (v,w) => little v w) ++ (b.zip (sizes b)).flatMap (fun (v,w) => little v w) := by
    rw [hsa, List.zip_append (by rw [hlenA]), List.flatMap_append]
  rw [hdata, List.length_append]
  -- control part splits: pack 2 of (ma++mb) where ma.length = a.length %4==0
  -- pack w xs = little (word w xs) (byteCount (xs.length*w))
  -- when (a.length*2) % 8 == 0, byteCount splits and word splits
  have hmod : (a.length * 2) % 8 = 0 := by omega
  have hbc : byteCount (((sizes a).map (·-1)).length * 2 + ((sizes b).map (·-1)).length * 2) =
      byteCount (((sizes a).map (·-1)).length * 2) + byteCount (((sizes b).map (·-1)).length * 2) := by
    have hla : ((sizes a).map (·-1)).length = a.length := by simp [sizes_length]
    have hlb : ((sizes b).map (·-1)).length = b.length := by simp [sizes_length]
    rw [hla, hlb]
    have h8 : a.length * 2 = 8 * (a.length * 2 / 8) := by omega
    rw [h8]
    rw [Nat.add_comm (8 * (a.length * 2 / 8)) (b.length * 2)]
    unfold byteCount
    omega
  -- easier: just use pack_length and byteCount additivity
  simp only [List.map_append, List.length_append, pack_length, List.length_map, sizes_length]
  have hbc2 : byteCount (a.length * 2 + b.length * 2) = byteCount (a.length * 2) + byteCount (b.length * 2) := by
    have h8 : a.length * 2 = 8 * (a.length / 4) := by omega
    rw [h8, Nat.add_comm (8 * (a.length / 4)) (b.length * 2)]
    unfold byteCount
    omega
  omega

end SeparatedLengths

/-! ## ConstantExceptions specific -/

namespace ConstantExceptions

private theorem zipIdx_replicate_filter_eq (x : Nat) (n : Nat) :
    ((List.replicate n x).zipIdx.filter (fun (v,_) => v != x)) = [] := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp [List.replicate, List.zipIdx_cons, List.filter_cons, ih]

theorem sound_ConstantCharge : ConstantCharge.Goal := by
  intro x n
  cases n with
  | zero =>
      simp [ConstantExceptions.write, little_length, uleb_len_eq_ubytes, ubytes, width]
  | succ n =>
      have hf : ((List.replicate (n+1) x).zipIdx.filter (fun (v,_) => v != x)) = [] :=
        zipIdx_replicate_filter_eq x (n+1)
      simp [ConstantExceptions.write, hf, little_length, uleb_len_eq_ubytes, ubytes, width]

end ConstantExceptions

/-! ## DictionaryRuns specific -/

namespace DictionaryRuns

theorem sound_RepeatCliff : RepeatCliff.Goal := by
  intro x n hn
  cases n with
  | zero => omega
  | succ n =>
      simp [DictionaryRuns.write, List.eraseDups, runs, List.idxOf, uleb_len_eq_ubytes, fixed, little_length, List.flatMap, List.map]
      -- uleb 1 + 4 + (uleb 0 + uleb (n+1)) = 6 + ubytes (n+1)
      simp [ubytes, width]
      omega

end DictionaryRuns

/-! ## PatchedOctet specific -/

namespace PatchedOctet

theorem sound_EscapeCost : EscapeCost.Goal := by
  intro xs
  induction xs with
  | nil => simp [PatchedOctet.write]
  | cons v xs ih =>
      by_cases hv : v < 255
      · simp [PatchedOctet.write, hv, ih]
      · have hv' : ¬ v < 255 := hv
        have hge : 255 ≤ v := Nat.le_of_not_lt hv
        simp [PatchedOctet.write, hv', hge, uleb_len_eq_ubytes, ih]
        omega

theorem sound_EscapeCliff : EscapeCliff.Goal := by
  intro v
  by_cases hv : v < 255 <;> simp [PatchedOctet.write, hv, uleb_len_eq_ubytes, ubytes, width] <;> omega

end PatchedOctet

/-! ## XorPredecessor specific -/

namespace XorPredecessor

theorem sound_EqualTail : EqualTail.Goal := by
  intro x n
  induction n with
  | zero =>
      simp [XorPredecessor.write, uleb_len_eq_ubytes]
  | succ n ih =>
      -- write (x :: List.replicate (n+1) x) = write (x :: x :: List.replicate n x)
      -- = uleb (x^^^0) ++ uleb (x^^^x) ++ write of tail zipped...
      -- easier compute directly
      simp [List.replicate, XorPredecessor.write, List.zip, uleb_len_eq_ubytes, ih]
      omega

end XorPredecessor

/-! ## OrdinalResidual specific -/

namespace OrdinalResidual

theorem sound_ConsecutiveKeys : ConsecutiveKeys.Goal := by
  intro n
  -- residuals of List.range n are all zeros
  have hres : residuals (List.range n) = List.replicate n 0 := by
    unfold residuals
    -- (List.range n).zipIdx.map (fun (v,i) => zig ((v:Int)-i)) = replicate n 0
    have hzip : (List.range n).zipIdx = (List.range n).map (fun i => (i, i)) := by
      -- zipIdx on range gives (i,i)
      induction n with
      | zero => simp
      | succ n ih =>
          simp [List.range_succ, List.zipIdx_append, List.zipIdx_cons, ih, List.map_append, Function.comp]
    rw [hzip]
    simp [List.map_map, Function.comp, zig]
    -- goal: (List.range n).map (fun _ => 0) = List.replicate n 0
    induction n <;> simp [List.range_succ, List.replicate, List.map_append, *]
  have hpeak : peak (List.replicate n 0) = 0 := by
    cases n <;> simp [peak, List.replicate, List.max?]
  have hbw : bitWidth (List.range n) = 0 := by
    simp [bitWidth, hres, hpeak, width]
  simp [OrdinalResidual.write, hbw, uleb_len_eq_ubytes, ubytes, width, pack, little, byteCount]

end OrdinalResidual

/-! ## SplitHigh specific -/

namespace SplitHigh

theorem sound_CommonUpper : CommonUpper.Goal := by
  intro xs h hne hall
  -- all x/256 = h, so runs of highs is [(h, xs.length)]
  have hruns : runs (xs.map (· / 256)) = [(h, xs.length)] := by
    induction xs with
    | nil => cases hne rfl
    | cons x xs ih =>
        have hx : x / 256 = h := hall x (by simp)
        cases xs with
        | nil => simp [runs, hx]
        | cons y ys =>
            have hy : y / 256 = h := hall y (by simp)
            have hall' : ∀ z ∈ y::ys, z / 256 = h := fun z hz => hall z (by simp [hz])
            have ih' := ih (by simp) hall'
            simp [runs, hx, hy, ih']
  simp [SplitHigh.write, hruns, uleb_len_eq_ubytes, List.length_map, List.map_map, Function.comp]
  omega

end SplitHigh

/-! ## MinimumDelta specific -/

namespace MinimumDelta

private theorem delta_arith (d p k : Nat) :
    delta (p * d) ((List.range k).map (fun i => (p + i + 1) * d)) = List.replicate k (zig (d : Int)) := by
  induction k generalizing p with
  | zero => simp [delta]
  | succ k ih =>
      simp [List.range_succ, List.map, delta]
      have h1 : ((p + 0 + 1) * d : Int) - (p * d : Int) = (d : Int) := by
        push_cast; ring
      simp [h1]
      have h2 : delta (((p + 0 + 1) * d)) (((List.range k).map (fun i => (p + (i + 1) + 1) * d))) =
          List.replicate k (zig (d : Int)) := by
        have := ih (p + 1)
        simpa [Nat.add_assoc, Nat.add_left_comm, Nat.add_comm] using this
      simp [h2, List.replicate]

theorem sound_ArithmeticKeys : ArithmeticKeys.Goal := by
  intro d n hn
  cases n with
  | zero => omega
  | succ n =>
      have hdelta : delta 0 ((List.range (n+1)).map (fun i => (i+1) * d)) = List.replicate (n+1) (zig (d : Int)) := by
        have := delta_arith d 0 (n+1)
        simpa [Nat.zero_add] using this
      have hds : delta 0 ((List.range (n+1)).map (fun i => (i+1) * d)) ≠ [] := by
        rw [hdelta]; simp
      have hmin : (delta 0 ((List.range (n+1)).map (fun i => (i+1) * d))).min?.getD 0 = zig (d : Int) := by
        rw [hdelta]
        simp [List.replicate]
        cases n <;> simp [List.replicate, List.min?, Nat.min_self]
      have hoff : (delta 0 ((List.range (n+1)).map (fun i => (i+1) * d))).map (· - (delta 0 ((List.range (n+1)).map (fun i => (i+1) * d))).min?.getD 0) =
          List.replicate (n+1) 0 := by
        rw [hdelta, hmin]
        simp [List.map_replicate]
      have hpeak : peak (List.replicate (n+1) 0) = 0 := by
        cases n <;> simp [peak, List.replicate, List.max?]
      have hw : width 0 = 0 := by simp [width]
      simp [MinimumDelta.write, hdelta, hmin, hoff, hpeak, hw, pack, little, byteCount, uleb_len_eq_ubytes]
      -- length = ubytes (zig (d:Int)) + ubytes 0 + 0
      have hz : zig (d : Int) = 2 * d := by simp [zig]
      simp [hz, ubytes, width]

end MinimumDelta

/-! ## SparsePlanes specific -/

namespace SparsePlanes

private theorem testBit_two_pow_self (b i : Nat) : (2^b).testBit i = decide (i = b) := by
  by_cases h : i = b
  · subst h
    simp [Nat.testBit_two_pow]
  · have h' : i ≠ b := h
    simp [Nat.testBit_two_pow, h']

theorem sound_OneHotOutlier : OneHotOutlier.Goal := by
  intro n b
  -- xs = replicate n 0 ++ [2^b]
  let xs := List.replicate n 0 ++ [2^b]
  have hpeak : peak xs = 2^b := by
    simp [xs, peak, List.max?, List.append_assoc]
    -- max? of replicate n 0 ++ [2^b] is 2^b
    induction n with
    | zero => simp
    | succ n ih => simp [List.replicate, List.max?, ih, Nat.max_eq_right (Nat.zero_le _)]
  have hw : width (peak xs) = b + 1 := by
    rw [hpeak]
    by_cases hb : b = 0
    · subst hb; simp [width]
    · have hne : 2^b ≠ 0 := by positivity
      rw [show width (2^b) = (2^b).log2 + 1 from by unfold width; simp [hne]]
      simp [Nat.log2_pow]
  -- each bitplane
  have hplane : ∀ i, i < b + 1 →
      (xs.zipIdx.filter (fun (v,_) => v.testBit i)).length = if i = b then 1 else 0 := by
    intro i hi
    by_cases hib : i = b
    · subst hib
      -- only the last element has bit b set
      have : (xs.zipIdx.filter (fun (v,_) => v.testBit b)) = [((2^b), n)] := by
        simp [xs, List.zipIdx_append, List.zipIdx_replicate, List.filter_append, List.filter_map,
          List.filter_cons, Nat.testBit_zero, testBit_two_pow_self]
      simp [this]
    · have hib' : i ≠ b := hib
      have hlt : i < b := by omega
      have : (xs.zipIdx.filter (fun (v,_) => v.testBit i)) = [] := by
        simp [xs, List.zipIdx_append, List.zipIdx_replicate, List.filter_append, List.filter_map,
          List.filter_cons, Nat.testBit_zero, testBit_two_pow_self, hib']
      simp [this, hib']
  -- compute write length
  simp only [SparsePlanes.write, hpeak, hw]
  -- length = (uleb (b+1)).length + sum of plane lengths
  simp only [List.length_append, uleb_len_eq_ubytes]
  -- sum over range (b+1)
  have hsum : ((List.range (b+1)).flatMap (fun i =>
      let ps := xs.zipIdx.filter (fun (v,_) => v.testBit i)
      uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j))).length = (b+1) + ubytes n := by
    -- split into i < b and i == b
    have hsplit : List.range (b+1) = List.range b ++ [b] := by
      rw [List.range_succ]
    rw [hsplit, List.flatMap_append, List.length_append]
    -- range b part: each gives uleb 0 = 1 byte
    have h1 : ((List.range b).flatMap (fun i =>
        let ps := xs.zipIdx.filter (fun (v,_) => v.testBit i)
        uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j))).length = b := by
      have : (List.range b).flatMap (fun i =>
          let ps := xs.zipIdx.filter (fun (v,_) => v.testBit i)
          uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j)) =
        (List.range b).flatMap (fun _ => uleb 0) := by
        apply List.flatMap_congr
        intro i hi
        have hi' : i < b + 1 := by simp at hi; omega
        have hlen := hplane i hi'
        have hib : i ≠ b := by simp at hi; omega
        simp at hlen
        simp [hib] at hlen
        -- filter gives empty list
        have hempty : xs.zipIdx.filter (fun (v,_) => v.testBit i) = [] := by
          rw [List.length_eq_zero_iff] at hlen
          exact hlen
        simp [hempty]
      rw [this, List.length_flatMap]
      simp [uleb_len_eq_ubytes, ubytes, width]
    -- b part: uleb 1 + uleb n
    have h2 : (([b] : List Nat).flatMap (fun i =>
        let ps := xs.zipIdx.filter (fun (v,_) => v.testBit i)
        uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j))).length = 1 + ubytes n := by
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.length_append, uleb_len_eq_ubytes]
      have hlen := hplane b (Nat.lt_succ_self b)
      simp at hlen
      -- ps has length 1 and is [((2^b), n)]
      have hps : xs.zipIdx.filter (fun (v,_) => v.testBit b) = [((2^b), n)] := by
        -- from above
        simp [xs, List.zipIdx_append, List.zipIdx_replicate, List.filter_append, List.filter_map,
          List.filter_cons, Nat.testBit_zero, testBit_two_pow_self]
      simp [hps, uleb_len_eq_ubytes, ubytes, width]
    rw [h1, h2]
  rw [hsum]
  omega

end SparsePlanes

/-! ## SharedDivisor specific -/

namespace SharedDivisor

theorem sound_ScaledBinary : ScaledBinary.Goal := by
  intro g xs hg hgmem hall
  -- all elements are 0 or g, and g is in xs
  have hgcd : xs.foldl Nat.gcd 0 = g := by
    -- gcd of a list of 0/g values containing g is g
    have hdiv : ∀ x ∈ xs, g ∣ x := by
      intro x hx
      rcases hall x hx with rfl | rfl <;> simp
    have hdvd : xs.foldl Nat.gcd 0 ∣ g := by
      -- foldl gcd divides every element
      have aux : ∀ (l : List Nat) (acc : Nat), (∀ x ∈ l, g ∣ x) → acc ∣ g → l.foldl Nat.gcd acc ∣ g := by
        intro l acc hl hacc
        induction l generalizing acc with
        | nil => simpa using hacc
        | cons a l ih =>
            simp only [List.foldl_cons]
            apply ih
            · intro x hx; exact hl x (by simp [hx])
            · exact Nat.dvd_trans (Nat.gcd_dvd_right acc a) hacc
      exact aux xs 0 hdiv (by simp)
    have hgdiv : g ∣ xs.foldl Nat.gcd 0 := by
      -- g divides foldl gcd if g divides all and g divides acc
      have aux : ∀ (l : List Nat) (acc : Nat), (∀ x ∈ l, g ∣ x) → g ∣ acc → g ∣ l.foldl Nat.gcd acc := by
        intro l acc hl hacc
        induction l generalizing acc with
        | nil => simpa using hacc
        | cons a l ih =>
            simp only [List.foldl_cons]
            apply ih
            · intro x hx; exact hl x (by simp [hx])
            · exact Nat.dvd_gcd hacc (hl a (by simp))
      exact aux xs 0 hdiv (by simp)
    exact Nat.dvd_antisymm hdvd hgdiv
  have hvs : xs.map (· / g) = xs.map (fun x => if x = 0 then 0 else 1) := by
    apply List.map_congr_left
    intro x hx
    rcases hall x hx with rfl | rfl
    · simp
    · simp [Nat.div_self hg]
  have hpeak1 : peak (xs.map (· / g)) = 1 := by
    -- contains g/g=1 and rest 0 or 1
    have h1mem : 1 ∈ xs.map (· / g) := by
      refine List.mem_map.mpr ⟨g, hgmem, ?_⟩
      simp [Nat.div_self hg]
    have hle1 : ∀ v ∈ xs.map (· / g), v ≤ 1 := by
      intro v hv
      rcases List.mem_map.mp hv with ⟨x, hx, rfl⟩
      rcases hall x hx with rfl | rfl
      · simp
      · simp [Nat.div_self hg]
    -- peak is max, so 1
    unfold peak
    have hne : (xs.map (· / g)) ≠ [] := by
      intro he
      have : xs = [] := by simpa using (List.map_eq_nil_iff.mp he)
      subst this
      simp at hgmem
    rcases List.max?_eq_some_iff.mp (show (xs.map (· / g)).max? = some ((xs.map (· / g)).max?.getD 0) from by
      cases h : (xs.map (· / g)).max? <;> simp_all [List.max?_eq_none_iff]) with ⟨hm, hallm⟩
    have hle : (xs.map (· / g)).max?.getD 0 ≤ 1 := by
      apply le_trans _ (le_refl 1)
      exact hallm _ hm |> fun _ => by
        have := hle1 _ hm
        omega
    have hge : 1 ≤ (xs.map (· / g)).max?.getD 0 := by
      have := hallm 1 h1mem
      exact this
    omega
  have hw : width (peak (xs.map (· / g))) = 1 := by
    rw [hpeak1]; simp [width]
  simp [SharedDivisor.write, hgcd, hpeak1, hw, uleb_len_eq_ubytes, pack_length]
  -- length = ubytes g + ubytes 1 + byteCount (xs.length * 1)
  have huby1 : ubytes 1 = 1 := by simp [ubytes, width]
  simp [huby1]
  omega

end SharedDivisor


/-! ## More specific proofs -/

namespace ConstantExceptions

theorem sound_ModalBeatsFirst : ModalBeatsFirst.Goal := by
  intro a b n hab hn
  -- list1 = b :: a :: List.replicate (n-1) b, base = b, exceptions = [(a,1)]
  -- list2 = a :: List.replicate n b, base = a, exceptions = n items at 1..n
  -- compute lengths
  have hn1 : 1 ≤ n - 1 := by omega
  -- For list1: b :: a :: replicate (n-1) b
  -- base = b, zipIdx gives (b,0),(a,1),(b,2),...,(b,n) ; filter v!=b gives [(a,1)]
  have hex1 : ((b :: a :: List.replicate (n-1) b).zipIdx.filter (fun (v,_) => v != b)) = [(a,1)] := by
    simp [List.zipIdx_cons, List.filter_cons, hab]
    have : ((List.replicate (n-1) b).zipIdx.map (fun (v,i) => (v, i + 2))).filter (fun (v,_) => v != b) = [] := by
      induction n - 1 with
      | zero => simp
      | succ k ih =>
          simp [List.replicate, List.zipIdx_cons, List.filter_cons, ih]
    rw [this]
  -- For list2: a :: replicate n b
  -- base = a, exceptions are all b's at positions 1..n, count n
  have hex2_len : ((a :: List.replicate n b).zipIdx.filter (fun (v,_) => v != a)).length = n := by
    simp [List.zipIdx_cons, List.filter_cons]
    have hab' : (b != a) = true := by
      cases h : b == a
      · simp [bne, h]
      · exfalso; apply hab; exact (eq_comm.mp (of_decide_eq_true h))
    -- filter on replicate n b shifted gives n elements
    have hrep : ∀ m off, (((List.replicate m b).zipIdx.map (fun (v,i) => (v, i + off))).filter (fun (v,_) => v != a)).length = m := by
      intro m
      induction m with
      | zero => simp
      | succ m ih =>
          intro off
          simp [List.replicate, List.zipIdx_cons, List.filter_cons, hab', ih]
    exact hrep n 1
  -- Now compute lengths
  have hw1 : (ConstantExceptions.write (b :: a :: List.replicate (n-1) b)).length = 4 + ubytes 1 + (ubytes 1 + 4) := by
    simp [ConstantExceptions.write, hex1, little_length, uleb_len_eq_ubytes]
  have hw2_ge : 4 + ubytes n + n * (1 + 4) ≤ (ConstantExceptions.write (a :: List.replicate n b)).length := by
    -- each exception costs at least ubytes(i)+4 >= 1+4
    simp only [ConstantExceptions.write, little_length, List.length_append, uleb_len_eq_ubytes]
    -- ex length is n
    rw [hex2_len]
    -- flatMap length >= n*5
    have hflat : n * (1 + 4) ≤ ((a :: List.replicate n b).zipIdx.filter (fun (v,_) => v != a)).flatMap
        (fun (v,i) => uleb i ++ little v 4) |>.length := by
      have hle : ∀ e ∈ (a :: List.replicate n b).zipIdx.filter (fun (v,_) => v != a),
          1 + 4 ≤ (uleb e.2 ++ little e.1 4).length := by
        intro e _
        simp [uleb_len_eq_ubytes, little_length]
        have : 1 ≤ ubytes e.2 := by unfold ubytes; exact Nat.le_max_left _ _
        omega
      calc n * (1 + 4)
          = ((a :: List.replicate n b).zipIdx.filter (fun (v,_) => v != a)).length * (1 + 4) := by rw [hex2_len]
        _ ≤ (((a :: List.replicate n b).zipIdx.filter (fun (v,_) => v != a)).map
              (fun e => (uleb e.2 ++ little e.1 4).length)).sum := by
            have hsum : ∀ (l : List ((Nat × Nat))), (∀ e ∈ l, 1 + 4 ≤ (uleb e.2 ++ little e.1 4).length) →
                l.length * (1 + 4) ≤ (l.map (fun e => (uleb e.2 ++ little e.1 4).length)).sum := by
              intro l hl
              induction l with
              | nil => simp
              | cons e l ih =>
                  simp only [List.length_cons, List.map_cons, List.sum_cons]
                  have he := hl e (by simp)
                  have ih' := ih (fun x hx => hl x (by simp [hx]))
                  have : (l.length + 1) * (1 + 4) = l.length * (1 + 4) + (1 + 4) := by ring
                  omega
            exact hsum _ hle
        _ = ((a :: List.replicate n b).zipIdx.filter (fun (v,_) => v != a)).flatMap
              (fun e => uleb e.2 ++ little e.1 4) |>.length := by
            rw [List.length_flatMap]
            simp [Function.comp]
    omega
  -- combine: 4+ubytes1+ubytes1+4 = 10, and rhs >= 4+ubytes n+5*n >= 4+1+15 = 20 for n>=3
  have hub1 : ubytes 1 = 1 := by simp [ubytes, width]
  rw [hw1, hub1]
  have hge3 : 3 ≤ n := hn
  have hubn : 1 ≤ ubytes n := by unfold ubytes; exact Nat.le_max_left _ _
  omega

end ConstantExceptions


namespace DictionaryRuns

private theorem eraseDups_replicate_append (a b : Nat) (hab : a ≠ b) (n m : Nat) :
    (List.replicate (n+1) a ++ List.replicate (m+1) b).eraseDups = [a,b] := by
  have ha_notin : a ∉ (List.replicate n a ++ List.replicate (m+1) b).eraseDups.filter (· != a) := by
    simp [List.mem_filter]
  -- compute stepwise
  simp only [List.replicate, List.cons_append, List.eraseDups_cons]
  have hne : (a == a) = true := by simp
  -- first element a kept
  simp only [bne_iff_ne, ne_eq, not_true_eq_false, decide_false, Bool.not_false, List.filter_true]
  -- remove a's from rest
  suffices h : (List.replicate n a ++ List.replicate (m+1) b).eraseDups.filter (· != a) = [b] by
    simp [h]
  induction n with
  | zero =>
      simp [List.replicate, List.eraseDups]
      simp [hab]
      have hbb : (b == b) = true := by simp
      simp [List.filter]
      intro x hx
      -- remaining replicate m b filtered
      have : (List.replicate m b).eraseDups.filter (· != b) = [] := by
        induction m with
        | zero => simp
        | succ m ih => simp [List.replicate, List.eraseDups, ih]
      simp [this]
  | succ n ih =>
      simp [List.replicate, List.eraseDups, ih]

private theorem runs_replicate_two (a b : Nat) (hab : a ≠ b) (n m : Nat) :
    runs ((List.replicate (n+1) a ++ List.replicate (m+1) b).map ([a,b].idxOf)) = [(0, n+1), (1, m+1)] := by
  -- map gives replicate (n+1) 0 ++ replicate (m+1) 1
  have hidxa : ([a,b] : List Nat).idxOf a = 0 := by simp [List.idxOf]
  have hidxb : ([a,b] : List Nat).idxOf b = 1 := by
    simp [List.idxOf, hab]
  have hmap : ((List.replicate (n+1) a ++ List.replicate (m+1) b).map ([a,b].idxOf)) =
      List.replicate (n+1) 0 ++ List.replicate (m+1) 1 := by
    rw [List.map_append, List.map_replicate, List.map_replicate, hidxa, hidxb]
  rw [hmap]
  -- runs of replicate (n+1) 0 ++ replicate (m+1) 1
  -- compute runs
  have hruns0 : ∀ k, runs (List.replicate (k+1) 0) = [(0, k+1)] := by
    intro k
    induction k with
    | zero => simp [runs, List.replicate]
    | succ k ih =>
        simp [List.replicate, runs, ih]
  have hruns01 : ∀ k m, runs (List.replicate (k+1) 0 ++ List.replicate (m+1) 1) = [(0,k+1),(1,m+1)] := by
    intro k m
    induction k with
    | zero =>
        simp [List.replicate, runs, hruns0]
        -- runs (0 :: replicate (m+1) 1) = (0,1) :: runs? No wait
        -- replicate 1 0 ++ replicate (m+1) 1 = [0] ++ replicate (m+1) 1
        simp [List.replicate, runs]
        have : runs (List.replicate (m+1) (1:Nat)) = [(1, m+1)] := by
          induction m with
          | zero => simp [runs, List.replicate]
          | succ m ih => simp [List.replicate, runs, ih]
        simp [this]
    | succ k ih =>
        simp [List.replicate, runs, ih]
  exact hruns01 n m

theorem sound_DictionaryBoundary : DictionaryBoundary.Goal := by
  intro a b hab n m hn hm
  cases n with
  | zero => omega
  | succ n =>
      cases m with
      | zero => omega
      | succ m =>
          have hdict := eraseDups_replicate_append a b hab n m
          have hruns := runs_replicate_two a b hab n m
          -- write = uleb d.length ++ fixed d ++ flatMap runs...
          simp only [DictionaryRuns.write, hdict, hruns, List.length_cons, List.length_nil,
            List.map, List.flatMap, List.append_nil, List.length_append, uleb_len_eq_ubytes, fixed, little_length]
          -- uleb 2 + (little a 4 ++ little b 4) + (uleb 0 ++ uleb (n+1) ++ uleb 1 ++ uleb (m+1))
          simp [ubytes, width]
          omega

end DictionaryRuns

/-! ## Generic instantiations -/

theorem sound_SeparatedLengths_StrictSavings : SeparatedLengths.StrictSavings.Goal := strictSavings_holds SeparatedLengths.write
theorem sound_SeparatedLengths_HotPointReads : SeparatedLengths.HotPointReads.Goal := hotPointReads_holds SeparatedLengths.write
theorem sound_SeparatedLengths_SeekCap : SeparatedLengths.SeekCap.Goal := seekCap_holds SeparatedLengths.write
theorem sound_SeparatedLengths_ParallelEncode : SeparatedLengths.ParallelEncode.Goal := parallelEncode_holds SeparatedLengths.write
theorem sound_SeparatedLengths_MandatoryCuts : SeparatedLengths.MandatoryCuts.Goal := mandatoryCuts_holds SeparatedLengths.write
theorem sound_ConstantExceptions_StrictSavings : ConstantExceptions.StrictSavings.Goal := strictSavings_holds ConstantExceptions.write
theorem sound_ConstantExceptions_HotPointReads : ConstantExceptions.HotPointReads.Goal := hotPointReads_holds ConstantExceptions.write
theorem sound_ConstantExceptions_SeekCap : ConstantExceptions.SeekCap.Goal := seekCap_holds ConstantExceptions.write
theorem sound_ConstantExceptions_ParallelEncode : ConstantExceptions.ParallelEncode.Goal := parallelEncode_holds ConstantExceptions.write
theorem sound_ConstantExceptions_MandatoryCuts : ConstantExceptions.MandatoryCuts.Goal := mandatoryCuts_holds ConstantExceptions.write
theorem sound_DictionaryRuns_StrictSavings : DictionaryRuns.StrictSavings.Goal := strictSavings_holds DictionaryRuns.write
theorem sound_DictionaryRuns_HotPointReads : DictionaryRuns.HotPointReads.Goal := hotPointReads_holds DictionaryRuns.write
theorem sound_DictionaryRuns_SeekCap : DictionaryRuns.SeekCap.Goal := seekCap_holds DictionaryRuns.write
theorem sound_DictionaryRuns_ParallelEncode : DictionaryRuns.ParallelEncode.Goal := parallelEncode_holds DictionaryRuns.write
theorem sound_DictionaryRuns_MandatoryCuts : DictionaryRuns.MandatoryCuts.Goal := mandatoryCuts_holds DictionaryRuns.write
theorem sound_PatchedOctet_StrictSavings : PatchedOctet.StrictSavings.Goal := strictSavings_holds PatchedOctet.write
theorem sound_PatchedOctet_HotPointReads : PatchedOctet.HotPointReads.Goal := hotPointReads_holds PatchedOctet.write
theorem sound_PatchedOctet_SeekCap : PatchedOctet.SeekCap.Goal := seekCap_holds PatchedOctet.write
theorem sound_PatchedOctet_ParallelEncode : PatchedOctet.ParallelEncode.Goal := parallelEncode_holds PatchedOctet.write
theorem sound_PatchedOctet_MandatoryCuts : PatchedOctet.MandatoryCuts.Goal := mandatoryCuts_holds PatchedOctet.write
theorem sound_XorPredecessor_StrictSavings : XorPredecessor.StrictSavings.Goal := strictSavings_holds XorPredecessor.write
theorem sound_XorPredecessor_HotPointReads : XorPredecessor.HotPointReads.Goal := hotPointReads_holds XorPredecessor.write
theorem sound_XorPredecessor_SeekCap : XorPredecessor.SeekCap.Goal := seekCap_holds XorPredecessor.write
theorem sound_XorPredecessor_ParallelEncode : XorPredecessor.ParallelEncode.Goal := parallelEncode_holds XorPredecessor.write
theorem sound_XorPredecessor_MandatoryCuts : XorPredecessor.MandatoryCuts.Goal := mandatoryCuts_holds XorPredecessor.write
theorem sound_OrdinalResidual_StrictSavings : OrdinalResidual.StrictSavings.Goal := strictSavings_holds OrdinalResidual.write
theorem sound_OrdinalResidual_HotPointReads : OrdinalResidual.HotPointReads.Goal := hotPointReads_holds OrdinalResidual.write
theorem sound_OrdinalResidual_SeekCap : OrdinalResidual.SeekCap.Goal := seekCap_holds OrdinalResidual.write
theorem sound_OrdinalResidual_ParallelEncode : OrdinalResidual.ParallelEncode.Goal := parallelEncode_holds OrdinalResidual.write
theorem sound_OrdinalResidual_MandatoryCuts : OrdinalResidual.MandatoryCuts.Goal := mandatoryCuts_holds OrdinalResidual.write
theorem sound_SplitHigh_StrictSavings : SplitHigh.StrictSavings.Goal := strictSavings_holds SplitHigh.write
theorem sound_SplitHigh_HotPointReads : SplitHigh.HotPointReads.Goal := hotPointReads_holds SplitHigh.write
theorem sound_SplitHigh_SeekCap : SplitHigh.SeekCap.Goal := seekCap_holds SplitHigh.write
theorem sound_SplitHigh_ParallelEncode : SplitHigh.ParallelEncode.Goal := parallelEncode_holds SplitHigh.write
theorem sound_SplitHigh_MandatoryCuts : SplitHigh.MandatoryCuts.Goal := mandatoryCuts_holds SplitHigh.write
theorem sound_MinimumDelta_StrictSavings : MinimumDelta.StrictSavings.Goal := strictSavings_holds MinimumDelta.write
theorem sound_MinimumDelta_HotPointReads : MinimumDelta.HotPointReads.Goal := hotPointReads_holds MinimumDelta.write
theorem sound_MinimumDelta_SeekCap : MinimumDelta.SeekCap.Goal := seekCap_holds MinimumDelta.write
theorem sound_MinimumDelta_ParallelEncode : MinimumDelta.ParallelEncode.Goal := parallelEncode_holds MinimumDelta.write
theorem sound_MinimumDelta_MandatoryCuts : MinimumDelta.MandatoryCuts.Goal := mandatoryCuts_holds MinimumDelta.write
theorem sound_SparsePlanes_StrictSavings : SparsePlanes.StrictSavings.Goal := strictSavings_holds SparsePlanes.write
theorem sound_SparsePlanes_HotPointReads : SparsePlanes.HotPointReads.Goal := hotPointReads_holds SparsePlanes.write
theorem sound_SparsePlanes_SeekCap : SparsePlanes.SeekCap.Goal := seekCap_holds SparsePlanes.write
theorem sound_SparsePlanes_ParallelEncode : SparsePlanes.ParallelEncode.Goal := parallelEncode_holds SparsePlanes.write
theorem sound_SparsePlanes_MandatoryCuts : SparsePlanes.MandatoryCuts.Goal := mandatoryCuts_holds SparsePlanes.write
theorem sound_SharedDivisor_StrictSavings : SharedDivisor.StrictSavings.Goal := strictSavings_holds SharedDivisor.write
theorem sound_SharedDivisor_HotPointReads : SharedDivisor.HotPointReads.Goal := hotPointReads_holds SharedDivisor.write
theorem sound_SharedDivisor_SeekCap : SharedDivisor.SeekCap.Goal := seekCap_holds SharedDivisor.write
theorem sound_SharedDivisor_ParallelEncode : SharedDivisor.ParallelEncode.Goal := parallelEncode_holds SharedDivisor.write
theorem sound_SharedDivisor_MandatoryCuts : SharedDivisor.MandatoryCuts.Goal := mandatoryCuts_holds SharedDivisor.write


/-! ## GreedyCut refutations -/

theorem refutation_ConstantExceptions_GreedyCut : ¬ ConstantExceptions.GreedyCut.Goal := by
  intro h
  have hne1 : ([0] : List Nat) ≠ [] := by decide
  have hne2 : ([1] : List Nat) ≠ [] := by decide
  have hne3 : ([1] : List Nat) ≠ [] := by decide
  have hcond : chosen ConstantExceptions.write ([0] ++ [1]) ≤ chosen ConstantExceptions.write [0] + chosen ConstantExceptions.write [1] := by native_decide
  have := h [0] [1] [1] hne1 hne2 hne3 hcond
  revert this
  native_decide

theorem refutation_DictionaryRuns_GreedyCut : ¬ DictionaryRuns.GreedyCut.Goal := by
  intro h
  have hne1 : ([0,1] : List Nat) ≠ [] := by decide
  have hne2 : ([2] : List Nat) ≠ [] := by decide
  have hne3 : ([2,2] : List Nat) ≠ [] := by decide
  have hcond : chosen DictionaryRuns.write ([0,1] ++ [2]) ≤ chosen DictionaryRuns.write [0,1] + chosen DictionaryRuns.write [2] := by native_decide
  have := h [0,1] [2] [2,2] hne1 hne2 hne3 hcond
  revert this
  native_decide

theorem refutation_OrdinalResidual_GreedyCut : ¬ OrdinalResidual.GreedyCut.Goal := by
  intro h
  have hne1 : ([127] : List Nat) ≠ [] := by decide
  have hne2 : ([0,0] : List Nat) ≠ [] := by decide
  have hne3 : ([1,2] : List Nat) ≠ [] := by decide
  have hcond : chosen OrdinalResidual.write ([127] ++ [0,0]) ≤ chosen OrdinalResidual.write [127] + chosen OrdinalResidual.write [0,0] := by native_decide
  have := h [127] [0,0] [1,2] hne1 hne2 hne3 hcond
  revert this
  native_decide

theorem refutation_MinimumDelta_GreedyCut : ¬ MinimumDelta.GreedyCut.Goal := by
  intro h
  have hne1 : ([0] : List Nat) ≠ [] := by decide
  have hne2 : ([65535] : List Nat) ≠ [] := by decide
  have hne3 : ([0,65535] : List Nat) ≠ [] := by decide
  have hcond : chosen MinimumDelta.write ([0] ++ [65535]) ≤ chosen MinimumDelta.write [0] + chosen MinimumDelta.write [65535] := by native_decide
  have := h [0] [65535] [0,65535] hne1 hne2 hne3 hcond
  revert this
  native_decide

theorem refutation_SparsePlanes_GreedyCut : ¬ SparsePlanes.GreedyCut.Goal := by
  intro h
  have hne1 : ([0] : List Nat) ≠ [] := by decide
  have hne2 : ([0,128] : List Nat) ≠ [] := by decide
  have hne3 : ([127] : List Nat) ≠ [] := by decide
  have hcond : chosen SparsePlanes.write ([0] ++ [0,128]) ≤ chosen SparsePlanes.write [0] + chosen SparsePlanes.write [0,128] := by native_decide
  have := h [0] [0,128] [127] hne1 hne2 hne3 hcond
  revert this
  native_decide

theorem refutation_SharedDivisor_GreedyCut : ¬ SharedDivisor.GreedyCut.Goal := by
  intro h
  have hne1 : ([1] : List Nat) ≠ [] := by decide
  have hne2 : ([65536] : List Nat) ≠ [] := by decide
  have hne3 : ([0,0] : List Nat) ≠ [] := by decide
  have hcond : chosen SharedDivisor.write ([1] ++ [65536]) ≤ chosen SharedDivisor.write [1] + chosen SharedDivisor.write [65536] := by native_decide
  have := h [1] [65536] [0,0] hne1 hne2 hne3 hcond
  revert this
  native_decide


namespace SharedDivisor

theorem sound_ScaleInvariance : ScaleInvariance.Goal := by
  intro xs g hne hgcd hg
  have hgcd_scaled : (xs.map (·*g)).foldl Nat.gcd 0 = g := by
    -- gcd of scaled list is g * gcd of list = g*1
    have hdiv : ∀ x ∈ xs.map (·*g), g ∣ x := by
      intro x hx
      rcases List.mem_map.mp hx with ⟨y, _, rfl⟩
      exact Nat.dvd_mul_right g y
    have hdvd : (xs.map (·*g)).foldl Nat.gcd 0 ∣ g := by
      have aux : ∀ (l : List Nat) (acc : Nat), (∀ x ∈ l, g ∣ x) → acc ∣ g → l.foldl Nat.gcd acc ∣ g := by
        intro l acc hl hacc
        induction l generalizing acc with
        | nil => simpa using hacc
        | cons a l ih =>
            simp only [List.foldl_cons]
            exact ih (fun x hx => hl x (by simp [hx])) (Nat.dvd_trans (Nat.gcd_dvd_right acc a) hacc)
      exact aux _ 0 hdiv (by simp)
    have hgdiv : g ∣ (xs.map (·*g)).foldl Nat.gcd 0 := by
      have aux : ∀ (l : List Nat) (acc : Nat), (∀ x ∈ l, g ∣ x) → g ∣ acc → g ∣ l.foldl Nat.gcd acc := by
        intro l acc hl hacc
        induction l generalizing acc with
        | nil => simpa using hacc
        | cons a l ih =>
            simp only [List.foldl_cons]
            exact ih (fun x hx => hl x (by simp [hx])) (Nat.dvd_gcd hacc (hl a (by simp)))
      exact aux _ 0 hdiv (by simp)
    exact Nat.dvd_antisymm hdvd hgdiv
  have hvs : (xs.map (·*g)).map (·/g) = xs := by
    simp [List.map_map, Function.comp, Nat.mul_div_cancel_left _ hg]
  have hpeak : peak ((xs.map (·*g)).map (·/g)) = peak xs := by rw [hvs]
  simp [SharedDivisor.write, hgcd_scaled, hgcd, hvs, hpeak, uleb_len_eq_ubytes, pack_length]
  have huby1 : ubytes 1 = 1 := by simp [ubytes, width]
  simp [huby1]
  omega

end SharedDivisor

namespace MinimumDelta

theorem sound_ConstantAmbiguity : ConstantAmbiguity.Goal := by
  intro x n hn
  cases n with
  | zero => omega
  | succ n =>
      -- write (replicate (n+1) 0) has length 2
      have h0 : (MinimumDelta.write (List.replicate (n+1) 0)).length = 2 := by
        have hdelta : delta 0 (List.replicate (n+1) 0) = List.replicate (n+1) 0 := by
          induction n with
          | zero => simp [delta, List.replicate, zig]
          | succ n ih => simp [List.replicate, delta, zig, ih]
        have hmin : (List.replicate (n+1) 0).min?.getD 0 = 0 := by
          cases n <;> simp [List.replicate, List.min?]
        have hoff : (List.replicate (n+1) 0).map (· - 0) = List.replicate (n+1) 0 := by simp
        have hpeak : peak (List.replicate (n+1) 0) = 0 := by cases n <;> simp [peak, List.replicate, List.max?]
        simp [MinimumDelta.write, hdelta, hmin, hoff, hpeak, width, pack, little, byteCount, uleb_len_eq_ubytes, ubytes]
      -- write (replicate (n+1) x) has length >= 2
      have h2 : 2 ≤ (MinimumDelta.write (List.replicate (n+1) x)).length := by
        simp [MinimumDelta.write, List.length_append, uleb_len_eq_ubytes]
        have : 1 ≤ ubytes ((delta 0 (List.replicate (n+1) x)).min?.getD 0) := by unfold ubytes; exact Nat.le_max_left _ _
        have : 1 ≤ ubytes (width (peak ((delta 0 (List.replicate (n+1) x)).map (· - (delta 0 (List.replicate (n+1) x)).min?.getD 0)))) := by unfold ubytes; exact Nat.le_max_left _ _
        omega
      omega

end MinimumDelta


namespace OrdinalResidual

theorem sound_ShiftedShard : ShiftedShard.Goal := by
  intro a b
  -- residuals(a ++ b.map (·+a.length)) = residuals(a) ++ residuals(b)
  have hres : residuals (a ++ b.map (· + a.length)) = residuals a ++ residuals b := by
    unfold residuals
    -- zipIdx of append
    have h1 : (a ++ b.map (· + a.length)).zipIdx = a.zipIdx ++ (b.map (· + a.length)).zipIdx.map (fun (v,i) => (v, i + a.length)) := by
      rw [List.zipIdx_append]
      simp [List.length_map]
    rw [h1, List.map_append]
    congr 1
    · -- second part maps to residuals b
      -- (b.map (·+a.length)).zipIdx.map (fun (v,i) => (v,i+a.length)) then map residual
      -- = b.zipIdx.map (fun (v,i) => (v+a.length, i+a.length)) then residual
      -- = b.zipIdx.map (fun (v,i) => zig(((v+a.length):Int) - (i+a.length))) = b.zipIdx.map (fun (v,i)=>zig((v:Int)-i))
      have h2 : (b.map (· + a.length)).zipIdx = b.zipIdx.map (fun (v,i) => (v + a.length, i)) := by
        induction b generalizing a with
        | nil => simp
        | cons x xs ih =>
            simp [List.zipIdx_cons, ih, List.map_map, Function.comp, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]
      rw [h2]
      simp [List.map_map, Function.comp]
      congr 1
      ext ⟨v,i⟩
      simp
      congr 1
      omega
  simp [bitWidth, hres, peak]
  -- peak of append = max of peaks
  have hpeak_append : ∀ (xs ys : List Nat), (xs ++ ys).max?.getD 0 = max (xs.max?.getD 0) (ys.max?.getD 0) := by
    intro xs ys
    induction xs with
    | nil => simp
    | cons x xs ih =>
        simp [List.max?, ih, Nat.max_assoc]
  rw [hpeak_append]
  simp [width, peak]
  by_cases ha : (residuals a).max?.getD 0 = 0 <;> by_cases hb : (residuals b).max?.getD 0 = 0 <;>
    simp [ha, hb, Nat.max_eq_left, Nat.max_eq_right, Nat.le_of_not_lt, Nat.lt_of_not_ge] <;>
    omega

end OrdinalResidual

namespace SplitHigh

theorem sound_HighRunMerge : HighRunMerge.Goal := by
  intro x y n m hxy hn hm
  cases n with
  | zero => omega
  | succ n =>
      cases m with
      | zero => omega
      | succ m =>
          -- highs: replicate (n+1) (x/256) ++ replicate (m+1) (y/256) = replicate (n+1+m+1) (x/256)
          have hhigh : (List.replicate (n+1) x ++ List.replicate (m+1) y).map (· / 256) =
              List.replicate (n+1+m+1) (x / 256) := by
            rw [List.map_append, List.map_replicate, List.map_replicate, hxy]
            -- replicate (n+1) (x/256) ++ replicate (m+1) (x/256)
            rw [← List.replicate_append]
            congr 1
            omega
          have hruns : runs (List.replicate (n+1+m+1) (x / 256)) = [((x/256), n+1+m+1)] := by
            induction (n+1+m+1) with
            | zero => simp at *
            | succ k ih =>
                cases k with
                | zero => simp [runs, List.replicate]
                | succ k ih2 =>
                    simp [List.replicate, runs, ih2]
          simp [SplitHigh.write, hhigh, hruns, uleb_len_eq_ubytes, List.length_append, List.length_map, List.map_append, List.map_replicate]
          omega

end SplitHigh

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection
