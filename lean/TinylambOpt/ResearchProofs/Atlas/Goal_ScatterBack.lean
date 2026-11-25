import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ScatterBack

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather

/-!
`Gather.ScatterBack.Goal` は、遅延 gather で並べ替えたページ列から
要求 ordinal 列へ値を scatter し直す操作が、元の直接読み出しと一致することを述べる。

`addresses = ids.mergeSort.eraseDups` は `ids` に現れる ordinal の集合なので、
各 `i ∈ ids` は必ず `addresses` に含まれ、`addresses.idxOf i` は有効な添字である。
`List.getElem_idxOf` によりその位置の要素は `i` そのものなので、
キャッシュされた `payload[i]?` を取り出して `Option.join` しても変化しない。
小入力（`payload` 長 ≤3、`ids` 長 ≤3 の全組合わせ）は `#eval` / `#guard` で確認済み。
-/

theorem sound_ScatterBack : Gather.ScatterBack.Goal := by
  intro payload ids
  dsimp
  apply List.map_congr_left
  intro i hi
  let addresses := ids.mergeSort.eraseDups
  have hiaddr : i ∈ addresses := by
    dsimp [addresses]
    rw [List.mem_eraseDups]
    simpa using hi
  have hidxlt : addresses.idxOf i < addresses.length :=
    List.idxOf_lt_length_of_mem hiaddr
  have hget : addresses[addresses.idxOf i] = i :=
    List.getElem_idxOf hidxlt
  have hcache : (addresses.map (fun j => payload[j]?))[addresses.idxOf i]? = some (payload[i]?) := by
    simp [List.getElem?_eq_getElem hidxlt, hget]
  rw [hcache]
  cases payload[i]? <;> rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_ScatterBack
