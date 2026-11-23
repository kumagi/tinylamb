# パイプライン融合で LIMIT の需要を下流から伝える

## モデル上の証明

Filter・Projection・Limit の物理配置について、`TinylambOpt.Research.PullDemandFusion.pull_demand_fusion_sound` は許可された配置で出力列と最初の失敗が一致し、需要駆動側の評価行数が増えないことを証明している。実装上の失敗時点の照合は別途必要。

## モデルと適用条件

親の要求単位、バッチ先読み、エラーを確定する演算子、OFFSET と明示的 LIMIT 0 を表現する。

## 回帰例

第1行が通過し第2行が CAST エラー。チャンク全体を既に評価した経路と行単位要求の経路で観測が異なる。

## 完了条件

モデルでは eager と row-demand の二経路について許可条件の健全性と CAST の拒否例を示している。実際の実行経路との契約照合は残る。

実装との照合先は [executor/detail/pipeline/pipeline_ops.cpp](../../../executor/detail/pipeline/pipeline_ops.cpp)。
