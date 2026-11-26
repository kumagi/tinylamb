"""Compare the two pinned runs and audit one new declaration per losing query."""
import argparse
from collections import defaultdict
import re

from benchmark_rematch import BENCH, HERE, LEAN, speed_losses

RUN = BENCH / "three-db-sf10-20261122/after-ed-cb-20261126"
MODELS = LEAN / "TinylambOpt/ResearchGoals/November26"
OUTPUT = HERE / "november26-queries.md"


def by_query(losses):
    result = defaultdict(dict)
    for row in losses:
        key = (row.suite, row.case)
        assert row.rival not in result[key]
        result[key][row.rival] = row
    return result


def read_goals():
    goals = {}
    for path in MODELS.glob("*.lean"):
        src = path.read_text()
        matches = list(re.finditer(r"^/-- (.+?) -/\ndef (\w+)\.Goal : Prop :=", src, re.M))
        assert len(matches) == len(re.findall(r"^def \w+\.Goal\b", src, re.M))
        for m in matches:
            key = f"{path.stem}.{m[2]}"
            assert key not in goals
            line = src.count("\n", 0, m.start(2)) + 1
            goals[key] = (m[1], f"../../{path.relative_to(LEAN)}#L{line}")
    return goals


def render():
    old = by_query(speed_losses())
    current = by_query(speed_losses(RUN, RUN / "analytics", RUN / "cloud_bi/measurements.json"))
    goals = read_goals()
    keys_used = set()
    new = current.keys() - old.keys()
    resolved = old.keys() - current.keys()
    both = sum(len(r) == 2 for r in current.values())
    duals = sum(len(r) for r in current.values())
    lines = [
        "# 11月26日測定の敗北クエリと新しい Lean 命題",
        "",
        f"8スレッドの比較で正解照合を通り、相手より中央値が遅かった **{len(current)} クエリ**に1件ずつ命題を追加した。"
        f"対戦相手別には {duals} 敗で、うち {both} クエリは両者に敗北した。"
        f"前回と比べて初めて負けたクエリは {len(new)} 件、今回も負けたクエリは {len(current)-len(new)} 件、"
        f"今回両者に負けなかった前回の敗北クエリは {len(resolved)} 件。",
        "",
        "入力は after-ed-cb-20261126 の TPC-H SF10（22問）、TPC-DS SF10（99問）、ClickBench 全量（43問）、"
        "派生分析5種（26問）、クラウド・BI（tinylamb のある23ケース）。"
        "TPC-DS Q44 は結果不一致。Iceberg native の3ケースは tinylamb の測定がない。"
        "CostBench と PBench の記録には今回の敗北がなかった。",
        "",
        "速度は各ハーネスの保存サンプルの中央値で判定した。通常はウォームアップ1回、計測3回。"
        "主要3系列の ClickHouse の記録は前回と全164問で同一。派生分析はハーネスの多重集合照合と数値許容差に従う。"
        "表は負けた相手の時間だけを載せ、表示は ms を小数6桁に丸めた。"
        "両者に勝つには、次回同じ条件で正解した上で相手の短い方を下回る必要がある。僅差も含み、有意差は判定していない。",
        "",
        "「新規」は前回どちらにも負けなかったクエリ。「継続」は前回も少なくとも一方に負けたクエリ。"
        "各提出型は SQL 内の特定の演算を扱い、計測項目は仮説が効くかを見分けるために選んだ。"
        "整数はオーバーフローのない算術、順序キーは照合順と同点処理を含む。"
        "非 NULL・一意性・順序・正確な割り切れ方などの条件を使う経路ではそれを確認し、"
        "丸め・評価エラー・適用条件が合わない区画を通常経路に戻す。",
        "",
        "測定元は [TPC-H](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/tpch/results.jsonl)、"
        "[TPC-DS](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/tpcds/results.jsonl)、"
        "[ClickBench](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/clickbench/results.jsonl)、"
        "[派生分析](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/analytics/results.jsonl)、"
        "[クラウド・BI](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/cloud_bi/measurements.json)。"
        "前回の個別案は[11月25日の対応表](benchmark-rematch-ledger.md)にある。",
        "",
        "## 実データで確かめた境界",
        "",
        "- TPC-DS Q45 の tinylamb 中央値は前回の11.673 msから204.192 msに変化した。"
        "今回の3サンプルは191.653、204.192、205.215 ms。SF10 の item で調べると指定10物理キーは9商品IDに対応し、"
        "同じ商品IDを持つ履歴を含めると20物理キーになる。DuckDB の参照DBで逆展開してから売上側を絞る SQL は、"
        "元の49行と一致した。この fixture では10キーを直に使う近道も最終49行が偶然一致するため、"
        "命題では同一商品に複数履歴がある入力と ZIP の OR 条件を含めて検証できるようにした。",
        "- JOB J3 では、今回の分析用 DB を DuckDB 2.0.0 で読み、女性・役柄条件後の325,495行で映画コードを照合した。"
        "適格映画IDの重複は0、非ゼロコードは79,291行。コードの和に 1969 × 79,291 を加えた制作年の和は 157,120,459 で、"
        "直接 SUM した値と一致した。この方式なら、不適格の印と1970～2010年を1バイトに収められる。",
        "- JOB J5 では同じ DB の aka_title 全行で、元の前方一致と半開区間の判定が食い違った行は0件。"
        "この範囲化を使う索引には、ベンチマークと同じ文字列順が必要になる。",
        "- Iceberg SQL base の fixture では96ファイルに各48行、rid はファイルごとの連番。"
        "各ファイルを1区間として合成した COUNT は4,608、SUM は10,614,528で期待値と一致する。"
        "適用には連番の保証が必要で、欠番は区間を分割し、空入力なら件数を使って SUM の NULL を復元する。"
        "[fixture の生成コード](../../../benchmark/comparison/cloud_bi_bench.py#L144)",
        "",
    ]
    for suite in dict.fromkeys(k[0] for k in current):
        lines += [f"## {suite}", "", "| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |",
                  "|---|---|---:|---:|---:|---|"]
        for (s, q), rivals in current.items():
            if s != suite:
                continue
            row = next(iter(rivals.values()))
            key = row.goal.removesuffix(row.rival)
            assert key not in keys_used
            keys_used.add(key)
            assert all(r.goal == key + e and r.current == row.current for e, r in rivals.items())
            proposal, link = goals[key]
            times = [f"{rivals[e].target:.6f}" if e in rivals else "—" for e in ("DuckDB", "ClickHouse")]
            status = "新規" if (s, q) in new else "継続"
            lines.append(f"| {q} | {status} | {row.current:.6f} | {' | '.join(times)} | [{key}]({link})　{proposal} |")
        lines.append("")
    assert keys_used == goals.keys(), f"unassigned goals: {goals.keys()-keys_used}"
    lines += ["## 再確認", "", "保存済みの両測定系列がある作業ツリーで、以下を実行する。"
              "監査は速度の勝敗を生データから再計算し、対応表の数値と全宣言の一対一対応を照合する。",
              "", "```sh", "python3 lean/issues/research/fixtures/november26_queries.py",
              "(cd lean && lake build CheckProofs)", "```", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="regenerate the ledger")
    args = parser.parse_args()
    content = render()
    if args.write:
        OUTPUT.write_text(content)
    elif OUTPUT.read_text() != content:
        raise SystemExit("Ledger differs from measurements or Lean declarations")
    print(f"{len(read_goals())} losing queries matched to as many new goals")
