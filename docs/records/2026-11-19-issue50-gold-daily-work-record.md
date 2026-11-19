# Gold update の日次 schedule 配線（Issue #50）作業記録

作成: 2026-11-19。対象: Issue #50（P2-T5 後続）。
成果物: .github/workflows/daily_parse.yml、.github/workflows/gold.yml、
pipeline/monitor.py、tests/test_monitor.py の変更。

## 目的

P2-T5（Issue #20）で作った Gold の update（日次差分更新）を日次定期
実行へ配線する。変更前の gold.yml は手動 dispatch のみで、Silver に
日次で事実が増えても Gold が自動では追随しなかった。あわせて、Gold の
件数信号を monitor の点検へ追加する（前提だった Issue #41 は着地済みで、
monitor.py には会社マスタの節と件数信号が入っている。その流儀に
合わせた）。

## 前提の確認（gold.py の update の仕組み、実物で確認）

- update は月ごとの透かし（行数・最大提出日時・内容ハッシュ sha256）を
  gold/state.json の前回値と比較し、変化した月にある書類の会社だけを
  再生成して差し替える。変化が無ければ何も書かずに終わる（冪等）。
  したがって、実行のたびに「前回適用分以降の全変化」を回収する形で
  あり、1回実行が落ちても次の実行が透かしで回収する。
- state.json と summary.json は、対象月（--since-month 以降の月）の
  集合で書き直される。現在の Gold は全期間 build（since_month=
  2016-03、対象129か月）で作られているため、既定値 2021-11 のまま
  update を実行すると、透かしと summary の source_months が61か月に
  縮み、2021-10 以前の月の Silver の変化を検出できなくなる。日次配線
  では --since-month 2016-03 を必ず渡す。

## 方式の決定と根拠

主経路は daily_parse への相乗りとし、gold.yml への schedule は
予備経路として併せて実装した。

- 順序の保証の強さ: 相乗り（needs で merge の後段）は同一 run の中で
  順序が決定的に保証される。対して時刻だけの前後関係は、GitHub
  Actions のスケジューラ遅延の実測（2026-11-17〜18 に全ワークフロー
  で6〜10時間の遅延、daily_codelist の 09:15 JST 枠は実行自体が
  欠落）では保証にならない。Issue #41 で会社マスタ update に
  daily_codelist への相乗りが採用されたのと同じ判断である。
- 遅延への耐性: 相乗りは解析 run が遅れて着地した時点そのままで
  後段が動くため、遅延の影響を受けない。予備経路の schedule は、
  daily_parse の run 自体が起動しなかった日の回収を担う。両経路とも
  update が冪等なため、重なって実行されても二重には適用されない。
- 競合の防止: 手動 dispatch・schedule・後段ジョブは、concurrency
  グループ gold-write（cancel-in-progress: false、queue: max）を
  共有して直列化する（Issue #41 の company-master-write と同じ型）。
- コスト: 相乗りは daily_parse の run に update の実行時間（入力の
  取得と透かしの照合が支配的。全期間 build の実績は約30分）が
  乗る。予備経路が同日に重なった場合は、変化なしとして短時間で
  終わる見込みだが、入力取得とハッシュ照合の費用は残る。これは
  順序保証と回収可能性への対価として受ける。
- Issue 本文の変更範囲（gold.yml への schedule 追加）は予備経路
  として実装した。主経路を相乗りにしたのは上記の根拠による。

## 変更内容

- daily_parse.yml: merge の後段に gold-update ジョブを追加した。
  - needs は [plan, merge]。条件は merge が success の場合、または
    対象が0件で merge が実行されなかった場合（needs.plan.outputs.
    targets == '0'）に実行する。対象0件の日も実行するのは、手動
    経路（backfill_missing、lineage_repair）で Silver が変わった分を
    透かしで回収するためである。解析が失敗した日は merge が
    success にならず対象も0件でないため実行せず、翌日の実行が
    透かしで回収する。
  - 実行は `python -m pipeline.gold update --work-dir work/gold
    --upload --since-month 2016-03`。timeout は gold.yml と同じ
    350分。concurrency は gold-write。
- gold.yml: schedule（毎日 20:30 JST、cron "30 11 * * *"）を追加し、
  gold ジョブへ concurrency（gold-write）を追加した。schedule で
  起動した場合は inputs が空になるため、実行ステップでイベント名を
  見て command=update、since_month=2016-03 を既定値として入れる。
  手動 dispatch の挙動は従来どおり（since_month が空なら gold.py の
  既定値 2021-11）。
- monitor.py: Gold の点検を追加した（会社マスタの流儀に合わせた）。
  - 日次報告に gold 節を追加: 状態（ok／stale／missing）、生成日時、
    annual・quarterly・dividends の行数、会社数、比較に使った
    解析の最新の日。
  - 件数信号に gold_annual_rows、gold_quarterly_rows、
    gold_dividend_rows、gold_companies を追加した。
  - stale の判定: 前日（点検対象日）以前に Silver へ解析が追記
    された最新の日より、Gold の生成日（summary の generated_at を
    JST の日付にしたもの）が古い場合に gold_stale 警報にする。
    当日分の解析は、その日の解析 run の後段で Gold が更新される
    途中でありうるため比較に含めない（monitor_gap 等の既存の
    考え方と同じく、前日以前を確定分として扱う）。
  - gold/summary.json が無い場合は missing とし、警報にはしない
    （会社マスタと同じ扱い）。

## 検証結果

- 既存テストのベースラインは 261件全件合格。変更後は **265件全件
  合格**（`python -m unittest discover -s tests`、tests.yml の CI と
  同じコマンド）。新規は test_monitor.py の GoldMonitorTest 4件
  （正常系で信号が載ること、stale で gold_stale 警報が報告と状態
  ファイルに載ること、missing で警報にならないこと、当日分の解析
  では stale にならないこと）。既存の monitor テスト（会社マスタの
  警報が単独で出ること等）は不変で合格している。
- 両ワークフローを YAML として読み込み、triggers と jobs の構成、
  gold-update の needs・if・concurrency、gold.yml の schedule と
  concurrency が意図どおりであることを確認した。
- ワークフローの実行ステップを抽出し、python をスタブに替えて
  分岐を実地検証した:
  - gold.yml、schedule 起動（command・since_month が空）:
    `pipeline.gold update --since-month 2016-03` が呼ばれ exit 0。
  - gold.yml、手動 dispatch の build（since_month 空）: 従来どおり
    `--since-month` は付かず exit 0。
  - gold.yml、手動 dispatch の update（since_month=2020-01）:
    `--since-month 2020-01` が付き exit 0。
  - gold.yml、since_month=2020-13: 形式検査で exit 1、python は
    呼ばれない（従来の検査が schedule 経路でも効くことの確認）。
  - daily_parse の gold-update ステップ: `pipeline.gold update
    --since-month 2016-03` が呼ばれ exit 0。

## 完了条件への対応

- 「Silver の日次差分が入った翌日までに Gold の update が自動で
  適用され summary.json の件数が増えること」: 主経路では解析と
  同じ run の中で適用されるため、翌日までを満たす。実地の確認
  （実際の日次 run で件数が増えること）はマージ後の運転で行う。
- 「件数信号が monitor の点検に載ること」: 上記のとおり追加し、
  テストで固定した。実物の報告への反映はマージ後の monitor run で
  確認する。
- 「手動 dispatch と schedule の重なりで競合しないこと」:
  concurrency グループ gold-write で直列化した（相乗りジョブも
  同じグループ）。

## 注意点と残件

- gold.yml を手動 dispatch で update する場合、since_month を
  指定しなければ gold.py の既定値 2021-11 が使われる（従来の
  挙動。本 PR では変えていない）。全期間の Gold を維持する手動
  update では since_month=2016-03 を指定すること。日次配線の
  両経路は常に 2016-03 を渡す。
- backfill_missing のワークフローは自前の解析で Silver へ書くため、
  その直後には Gold が未反映の期間が生じる。翌日の daily_parse の
  gold-update（対象0件の日も実行される）か gold.yml の schedule が
  回収し、その間は monitor の gold_stale が可視化する。
- gold_stale の誤警報の窓: 解析 run が JST の日付をまたいで長時間
  実行中の場合、前日以前の解析分について Gold が未更新のまま
  警報が出うる。run の完了で解消する性質のもので、会社マスタの
  stale と同じ性格である。
