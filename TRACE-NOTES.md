# JSONL trace sink — implementation notes

2026-09-23。branch `feat/jsonl-capture-sink`（base = `d1c6429` "changelog: v257"、
upstream の `e054c90a` の 1 commit 先。`e054c90a` は HEAD の祖先）。未 commit。

llama-swap に「1 リクエスト 1 行の JSONL を書き出す write-only の出口」を足した。
既存の in-memory capture ring（`captureBuffer` / `/api/captures/{id}`）、メトリクス、
activity 行の挙動は変えていない（根拠は §4）。

**2026-09-23 改訂（第 2 版）**: 出口を「平文 1 ファイル / FIFO」から
**「ディレクトリに zstd 圧縮 ＋ サイズでローテーション」** に変えた。FIFO 出力は廃止
（設定の `path` を `dir` / `maxFileBytes` / `level` に置換）。外部分割器（`s6-log`）を
使う計画は取りやめ、圧縮とローテーションを llama-swap 自身が持つ。
影響する節は §1・§2.2・§2.3・§3・§6・§8。それ以外（レコードの中身・emit 箇所・
`cf` マスク・`outcome` の 4 値）は第 1 版のまま。

## 1. 触ったファイル

| ファイル | 変更 |
|---|---|
| `internal/server/trace.go` | 新規。sink 本体（writer goroutine・zstd ファイル・ローテーション・レコード組み立て・符号化） |
| `internal/server/trace_test.go` | 新規。17 テスト（うち 2 本は 2 subtest） |
| ~~`internal/server/trace_fifo_test.go`~~ | 第 2 版で削除（FIFO 出力の廃止に伴い） |
| `internal/server/metrics.go` | `trace` フィールド／`attachTrace`／`wantsRequestCapture`／`Close`／`record` 内の 5 箇所の emit／`successOutcome` |
| `internal/server/metrics_middleware.go` | 1 行。リクエスト本文の buffering 条件を `mm.enableCaptures` → `mm.wantsRequestCapture()` |
| `internal/server/server.go` | `attachTrace` の配線、`Server.Shutdown` から `metrics.Close()` |
| `internal/config/config.go` | `TraceConfig` と `Config.Trace` |
| `config-schema.json` | `trace` プロパティ |
| `docs/config.example.yaml` | コメントアウトした `trace:` 例（`# store:` と同じ扱い） |
| `docs/kb/guides/operations/observability-storage-and-activity.md` | `trace` の節と `config_keys` |

設定:

```yaml
trace:
  enabled: false                       # 既定 false
  dir: /var/log/llama-swap/trace       # 出力先ディレクトリ（無ければ作る）
  maxFileBytes: 268435456              # 圧縮後がこれを超えたらローテーション（既定 256 MiB）
  level: 3                             # zstd レベル。省略時は klauspost の既定
  includeAborted: false                # 既定 false。true なら 499 も 1 行
```

## 2. 設計判断

### 2.1 emit する場所 — `storeCapture` の「中」ではなく「隣」

指示書は「足す場所は `metrics.go:267` の `storeCapture`」だった。実装では
`storeCapture` の呼び出しの直後（`record` 内）に `mp.writeTrace(...)` を置いた。
現在の行番号は `metrics.go:208`（499）・`:237`（非 200）・`:259`（本文が空）・
`:282`（展開失敗）・`:336`（200）。理由は 3 つで、
いずれも「封筒が揃うのは storeCapture の呼び出し地点であって storeCapture の中ではない」に帰着する。

1. `storeCapture` は `if !mp.enableCaptures { return false }` で即 return する。sink は
   `captureBuffer: 0` でも動く必要があるので、この gate の内側には置けない。
2. `storeCapture` は activity 行（`tm`）を受け取らない。**join に必要な timestamp・
   モデル・status・duration・トークンが引数に無い。** 署名を変えると既存の呼び出しと
   テストに波及する。
3. 非 200 の呼び出し（`:223`）は `body` に `nil` を渡す。**sink が残したい失敗応答の本文は
   呼び出し側の `decoded` にしか無い。**

### 2.2 出力先はディレクトリ、圧縮とローテーションは自前（第 2 版）

第 1 版の FIFO 出力（読み手が付くまで `open(2)` が block する前提の別 goroutine
blocking open、`O_NONBLOCK` を使わない理由、EPIPE の扱い）は**廃止した**。
現在の形は:

- 設定は `dir`（ディレクトリ。無ければ `MkdirAll`）。ファイルは
  `trace-<timestamp>.jsonl.zst`（timestamp は Go の `20060102T150405Z0700`）。
  **名前は open した時点で確定し、rename しない。** 同じ秒に 2 本開く場合は
  `_001` …（ゼロ埋め）の接尾辞。`'_' > '.'` なので、**接尾辞なしの名前がその秒の先頭に来て、
  以後は連番順**に並ぶ＝ ls / glob の順序がそのまま書いた順序になる。open は `O_EXCL` なので
  stat→create の競合が無い。
- 圧縮は `github.com/klauspost/compress/zstd`（既に直接依存、`captures.go` が使用）。
  **`WithEncoderConcurrency(1)`**: writer goroutine が 1 本でファイルとエンコーダを
  単独所有する不変条件を保つ＋実測スループットが 1 MB/s を大きく下回るため単スレッドで
  3 桁の余裕がある。既定（GOMAXPROCS）だとファイルごとにブロックワーカーの pool が立つ。
- **レコードごとに `Flush()`**。(a) プロセスが落ちても直前のレコードまで展開できる。
  (b) 圧縮後バイト数が正確に分かり、ローテーション判定がぶれない（flush しないと
  エンコーダ内部に溜まりファイルサイズが遅れて追従する）。1 レコードは数百 KB なので
  flush 境界による圧縮率の劣化は無視できる。
- **ローテーション時にエンコーダを `Close()`** してフレームを閉じる。切り出された 1 本 1 本が
  単独で `zstd -d` できる完全なストリーム。**辞書（`--train`）も差分（`--patch-from`）も
  使わない** — 実測で辞書は独立圧縮に負け、差分は復号時に依存の鎖を作るため。
- 閾値（`maxFileBytes`）は**圧縮後バイト数**で、レコードを書いて flush した後に判定する。
  レコードの途中では切らないので、実サイズは**閾値 ＋ 最後の 1 レコード**まで伸びる
  （ハードな上限ではない）。この性質はコード・schema・KB ガイド・config 例に明記した。
- **削除（世代上限）は実装しない。** 保管側の方針が「コピーのみ・削除しない」なので、
  `n` 相当のパラメータを作っていない。

**行が混ざらない根拠**は第 1 版と同じ: 符号化済みの 1 行を channel 越しに渡し、
**ファイルとエンコーダを所有する goroutine は 1 本だけ**。並行リクエストが同じ
fd / エンコーダに同時に触ることがない。

### 2.3 閉じたファイルは開き直さない

ローテーションで閉じたファイルは**二度と開かない・rename しない・名前を再利用しない**。
だから「最新でないファイル」は確定済みで、外部のコピー元として安全に扱える。
書き込みエラーは 1 回だけ警告して以後は黙って捨てる（リクエストは壊さない）。
open 自体に失敗した場合は `broken` を立てて以後リトライしない（書けないディレクトリが
走行中に書けるようになることは無く、レコードごとのリトライはログを埋めるだけ）。

### 2.4 本文は verbatim

`req.body` / `resp.body` は JSON の**文字列**として埋める。JSON として parse して
埋め直していない（キー順・空白が変わり「線を流れたバイト」でなくなるため）。
`encoding/json` は**不正な UTF-8 を U+FFFD に置換してしまう**ので、`utf8.Valid` で判定し、
有効なら `body_encoding: "utf8"`、そうでなければ base64 にする。
`Encoder.SetEscapeHTML(false)` にしてあるので `<` `>` `&` もそのまま出る（どちらでも
decode 後のバイトは同一だが、目で読めた方がよい）。

### 2.5 `outcome` の 4 値

| 値 | 条件 |
|---|---|
| `ok` | status 200 で、クライアント接続が生きたまま終わった |
| `upstream_error` | 200 でも 499 でもない status |
| `client_disconnected` | 499 センチネル（応答 status を書く前にクライアントが切れた） |
| `client_disconnected_mid_stream` | status は 200 だが、`swaputil.ClientContext(r.Context()).Err() != nil` |

4 番目が指示書の言う穴を塞ぐ。`swaputil.MarkClientClosed` は「まだ status を書いていない」
ときしか 499 を立てないので、**SSE で既にヘッダが飛んだ後の切断は 200 のまま**記録され、
activity 行からは正常終了と区別できない。JSONL はここを区別する。

判定に使うのは request の context ではなく**クライアント接続自身の context**
（`swaputil.ClientContext`）。`MarkClientClosed` と同じ理由で、inflight tracker の
「オペレータによるキャンセル」や peer router の shutdown による**サーバ側キャンセルを
クライアント切断と誤認しない**ため。この区別は
`TestTrace_ServerSideCancelIsNotMidStream` で固定してある。

### 2.6 レコードの各フィールドの出どころ

| フィールド | 出どころ | 備考 |
|---|---|---|
| `id` / `ts` / `status` / `duration_ms` / `path` | activity 行 `tm` | `ts` は RFC3339 + ミリ秒 + ローカルオフセット |
| `used_model` | `tm.Model`（= `record` の `modelID`、解決後） | |
| `requested_model` | `swaputil.ReadContext(r.Context()).Model`（alias 側） | context が無ければ `""` |
| `tokens` | `tm.Tokens.InputTokens` / `OutputTokens` | 後述 |
| `error` | `tm.ErrorMsg`、空なら `null` | |
| `req.headers` | middleware が redact 済みのもの | |
| `resp.headers` | `headerMap(recorder.Header())` → `redactHeaders` | |
| `req.body` / `resp.body` | verbatim（§2.4） | |

**`tokens` を `null` にする場合**: `outcome` が `upstream_error` / `client_disconnected`
のときは `null`。これらの経路ではトークン解析自体が走っていないので、`{"input":0,"output":0}`
と書くと「0 と実測された」に見えてしまう。`null` は「測っていない」の意。
`ok` / `client_disconnected_mid_stream` では解析が走っているので、結果が 0 でも数値で出す。

`duration_ms` と `used_model` は全経路で取れている（`record` の冒頭で `tm` に入る）。
**取れなかった値は `requested_model` だけ**で、これは request context に
`ReqContextData` が無い場合（通常の metered 経路では middleware が必ず入れるので空にならない）。

### 2.7 `cf` マスクを sink にも適用し、「落とした」事実を残す（裁定 1）

`captureFieldsByPath` が本文を捨てると決めている経路（`/v1/audio/speech` 等）では、
JSONL にも本文を書かない。ただし黙って消さず、落ちたことを行に残す:

```json
"resp": {"headers": {…}, "body": null, "body_omitted": "route_policy", "body_bytes": 1200}
```

- `body` は `*string` にした。**null（そもそも無い）と `""`（空だった）は別の事実**なので、
  同じ表現を共有させない。`body_encoding` は本文があるときだけ出る（`omitempty`）。
- `body_omitted` は理由の識別子（bool ではない）。サイズ上限等の理由を後から足せる。
- **req 側の `body_bytes` は `Content-Length`**。応答と違い、req 本文は
  `cf&captureReqBody == 0` のとき middleware がそもそも buffer しない
  （`metrics_middleware.go`）ので、測った長さが存在しない。計測していない場合は
  `body_bytes` を出さない（null）。
- 非 200 経路に渡すのは**ルートの `cf` そのもの**で、ring 側の `cf&^captureRespBody` ではない。
  「失敗応答の本文を残す」（§5 で維持された判断）は変えていない。

### 2.8 200 で行が出なかった 2 経路を塞いだ（裁定 2）

`record` の「本文が空」「展開失敗」の 2 経路は `queueAndEmit(); return` で早期に戻り、
**成功したリクエストが JSONL に 1 行も残らなかった**。emit を 2 つ足した。
**既存の `queueAndEmit(); return` は変えていない**（emit を挟んだだけ）。

- **本文が空**: `resp.body: ""` / `body_encoding: "utf8"`（`body_omitted` は付かない。
  空だったのであって落としたのではない）。
- **展開失敗**: `error` に既存の `tm.ErrorMsg`（`response decompression failed: …`）、
  `resp.body` は**展開前の生バイトを base64**。このときだけ `resp.headers` の
  `Content-Encoding` を**残す**（§2.7 と逆）。その行の body は展開後ではなく線を流れた形なので、
  `Content-Encoding` はまだそのバイトを正しく説明している。消費側が gunzip すべき対象は
  まさにこれ。実装上は `traceEvent.respIsWire` が両方（base64 強制 + ヘッダ温存）を決める。
- **`outcome` は裁定文では `"ok"` だったが、実装は `successOutcome(r)` を通している。**
  通常は `ok` になる。クライアントが実際に途中で切れていた場合だけ
  `client_disconnected_mid_stream` になり、そこを `ok` と書くと §2.5 で塞いだ穴が
  この 2 経路から再び開く。裁定の趣旨（「エラーではなく成功として記録せよ」）は満たしている。
  **監督の確認事項**（§5-1）。

### 2.9 `writeTrace` の引数を struct にした

裁定 1・2 で `cf` と `respIsWire` が増え、位置引数が 9 個になるところだった
（`reqBody` / `respBody` のように取り違えやすい同型引数が並ぶ）。`traceEvent` に畳んだ。
`record` 側の呼び出しは 5 箇所とも複合リテラル。**呼び出しを足しただけで、
`record` の既存の式・early return の条件は無変更**（§4-3 は維持）。

### 2.10 `resp.headers` から `Content-Encoding` を落とす

`resp.body` に入るのは `record` が展開した後の本文（gzip/deflate は
`decompressBody` が展開済み）。`Content-Encoding: gzip` を残すと、消費側が平文を
gunzip しようとする。既存の `storeCapture` も同じ理由で落としているので、それに揃えた。

## 3. ライフサイクル

- `Server.Shutdown`（`server.go`）の末尾、`wg.Wait()` の後に `s.metrics.Close()` を足した。
  これで dead code だった `metricsMonitor.Close()` が生きる。`Close` は sink に quit を送り、
  **既に queue に載っている行を drain し、開いているファイルの zstd フレームを閉じてから
  ファイルを閉じる**。
- **`Close` の待ちは無制限にした**（第 1 版の 3 秒 timeout は削除）。timeout の理由だった
  「読み手の付かない FIFO で writer が `open(2)` に永久に park し得る」は FIFO 廃止で
  消滅し、writer が block し得るのは通常ファイルへの write だけになった。逆に
  **最後のフレームを閉じられるのは Close だけ**なので、ここで打ち切る方が害が大きい。
- Close されずにプロセスが死んだ場合は、最後のファイルがフレーム未終端（epilogue 無し）
  になる。レコードごとに flush しているので**そこまでのレコードは展開でき**、復号器は
  末尾で unexpected EOF を報告する（`TestTrace_UnclosedFileReadsToLastFlush`）。
- 書き込み失敗でリクエスト処理は壊れない。`write` は non-blocking（channel が満杯なら
  ドロップ + 警告）、ファイル write のエラーは 1 回だけ警告して続行。
  encode 失敗も警告して 1 行捨てるだけ。

## 4. 既存の挙動を変えていない根拠

1. **capture ring**: `addCapture` / `storeCapture` / `captureCache` / `compressCapture` を
   1 文字も触っていない。sink は `captureCache` を読みも書きもしない。
2. **`/api/captures/{id}`**: `captures.go` は `ReqRespCapture` も含めて無変更。
3. **メトリクス・activity 行**: `record` への変更は「`writeTrace` の呼び出しを 5 箇所
   足した」だけ（裁定 2 で 3 → 5）。`tm` を組み立てる式・`queueMetrics`・`emitMetric`・`cf` マスク・早期 return の
   条件はいずれも無変更（`git diff internal/server/metrics.go` で確認できる）。
4. **middleware**: 変更は buffering 条件 1 行のみ
   （`mm.enableCaptures` → `mm.wantsRequestCapture()` = `enableCaptures || trace != nil`）。
   sink が無効なら式は `mm.enableCaptures` と同値。**何を buffer するか（`cf` マスク）は無変更。**
   sink は `captureBuffer: 0` でも req body が要るので、この 1 行が必要だった。
5. **既定値**: `TraceConfig` の zero value は `enabled: false`。既存の設定ファイルの
   挙動は変わらない。`TestTrace_DisabledWritesNothing` で「ファイルすら作らない」ことと
   「activity 行は従来どおり 1 件入る」ことを固定してある。
6. `go test ./...` / `go test -race ./internal/...` いずれも既存テストを含め全て pass。

## 5. 監督が検分すべき点（設計の穴・判断待ち）

**2026-09-23 の裁定で 1・2 は解決済み**（§2.7 / §2.8 に実装した）。残るのは以下。

1. **裁定 2 の `outcome` の読み替え。** 裁定文は新設 2 経路とも `outcome: "ok"` と書いているが、
   実装は `successOutcome(r)` を通した（§2.8 末尾）。通常は `ok`、クライアントが実際に切れていた
   ときだけ `client_disconnected_mid_stream`。**文字どおり `ok` 固定にすべきなら差し戻し。**
2. **req 側の `body_bytes` は `Content-Length`**（§2.7）。応答側の「落とした実バイト数」とは
   出どころが違う。`-1`（宣言なし）のときは `body_bytes` を出さない。
   実測を出したければ middleware に buffer させる必要があり、それは `cf` を適用する目的
   （巨大バイナリを読まない）と衝突する。
3. **`body_omitted` の値は現在 `route_policy` の 1 つだけ。** 将来の理由（サイズ上限等）を
   足す前提の識別子にしてある。

### 5.1 裁定前の判断待ち（1・2 は解決。3 以降は未解決のまま残る検分点）

1. ~~**`cf` マスクを sink 側に適用していない。**~~ → **裁定 1 で適用済み**（§2.7）。以下は当時の記述。
   指示書のレコード様式に `cf` の話が無いので、
   取れる本文はそのまま出している。結果として
   `/v1/audio/speech` `/v1/images/generations` `/sdapi/v1/txt2img` 等、
   `captureFieldsByPath`（`captures.go:41`）が「本文を捨てる」と決めている経路でも、
   **JSONL には応答本文が base64 で丸ごと載る**（音声・画像がそのまま巨大な 1 行になる）。
   ring 側の挙動は変えていないので既存機能に影響は無いが、FIFO に流す運用では詰まり得る。
   「sink も `cf` を尊重する」に倒すなら 1 行で変えられる。**判断を仰ぎたい。**
2. ~~**JSONL 行が出ない 200 の経路が 2 つ残っている。**~~ → **裁定 2 で塞いだ**（§2.8）。
   以下は当時の記述。`record` は
   `:234`（応答本文が空）と `:244`（Content-Encoding の展開失敗）で
   `storeCapture` に到達せず早期 return する。指示書が emit 箇所として挙げたのは
   `:199`/`:259`（現 `:223`/`:287`）の 2 箇所だったのでそこに揃えた。
   ここも塞ぐなら emit 箇所を 2 つ増やす。**設計の拡張になるので独断でやっていない。**
3. **`x-claude-code-session-id` は redact されない**（`sensitiveHeaders` は 5 つのみ）。
   既存の redaction をそのまま通しただけで、sink 側で足していない。
   ただし sink はリクエスト／レスポンス本文を全部保存するので、
   **ring より機微情報の露出面は明確に大きい**。出力先のパーミッションは運用側の責任。
4. **`trace.enabled: true` かつ `dir: ""`（および `MkdirAll` の失敗）は起動エラーに
   せず、警告を出して無効化**している（`load.go` の検証に手を入れると blast radius が
   広がるため）。起動時に落としたいなら `load.go` 側に移す。
5. **`docs/` と `config-schema.json` にも手を入れた。** 指示書には無かったが、
   リポジトリの `AGENTS.md` が「設定オプションを足したら `docs/kb/guides/` と
   `config.example.yaml` と `config-schema.json` を更新せよ」と要求しているため。
   `config.example.yaml` へはコメントアウトした形で入れた（`# store:` と同じ扱い。
   `internal/docagent/golden_test.go` の section 一覧を触らずに済む）。不要なら落とせる。
6. **queue 深さ 1024**（`traceQueueDepth`）と **既定閾値 256 MiB**
   （`traceDefaultMaxFileBytes`）は根拠のある実測値ではなく、設計上の既定値。
   第 1 版にあった `traceCloseTimeout`（3 秒）は第 2 版で削除した（§3）。
7. **同一秒の連番は 3 桁ゼロ埋め**で、`traceMaxNameSeq = 1000` に達すると
   open がエラーになる（= sink が止まる）。桁が増えると名前が open 順に並ばなくなるため
   そこで切っている。1 秒に 1000 本ローテーションする設定は誤設定の域。

## 6. テスト

`internal/server/trace_test.go`（17 本、うち 2 本は 2 subtest）。
指示書が求めた 6 本は以下:

| 指示書の要求 | テスト |
|---|---|
| 200 非ストリームで本文がバイト単位一致 | `TestTrace_SuccessIsByteExact` |
| SSE で `data:` 行の全文 | `TestTrace_StreamingKeepsWholeEventStream` |
| 非 200 で `upstream_error` と本文 | `TestTrace_UpstreamErrorKeepsResponseBody` |
| `enabled: false` で何も書かれない | `TestTrace_DisabledWritesNothing` |
| 不正な UTF-8 が base64 に | `TestTrace_InvalidUTF8IsBase64` |
| 並行リクエストで行が混ざらない | `TestTrace_ConcurrentRequestsDoNotInterleave` |

並行テストは 24 並列・本文それぞれ 32 KiB で、行が混ざれば
JSON の parse が壊れるか marker が食い違う形にしてある。

追加した 4 本（指示書の要求ではないが、設計の要を固定するもの）:

- `TestTrace_MidStreamDisconnect` — 200 のまま切れた SSE が
  `client_disconnected_mid_stream` になる（塞いだ穴そのもの）
- `TestTrace_ServerSideCancelIsNotMidStream` — サーバ側キャンセルは切断扱いしない
- `TestTrace_AbortedRequest`（2 subtest）— 499 は既定で出ない／
  `includeAborted: true` で req のみ 1 行
- `TestTrace_RedactsSensitiveHeaders` — 既存の redaction を通っている

裁定（2026-09-23）で追加した 3 本:

| 裁定の要求 | テスト |
|---|---|
| `cf` で本文が落ちる経路で `body_omitted` と `body_bytes` | `TestTrace_RoutePolicyOmitsBody`（2 subtest） |
| 200 で本文が空のとき 1 行出る | `TestTrace_EmptyResponseBodyStillEmitsLine` |
| 展開失敗で生バイトが base64 | `TestTrace_DecompressionFailureKeepsWireBytes` |

- `RoutePolicyOmitsBody` は `captureFieldsFor(path)` を通して**実際のマスク表**を使う
  （`/v1/audio/speech` = resp 本文が落ちる／`/v1/audio/transcriptions` = req 本文が落ちる）。
  落ちた側は `body: null` + `body_omitted` + `body_bytes` + `body_encoding` 無し、
  残る側は verbatim、の両方を見る。
- `EmptyResponseBodyStillEmitsLine` は行が 1 本出ることに加えて、
  **`body: ""` であって `body_omitted` が付かない**ことを見る（空と落としたの区別）。
- `DecompressionFailureKeepsWireBytes` は base64 を decode して生バイト一致を見るほか、
  **`Content-Encoding: gzip` がヘッダに残っている**ことを見る（§2.8）。

既存 11 本のうち body を読む箇所は、`Body` が `*string` になったのに伴い
テスト側の `tracePayload.body()` ヘルパ経由に書き換えた（判定内容は不変）。

第 2 版（zstd ＋ ローテーション）で追加した 4 本:

| 要求 | テスト |
|---|---|
| 閾値を小さくして複数ファイルが生成される／各ファイルが単独で展開できる／全ファイルを展開して連結したものが書き込んだレコード列とバイト一致／各行が JSON として妥当 | `TestTrace_RotationKeepsEveryByte` |
| リクエスト経路からもローテーションが起き、順序が保たれる | `TestTrace_RotatesAcrossRequests` |
| `Close()` されずに終わっても flush 済みレコードまで展開できる | `TestTrace_UnclosedFileReadsToLastFlush` |
| 同一秒でも名前が衝突せず、名前が open 順に並ぶ | `TestTrace_NamesAreUniqueAndSorted` |

- `RotationKeepsEveryByte` は `newTraceWriter` に直接 4 KiB のランダム hex を
  含むレコードを 24 本渡し（`traceQueueDepth` の 1024 より十分少ないのでドロップ 0 を
  検査）、閾値 1 KiB で複数ファイルに割る。各ファイルは**その都度新しい `zstd.Decoder`**
  で開く（辞書も前のファイルも視界に無いので、自己完結でなければ失敗する）。
- テスト側のヘルパ（`traceFiles` / `decodeTraceFile` / `readTraceBytes`）は
  「ディレクトリ内の全ファイルを名前順に展開して連結」を担い、既存 13 本はこの経路に
  載せ替えただけで判定内容は不変。
- 第 2 版で削除: `trace_fifo_test.go`（`TestTrace_FIFOTargetDoesNotBlock`）。

### 6.1 外部オラクル（zstd CLI）での確認

Go の decoder だけでなく **zstd CLI 1.5.7（`nix shell nixpkgs#zstd`）** で同じ主張を取った。
一時テストで 12 本のファイル（閾値 1 KiB・level 3）を吐かせて:

```
$ zstd -t trace-20260923T123819+0900.jsonl.zst
trace-20260923T123819+0900.jsonl.zst: 8466 bytes
$ zstd -t *.zst
12 files decompressed : 101595 bytes total
$ zstd -dc *.zst > joined.jsonl
$ cmp joined.jsonl expected.jsonl && echo IDENTICAL
IDENTICAL
```

（`expected.jsonl` は sink に渡した行をそのまま連結したもの。一時テストは削除済み。）

## 7. 出力例

```json
{"id":1,"ts":"2026-09-23T04:09:52.721+09:00","outcome":"ok","path":"/v1/chat/completions","requested_model":"sonnet","used_model":"claude-sonnet-4","status":200,"duration_ms":0,"tokens":{"input":12,"output":7},"error":null,"req":{"headers":{"Authorization":"[REDACTED]","Content-Type":"application/json"},"body":"{\"model\":\"sonnet\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}","body_encoding":"utf8"},"resp":{"headers":{"Content-Type":"application/json"},"body":"{\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7},\"choices\":[{\"text\":\"<b>hi</b>\"}]}","body_encoding":"utf8"}}
```

実際に 1 行で出る（改行は行末の 1 つだけ）。`<b>` が escape されていないのは
`SetEscapeHTML(false)` のため。

## 8. 実行環境（Go）

`go` が PATH に無かった。nix store の Go 1.24.13 を使い、`go.mod` の `go 1.27.1` 指定に
よる自動 toolchain 取得（`go: downloading go1.27.1`）で 1.27.1 が使われる。再現手順:

```bash
export PATH=/nix/store/sqcn3z4zax0anz48ymwfqs2dp7q6vhqy-go-1.24.13/bin:$PATH
go build ./...
make test-dev                    # = go test -short ./... → 全 ok, exit 0
go test ./...                    # = 24 packages ok, exit 0
go test -race -count=1 ./internal/...   # = make test-all 相当、exit 0
gofmt -l ./internal ./cmd        # 出力なし
go vet ./...                     # exit 0
```

`nix run nixpkgs#go` でも代替可（ネットワークは到達する）。

**`make test-dev` の 2 段目 `staticcheck ./...` は環境に staticcheck が無く走らない**
（`sh: line 1: staticcheck: command not found`。Makefile が `|| true` にしているので
exit 0 は保たれる）。`nix run nixpkgs#go-tools` の staticcheck 2026.2.1 は go1.26 ビルドで、
本リポジトリの go1.27 モジュールを解析できず全 package が
`package requires newer Go version go1.27` で compile error になる。

**第 2 版で取得方法を見つけた**: go.mod と同じ go1.27.1 から staticcheck をビルドすれば
バージョン差が出ない。

```bash
nix shell nixpkgs#go --command go run honnef.co/go/tools/cmd/staticcheck@latest ./...
```

結果は **16 件、すべて上流由来**（`cmd/kubeswap` 3 / `cmd/vllm-wrapper` 2 /
`internal/config/mcpprovider.go` 1 / `internal/perf` 9 / `internal/swaputil/http.go` 1）で、
**trace sink が触った範囲（`internal/server/trace*.go` / `internal/config/config.go`）
への指摘は 0 件**。上流由来の 16 件は直していない。

## 9. 第 3 版（2026-09-23）— 状態記録・チェックポイント・マスク

第 2 版までの出口は**リクエストの記録しか持たなかった**ため、記録を見ても
「どのバックエンドが、どの設定で答えたのか」が分からず、推論サーバの入出力を再現できなかった。
第 3 版でそこを埋める。**レコードの verbatim 性・`body: null` と `body: ""` の型レベルの区別・
単一 writer goroutine の所有・投入側の非ブロック性・ローテーションの意味論・`record` の 5 箇所の
emit 位置**はいずれも無変更。

### 9.1 触ったファイル（第 3 版）

| ファイル | 変更 |
|---|---|
| `internal/server/trace.go` | `type` / `v` の導入、`request` レコードに `method` / `remote_ip` / クエリ込み `path`、writer への `mask` / `checkpoint` フック |
| `internal/server/tracemask.go` | 新規。マスク機構（`maskPaths` の sjson 適用、`maskEnv` の名前指定、本文パスの拒否） |
| `internal/server/tracestate.go` | 新規。イベントバス購読・スナップショット・チェックポイント描画・Server 側の状態 seam |
| `internal/server/tracestate_test.go` | 新規。14 テスト |
| `internal/server/trace_test.go` | ヘルパ 2 箇所の署名追従のみ |
| `internal/server/metrics.go` | `traceState` フィールド、`attachTrace` の組み立て順、`Close` で購読解除 |
| `internal/server/server.go` | 1 行（`attachTrace` に `s.traceState` を渡す） |
| `internal/config/config.go` | `TraceStateConfig`、`MaskPaths`、`MaskEnv` |
| `config-schema.json` / `docs/config.example.yaml` / `docs/kb/guides/operations/observability-storage-and-activity.md` | 新設定の説明（機密が入ること・マスクの案内・fail-open と本文非対象の明記） |

### 9.2 レコードの種別と形式版

全レコードの先頭に `"type"` と `"v"`（`traceFormatVersion = 1`）を置いた。
種別は `request` / `backend` / `config` / `checkpoint`。
`backend` / `config` / `checkpoint` は**ペイロードを種別名のキーの下に入れ子**にしてある
（`{"type":"backend", …, "backend":{…}}`）。マスクのパスが
`backend.cmd` / `req.headers.Authorization` のように種別ごとに一意に書けるため。

### 9.3 マクロ展開後の `cmd` は取れる（実測）

設定上の `cmd` はテンプレートだが、**マクロ展開は設定ロード時に完了している**
（`internal/config/macros.go` の `resolveConfigMacros` が `${…}` / `${PORT}` / `${MODEL_ID}` /
`${env.*}` を typed `Config` に焼き込む）。`${PID}` だけは実行時展開だが、これは `cmdStop`
専用で `cmd` には現れない（`config/macros.go:351` の `allowPID`）。したがって
`cfg.Models[id].Cmd` は展開後の実物で、`SanitizedCommand()`（`process.doStart` が
`exec.Command` に渡すのと**同じ呼び出し**）で argv になる。upstream も同様に
`Proxy` が解決済み（`http://localhost:5800`）。

### 9.4 `ReloadingStateStart` は現状どこからも emit されていない

`swaputil.ReloadingStateStart` / `End` の両方を書く実装にしたが、**リポジトリ内で
`event.Emit` しているのは `llama-swap.go:386` の `ReloadingStateEnd` だけ**で、
`Start` の emit 箇所が存在しない。しかも `End` は新 Server 構築・旧 Server shutdown の
**3 秒後**に `time.AfterFunc` で飛ぶので、受け取るのは新 Server 側の出口になる。
実装は両方を扱うので、`Start` が emit されるようになればそのまま記録される。

### 9.5 チェックポイントの seam を writer の外に置いた

要求は「writer goroutine をプロセス管理の取得でブロックさせない」。取った形:

- `traceStateFunc`（構築時に渡す）が唯一の窓口。`Server.traceState` が実装で、
  `s.local.RunningModels()`・`s.ActiveProfile()`・`yaml.Marshal(s.cfg)` を読む。
- **呼ぶのは tracer の goroutine だけ**（構築時と各イベント配送時）。結果は
  `atomic.Pointer[traceSnapshot]` に publish する。
- writer goroutine が呼ぶ `checkpointLine(now)` は**この atomic を load して整形するだけ**。
  ルータのロックにも、起動中のプロセスにも触らない。実効設定の JSON 化は
  `sync.OnceValues` で tracer 側に 1 回だけ寄せてある（設定は Server の生存期間中不変）。

### 9.6 順序は同じキューで保つ

状態記録はイベント経由で非同期に届くが、**`traceWriter.write` という同じ channel** に
入れる。別経路でファイルに直接書かせていない（単一 writer goroutine の所有を壊さないため）。
`TestTrace_StateAndRequestsKeepOrder` が backend / request / config の並びを固定する。

チェックポイントだけは writer がファイルを開いた直後に**ファイルの 1 行目として**書く。
サイズ閾値の判定はこのとき行わない（どのファイルも「前文 ＋ 最低 1 レコード」になる）。

### 9.7 マスクの境界

- `maskPaths` は**符号化済みの行に sjson で適用**する。sjson は触らない部分をバイト列のまま
  残すので、**マスクした行でも本文は verbatim のまま**。存在しないパスは
  `gjson.Exists` で弾いてから set する（sjson は無いパスを**作ってしまう**ため。
  作らせるとリクエストレコードに `backend` が生えて「フィールドの不在」の意味が壊れる）。
- **`req.body` / `resp.body` およびそれを含む `req` / `resp` は起動時に拒否**し、警告を出す。
  本文を verbatim で持つのがこの形式の根本の約束なので、パスで書き換えさせない。
- `maskEnv` は**レコード組み立て時**に適用する。env は `"NAME=value"` の配列で
  名前が値の内側にあり、JSON パスでは 1 エントリを選べないため。チェックポイントが運ぶ
  実効設定の `models.*.env` にも、汎用値を歩いて同じマスクを掛ける。
- **fail-open**（列挙漏れは漏れる）と**本文非対象**は設定の説明 3 箇所すべてに明記した。

### 9.8 状態記録は既定 false の個別スイッチ

`trace.enabled` は出口全体の親スイッチのまま。そのうえで
`trace.state.backend` / `.config` / `.checkpoint` を**既定 false** で追加した。
`request` レコードへの `type` / `v` / `method` / 完全 `path` / `remote_ip` の追加は
スイッチ無しで常に入る（機密ではないため）。
