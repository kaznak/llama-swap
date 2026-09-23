# JSONL capture sink — implementation notes

2026-09-23。branch `feat/jsonl-capture-sink`（base = `d1c6429` "changelog: v257"、
upstream の `e054c90a` の 1 commit 先。`e054c90a` は HEAD の祖先）。未 commit。

llama-swap に「1 リクエスト 1 行の JSONL を書き出す write-only の出口」を足した。
既存の in-memory capture ring（`captureBuffer` / `/api/captures/{id}`）、メトリクス、
activity 行の挙動は変えていない（根拠は §4）。

## 1. 触ったファイル

| ファイル | 変更 |
|---|---|
| `internal/server/capturelog.go` | 新規。sink 本体（writer goroutine・レコード組み立て・符号化） |
| `internal/server/capturelog_test.go` | 新規。13 テスト（うち 2 本は 2 subtest） |
| `internal/server/capturelog_fifo_test.go` | 新規。FIFO テスト（`//go:build unix`） |
| `internal/server/metrics.go` | `captureLog` フィールド／`attachCaptureLog`／`wantsRequestCapture`／`Close`／`record` 内の 5 箇所の emit／`successOutcome` |
| `internal/server/metrics_middleware.go` | 1 行。リクエスト本文の buffering 条件を `mm.enableCaptures` → `mm.wantsRequestCapture()` |
| `internal/server/server.go` | `attachCaptureLog` の配線、`Server.Shutdown` から `metrics.Close()` |
| `internal/config/config.go` | `CaptureLogConfig` と `Config.CaptureLog` |
| `config-schema.json` | `captureLog` プロパティ |
| `docs/config.example.yaml` | コメントアウトした `captureLog:` 例（`# store:` と同じ扱い） |
| `docs/kb/guides/operations/observability-storage-and-activity.md` | `captureLog` の節と `config_keys` |

設定:

```yaml
captureLog:
  enabled: false          # 既定 false
  path: ""                # 通常ファイルでも FIFO でもよい
  includeAborted: false   # 既定 false。true なら 499 も 1 行
```

## 2. 設計判断

### 2.1 emit する場所 — `storeCapture` の「中」ではなく「隣」

指示書は「足す場所は `metrics.go:267` の `storeCapture`」だった。実装では
`storeCapture` の呼び出しの直後（`record` 内）に `mp.writeCaptureLog(...)` を置いた。
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

### 2.2 FIFO — 「遅延オープン」ではなく「別 goroutine での blocking open」

FIFO を書き込みで open すると読み手が付くまで block する（`open(2)`）。起動時に開くと
読み手が居ないと起動できない。採った形は:

- sink は**専用の writer goroutine を 1 本持ち、その goroutine の中で blocking open する**。
  起動パス（`attachCaptureLog`）は goroutine を起こして即 return するので、決して block しない。
  読み手が付くまでのレコードは容量 1024 の channel に溜まり、付いた瞬間に流れ出す。
  溢れたら**ドロップして警告**（リクエストは絶対に待たせない）。
- **`O_NONBLOCK` は使わない。** open だけ non-blocking にしても fd に `O_NONBLOCK` が残り、
  以後の write も non-blocking になる。FIFO への non-blocking write は `PIPE_BUF` (4096) を
  超えると**部分書き込みし得る**ので、JSON 行が途中で切れて次の行と繋がる。これを避けるには
  open 後に `fcntl(2)` で `O_NONBLOCK` を落とす必要があるが、Windows ビルドに移植できない。
  blocking open + blocking write + 単一 goroutine なら、行は常に丸ごと 1 回の `Write` で出る。
- 理由はコード中のコメント（`capturelog.go` の `open()` の doc comment）にも残してある。

**行が混ざらない根拠**もここ: 符号化済みの 1 行を channel 越しに渡し、**ファイルを所有する
goroutine は 1 本だけ**。並行リクエストが同じ fd に同時に書くことがない。

### 2.3 再オープンしない

ローテーション用の再オープン（SIGHUP 等）は実装していない。外部ローテータが FIFO の
読み手として受ける前提。**書き込みエラー（読み手が消えた場合の EPIPE を含む）でも
再オープンしない** — 1 回だけ警告を出して以後は黙って捨てる。ファイルを裏で rename する
ローテータに対しては、再オープンしない方が（rotate 後の inode に書き続けるより）まだ正直。
この判断もコードのコメントと KB ガイドに書いてある。

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
`TestCaptureLog_ServerSideCancelIsNotMidStream` で固定してある。

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
  まさにこれ。実装上は `captureLogEvent.respIsWire` が両方（base64 強制 + ヘッダ温存）を決める。
- **`outcome` は裁定文では `"ok"` だったが、実装は `successOutcome(r)` を通している。**
  通常は `ok` になる。クライアントが実際に途中で切れていた場合だけ
  `client_disconnected_mid_stream` になり、そこを `ok` と書くと §2.5 で塞いだ穴が
  この 2 経路から再び開く。裁定の趣旨（「エラーではなく成功として記録せよ」）は満たしている。
  **監督の確認事項**（§5-1）。

### 2.9 `writeCaptureLog` の引数を struct にした

裁定 1・2 で `cf` と `respIsWire` が増え、位置引数が 9 個になるところだった
（`reqBody` / `respBody` のように取り違えやすい同型引数が並ぶ）。`captureLogEvent` に畳んだ。
`record` 側の呼び出しは 5 箇所とも複合リテラル。**呼び出しを足しただけで、
`record` の既存の式・early return の条件は無変更**（§4-3 は維持）。

### 2.10 `resp.headers` から `Content-Encoding` を落とす

`resp.body` に入るのは `record` が展開した後の本文（gzip/deflate は
`decompressBody` が展開済み）。`Content-Encoding: gzip` を残すと、消費側が平文を
gunzip しようとする。既存の `storeCapture` も同じ理由で落としているので、それに揃えた。

## 3. ライフサイクル

- `Server.Shutdown`（`server.go`）の末尾、`wg.Wait()` の後に `s.metrics.Close()` を足した。
  これで dead code だった `metricsMonitor.Close()` が生きる。`Close` は sink に quit を送り、
  **既に queue に載っている行を drain してからファイルを閉じる**。
- `Close` は最大 3 秒しか待たない。**読み手の付かない FIFO では writer が `open(2)` で
  永久に止まり得る**ので、shutdown がそれを相続しないようにした（その場合は
  「開けていない＝flush するものが無い」ので捨ててよい）。警告は出る。
- 書き込み失敗でリクエスト処理は壊れない。`write` は non-blocking（channel が満杯なら
  ドロップ + 警告）、ファイル write のエラーは 1 回だけ警告して続行。
  encode 失敗も警告して 1 行捨てるだけ。

## 4. 既存の挙動を変えていない根拠

1. **capture ring**: `addCapture` / `storeCapture` / `captureCache` / `compressCapture` を
   1 文字も触っていない。sink は `captureCache` を読みも書きもしない。
2. **`/api/captures/{id}`**: `captures.go` は `ReqRespCapture` も含めて無変更。
3. **メトリクス・activity 行**: `record` への変更は「`writeCaptureLog` の呼び出しを 5 箇所
   足した」だけ（裁定 2 で 3 → 5）。`tm` を組み立てる式・`queueMetrics`・`emitMetric`・`cf` マスク・早期 return の
   条件はいずれも無変更（`git diff internal/server/metrics.go` で確認できる）。
4. **middleware**: 変更は buffering 条件 1 行のみ
   （`mm.enableCaptures` → `mm.wantsRequestCapture()` = `enableCaptures || captureLog != nil`）。
   sink が無効なら式は `mm.enableCaptures` と同値。**何を buffer するか（`cf` マスク）は無変更。**
   sink は `captureBuffer: 0` でも req body が要るので、この 1 行が必要だった。
5. **既定値**: `CaptureLogConfig` の zero value は `enabled: false`。既存の設定ファイルの
   挙動は変わらない。`TestCaptureLog_DisabledWritesNothing` で「ファイルすら作らない」ことと
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
4. **`captureLog.enabled: true` かつ `path: ""` は起動エラーにせず、警告を出して無効化**
   している（`load.go` の検証に手を入れると blast radius が広がるため）。
   起動時に落としたいなら `load.go` 側に移す。
5. **`docs/` と `config-schema.json` にも手を入れた。** 指示書には無かったが、
   リポジトリの `AGENTS.md` が「設定オプションを足したら `docs/kb/guides/` と
   `config.example.yaml` と `config-schema.json` を更新せよ」と要求しているため。
   `config.example.yaml` へはコメントアウトした形で入れた（`# store:` と同じ扱い。
   `internal/docagent/golden_test.go` の section 一覧を触らずに済む）。不要なら落とせる。
6. **`Close` の 3 秒**（`captureLogCloseTimeout`）と **queue 深さ 1024**
   （`captureLogQueueDepth`）は根拠のある実測値ではなく、設計上の既定値。

## 6. テスト

`internal/server/capturelog_test.go`（13 本、うち 2 本は 2 subtest）と
`capturelog_fifo_test.go`（1 本）。
指示書が求めた 6 本は以下:

| 指示書の要求 | テスト |
|---|---|
| 200 非ストリームで本文がバイト単位一致 | `TestCaptureLog_SuccessIsByteExact` |
| SSE で `data:` 行の全文 | `TestCaptureLog_StreamingKeepsWholeEventStream` |
| 非 200 で `upstream_error` と本文 | `TestCaptureLog_UpstreamErrorKeepsResponseBody` |
| `enabled: false` で何も書かれない | `TestCaptureLog_DisabledWritesNothing` |
| 不正な UTF-8 が base64 に | `TestCaptureLog_InvalidUTF8IsBase64` |
| 並行リクエストで行が混ざらない | `TestCaptureLog_ConcurrentRequestsDoNotInterleave` |

並行テストは 24 並列・本文それぞれ 32 KiB（`PIPE_BUF` の 8 倍）で、行が混ざれば
JSON の parse が壊れるか marker が食い違う形にしてある。

追加した 5 本（指示書の要求ではないが、設計の要を固定するもの）:

- `TestCaptureLog_MidStreamDisconnect` — 200 のまま切れた SSE が
  `client_disconnected_mid_stream` になる（塞いだ穴そのもの）
- `TestCaptureLog_ServerSideCancelIsNotMidStream` — サーバ側キャンセルは切断扱いしない
- `TestCaptureLog_AbortedRequest`（2 subtest）— 499 は既定で出ない／
  `includeAborted: true` で req のみ 1 行
- `TestCaptureLog_RedactsSensitiveHeaders` — 既存の redaction を通っている
- `TestCaptureLog_FIFOTargetDoesNotBlock` — FIFO を読み手なしで指定しても
  attach も `record` も block しない。**レコードを先に積んでから読み手を付けて**、
  その 1 行が流れてくることを確認する

裁定（2026-09-23）で追加した 3 本:

| 裁定の要求 | テスト |
|---|---|
| `cf` で本文が落ちる経路で `body_omitted` と `body_bytes` | `TestCaptureLog_RoutePolicyOmitsBody`（2 subtest） |
| 200 で本文が空のとき 1 行出る | `TestCaptureLog_EmptyResponseBodyStillEmitsLine` |
| 展開失敗で生バイトが base64 | `TestCaptureLog_DecompressionFailureKeepsWireBytes` |

- `RoutePolicyOmitsBody` は `captureFieldsFor(path)` を通して**実際のマスク表**を使う
  （`/v1/audio/speech` = resp 本文が落ちる／`/v1/audio/transcriptions` = req 本文が落ちる）。
  落ちた側は `body: null` + `body_omitted` + `body_bytes` + `body_encoding` 無し、
  残る側は verbatim、の両方を見る。
- `EmptyResponseBodyStillEmitsLine` は行が 1 本出ることに加えて、
  **`body: ""` であって `body_omitted` が付かない**ことを見る（空と落としたの区別）。
- `DecompressionFailureKeepsWireBytes` は base64 を decode して生バイト一致を見るほか、
  **`Content-Encoding: gzip` がヘッダに残っている**ことを見る（§2.8）。

既存 11 本のうち body を読む箇所は、`Body` が `*string` になったのに伴い
テスト側の `captureLogPayload.body()` ヘルパ経由に書き換えた（判定内容は不変）。

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

**`make test-dev` の 2 段目 `staticcheck ./...` は環境に staticcheck が無く走っていない**
（Makefile が `|| true` にしているので exit 0 は保たれる）。
`nix run nixpkgs#go-tools` の staticcheck 2026.2.1 は go1.26 ビルドで、
本リポジトリの go1.27 モジュールを解析できず全 package が
`package requires newer Go version go1.27` で compile error になる。
**したがって静的解析の結果は「緑」ではなく「未取得」**。代わりに `go vet ./...`（exit 0）を取った。
