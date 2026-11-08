# Cadge

乞討媒合服務：接收 OpenAI Chat Completions 請求，讓受信任的 Agent 原子領取，再將 Agent 交付的結果轉交給原始呼叫者。Cadge 不呼叫模型、不產生答案，也不需要 OpenAI API key。

使用 TypeScript、Cloudflare Workers、單一 SQLite-backed Durable Object；不需另外建立 Cloudflare Queue、D1 或 KV。

## 本機執行

需要目前 Wrangler 支援的 Node.js LTS 與 npm。

```sh
npm ci
cp .dev.vars.example .dev.vars
npm run dev
```

`.dev.vars` 只供本機使用，不納入版本控制。正式環境必須使用兩個不同、足夠長的隨機 token。

```sh
npm run check
npm test
```

`npm test` 先打包 Worker，再用 Miniflare/workerd 的 SQLite Durable Object 執行 HTTP 整合測試，包括競爭領取、交付對應、權限、取消、逾時，以及 Agent 上傳尚未結束時 Client 就收到首段 SSE 的即時轉送驗證。

## API

所有路由均需 `Authorization: Bearer <token>`。Client token 不能存取 Agent 路由，反之亦然。成功及錯誤回應均不快取。

| 路由 | Token | 用途 |
|---|---|---|
| `GET /v1/models` | `CLIENT_TOKEN` | 列出設定的模型識別字 |
| `POST /v1/chat/completions` | `CLIENT_TOKEN` | 入列並等待 Agent 交付 |
| `POST /beg` | `AGENT_TOKEN` | 原子領取最早尚未被領取、未逾時的工作 |
| `POST /deliver` | `AGENT_TOKEN` | 用領取 UUID 交付完整 JSON、即時 SSE 或處理失敗訊息 |

### 1. Client 提問

```sh
curl http://localhost:8787/v1/chat/completions \
  -H 'Authorization: Bearer local-client-token-change-before-deploy' \
  -H 'Content-Type: application/json' \
  -d '{"model":"cadge","messages":[{"role":"user","content":"你好"}]}'
```

這個連線會等待交付，預設 120 秒包含佇列與首段 SSE 等待；串流首段資料到達後永久解除此期限，非串流則必須在期限內完成。既有 OpenAI Chat provider 設定 `base_url=https://<worker-host>/v1`、API key 為 `CLIENT_TOKEN`、model 為 `MODEL_ID`；呼叫端也必須容許長串流。即時串流請在 body 加上 `"stream":true`；用 curl 觀察時加 `-N`，避免終端輸出被 curl 緩衝。

### 2. Agent 乞討

```sh
curl -i http://localhost:8787/beg \
  -X POST \
  -H 'Authorization: Bearer local-agent-token-change-before-deploy'
```

有工作時回 `200`，例如：

```json
{
  "id": "727bb93b-9a25-4754-a1ec-12c2a9cc89ad",
  "request": {
    "model": "cadge",
    "messages": [{ "role": "user", "content": "你好" }]
  },
  "expires_at": 1794048120000
}
```

- `id` 是**領取成功時產生的 UUID**，供交付使用，不是 OpenAI completion id。
- `request` 保留原始請求欄位，包括 `tools`、參數與 `stream`；Cadge 不計算 token，也不實作模型參數的語意。
- `expires_at` 為 Unix 毫秒的初始等待期限；從 Client 入列開始計時，不是從 Agent 領取開始。串流首段資料到達後解除此期限。
- 沒工作回 `204 No Content`，無 body，附 `Retry-After: 5`。Agent 必須自行實作等待；此值只是提示，不是伺服器限流。
- SQL 單一 `UPDATE … RETURNING` 完成選取與領取，同一筆工作不會被兩次成功領取。

Agent 可以自行選擇如何取得答案。如果轉送至真正的 OpenAI provider，應自行映射模型識別字，並依 `job.request.stream` 選擇模式：非串流交付完整 completion；`stream: true` 則立即轉送上游原始 OpenAI SSE，不要先收集整份結果，也不要把 SSE 文字當 completion JSON。

Agent 應在 `expires_at` 前交付首個非空 SSE chunk；僅認領或開啟上傳標頭不算開始回覆。**首段資料到達後立即解除 deadline，不再限制整段串流總時長或相鄰 chunk 間隔**，不因沉默幾秒或原始 120 秒已到而中斷。暖機慢、例如 50 秒才開始送資料，只要首段仍在初始期限內即可。非串流請求則必須在初始期限內交付完整 completion。

### 3. Agent 交付

#### 非串流交付

適用於 `job.request.stream` 為 `false` 或省略的工作。將上一步取得的 `id` 放入 body：

```sh
curl http://localhost:8787/deliver \
  -H 'Authorization: Bearer local-agent-token-change-before-deploy' \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"727bb93b-9a25-4754-a1ec-12c2a9cc89ad",
    "response":{
      "id":"chatcmpl-example",
      "object":"chat.completion",
      "created":1794048000,
      "model":"cadge",
      "choices":[{
        "index":0,
        "message":{"role":"assistant","content":"你好！"},
        "finish_reason":"stop"
      }],
      "usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}
    }
  }'
```

交付回 `200 {"accepted":true}`，代表已收件，不保證原始 Client 仍在等待。Client 仍等待時，收到完整 `response`，不加 Cadge 外層包裝；Client 已取消／請求已結束時，結果直接丟棄，不保存，也不向 Agent 揭露取消狀態。`response` 與 `error` 只能提供其中一個。

#### 即時串流交付

`job.request.stream === true` 時，將 **raw OpenAI SSE 分成有限長度的 POST**，不再使用一條持續寫入的 chunked HTTP upload。入口代理可能等整個 request EOF 才交給 Worker；有限分段讓每段能立即完成上傳並轉送，不受整份答案生成時間影響。

每段使用同一個 `/deliver`，body 是原始位元組，不加 JSON 外層：

| Header | 值 |
|---|---|
| `Content-Type` | `text/event-stream` |
| `X-Cadge-Claim-ID` | `/beg` 回傳的完整 UUID |
| `X-Cadge-Stream-Sequence` | 從 `0` 開始、逐段加一的非負 safe integer |
| `X-Cadge-Stream-End` | 一般段為 `0`；producer EOF 後送空 body 的最後一段為 `1` |

每段最多 **65,536 bytes**，準備好多少就送多少，不等待湊滿。每段都必須讀到 `200 {"accepted":true}` 才送下一段，不可平行上傳或自動重試。單段 ACK 不代表整份答案完成；最後 EOF 段的 ACK 才完成交付。可用有限 bytes body 的 `Content-Length` 避免重新形成長時間 chunked upload。

```sh
# chunk.sse 必須不超過 65,536 bytes；之後每段序號依序增加。
curl http://localhost:8787/deliver \
  -H 'Authorization: Bearer local-agent-token-change-before-deploy' \
  -H 'Content-Type: text/event-stream' \
  -H 'X-Cadge-Claim-ID: 727bb93b-9a25-4754-a1ec-12c2a9cc89ad' \
  -H 'X-Cadge-Stream-Sequence: 0' \
  -H 'X-Cadge-Stream-End: 0' \
  --data-binary @chunk.sse

# producer 已經 EOF；假設前面只送過一段。
curl http://localhost:8787/deliver \
  -H 'Authorization: Bearer local-agent-token-change-before-deploy' \
  -H 'Content-Type: text/event-stream' \
  -H 'X-Cadge-Claim-ID: 727bb93b-9a25-4754-a1ec-12c2a9cc89ad' \
  -H 'X-Cadge-Stream-Sequence: 1' \
  -H 'X-Cadge-Stream-End: 1' \
  --data-binary ''
```

一般 Agent 請直接使用 `deliver.mjs --stream`、`poll.mjs` 或下方的 Muse integration；它們自動分段並處理 EOF。舊的單一長連線 uploader 不相容，部署時必須同步更新 Agent。Client 的 OpenAI SSE 介面不變。

Cadge 保留一條 Client SSE，依序轉送各段，使用下游背壓限制記憶體。SSE 註解、UTF-8、文字／tool call chunks、usage 與 `[DONE]` 均原樣傳遞；SSE frame 或 UTF-8 字元可跨分段邊界。Cadge 不解析或補造 SSE 內容，也不插入 heartbeat。Client 中途離開後，識別碼保留期間內仍正常接收並丟棄後續段，不通知或中斷 Agent。

Node uploader 在等待上一段 ACK 時使用有限預讀緩衝，下一段合併當時已經就緒的位元組；不等待湊滿、不加 batching timer，避免每個小 token write 都多付一次 HTTP 往返。這不改變 SSE 內容或分段序號規則。

`/beg` 的 `204`／`200` 已直接區分空佇列與領取成功，沒有第二個派發 API。Muse 的 `wake → side_chat` 是執行該筆工作的模型入口，不是再派發一次。預設 5 秒空輪詢仍可能增加最多約 5 秒的起始等待；Muse 排程與模型生成也在 Cadge 傳輸之外。比較原生 API 時，應分別量測領取等待、上游首個內容與 Client 收到該內容的時間，不把模型思考或 Agent 喚醒算成 relay 延遲。

#### 處理失敗

Agent 處理失敗也必須交付，讓 Client 不必等到逾時：

```json
{
  "id": "727bb93b-9a25-4754-a1ec-12c2a9cc89ad",
  "error": { "message": "Agent 無法完成此次請求。" }
}
```

串流尚未開始前，兩種模式都能交付上述 JSON error：Agent 收到交付成功；若 Client 仍在等待，收到 OpenAI 形狀的 `502`、`error.code=agent_error`。若 Client 已取消或請求已結束，則只收件並丟棄。串流已開始後不能再對同一 UUID 交付 JSON error，應在原 SSE 中回報錯誤並結束，不可補造成功答案或 `[DONE]`。

### 錯誤與生命週期

| 情況 | HTTP |
|---|---:|
| Token 缺少、錯誤或角色不符 | 401 |
| JSON 或請求／交付基本格式錯誤 | 400 |
| 未知模型、未知 UUID、正在／已成功交付的 UUID | 404 |
| 已取消／逾時的已領取 UUID，仍在識別碼保留期間內 | Agent 200，正常收件回應，結果丟棄 |
| JSON 成功交付給串流工作／SSE 交付給非串流工作 | 409，`stream_required`／`stream_not_requested`，不消耗領取 UUID |
| SSE 段重疊或序號不是下一個預期值 | 409，`stream_packet_in_progress`／`stream_sequence_mismatch`，不消耗預期序號 |
| JSON body 超過 256 KiB／單一 SSE 段超過 65,536 bytes | 413 |
| Agent 回報失敗 | Client 502 |
| 缺少／重複 secrets、設定錯誤或協調器中斷 | 503 |
| 尚未開始回傳 SSE 就到期 | Client 504 |
| 有效 SSE 段讀取或轉送中斷 | Client SSE error，`agent_stream_error`，不補 `[DONE]` |

256 KiB 上限適用 Client JSON 請求與 JSON 交付；SSE 每段上限為 65,536 bytes，整份串流沒有總長度上限。兩者依實際位元組數檢查，不只信任 `Content-Length`。大型圖片請使用 URL，而不是大型 base64 請求 body。

待辦請求保存在 SQLite；交付、取消或到期時刪除 prompt，不保存結果。取消／逾時的已領取 UUID 僅保留約 5 分鐘的無內容標記，期間內晚到的有效交付仍回 `200 {"accepted":true}`，不回傳 `discarded` 等額外欄位。超過識別碼保留期則視為未知 UUID、回 `404`；標記由 alarm 批次清理。

**不自動重新派工或重送分段。** Agent 領取後失聯、初始期限內仍未開始回覆時，Client 回 `504`。已收到首段的串流不再受初始期限限制；有效分段的傳輸失敗以 SSE 錯誤結束。若 Agent 在兩段之間失聯，服務端無從區分失聯與合法沉默，因此不另設 idle timeout，需由 Client 取消連線。領取或分段 ACK 在網路途中遺失時，不可盲目重試；本服務不保證外部推理 exactly-once。已結束工作的正常收件回應不代表重新轉交。

HTTP 連線與記憶體中的等待者不能跨 runtime 重啟還原。重啟時清除無法再交付的前一個 runtime 工作；Client 可能收到連線錯誤或 `503`。此 API 沒有非同步結果查詢或跨連線恢復。

### Client 取消／不再等待

此處 Client 指呼叫 `/v1/chat/completions` 的原始提問者，不是 polling 的 Agent。收到 Cloudflare 的請求取消通知後：

- **Agent 還沒領取**：刪除該筆乞討與 prompt，移除等待者及計時器，後續 `/beg` 不會再拿到它。
- **Agent 已領取**：同樣刪除服務端工作與 prompt，但不嘗試中斷外部 Agent。Agent 後續交付時，在識別碼保留期間內正常回 `200 {"accepted":true}`；內容直接丟棄、不保存、不轉交，也不告訴 Agent Client 已放棄。
- **已開始串流**：停止往 Client 轉送，解除下游背壓，繼續接收並丟棄 Agent 上傳直到 EOF，再正常回覆收件成功；不因 Client 取消而取消 Agent 的上傳。
- **取消比入列更早到**：內部請求 UUID 與 deadline 防止取消後又被延遲的入列請求建立成新工作；這不是公開的取消 API，外部不能指定內部 UUID。

`wrangler.jsonc` 已顯式啟用 `enable_request_signal`。入口收到 abort 後，以 `ctx.waitUntil` 發出不帶已取消 signal 的內部取消通知，確保 Durable Object 執行清理；不只依賴跨服務轉傳 AbortSignal。

**停止等待不一定等於取消 HTTP。** Client 端若只做 `Promise.race`／忽略回應，卻沒有 abort／close 實際連線，伺服器無從知道；網路中斷、代理或 runtime 也可能延遲／沒有傳遞取消通知。尚未開始回覆的請求仍由 `REQUEST_TIMEOUT_MS`（預設 120 秒）清理；已開始的串流則等待 EOF、傳輸失敗或實際取消通知，沒有另一個總時長／idle timeout，不能承諾所有 `Ctrl-C` 都在固定毫秒內被偵測。

本機的 Wrangler／Miniflare 代理不一定轉傳斷線事件。取消回歸測試直接連 workerd socket，以真實 TCP reset 驗證；普通 TCP FIN 是半關閉，可能要等後續寫入才偵測到。正式環境的代理／Client 取消行為仍需在自己的部署驗證。

### OpenAI 相容範圍

支援 `/v1/models` 與 `/v1/chat/completions`，不是 OpenAI 全部 API；未實作 Responses、Embeddings、檔案及模型管理。

- 非串流：轉交完整 completion，保留額外回應欄位。
- `stream: true`：Agent 上傳 raw OpenAI SSE，Cadge 立即逐段 relay，不等完整答案，也不將完整 JSON 轉成模擬串流。
- `stream_options.include_usage: true`：保留在原始工作中；由 Agent 產生 usage SSE，Cadge 原樣轉送，不自行計算或補上 usage。
- 等待過程不發送 SSE heartbeat；Client、代理或其他上游需容許設定的等待時間。

### 長串流與平台限制

[Cloudflare HTTP Workers](https://developers.cloudflare.com/workers/platform/limits/#duration) 與 [Durable Object HTTP](https://developers.cloudflare.com/durable-objects/platform/limits/#wall-time-limits-by-invocation-type) 在呼叫端保持連線時沒有固定 wall-clock 執行時限，不是每 30 秒必須收到一個 chunk。CPU 限制不包含等待網路資料的時間；`waitUntil()` 的 30 秒寬限是回應結束／斷線後的背景工作限制，不是正常串流上限。平台仍有 CPU、記憶體等限制，連線也可能因 runtime 更新或其他網路錯誤中斷。

Cadge 自己的 `REQUEST_TIMEOUT_MS` 從入列起算，預設 120 秒，僅限制排隊與等待首段 SSE 的時間；非串流則等待完整回覆。首個非空 SSE chunk 到達後解除期限，沒有串流總時長或 chunk idle timeout，也不需要 heartbeat 延命。Client／代理仍可能有各自的 timeout。Agent 分段寫入與 `accepted:true` 只代表交付端收件，不能單憑它們定位 Client 緩衝或中途斷線原因。Cadge 不主動插入 heartbeat；上游的 SSE 註解照原樣轉送。


## Agent hooks

repo 內附三個可直接執行的 Node.js scripts，沒有額外 runtime 套件，也不綁特定 Agent 產品。可用 `node scripts/…` 或直接執行 `./scripts/…`；各 script 都支援 `--help`。

先在 Agent 執行環境設定：

```sh
export CADGE_URL='http://localhost:8787'
export CADGE_AGENT_TOKEN='local-agent-token-change-before-deploy'
```

正式環境改用自己的 HTTPS origin 與 `AGENT_TOKEN`；`CADGE_URL` 不可包含 `/v1`。hooks 不會自動讀取 `.dev.vars`，需要獨立設定環境變數。

### 單次乞討 hook

```sh
./scripts/beg.mjs
```

領取成功時 stdout 是一筆完整 job JSON：`{id, request, expires_at}`；沒有工作時 stdout 為空、exit code 為 `0`。這個 hook **只問一次，不自動 sleep**，適合掛在既有 Agent 的 idle／取得下一份工作事件。設定、網路或 HTTP 錯誤寫入 stderr，exit code 非零。

Agent 要保留 `id`，讀取同一筆 `request.messages` 與其他模型參數，再於 `expires_at` 前送出首段 SSE 或完成非串流交付。job JSON 包含使用者 prompt，不要寫入公開 log；若存成檔案，使用私人目錄並在交付後移除。

### 單次交付 hook

把 Agent 真正產生的完整 OpenAI completion JSON 放在 `completion.json`，把領到的 UUID 放在 `CADGE_JOB_ID`：

```sh
./scripts/deliver.mjs "$CADGE_JOB_ID" completion.json
# 也可由 stdin 輸入；省略檔名或使用 - 均可
./scripts/deliver.mjs "$CADGE_JOB_ID" < completion.json
# 處理失敗時，交付錯誤而不是捏造答案
./scripts/deliver.mjs "$CADGE_JOB_ID" --error 'Agent 無法完成此次請求。'
# stream:true 的工作：raw SSE 檔案，或由 stdin 持續輸入 SSE
./scripts/deliver.mjs "$CADGE_JOB_ID" --stream response.sse
./scripts/deliver.mjs "$CADGE_JOB_ID" --stream < response.sse
```

以上為**替代用法，不是依序執行**；每份工作只能開始一次有效交付。成功 stdout 為 `{"accepted":true}`，串流需等到 stdin／檔案 EOF 及伺服器收件成功才輸出；錯誤 exit code 非零。JSON 模式輸入 `chat.completion` 物件本身，不要再包 `{id,response}`；`--stream [sse-file|-]` 則輸入 raw OpenAI SSE，省略來源或 `-` 都讀 stdin。不要輸入純文字或 Agent 工具自己的 JSON envelope；hook 不驗證 SSE 內容。

### 持續 polling 並呼叫自己的 Agent

```sh
./scripts/poll.mjs -- ./my-agent
# 改成空等時每 10 秒 polling
CADGE_POLL_INTERVAL_MS=10000 ./scripts/poll.mjs -- ./my-agent
```

`./my-agent` 是**你現有的 Agent／adapter 執行檔**，不是 repo 內附的模型程式。呼叫方式也可以是 `-- node /absolute/path/to/agent.mjs`，或任何符合下列介面的 executable 與參數：

- stdin：原始 `job.request` JSON 加換行，所有欄位保留；adapter 自行映射真正 provider 的模型，並遵守原始 `stream` 模式。
- stdout：`stream: true` 時為 raw OpenAI SSE，poller 立即以背壓上傳，不收集或重新編碼；其他工作為恰好一個完整 `chat.completion` JSON。log 一律寫到 stderr。
- 環境變數：額外提供 `CADGE_JOB_ID` 與毫秒單位的 `CADGE_EXPIRES_AT`；後者是首段 SSE／非串流完整回覆的初始等待期限，不是已開始串流的結束時間。
- 成功 exit `0`；失敗 exit 非零。adapter 繼承 Agent 環境，請只執行受信任的命令。

poller 預設空等 **5 秒**再領，`CADGE_POLL_INTERVAL_MS=10000` 可改成 10 秒；成功交付後立即領下一筆。它不使用 shell 解譯命令，不會自動重試結果不明的領取／交付。非串流 stdout 格式錯誤或子程式失敗，交付一次 JSON error 後非零退出。串流子程式啟動失敗也走 JSON error；已開始上傳後失敗，則附加 SSE error 事件、完成該次上傳，再非零退出，不另送第二次交付，也不補 `[DONE]`。

`Ctrl-C`／`SIGTERM` 會停止 polling 並終止正在執行的子程式，不會交付假答案。尚未輸出非空 SSE chunk 的串流子程式及尚未完成的非串流程式，在初始 deadline 到達時也會終止；**串流首段輸出後永久解除此 timer，不再因原始 deadline 殺掉子程式**。POSIX 系統會終止子程式群組，必要時在 1 秒後強制結束。尚未開始交付的工作不自動重派，原始 Client 會逾時；因 Agent 自身停止而中斷的串流仍回報錯誤。這不是 Client 取消時通知 Agent。


### Muse

`integrations/muse/` 是實際 Muse hook 的更新來源，保留 Secure Vault／surrogate 認證，不包含 token：

```sh
bash integrations/muse/install.sh
```

安裝後將印出的 hook JSON 匯入／更新至 Muse，再啟用 hook；安裝器不會自行啟用遠端工作。Worker 與 Agent 必須一起更新，不能沿用舊的長連線 `deliver.py`。

串流 worker 使用 `start_stream.sh` 建立每個完整 claim UUID 專用的串流檔，逐段 append SSE，最後呼叫 `finish_stream.sh <dir>`。完成 helper 依明確的 producer EOF 標記排空所有位元組並等待最後 ACK，不再猜測 sleep 兩秒或 kill tail 的時機。首段後沒有 producer idle／總時長期限。詳細介面見 [Muse integration](integrations/muse/README.md)。

不想 clone repo 時，可從 [Muse packet-stream release](https://github.com/darkautism/cadge/releases/tag/muse-packet-stream-20261108) 取得同一套已驗證的更新包。在空目錄執行（需要已登入且有 repo 存取權的 GitHub CLI）：

```sh
gh release download muse-packet-stream-20261108 --repo darkautism/cadge --pattern cadge-muse-packet-stream.tar.gz
tar -xzf cadge-muse-packet-stream.tar.gz
bash muse/install.sh
```

此包對應 `0ef7f820c487a381c91eb9e606b3b5eadc088837`，SHA-256 為 `1592f272179d448ed10242839d27ae42f76a6a2c871c7da2d42e1cc3b760973a`。安裝後仍須由 operator 註冊／確認 Muse credential、匯入 hook JSON 並明確啟用；下載或安裝不等於遠端已啟用。


## 部署到 Cloudflare

### 1. 安裝與登入

先準備 Cloudflare 帳號；此架構可使用 Workers Free 與 SQLite-backed Durable Objects，無須另外租伺服器。在 repo 根目錄執行：

```sh
npm ci
npx wrangler login
```

登入會開啟瀏覽器要求授權。多帳號使用者請確認選中要部署的帳號。

### 2. 確認設定

編輯 `wrangler.jsonc`：`name` 是 Worker 名稱，預設 `cadge`；`vars.MODEL_ID` 是 Client 使用的模型名；`vars.REQUEST_TIMEOUT_MS` 是排隊與等待首段 SSE／非串流完整回覆的初始期限，並非已開始串流的總期限。若帳號已有同名但不相關的 Worker，先改 `name`，避免覆蓋。

保留 `durable_objects` binding 與 `migrations`。它們會在首次部署時建立 SQLite-backed `Broker`，不需要手動建立 Queue、D1 或 KV。不要把 token 放進 `vars`。

`observability` 已啟用 logs 與 traces：兩者 `head_sampling_rate: 1`、`persist: true`，並開啟 `logs.invocation_logs`；這是完整取樣，不只記錄錯誤，會增加相應的可觀測性用量。可在 Cloudflare Dashboard 的 Worker Observability 查看部署後的新紀錄。本設定使用目前 Wrangler 支援的 `issues.enabled: false` 關閉 Issues，不是 `issue_detection` 欄位；不額外輸出 prompt、Bearer token 或答案內容。

### 3. 發布 Worker

```sh
npm run check
npm run deploy
```

Wrangler 會顯示正式的 `https://…workers.dev` 網址；首次使用時依提示設定 workers.dev 子網域。下方的 `CADGE_URL` 請填入這個**完整 origin，不含 `/v1`**。此時 secrets 尚未設定，API 回 `503` 是預期的 fail-closed 行為。

### 4. 設定正式 secrets

在同一個 shell 產生兩組不同 token，再寫入 Cloudflare：

```sh
export CLIENT_TOKEN="$(openssl rand -hex 32)"
export AGENT_TOKEN="$(openssl rand -hex 32)"
printf '%s' "$CLIENT_TOKEN" | npx wrangler secret put CLIENT_TOKEN
printf '%s' "$AGENT_TOKEN" | npx wrangler secret put AGENT_TOKEN
```

請立即將兩組值安全保存在自己的密碼管理工具，分別提供給 Client 與 Agent。以上 shell 變數不會跨終端保留；Cloudflare 不提供 secret 明文讀回。`secret put` 會直接部署新版本，兩組設定完不必再重新 deploy。

本機 `.dev.vars` **不會自動成為正式 secret**；不要用範例中的本機 token 上線，也不要把 token 寫入 repo。正式服務一律使用 HTTPS。

### 5. 驗證部署並接上 Client／Agent

將下方網址換成步驟 3 的實際輸出：

```sh
export CADGE_URL='https://cadge.YOUR-SUBDOMAIN.workers.dev'
export CADGE_AGENT_TOKEN="$AGENT_TOKEN"

curl --fail-with-body "$CADGE_URL/v1/models" \
  -H "Authorization: Bearer $CLIENT_TOKEN"

curl -i -X POST "$CADGE_URL/beg" \
  -H "Authorization: Bearer $CADGE_AGENT_TOKEN"
```

模型清單應包含設定的 `MODEL_ID`；全新、沒有待辦工作的部署，`/beg` 應回 `204` 與 `Retry-After: 5`。`/beg` 是實際領取，不是健康檢查；已有工作時會領走一筆，必須處理交付。

既有 OpenAI Chat provider 使用 `base_url=$CADGE_URL/v1`、API key=`CLIENT_TOKEN`、model=`MODEL_ID`。Agent 使用下方設定表與「Agent hooks」章節的 scripts；只把 `AGENT_TOKEN` 給 Agent，不要混用 Client token。

後續修改程式或 `vars` 後，執行 `npm run deploy` 更新同一個 Worker；既有 secrets 保留。`401` 先檢查 token 角色；`503` 檢查兩個 secrets 與 timeout 設定；Client 的 `504` 表示 deadline 內沒有完成交付。

### 6. 用 WAF 只放行 Agent 與既有 Backend，保護免費額度

**Worker 內回 `401` 仍然會消耗 Worker 請求額度。** 要把未授權流量擋在 Worker 執行前，請使用 Cloudflare 自有 zone 上的 WAF Custom Rule。不能把自己 zone 的 WAF 規則套到 Cloudflare 管理的 `workers.dev` 網域。

#### 綁定自有網域並關閉繞過入口

先準備已加入 Cloudflare 且啟用的網域，例如 `example.com`。WAF 免費層支援 Custom Rules；網域本身需要你持有，網域註冊費不包含在 Workers 免費層內。

把以下欄位**合併**進 `wrangler.jsonc`，保留原本的 main、binding、migration 與 vars，並把 hostname 換成自己的：

```jsonc
{
  "workers_dev": false,
  "preview_urls": false,
  "routes": [
    { "pattern": "cadge.example.com", "custom_domain": true }
  ]
}
```

設定完成後 `npm run deploy`；Wrangler 會建立 Custom Domain 的 DNS 與憑證。將 Client／Agent 的服務網址改為 `https://cadge.example.com`，不要再用舊的 workers.dev 網址。也可從 Worker 的 **Settings → Domains & Routes** 檢查設定。只在 dashboard 關掉 workers.dev 而不更新 repo 設定，下次 deploy 可能重新開啟。

`preview_urls: false` 顯式關閉 version／aliased version URL；目前含 Durable Object 的 Worker 不產生這類 URL，仍建議保留此設定。若另行啟用了 branch Previews、其他 Custom Domain 或其他 routes，也要移除公開入口或套用同等保護，不能留下未受 WAF 保護的替代網址。

#### 建立預設拒絕的 IP 白名單

在 Cloudflare dashboard 選擇該網域，進入 **Security → Security rules → Create rule → Custom rules**（舊介面為 **Security → WAF → Custom rules**），使用 expression editor，動作選 **Block**，不是 Challenge，也不是 Skip。以下範例只占一條 Custom Rule：

```text
(http.host eq "cadge.example.com" and not (
  (
    http.request.uri.path in {"/beg" "/deliver"}
    and http.request.method eq "POST"
    and ip.src in {203.0.113.10 203.0.113.11}
  )
  or
  (
    ip.src in {198.51.100.20}
    and (
      (http.request.uri.path eq "/v1/models" and http.request.method eq "GET")
      or
      (http.request.uri.path eq "/v1/chat/completions" and http.request.method eq "POST")
    )
  )
))
```

**請先替換示範值再 Deploy 規則**：`203.0.113.10`、`203.0.113.11` 是 Agent 的對外出口 IP；`198.51.100.20` 是既有 Backend 的出口 IP。這些都是文件用保留 IP，不是真正可用的來源。需要時加入實際 IPv6 或最小必要 CIDR；NAT／VPN 環境請填 Cloudflare 實際看到的出口 IP，不是內網 IP。

這條規則只讓 Agent IP 存取乞討／交付，只讓 Backend IP 存取 OpenAI 入口；其他來源、路徑與 method 一律在邊緣拒絕。**不能只保護 `/beg` 而放任其他路由公開**，否則攻擊者仍能打其他路徑耗用 Worker。也不能把整個 hostname 設成「只准 Agent」卻忘記 Backend，否則正常提問會一起被擋。

如果 Agent 沒有固定 IP，可以將 Agent 那一段 `ip.src in {...}` 改為：

```text
any(http.request.headers["authorization"][*] eq "Bearer YOUR_ACTUAL_AGENT_TOKEN")
```

Backend 沒有固定 IP 時，其來源條件也可用相同方式比對 **CLIENT_TOKEN**。這是替代來源條件，不要把兩個 token 混在同一角色。實際 token 只填在 Cloudflare 規則設定，**不要把填好 secret 的 expression 提交到 repo**；輪替 token 時須同步修改 WAF 與 Worker secret。Header 比對可擋無 token 的濫打，但 token 洩漏後不能阻止對方耗用額度。不要以容易偽造的 User-Agent 或自訂「我是 Agent」旗標作為白名單。

#### 驗證規則真的先擋流量

1. 從白名單 Agent 主機帶正確 Agent token 呼叫 `/beg`：空佇列應回 `204`；有工作會被實際領取，記得交付。
2. 從白名單 Backend 主機帶 Client token 呼叫 `/v1/models`：應回 `200`。
3. 使用 IP 白名單時，從**不在名單**的另一個網路呼叫相同網址，即使帶正確 token 也應回 Cloudflare WAF 的 `403`，不是 Cadge 的 JSON `401`。Header 模式則測試缺少／錯誤 token。
4. 在 zone 的 **Security Events** 確認被擋請求命中這條規則；配合 Worker 即時 logs／metrics 確認被擋請求未進入 Worker。這部分必須在實際 Cloudflare zone 驗證，本機 Wrangler 無法模擬 WAF。
5. 確認舊的 workers.dev／version／preview／其他替代網址不能再進入服務；檢查沒有較前面的 Skip 規則或 IP Access Allow 例外繞過此規則。若其他 Bot／Challenge 規則擋到合法機器請求，從 Security Events 找到來源再調整，不要讓 Agent 解瀏覽器挑戰。

WAF 保護的是未授權的邊緣流量，不是「保證永不超額」；白名單 Agent 自己 polling、合法 Backend 流量與其他帳號內服務仍計入各自免費配額。Worker 內原有的 Bearer 驗證也必須保留，IP 白名單不取代應用層驗證。

官方參考：[WAF Custom Rules 與免費規則數](https://developers.cloudflare.com/waf/custom-rules/)、[建立規則](https://developers.cloudflare.com/waf/custom-rules/create-dashboard/)、[Custom Domains](https://developers.cloudflare.com/workers/configuration/routing/custom-domains/)、[關閉 workers.dev](https://developers.cloudflare.com/workers/configuration/routing/workers-dev/#disabling-workersdev)、[關閉 Version URLs](https://developers.cloudflare.com/workers/versions-and-deployments/version-urls/#enable-or-disable-version-urls)。

### 設定與信任邊界


| 設定 | 預設值 | 說明 |
|---|---|---|
| `MODEL_ID` | `cadge` | 對 Client 公告並接受的模型識別字 |
| `REQUEST_TIMEOUT_MS` | `120000` | 100–600000 毫秒，含佇列與首段 SSE 等待；首段後解除。非串流等待完整回覆 |
| `CLIENT_TOKEN` | 無 | Client 用 secret |
| `AGENT_TOKEN` | 無 | Agent 用 secret，不能與 Client 相同 |

Agent token 是同一受信任群組的共同憑證，沒有個別 Agent 身分或 per-client 佇列隔離。所有獲授權 Agent 都可能領到使用者完整 prompt；UUID 是群組內的交付能力憑證，不是逐 Agent 的權限綁定。不要將 Agent token 發給不能信任的對象。本服務驗證 JSON 交付結構與 SSE 交付標頭，不驗證答案正確性或 SSE event 內容。

## 免費層可行性與 polling 成本

Cloudflare 官方目前支援在 Workers Free 使用 **SQLite-backed Durable Objects**。此專案沒有付費限定的 KV-backed Durable Objects 依賴。不過免費額度由帳號共享，並非每次空輪詢都免費、不計次。

| 項目 | 免費層額度 |
|---|---:|
| Worker 請求 | 100,000／日 |
| Worker CPU | 10 ms／次 |
| Durable Object 請求 | 100,000／日 |
| Durable Object duration | 13,000 GB-s／日 |
| SQLite 讀取列數 | 5,000,000／日 |
| SQLite 寫入列數 | 100,000／日 |
| SQLite 儲存 | 帳號總共 5 GB |

`204` 只是最省 body 的正確語意，**沒有任何「free-call 狀態碼」**。每次 `/beg` 都算 Worker 請求，並呼叫一次 Durable Object；`200`、`204`、`304` 或 `404` 不會改變這點。

預設每個 Agent 空等時每 **5 秒** polling，可用 hook 的 `CADGE_POLL_INTERVAL_MS=10000` 改成 10 秒，不強制退避。每個 Agent 每日空 polling 約 17,280 次（5 秒）或 8,640 次（10 秒）；每個 SSE 分段與最後 EOF 也各算一次 Worker 與 Durable Object 請求，須另留額度給提問、非串流交付、其他 API 與 alarms。成功交付後會立即領下一筆。

目前是一個共用 Durable Object，等待中的 Client 會讓它保持 active，duration 不是 CPU time。依官方定價範例的 128 MB 換算，單一 Object 連續 active 24 小時約 `86400 × 0.128 = 11059.2 GB-s`；其他 Objects 與用量仍共用配額。空 polling 不重設相同 deadline 的 alarm，避免無意義的 alarm 寫入，但仍會有 SQL 讀取成本。

HTTP-triggered Worker 沒有固定的總等待秒數限制，不代表連線永不中斷：呼叫端／代理 timeout、runtime 更新與重新啟動仍可能中斷。大量常駐 Agent 或更低領取延遲需求，可另評估 long polling 或 hibernation WebSocket；本專案目前沒有提供這兩種 Agent 協定。

免費層適合小量請求與受控 Agent 數量，不能保證任意負載永遠零成本。超出免費配額會使操作失敗；正式部署後需觀察實際 CPU、SQL 和請求用量。

官方依據（查閱日期 2026-11-07）：

- [Workers 定價](https://developers.cloudflare.com/workers/platform/pricing/)
- [Workers 限制與 HTTP duration](https://developers.cloudflare.com/workers/platform/limits/)
- [Durable Objects 定價與免費額度](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [Durable Objects 限制](https://developers.cloudflare.com/durable-objects/platform/limits/)
