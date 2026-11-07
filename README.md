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

`npm test` 先打包 Worker，再用 Miniflare/workerd 的 SQLite Durable Object 執行 HTTP 整合測試，包括競爭領取、交付對應、權限、逾時與 SSE。

## API

所有路由均需 `Authorization: Bearer <token>`。Client token 不能存取 Agent 路由，反之亦然。成功及錯誤回應均不快取。

| 路由 | Token | 用途 |
|---|---|---|
| `GET /v1/models` | `CLIENT_TOKEN` | 列出設定的模型識別字 |
| `POST /v1/chat/completions` | `CLIENT_TOKEN` | 入列並等待 Agent 交付 |
| `POST /beg` | `AGENT_TOKEN` | 原子領取最早尚未被領取、未逾時的工作 |
| `POST /deliver` | `AGENT_TOKEN` | 用領取 UUID 交付結果或處理失敗訊息 |

### 1. Client 提問

```sh
curl http://localhost:8787/v1/chat/completions \
  -H 'Authorization: Bearer local-client-token-change-before-deploy' \
  -H 'Content-Type: application/json' \
  -d '{"model":"cadge","messages":[{"role":"user","content":"你好"}]}'
```

這個連線會等待交付，預設最多 120 秒。既有 OpenAI Chat provider 設定 `base_url=https://<worker-host>/v1`、API key 為 `CLIENT_TOKEN`、model 為 `MODEL_ID`；呼叫端的 timeout 需要比 Cadge 等待時間長。

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
- `expires_at` 為 Unix 毫秒；從 Client 入列開始計時，不是從 Agent 領取開始。
- 沒工作回 `204 No Content`，無 body，附 `Retry-After: 5`。Agent 必須自行實作等待；此值只是提示，不是伺服器限流。
- SQL 單一 `UPDATE … RETURNING` 完成選取與領取，同一筆工作不會被兩次成功領取。

Agent 可以自行選擇如何取得答案。如果轉送至真正的 OpenAI provider，應將模型識別字映射成該 provider 的模型，並用 `stream: false`（移除 `stream_options`）取得完整 completion 後交付；不要把上游 SSE 文字當 completion JSON。

### 3. Agent 交付

將上一步取得的 `id` 放入 body：

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

成功回 `200 {"accepted":true}`；原始 Client 收到完整的 `response`，不加 Cadge 外層包裝。`response` 與 `error` 只能提供其中一個。

Agent 處理失敗也必須交付，讓 Client 不必等到逾時：

```json
{
  "id": "727bb93b-9a25-4754-a1ec-12c2a9cc89ad",
  "error": { "message": "Agent 無法完成此次請求。" }
}
```

此時 Agent 收到交付成功，Client 收到 OpenAI 形狀的 `502`、`error.code=agent_error`。

### 錯誤與生命週期

| 情況 | HTTP |
|---|---:|
| Token 缺少、錯誤或角色不符 | 401 |
| JSON 或請求／交付基本格式錯誤 | 400 |
| 未知模型、未知 UUID、已成功交付的 UUID | 404 |
| 已逾時或呼叫者已不再等待的 UUID，保留期間內 | 410 |
| Body 超過 256 KiB | 413 |
| Agent 回報失敗 | Client 502 |
| 缺少／重複 secrets、設定錯誤或協調器中斷 | 503 |
| 等待 Agent 逾時 | Client 504 |

請求 body 上限也適用交付，依實際 UTF-8 位元組數檢查，不依賴 `Content-Length`。大型圖片請使用 URL，而不是大型 base64 body。

待辦請求保存在 SQLite；交付或到期時刪除 prompt，不保存結果。逾時的已領取 UUID 僅保留約 5 分鐘的無內容標記以回 `410`，超過保留期回 `404`，並由 alarm 批次清理。

**不自動重新派工。** Agent 領取後失聯，原始請求在 deadline 時回 `504`。交付只接受一次，但這不等於外部模型推理 exactly-once：若領取回應在網路途中遺失，該工作仍已領取。

HTTP 連線與記憶體中的等待者不能跨 runtime 重啟還原。重啟時清除無法再交付的前一個 runtime 工作；Client 可能收到連線錯誤或 `503`。主動斷線在 runtime 有傳遞 abort 時會提前清理，否則等到 deadline。此 API 沒有非同步結果查詢或跨連線恢復。

### OpenAI 相容範圍

支援 `/v1/models` 與 `/v1/chat/completions`，不是 OpenAI 全部 API；未實作 Responses、Embeddings、檔案及模型管理。

- 非串流：轉交完整 completion，保留額外回應欄位。
- `stream: true`：等待完整交付後，以 `chat.completion.chunk` SSE 回傳內容、tool calls、finish reason 與 `[DONE]`；**不是即時逐 token 生成**。
- `stream_options.include_usage: true`：若 Agent 提供 usage，增加 usage chunk。
- 等待過程不發送 SSE heartbeat；Client、代理或其他上游需容許設定的等待時間。

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

Agent 要保留 `id`，讀取 `request.messages` 與其他模型參數，再於 `expires_at` 前完成工作。job JSON 包含使用者 prompt，不要寫入公開 log；若存成檔案，使用私人目錄並在交付後移除。

### 單次交付 hook

把 Agent 真正產生的完整 OpenAI completion JSON 放在 `completion.json`，把領到的 UUID 放在 `CADGE_JOB_ID`：

```sh
./scripts/deliver.mjs "$CADGE_JOB_ID" completion.json
# 也可由 stdin 輸入；省略檔名或使用 - 均可
./scripts/deliver.mjs "$CADGE_JOB_ID" < completion.json
# 處理失敗時，交付錯誤而不是捏造答案
./scripts/deliver.mjs "$CADGE_JOB_ID" --error 'Agent 無法完成此次請求。'
```

三行是**替代用法，不是依序執行**；每份工作只能交付一次。成功 stdout 為 `{"accepted":true}`，錯誤 exit code 非零。檔案／stdin 內容是 `chat.completion` 物件本身，不要再包 `{id,response}`；hook 會處理外層包裝。不要把 SSE、純文字或 Agent 工具自己的 JSON envelope 直接交付。

### 持續 polling 並呼叫自己的 Agent

```sh
./scripts/poll.mjs -- ./my-agent
# 改成空等時每 10 秒 polling
CADGE_POLL_INTERVAL_MS=10000 ./scripts/poll.mjs -- ./my-agent
```

`./my-agent` 是**你現有的 Agent／adapter 執行檔**，不是 repo 內附的模型程式。呼叫方式也可以是 `-- node /absolute/path/to/agent.mjs`，或任何符合下列介面的 executable 與參數：

- stdin：原始 `job.request` JSON，所有欄位保留。若呼叫真正的 OpenAI provider，adapter 自行映射模型並改用非串流請求取得完整回應。
- stdout：恰好一個完整 `chat.completion` JSON；log 請寫到 stderr。
- 環境變數：額外提供 `CADGE_JOB_ID` 與毫秒單位的 `CADGE_EXPIRES_AT`。
- 成功 exit `0`；失敗 exit 非零。adapter 繼承 Agent 環境，請只執行受信任的命令。

poller 預設空等 **5 秒**再領，`CADGE_POLL_INTERVAL_MS=10000` 可改成 10 秒；成功交付後立即領下一筆。它不使用 shell 解譯命令，不會自動重試結果不明的領取／交付。子程式失敗或 stdout 格式錯誤時，交付一次錯誤後非零退出，方便由既有的程序管理器處理。

`Ctrl-C`／`SIGTERM` 會停止 polling 並終止正在執行的子程式，不會交付假答案；執行超過 job deadline 也會終止。POSIX 系統會終止子程式群組，必要時在 1 秒後強制結束。停止時尚未交付的工作不自動重派，原始 Client 會逾時。


## 部署到 Cloudflare

### 1. 安裝與登入

先準備 Cloudflare 帳號；此架構可使用 Workers Free 與 SQLite-backed Durable Objects，無須另外租伺服器。在 repo 根目錄執行：

```sh
npm ci
npx wrangler login
```

登入會開啟瀏覽器要求授權。多帳號使用者請確認選中要部署的帳號。

### 2. 確認設定

編輯 `wrangler.jsonc`：`name` 是 Worker 名稱，預設 `cadge`；`vars.MODEL_ID` 是 Client 使用的模型名；`vars.REQUEST_TIMEOUT_MS` 是整筆請求的等待期限。若帳號已有同名但不相關的 Worker，先改 `name`，避免覆蓋。

保留 `durable_objects` binding 與 `migrations`。它們會在首次部署時建立 SQLite-backed `Broker`，不需要手動建立 Queue、D1 或 KV。不要把 token 放進 `vars`。

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
| `REQUEST_TIMEOUT_MS` | `120000` | 100–600000 毫秒，含佇列等待時間 |
| `CLIENT_TOKEN` | 無 | Client 用 secret |
| `AGENT_TOKEN` | 無 | Agent 用 secret，不能與 Client 相同 |

Agent token 是同一受信任群組的共同憑證，沒有個別 Agent 身分或 per-client 佇列隔離。所有獲授權 Agent 都可能領到使用者完整 prompt；UUID 是群組內的交付能力憑證，不是逐 Agent 的權限綁定。不要將 Agent token 發給不能信任的對象。本服務驗證交付格式，不驗證答案正確性。

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

預設每個 Agent 空等時每 **5 秒** polling，可用 hook 的 `CADGE_POLL_INTERVAL_MS=10000` 改成 10 秒，不強制退避。每個 Agent 每日空 polling 約 17,280 次（5 秒）或 8,640 次（10 秒）；少量 Agent 可在免費請求額度內運作，另留額度給提問、交付、其他 API 與 alarms。成功交付後會立即領下一筆。

目前是一個共用 Durable Object，等待中的 Client 會讓它保持 active，duration 不是 CPU time。依官方定價範例的 128 MB 換算，單一 Object 連續 active 24 小時約 `86400 × 0.128 = 11059.2 GB-s`；其他 Objects 與用量仍共用配額。空 polling 不重設相同 deadline 的 alarm，避免無意義的 alarm 寫入，但仍會有 SQL 讀取成本。

HTTP-triggered Worker 沒有固定的總等待秒數限制，不代表連線永不中斷：呼叫端／代理 timeout、runtime 更新與重新啟動仍可能中斷。大量常駐 Agent 或更低領取延遲需求，可另評估 long polling 或 hibernation WebSocket；本專案目前沒有提供這兩種 Agent 協定。

免費層適合小量請求與受控 Agent 數量，不能保證任意負載永遠零成本。超出免費配額會使操作失敗；正式部署後需觀察實際 CPU、SQL 和請求用量。

官方依據（查閱日期 2026-11-07）：

- [Workers 定價](https://developers.cloudflare.com/workers/platform/pricing/)
- [Workers 限制與 HTTP duration](https://developers.cloudflare.com/workers/platform/limits/)
- [Durable Objects 定價與免費額度](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [Durable Objects 限制](https://developers.cloudflare.com/durable-objects/platform/limits/)
