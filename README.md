# camel-queue-gateway

`camelStream` 앞에 두는 단일 프로세스 FIFO 프록시예요. 여러 클라이언트 요청의 도착 순서를 고정하고, 네 개의 inference endpoint 전체에서 upstream 동시 실행을 정확히 1개로 제한해요. OpenAI/Anthropic JSON과 SSE는 변환하거나 재직렬화하지 않고 raw byte 단위로 전달·기록해요.

공식 camelStream 문서 기준으로 다음 경로를 허용해요.

| Method | Path | 로컬 FIFO |
|---|---|---:|
| POST | `/v1/chat/completions` | 사용 |
| POST | `/v1/responses` | 사용 |
| POST | `/v1/messages` | 사용 |
| POST | `/v1/messages/count_tokens` | 사용 |
| GET | `/v1/models` | 우회 |

`/v1/keys` 같은 Management API와 미등록 경로는 upstream으로 보내지 않아요.

## 핵심 보장

- 요청 도착 시 FIFO 위치를 먼저 예약한 뒤 본문을 `/var/lib/camel-gateway/`에 스풀해요. 큰 대기 요청 여러 개를 RAM에 보관하지 않아요.
- 가장 앞선 요청의 본문 수신이 끝나지 않았더라도 뒤 요청이 먼저 upstream으로 나가지 않아요.
- 단일 Uvicorn worker, 단일 프로세스 파일 잠금, 단일 전역 FIFO 임대로 inference upstream 동시성을 1로 고정해요.
- FIFO 임대는 upstream 응답이 끝나거나 취소되고 로그·메타데이터 마무리가 끝날 때까지 유지해요.
- 대기 중 연결이 끊긴 요청은 큐에서 제거해 upstream으로 보내지 않아요.
- 스트리밍 중 연결이 끊기면 upstream read task와 연결을 취소하고 슬롯을 해제해요.
- 응답 상태를 알기 전에는 임의의 `200`, 가짜 SSE heartbeat, 임시 응답 헤더를 보내지 않아요.
- 재시작 시 `receiving`, `queued`, `running` 요청은 `interrupted`로 표시하고 자동 재생하지 않아요.
- Gateway key 평문과 실제 Camel key는 DB, request/response body 로그, header 로그에 저장하지 않아요.
- token usage는 upstream이 실제로 보고한 값만 `reported`로 합산해요. 값이 없으면 `unavailable`이며 임의 tokenizer 추정치를 만들지 않아요.

## 파일 구조

```text
camel-queue-gateway/
├── app/
│   ├── __init__.py
│   ├── auth.py             Gateway key 추출·검증
│   ├── cli.py              key/stats/queue/history CLI
│   ├── config.py           환경 설정
│   ├── db.py               SQLite 스키마·집계
│   ├── headers.py          인증 교체·hop-by-hop 제거·redaction
│   ├── main.py             Starlette 앱과 lifespan
│   ├── process_lock.py     데이터 디렉터리 단일 프로세스 잠금
│   ├── proxy.py            spool/FIFO/retry/raw streaming tee
│   ├── queue.py            2단계 strict FIFO 예약·임대
│   ├── timeutil.py         UTC 시간·duration 계산
│   └── usage.py            JSON/SSE usage 정규화
├── scripts/
│   └── acceptance.sh
├── tests/
│   ├── conftest.py
│   ├── test_auth_and_endpoints.py
│   ├── test_fifo_queue.py
│   ├── test_resilience_and_metadata.py
│   ├── test_retry_status_and_keys.py
│   └── test_streaming_and_usage.py
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── pytest.ini
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

## 배포 경로

```text
/opt/camel-gateway/       애플리케이션과 docker-compose.yml
/etc/camel-gateway/       root 전용 gateway.env와 Camel API key
/var/lib/camel-gateway/   SQLite, lock, queued body spool
/var/log/camel-gateway/   날짜별 raw request/response 로그
```

컨테이너 UID/GID는 `10001:10001`이에요. `/var/lib`와 `/var/log`만 쓰기 가능하고, 컨테이너 root filesystem은 read-only예요.

## Debian 13 설치

ZIP을 `/tmp/camel-queue-gateway.zip`에 둔 예시예요.

```bash
sudo install -d -o root -g root -m 0755 /opt/camel-gateway
sudo unzip -q /tmp/camel-queue-gateway.zip -d /tmp/camel-gateway-unpack
sudo cp -a /tmp/camel-gateway-unpack/camel-queue-gateway/. /opt/camel-gateway/
sudo chown -R root:root /opt/camel-gateway

sudo install -d -o root -g root -m 0700 /etc/camel-gateway
sudo install -d -o 10001 -g 10001 -m 0750 /var/lib/camel-gateway
sudo install -d -o 10001 -g 10001 -m 0750 /var/log/camel-gateway
```

### root 전용 secret 생성

```bash
sudo install -o root -g root -m 0600 \
  /opt/camel-gateway/.env.example \
  /etc/camel-gateway/gateway.env

sudoedit /etc/camel-gateway/gateway.env
```

최소한 아래 값을 실제 inference key로 바꿔요.

```dotenv
CAMEL_API_KEY=qaml_live_REPLACE_ME
```

실제 key는 image layer와 `/opt/camel-gateway` 소스 트리에 들어가지 않아요. Docker Compose를 실행할 수 있는 사용자는 사실상 root 권한과 동등하므로 Compose 명령은 root 또는 제한된 운영 계정으로만 실행하는 편이 안전해요.

### 빌드와 실행

```bash
cd /opt/camel-gateway
sudo docker compose config --quiet
sudo docker compose build --pull
sudo docker compose up -d
sudo docker compose ps
```

컨테이너는 반드시 worker 1개로 실행되고 호스트에는 아래 주소로만 publish돼요.

```text
127.0.0.1:8000:8000
```

상태 확인:

```bash
curl --fail --silent http://127.0.0.1:8000/healthz | python3 -m json.tool
```

예상 형식:

```json
{
  "status": "ok",
  "queue_depth": 0,
  "upstream_busy": false
}
```

## Gateway client key 관리

키 생성 시 평문은 한 번만 출력돼요. DB에는 `key_prefix`와 SHA-256 hash만 저장해요.

```bash
cd /opt/camel-gateway
sudo docker compose exec gateway python -m app.cli key create --name hermes
sudo docker compose exec gateway python -m app.cli key create --name laptop
sudo docker compose exec gateway python -m app.cli key create --name codex
sudo docker compose exec gateway python -m app.cli key list
```

비활성화·재활성화·폐기:

```bash
sudo docker compose exec gateway python -m app.cli key disable hermes
sudo docker compose exec gateway python -m app.cli key enable hermes
sudo docker compose exec gateway python -m app.cli key revoke hermes
```

`revoke`는 row를 삭제하지 않으므로 과거 request와 usage 통계가 유지돼요. 폐기한 동일 이름으로 새 키를 만들지는 못해요. 새 이름으로 생성해 명시적으로 구분하세요.

## API 사용 예시

아래의 `GATEWAY_KEY`는 `cgk_...` 형식의 Gateway client key예요. 실제 `qaml_live_...` key가 아니에요.

```bash
export GATEWAY_URL=http://127.0.0.1:8000
export GATEWAY_KEY='cgk_REPLACE_ME'
```

### OpenAI Chat Completions SSE

```bash
curl --no-buffer "$GATEWAY_URL/v1/chat/completions" \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "auto",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": true,
    "stream_options": {"include_usage": true}
  }'
```

### OpenAI Responses SSE

```bash
curl --no-buffer "$GATEWAY_URL/v1/responses" \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "auto",
    "input": "Give me three migration tool names.",
    "stream": true
  }'
```

### Anthropic Messages SSE

```bash
curl --no-buffer "$GATEWAY_URL/v1/messages" \
  -H "x-api-key: $GATEWAY_KEY" \
  -H 'anthropic-version: 2023-06-01' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "auto",
    "max_tokens": 1024,
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": true
  }'
```

### Anthropic count_tokens

```bash
curl "$GATEWAY_URL/v1/messages/count_tokens" \
  -H "x-api-key: $GATEWAY_KEY" \
  -H 'anthropic-version: 2023-06-01' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "auto",
    "messages": [{"role": "user", "content": "Count this"}]
  }'
```

### Models

```bash
curl "$GATEWAY_URL/v1/models" \
  -H "Authorization: Bearer $GATEWAY_KEY"
```

Gateway는 client의 `Authorization`/`x-api-key`를 upstream에 전달하지 않아요. OpenAI 경로는 실제 Camel key를 Bearer로, Anthropic 경로는 `x-api-key`로 새로 구성해요. `anthropic-version`, `anthropic-beta`, content type 등 프로토콜 header는 유지해요. 전송 압축은 `Accept-Encoding: identity`로 요청해 raw 로그와 usage parsing이 재압축 없이 일치하도록 해요.

응답에는 다음 Gateway header가 추가돼요.

```text
x-camel-gateway-request-id
x-camel-gateway-queue-ms
```

upstream의 HTTP status, `Content-Type`, SSE event/data, `Retry-After`, `x-camel-queue-ms`, `x-camel-stream-limit`는 그대로 전달해요.

## FIFO와 disconnect 동작

네 inference endpoint는 하나의 큐를 공유해요.

```text
A running
B queued #1
C queued #2
D queued #3
```

A의 upstream body가 끝나거나 연결이 취소되고 임대가 해제된 뒤에만 B를 보내요. `/v1/models`는 이 큐를 사용하지 않아요.

운영 중 DB 기준 상태 확인:

```bash
sudo docker compose exec gateway python -m app.cli queue
sudo docker compose exec gateway python -m app.cli history --limit 20
```

실제 Camel API를 쓰지 않고 strict FIFO와 upstream max concurrency 1을 검증하는 테스트:

```bash
python3.13 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
pytest -q tests/test_fifo_queue.py
```

## request/response 로그

정상 완료된 요청 예시:

```text
/var/log/camel-gateway/2026-08-29/<request-id>.request
/var/log/camel-gateway/2026-08-29/<request-id>.response
```

대기 중에는 request body가 아래에 있어요.

```text
/var/lib/camel-gateway/spool/<request-id>.body
```

upstream 요청 시작 후 request body 전송이 끝나면 날짜별 `.request` 로그로 이동해요. 파일시스템이 다르면 copy 후 spool을 삭제해요. 이동에 실패하면 DB에는 실제로 남아 있는 spool 경로를 기록해요.

SSE response는 event를 파싱해 재작성하지 않고 upstream chunk를 raw `.response` 파일과 client 양쪽으로 tee해요. header 메타데이터는 SQLite에 JSON으로 저장하되 인증 계열 이름과 이름에 `token`, `secret`, `authorization`, `api-key`, `cookie`, `jwt`, `credential`, `session`, `signature`가 포함된 header 값은 `[REDACTED]`로 바꿔요.

주의: request/response **본문 자체**는 호환성과 감사 목적상 raw로 기록해요. 사용자가 JSON body 안에 secret을 넣으면 그 값은 body 로그에 남을 수 있어요. 로그 디렉터리를 root와 컨테이너 UID 외에는 읽지 못하게 유지하고 별도 보존·삭제 정책을 적용하세요.

## SQLite 메타데이터

`requests`에는 다음 계열을 저장해요.

```text
request_id, gateway_key_id, gateway_key_name
endpoint, method, model, stream, state
received_at, queue_started_at, upstream_started_at, first_byte_at, completed_at
queue_wait_ms, upstream_queue_ms, ttfb_ms, generation_ms, total_duration_ms
http_status, retry_count, retry_allowed, client_disconnected, error
input_tokens, output_tokens, total_tokens, usage_source, usage_details_json
counted_input_tokens, token_count_query
camel_stream_limit
request_path, response_path, request_bytes, response_bytes
redacted request_headers_json, response_headers_json
```

SQLite는 WAL mode를 사용해요. 데이터 디렉터리의 `gateway.lock`에 Linux `flock`을 잡아 동일 DB를 사용하는 두 번째 Gateway 프로세스가 시작되지 않게 해요. worker를 늘리거나 같은 `/var/lib/camel-gateway`를 공유하지 않는 두 컨테이너를 따로 실행하면 전역 concurrency=1 보장이 깨지므로 그렇게 배포하지 마세요.

## token usage 통계

지원 형식:

- Chat Completions: `prompt_tokens`, `completion_tokens`, `total_tokens`
- Responses: `input_tokens`, `output_tokens`, `total_tokens`
- Anthropic Messages: `input_tokens`, `output_tokens`; total이 없으면 보고된 두 값을 합산
- SSE: HTTP chunk가 아니라 빈 줄로 구분된 SSE event를 증분 파싱
- cached/reasoning 등 추가 usage object: `usage_details_json`에 원문 구조 보존

기본 통계:

```bash
sudo docker compose exec gateway python -m app.cli stats
sudo docker compose exec gateway python -m app.cli stats --key hermes
sudo docker compose exec gateway python -m app.cli stats --period today
sudo docker compose exec gateway python -m app.cli stats --period 24h
sudo docker compose exec gateway python -m app.cli stats --period 7d
sudo docker compose exec gateway python -m app.cli stats \
  --from 2026-08-01 --to 2026-08-31
```

날짜만 지정한 `--to 2026-08-31`은 8월 31일 전체를 포함하도록 내부적으로 9월 1일 00:00 UTC 미만으로 처리해요. `today`도 UTC 기준이에요.

출력에는 key별 행과 `TOTAL` 행이 있으며 다음을 포함해요.

```text
request/success/failure
reported_input_tokens
reported_output_tokens
reported_total_tokens
usage_reported_requests
usage_unknown_requests
usage_coverage_percent
average_queue_wait_ms
average_generation_ms
current queue depth/running
```

`/v1/messages/count_tokens`는 generation usage에 더하지 않고 `token_count_queries`, `counted_input_tokens`로 별도 표시해요.

## Retry 정책

기본값:

```dotenv
GATEWAY_MAX_RETRIES=1
GATEWAY_RETRY_POST_GENERATION=false
GATEWAY_RETRY_MAX_DELAY_SECONDS=60
```

`/v1/models`와 `/v1/messages/count_tokens`는 429/502/503 또는 response header 이전 transport error에 대해 최대 1회 재시도해요. `Retry-After`가 있으면 초 또는 HTTP date를 해석해 기다려요. 설정한 최대 delay보다 큰 `Retry-After`는 짧게 잘라 재시도하지 않고 원래 응답을 client에 전달해요.

Chat/Responses/Messages generation POST는 중복 생성 위험 때문에 기본적으로 재시도하지 않아요. 꼭 필요할 때만 root 전용 env에서 아래를 켜세요.

```dotenv
GATEWAY_RETRY_POST_GENERATION=true
```

response body/header가 client로 전달되기 시작한 뒤에는 어떤 endpoint도 재시도하지 않아요.

## Cloudflare Named Tunnel 예시

`cloudflared`가 호스트에서 실행되고 Gateway는 `127.0.0.1:8000`에만 publish된다는 전제예요.

```yaml
# /etc/cloudflared/config.yml
tunnel: <TUNNEL-UUID>
credentials-file: /root/.cloudflared/<TUNNEL-UUID>.json

ingress:
  - hostname: stream-gateway.example.com
    service: http://127.0.0.1:8000
  - service: http_status:404
```

검증과 DNS route:

```bash
sudo cloudflared tunnel ingress validate
cloudflared tunnel route dns <TUNNEL-UUID-or-NAME> stream-gateway.example.com
sudo systemctl restart cloudflared
sudo systemctl status cloudflared --no-pager
```

Cloudflare Tunnel은 origin response의 `Content-Type: text/event-stream`을 보고 스트리밍을 버퍼링하지 않아요. Gateway가 upstream content type을 보존하므로 camelStream SSE에 필요한 별도 변환은 없어요.

### Cloudflare 524 주의

Gateway는 queue 대기 중 가짜 status/heartbeat를 보내지 않아요. 따라서 Cloudflare가 origin에 연결한 뒤 **첫 HTTP response를 받기까지** 로컬 FIFO 대기가 길어지면 Cloudflare의 Proxy Read Timeout 영향을 받아요. Cloudflare 공식 문서의 2026년 7월 기준 기본값은 125초예요. 대기가 이를 넘을 수 있는 환경에서는 다음 중 하나가 필요해요.

- queue가 125초를 넘지 않도록 upstream 처리량과 호출량을 관리
- 장시간 요청은 Cloudflare proxy를 우회한 사설 경로로 호출
- 지원되는 Enterprise timeout 상향 검토

가짜 SSE heartbeat로 우회하면 실제 upstream status 전에 client status가 확정되어 이 프로젝트의 핵심 요구를 깨므로 구현하지 않았어요.

## 로컬 자동 테스트

실제 camelStream key나 token을 사용하지 않고 `httpx.MockTransport` 기반 mock upstream으로 실행해요.

```bash
cd /opt/camel-gateway
python3.13 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
pytest -q
```

한 번에 점검:

```bash
./scripts/acceptance.sh
```

Docker image까지 빌드하려면:

```bash
RUN_DOCKER_BUILD=1 ./scripts/acceptance.sh
```

자동 테스트 범위:

```text
유효/무효/충돌 Gateway key
Authorization Bearer와 x-api-key
upstream 실제 Camel 인증으로 교체
allowlist와 Management API 차단
request/response hop-by-hop header 제거
네 inference endpoint의 공용 FIFO
동시 요청 upstream max concurrency=1
strict arrival FIFO
queued ticket disconnect 제거
request upload task 취소 시 FIFO 예약 회수
streaming disconnect 시 upstream close와 FIFO 임대 해제
finalize 호출 task가 취소돼도 FIFO 임대 해제
대용량 request metadata의 상수 메모리 스캔
감사 DB write 실패 시 FIFO 임대 해제·요청 지속
런타임 response/request log 경로 장애 시 passthrough 지속
/models queue bypass
OpenAI Chat/Responses SSE raw passthrough
Anthropic Messages SSE raw passthrough
SSE event가 HTTP chunk 경계에서 잘린 경우의 usage parsing
count_tokens 별도 통계
HTTP status/Retry-After/Camel header 전달
503 Retry-After 재시도
Generation 429 기본 무재시도
명시적 generation retry
request/response raw 파일 로그
secret header redaction과 평문 key 비저장
key별 usage와 TOTAL
usage 없는 요청의 unavailable 처리
재시작 interrupted 표시
```

## Acceptance checklist

배포 후 아래를 직접 확인해요.

- [ ] `docker compose config --quiet`가 성공해요.
- [ ] `docker compose ps`에서 컨테이너가 `healthy`예요.
- [ ] publish가 `127.0.0.1:8000->8000`뿐이에요.
- [ ] `docker inspect`에서 worker가 1개이고 `read_only`, `cap_drop=ALL`, `no-new-privileges`가 적용돼요.
- [ ] `/etc/camel-gateway/gateway.env`가 `root:root`, mode `0600`이에요.
- [ ] `/var/lib/camel-gateway`와 `/var/log/camel-gateway` 외에는 컨테이너 쓰기가 실패해요.
- [ ] `/healthz`에 secret, key name, client 정보가 없어요.
- [ ] `key create` 평문이 한 번만 출력되고 `key list`에는 prefix만 보여요.
- [ ] 잘못된 Gateway key가 401이고 upstream 호출 수가 증가하지 않아요.
- [ ] `/v1/keys`가 404이며 upstream에 도달하지 않아요.
- [ ] `pytest -q`가 모두 통과해요.
- [ ] FIFO 테스트에서 upstream 최대 동시 실행이 1이에요.
- [ ] mixed endpoint FIFO 테스트에서 네 inference endpoint 순서가 보존돼요.
- [ ] `/v1/models`가 generation 실행 중에도 응답해요.
- [ ] SSE 첫 chunk가 전체 완료 전 client에 도착해요.
- [ ] stream 종료 후 `.response`가 client가 받은 raw SSE와 같아요.
- [ ] streaming client disconnect 시 upstream 연결이 닫히고 다음 FIFO 요청이 실행돼요.
- [ ] 로그 경로에 일시 장애가 나도 upstream 응답 passthrough와 FIFO 임대 해제가 계속돼요.
- [ ] request/response header JSON에 평문 Gateway/Camel key가 없어요.
- [ ] `stats`의 key별 합과 `TOTAL` 합이 테스트 데이터와 같아요.
- [ ] usage 없는 성공 요청 수가 `usage_unknown_requests`에 포함돼요.
- [ ] `count_tokens` 값이 generation token total에 섞이지 않아요.
- [ ] 컨테이너 강제 재시작 후 이전 `queued/running` row가 `interrupted`이고 replay되지 않아요.
- [ ] Cloudflare ingress 마지막 규칙이 catch-all `http_status:404`예요.
- [ ] Cloudflare 경유 SSE가 `Content-Type: text/event-stream`으로 실시간 전달돼요.
- [ ] 예상 최대 queue wait가 Cloudflare 기본 Proxy Read Timeout보다 짧거나 별도 운영 대책이 있어요.

## 공식 참고 문서

- camelStream overview: <https://camelai.com/docs/stream/overview>
- camelStream authentication: <https://camelai.com/docs/stream/authentication>
- camelStream endpoints: <https://camelai.com/docs/stream/endpoints>
- camelStream queues/errors: <https://camelai.com/docs/stream/queues-and-errors>
- Cloudflare locally-managed tunnel config: <https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/local-management/configuration-file/>
- Cloudflare Tunnel DNS route: <https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/routing-to-tunnel/dns/>
- Cloudflare Tunnel streaming troubleshooting: <https://developers.cloudflare.com/cloudflare-one/troubleshooting/tunnel/>
- Cloudflare 524 timeout: <https://developers.cloudflare.com/support/troubleshooting/http-status-codes/cloudflare-5xx-errors/error-524/>
