# **6주차 과제 해설**

# **Transactional Outbox Pattern과 이벤트 발행 정합성**

---

## **이번 주차의 핵심**

```text
쿠폰 발급 결과와 CouponIssued 발행 의도를
같은 PostgreSQL 트랜잭션에 저장한다.

별도 Publisher가 Outbox를 읽어 Kafka에 발행한다.

Kafka 발행은 중복될 수 있으므로
Consumer는 eventMessageId를 기준으로 멱등하게 처리한다.
```

Outbox가 만드는 보장은 물리적인 Exactly-Once가 아니다.

```text
DB 발급 결과가 Commit되었다면
복구에 필요한 이벤트 발행 정보도 DB에 남아 있다.
```

---

# **과제 1. Dual Write 문제와 Outbox 적용 범위 분석하기**

## **1-1. 사용하는 저장소**

```text
1. PostgreSQL
   - coupon_event
   - coupon_issue
   - coupon_issue_request

2. Kafka Broker
   - coupon.issued Topic
```

## **1-2. `@Transactional`만으로 원자화되지 않는 이유**

Spring의 일반적인 `@Transactional`은 PostgreSQL의 로컬 트랜잭션을 제어한다. Kafka Broker는 별도의 저장소와 트랜잭션 경계를 가지므로 DB Rollback이 이미 저장된 Kafka Record를 지우지 않고, Kafka 실패가 DB Commit을 자동으로 되돌리지도 않는다.

두 저장소를 하나의 메서드에서 호출했다는 사실은 두 저장소의 원자성을 의미하지 않는다.

## **1-3. DB First의 장애 구간**

```text
DB Commit
    ↓
[프로세스 종료 또는 Kafka 발행 실패]
    ↓
Kafka 발행

최종 DB 상태:
쿠폰 발급 결과가 존재한다.

최종 Kafka 상태:
CouponIssued Record가 없을 수 있다.

후속 서비스에 미치는 영향:
알림, 마이페이지와 통계가 발급 사실을 알지 못한다.

복구에 필요한 정보:
eventMessageId, Topic, Partition Key, Payload처럼
무엇을 발행해야 하는지 알려 주는 영속적인 기록이 필요하다.
```

## **1-4. Kafka First의 장애 구간**

```text
Kafka 발행
    ↓
[DB 오류 또는 프로세스 종료]
    ↓
DB Commit

최종 DB 상태:
쿠폰 발급 결과가 없다.

최종 Kafka 상태:
CouponIssued Record가 존재한다.

후속 서비스에 미치는 영향:
실제로 발급되지 않은 쿠폰을 발급 성공으로 처리할 수 있다.
```

저장 순서만 바꿔서는 Dual Write 문제가 해결되지 않는다.

## **1-5. `AFTER_COMMIT`만으로 부족한 이유**

`AFTER_COMMIT`은 DB Commit 전에 이벤트를 발행하는 문제는 피하지만 발행 의도를 영속화하지 않는다.

```text
DB Commit
  ↓
Listener 실행 전 프로세스 종료
  ↓
CouponIssued 유실
```

또한 `KafkaTemplate.send()` 호출은 Broker ACK 완료와 다르다.

```text
send() 호출
  ↓
ACK 확인 전 프로세스 종료
  ↓
Kafka 저장 여부를 확정할 수 없음
```

메모리에만 있던 이벤트는 재시작 후 복구 기준이 될 수 없다.

## **1-6. Outbox의 적용 범위**

| 구간 | 해결되는가? | 이유 |
| --- | --- | --- |
| `coupon_issue` 저장 ↔ `CouponIssued` 발행 의도 저장 | 예 | 같은 PostgreSQL 트랜잭션에 저장한다. |
| Redis `SUCCESS` ↔ 최초 요청 Kafka 발행 | 아니요 | Redis와 Kafka는 이번 Outbox 트랜잭션 밖에 있다. |
| 원본 Kafka 소비 ↔ 발급 DB Commit | 아니요 | Kafka Offset과 DB Commit은 하나의 로컬 트랜잭션이 아니다. DB Commit 후 Offset Commit과 멱등 처리로 대응한다. |
| Consumer DB 저장 ↔ 외부 SMS API 호출 | 아니요 | Consumer DB와 외부 Provider 사이에 새로운 Dual Write가 생긴다. |

## **1-7. 문장 완성**

```text
Transactional Outbox가 직접 보장하는 것:
비즈니스 결과가 DB에 Commit되면 발행할 이벤트 기록도
같은 DB에 함께 남는 로컬 트랜잭션 원자성이다.

Transactional Outbox가 직접 보장하지 않는 것:
Kafka Record의 물리적인 Exactly-Once 생성,
모든 Consumer의 처리 완료,
외부 API의 정확히 한 번 실행이다.
```

---

# **과제 2. CouponIssued 이벤트와 Outbox 테이블 설계하기**

## **2-1. 명령과 이벤트의 차이**

```text
IssueCoupon
→ 쿠폰을 발급하라는 명령이다.
→ 아직 성공이 확정되지 않았다.

CouponIssued
→ DB 트랜잭션으로 쿠폰이 발급되었다는 과거의 사실이다.
→ Consumer가 쿠폰을 다시 발급하면 안 된다.
```

Notification, MyPage와 Analytics는 `CouponIssued`를 이용해 각자의 후속 작업만 수행한다.

## **2-2. CouponIssued JSON**

```json
{
  "eventMessageId": "8a78e94b-5c3d-46a7-a364-538cd1f856c9",
  "eventType": "COUPON_ISSUED",
  "schemaVersion": 1,
  "aggregateVersion": 1,
  "occurredAt": "2026-07-26T03:00:01Z",
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "data": {
    "couponIssueId": 5001,
    "couponEventId": 100,
    "userId": 10,
    "issuedAt": "2026-07-26T03:00:01Z"
  }
}
```

## **2-3. ID 역할 비교**

| 값 | 식별 대상 | 재발행 시 변경되는가? | Consumer 멱등성 Key로 적절한가? |
| --- | --- | --- | --- |
| `couponEventId` | 쿠폰 행사 | 아니요 | 아니요. 한 행사에 여러 발급이 존재한다. |
| `couponIssueId` | 하나의 쿠폰 발급 Aggregate | 아니요 | 보조 제약으로는 유용하지만 여러 이벤트 유형을 구분하지 못한다. |
| `eventMessageId` | 하나의 논리 이벤트 | 아니요 | 예. 같은 이벤트의 재발행에도 유지한다. |
| Kafka Partition + Offset | 특정 Topic Record의 물리적 위치 | 예 | 아니요. 재발행 Record는 다른 Offset을 가진다. |

## **2-4. 발생 시각과 발행 시각**

```text
occurredAt:
쿠폰 발급 DB 트랜잭션에서 비즈니스 사실이 발생한 시각이다.

publishedAt:
Publisher가 Kafka Broker ACK를 확인하고
Outbox 완료 상태를 저장한 시각이다.

Publisher 재시도 시 변경하면 안 되는 값:
eventMessageId, occurredAt, requestId와 저장된 Payload 전체다.
```

## **2-5. Payload를 발행 시점에 다시 만들면 안 되는 이유**

Outbox 생성과 실제 발행 사이에 원본 데이터가 변경될 수 있다. Publisher가 현재 테이블을 다시 조회하면 사건 발생 시점의 사실과 발행 시점의 현재 상태가 섞인다.

따라서 발급 트랜잭션에서 완성된 불변 Snapshot을 `payload`에 저장하고 Publisher는 이를 그대로 전달한다.

## **2-6. Outbox DDL**

```sql
CREATE TABLE outbox_event (
    event_id UUID PRIMARY KEY,
    request_id UUID NOT NULL,
    deduplication_key VARCHAR(200) NOT NULL UNIQUE,

    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    schema_version INT NOT NULL,
    aggregate_version BIGINT NOT NULL,

    topic VARCHAR(200) NOT NULL,
    partition_key VARCHAR(200) NOT NULL,
    payload JSONB NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    claim_token UUID,
    lease_until TIMESTAMPTZ,
    last_error VARCHAR(1000),

    occurred_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_outbox_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED')),
    CONSTRAINT chk_outbox_schema_version
        CHECK (schema_version >= 1),
    CONSTRAINT chk_outbox_aggregate_version
        CHECK (aggregate_version >= 1),
    CONSTRAINT chk_outbox_attempt_count
        CHECK (attempt_count >= 0),
    CONSTRAINT chk_outbox_event_id_matches_payload
        CHECK (
            payload ? 'eventMessageId'
            AND payload ->> 'eventMessageId' = event_id::text
        ),
    CONSTRAINT chk_outbox_state_fields
        CHECK (
            (status = 'PENDING'
             AND claim_token IS NULL
             AND lease_until IS NULL
             AND published_at IS NULL)
            OR
            (status = 'PROCESSING'
             AND claim_token IS NOT NULL
             AND lease_until IS NOT NULL
             AND published_at IS NULL)
            OR
            (status = 'PUBLISHED'
             AND claim_token IS NULL
             AND lease_until IS NULL
             AND published_at IS NOT NULL)
        )
);

CREATE INDEX idx_outbox_pending
    ON outbox_event (next_attempt_at, created_at, event_id)
    WHERE status = 'PENDING';

CREATE INDEX idx_outbox_expired_processing
    ON outbox_event (lease_until, event_id)
    WHERE status = 'PROCESSING';
```

## **2-7. `event_id`와 Payload의 불변조건**

애플리케이션에서 Envelope를 만든 뒤 같은 객체의 `eventMessageId`를 `event_id`와 Payload에 사용한다. 위 DDL처럼 JSON 문자열과 `event_id::text`를 비교하는 `CHECK`를 추가하면 DB에서도 방어할 수 있다.

Payload 구조가 여러 버전을 지원한다면 DB `CHECK`가 배포 호환성을 방해할 수 있으므로, 계약 테스트와 INSERT 전 검증을 기본으로 하고 DB 제약 사용 여부를 결정한다.

## **2-8. `deduplication_key`**

```text
Key 형식:
COUPON_ISSUED:{couponIssueId}

예시:
COUPON_ISSUED:5001
```

같은 Aggregate에서 같은 이벤트 유형이 여러 번 발생할 수 있다면 비즈니스 발생 순번을 추가한다.

```text
COUPON_ISSUED:{couponIssueId}:{aggregateVersion}
```

`schemaVersion`은 Payload 형식의 버전이므로 비즈니스 발생 순번 대신 사용할 수 없다.

## **2-9. UNIQUE 충돌 처리**

PostgreSQL에서 일반 `INSERT`의 UNIQUE 위반이 발생하면 현재 트랜잭션은 실패 상태가 된다. 예외만 잡고 같은 트랜잭션에서 조회할 수 없다.

가능한 방법은 다음과 같다.

| 방법 | 처리 |
| --- | --- |
| 전체 Rollback | Rollback 후 새 트랜잭션에서 기존 행을 조회한다. |
| Savepoint | INSERT 전 Savepoint를 만들고 충돌 시 그 지점까지 Rollback한 뒤 조회한다. |
| `ON CONFLICT DO NOTHING RETURNING` | 권장 방식이다. 반환 행이 없으면 같은 트랜잭션에서 기존 행을 조회하여 event type, aggregate, request와 payload 동일성을 비교한다. |

충돌했다고 무조건 성공 처리하지 않는다. 같은 `deduplication_key`에 다른 Payload가 연결되어 있다면 불변식 위반으로 알람을 발생시킨다.

---

# **과제 3. 발급 결과와 Outbox를 하나의 트랜잭션으로 저장하기**

## **3-1. 전체 SQL 트랜잭션**

```sql
BEGIN;

-- 1. PENDING 요청의 처리 권한 획득
UPDATE coupon_issue_request
SET status = 'PROCESSING',
    processing_started_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PENDING';
-- affected rows = 1이어야 계속 진행한다.

-- 2. 남은 수량이 있을 때만 증가
UPDATE coupon_event
SET issued_count = issued_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :couponEventId
  AND issued_count < total_quantity;
-- affected rows = 0이면 행사 없음 또는 수량 소진이다.

-- 3. 발급 기록 저장
INSERT INTO coupon_issue (
    event_id,
    user_id,
    status,
    issued_at
) VALUES (
    :couponEventId,
    :userId,
    'ISSUED',
    CURRENT_TIMESTAMP
)
RETURNING id, issued_at;

-- 4. 요청 최종 성공 확정
UPDATE coupon_issue_request
SET status = 'ISSUED',
    coupon_issue_id = :couponIssueId,
    completed_at = :issuedAt,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING';
-- affected rows = 1이어야 한다.

-- 5. 완성된 CouponIssued를 같은 트랜잭션에 저장
INSERT INTO outbox_event (
    event_id,
    request_id,
    deduplication_key,
    aggregate_type,
    aggregate_id,
    event_type,
    schema_version,
    aggregate_version,
    topic,
    partition_key,
    payload,
    occurred_at
) VALUES (
    :eventMessageId,
    :requestId,
    'COUPON_ISSUED:' || CAST(:couponIssueId AS VARCHAR),
    'COUPON_ISSUE',
    CAST(:couponIssueId AS VARCHAR),
    'COUPON_ISSUED',
    1,
    1,
    'coupon.issued',
    CAST(:couponIssueId AS VARCHAR),
    CAST(:payload AS JSONB),
    :issuedAt
);

COMMIT;
```

애플리케이션은 각 SQL의 영향받은 행 수가 기대와 다르면 `COMMIT`하지 않고 전체를 Rollback한다.

## **3-2. 영향받은 행 수**

| 작업 | 성공 경로 | 예상과 다를 때 처리 |
| --- | ---: | --- |
| `PENDING → PROCESSING` | 1 | 현재 request 상태를 확인한다. `ISSUED/FAILED` 중복은 정상 종료하고, 불일치는 오류로 분류한다. |
| `issued_count` 조건부 증가 | 1 | 0이면 행사 없음 또는 품절을 구분하고 전체 Rollback한다. |
| `coupon_issue INSERT` | 1 | UNIQUE 위반이나 DB 오류면 전체 Rollback한다. |
| `PROCESSING → ISSUED` | 1 | 0이면 상태 경쟁이므로 전체 Rollback한다. |
| `outbox_event INSERT` | 1 | 0 또는 예외면 동일성 확인 후, 정상 성공 경로가 아니면 전체 Rollback한다. |

품절이나 사용자 중복처럼 최종 비즈니스 실패로 확정할 수 있는 경우에는 성공 트랜잭션을 먼저 Rollback한 뒤, 별도 트랜잭션에서 request를 `FAILED`와 구체적인 실패 사유로 변경한다.

## **3-3. Outbox INSERT 실패 결과**

```text
트랜잭션 결과:
전체 Rollback

coupon_event.issued_count:
증가 전 값으로 복구

coupon_issue:
생성되지 않음

coupon_issue_request:
PENDING 상태 유지

outbox_event:
생성되지 않음
```

## **3-4. 발급 결과만 Commit하면 안 되는 이유**

DB에는 발급 성공이 남지만 후속 서비스가 알아야 할 이벤트 발행 정보가 사라진다. 프로세스 재시작 후 누락된 이벤트의 ID, Topic과 Payload를 알 수 없으므로 Outbox의 복구 가능성 자체가 깨진다.

## **3-5. 이미 `ISSUED`인 원본 메시지 재전달**

```text
issued_count를 다시 증가시키는가?
아니요.

coupon_issue를 다시 생성하는가?
아니요.

Outbox를 다시 INSERT하는가?
아니요. 기존 성공 트랜잭션에서 함께 Commit되었어야 한다.

Consumer 처리는 성공으로 종료할 수 있는가?
예. 기존 결과를 확인하고 정상적인 멱등 처리로 종료한 뒤 Offset을 Commit한다.
```

`ISSUED`인데 대응하는 Outbox가 없다면 새 쿠폰을 발급하지 말고 설계 가정 위반으로 알람을 발생시킨다.

## **3-6. DB Commit 후 원본 Offset Commit 전 종료**

```text
DB 상태:
request ISSUED, coupon_issue와 Outbox PENDING이 존재한다.

Outbox 상태:
PENDING이며 Publisher가 별도로 발행할 수 있다.

Kafka 원본 메시지:
Offset이 Commit되지 않아 재전달될 수 있다.

재시작 후 필요한 멱등성 기준:
requestId와 request 상태를 확인하여 발급과 Outbox 생성을 반복하지 않는다.
```

## **3-7. 원본 Consumer Offset 설정**

```text
enable.auto.commit:
false로 두고 DB 처리 완료 전에 자동 Commit되지 않게 한다.

AckMode:
선택한 모드가 DB Commit 이후에만 Offset을 반영하는지 확인한다.
수동 Ack를 사용한다면 서비스 트랜잭션 성공 뒤 호출한다.

Listener 예외 처리:
일시적 DB 오류를 삼키지 않고 Error Handler로 전달한다.

Error Handler:
예외를 복구 완료로 간주해 Offset을 앞당기는 설정인지 확인한다.
재시도 가능 오류는 원본 Record가 다시 처리되도록 구성한다.
```

DB Commit과 Kafka Offset Commit은 원자적이지 않다. 따라서 안전한 순서는 유실을 피하기 위해 DB를 먼저 Commit하고, 그 사이 장애로 생기는 재전달은 requestId 멱등성으로 처리하는 것이다.

---

# **과제 4. Polling Publisher의 Claim, Lease, Claim Token 설계하기**

## **4-1. 단순 SELECT 후 UPDATE의 Race Condition**

```text
Publisher A                    Publisher B
------------------------------------------------
PENDING evt-001 SELECT
                               PENDING evt-001 SELECT
evt-001 PROCESSING UPDATE
                               evt-001 PROCESSING UPDATE
Kafka 발행
                               같은 evt-001 Kafka 발행
```

조회와 처리 권한 획득이 원자적이지 않아 두 Publisher가 같은 행을 처리한다.

## **4-2. `SKIP LOCKED` Claim SQL**

```sql
BEGIN;

WITH candidates AS (
    SELECT event_id
    FROM outbox_event
    WHERE status = 'PENDING'
      AND next_attempt_at <= CURRENT_TIMESTAMP
    ORDER BY next_attempt_at, created_at, event_id
    LIMIT :batchSize
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_event AS outbox
SET status = 'PROCESSING',
    claim_token = :claimToken,
    lease_until = CURRENT_TIMESTAMP
        + (:leaseSeconds * INTERVAL '1 second'),
    attempt_count = attempt_count + 1,
    updated_at = CURRENT_TIMESTAMP
FROM candidates
WHERE outbox.event_id = candidates.event_id
RETURNING outbox.*;

COMMIT;
```

`SKIP LOCKED`는 다른 Publisher가 잠근 후보를 기다리지 않고 건너뛴다. Claim 경쟁을 줄이는 장치이지 장애 이후의 중복 발행까지 없애는 장치는 아니다.

## **4-3. Claim 트랜잭션을 짧게 끝내는 이유**

DB Row Lock을 잡고 Kafka ACK를 기다리면 다음 문제가 생긴다.

```text
1. DB Connection과 Row Lock을 네트워크 대기 시간 동안 점유한다.
2. 다른 Publisher의 Claim이 지연되어 처리량이 떨어진다.
3. Kafka 지연이 DB Transaction Timeout과 Rollback으로 전파된다.
4. 긴 트랜잭션이 Vacuum, 장애 복구와 운영 부하를 키운다.
```

따라서 `Claim Commit → Kafka 발행 → 완료 상태의 별도 Transaction` 순서를 사용한다.

## **4-4. 단일 Publisher에도 Lease가 필요한 이유**

인스턴스가 하나여도 `PROCESSING` Claim 직후 프로세스가 종료될 수 있다. Lease나 시작 시 stale 복구가 없으면 해당 행은 영원히 `PROCESSING`에 남는다.

Lease는 동시 실행 수가 아니라 **처리 권한의 만료와 회수 기준**을 제공한다.

## **4-5. 만료된 행 복구 SQL**

```sql
UPDATE outbox_event
SET status = 'PENDING',
    claim_token = NULL,
    lease_until = NULL,
    next_attempt_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'PROCESSING'
  AND lease_until <= CURRENT_TIMESTAMP;
```

## **4-6. Claim Token이 없을 때의 문제**

Lease가 만료된 뒤 Publisher B가 행을 회수했는데 Publisher A가 늦게 결과를 기록할 수 있다.

```text
A의 늦은 성공 UPDATE
→ B가 처리 중인 행을 PUBLISHED로 덮어씀

A의 늦은 실패 UPDATE
→ B가 성공한 행을 다시 PENDING으로 되돌림
```

Claim Token은 현재 처리 권한자가 누구인지 구분하여 오래된 작업자의 상태 변경을 막는다.

## **4-7. 성공과 실패 UPDATE의 공통 조건**

```sql
WHERE event_id = :eventId
  AND status = 'PROCESSING'
  AND claim_token = :claimToken
```

영향받은 행이 0이면 권한이 만료되었거나 다른 Publisher가 회수한 것이므로 현재 상태를 덮어쓰지 않는다.

## **4-8. Lease 시간 계산**

동기 순차 발행에서는 다음보다 길어야 한다.

```text
Lease 시간
>
(Batch 크기 × 메시지 한 건의 최악 발행·ACK 대기 시간)
+ 완료 상태 DB 반영 시간
+ Scheduler, GC Pause와 네트워크 지연에 대한 안전 여유
```

Lease가 과도하게 길면 장애 회복이 느려지고, 너무 짧으면 정상 처리 중인 행을 다른 Publisher가 회수한다. 작은 Batch, Lease 연장 또는 제한된 병렬 발행을 함께 고려한다.

---

# **과제 5. Kafka ACK, Timeout, 재시도 상태 전이 설계하기**

## **5-1. 네 상태의 의미**

```text
KafkaTemplate.send() 호출 완료:
Producer에 비동기 전송을 요청했다. Broker 저장 성공은 아니다.

CompletableFuture 정상 완료:
설정한 acks 조건에 따른 전송 완료다.
acks=1/all이면 해당 Broker ACK를 확인한 것이고,
acks=0이면 Broker 저장 확인을 뜻하지 않는다.
이번 설계는 acks=all을 전제로 한다.

outbox = PUBLISHED:
Broker ACK 뒤 완료 상태까지 PostgreSQL에 저장되었다.

모든 후속 Consumer 처리 완료:
각 Consumer Group이 자신의 작업을 끝낸 별도 상태다.
PUBLISHED만으로 알 수 없다.
```

## **5-2. ACK 성공 처리 SQL**

```sql
UPDATE outbox_event
SET status = 'PUBLISHED',
    published_at = CURRENT_TIMESTAMP,
    claim_token = NULL,
    lease_until = NULL,
    last_error = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE event_id = :eventId
  AND status = 'PROCESSING'
  AND claim_token = :claimToken;
```

영향받은 행이 1일 때만 현재 Publisher가 완료 상태를 저장한 것이다.

## **5-3. 실패 후 재시도 SQL**

```sql
UPDATE outbox_event
SET status = 'PENDING',
    next_attempt_at = :nextAttemptAt,
    claim_token = NULL,
    lease_until = NULL,
    last_error = :sanitizedError,
    updated_at = CURRENT_TIMESTAMP
WHERE event_id = :eventId
  AND status = 'PROCESSING'
  AND claim_token = :claimToken;
```

`attempt_count`는 Claim할 때 이미 증가시켰으므로 여기서 다시 증가시키지 않는다.

## **5-4. Timeout의 모호함**

```text
상황 A:
Broker에 저장되지 못한 채 Timeout이 발생했다.

상황 B:
Broker에는 저장되었지만 ACK가 유실되거나 늦게 도착하여
Publisher가 Timeout으로 판단했다.
```

Publisher는 두 상황을 완전히 구분할 수 없다. Timeout을 받았다는 이유로 `PUBLISHED`로 바꾸거나 Record가 없다고 단정할 수 없다.

## **5-5. Timeout 이후 정책**

```text
Outbox 상태:
PUBLISHED로 바꾸지 않고 PENDING 재시도 대상으로 둔다.

재시도 여부:
Exponential Backoff와 Jitter 뒤 재시도한다.

Kafka 중복 가능성:
상황 B였다면 같은 eventMessageId가 다시 저장될 수 있다.

Consumer에게 필요한 방어:
eventMessageId 기반 멱등 처리다.
```

## **5-6. Backoff와 Jitter**

```text
Exponential Backoff:
연속 실패할수록 재시도 간격을 늘려 장애 중 Kafka와 DB에
지속적인 부하를 주지 않는다.

Jitter:
여러 Publisher가 같은 시각에 재시도하는 Thundering Herd를 막기 위해
작은 무작위 지연을 더한다.
```

예시는 `1초 → 2초 → 4초 → 8초 + Jitter`다.

## **5-7. Producer Idempotence의 범위**

| 중복 원인 | 방어 가능한가? | 이유 |
| --- | --- | --- |
| Producer 내부 네트워크 재시도 | 예 | 같은 Producer 세션의 재시도 중복을 Broker가 식별한다. |
| Kafka 성공 후 PUBLISHED 변경 전 종료 | 아니요 | 새 프로세스가 애플리케이션 수준에서 Outbox를 다시 발행한다. |
| 운영자가 같은 Outbox를 다시 PENDING으로 변경 | 아니요 | 별도의 애플리케이션 발행 호출이다. |

따라서 Producer Idempotence를 켜도 Consumer 멱등성은 필요하다.

## **5-8. `whenComplete`에서 DB 작업을 직접 하면 위험한 이유**

콜백은 Kafka Producer I/O 스레드에서 실행될 수 있다. 그 안에서 블로킹 DB 작업을 수행하면 ACK 처리 스레드가 막혀 다른 전송의 지연과 Timeout을 만들 수 있다.

완료 결과는 크기가 제한된 전용 Executor로 넘기고, 성공·실패 상태 UPDATE는 짧은 별도 DB 트랜잭션으로 수행한다. Executor Queue 크기와 거부 정책을 두어 Backpressure도 관리한다.

---

# **과제 6. Outbox 장애 시나리오 분석하기**

## **6-1. 발급 트랜잭션 Commit 전 장애**

| 항목 | 결과 |
| --- | --- |
| `coupon_issue` | Rollback되어 없음 |
| request | `PENDING` 유지 |
| `outbox_event` | 없음 |
| Kafka Record | 없음 |
| 복구 방법 | 원본 `coupon.issue.requested`를 다시 처리한다. |
| 중복 가능성 | 원본은 재전달될 수 있지만 DB Commit 결과가 없으므로 멱등하게 다시 처리한다. |

## **6-2. DB Commit 후 원본 Offset Commit 전 장애**

| 항목 | 결과 |
| --- | --- |
| `coupon_issue` | 존재 |
| request | `ISSUED` |
| `outbox_event` | `PENDING`으로 존재 |
| 원본 Kafka 메시지 | Offset 미반영으로 재전달 가능 |
| 결과 Kafka Record | Publisher 실행 시점에 따라 이미 존재할 수도, 아직 없을 수도 있음 |
| 복구 방법 | requestId 멱등 처리로 재발급 없이 정상 종료하고, Outbox Publisher가 결과 이벤트를 발행한다. |
| 중복 가능성 | 원본 메시지는 재전달되지만 발급·Outbox는 requestId로 중복 생성하지 않는다. |

## **6-3. Claim Commit 후 Kafka 발행 전 종료**

| 항목 | 결과 |
| --- | --- |
| 발급 DB | `ISSUED`로 확정되어 변경 없음 |
| Outbox | `PROCESSING`, Claim Token과 Lease 존재 |
| Kafka Record | 없음 |
| 복구 조건 | `lease_until` 만료 |
| 복구 주체 | 복구 Scheduler 또는 다음 Publisher |
| 중복 가능성 | 이 지점에서 Record는 없지만 실제 발행 시작 여부가 모호한 일반 장애에서는 중복을 허용한다. |

## **6-4. Kafka 성공 후 PUBLISHED 변경 전 종료**

| 항목 | 결과 |
| --- | --- |
| 발급 DB | `ISSUED`로 확정되어 변경 없음 |
| Outbox | `PROCESSING`, 이후 Lease 만료로 `PENDING` 가능 |
| Kafka Record | 존재 |
| 재발행 | 가능 |
| 복구 주체 | Lease 회수 후 다시 Claim한 Publisher |
| 필요한 방어 | 동일한 `eventMessageId`를 Consumer가 멱등 처리한다. |

## **6-5. Lease 만료 후 Publisher B가 회수한 상황**

```text
Publisher A의 늦은 성공 UPDATE:
affected rows = 0

Publisher A의 늦은 실패 UPDATE:
affected rows = 0

0행이어야 하는 조건:
event_id는 같지만 status가 PROCESSING이 아니거나
현재 claim_token이 token-A와 다르다.
```

A는 Kafka ACK를 받았더라도 B의 상태를 덮어쓰지 않는다. 이때 Record 중복 가능성은 Consumer가 처리한다.

DB의 쿠폰 발급 결과는 이미 확정되어 바뀌지 않는다. Outbox는 B의 최신 Claim을 유지하며, A와 B의 발행이 모두 성공하면 같은 `eventMessageId`의 Kafka Record가 중복될 수 있다.

## **6-6. Kafka 장애가 30분 지속되는 상황**

```text
DB 비즈니스 상태:
이미 ISSUED인 쿠폰 발급 결과는 그대로 유지한다.

Outbox 상태:
발행 실패·Timeout 뒤 Backoff가 설정된 PENDING이 반복된다.

Kafka Record:
장애 중에는 새 Record가 없을 수 있고,
모호한 Timeout이 있었다면 일부 Record가 존재할 수도 있다.

PENDING 수:
새 Outbox 생성 속도만큼 계속 증가한다.

가장 오래된 PENDING 대기 시간:
계속 증가하여 30분 이상이 된다.

attempt_count:
Claim과 실패가 반복된 행은 증가한다.

Kafka 복구 후:
오래된 next_attempt_at부터 다시 발행하고 적체를 점진적으로 해소한다.

복구 주체와 중복 가능성:
Outbox Publisher가 복구하며 Timeout 재시도로 중복 발행될 수 있다.

운영 알람:
PENDING 수, oldest age, 실패율, attempt_count,
생성량 대비 발행량과 Kafka 오류에 알람을 건다.
```

즉시 무한 재시도하지 않고 Backoff로 Kafka와 DB를 보호한다.

## **6-7. 두 재시도 대상의 구분**

```text
발급 DB 트랜잭션 실패:
원본 coupon.issue.requested를 재처리한다.

발급 DB Commit 성공 후 CouponIssued 발행 실패:
쿠폰 발급은 그대로 두고 Outbox 이벤트만 재발행한다.
```

두 번째 경우에는 `request = ISSUED`와 `coupon_issue`가 이미 최종 원장에 존재한다. 발급 로직을 다시 실행하면 수량 증가와 중복 발급을 다시 시도하게 되므로 절대 재실행하지 않는다.

---

# **과제 7. Consumer 멱등 처리 설계하기**

## **7-1. Kafka Offset을 멱등성 Key로 사용할 수 없는 이유**

같은 논리 이벤트가 재발행되면 `eventMessageId`는 같지만 Kafka Record의 Partition과 Offset은 달라질 수 있다.

```text
Offset 100 / eventMessageId = evt-001
Offset 101 / eventMessageId = evt-001
```

Offset은 특정 Consumer Group의 물리적 소비 위치이지 비즈니스 이벤트의 ID가 아니다.

## **7-2. `processed_event` DDL**

```sql
CREATE TABLE processed_event (
    subscriber_id VARCHAR(100) NOT NULL,
    event_message_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (subscriber_id, event_message_id)
);
```

## **7-3. 안정적인 `subscriber_id`**

적절한 값은 다음과 같다.

```text
mypage-coupon-projection
```

Pod 이름이나 IP는 재시작·배포 때 바뀐다. 이를 사용하면 새 인스턴스가 같은 이벤트를 처음 보는 것으로 판단한다. `subscriber_id`는 프로세스가 아니라 논리적인 Handler를 식별해야 한다.

## **7-4. `processed_event`와 조회 모델의 원자적 변경**

```sql
BEGIN;

WITH accepted AS (
    INSERT INTO processed_event (
        subscriber_id,
        event_message_id
    ) VALUES (
        'mypage-coupon-projection',
        :eventMessageId
    )
    ON CONFLICT DO NOTHING
    RETURNING event_message_id
)
INSERT INTO user_coupon_view (
    user_id,
    coupon_issue_id,
    coupon_event_id,
    status,
    issued_at
)
SELECT
    :userId,
    :couponIssueId,
    :couponEventId,
    'ISSUED',
    :issuedAt
FROM accepted;

COMMIT;
```

`accepted`가 0행이면 조회 모델 INSERT도 0행이므로 중복 이벤트를 정상 종료한다. 조회 모델 변경이 실패하면 CTE의 `processed_event` INSERT도 함께 Rollback된다.

서로 다른 `eventMessageId`가 같은 `couponIssueId`를 주장하면 조회 모델의 PK/UNIQUE 제약으로 불변식 위반을 드러내고 전체를 Rollback해야 한다.

## **7-5. Consumer DB Commit 후 Offset Commit 전 종료**

```text
Consumer DB 상태:
processed_event와 user_coupon_view가 함께 Commit되었다.

Consumer Group Offset:
아직 반영되지 않았다.

재시작 후 Kafka 전달:
같은 Record가 다시 전달될 수 있다.

재전달된 이벤트 처리:
processed_event PK 충돌을 ON CONFLICT DO NOTHING으로 처리하여
조회 모델을 다시 변경하지 않고 정상 종료한다.
```

## **7-6. 외부 SMS는 별도의 Dual Write다**

`processed_event` 저장과 외부 SMS 전송은 같은 로컬 DB 트랜잭션에 들어가지 않는다.

```text
SMS 전송 성공
  ↓
processed_event Commit 전 장애
  ↓
재전달 후 SMS 중복 전송 가능
```

반대로 `processed_event`를 먼저 Commit하고 SMS 호출 전에 종료되면 SMS가 누락될 수 있다.

## **7-7. Notification Service의 추가 장치**

```text
1. processed_event와 notification_job을 같은 DB 트랜잭션에 저장하고
   별도 Sender가 작업을 전송한다.

2. Notification Service 내부 Outbox를 사용한다.

3. notificationId를 외부 Provider의 Idempotency Key로 전달한다.

4. 발송 결과 조회와 보정 작업을 운영한다.
```

Provider가 멱등 키를 지원하지 않으면 물리적인 중복 전송 가능성을 완전히 제거할 수 없다.

## **7-8. `processed_event` 보관 기간**

보관 기간은 이벤트가 다시 나타날 수 있는 최대 기간보다 길어야 한다.

```text
- Kafka Topic 보관 기간 안의 Record Replay
- Consumer Offset 되감기
- 오래 지연된 Outbox 재발행
- Kafka ACK 후 PUBLISHED 전 장애로 인한 재발행
- 운영자의 수동 재발행
- 백업 복구나 Projection 재구축
```

단순히 “정상 소비 후 7일”처럼 정하지 않고 최대 재발행·Replay 정책과 안전 여유를 합쳐 결정한다. 삭제 후 오래된 이벤트가 다시 오면 비즈니스 중복이 재발할 수 있다.

---

# **과제 8. Partition Key와 이벤트 순서 문제 분석하기**

## **8-1. 같은 Partition Key를 사용하는 이유**

`couponIssueId`를 Key로 사용하면 같은 Topic에서 하나의 쿠폰 발급 건에 대한 `Issued`, `Cancelled` 같은 이벤트가 같은 Partition으로 간다. Kafka는 그 Partition에 **기록된 순서**를 유지한다.

이 보장은 모든 쿠폰의 전역 순서가 아니라 하나의 Aggregate에 필요한 국소 순서다.

## **8-2. 같은 Key만으로 비즈니스 발생 순서가 보장되지 않는 이유**

```text
1. 여러 Publisher가 서로 다른 Outbox 행을 동시에 Claim할 수 있다.
2. SKIP LOCKED는 잠긴 오래된 행을 건너뛰고 뒤의 행을 Claim할 수 있다.
3. version 1 발행 실패와 Backoff 중 version 2가 먼저 성공할 수 있다.
4. Producer의 재시도·동시 전송 설정과 애플리케이션 재발행이
   Broker 도착 순서에 영향을 줄 수 있다.
5. 서로 다른 Topic이면 같은 Key라도 Topic 사이 순서는 보장되지 않는다.
```

Kafka가 보장하는 것은 같은 Topic-Partition에 실제로 Append된 로그 순서이지 DB에서 사건이 발생한 순서가 아니다.

## **8-3. 순서 역전 시나리오**

```text
DB 발생 순서

1. CouponIssued
   aggregateVersion = 1

2. CouponCancelled
   aggregateVersion = 2

Publisher 처리

Publisher A:
version 1을 Claim했지만 Kafka 발행 실패 후 Backoff

Publisher B:
version 2를 Claim하고 먼저 Kafka 발행 성공

Kafka 저장 순서:
CouponCancelled(version=2)
→ CouponIssued(version=1)
```

## **8-4. `occurredAt`만으로 정렬하면 안 되는 이유**

```text
1. 여러 서버의 시계가 완전히 동기화되지 않아 Clock Skew가 생길 수 있다.
2. Timestamp 해상도가 같아 동일 시각 값이 생길 수 있다.
3. 직렬화 오류나 잘못된 시스템 시계로 값이 부정확할 수 있다.
4. 시각은 순서 비교 값일 뿐 누락된 중간 상태를 식별하지 못한다.
```

## **8-5. 최신 상태 Projection의 조건부 UPSERT**

```sql
INSERT INTO user_coupon_view (
    coupon_issue_id,
    status,
    last_aggregate_version,
    last_event_at
) VALUES (
    :couponIssueId,
    :status,
    :aggregateVersion,
    :occurredAt
)
ON CONFLICT (coupon_issue_id)
DO UPDATE
SET status = EXCLUDED.status,
    last_aggregate_version = EXCLUDED.last_aggregate_version,
    last_event_at = EXCLUDED.last_event_at
WHERE EXCLUDED.last_aggregate_version
      > user_coupon_view.last_aggregate_version;
```

이 SQL은 각 이벤트가 해당 Version의 완전한 최신 상태를 담는 Projection에 적합하다.

## **8-6. Version 2가 먼저 온 경우**

```text
먼저 도착:
CouponCancelled(version=2) → CANCELLED 저장

나중 도착:
CouponIssued(version=1)

최종 MyPage 상태:
CANCELLED, last_aggregate_version = 2

version=1 처리 결과:
WHERE 조건이 거짓이므로 affected rows = 0,
최신 상태를 덮어쓰지 않는다.
```

## **8-7. 낮은 Version을 모두 버릴 수 있는가**

| Consumer | 생략 가능 여부 | 이유 또는 추가 설계 |
| --- | --- | --- |
| 최신 상태 MyPage Projection | 조건부 가능 | 이벤트가 완전한 상태를 담고 더 높은 Version만 반영한다면 과거 Version이 최신 상태를 덮을 필요가 없다. |
| 모든 발급·취소를 집계하는 Analytics | 불가능 | 각 사건이 집계에 필요하다. Version Gap을 감지하고 Buffer, Retry 또는 Replay해야 한다. |
| 발급·취소 알림을 각각 보내는 Notification | 불가능 | 각 알림이 업무적으로 필요하다. 순서별 작업 보관과 누락 복구가 필요하다. |

## **8-8. `aggregateVersion`의 정확한 역할**

```text
aggregateVersion은 이벤트 순서 역전을
자동으로 방지하는 값이 아니다.

aggregateVersion은 같은 Aggregate에서 발생한 상태 변경의
단조 증가하는 논리적 순번을 식별하기 위한 값이다.

최신 상태 Projection에서는 낮은 버전이
최신 상태를 덮어쓰는 것을 방지할 수 있다.

모든 상태 전이를 순서대로 처리해야 하는 Consumer는
추가로 Version Gap 감지, Buffer, Retry 또는 Replay가 필요하다.
```

---

# **과제 9. Outbox 운영 지표와 보관 정책 설계하기**

## **9-1. 운영 지표**

장애 기준은 고정 숫자보다 서비스의 발행 지연 SLO와 평상시 처리량을 기준으로 잡는다.

| 지표 | 의미 | 장애 신호 |
| --- | --- | --- |
| PENDING 수 | 발행을 기다리는 이벤트 수 | 지속적으로 증가하거나 기준 Backlog 초과 |
| 가장 오래된 PENDING 대기 시간 | 최악의 발행 지연 | 발행 지연 SLO 초과 |
| PROCESSING 수 | Publisher가 Claim한 이벤트 수 | 처리량 대비 비정상적으로 많고 감소하지 않음 |
| 만료된 PROCESSING 수 | 권한이 만료된 미완료 작업 수 | 0보다 큰 상태가 반복되거나 증가 |
| `attempt_count` | 발행 권한 획득·시도 횟수 | 특정 이벤트에서 계속 증가 |
| Outbox 생성 처리량 | DB에서 발생하는 이벤트 속도 | 발행량보다 장시간 큼 |
| Kafka 발행 처리량 | ACK를 받은 이벤트 속도 | 생성량보다 낮거나 0으로 하락 |
| DB 발생부터 Kafka ACK까지 시간 | End-to-end 발행 지연 | p95/p99가 SLO 초과 |

## **9-2. 초당 2,000건 생성, 1,500건 발행**

```text
순 적체 속도:
초당 500건

1분 후 적체 증가량:
500 × 60 = 30,000건

10분 후 적체 증가량:
500 × 600 = 300,000건

가장 먼저 확인할 지표:
생성·발행 처리량 차이, PENDING 수와 oldest pending age

가능한 대응:
Publisher 처리 병목과 Kafka 지연을 확인하고,
Batch·Connection 사용을 조정하거나 Publisher를 확장한다.
필요하면 입력 Backpressure와 이벤트 생성량 제한도 검토한다.
```

Publisher를 늘리기 전에 Kafka Partition 수, DB Claim 부하와 병목이 실제로 병렬화 가능한지 확인한다.

## **9-3. 즉시 알람이 필요한 설계 가정 위반**

```text
1. request = ISSUED인데 대응하는 CouponIssued Outbox가 없다.
2. status = PROCESSING인데 claim_token 또는 lease_until이 NULL이다.
3. status = PUBLISHED인데 published_at이 NULL이다.
4. status = PENDING인데 claim_token이 남아 있다.
5. event_id와 payload.eventMessageId가 다르다.
6. 같은 deduplication_key에 서로 다른 Payload가 연결된다.
```

## **9-4. PUBLISHED만 정리하는 이유**

```text
PENDING을 삭제하면:
아직 Kafka에 전달되지 않은 이벤트가 유실된다.

PROCESSING을 삭제하면:
발행 중이거나 Lease 복구를 기다리는 이벤트가 유실된다.

PUBLISHED를 보관하는 목적:
발행 감사, 장애 분석, 중복·정합성 확인과 운영 복구 근거다.
```

## **9-5. 대량 삭제의 문제**

한 번에 많은 행을 삭제하면 긴 트랜잭션, DB I/O와 WAL 증가, Row Lock 유지, Replica 지연과 Table Bloat가 발생한다. Vacuum이 정리해야 할 Dead Tuple도 한꺼번에 늘어난다.

## **9-6. 작은 Batch 정리 흐름**

```sql
BEGIN;

WITH targets AS (
    SELECT event_id
    FROM outbox_event
    WHERE status = 'PUBLISHED'
      AND published_at < :retentionCutoff
    ORDER BY published_at, event_id
    LIMIT :batchSize
    FOR UPDATE SKIP LOCKED
)
DELETE FROM outbox_event AS outbox
USING targets
WHERE outbox.event_id = targets.event_id;

COMMIT;
```

작은 Batch를 `삭제 → Commit → 짧은 대기` 순서로 반복하고, 삭제 속도와 DB 부하를 모니터링한다. 감사 보관이 필요하면 삭제 전에 Archive한다.

## **9-7. 세 보관 기간의 목적**

| 보관 대상 | 목적 | 너무 짧을 때 문제 |
| --- | --- | --- |
| Outbox PUBLISHED | 발행 감사, 정합성 조사와 운영 복구 | 누락·중복 원인 조사와 재발행 근거가 사라진다. |
| Kafka Topic Record | Consumer 재처리, 새 Consumer Backfill | Offset Rewind나 Projection Replay가 불가능하다. |
| Consumer `processed_event` | 재전달·재발행 중복 방어 | 오래된 이벤트 재등장 시 비즈니스 결과가 중복 반영된다. |

세 기간은 서로 독립적이지만, `processed_event`는 가능한 최대 재등장 기간을 포함해야 한다.

## **9-8. `last_error` 저장 원칙**

전체 Stack Trace는 크기가 크고 반복 저장 시 테이블과 인덱스를 팽창시킨다. 메시지에는 개인정보, 인증 정보나 내부 주소가 포함될 수도 있다.

DB에는 길이가 제한된 오류 코드와 안전한 요약, Trace ID만 저장하고 상세 Stack Trace는 접근 통제가 있는 Log·Trace 시스템에 저장한다.

---

# **과제 10. 잘못된 Outbox 구현 리뷰하기**

## **10-1. 문제점과 수정 방향**

| 번호 | 문제 위치 | 문제와 장애 | 수정 방향 |
| ---: | --- | --- | --- |
| 1 | `issueCoupon()`의 `kafkaTemplate.send()` | DB와 Kafka 직접 Dual Write로 이벤트 유실·허위 이벤트가 가능하다. | 발급 결과와 Outbox만 같은 DB 트랜잭션에 저장한다. |
| 2 | `issueCoupon()` 전체 | 발급 결과와 Outbox의 원자적 저장이 없다. | `coupon_issue`, request와 Outbox를 함께 Commit한다. |
| 3 | `issueRepository.save()` | 수량 조건, 사용자 UNIQUE와 요청 상태 전이가 보이지 않는다. | 조건부 수량 UPDATE, DB 제약과 request 멱등 처리를 사용한다. |
| 4 | `findTop100ByStatus()` | 여러 Publisher가 같은 PENDING 행을 조회한다. | `FOR UPDATE SKIP LOCKED` Claim을 사용한다. |
| 5 | `publishOutbox()`의 긴 `@Transactional` | Kafka 네트워크 호출 중 DB 트랜잭션과 Lock을 점유한다. | Claim을 짧게 Commit하고 Kafka는 트랜잭션 밖에서 호출한다. |
| 6 | `kafkaTemplate.send()` 결과 무시 | Broker ACK를 확인하지 않는다. | Future 정상 완료 뒤에만 완료 UPDATE한다. |
| 7 | `event.markPublished()` | ACK 전에 PUBLISHED로 바뀌어 실패 Record가 유실된다. | ACK 후 별도 트랜잭션에서 PUBLISHED로 바꾼다. |
| 8 | `createPayloadFromCurrentCouponIssue()` | 발생 시점과 발행 시점 데이터가 섞인다. | Outbox에 저장된 불변 Payload를 그대로 보낸다. |
| 9 | Publisher 상태 모델 | Claim Token과 Lease가 없다. | PROCESSING, token과 lease를 저장한다. |
| 10 | `updateAllProcessingToPending()` | 정상 처리 중인 행까지 탈취한다. | 만료된 `lease_until` 행만 복구한다. |
| 11 | 성공·실패 상태 변경 | 오래된 Publisher의 상태 덮어쓰기를 막지 못한다. | `event_id + PROCESSING + claim_token` 조건을 사용한다. |
| 12 | Publisher 실패 경로 | Backoff, Jitter, `last_error`와 다음 시각이 없다. | PENDING 복구 시 재시도 정보를 저장한다. |
| 13 | `onResult()` | 조회 모델과 processed 기록이 한 트랜잭션이 아니다. | 두 변경을 같은 Consumer DB 트랜잭션에 넣는다. |
| 14 | `onResult()` 저장 순서 | 조회 모델 저장 후 장애가 나면 재전달에서 중복 변경된다. | processed 등록 권한을 원자적으로 얻은 경우에만 비즈니스 변경한다. |
| 15 | Partition + Offset 저장 | 같은 논리 이벤트의 재발행을 구분하지 못한다. | `subscriber_id + eventMessageId`를 PK로 사용한다. |
| 16 | Consumer 중복 등록 | 동시 `SELECT` 후 INSERT 방어가 없다. | `ON CONFLICT DO NOTHING RETURNING`을 사용한다. |

## **10-2. 필수 검토 항목 연결**

| 검토 항목 | 문제 코드 | 수정 방향 |
| --- | --- | --- |
| 발급 트랜잭션에서 Kafka 직접 호출 | `issueCoupon()`의 `kafkaTemplate.send()` | Outbox INSERT로 대체 |
| 발급 결과와 Outbox 원자성 없음 | `issueCoupon()`에 Outbox 저장 없음 | 같은 DB 트랜잭션에 저장 |
| 동시 Claim 방어 없음 | `findTop100ByStatus("PENDING")` | `SKIP LOCKED` 조건부 Claim |
| DB 트랜잭션에서 Kafka 호출 | `publishOutbox()`의 `@Transactional` 루프 | Claim Commit 뒤 트랜잭션 밖 발행 |
| ACK 전에 PUBLISHED | `send()` 직후 `markPublished()` | ACK 확인 후 상태 변경 |
| Claim Token 없음 | Publisher 전체 | Token 조건부 성공·실패 UPDATE |
| 모든 PROCESSING 회수 | `updateAllProcessingToPending()` | 만료 Lease만 회수 |
| 현재 테이블에서 Payload 생성 | `createPayloadFromCurrentCouponIssue()` | 저장된 Payload 사용 |
| Offset을 멱등성 Key로 사용 | `ProcessedEvent(partition, offset)` | `eventMessageId` 사용 |
| Consumer 원자성 부족 | 두 Repository `save()` | 한 트랜잭션에서 processed 등록 후 비즈니스 변경 |

## **10-3. 수정된 전체 구조**

```text
Coupon Issue Consumer
        |
        v
+------------------------------------------+
| 발급 DB Transaction                     |
|------------------------------------------|
| request 처리 권한                       |
| issued_count 조건부 증가                |
| coupon_issue 저장                       |
| request ISSUED                          |
| outbox_event PENDING 저장               |
+------------------------------------------+
        |
        | Commit 후 원본 Offset Commit
        v
Outbox Poller
        |
        v
+------------------------------------------+
| Claim Transaction                       |
| SKIP LOCKED + PROCESSING                 |
| claim_token + lease_until                |
+------------------------------------------+
        |
        | Commit
        v
Kafka 발행 → Broker ACK
        |
        +-- 성공 → token 조건부 PUBLISHED Transaction
        |
        +-- 실패 → token 조건부 PENDING Transaction
        |
        v
Kafka Result Consumer
        |
        v
+------------------------------------------+
| Consumer DB Transaction                 |
| processed_event 원자적 등록             |
| Consumer Business Table 변경            |
+------------------------------------------+
        |
        | Commit
        v
Consumer Group Offset Commit
```

## **10-4. 발행 보장 수준**

| 항목 | 수준 | 이유 |
| --- | --- | --- |
| DB 발급 결과와 Outbox 기록 원자성 | 보장 | 같은 PostgreSQL 트랜잭션이다. |
| Kafka Record의 물리적 Exactly-Once | 보장하지 않음 | ACK 후 완료 표시 전 장애에서 재발행된다. |
| Publisher 복구 후 재발행 | 조건부 보장 | Publisher 실행, Lease 회수, 재시도와 Kafka 회복이 필요하다. |
| 비즈니스 발생 순서대로 전달 | 보장하지 않음 | 다중 Claim, Backoff와 재발행으로 역전될 수 있다. |
| Consumer 중복 비즈니스 반영 방지 | 조건부 보장 | `processed_event`와 비즈니스 변경의 원자성 및 충분한 보관 기간이 필요하다. |
| 모든 Consumer Group 처리 완료 | 보장하지 않음 | 각 Group은 독립적으로 실패하고 지연된다. |
| 외부 SMS Exactly-Once | 보장하지 않음 | 외부 Provider는 Consumer DB 트랜잭션 밖에 있다. |

---

# **보너스 과제 1. Polling Publisher와 CDC 비교하기**

| 항목 | Polling Publisher | CDC |
| --- | --- | --- |
| 이벤트 조회 기준 | Outbox 상태와 `next_attempt_at`을 주기적으로 조회 | PostgreSQL WAL의 Commit Log를 읽음 |
| 추가 인프라 | 애플리케이션 Scheduler 중심 | Connector, Replication Slot과 CDC 운영 환경 필요 |
| DB 조회 부하 | 주기적인 SELECT·UPDATE 부하 존재 | 반복 Polling은 줄지만 WAL 보존·복제 부하 존재 |
| 발행 지연 | Polling 주기만큼 지연 가능 | 보통 Commit Log를 빠르게 전달 |
| Claim·Lease | 애플리케이션이 직접 구현 | 일반적으로 Polling용 Claim·Lease는 불필요 |
| 재시도 운영 | 상태, Backoff와 Lease를 직접 관리 | Connector Offset, 오류 정책과 재시작을 관리 |
| 순서 처리 | 다중 Claim과 Backoff로 역전 가능 | DB Commit 순서를 얻기 유리하지만 Kafka·Consumer 순서까지 보장하지 않음 |
| Outbox 정리 | PUBLISHED 또는 별도 발행 완료 기준으로 정리 | Connector가 안전하게 읽은 위치와 보관 정책을 고려해 정리 |

CDC가 WAL Commit 순서를 읽더라도 모든 Consumer의 비즈니스 처리 순서가 자동으로 보장되지는 않는다.

```text
- 서로 다른 Aggregate와 Topic·Partition의 전역 순서는 없다.
- Connector 또는 Producer 재시도 정책을 고려해야 한다.
- Consumer 병렬 처리와 외부 API 완료 순서는 Kafka 순서와 다를 수 있다.
- Consumer는 aggregateVersion과 멱등 처리를 여전히 사용해야 한다.
```

---

# **보너스 과제 2. 장애 주입 테스트 설계하기**

| 장애 지점 | 테스트 방법 | 기대 DB 상태 | 기대 Kafka 상태 | 검증할 불변식 |
| --- | --- | --- | --- | --- |
| Outbox INSERT 직전 | INSERT 직전에 예외를 던짐 | 발급 전체 Rollback, request PENDING | 결과 Record 없음 | 발급 결과와 Outbox가 따로 Commit되지 않음 |
| 발급 DB Commit 직후 | Commit 직후 프로세스 강제 종료 | request ISSUED, issue와 PENDING Outbox 존재 | 아직 없을 수 있음 | 원본 재전달에도 발급·Outbox가 중복되지 않음 |
| Claim Commit 직후 | PROCESSING Commit 뒤 Publisher 종료 | token·lease가 있는 PROCESSING | 없음 | Lease 만료 후 회수하여 발행 가능 |
| Kafka ACK 직후 | ACK 수신 직후 프로세스 종료 | PROCESSING 또는 이후 PENDING | Record 존재 | 재발행돼도 같은 eventMessageId 사용 |
| PUBLISHED UPDATE 직전 | 상태 UPDATE 직전에 종료 | PROCESSING | Record 존재 | Lease 복구 후 중복 발행을 Consumer가 방어 |
| Consumer DB Commit 직후 | DB Commit 뒤 Offset 전 종료 | processed_event와 비즈니스 결과 존재 | 같은 Record 재전달 가능 | 재전달에도 비즈니스 결과 한 번만 반영 |

장애 지점은 매우 짧은 Race Window이므로 한 번의 성공만으로 안전성을 증명할 수 없다. 반복 실행, 여러 Publisher·Consumer 동시 실행과 무작위 종료를 사용하고 다음 불변식을 매번 검사한다.

```text
- request ISSUED이면 coupon_issue와 Outbox가 함께 존재한다.
- 쿠폰 수량과 사용자 UNIQUE가 깨지지 않는다.
- 오래된 Claim Token이 최신 상태를 덮어쓰지 않는다.
- 같은 eventMessageId가 여러 번 전달돼도 Consumer 결과는 한 번만 반영된다.
```

---

# **전체 흐름 최종 정리**

```text
coupon.issue.requested
        |
        v
Coupon Issue Consumer
        |
        v
+------------------------------------------+
| PostgreSQL Transaction                   |
|------------------------------------------|
| request PENDING -> PROCESSING            |
| issued_count 조건부 증가                |
| coupon_issue INSERT                      |
| request -> ISSUED                        |
| CouponIssued Outbox PENDING INSERT       |
+------------------------------------------+
        |
        | Commit
        v
원본 Kafka Offset Commit

Outbox Publisher
        |
        | SKIP LOCKED Claim + Token + Lease
        v
Kafka coupon.issued
        |
        +-- ACK 성공 -> PUBLISHED
        |
        +-- 실패/Timeout -> Backoff 후 재시도
        |
        v
Result Consumer
        |
        | processed_event + 비즈니스 변경
        v
Consumer DB Commit
        |
        v
Consumer Offset Commit
```

---

# **핵심 질문 답변**

## **1. Transactional Outbox가 필요한 가장 중요한 이유**

DB 비즈니스 결과가 Commit될 때 발행할 이벤트 의도도 같은 트랜잭션에 남겨, 프로세스 장애 후에도 누락된 발행을 복구하기 위해서다.

## **2. DB 발급 성공과 Kafka 이벤트 발행 성공이 다른 이유**

PostgreSQL Commit과 Kafka Broker 저장은 서로 다른 저장소에서 독립적으로 완료되기 때문이다.

## **3. Outbox 행이 존재한다는 의미**

DB에 확정된 비즈니스 사실과 연결된 이벤트 발행 정보가 영속적으로 기록되었다는 뜻이지, Kafka 발행 완료를 뜻하지는 않는다.

## **4. Outbox PUBLISHED의 의미**

Publisher가 Kafka ACK를 확인하고 완료 상태를 DB에 저장했다는 뜻이며, 모든 Consumer 처리나 외부 알림 완료를 뜻하지 않는다.

## **5. Kafka Timeout 이후 재시도해야 하는 이유**

Record가 저장되지 않았을 가능성과 저장됐지만 ACK만 확인하지 못했을 가능성을 구분할 수 없으므로 유실을 피하기 위해 재시도해야 한다.

## **6. Outbox를 사용해도 Consumer 멱등성이 필요한 이유**

Kafka ACK 후 Outbox 완료 표시 전 장애에서는 같은 `eventMessageId`가 다시 발행될 수 있기 때문이다.

## **7. `eventMessageId`와 Kafka Offset의 차이**

`eventMessageId`는 재발행에도 유지되는 논리 이벤트 ID이고, Offset은 특정 Kafka Record의 물리적 위치다.

## **8. Claim Token이 없는 Lease가 안전하지 않은 이유**

Lease가 만료된 오래된 Publisher가 새 처리 권한자의 성공·실패 상태를 뒤늦게 덮어쓸 수 있기 때문이다.

## **9. 같은 Partition Key가 비즈니스 순서를 보장하지 않는 이유**

여러 Publisher의 Claim, `SKIP LOCKED`, 실패 Backoff와 재발행으로 DB 발생 순서와 Kafka Append 순서가 달라질 수 있기 때문이다.

## **10. 결과 이벤트 발행 실패 시 발급 로직을 다시 실행하면 안 되는 이유**

쿠폰 발급은 이미 DB에서 확정되었으므로 발급을 다시 실행하면 수량과 중복 불변식을 다시 변경하려 하며, 복구 대상은 발급이 아니라 Outbox 이벤트이기 때문이다.

---

# **핵심 요약**

```text
1. 발급 결과와 Outbox는 같은 PostgreSQL 트랜잭션으로 저장한다.

2. Claim은 SKIP LOCKED로 짧게 수행하고 Kafka 호출은 트랜잭션 밖에서 한다.

3. Lease는 중단된 작업을 회수하고 Claim Token은 오래된 작업자의 덮어쓰기를 막는다.

4. Broker ACK 뒤에만 PUBLISHED로 변경한다.

5. Timeout과 ACK 이후 장애 때문에 Kafka 중복 발행은 남는다.

6. Consumer는 eventMessageId와 로컬 DB 트랜잭션으로 중복 반영을 막는다.

7. 같은 Partition Key는 Kafka에 기록된 순서를 유지할 뿐 비즈니스 발생 순서를 만들지 않는다.

8. 결과 이벤트 발행 실패에서는 쿠폰을 다시 발급하지 않고 Outbox만 재시도한다.
```
