# 6주차 개념 설명: Transactional Outbox Pattern과 이벤트 발행 정합성

## 주제

쿠폰 발급 결과를 PostgreSQL에 저장하는 작업과 `CouponIssued` 이벤트를 Kafka에 발행하는 작업 사이의 Dual Write 문제를 이해하고, Transactional Outbox Pattern으로 발행 의도를 안전하게 남긴다.

이번 주차의 핵심은 다음 한 문장으로 요약할 수 있다.

```
쿠폰 발급 결과와 Kafka 이벤트를 직접 함께 저장하지 않는다.

쿠폰 발급 결과와 이벤트 발행 의도를
같은 DB 트랜잭션에 저장한 뒤,

별도의 Publisher가 Outbox 이벤트를 Kafka에 전달한다.
```

---

## 1. 5주차까지 만든 구조와 남은 문제

5주차까지 쿠폰 발급 요청은 다음 흐름으로 처리했다.

!image.png

이 DB 트랜잭션이 Commit된 시점을 최종 쿠폰 발급 성공으로 본다.

다음 세 작업은 함께 성공하거나 함께 실패한다.

```
coupon_event.issued_count 증가
coupon_issue 발급 기록 저장
coupon_issue_request ISSUED 변경
```

하지만 쿠폰 발급이 성공한 뒤에는 다른 서비스에도 결과를 알려야 한다.

```
Coupon Service
      |
      | CouponIssued
      v
Kafka
      |
      +-- Notification Consumer
      +-- MyPage Consumer
      +-- Analytics Consumer
```

여기서 새로운 문제가 생긴다.

```
PostgreSQL에 쿠폰 발급 결과 저장

+

Kafka에 CouponIssued 이벤트 발행
```

PostgreSQL과 Kafka는 서로 다른 시스템이다. 하나의 메서드 안에서 호출하거나 `@Transactional`을 붙였다고 해서 두 시스템이 하나의 원자적 트랜잭션으로 묶이지 않는다.

이번 주차에서는 **DB에 확정된 쿠폰 발급 결과와 후속 결과 이벤트 발행 사이의 정합성**을 다룬다.

---

## 2. Dual Write 문제

하나의 비즈니스 작업에서 서로 다른 저장소에 각각 쓰는 문제를 Dual Write 문제라고 한다.

가장 단순한 구현은 다음과 같다.

```java
@Transactional
public void issueCoupon(IssueMessage message) {
    increaseIssuedCount(message.eventId());

    CouponIssue issue = saveCouponIssue(message);

    markRequestIssued(message.requestId(), issue.getId());

    kafkaTemplate.send(
        "coupon.issued",
        issue.getId().toString(),
        CouponIssuedEvent.from(message, issue)
    );
}
```

코드상으로는 하나의 메서드 안에 있지만 실제로는 다음 두 저장소를 사용한다.

```
PostgreSQL Transaction

Kafka Broker
```

Spring의 DB 트랜잭션이 Kafka Broker의 저장까지 자동으로 Rollback해 주지는 않는다.

따라서 DB와 Kafka 사이에는 한쪽만 성공할 수 있는 장애 구간이 생긴다.

---

## 3. 순서를 바꾸어도 해결되지 않는다

!image.png

### 3-1. DB를 먼저 Commit하는 경우

```
DB 발급 결과 Commit
        |
        v
Kafka 결과 이벤트 발행
```

DB Commit 후 Kafka 발행 전에 서버가 종료되면 다음 상태가 된다.

```
PostgreSQL
→ 쿠폰 발급 완료

Kafka
→ CouponIssued 이벤트 없음

후속 서비스
→ 발급 사실을 알 수 없음
```

쿠폰은 이미 발급되었으므로 발급 로직을 다시 실행하면 안 된다.

필요한 작업은 다음과 같다.

```
쿠폰 재발급
→ 하면 안 됨

누락된 CouponIssued 이벤트 재발행
→ 필요
```

하지만 이벤트 발행 정보가 메모리에만 있었다면 서버가 다시 시작된 뒤 무엇을 발행해야 하는지 알기 어렵다.

### 3-2. Kafka를 먼저 발행하는 경우

```
Kafka 결과 이벤트 발행
        |
        v
DB 발급 결과 저장
```

Kafka 발행 후 DB 트랜잭션이 실패하면 다음 상태가 된다.

```
Kafka
→ CouponIssued 이벤트 존재

PostgreSQL
→ 실제 쿠폰 발급 기록 없음
```

후속 서비스는 존재하지 않는 발급 결과를 처리할 수 있다.

따라서 순서를 바꾸는 것만으로는 Dual Write 문제가 해결되지 않는다.

```
DB Commit과 Kafka 저장 사이에는
항상 장애 구간이 존재한다.
```

---

## 4. `AFTER_COMMIT`만으로 부족한 이유

!image.png

DB Commit이 끝난 뒤 Kafka를 호출하도록 만들 수 있다.

```java
@TransactionalEventListener(
    phase = TransactionPhase.AFTER_COMMIT
)
public void publish(CouponIssuedEvent event) {
    kafkaTemplate.send(
        "coupon.issued",
        event.couponIssueId().toString(),
        event
    );
}
```

이 방식은 DB Commit 전에 `CouponIssued`가 발행되는 문제를 피한다.

하지만 다음 장애는 남는다.

```
DB Commit 성공
        |
        X Listener 실행 전 서버 종료
        |
        v
Kafka 이벤트 누락
```

Listener가 실행되더라도 `KafkaTemplate.send()`는 비동기다.

```
kafkaTemplate.send() 호출
≠
Kafka Broker ACK 확인
```

비동기 발행이 끝나기 전에 프로세스가 종료되거나 발행이 실패하면 DB 결과는 이미 Commit되었으므로 되돌릴 수 없다.

핵심 문제는 다음과 같다.

```
발행할 이벤트가 메모리에만 존재한다.

→ 프로세스가 종료되면 복구 기준이 사라질 수 있다.
```

Outbox Pattern은 발행할 이벤트를 메모리가 아니라 DB에 저장한다.

---

## 5. Transactional Outbox Pattern

!image.png

Transactional Outbox Pattern은 비즈니스 데이터와 발행할 이벤트를 **같은 DB의 같은 트랜잭션**에 저장한다.

```
DB Transaction 시작
        |
        v
request PENDING → PROCESSING
        |
        v
coupon_event.issued_count 증가
        |
        v
coupon_issue 저장
        |
        v
request → ISSUED
        |
        v
outbox_event 저장
        |
        v
Commit
```

이 구조에서는 다음 두 결과만 가능하다.

```
성공

coupon_issue 저장
request ISSUED
outbox_event 저장

→ 모두 Commit
```

```
실패

coupon_issue 저장 안 됨
request ISSUED 변경 안 됨
outbox_event 저장 안 됨

→ 모두 Rollback
```

따라서 다음 상태가 발생하지 않는다.

```
쿠폰 발급 결과는 Commit되었는데
발행할 이벤트 기록이 없는 상태
```

DB Commit 이후에는 별도의 Outbox Publisher가 저장된 행을 읽어 Kafka에 발행한다.

```
PostgreSQL

coupon_issue
outbox_event PENDING
      |
      v
Outbox Publisher
      |
      v
Kafka coupon.issued
```

여기서 Outbox 행은 이벤트가 이미 Kafka에 발행되었다는 뜻이 아니다.

```
Outbox 행 존재

→ 발행해야 할 이벤트가 DB에 안전하게 기록되었다.
```

---

## 6. 이번 주차에서 Outbox가 해결하는 범위

이번 주차의 Outbox 적용 범위는 다음이다.

```
DB에 확정되는 쿠폰 발급 성공

+

후속 서비스에 전달할 CouponIssued 이벤트
```

구체적으로 다음 두 작업을 하나의 DB 트랜잭션으로 연결한다.

```
coupon_issue 저장과 request ISSUED 확정

+

CouponIssued 발행 의도 저장
```

### 6-1. 원본 Kafka 메시지 소비와 DB Commit

!image.png

Outbox는 다음 구간을 원자화하지 않는다.

```
Kafka coupon.issue.requested 소비

+

PostgreSQL 발급 트랜잭션
```

다음 장애 구간은 그대로 존재한다.

```
발급 DB Transaction Commit
        |
        X 원본 Kafka Offset Commit 전 종료
```

이 경우 원본 메시지가 다시 전달될 수 있다.

5주차에서 만든 `requestId`와 요청 최종 상태가 이 중복을 막는다.

```
재전달된 requestId 확인
→ request = ISSUED
→ 발급 로직과 Outbox INSERT를 다시 실행하지 않음
→ 정상 처리로 종료
```

따라서 두 패턴의 역할은 다르다.

```
requestId 기반 멱등성
→ 원본 요청의 중복 처리 방어

Transactional Outbox
→ DB 결과 이벤트의 발행 의도 유실 방어
```

### 6-2. Redis와 최초 Kafka 요청 발행

!image.png

이번 주차의 결과 Outbox가 다음 구간까지 자동으로 해결하는 것은 아니다.

```
Redis Lua Script SUCCESS

+

최초 coupon.issue.requested Kafka 발행
```

Redis와 PostgreSQL은 서로 다른 저장소이므로 하나의 일반적인 PostgreSQL 트랜잭션에 함께 포함되지 않는다.

다만 API Server가 다음 구조를 선택한다면 일부 구간에는 Outbox를 적용할 수 있다.

```
Redis SUCCESS
        |
        v
PostgreSQL Transaction

coupon_issue_request PENDING INSERT
+
CouponIssueRequested Outbox INSERT
```

이 경우에도 다음 구간은 여전히 원자적이지 않다.

```
Redis SUCCESS

↔

PostgreSQL 요청·Outbox 저장
```

이번 주차에서는 범위를 넓히지 않고 **발급 성공 결과와 `CouponIssued` 발행 사이**만 다룬다.

Redis와 PostgreSQL의 상태가 어긋나는 구체적인 경로와 그 영향은 11-2, 11-3에서 다시 다룬다.

### 6-3. 실패 이벤트의 범위

!image.png

이번 주차의 기본 이벤트는 발급 성공 사실을 나타내는 `CouponIssued`다.

```
request = ISSUED
→ CouponIssued Outbox 저장
```

다음 최종 비즈니스 실패에는 성공 이벤트를 저장하지 않는다.

```
SOLD_OUT
DUPLICATE_USER
INVALID_EVENT
```

향후 실패 결과도 다른 서비스에 전달해야 한다면 다음 이벤트를 별도로 설계할 수 있다.

```
CouponIssueFailed
```

이 경우에도 `request FAILED` 변경과 실패 Outbox INSERT를 같은 DB 트랜잭션으로 묶어야 한다.

DB Timeout이나 Deadlock 같은 일시적 기술 실패는 최종 실패 이벤트로 확정하지 않는다. 원본 메시지 재처리 대상으로 남긴다.

---

## 7. 전체 처리 구조

!image.png

Coupon Issue Consumer와 Outbox Publisher의 역할은 다르다.

```
Coupon Issue Consumer

→ 쿠폰 발급 비즈니스 로직 수행
→ 수량과 발급 기록의 정합성 보장
→ 요청 최종 상태 확정
→ 결과 이벤트 발행 의도 저장
```

```
Outbox Publisher

→ 이미 확정된 이벤트를 Kafka에 전달
→ 쿠폰 수량을 증가시키지 않음
→ coupon_issue를 다시 생성하지 않음
```

---

## 8. 이벤트는 이미 발생한 사실을 표현한다

!image.png

결과 이벤트의 이름은 명령보다 이미 발생한 사실을 표현해야 한다.

```
IssueCoupon
→ 쿠폰을 발급하라는 명령

CouponIssued
→ 쿠폰이 발급되었다는 사실
```

`CouponIssued`는 DB 트랜잭션에서 발급이 확정되었다는 과거의 사실이다.

Consumer는 이 이벤트를 받아 쿠폰을 다시 발급하지 않는다.

```
Notification Consumer
→ 발급 성공 알림 생성

MyPage Consumer
→ 사용자 쿠폰 조회 모델 갱신

Analytics Consumer
→ 발급 성공 통계 반영
```

---

## 9. CouponIssued 이벤트 구조

Kafka의 Partition과 Offset은 Kafka 안의 물리적인 위치다.

같은 논리 이벤트가 재발행되면 서로 다른 Offset을 가질 수 있다.

```
Partition 1 / Offset 100 / eventMessageId = evt-001
Partition 1 / Offset 101 / eventMessageId = evt-001
```

따라서 결과 이벤트 자체를 식별할 ID가 필요하다.

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

각 필드의 의미는 다음과 같다.

| 필드 | 의미 |
| --- | --- |
| `eventMessageId` | 결과 이벤트 자체의 고유 ID |
| `eventType` | 이미 발생한 비즈니스 사실 |
| `schemaVersion` | 이벤트 Payload 형식의 스키마 버전 |
| `aggregateVersion` | 같은 Aggregate에서 몇 번째 상태 변경인지 나타내는 순서 버전 |
| `occurredAt` | 비즈니스 결과가 발생한 시각 |
| `requestId` | 원래 쿠폰 발급 요청 식별자 |
| `data` | 후속 Consumer가 사용할 결과 데이터 |

기존 시스템에서 `eventId`는 쿠폰 행사 ID로 사용했다.

다음 두 값을 혼동하지 않도록 이름을 구분한다.

```
couponEventId
→ 쿠폰 행사 ID

eventMessageId
→ 결과 이벤트 메시지 ID
```

### 9-1. `occurredAt`, `issuedAt`, `aggregateVersion`

!image.png

`occurredAt`은 Publisher가 Kafka에 보낸 시각이 아니다.

```
occurredAt
→ 쿠폰 발급이라는 비즈니스 결과가 발생한 시각

publishedAt
→ Publisher가 Kafka ACK를 확인한 뒤 DB에 기록한 시각
```

발급 성공 이벤트에서는 `occurredAt`과 `issuedAt`을 같은 기준 시각으로 사용할 수 있다.

Publisher 재시도로 발행 시각이 바뀌더라도 `occurredAt`과 Payload는 변경하지 않는다.

다만 `occurredAt`은 이벤트 순서를 안전하게 판단하는 주 기준으로 사용하지 않는다.

```
서로 다른 이벤트가 같은 시각을 가질 수 있다.
서버나 DB의 시간 정밀도에 따라 Timestamp가 같아질 수 있다.
여러 컴포넌트의 시계가 완전히 일치하지 않을 수 있다.
```

따라서 역할을 다음과 같이 구분한다.

```
schemaVersion
→ Payload 형식의 버전

aggregateVersion
→ 같은 Aggregate의 상태 변경 순서

occurredAt
→ 비즈니스 발생 시각과 감사·추적 정보
```

예를 들어 하나의 `CouponIssue`가 다음과 같이 변경되었다면 Aggregate 버전은 증가한다.

```
CouponIssued
→ aggregateVersion = 1

CouponCancelled
→ aggregateVersion = 2
```

`aggregateVersion`은 Publisher가 임의로 생성하는 값이 아니다. 도메인 상태 변경과 같은 DB 트랜잭션에서 확정한 값을 Outbox Payload에 저장해야 한다.

### 9-2. 이벤트 Payload는 Outbox에 완성된 형태로 저장한다

!image.png

Publisher가 발행 시점에 현재 비즈니스 테이블을 다시 조회해 Payload를 만들면 안 된다.

```
Outbox INSERT 시점
→ 발행할 Payload를 확정하여 저장

Publisher 실행 시점
→ 저장된 Payload를 그대로 전달
```

발행을 기다리는 사이 원본 데이터가 변경될 수 있기 때문이다.

Outbox 행은 특정 시점에 발생한 사실의 불변 Snapshot으로 다룬다.

### 9-3. Partition Key와 순서 보장 범위

!image.png

Kafka에서 같은 Key를 가진 Record는 같은 Partition으로 전달된다.

```
같은 Partition Key
→ 같은 Partition으로 전달됨
```

여기서 자주 하는 오해가 있다.

```
같은 Partition에 저장된다
≠
비즈니스 발생 순서대로 저장된다
```

Partition 안의 순서는 **Producer가 `send()`를 호출한 순서**로 결정된다.

Outbox의 DB 저장 순서가 아니다.

**Polling Outbox Publisher 구조에서는 발행 순서가 DB 저장 순서와 달라질 수 있다.**

주된 원인은 세 가지다.

```
1. FOR UPDATE SKIP LOCKED

→ 잠긴 행을 건너뛰고 다음 행을 가져온다.
→ 애초에 순서를 포기하는 장치다.

2. 실패 후 Backoff 재시도

→ 먼저 저장된 이벤트가 발행에 실패하면
   next_attempt_at 이후로 밀린다.

3. 여러 Publisher 인스턴스의 병렬 처리

→ 서로 다른 인스턴스가 서로 다른 속도로 발행한다.
```

구체적인 역전 시나리오는 다음과 같다.

```
DB 저장 순서

outbox 1 : CouponIssued     (couponIssueId = 5001)
outbox 2 : CouponCancelled  (couponIssueId = 5001)

발행 과정

Publisher A : outbox 1 Claim → Kafka 발행 실패
              → PENDING, next_attempt_at = +4초

Publisher B : outbox 2 Claim → 발행 성공

Kafka Partition 저장 순서

Offset 200 : CouponCancelled
Offset 201 : CouponIssued
```

단일 Publisher 인스턴스만 사용하더라도 Backoff 재시도만으로 역전이 발생한다.

따라서 이번 주차의 설계는 순서를 보장하지 않는다는 것을 명시적으로 선언한다.

```
이번 주차의 발행 보장 수준

→ At-Least-Once Publication
→ 순서 보장 없음 (No Ordering Guarantee)
```

그렇다면 Partition Key를 정하는 이유는 무엇인가.

```
1. 같은 Aggregate의 이벤트를 한 Partition에 모아
   Consumer 병렬 처리 단위를 명확히 한다.

2. 특정 Partition으로 부하가 몰리지 않도록 분산한다.

3. 향후 순서 보장을 도입할 때의 기준 단위를 미리 정한다.
```

이번 주차에서는 `CouponIssue`를 Aggregate로 본다.

```
aggregateType = COUPON_ISSUE
aggregateId   = couponIssueId
partitionKey  = couponIssueId
```

순서가 실제로 필요해지는 시점에는 다음 선택지를 검토한다.

| 방식 | 내용 | 비용 |
| --- | --- | --- |
| A. 순서 미보장 (이번 주차) | Consumer가 `aggregateVersion`으로 out-of-order를 방어 | 낮음 |
| B. Aggregate 단위 직렬화 | 같은 `aggregate_id`에 미완료 행이 있으면 다음 행을 Claim하지 않음 | Claim 쿼리 복잡도 상승, 처리량 저하 |
| C. CDC 전환 | Connector가 WAL 순서를 따라 전달 | CDC 인프라 운영 필요 |

이번 주차에서는 A를 선택하고, Consumer 측 방어 방법은 20-4에서 다룬다.

---

## 10. Outbox 테이블 설계

Polling Publisher를 기준으로 다음과 같이 설계할 수 있다.

```sql
CREATE TABLE outbox_event (
    event_id UUID PRIMARY KEY,

    -- 도메인 추적용 컬럼이다. 필수가 아니다.
    request_id UUID,

    deduplication_key VARCHAR(200) NOT NULL UNIQUE,

    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,

    event_type VARCHAR(100) NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
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
        CHECK (
            status IN (
                'PENDING',
                'PROCESSING',
                'PUBLISHED'
            )
        ),

    CONSTRAINT chk_outbox_schema_version
        CHECK (schema_version >= 1),

    CONSTRAINT chk_outbox_aggregate_version
        CHECK (aggregate_version >= 1),

    CONSTRAINT chk_outbox_attempt_count
        CHECK (attempt_count >= 0),

    CONSTRAINT chk_outbox_state_fields
        CHECK (
            (
                status = 'PENDING'
                AND claim_token IS NULL
                AND lease_until IS NULL
                AND published_at IS NULL
            )
            OR
            (
                status = 'PROCESSING'
                AND claim_token IS NOT NULL
                AND lease_until IS NOT NULL
                AND published_at IS NULL
            )
            OR
            (
                status = 'PUBLISHED'
                AND claim_token IS NULL
                AND lease_until IS NULL
                AND published_at IS NOT NULL
            )
        )
);
```

발행 가능한 `PENDING` 행을 찾기 위한 Partial Index를 추가한다.

```sql
CREATE INDEX idx_outbox_pending
ON outbox_event (
    next_attempt_at,
    created_at,
    event_id
)
WHERE status = 'PENDING';
```

Lease가 만료된 `PROCESSING` 행을 찾기 위한 인덱스도 추가한다.

```sql
CREATE INDEX idx_outbox_expired_processing
ON outbox_event (
    lease_until,
    event_id
)
WHERE status = 'PROCESSING';
```

`updated_at`의 기본값은 INSERT 시점에만 적용된다.

상태가 변경될 때마다 애플리케이션 UPDATE문 또는 DB Trigger로 직접 갱신해야 한다.

### 10-1. 상태의 의미

!image.png

| 상태 | 의미 |
| --- | --- |
| `PENDING` | 발행할 이벤트가 DB에 저장되었고 아직 Claim되지 않음 |
| `PROCESSING` | 특정 Publisher가 발행 권한을 얻어 처리 중 |
| `PUBLISHED` | Kafka ACK 확인 후 완료 상태까지 DB에 반영됨 |

다음 상태는 서로 같은 의미가 아니다.

```
request = ISSUED
≠
outbox = PUBLISHED
```

```
outbox = PUBLISHED
≠
모든 Consumer 처리 완료
```

### 10-2. `request_id`는 필수 컬럼이 아니다

!image.png

`outbox_event`는 특정 도메인 전용 테이블이 아니라 **여러 종류의 결과 이벤트를 담는 범용 발행 큐**다.

`request_id`를 `NOT NULL`로 만들면 다음 이벤트를 저장할 수 없다.

```
운영자가 직접 취소한 CouponCancelled
만료 배치가 생성한 CouponExpired
정산 배치가 생성한 통계 이벤트
```

이 이벤트들은 `coupon_issue_request`에서 출발하지 않는다.

따라서 역할을 다음과 같이 분리한다.

```
aggregate_type / aggregate_id
→ 이벤트가 속한 도메인 객체를 식별하는 필수 정보

request_id
→ 원래 사용자 요청과 연결하기 위한 선택적 추적 정보
```

`CouponIssued`에는 `request_id`가 항상 존재하므로 애플리케이션 코드에서 반드시 채운다.

DB 제약으로 강제하지 않을 뿐이다.

### 10-3. `deduplication_key`

!image.png

`deduplication_key`는 같은 비즈니스 결과의 Outbox 행이 여러 번 만들어지는 것을 막는다.

`request_id`에 의존하지 않도록 Aggregate 기준으로 정의한다.

```
{eventType}:{aggregateType}:{aggregateId}
```

예시는 다음과 같다.

```
COUPON_ISSUED:COUPON_ISSUE:5001
```

`event_id`와 `deduplication_key`의 역할은 다르다.

```
event_id
→ 생성된 이벤트 메시지의 식별자

deduplication_key
→ 같은 비즈니스 결과의 중복 생성 방지 기준
```

같은 Aggregate에서 여러 번 발생할 수 있는 이벤트라면 이 규칙만으로 부족하다.

```
CouponUsed
→ 같은 couponIssueId에서 반복될 수 있음
→ 발생 시점이나 시퀀스 같은 추가 구분자가 필요
```

`CouponIssued`는 하나의 `couponIssueId`에 대해 한 번만 발생하므로 위 규칙으로 충분하다.

### 10-4. `deduplication_key` UNIQUE 위반이 발생하면 어떻게 하는가

!image.png

**정상 경로에서는 이 위반이 발생하지 않아야 한다.**

중복 처리를 실제로 막는 것은 11절의 첫 번째 UPDATE다.

```
UPDATE coupon_issue_request
SET status = 'PROCESSING'
WHERE request_id = :requestId
  AND status = 'PENDING'
```

이미 처리된 요청이라면 이 UPDATE가 0행이 되어 Outbox INSERT까지 도달하지 않는다.

`deduplication_key`의 UNIQUE 제약은 그 위에 놓인 마지막 안전장치다.

따라서 위반이 발생했다는 것은 다음을 의미한다.

```
설계 가정이 깨졌다.

→ 요청 상태 관리에 결함이 있거나
→ 서로 다른 요청이 같은 Aggregate에 대해
   같은 이벤트를 만들려 했거나
→ deduplication_key 생성 규칙이 잘못되었다.
```

UNIQUE 충돌이 발생했다는 것은 같은 Key를 가진 기존 Outbox 행이 이미 존재한다는 뜻이다. 따라서 충돌했다고 해서 발행할 이벤트 행이 전혀 없는 것은 아니다.

그럼에도 현재 설계에서는 `ON CONFLICT DO NOTHING`으로 조용히 넘기지 않는다.

```
ON CONFLICT DO NOTHING 사용 시

→ 기존 Outbox 행과 현재 생성하려던 이벤트가
   실제로 같은 비즈니스 결과인지 확인하지 않음
→ request_id, aggregate_id, event_type, payload가
   서로 다른 충돌도 조용히 숨길 수 있음
→ 요청 상태 관리 결함이나 Key 생성 버그를 발견하기 어려움
```

따라서 현재 설계에서는 전체 트랜잭션을 Rollback하고 알람을 발생시킨다.

멱등 INSERT 방식으로 설계하고 싶다면 충돌 후 기존 행을 조회해 다음 값이 모두 같은지 확인해야 한다.

```
request_id
aggregate_type / aggregate_id
event_type
schema_version
aggregate_version
payload
```

동일한 이벤트임이 확인된 경우에만 기존 Outbox 행을 재사용하는 별도 정책을 둘 수 있다.

---

## 11. 발급 트랜잭션에서 Outbox 저장하기

쿠폰 발급 성공 시 다음 작업을 하나의 DB 트랜잭션으로 수행한다.

```
1. 요청 처리 권한 획득
2. 수량 확보
3. 발급 기록 저장
4. 요청 성공 상태 확정
5. CouponIssued Outbox 저장
```

정상 SQL 흐름은 다음과 같다.

```sql
BEGIN;

-- 1. 한 Consumer만 처리 권한을 얻는다.
UPDATE coupon_issue_request
SET status = 'PROCESSING',
    processing_started_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PENDING';

-- affected rows = 1인 경우에만 계속 진행한다.

-- 2. 수량이 남아 있을 때만 증가한다.
UPDATE coupon_event
SET issued_count = issued_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :couponEventId
  AND issued_count < total_quantity;

-- affected rows = 1인 경우에만 발급 INSERT를 수행한다.

-- 3. 최종 발급 기록을 저장하고 ID와 발급 시각을 얻는다.
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

-- 반환된 값을 couponIssueId와 issuedAt으로 사용한다.

-- 4. 요청 상태를 최종 성공으로 변경한다.
UPDATE coupon_issue_request
SET status = 'ISSUED',
    coupon_issue_id = :couponIssueId,
    completed_at = :issuedAt,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING';

-- affected rows = 1이어야 한다.

-- 5. 발급 성공 이벤트를 같은 트랜잭션에 저장한다.
--    ON CONFLICT를 사용하지 않는다. (10-4 참고)
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
    :deduplicationKey,
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

각 단계의 영향받은 행 수를 확인해야 한다.

```
처리 권한 획득
→ 반드시 1행

수량 UPDATE
→ 성공 경로에서는 반드시 1행

최종 ISSUED 변경
→ 반드시 1행

Outbox INSERT
→ 반드시 1행
```

첫 번째 처리 권한 획득이 0행이면 바로 발급 실패로 판단하거나 예외를 던지지 않는다.

기존 요청 상태를 다시 확인한다.

```
request = ISSUED
→ 이미 완료된 요청의 재전달
→ 새 발급과 새 Outbox 없이 정상 종료

request = FAILED
→ 이미 최종 실패한 요청의 재전달
→ 새 발급과 새 Outbox 없이 정상 종료

request = PROCESSING
→ 11-5 참고

요청 행 없음 또는 메시지 내용 불일치
→ 등록 불일치나 잘못된 메시지로 분류
```

마지막 상태 변경이 0행인데 발급 결과와 Outbox를 Commit하면 요청 상태와 실제 결과가 어긋난다.

예상한 결과가 아니면 전체 트랜잭션을 Rollback해야 한다.

애플리케이션 코드의 구조는 다음과 같다.

```java
@Transactional
public void issueCoupon(IssueMessage message) {
    acquireRequest(message.requestId());

    increaseIssuedCount(message.eventId());

    CouponIssue issue = saveCouponIssue(message);

    markRequestIssued(
        message.requestId(),
        issue.getId(),
        issue.getIssuedAt()
    );

    CouponIssuedEvent event =
        CouponIssuedEvent.from(message, issue);

    saveOutboxEvent(event);
}
```

핵심은 발급 트랜잭션에서 Kafka를 호출하지 않는 것이다.

```
kafkaTemplate.send()
→ 발급 트랜잭션에서 사용하지 않음

outboxRepository.save()
→ 발급 트랜잭션에서 사용
```

### 11-1. Outbox INSERT가 실패한 경우

```
issued_count 증가 성공
coupon_issue INSERT 성공
request ISSUED 변경 성공
outbox_event INSERT 실패
```

전체 트랜잭션을 Rollback한다.

```
issued_count 증가 취소
coupon_issue 저장 취소
request ISSUED 변경 취소
outbox_event 없음
```

발급 결과만 Commit하고 Outbox 저장 실패를 무시하면 안 된다.

### 11-2. 수량이 소진된 경우

조건부 수량 UPDATE가 0행이면 `CouponIssued` 이벤트를 저장하지 않는다.

```
issued_count UPDATE 0행
→ coupon_issue 없음
→ request FAILED / SOLD_OUT
→ CouponIssued Outbox 없음
```

실패 결과 이벤트를 지원한다면 `request FAILED`와 `CouponIssueFailed` Outbox를 같은 트랜잭션에 저장한다.

조건부 UPDATE가 0행인 원인은 수량 소진뿐만 아니라 이벤트 행이 존재하지 않는 경우도 포함할 수 있다. 이벤트 상태와 기간까지 조건에 넣었다면 종료되었거나 비활성화된 경우도 0행이 될 수 있으므로, 최종 실패 원인은 별도로 구분해야 한다.

여기서 반드시 확인해야 할 사실이 있다.

**Redis가 SUCCESS로 판정한 요청이 DB에서 실제 `SOLD_OUT`으로 실패했다면 Redis와 DB의 선착순 상태가 어긋난 것이다.**

다만 다음 비교는 장애를 판단하는 기준으로 사용할 수 없다.

```
Redis 통과 인원 > DB issued_count
```

비동기 처리 중에는 Redis를 통과했지만 아직 Kafka나 Consumer에서 처리 중인 요청이 존재하므로 이 차이는 정상적으로 발생할 수 있다.

```
Redis 통과 인원
=
DB 발급 완료 인원
+
아직 처리 중인 요청
+
최종 실패했지만 Redis에 남아 있는 요청
```

모든 정상 요청의 처리가 끝난 안정 상태에서는 Redis 통과 인원과 DB 발급 수량이 수렴하는 것을 기대한다.

Redis SUCCESS 이후 DB가 이미 품절이었다면 다음 가능성을 확인해야 한다.

```
Redis 통과 사용자 데이터의 일부 유실
Redis Key 초기화·만료·Eviction
Redis limit와 DB total_quantity 불일치
Redis를 거치지 않은 관리자·배치 발급
복구 작업 이후 두 저장소의 기준 불일치
```

`request FAILED / SOLD_OUT` 확정만으로 저장소 상태가 자동으로 복구되지는 않는다.

```
DB
→ 해당 요청을 SOLD_OUT으로 확정

Redis
→ 현재 사용자가 통과 사용자 Set에 남아 있을 수 있음
```

그렇다고 현재 사용자를 즉시 Redis에서 제거해 다음 사용자를 통과시켜서도 안 된다. DB가 실제로 품절이라면 새 사용자도 발급될 수 없기 때문이다.

이 경로는 정상 비즈니스 흐름이 아니라 **저장소 간 divergence 신호**로 다룬다.

```
이 경로 진입 시

→ 경고 로그와 알람 발생
→ 오래된 처리 중 요청을 제외한 수렴 차이 확인
→ Redis limit, Set, DB issued_count와 발급 기록 비교
→ 원인별 보정(reconciliation) 정책 수행
```

보정 배치의 구체적 설계는 이번 주차 범위가 아니지만, 단순한 Redis·DB 숫자 차이가 아니라 **처리가 끝난 뒤에도 남아 있는 비수렴 상태**를 기준으로 판단해야 한다.

### 11-3. 사용자 중복 발급인 경우

`coupon_issue`의 `UNIQUE(event_id, user_id)` 위반이 발생하면 PostgreSQL 트랜잭션 전체가 실패한다.

```
PENDING → PROCESSING
issued_count 증가
coupon_issue UNIQUE 위반

→ 전체 Rollback
```

Rollback 후 별도 트랜잭션에서 기존 `coupon_issue`와 요청 상태를 확인한다.

현재 구조에서 같은 `requestId`의 재전달은 첫 번째 요청 상태 UPDATE에서 차단되어야 한다. 따라서 이 지점의 UNIQUE 위반은 일반적으로 다음 상황을 의미한다.

```
다른 requestId로 같은 사용자가 다시 요청함
Redis에서 해당 사용자의 통과 기록이 유실되었음
Redis를 우회한 발급 경로가 존재함
요청 상태 또는 중복 판정 로직에 결함이 있음
```

기존 발급 기록이 정상적으로 존재하고 현재 요청과 다른 요청에서 발급된 것이 확인되면, 현재 요청을 `FAILED / DUPLICATE_USER`로 확정한다.

성공 발급이 아니므로 현재 요청을 위한 `CouponIssued` Outbox는 만들지 않는다.

다만 이 경로를 **무조건 Redis 자리 한 개가 낭비된 상태**라고 해석하면 안 된다.

현재 선착순 판정은 별도 숫자 Counter가 아니라 Redis Set의 고유 사용자 수를 기준으로 한다.

예를 들어 DB에서 이미 쿠폰을 받은 사용자의 Redis Set 기록만 유실되었다고 가정한다.

```
DB issued_count = 10
Redis Set 인원 = 9

기존 발급 사용자가 다시 요청
→ Redis Lua Script가 사용자를 Set에 다시 추가
→ DB에서는 UNIQUE 위반
→ DB 트랜잭션 Rollback

최종 상태
DB issued_count = 10
Redis Set 인원 = 10
```

이 경우 현재 요청에서 사용자를 Redis Set에 다시 추가한 것은 아무에게도 발급되지 않은 자리를 소비한 것이 아니라, 누락된 Redis 상태를 DB와 다시 수렴시킨 것이다.

따라서 DB 중복 발급이 확인되었다고 현재 사용자를 Redis에서 무조건 제거하면 안 된다.

```
SREM을 수행하면
→ DB에는 이미 발급 기록이 존재하지만
→ Redis에서는 해당 사용자가 다시 누락됨
→ 이후 같은 사용자가 다시 SUCCESS를 받을 수 있음
```

다음 항목을 함께 확인해 원인을 분류해야 한다.

```
기존 coupon_issue의 requestId와 발급 시각
현재 requestId와 기존 요청 상태
Redis Set에 현재 사용자가 존재하는지
Redis limit와 DB total_quantity
Redis Set 인원과 안정 상태의 DB issued_count
```

별도 Counter를 중복 확인과 독립적으로 증가시키는 구조이거나, 실패한 요청의 자리를 별도로 보유하는 구조라면 실제 자리 누수가 발생할 수 있다. 하지만 현재의 Set + SCARD 설계에서는 UNIQUE 위반만으로 자리 누수를 단정할 수 없다.

이 경로에서는 다음을 수행한다.

```
현재 요청을 DUPLICATE_USER로 확정
기존 DB 발급 결과를 사용자에게 안내
경고 로그와 알람 발생
Redis와 DB의 비수렴 여부 확인
원인에 맞는 보정 수행
```

`request = FAILED / DUPLICATE_USER` 확정은 현재 요청의 사용자 응답을 결정하는 작업이다. Redis 상태를 변경할지는 기존 DB 발급과 Redis Set 상태를 확인한 뒤 별도로 결정한다.

### 11-4. 일시적 DB 오류인 경우

DB 연결 오류, Deadlock, Lock Timeout 같은 일시적 기술 실패는 다음 상태로 Rollback된다.

```
request = PENDING
issued_count 증가 없음
coupon_issue 없음
outbox_event 없음
```

이 오류를 최종 비즈니스 실패로 저장하지 않는다.

Listener가 예외를 다시 던져 원본 `coupon.issue.requested` 재처리 정책으로 넘긴다.

### 11-5. `request = PROCESSING`을 관측한 경우

이번 주차 설계에서 요청 상태 전이는 **하나의 트랜잭션 안에서** 일어난다.

```
BEGIN
  PENDING → PROCESSING
  ...
  PROCESSING → ISSUED
COMMIT
```

따라서 `PROCESSING` 상태는 트랜잭션 내부에서만 존재하고 밖으로 Commit되지 않는다.

```
트랜잭션 성공
→ 다른 세션이 관측하는 최종 상태는 ISSUED

트랜잭션 실패
→ Rollback되어 PENDING으로 되돌아감
```

**즉, 다른 Consumer가 Commit된 `PROCESSING`을 조회하는 상황은 현재 설계에서 발생하지 않는다.**

그럼에도 코드에 분기를 남기는 이유는 방어를 위해서다.

```
request = PROCESSING을 관측했다

→ 설계 가정이 깨졌다는 신호
→ 경고 로그와 알람 발생
→ 발급 로직을 새로 실행하지 않고 재처리 대상으로 남김
```

이 분기를 “정상적인 동시 처리 상황”으로 취급하면 안 된다.

`PROCESSING`을 Commit하는 별도 Claim 트랜잭션 구조를 사용할 때에만 Lease와 처리자 확인 로직이 의미를 가진다.

```
현재 설계 (단일 트랜잭션)
→ Commit된 PROCESSING 없음
→ 관측되면 이상 상황

별도 Claim 트랜잭션 구조
→ Commit된 PROCESSING 존재
→ Lease 기반 회수 정책 필요
```

---

## 12. 원본 Kafka Offset은 언제 Commit하는가

원본 메시지 처리는 다음 순서를 따른다.

```
coupon.issue.requested 수신
        |
        v
발급 결과 + Outbox DB Transaction
        |
        | Commit 성공
        v
원본 Kafka Offset Commit
```

DB Commit 전에 Offset을 먼저 Commit하면 다음 문제가 생긴다.

```
Offset Commit
        |
        X DB Transaction 실패

→ Kafka는 처리 완료로 판단
→ DB 발급 결과 없음
→ 메시지 유실
```

DB Commit 후 Offset Commit 전에 Consumer가 종료될 수 있다.

```
DB
→ request ISSUED
→ coupon_issue 존재
→ Outbox PENDING 존재

Kafka
→ Offset 미반영
→ 원본 메시지 재전달
```

재전달된 Consumer는 기존 요청 상태를 확인한다.

```
request = ISSUED
→ 새 수량 증가 없음
→ 새 coupon_issue 없음
→ 새 Outbox 없음
→ 정상 처리 후 Offset Commit
```

Outbox와 requestId 멱등 처리를 함께 사용해야 하는 이유다.

---

## 13. Polling Outbox Publisher

Polling Publisher는 일정 주기로 발행 가능한 Outbox 행을 조회한다.

```
Scheduler
    |
    v
만료된 PROCESSING 복구
    |
    v
PENDING 조회
    |
    v
PROCESSING Claim
    |
    v
Claim Transaction Commit
    |
    v
Kafka 발행 및 ACK 확인
    |
    +-- ACK 성공 → PUBLISHED
    |
    +-- 실패 → PENDING + 다음 재시도 시각
```

### 13-1. 이번 주차의 기본 구현 모델은 동기 발행이다

Kafka 발행 결과를 확인하는 방식은 두 가지가 있다.

```
동기 (Synchronous)
→ send() 이후 timeout을 두고 결과를 기다린다.

비동기 (Asynchronous)
→ Callback에서 결과를 처리한다.
```

두 방식은 처리량과 구현 난이도가 다르다.

| 항목 | 동기 | 비동기 |
| --- | --- | --- |
| 구현 난이도 | 낮음 | 높음 |
| Lease 시간 산정 | 단순 | Batch 전체를 고려해야 함 |
| 스레드 관리 | 불필요 | 전용 Executor 필요 |
| 처리량 | 낮음 (건당 왕복 대기) | 높음 |
| 장애 추적 | 스택 트레이스가 그대로 이어짐 | 콜백 경계에서 끊김 |

**이번 주차의 기본 구현은 작은 Batch를 사용하는 동기 발행이다.**

```
batchSize
→ 작게 시작한다. (예: 50~100)

발행 방식
→ Batch 내 순차 동기 발행

Lease
→ batchSize × 건당 최대 대기 시간보다 충분히 길게
```

비동기 발행은 처리량이 부족해질 때 도입하는 확장이며, 16-3에서 별도로 다룬다.

두 모델을 섞어 구현하면 Lease 시간과 스레드 모델을 동시에 잘못 계산하기 쉽다.

### 13-2. Kafka 호출은 DB 트랜잭션 밖에서 수행한다

```
Claim Transaction
→ 짧게 Commit

Kafka 발행
→ DB Lock을 보유하지 않음

완료 상태 Transaction
→ 짧게 Commit
```

DB Row Lock을 잡은 채 Kafka ACK를 기다리면 다음 문제가 생긴다.

```
DB Connection 장시간 점유
Row Lock 장시간 유지
Publisher 간 대기 증가
트랜잭션 Timeout
```

---

## 14. 여러 Publisher의 동시 Claim

여러 Publisher 인스턴스가 동시에 같은 `PENDING` 행을 가져가면 동일 이벤트를 불필요하게 동시에 발행할 수 있다.

PostgreSQL에서는 `FOR UPDATE SKIP LOCKED`를 이용해 queue 형태의 행을 나누어 가져갈 수 있다.

Claim 대상 조회와 `PROCESSING` 변경은 같은 짧은 DB 트랜잭션에서 수행한다.

```sql
BEGIN;

WITH candidates AS (
    SELECT event_id
    FROM outbox_event
    WHERE status = 'PENDING'
      AND next_attempt_at <= CURRENT_TIMESTAMP
    ORDER BY
        next_attempt_at,
        created_at,
        event_id
    LIMIT :batchSize
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_event AS outbox
SET status = 'PROCESSING',
    claim_token = :claimToken,
    lease_until =
        CURRENT_TIMESTAMP
        + (:leaseSeconds * INTERVAL '1 second'),
    attempt_count = attempt_count + 1,
    updated_at = CURRENT_TIMESTAMP
FROM candidates
WHERE outbox.event_id = candidates.event_id
RETURNING outbox.*;

COMMIT;
```

이번 설계에서 `attempt_count`는 **Publisher가 발행 권한을 얻은 횟수**다.

Claim할 때 증가시키므로 다음 장애도 시도 횟수에 포함된다.

```
PROCESSING Claim
        |
        X Kafka 발행 전 Publisher 종료
```

`SKIP LOCKED`는 물리적인 중복 발행을 완전히 제거하는 장치가 아니다.

동시에 같은 PENDING 행을 Claim하는 경쟁을 줄이는 장치다.

`ORDER BY`가 있지만 `SKIP LOCKED`가 잠긴 행을 건너뛰므로 **전체 발행 순서는 보장되지 않는다**. 9-3에서 설명한 대로다.

---

## 15. Lease와 Claim Token

Publisher가 `PROCESSING`으로 변경한 직후 종료될 수 있다.

```
PENDING
   |
   | Publisher A Claim
   v
PROCESSING
   |
   X Publisher A 종료
```

`PROCESSING`만 저장하고 복구 정책이 없다면 이 행은 영원히 처리되지 않을 수 있다.

이 문제를 해결하기 위해 `lease_until`을 둔다.

```
lease_until 만료 전
→ 기존 Publisher가 처리 중이라고 판단

lease_until 만료 후
→ 다른 Publisher가 회수 가능
```

단일 Publisher 인스턴스만 사용하더라도 Lease 또는 시작 시 stale `PROCESSING` 복구가 필요하다.

인스턴스 수가 하나여도 Claim Commit 직후 프로세스가 종료될 수 있기 때문이다.

### 15-1. 만료된 PROCESSING 회수

가장 단순한 방식은 만료된 행을 다시 `PENDING`으로 돌리는 것이다.

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

복구된 행은 다음 Polling에서 다시 Claim된다.

```
PROCESSING 만료
→ PENDING 복구
→ 새 claim_token으로 Claim
→ Kafka 재발행
```

이 과정에서 기존 Publisher의 Kafka 발행이 늦게 성공할 수 있으므로 중복 발행 가능성은 남는다.

### 15-2. Claim Token이 필요한 이유

다음 상황을 생각한다.

```
Publisher A
→ claimToken = token-A
→ 처리가 길어져 Lease 만료

Publisher B
→ 만료 행 회수
→ claimToken = token-B
→ 처리 시작

Publisher A
→ 뒤늦게 Kafka 결과 수신
→ DB 상태 변경 시도
```

Publisher A가 조건 없이 상태를 변경하면 최신 처리 권한을 가진 Publisher B의 상태를 덮어쓸 수 있다.

따라서 성공과 실패 상태 변경 모두 `claim_token`을 확인한다.

### 15-3. Lease 시간

동기 발행 모델에서는 Lease 시간을 다음 기준으로 계산한다.

```
lease_seconds
>
batchSize × 건당 최대 대기 시간
+
여유 시간
```

건당 최대 대기 시간은 Publisher가 `get()`에 지정한 timeout이며, 이 값은 Producer의 `delivery.timeout.ms`와 함께 설계해야 한다.

```
Lease가 너무 짧음
→ Kafka 발행이 아직 진행 중인데 Lease 만료
→ 다른 Publisher가 회수
→ 동시에 중복 발행 가능
```

다음 방법 중 하나를 선택할 수 있다.

```
작은 Batch 사용            ← 이번 주차의 기본 선택
충분한 Lease 시간 사용
발행 중 Lease 연장
Batch 내 병렬·비동기 발행  ← 16-3의 확장 모델
```

---

## 16. Kafka ACK 확인

Outbox를 `PUBLISHED`로 변경하는 시점은 Kafka 발행 메서드를 호출한 직후가 아니다.

```
Kafka 발행 요청
        |
        v
Broker ACK 성공 확인
        |
        v
Outbox PUBLISHED 변경
```

Spring Kafka의 `KafkaTemplate.send()`는 즉시 `CompletableFuture`를 반환한다.

기본 구현에서는 timeout을 지정해 결과를 기다린다.

```java
for (OutboxEvent event : claimedEvents) {
    try {
        kafkaTemplate
            .send(
                event.getTopic(),
                event.getPartitionKey(),
                event.getPayload()
            )
            .get(sendTimeoutMs, TimeUnit.MILLISECONDS);

        markPublished(
            event.getEventId(),
            event.getClaimToken()
        );

    } catch (Exception e) {
        scheduleRetry(
            event.getEventId(),
            event.getClaimToken(),
            e
        );
    }
}
```

여기서 지켜야 할 규칙은 다음과 같다.

```
이 반복문은 DB 트랜잭션 밖에서 실행한다.

markPublished와 scheduleRetry는
각각 짧은 별도 트랜잭션으로 실행한다.

한 건의 실패가 나머지 Batch 처리를 중단시키지 않는다.
```

### 16-1. ACK 설정

Producer의 ACK 설정에 따라 성공의 내구성이 달라진다.

```
acks = 0
→ Broker 응답을 기다리지 않음

acks = 1
→ Leader 기록만 확인

acks = all
→ In-Sync Replica의 확인을 기다림
```

결과 이벤트 Publisher에서는 일반적으로 다음 설정을 사용한다.

```
acks = all
enable.idempotence = true
```

Producer Idempotence는 Producer 내부 재시도로 생기는 중복을 줄인다.

하지만 다음 애플리케이션 수준 재발행까지 제거하지는 않는다.

```
Kafka 발행 성공
→ Outbox PUBLISHED 변경 전 프로세스 종료
→ 새 Publisher가 같은 Outbox 재발행
```

따라서 Consumer 멱등성은 여전히 필요하다.

### 16-2. Timeout의 의미

Publisher가 Timeout을 받았다고 해서 Kafka에 Record가 반드시 없는 것은 아니다.

```
상황 A
→ Broker에 저장되지 못하고 Timeout

상황 B
→ Broker에는 저장되었지만 ACK를 확인하지 못하고 Timeout
```

Publisher는 두 상황을 완벽하게 구분할 수 없다.

따라서 Timeout이면 `PUBLISHED`로 변경하지 않고 재시도한다.

```
Timeout
→ Outbox 미완료
→ 재시도
→ Kafka 중복 가능
→ Consumer 멱등 처리
```

### 16-3. 확장: 비동기 발행

처리량이 부족해지면 Batch 내 발행을 비동기로 바꿀 수 있다.

이때 **절대 해서는 안 되는 구현**이 있다.

```java
// 잘못된 구현
future.whenComplete((result, exception) -> {
    markPublished(...);   // DB 트랜잭션
});
```

`whenComplete`는 Future를 완료시킨 스레드에서 콜백을 실행한다.

그 스레드는 **Kafka Producer의 내부 Sender 스레드**다.

```
Sender 스레드에서 DB 작업 수행

→ Connection Pool 대기
→ 트랜잭션 Commit 대기
→ Sender 스레드 블로킹
→ 해당 Producer의 모든 전송이 함께 지연
```

부하 상황에서 이 구조는 다음 연쇄 장애를 만든다.

```
Sender 스레드 블로킹
→ 발행 지연 증가
→ Lease 만료
→ 다른 Publisher가 회수
→ 중복 발행 증가
→ 적체 심화
```

반드시 전용 Executor를 지정한다.

```java
future.whenCompleteAsync((result, exception) -> {
    if (exception == null) {
        markPublished(
            event.getEventId(),
            event.getClaimToken()
        );
        return;
    }

    scheduleRetry(
        event.getEventId(),
        event.getClaimToken(),
        exception
    );
}, outboxCallbackExecutor);
```

원칙은 다음과 같다.

```
Kafka Producer 콜백 스레드에서
DB 작업이나 블로킹 I/O를 수행하지 않는다.
```

비동기 모델을 사용할 때는 Lease 계산 기준도 달라진다.

```
동기
→ batchSize × 건당 timeout

비동기
→ Batch 전체가 완료되기까지의 시간
→ Executor 큐 대기 시간까지 포함
```

---

## 17. 발행 성공 처리

Kafka ACK를 확인한 Publisher만 자신의 Claim Token으로 `PUBLISHED` 상태 변경을 시도한다.

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

영향받은 행 수를 확인한다.

```
affected rows = 1

→ 현재 처리 권한을 가진 Publisher가 완료 상태 저장
```

```
affected rows = 0

→ Lease 만료
→ 다른 Publisher가 권한 회수
→ 이미 완료됨
→ 현재 Publisher는 상태를 덮어쓰지 않음
```

ACK를 받았더라도 상태 UPDATE가 0행이면 Kafka에 Record가 존재할 수 있다.

새 Publisher가 다시 발행할 수 있으므로 Consumer의 `eventMessageId` 멱등 처리가 필요하다.

---

## 18. 발행 실패와 재시도

Kafka 발행이 실패하면 현재 Claim Token을 가진 Publisher만 이벤트를 `PENDING`으로 되돌린다.

```sql
UPDATE outbox_event
SET status = 'PENDING',
    next_attempt_at = :nextAttemptAt,
    claim_token = NULL,
    lease_until = NULL,
    last_error = :lastError,
    updated_at = CURRENT_TIMESTAMP
WHERE event_id = :eventId
  AND status = 'PROCESSING'
  AND claim_token = :claimToken;
```

실패 경로에도 `claim_token` 조건이 필요하다.

오래된 Publisher가 새 Publisher의 상태를 덮어쓰는 것을 막기 위해서다.

재시도는 즉시 반복하지 않고 지연시킨다.

```
1차 실패 → 1초 후
2차 실패 → 2초 후
3차 실패 → 4초 후
4차 실패 → 8초 후
```

모든 인스턴스가 같은 시각에 재시도하지 않도록 작은 무작위 지연인 Jitter를 추가할 수 있다.

```
next delay
=
exponential backoff
+
jitter
```

재시도로 인해 `next_attempt_at`이 뒤로 밀리면 **DB 저장 순서와 발행 순서가 달라진다**. 9-3에서 설명한 순서 미보장의 주요 원인이다.

이번 주차에서는 재시도 가능한 상태와 적체 감지까지 다룬다.

최대 재시도 횟수, 영구 실패 분류, DLQ와 운영자 재처리는 8주차에서 구체화한다.

---

## 19. Outbox에서도 중복 발행은 발생한다

다음 장애 구간은 완전히 제거할 수 없다.

```
Kafka 발행 성공
        |
        v
Broker ACK 수신
        |
        X Publisher 종료
        |
        v
Outbox PUBLISHED 변경 안 됨
```

최종 상태는 다음과 같다.

```
Kafka
→ CouponIssued Record 존재

Outbox
→ PROCESSING 또는 복구 후 PENDING
```

Lease 만료 후 같은 이벤트가 다시 발행될 수 있다.

```
Kafka Record 1
eventMessageId = evt-001

Kafka Record 2
eventMessageId = evt-001
```

따라서 Polling Outbox Publisher의 발행 의미는 다음에 가깝다.

```
At-Least-Once Publication
순서 보장 없음
```

단, Outbox 행이 존재하는 것만으로 자동으로 최소 한 번 발행되는 것은 아니다.

다음 조건이 필요하다.

```
Publisher가 계속 동작한다.
Kafka 장애가 회복된다.
발행 실패를 재시도한다.
Outbox 행을 발행 전에 삭제하지 않는다.
영구 실패를 운영에서 확인하고 복구한다.
```

Outbox가 직접 보장하는 핵심은 다음이다.

```
DB 발급 결과가 Commit되면
발행할 이벤트 기록도 반드시 DB에 남는다.
```

Outbox가 보장하지 않는 것은 다음이다.

```
Kafka Record가 물리적으로 정확히 한 번만 생성됨

이벤트가 비즈니스 발생 순서대로 전달됨

모든 Consumer가 반드시 처리 완료함

외부 알림이 정확히 한 번 전송됨
```

---

## 20. Consumer 멱등 처리

같은 `eventMessageId`가 여러 번 전달될 수 있으므로 Consumer는 결과를 한 번만 반영해야 한다.

처리한 이벤트 ID를 Consumer 자신의 DB에 저장할 수 있다.

```sql
CREATE TABLE processed_event (
    subscriber_id VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        subscriber_id,
        event_id
    )
);
```

`subscriber_id`는 실행 중인 프로세스 인스턴스 ID가 아니다.

안정적인 논리 구독자 이름을 사용한다.

```
mypage-coupon-projection
notification-coupon-issued
analytics-coupon-issued
```

서비스별 DB가 완전히 분리되어 있고 하나의 Handler만 존재한다면 각 DB에서 `event_id`만 Primary Key로 사용해도 된다.

`subscriber_id`는 같은 DB 안에서 서로 독립된 여러 Handler가 동일 이벤트를 각각 처리해야 할 때 유용하다.

### 20-1. MyPage Consumer 예시

Consumer는 다음 두 작업을 자신의 DB 트랜잭션으로 묶는다.

```
processed_event 저장

+

사용자 쿠폰 조회 모델 변경
```

```sql
BEGIN;

INSERT INTO processed_event (
    subscriber_id,
    event_id
) VALUES (
    'mypage-coupon-projection',
    :eventMessageId
)
ON CONFLICT DO NOTHING
RETURNING event_id;
```

반환된 행이 없다면 이미 처리한 이벤트다.

```
중복 이벤트
→ 조회 모델을 다시 변경하지 않음
→ 정상 처리로 종료
```

최초 처리라면 조회 모델을 변경한다.

```sql
INSERT INTO user_coupon_view (
    user_id,
    coupon_issue_id,
    coupon_event_id,
    issued_at
) VALUES (
    :userId,
    :couponIssueId,
    :couponEventId,
    :issuedAt
);

COMMIT;
```

두 작업 중 하나라도 실패하면 `processed_event`도 함께 Rollback되어야 한다.

Consumer Offset은 이 DB 트랜잭션 Commit 뒤에 반영한다.

```
Consumer DB Transaction Commit
        |
        v
Consumer Group Offset Commit
```

여기서 `ON CONFLICT DO NOTHING`을 쓰는 것은 10-4의 Outbox INSERT와 목적이 다르다.

```
Outbox INSERT의 UNIQUE 위반
→ 발생하면 안 되는 이상 상황
→ 조용히 넘기면 불일치나 Key 생성 버그를 숨길 수 있음
→ Rollback과 알람

Consumer processed_event의 충돌
→ At-Least-Once 구조에서 정상적으로 예상되는 상황
→ 조용히 건너뛰는 것이 올바른 처리
```

### 20-2. Consumer Commit 후 Offset Commit 전 장애

```
processed_event Commit
조회 모델 변경 Commit
        |
        X Offset Commit 전 Consumer 종료
```

같은 Kafka Record가 재전달된다.

```
processed_event 중복
→ 비즈니스 변경 생략
→ 정상 처리
→ Offset Commit
```

### 20-3. `processed_event` 보관 정책

`processed_event`는 처리한 이벤트마다 한 행씩 쌓이므로 무한히 증가한다.

선착순 이벤트처럼 단시간에 대량 발급이 발생하는 시스템에서는 Outbox보다 빠르게 커질 수 있다.

따라서 오래된 행을 삭제해야 한다.

여기에 함정이 있다.

**보관 기간을 너무 짧게 잡으면 중복 방어가 뚫린다.**

```
1. eventMessageId = evt-001 처리 완료
2. processed_event 보관 기간 경과 → 삭제
3. Kafka Topic에는 아직 Record가 남아 있음
4. Consumer Group Offset 초기화 또는 재처리 수행
5. evt-001 재전달
6. processed_event에 기록이 없음
7. 조회 모델 중복 반영
```

따라서 다음 조건을 만족해야 한다.

```
processed_event 보관 기간
>
Kafka Topic 보관 기간 + 최대 재전달 지연
```

`최대 재전달 지연`에는 다음이 포함된다.

```
Consumer 장애 복구 시간
Offset 되감기 운영 작업 가능 범위
Outbox 재발행 지연 최대치
```

삭제는 Outbox와 마찬가지로 작은 Batch로 나누어 수행한다.

```
processed_at < :cutoff 인 행
→ 1,000건 DELETE
→ Commit
→ 잠시 대기
→ 반복
```

### 20-4. 순서 역전 방어

9-3에서 설명했듯 이 구조는 이벤트 순서를 보장하지 않는다.

`CouponIssued` 하나만 존재하는 이번 주차에서는 문제가 드러나지 않는다.

하지만 같은 Aggregate에 대한 이벤트가 둘 이상이 되는 순간 문제가 된다.

```
발생 순서
CouponIssued  (aggregateVersion = 1)
→ CouponCancelled (aggregateVersion = 2)

전달 순서
CouponCancelled → CouponIssued

순진한 Consumer
→ 취소 반영
→ 발급 반영
→ 최종 상태: 발급됨 (틀림)
```

Consumer는 도착 순서를 신뢰하지 않고 `aggregateVersion`으로 방어해야 한다.

`occurredAt`만 사용하는 것은 안전하지 않다.

```
두 이벤트가 같은 Timestamp를 가질 수 있다.
서버와 DB의 시계나 시간 정밀도가 다를 수 있다.
조회 행이 없는 경우와 오래된 이벤트인 경우를 UPDATE 0행만으로 구분하기 어렵다.
```

따라서 조회 모델에는 마지막으로 반영한 Aggregate 버전을 저장한다.

```sql
INSERT INTO user_coupon_view (
    coupon_issue_id,
    user_id,
    coupon_event_id,
    status,
    last_aggregate_version,
    last_event_at
) VALUES (
    :couponIssueId,
    :userId,
    :couponEventId,
    :newStatus,
    :aggregateVersion,
    :occurredAt
)
ON CONFLICT (coupon_issue_id)
DO UPDATE
SET status = EXCLUDED.status,
    last_aggregate_version = EXCLUDED.last_aggregate_version,
    last_event_at = EXCLUDED.last_event_at
WHERE user_coupon_view.last_aggregate_version
      < EXCLUDED.last_aggregate_version;
```

```
affected rows = 1
→ 최초 이벤트이거나 더 최신 버전을 반영함

affected rows = 0
→ 같거나 더 높은 Aggregate 버전이 이미 반영되어 있음
→ 현재 이벤트의 상태 변경을 생략함
```

예를 들어 `CouponCancelled(version=2)`가 먼저 도착하면 조회 행을 취소 상태로 생성한다.

나중에 `CouponIssued(version=1)`이 도착하더라도 현재 저장된 버전보다 낮으므로 상태를 되돌리지 않는다.

같은 Aggregate에서 서로 다른 이벤트가 동일한 `aggregateVersion`을 가지는 것은 정상 상황이 아니다. 이런 충돌은 상태 변경을 생략하는 것과 별개로 로그와 알람 대상으로 둔다.

정리하면 Consumer에게는 두 가지 방어가 필요하다.

```
processed_event
→ 같은 eventMessageId의 중복 반영 방어

aggregateVersion 비교
→ 오래된 이벤트가 최신 상태를 덮어쓰는 것 방어
```

두 방어는 서로를 대체하지 않는다.

`occurredAt`은 비즈니스 발생 시각과 감사·추적 정보로 유지하지만, 순서 정합성의 주 기준은 `aggregateVersion`으로 둔다.

### 20-5. 외부 알림 API는 별도 문제다

Notification Consumer가 이메일이나 SMS 같은 외부 API를 호출하면 다음 Dual Write가 다시 생긴다.

```
processed_event DB 저장

+

외부 알림 API 호출
```

외부 API 호출은 일반적인 로컬 DB 트랜잭션에 포함되지 않는다.

실서비스에서는 다음 방법을 고려한다.

```
Notification Service 내부 Outbox

알림 작업 테이블과 별도 Sender

외부 알림 Provider의 Idempotency Key

notificationId 기반 중복 전송 방어
```

`processed_event`만 추가했다고 외부 알림이 물리적으로 정확히 한 번 전송되는 것은 아니다.

---

## 21. 주요 장애 상황

| 장애 상황 | 결과 | 처리 |
| --- | --- | --- |
| 발급 트랜잭션 Commit 전 장애 | 발급 결과와 Outbox 모두 없음 | 원본 요청 메시지 재처리 |
| 발급 DB Commit 후 원본 Offset Commit 전 장애 | 발급 결과와 Outbox 존재, 원본 메시지 재전달 가능 | requestId 멱등 처리 |
| DB Commit 후 Publisher 실행 전 장애 | Outbox `PENDING` | Publisher 복구 후 발행 |
| Publisher Claim 후 발행 전 장애 | Outbox `PROCESSING` | Lease 만료 후 회수 |
| Kafka 발행 실패 | Outbox 미완료 | Backoff 후 재시도 |
| Kafka Timeout | Kafka 저장 여부 불명확 | 미완료 유지 후 재시도, 중복 허용 |
| Kafka 발행 성공 후 완료 표시 전 장애 | Kafka Record 존재, Outbox 미완료 | 재발행 가능, Consumer 멱등 처리 |
| 재시도로 발행 순서 역전 | 오래된 이벤트가 나중에 도착 | Consumer가 `aggregateVersion` 비교로 방어 |
| Consumer DB Commit 후 Offset Commit 전 장애 | 결과 이벤트 재전달 | `processed_event`로 중복 방어 |
| Redis SUCCESS 후 DB `SOLD_OUT` | Redis는 통과시켰지만 DB는 이미 품절 | 알람, 원인 분석과 보정 필요 |
| Redis SUCCESS 후 DB `DUPLICATE_USER` | DB에는 기존 발급이 있으나 Redis 중복 판정이 놓침 | 기존 발급과 Redis Set을 비교해 원인별 보정 |

특히 다음 두 재시도 대상을 구분해야 한다.

```
발급 DB 트랜잭션 실패

→ coupon.issue.requested 메시지 재처리
```

```
발급 DB Commit 성공
→ CouponIssued 발행 실패

→ 쿠폰을 다시 발급하지 않음
→ Outbox 이벤트만 재발행
```

결과 이벤트 발행 실패를 이유로 이미 발급된 쿠폰을 취소하거나 다시 발급하면 안 된다.

마지막 두 행은 성격이 다르다.

```
위쪽 장애들
→ 이번 주차의 패턴으로 방어됨

Redis 관련 divergence
→ 이번 주차의 패턴으로 방어되지 않음
→ 별도 보정 메커니즘 필요
```

---

## 22. Polling Publisher와 CDC

Outbox 이벤트를 Kafka에 전달하는 대표적인 방법은 Polling과 CDC다.

### 22-1. Polling Publisher

애플리케이션이 일정 주기로 Outbox 테이블을 조회한다.

```
Scheduler
→ PENDING 조회
→ Claim
→ Kafka 발행
→ PUBLISHED 변경
```

장점은 다음과 같다.

```
애플리케이션 코드로 동작을 이해하기 쉽다.
별도의 CDC 인프라 없이 시작할 수 있다.
재시도와 상태를 애플리케이션에서 직접 제어할 수 있다.
```

단점은 다음과 같다.

```
주기적인 DB 조회 부하
Polling 간격만큼의 발행 지연
Claim, Lease, 재시도 로직 직접 구현
PUBLISHED 행 정리 정책 필요
발행 순서 보장 없음
```

### 22-2. CDC

CDC는 DB의 Transaction Log를 읽어 Outbox INSERT를 Kafka에 전달한다.

```
DB Commit
→ PostgreSQL WAL
→ CDC Connector
→ Kafka
```

CDC 방식에서는 애플리케이션 Polling Publisher의 `PENDING → PROCESSING → PUBLISHED` 상태 머신을 그대로 사용하지 않는 경우가 많다.

```
Polling
→ 애플리케이션이 행을 Claim하고 완료 상태 관리

CDC
→ Connector가 Commit Log에서 INSERT를 읽어 전달
```

장점은 다음과 같다.

```
반복 Polling 감소
DB Commit Log 기반의 빠른 전달
애플리케이션 Publisher 코드 감소
Commit 순서를 따르므로 순서 보장에 유리
```

단점은 다음과 같다.

```
CDC Connector 운영 필요
PostgreSQL WAL과 Replication Slot 관리 필요
Connector 장애와 적체 모니터링 필요
별도의 Outbox 정리 정책 필요
```

두 방식 모두 Consumer 멱등성을 준비하는 것이 안전하다.

이번 주차의 기본 구현 모델은 내부 동작을 직접 이해하기 쉬운 Polling Publisher다.

---

## 23. 운영 지표와 알람

Outbox는 발행할 이벤트가 DB 행으로 남기 때문에 적체 상태를 확인할 수 있다.

중요한 지표는 다음과 같다.

```
PENDING 이벤트 수

가장 오래된 PENDING 이벤트의 대기 시간

PROCESSING 이벤트 수

Lease가 만료된 PROCESSING 이벤트 수

발행 성공·실패 횟수

이벤트별 attempt_count

DB 발생 시점부터 Kafka ACK까지 걸린 시간

시간당 Outbox 생성·발행 처리량

처리 중 요청을 제외한 Redis 통과 인원과 DB issued_count의 비수렴 차이
```

특히 다음 상태는 장애 신호가 될 수 있다.

```
PENDING 행이 계속 증가한다.

가장 오래된 PENDING 이벤트가 오래 남아 있다.

같은 이벤트의 attempt_count가 계속 증가한다.

Lease가 만료된 PROCESSING 행이 반복적으로 발생한다.

Outbox 생성 속도보다 발행 속도가 계속 느리다.

deduplication_key UNIQUE 위반이 발생한다.

request = PROCESSING 상태가 관측된다.

SOLD_OUT 또는 DUPLICATE_USER 확정이 반복적으로 발생한다.
```

`deduplication_key` 충돌과 Commit된 `request = PROCESSING` 관측은 설계 가정 위반이므로 즉시 알람 대상으로 둔다.

`SOLD_OUT` 또는 `DUPLICATE_USER`가 반복되는 현상은 Redis와 DB의 기준이 수렴하지 않는 신호일 수 있으므로, 처리 중 요청을 제외한 뒤 원인을 분석한다.

`last_error`에는 전체 Stack Trace나 민감 정보를 그대로 저장하지 않는다.

```
DB
→ 검색 가능한 짧은 오류 분류와 메시지

Log·Trace
→ 상세 Stack Trace와 진단 정보
```

---

## 24. Outbox 행 보관과 삭제

`PUBLISHED` 행을 무기한 보관하면 테이블과 인덱스가 계속 커진다.

일정 기간 보관한 뒤 작은 Batch로 삭제하거나 Archive해야 한다.

```
PUBLISHED
→ 보관 기간 경과
→ 작은 Batch 삭제
```

주의할 점은 다음과 같다.

```
PENDING 삭제 금지
PROCESSING 삭제 금지
발행 성공을 확인하지 못한 행 삭제 금지
```

대량 삭제는 DB 부하와 테이블 Bloat를 만들 수 있으므로 한 번에 모두 삭제하지 않는다.

```
DELETE 1,000건
→ Commit
→ 잠시 대기
→ 반복
```

Consumer가 아직 처리하지 않았더라도 Kafka에 Record가 정상 저장된 뒤라면 이후 전달 책임은 Kafka와 Consumer Group에 있다.

Outbox 보관 기간과 Kafka Topic 보관 기간은 서로 다른 정책이다.

세 가지 보관 기간이 서로 다른 목적을 가진다는 점을 정리하면 다음과 같다.

```
Outbox 보관 기간
→ 발행 이력 확인과 장애 조사

Kafka Topic 보관 기간
→ Consumer 재처리 가능 범위

processed_event 보관 기간
→ 중복 방어 가능 범위
→ Kafka 보관 기간보다 길어야 함 (20-3)
```

---

## 25. 상태별 의미

| 상태 | 의미 |
| --- | --- |
| `request = PENDING` | 발급 요청의 최종 결과가 아직 확정되지 않음 |
| `request = PROCESSING` | 발급 트랜잭션 내부의 중간 상태이며 Commit되지 않음 (11-5) |
| `request = ISSUED` | DB에서 쿠폰 발급이 최종 확정됨 |
| `request = FAILED` | 재시도하지 않는 최종 비즈니스 실패 |
| `outbox = PENDING` | 발행할 결과 이벤트가 DB에 안전하게 기록됨 |
| `outbox = PROCESSING` | Publisher가 이벤트 발행 권한을 가지고 있음 |
| `outbox = PUBLISHED` | Kafka ACK 후 완료 상태까지 DB에 기록됨 |
| Kafka Record 존재 | 결과 이벤트가 Kafka Topic에 저장됨 |
| Consumer Offset Commit | 해당 Consumer Group이 해당 위치까지 처리를 완료했다고 기록함 |
| Notification 완료 | 알림 서비스의 별도 후속 작업이 완료됨 |
| MyPage 반영 완료 | 조회 모델 갱신이 완료됨 |

다음 상태는 같은 의미가 아니다.

```
DB ISSUED
≠
Outbox PUBLISHED
```

```
Outbox PUBLISHED
≠
모든 Consumer 처리 완료
```

```
Consumer Offset Commit
≠
다른 Consumer Group 처리 완료
```

각 Consumer Group은 자신의 Offset과 처리 결과를 독립적으로 관리한다.

---

## 28. 전체 흐름 정리

```
+------------------------------------------+
| Coupon Issue Consumer                    |
|------------------------------------------|
| coupon.issue.requested 수신              |
+------------------------------------------+
                    |
                    v
+------------------------------------------+
| PostgreSQL Transaction                   |
|------------------------------------------|
| request PENDING → PROCESSING             |
| issued_count 조건부 증가                 |
| coupon_issue INSERT                      |
| request → ISSUED                         |
| CouponIssued Outbox INSERT               |
+------------------------------------------+
                    |
                    | Commit
                    v
+------------------------------------------+
| Original Kafka Offset Commit             |
+------------------------------------------+

+------------------------------------------+
| Outbox Poller                            |
|------------------------------------------|
| 만료된 PROCESSING 복구                   |
| 발행 가능한 PENDING 조회                 |
| SKIP LOCKED로 Claim                      |
| claim_token과 lease_until 저장           |
+------------------------------------------+
                    |
                    | Claim Commit
                    v
+------------------------------------------+
| Kafka Producer (동기 발행)               |
|------------------------------------------|
| topic + partitionKey + payload 발행      |
| timeout 내 ACK 확인                      |
+------------------------------------------+
                    |
          +---------+------------------+
          |                            |
          | Broker ACK 성공            | 실패 또는 Timeout
          v                            v
+---------------------------+   +---------------------------+
| Kafka Record 존재         |   | Outbox PENDING            |
| Publisher → PUBLISHED     |   | next_attempt_at 저장      |
+---------------------------+   +---------------------------+
          |
          +-- Notification Consumer
          +-- MyPage Consumer
          +-- Analytics Consumer
                    |
                    v
+------------------------------------------+
| Consumer DB Transaction                  |
|------------------------------------------|
| processed_event INSERT (중복 확인)       |
| aggregateVersion으로 순서 역전 방어      |
| Consumer 비즈니스 데이터 변경            |
+------------------------------------------+
                    |
                    | Commit
                    v
+------------------------------------------+
| Consumer Group Offset Commit             |
+------------------------------------------+
```

---
