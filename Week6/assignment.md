# **6주차 과제: Transactional Outbox Pattern과 이벤트 발행 정합성**

## **1. 과제 목표**

이번 과제의 목표는 **쿠폰 발급 결과를 PostgreSQL에 저장하는 작업과 `CouponIssued` 이벤트를 Kafka에 발행하는 작업 사이의 Dual Write 문제를 분석하고, Transactional Outbox Pattern으로 복구 가능한 이벤트 발행 구조를 설계하는 것**이다.

5주차까지 쿠폰 발급 Consumer는 다음 작업을 하나의 DB 트랜잭션으로 처리했다.

```
coupon_issue_request PENDING → PROCESSING

coupon_event.issued_count 증가

coupon_issue 발급 기록 저장

coupon_issue_request → ISSUED
```

DB 트랜잭션이 Commit된 시점을 최종 쿠폰 발급 성공으로 본다.

하지만 발급 결과를 Notification, MyPage, Analytics 같은 후속 서비스에 전달하려면 별도의 결과 이벤트가 필요하다.

```
PostgreSQL
→ 쿠폰 발급 결과 저장

Kafka
→ CouponIssued 이벤트 발행
```

PostgreSQL과 Kafka는 서로 다른 시스템이다.

하나의 메서드 안에서 두 작업을 호출하거나 DB 메서드에 `@Transactional`을 붙이는 것만으로는 두 시스템이 하나의 원자적 트랜잭션이 되지 않는다.

이번 과제에서는 다음 구조를 설계한다.

```
Coupon Issue Consumer
        ↓
+------------------------------------------+
| PostgreSQL Transaction                   |
|------------------------------------------|
| coupon_issue 저장                        |
| request ISSUED 변경                      |
| CouponIssued Outbox 저장                 |
+------------------------------------------+
        ↓ Commit
Outbox Publisher
        ↓
Kafka coupon.issued
        ↓
Result Consumer
```

이번 과제에서 중요하게 볼 내용은 다음과 같다.

```
PostgreSQL과 Kafka 사이의 Dual Write 문제

DB First와 Kafka First가 각각 실패하는 이유

AFTER_COMMIT만으로 이벤트 유실을 막을 수 없는 이유

비즈니스 결과와 Outbox를 같은 트랜잭션에 저장하는 이유

Outbox 이벤트 Envelope와 테이블 설계

PENDING / PROCESSING / PUBLISHED 상태의 의미

FOR UPDATE SKIP LOCKED를 이용한 동시 Claim

Claim Token과 Lease가 필요한 이유

Kafka ACK와 메서드 호출 성공의 차이

Timeout과 재시도에서 발생하는 중복 발행

At-Least-Once Publication과 Consumer 멱등 처리

eventMessageId와 Kafka Offset의 차이

Partition Key와 이벤트 순서 보장의 차이

aggregateVersion의 역할과 적용 범위

Outbox 적체를 감지하기 위한 운영 지표
```

이번 과제의 핵심은 다음과 같다.

```
Outbox는 Kafka Record를 정확히 한 번 생성하는 패턴이 아니다.

Outbox는 DB 결과가 Commit되었을 때
발행할 이벤트 기록도 같은 DB에 반드시 남도록 만드는 패턴이다.

Publisher는 남아 있는 이벤트를 재시도할 수 있다.

재시도로 중복 발행이 발생할 수 있으므로
Consumer도 멱등하게 처리해야 한다.
```

---

## **2. 기본 상황**

서비스에서 선착순 쿠폰 이벤트를 진행한다고 가정한다.

```
이벤트 ID: 100

이벤트 이름:
여름맞이 5,000원 할인 쿠폰 이벤트

전체 쿠폰 수량:
1,000개

발급 조건:
사용자 1명당 해당 이벤트 쿠폰 1개

DBMS:
PostgreSQL

메시지 브로커:
Kafka

원본 요청 Topic:
coupon.issue.requested

결과 이벤트 Topic:
coupon.issued
```

쿠폰 발급 성공 기준은 다음과 같다.

```
coupon_event.issued_count 증가

coupon_issue 발급 기록 저장

coupon_issue_request ISSUED 변경

CouponIssued Outbox 저장

위 작업을 포함한 PostgreSQL Transaction Commit
```

발급 결과 이벤트는 다음 후속 서비스가 소비한다고 가정한다.

```
Notification Consumer
→ 쿠폰 발급 알림 처리

MyPage Consumer
→ 사용자 쿠폰 조회 모델 갱신

Analytics Consumer
→ 쿠폰 발급 통계 반영
```

기본 이벤트 Envelope는 다음 필드를 가진다.

```json
{
  "eventMessageId": "UUID",
  "eventType": "COUPON_ISSUED",
  "schemaVersion": 1,
  "aggregateVersion": 1,
  "occurredAt": "2026-07-26T03:00:01Z",
  "requestId": "UUID",
  "data": {
    "couponIssueId": 5001,
    "couponEventId": 100,
    "userId": 10,
    "issuedAt": "2026-07-26T03:00:01Z"
  }
}
```

---

## **3. 이번 과제의 전제 조건**

이번 과제에서는 다음 전제를 따른다.

```
쿠폰 발급 비즈니스 데이터와 Outbox는
같은 PostgreSQL Database에 저장한다.

coupon_issue_request,
coupon_event,
coupon_issue,
outbox_event는
필요한 경우 하나의 로컬 DB 트랜잭션에 포함할 수 있다.

원본 coupon.issue.requested 메시지는
5주차의 requestId 기반 멱등 처리로 중복을 방어한다.

자동 Offset Commit은 사용하지 않는다.

원본 Consumer는 발급 DB 트랜잭션이 Commit된 뒤에만
처리 완료로 간주한다.

Spring Kafka의 실제 Offset Commit 시점은
설정한 AckMode와 Error Handler 정책에 맞게 구성한다.

Outbox Publisher는 Polling 방식으로 구현한다.

이번 주차의 기본 Publisher는
작은 Batch를 순차 동기 방식으로 발행한다.

Kafka 발행은 Claim 트랜잭션 밖에서 수행한다.

Publisher는 Kafka Broker ACK를 확인한 뒤
Outbox를 PUBLISHED로 변경한다.

Publisher는 같은 Outbox 이벤트를
두 번 이상 Kafka에 발행할 수 있다.

Consumer는 eventMessageId를 기준으로
중복 비즈니스 반영을 방어한다.
```

이번 과제의 Outbox가 직접 해결하는 범위는 다음과 같다.

```
PostgreSQL에 확정되는 쿠폰 발급 성공

+

후속 서비스에 전달할 CouponIssued 발행 의도
```

다음 구간은 이번 Outbox가 자동으로 원자화하지 않는다.

```
Redis 선착순 판정
↔ 최초 coupon.issue.requested Kafka 발행

원본 coupon.issue.requested Kafka 소비
↔ PostgreSQL 발급 트랜잭션

Result Consumer의 로컬 DB 저장
↔ 이메일·SMS 같은 외부 API 호출
```

이번 과제에서는 아래 내용의 상세 구현은 다루지 않는다.

```
Kafka Transaction을 이용한 전체 구조

XA 또는 2PC

Debezium Connector 실제 설치

최대 재시도 이후 DLQ 운영

영구 실패 이벤트의 운영자 재처리 UI

Redis와 PostgreSQL의 보정 배치 상세 구현
```

---

## **4. 제출 형식**

제출 파일은 아래 경로에 작성해서 PR을 생성한다.

```
submissions/Week6/기수_이름.md
```

예시는 다음과 같다.

```
submissions/Week6/13기_김기민.md
```

제출 문서에는 아래 필수 과제를 모두 포함한다.

```
과제 1. Dual Write 문제와 Outbox 적용 범위 분석하기

과제 2. CouponIssued 이벤트와 Outbox 테이블 설계하기

과제 3. 발급 결과와 Outbox를 하나의 트랜잭션으로 저장하기

과제 4. Polling Publisher의 Claim, Lease, Claim Token 설계하기

과제 5. Kafka ACK, Timeout, 재시도 상태 전이 설계하기

과제 6. Outbox 장애 시나리오 분석하기

과제 7. Consumer 멱등 처리 설계하기

과제 8. Partition Key와 이벤트 순서 문제 분석하기

과제 9. Outbox 운영 지표와 보관 정책 설계하기

과제 10. 잘못된 Outbox 구현 리뷰하기
```

모든 답변에는 단순 결론뿐 아니라 **그 상태가 만들어지는 장애 지점과 복구 기준**을 함께 작성한다.

---

# **5. 필수 과제**

---

## **과제 1. Dual Write 문제와 Outbox 적용 범위 분석하기**

쿠폰 발급 결과를 DB에 저장하는 작업과 Kafka에 결과 이벤트를 발행하는 작업을 하나의 메서드에서 직접 수행한다고 가정한다.

```java
@Transactional
public void issueCoupon(IssueMessage message) {
    increaseIssuedCount(message.eventId());

    CouponIssue issue = saveCouponIssue(message);

    markRequestIssued(
        message.requestId(),
        issue.getId()
    );

    kafkaTemplate.send(
        "coupon.issued",
        issue.getId().toString(),
        CouponIssuedEvent.from(message, issue)
    );
}
```

### **1. 위 코드가 사용하는 저장소 두 개를 작성한다**

```

```

### **2. `@Transactional`만으로 두 저장소가 원자적으로 묶이지 않는 이유를 설명한다**

```

```

### **3. DB를 먼저 Commit한 뒤 Kafka에 발행하는 흐름의 장애 구간을 작성한다**

다음 형식을 완성한다.

```
DB Commit
    ↓
[장애 지점]
    ↓
Kafka 발행

최종 DB 상태:

최종 Kafka 상태:

후속 서비스에 미치는 영향:

복구에 필요한 정보:
```

### **4. Kafka를 먼저 발행한 뒤 DB에 저장하는 흐름의 장애 구간을 작성한다**

```
Kafka 발행
    ↓
[장애 지점]
    ↓
DB Commit

최종 DB 상태:

최종 Kafka 상태:

후속 서비스에 미치는 영향:
```

### **5. `AFTER_COMMIT` Listener만으로 이벤트 유실을 해결할 수 없는 이유를 설명한다**

다음 두 장애를 반드시 포함한다.

```
DB Commit 후 Listener 실행 전 프로세스 종료

KafkaTemplate.send() 호출 후
Broker ACK 확인 전 프로세스 종료
```

```

```

### **6. 이번 과제에서 Outbox가 해결하는 범위와 해결하지 않는 범위를 구분한다**

| 구간 | 이번 Outbox로 해결되는가? | 이유 |
| --- | --- | --- |
| coupon_issue 저장 ↔︎ CouponIssued 발행 의도 저장 |  |  |
| Redis SUCCESS ↔︎ 최초 요청 Kafka 발행 |  |  |
| 원본 Kafka 소비 ↔︎ 발급 DB Commit |  |  |
| Consumer DB 저장 ↔︎ 외부 SMS API 호출 |  |  |

### **7. 다음 문장을 완성한다**

```
Transactional Outbox가 직접 보장하는 것:

Transactional Outbox가 직접 보장하지 않는 것:
```

---

## **과제 2. CouponIssued 이벤트와 Outbox 테이블 설계하기**

### **1. `CouponIssued`가 명령이 아니라 이미 발생한 사실을 나타내야 하는 이유를 설명한다**

다음 두 이름의 의미 차이를 포함한다.

```
IssueCoupon

CouponIssued
```

```

```

### **2. CouponIssued 이벤트 JSON을 완성한다**

반드시 다음 필드를 포함한다.

```
eventMessageId
eventType
schemaVersion
aggregateVersion
occurredAt
requestId
couponIssueId
couponEventId
userId
issuedAt
```

```json
{

}
```

### **3. 다음 ID의 역할을 비교한다**

| 값 | 식별 대상 | 재발행 시 값이 바뀌는가? | Consumer 멱등성 Key로 적절한가? |
| --- | --- | --- | --- |
| couponEventId |  |  |  |
| couponIssueId |  |  |  |
| eventMessageId |  |  |  |
| Kafka Partition + Offset |  |  |  |

### **4. `occurredAt`과 `publishedAt`의 차이를 설명한다**

```
occurredAt:

publishedAt:

Publisher 재시도 시 변경하면 안 되는 값:
```

### **5. Publisher가 발행 시점에 현재 비즈니스 테이블을 다시 조회해 Payload를 만들면 안 되는 이유를 설명한다**

```

```

### **6. 다음 Outbox 테이블 DDL을 완성한다**

아래 요구사항을 만족해야 한다.

```
event_id는 UUID Primary Key

같은 비즈니스 결과의 중복 Outbox 생성을 막는
deduplication_key UNIQUE

Aggregate 식별 정보 저장

Topic, Partition Key, Payload 저장

PENDING / PROCESSING / PUBLISHED 상태만 허용

재시도 횟수와 다음 재시도 시각 저장

Claim Token과 Lease 저장

발생 시각과 발행 완료 시각 저장
```

```sql
CREATE TABLE outbox_event (

);
```

### **7. 다음 불변조건을 애플리케이션 또는 DB에서 어떻게 검증할지 작성한다**

```
outbox_event.event_id
=
payload.eventMessageId
```

```

```

### **8. `deduplication_key`를 설계한다**

`CouponIssued`는 하나의 `couponIssueId`에 대해 한 번만 발생한다고 가정한다.

```
Key 형식:

예시:
```

같은 Aggregate에서 같은 이벤트 유형이 여러 번 발생할 수 있다면 어떤 값을 추가해야 하는지도 작성한다.

```

```

### **9. 일반 INSERT에서 `deduplication_key` UNIQUE 위반이 발생한 뒤 기존 행을 확인하려면 어떤 트랜잭션 처리가 필요한지 설명한다**

다음 선택지를 비교한다.

```
전체 Rollback 후 새 트랜잭션에서 조회

Savepoint 사용

ON CONFLICT DO NOTHING RETURNING 후
기존 행 조회·동일성 비교
```

```

```

---

## **과제 3. 발급 결과와 Outbox를 하나의 트랜잭션으로 저장하기**

쿠폰 발급 성공 시 다음 작업을 하나의 DB 트랜잭션으로 수행한다.

```
1. 요청 처리 권한 획득

2. 수량 확보

3. coupon_issue 저장

4. request ISSUED 확정

5. CouponIssued Outbox 저장
```

### **1. 전체 SQL 트랜잭션을 완성한다**

```sql
BEGIN;

-- 1. PENDING 요청에 대한 처리 권한 획득

-- affected rows가 몇 행이어야 하는가?

-- 2. 수량이 남아 있을 때만 issued_count 증가

-- affected rows가 0일 때 가능한 원인은 무엇인가?

-- 3. coupon_issue 저장 후 id와 issued_at 획득

-- 4. request를 ISSUED로 변경

-- 5. 동일한 트랜잭션에서 CouponIssued Outbox 저장

COMMIT;
```

### **2. 각 단계에서 확인해야 하는 영향받은 행 수를 작성한다**

| 작업 | 성공 경로의 영향받은 행 수 | 예상과 다를 때 처리 |
| --- | --- | --- |
| PENDING → PROCESSING |  |  |
| issued_count 조건부 증가 |  |  |
| coupon_issue INSERT |  |  |
| PROCESSING → ISSUED |  |  |
| outbox_event INSERT |  |  |

### **3. Outbox INSERT가 실패한 경우 최종 상태를 작성한다**

장애 직전 상황은 다음과 같다.

```
issued_count 증가 성공

coupon_issue INSERT 성공

request ISSUED 변경 성공

outbox_event INSERT 실패
```

```
트랜잭션 결과:

coupon_event.issued_count:

coupon_issue:

coupon_issue_request:

outbox_event:
```

### **4. Outbox 저장 실패를 무시하고 발급 결과만 Commit하면 안 되는 이유를 설명한다**

```

```

### **5. 원본 메시지가 재전달되었지만 request가 이미 ISSUED인 경우의 처리를 작성한다**

```
issued_count를 다시 증가시키는가?

coupon_issue를 다시 생성하는가?

Outbox를 다시 INSERT하는가?

Consumer 처리는 성공으로 종료할 수 있는가?
```

### **6. 발급 DB Commit 후 원본 Kafka Offset Commit 전에 Consumer가 종료되는 상황을 분석한다**

```
DB 상태:

Outbox 상태:

Kafka 원본 메시지:

재시작 후 필요한 멱등성 기준:
```

### **7. 원본 Consumer의 Offset Commit 설정에서 확인해야 할 항목을 작성한다**

다음을 반드시 포함한다.

```
enable.auto.commit

AckMode

Listener 예외 처리

Error Handler가 실패를 정상 처리로 바꾸는지 여부
```

```

```

---

## **과제 4. Polling Publisher의 Claim, Lease, Claim Token 설계하기**

여러 Publisher 인스턴스가 같은 Outbox 테이블을 Polling한다고 가정한다.

### **1. 단순 SELECT 후 UPDATE 방식의 Race Condition을 작성한다**

```
Publisher A                    Publisher B
------------------------------------------------
```

### **2. `FOR UPDATE SKIP LOCKED`를 이용한 Claim SQL을 완성한다**

다음 요구사항을 만족해야 한다.

```
status = PENDING

next_attempt_at이 현재 시각 이하

오래된 이벤트부터 조회

Batch 크기 제한

잠긴 행은 대기하지 않고 건너뜀

PROCESSING 상태로 변경

claim_token과 lease_until 저장

attempt_count 증가

Claim한 행 반환
```

```sql
BEGIN;

WITH candidates AS (

)
UPDATE outbox_event AS outbox
SET

FROM candidates
WHERE

RETURNING outbox.*;

COMMIT;
```

### **3. Claim 트랜잭션을 짧게 Commit한 뒤 Kafka를 호출해야 하는 이유를 설명한다**

DB Row Lock을 잡은 채 Kafka ACK를 기다릴 때의 문제를 최소 세 가지 작성한다.

```
1.

2.

3.
```

### **4. 단일 Publisher만 있어도 Lease 또는 stale PROCESSING 복구가 필요한 이유를 설명한다**

```

```

### **5. 만료된 PROCESSING 행을 PENDING으로 복구하는 SQL을 작성한다**

```sql

```

### **6. 다음 경쟁 상황에서 Claim Token이 없을 때 발생하는 문제를 설명한다**

```
Publisher A
→ token-A로 Claim
→ 처리 지연
→ Lease 만료

Publisher B
→ 만료 행 회수
→ token-B로 Claim

Publisher A
→ 뒤늦게 성공 또는 실패 상태 UPDATE
```

```

```

### **7. 성공과 실패 UPDATE에 들어가야 하는 공통 WHERE 조건을 작성한다**

```sql
WHERE
```

### **8. 동기 순차 발행 모델에서 Lease 시간을 계산할 때 고려할 항목을 작성한다**

```
Lease 시간
>

+
```

---

## **과제 5. Kafka ACK, Timeout, 재시도 상태 전이 설계하기**

### **1. 다음 네 상태의 의미 차이를 설명한다**

```
KafkaTemplate.send() 호출 완료:

CompletableFuture 정상 완료:

outbox = PUBLISHED:

모든 후속 Consumer 처리 완료:
```

### **2. Kafka ACK 성공 후 PUBLISHED로 변경하는 SQL을 작성한다**

다음 조건을 반드시 포함한다.

```
event_id 일치

status = PROCESSING

claim_token 일치
```

```sql

```

### **3. 발행 실패 후 PENDING으로 되돌리는 SQL을 작성한다**

다음 값을 함께 갱신한다.

```
next_attempt_at

claim_token

lease_until

last_error

updated_at
```

```sql

```

### **4. Timeout을 받았다고 Kafka에 Record가 없다고 단정할 수 없는 이유를 설명한다**

다음 두 상황을 구분한다.

```
상황 A:
Broker에 저장되지 못하고 Timeout

상황 B:
Broker에는 저장되었지만
Publisher가 ACK를 확인하지 못하고 Timeout
```

```

```

### **5. Timeout 이후 재시도 정책을 작성한다**

```
Outbox 상태:

재시도 여부:

Kafka 중복 가능성:

Consumer에게 필요한 방어:
```

### **6. Exponential Backoff와 Jitter를 사용하는 이유를 설명한다**

```
Exponential Backoff:

Jitter:
```

### **7. Producer Idempotence가 해결하는 중복과 해결하지 못하는 중복을 구분한다**

| 중복 발생 원인 | Producer Idempotence로 방어 가능한가? | 이유 |
| --- | --- | --- |
| Producer 내부 네트워크 재시도 |  |  |
| Kafka 발행 성공 후 PUBLISHED 변경 전 프로세스 종료 |  |  |
| 운영자가 같은 Outbox를 다시 PENDING으로 변경 |  |  |

### **8. 비동기 발행으로 확장할 때 `whenComplete` 안에서 DB 작업을 직접 수행하는 것이 위험한 이유를 설명한다**

콜백이 Producer I/O 스레드에서 실행될 수 있다는 점과 전용 Executor의 필요성을 포함한다.

```

```

---

## **과제 6. Outbox 장애 시나리오 분석하기**

각 장애 시나리오에 대해 다음을 작성한다.

```
DB의 비즈니스 상태

Outbox 상태

Kafka Record 존재 가능성

복구 주체

중복 가능성
```

### **1. 발급 트랜잭션 Commit 전 장애**

| 항목 | 결과 |
| --- | --- |
| coupon_issue |  |
| request |  |
| outbox_event |  |
| 복구 방법 |  |

### **2. 발급 DB Commit 후 원본 Offset Commit 전 장애**

| 항목 | 결과 |
| --- | --- |
| coupon_issue |  |
| request |  |
| outbox_event |  |
| 원본 Kafka 메시지 |  |
| 복구 방법 |  |

### **3. Outbox Claim Commit 후 Kafka 발행 전 Publisher 종료**

| 항목 | 결과 |
| --- | --- |
| outbox status |  |
| Kafka Record |  |
| 복구 조건 |  |
| 복구 주체 |  |

### **4. Kafka 발행 성공 후 PUBLISHED 변경 전 Publisher 종료**

| 항목 | 결과 |
| --- | --- |
| outbox status |  |
| Kafka Record |  |
| 재발행 가능성 |  |
| 필요한 Consumer 방어 |  |

### **5. Publisher A의 Lease 만료 후 Publisher B가 회수한 상황**

```
Publisher A가 늦게 성공 UPDATE:

Publisher A가 늦게 실패 UPDATE:

두 UPDATE에서 affected rows가 0이어야 하는 조건:
```

### **6. Kafka 장애가 30분 동안 지속되는 상황**

```
PENDING 수의 변화:

가장 오래된 PENDING 대기 시간:

attempt_count:

Kafka 복구 후 처리:

운영 알람:
```

### **7. 다음 두 재시도 대상을 구분한다**

```
발급 DB 트랜잭션 실패:

발급 DB Commit 성공 후 CouponIssued 발행 실패:
```

결과 이벤트 발행 실패를 이유로 쿠폰을 다시 발급하면 안 되는 이유도 작성한다.

```

```

---

## **과제 7. Consumer 멱등 처리 설계하기**

같은 논리 이벤트가 다음처럼 두 번 Kafka에 저장되었다고 가정한다.

```
Partition 1 / Offset 100 / eventMessageId = evt-001

Partition 1 / Offset 101 / eventMessageId = evt-001
```

### **1. Kafka Offset을 Consumer 멱등성 Key로 사용하면 안 되는 이유를 설명한다**

```

```

### **2. processed_event 테이블 DDL을 작성한다**

다음 요구사항을 만족해야 한다.

```
논리 Subscriber 식별자 저장

eventMessageId 저장

처리 완료 시각 저장

같은 Subscriber가 같은 이벤트를
두 번 처리하지 못하도록 Primary Key 구성
```

```sql
CREATE TABLE processed_event (

);
```

### **3. `subscriber_id`에 프로세스 인스턴스 ID를 사용하면 안 되는 이유를 설명한다**

다음 중 적절한 값을 선택하고 이유를 작성한다.

```
pod-7f8c9d

server-10.0.0.12

mypage-coupon-projection
```

```

```

### **4. processed_event 저장과 MyPage 조회 모델 변경을 하나의 트랜잭션으로 작성한다**

```sql
BEGIN;

-- 1. eventMessageId 중복 확인

-- 2. 최초 이벤트인 경우에만 조회 모델 변경

COMMIT;
```

### **5. Consumer DB Commit 후 Offset Commit 전에 종료되는 상황을 분석한다**

```
Consumer DB 상태:

Consumer Group Offset:

재시작 후 Kafka 전달:

재전달된 이벤트 처리:
```

### **6. `processed_event`만으로 외부 SMS가 정확히 한 번 전송된다고 보장할 수 없는 이유를 설명한다**

```

```

### **7. Notification Service에서 추가로 적용할 수 있는 방법을 두 가지 이상 작성한다**

```
1.

2.

3.
```

### **8. processed_event 보관 기간을 정할 때 고려할 중복 재등장 경로를 작성한다**

다음을 포함한다.

```
Kafka Topic 보관 기간

Consumer Offset 되감기

Outbox 지연 재발행

운영자 수동 재발행
```

```

```

---

## **과제 8. Partition Key와 이벤트 순서 문제 분석하기**

이번 과제에서는 다음 값을 사용한다고 가정한다.

```
aggregateType = COUPON_ISSUE

aggregateId = couponIssueId

partitionKey = couponIssueId
```

### **1. 같은 Partition Key를 사용하는 이유를 작성한다**

```

```

### **2. 같은 Key를 사용한다고 비즈니스 발생 순서까지 보장되는 것은 아닌 이유를 설명한다**

다음을 반드시 포함한다.

```
여러 Publisher 인스턴스

SKIP LOCKED

발행 실패와 Backoff

Producer 설정과 재시도
```

```

```

### **3. 다음 순서 역전 시나리오를 완성한다**

```
DB 발생 순서

CouponIssued
aggregateVersion = 1

CouponCancelled
aggregateVersion = 2

Publisher 처리

Publisher A:

Publisher B:

Kafka 저장 순서:
```

### **4. `occurredAt`만으로 이벤트 순서를 판단하면 안 되는 이유를 두 가지 이상 작성한다**

```
1.

2.

3.
```

### **5. MyPage처럼 최신 상태만 필요한 Projection의 순서 역전 방어 SQL을 작성한다**

다음 컬럼을 사용한다.

```
coupon_issue_id

status

last_aggregate_version

last_event_at
```

```sql

```

### **6. aggregateVersion이 더 큰 이벤트가 먼저 도착한 경우의 결과를 작성한다**

```
먼저 도착:
CouponCancelled(version=2)

나중 도착:
CouponIssued(version=1)

최종 MyPage 상태:

version=1 처리 결과:
```

### **7. 모든 Consumer가 낮은 aggregateVersion 이벤트를 버려도 되는지 판단한다**

다음 Consumer별로 답한다.

| Consumer | 낮은 Version 이벤트 생략 가능 여부 | 이유 또는 추가로 필요한 설계 |
| --- | --- | --- |
| 최신 상태만 저장하는 MyPage Projection |  |  |
| 모든 발급·취소 건수를 집계하는 Analytics |  |  |
| 발급과 취소 알림을 각각 보내는 Notification |  |  |

### **8. `aggregateVersion`의 역할을 정확히 정의한다**

다음 문장을 완성한다.

```
aggregateVersion은 이벤트 순서 역전을
자동으로 ____________________ 값이 아니다.

aggregateVersion은 같은 Aggregate에서 발생한 상태 변경의
____________________ 을 식별하기 위한 값이다.

최신 상태 Projection에서는 낮은 버전이
최신 상태를 ____________________ 것을 방지할 수 있다.

모든 상태 전이를 순서대로 처리해야 하는 Consumer는
추가로 ____________________ 이 필요하다.
```

---

## **과제 9. Outbox 운영 지표와 보관 정책 설계하기**

### **1. 다음 운영 지표의 의미와 장애 판단 기준을 작성한다**

| 지표 | 무엇을 의미하는가? | 어떤 상태가 장애 신호인가? |
| --- | --- | --- |
| PENDING 이벤트 수 |  |  |
| 가장 오래된 PENDING 대기 시간 |  |  |
| PROCESSING 이벤트 수 |  |  |
| 만료된 PROCESSING 수 |  |  |
| attempt_count |  |  |
| Outbox 생성 처리량 |  |  |
| Kafka 발행 처리량 |  |  |
| DB 발생부터 Kafka ACK까지의 시간 |  |  |

### **2. Outbox 생성 속도가 초당 2,000건이고 Publisher 처리 속도가 초당 1,500건인 상황을 분석한다**

```
1분 후 예상 적체 증가량:

10분 후 예상 적체 증가량:

가장 먼저 확인할 지표:

가능한 대응:
```

### **3. 즉시 알람이 필요한 설계 가정 위반을 세 가지 이상 작성한다**

```
1.

2.

3.

4.
```

### **4. PUBLISHED 행만 정리해야 하는 이유를 설명한다**

```
PENDING을 삭제하면:

PROCESSING을 삭제하면:

PUBLISHED를 보관하는 목적:
```

### **5. PUBLISHED 행을 한 번에 대량 삭제하면 안 되는 이유를 설명한다**

다음 항목을 포함한다.

```
긴 트랜잭션

DB 부하

Lock

Table Bloat
```

```

```

### **6. 작은 Batch로 정리하는 운영 흐름을 작성한다**

```

```

### **7. 다음 세 보관 기간의 목적을 비교한다**

| 보관 대상 | 목적 | 너무 짧을 때 발생하는 문제 |
| --- | --- | --- |
| Outbox PUBLISHED 행 |  |  |
| Kafka Topic Record |  |  |
| Consumer processed_event |  |  |

### **8. `last_error`에 전체 Stack Trace나 민감 정보를 저장하면 안 되는 이유를 설명한다**

```

```

---

## **과제 10. 잘못된 Outbox 구현 리뷰하기**

다음은 의도적으로 문제가 포함된 구현이다.

```java
@Transactional
public void issueCoupon(IssueMessage message) {
    CouponIssue issue = issueRepository.save(
        CouponIssue.from(message)
    );

    kafkaTemplate.send(
        "coupon.issued",
        issue.getId().toString(),
        CouponIssuedEvent.from(message, issue)
    );
}

@Scheduled(fixedDelay = 1000)
@Transactional
public void publishOutbox() {
    List<OutboxEvent> events =
        outboxRepository.findTop100ByStatus("PENDING");

    for (OutboxEvent event : events) {
        kafkaTemplate.send(
            event.getTopic(),
            event.getPartitionKey(),
            createPayloadFromCurrentCouponIssue(event)
        );

        event.markPublished();
    }
}

public void recoverProcessing() {
    outboxRepository.updateAllProcessingToPending();
}

public void onResult(CouponIssuedEvent event) {
    userCouponViewRepository.save(
        UserCouponView.from(event)
    );

    processedEventRepository.save(
        new ProcessedEvent(
            event.getKafkaPartition(),
            event.getKafkaOffset()
        )
    );
}
```

### **1. 위 코드에서 문제를 최소 10개 찾는다**

각 문제마다 다음 형식으로 작성한다.

```
문제 위치:

문제 내용:

발생 가능한 장애:

수정 방법:
```

### **2. 반드시 찾아야 하는 문제**

다음 항목이 코드의 어느 부분과 연결되는지 작성한다.

| 검토 항목 | 문제가 발생하는 코드 | 수정 방향 |
| --- | --- | --- |
| 발급 트랜잭션에서 Kafka 직접 호출 |  |  |
| 발급 결과와 Outbox의 원자적 저장 없음 |  |  |
| 여러 Publisher의 동시 Claim 방어 없음 |  |  |
| DB 트랜잭션 안에서 Kafka ACK 대기 또는 발행 수행 |  |  |
| ACK 확인 전에 PUBLISHED 변경 |  |  |
| Claim Token 없음 |  |  |
| Lease 만료 조건 없이 모든 PROCESSING 회수 |  |  |
| 현재 비즈니스 테이블에서 Payload 재생성 |  |  |
| Kafka Offset을 멱등성 Key로 사용 |  |  |
| processed_event와 비즈니스 변경의 트랜잭션 순서·원자성 부족 |  |  |

### **3. 수정된 전체 구조를 다이어그램으로 작성한다**

반드시 다음 구성 요소를 포함한다.

```
Coupon Issue Consumer

발급 DB Transaction

outbox_event

Claim Transaction

Kafka 발행 및 ACK

성공·실패 상태 Transaction

Kafka Result Consumer

processed_event

Consumer Business Table
```

```

```

### **4. 수정된 구조의 발행 보장 수준을 작성한다**

다음 항목별로 `보장`, `조건부 보장`, `보장하지 않음` 중 하나를 선택하고 이유를 작성한다.

| 항목 | 수준 | 이유 |
| --- | --- | --- |
| DB 발급 결과와 Outbox 기록의 원자성 |  |  |
| Kafka Record의 물리적인 Exactly-Once 생성 |  |  |
| Publisher 복구 후 재발행 |  |  |
| 비즈니스 발생 순서대로 전달 |  |  |
| Consumer의 중복 비즈니스 반영 방지 |  |  |
| 모든 Consumer Group의 처리 완료 |  |  |
| 외부 SMS의 정확히 한 번 전송 |  |  |

---
