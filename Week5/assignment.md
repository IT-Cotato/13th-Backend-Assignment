# **5주차 과제**

# **requestId 기반 멱등성과 중복 처리 구조 설계**

> 모든 답변은 각 문항 아래의 코드블록 안에 작성한다. 표 형식 문항도 코드블록 안의 표를 수정해 제출한다.
> 

## **1. 과제 목표**

이번 5주차 과제의 목표는 **같은 API 요청이나 Kafka 메시지가 여러 번 전달되더라도 최종 쿠폰 발급 결과가 한 번만 반영되도록 멱등 처리 구조를 설계하는 것**이다.

1주차에서는 요청 접수와 실제 발급 처리를 분리했다.

2주차에서는 Kafka를 이용해 쿠폰 발급 요청을 비동기적으로 전달했다.

3주차에서는 Redis를 이용해 사용자 중복 요청과 수량 초과 요청을 빠르게 차단했다.

4주차에서는 DB의 조건부 `UPDATE`, `UNIQUE(event_id, user_id)`, Transaction을 이용해 최종 수량과 사용자 중복 발급을 방어했다.

이번 5주차에서는 다음 문제를 추가로 해결한다.

```
같은 논리적인 요청이 여러 번 전달되었는가?

같은 사용자가 새로운 요청을 다시 보낸 것인가?

Consumer가 이미 성공한 메시지를 다시 읽은 것인가?

요청이 아직 처리 중인가?

이전에 성공하거나 실패했다면 어떤 결과를 다시 반환해야 하는가?
```

기본 흐름은 다음과 같다.

```
Client
  |
  | Idempotency-Key: requestId
  v
API Server
  |
  | 인증된 userId 확인
  | requestHash 생성
  v
Redis
  |
  | 사용자 + requestId 기준 판정
  v
coupon_issue_request
  |
  | PENDING 요청 상태 저장
  v
Kafka
  |
  | requestId가 포함된 메시지 전달
  v
Coupon Issue Consumer
  |
  | requestId 기준 처리 권한 획득
  v
DB Transaction
  |
  | 수량 증가
  | 발급 기록 저장
  | 요청 상태 확정
  v
Kafka Offset Commit
```

이번 과제에서 중요하게 볼 내용은 다음과 같다.

```
Idempotency-Key와 requestId의 역할

같은 요청과 같은 사용자의 차이

Kafka Producer 멱등성과 비즈니스 멱등성의 차이

requestId와 Kafka Partition + Offset의 차이

requestHash를 이용한 잘못된 키 재사용 방지

coupon_issue_request 요청 상태 테이블 설계

PENDING / PROCESSING / ISSUED / FAILED 상태 전이

동일 requestId 동시 요청의 최초 등록자 결정

Consumer의 조건부 상태 전이와 처리 권한 획득

발급 수량, 발급 기록, 요청 상태를 묶는 DB Transaction

UNIQUE 위반 Rollback 이후 상태 확정

DB Commit과 Kafka Offset Commit의 순서

중복 메시지를 예외가 아니라 멱등 성공으로 처리하는 방법

Redis, DB, Kafka 사이의 일시적 불일치와 복구 방향
```

이번 과제의 핵심은 다음과 같다.

```
중복 전달을 완전히 없애는 것이 목표가 아니다.

같은 요청이 여러 번 전달되더라도
최종 비즈니스 결과가 한 번만 반영되게 만드는 것이 목표다.
```

---

## **2. 기본 상황**

서비스에서 다음 선착순 쿠폰 이벤트를 진행한다고 가정한다.

```
이벤트 ID: 100

이벤트 이름: 여름맞이 5,000원 할인 쿠폰 이벤트

전체 쿠폰 수량: 1,000개

발급 조건:
사용자 1명당 해당 이벤트 쿠폰 1개만 발급 가능

DBMS: PostgreSQL

DB 격리 수준: READ COMMITTED

최종 발급 성공 기준:
coupon_event.issued_count 증가,
coupon_issue 저장,
coupon_issue_request ISSUED 변경이
하나의 트랜잭션으로 Commit된 시점
```

사용자 `userId = 10`이 다음 요청을 보낸다.

```
POST /api/coupon-events/100/issue
Authorization: Bearer ...
Idempotency-Key: 4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31
```

서버 내부에서는 `Idempotency-Key`를 `requestId`로 사용한다.

```json
{
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "eventId": 100,
  "userId": 10,
  "requestedAt": "2026-07-24T12:00:00+09:00"
}
```

하지만 실제 시스템에서는 다음 상황이 발생할 수 있다.

```
상황 1.
서버는 요청을 처리했지만 응답만 유실되어
Client가 같은 requestId로 재시도한다.

상황 2.
사용자가 버튼을 여러 번 눌러
서로 다른 requestId로 같은 쿠폰을 다시 요청한다.

상황 3.
애플리케이션 코드가 같은 메시지를 send()로 두 번 발행한다.

상황 4.
Consumer가 DB Commit 후 Offset Commit 전에 종료되어
같은 Kafka Record를 다시 읽는다.

상황 5.
원본 Topic의 메시지가 Retry Topic으로 이동하여
Partition과 Offset은 달라졌지만 같은 requestId가 다시 전달된다.

상황 6.
같은 requestId가 다른 eventId 또는 다른 userId 요청에 재사용된다.
```

이 모든 상황을 단순히 `UNIQUE(event_id, user_id)` 하나로만 처리할 수는 없다.

```
UNIQUE(event_id, user_id)
→ 같은 사용자의 최종 중복 발급을 막는다.

requestId
→ 같은 논리적인 요청의 재처리를 식별한다.

requestHash
→ 같은 requestId가 다른 요청에 재사용되는 것을 막는다.
```

---

## **3. 이번 과제의 전제 조건**

이번 과제에서는 다음 전제를 따른다.

```
Client는 쿠폰 발급 요청을 보내기 전에 UUID 형식의 requestId를 생성한다.

같은 논리적인 요청을 재시도할 때는
최초 요청과 동일한 requestId를 재사용한다.

새로운 논리적인 작업에는 새로운 requestId를 사용한다.

userId는 Request Body가 아니라
인증된 사용자 정보에서 가져온다.

requestHash는 다음 Canonical String을 기준으로 생성한다.

COUPON_ISSUE|eventId|authenticatedUserId

Hash 알고리즘은 SHA-256을 사용한다.
```

이번 과제에서는 3주차의 Redis 구조를 다음처럼 확장한다고 가정한다.

```
Redis 판정 기준

- eventId + userId
- 해당 사용자의 최초 requestId
```

Redis Lua Script의 반환값은 다음 네 가지로 사용한다.

| **반환값** | **의미** |
| --- | --- |
| `SUCCESS` | 새로운 사용자가 새로운 requestId로 선착순 판정을 통과함 |
| `IDEMPOTENT_RETRY` | 같은 사용자와 같은 requestId가 다시 들어옴 |
| `DUPLICATE_USER` | 같은 사용자가 다른 requestId로 다시 요청함 |
| `SOLD_OUT` | Redis 기준 제한 수량에 도달함 |

이번 과제에서는 Redis를 통과한 요청만 DB 요청 상태 테이블에 저장하는 방식을 기본으로 사용한다.

```
Redis SUCCESS
→ coupon_issue_request PENDING 저장
→ Kafka 발행

Redis DUPLICATE_USER / SOLD_OUT
→ coupon_issue_request를 만들지 않음
→ Kafka에 발행하지 않음
```

요청 상태는 다음 네 가지로 단순화한다.

```
PENDING
PROCESSING
ISSUED
FAILED
```

최종 실패 코드는 다음 예시를 사용할 수 있다.

```
SOLD_OUT
DUPLICATE_USER
INVALID_EVENT
REQUEST_MISMATCH
```

다음 오류는 최종 비즈니스 실패가 아니라 일시적 시스템 오류로 본다.

```
DB 연결 일시 실패
Deadlock
네트워크 Timeout
Connection Pool 일시 부족
Consumer 프로세스 장애
```

일시적 시스템 오류가 발생하면 요청 상태를 `FAILED`로 확정하지 않고 트랜잭션을 Rollback한 뒤 Kafka 재전달을 통해 다시 처리한다.

이번 주차의 기본 Consumer 처리는 짧은 DB 작업이므로 다음 범위를 하나의 트랜잭션으로 묶는다.

```
PENDING → PROCESSING 처리 권한 획득
coupon_event.issued_count 증가
coupon_issue INSERT
coupon_issue_request ISSUED 또는 FAILED 변경
```

이번 주차에서는 아래 내용을 상세 구현하지 않는다.

```
Outbox Publisher 상세 구현

Retry Topic과 DLQ 상세 정책

쿠폰 발급 취소와 재발급

외부 결제나 외부 API 호출이 포함된 장시간 작업

Redis와 RDB를 하나의 분산 트랜잭션으로 묶는 방법
```

다만 Redis, DB, Kafka 사이에서 어떤 불일치가 발생할 수 있는지와 복구 방향은 설명해야 한다.

---

## **4. 제출 형식**

제출 파일은 아래 경로에 작성해서 PR을 생성한다.

```
submissions/Week5/기수_이름.md
```

예시는 다음과 같다.

```
submissions/Week5/13기_김기민.md
```

답안은 다음 규칙에 맞춰 작성한다.

```
- 모든 답안은 각 문항 바로 아래에 제공된 코드블록 안에 작성한다.
- 서술형 답안은 빈 코드블록 안에 작성한다.
- 비교표와 상태표는 코드블록 안의 표를 직접 채운다.
- 다이어그램은 코드블록 안에 ASCII 또는 Mermaid 형식으로 작성한다.
- SQL, JSON, HTTP 요청·응답은 제공된 언어별 코드블록 안에 작성한다.
- 코드블록 밖에는 답안을 작성하지 않는다.
```

제출 문서에는 아래 필수 과제를 모두 포함한다.

```
과제 1. requestId 기반 전체 멱등 처리 구조 그리기

과제 2. 중복 유형과 멱등성 경계 구분하기

과제 3. Idempotency-Key와 requestHash 설계하기

과제 4. coupon_issue_request 테이블과 상태 전이 설계하기

과제 5. API Server의 멱등 요청 접수 흐름 설계하기

과제 6. Consumer의 조건부 상태 전이와 처리 권한 설계하기

과제 7. 발급 트랜잭션과 Rollback 이후 상태 확정 설계하기

과제 8. Kafka Offset과 중복 메시지 처리 설계하기

과제 9. Redis, DB, Kafka 불일치와 복구 정책 설계하기

과제 10. 나쁜 멱등 설계의 문제점 찾기
```

---

# **5. 필수 과제**

---

## **과제 1. requestId 기반 전체 멱등 처리 구조 그리기**

Client의 최초 요청부터 Kafka Offset Commit까지의 전체 흐름을 다이어그램으로 작성한다.

반드시 아래 구성 요소를 포함해야 한다.

```
Client

Idempotency-Key

API Server

Redis Lua Script

coupon_issue_request

Kafka Producer

coupon.issue.requested Topic

Coupon Issue Consumer

coupon_event

coupon_issue

Kafka Offset Commit
```

### **1. 전체 요청 처리 흐름 다이어그램**

```

```

### **2. 하나의 requestId가 전체 흐름에서 어떻게 전달되는지 작성하기**

아래 흐름을 완성한다.

```
HTTP Idempotency-Key
        |
        v

        |
        v
coupon_issue_request.request_id
        |
        v

        |
        v
Consumer 멱등 처리
```

### **3. 각 구성 요소의 역할**

```
| **구성 요소** | **역할** |
| --- | --- |
| Redis |  |
| Kafka |  |
| coupon_issue_request |  |
| coupon_event |  |
| coupon_issue |  |
| Kafka Offset |  |
```

### **4. 각 단계의 성공이 의미하는 것**

```
| **단계** | **의미** | **최종 발급 성공 여부** |
| --- | --- | --- |
| Redis `SUCCESS` |  |  |
| request `PENDING` |  |  |
| request `PROCESSING` |  |  |
| request `ISSUED` |  |  |
| request `FAILED` |  |  |
| Kafka Offset Commit |  |  |
```

### **5. 최종 쿠폰 발급 성공으로 판단할 수 있는 시점**

```

```

### **6. 요청이 여러 번 도착해도 최종 결과가 한 번만 반영되어야 한다는 의미**

```

```

---

## **과제 2. 중복 유형과 멱등성 경계 구분하기**

같아 보이는 중복도 원인과 방어 장치가 다르다.

아래 상황을 읽고 각각 어떤 중복인지 구분한다.

### **1. 상황별 중복 유형 분석**

```
| **상황** | **같은 논리적 요청인가?** | **식별 기준** | **주요 방어 장치** |
| --- | --- | --- | --- |
| 응답 유실 후 같은 requestId로 API 재시도 |  |  |  |
| 반복 클릭으로 다른 requestId가 생성됨 |  |  |  |
| 애플리케이션이 같은 requestId 메시지를 `send()` 두 번 호출 |  |  |  |
| DB Commit 후 Offset Commit 전 장애로 같은 Record 재전달 |  |  |  |
| 원본 Topic과 Retry Topic에 같은 requestId가 존재 |  |  |  |
| 같은 requestId가 다른 eventId에 재사용됨 |  |  |  |
```

### **2. 같은 요청과 같은 사용자의 차이**

아래 두 상황의 차이를 설명한다.

```
상황 A
requestId = req-001
이 요청이 여러 번 전달됨

상황 B
requestId = req-A, req-B, req-C
모두 eventId = 100, userId = 10
```

답변:

```

```

### **3. 중복 방어 장치별 역할 비교**

```
| **장치** | **식별 기준** | **막는 문제** | **막지 못하는 문제** |
| --- | --- | --- | --- |
| Redis 사용자 판정 |  |  |  |
| requestId |  |  |  |
| requestHash |  |  |  |
| `UNIQUE(event_id, user_id)` |  |  |  |
| Kafka Partition + Offset |  |  |  |
```

### **4. Kafka Producer 멱등성이 막는 중복**

다음 흐름을 기준으로 설명한다.

```
애플리케이션이 send() 한 번 호출
→ Broker 저장 성공
→ ACK 유실
→ Producer 내부 재시도
```

```

```

### **5. 애플리케이션이 send()를 두 번 호출하면 Producer 멱등성으로 막을 수 없는 이유**

```java
kafkaTemplate.send(topic, message);
kafkaTemplate.send(topic, message);
```

```

```

### **6. requestId와 Kafka Partition + Offset의 차이**

```
requestId:

Partition + Offset:
```

### **7. Consumer 멱등성의 기준을 Offset이 아니라 requestId로 두어야 하는 이유**

아래 상황을 포함해서 설명한다.

```
원본 Topic
Partition 0 / Offset 100 / req-001

Retry Topic
Partition 2 / Offset 35 / req-001
```

```

```

---

## **과제 3. Idempotency-Key와 requestHash 설계하기**

HTTP API에서 동일한 논리적 요청의 재시도를 식별할 수 있도록 `Idempotency-Key`와 `requestHash`를 설계한다.

### **1. Idempotency-Key의 의미**

```

```

### **2. 누가 requestId를 생성해야 하는가?**

다음 두 방식을 비교하고 이번 구조에 더 적합한 방식을 선택한다.

```
방식 A
Client가 요청 전 requestId 생성

방식 B
API Server가 요청을 받은 뒤 requestId 생성
```

```
| **방식** | **장점** | **단점** |
| --- | --- | --- |
| Client 생성 |  |  |
| API Server 생성 |  |  |
```

내가 선택한 방식:

```

```

선택한 이유:

```

```

### **3. 같은 requestId를 재사용해야 하는 상황과 새로 생성해야 하는 상황**

```
| **상황** | **같은 requestId 재사용 또는 새 requestId 생성** | **이유** |
| --- | --- | --- |
| 네트워크 Timeout 후 동일 작업 재시도 |  |  |
| 응답 유실 후 동일 작업 재시도 |  |  |
| 모바일 앱 재연결 후 동일 작업 전송 |  |  |
| 다른 쿠폰 이벤트 발급 요청 |  |  |
| 기존 요청을 취소하고 새로운 작업 시작 |  |  |
```

### **4. requestId의 형식과 검증 규칙 설계하기**

아래 항목을 포함한다.

```
허용 형식

최대 길이

빈 값 처리

허용하지 않는 문자

DB 유일성 보장 방식
```

답변:

```

```

### **5. requestHash를 만드는 Canonical String 작성하기**

이번 과제의 기준은 다음과 같다.

```
작업 종류: COUPON_ISSUE
이벤트 ID: 100
인증된 사용자 ID: 10
```

Canonical String:

```

```

SHA-256 결과를 저장할 컬럼:

```

```

### **6. 전체 JSON 문자열을 그대로 Hash하면 안 되는 이유**

아래 두 JSON을 기준으로 설명한다.

```json
{ "eventId": 100, "userId": 10 }
```

```json
{"userId":10,"eventId":100}
```

```

```

### **7. requestHash에 포함할 값과 포함하지 않을 값 구분하기**

```
| **값** | **포함 여부** | **이유** |
| --- | --- | --- |
| 작업 종류 `COUPON_ISSUE` |  |  |
| eventId |  |  |
| 인증된 userId |  |  |
| 요청 전송 시각 |  |  |
| Trace ID |  |  |
| 매번 바뀌는 nonce |  |  |
| Gateway가 추가한 가변 Header |  |  |
```

### **8. Request Body의 userId를 requestHash에 그대로 사용하면 안 되는 이유**

```

```

### **9. 같은 requestId에 다른 내용이 들어온 경우 처리 설계**

```
기존 요청
requestId = req-001
requestHash = hash(COUPON_ISSUE|100|10)

새 요청
requestId = req-001
requestHash = hash(COUPON_ISSUE|200|10)
```

아래 항목을 작성한다.

```
HTTP Status Code:

에러 코드:

Kafka 발행 여부:

기존 요청 상태 변경 여부:

사용자 응답:
```

---

## **과제 4. coupon_issue_request 테이블과 상태 전이 설계하기**

동일한 요청의 처리 상태와 최종 결과를 저장하기 위한 `coupon_issue_request` 테이블을 설계한다.

### **1. coupon_issue_request와 coupon_issue의 역할 차이**

```
| **테이블** | **저장하는 대상** | **표현할 수 있는 상태** |
| --- | --- | --- |
| coupon_issue_request |  |  |
| coupon_issue |  |  |
```

### **2. 요청 상태 모델 설명하기**

```
| **상태** | **의미** | **최종 상태 여부** |
| --- | --- | --- |
| `PENDING` |  |  |
| `PROCESSING` |  |  |
| `ISSUED` |  |  |
| `FAILED` |  |  |
```

### **3. 상태 전이 다이어그램 작성하기**

아래 허용 상태 전이와 금지 상태 전이를 구분해서 표현한다.

```
허용 후보
PENDING → PROCESSING
PROCESSING → ISSUED
PROCESSING → FAILED

금지 후보
ISSUED → PROCESSING
ISSUED → FAILED
FAILED → PROCESSING
FAILED → ISSUED
```

```

```

### **4. 단일 트랜잭션 구조에서 외부 조회 시 PROCESSING이 거의 보이지 않을 수 있는 이유**

```

```

### **5. coupon_issue_request DDL 작성하기**

아래 컬럼을 포함한다.

```
request_id

event_id

user_id

request_hash

status

failure_code

failure_message

coupon_issue_id

requested_at

processing_started_at

completed_at

created_at

updated_at
```

아래 제약 조건을 포함한다.

```
request_id Primary Key

coupon_event Foreign Key

coupon_issue Foreign Key

coupon_issue_id UNIQUE

status CHECK

FAILED일 때만 failure_code 필수

ISSUED 또는 FAILED일 때 completed_at 필수

ISSUED일 때만 coupon_issue_id 필수

PROCESSING일 때 processing_started_at 필수
```

```sql

```

### **6. 요청 상태 테이블에 `UNIQUE(event_id, user_id)`를 두지 않는 이유**

아래 이력이 모두 저장되어야 한다는 점을 포함해서 설명한다.

```
req-A → ISSUED
req-B → FAILED / DUPLICATE_USER
```

```

```

### **7. 상태 변경 SQL에 출발 상태를 조건으로 포함해야 하는 이유**

잘못된 예:

```sql
UPDATE coupon_issue_request
SET status = 'ISSUED'
WHERE request_id = :requestId;
```

올바른 상태 전이 SQL을 직접 작성한다.

```sql

```

### **8. 최종 상태 변경의 영향받은 row 수가 0인 경우**

가능한 원인을 작성한다.

```
1.

2.

3.
```

### **9. 운영 복구를 위한 인덱스 설계하기**

오래된 `PENDING` 또는 `PROCESSING` 요청을 찾기 위한 인덱스를 작성한다.

```sql

```

### **10. 요청 상태 보관 기간과 Idempotency-Key 유효 기간의 관계**

아래 식을 완성하고 이유를 설명한다.

```
Idempotency-Key 유효 기간
___
요청 상태 보관 기간
```

이유:

```

```

---

## **과제 5. API Server의 멱등 요청 접수 흐름 설계하기**

API Server가 Redis 판정, 요청 상태 등록, Kafka 발행을 어떤 순서로 처리할지 설계한다.

이번 과제에서는 Redis를 통과한 요청만 DB에 저장하는 방식을 기본으로 한다.

### **1. API Server 전체 처리 흐름 다이어그램**

다음 결과를 모두 포함한다.

```
SUCCESS
IDEMPOTENT_RETRY
DUPLICATE_USER
SOLD_OUT
requestId 충돌
Kafka 발행 실패
```

```

```

### **2. Redis 결과별 API 처리**

```
| **Redis 결과** | **DB 요청 상태 등록** | **Kafka 발행** | **API 응답 방향** |
| --- | --- | --- | --- |
| `SUCCESS` |  |  |  |
| `IDEMPOTENT_RETRY` |  |  |  |
| `DUPLICATE_USER` |  |  |  |
| `SOLD_OUT` |  |  |  |
```

### **3. 동일 requestId 동시 요청에서 SELECT 후 INSERT가 안전하지 않은 이유**

아래 흐름을 시간 순서로 완성한다.

```
시간       API Server A               API Server B
----------------------------------------------------------
T1
T2
T3
T4
T5
T6
```

문제의 이름:

```

```

### **4. DB 유일성 제약을 이용한 최초 등록자 결정 SQL 작성하기**

PostgreSQL의 `ON CONFLICT DO NOTHING`을 사용한다.

```sql

```

### **5. INSERT 영향받은 row 수 처리**

```
affected rows = 1
→

affected rows = 0
→
```

### **6. 기존 행의 requestHash 확인 결과에 따른 처리**

```
| **조건** | **판단** | **처리** |
| --- | --- | --- |
| requestId 같음 + requestHash 같음 |  |  |
| requestId 같음 + requestHash 다름 |  |  |
```

### **7. Redis가 `IDEMPOTENT_RETRY`를 반환했지만 DB에 요청 행이 없는 경우**

이 상태가 발생할 수 있는 이유를 설명한다.

```

```

이 상태를 무조건 정상 `PENDING`으로 응답하면 안 되는 이유:

```

```

재조회·복구·보상 방향:

```

```

### **8. Redis가 `SUCCESS`를 반환했지만 DB INSERT가 충돌한 경우**

다음 상황을 고려한다.

```
과거 요청 req-001은 DB에 존재한다.
Redis 데이터는 초기화되었다.
Client가 req-001을 다시 전송한다.
Redis는 새로운 요청으로 판단해 SUCCESS를 반환한다.
DB INSERT는 request_id 충돌로 0행이다.
```

아래 항목을 작성한다.

```
기존 requestHash 확인:

Kafka 발행 여부:

Redis에서 새로 차지한 자리 처리:

API 응답:
```

### **9. 최초 요청 접수 성공 응답 설계하기**

HTTP Status Code:

```

```

응답 JSON:

```json
{
  "requestId": "",
  "status": "",
  "message": ""
}
```

이 응답이 최종 발급 성공을 의미하지 않는 이유:

```

```

### **10. 동일 requestId 재요청의 응답 설계하기**

`PENDING`, `ISSUED`, `FAILED` 상태 각각에 대해 응답을 설계한다.

#### **PENDING**

```json
{

}
```

#### **ISSUED**

```json
{

}
```

#### **FAILED**

```json
{

}
```

### **11. 같은 requestId에 다른 내용이 들어온 경우 응답 설계하기**

```json
{
  "requestId": "",
  "errorCode": "",
  "message": ""
}
```

HTTP Status Code와 이유:

```

```

### **12. 상태 조회 API의 인증 조건 작성하기**

API:

```
GET /api/coupon-issue-requests/{requestId}
```

조회 SQL:

```sql

```

`requestId`만으로 조회하면 안 되는 이유:

```

```

---

## **과제 6. Consumer의 조건부 상태 전이와 처리 권한 설계하기**

Kafka 메시지가 중복 전달되더라도 한 Consumer만 실제 발급 로직을 수행하도록 처리 권한 획득 방식을 설계한다.

### **1. Kafka 메시지와 요청 상태 행의 일치 검증**

다음 필드를 비교한다.

```
| **필드** | **Kafka Message** | **coupon_issue_request** | **불일치 시 처리** |
| --- | --- | --- | --- |
| requestId |  |  |  |
| eventId |  |  |  |
| userId |  |  |  |
```

메시지 내용 불일치를 재시도 대상으로 두기 어려운 이유:

```

```

### **2. SELECT 후 UPDATE 방식의 Race Condition 분석하기**

위험한 흐름은 다음과 같다.

```
1. 상태를 SELECT한다.
2. PENDING인지 애플리케이션에서 확인한다.
3. PROCESSING으로 UPDATE한다.
```

두 Consumer가 동시에 실행되는 시간 순서 다이어그램을 작성한다.

```
시간       Consumer A                  Consumer B
------------------------------------------------------------
T1
T2
T3
T4
T5
T6
T7
```

### **3. 조건부 UPDATE로 처리 권한 획득하기**

SQL을 작성한다.

```sql

```

### **4. 영향받은 row 수의 의미**

```
affected rows = 1
→

affected rows = 0
→
```

### **5. 두 Consumer가 동시에 같은 requestId를 처리할 때 DB 내부 동작**

현재 상태는 `PENDING`이다.

```
Consumer A:

Consumer B:
```

`READ COMMITTED`에서 두 번째 Consumer의 `WHERE status = 'PENDING'` 조건이 어떻게 처리되는지 설명한다.

```

```

### **6. REPEATABLE READ 이상에서 달라질 수 있는 점**

```

```

### **7. affected rows가 0일 때 상태별 처리**

```
| **조회 결과** | **의미** | **Consumer 처리** | **Offset 처리 방향** |
| --- | --- | --- | --- |
| 요청 행 없음 |  |  |  |
| `PENDING` |  |  |  |
| `PROCESSING` |  |  |  |
| `ISSUED` |  |  |  |
| `FAILED` |  |  |  |
```

### **8. ISSUED 또는 FAILED 중복 메시지를 예외로 던지면 안 되는 이유**

아래 잘못된 흐름을 기준으로 설명한다.

```
ISSUED 확인
→ DuplicateMessageException
→ Retry Topic
→ 다시 ISSUED 확인
→ 다시 예외
→ DLQ
```

```

```

### **9. 최종 상태를 확인하고 아무 변경 없이 종료하는 것도 성공적인 처리인 이유**

```

```

---

## **과제 7. 발급 트랜잭션과 Rollback 이후 상태 확정 설계하기**

처리 권한 획득부터 최종 요청 상태 변경까지를 하나의 DB 트랜잭션으로 설계한다.

### **1. 트랜잭션에 포함할 작업과 포함하지 않을 작업 구분하기**

```
| **작업** | **발급 트랜잭션 포함 여부** | **이유** |
| --- | --- | --- |
| PENDING → PROCESSING |  |  |
| coupon_event 수량 확보 |  |  |
| coupon_issue INSERT |  |  |
| request ISSUED / FAILED 변경 |  |  |
| 이메일 발송 |  |  |
| 앱 푸시 발송 |  |  |
| 마이페이지 조회 모델 갱신 |  |  |
| 외부 API 호출 |  |  |
```

### **2. 전체 발급 트랜잭션 흐름 다이어그램**

다음 경로를 모두 포함한다.

```
처리 권한 획득 0행

수량 UPDATE 0행

coupon_issue INSERT 성공

coupon_issue UNIQUE 위반

최종 ISSUED 변경 0행

일시적 DB 오류
```

```

```

### **3. 정상 발급 SQL 흐름 완성하기**

```sql
BEGIN;

-- 1. PENDING 요청의 처리 권한 획득

-- 2. 수량이 남아 있을 때만 issued_count 증가

-- 3. 최종 발급 기록 저장

-- 4. 요청 상태를 ISSUED로 변경

COMMIT;
```

### **4. 각 단계에서 반드시 확인할 영향받은 row 수**

```
| **단계** | **정상적으로 기대하는 row 수** | **기대값과 다를 때 처리** |
| --- | --- | --- |
| 처리 권한 획득 |  |  |
| 수량 UPDATE |  |  |
| 최종 ISSUED 변경 |  |  |
```

### **5. 마지막 ISSUED 상태 변경이 0행인데 발급 결과만 Commit하면 안 되는 이유**

```

```

### **6. DB 기준 수량이 소진된 경우 처리**

조건부 UPDATE가 0행이고 이벤트가 존재하며 수량이 모두 소진된 것으로 확정되었다고 가정한다.

요청 상태를 `FAILED / SOLD_OUT`으로 변경하는 SQL을 작성한다.

```sql

```

발급 행 생성 여부:

```

```

같은 메시지가 다시 들어왔을 때 처리:

```

```

### **7. 서로 다른 requestId로 같은 사용자가 다시 발급을 시도한 경우**

이 시나리오는 Redis 관리자 admission 기록이 유실되거나 만료되어 req-B가 SUCCESS로 통과한 상황을 가정한다. 이때 DB의 UNIQUE(event_id, user_id)가 최종 방어선이 된다.

```
기존 요청
req-A / eventId=100 / userId=10 / ISSUED

새 요청
req-B / eventId=100 / userId=10 / PENDING
```

`req-B` 처리 중 `coupon_issue` INSERT에서 `UNIQUE(event_id, user_id)` 위반이 발생했다.

아래 항목을 설명한다.

```
첫 번째 트랜잭션의 상태:

PENDING → PROCESSING 전이의 결과:

앞에서 증가한 issued_count의 결과:

같은 트랜잭션에서 FAILED로 바꿀 수 있는가?:
```

### **8. UNIQUE 위반 Rollback 후 별도 트랜잭션 처리 순서**

```
1.

2.

3.

4.

5.
```

### **9. Rollback 후 요청 상태를 어떤 출발 상태에서 FAILED로 변경해야 하는가?**

잘못된 SQL:

```sql
UPDATE coupon_issue_request
SET status = 'FAILED'
WHERE request_id = :requestId
  AND status = 'PROCESSING';
```

올바른 SQL을 작성한다.

```sql

```

왜 `PROCESSING`이 아닌지 설명한다.

```

```

### **10. 일시적 DB 오류가 발생한 경우**

다음 항목을 작성한다.

```
요청 상태:

issued_count:

coupon_issue:

FAILED 저장 여부:

Kafka Listener 예외 처리:
```

### **11. 최종 비즈니스 실패와 일시적 시스템 오류 비교**

```
| **오류** | **최종 FAILED 저장 여부** | **Kafka 재시도 여부** | **이유** |
| --- | --- | --- | --- |
| DB 연결 일시 실패 |  |  |  |
| Deadlock |  |  |  |
| SOLD_OUT |  |  |  |
| DUPLICATE_USER |  |  |  |
| 메시지와 요청 행 불일치 |  |  |  |
```

---

## **과제 8. Kafka Offset과 중복 메시지 처리 설계하기**

DB 비즈니스 결과와 Kafka 소비 위치를 안전하게 연결하도록 Offset 처리 순서를 설계한다.

### **1. 안전한 처리 순서 작성하기**

아래 흐름을 완성한다.

```
1. Kafka 메시지를 읽는다.

2.

3.

4.

5.

6.

7. Kafka Offset을 Commit한다.
```

### **2. DB 결과 확정과 Offset Commit의 역할 차이**

```
| **항목** | **의미** |
| --- | --- |
| DB Commit |  |
| Kafka Offset Commit |  |
```

### **3. Offset을 먼저 Commit한 뒤 DB 처리가 실패하는 상황**

```
1. Offset Commit
2. DB 처리 시도
3. DB 장애
```

Kafka 상태:

```

```

DB 상태:

```

```

사용자 요청에 발생하는 문제:

```

```

### **4. DB Commit 후 Offset Commit 전에 Consumer가 종료되는 상황**

```
DB Commit 성공
→ Consumer 종료
→ Offset Commit 실패
```

Consumer 재시작 후 발생하는 일:

```

```

request 상태가 `ISSUED`인 경우 처리:

```

```

### **5. 같은 Kafka Record 재전달과 같은 requestId의 다른 Record 비교**

```
| **구분** | **Partition** | **Offset** | **requestId** |
| --- | --- | --- | --- |
| Offset 미반영으로 동일 Record 재전달 |  |  |  |
| Producer가 같은 요청을 두 번 발행 |  |  |  |
```

두 현상을 모두 requestId로 방어해야 하는 이유:

```

```

### **6. 자동 커밋 사용 시 확인해야 할 항목**

```
1.

2.

3.

4.

5.
```

### **7. At-Least-Once와 비즈니스 결과의 차이**

아래 문장을 완성한다.

```
메시지 전달 횟수
→

쿠폰 발급 결과
→
```

### **8. 다음 상태에서 Offset 처리 방향 작성하기**

```
| **request 상태 또는 처리 결과** | **Offset 처리 방향** | **이유** |
| --- | --- | --- |
| 새 `PENDING` 요청 처리 성공 |  |  |
| 기존 `ISSUED` 요청 중복 메시지 |  |  |
| 기존 `FAILED` 요청 중복 메시지 |  |  |
| 일시적 DB 오류로 Rollback |  |  |
| 요청 행 없음 |  |  |
| 메시지 내용 불일치 |  |  |
```

---

## **과제 9. Redis, DB, Kafka 불일치와 복구 정책 설계하기**

Redis, DB, Kafka는 하나의 로컬 트랜잭션으로 묶이지 않는다.

따라서 각 경계에서 생길 수 있는 불일치를 분석하고 복구 방향을 설계한다.

### **1. 장애 시점별 상태와 복구 방향**

```
| **장애 상황** | **Redis 상태** | **DB 요청 상태** | **Kafka 상태** | **복구 방향** |
| --- | --- | --- | --- | --- |
| Redis SUCCESS 후 DB 요청 행 INSERT 전 장애 |  |  |  |  |
| DB에 PENDING 저장 후 Kafka 발행 전 장애 |  |  |  |  |
| Kafka 발행 성공 후 API 응답 유실 |  |  |  |  |
| Consumer가 DB 처리 전 종료 |  |  |  |  |
| DB Commit 후 Offset Commit 전 종료 |  |  |  |  |
| DB ISSUED 후 Redis 데이터 유실 |  |  |  |  |
```

### **2. Redis SUCCESS 후 DB 요청 행이 없는 상태**

이 상태가 발생하는 흐름을 다이어그램으로 작성한다.

```

```

동일 requestId 재시도 시 처리 방향:

```

```

일정 시간 이상 지속될 때 가능한 보상:

```

```

보상 시 현재 Redis 자리의 requestId 소유권을 확인해야 하는 이유:

```

```

### **3. PENDING 저장 뒤 Kafka 발행 실패**

최종 상태를 작성한다.

```
Redis:

DB:

Kafka:
```

이 요청이 계속 `PENDING`에 남는 이유:

```

```

복구를 위해 필요한 인덱스 또는 조회 조건:

```sql

```

복구 방향:

```

```

### **4. Kafka 발행 Timeout이 모호한 이유**

아래 두 상황을 설명한다.

```
상황 A
Broker가 메시지를 저장하지 못하고 Timeout

상황 B
Broker는 메시지를 저장했지만 ACK만 유실되어 Timeout
```

Producer가 두 상황을 구분하기 어려운 이유:

```

```

같은 requestId로 재발행했을 때 가능한 Kafka Record:

```

```

Consumer가 안전하게 처리할 수 있는 이유:

```

```

### **5. Outbox Pattern이 줄일 수 있는 불일치**

아래 두 작업을 하나의 DB 트랜잭션으로 묶는 구조를 작성한다.

```
coupon_issue_request INSERT

outbox_event INSERT
```

다이어그램:

```

```

### **6. Outbox Pattern이 해결하지 못하는 구간**

아래 구간을 기준으로 설명한다.

```
Redis 선착순 판정
↔
DB 요청 상태 저장
```

```

```

### **7. 최종 발급 결과의 기준 데이터**

다음 중 최종 발급 여부를 판단할 기준을 선택하고 이유를 설명한다.

```
Redis 통과 기록

Kafka 메시지 존재 여부

coupon_issue DB 기록

coupon_issue_request 상태
```

선택:

```

```

이유:

```

```

### **8. 재실행 가능한 설계 정리하기**

```
| **단계** | **중복 또는 재실행 방어 기준** |
| --- | --- |
| Redis 판정 |  |
| DB 요청 등록 |  |
| Kafka 재발행 |  |
| Consumer 처리 |  |
```

### **9. 운영 메트릭과 경고 항목 설계하기**

아래 항목을 포함해 최소 5개 작성한다.

```
오래된 PENDING 요청 수

오래된 PROCESSING 요청 수

requestId 충돌 횟수

Redis SUCCESS 후 DB 행 누락 횟수

Kafka 발행 실패 횟수

멱등 중복 메시지 처리 횟수
```

```
| **메트릭** | **의미** | **경고가 필요한 조건** |
| --- | --- | --- |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
```

---

## **과제 10. 나쁜 멱등 설계의 문제점 찾기**

아래 시스템은 `requestId`를 사용하고 있지만 안전한 멱등 구조라고 보기 어렵다.

```
1. API Server가 요청을 받을 때마다 새로운 requestId를 생성한다.

2. 같은 requestId가 들어오면 요청 내용은 확인하지 않고
   기존 결과를 무조건 반환한다.

3. 요청 상태는 coupon_issue에 성공 기록만 저장한다.

4. Consumer는 SELECT로 PENDING을 확인한 뒤
   조건 없이 PROCESSING으로 UPDATE한다.

5. 최종 상태 변경 SQL에 이전 상태 조건이 없다.

6. request가 ISSUED이면 DuplicateMessageException을 던진다.

7. Kafka Offset은 DB 처리 전에 자동 Commit될 수 있다.

8. DB Deadlock도 즉시 request FAILED로 저장한다.

9. coupon_issue UNIQUE 위반 후 같은 트랜잭션에서
   request를 FAILED로 변경하려고 한다.

10. UNIQUE 위반 트랜잭션이 Rollback된 뒤에도
    WHERE status = 'PROCESSING'으로 FAILED 변경을 시도한다.

11. 상태 조회 API는 requestId만 알면 누구나 조회할 수 있다.

12. Idempotency-Key 재사용 가능 기간보다 먼저
    coupon_issue_request 행을 삭제한다.
```

### **1. 문제점과 개선 방향 작성하기**

최소 10개 이상 작성한다.

```
| **문제점** | **왜 문제인가?** | **개선 방향** |
| --- | --- | --- |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
```

### **2. 다음 잘못된 SQL을 안전한 조건부 상태 전이로 수정하기**

잘못된 SQL:

```sql
UPDATE coupon_issue_request
SET status = 'PROCESSING'
WHERE request_id = :requestId;
```

수정 SQL:

```sql

```

### **3. 다음 잘못된 최종 성공 SQL을 수정하기**

잘못된 SQL:

```sql
UPDATE coupon_issue_request
SET status = 'ISSUED',
    coupon_issue_id = :couponIssueId
WHERE request_id = :requestId;
```

수정 SQL:

```sql

```

### **4. 중복 메시지를 정상적으로 종료시키는 처리 흐름 작성하기**

```
ISSUED:

FAILED:
```

### **5. 이번 5주차 설계에서 최종적으로 지켜야 하는 불변식 정리하기**

최소 6개 작성한다.

```
1.

2.

3.

4.

5.

6.
```

---
