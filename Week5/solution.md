# **5주차 과제 해설**

# **requestId 기반 멱등성과 중복 처리 구조 설계**

---

## **이번 주차의 핵심**

```
이번 주차의 목표는 중복 요청과 중복 메시지를
물리적으로 완전히 없애는 것이 아니다.

같은 논리적 요청이 여러 번 전달되더라도
최종 비즈니스 결과가 한 번만 반영되도록 만드는 것이 목표다.

requestId
→ 같은 논리적 요청의 재전달을 식별한다.

requestHash
→ 같은 requestId가 다른 요청 내용에 재사용되는 것을 막는다.

coupon_issue_request
→ 요청의 처리 상태와 최종 결과를 저장한다.

조건부 상태 전이
→ 여러 Consumer 중 한 Consumer만 처리 권한을 얻도록 한다.

DB Transaction과 UNIQUE 제약
→ 수량, 발급 기록, 요청 결과의 최종 정합성을 보장한다.
```

```
메시지는 여러 번 전달될 수 있다.

하지만

같은 requestId의 발급 결과는 한 번만 확정되어야 한다.
같은 사용자의 같은 이벤트 쿠폰은 한 번만 발급되어야 한다.
최종 상태는 오래된 처리자가 덮어쓰면 안 된다.
```

---

# **과제 1. requestId 기반 전체 멱등 처리 구조 그리기**

## **1-1. 전체 요청 처리 흐름 다이어그램**

```
+--------+
| Client |
+--------+
    |
    | POST /api/coupon-events/100/issue
    | Idempotency-Key: req-001
    v
+-------------------------------------------+
| API Server                                |
|-------------------------------------------|
| 1. 인증된 userId 확인                     |
| 2. requestId UUID 형식 검증               |
| 3. requestHash 생성                       |
|    COUPON_ISSUE|100|10                    |
+-------------------------------------------+
    |
    v
+-------------------------------------------+
| Redis Lua Script                          |
|-------------------------------------------|
| eventId + userId 중복 확인                |
| 최초 requestId 확인                       |
| 제한 수량 확인                            |
+-------------------------------------------+
    |
    +-- IDEMPOTENT_RETRY
    |      → coupon_issue_request 조회
    |      → 기존 상태와 결과 반환
    |
    +-- DUPLICATE_USER
    |      → 새 요청 등록·Kafka 발행 없음
    |
    +-- SOLD_OUT
    |      → 새 요청 등록·Kafka 발행 없음
    |
    +-- SUCCESS
           |
           v
+-------------------------------------------+
| PostgreSQL                                |
|-------------------------------------------|
| coupon_issue_request                      |
| request_id = req-001                      |
| request_hash = hash(COUPON_ISSUE|100|10)  |
| status = PENDING                          |
+-------------------------------------------+
           |
           | INSERT ... ON CONFLICT DO NOTHING
           |
           +-- 충돌
           |     → 기존 requestHash 비교
           |     → 동일하면 기존 결과 반환
           |     → 다르면 409 REQUEST_MISMATCH
           |
           +-- 최초 등록
                  |
                  v
          +-------------------------+
          | Kafka Producer          |
          | key와 payload에         |
          | requestId 포함          |
          +-------------------------+
                  |
                  v
          +-------------------------+
          | coupon.issue.requested  |
          +-------------------------+
                  |
                  v
          +-------------------------+
          | Coupon Issue Consumer   |
          +-------------------------+
                  |
                  | requestId로 요청 행 조회·검증
                  v
+------------------------------------------------+
| PostgreSQL Transaction                         |
|------------------------------------------------|
| 1. request PENDING → PROCESSING 조건부 UPDATE  |
| 2. coupon_event.issued_count 조건부 증가       |
| 3. coupon_issue INSERT                         |
| 4. request PROCESSING → ISSUED                 |
+------------------------------------------------+
                  |
                  | Commit 성공
                  v
          +-------------------------+
          | Kafka Offset Commit     |
          +-------------------------+

최종 발급 성공 기준
→ 수량 증가, 발급 기록, request ISSUED가
   하나의 DB 트랜잭션으로 Commit된 시점
```

## **1-2. 하나의 requestId가 전체 흐름에서 전달되는 과정**

```
HTTP Idempotency-Key
        |
        v
API Server의 requestId
        |
        +-- requestHash 생성과 기존 요청 조회 기준
        |
        v
Redis의 사용자별 최초 requestId
        |
        v
coupon_issue_request.request_id
        |
        v
Kafka Message의 requestId
        |
        v
Consumer의 조건부 상태 전이와 멱등 처리 기준
```

## **1-3. 각 구성 요소의 역할**

```
| 구성 요소 | 역할 |
| --- | --- |
| Redis | 사용자 중복과 선착순 수량을 빠르게 판정하고, 같은 사용자에게 저장된 최초 requestId를 이용해 동일 요청 재시도와 새로운 중복 요청을 구분한다. |
| Kafka | 요청을 API Server에서 Consumer로 비동기 전달하고, Consumer 장애 시 미처리 메시지를 재전달한다. |
| coupon_issue_request | requestId별 요청 내용, 처리 상태, 실패 이유, 최종 couponIssueId를 저장하는 멱등성 원장이다. |
| coupon_event | total_quantity와 issued_count를 저장하며 조건부 UPDATE로 전체 수량 초과를 막는다. |
| coupon_issue | 실제로 발급에 성공한 쿠폰 기록을 저장하고 UNIQUE(event_id, user_id)로 사용자 중복 발급을 막는다. |
| Kafka Offset | Consumer Group이 Topic Partition의 어느 위치까지 처리했는지 나타내는 소비 위치다. 비즈니스 요청 ID가 아니다. |
```

## **1-4. 각 단계의 성공이 의미하는 것**

```
| 단계 | 의미 | 최종 발급 성공 여부 |
| --- | --- | --- |
| Redis SUCCESS | Redis 기준 중복·선착순 판정을 통과했다. | 아니오 |
| request PENDING | 요청 행이 등록되었지만 Kafka 발행 또는 Consumer 처리가 남아 있다. | 아니오 |
| request PROCESSING | Consumer가 처리 권한을 얻어 발급 트랜잭션을 수행 중이다. | 아니오 |
| request ISSUED | 수량 증가, 발급 기록, 요청 성공 상태가 함께 Commit되었다. | 예 |
| request FAILED | 재시도해도 결과가 바뀌지 않는 최종 비즈니스 실패가 확정되었다. | 아니오 |
| Kafka Offset Commit | 해당 Consumer Group이 해당 위치까지 처리를 완료했다고 기록했다. 발급 성공 여부 자체는 DB 상태로 판단한다. | 그 자체만으로는 판단 불가 |
```

## **1-5. 최종 쿠폰 발급 성공 시점**

```
다음 세 작업이 하나의 PostgreSQL 트랜잭션으로
Commit된 시점을 최종 발급 성공으로 판단한다.

1. coupon_event.issued_count 증가
2. coupon_issue 발급 기록 저장
3. coupon_issue_request.status = ISSUED 변경

Redis SUCCESS나 Kafka 발행 성공은
최종 발급 처리에 들어갈 자격 또는 요청 전달 성공일 뿐이다.
```

## **1-6. 요청이 여러 번 도착해도 최종 결과가 한 번만 반영된다는 의미**

```
같은 requestId가 API나 Kafka에 여러 번 도착해도
첫 번째 유효 처리만 비즈니스 상태를 변경해야 한다.

첫 처리
→ PENDING을 PROCESSING으로 변경
→ 수량 1 증가
→ coupon_issue 1행 저장
→ request ISSUED 확정

이후 중복 처리
→ 기존 ISSUED 상태 확인
→ 수량을 다시 증가시키지 않음
→ coupon_issue를 다시 저장하지 않음
→ 저장된 동일 결과를 반환하거나 정상 종료

또한 다른 requestId로 같은 사용자가 요청하더라도
coupon_issue의 UNIQUE(event_id, user_id)가
최종 발급 결과를 한 건으로 제한해야 한다.
```

---

# **과제 2. 중복 유형과 멱등성 경계 구분하기**

## **2-1. 상황별 중복 유형 분석**

```
| 상황 | 같은 논리적 요청인가? | 식별 기준 | 주요 방어 장치 |
| --- | --- | --- | --- |
| 응답 유실 후 같은 requestId로 API 재시도 | 예 | requestId와 requestHash | coupon_issue_request PK, 기존 상태 반환 |
| 반복 클릭으로 다른 requestId가 생성됨 | 아니오. 같은 사용자의 새로운 요청들이다. | eventId + userId | Redis 사용자 판정, UNIQUE(event_id, user_id) |
| 애플리케이션이 같은 requestId 메시지를 send() 두 번 호출 | 예 | Kafka Record는 달라도 requestId가 같음 | Consumer의 requestId 조건부 상태 전이 |
| DB Commit 후 Offset Commit 전 장애로 같은 Record 재전달 | 예 | 같은 Topic/Partition/Offset과 같은 requestId | requestId 멱등 처리, 최종 상태 확인 |
| 원본 Topic과 Retry Topic에 같은 requestId가 존재 | 예 | Partition과 Offset은 달라도 requestId가 같음 | requestId 멱등 처리 |
| 같은 requestId가 다른 eventId에 재사용됨 | 동일 요청이 아니라 잘못된 키 재사용 | requestId 같음 + requestHash 다름 | requestHash 비교, REQUEST_MISMATCH |
```

## **2-2. 같은 요청과 같은 사용자의 차이**

```
상황 A
requestId = req-001이 여러 번 전달됨

→ 하나의 논리적 요청이 네트워크 재시도나 메시지 재전달로
   여러 번 나타난 것이다.
→ requestId 기준으로 기존 처리 상태와 결과를 재사용한다.

상황 B
req-A, req-B, req-C가 모두 eventId=100, userId=10

→ 같은 사용자가 새로운 논리적 요청을 여러 번 만든 것이다.
→ requestId만 보면 서로 다른 요청이므로 모두 신규다.
→ Redis의 사용자 중복 판정과
   coupon_issue의 UNIQUE(event_id, user_id)로 막아야 한다.

따라서 다음 두 질문은 서로 다르다.

“이미 처리한 바로 그 요청인가?”
→ requestId

“이 사용자가 이 이벤트의 쿠폰을 이미 발급받았는가?”
→ eventId + userId
```

## **2-3. 중복 방어 장치별 역할 비교**

```
| 장치 | 식별 기준 | 막는 문제 | 막지 못하는 문제 |
| --- | --- | --- | --- |
| Redis 사용자 판정 | eventId + userId와 최초 requestId | 앞단의 반복 사용자 요청, 선착순 수량 초과 | Redis 유실·우회 후 DB에 도달한 중복, Kafka 재전달 |
| requestId | 논리적 요청 ID | 같은 요청의 API 재시도와 Kafka 재전달 | 다른 requestId로 들어온 같은 사용자의 중복 요청 |
| requestHash | 작업 종류 + eventId + 인증된 userId | 같은 requestId의 다른 요청 내용 재사용 | 서로 다른 requestId의 동일 사용자 중복 |
| UNIQUE(event_id, user_id) | 최종 발급의 이벤트와 사용자 | 같은 사용자의 최종 중복 발급 | 같은 requestId의 처리 상태와 기존 실패 결과 반환 |
| Kafka Partition + Offset | Kafka 안의 물리적 Record 위치 | Consumer Group의 소비 진행 위치 관리 | Retry Topic 재발행, 별도 Record로 만들어진 동일 논리 요청 식별 |
```

## **2-4. Kafka Producer 멱등성이 막는 중복**

```
애플리케이션이 send()를 한 번 호출했는데
Broker 저장 성공 후 ACK만 유실되면
Producer는 같은 전송을 내부적으로 재시도할 수 있다.

Kafka Producer Idempotence는 Producer ID와 Sequence를 이용해
이 내부 재시도로 같은 Record가 Broker Log에
중복 저장되는 가능성을 줄이거나 방지한다.

하지만 이는 Producer 세션과 전송 프로토콜 범위의 멱등성이다.
쿠폰 발급이라는 비즈니스 작업의 멱등성을 대신하지 않는다.
```

## **2-5. 애플리케이션이 send()를 두 번 호출한 경우**

```
kafkaTemplate.send(topic, message);
kafkaTemplate.send(topic, message);

위 코드는 Producer 입장에서 서로 다른 두 개의 정상 전송 요청이다.

각 호출에는 서로 다른 Producer Sequence가 부여될 수 있으므로
Producer Idempotence는 두 Record를 중복이라고 판단하지 않는다.

따라서 Kafka에는 다음처럼 두 Record가 저장될 수 있다.

Offset 100 → requestId=req-001
Offset 101 → requestId=req-001

두 Record가 같은 비즈니스 요청인지는
Consumer가 requestId를 기준으로 판단해야 한다.
```

## **2-6. requestId와 Kafka Partition + Offset의 차이**

```
requestId:
클라이언트부터 Consumer까지 전달되는 논리적 요청 식별자다.
재발행되거나 Retry Topic으로 이동해도 같은 요청이면 유지한다.

Partition + Offset:
특정 Kafka Topic 안에서 Record가 저장된 물리적 위치다.
같은 요청이 다시 발행되면 새로운 Partition 또는 Offset을 가질 수 있다.
```

## **2-7. Consumer 멱등성 기준으로 requestId를 사용해야 하는 이유**

```
원본 Topic
Partition 0 / Offset 100 / req-001

Retry Topic
Partition 2 / Offset 35 / req-001

두 Record는 Kafka 위치가 다르지만 같은 논리적 요청이다.

Partition + Offset을 멱등성 Key로 사용하면
두 Record를 서로 다른 요청으로 판단해 발급 로직을 두 번 실행할 수 있다.

requestId를 사용하면 Topic, Partition, Offset이 달라도
req-001이라는 같은 요청으로 식별할 수 있다.

Offset은 소비 위치 관리에 사용하고,
비즈니스 멱등성은 requestId로 관리해야 한다.
```

---

# **과제 3. Idempotency-Key와 requestHash 설계하기**

## **3-1. Idempotency-Key의 의미**

```
Idempotency-Key는 클라이언트가 하나의 논리적 작업에 부여하는
안정적인 식별자다.

동일 작업의 응답 유실, Timeout, 재연결 재시도에서는
같은 Idempotency-Key를 사용한다.

서버는 이 값을 requestId로 저장하고
이미 처리한 요청이면 비즈니스 로직을 다시 실행하지 않고
기존 상태와 결과를 반환한다.
```

## **3-2. requestId 생성 주체 비교**

```
| 방식 | 장점 | 단점 |
| --- | --- | --- |
| Client 생성 | 요청 전부터 ID가 존재하므로 응답을 받지 못해도 동일 ID로 재시도할 수 있다. 네트워크 경계 전체에서 같은 요청을 추적할 수 있다. | Client가 UUID 생성·보관·재사용 규칙을 지켜야 하며 서버가 형식과 오용을 검증해야 한다. |
| API Server 생성 | ID 형식과 생성 규칙을 서버가 통제하기 쉽다. | 첫 요청의 응답이 유실되면 Client가 서버가 만든 ID를 알지 못해 동일 요청을 식별하여 재시도하기 어렵다. |
```

```
내가 선택한 방식:

Client가 요청을 보내기 전에 UUID 형식의 requestId를 생성한다.
```

```
선택한 이유:

멱등성이 가장 필요한 상황은
서버가 요청을 처리했지만 Client가 응답을 받지 못한 상황이다.

Client가 요청 전부터 requestId를 알고 있어야
응답 유실 후에도 같은 ID로 안전하게 재시도할 수 있다.

서버는 인증된 사용자, UUID 형식, requestHash를 검증해
Client 생성 ID의 오용을 방어한다.
```

## **3-3. requestId 재사용과 신규 생성 기준**

```
| 상황 | 같은 requestId 재사용 또는 새 requestId 생성 | 이유 |
| --- | --- | --- |
| 네트워크 Timeout 후 동일 작업 재시도 | 같은 requestId 재사용 | 최초 요청의 처리 여부가 불명확하므로 같은 논리 작업으로 조회·재시도해야 한다. |
| 응답 유실 후 동일 작업 재시도 | 같은 requestId 재사용 | 서버에서 이미 처리되었을 수 있으므로 기존 결과를 받아야 한다. |
| 모바일 앱 재연결 후 동일 작업 전송 | 같은 작업을 이어가는 경우 같은 requestId 재사용 | 연결은 달라져도 비즈니스 의도는 동일하다. |
| 다른 쿠폰 이벤트 발급 요청 | 새 requestId 생성 | eventId가 다른 새로운 비즈니스 작업이다. |
| 기존 요청을 취소하고 새로운 작업 시작 | 새 requestId 생성 | 이전 작업의 재시도가 아니라 새로운 의도다. |
```

## **3-4. requestId 형식과 검증 규칙**

```
허용 형식:
RFC 4122 계열 UUID의 정규 문자열 형식
예: 4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31

최대 길이:
정규 UUID 문자열 기준 36자

빈 값 처리:
Idempotency-Key가 없거나 공백이면 400 Bad Request

허용하지 않는 값:
UUID로 파싱할 수 없는 문자열
앞뒤 공백이 포함된 값
임의 SQL 조각이나 제어 문자
서버 정책상 허용하지 않는 UUID 버전

DB 유일성:
coupon_issue_request.request_id UUID PRIMARY KEY

추가 검증:
requestId가 이미 존재하면 requestHash를 비교해
동일 요청 재시도와 잘못된 키 재사용을 구분한다.
```

## **3-5. requestHash Canonical String과 저장 컬럼**

```
Canonical String:

COUPON_ISSUE|100|10
```

```
SHA-256 결과 저장 컬럼:

coupon_issue_request.request_hash CHAR(64) NOT NULL

SHA-256의 32바이트 값을 16진수 소문자 64자로 저장한다고 가정한다.
```

## **3-6. 전체 JSON 문자열을 그대로 Hash하면 안 되는 이유**

```
다음 두 JSON은 의미상 같은 요청이다.

{ "eventId": 100, "userId": 10 }
{"userId":10,"eventId":100}

하지만 원문 문자열은 다음 요소 때문에 달라질 수 있다.

- 필드 순서
- 공백과 줄바꿈
- 숫자·문자열 직렬화 방식
- Serializer 설정
- 불필요한 선택 필드

원문 JSON을 그대로 Hash하면
같은 의미의 요청이 서로 다른 Hash를 만들 수 있다.

따라서 멱등성 판단에 필요한 안정적인 필드만
정해진 순서와 구분자로 Canonical String을 만든 뒤 Hash한다.
```

## **3-7. requestHash 포함 값 구분**

```
| 값 | 포함 여부 | 이유 |
| --- | --- | --- |
| 작업 종류 COUPON_ISSUE | 포함 | 다른 종류의 작업이 같은 ID 조합을 사용하는 충돌을 막는다. |
| eventId | 포함 | 어떤 쿠폰 이벤트를 발급하는지 결정하는 핵심 입력이다. |
| 인증된 userId | 포함 | 누구의 쿠폰을 발급하는지 결정하는 핵심 입력이다. |
| 요청 전송 시각 | 제외 | 재시도할 때 달라질 수 있어 같은 요청의 Hash를 불안정하게 만든다. |
| Trace ID | 제외 | 관측용 값이며 재시도·서비스 경계를 지날 때 바뀔 수 있다. |
| 매번 바뀌는 nonce | 제외 | 포함하면 모든 재시도가 서로 다른 요청처럼 보인다. |
| Gateway가 추가한 가변 Header | 제외 | 비즈니스 결과를 결정하지 않고 인프라에 따라 달라질 수 있다. |
```

## **3-8. Request Body의 userId를 그대로 사용하면 안 되는 이유**

```
Request Body의 userId는 Client가 임의로 변경할 수 있다.

이를 발급 대상이나 requestHash에 사용하면
공격자가 다른 사용자의 ID를 넣어 쿠폰을 요청하거나
기존 requestId의 소유자를 위조할 수 있다.

userId는 반드시 검증된 Access Token이나 Session에서 얻은
authenticatedUserId를 사용해야 한다.

Request Body에 userId가 필요하지 않다면 받지 않는 것이 안전하다.
받아야 한다면 인증된 userId와 일치하는지 별도로 검증한다.
```

## **3-9. 같은 requestId에 다른 내용이 들어온 경우**

```
HTTP Status Code:
409 Conflict

에러 코드:
REQUEST_MISMATCH

Kafka 발행 여부:
발행하지 않는다.

기존 요청 상태 변경 여부:
변경하지 않는다.

사용자 응답:
같은 Idempotency-Key가 이전 요청과 다른 내용에 사용되었음을 알리고,
새로운 작업이라면 새로운 Idempotency-Key로 요청하도록 안내한다.

중요한 점:
기존 req-001의 처리 결과를 새 eventId=200 요청의 결과처럼
반환해서도 안 되고, 기존 요청을 덮어써서도 안 된다.
```

---

# **과제 4. coupon_issue_request 테이블과 상태 전이 설계하기**

## **4-1. coupon_issue_request와 coupon_issue의 역할 차이**

```
| 테이블 | 저장하는 대상 | 표현할 수 있는 상태 |
| --- | --- | --- |
| coupon_issue_request | requestId로 식별되는 요청의 내용, 처리 생명주기, 실패 이유, 최종 결과 연결 | PENDING, PROCESSING, ISSUED, FAILED |
| coupon_issue | 실제로 발급에 성공한 사용자 쿠폰 기록 | 최종 발급 성공 기록만 저장하므로 이번 과제에서는 ISSUED만 표현 |
```

## **4-2. 요청 상태 모델**

```
| 상태 | 의미 | 최종 상태 여부 |
| --- | --- | --- |
| PENDING | 요청 행은 생성되었지만 Kafka 발행 또는 Consumer의 최종 처리가 끝나지 않았다. | 아니오 |
| PROCESSING | Consumer가 조건부 상태 전이로 처리 권한을 얻어 발급 트랜잭션을 수행 중이다. | 아니오 |
| ISSUED | 수량 증가, 발급 기록, 요청 성공 상태가 함께 Commit되었다. | 예 |
| FAILED | SOLD_OUT, DUPLICATE_USER처럼 재시도해도 결과가 바뀌지 않는 비즈니스 실패가 확정되었다. | 예 |
```

## **4-3. 상태 전이 다이어그램**

```
                     허용

PENDING
   |
   | 처리 권한 획득
   v
PROCESSING
   |
   +---------------------------+
   |                           |
   | 발급 Commit               | 최종 비즈니스 실패 Commit
   v                           v
ISSUED                       FAILED

                     금지

ISSUED  -X-> PROCESSING
ISSUED  -X-> FAILED
FAILED  -X-> PROCESSING
FAILED  -X-> ISSUED

최종 상태는 불변으로 취급한다.
같은 메시지가 다시 도착하면 상태를 변경하지 않고
기존 결과로 정상 종료한다.
```

## **4-4. 외부 조회에서 PROCESSING이 거의 보이지 않는 이유**

```
이번 과제에서는 다음 전이를 하나의 트랜잭션 안에서 수행한다.

BEGIN
PENDING → PROCESSING
수량 증가
coupon_issue INSERT
PROCESSING → ISSUED 또는 FAILED
COMMIT

PostgreSQL의 다른 세션은 Commit되지 않은 중간 변경을 볼 수 없다.

따라서 외부에서는 일반적으로 다음처럼 보인다.

Commit 전  → PENDING
Commit 후  → ISSUED 또는 FAILED

PROCESSING은 트랜잭션 내부에서
처리 권한을 표시하기 위해 잠시 사용된다.

PROCESSING을 별도 트랜잭션으로 Commit하는 구조라면
외부에서도 보이지만, 그때는 Lease와 고착 복구 정책이 필요하다.
```

## **4-5. coupon_issue_request DDL**

```sql
CREATE TABLE coupon_issue_request (
    request_id UUID PRIMARY KEY,

    event_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,

    request_hash CHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL,

    failure_code VARCHAR(50),
    failure_message VARCHAR(255),

    coupon_issue_id BIGINT,

    requested_at TIMESTAMPTZ NOT NULL,
    processing_started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_coupon_issue_request_event
        FOREIGN KEY (event_id)
        REFERENCES coupon_event(id),

    CONSTRAINT fk_coupon_issue_request_issue
        FOREIGN KEY (coupon_issue_id)
        REFERENCES coupon_issue(id),

    CONSTRAINT uk_coupon_issue_request_issue
        UNIQUE (coupon_issue_id),

    CONSTRAINT chk_coupon_issue_request_status
        CHECK (
            status IN (
                'PENDING',
                'PROCESSING',
                'ISSUED',
                'FAILED'
            )
        ),

    CONSTRAINT chk_coupon_issue_request_failure
        CHECK (
            (
                status = 'FAILED'
                AND failure_code IS NOT NULL
            )
            OR
            (
                status <> 'FAILED'
                AND failure_code IS NULL
                AND failure_message IS NULL
            )
        ),

    CONSTRAINT chk_coupon_issue_request_completed
        CHECK (
            (
                status IN ('ISSUED', 'FAILED')
                AND completed_at IS NOT NULL
            )
            OR
            (
                status IN ('PENDING', 'PROCESSING')
                AND completed_at IS NULL
            )
        ),

    CONSTRAINT chk_coupon_issue_request_issue_result
        CHECK (
            (
                status = 'ISSUED'
                AND coupon_issue_id IS NOT NULL
            )
            OR
            (
                status <> 'ISSUED'
                AND coupon_issue_id IS NULL
            )
        ),

    CONSTRAINT chk_coupon_issue_request_processing
        CHECK (
            (
                status = 'PROCESSING'
                AND processing_started_at IS NOT NULL
            )
            OR
            status <> 'PROCESSING'
        )
);
```

## **4-6. 요청 상태 테이블에 UNIQUE(event_id, user_id)를 두지 않는 이유**

```
coupon_issue_request는 최종 발급 원장이 아니라
사용자가 시도한 요청의 이력을 저장하는 테이블이다.

같은 사용자가 서로 다른 requestId로 요청하면
다음 두 이력이 모두 남아야 한다.

req-A → ISSUED
req-B → FAILED / DUPLICATE_USER

coupon_issue_request에 UNIQUE(event_id, user_id)를 두면
req-B 행 자체를 저장할 수 없어
두 번째 요청의 실패 이유와 결과를 조회할 수 없다.

역할을 다음처럼 분리한다.

coupon_issue_request
→ 여러 요청 이력 저장

coupon_issue의 UNIQUE(event_id, user_id)
→ 실제 최종 발급은 사용자당 한 건만 허용
```

## **4-7. 출발 상태를 포함한 올바른 상태 전이 SQL**

```sql
UPDATE coupon_issue_request
SET status = 'ISSUED',
    coupon_issue_id = :couponIssueId,
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING';
```

```
출발 상태를 조건에 포함하면
오직 현재 처리 권한을 가진 정상 흐름만
PROCESSING에서 ISSUED로 전이할 수 있다.

조건이 없으면 오래된 Consumer가
이미 FAILED인 요청을 ISSUED로 덮어쓰거나
이미 끝난 상태를 다시 변경할 수 있다.

UPDATE 후 affected rows가 반드시 1인지 확인해야 한다.
```

## **4-8. 최종 상태 변경이 0행인 원인**

```
1. requestId에 해당하는 요청 행이 존재하지 않는다.

2. 요청 상태가 PROCESSING이 아니라
   PENDING, ISSUED, FAILED 등 다른 상태다.

3. 다른 처리자가 먼저 최종 상태를 변경했거나
   메시지와 요청 행의 불일치로 정상 처리 권한이 없다.

0행인데도 coupon_issue와 issued_count를 Commit하면
요청 상태와 실제 발급 결과가 어긋나므로
정상 발급 경로에서는 전체 트랜잭션을 Rollback해야 한다.
```

## **4-9. 운영 복구용 인덱스**

```sql
CREATE INDEX idx_coupon_issue_request_status_updated
ON coupon_issue_request (
    status,
    updated_at
);
```

```
예를 들어 다음 조건을 효율적으로 조회하는 데 사용한다.

status = 'PENDING'
AND updated_at < :pendingCutoff

status = 'PROCESSING'
AND updated_at < :processingCutoff

실제 트래픽 분포와 조회 패턴에 따라
PENDING과 PROCESSING용 Partial Index를 분리할 수도 있다.
```

## **4-10. 상태 보관 기간과 Idempotency-Key 유효 기간**

```
Idempotency-Key 유효 기간
≤
요청 상태 보관 기간
```

```
Client가 아직 과거 Idempotency-Key를 재사용할 수 있는데
coupon_issue_request 행을 먼저 삭제하면
서버는 과거 요청의 존재와 requestHash를 확인할 수 없다.

그 결과 동일한 재시도를 신규 요청으로 잘못 판단할 수 있다.

따라서 요청 상태 행은 최소한
Idempotency-Key 재사용 가능 기간보다 오래 보관해야 한다.
삭제가 필요하면 그 이후에 삭제하거나 Archive한다.
```

---

# **과제 5. API Server의 멱등 요청 접수 흐름 설계하기**

## **5-1. API Server 전체 처리 흐름**

```
Client
  |
  | Idempotency-Key: requestId
  v
API Server
  |
  | 인증된 userId 확인
  | requestId 형식 검증
  | requestHash 생성
  v
Redis Lua Script
  |
  +-- SOLD_OUT
  |     → DB 요청 행 생성 안 함
  |     → Kafka 발행 안 함
  |     → 품절 응답
  |
  +-- DUPLICATE_USER
  |     → DB 요청 행 생성 안 함
  |     → Kafka 발행 안 함
  |     → 사용자 중복 응답
  |
  +-- IDEMPOTENT_RETRY
  |     |
  |     v
  |   requestId로 DB 조회
  |     |
  |     +-- 행 존재 + requestHash 일치
  |     |     → PENDING / ISSUED / FAILED 기존 결과 반환
  |     |
  |     +-- 행 존재 + requestHash 불일치
  |     |     → 409 REQUEST_MISMATCH
  |     |
  |     +-- 행 없음
  |           → 정상 PENDING으로 단정 금지
  |           → 짧은 재조회 또는 DB 등록 복구
  |           → 장기 미복구 시 Redis 자리 보상
  |
  +-- SUCCESS
        |
        v
  INSERT coupon_issue_request(PENDING)
  ON CONFLICT(request_id) DO NOTHING
        |
        +-- affected rows = 0
        |     |
        |     v
        |   기존 요청 조회
        |     |
        |     +-- Hash 불일치
        |     |     → 409 REQUEST_MISMATCH
        |     |
        |     +-- Hash 일치
        |           → 기존 상태 반환
        |           → Redis가 새로 차지한 자리는
        |              소유권 확인 후 원인별 보상
        |
        +-- affected rows = 1
              |
              v
         Kafka 발행
              |
              +-- ACK 성공
              |     → 202 Accepted / PENDING
              |
              +-- 실패 또는 Timeout
                    → PENDING과 Kafka 사이 불일치 가능
                    → 발행 재시도·복구 대상 기록
                    → Timeout은 Broker 저장 여부가 불명확
```

## **5-2. Redis 결과별 API 처리**

```
| Redis 결과 | DB 요청 상태 등록 | Kafka 발행 | API 응답 방향 |
| --- | --- | --- | --- |
| SUCCESS | coupon_issue_request를 PENDING으로 등록한다. | 최초 등록에 성공한 요청만 발행한다. | Kafka ACK까지 확인했다면 202 Accepted와 PENDING을 반환한다. |
| IDEMPOTENT_RETRY | 새 행을 만들지 않고 기존 requestId 행을 조회한다. 행이 없으면 복구 흐름으로 보낸다. | 기존 행이 정상 존재하면 새 메시지를 만들지 않는다. 오래된 PENDING 발행 누락은 별도 복구 정책으로 처리한다. | 기존 PENDING, ISSUED, FAILED 상태와 결과를 반환한다. |
| DUPLICATE_USER | 기본 방식에서는 요청 행을 만들지 않는다. | 발행하지 않는다. | 이미 같은 이벤트를 요청한 사용자라는 응답을 반환한다. |
| SOLD_OUT | 기본 방식에서는 요청 행을 만들지 않는다. | 발행하지 않는다. | Redis 기준 품절 응답을 반환한다. |
```

## **5-3. SELECT 후 INSERT의 Race Condition**

```
시간       API Server A               API Server B
----------------------------------------------------------
T1         req-001 SELECT → 없음
T2                                    req-001 SELECT → 없음
T3         최초 요청이라고 판단
T4                                    최초 요청이라고 판단
T5         INSERT 시도
T6                                    INSERT 시도

두 서버 모두 사전 조회에서는 행이 없다고 보므로
애플리케이션의 SELECT만으로 최초 등록자를 결정할 수 없다.

문제의 이름:
Check-Then-Act Race Condition

해결:
request_id Primary Key와
INSERT ... ON CONFLICT DO NOTHING의 결과로
최초 등록자를 결정한다.
```

## **5-4. DB 유일성 제약을 이용한 최초 등록 SQL**

```sql
INSERT INTO coupon_issue_request (
    request_id,
    event_id,
    user_id,
    request_hash,
    status,
    requested_at
)
VALUES (
    :requestId,
    :eventId,
    :authenticatedUserId,
    :requestHash,
    'PENDING',
    :requestedAt
)
ON CONFLICT (request_id) DO NOTHING;
```

## **5-5. INSERT 영향받은 행 수 처리**

```
affected rows = 1
→ 이 실행이 requestId의 최초 등록 권한을 얻었다.
→ PENDING 행을 저장한 뒤 Kafka 발행을 진행한다.

affected rows = 0
→ 같은 requestId의 기존 행이 이미 존재한다.
→ 기존 행을 조회해 requestHash와 인증된 userId를 확인한다.
→ Hash가 같으면 기존 상태를 반환한다.
→ Hash가 다르면 409 REQUEST_MISMATCH로 거절한다.
```

## **5-6. 기존 행의 requestHash 확인 결과**

```
| 조건 | 판단 | 처리 |
| --- | --- | --- |
| requestId 같음 + requestHash 같음 | 동일 논리 요청의 재시도 또는 동시 요청 | 새 비즈니스 작업과 새 Kafka 메시지를 만들지 않고 기존 상태와 결과를 반환한다. |
| requestId 같음 + requestHash 다름 | Idempotency-Key가 다른 요청 내용에 잘못 재사용됨 | 409 Conflict와 REQUEST_MISMATCH를 반환하고 기존 행을 변경하지 않는다. |
```

## **5-7. IDEMPOTENT_RETRY인데 DB 요청 행이 없는 경우**

```
발생 가능한 흐름:

Redis Lua Script SUCCESS
→ Redis에 userId와 최초 requestId 저장
→ API Server 장애
→ coupon_issue_request INSERT 실행 안 됨

이후 같은 요청이 재시도되면
Redis에는 같은 userId와 requestId가 있으므로
IDEMPOTENT_RETRY가 반환되지만 DB 행은 없다.
```

```
무조건 정상 PENDING으로 응답하면 안 되는 이유:

PENDING은 내구성 있는 DB 요청 행이 존재한다는 전제의 상태다.
DB 행이 없으면 Consumer가 검증할 요청도 없고
Kafka 메시지가 발행되었다는 보장도 없다.

사용자는 영원히 존재하지 않는 요청의 완료를 기다릴 수 있다.
```

```
재조회·복구·보상 방향:

1. 동시 요청의 DB INSERT가 진행 중일 수 있으므로 짧게 재조회한다.
2. 여전히 없으면 같은 requestId와 requestHash로 DB 등록을 재시도한다.
3. 등록에 성공하면 Kafka 발행을 수행한다.
4. 복구를 자동 수행할 수 없거나 일정 시간 이상 지속되면
   Redis에 저장된 현재 자리의 requestId 소유권을 확인한다.
5. 소유권이 같은 경우에만 Lua Script로 Redis 자리를 보상한다.
6. 누락 메트릭과 경고 로그를 남긴다.
```

## **5-8. Redis SUCCESS 후 DB INSERT가 충돌한 경우**

```
기존 requestHash 확인:

기존 DB 행의 requestHash와 현재 요청의 requestHash를 비교한다.
다르면 409 REQUEST_MISMATCH다.
같으면 과거 동일 요청이 이미 DB에 등록된 것이다.

Kafka 발행 여부:

기존 상태가 이미 ISSUED 또는 FAILED라면 새로 발행하지 않는다.
PENDING이라면 과거 발행 여부가 불명확할 수 있으므로
중복 안전한 발행 복구 정책 또는 Outbox를 사용한다.
현재 API 호출이 무조건 새 메시지를 만드는 방식은 피한다.

Redis에서 새로 차지한 자리 처리:

Redis 초기화로 과거 요청이 새 자리처럼 등록된 것이므로
기존 DB 결과와 Redis의 현재 requestId 소유권을 확인한다.
불필요하게 다시 차지한 자리라면 Lua Script로 조건부 보상한다.
단순 SREM처럼 다른 최신 요청의 자리를 지우면 안 된다.

API 응답:

requestHash가 같다면 DB의 기존 PENDING, ISSUED, FAILED 상태를 반환한다.
requestHash가 다르면 409 Conflict를 반환한다.
```

## **5-9. 최초 요청 접수 성공 응답**

```
HTTP Status Code:
202 Accepted
```

```json
{
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다."
}
```

```
202 Accepted와 PENDING은
비동기 처리 흐름에 요청이 접수되었다는 의미다.

Consumer의 수량 확보, coupon_issue 저장,
request ISSUED 트랜잭션이 아직 남아 있으므로
최종 발급 성공을 의미하지 않는다.
```

## **5-10. 동일 requestId 재요청 응답**

### **5-10-1. PENDING**

```json
{
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "status": "PENDING",
  "message": "쿠폰 발급 요청을 처리하고 있습니다."
}
```

```
HTTP Status는 API 정책에 따라
202 Accepted 또는 기존 요청 조회 의미의 200 OK를 사용할 수 있다.
중요한 것은 새 요청과 새 Kafka 메시지를 만들지 않는 것이다.
```

### **5-10-2. ISSUED**

```json
{
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "status": "ISSUED",
  "couponIssueId": 5001,
  "message": "이미 쿠폰 발급이 완료되었습니다."
}
```

### **5-10-3. FAILED**

```json
{
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "status": "FAILED",
  "failureCode": "SOLD_OUT",
  "message": "쿠폰이 모두 소진되었습니다."
}
```

## **5-11. 같은 requestId에 다른 내용이 들어온 경우**

```json
{
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "errorCode": "REQUEST_MISMATCH",
  "message": "동일한 요청 식별자가 다른 요청 내용에 사용되었습니다."
}
```

```
HTTP Status Code:
409 Conflict

이유:
requestId는 이미 기존 요청에 귀속되어 있다.
새 요청 내용으로 덮어쓰거나 기존 결과를 잘못 반환할 수 없으므로
충돌로 거절하고 새로운 작업에는 새 requestId를 사용하게 한다.
```

## **5-12. 상태 조회 API 인증 조건**

```
GET /api/coupon-issue-requests/{requestId}

Authorization에서 authenticatedUserId를 얻는다.
```

```sql
SELECT request_id,
       event_id,
       status,
       failure_code,
       failure_message,
       coupon_issue_id,
       requested_at,
       completed_at
FROM coupon_issue_request
WHERE request_id = :requestId
  AND user_id = :authenticatedUserId;
```

```
requestId만으로 조회하면 안 되는 이유:

requestId가 로그, URL, 브라우저 기록 등에서 노출되었을 때
다른 사용자가 타인의 쿠폰 요청 상태와 실패 이유,
couponIssueId를 조회할 수 있다.

요청 ID는 인증 수단이 아니다.
반드시 인증된 사용자 소유권을 함께 확인해야 한다.

또한 Redis에서 SOLD_OUT 또는 DUPLICATE_USER로 차단되어
DB 요청 행이 생성되지 않은 requestId는
기본 방식의 조회 API에서 404가 될 수 있음을 문서화해야 한다.
```

---

# **과제 6. Consumer의 조건부 상태 전이와 처리 권한 설계하기**

## **6-1. Kafka 메시지와 요청 상태 행 일치 검증**

```
| 필드 | Kafka Message | coupon_issue_request | 불일치 시 처리 |
| --- | --- | --- | --- |
| requestId | message.requestId | request_id | 요청 행을 찾을 수 없으면 발급하지 않고 등록 불일치 복구·DLQ 정책으로 보낸다. |
| eventId | message.eventId | event_id | 발급을 중단하고 REQUEST_MISMATCH 또는 손상된 메시지로 기록한다. |
| userId | message.userId | user_id | 발급을 중단하고 REQUEST_MISMATCH 또는 보안 이상으로 기록한다. |
```

```
메시지 내용 불일치를 재시도 대상으로 두기 어려운 이유:

requestId는 같지만 eventId나 userId가 다르다는 것은
일시적인 DB 연결 오류가 아니라
생산자 버그, 데이터 손상, 잘못된 키 재사용을 뜻한다.

같은 메시지를 그대로 다시 처리해도 값은 바뀌지 않으므로
무한 Retry로 해결되지 않는다.

발급은 금지하고,
안정적인 실패 결과 또는 DLQ·운영 확인 대상으로 분류한다.
```

## **6-2. SELECT 후 UPDATE Race Condition**

```
시간       Consumer A                  Consumer B
------------------------------------------------------------
T1         status SELECT → PENDING
T2                                     status SELECT → PENDING
T3         처리 가능 판단
T4                                     처리 가능 판단
T5         PROCESSING UPDATE
T6                                     PROCESSING UPDATE
T7         발급 로직 실행              발급 로직 실행

두 Consumer 모두 SELECT 시점에는 PENDING을 보았기 때문에
애플리케이션의 if (status == PENDING) 검사는
한 Consumer에게만 처리 권한을 주지 못한다.

상태 확인과 변경을 분리한 Check-Then-Act Race Condition이다.
```

## **6-3. 조건부 UPDATE로 처리 권한 획득**

```sql
UPDATE coupon_issue_request
SET status = 'PROCESSING',
    processing_started_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PENDING';
```

## **6-4. 처리 권한 UPDATE의 영향받은 행 수**

```
affected rows = 1
→ 내가 PENDING을 PROCESSING으로 변경했다.
→ 이 트랜잭션이 발급 로직을 계속 수행할 권한을 얻었다.

affected rows = 0
→ 요청 행이 없거나 상태가 이미 PENDING이 아니다.
→ 다른 Consumer가 처리 중이거나
   이미 ISSUED 또는 FAILED로 끝났을 수 있다.
→ 현재 상태를 다시 조회해 원인별로 처리한다.
```

## **6-5. 두 Consumer가 동시에 같은 requestId를 처리할 때**

```
현재 상태:
PENDING

Consumer A:
행에 대한 Write Lock을 먼저 획득한다.
PENDING → PROCESSING 조건부 UPDATE가 1행 성공한다.
이후 같은 트랜잭션에서 발급 로직을 진행한다.

Consumer B:
같은 행을 UPDATE하려다 Consumer A의 트랜잭션 종료까지 대기한다.
A가 Commit하면 최신 행 상태를 기준으로 WHERE를 다시 평가한다.
상태가 더 이상 PENDING이 아니므로 affected rows = 0이 된다.
발급 로직을 실행하지 않는다.
```

```
PostgreSQL READ COMMITTED에서는
UPDATE가 잠긴 행을 기다린 뒤
최신 Commit 상태를 기준으로 WHERE 조건을 다시 평가한다.

따라서 애플리케이션에서 SELECT와 UPDATE를 분리하지 않고
WHERE status = 'PENDING' 조건부 UPDATE 한 문장으로 실행하면
처리 권한을 한 트랜잭션으로 제한할 수 있다.
```

## **6-6. REPEATABLE READ 이상에서 달라지는 점**

```
PostgreSQL REPEATABLE READ 이상에서는
동시에 수정된 행을 대기한 뒤 단순히 0행으로 처리하는 대신
could not serialize access 같은 직렬화 오류가 발생할 수 있다.

이 경우 특정 SQL 한 문장만 다시 실행하는 것이 아니라
현재 트랜잭션 전체를 Rollback한 뒤 재시도해야 한다.

이번 과제는 PostgreSQL 기본 격리 수준인
READ COMMITTED의 동작을 전제로 한다.
```

## **6-7. affected rows가 0일 때 상태별 처리**

```
| 조회 결과 | 의미 | Consumer 처리 | Offset 처리 방향 |
| --- | --- | --- | --- |
| 요청 행 없음 | Kafka 메시지와 DB 요청 등록이 불일치한다. | 즉시 발급하지 않는다. 짧은 복구 재시도 후에도 없으면 운영 알림 또는 DLQ로 보낸다. | 복구 정책이 끝나기 전에는 Commit하지 않는다. 영구 불일치로 DLQ 등에 내구성 있게 넘긴 뒤에는 Commit할 수 있다. |
| PENDING | 정상이라면 조건부 UPDATE가 성공해야 하므로 경쟁·격리 수준·트랜잭션 경계를 다시 확인해야 한다. | 트랜잭션 전체 재시도 또는 이상 상황으로 기록한다. | 결과가 확정되기 전에는 Commit하지 않는다. |
| PROCESSING | 별도 Claim 트랜잭션 구조라면 다른 Consumer가 처리 중이다. 단일 트랜잭션 구조에서 Commit된 PROCESSING은 설계 가정 위반이다. | 단일 트랜잭션이면 재시도·알림, 별도 Claim이면 Lease와 소유자를 확인한다. | 기존 처리자의 실패 가능성을 고려해 무조건 즉시 Commit하지 않는다. |
| ISSUED | 동일 요청이 이미 최종 성공했다. | 새 발급 없이 멱등 성공으로 종료한다. | Commit |
| FAILED | 동일 요청이 이미 최종 비즈니스 실패했다. | 기존 실패 결과를 유지하고 정상 종료한다. | Commit |
```

## **6-8. ISSUED 또는 FAILED 중복 메시지를 예외로 던지면 안 되는 이유**

```
ISSUED와 FAILED는 이미 확정된 최종 상태다.

이 상태에서 중복 메시지가 도착한 것은
비즈니스 처리 실패가 아니라 At-Least-Once 전달에서
정상적으로 예상되는 재전달이다.

이를 DuplicateMessageException으로 처리하면 다음 문제가 생긴다.

ISSUED 확인
→ 예외
→ Retry Topic
→ 다시 ISSUED 확인
→ 다시 예외
→ DLQ

이미 성공한 요청이 장애 메시지처럼 취급되고
Retry와 DLQ를 불필요하게 점유한다.

따라서 최종 상태를 확인한 뒤
아무 비즈니스 변경 없이 성공적으로 종료해야 한다.
```

## **6-9. 아무 변경 없이 종료하는 것도 성공적인 처리인 이유**

```
Consumer의 목적은 메시지를 받을 때마다
반드시 새로운 INSERT나 UPDATE를 만드는 것이 아니다.

메시지가 나타내는 요청의 결과가 DB에
이미 정확하게 반영되어 있는지 보장하는 것이 목적이다.

ISSUED라면 발급 결과가 이미 존재하고,
FAILED라면 최종 실패 결과가 이미 존재한다.

따라서 기존 결과를 확인하고 재실행을 생략하는 것은
멱등 함수가 같은 입력에 같은 결과를 반환하는 정상 동작이다.

이 경우 메시지는 성공적으로 처리된 것이므로
Offset을 Commit할 수 있다.
```

---

# **과제 7. 발급 트랜잭션과 Rollback 이후 상태 확정 설계하기**

## **7-1. 발급 트랜잭션 포함 범위**

```
| 작업 | 발급 트랜잭션 포함 여부 | 이유 |
| --- | --- | --- |
| PENDING → PROCESSING | 포함 | 발급 처리 권한과 이후 DB 변경을 같은 원자적 결과로 만든다. |
| coupon_event 수량 확보 | 포함 | 발급 기록과 수량 증가가 함께 성공하거나 함께 취소되어야 한다. |
| coupon_issue INSERT | 포함 | 실제 발급 원장과 수량을 일치시켜야 한다. |
| request ISSUED / FAILED 변경 | 포함 | 사용자가 조회하는 최종 상태와 실제 발급 결과를 일치시켜야 한다. |
| 이메일 발송 | 미포함 | 외부 I/O가 느리거나 실패하면 DB Lock과 Connection을 오래 점유한다. |
| 앱 푸시 발송 | 미포함 | 외부 서비스 장애가 핵심 발급 트랜잭션에 전파되면 안 된다. |
| 마이페이지 조회 모델 갱신 | 미포함 | 별도 관심사이며 결과 이벤트로 비동기 갱신할 수 있다. |
| 외부 API 호출 | 미포함 | 로컬 DB 트랜잭션으로 원자성을 보장할 수 없고 처리 시간이 길다. |
```

## **7-2. 전체 발급 트랜잭션 흐름**

```
Kafka 메시지 수신
        |
        v
요청 행과 메시지 내용 검증
        |
        +-- 행 없음 / 내용 불일치
        |      → 발급 금지
        |      → 복구 또는 최종 오류 정책
        |
        v
DB Transaction 시작
        |
        v
PENDING → PROCESSING 조건부 UPDATE
        |
        +-- 0행
        |     → 기존 상태 조회
        |     +-- ISSUED / FAILED
        |     |     → 멱등 성공 종료
        |     +-- 그 외
        |           → Rollback·재시도 또는 운영 확인
        |
        +-- 1행
              |
              v
      coupon_event 조건부 UPDATE
              |
              +-- 0행
              |     → 원인 조회
              |     +-- 수량 소진
              |           → request FAILED / SOLD_OUT
              |           → coupon_issue 없음
              |           → Commit
              |
              +-- 1행
                    |
                    v
            coupon_issue INSERT
                    |
                    +-- 성공
                    |     |
                    |     v
                    |  PROCESSING → ISSUED
                    |     |
                    |     +-- 1행 → Commit
                    |     |
                    |     +-- 0행 → 전체 Rollback
                    |
                    +-- UNIQUE(event_id, user_id) 위반
                          |
                          v
                    첫 트랜잭션 전체 Rollback
                    - PROCESSING → PENDING 원복
                    - issued_count 증가 취소
                          |
                          v
                    새 트랜잭션 시작
                    - 기존 coupon_issue 확인
                    - PENDING → FAILED / DUPLICATE_USER
                    - Commit

일시적 DB 오류
→ 전체 Rollback
→ request는 PENDING
→ Listener 예외 재전파
→ Kafka 재전달
```

## **7-3. 정상 발급 SQL 흐름**

```sql
BEGIN;

-- 1. 한 Consumer만 PENDING 요청의 처리 권한을 얻는다.
UPDATE coupon_issue_request
SET status = 'PROCESSING',
    processing_started_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PENDING';
-- affected rows = 1인 경우에만 계속한다.

-- 2. 수량이 남아 있을 때만 issued_count를 증가시킨다.
UPDATE coupon_event
SET issued_count = issued_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :eventId
  AND issued_count < total_quantity;
-- affected rows = 1인 경우에만 발급 INSERT를 수행한다.

-- 3. 최종 발급 기록을 저장하고 ID를 얻는다.
INSERT INTO coupon_issue (
    event_id,
    user_id,
    status,
    issued_at
)
VALUES (
    :eventId,
    :userId,
    'ISSUED',
    CURRENT_TIMESTAMP
)
RETURNING id;

-- 4. 요청 상태를 최종 성공으로 변경한다.
UPDATE coupon_issue_request
SET status = 'ISSUED',
    coupon_issue_id = :couponIssueId,
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING';
-- affected rows = 1이어야 한다.

COMMIT;
```

## **7-4. 각 단계에서 확인할 영향받은 행 수**

```
| 단계 | 정상적으로 기대하는 row 수 | 기대값과 다를 때 처리 |
| --- | ---: | --- |
| 처리 권한 획득 | 1 | 0이면 발급을 진행하지 않고 기존 요청 상태를 확인한다. |
| 수량 UPDATE | 성공 경로는 1, 실패 경로는 0 | 0이면 이벤트 존재·상태·수량을 확인해 SOLD_OUT 등 원인을 확정한다. |
| 최종 ISSUED 변경 | 1 | 0이면 요청 상태와 발급 결과가 어긋나므로 전체 트랜잭션을 Rollback한다. |
```

## **7-5. ISSUED 변경 0행인데 발급 결과만 Commit하면 안 되는 이유**

```
issued_count와 coupon_issue만 Commit되고
request가 ISSUED로 바뀌지 않으면 다음 불일치가 생긴다.

실제 DB 발급 기록
→ 존재

요청 상태 조회
→ PENDING 또는 다른 상태

사용자는 발급되지 않았다고 오해해 재시도할 수 있고,
운영 복구 작업도 완료 요청을 미처리 요청으로 판단할 수 있다.

수량, 발급 기록, 요청 최종 상태는 하나의 비즈니스 결과이므로
마지막 상태 변경이 0행이면 전체를 Rollback해야 한다.
```

## **7-6. DB 기준 수량 소진 처리**

```sql
UPDATE coupon_issue_request
SET status = 'FAILED',
    failure_code = 'SOLD_OUT',
    failure_message = '쿠폰이 모두 소진되었습니다.',
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING';
```

```
발급 행 생성 여부:

coupon_issue를 생성하지 않는다.
수량 조건부 UPDATE가 0행이므로 확보한 쿠폰이 없다.
```

```
같은 메시지가 다시 들어왔을 때 처리:

request = FAILED와 failure_code = SOLD_OUT을 확인한다.
수량 UPDATE와 발급 INSERT를 다시 수행하지 않는다.
기존 최종 실패로 정상 종료하고 Offset을 Commit한다.
```

## **7-7. 서로 다른 requestId의 사용자 중복 발급**

```
첫 번째 트랜잭션의 상태:

coupon_issue INSERT의 UNIQUE(event_id, user_id) 위반으로
PostgreSQL 트랜잭션이 실패 상태가 된다.
전체 트랜잭션을 Rollback해야 한다.

PENDING → PROCESSING 전이의 결과:

같은 트랜잭션의 변경이므로 Rollback되어 취소된다.
req-B의 상태는 다시 PENDING이다.

앞에서 증가한 issued_count의 결과:

같은 트랜잭션에서 증가했으므로 함께 Rollback된다.
실제 수량은 증가하지 않는다.

같은 트랜잭션에서 FAILED로 바꿀 수 있는가?:

일반적인 PostgreSQL 트랜잭션에서는 할 수 없다.
제약 조건 위반 후 현재 트랜잭션은 실패 상태이므로
Rollback 전에는 정상 UPDATE를 계속 실행할 수 없다.
```

## **7-8. UNIQUE 위반 Rollback 후 별도 트랜잭션 처리 순서**

```
1. UNIQUE 위반이 발생한 첫 발급 트랜잭션을 전체 Rollback한다.

2. 새로운 DB 트랜잭션을 시작한다.

3. eventId와 userId로 기존 coupon_issue를 조회해
   실제 기존 발급이 존재하는지 확인한다.

4. 현재 req-B가 PENDING이고 요청 내용이 일치하는지 확인한 뒤
   PENDING → FAILED / DUPLICATE_USER 조건부 UPDATE를 수행한다.

5. 영향받은 행 수가 1인지 확인하고 Commit한다.
   0이면 다른 처리자가 상태를 바꾸었을 수 있으므로 현재 상태를 재확인한다.
```

## **7-9. Rollback 후 PENDING에서 FAILED로 변경**

```sql
UPDATE coupon_issue_request
SET status = 'FAILED',
    failure_code = 'DUPLICATE_USER',
    failure_message = '이미 발급받은 사용자입니다.',
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PENDING';
```

```
왜 PROCESSING이 아닌가:

PENDING → PROCESSING 전이는
UNIQUE 위반이 발생한 첫 트랜잭션 안에서 실행되었다.

그 트랜잭션을 Rollback하면
PROCESSING 전이도 함께 취소되어 DB에는 다시 PENDING이 남는다.

새 트랜잭션에서 WHERE status = 'PROCESSING'을 사용하면
항상 0행이 되어 요청이 PENDING에 고착될 수 있다.
```

## **7-10. 일시적 DB 오류 처리**

```
요청 상태:
전체 Rollback 후 PENDING

issued_count:
증가가 있었다면 Rollback되어 원래 값으로 복구

coupon_issue:
저장되지 않음

FAILED 저장 여부:
저장하지 않음

Kafka Listener 예외 처리:
예외를 삼키지 않고 다시 던진다.
Offset을 완료 처리하지 않아 Kafka 재전달 또는
설정된 Retry 정책으로 다시 처리되게 한다.
```

## **7-11. 최종 비즈니스 실패와 일시적 시스템 오류 비교**

```
| 오류 | 최종 FAILED 저장 여부 | Kafka 재시도 여부 | 이유 |
| --- | --- | --- | --- |
| DB 연결 일시 실패 | 아니오 | 예 | 연결이 회복되면 같은 요청이 성공할 수 있다. |
| Deadlock | 아니오 | 예 | 경쟁 상황에 따른 일시적 트랜잭션 실패이며 전체 트랜잭션 재시도로 해결될 수 있다. |
| SOLD_OUT | 예 | 아니오 | DB 수량이 소진된 최종 비즈니스 결과다. |
| DUPLICATE_USER | 예 | 아니오 | 기존 발급이 확인된 최종 비즈니스 결과다. |
| 메시지와 요청 행 불일치 | REQUEST_MISMATCH 등으로 확정하거나 DLQ에 내구성 있게 기록 | 동일 메시지의 일반 재시도는 아니오 | 같은 메시지를 반복해도 eventId나 userId 불일치는 사라지지 않는다. |
```

---

# **과제 8. Kafka Offset과 중복 메시지 처리 설계하기**

## **8-1. 안전한 처리 순서**

```
1. Kafka 메시지를 읽는다.

2. requestId로 coupon_issue_request를 조회하고
   eventId와 userId가 메시지와 일치하는지 검증한다.

3. DB 트랜잭션을 시작한다.

4. PENDING → PROCESSING 조건부 UPDATE로 처리 권한을 얻는다.
   0행이면 기존 상태를 확인한다.

5. 권한을 얻은 경우 수량 확보, coupon_issue 저장,
   request ISSUED 또는 FAILED 확정을 수행한다.

6. DB 트랜잭션을 Commit한다.

7. Kafka Offset을 Commit한다.

핵심 순서:
DB 결과 확정 → Kafka Offset Commit
```

## **8-2. DB Commit과 Offset Commit의 역할 차이**

```
| 항목 | 의미 |
| --- | --- |
| DB Commit | 쿠폰 수량, 발급 기록, 요청 최종 상태라는 비즈니스 결과를 내구성 있게 확정한다. |
| Kafka Offset Commit | 해당 Consumer Group이 해당 Partition 위치까지 메시지를 처리했다고 Kafka에 기록한다. |
```

```
DB Commit은 “쿠폰 발급 결과가 무엇인가?”를 결정한다.

Offset Commit은 “이 Consumer Group이 어디까지 읽었는가?”를 결정한다.

Offset이 Commit되었다고 DB 발급 성공이 자동 보장되는 것이 아니며,
DB가 Commit되었다고 Offset이 자동으로 함께 Commit되는 것도 아니다.
```

## **8-3. Offset을 먼저 Commit한 뒤 DB 처리가 실패한 경우**

```
Kafka 상태:

Consumer Group Offset은 해당 Record 뒤로 이동했다.
Kafka는 이 Consumer Group이 메시지를 처리한 것으로 본다.
정상적인 재시작에서는 해당 Record를 다시 전달하지 않는다.
```

```
DB 상태:

발급 트랜잭션이 실패했으므로
request는 PENDING이거나 요청 행이 없고,
issued_count 증가와 coupon_issue 발급 기록도 없다.
```

```
사용자 요청에 발생하는 문제:

처리해야 할 메시지는 다시 전달되지 않는데
DB에는 최종 결과가 없다.

사용자는 요청이 계속 PENDING에 머물거나
실제로는 발급되지 않은 요청을 영원히 기다리게 된다.
메시지가 유실된 것과 같은 결과다.
```

## **8-4. DB Commit 후 Offset Commit 전 Consumer 종료**

```
Consumer 재시작 후 발생하는 일:

DB에는 ISSUED, coupon_issue, issued_count 증가가 남아 있지만
Kafka에는 완료 Offset이 반영되지 않았다.

따라서 같은 Topic/Partition/Offset의 Record가
같은 Consumer Group에 다시 전달될 수 있다.
```

```
request 상태가 ISSUED인 경우 처리:

PENDING → PROCESSING UPDATE는 0행이다.
기존 상태를 조회해 ISSUED임을 확인한다.

새 수량 증가와 coupon_issue INSERT를 수행하지 않는다.
기존 성공 결과를 유지한 채 메시지를 정상 처리로 종료하고
이번에는 Offset을 Commit한다.
```

## **8-5. 같은 Record 재전달과 같은 requestId의 다른 Record**

```
| 구분 | Partition | Offset | requestId |
| --- | --- | --- | --- |
| Offset 미반영으로 동일 Record 재전달 | 원본과 동일, 예: 0 | 원본과 동일, 예: 100 | req-001 |
| Producer가 같은 요청을 두 번 발행 | 같은 Key면 같은 Partition일 수 있으나 별도 Record다. 예: 0 | 서로 다름, 예: 100과 101 | 두 Record 모두 req-001 |
```

```
첫 번째 경우는 Kafka의 같은 물리 Record가 재전달된 것이다.
두 번째 경우는 Kafka에 서로 다른 두 Record가 존재한다.

두 현상의 Partition·Offset 관계는 다르지만
비즈니스 관점에서는 모두 같은 req-001이다.

따라서 Offset만으로는 두 번째 중복을 식별할 수 없고,
두 경우 모두 requestId로 기존 요청 결과를 확인해야 한다.
```

## **8-6. 자동 커밋 사용 시 확인해야 할 항목**

```
1. enable.auto.commit이 false인지 확인한다.

2. Spring Kafka의 AckMode가 RECORD, BATCH,
   MANUAL, MANUAL_IMMEDIATE 중 무엇인지 확인한다.

3. Listener가 예외를 던졌을 때
   Offset이 완료 처리되지 않는지 확인한다.

4. DB Transaction이 Rollback되었을 때
   Record가 재전달되거나 Retry 정책으로 이동하는지 확인한다.

5. Error Handler가 예외를 삼키고 정상 처리로 바꾸는지,
   Retry Topic 또는 DLQ로 이동할 때 원본 Offset을
   언제 Commit하는지 확인한다.

설정 방식은 달라도 원칙은 같다.
DB 결과가 확정된 뒤에만 메시지를 완료 처리해야 한다.
```

## **8-7. At-Least-Once와 비즈니스 결과**

```
메시지 전달 횟수
→ 한 번 이상일 수 있다.

쿠폰 발급 결과
→ 같은 requestId에는 한 번만 반영되어야 한다.

At-Least-Once 전달 자체를 없애려 하지 않고,
requestId, 조건부 상태 전이, UNIQUE 제약, DB Transaction으로
중복 전달을 안전하게 흡수한다.
```

## **8-8. 상태별 Offset 처리 방향**

```
| request 상태 또는 처리 결과 | Offset 처리 방향 | 이유 |
| --- | --- | --- |
| 새 PENDING 요청 처리 성공 | DB Commit 후 Offset Commit | 비즈니스 결과가 먼저 확정되어야 한다. |
| 기존 ISSUED 요청 중복 메시지 | 새 발급 없이 Offset Commit | 이미 성공한 최종 결과가 있으므로 멱등 성공이다. |
| 기존 FAILED 요청 중복 메시지 | 기존 실패를 유지하고 Offset Commit | 재시도해도 바뀌지 않는 최종 비즈니스 결과다. |
| 일시적 DB 오류로 Rollback | Offset Commit하지 않음 | DB가 회복된 뒤 같은 요청을 다시 처리해야 한다. |
| 요청 행 없음 | 즉시 발급·Commit하지 않고 등록 복구 또는 Retry, 이후 영구 불일치면 DLQ | Kafka와 DB 등록이 어긋난 상태이므로 복구 기회를 남겨야 한다. |
| 메시지 내용 불일치 | 발급 금지. 일반 Retry 대신 DLQ나 내구성 있는 오류 기록 후 Offset Commit | 같은 메시지를 반복해도 불일치는 해결되지 않는다. |
```

---

# **과제 9. Redis, DB, Kafka 불일치와 복구 정책 설계하기**

## **9-1. 장애 시점별 상태와 복구 방향**

```
| 장애 상황 | Redis 상태 | DB 요청 상태 | Kafka 상태 | 복구 방향 |
| --- | --- | --- | --- | --- |
| Redis SUCCESS 후 DB 요청 행 INSERT 전 장애 | 사용자와 requestId가 통과 자리로 남음 | 요청 행 없음 | 메시지 없음 | 동일 requestId 재시도에서 DB 등록을 복구하거나, 장기 미복구 시 requestId 소유권을 확인한 뒤 Redis 자리를 보상한다. |
| DB에 PENDING 저장 후 Kafka 발행 전 장애 | 통과 기록 존재 | PENDING | 메시지 없음 | 오래된 PENDING을 찾아 같은 requestId로 재발행한다. 장기적으로는 요청 Outbox를 사용한다. |
| Kafka 발행 성공 후 API 응답 유실 | 통과 기록 존재 | PENDING 또는 Consumer 처리 후 최종 상태 | 메시지 존재 | Client는 같은 requestId로 재시도하고 API는 기존 DB 상태를 반환한다. 새 논리 요청을 만들지 않는다. |
| Consumer가 DB 처리 전 종료 | 통과 기록 존재 | PENDING | Record 존재, Offset 미반영 | Kafka 재전달 후 정상 처리한다. |
| DB Commit 후 Offset Commit 전 종료 | 통과 기록이 보통 존재 | ISSUED 또는 FAILED | 같은 Record가 재전달될 수 있음 | 최종 상태를 확인하고 비즈니스 변경 없이 정상 종료한 뒤 Offset을 Commit한다. |
| DB ISSUED 후 Redis 데이터 유실 | 통과 기록 일부 또는 전체 없음 | ISSUED, coupon_issue 존재 | 이미 처리되었거나 보관 중 | DB를 최종 기준으로 삼고 Redis를 복원·보정한다. DB UNIQUE가 이후 중복 발급을 막는다. |
```

## **9-2. Redis SUCCESS 후 DB 요청 행이 없는 상태**

```
Client
  |
  v
Redis Lua Script
  |
  | SUCCESS
  | userId=10 → requestId=req-001 저장
  v
API Server
  |
  X DB INSERT 전 프로세스 종료

최종 상태

Redis
→ req-001이 자리를 차지함

DB
→ coupon_issue_request(req-001) 없음

Kafka
→ req-001 메시지 없음
```

```
동일 requestId 재시도 시 처리 방향:

Redis는 IDEMPOTENT_RETRY를 반환한다.
API는 DB 행이 없다고 즉시 신규 Redis 자리를 만들지 않는다.

짧게 재조회한 뒤에도 행이 없으면
동일 requestId, requestHash, authenticatedUserId로
coupon_issue_request 등록을 재시도한다.
등록 성공 후 Kafka 발행을 이어간다.
```

```
일정 시간 이상 지속될 때 가능한 보상:

자동 복구로 DB 등록을 완료할 수 없다면
Redis Lua Script로 현재 사용자의 통과 기록을 제거해
차지한 자리를 반환할 수 있다.

보상 실패와 재시도를 추적할 별도 복구 작업,
메트릭, 운영 알림도 필요하다.
```

```
보상 시 requestId 소유권을 확인해야 하는 이유:

장애 이후 같은 사용자에게 더 최신 요청이 연결되었을 수 있다.
오래된 req-001 복구 작업이 userId만 보고 값을 삭제하면
새 요청 req-002의 정상 통과 자리까지 제거할 수 있다.

현재 Redis에 저장된 requestId가 req-001과 같은 경우에만
조건부로 삭제해야 한다.
확인과 삭제는 Lua Script 하나에서 원자적으로 수행한다.
```

## **9-3. PENDING 저장 뒤 Kafka 발행 실패**

```
Redis:
사용자와 requestId의 통과 기록 존재

DB:
coupon_issue_request.status = PENDING

Kafka:
명확한 발행 실패라면 Record 없음
Timeout이라면 Record 존재 여부가 불명확
```

```
이 요청이 계속 PENDING에 남는 이유:

PENDING 행 저장과 Kafka 발행은 서로 다른 저장소에 대한 쓰기다.
DB Commit 뒤 Kafka 발행에 실패하면
Consumer가 처리할 메시지가 없으므로
request를 ISSUED나 FAILED로 바꿀 주체가 없다.
```

```sql
CREATE INDEX idx_coupon_issue_request_status_updated
ON coupon_issue_request (
    status,
    updated_at
);

-- 복구 후보 예시
SELECT request_id,
       event_id,
       user_id,
       requested_at
FROM coupon_issue_request
WHERE status = 'PENDING'
  AND updated_at < :staleCutoff
ORDER BY updated_at
LIMIT :batchSize;
```

```
복구 방향:

오래된 PENDING 요청을 찾아 같은 requestId로 Kafka에 재발행한다.
이미 첫 발행이 성공했을 가능성이 있어도
Consumer가 requestId로 중복을 흡수할 수 있다.

다만 PENDING은 “발행 전”과 “발행 후 Consumer 대기”를
동시에 표현하므로 단순 재발행은 중복을 만들 수 있다.

더 명확한 구조가 필요하면
요청 행과 outbox_event를 같은 DB 트랜잭션에 저장하고
별도 Publisher가 발행하도록 설계한다.
```

## **9-4. Kafka 발행 Timeout의 모호함**

```
상황 A:

Broker가 Record를 저장하지 못했다.
Producer는 제한 시간 안에 ACK를 받지 못하고 Timeout을 반환했다.

상황 B:

Broker는 Record를 정상 저장했다.
하지만 네트워크 문제로 ACK가 Producer에 도착하지 않아
Producer는 똑같이 Timeout을 반환했다.
```

```
Producer가 두 상황을 구분하기 어려운 이유:

Producer가 관측한 사실은 “제한 시간 안에 성공 ACK를 받지 못했다”뿐이다.
ACK가 없다는 사실만으로 Broker에 Record가 없다고 증명할 수 없다.
```

```
같은 requestId로 재발행했을 때 가능한 Kafka Record:

Partition 0 / Offset 100 / requestId=req-001
Partition 0 / Offset 101 / requestId=req-001

첫 발행이 실제로 저장된 상태에서 재발행하면
동일 논리 요청의 Record가 두 개 존재할 수 있다.
```

```
Consumer가 안전하게 처리할 수 있는 이유:

첫 Record만 PENDING → PROCESSING 권한을 얻어 실제 발급한다.
두 번째 Record는 request ISSUED 또는 FAILED를 확인한다.

수량 증가와 발급 INSERT를 다시 수행하지 않고
기존 최종 결과로 정상 종료한다.
```

## **9-5. Outbox Pattern이 줄일 수 있는 불일치**

```
API Server
    |
    v
+----------------------------------------+
| PostgreSQL Transaction                 |
|----------------------------------------|
| coupon_issue_request INSERT            |
| status = PENDING                       |
|                                        |
| outbox_event INSERT                    |
| type = COUPON_ISSUE_REQUESTED          |
| requestId = req-001                    |
+----------------------------------------+
    |
    | 함께 Commit 또는 함께 Rollback
    v
Outbox Publisher
    |
    | 실패 시 outbox_event 기준 재시도
    v
Kafka coupon.issue.requested
```

```
요청 행이 Commit되었다면 발행할 메시지 정보도 DB에 남는다.
프로세스가 DB Commit 직후 종료되어도
Outbox Publisher가 나중에 발행을 재시도할 수 있다.
```

## **9-6. Outbox가 해결하지 못하는 Redis와 DB 구간**

```
Redis와 PostgreSQL은 서로 다른 저장소다.

Redis Lua Script SUCCESS
        |
        X API Server 종료
        |
        v
PostgreSQL Transaction 실행 안 됨

요청 행과 Outbox를 같은 PostgreSQL 트랜잭션으로 묶어도
Redis SUCCESS 자체는 그 트랜잭션에 포함되지 않는다.

따라서 다음 상태는 여전히 가능하다.

Redis
→ 자리를 차지함

PostgreSQL
→ 요청 행과 Outbox가 없음

이 구간에는 requestId 기반 등록 재시도,
Redis 자리 소유권 확인, 조건부 보상,
수렴 확인과 운영 알림이 별도로 필요하다.
```

## **9-7. 최종 발급 결과의 기준 데이터**

```
선택:

최종 성공 여부의 원장은 coupon_issue DB 기록이다.
사용자 요청 결과는 coupon_issue_request의 ISSUED / FAILED 상태로 조회한다.

두 값은 같은 발급 트랜잭션으로 Commit되므로
정상 상태에서는 서로 일치해야 한다.
```

```
이유:

Redis 통과 기록
→ 선착순 admission 결과일 뿐 최종 발급이 아니다.

Kafka 메시지 존재
→ 처리 요청이 전달되었다는 뜻일 뿐 DB 성공이 아니다.

coupon_issue
→ 실제 최종 발급 성공 기록이다.

coupon_issue_request
→ requestId별 진행 상태와 최종 성공·실패 결과를 제공한다.

따라서 최종 성공은 DB를 기준으로 판단하며,
두 DB 테이블이 불일치하면 트랜잭션 불변식이 깨진 장애다.
```

## **9-8. 단계별 재실행 방어 기준**

```
| 단계 | 중복 또는 재실행 방어 기준 |
| --- | --- |
| Redis 판정 | eventId + userId와 해당 사용자의 최초 requestId를 Lua Script에서 원자적으로 비교 |
| DB 요청 등록 | request_id Primary Key, INSERT ... ON CONFLICT, requestHash 비교 |
| Kafka 재발행 | 같은 requestId를 유지하고 중복 Record 생성을 허용 |
| Consumer 처리 | requestId 조회, 메시지 내용 검증, PENDING 조건부 상태 전이, DB UNIQUE와 Transaction |
```

## **9-9. 운영 메트릭과 경고 항목**

```
| 메트릭 | 의미 | 경고가 필요한 조건 |
| --- | --- | --- |
| 오래된 PENDING 요청 수 | DB에 접수됐지만 발행 또는 Consumer 처리가 끝나지 않은 요청 수 | 정상 처리 지연 시간을 넘긴 요청이 지속 증가할 때 |
| 오래된 PROCESSING 요청 수 | 처리 권한을 얻은 뒤 최종 상태로 가지 못한 요청 수 | 단일 트랜잭션 구조에서 Commit된 PROCESSING이 관측되거나, Lease 구조에서 만료가 반복될 때 |
| requestId 충돌 횟수 | 같은 requestId INSERT가 기존 행과 충돌한 횟수 | requestHash 불일치 충돌이 한 건이라도 발생하거나 동일 Client에서 급증할 때 |
| Redis SUCCESS 후 DB 행 누락 수 | Redis에는 admission이 있으나 요청 행이 없는 수 | 짧은 동시 처리 허용 시간을 넘긴 누락이 존재할 때 |
| Kafka 발행 실패·Timeout 횟수 | PENDING과 Kafka 사이의 발행 장애 빈도 | 일정 시간 실패율이 임계치를 넘거나 연속 실패할 때 |
| 멱등 중복 메시지 처리 수 | ISSUED 또는 FAILED 상태에서 흡수한 중복 Record 수 | 평소 기준보다 급증해 Producer 중복 발행이나 Offset 문제를 의심할 때 |
| requestHash 불일치 수 | 같은 requestId가 다른 요청 내용에 사용된 횟수 | 데이터 손상이나 오용 가능성이 있으므로 즉시 알림 |
| UNIQUE(event_id, user_id) 위반 수 | Redis가 놓친 사용자 중복이 DB까지 도달한 횟수 | 반복 발생하여 Redis 유실·우회 경로를 의심할 때 |
```

---

# **과제 10. 나쁜 멱등 설계의 문제점 찾기**

## **10-1. 문제점과 개선 방향**

```
| 문제점 | 왜 문제인가? | 개선 방향 |
| --- | --- | --- |
| API Server가 요청마다 새 requestId를 생성한다. | 응답 유실 후 Client가 재시도하면 이전 요청과 연결할 ID가 없어 매번 신규 요청이 된다. | Client가 요청 전에 UUID를 만들고 동일 작업 재시도에서 같은 Idempotency-Key를 사용한다. |
| 같은 requestId의 요청 내용을 확인하지 않는다. | 같은 키가 다른 eventId나 userId에 재사용되어도 과거 결과를 잘못 반환할 수 있다. | 안정적인 Canonical String의 requestHash를 저장하고 기존 Hash와 비교한다. |
| coupon_issue 성공 기록만 저장한다. | PENDING, PROCESSING, FAILED와 실패 이유를 표현할 수 없어 요청 진행 상태와 재시도 판단이 어렵다. | 별도 coupon_issue_request 테이블에 요청 생명주기를 저장한다. |
| Consumer가 SELECT로 PENDING을 확인한 뒤 UPDATE한다. | 두 Consumer가 동시에 PENDING을 읽고 둘 다 발급을 시작하는 Check-Then-Act 경쟁이 생긴다. | WHERE status='PENDING'을 포함한 조건부 UPDATE의 영향받은 행 수로 권한을 획득한다. |
| 최종 상태 변경에 이전 상태 조건이 없다. | 오래된 Consumer가 ISSUED나 FAILED 같은 최종 상태를 덮어쓸 수 있다. | PROCESSING → ISSUED처럼 출발 상태를 WHERE에 포함하고 1행 변경을 검증한다. |
| ISSUED 중복 메시지에 예외를 던진다. | 이미 성공한 요청이 Retry Topic을 반복한 뒤 DLQ로 갈 수 있다. | 새 발급 없이 기존 성공 결과로 정상 종료하고 Offset을 Commit한다. |
| Kafka Offset이 DB 처리 전에 자동 Commit될 수 있다. | DB 실패 후 Record가 재전달되지 않아 요청이 유실된 것과 같은 결과가 된다. | 자동 Commit을 끄고 DB 결과 확정 뒤 Offset을 완료 처리하도록 AckMode와 Error Handler를 구성한다. |
| Deadlock을 즉시 FAILED로 저장한다. | Deadlock은 재시도하면 성공할 수 있는 일시적 기술 오류인데 최종 실패로 굳어진다. | 전체 Rollback 후 Listener 예외를 재전파해 Kafka 재처리 대상으로 둔다. |
| coupon_issue UNIQUE 위반 후 같은 트랜잭션에서 FAILED UPDATE를 시도한다. | PostgreSQL에서는 제약 위반 후 현재 트랜잭션이 실패 상태라 정상 SQL을 계속 실행할 수 없다. | 첫 트랜잭션을 Rollback하고 새 트랜잭션에서 기존 발급을 확인한 뒤 실패를 확정한다. |
| Rollback 후에도 WHERE status='PROCESSING'을 사용한다. | PENDING → PROCESSING 변경도 Rollback되어 실제 상태는 PENDING이므로 UPDATE가 항상 0행이 된다. | 새 트랜잭션에서는 WHERE status='PENDING'으로 FAILED / DUPLICATE_USER를 조건부 확정한다. |
| 상태 조회 API가 requestId만 확인한다. | requestId를 알게 된 다른 사용자가 타인의 요청 상태와 couponIssueId를 조회할 수 있다. | 조회 조건에 인증된 userId를 포함한다. |
| Idempotency-Key 유효 기간보다 먼저 요청 행을 삭제한다. | 유효한 과거 재시도를 신규 요청으로 잘못 판단할 수 있다. | 요청 상태 보관 기간을 Idempotency-Key 유효 기간 이상으로 설정한다. |
```

## **10-2. 안전한 PROCESSING 조건부 상태 전이**

```sql
UPDATE coupon_issue_request
SET status = 'PROCESSING',
    processing_started_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PENDING';
```

```
affected rows = 1
→ 처리 권한 획득

affected rows = 0
→ 발급 로직을 실행하지 않고 기존 상태 조회
```

## **10-3. 안전한 최종 성공 상태 전이**

```sql
UPDATE coupon_issue_request
SET status = 'ISSUED',
    coupon_issue_id = :couponIssueId,
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING';
```

```
정상 발급 트랜잭션에서는 affected rows가 반드시 1이어야 한다.

0행이면 issued_count와 coupon_issue만 Commit하지 않고
전체 발급 트랜잭션을 Rollback한다.
```

## **10-4. 중복 메시지 정상 종료 흐름**

```
ISSUED:

1. PENDING → PROCESSING 조건부 UPDATE가 0행이다.
2. 요청 행을 다시 조회한다.
3. status = ISSUED와 기존 coupon_issue_id를 확인한다.
4. issued_count 증가와 coupon_issue INSERT를 수행하지 않는다.
5. 기존 성공 결과를 유지하고 Listener를 정상 종료한다.
6. Offset을 Commit한다.

FAILED:

1. PENDING → PROCESSING 조건부 UPDATE가 0행이다.
2. 요청 행을 다시 조회한다.
3. status = FAILED와 failure_code를 확인한다.
4. 발급 로직을 다시 실행하지 않는다.
5. 기존 최종 실패를 유지하고 Listener를 정상 종료한다.
6. Offset을 Commit한다.
```

---

# **전체 흐름 최종 정리**

```
+--------+
| Client |
+--------+
    |
    | Idempotency-Key: req-001
    v
+-----------------------------------------+
| API Server                              |
|-----------------------------------------|
| authenticatedUserId 확인               |
| requestId UUID 검증                     |
| requestHash 생성                        |
+-----------------------------------------+
    |
    v
+-----------------------------------------+
| Redis Lua Script                        |
|-----------------------------------------|
| eventId + userId 중복 확인              |
| 최초 requestId 확인                     |
| 제한 수량 확인                          |
+-----------------------------------------+
    |
    +-- IDEMPOTENT_RETRY
    |      → 기존 DB 상태 반환
    |
    +-- DUPLICATE_USER / SOLD_OUT
    |      → 새 DB 요청 행·Kafka 발행 없음
    |
    +-- SUCCESS
           |
           v
+-----------------------------------------+
| coupon_issue_request                    |
|-----------------------------------------|
| INSERT PENDING                          |
| request_id PK                           |
| request_hash 비교                       |
+-----------------------------------------+
           |
           | 최초 등록 성공
           v
+-----------------------------------------+
| Kafka coupon.issue.requested            |
|-----------------------------------------|
| requestId, eventId, userId              |
+-----------------------------------------+
           |
           v
+-----------------------------------------+
| Coupon Issue Consumer                   |
|-----------------------------------------|
| 요청 행과 메시지 내용 일치 검증         |
+-----------------------------------------+
           |
           v
+------------------------------------------------+
| PostgreSQL Transaction                         |
|------------------------------------------------|
| PENDING → PROCESSING 조건부 UPDATE             |
| coupon_event.issued_count 조건부 증가          |
| coupon_issue INSERT                            |
| PROCESSING → ISSUED 또는 FAILED                |
+------------------------------------------------+
           |
           | Commit 성공
           v
+-----------------------------------------+
| Kafka Offset Commit                     |
+-----------------------------------------+

중복 API 요청
→ requestId + requestHash로 기존 결과 반환

중복 Kafka 메시지
→ 최종 상태 확인 후 새 발급 없이 정상 종료

다른 requestId의 사용자 중복
→ UNIQUE(event_id, user_id)가 최종 방어

일시적 DB 오류
→ 전체 Rollback 후 Kafka 재처리
```
