# 5주차 개념 설명: requestId 기반 멱등성과 중복 처리

## 주제

선착순 쿠폰 발급 시스템에서 같은 API 요청이나 Kafka 메시지가 여러 번 전달되더라도 최종 비즈니스 결과가 한 번만 반영되도록 `requestId`, 요청 상태 테이블, DB 제약 조건과 트랜잭션을 함께 설계한다.

이번 주차의 목표는 다음 한 문장으로 요약된다.

```
중복 전달을 완전히 없애는 것이 아니라,

중복 전달이 발생하더라도
최종 비즈니스 결과가 한 번만 반영되게 만든다.
```

---

## 1. 왜 멱등 처리가 필요한가

### 1-1. 지금까지 만든 구조와 남은 문제

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/767976a8-9803-4a08-b5b5-b945b95a6e8b" />

4주차까지의 처리 흐름은 다음과 같다.

```
Client
  |
  | 쿠폰 발급 요청
  v
API Server
  |
  | 인증 및 이벤트 검증
  v
Redis
  |
  | 사용자 중복과 잔여 수량 판정
  v
Kafka
  |
  | coupon.issue.requested
  v
Consumer
  |
  | DB 트랜잭션
  v
DB
```

각 구성 요소가 해결하는 문제는 서로 다르다.

```
Redis
- 성공 가능성이 없는 요청을 앞단에서 빠르게 차단한다.
- 동일 사용자가 여러 선착순 자리를 차지하지 못하게 한다.

Kafka
- API Server와 Consumer를 분리한다.
- 순간적인 요청 폭주를 저장하고 완충한다.

DB
- 조건부 UPDATE로 전체 수량 초과를 최종 방어한다.
- UNIQUE(event_id, user_id)로 사용자 중복 발급을 막는다.
```

하지만 이 구조로는 다음 질문에 답하기 어렵다.

```
이 메시지는 이미 성공한 요청의 재전달인가?
다른 requestId로 들어온 사용자 중복 요청인가?
요청이 현재 처리 중인가?
이전에 실패했다면 실패 이유는 무엇인가?
사용자에게 어떤 결과를 다시 보여줘야 하는가?
```

Redis와 DB의 방어 장치는 모두 `eventId + userId`를 기준으로 한다. 즉 **같은 사용자인지**는 알 수 있지만 **같은 논리적 요청인지**는 알 수 없다. 이를 위해 요청 식별자와 요청 상태를 추가한다.

### 1-2. 같은 요청이 여러 번 전달되는 이유

#### **1-2-a. 응답 유실 후 클라이언트 재시도**

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/4c49d9f5-1d6a-485c-866b-c8fbce7d7003" />

`requestId`는 API를 호출할 때마다 새로 만드는 값이 아니라, **하나의 논리적인 요청에 대해 한 번만 생성하는 식별자**다.

```
하나의 쿠폰 발급 요청
→ requestId = req-001 생성
→ 최초 전송과 이후 재시도에서 계속 req-001 사용
```

다음과 같이 서버가 요청을 정상적으로 처리했지만 응답만 유실될 수 있다.

```
Client
  |
  | requestId = req-001
  v
API Server
  |
  | Redis 판정 통과, Kafka 발행 성공
  v
응답 전송
  |
  X 네트워크 단절
```

클라이언트는 다음 두 상황을 구분할 수 없다.

```
상황 A
서버가 요청을 받지 못했다.

상황 B
서버는 요청을 처리했지만 응답만 유실되었다.
```

따라서 클라이언트는 재시도할 때 새로운 `requestId`를 생성하지 않고, 최초 요청에 사용한 값을 그대로 재사용해야 한다.

```
최초 요청
requestId = req-001

응답 유실 후 재시도
requestId = req-001
```

서버는 `req-001`이 이미 등록되어 있는지 확인한다.

```
requestId가 처음 들어옴
→ 새로운 요청 등록
→ Kafka 메시지 발행

같은 requestId가 다시 들어옴
→ 새로운 작업을 만들지 않음
→ 기존 요청의 상태와 결과 반환
```

재시도할 때마다 새로운 `requestId`를 생성하면 서버는 각각을 서로 다른 요청으로 인식하므로 멱등 처리를 할 수 없다.

#### 1-2-b. Producer 멱등성만으로 부족한 이유

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/2ee0b253-f214-4c2e-9611-2b5b3b3d8990" />

Kafka Producer 멱등성은 **애플리케이션에서 한 번 호출한 `send()`가 Producer 내부에서 재전송될 때**, 같은 Record가 브로커에 중복 저장되는 것을 막는다.

여기서 중요한 점은 애플리케이션 코드가 `send()`를 다시 호출하는 것이 아니라, Kafka Producer가 ACK 유실이나 네트워크 오류 때문에 **동일한 전송 요청을 내부적으로 재시도한다는 것**이다.

```
애플리케이션
  |
  | send(message) 한 번 호출
  v
Kafka Producer
  |
  | Broker로 Record 전송
  v
Broker
  |
  | 저장 성공
  v
ACK 유실
  |
  v
Kafka Producer 내부 재시도
  |
  v
Broker가 Producer ID와 Sequence Number 확인
  |
  +-- 이미 저장한 Sequence
      → 중복 Record를 다시 저장하지 않음
```

최초 전송과 내부 재시도에는 같은 Sequence Number가 사용된다.

```
최초 전송
Producer ID = 10
Sequence Number = 25

Producer 내부 재시도
Producer ID = 10
Sequence Number = 25
```

Broker는 `Sequence Number = 25`를 이미 저장했다면 동일한 전송의 재시도로 판단한다.

반면 애플리케이션 코드가 직접 `send()`를 두 번 호출하면 Kafka Producer는 이를 서로 다른 두 개의 전송 요청으로 본다.

```java
kafkaTemplate.send(topic, message);
kafkaTemplate.send(topic, message);
```

각 호출에는 새로운 Sequence Number가 붙는다.

```
첫 번째 send()
Producer ID = 10
Sequence Number = 25
→ 저장

두 번째 send()
Producer ID = 10
Sequence Number = 26
→ 저장
```

Kafka는 두 번째 호출을 첫 번째 호출의 재시도가 아니라 정상적인 새 Record로 판단하므로 둘 다 저장한다.

```
Producer 내부 재시도
→ 하나의 send()에서 발생
→ 같은 Sequence Number
→ Producer 멱등성으로 중복 저장 방지

애플리케이션의 send() 재호출
→ 서로 다른 두 개의 send()
→ 서로 다른 Sequence Number
→ Producer 멱등성으로 막지 못함
```

따라서 다음 중복은 Producer 멱등성만으로 해결할 수 없다.

```
- 애플리케이션 코드가 send()를 두 번 호출
- 클라이언트가 같은 API 요청을 다시 전송
- 다른 Producer 인스턴스가 같은 요청을 발행
- 운영자가 동일 메시지를 수동 재발행
- Retry Topic이나 복구 배치에서 다시 발행
```

이 경우에는 Kafka Record가 여러 개 생성되더라도 메시지 안의 `requestId`는 동일하게 유지해야 한다.

```
Offset 100 → requestId = req-001
Offset 101 → requestId = req-001
```

Consumer는 두 Record의 Offset이 다르더라도 같은 `requestId`인지 확인한다.

```
req-001 최초 처리
→ 실제 쿠폰 발급

req-001 재처리
→ 기존 ISSUED 또는 FAILED 결과 확인
→ 발급 로직을 다시 실행하지 않음
```

즉 두 멱등성의 역할은 다르다.

```
Kafka Producer 멱등성
→ 한 번의 send()가 내부 재시도되면서 발생하는 브로커 중복 저장 방지

requestId 기반 멱등성
→ 서로 다른 send(), 재발행, API 재시도로 생성된 비즈니스 중복 처리 방지
```

따라서 Producer 멱등성과 별개로, 동일한 논리적 요청을 식별하는 `requestId` 기반 멱등 처리가 필요하다.

#### 1-2-c. DB Commit 후 Offset Commit 전에 Consumer 종료

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/39b1c38c-7b25-4cf7-bec0-276118fe6bfc" />

```
1. Consumer가 메시지를 읽는다.     
2. DB 발급 트랜잭션을 Commit한다.  
3. Offset을 Commit하기 전에 Consumer가 종료된다.
4. 새 Consumer가 마지막 Commit Offset부터 다시 읽는다.
```

이때 재전달되는 것은 **같은 Partition의 같은 Offset을 가진 기존 Record**다.

```
Partition 2 / Offset 100 / req-001
        |
        | DB 처리는 완료했지만 Offset 미반영
        v
Partition 2 / Offset 100 / req-001 재전달
```

반면 Producer가 같은 메시지를 두 번 발행하면 **서로 다른 Record**가 생성된다.

```
Partition 2 / Offset 100 / req-001
Partition 2 / Offset 101 / req-001
```

두 현상은 원인이 다르지만, Consumer 입장에서는 모두 동일한 `requestId`를 중복 처리할 위험이라는 점이 같다. 그래서 Consumer 멱등성의 기준은 Offset이 아니라 `requestId`여야 한다.

#### 1-2-d. 사용자의 반복 클릭

<img width="1528" height="526" alt="image" src="https://github.com/user-attachments/assets/83d5667d-fc82-4701-a050-f72596b8c76e" />

반복 클릭이 항상 같은 논리적 요청인 것은 아니다. 클라이언트가 클릭할 때마다 새 키를 만들면 다음과 같다.

```
userId = 10, eventId = 100

클릭 1 → requestId = req-A
클릭 2 → requestId = req-B
클릭 3 → requestId = req-C
```

`requestId` 기준에서는 세 요청이 모두 다르다. 이 중복은 Redis의 `eventId + userId` 판정과 DB의 `UNIQUE(event_id, user_id)`가 막아야 한다.

### 1-3. 멱등성의 의미

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/2a10e7c8-18fb-4e49-80a2-e234f19534d2" />

멱등성은 같은 작업을 여러 번 시도해도 최종 결과가 한 번 수행한 것과 같아지는 성질이다.

```
사용자 상태를 ACTIVE로 변경한다.
→ 몇 번 실행해도 최종 상태는 ACTIVE (멱등적)

잔액에 10,000원을 더한다.
→ 두 번 실행하면 20,000원 증가 (멱등적이지 않음)
```

쿠폰 발급도 기본적으로는 멱등적이지 않다. 같은 발급 로직을 다시 실행하면 수량이 다시 증가하거나 발급 행이 다시 만들어지기 때문이다. 따라서 시스템이 명시적으로 멱등성을 만들어야 한다.

```
같은 requestId를 처음 처리
→ 실제 발급 로직 실행

같은 requestId를 다시 처리
→ 발급 로직을 실행하지 않고 저장된 결과 사용
```

멱등성은 다음 뜻이 아니다.

```
요청이 물리적으로 한 번만 도착한다.
Consumer 코드가 한 번만 호출된다.
DB 접근이 한 번만 발생한다.
```

요청과 메시지는 여러 번 도착할 수 있고 처리 로직도 여러 번 시작될 수 있다. 다만 최종 비즈니스 결과가 한 번만 반영될 뿐이다.

### 1-4. 중복 예외를 무시하는 것과의 차이

멱등 처리를 “Unique 예외를 무시하는 것”으로 이해하면 안 된다. Unique 제약은 중복 데이터의 저장을 막지만, 멱등 처리는 그보다 넓다.

```
1. 같은 요청인지 식별한다.
2. 요청이 등록되었는지 확인한다.
3. 처리 중이라면 새 처리를 시작하지 않는다.
4. 이미 성공했다면 기존 성공 결과를 사용한다.
5. 최종 실패했다면 기존 실패 결과를 사용한다.
6. 같은 requestId가 다른 요청에 사용되면 거절한다.
```

멱등성은 예외를 숨기는 기술이 아니라, 같은 요청에 대해 일관된 상태와 결과를 제공하는 설계다.

---

## 2. requestId와 Idempotency-Key

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/2e64f65d-ebee-4cb7-9a62-9772356f2deb" />

`Idempotency-Key`는 클라이언트가 동일한 요청의 재시도임을 서버에 알려주기 위해 전달하는 고유한 식별자다. 네트워크 오류로 응답을 받지 못해 같은 요청을 다시 보내더라도, 동일한 `Idempotency-Key`를 사용하면 서버는 새로운 요청이 아니라 기존 요청의 재시도로 판단할 수 있다.

`requestId`는 하나의 논리적인 쿠폰 발급 요청을 식별하는 값이다. HTTP API에서는 `Idempotency-Key` 헤더로 전달한다.

```
POST /api/coupon-events/100/issue
Authorization: Bearer ...
Idempotency-Key: 4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31
```

서버 내부에서는 이 값을 `requestId`로 사용하고, 하나의 식별자가 전체 처리 흐름을 따라간다.

```
HTTP Idempotency-Key
        |
        v
애플리케이션 requestId
        |
        v
coupon_issue_request.request_id
        |
        v
Kafka Message requestId
        |
        v
Consumer 멱등 처리
```

### 2-1. requestId와 Kafka Offset의 차이

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/8446659f-e267-44d7-a0d8-740a5b79ad79" />

```
requestId
→ 어떤 쿠폰 발급 요청인가?

Partition + Offset
→ Kafka 안의 어느 위치에 저장된 레코드인가?
```

같은 요청이 Retry Topic으로 이동하면 Partition과 Offset은 달라진다.

```
원본 Topic   : Partition 0 / Offset 100 / req-001
Retry Topic  : Partition 2 / Offset 35  / req-001
```

Kafka상의 위치는 달라졌지만 같은 비즈니스 요청이다. Consumer 멱등성은 반드시 `requestId`를 기준으로 판단한다.

### 2-2. 누가 requestId를 생성하는가

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/382f087f-90ab-4afc-89f7-a253dec32484" />

클라이언트 재시도를 같은 요청으로 묶으려면 **클라이언트가 요청을 보내기 전에** 생성하는 방식이 적합하다.

예를 들어 사용자가 쿠폰 발급 버튼을 누르면 프론트가 UUID를 한 번 생성한다.

서버가 요청을 받은 뒤에 생성하면 응답 유실 시 클라이언트가 기존 ID를 알 수 없다.

```
첫 요청   → 서버가 req-001 생성 → 처리 성공 → 응답 유실
재시도    → 서버가 req-002 생성
```

서버 관점에서는 서로 다른 요청으로 보인다.

다만 서버 간 내부 호출이나 클라이언트가 키를 만들기 어려운 환경에서는 Gateway 또는 최초 진입 서버가 생성할 수도 있다. 이때는 재시도하는 주체가 동일한 키를 보존할 수 있어야 한다.

### 2-3. 언제 같은 requestId를 재사용하는가

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/4d7699fc-2a26-4654-8c55-4ef0899fc8d1" />

같은 작업의 결과가 불명확해 다시 확인하거나 재시도할 때만 같은 값을 사용한다.

```
같은 requestId 재사용
- 네트워크 타임아웃 후 재시도
- 응답 유실 후 재시도
- 모바일 앱 재연결 후 동일 작업 재전송
- Gateway가 동일 요청을 재시도

새로운 requestId 사용
- 다른 이벤트 쿠폰 요청
- 기존 요청과 의미가 다른 작업
- 이전 요청을 취소한 뒤 새 요청 생성
```

`requestId`는 한 번의 버튼 클릭이 아니라 하나의 논리적인 작업을 나타낸다.

### 2-4. requestId의 형식과 검증

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/5713c923-2df9-4164-b199-ecabb812e580" />

전체 요청 상태 저장소에서 충돌하지 않도록 UUID를 사용하는 것이 편리하다.

```
550e8400-e29b-41d4-a716-446655440000
```

반드시 UUID여야 하는 것은 아니다. 서비스 전체에서 유일하고, 길이와 문자 형식이 검증되며, 추측하기 어려운 값이면 다른 형식도 사용할 수 있다. 최종 유일성은 DB의 Primary Key가 보장한다.

**API 앞단에서 형식과 길이를 먼저 검증해야 한다.** 지나치게 긴 키나 허용하지 않는 문자가 포함된 키를 그대로 받으면 로그와 DB 인덱스에 불필요한 부담을 준다.

### 2-5. 같은 requestId에 다른 내용이 들어온 경우

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/1bccb043-44e1-4971-a664-5880b9fddc3a" />

같은 `requestId`라는 이유만으로 무조건 기존 결과를 반환하면 안 된다.

```
기존 요청                   새 요청
requestId = req-001        requestId = req-001
eventId   = 100            eventId   = 200
userId    = 10             userId    = 10
```

기존 결과를 그대로 돌려주면 이벤트 100의 결과가 이벤트 200 요청에 연결된다. 사용자가 다르면 다른 사용자의 발급 결과가 노출될 수도 있다.

```
requestId 같음 + requestHash 같음
→ 같은 요청의 재시도

requestId 같음 + requestHash 다름
→ 잘못된 Idempotency-Key 재사용
→ 409 Conflict
```

### 2-6. requestHash를 만드는 방법

<img width="1724" height="650" alt="image" src="https://github.com/user-attachments/assets/20c0ccea-7581-43f7-9559-94311cb0547c" />

전체 JSON 문자열을 그대로 해시하면 안 된다. 의미가 같아도 필드 순서나 공백에 따라 문자열이 달라지기 때문이다.

```json
{ "eventId": 100, "userId": 10 }
```

```json
{"userId":10,"eventId":100}
```

필요한 필드만 정해진 순서로 정규화한 뒤 해시한다.

```
Canonical String
COUPON_ISSUE|100|10

request_hash = SHA-256(COUPON_ISSUE|100|10)
```

다음 값은 해시에 포함하지 않는다.

```
- 요청 전송 시각
- Trace ID
- 매번 바뀌는 nonce
- Gateway가 추가한 가변 Header
- JSON 직렬화 문자열 전체
```

같은 논리적 요청의 재시도에서도 달라질 수 있는 값이기 때문이다.

### 2-7. 인증된 사용자 ID 사용

<img width="1720" height="512" alt="image" src="https://github.com/user-attachments/assets/239bd484-5f09-4490-8b4f-d99c187f2b4c" />

Request Body의 `userId`를 그대로 신뢰하면 안 된다. 사용자가 다른 사람의 ID를 넣어 요청할 수 있다.

```
Authorization Token
        |
        v
Authenticated Principal
        |
        v
userId 추출
```

`request_hash`도 인증된 사용자 ID를 기준으로 생성하고, 상태 조회 API에서도 소유자를 다시 확인한다.

---

## 3. 같은 요청과 같은 사용자는 다르다

<img width="1548" height="502" alt="image" src="https://github.com/user-attachments/assets/4e211686-9305-492c-8dda-e2ada461231b" />

```
같은 requestId
→ 같은 요청이 다시 전달된 경우
→ 기존 처리 결과를 사용한다.

다른 requestId + 같은 eventId, userId
→ 같은 사용자가 새 요청으로 다시 발급을 시도한 경우
→ 사용자 중복 발급으로 처리한다.
```

### 3-1. 중복 방어 장치들의 역할 구분

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/2053a680-32b6-41c0-828a-d1f20c75eaa4" />

| 장치 | 식별 기준 | 해결하는 문제 |
| --- | --- | --- |
| Redis Set/Lua | `eventId + userId` | 동일 사용자의 반복 요청과 수량 초과를 앞단에서 차단 |
| `requestId` | `requestId` | 동일한 논리적 요청의 재시도와 메시지 중복 처리 |
| `request_hash` | `requestId + 요청 의미` | 같은 키를 다른 요청에 재사용하는 오류 방지 |
| 요청 상태 테이블 | `requestId` | 처리 상태와 기존 결과 저장 |
| 조건부 `UPDATE` | `eventId + 남은 수량` | DB 기준 전체 수량 초과 방지 |
| DB `UNIQUE` | `eventId + userId` | 최종 사용자 중복 발급 방지 |
| DB Transaction | 수량·발급·상태 변경 | 여러 변경을 함께 Commit 또는 Rollback |
| Kafka Offset | Partition의 소비 위치 | Consumer가 어디까지 읽었는지 기록 |

각 장치는 서로 다른 문제를 해결하므로 하나만 사용해서는 충분하지 않다.

`requestId`만 사용하면 같은 요청의 재전송은 식별할 수 있지만, 사용자가 새로운 `requestId`를 만들어 같은 쿠폰을 다시 요청하는 것은 막을 수 없다. 반대로 `UNIQUE(event_id, user_id)`만 사용하면 최종 중복 발급은 막을 수 있지만, 동일한 `requestId`가 다시 들어왔을 때 이전 요청이 처리 중인지, 성공했는지, 실패했는지와 어떤 결과를 반환해야 하는지는 알 수 없다.

Redis만 사용하면 빠르게 중복 요청과 수량 초과를 차단할 수 있지만, 데이터 유실이나 장애가 발생했을 때 최종 정합성을 보장할 수 없다. DB 제약만 사용하면 최종 데이터는 보호할 수 있지만, 모든 요청이 DB까지 도달해 부하가 커지고 중복 요청의 처리 상태를 설명하기 어렵다.

또한 DB Transaction이 없으면 수량은 감소했지만 발급 행 저장은 실패하거나, 발급은 성공했지만 요청 상태는 갱신되지 않는 부분 성공이 발생할 수 있다. Kafka Offset만으로도 메시지를 다시 읽는 것은 제어할 수 있지만, Consumer가 DB 반영 후 Offset Commit 전에 장애가 나면 같은 메시지가 다시 전달될 수 있으므로 비즈니스 멱등 처리를 대신할 수 없다.

따라서 Redis는 앞단에서 불필요한 요청을 줄이고, `requestId`와 요청 상태 테이블은 동일 요청의 재시도와 결과 재현을 담당하며, DB의 조건부 `UPDATE`, `UNIQUE`, Transaction은 최종 수량과 발급 데이터의 정합성을 보장한다.

### 3-2. Redis 판정과 DB 발급은 다르다

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/45e54a15-f7f4-4a67-b880-1c2f3f7ce234" />

Redis의 사용자 중복 검사는 다음 질문에 답한다.

```
이 사용자가 이 이벤트의 선착순 판정을 이미 통과했는가?
```

이 검사는 DB까지 도달할 요청 수를 줄인다. 하지만 Redis 통과가 최종 발급 성공을 의미하지는 않는다.

```
Redis SUCCESS
≠
DB ISSUED
```

Redis 판정 뒤에는 Kafka 발행과 Consumer의 DB 처리가 남아 있다. Redis 기록만 보고 최종 발급 결과를 반환하면 안 된다.

DB의 `UNIQUE(event_id, user_id)`는 최종 방어선이다. Redis 데이터가 유실되거나, 애플리케이션 로직이 잘못되거나, 다른 경로로 같은 사용자 요청이 들어와도 중복 발급 행의 저장을 막는다. 다만 Unique 위반만으로는 요청 상태를 설명할 수 없다.

### 3-3. Kafka Partition Key의 역할

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/653aedec-2b56-4daf-a6a0-61ce5441f60c" />

메시지 Key를 `requestId`로 정하면 같은 키를 가진 Record는 일반적으로 같은 Partition에 배치된다.

```
requestId = req-001
        |
        v
hash(req-001)
        |
        v
Partition 2
```

장점은 다음과 같다.

```
- 동일 requestId Record의 Partition 내 순서를 다루기 쉽다.
- 같은 요청이 여러 Partition에서 동시에 처리될 가능성을 줄인다.
- 특정 eventId 하나에 트래픽이 몰리는 Hot Partition을 피하기 쉽다.
```

하지만 다음을 보장하지는 않는다.

```
같은 Partition에 들어간다.
≠ 정확히 한 번만 처리된다.
```

또한 다음 조건에서는 같은 키의 Partition 매핑 자체가 달라질 수 있다.

```
- Partition 수 변경
- Partitioner 구현 변경
- Key 직렬화 방식 변경
```

같은 Consumer Group에서는 하나의 Partition이 동시에 한 Consumer에게만 할당되지만, 다음 상황에서는 여전히 동일 `requestId` 처리 경쟁이 가능하다.

```
- 같은 요청이 다른 Partition에 잘못 발행됨
- 다른 Consumer Group이 같은 Topic을 처리함
- Retry Topic과 원본 Topic이 동시에 처리됨
- 운영자가 병렬 재처리함
```

따라서 Partition Key는 순서와 분산을 돕는 장치이며, 최종 방어선은 DB의 조건부 상태 전이다.

---

## 4. 요청 상태 저장

### 4-1. 요청 상태 테이블이 필요한 이유

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/2b536ab9-98e6-4b40-87ec-2054e6ab4bc4" />

사용자는 다음과 같은 API로 비동기 처리 결과를 조회할 수 있어야 한다.

```
GET /api/coupon-issue-requests/{requestId}
```

Kafka는 메시지를 전달하고 보관하는 시스템이지 요청 상태 조회 저장소가 아니다. 메시지가 Topic에 남아 있다는 사실만으로는 다음 중 어느 상태인지 알 수 없다.

```
- Consumer가 아직 읽지 않았다.
- Consumer가 현재 처리하고 있다.
- DB 처리는 끝났지만 메시지 보관 기간이 남아 있다.
- 처리에 실패해 Retry를 기다리고 있다.
- DB 커밋 뒤 Offset 커밋 전에 장애가 발생했다.
```

Redis에도 최종 발급 상태는 없다.

```
Redis SUCCESS = 선착순 판정 통과
Redis SUCCESS ≠ 최종 쿠폰 발급 성공
```

`coupon_issue`에는 발급에 성공한 행만 저장되므로 대기·처리 중·실패 상태를 표현할 수 없다. 따라서 별도의 요청 상태 테이블 `coupon_issue_request`가 필요하다.

```
coupon_issue_request
→ 요청 자체의 생명주기와 결과를 저장

coupon_issue
→ 실제 발급 성공 기록만 저장
```

### 4-2. 요청 상태 모델

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/fc593284-e8e2-404e-8f39-586200cfaa70" />

| 상태 | 의미 |
| --- | --- |
| `PENDING` | 요청 행이 생성되었지만 최종 처리가 끝나지 않은 상태 |
| `PROCESSING` | Consumer가 처리 권한을 얻어 발급을 처리 중인 상태 |
| `ISSUED` | 수량 변경·발급 기록·상태 변경이 함께 Commit된 최종 성공 상태 |
| `FAILED` | 재시도해도 결과가 바뀌지 않는 최종 실패 상태 |

`PENDING`은 두 순간을 함께 나타낼 수 있다는 점에 주의한다.

```
상황 A: DB 요청 행 생성 후 Kafka 발행 전
상황 B: Kafka 발행 성공 후 Consumer 처리 전
```

운영에서 둘을 구분해야 한다면 `PUBLISH_PENDING`, `QUEUED` 같은 상태를 추가할 수 있다. 이번 과제에서는 단순화하되, `PENDING`만 보고 Kafka에 메시지가 반드시 있다고 단정하면 안 된다. 오래된 `PENDING`은 별도 복구 대상이다.

`PROCESSING`은 단일 트랜잭션 안에서 `PENDING → PROCESSING → ISSUED`를 모두 처리하면 다른 트랜잭션에서 거의 볼 수 없다. 커밋 전에는 `PENDING`, 커밋 후에는 `ISSUED` 또는 `FAILED`가 보인다. 이는 잘못된 동작이 아니라 `PROCESSING`이 트랜잭션 내부의 처리 권한 표시로 사용되기 때문이다.

`FAILED`의 실패 코드 예시는 다음과 같다.

```
SOLD_OUT          DB 기준 수량 소진
DUPLICATE_USER    같은 사용자가 다른 requestId로 이미 발급받음
INVALID_EVENT     처리할 수 없는 이벤트
REQUEST_MISMATCH  메시지와 요청 행의 내용 불일치
```

실패 코드는 애플리케이션이 판단할 수 있는 안정적인 값으로 두고, 사용자 메시지는 별도로 관리한다.

### 4-3. 일시적 오류와 최종 실패

모든 예외를 `FAILED`로 바꾸면 안 된다. 다음 오류는 잠시 후 다시 처리하면 성공할 수 있다.

```
- DB 연결 일시 실패
- Deadlock
- 네트워크 Timeout
- Connection Pool 일시 부족
- Kafka Consumer 프로세스 장애
```

```
일시적 시스템 오류
→ Rollback
→ Kafka 재전달로 재시도

최종 비즈니스 실패
→ FAILED 저장
→ 같은 메시지가 와도 기존 결과 사용
```

Retry 횟수와 DLQ 정책이 추가되면 `RETRY_PENDING`, `DEAD_LETTERED` 같은 상태를 둘 수 있다. 이 부분은 재시도 정책을 다루는 주차에서 확장한다.

### 4-4. 허용되는 상태 전이

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/945b2b2b-b77e-4b09-8cba-5fa0d61b4fce" />

```
PENDING
  |
  v
PROCESSING
  |
  +--------------------+
  |                    |
  v                    v
ISSUED               FAILED
```

단일 트랜잭션 외부에서는 `PENDING → ISSUED`, `PENDING → FAILED`로 보인다. 내부의 `PROCESSING`이 외부에 보이지 않기 때문이다.

허용하지 않는 전이는 다음과 같다.

```
ISSUED → PROCESSING
ISSUED → FAILED
FAILED → PROCESSING
FAILED → ISSUED
```

따라서 상태 변경 SQL에는 반드시 이전 상태를 조건으로 넣는다.

```sql
-- 잘못된 예: 현재 상태를 확인하지 않는다
UPDATE coupon_issue_request
SET status = 'ISSUED'
WHERE request_id = :requestId;
```

```sql
-- 올바른 예: 출발 상태를 조건에 포함한다
UPDATE coupon_issue_request
SET status = 'ISSUED',
    coupon_issue_id = :couponIssueId,
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING';
```

영향받은 행 수를 반드시 확인한다.

```
affected rows = 1  → 정상적으로 최종 상태 변경
affected rows = 0  → 요청 없음 / 상태 불일치 / 다른 처리자가 이미 변경
```

조건 없이 상태를 덮어쓰면 오래된 Consumer가 최신 결과를 변경할 수 있다.

### 4-5. coupon_issue_request 테이블 설계

PostgreSQL 기준 예시는 다음과 같다.

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
        FOREIGN KEY (event_id) REFERENCES coupon_event(id),

    CONSTRAINT fk_coupon_issue_request_issue
        FOREIGN KEY (coupon_issue_id) REFERENCES coupon_issue(id),

    CONSTRAINT uk_coupon_issue_request_issue
        UNIQUE (coupon_issue_id),

    CONSTRAINT chk_coupon_issue_request_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'ISSUED', 'FAILED')),

    CONSTRAINT chk_coupon_issue_request_failure
        CHECK (
            (status = 'FAILED' AND failure_code IS NOT NULL)
            OR
            (status <> 'FAILED'
                AND failure_code IS NULL
                AND failure_message IS NULL)
        ),

    CONSTRAINT chk_coupon_issue_request_completed
        CHECK (
            (status IN ('ISSUED', 'FAILED') AND completed_at IS NOT NULL)
            OR
            (status IN ('PENDING', 'PROCESSING') AND completed_at IS NULL)
        ),

    CONSTRAINT chk_coupon_issue_request_issue_result
        CHECK (
            (status = 'ISSUED' AND coupon_issue_id IS NOT NULL)
            OR
            (status <> 'ISSUED' AND coupon_issue_id IS NULL)
        ),

    CONSTRAINT chk_coupon_issue_request_processing
        CHECK (
            (status = 'PROCESSING' AND processing_started_at IS NOT NULL)
            OR
            (status <> 'PROCESSING')
        )
);
```

`request_id`를 Primary Key로 두면 같은 요청이 동시에 들어와도 한 행만 등록된다. Primary Key 충돌은 무조건 시스템 장애가 아니라 다음 세 가지일 수 있으므로, 기존 행의 `request_hash`를 확인해 구분한다.

```
- 정상적인 재시도
- 동시에 들어온 동일 요청
- 잘못된 requestId 재사용
```

`UNIQUE(coupon_issue_id)`는 하나의 발급 행이 여러 성공 요청에 연결되는 것을 막는다.

애플리케이션 코드만으로도 검증할 수 있지만, DB 제약을 함께 사용하면 버그나 다른 쓰기 경로가 생겼을 때도 잘못된 상태를 막을 수 있다.

### 4-6. 요청 테이블에 UNIQUE(event_id, user_id)를 두지 않는 이유

<img width="1166" height="355" alt="image" src="https://github.com/user-attachments/assets/c5179341-9dd7-4aad-a7f2-7710e3e34a50" />

이 제약을 두면 같은 사용자의 요청 이력을 하나만 저장할 수 있게 된다.

```
req-A → 최종 ISSUED
req-B → FAILED / DUPLICATE_USER
```

두 이력이 모두 남아야 사용자가 두 번째 요청의 실패 이유를 조회할 수 있다. 사용자별 최종 발급 1회 규칙은 `coupon_issue`의 `UNIQUE(event_id, user_id)`가 담당한다.

### 4-7. 인덱스와 보관 기간

<img width="1167" height="407" alt="image" src="https://github.com/user-attachments/assets/781c6812-30d8-4d98-8bd1-f1d72dcac51d" />

Primary Key는 `requestId` 단건 조회를 처리한다. 운영 복구를 위해 상태와 시각 기준 조회가 필요하다.

```sql
CREATE INDEX idx_coupon_issue_request_status_updated
ON coupon_issue_request (status, updated_at);
```

이 인덱스는 오래된 `PENDING`이나 `PROCESSING` 요청을 찾을 때 사용한다.

요청 상태를 영구 보관할 필요가 없다면 보관 기간을 정해야 한다. 다만 **사용자가 같은 `Idempotency-Key`를 재사용할 수 있는 기간보다 먼저 행을 삭제하면 과거 요청을 신규 요청으로 잘못 인식한다.**

```
Idempotency-Key 유효 기간
≤
요청 상태 보관 기간
```

삭제나 아카이브 정책은 이 관계를 반드시 만족해야 한다.

---

## 5. API Server의 처리 흐름

### 5-1. Redis 판정과 DB 요청 등록의 순서

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/31125115-14c5-4b4f-ba56-9870e7d32484" />

`coupon_issue_request`를 언제 저장할지는 트래픽과 복구 요구사항 사이의 선택이다.

#### 방식 A. DB 요청 등록을 먼저 하는 방식

```
Client → coupon_issue_request INSERT → Redis 판정 → Kafka 발행
```

```
장점
- 모든 requestId가 내구성 있게 저장된다.
- 동일 requestId 동시 요청을 DB Primary Key로 먼저 방어한다.
- SOLD_OUT, DUPLICATE_USER 결과도 요청 이력으로 남길 수 있다.

단점
- 대량 요청 전체가 DB 쓰기를 수행한다.
- Redis를 앞단 필터로 둔 효과가 줄어든다.
- 이벤트 시작 시 요청 테이블에 쓰기가 집중된다.
```

#### 방식 B. Redis 통과 요청만 저장하는 방식

```
Client
  |
  v
Redis Lua Script
  |
  +-- SOLD_OUT / DUPLICATE_USER
  |      → DB 요청 행 생성하지 않음
  |
  +-- SUCCESS
         |
         v
    coupon_issue_request INSERT
         |
         v
      Kafka 발행
```

```
장점
- Redis를 통과한 요청만 DB에 기록한다.
- DB 쓰기 부하를 크게 줄일 수 있다.
- 3주차의 Redis 필터링 목적을 유지한다.

단점
- 품절과 사용자 중복 요청의 requestId는 DB에 남지 않는다.
- Redis SUCCESS 후 DB INSERT 전에 장애가 날 수 있다.
```

#### 과제 기본 선택

이번 시스템은 Redis가 대량 요청을 먼저 차단하는 구조를 유지하므로 **방식 B**를 기본으로 사용한다.

```
1. Redis Lua Script에서 eventId + userId + requestId를 판정한다.
2. SUCCESS인 요청만 coupon_issue_request를 PENDING으로 저장한다.
3. 요청 상태 저장 후 Kafka에 메시지를 발행한다.
4. Consumer는 요청 상태 행이 존재하는 메시지만 처리한다.
```

대신 다음 상태를 반드시 복구 대상으로 정의해야 한다.

```
Redis SUCCESS → DB 요청 행 없음
```

동일 요청의 재시도가 이 상태를 발견하면 새 자리를 만들지 않고, 짧은 시간 후 재조회하거나 DB 등록을 재시도하고, 일정 시간 이상 지속되면 Redis 자리를 보상한다. 운영 로그와 메트릭도 남긴다.

### 5-2. Redis에 requestId를 함께 저장하는 이유

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/53a98ca1-efd9-4e03-9246-86b4f339fab5" />

3주차의 Set에는 사용자 ID만 있어 최초 요청 ID를 알 수 없다.

```
coupon:{100}:admitted-users
user:10  user:20  user:30
```

사용자별 최초 통과 `requestId`를 함께 저장한다.

```
coupon:{100}:admission-requests
user:10 → req-001
user:20 → req-002
```

그러면 Lua Script 반환값을 다음처럼 세분화할 수 있다.

| 반환값 | 의미 | API 처리 |
| --- | --- | --- |
| `SUCCESS` | 새로운 사용자가 선착순 통과 | 요청 상태 생성 후 Kafka 발행 |
| `IDEMPOTENT_RETRY` | 동일 사용자·동일 requestId 재요청 | DB의 기존 요청 상태 반환 |
| `DUPLICATE_USER` | 동일 사용자가 다른 requestId로 재요청 | 새 Kafka 발행 금지 |
| `SOLD_OUT` | 제한 수량 도달 | 품절 응답 |

사용자 확인, 수량 확인, 최초 `requestId` 저장은 Lua Script 안에서 원자적으로 처리해야 한다. 명령을 애플리케이션에서 따로 실행하면 두 요청이 중간에 끼어들 수 있다.

주의할 점은 다음과 같다.

```
Redis가 IDEMPOTENT_RETRY를 반환했는데
DB에 requestId 행이 아직 없다면
정상 PENDING으로 단정하면 안 된다.
```

Redis 통과 후 DB 등록이 완료되지 않은 중간 실패일 수 있으므로 재조회·복구·보상 정책이 필요하다.

### 5-3. 같은 requestId가 동시에 들어오는 경우

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/5c2c6070-1eeb-469f-80cd-274d2b051f18" />

Load Balancer 뒤의 두 API Server가 같은 요청을 동시에 받을 수 있다. 다음 구조는 안전하지 않다.

```
1. requestId가 존재하는지 SELECT
2. 없으면 INSERT
```

```
시간       API Server A               API Server B
----------------------------------------------------------
T1         req-001 조회 → 없음
T2                                    req-001 조회 → 없음
T3         최초 요청 판단
T4                                    최초 요청 판단
T5         INSERT 시도
T6                                    INSERT 시도
```

조회와 저장이 분리된 Check-Then-Act 경쟁이다. 최초 등록 권한은 사전 조회가 아니라 DB의 유일성 제약으로 결정한다.

```sql
INSERT INTO coupon_issue_request (
    request_id, event_id, user_id,
    request_hash, status, requested_at
)
VALUES (
    :requestId, :eventId, :userId,
    :requestHash, 'PENDING', :requestedAt
)
ON CONFLICT (request_id) DO NOTHING;
```

```
affected rows = 1
→ 최초 등록자

affected rows = 0
→ 기존 행 조회
→ request_hash 같음  : 기존 상태 반환
→ request_hash 다름  : 409 Conflict
```

Redis가 `SUCCESS`를 반환했는데 DB INSERT에서 충돌이 발생할 수도 있다. Redis 데이터가 초기화된 뒤 과거 요청이 재시도된 경우다. 이때도 단순 오류로 처리하지 않고 기존 행의 해시와 상태를 확인하며, Redis에서는 자리를 다시 차지했을 수 있으므로 수량과 사용자 기록을 보상해야 한다.

### 5-4. 응답 규칙

최초 요청이 정상 접수되면 `202 Accepted`를 반환한다.

```json
{
  "requestId": "req-001",
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다."
}
```

`202 Accepted`는 비동기 처리 흐름에 들어갔다는 의미이며 최종 발급 성공이 아니다. Consumer의 DB 처리가 남아 있으므로 `ISSUED`라고 응답하면 안 된다.

같은 `requestId`가 다시 들어오면 새 Kafka 메시지를 만들지 않고 저장된 상태를 반환한다.

```json
{
  "requestId": "req-001",
  "status": "ISSUED",
  "couponIssueId": 5001,
  "message": "이미 쿠폰 발급이 완료되었습니다."
}
```

```json
{
  "requestId": "req-001",
  "status": "FAILED",
  "failureCode": "SOLD_OUT",
  "message": "쿠폰이 모두 소진되었습니다."
}
```

재요청에 `200 OK`를 쓸지 `202 Accepted`를 쓸지는 API 정책으로 정한다. 중요한 것은 새 작업을 만들지 않는 것이다.

같은 키에 다른 요청 내용이 들어오면 거절한다.

```
HTTP/1.1 409 Conflict
```

```json
{
  "requestId": "req-001",
  "errorCode": "IDEMPOTENCY_KEY_REUSED",
  "message": "동일한 요청 식별자가 다른 요청에 사용되었습니다."
}
```

이 요청은 Redis 판정이나 Kafka 발행으로 진행시키지 않는다.

### 5-5. 상태 조회 API

```
GET /api/coupon-issue-requests/{requestId}
```

```json
{
  "requestId": "req-001",
  "eventId": 100,
  "status": "ISSUED",
  "couponIssueId": 5001,
  "completedAt": "2026-07-24T12:00:01+09:00"
}
```

조회 조건에는 **반드시 인증된 사용자 ID를 포함한다.**

```sql
SELECT request_id,
       event_id,
       status,
       failure_code,
       coupon_issue_id,
       requested_at,
       completed_at
FROM coupon_issue_request
WHERE request_id = :requestId
  AND user_id = :authenticatedUserId;
```

`requestId`를 안다는 이유만으로 다른 사용자의 발급 결과를 조회할 수 있게 하면 안 된다.

방식 B를 선택했다면 한 가지 정책을 더 정해야 한다. Redis에서 `SOLD_OUT`이나 `DUPLICATE_USER`로 거절된 `requestId`는 DB에 행이 없으므로 이 API가 `404`를 반환한다.

```
API 응답에서 이미 품절/중복 결과를 받은 requestId
→ 이후 상태 조회는 404

즉, 조회 API는 "접수된 요청"의 상태만 조회할 수 있다.
```

이 동작을 API 문서에 명시하거나, 이력 조회가 필요하다면 방식 A를 선택해야 한다.

---

## 6. Consumer의 멱등 처리

### 6-1. Consumer 멱등성이 별도로 필요한 이유

<img width="1173" height="336" alt="image" src="https://github.com/user-attachments/assets/0e352a67-984f-423b-9691-7e94a632b90f" />

API Server에서 같은 요청을 차단했더라도 Kafka 뒤에서 중복이 발생할 수 있다.

```
API 멱등성
→ 클라이언트의 동일 요청 재시도 방어

Consumer 멱등성
→ Kafka Record 재전달, 중복 발행, 재처리 방어
```

두 지점 모두 필요하다.

### 6-2. Kafka 메시지 구조와 검증

<img width="1170" height="432" alt="image" src="https://github.com/user-attachments/assets/fb0521ed-2179-46be-8016-b73685522117" />

```json
{
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "eventId": 100,
  "userId": 10,
  "requestedAt": "2026-07-24T12:00:00+09:00"
}
```

Consumer는 메시지를 받자마자 발급하지 않고, `requestId`로 요청 행을 조회해 내용이 일치하는지 확인한다.

```
Kafka Message              coupon_issue_request
requestId = req-001        request_id = req-001
eventId   = 100            event_id   = 100
userId    = 10             user_id    = 10
```

```
모두 일치
→ 정상 처리

requestId는 같지만 eventId 또는 userId가 다름
→ 손상된 메시지 또는 잘못된 재사용
→ 발급 처리 금지
→ DLQ 또는 운영 확인 후보
```

이 오류는 반복한다고 해결되는 일시적 장애가 아니므로 재시도 대상으로 두지 않는다.

Consumer는 메시지의 값만 믿지 않고 **DB에 저장된 요청 상태를 기준으로** 처리한다.

### 6-3. SELECT 후 UPDATE가 안전하지 않은 이유

<img width="1171" height="344" alt="image" src="https://github.com/user-attachments/assets/c76ef04c-e3bb-4fa3-8f5c-5f2cdbbd2613" />

```
1. 상태를 SELECT한다.
2. PENDING인지 애플리케이션에서 확인한다.
3. PROCESSING으로 UPDATE한다.
```

두 Consumer가 동시에 조회하면 둘 다 `PENDING`을 볼 수 있다.

```
시간       Consumer A                  Consumer B
------------------------------------------------------------
T1         status 조회 → PENDING
T2                                     status 조회 → PENDING
T3         처리 가능 판단
T4                                     처리 가능 판단
T5         PROCESSING 저장
T6                                     PROCESSING 저장
T7         발급 처리 시작              발급 처리 시작
```

애플리케이션의 `if (status == PENDING)`만으로는 처리 권한을 하나로 제한할 수 없다.

### 6-4. 조건부 UPDATE로 처리 권한 획득

<img width="1167" height="425" alt="image" src="https://github.com/user-attachments/assets/ec8f40ed-e61a-42b8-b365-55483768c453" />

상태 확인과 변경을 한 문장으로 처리한다.

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
→ 내가 PENDING을 PROCESSING으로 변경했다. 처리 권한 획득

affected rows = 0
→ 요청이 없거나, 다른 Consumer가 처리했거나, 이미 최종 상태다.
```

두 Consumer가 동시에 실행하면 DB가 같은 행에 대한 쓰기 충돌을 조정한다.

```
현재 상태: PENDING

Consumer A
→ Row Lock 획득 → PROCESSING 변경 → affected rows = 1

Consumer B
→ A의 트랜잭션 종료까지 대기
→ 최신 상태로 WHERE 조건 재평가
→ status가 더 이상 PENDING이 아니므로 0행
```

> **격리 수준 주의**
“대기 후 최신 상태로 조건을 재평가한다”는 동작은 PostgreSQL의 기본 격리 수준인 **READ COMMITTED**에서 성립한다. `REPEATABLE READ` 이상에서는 조건을 재평가하는 대신 직렬화 오류(`could not serialize access`)가 발생하며, 애플리케이션이 트랜잭션 전체를 재시도해야 한다. 이번 과제는 READ COMMITTED를 전제로 한다.
> 

### 6-5. affected rows가 0인 경우

<img width="1171" height="463" alt="image" src="https://github.com/user-attachments/assets/3d8e0d18-1fc5-4893-895c-3109fc5aa6ee" />

0행을 바로 실패로 처리하면 안 된다. 기존 요청 상태를 다시 조회해 구분한다.

```
요청 행 없음
→ DB 요청 등록과 Kafka 메시지가 불일치

PROCESSING
→ 다른 Consumer가 처리 중

ISSUED
→ 이미 성공한 요청의 중복 메시지

FAILED
→ 이미 최종 실패한 요청의 중복 메시지
```

단일 트랜잭션 방식에서는 먼저 처리한 Consumer가 커밋할 때까지 UPDATE가 대기하므로, 그 후 보이는 상태는 보통 `ISSUED`나 `FAILED`다.

`PROCESSING`이 별도 트랜잭션으로 커밋되는 구조라면 다른 Consumer가 실제로 작업 중일 수 있다. 이때 바로 Offset을 커밋하면 기존 처리자가 실패했을 때 요청이 고착되고, 무조건 즉시 재시도하면 처리 중인 작업과 계속 충돌한다. 이 문제는 7-7의 Lease로 다룬다.

### 6-6. 최종 상태의 중복 메시지는 정상 처리다

기존 상태가 `ISSUED`라면 다음이 이미 저장되어 있다.

```
issued_count 반영 완료
coupon_issue 저장 완료
request 상태 ISSUED
```

새 발급 로직을 실행하지 않고, 메시지를 정상 처리된 것으로 보고 Offset을 커밋한다. 최종 확정된 `FAILED`도 마찬가지다.

중복 메시지를 매번 예외로 던지면 다음 흐름이 생긴다.

```
ISSUED 확인 → DuplicateMessageException → Retry Topic
→ 다시 ISSUED 확인 → 다시 예외 → DLQ
```

이미 정상 완료된 요청이 불필요하게 재시도된다. **최종 상태를 확인하고 아무 변경 없이 끝내는 것도 성공적인 처리다.**

---

## 7. 발급 작업은 하나의 DB 트랜잭션으로 처리한다

쿠폰 발급처럼 외부 네트워크 호출 없이 짧은 DB 작업으로 끝나는 경우, 처리 권한 획득부터 최종 상태 변경까지를 하나의 트랜잭션으로 묶는 것이 가장 단순하다.

### 7-1. 트랜잭션에 포함할 작업

```
포함한다
- 요청 처리 권한 획득
- 쿠폰 수량 확보
- 발급 기록 저장
- 요청 최종 상태 저장

포함하지 않는다
- 알림 발송, 이메일 전송
- 마이페이지 데이터 갱신
- 통계 서비스 호출
- 외부 API 호출
```

외부 호출은 응답 시간이 길고 실패 시점을 예측하기 어렵다. 트랜잭션 안에서 호출하면 Row Lock과 DB Connection을 오래 점유하고, 외부 서비스 장애가 핵심 발급 트랜잭션에 전파된다. 핵심 결과를 먼저 짧게 커밋하고 후속 작업은 별도 이벤트로 분리한다.

### 7-2. 전체 처리 흐름

```
Consumer 메시지 수신
        |
        v
DB Transaction 시작
        |
        v
PENDING → PROCESSING 조건부 UPDATE
        |
        +-- 0행
        |    → 기존 상태 확인
        |    → ISSUED/FAILED면 멱등 성공 종료
        |
        +-- 1행
             |
             v
      coupon_event 조건부 UPDATE
             |
             +-- 0행
             |    → SOLD_OUT 확정
             |    → request FAILED 변경 → Commit
             |
             +-- 1행
                  |
                  v
            coupon_issue INSERT
                  |
                  +-- 성공
                  |    → request ISSUED 변경 → Commit
                  |
                  +-- UNIQUE 충돌
                       → Rollback
                       → 별도 트랜잭션에서 결과 확정
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
WHERE id = :eventId
  AND issued_count < total_quantity;
-- affected rows = 1인 경우에만 INSERT한다.

-- 3. 최종 발급 기록을 저장한다.
INSERT INTO coupon_issue (
    event_id, user_id, status, issued_at
)
VALUES (
    :eventId, :userId, 'ISSUED', CURRENT_TIMESTAMP
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

COMMIT;
```

각 단계의 영향받은 행 수를 반드시 확인한다.

```
처리 권한 획득   → 반드시 1행
수량 UPDATE      → 0행 또는 1행
최종 ISSUED 변경 → 반드시 1행
```

마지막 상태 변경이 0행인데 발급 결과만 커밋하면 요청 상태와 실제 발급 기록이 어긋난다. 이 경우 전체 트랜잭션을 롤백해야 한다.

### 7-3. 하나의 트랜잭션이 필요한 이유

```
수량 증가만 Commit
→ 쿠폰 한 장이 실제 사용자 없이 소모된다.

발급 기록만 Commit
→ 전체 수량 제한을 믿을 수 없게 된다.

요청 상태만 ISSUED
→ 상태 조회는 성공을 반환하지만 실제 쿠폰이 없다.
```

따라서 다음 불변식을 지켜야 한다.

```
coupon_event.issued_count 증가
coupon_issue 저장
coupon_issue_request ISSUED 변경

세 작업은 함께 성공하거나 함께 실패한다.
```

### 7-4. DB 기준 수량이 소진된 경우

수량 조건부 UPDATE가 0행이면 쿠폰을 확보하지 못한 것이다.

```sql
UPDATE coupon_event
SET issued_count = issued_count + 1
WHERE id = :eventId
  AND issued_count < total_quantity;
```

조건에 수량만 포함했다면 `0행 = DB 기준 SOLD_OUT`으로 단순화할 수 있다. 상태나 기간 조건까지 넣었다면 원인이 여러 개이므로 별도 조회로 구분해야 한다.

```sql
WHERE id = :eventId
  AND status = 'OPEN'
  AND CURRENT_TIMESTAMP >= start_at
  AND CURRENT_TIMESTAMP < end_at
  AND issued_count < total_quantity
```

품절이 확정되면 같은 트랜잭션에서 요청을 최종 실패로 변경하고 커밋한다.

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

발급 행은 만들지 않는다. 같은 메시지가 다시 들어오면 기존 `FAILED`를 확인하고 끝낸다.

### 7-5. UNIQUE 위반과 Rollback 이후의 상태

같은 사용자가 서로 다른 `requestId`로 DB 처리까지 도달할 수 있다.

```
기존 요청: req-A / eventId 100 / userId 10 / ISSUED
새 요청  : req-B / eventId 100 / userId 10
```

`req-B`는 `requestId` 기준으로는 신규 요청이지만 `coupon_issue`의 `UNIQUE(event_id, user_id)`와 충돌한다. 이는 같은 요청의 재처리가 아니라 사용자 중복 발급 시도다.

PostgreSQL에서는 제약 조건 위반이 발생한 트랜잭션을 계속 사용할 수 없다. 따라서 같은 트랜잭션에서 곧바로 상태를 `FAILED`로 바꿀 수 없다.

```
1. 현재 발급 트랜잭션 Rollback
2. 새 트랜잭션 시작
3. 기존 coupon_issue 확인
4. 현재 요청을 FAILED / DUPLICATE_USER로 확정
5. Commit
```

여기서 가장 중요한 점은 다음이다.

> **Rollback되면 첫 트랜잭션의 `PENDING → PROCESSING` 전이와 `issued_count` 증가도 함께 취소된다.**
따라서 새 트랜잭션에서 요청 상태는 `PROCESSING`이 아니라 다시 **`PENDING`**이다.
> 

```sql
UPDATE coupon_issue_request
SET status = 'FAILED',
    failure_code = 'DUPLICATE_USER',
    failure_message = '이미 발급받은 사용자입니다.',
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PENDING';   -- PROCESSING이 아니다
```

조건을 `PROCESSING`으로 두면 항상 0행이 반환되어 요청이 영원히 `PENDING`에 남는다. 실패 상태 변경 SQL의 출발 상태는 **실제 롤백 결과에 맞춰야 한다.**

이 두 번째 처리도 다른 처리자와 경쟁할 수 있으므로 영향받은 행 수와 현재 상태를 확인한다.

한편 `issued_count` 증가가 함께 롤백되는 것은 의도된 동작이다.

```
조건부 UPDATE 성공        → issued_count + 1
coupon_issue UNIQUE 위반  → 전체 Rollback → issued_count 증가도 취소
```

예외를 쓰지 않으려면 `INSERT ... ON CONFLICT DO NOTHING RETURNING id`와 처리 순서를 다시 설계할 수도 있다. 다만 수량 증가와 INSERT 결과를 여전히 하나의 원자적 결과로 맞춰야 하므로 `DO NOTHING`만 추가해서는 충분하지 않다.

### 7-6. 일시적 DB 오류

Deadlock이나 연결 오류가 발생하면 발급 트랜잭션 전체가 롤백된다.

```
request = PENDING
issued_count 증가 없음
coupon_issue 없음
```

이 오류를 최종 `FAILED`로 저장하지 않는다. Listener에서 예외를 다시 던져 Kafka 재처리 정책으로 넘긴다. 재전달된 메시지는 다시 `PENDING → PROCESSING` 권한 획득을 시도한다.

### 7-7. PROCESSING을 별도로 커밋하는 방식

외부 시스템 호출이나 긴 계산이 포함된다면 처리 권한 획득을 별도 트랜잭션으로 커밋할 수 있다.

```
Transaction A : PENDING → PROCESSING → Commit
긴 작업 수행
Transaction B : PROCESSING → ISSUED / FAILED → Commit
```

```
장점
- 긴 작업 동안 DB 트랜잭션과 Row Lock을 유지하지 않는다.
- 다른 Consumer와 사용자에게 PROCESSING을 보여줄 수 있다.

단점
- PROCESSING Commit 직후 Consumer가 종료되면 요청이 고착된다.
- 처리 권한 회수 정책이 필요하다.
- 느린 기존 Consumer와 새 Consumer가 동시에 실행될 수 있다.
```

이를 위해 처리 임대(Lease) 컬럼을 둔다.

```sql
ALTER TABLE coupon_issue_request
ADD COLUMN processing_owner VARCHAR(100),
ADD COLUMN lease_until TIMESTAMPTZ,
ADD COLUMN lease_version BIGINT NOT NULL DEFAULT 0,
ADD COLUMN attempt_count INT NOT NULL DEFAULT 0;
```

만료된 권한은 다른 Consumer가 회수할 수 있다.

```sql
UPDATE coupon_issue_request
SET processing_owner = :newConsumerId,
    lease_until = CURRENT_TIMESTAMP + INTERVAL '30 seconds',
    lease_version = lease_version + 1,
    attempt_count = attempt_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING'
  AND lease_until < CURRENT_TIMESTAMP
RETURNING lease_version;
```

#### 임대 시간만으로 충분하지 않은 이유

```
Consumer A (lease_version = 1)
→ 처리가 예상보다 오래 걸림 → lease 만료

Consumer B
→ 권한 회수 (lease_version = 2)

A와 B가 잠시 동시에 실행될 수 있음
```

최종 반영 시 자신이 획득한 버전을 조건에 포함한다.

```sql
UPDATE coupon_issue_request
SET status = 'ISSUED',
    coupon_issue_id = :couponIssueId,
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE request_id = :requestId
  AND status = 'PROCESSING'
  AND lease_version = :acquiredLeaseVersion;
```

```
Consumer A의 version = 1
DB의 version         = 2
→ 0행 → 오래된 처리자의 최종 상태 반영 차단
```

단순 UUID 소유권 토큰도 현재 소유자 비교에는 쓸 수 있다. 다만 **Fencing Token**이라고 부를 때는 새 처리자가 항상 더 큰 값을 받는 **단조 증가 버전**을 의미하며, 외부 저장소도 이 버전을 검사해야 오래된 처리자의 쓰기를 거부할 수 있다.

Fencing Version이 있어도 조건부 UPDATE, `UNIQUE(event_id, user_id)`, DB 트랜잭션은 계속 필요하다.

이번 과제의 발급 로직은 짧은 DB 작업으로 끝나므로 **단일 트랜잭션 방식을 기본으로 선택한다.** Lease 방식은 외부 시스템 호출처럼 작업이 길어질 때 고려한다.

---

## 8. Kafka Offset 처리

### 8-1. 안전한 처리 순서

```
1. Kafka 메시지를 읽는다.
2. DB 트랜잭션을 시작한다.
3. 요청 처리 권한을 얻는다.
4. 수량과 발급 기록을 처리한다.
5. 요청 상태를 최종 상태로 변경한다.
6. DB 트랜잭션을 Commit한다.
7. Kafka Offset을 Commit한다.
```

원칙은 하나다.

```
DB 결과 확정 → Kafka Offset Commit
```

Offset 커밋은 Kafka 관점의 처리 완료 표시이고, DB 커밋은 비즈니스 관점의 처리 완료다. 두 시점의 순서를 잘못 잡으면 유실이나 중복이 생긴다.

### 8-2. Offset을 먼저 Commit하면

```
1. Offset Commit
2. DB 처리 시도
3. DB 장애
```

Kafka는 메시지가 처리되었다고 기억하지만 DB에는 발급 결과가 없다. Consumer가 다시 실행되어도 해당 레코드가 재전달되지 않으므로 **사용자 요청이 유실된 것과 같은 결과**가 된다.

### 8-3. DB Commit 후 Offset Commit 전에 장애

이 상황은 완전히 제거할 수 없다.

```
DB Commit 성공 → Consumer 종료 → Offset Commit 실패
```

DB에는 `request = ISSUED`, `coupon_issue` 존재, `issued_count` 증가가 남아 있고, Kafka에는 완료 Offset이 없다. Consumer가 다시 실행되면 같은 레코드가 전달된다.

```
재전달된 req-001
→ status = ISSUED 확인
→ 새 발급 없음
→ 메시지 처리 성공 → Offset Commit
```

중복 전달을 허용하되 Consumer 멱등성으로 안전하게 흡수한다.

### 8-4. 자동 커밋 주의

Kafka Consumer의 자동 커밋 시점은 DB 트랜잭션 커밋 시점과 정확히 맞지 않을 수 있다. 발급 처리에서는 다음을 확인한다.

```
- 자동 커밋 사용 여부
- 수동 Acknowledgment 방식
- Listener 예외 발생 시 Offset 처리
- DB Rollback 시 메시지 재전달 방식
- Retry Topic 이동 시 원본 레코드 완료 처리
```

프레임워크 설정은 달라도 원칙은 같다. DB 결과가 확정된 뒤 메시지를 완료 처리하도록 구성한다.

### 8-5. At-Least-Once와 비즈니스 결과

이 구조는 Kafka 메시지가 한 번 이상 전달될 수 있음을 받아들인다.

```
메시지 전달 횟수 → 한 번 이상일 수 있음
쿠폰 발급 결과   → 한 번만 반영
```

정확히 한 번 처리되는 것처럼 보이게 만드는 핵심은 전달 횟수를 통제하는 것이 아니라, **DB 결과의 중복 반영을 막는 데** 있다.

---

## 9. 서로 다른 저장소 사이의 불일치

API Server의 처리에는 Redis, DB, Kafka가 함께 사용되지만 이 작업들은 하나의 트랜잭션으로 묶이지 않는다.

```
- Redis에는 통과 기록이 있지만 DB 요청 행이 없는 상태
- DB에는 PENDING 요청이 있지만 Kafka 메시지가 없는 상태
- 같은 requestId 메시지가 두 번 저장된 상태
```

### 9-1. PENDING 저장 뒤 Kafka 발행 실패

```
Redis → 통과 기록 존재
DB    → request PENDING
Kafka → 메시지 없음
```

Consumer는 이 요청을 알 수 없으므로 요청이 계속 `PENDING`에 남는다.

```
- Kafka 발행이 확인되지 않았다면 202 Accepted를 반환하지 않는다.
- 발행을 재시도한다.
- 오래된 PENDING 요청을 찾는 복구 배치를 둔다.
- 발행 실패 횟수와 마지막 오류를 기록한다.
```

### 9-2. Kafka 발행 Timeout의 모호함

Producer가 Timeout을 받았다고 해서 메시지가 반드시 저장되지 않은 것은 아니다.

```
상황 A: Broker가 저장하지 못함 → Timeout
상황 B: Broker는 저장했지만 ACK만 유실 → Timeout
```

Producer는 두 상황을 구분하지 못한다. 같은 `requestId`로 재발행하면 동일 요청 레코드가 두 개 생길 수 있다.

```
Offset 100 → req-001
Offset 101 → req-001
```

Consumer는 첫 메시지만 실제 발급하고 두 번째는 기존 최종 상태로 처리한다. 즉 **중복 발행을 허용하고 Consumer 멱등성으로 방어**한다.

### 9-3. Outbox가 해결하는 범위

요청 상태 저장과 Kafka 발행 사이의 불일치는 Outbox로 줄일 수 있다.

```
DB Transaction

coupon_issue_request INSERT
outbox_event INSERT

→ 함께 Commit
```

별도 Publisher가 Outbox 행을 읽어 발행하고, 실패하면 Outbox 행을 기준으로 다시 시도한다.

하지만 Outbox가 다음 구간까지 원자적으로 묶어주지는 않는다.

```
Redis 선착순 판정
↔ DB 요청 상태 저장
```

이 구간에는 여전히 보상이나 복구 정책이 필요하다.

### 9-4. Redis와 DB 사이의 복구

Redis에 통과 기록이 있지만 DB 요청 행이 없다면 다음을 고려한다.

```
- 동일 requestId의 DB 등록을 다시 시도한다.
- Redis 기록의 생성 시각을 확인하고 잠시 기다린다.
- 일정 시간 이상 DB 행이 없으면 Redis 자리를 보상한다.
- 복구 대상 목록과 메트릭을 남긴다.
```

보상으로 수량을 되돌릴 때도 다른 요청과 경쟁하므로 Lua Script로 조건을 확인해야 한다. 현재 사용자에게 저장된 `requestId`가 복구하려는 요청과 같은지 확인한 뒤 삭제하거나 수량을 되돌린다.

### 9-5. 장애 시점별 결과 요약

| 장애 시점 | DB 상태 | 이후 처리 |
| --- | --- | --- |
| Consumer가 DB 처리 전 종료 | `PENDING`, 발급 없음 | Offset 미반영 → 재전달 후 처리 |
| 단일 트랜잭션 처리 중 종료 | Rollback → `PENDING`, 수량 원복 | 재전달 후 재처리 |
| DB Commit 후 Offset Commit 전 종료 | `ISSUED`, 발급 존재 | 재전달 시 기존 결과 확인 후 완료 |
| Offset Commit까지 성공 후 종료 | `ISSUED` | 정상 완료 |
| 별도 Claim Commit 후 종료 | `PROCESSING`, 발급 없음 | lease 만료 후 권한 회수 |
| Redis SUCCESS 후 DB 등록 실패 | 요청 행 없음 | 등록 재시도 또는 Redis 보상 |
| PENDING 저장 후 Kafka 발행 실패 | `PENDING`, 메시지 없음 | 발행 재시도 또는 Outbox 복구 |

### 9-6. 분산 트랜잭션 대신 재실행 가능한 설계

Redis, DB, Kafka를 하나의 로컬 트랜잭션으로 묶을 수는 없다. 모든 단계를 한 번만 실행하도록 만드는 것보다, 각 단계의 결과를 기록하고 다시 실행해도 안전하게 만드는 편이 현실적이다.

```
Redis 판정      → requestId와 사용자 기준으로 중복 확인
DB 요청 등록    → Primary Key와 requestHash로 중복 확인
Kafka 발행      → 같은 requestId 재발행 허용
Consumer 처리   → 조건부 상태 전이와 DB 제약으로 중복 방어
```

각 저장소의 경계에서 어떤 불일치가 생길 수 있는지 알고, 재시도와 보상 기준을 정하는 것이 핵심이다.

---

## 10. 자주 하는 설계 실수

### 10-1. requestId 확인 없이 바로 발급

```
Kafka 메시지 수신 → coupon_event UPDATE → coupon_issue INSERT
```

같은 메시지가 재전달될 때마다 발급 로직이 다시 실행된다.

### 10-2. SELECT 후 상태 변경

```
SELECT status → 애플리케이션에서 PENDING 확인 → UPDATE PROCESSING
```

Check와 Act 사이에 다른 Consumer가 끼어든다. 반드시 `WHERE status = 'PENDING'`을 포함한 조건부 UPDATE로 바꾼다.

### 10-3. 상태를 조건 없이 변경

```sql
UPDATE coupon_issue_request
SET status = 'ISSUED'
WHERE request_id = :requestId;
```

최종 `FAILED`를 `ISSUED`로 덮어쓰거나, 오래된 Consumer가 최신 결과를 변경할 수 있다.

### 10-4. Rollback 후 상태를 잘못 가정

`UNIQUE` 위반으로 롤백한 뒤 `WHERE status = 'PROCESSING'`으로 `FAILED`를 시도하면 항상 0행이다. 롤백으로 상태가 `PENDING`으로 되돌아갔기 때문이다. 요청이 영원히 `PENDING`에 남는다.

### 10-5. DB 처리 전에 Offset Commit

DB 실패 시 Kafka가 메시지를 다시 전달하지 않아 요청이 유실된다.

### 10-6. 중복 메시지를 예외로 처리

```
status = ISSUED → DuplicateMessageException → Retry → DLQ
```

이미 성공한 요청이 불필요하게 재시도된다. 최종 상태 확인 후 아무 변경 없이 종료하는 것이 올바른 멱등 처리다.

### 10-7. 외부 네트워크 호출을 발급 트랜잭션에 포함

```
Transaction 시작 → Row Lock 획득 → 알림 API 3초 대기 → Commit
```

Row Lock 보유 시간이 길어지고 Connection Pool이 오래 점유되며, 외부 서비스 장애가 핵심 발급 흐름에 전파된다. 알림과 마이페이지 갱신은 결과 이벤트로 분리한다.

### 10-8. 상태 조회 API에서 소유자를 확인하지 않음

`requestId`만 알면 다른 사용자의 발급 결과를 조회할 수 있게 된다. 조회 조건에 인증된 `userId`를 반드시 포함한다.

---

## 11. 전체 흐름 정리

```
+--------+
| Client |
+--------+
    |
    | Idempotency-Key: req-001
    v
+--------------------------------------+
| API Server                           |
|--------------------------------------|
| 인증된 userId 확인                    |
| requestId 형식 검증                   |
| requestHash 생성                     |
+--------------------------------------+
    |
    v
+--------------------------------------+
| Redis Lua Script                     |
|--------------------------------------|
| eventId + userId 중복 확인            |
| 수량 확인                            |
| userId -> requestId 확인             |
+--------------------------------------+
    |
    +-- IDEMPOTENT_RETRY → 기존 DB 상태 반환
    +-- DUPLICATE_USER   → 새 Kafka 발행 금지
    +-- SOLD_OUT         → 품절 응답
    +-- SUCCESS
           |
           v
+--------------------------------------+
| coupon_issue_request                 |
|--------------------------------------|
| request_id = req-001                 |
| status = PENDING                     |
| request_hash = ...                   |
+--------------------------------------+
    |
    | Kafka 발행
    v
+--------------------------------------+
| coupon.issue.requested               |
| key = requestId                      |
+--------------------------------------+
    |
    v
+--------------------------------------+
| Coupon Issue Consumer                |
|--------------------------------------|
| 메시지와 요청 행 일치 검증            |
| PENDING 처리 권한 조건부 획득         |
| 기존 ISSUED / FAILED 확인             |
+--------------------------------------+
    |
    v
+--------------------------------------+
| DB Transaction                       |
|--------------------------------------|
| request PENDING → PROCESSING         |
| coupon_event 조건부 UPDATE            |
| coupon_issue INSERT                  |
| request ISSUED / FAILED 변경          |
+--------------------------------------+
    |
    | Commit 성공
    v
+--------------------------------------+
| Kafka Offset Commit                  |
+--------------------------------------+
```

### 11-1. 요청 상태별 Consumer와 Offset 처리 기준

| 요청 상태 | Consumer 처리 | Offset 처리 |
| --- | --- | --- |
| `PENDING` | 조건부 UPDATE로 권한 획득 후 발급 처리 | DB 결과 확정 후 Commit |
| `PROCESSING` | 단일 트랜잭션인지 별도 Claim인지에 따라 판단 | 재시도 또는 lease 확인 |
| `ISSUED` | 기존 성공 결과 유지, 새 발급 없음 | Commit |
| `FAILED` | 기존 최종 실패 유지 | Commit |
| 요청 행 없음 | 잘못된 메시지 또는 등록 불일치 | Retry·복구·DLQ 정책 |
| 메시지 내용 불일치 | 발급 금지 | DLQ 후보 |

### 11-2. 핵심 요약

```
requestId는
같은 논리적 요청의 재처리를 막는다.

request_hash는
같은 키가 다른 요청에 재사용되는 것을 막는다.

UNIQUE(event_id, user_id)는
같은 사용자의 최종 중복 발급을 막는다.

조건부 상태 전이는
여러 Consumer 중 한 명만 처리하도록 만든다.

DB 트랜잭션은
수량, 발급 기록, 요청 상태의 일관성을 지킨다.

Offset은 DB 커밋 뒤에 반영한다.

Redis, DB, Kafka는 하나로 묶을 수 없으므로
각 단계를 재실행 가능하게 만들고 복구 정책을 정한다.
```
