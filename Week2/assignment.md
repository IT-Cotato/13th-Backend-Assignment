# 2주차 과제

# Kafka 기반 비동기 쿠폰 발급 요청 처리 구조 설계

## 1. 과제 목표

이번 2주차 과제의 목표는 **Kafka를 이용해 쿠폰 발급 요청을 비동기적으로 처리하는 구조를 설계하는 것**이다.

1주차에서는 API Server가 모든 쿠폰 발급 처리를 직접 수행하는 구조와, 요청 접수와 실제 처리를 분리하는 비동기 구조를 비교했다.

이번 주차에서는 그 비동기 구조를 Kafka 기반으로 구체화한다.

이번 과제에서 설계해야 하는 기본 흐름은 다음과 같다.

```
Client
  ↓
API Server
  ↓
Kafka Producer
  ↓
coupon.issue.requested Topic
  ↓
Coupon Issue Consumer
  ↓
DB
```

이번 과제에서 중요하게 볼 내용은 다음과 같다.

```
Topic 설계

Message 구조 설계

Partition Key 선택

Consumer Group 설계

Offset Commit과 중복 처리 가능성 이해

Consumer Lag이 사용자 경험에 미치는 영향 이해

Kafka 발행 실패 시 API 응답 설계
```

이번 과제의 핵심은 Kafka를 구현하는 것이 아니라, **Kafka를 시스템 구조 안에서 어떻게 사용할지 설계하는 것**이다.

---

## 2. 기본 상황

서비스에서 선착순 쿠폰 이벤트를 진행한다고 가정한다.

```
이벤트 이름: 여름맞이 5,000원 할인 쿠폰 이벤트
쿠폰 수량: 1,000개
예상 요청 수: 이벤트 시작 직후 1분 안에 최대 10만 건
발급 조건: 사용자 1명당 하나의 이벤트에서 쿠폰 1개만 발급 가능
응답 방식: 요청 직후 최종 발급 결과를 바로 받지 않아도 됨
결과 확인 방식: 발급 상태 조회 API 또는 마이페이지에서 확인
```

이번 주차에서는 API Server가 쿠폰 발급 요청을 받으면 Kafka에 메시지를 발행하고, Consumer가 해당 메시지를 읽어 실제 쿠폰 발급 처리를 수행하는 구조를 설계한다.

단, 이번 주차에서는 Redis 선착순 판정, DB Unique Key, requestId 멱등성, Outbox Pattern, Retry Topic, DLQ는 깊게 다루지 않는다.

---

## 3. 이번 과제의 전제 조건

이번 과제에서는 아래 전제를 따른다.

```
API Server는 쿠폰 발급 요청을 받으면 Kafka에 메시지를 발행한다.

Kafka 발행이 성공했다는 것은 Producer가 Kafka Broker로부터 발행 성공 응답을 받은 상황을 의미한다.

Kafka 발행이 실패했다면 아직 쿠폰 발급 요청이 시스템에 안전하게 접수된 것이 아니다.

따라서 Kafka 발행에 실패한 경우에는 사용자에게 요청 접수 성공 응답을 주면 안 된다.
```

또한 발급 상태 조회를 위해 요청을 식별할 수 있는 값이 필요하다.

```
requestId는 쿠폰 발급 요청을 식별하기 위한 값이다.

이번 주차에서는 requestId를 요청 식별자와 상태 조회에 필요한 값 정도로만 이해한다.

requestId를 이용한 멱등성 처리와 중복 요청 방지는 5주차에서 자세히 다룬다.
```

Consumer Lag 상황에서 사용자가 발급 상태를 조회할 수 있어야 하므로, 이번 과제에서는 발급 요청 상태를 저장할 수 있는 저장소가 있다고 가정한다.

```
상태 예시:
PENDING
ISSUED
FAILED
```

단, 상태 저장 테이블의 상세 설계는 이번 주차의 핵심이 아니다.

---

## 4. 제출 형식

제출 파일은 아래 경로에 작성해서 PR을 작성한다.

```
submissions/Week2/기수_이름.md
```

예시는 다음과 같다.

```
submissions/Week2/13기_김기민.md
```

제출 문서에는 아래 필수 과제를 모두 포함한다.

```
과제 1. Kafka 기반 비동기 처리 구조 그리기

과제 2. Topic 설계하기

과제 3. Kafka Message 구조 설계하기

과제 4. Kafka 발행 성공 / 실패에 따른 API 응답 설계하기

과제 5. Partition Key 선택하기

과제 6. Consumer Group과 Partition 수 설계하기

과제 7. Offset Commit 실패 상황 분석하기

과제 8. Consumer Lag이 생겼을 때 사용자 경험 설계하기

과제 9. 나쁜 Kafka 설계의 문제점 찾기
```

---

# 5. 필수 과제

---

## 과제 1. Kafka 기반 비동기 처리 구조 그리기

쿠폰 발급 요청이 들어왔을 때 Kafka를 이용해 비동기적으로 처리하는 구조를 다이어그램으로 작성한다.

반드시 아래 구성 요소를 포함해야 한다.

```
Client

API Server

Kafka Producer

coupon.issue.requested Topic

Coupon Issue Consumer

DB
```

### 1. 전체 요청 처리 흐름 다이어그램

```

```

### 2. API Server의 역할

```

```

### 3. Kafka의 역할

```

```

### 4. Consumer의 역할

```

```

### 5. DB의 역할

```

```

### 6. 사용자가 요청 직후 최종 발급 결과를 바로 받지 않는 이유

```

```

### 7. 요청 접수 성공과 쿠폰 발급 성공의 차이

```

```

---

## 과제 2. Topic 설계하기

쿠폰 발급 요청 메시지를 담을 Kafka Topic을 설계한다.

### 1. Topic 이름

```

```

### 2. Topic에 담기는 메시지의 의미

```

```

### 3. Producer는 어떤 컴포넌트이며 언제 메시지를 발행하는가?

```

```

### 4. Consumer는 어떤 컴포넌트이며 어떤 처리를 수행하는가?

```

```

### 5. 이 Topic이 필요한 이유

```

```

### 6. 이 Topic에 메시지가 들어갔다는 것은 어떤 의미인가?

```

```

### 7. 이 Topic에 메시지가 들어갔다는 것이 쿠폰 발급 완료를 의미하지 않는 이유

```

```

---

## 과제 3. Kafka Message 구조 설계하기

`coupon.issue.requested` Topic에 발행할 메시지 구조를 JSON 형태로 설계한다.

### 1. Kafka Message JSON 예시

```json
{
  "requestId": ,
  "eventId": ,
  "userId": ,
  "requestedAt":
}
```

### 2. 각 필드의 의미

| 필드 | 의미 | 필요한 이유 |
| --- | --- | --- |
| requestId |  |  |
| eventId |  |  |
| userId |  |  |
| requestedAt |  |  |

### 3. Consumer가 이 메시지를 보고 어떤 처리를 할 수 있는가?

```

```

### 4. 메시지에 너무 많은 정보를 넣는 것이 왜 좋지 않을 수 있는가?

```

```

### 5. requestId는 이번 주차에서 어떤 정도로만 이해하면 되는가?

```

```

---

## 과제 4. Kafka 발행 성공 / 실패에 따른 API 응답 설계하기

API Server는 사용자의 쿠폰 발급 요청을 받은 뒤 Kafka에 메시지를 발행한다.

이때 Kafka 발행이 성공한 경우와 실패한 경우의 API 응답을 각각 설계한다.

이번 과제에서는 Kafka 발행 성공을 아래와 같이 정의한다.

```
Kafka 발행 성공:
Producer가 Kafka Broker로부터 발행 성공 응답을 받은 경우

Kafka 발행 실패:
Producer가 Kafka Broker에 메시지를 정상적으로 발행하지 못한 경우
```

---

### 상황 A. Kafka 발행 성공

API Server가 Kafka에 쿠폰 발급 요청 메시지를 정상적으로 발행했다.

이때 사용자에게 어떤 응답을 줄 것인지 작성한다.

#### 1. Kafka 발행 성공 시 응답 JSON

```json
{
  "status": ,
  "message": ,
  "requestId":
}
```

#### 2. 이 응답이 쿠폰 발급 성공을 의미하지 않는 이유

```

```

#### 3. 요청 접수 성공과 쿠폰 발급 성공의 차이

```

```

#### 4. requestId를 응답에 포함하는 이유

```

```

---

### 상황 B. Kafka 발행 실패

API Server가 Kafka에 메시지를 발행하려고 했지만 실패했다.

이때 사용자에게 요청 접수 성공 응답을 줘도 되는지 판단하고, 적절한 응답을 설계한다.

#### 1. Kafka 발행 실패 시 응답 JSON

```json
{
  "status": ,
  "message": ,
  "requestId":
}
```

#### 2. Kafka 발행에 실패했는데 요청 접수 성공 응답을 주면 안 되는 이유

```

```

#### 3. 이 상황에서 API Server가 주의해야 할 점

```

```

---

## 과제 5. Partition Key 선택하기

`coupon.issue.requested` Topic의 Partition Key를 무엇으로 정할지 선택한다.

아래 후보 중 하나를 선택하거나, 직접 다른 Key를 제안해도 된다.

```
1. userId

2. eventId

3. requestId

4. 그 외 직접 선택
```

특히 `eventId`를 Partition Key로 선택한 경우에는 아래 상황을 반드시 고려한다.

```
이벤트 시작 직후 대부분의 요청이 eventId = 1 하나에 몰린다.
```

이때 모든 메시지가 특정 Partition에 몰릴 수 있는지 설명한다.

이번 과제에서는 정답 Partition Key를 하나로 고정하지 않는다.

중요한 것은 자신이 선택한 Partition Key가 아래 항목에 어떤 영향을 주는지 설명하는 것이다.

```
메시지 분산

순서 보장

특정 Partition 쏠림 가능성

Consumer 병렬 처리
```

### 1. Partition Key 후보별 장단점 비교

| Partition Key 후보 | 장점 | 단점 |
| --- | --- | --- |
| userId |  |  |
| eventId |  |  |
| requestId |  |  |

### 2. 내가 선택한 Partition Key

```

```

### 3. 선택한 이유

```

```

### 4. 메시지 분산에 어떤 영향을 주는가?

```

```

### 5. 순서 보장에 어떤 영향을 주는가?

```

```

### 6. 특정 Partition에 메시지가 몰릴 가능성은 없는가?

```

```

### 7. 이 Key를 선택했을 때 발생할 수 있는 단점

```

```

### 8. eventId를 Partition Key로 사용할 때 주의해야 할 점

```

```

### 9. Kafka는 어떤 단위에서 메시지 순서를 보장하는가? 서로 다른 Partition에 들어간 메시지 A, B의 전체 처리 순서는 보장되는가? 그 이유는?

```

```

---

## 과제 6. Consumer Group과 Partition 수 설계하기

쿠폰 발급 요청을 처리하는 Consumer Group을 설계한다.

기본 조건은 다음과 같다.

```
이벤트 시작 직후 1분 안에 최대 10만 건의 요청이 들어올 수 있다.

Consumer는 coupon.issue.requested Topic을 읽는다.

Consumer는 실제 쿠폰 발급 처리를 수행한다.
```

이번 과제에서 정확한 처리량 계산이 목적은 아니다.

중요한 것은 아래 내용을 이해하고 설명하는 것이다.

```
Partition 수가 Consumer 병렬 처리에 어떤 영향을 주는가?

Consumer Group은 왜 필요한가?

Consumer 수와 Partition 수의 관계는 무엇인가?

Consumer 수만 무작정 늘리면 왜 안 되는가?
```

### 1. Topic 이름

```

```

### 2. Partition 수

```

```

### 3. Consumer Group 이름

```

```

### 4. Consumer 수

```

```

### 5. Partition과 Consumer 매핑

```
(매핑 예시: Partition 0 → Consumer 1)
```

### 6. 이 구조를 선택한 이유

```

```

### 7. Partition이 3개이고 Consumer가 5개라면 실제로 동시에 처리할 수 있는 Consumer는 최대 몇 개인가?

```

```

### 8. Consumer 수가 Partition 수보다 많으면 어떤 일이 발생하는가?

```

```

### 9. Consumer 수가 Partition 수보다 적으면 어떤 일이 발생하는가?

```

```

### 10. Consumer 수만 무작정 늘리면 안 되는 이유는 무엇인가?

```

```

---

## 과제 7. Offset Commit 실패 상황 분석하기

Kafka Consumer가 메시지를 처리하는 도중 장애가 발생하는 상황을 분석한다.

아래 상황을 읽고 질문에 답한다.

```
1. Consumer가 Kafka 메시지를 읽었다.

2. Consumer가 DB 저장까지 성공했다.

3. 하지만 Offset Commit 전에 Consumer가 장애로 종료되었다.

4. Consumer가 다시 실행되었다.
```

이번 과제에서는 이 문제를 완전히 해결하는 것이 목적이 아니다.

중요한 것은 Kafka Consumer에서 아래 상황이 발생할 수 있음을 이해하는 것이다.

```
DB 처리는 성공했지만 Offset Commit은 실패할 수 있다.

Offset Commit이 실패하면 Kafka는 해당 메시지가 처리 완료되었다고 판단하지 못할 수 있다.

그 결과 같은 메시지가 다시 읽히고 다시 처리될 수 있다.
```

### 1. Consumer가 다시 실행되면 어떤 메시지를 다시 읽을 수 있는가?

```

```

### 2. Kafka 입장에서는 왜 이 메시지가 처리 완료되었다고 판단하지 못하는가?

```

```

### 3. 이 상황에서 같은 메시지가 다시 처리될 수 있는가?

```

```

### 4. 같은 메시지가 다시 처리되면 쿠폰 발급 시스템에서는 어떤 문제가 생길 수 있는가?

```

```

### 5. 이 문제를 해결하려면 나중에 어떤 장치가 필요할 수 있는가?

아래 키워드를 참고해서 작성한다.

```
DB Unique Key

requestId 기반 멱등성

처리 상태 저장

중복 메시지 방어
```

답변:

```

```

---

## 과제 8. Consumer Lag이 생겼을 때 사용자 경험 설계하기

Kafka를 사용하면 API Server는 사용자에게 빠르게 요청 접수 응답을 줄 수 있다.

하지만 Consumer 처리 속도가 느리면 Kafka에 메시지가 쌓일 수 있다.

아래 상황을 읽고 답한다.

```
이벤트 시작 직후 요청이 급격히 증가했다.

API Server는 Kafka에 메시지를 빠르게 발행하고 있다.

하지만 Consumer 처리 속도가 Producer 발행 속도보다 느리다.

Kafka에 아직 처리되지 않은 메시지가 계속 쌓이고 있다.
```

이번 과제에서는 사용자가 발급 상태를 조회할 수 있도록 요청 상태가 저장되어 있다고 가정한다.

상태는 아래와 같이 단순화해서 사용한다.

```
PENDING: 요청은 접수되었지만 아직 처리되지 않은 상태

ISSUED: 쿠폰 발급이 성공한 상태

FAILED: 쿠폰 발급이 실패한 상태
```

### 1. Consumer Lag이란 무엇인가?

```

```

### 2. Consumer Lag이 증가하면 사용자 경험에 어떤 영향이 있는가?

```

```

### 3. 요청 직후 사용자에게 줄 응답 JSON

```json
{
  "status": ,
  "message": ,
  "requestId":
}
```

### 4. 결과 조회 API에서 PENDING 상태일 때 응답 JSON

```json
{
  "status": ,
  "message": ,
  "requestId":
}
```

### 5. 발급 성공 시 응답 JSON

```json
{
  "status": ,
  "message": ,
  "requestId":
}
```

### 6. 발급 실패 시 응답 JSON

```json
{
  "status": ,
  "message": ,
  "requestId":
}
```

### 7. 운영자가 Consumer Lag을 보고 어떤 판단을 할 수 있는가?

```

```

### 8. Kafka를 사용해도 최종 발급 결과가 늦어질 수 있는 이유

```

```

---

## 과제 9. 나쁜 Kafka 설계의 문제점 찾기

아래 Kafka 설계를 읽고 문제점을 찾는다.

```
Client
  ↓
API Server
  ↓
Kafka 발행
  ↓
coupon.issue.requested Topic
  ↓
Partition 1개
  ↓
Consumer 1개
  ↓
DB 저장
```

추가 조건은 다음과 같다.

```
이벤트 시작 직후 1분 안에 최대 10만 건의 요청이 들어온다.

Partition Key는 eventId로 설정했다.

대부분의 요청은 eventId = 1이다.

Kafka 발행에 실패해도 API Server는 사용자에게 요청 접수 성공 응답을 준다.

Consumer가 DB 저장 후 Offset Commit 전에 죽는 상황은 고려하지 않았다.

Consumer Lag이 증가해도 사용자는 처리 상태를 확인할 수 없다.
```

위 설계의 문제점을 5가지 이상 작성하고, 각각의 개선 방향을 작성한다.

### 문제점과 개선 방향

| 문제점 | 왜 문제인가? | 개선 방향 |
| --- | --- | --- |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |

---
