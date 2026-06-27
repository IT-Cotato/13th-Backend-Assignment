# **2주차 개념 설명**

# **Kafka 기반 비동기 쿠폰 발급 요청 처리 구조**

## **1. 이번 주차에서 다룰 내용**

1주차에서는 선착순 쿠폰 발급 요청을 **동기적으로 바로 처리하는 구조**와 **비동기적으로 나중에 처리하는 구조**를 비교했다.

이번 2주차에서는 1주차에서 다룬 비동기 구조를 조금 더 구체화한다.

이번 주차의 핵심은 다음과 같다.

```
API Server가 쿠폰 발급 요청을 직접 끝까지 처리하지 않고,
Kafka에 쿠폰 발급 요청 메시지를 발행한 뒤,
Consumer가 Kafka 메시지를 읽어서 실제 쿠폰 발급 처리를 수행하는 구조를 설계한다.
```

즉, 이번 주차의 주제는 다음과 같다.

```
Kafka 기반 비동기 쿠폰 발급 요청 처리 구조 설계
```

이번 주차에서 중요하게 볼 개념은 다음과 같다.

```
Topic
Partition
Message
Partition Key
Producer
Consumer
Consumer Group
Offset
Offset Commit
Consumer Lag
```

이번 주차의 목표는 Kafka를 깊게 구현하는 것이 아니다.

중요한 것은 다음 질문에 답할 수 있는 것이다.

```
쿠폰 발급 요청을 어떤 Topic에 보낼 것인가?

Kafka 메시지에는 어떤 데이터가 들어가야 하는가?

Partition Key는 무엇으로 정할 것인가?

Consumer Group은 어떻게 구성할 것인가?

Consumer 처리가 늦어지면 사용자 경험은 어떻게 달라지는가?

Kafka 메시지는 항상 한 번만 처리된다고 볼 수 있는가?
```

---

# **2. 1주차 복습: 왜 비동기 처리가 필요했는가?**

선착순 쿠폰 이벤트에서는 특정 시점에 요청이 한 번에 몰릴 수 있다.

예를 들어 다음과 같은 이벤트가 있다고 가정한다.

```
이벤트 이름: 여름맞이 5,000원 할인 쿠폰 이벤트
쿠폰 수량: 1,000개
예상 요청 수: 이벤트 시작 직후 1분 안에 최대 10만 건
발급 조건: 사용자 1명당 1개만 발급 가능
```

이벤트 시작 시간이 되면 많은 사용자가 동시에 쿠폰 발급 버튼을 누른다.

이때 API Server가 모든 요청을 직접 처리한다고 생각해보자.

```
Client
  ↓
API Server
  ↓
DB 조회
  ↓
DB 저장
  ↓
최종 발급 결과 응답
```

이 구조에서는 API Server가 요청 하나를 받을 때마다 직접 DB를 조회하고 저장해야 한다.

요청 수가 적을 때는 큰 문제가 없을 수 있다.

하지만 이벤트 시작 직후처럼 요청이 한 번에 몰리면 문제가 생긴다.

```
문제 1. API Server의 요청 처리 시간이 길어진다.

문제 2. DB에 동시에 많은 요청이 몰린다.

문제 3. 사용자는 응답을 오래 기다릴 수 있다.

문제 4. API Server 장애나 DB 부하가 전체 사용자 경험에 직접 영향을 준다.
```

그래서 선착순 쿠폰 발급 시스템에서는 요청을 받은 즉시 최종 발급 결과를 응답하지 않아도 된다고 가정한다.

대신 API Server는 사용자에게 먼저 다음과 같은 응답을 줄 수 있다.

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다."
}
```

여기서 중요한 점은 다음과 같다.

```
요청 접수 성공과 쿠폰 발급 성공은 다르다.
```

요청 접수 성공은 API Server가 사용자의 요청을 정상적으로 받았다는 뜻이다.

쿠폰 발급 성공은 실제로 쿠폰이 발급되어 DB에 저장되었다는 뜻이다.

비동기 구조에서는 이 두 개를 분리해서 생각해야 한다.

```
요청 접수
→ 빠르게 응답

실제 발급 처리
→ 뒤에서 Consumer가 처리
```

이때 API Server와 실제 발급 처리 로직 사이에서 요청을 안전하게 전달해주는 역할을 Kafka가 담당한다.

---

# **3. Kafka를 사용한 기본 구조**

!image.png

Kafka를 사용하면 API Server와 실제 쿠폰 발급 처리 로직을 분리할 수 있다.

기본 구조는 다음과 같다.

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

각 구성 요소의 역할은 다음과 같다.

| **구성 요소** | **역할** |
| --- | --- |
| Client | 쿠폰 발급 요청을 보내는 사용자 |
| API Server | 요청을 받고 Kafka에 쿠폰 발급 요청 메시지를 발행 |
| Kafka Producer | Kafka Topic에 메시지를 보내는 역할 |
| Kafka Topic | 쿠폰 발급 요청 메시지가 저장되는 공간 |
| Consumer | Kafka에서 메시지를 읽고 실제 쿠폰 발급 처리 수행 |
| DB | 최종 쿠폰 발급 기록 저장 |

이 구조에서 API Server는 실제 쿠폰 발급 처리를 끝까지 수행하지 않는다.

API Server는 사용자의 요청을 검증하고, Kafka에 메시지를 발행한 뒤, 사용자에게 빠르게 응답한다.

```
API Server의 핵심 역할

1. 쿠폰 발급 요청을 받는다.
2. 기본적인 요청값을 검증한다.
3. Kafka에 쿠폰 발급 요청 메시지를 발행한다.
4. 사용자에게 요청 접수 응답을 반환한다.
```

Consumer는 Kafka에 쌓인 메시지를 읽고 실제 발급 처리를 수행한다.

```
Consumer의 핵심 역할

1. Kafka Topic에서 쿠폰 발급 요청 메시지를 읽는다.
2. 메시지에 들어있는 eventId, userId 등을 확인한다.
3. 실제 쿠폰 발급 처리를 수행한다.
4. 최종 발급 결과를 DB에 저장한다.
```

정리하면 다음과 같다.

```
API Server는 요청 접수를 담당한다.
Consumer는 실제 처리를 담당한다.
Kafka는 두 컴포넌트 사이에서 요청 메시지를 전달한다.
```

---

# **4. Kafka를 단순 Queue처럼 보면 안 되는 이유**

Kafka를 처음 보면 단순히 메시지를 잠깐 담아두는 Queue처럼 느껴질 수 있다.

일반적인 Queue를 단순화해서 보면 다음과 같다.

```
Producer가 메시지를 넣는다.
Consumer가 메시지를 가져간다.
가져간 메시지는 Queue에서 사라진다.
```

하지만 Kafka는 이렇게만 이해하면 부족하다.

Kafka에서는 메시지를 Consumer가 읽었다고 해서 바로 삭제하지 않는다.

메시지는 Topic의 Partition에 일정 기간 저장된다.

그리고 Consumer는 자신이 어디까지 읽었는지를 Offset으로 관리한다.

```
Kafka의 특징

1. 메시지는 Topic에 저장된다.
2. Topic은 여러 Partition으로 나뉠 수 있다.
3. Consumer가 메시지를 읽어도 메시지가 바로 삭제되지 않는다.
4. Consumer는 Offset을 통해 어디까지 읽었는지 관리한다.
5. Consumer가 다시 실행되면 마지막으로 Commit한 Offset 이후부터 다시 읽을 수 있다.
```

이 차이가 중요하다.

왜냐하면 Kafka에서는 같은 메시지를 다시 읽을 수도 있고, Consumer가 처리하지 못한 메시지가 Topic에 쌓일 수도 있기 때문이다.

따라서 Kafka를 사용하는 시스템에서는 다음 내용을 반드시 고려해야 한다.

```
메시지가 쌓이면 어떻게 되는가?

Consumer가 어디까지 처리했는지 어떻게 아는가?

Consumer가 죽었다가 다시 살아나면 어디서부터 다시 처리하는가?

같은 메시지를 다시 처리할 가능성은 없는가?
```

이 질문들이 Offset, Offset Commit, Consumer Lag 개념과 연결된다.

---

# **5. Kafka 핵심 개념**

## **5.1 Producer**

Producer는 Kafka에 메시지를 보내는 역할이다.

이번 쿠폰 발급 시스템에서는 API Server가 Producer 역할을 한다.

```
API Server
  ↓
Kafka Producer
  ↓
coupon.issue.requested Topic
```

사용자가 쿠폰 발급 버튼을 누르면 API Server는 쿠폰 발급 요청 메시지를 Kafka Topic에 발행한다.

예를 들어 다음과 같은 메시지를 보낼 수 있다.

```json
{
  "requestId": "req-123",
  "eventId": 1,
  "userId": 100,
  "requestedAt": "2026-06-26T12:00:00"
}
```

Producer 입장에서 중요한 것은 Kafka 발행이 성공했는지 확인하는 것이다.

Kafka에 메시지를 넣지 못했는데 사용자에게 요청 접수 성공 응답을 주면 문제가 생긴다.

왜냐하면 사용자는 요청이 접수되었다고 생각하지만, 실제로는 Kafka에 요청 메시지가 없기 때문이다.

```
Kafka 발행 성공
→ 사용자에게 요청 접수 성공 응답 가능

Kafka 발행 실패
→ 요청 접수 실패 응답을 주거나 재시도 전략 필요
```

이번 주차에서는 재시도 전략을 깊게 다루지는 않는다.

다만 다음 원칙은 기억해야 한다.

```
Kafka에 메시지가 정상적으로 발행되지 않았다면,
쿠폰 발급 요청이 접수되었다고 보면 안 된다.
```

---

## **5.2 Topic**

Topic은 Kafka에서 메시지가 저장되는 논리적인 공간이다.

쿠폰 발급 요청 메시지를 담기 위한 Topic을 하나 만든다면 다음과 같이 이름을 정할 수 있다.

```
coupon.issue.requested
```

이 이름은 다음 의미를 가진다.

```
coupon
→ 쿠폰 도메인

issue
→ 발급

requested
→ 발급 요청이 들어왔음
```

즉, `coupon.issue.requested` Topic은 다음 메시지를 담는다.

```
"쿠폰 발급 요청이 들어왔다."
```

Topic 이름은 너무 추상적이면 안 된다.

예를 들어 다음 이름은 좋지 않다.

```
event
message
queue
coupon
```

이름만 봐서는 어떤 메시지가 들어있는지 알기 어렵기 때문이다.

반면 다음 이름은 의미가 더 명확하다.

```
coupon.issue.requested
```

이 Topic을 보면 다음 내용을 예측할 수 있다.

```
쿠폰 발급 요청과 관련된 메시지가 들어있다.
아직 발급 완료가 아니라 발급 요청 단계의 메시지다.
이 Topic을 읽는 Consumer는 실제 쿠폰 발급 처리를 담당할 가능성이 높다.
```

---

## **5.3 Message**

Message는 Kafka Topic에 저장되는 하나의 데이터 단위다.

쿠폰 발급 요청 메시지에는 최소한 다음 정보가 필요하다.

```
requestId
eventId
userId
requestedAt
```

예시는 다음과 같다.

```json
{
  "requestId": "req-123",
  "eventId": 1,
  "userId": 100,
  "requestedAt": "2026-06-26T12:00:00"
}
```

각 필드의 의미는 다음과 같다.

| **필드** | **의미** |
| --- | --- |
| requestId | 하나의 요청을 식별하기 위한 ID |
| eventId | 어떤 쿠폰 이벤트에 대한 요청인지 나타내는 ID |
| userId | 어떤 사용자가 요청했는지 나타내는 ID |
| requestedAt | 사용자가 요청한 시간 |

여기서 `requestId`는 나중에 멱등성 처리와 연결된다.

하지만 이번 2주차에서는 `requestId`를 깊게 다루지 않는다.

이번 주차에서는 다음 정도만 이해하면 된다.

```
requestId는 요청 하나를 구분하기 위한 식별자다.
같은 요청이 여러 번 들어왔는지 판단할 때 사용할 수 있다.
구체적인 멱등성 처리는 5주차에서 다룬다.
```

Kafka 메시지를 설계할 때는 다음 질문을 해봐야 한다.

```
Consumer가 이 메시지만 보고 필요한 처리를 할 수 있는가?

메시지에 너무 많은 정보가 들어가 있지는 않은가?

메시지에 꼭 필요한 식별자가 들어가 있는가?

나중에 문제를 추적할 수 있는 정보가 들어가 있는가?
```

예를 들어 Consumer가 쿠폰 발급 처리를 하려면 최소한 어떤 이벤트인지, 어떤 사용자인지 알아야 한다.

그래서 `eventId`와 `userId`는 필요하다.

또한 나중에 장애나 중복 요청을 추적하려면 `requestId`가 필요할 수 있다.

---

## **5.4 Partition**

!image.png

Partition은 하나의 Topic을 여러 조각으로 나눈 것이다.

Topic이 하나의 큰 메시지 저장 공간이라면, Partition은 그 저장 공간을 나누는 단위라고 볼 수 있다.

예를 들어 `coupon.issue.requested` Topic에 Partition이 3개 있다면 다음과 같다.

```
coupon.issue.requested Topic

Partition 0
Partition 1
Partition 2
```

메시지는 이 Partition 중 하나에 저장된다.

```
Message A → Partition 0
Message B → Partition 1
Message C → Partition 2
Message D → Partition 0
```

Partition이 중요한 이유는 병렬 처리 때문이다.

Partition이 1개라면 같은 Consumer Group 안에서 한 번에 하나의 Consumer만 해당 Partition을 처리할 수 있다.

```
Partition 1개
Consumer 3개

Partition 0 → Consumer 1

Consumer 2, Consumer 3은 같은 Group 안에서 이 Partition을 동시에 처리할 수 없음
```

반면 Partition이 3개라면 Consumer도 최대 3개까지 병렬로 처리할 수 있다.

```
Partition 3개
Consumer 3개

Partition 0 → Consumer 1
Partition 1 → Consumer 2
Partition 2 → Consumer 3
```

즉, Partition 수는 Consumer 병렬 처리와 직접 연결된다.

```
Partition 수가 너무 적으면 병렬 처리에 한계가 생긴다.
Consumer 수를 늘려도 Partition 수보다 더 많이 병렬 처리할 수 없다.
```

---

## **5.5 Partition Key**

!image.png

Partition Key는 메시지를 어떤 Partition에 보낼지 결정하는 기준이다.

Kafka Producer가 메시지를 보낼 때 Key를 지정하면, Kafka는 그 Key를 기준으로 메시지를 특정 Partition에 배치한다.

예를 들어 `userId`를 Partition Key로 사용한다고 해보자.

```
userId = 100 → Partition 0
userId = 101 → Partition 1
userId = 102 → Partition 2
userId = 100 → Partition 0
```

같은 Key를 가진 메시지는 보통 같은 Partition으로 들어간다.

따라서 `userId = 100`인 사용자의 요청은 같은 Partition에 들어가게 된다.

Partition Key가 중요한 이유는 두 가지다.

```
1. 메시지 분산
2. 순서 보장
```

메시지를 여러 Partition에 고르게 분산시키면 Consumer들이 병렬로 처리하기 좋다.

반대로 특정 Key에 메시지가 몰리면 특정 Partition에만 메시지가 쌓일 수 있다.

이것을 Partition 쏠림이라고 볼 수 있다.

또한 Kafka는 같은 Partition 안에서는 메시지 순서를 보장한다.

하지만 서로 다른 Partition 사이의 전체 순서는 보장하지 않는다.

```
Partition 0 안의 순서
→ 보장됨

Partition 1 안의 순서
→ 보장됨

Partition 0과 Partition 1 사이의 전체 순서
→ 보장되지 않음
```

그래서 Partition Key를 정할 때는 다음 질문을 해야 한다.

```
어떤 기준의 순서가 중요한가?

어떤 Key를 사용하면 메시지가 한 Partition에 몰리지 않는가?

Consumer가 병렬로 처리하기 좋은 구조인가?
```

---

## **5.6 Consumer**

!image.png

Consumer는 Kafka Topic에서 메시지를 읽어 처리하는 역할이다.

이번 쿠폰 발급 시스템에서는 `Coupon Issue Consumer`가 이 역할을 한다.

```
coupon.issue.requested Topic
  ↓
Coupon Issue Consumer
  ↓
DB 저장
```

Consumer는 Kafka에서 쿠폰 발급 요청 메시지를 읽는다.

그리고 메시지 안에 들어있는 `eventId`, `userId`, `requestId` 등을 바탕으로 실제 쿠폰 발급 처리를 수행한다.

Consumer의 기본 흐름은 다음과 같다.

```
1. Kafka Topic에서 메시지를 읽는다.
2. 메시지 내용을 확인한다.
3. 쿠폰 발급 로직을 수행한다.
4. DB에 발급 기록을 저장한다.
5. 처리가 끝난 메시지의 Offset을 Commit한다.
```

이번 2주차에서는 Consumer가 DB 저장을 수행한다고 가정한다.

하지만 DB 정합성, Unique Key, 중복 발급 방지 같은 내용은 4주차와 5주차에서 더 자세히 다룬다.

이번 주차에서는 Consumer의 역할을 다음 정도로 이해하면 된다.

```
Consumer는 Kafka에 쌓인 쿠폰 발급 요청 메시지를 읽고,
실제 쿠폰 발급 처리를 담당하는 컴포넌트다.
```

---

## **5.7 Consumer Group**

!image.png

Consumer Group은 여러 Consumer가 하나의 Topic 메시지를 나누어 처리할 수 있게 해주는 묶음이다.

예를 들어 다음과 같은 Consumer Group이 있다고 해보자.

```
Consumer Group: coupon-issue-group
Consumer 1
Consumer 2
Consumer 3
```

그리고 Topic의 Partition이 3개라면 다음처럼 처리할 수 있다.

```
coupon.issue.requested Topic

Partition 0 → Consumer 1
Partition 1 → Consumer 2
Partition 2 → Consumer 3
```

이 구조에서는 세 Consumer가 메시지를 나누어 처리한다.

그래서 Consumer가 하나일 때보다 처리량을 높일 수 있다.

```
Consumer 1개
→ 모든 메시지를 혼자 처리

Consumer 3개
→ Partition 단위로 메시지를 나누어 처리
```

하지만 Consumer 수를 무조건 많이 늘린다고 좋은 것은 아니다.

같은 Consumer Group 안에서는 하나의 Partition을 동시에 여러 Consumer가 처리할 수 없다.

예를 들어 Partition이 3개인데 Consumer가 5개라면 다음과 같다.

```
Partition 0 → Consumer 1
Partition 1 → Consumer 2
Partition 2 → Consumer 3

Consumer 4 → 할당받을 Partition 없음
Consumer 5 → 할당받을 Partition 없음
```

즉, Consumer 수가 Partition 수보다 많으면 일부 Consumer는 놀게 된다.

반대로 Partition이 5개인데 Consumer가 2개라면 한 Consumer가 여러 Partition을 담당할 수 있다.

```
Partition 0 → Consumer 1
Partition 1 → Consumer 1
Partition 2 → Consumer 2
Partition 3 → Consumer 2
Partition 4 → Consumer 2
```

정리하면 다음과 같다.

```
Consumer Group은 Consumer들이 메시지를 나누어 처리하게 해준다.

병렬 처리의 최대 단위는 Partition 수에 영향을 받는다.

Consumer 수가 Partition 수보다 많으면 남는 Consumer가 생길 수 있다.

Consumer 수가 Partition 수보다 적으면 한 Consumer가 여러 Partition을 처리할 수 있다.
```

---

## **5.8 Offset**

Offset은 Partition 안에서 각 메시지가 가지는 위치 번호다.

예를 들어 Partition 0에 메시지가 순서대로 쌓이면 다음과 같다.

```
Partition 0

Offset 0 → Message A
Offset 1 → Message B
Offset 2 → Message C
Offset 3 → Message D
```

Consumer는 이 Offset을 기준으로 어디까지 메시지를 읽었는지 관리한다.

예를 들어 Consumer가 Offset 2까지 처리했다면 다음에 Offset 3부터 읽으면 된다.

```
처리 완료한 Offset: 2
다음에 읽을 Offset: 3
```

Offset이 중요한 이유는 Consumer가 장애로 종료되었다가 다시 실행될 수 있기 때문이다.

Consumer가 다시 실행되었을 때 Kafka는 다음 질문에 답해야 한다.

```
이 Consumer는 어디서부터 다시 읽어야 하는가?
```

이때 사용하는 것이 Offset이다.

---

## **5.9 Offset Commit**

!image.png

Offset Commit은 Consumer가 특정 Offset까지 처리했다고 Kafka에 기록하는 것이다.

예를 들어 Consumer가 Offset 10까지 처리하고 Commit했다면 다음 의미가 된다.

```
Consumer가 Offset 10번 메시지까지 처리했다면,
다음에 읽어야 할 위치는 Offset 11이다.

따라서 Kafka에는 보통 "다음에 읽을 Offset"을 기준으로 Commit된다.
즉, Offset 10번 메시지까지 처리했다면 Commit되는 위치는 Offset 11로 이해할 수 있다.
```

Consumer의 기본 처리 흐름을 보면 다음과 같다.

```
1. 메시지를 읽는다.
2. 메시지를 처리한다.
3. 처리가 성공하면 Offset을 Commit한다.
```

중요한 점은 메시지 읽기와 Offset Commit이 같은 것이 아니라는 점이다.

```
메시지를 읽었다
≠
처리가 끝났다

Offset을 Commit했다
=
여기까지 처리했다고 기록했다
```

예를 들어 다음 상황을 생각해보자.

```
Consumer가 메시지를 읽었다.
하지만 처리하기 전에 Consumer가 죽었다.
Offset Commit도 하지 못했다.
```

이 경우 Consumer가 다시 실행되면 같은 메시지를 다시 읽을 수 있다.

이번에는 다른 상황을 생각해보자.

```
Consumer가 메시지를 읽었다.
DB 저장까지 성공했다.
하지만 Offset Commit 전에 Consumer가 죽었다.
```

이 경우에도 Consumer가 다시 실행되면 같은 메시지를 다시 읽을 수 있다.

왜냐하면 Kafka 입장에서는 해당 Offset이 처리 완료되었다는 Commit 기록을 받지 못했기 때문이다.

따라서 Kafka Consumer 구조에서는 같은 메시지가 다시 처리될 수 있다고 봐야 한다.

```
Kafka를 사용한다고 해서 메시지가 반드시 한 번만 처리되는 것은 아니다.
실무에서는 같은 메시지가 한 번 이상 처리될 수 있다고 가정해야 한다.
```

이것을 보통 at-least-once 처리 가능성과 연결해서 이해할 수 있다.

이번 주차에서는 여기까지만 이해하면 된다.

중복 처리 문제를 어떻게 안전하게 막을지는 5주차에서 `requestId` 기반 멱등성과 함께 다룬다.

---

# **6. Kafka 메시지 처리 보장과 중복 처리 가능성**

Kafka를 사용하면 메시지가 안전하게 전달되는 구조를 만들 수 있다.

하지만 이것이 곧 “모든 메시지가 정확히 한 번만 처리된다”는 뜻은 아니다.

Consumer가 메시지를 읽고 처리하는 도중 장애가 발생할 수 있다.

특히 다음 상황을 조심해야 한다.

```
상황 1. 메시지를 읽었지만 처리 전에 장애 발생

상황 2. DB 저장은 성공했지만 Offset Commit 전에 장애 발생

상황 3. Consumer 재시작 후 마지막 Commit 지점부터 다시 읽음
```

이런 상황에서는 같은 메시지가 다시 처리될 수 있다.

쿠폰 발급 시스템에서는 이것이 큰 문제가 될 수 있다.

예를 들어 같은 메시지가 두 번 처리되면 같은 사용자에게 같은 쿠폰이 두 번 발급될 수도 있다.

```
Consumer가 메시지 처리
→ DB 저장 성공
→ Offset Commit 전 장애
→ Consumer 재시작
→ 같은 메시지 다시 처리
→ 중복 발급 위험
```

이번 주차에서는 이 문제의 존재만 이해하면 된다.

해결책은 뒤 주차에서 다룬다.

```
4주차
→ DB Unique Key를 통한 최종 중복 방지

5주차
→ requestId 기반 멱등성 처리

8주차
→ 실패 메시지 재처리, DLQ, 보상 트랜잭션
```

!image.png

이번 주차의 핵심은 다음 문장이다.

```
Kafka를 사용하는 구조에서는 같은 메시지가 다시 처리될 수 있다고 가정해야 한다.
```

---

# **7. Topic을 어떻게 설계할 것인가?**

!image.png

이번 2주차에서는 최소한 하나의 Topic을 설계한다.

```
coupon.issue.requested
```

이 Topic은 쿠폰 발급 요청이 들어왔다는 메시지를 담는다.

즉, API Server는 사용자 요청을 받으면 이 Topic에 메시지를 발행한다.

```
Client
  ↓
API Server
  ↓
coupon.issue.requested Topic
```

Consumer는 이 Topic을 읽고 실제 쿠폰 발급 처리를 수행한다.

```
coupon.issue.requested Topic
  ↓
Coupon Issue Consumer
  ↓
DB
```

이 Topic에 들어갈 메시지 예시는 다음과 같다.

```json
{
  "requestId": "req-123",
  "eventId": 1,
  "userId": 100,
  "requestedAt": "2026-06-26T12:00:00"
}
```

Topic을 설계할 때는 다음 기준을 생각해야 한다.

```
1. 이 Topic에는 어떤 의미의 메시지가 들어가는가?

2. 누가 이 Topic에 메시지를 발행하는가?

3. 누가 이 Topic을 소비하는가?

4. 이 Topic의 메시지는 어떤 작업을 유발하는가?

5. Topic 이름만 보고도 메시지 의미를 어느 정도 알 수 있는가?
```

이번 시스템에서는 다음과 같이 정리할 수 있다.

| **항목** | **내용** |
| --- | --- |
| Topic 이름 | coupon.issue.requested |
| 메시지 의미 | 쿠폰 발급 요청이 접수됨 |
| Producer | API Server |
| Consumer | Coupon Issue Consumer |
| 처리 결과 | 실제 쿠폰 발급 처리 및 DB 저장 |

주의할 점은 `coupon.issue.requested`는 발급 완료 이벤트가 아니라는 것이다.

이 Topic에 메시지가 들어갔다고 해서 쿠폰 발급이 완료된 것은 아니다.

정확히는 다음 의미다.

```
쿠폰 발급 요청이 들어왔고,
이 요청을 나중에 Consumer가 처리해야 한다.
```

따라서 사용자에게도 “발급 완료”가 아니라 “요청 접수” 또는 “처리 중”이라는 응답을 주는 것이 자연스럽다.

---

# **8. Producer 발행 성공과 실패**

!image.png

API Server가 Kafka Producer 역할을 한다면 중요한 문제가 하나 생긴다.

```
Kafka 발행에 실패하면 어떻게 해야 하는가?
```

예를 들어 사용자가 쿠폰 발급 요청을 보냈다.

API Server는 Kafka에 메시지를 발행하려고 했다.

그런데 Kafka 장애나 네트워크 문제로 메시지 발행에 실패했다.

```
Client
  ↓
API Server
  ↓
Kafka 발행 실패
```

이때 사용자에게 다음과 같이 응답하면 문제가 된다.

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다."
}
```

왜냐하면 실제로는 Kafka에 메시지가 들어가지 않았기 때문이다.

Kafka에 메시지가 없으면 Consumer는 이 요청을 처리할 수 없다.

즉, 사용자 입장에서는 요청이 접수된 것처럼 보이지만 시스템 내부에서는 처리할 요청이 사라진 상태가 된다.

따라서 Kafka 발행 실패 시에는 요청 접수 성공으로 응답하면 안 된다.

더 적절한 응답은 다음과 같다.

```json
{
  "status": "FAILED",
  "message": "쿠폰 발급 요청 접수에 실패했습니다. 잠시 후 다시 시도해주세요."
}
```

정리하면 다음과 같다.

```
Kafka 발행 성공
→ 요청 접수 성공 응답 가능

Kafka 발행 실패
→ 요청 접수 실패 응답 필요
```

여기서도 중요한 구분은 다음과 같다.

```
요청 접수 성공
≠
쿠폰 발급 성공
```

Kafka 발행 성공은 요청이 처리 대기열에 들어갔다는 뜻이다.

아직 실제 쿠폰 발급이 완료된 것은 아니다.

# **9. Partition Key를 왜 정해야 하는가?**

Kafka에서 Partition Key는 메시지를 어떤 Partition에 보낼지 결정하는 기준이다.

Partition Key를 어떻게 정하느냐에 따라 메시지 분산과 순서 보장이 달라진다.

이번 쿠폰 발급 시스템에서 고려할 수 있는 Partition Key 후보는 다음과 같다.

```
1. userId
2. eventId
3. requestId
```

각각의 특징을 살펴보자.

---

## **9.1 userId를 Partition Key로 사용하는 경우**

!image.png

`userId`를 Partition Key로 사용하면 같은 사용자의 요청은 같은 Partition으로 들어간다.

```
userId = 100 → Partition 0
userId = 100 → Partition 0
userId = 100 → Partition 0
```

장점은 같은 사용자의 요청 순서를 관리하기 쉽다는 것이다.

예를 들어 한 사용자가 쿠폰 발급 버튼을 여러 번 눌렀다면 같은 사용자의 요청들이 같은 Partition에 들어가게 된다.

Kafka는 같은 Partition 안에서는 순서를 보장하므로, 같은 사용자 기준의 순서를 다루기 유리하다.

```
장점
- 같은 사용자의 요청이 같은 Partition으로 들어간다.
- 같은 사용자 기준의 순서를 생각하기 쉽다.
- 중복 요청 흐름을 추적하기 좋다.
```

단점도 있다.

특정 사용자가 비정상적으로 많은 요청을 보내면 해당 사용자의 메시지가 특정 Partition에 몰릴 수 있다.

하지만 일반적인 쿠폰 발급 이벤트에서는 사용자가 매우 많고, 각 사용자의 요청 수는 상대적으로 제한적이라고 볼 수 있다.

그래서 `userId`는 꽤 현실적인 선택지가 될 수 있다.

---

## **9.2 eventId를 Partition Key로 사용하는 경우**

!image.png

`eventId`를 Partition Key로 사용하면 같은 쿠폰 이벤트에 대한 요청은 같은 Partition으로 들어갈 가능성이 높다.

```
eventId = 1 → Partition 0
eventId = 1 → Partition 0
eventId = 1 → Partition 0
```

처음 보면 좋아 보일 수 있다.

같은 이벤트의 요청을 한 곳에 모을 수 있기 때문이다.

하지만 선착순 쿠폰 이벤트에서는 대부분의 요청이 특정 이벤트 하나에 몰릴 가능성이 높다.

예를 들어 지금 진행 중인 이벤트가 `eventId = 1` 하나라면 모든 요청이 같은 Key를 가진다.

그러면 메시지가 특정 Partition에 몰릴 수 있다.

```
eventId = 1인 요청 10만 건
→ 모두 같은 Partition으로 이동할 수 있음
→ Partition이 여러 개 있어도 병렬 처리 효과가 줄어듦
```

이 구조에서는 Partition을 여러 개 만들어도 실제로는 하나의 Partition만 바쁘게 일할 수 있다.

결과적으로 Consumer Group을 구성해도 병렬 처리 효과가 떨어진다.

```
장점
- 같은 이벤트 요청을 묶어서 생각하기 쉽다.

단점
- 인기 이벤트 하나에 요청이 몰리면 특정 Partition으로 쏠릴 수 있다.
- Partition을 여러 개 둬도 병렬 처리 효과가 줄어들 수 있다.
- Consumer Lag이 특정 Partition에 집중될 수 있다.
```

따라서 선착순 쿠폰 발급 요청 Topic에서 `eventId`만 Partition Key로 사용하는 것은 조심해야 한다.

---

## **9.3 requestId를 Partition Key로 사용하는 경우**

!image.png

`requestId`를 Partition Key로 사용하면 요청마다 다른 Key가 사용될 가능성이 높다.

```
requestId = req-1 → Partition 0
requestId = req-2 → Partition 1
requestId = req-3 → Partition 2
```

장점은 메시지가 비교적 고르게 분산될 수 있다는 점이다.

요청마다 requestId가 다르면 특정 Partition에만 몰릴 가능성이 줄어든다.

```
장점
- 메시지 분산에 유리할 수 있다.
- 특정 이벤트 하나에 요청이 몰려도 Partition 분산이 가능하다.
```

하지만 단점도 있다.

같은 사용자가 여러 번 요청하더라도 requestId가 매번 다르면 서로 다른 Partition으로 갈 수 있다.

그러면 같은 사용자 기준의 요청 순서를 보장하기 어렵다.

```
단점
- 같은 사용자의 요청이 서로 다른 Partition으로 갈 수 있다.
- 같은 사용자 기준의 순서 보장이 어렵다.
- 중복 요청을 사용자 단위로 추적하기 어려울 수 있다.
```

따라서 `requestId`는 분산에는 유리할 수 있지만, 사용자 기준의 흐름을 유지해야 하는 경우에는 신중하게 선택해야 한다.

---

## **9.4 Partition Key 선택 정리**

각 Partition Key 후보를 정리하면 다음과 같다.

| **Partition Key** | **장점** | **단점** |
| --- | --- | --- |
| userId | 같은 사용자 요청이 같은 Partition으로 들어감, 사용자 기준 순서 관리에 유리 | 특정 사용자의 요청이 과도하게 많으면 일부 Partition에 부하 가능 |
| eventId | 같은 이벤트 요청을 묶어서 보기 쉬움 | 인기 이벤트 하나에 요청이 몰리면 특정 Partition에 쏠릴 가능성이 큼 |
| requestId | 요청 단위로 분산되기 쉬움 | 같은 사용자 기준 순서 보장이 어려움 |

이번 과제에서는 정답을 하나로 고정하지 않는다.

중요한 것은 자신이 선택한 Partition Key의 이유를 설명하는 것이다.

다만 선착순 쿠폰 시스템에서는 `eventId`만 Key로 사용하는 것은 위험할 수 있다.

왜냐하면 하나의 이벤트에 요청이 몰리는 상황이 바로 이 시스템의 핵심 문제이기 때문이다.

2주차 과제에서는 다음 질문에 답할 수 있어야 한다.

```
내가 선택한 Partition Key는 무엇인가?

그 Key를 선택한 이유는 무엇인가?

이 Key는 메시지 분산에 어떤 영향을 주는가?

이 Key는 순서 보장에 어떤 영향을 주는가?

특정 Partition에 메시지가 몰릴 위험은 없는가?
```

---

# **10. 메시지 순서 보장**

!image.png

Kafka의 순서 보장을 이해할 때 가장 중요한 문장은 다음과 같다.

```
Kafka는 같은 Partition 안에서는 메시지 순서를 보장한다.
하지만 서로 다른 Partition 사이의 전체 순서는 보장하지 않는다.
```

예를 들어 Partition 0에 다음 메시지가 들어갔다고 해보자.

```
Partition 0

Offset 0 → A
Offset 1 → B
Offset 2 → C
```

이 경우 Consumer는 같은 Partition 안에서 A, B, C 순서로 메시지를 읽는다.

하지만 Partition이 여러 개라면 전체 순서는 달라질 수 있다.

```
Partition 0
Offset 0 → A
Offset 1 → C

Partition 1
Offset 0 → B
Offset 1 → D
```

이 경우 전체 Topic 기준으로 A, B, C, D 순서가 반드시 보장되는 것은 아니다.

각 Partition 내부의 순서만 보장된다.

이것이 Partition Key와 연결된다.

같은 기준의 메시지 순서를 보장하고 싶다면 그 기준을 Partition Key로 잡아 같은 Partition에 들어가게 해야 한다.

예를 들어 같은 사용자의 요청 순서가 중요하다면 `userId`를 Key로 고려할 수 있다.

```
userId = 100인 요청들
→ 같은 Partition으로 이동
→ 같은 사용자 기준 순서 관리에 유리
```

반대로 요청을 최대한 고르게 분산시키고 싶다면 `requestId` 같은 값을 고려할 수 있다.

하지만 이 경우 같은 사용자 요청이 서로 다른 Partition으로 갈 수 있어 사용자 기준 순서 보장은 약해질 수 있다.

정리하면 다음과 같다.

```
순서 보장을 강하게 생각하면 분산이 어려워질 수 있다.

분산을 강하게 생각하면 특정 기준의 순서 보장이 어려워질 수 있다.

Partition Key는 이 둘 사이의 균형을 잡는 설계 포인트다.
```

---

# **11. Consumer Group과 병렬 처리 설계**

!image.png

Kafka를 사용하는 이유 중 하나는 Consumer를 여러 개 두어 병렬로 처리할 수 있기 때문이다.

하지만 Consumer를 여러 개 둔다고 항상 처리량이 무한히 증가하는 것은 아니다.

Consumer 병렬 처리에는 Partition 수가 큰 영향을 준다.

---

## **11.1 Partition 1개, Consumer 3개**

Topic의 Partition이 1개뿐인데 Consumer가 3개라고 해보자.

```
Topic: coupon.issue.requested
Partition: 1개
Consumer Group: coupon-issue-group
Consumer: 3개
```

이 경우 같은 Consumer Group 안에서는 하나의 Partition을 하나의 Consumer만 처리할 수 있다.

```
Partition 0 → Consumer 1

Consumer 2 → 대기
Consumer 3 → 대기
```

즉, Consumer를 3개 띄웠지만 실제로 메시지를 처리하는 Consumer는 1개뿐이다.

이 구조에서는 Consumer 수를 늘려도 병렬 처리 효과가 거의 없다.

---

## **11.2 Partition 3개, Consumer 3개**

이번에는 Partition이 3개이고 Consumer도 3개라고 해보자.

```
Topic: coupon.issue.requested
Partition: 3개
Consumer Group: coupon-issue-group
Consumer: 3개
```

이 경우 다음과 같이 나누어 처리할 수 있다.

```
Partition 0 → Consumer 1
Partition 1 → Consumer 2
Partition 2 → Consumer 3
```

각 Consumer가 하나의 Partition을 담당하므로 병렬 처리가 가능하다.

이 구조에서는 Consumer 1개가 모든 메시지를 처리할 때보다 처리량을 높일 수 있다.

---

## **11.3 Partition 3개, Consumer 5개**

이번에는 Partition이 3개인데 Consumer가 5개라고 해보자.

```
Topic: coupon.issue.requested
Partition: 3개
Consumer Group: coupon-issue-group
Consumer: 5개
```

이 경우 최대 3개의 Consumer만 Partition을 할당받을 수 있다.

```
Partition 0 → Consumer 1
Partition 1 → Consumer 2
Partition 2 → Consumer 3

Consumer 4 → 할당받을 Partition 없음
Consumer 5 → 할당받을 Partition 없음
```

즉, Consumer 수가 Partition 수보다 많으면 일부 Consumer는 놀게 된다.

따라서 Consumer 수만 무작정 늘리는 것은 좋은 해결책이 아니다.

---

## **11.4 Partition 5개, Consumer 2개**

이번에는 Partition이 5개이고 Consumer가 2개라고 해보자.

```
Topic: coupon.issue.requested
Partition: 5개
Consumer Group: coupon-issue-group
Consumer: 2개
```

이 경우 한 Consumer가 여러 Partition을 담당한다.

```
Partition 0 → Consumer 1
Partition 1 → Consumer 1
Partition 2 → Consumer 2
Partition 3 → Consumer 2
Partition 4 → Consumer 2
```

이 구조에서는 모든 Partition이 처리되지만, 각 Consumer가 여러 Partition을 담당하기 때문에 Consumer 하나당 처리 부담이 커질 수 있다.

정리하면 다음과 같다.

```
Partition 수가 병렬 처리의 기본 단위가 된다.

Consumer 수는 Partition 수를 고려해서 정해야 한다.

Consumer 수가 Partition 수보다 많으면 남는 Consumer가 생긴다.

Consumer 수가 Partition 수보다 적으면 한 Consumer가 여러 Partition을 처리한다.
```

---

# **12. Consumer Lag과 사용자 경험**

!image.png

Kafka를 사용하면 API Server는 빠르게 응답할 수 있다.

하지만 이것이 실제 쿠폰 발급도 항상 빠르게 끝난다는 뜻은 아니다.

Kafka 구조에서는 요청이 다음과 같이 처리된다.

```
사용자 요청
  ↓
API Server가 Kafka에 메시지 발행
  ↓
사용자에게 요청 접수 응답
  ↓
Consumer가 나중에 메시지 처리
  ↓
DB에 발급 결과 저장
```

여기서 Consumer 처리 속도가 Producer 발행 속도보다 느리면 Kafka에 메시지가 계속 쌓인다.

```
Producer가 초당 1,000개 메시지 발행
Consumer가 초당 300개 메시지 처리

초당 700개 메시지가 쌓임
```

이렇게 Consumer가 아직 처리하지 못하고 밀린 메시지 수를 Consumer Lag이라고 한다.

```
Consumer Lag
= log-end-offset(최신 적재 오프셋) − committed offset(소비자가 커밋한 오프셋)
= 현재 Partition에 쌓인 최신 메시지 위치와 Consumer Group이 처리 완료했다고 기록한 Offset 사이 의 차이

즉, Consumer가 아직 처리하지 못하고 밀려 있는 메시지 수를 의미한다.
```

Consumer Lag이 커지면 사용자 경험에 영향을 준다.

사용자는 요청 직후 다음 응답을 받을 수 있다.

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다."
}
```

하지만 실제 Consumer 처리가 늦어지고 있다면 결과 조회 API에서는 아직 처리 중 상태가 보일 수 있다.

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 처리 중입니다."
}
```

Consumer 처리가 끝난 뒤에야 다음과 같은 결과를 볼 수 있다.

```json
{
  "status": "ISSUED",
  "message": "쿠폰이 발급되었습니다."
}
```

또는 쿠폰 수량이 소진되었다면 다음과 같은 결과가 될 수 있다.

```json
{
  "status": "FAILED",
  "message": "쿠폰이 모두 소진되었습니다."
}
```

Consumer Lag이 커지면 다음 문제가 발생한다.

```
1. 사용자가 최종 결과를 늦게 확인한다.

2. 마이페이지에 쿠폰이 늦게 반영된다.

3. 알림 발송이 늦어질 수 있다.

4. 운영자는 시스템이 정상 처리 중인지 확인하기 어려워질 수 있다.

5. Lag이 계속 증가하면 Consumer 처리량이 부족하다는 신호일 수 있다.
```

따라서 Kafka를 쓴다고 해서 병목이 사라지는 것은 아니다.

정확히는 병목의 위치가 바뀐다.

```
Kafka 도입 전
→ API Server와 DB에 부하 집중

Kafka 도입 후
→ API Server 응답은 빨라짐
→ 하지만 Consumer 처리 속도가 느리면 Kafka에 메시지가 쌓임
→ Consumer Lag이 증가
```

즉, Kafka는 요청을 잠시 쌓아두고 뒤에서 처리할 수 있게 해준다.

하지만 Consumer가 계속 느리면 최종 처리는 늦어진다.

이번 주차에서 꼭 기억해야 할 문장은 다음과 같다.

```
Kafka를 넣으면 API 응답은 빨라질 수 있지만,
전체 시스템 처리량이 무한히 증가하는 것은 아니다.

Consumer 처리 속도가 부족하면 병목은 Consumer 쪽으로 이동하고,
그 결과 Consumer Lag이 증가한다.
```

---

# **13. API 응답 상태 설계**

!image.png

Kafka 기반 비동기 구조에서는 API 응답 상태를 잘 설계해야 한다.

사용자에게 바로 “쿠폰 발급 성공”을 응답하면 안 된다.

왜냐하면 API Server는 Kafka에 메시지를 넣었을 뿐이고, 실제 쿠폰 발급은 Consumer가 나중에 처리하기 때문이다.

따라서 요청 직후 응답은 다음처럼 설계할 수 있다.

```json
{
  "status": "ACCEPTED",
  "message": "쿠폰 발급 요청이 접수되었습니다."
}
```

또는 처리 대기 상태를 강조해서 다음처럼 응답할 수도 있다.

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다."
}
```

둘 중 어떤 표현을 사용하든 중요한 것은 다음이다.

```
이 응답은 쿠폰 발급 성공이 아니다.
이 응답은 요청이 접수되었다는 뜻이다.
```

Kafka 발행에 실패한 경우에는 다음과 같이 응답할 수 있다.

```json
{
  "status": "FAILED",
  "message": "쿠폰 발급 요청 접수에 실패했습니다. 잠시 후 다시 시도해주세요."
}
```

결과 조회 API에서는 다음과 같은 상태를 보여줄 수 있다.

```
PENDING
→ 요청은 접수되었지만 아직 처리 중

ISSUED
→ 쿠폰 발급 성공

FAILED
→ 쿠폰 발급 실패
```

예시는 다음과 같다.

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 처리 중입니다."
}
```

```json
{
  "status": "ISSUED",
  "message": "쿠폰이 발급되었습니다."
}
```

```json
{
  "status": "FAILED",
  "message": "쿠폰 발급에 실패했습니다."
}
```

이 구조에서 중요한 구분은 다음과 같다.

```
요청 API
→ 요청 접수 여부를 응답

결과 조회 API
→ 실제 발급 결과를 응답
```

---

# **14. Kafka를 사용해도 모든 문제가 해결되는 것은 아니다**

Kafka를 사용하면 API Server와 실제 처리 로직을 분리할 수 있다.

하지만 Kafka를 넣는다고 모든 문제가 자동으로 해결되는 것은 아니다.

Kafka를 사용할 때도 다음 문제를 설계해야 한다.

```
1. 어떤 Topic을 만들 것인가?

2. 메시지에는 어떤 필드를 넣을 것인가?

3. Partition Key는 무엇으로 할 것인가?

4. Partition 수는 어떻게 정할 것인가?

5. Consumer Group은 어떻게 구성할 것인가?

6. Consumer Lag이 커지면 어떻게 볼 것인가?

7. Kafka 발행 실패 시 사용자에게 어떤 응답을 줄 것인가?

8. Consumer가 같은 메시지를 다시 읽으면 어떻게 될 것인가?
```

이번 2주차에서 모든 문제의 해결책을 다루지는 않는다.

하지만 Kafka 기반 구조를 설계하려면 위 질문들을 인식하고 있어야 한다.

특히 다음 세 가지는 꼭 기억해야 한다.

```
Kafka는 요청을 뒤로 넘겨주는 비동기 메시지 시스템이다.

Kafka를 사용하면 API 응답은 빨라질 수 있지만, Consumer가 느리면 최종 처리는 늦어진다.

Kafka 메시지는 장애 상황에서 중복 처리될 수 있다고 가정해야 한다.
```

---

# **15. 이번 주차에서 다루지 않는 것**

!image.png

이번 2주차는 Kafka 기반 비동기 요청 처리 구조를 설계하는 주차다.

따라서 아래 내용은 깊게 다루지 않는다.

```
Redis 선착순 판정

DB Unique Key

requestId 기반 멱등성 처리

Outbox Pattern

Retry Topic / DLQ

보상 트랜잭션

알림, 마이페이지, 통계 서비스 이벤트 분리
```

각 내용은 뒤 주차에서 다룬다.

| **내용** | **다루는 주차** |
| --- | --- |
| Redis 선착순 판정 | 3주차 |
| DB Unique Key와 최종 정합성 | 4주차 |
| requestId 기반 멱등성 | 5주차 |
| Outbox Pattern | 6주차 |
| EDA 기반 서비스 분리 | 7주차 |
| Retry / DLQ / 보상 트랜잭션 | 8주차 |

이번 주차에서는 다음 범위에 집중한다.

```
API Server가 Kafka에 쿠폰 발급 요청 메시지를 발행한다.

Consumer가 Kafka 메시지를 읽고 실제 처리를 수행한다.

Topic과 Message 구조를 설계한다.

Partition Key를 선택하고 이유를 설명한다.

Consumer Group과 Partition 관계를 이해한다.

Offset과 Commit으로 인해 중복 처리 가능성이 있음을 이해한다.

Consumer Lag이 사용자 경험에 어떤 영향을 주는지 이해한다.
```

---
