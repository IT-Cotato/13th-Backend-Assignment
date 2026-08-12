# **Kafka 기반 비동기 쿠폰 발급 요청 처리 구조 설계**

---

# **과제 1. Kafka 기반 비동기 처리 구조 그리기**

## **1. 전체 요청 처리 흐름 다이어그램**

<img width="1086" height="1448" alt="image" src="https://github.com/user-attachments/assets/017d7d36-863b-4e0f-8d0e-3732dd914589" />

```
[Client]
   |
   | 1. 쿠폰 발급 요청
   v
[API Server]
   |
   | 2. 로그인 / 요청값 검증
   | 3. requestId 생성
   | 4. Kafka 메시지 생성
   v
[Kafka Producer]
   |
   | 5. 메시지 발행
   v
[Kafka Broker / Topic]
   |
   | 6. 발행 성공 ACK 반환
   v
[Kafka Producer]
   |
   | 7. API Server에 발행 성공 결과 전달
   v
[API Server]
   |
   | 8. 사용자에게 PENDING 응답
   v
[Client]

이후 비동기 처리

[coupon.issue.requested Topic]
   |
   | 9. Consumer가 메시지 Poll
   v
[Coupon Issue Consumer]
   |
   | 10. 쿠폰 발급 처리
   | 11. DB 저장
   | 12. 처리 성공 후 Offset Commit
   v
[DB]
   |
   | 12. 저장 성공
   v
[Coupon Issue Consumer]
   |
   | 13. Offset Commit
   v

[Kafka]
```

## **2. API Server의 역할**

```
API Server는 쿠폰 발급 요청을 받고,
기본 검증 후 Kafka에 발급 요청 메시지를 발행한다.

Kafka 발행이 성공하면 사용자에게 PENDING 응답을 준다.
단, 이 응답은 최종 발급 성공이 아니라 요청 접수 성공이다.
```

## **3. Kafka의 역할**

```
Kafka는 API Server와 Consumer 사이에서
쿠폰 발급 요청 메시지를 저장하고 전달한다.

순간적으로 요청이 몰려도 메시지를 Topic에 쌓아두고,
Consumer가 처리 가능한 속도로 읽어갈 수 있게 한다.
```

## **4. Consumer의 역할**

```
Consumer는 coupon.issue.requested Topic에서 메시지를 읽고
실제 쿠폰 발급 처리를 수행한 뒤 DB에 결과를 저장한다.
```

## **5. DB의 역할**

```
DB 또는 별도의 상태 저장소는 쿠폰 발급 요청 상태를 저장한다.

예를 들어 Kafka 발행 성공 직후에는 requestId 기준으로 PENDING 상태를 저장하고,
Consumer가 실제 발급 처리를 완료하면 ISSUED 또는 FAILED로 상태를 변경한다.

최종 쿠폰 발급 성공은 Kafka 발행 성공이 아니라,
Consumer가 DB에 발급 기록을 저장하고 상태를 ISSUED로 변경한 시점이다.
```

## **6. 사용자가 요청 직후 최종 발급 결과를 바로 받지 않는 이유**

```
선착순 이벤트에서는 요청이 한순간에 몰린다.

API Server가 모든 요청을 직접 DB까지 처리하면
응답 지연과 DB 부하가 커진다.

그래서 요청 접수와 실제 발급 처리를 분리하고,
사용자는 나중에 상태 조회 API나 마이페이지에서 결과를 확인한다.
```

## **7. 요청 접수 성공과 쿠폰 발급 성공의 차이**

```
요청 접수 성공
= Kafka에 발급 요청 메시지가 정상적으로 들어감

쿠폰 발급 성공
= Consumer가 DB에 발급 기록을 최종 저장함
```

---

# **과제 2. Topic 설계하기**

## **1. Topic 이름**

```
coupon.issue.requested
```

## **2. Topic에 담기는 메시지의 의미**

```
쿠폰 발급 요청이 들어왔다는 의미다.
발급 완료 이벤트가 아니라, Consumer가 나중에 처리해야 할 요청 이벤트다.
```

## **3. Producer는 어떤 컴포넌트이며 언제 메시지를 발행하는가?**

```
Producer는 API Server다.

사용자의 쿠폰 발급 요청을 받은 뒤
로그인 여부와 기본 요청값을 확인하고 Kafka에 메시지를 발행한다.
```

## **4. Consumer는 어떤 컴포넌트이며 어떤 처리를 수행하는가?**

```
Consumer는 Coupon Issue Consumer다.

coupon.issue.requested Topic을 읽고,
eventId와 userId를 바탕으로 실제 쿠폰 발급 처리를 수행한다.
```

## **5. 이 Topic이 필요한 이유**

```
API Server와 실제 발급 처리를 분리하기 위해 필요하다.

API Server는 빠르게 요청을 접수하고,
Consumer는 뒤에서 DB 저장을 처리할 수 있다.
```

## **6. 이 Topic에 메시지가 들어갔다는 것은 어떤 의미인가?**

```
쿠폰 발급 요청이 Kafka에 접수되었다는 의미다.

즉, Consumer가 나중에 처리할 수 있는 상태가 된 것이다.
```

## **7. 이 Topic에 메시지가 들어갔다는 것이 쿠폰 발급 완료를 의미하지 않는 이유**

```
아직 Consumer가 메시지를 읽고 DB에 저장하지 않았기 때문이다.

최종 발급 성공은 DB 저장 성공 이후에 결정된다.
```

---

# **과제 3. Kafka Message 구조 설계하기**

## **1. Kafka Message JSON 예시**

```json
{
  "requestId": "req-20260703-000001",
  "eventId": 1,
  "userId": 100,
  "requestedAt": "2026-06-26T12:00:00"
}
```

## **2. 각 필드의 의미**

| **필드** | **의미** | **필요한 이유** |
| --- | --- | --- |
| requestId | 요청 식별자 | 상태 조회, 로그 추적, 이후 멱등성 처리에 사용 |
| eventId | 쿠폰 이벤트 ID | 어떤 이벤트의 쿠폰을 발급할지 판단 |
| userId | 사용자 ID | 누구에게 쿠폰을 발급할지 판단 |
| requestedAt | 요청 시간 | 요청 추적과 장애 분석에 사용 |

## **3. Consumer가 이 메시지를 보고 어떤 처리를 할 수 있는가?**

```
Consumer는 eventId로 쿠폰 이벤트를 확인하고,
userId로 발급 대상을 확인한 뒤,
DB에 쿠폰 발급 결과를 저장할 수 있다.
```

## **4. 메시지에 너무 많은 정보를 넣는 것이 왜 좋지 않을 수 있는가?**

```
메시지가 커지고,
개인정보 노출 위험이 커지며,
DB 정보와 Kafka 메시지 정보가 달라질 수 있다.

따라서 필요한 식별자 중심으로 설계하는 것이 좋다.
```

## **5. requestId는 이번 주차에서 어떤 정도로만 이해하면 되는가?**

```
이번 주차에서는 requestId를 요청 식별자와 상태 조회용 값으로 이해하면 된다.

requestId 기반 멱등성 처리는 5주차에서 자세히 다룬다.
```

---

# **과제 4. Kafka 발행 성공 / 실패에 따른 API 응답 설계하기**

## **상황 A. Kafka 발행 성공**

### **1. Kafka 발행 성공 시 응답 JSON**

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다. 최종 결과는 마이페이지에서 확인해주세요.",
  "requestId": "req-20260703-000001"
}
```

### **2. 이 응답이 쿠폰 발급 성공을 의미하지 않는 이유**

```
Kafka 발행 성공은 요청이 Topic에 들어갔다는 뜻이다.

아직 Consumer가 메시지를 처리하지 않았고,
DB에 발급 기록도 저장되지 않았다.
```

### **3. 요청 접수 성공과 쿠폰 발급 성공의 차이**

```
요청 접수 성공
= Kafka에 메시지가 들어감

쿠폰 발급 성공
= DB에 발급 기록이 저장됨
```

### **4. requestId를 응답에 포함하는 이유**

```
사용자가 나중에 발급 상태를 조회할 수 있게 하기 위해서다.
또한 로그 추적과 이후 멱등성 처리에도 사용할 수 있다.
```

## **상황 B. Kafka 발행 실패**

### **1. Kafka 발행 실패 시 응답 JSON**

```json
{
  "status": "ACCEPT_FAILED",
  "message": "쿠폰 발급 요청을 접수하지 못했습니다. 잠시 후 다시 시도해주세요.",
  "requestId": null
}

ACCEPT_FAILED
= 쿠폰 발급 요청 접수에 실패했다.
= Kafka에 메시지가 들어가지 않았다.
= 사용자는 나중에 상태 조회를 기다리면 안 된다.
= 잠시 후 다시 요청해야 한다.
```

### **2. Kafka 발행에 실패했는데 요청 접수 성공 응답을 주면 안 되는 이유**

```
Kafka에 메시지가 없으면 Consumer가 처리할 수 없다.

그런데 PENDING 응답을 주면 사용자는 요청이 접수되었다고 오해하게 된다.
```

### **3. 이 상황에서 API Server가 주의해야 할 점**

```
발행 실패 시에는 요청 접수 실패 응답을 반환해야 한다.

Kafka에 메시지가 없으므로 사용자가 발급 결과를 기다릴 수 있는
정상 접수 상태로 처리하면 안 된다.

필요하다면 운영 로그나 실패 이력은 남길 수 있지만,
PENDING 상태로 저장하거나 성공 응답을 주면 안 된다.
```

---

# **과제 5. Partition Key 선택하기**

## **1. Partition Key 후보별 장단점 비교**

| **Partition Key 후보** | **장점** | **단점** |
| --- | --- | --- |
| userId | 같은 사용자 요청이 같은 Partition으로 들어간다. 사용자 기준 순서 추적에 유리하다. | 특정 사용자의 요청이 많으면 일부 Partition에 몰릴 수 있다. |
| eventId | 같은 이벤트 요청을 묶어 보기 쉽다. | 인기 이벤트 하나에 요청이 몰리면 특정 Partition에 쏠릴 수 있다. |
| requestId | 요청 단위 분산에 유리하다. | 같은 사용자 요청이 서로 다른 Partition으로 갈 수 있다. |

## **2. 내가 선택한 Partition Key**

```
userId
```

## **3. 선택한 이유**

```
userId를 Key로 쓰면 장점:
- 사용자별 요청 순서 추적이 쉽다.
- 같은 사용자의 중복 요청이 같은 Partition에 들어간다.
- eventId 하나에 요청이 몰려도 어느 정도 Partition 분산이 된다.

단점:
- 전체 이벤트 기준 선착순 순서는 보장하지 못한다.
- 같은 eventId의 요청이 여러 Partition에 흩어진다.
- 따라서 “이벤트 전체 기준으로 1,000명만 정확히 자르기”는 Kafka Partition Key만으로 해결할 수 없다.
- 수량 제한은 Redis나 DB 조건부 처리 같은 별도 장치가 필요하다.
```

## **4. 메시지 분산에 어떤 영향을 주는가?**

```
userId가 다양하면 메시지가 여러 Partition에 분산된다.

eventId를 Key로 쓰는 것보다
인기 이벤트 하나에 모든 메시지가 몰릴 위험이 낮다.
```

## **5. 순서 보장에 어떤 영향을 주는가?**

```
Kafka는 같은 Partition 안에서만 순서를 보장한다.

userId를 Key로 쓰면 같은 사용자의 요청은 같은 Partition에 들어가므로
사용자 기준 순서를 보기 좋다.

단, 전체 이벤트 기준 순서는 보장하지 않는다.
```

## **6. 특정 Partition에 메시지가 몰릴 가능성은 없는가?**

```
가능성은 있다.

특정 사용자가 비정상적으로 많은 요청을 보내면
그 userId에 해당하는 Partition에 메시지가 몰릴 수 있다.

하지만 일반적으로는 여러 사용자가 요청하므로 eventId보다 분산에 유리하다.
```

## **7. 이 Key를 선택했을 때 발생할 수 있는 단점**

```
전체 이벤트 기준 순서는 보장되지 않는다.

또한 userId만으로 중복 발급이나 수량 초과를 막을 수는 없다.
이 문제는 Redis, DB Unique Key, requestId 멱등성으로 보완해야 한다.
```

## **8. eventId를 Partition Key로 사용할 때 주의해야 할 점**

```
이벤트 하나에 대부분의 요청이 몰리면
eventId가 모두 같아져 특정 Partition에 메시지가 집중될 수 있다.

Partition 수: 6개
Partition Key: eventId
요청 대부분: eventId = 1

eventId = 1 → hash(eventId) → Partition 2

결과:

Partition 0: 거의 없음
Partition 1: 거의 없음
Partition 2: 요청 대부분 집중
Partition 3: 거의 없음
Partition 4: 거의 없음
Partition 5: 거의 없음

이러면 Partition을 6개 만들어도 실제로는 Partition 1개처럼 동작할 수 있다.
그래서 인기 이벤트 단일 트래픽에서는 eventId Key가 병목이 될 수 있다.
```

## **9. Kafka는 어떤 단위에서 메시지 순서를 보장하는가?**

```
Kafka는 Partition 내부에서만 순서를 보장한다.

서로 다른 Partition에 들어간 메시지 A, B의 전체 처리 순서는 보장되지 않는다.
각 Partition이 독립적으로 처리되기 때문이다.
```

---

# **과제 6. Consumer Group과 Partition 수 설계하기**

## **1. Topic 이름**

```
coupon.issue.requested
```

## **2. Partition 수**

```
6개
```

## **3. Consumer Group 이름**

```
coupon-issue-group
```

## **4. Consumer 수**

```
3개
```

## **5. Partition과 Consumer 매핑**

```
Partition 0 → Consumer 1
Partition 1 → Consumer 1

Partition 2 → Consumer 2
Partition 3 → Consumer 2

Partition 4 → Consumer 3
Partition 5 → Consumer 3
```

## **6. 이 구조를 선택한 이유**

```
10만 건의 요청이 몰릴 수 있으므로
Consumer 1개만으로 처리하면 Lag이 커질 수 있다.

Partition을 6개로 두면
현재는 Consumer 3개가 나누어 처리하고,
나중에 최대 6개 Consumer까지 확장할 수 있다.

단, Partition 수를 너무 많이 늘리면 운영 복잡도와 리밸런싱 비용이 증가할 수 있으므로, 예상 트래픽, Consumer 처리 속도, DB 처리량을 고려해서 정해야 한다.

Consumer를 늘려도 DB 저장 속도가 느리면 전체 처리량은 늘지 않는다.
따라서 Consumer 확장은 Kafka만 보고 결정하는 것이 아니라,
DB Connection Pool, DB write 처리량, Lock 경합까지 함께 고려해야 한다.
```

## **7. Partition이 3개이고 Consumer가 5개라면 실제로 동시에 처리할 수 있는 Consumer는 최대 몇 개인가?**

```
최대 3개다.

같은 Consumer Group 안에서
하나의 Partition은 하나의 Consumer에게만 할당되기 때문이다.
```

## **8. Consumer 수가 Partition 수보다 많으면 어떤 일이 발생하는가?**

```
일부 Consumer는 Partition을 할당받지 못하고 대기한다.

따라서 Consumer 수를 늘려도 처리량이 더 증가하지 않을 수 있다.
```

## **9. Consumer 수가 Partition 수보다 적으면 어떤 일이 발생하는가?**

```
한 Consumer가 여러 Partition을 담당한다.

모든 Partition은 처리되지만,
Consumer 하나당 처리 부담이 커질 수 있다.
```

## **10. Consumer 수만 무작정 늘리면 안 되는 이유는 무엇인가?**

```
Kafka의 병렬 처리 한계는 Partition 수에 영향을 받는다.

Partition 수보다 Consumer 수가 많으면 남는 Consumer가 생긴다.
또한 DB가 병목이면 Consumer만 늘려도 전체 처리량은 크게 늘지 않는다.
```

---

# **과제 7. Offset Commit 실패 상황 분석하기**

## **1. Consumer가 다시 실행되면 어떤 메시지를 다시 읽을 수 있는가?**

```
마지막으로 Commit된 Offset 이후의 메시지를 다시 읽을 수 있다.

DB 저장은 성공했더라도 Offset Commit이 안 됐다면
같은 메시지를 다시 읽을 수 있다.
```

## **2. Kafka 입장에서는 왜 이 메시지가 처리 완료되었다고 판단하지 못하는가?**

```
Kafka는 DB 저장 여부를 알지 못한다.

Kafka가 처리 완료로 판단하는 기준은 Offset Commit이다.
Commit이 없으면 처리 완료로 보지 않는다.

일반적으로 안전한 처리를 위해서는 DB 저장이 성공한 뒤 Offset Commit을 한다.
하지만 이 구조에서는 아래 문제가 생길 수 있다.

DB 저장 성공
→ Offset Commit 전 장애
→ 재시작 후 같은 메시지 재처리
→ 중복 발급 가능성

따라서 Consumer는 같은 메시지가 여러 번 처리될 수 있다는 전제로 설계해야 한다.
```

## **3. 이 상황에서 같은 메시지가 다시 처리될 수 있는가?**

```
그렇다.

DB 저장 후 Offset Commit 전에 Consumer가 죽으면
재시작 후 같은 메시지를 다시 처리할 수 있다.
```

## **4. 같은 메시지가 다시 처리되면 쿠폰 발급 시스템에서는 어떤 문제가 생길 수 있는가?**

```
같은 사용자에게 같은 쿠폰이 중복 발급될 수 있다.

특히 DB Unique Key나 멱등성 처리가 없으면
중복 발급 기록이 저장될 위험이 있다.
```

## **5. 이 문제를 해결하려면 나중에 어떤 장치가 필요할 수 있는가?**

```
1. DB Unique Key
   - UNIQUE(event_id, user_id)로 중복 발급 방지

2. requestId 기반 멱등성
   - 같은 요청이 다시 처리되어도 한 번만 반영

3. 처리 상태 저장
   - PENDING, ISSUED, FAILED 상태 관리

4. Consumer 중복 메시지 방어
   - 같은 메시지가 다시 들어올 수 있다고 가정하고 처리
```

---

# **과제 8. Consumer Lag이 생겼을 때 사용자 경험 설계하기**

## **1. Consumer Lag이란 무엇인가?**

```
Consumer가 아직 처리하지 못하고 밀려 있는 메시지 수다.

즉, Kafka에 쌓인 최신 Offset과
Consumer Group이 Commit한 Offset 사이의 차이다.
```

## **2. Consumer Lag이 증가하면 사용자 경험에 어떤 영향이 있는가?**

```
사용자는 PENDING 상태를 오래 보게 된다.

마이페이지 반영이나 알림이 늦어질 수 있고,
사용자가 불안해서 버튼을 다시 누를 수도 있다.
```

## **3. 요청 직후 사용자에게 줄 응답 JSON**

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다. 최종 결과는 마이페이지에서 확인해주세요.",
  "requestId": "req-20260703-000001"
}
```

## **4. 결과 조회 API에서 PENDING 상태일 때 응답 JSON**

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 처리 중입니다. 잠시 후 다시 확인해주세요.",
  "requestId": "req-20260703-000001"
}
```

## **5. 발급 성공 시 응답 JSON**

```json
{
  "status": "ISSUED",
  "message": "쿠폰이 발급되었습니다.",
  "requestId": "req-20260703-000001"
}
```

## **6. 발급 실패 시 응답 JSON**

```json
{
  "status": "FAILED",
  "message": "쿠폰 발급에 실패했습니다.",
  "requestId": "req-20260703-000001"
}

FAILED
= 요청은 Kafka에 정상 접수됨
= Consumer가 메시지를 읽음
= 발급 조건 불만족, 수량 소진, DB 저장 실패 등으로 최종 발급 실패 
= requestId로 조회 가능한 최종 상태
```

## **7. 운영자가 Consumer Lag을 보고 어떤 판단을 할 수 있는가?**

```
Consumer Lag이 커지면 운영자는 다음을 확인할 수 있다.

1. Consumer 처리 속도가 Producer 발행 속도를 따라가지 못하는지
2. 특정 Partition에만 Lag이 몰렸는지
3. DB 저장이 병목인지
4. Consumer 인스턴스를 늘릴 수 있는지
5. Partition 수가 충분한지
6. 사용자에게 PENDING 안내 문구를 더 명확히 보여줘야 하는지
```

## **8. Kafka를 사용해도 최종 발급 결과가 늦어질 수 있는 이유**

```
Kafka는 API 응답을 빠르게 만들 수 있지만,
실제 발급 처리는 Consumer가 수행한다.

Consumer 처리 속도나 DB 저장 속도가 느리면
최종 결과는 늦게 반영될 수 있다.
```

---

# **과제 9. 나쁜 Kafka 설계의 문제점 찾기**

## **문제점과 개선 방향**

| **문제점** | **왜 문제인가?** | **개선 방향** |
| --- | --- | --- |
| Partition이 1개뿐이다. | 병렬 처리 한계가 생긴다. | Partition 수를 늘린다. |
| Consumer가 1개뿐이다. | 처리 속도가 부족하면 Lag이 커진다. | Consumer Group에 여러 Consumer를 둔다. |
| Partition Key가 eventId다. | 인기 이벤트 하나에 요청이 몰리면 특정 Partition에 쏠릴 수 있다. | userId 또는 requestId 등 분산 가능한 Key를 검토한다. |
| Kafka 발행 실패에도 성공 응답을 준다. | Kafka에 메시지가 없으면 Consumer가 처리할 수 없다. | 발행 성공 확인 후 PENDING 응답을 준다. 실패 시 FAILED 응답을 준다. |
| Offset Commit 실패를 고려하지 않았다. | 같은 메시지가 다시 처리되어 중복 발급될 수 있다. | DB Unique Key, requestId 멱등성으로 방어한다. |
| Consumer Lag 상태를 사용자에게 보여주지 않는다. | 사용자는 처리 중인지 실패했는지 알 수 없다. | 상태 조회 API 또는 마이페이지에서 PENDING, ISSUED, FAILED를 제공한다. |
| Kafka만 믿고 중복 발급을 막으려 한다. | Kafka는 메시지 전달 도구이지 중복 발급 방지 도구가 아니다. | Redis, DB Unique Key, 멱등성 처리를 추가한다. |
