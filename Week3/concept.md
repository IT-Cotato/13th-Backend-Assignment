# **3주차 개념 설명: Redis 기반 선착순 쿠폰 판정 구조**

## **주제**

선착순 쿠폰 발급 시스템에서 Redis를 사용해 쿠폰 수량 초과 발급과 사용자 중복 발급을 빠르게 막는 구조를 설계한다.

---

## **1. 3주차 목표**

1주차에서는 사용자의 쿠폰 발급 요청을 API Server가 끝까지 직접 처리하지 않고, 요청 접수와 실제 발급 처리를 분리하는 구조를 배웠다.

2주차에서는 Kafka를 사용해 쿠폰 발급 요청을 비동기적으로 전달하고, Consumer가 뒤에서 실제 처리를 수행하는 구조를 배웠다.

3주차에서는 Redis를 도입한다.

3주차의 목표는 다음과 같다.

```
1. Redis가 왜 선착순 쿠폰 시스템에 필요한지 이해한다.
2. Redis로 쿠폰 수량 초과 발급을 빠르게 차단하는 방법을 이해한다.
3. Redis로 같은 사용자의 중복 요청을 빠르게 차단하는 방법을 이해한다.
4. Redis 명령을 잘못 조합했을 때 생기는 Race Condition을 이해한다.
5. Redis Lua Script를 사용해 여러 검사를 원자적으로 처리하는 이유를 이해한다.
6. INCR + Set 기반 방식의 장점과 한계를 이해한다.
7. Redis SUCCESS가 최종 쿠폰 발급 성공을 의미하지 않는다는 점을 이해한다.
8. Redis, Kafka, DB가 각각 어떤 역할을 나누어 가져야 하는지 이해한다.
9. 이후 4~8주차에서 Redis 이후의 정합성, 멱등성, Outbox, EDA, Retry/DLQ를 어떻게 이어서 배우는지 이해한다.
```

---

## **2. 지금까지의 구조 복습**

### **2-1. 1주차 구조**

1주차에서는 API Server가 최종 발급까지 기다리지 않고, 요청을 접수한 뒤 뒤에서 처리하는 구조를 배웠다.

```
Client
  |
  | 쿠폰 발급 요청
  v
API Server
  |
  | 요청 검증
  | 요청 접수
  v
Client
  |
  | "쿠폰 발급 요청이 접수되었습니다."
Worker
  |
  | 실제 쿠폰 발급 처리
  v
DB
```

핵심은 다음이다.

```
API Server가 최종 발급 결과를 기다리지 않는다.
사용자는 먼저 요청 접수 응답을 받는다.
실제 발급 결과는 나중에 조회한다.
```

---

### **2-2. 2주차 구조**

2주차에서는 Kafka를 도입해 요청을 비동기로 전달하는 구조를 배웠다.

```
Client
  |
  | 쿠폰 발급 요청
  v
API Server
  |
  | Kafka에 요청 메시지 발행
  v
Kafka
  |
  | coupon.issue.requested
  v
Consumer
  |
  | 메시지 소비
  | 실제 쿠폰 발급 처리
  v
DB
```

Kafka를 사용하면 요청이 한 번에 몰려도 Consumer가 처리 가능한 속도로 메시지를 읽어갈 수 있다.

```
요청 폭주
  |
  v
Kafka에 적재
  |
  v
Consumer가 처리 가능한 속도로 소비
```

하지만 Kafka만으로는 부족하다.

Kafka는 메시지를 전달하고 쌓아두는 역할을 한다.

그러나 아래 문제를 자동으로 해결해주지는 않는다.

```
1. 쿠폰 수량보다 많이 발급되는 문제
2. 같은 사용자가 여러 번 발급받는 문제
3. 품절 이후 요청까지 계속 DB로 흘러가는 문제
4. Consumer가 같은 메시지를 다시 읽는 문제
```

그래서 3주차에서는 Redis를 사용해 DB 앞단에서 선착순 여부와 중복 여부를 먼저 판단한다.

---

## **3. 왜 Redis가 필요한가?**

선착순 쿠폰 시스템에서 가장 위험한 상황은 이벤트 시작 순간에 요청이 폭발적으로 몰리는 것이다.

예를 들어 다음과 같은 이벤트가 있다고 하자.

```
쿠폰 수량: 1,000개
요청 수: 100,000건
조건: 사용자 1명당 1개만 발급 가능
```

Redis 같은 빠른 선착순 판정 계층이 없다면 성공 가능성이 낮은 요청도 Kafka와 Consumer를 거쳐 DB 판정 단계까지 도달할 가능성이 커진다.

```
요청 100,000건
  |
  v
Kafka
  |
  v
Consumer
  |
  v
DB에서 수량 확인 / 중복 확인 / 저장 시도
```

하지만 실제로 발급 가능한 쿠폰은 1,000개뿐이다.

즉, 99,000개의 요청은 결국 실패할 요청이다.

이 실패할 요청까지 전부 DB에서 판정하는 것은 비효율적이다.

Redis를 사용하면 DB에 도달하기 전에 다음을 빠르게 판단할 수 있다.

```
1. 이 사용자가 이미 요청했는가?
2. 쿠폰 수량이 아직 남아 있는가?
3. 선착순 안에 들어왔는가?
```

Redis를 앞단에 두면 구조가 이렇게 바뀐다.

```
요청 100,000건
  |
  v
Redis에서 선착순 / 중복 판정
  |
  | 성공 1,000건
  v
Kafka
  |
  v
Consumer
  |
  v
DB 저장
실패 99,000건
  |
  v
바로 품절 또는 중복 응답
```

핵심은 다음이다.

```
Redis는 DB 앞단에서 요청을 빠르게 걸러주는 필터 역할을 한다.
```

---

## **4. Redis 도입 전과 도입 후 구조**

<img width="1491" height="1055" alt="image" src="https://github.com/user-attachments/assets/1217ced1-0652-4561-97af-9687cf49994a" />

### **4-1. Redis 없이 DB에서 직접 판정하는 구조**

```
[Redis 없음]
Client
  |
  | 쿠폰 발급 요청
  v
API Server
  |
  | Kafka 메시지 발행
  v
Kafka
  |
  v
Consumer
  |
  | DB에서 남은 수량 조회
  | DB에서 중복 발급 확인
  | DB에 발급 기록 저장
  v
DB
```

문제는 다음과 같다.

```
1. 성공 가능성이 낮은 요청도 DB 판정 단계까지 도달할 수 있다.
2. DB에서 수량 조회와 저장이 반복된다.
3. 동시에 많은 요청이 오면 DB Lock 경합이 커진다.
4. 쿠폰이 품절된 이후에도 실패할 요청이 계속 DB까지 갈 수 있다.
5. 같은 사용자의 반복 클릭도 Kafka와 DB까지 전달될 수 있다.
```

---

### **4-2. Redis를 앞단에 두는 구조**

```
[Redis 사용]
Client
  |
  | 쿠폰 발급 요청
  v
API Server
  |
  | 1. 로그인 확인
  | 2. 이벤트 시간 확인
  | 3. Redis 선착순 / 중복 판정
  v
Redis
  |
  | SUCCESS / DUPLICATE / SOLD_OUT
  v
API Server
  |
  | SUCCESS인 경우만 Kafka 발행
  v
Kafka
  |
  v
Consumer
  |
  | DB에 최종 발급 기록 저장
  v
DB
```

장점은 다음과 같다.

```
1. DB까지 가는 요청 수를 줄일 수 있다.
2. 품절 이후 요청을 빠르게 차단할 수 있다.
3. 같은 사용자의 중복 요청을 빠르게 차단할 수 있다.
4. DB는 최종 발급 기록 저장에 집중할 수 있다.
5. 선착순 판정 속도가 빨라진다.
```

---

## **5. Redis의 역할**

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/50310185-85d3-4d39-9769-d701584ad554" />

3주차에서 Redis의 역할은 크게 세 가지다.

```
1. 쿠폰 수량 제한
2. 사용자 중복 요청 방지
3. 성공한 요청만 Kafka로 넘기기
```

### **5-1. 쿠폰 수량 제한**

쿠폰 수량이 1,000개라면 Redis는 1,000명까지만 통과시켜야 한다.

```
쿠폰 수량: 1,000개
1번째 사용자    → 통과
2번째 사용자    → 통과
...
1000번째 사용자 → 통과
1001번째 사용자 → 품절
1002번째 사용자 → 품절
```

---

### **5-2. 사용자 중복 요청 방지**

한 사용자는 같은 이벤트에서 쿠폰을 한 번만 받을 수 있어야 한다.

```
user:1 첫 번째 요청
  |
  v
Redis 통과
user:1 두 번째 요청
  |
  v
Redis에서 중복 요청으로 차단
```

---

### **5-3. 성공한 요청만 Kafka로 발행**

Redis를 통과하지 못한 요청은 Kafka로 보내지 않는다.

```
Client
  |
  v
API Server
  |
  v
Redis 판정
  |
  +-- SUCCESS   → Kafka 발행
  |
  +-- DUPLICATE → Kafka 발행 안 함
  |
  +-- SOLD_OUT  → Kafka 발행 안 함
```

이렇게 해야 Kafka와 Consumer, DB가 불필요한 실패 요청을 처리하지 않아도 된다.

---

## **6. Redis 자료구조 선택**

Redis에서는 여러 자료구조를 사용할 수 있다.

선착순 쿠폰 판정에서 자주 고민하는 자료구조는 다음과 같다.

| **자료구조** | **사용 목적** | **장점** | **단점** |
| --- | --- | --- | --- |
| String | 카운터 관리 | INCR/DECR로 빠르게 수량 관리 가능 | 사용자 중복 여부는 따로 관리해야 함 |
| Set | 통과 사용자 목록 관리 | 중복 사용자 방지에 좋음, SCARD로 개수 확인 가능 | Set이 커지면 메모리, TTL, 정리 전략을 고민해야 함 |
| Sorted Set | 요청 시간 또는 점수 기반 순서 관리 | 요청 순서, 순위 관리에 좋음 | 단순 수량 제한에는 Set보다 복잡함 |
| List | 요청 순서대로 저장 | 큐처럼 사용 가능 | 중복 검사와 수량 제한에는 적합하지 않음 |

3주차 기본 구조에서는 Redis Set + Lua Script 방식을 최종 권장 방식으로 설명한다.

다만 INCR + Set 방식도 함께 살펴보며, 요청 순번 기반 방식이 어떤 장점과 한계를 가지는지 비교한다.

---

## **7. Redis Set 기반 설계**

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/3d831cfa-2145-4be3-b343-be5a76b71cd8" />

### **7-1. Key 설계**

쿠폰 이벤트마다 Redis Key를 분리한다.

```
coupon:{eventId}:issued-users
```

예를 들어 이벤트 ID가 100이면 다음과 같은 Key를 사용할 수 있다.

```
coupon:100:issued-users
```

이 Set 안에는 Redis 선착순 판정에 성공한 사용자 ID를 저장한다.

```
Redis
이벤트 ID가 100인 Key 집합
key: coupon:100:issued-users
Set members:
+-----------------------+
| user:1                |
| user:5                |
| user:20               |
| user:77               |
| ...                   |
+-----------------------+
SCARD coupon:100:issued-users
= 현재 선착순 통과 사용자 수
```

---

### **7-2. Set으로 중복 요청을 막는 방식**

Redis Set은 같은 값을 중복해서 저장하지 않는다.

```
SADD coupon:100:issued-users user:1
```

처음 추가하면 성공한다.

```
결과: 1
의미: 새로 추가됨
```

같은 사용자를 다시 추가하면 실패한다.

```
SADD coupon:100:issued-users user:1
결과: 0
의미: 이미 존재함
```

즉, Set을 사용하면 같은 사용자가 여러 번 요청해도 한 번만 저장할 수 있다.

```
user:1 요청 1회차
  |
  v
SADD 성공
  |
  v
선착순 통과
user:1 요청 2회차
  |
  v
이미 Set에 존재
  |
  v
중복 요청 차단
```

---

### **7-3. Set으로 수량 제한을 확인하는 방식**

Set에 들어있는 사용자 수를 확인하면 현재까지 선착순 통과한 사용자 수를 알 수 있다.

```
SCARD coupon:100:issued-users
```

SCARD 명령어는 **지정된 Set에 저장된 고유 요소의 총 개수를 반환한다**

쿠폰 수량이 1,000개라면 다음처럼 판단할 수 있다.

```
현재 Set 크기: 999
요청 사용자: user:1000
999 < 1000
→ 아직 발급 가능
→ user:1000 추가
현재 Set 크기: 1000
요청 사용자: user:1001
1000 >= 1000
→ 이미 품절
→ 요청 차단
```

다만 중요한 점이 있다.

```
SCARD 자체보다 더 중요한 운영 고민은 다음이다.
1. Set에 저장되는 사용자 수가 많을 때 Redis 메모리 사용량
2. 이벤트 종료 후 Key 정리
3. TTL 설정
4. Redis 장애나 데이터 유실 시 복구 방식
5. DB와 Redis 데이터가 달라졌을 때 기준 데이터 결정
```

---

## **8. 잘못된 Redis 사용 예시**

<img width="1491" height="1055" alt="image" src="https://github.com/user-attachments/assets/017b1f2b-7145-4a7c-bcb4-7f3bcbd741c5" />

Redis를 사용한다고 해서 자동으로 안전해지는 것은 아니다.

명령을 여러 번 나누어 실행하면 중간에 다른 요청이 끼어들 수 있다.

### **8-1. 위험한 방식: SCARD 후 SADD**

다음과 같은 로직을 생각해보자.

```
1. SCARD로 현재 발급 수량 확인
2. 수량이 남아 있으면 SADD로 사용자 추가
```

코드 흐름은 이렇게 보인다.

```
current = SCARD coupon:100:issued-users
if current < limit:
    SADD coupon:100:issued-users userId
    return SUCCESS
else:
    return SOLD_OUT
```

겉으로는 괜찮아 보인다.

하지만 동시에 여러 요청이 들어오면 문제가 생길 수 있다.

---

### **8-2. Race Condition 예시**

쿠폰이 1개 남아 있다고 가정한다.

```
쿠폰 제한 수량: 1000
현재 발급 수량: 999
남은 수량: 1
```

동시에 요청 A와 요청 B가 들어온다.

```
시간 흐름
----------------------------------------------------
요청 A: SCARD 조회 → 999 확인
요청 B: SCARD 조회 → 999 확인
요청 A: 999 < 1000 이므로 발급 가능 판단
요청 B: 999 < 1000 이므로 발급 가능 판단
요청 A: SADD user:A 성공
요청 B: SADD user:B 성공
결과:
발급 수량 1001개
쿠폰 수량 초과 발급 발생
----------------------------------------------------
```

그림으로 보면 다음과 같다.

```
[현재 상태]
coupon:100:issued-users size = 999
             요청 A                         요청 B
               |                              |
               | SCARD = 999                  |
               |                              | SCARD = 999
               |                              |
               | 발급 가능 판단               | 발급 가능 판단
               |                              |
               | SADD user:A                 |
               |                              | SADD user:B
               v                              v
[결과]
coupon:100:issued-users size = 1001
문제:
쿠폰은 1000개인데 1001명이 통과했다.
```

핵심 문제는 다음이다.

```
수량 확인과 사용자 추가가 분리되어 있다.
```

수량 확인과 추가 사이에 다른 요청이 끼어들 수 있기 때문에 초과 발급이 발생한다.

---

## **9. 원자성이 필요한 이유**

선착순 쿠폰 판정에서는 아래 작업들이 하나의 작업처럼 실행되어야 한다.

```
1. 사용자가 이미 발급받았는지 확인한다.
2. 현재 발급 수량이 제한 수량보다 작은지 확인한다.
3. 조건을 만족하면 사용자를 발급 성공 목록에 추가한다.
```

이 세 작업이 분리되면 안 된다.

좋은 구조는 다음과 같다.

```
Redis에서 하나의 원자적 작업으로 처리
+------------------------------------------------+
| 1. 중복 사용자 확인                            |
| 2. 수량 초과 여부 확인                         |
| 3. 사용자 추가                                 |
+------------------------------------------------+
        |
        v
SUCCESS / DUPLICATE / SOLD_OUT 중 하나 반환
```

---

## **10. INCR + Set 기반 방식과 한계**

<img width="1181" height="1331" alt="image" src="https://github.com/user-attachments/assets/f93ff765-7ef0-46f5-98f3-f20b0f4351c3" />

### **10-1. INCR + Set 기반 방식**

Redis에서 선착순 쿠폰 수량을 빠르게 제한하기 위해 `INCR + Set` 방식을 생각할 수 있다.

이 방식은 요청이 들어올 때마다 Redis Counter를 증가시키고, 그 결과가 쿠폰 수량 안에 들어온 요청만 다음 단계로 통과시키는 방식이다.

그리고 별도로 Redis Set을 사용해 이미 통과한 사용자인지 확인한다.

예를 들어 이벤트 ID가 100이라면 다음과 같은 Redis Key를 사용할 수 있다.

```
coupon:100:request-count
coupon:100:issued-users
```

각 Key의 역할은 다음과 같다.

| **Redis Key** | **자료구조** | **역할** |
| --- | --- | --- |
| `coupon:100:request-count` | String Counter | 몇 번째 요청인지 계산 |
| `coupon:100:issued-users` | Set | 이미 통과한 사용자 목록 관리 |

기본 흐름은 다음과 같다.

```
Client
  |
  | 쿠폰 발급 요청
  v
API Server
  |
  | 1. INCR coupon:100:request-count
  v
Redis
  |
  | requestOrder 반환
  v
API Server
  |
  | 2. requestOrder <= limit 인지 확인
  | 3. issued-users Set에서 중복 사용자 확인
  | 4. 통과 가능하면 Kafka 발행
  v
Kafka
```

이 방식의 장점은 단순하고 빠르다는 것이다.

`INCR`는 Redis에서 원자적으로 동작한다. 따라서 동시에 많은 요청이 들어와도 각 요청은 서로 다른 순번을 받는다.

```
요청 A → INCR 결과 1
요청 B → INCR 결과 2
요청 C → INCR 결과 3
...
요청 1000 → INCR 결과 1000
요청 1001 → INCR 결과 1001
```

그래서 요청 순번이 쿠폰 수량을 초과하면 빠르게 품절 응답을 줄 수 있다.

```
requestOrder <= limit
→ 선착순 후보

requestOrder > limit
→ SOLD_OUT
```

즉, `INCR + Set` 방식의 핵심 아이디어는 다음과 같다.

```
INCR
→ 요청 순번을 빠르게 계산한다.

Set
→ 이미 통과한 사용자인지 확인한다.
```

---

### **10-2. INCR + Set 기반 방식의 문제점**

하지만 `INCR + Set` 방식은 주의해서 사용해야 한다.

가장 큰 문제는 `request-count`가 사용자 중복 여부를 알지 못한다는 점이다.

즉, 같은 사용자가 여러 번 요청해도 요청할 때마다 `request-count`가 증가할 수 있다.

예를 들어 쿠폰 수량이 3개라고 하자.

```
limit = 3
```

그런데 `user:1`이 버튼을 여러 번 누르면 다음과 같은 상황이 생길 수 있다.

```
user:1 첫 번째 요청 → INCR 결과 1
user:1 두 번째 요청 → INCR 결과 2
user:1 세 번째 요청 → INCR 결과 3
user:2 첫 번째 요청 → INCR 결과 4 → SOLD_OUT
```

그림으로 보면 다음과 같다.

```
request-count 기준 선착순 자리

+---------+---------+---------+---------+
| 순번 1  | 순번 2  | 순번 3  | 순번 4  |
+---------+---------+---------+---------+
| user:1  | user:1  | user:1  | user:2  |
+---------+---------+---------+---------+
   통과      중복      중복      품절
```

문제는 실제로는 `user:1` 한 명만 통과해야 하는데, `user:1`의 중복 요청이 선착순 자리를 여러 개 소비할 수 있다는 점이다.

즉, `INCR`는 숫자 증가를 원자적으로 처리해주지만, 사용자 중복 제거까지 자동으로 보장하지는 않는다.

```
INCR가 보장하는 것
→ 요청마다 서로 다른 숫자를 부여한다.

INCR가 보장하지 않는 것
→ 같은 사용자의 중복 요청이 여러 자리를 차지하지 못하게 막는 것
```

따라서 `request-count`를 최종 발급 수량처럼 해석하면 안 된다.

```
request-count
= 요청 순번 또는 전체 요청 수에 가까움

request-count
≠ 중복 제거된 실제 통과 사용자 수

실제 통과 사용자 수
= issued-users Set의 크기
```

또 다른 문제는 Redis 명령을 애플리케이션에서 여러 번 나누어 실행할 때 발생한다.

예를 들어 아래 작업들이 각각 따로 실행된다고 하자.

```
1. INCR request-count
2. requestOrder가 limit 이하인지 확인
3. SISMEMBER로 중복 여부 확인
4. SADD로 사용자 추가
```

이 작업들이 하나의 원자적 흐름으로 묶여 있지 않으면, 중간에 다른 요청이 끼어들 수 있다.

선착순 쿠폰 판정에서는 아래 작업들이 하나의 작업처럼 처리되어야 한다.

```
1. 중복 사용자 확인
2. 현재 통과 사용자 수 확인
3. 사용자 추가
```

결론적으로 `INCR + Set` 방식은 요청 순번을 빠르게 계산할 수 있다는 장점은 있지만, 정확한 중복 제거 기반 선착순 판정의 최종 선택지로 사용하기에는 한계가 있다.

---

## 11. 최종 선택: Redis Lua Script 기반 Set 판정 방식

<img width="1491" height="1055" alt="image" src="https://github.com/user-attachments/assets/9f063ccf-b44e-465a-b5a0-eba4811dbb50" />

Redis Lua Script를 사용하면 여러 Redis 명령을 하나의 스크립트로 묶어서 실행할 수 있다.

즉, 아래 작업을 하나의 원자적 흐름으로 처리할 수 있다.

```
SISMEMBER
SCARD
SADD
```

### **11-1. Lua Script 흐름**

```
API Server
  |
  | EVAL Lua Script
  v
Redis
  |
  | 1. 이미 Set에 userId가 있는지 확인
  | 2. Set 크기가 limit 이상인지 확인
  | 3. 통과 가능하면 userId 추가
  v
API Server
  |
  | SUCCESS / DUPLICATE / SOLD_OUT 반환
```

---

### **11-2. Lua Script 의사코드**

<img width="1491" height="1055" alt="image" src="https://github.com/user-attachments/assets/77ca1527-6eef-499f-8558-f46b14a43b47" />

```lua
-- KEYS[1] = issued user set key
-- ARGV[1] = userId
-- ARGV[2] = limit
local issuedUsersKey = KEYS[1]
local userId = ARGV[1]
local limit = tonumber(ARGV[2])
-- 1. 이미 통과한 사용자인지 확인한다.
if redis.call('SISMEMBER', issuedUsersKey, userId) == 1 then
    return 'DUPLICATE'
end
-- 2. 현재 통과한 사용자 수를 확인한다.
local currentCount = redis.call('SCARD', issuedUsersKey)
-- 3. 수량이 이미 다 찼다면 품절을 반환한다.
if currentCount >= limit then
    return 'SOLD_OUT'
end
-- 4. 아직 수량이 남아 있다면 선착순 통과 사용자로 등록한다.
redis.call('SADD', issuedUsersKey, userId)
-- 5. Redis 선착순 판정 통과를 반환한다.
return 'SUCCESS'
```

---

### **11-3. Lua Script 결과 의미**

| **반환값** | **의미** | **API Server 처리** |
| --- | --- | --- |
| SUCCESS | Redis 선착순 판정 통과 | Kafka 메시지 발행 시도 |
| DUPLICATE | 이미 통과한 사용자 | 중복 요청 응답 |
| SOLD_OUT | 쿠폰 수량 소진 | 품절 응답 |

중요한 점은 SUCCESS가 최종 쿠폰 발급 성공을 의미하지 않는다는 것이다.

```
Redis SUCCESS
= 선착순 판정을 통과했다.
= Kafka와 DB 저장 단계로 넘어갈 수 있다.
Redis SUCCESS
≠ DB에 쿠폰 발급 기록이 저장되었다.
≠ 사용자 쿠폰함에 쿠폰이 최종 반영되었다.
```

---

## **12. 전체 요청 처리 흐름**

3주차의 기본 흐름은 다음과 같다.

```
Client
  |
  | POST /coupon-events/{eventId}/issue
  v
API Server
  |
  | 1. 로그인 여부 확인
  | 2. 이벤트 시간 확인
  | 3. Redis Lua Script 실행
  v
Redis
  |
  | SUCCESS / DUPLICATE / SOLD_OUT
  v
API Server
  |
  | SUCCESS인 경우 Kafka 발행 시도
  | DUPLICATE인 경우 중복 응답
  | SOLD_OUT인 경우 품절 응답
  v
Kafka
  |
  | coupon.issue.requested
  v
Consumer
  |
  | DB에 최종 발급 기록 저장
  v
DB
```

---

## **13. 요청 결과별 상세 흐름**

### **13-1. 선착순 통과**

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/d8e9f999-7018-4b99-ae43-f578509efb46" />

```
Client
  |
  | 쿠폰 발급 요청
  v
API Server
  |
  | Redis Lua Script 실행
  v
Redis
  |
  | SUCCESS 반환
  v
API Server
  |
  | Kafka 메시지 발행
  | Kafka 발행 성공 확인
  v
Client
  |
  | PENDING 응답
  | "쿠폰 발급 요청이 접수되었습니다."
```

응답 예시는 다음과 같다.

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다.",
  "requestId": "req-001"
}
```

여기서 PENDING은 최종 발급 성공이 아니다.

```
PENDING
= Redis 선착순 판정 통과
= Kafka에 발급 요청 메시지 전달 성공
= Consumer가 DB 저장을 처리할 예정
PENDING
≠ 쿠폰 발급 완료
≠ DB 저장 완료
```

---

### **13-2. 이미 요청한 사용자**

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/710d0ff5-783f-46ba-8db3-ebf0c7fd8f4f" />

```
Client
  |
  | 같은 사용자가 다시 쿠폰 발급 요청
  v
API Server
  |
  | Redis Lua Script 실행
  v
Redis
  |
  | DUPLICATE 반환
  v
API Server
  |
  | 중복 요청 응답
  v
Client
```

응답 예시는 다음과 같다.

```json
{
  "status": "DUPLICATE",
  "message": "이미 쿠폰 발급 요청을 완료한 사용자입니다."
}
```

---

### **13-3. 쿠폰 품절**

<img width="1448" height="1086" alt="image" src="https://github.com/user-attachments/assets/d3544220-1d5e-44fb-91e7-0f9565e456d7" />

```
Client
  |
  | 쿠폰 발급 요청
  v
API Server
  |
  | Redis Lua Script 실행
  v
Redis
  |
  | SOLD_OUT 반환
  v
API Server
  |
  | 품절 응답
  v
Client
```

응답 예시는 다음과 같다.

```json
{
  "status": "SOLD_OUT",
  "message": "쿠폰이 모두 소진되었습니다."
}
```

---

### **13-4. Redis SUCCESS 이후 Kafka 발행 실패**

!image.png

Redis에서 SUCCESS가 나왔지만 Kafka 발행이 실패할 수도 있다.

```
Client
  |
  v
API Server
  |
  | Redis SUCCESS
  v
Redis
  |
  v
API Server
  |
  | Kafka 발행 시도
  v
Kafka 장애
```

이 경우 문제가 생긴다.

```
Redis에는 사용자가 통과한 것으로 기록됨
하지만 Kafka에는 메시지가 없음
Consumer는 DB 저장을 할 수 없음
사용자는 쿠폰 발급 결과를 받을 수 없음
```

이때는 보상 처리를 고민해야 한다.

```
Redis SUCCESS
  |
  v
Kafka 발행 실패
  |
  v
Redis에서 userId 제거 시도
  |
  v
Client에게 "요청 접수 실패, 다시 시도해주세요" 응답
```

다만 보상 처리 자체도 실패할 수 있다.

```
1. Redis SUCCESS
2. Kafka 발행 실패
3. Redis에서 userId 제거 시도
4. Redis 네트워크 오류로 제거 실패
결과:
Redis와 Kafka 상태가 또 불일치할 수 있다.
```

따라서 실제 운영 환경에서는 다음을 함께 고려해야 한다.

```
1. Kafka 발행 재시도
2. Redis 보상 처리
3. 실패 로그 저장
4. 복구 배치
5. 상태 테이블
6. 운영자 재처리 도구
```

이 내용은 5주차 requestId, 8주차 Retry/DLQ/보상 트랜잭션과 연결된다.

---

## **14. Redis가 앞단에서 걸러주는 요청**

Redis를 사용하면 요청 흐름이 다음처럼 바뀐다.

```
전체 요청 100,000건
        |
        v
+-----------------------+
| Redis 선착순 판정      |
+-----------------------+
        |
        | 성공 1,000건
        v
      Kafka
        |
        v
      DB 저장
        |
        | 실패 99,000건
        v
  SOLD_OUT / DUPLICATE 응답
```

핵심은 다음이다.

```
DB는 모든 요청을 받지 않는다.
DB는 Redis와 Kafka를 통과한 요청만 최종 저장한다.
```

## **15. Redis와 Kafka의 역할 차이**

Redis와 Kafka는 모두 대규모 트래픽 구조에서 사용되지만 역할이 다르다.

| **구분** | **Redis** | **Kafka** |
| --- | --- | --- |
| 주 역할 | 빠른 판정 | 비동기 메시지 전달 |
| 사용 목적 | 선착순 통과 여부, 중복 여부 판단 | 요청을 Consumer에게 안정적으로 전달 |
| 저장 데이터 | 임시 상태, 통과 사용자 목록, 카운터 | 쿠폰 발급 요청 메시지 |
| 강점 | 빠른 조회, 원자 연산, Set/Lua/INCR | 요청 완충, Consumer 병렬 처리 |
| 한계 | 최종 발급 기록 저장소로 쓰기에는 부적합 | 수량 초과와 중복 발급을 자동으로 막지 못함 |

그림으로 보면 다음과 같다.

```
Client
  |
  v
API Server
  |
  | "이 요청을 통과시켜도 되는가?"
  v
Redis
  |
  | SUCCESS
  v
API Server
  |
  | "이 요청을 비동기로 처리해줘"
  v
Kafka
  |
  v
Consumer
  |
  | "최종 발급 기록을 저장하자"
  v
DB
```

---

## **16. Redis와 DB의 역할 차이**

Redis에서 성공했다고 끝이 아니다.

Redis는 빠른 선착순 판정을 담당한다.

DB는 최종 발급 기록을 보장한다.

```
Redis
  |
  | 빠른 선착순 판정
  | 중복 요청 1차 차단
  v
Kafka
  |
  | 비동기 전달
  v
DB
  |
  | 최종 발급 기록 저장
  | Unique Key로 중복 발급 최종 방어
```

---

## **17. Redis 성공이 최종 성공이 아닌 이유**

!image.png

다음 상황을 생각해보자.

```
1. Redis에서 user:1 선착순 통과
2. API Server가 Kafka에 메시지 발행
3. Consumer가 메시지 소비
4. DB 저장 시도
5. DB 장애로 저장 실패
```

그림으로 보면 다음과 같다.

```
Redis
  |
  | user:1 SUCCESS
  v
Kafka
  |
  | 메시지 전달
  v
Consumer
  |
  | DB 저장 시도
  v
DB 장애 발생
결과:
Redis에는 성공으로 남아 있음
DB에는 발급 기록 없음
```

이런 문제가 있기 때문에 Redis는 최종 저장소가 될 수 없다.

3주차에서는 Redis로 선착순 판정을 하지만, 4주차 이후에는 DB Unique Key와 트랜잭션을 통해 최종 정합성을 보장해야 한다.

---

## **18. Redis SUCCESS, Kafka PENDING, DB ISSUED 구분**

!image.png

3주차에서 가장 중요한 상태 구분은 다음이다.

```
Redis SUCCESS
= 선착순 판정 통과
Kafka PENDING
= 발급 요청 메시지가 Kafka에 접수됨
DB ISSUED
= DB에 쿠폰 발급 기록이 최종 저장됨
```

흐름으로 보면 다음과 같다.

```
Client
  |
  v
API Server
  |
  | Redis Lua Script
  v
Redis
  |
  | SUCCESS
  v
API Server
  |
  | Kafka 발행 성공
  v
Kafka
  |
  v
Client
  |
  | PENDING 응답
  v
Consumer
  |
  | DB 저장 성공
  v
DB
  |
  v
ISSUED
```

즉, 각 단계의 의미는 다르다.

```
Redis SUCCESS
≠ 최종 발급 성공
Kafka PENDING
≠ 최종 발급 성공
DB ISSUED
= 최종 발급 성공
```

---

## **19. Redis 명령 방식 비교**

!image.png

### **19-1. 방식 A: DB Lock 사용**

```
Client
  |
  v
API Server
  |
  v
DB Lock
  |
  | 수량 조회
  | 수량 차감
  | 발급 기록 저장
  v
DB
```

장점은 DB만으로 정합성을 보장하기 쉽다는 것이다.

단점은 요청이 몰리면 DB Lock 경합이 커지고 DB가 병목이 될 수 있다는 것이다.

---

### **19-2. 방식 B: Redis INCR만 사용**

```
current = INCR coupon:100:issued-count
if current <= limit:
    SUCCESS
else:
    SOLD_OUT
```

장점은 수량 제한이 단순하다는 것이다.

단점은 사용자 중복 요청을 별도로 막아야 한다는 것이다.

```
user:1 요청 1회차 → INCR
user:1 요청 2회차 → 또 INCR
문제:
같은 사용자가 여러 번 카운트를 차지할 수 있다.
```

---

### **19-3. 방식 C: INCR + Set 방식**

```
1. request-count로 몇 번째 요청인지 확인한다.
2. issued-users Set으로 이미 통과한 사용자인지 확인한다.
3. 통과한 요청만 Kafka에 발행한다.
```

이 방식은 요청 순번을 명확하게 설명하기 좋다.

하지만 INCR만 단독으로 사용하면 중복 사용자가 여러 자리를 차지할 수 있다.

따라서 INCR + Set 방식은 참고 방식으로만 이해하고,
3주차의 최종 권장 방식은 중복 제거된 사용자 수를 기준으로 판단하는 Redis Set + Lua Script 방식으로 가져간다.

---

### **19-4. 방식 D: Redis Set + Lua Script 사용**

```
1. 사용자가 이미 Set에 있는지 확인
2. Set 크기가 limit보다 작은지 확인
3. 조건을 만족하면 Set에 사용자 추가
```

장점은 수량 제한과 중복 방지를 한 번에 처리할 수 있다는 것이다.단점은 최종 DB 저장 실패나 Kafka 발행 실패에 대한 별도 처리가 필요하다는 것이다.

---

## **20. Redis Key 설계 예시**

!image.png

이벤트별로 Redis Key를 분리한다.

```
coupon:{eventId}:request-count
coupon:{eventId}:issued-users
coupon:{eventId}:metadata
```

예시는 다음과 같다.

```
coupon:100:request-count
coupon:100:issued-users
coupon:100:metadata
```

발급 성공 사용자 Set은 다음과 같다.

```
coupon:100:issued-users
+------------------+
| user:1           |
| user:2           |
| user:3           |
| ...              |
+------------------+
```

요청 카운터는 다음과 같다.

```
coupon:100:request-count
+------------------+
| 1, 2, 3, ...     |
+------------------+
```

이벤트 메타 정보는 애플리케이션이나 DB에서 관리할 수도 있고, Redis에 캐시할 수도 있다.

```
coupon:100:metadata
+------------------+
| limit: 1000      |
| startAt: 12:00   |
| endAt: 13:00     |
+------------------+
```

다만 이벤트 수량 제한 같은 중요한 값은 DB에 원본을 두고 Redis에는 빠른 판정을 위한 상태를 두는 것이 안전하다.

Redis Key에는 TTL과 정리 전략도 필요하다.

```
예:
이벤트 종료 후 1일 뒤 Redis Key 삭제
coupon:100:issued-users
coupon:100:request-count
coupon:100:metadata
```

Redis는 빠른 판정을 위한 임시 상태 저장소이지, 영구적인 최종 원본 저장소가 아니다.

---

## **21. 요청 처리 의사코드**

API Server의 흐름은 다음과 같이 볼 수 있다.

```java
public IssueResponse issueCoupon(Long eventId, Long userId, String requestId) {
    // 1. 로그인 여부를 확인한다.
    validateLogin(userId);
    // 2. 이벤트 시작 시간과 종료 시간을 확인한다.
    validateEventTime(eventId);
    // 3. Redis Lua Script로 선착순 / 중복 여부를 판정한다.
    RedisResult result = redisCouponIssueService.tryIssue(eventId, userId);
    // 4. 이미 Redis에서 통과한 사용자라면 중복 요청으로 응답한다.
    if (result == RedisResult.DUPLICATE) {
        return IssueResponse.duplicate();
    }
    // 5. 쿠폰 수량이 이미 소진되었다면 품절 응답을 반환한다.
    if (result == RedisResult.SOLD_OUT) {
        return IssueResponse.soldOut();
    }
    try {
        // 6. Redis SUCCESS인 경우에만 Kafka로 발급 요청 메시지를 발행한다.
        //    이 단계가 성공해야 사용자에게 PENDING 응답을 줄 수 있다.
        kafkaProducer.sendCouponIssueRequested(eventId, userId, requestId);
        // 7. 최종 발급 성공이 아니라 "요청 접수 완료" 응답이다.
        return IssueResponse.pending(requestId);
    } catch (Exception e) {
        // 8. Kafka 발행 실패 시 Redis 성공 상태에 대한 보상 처리가 필요하다.
        //    예: issued-users Set에서 userId 제거, 실패 로그 저장, 재시도 큐 적재 등
        redisCouponIssueService.compensate(eventId, userId);
        // 9. Kafka에 요청이 접수되지 않았으므로 PENDING이 아니라 접수 실패 응답을 반환한다.
        return IssueResponse.failedToAccept();
    }
}
```

이 코드에서 중요한 점은 다음이다.

```
RedisResult.SUCCESS가 나왔다고 해서
return IssueResponse.success()를 하면 안 된다.
Redis 성공은 선착순 통과일 뿐이고,
최종 발급은 Consumer가 DB 저장을 완료해야 한다.
```

---

## **22. 상태 변화 관점으로 보기**

!image.png

사용자 입장에서 쿠폰 요청 상태는 다음처럼 변할 수 있다.

```
요청 전
  |
  | 쿠폰 발급 버튼 클릭
  v
REDIS_PASSED
  |
  | Kafka 발행 성공
  v
PENDING
  |
  | Consumer 처리 시작
  v
PROCESSING
  |
  | DB 저장 성공
  v
ISSUED
```

실패 흐름은 다음과 같다.

```
요청 전
  |
  | Redis에서 중복 판단
  v
DUPLICATE
요청 전
  |
  | Redis에서 품절 판단
  v
SOLD_OUT
REDIS_PASSED
  |
  | Kafka 발행 실패
  v
ACCEPT_FAILED
  |
  | Redis 보상 처리 또는 재시도 필요
PENDING
  |
  | DB 저장 실패
  v
FAILED 또는 RETRY
```

그림으로 보면 다음과 같다.

```
                      +------------+
                      | 요청 전     |
                      +------------+
                             |
                             v
                      +------------+
                      | Redis 판정  |
                      +------------+
                       /     |      \
                      /      |       \
                     v       v        v
              DUPLICATE  SOLD_OUT  SUCCESS
                                      |
                                      v
                                  Kafka 발행
                                  /        \
                                 /          \
                                v            v
                         발행 실패          발행 성공
                            |                 |
                            v                 v
                      ACCEPT_FAILED        PENDING
                                              |
                                              v
                                           Consumer
                                              |
                                      +-------+-------+
                                      |               |
                                      v               v
                                   ISSUED          FAILED
```

---

## **23. 중요한 오해 정리**

!image.png

### **오해 1. Redis를 쓰면 무조건 정확한 선착순이 보장된다**

정확히 말하면 Redis는 Redis에 도착한 요청 순서대로 원자적 처리를 해준다.

하지만 사용자가 실제로 버튼을 누른 시간과 Redis에 요청이 도착한 시간은 다를 수 있다.

```
user:A가 12:00:00.001에 버튼 클릭
user:B가 12:00:00.002에 버튼 클릭
하지만 네트워크 상황 때문에
user:B 요청이 Redis에 먼저 도착할 수 있음
```

따라서 시스템에서 말하는 선착순은 보통 다음 의미에 가깝다.

```
서버가 정상적으로 받은 요청 중
Redis 판정에 먼저 도달한 순서
```

---

### **오해 2. Redis SUCCESS는 쿠폰 발급 완료다**

아니다.

Redis SUCCESS는 선착순 통과다.

최종 쿠폰 발급 완료는 DB 저장이 끝났을 때다.

```
Redis SUCCESS
  |
  v
Kafka 발행
  |
  v
Consumer 처리
  |
  v
DB 저장 성공
  |
  v
최종 발급 완료
```

---

### **오해 3. Kafka만 있으면 중복 발급이 막힌다**

아니다.

Kafka는 메시지를 전달하고 저장하는 역할을 한다.

같은 사용자의 중복 요청이나 쿠폰 수량 초과 발급을 자동으로 막아주지는 않는다.

```
Kafka의 역할:
요청을 안정적으로 전달한다.
Redis의 역할:
이 요청을 통과시켜도 되는지 빠르게 판단한다.
DB의 역할:
최종 발급 기록의 정합성을 보장한다.
```

---

### **오해 4. Redis만 있으면 DB Unique Key는 필요 없다**

아니다.

Redis에서 중복을 막더라도 DB Unique Key는 필요하다.

이유는 다음과 같다.

```
1. Redis 장애가 날 수 있다.
2. Redis 데이터가 유실될 수 있다.
3. Kafka 메시지가 중복 소비될 수 있다.
4. Consumer가 같은 메시지를 다시 처리할 수 있다.
5. 애플리케이션 버그로 Redis를 우회하는 요청이 생길 수 있다.
6. 운영 중 수동 재처리나 배치 작업이 Redis를 거치지 않을 수도 있다.
```

따라서 DB는 마지막 방어선 역할을 해야 한다.

```sql
UNIQUE(event_id, user_id)
```

---

### **오해 5. request-count는 최종 발급 수량이다**

아니다.

request-count는 최종 발급 수량이 아니라 선착순 판정용 요청 순번에 가깝다.

```
request-count
= 몇 번째로 판정에 들어왔는지 나타내는 값
최종 발급 수량
= DB coupon_issue 저장 건수
```

특히 INCR + Set 방식에서는 품절 이후 요청도 카운터를 증가시킬 수 있다.

```
limit = 1000
1001번째 요청 → INCR 1001 → SOLD_OUT
1002번째 요청 → INCR 1002 → SOLD_OUT
1003번째 요청 → INCR 1003 → SOLD_OUT
```

따라서 request-count를 최종 발급 수량으로 보면 안 된다.

---

## **24. 최종 아키텍처 그림**

3주차 기준 전체 구조는 다음과 같다.

!image.png

```
+--------+
| Client |
+--------+
    |
    | 1. 쿠폰 발급 요청
    v
+------------+
| API Server |
+------------+
    |
    | 2. 로그인 / 이벤트 시간 검증
    v
+--------------------------------------+
| Redis                                |
|--------------------------------------|
| Lua Script                           |
|  - 중복 사용자 확인                      |
|  - SCARD로 수량 확인                    |
|  - 수량 초과 여부 확인                    |
|  - 성공 시 사용자 Set에 추가              |
+--------------------------------------+
    |
    | 3. SUCCESS / DUPLICATE / SOLD_OUT
    v
+------------+
| API Server |
+------------+
    |
    | 4. SUCCESS인 경우만 Kafka 발행
    |    Kafka 발행 실패 시 보상 처리 고민
    v
+-------------------------------+
| Kafka                         |
| topic: coupon.issue.requested |
+-------------------------------+
    |
    | 5. 메시지 소비
    v
+----------+
| Consumer |
+----------+
    |
    | 6. DB 최종 저장
    v
+-----------------------------+
| DB                          |
| coupon_issue                |
| UNIQUE(event_id, user_id)   |
+-----------------------------+
```

---

## **25. Redis가 줄여주는 부하**

Redis 도입 전에는 많은 요청이 DB 판정 단계까지 갈 수 있다.

```
요청 100,000건
  |
  v
DB에서 수량 확인 / 중복 확인 / 저장 시도
```

Redis 도입 후에는 성공 가능성이 있는 요청만 DB까지 간다.

```
요청 100,000건
  |
  v
Redis 판정
  |
  +-- 성공 1,000건   → Kafka → DB
  |
  +-- 실패 99,000건 → 바로 응답
```

결과적으로 DB의 역할이 바뀐다.

```
도입 전 DB:
많은 요청의 수량 확인 + 중복 확인 + 발급 저장
도입 후 DB:
Redis를 통과한 요청의 최종 발급 기록 저장
최종 중복 방지
```

---

## **26. 앞으로 남은 주차에서 배우는 내용**

3주차까지 오면 전체 구조는 다음 정도까지 발전한다.

```
Client
  |
  v
API Server
  |
  v
Redis 선착순 / 중복 판정
  |
  | SUCCESS
  v
Kafka
  |
  v
Consumer
  |
  v
DB 저장
```

하지만 아직 해결하지 못한 문제가 많다.

이 문제들을 4주차부터 8주차까지 하나씩 다룬다.

---

### **26-1. 4주차: DB 테이블 설계와 최종 정합성 보장**

!image.png

3주차에서 Redis가 선착순 판정을 해도 최종 발급 기록은 DB에 저장되어야 한다.

4주차에서는 DB가 마지막 방어선 역할을 하도록 설계한다.

핵심은 다음이다.

```sql
UNIQUE(event_id, user_id)
```

이 제약 조건을 두면 같은 사용자가 같은 이벤트 쿠폰을 두 번 받을 수 없다.

```
Redis 통과
  |
  v
Kafka 메시지 소비
  |
  v
DB 저장 시도
  |
  +-- 최초 저장이면 성공
  |
  +-- 이미 저장되어 있으면 Unique 제약으로 실패
```

4주차에서 배울 내용은 다음과 같다.

```
1. coupon_event 테이블 설계
2. coupon_issue 테이블 설계
3. UNIQUE(event_id, user_id) 제약 조건
4. DB Transaction
5. Redis가 놓친 중복을 DB가 최종 방어하는 구조
6. Redis 성공과 DB 저장 성공의 차이
```

---

### **26-2. 5주차: requestId 기반 멱등성과 중복 처리**

!image.png

사용자는 같은 요청을 여러 번 보낼 수 있다.

네트워크 문제나 브라우저 재시도 때문에 같은 요청이 반복될 수도 있다.

Kafka Consumer도 같은 메시지를 다시 읽을 수 있다.

5주차에서는 같은 요청이 여러 번 들어와도 결과가 한 번만 반영되도록 설계한다.

핵심은 requestId다.

```
requestId = 하나의 요청을 식별하는 ID
```

예를 들어 같은 requestId가 다시 들어오면 새 요청으로 처리하지 않고 기존 결과를 반환한다.

```
requestId: req-001 최초 요청
  |
  v
처리 시작
requestId: req-001 재요청
  |
  v
새로 처리하지 않음
기존 상태 조회 후 응답
```

5주차에서 배울 내용은 다음과 같다.

```
1. requestId의 역할
2. 멱등성의 의미
3. 같은 요청이 여러 번 들어오는 상황
4. 요청 상태 테이블 설계
5. PENDING / PROCESSING / ISSUED / FAILED 상태 관리
6. Kafka 중복 메시지 소비 방어
7. 사용자 중복 클릭과 시스템 재시도 구분
```

---

### **26-3. 6주차: Outbox Pattern과 이벤트 발행 정합성**

!image.png

쿠폰 발급이 DB에 저장된 뒤에는 다른 서비스에 발급 결과를 알려야 한다.

예를 들어 알림, 마이페이지, 통계 서비스가 필요하다.

단순하게 생각하면 Consumer가 DB 저장 후 Kafka에 결과 이벤트를 발행하면 된다.

```
DB 저장 성공
  |
  v
Kafka 결과 이벤트 발행
```

하지만 여기서 문제가 생긴다.

```
DB 저장은 성공했는데
Kafka 이벤트 발행은 실패할 수 있다.
```

이 경우 DB에는 쿠폰 발급 기록이 있는데, 알림 서비스나 마이페이지 서비스는 이 사실을 모를 수 있다.

6주차에서는 이 문제를 Outbox Pattern으로 해결한다.

```
Consumer
  |
  | DB Transaction 시작
  v
coupon_issue 저장
outbox_event 저장
  |
  | commit
  v
Outbox Publisher
  |
  | outbox_event 조회
  v
Kafka 결과 이벤트 발행
```

6주차에서 배울 내용은 다음과 같다.

```
1. DB 저장과 Kafka 이벤트 발행 사이의 불일치 문제
2. Outbox Pattern이 필요한 이유
3. coupon_issue와 outbox_event를 같은 트랜잭션으로 저장하는 구조
4. Outbox Publisher 역할
5. 이벤트 발행 실패 시 재시도 구조
6. Outbox는 DB 부하 감소가 아니라 이벤트 정합성 보장용이라는 점
```

---

### **26-4. 7주차: EDA 기반 서비스 분리와 조회 구조**

!image.png

쿠폰 발급이 끝난 뒤 해야 할 일이 많다.

```
1. 사용자에게 쿠폰 발급 알림 보내기
2. 마이페이지 쿠폰함 갱신하기
3. 운영자 통계 반영하기
```

이 작업들을 Coupon Service가 직접 호출하면 서비스 간 결합도가 높아진다.

```
Coupon Service
  |
  +--> Notification Service
  |
  +--> MyPage Service
  |
  +--> Analytics Service
```

이 구조에서는 Notification Service가 느려지면 Coupon Service까지 영향을 받을 수 있다.

7주차에서는 쿠폰 발급 결과를 이벤트로 발행하고, 각 서비스가 그 이벤트를 구독하는 구조를 배운다.

```
Coupon Issue Completed Event
  |
  +--> Notification Consumer
  |
  +--> MyPage Consumer
  |
  +--> Analytics Consumer
```

7주차에서 배울 내용은 다음과 같다.

```
1. EDA의 기본 개념
2. 쿠폰 발급 서비스와 후속 서비스 분리
3. Notification Service 분리
4. MyPage 조회 모델 분리
5. Analytics Service 분리
6. 알림 실패가 쿠폰 발급 실패로 이어지면 안 되는 이유
7. 조회 성능을 위한 Read Model 개념
```

---

### **26-5. 8주차: Retry / DLQ / 장애 처리와 보상 트랜잭션**

!image.png

마지막 8주차에서는 실패한 메시지를 어떻게 처리할지 배운다.

Consumer가 메시지를 처리하다가 실패할 수 있다.

```
1. DB 일시 장애
2. 네트워크 오류
3. 잘못된 메시지 형식
4. 이미 처리된 요청
5. Redis 성공 후 DB 저장 실패
6. 알림 서비스 장애
```

실패한 메시지를 무조건 다시 처리하면 무한 재시도에 빠질 수 있다.

반대로 바로 버리면 요청이 유실될 수 있다.

그래서 실패 유형을 나눠야 한다.

| **실패 유형** | **예시** | **처리 방향** |
| --- | --- | --- |
| 일시적 실패 | DB 일시 장애, 네트워크 오류 | Retry Topic |
| 영구 실패 | 잘못된 메시지 형식 | DLQ |
| 비즈니스 실패 | 이미 발급받은 사용자, 품절 | 실패 상태 저장 후 종료 |
| 외부 서비스 실패 | 알림 발송 실패 | 발급과 분리하여 별도 재시도 |
| 정합성 실패 | Redis 성공 후 DB 저장 실패 | 보상 트랜잭션 검토 |

8주차에서 배울 내용은 다음과 같다.

```
1. Retry Topic
2. DLQ
3. 재시도 횟수 제한
4. 실패 유형 분류
5. Poison Message 처리
6. Redis와 DB 불일치 복구
7. 보상 트랜잭션
8. 운영자가 실패 메시지를 확인하고 재처리하는 구조
```

---

## **27. 3주차와 남은 주차의 연결 흐름**

전체 학습 흐름은 다음과 같다.

```
1주차
요청 처리 구조 설계
  |
  | API Server가 최종 결과를 기다리지 않게 분리
  v
2주차
Kafka 기반 비동기 요청 처리
  |
  | 요청을 Kafka에 쌓고 Consumer가 처리
  v
3주차
Redis 선착순 판정
  |
  | DB 앞단에서 수량 초과와 중복 요청을 빠르게 차단
  v
4주차
DB 최종 정합성
  |
  | Redis가 놓친 문제를 DB Unique Key로 최종 방어
  v
5주차
requestId 멱등성
  |
  | 같은 요청과 중복 메시지가 여러 번 처리되어도 결과는 한 번만 반영
  v
6주차
Outbox Pattern
  |
  | DB 저장과 이벤트 발행 사이의 정합성 보장
  v
7주차
EDA 서비스 분리
  |
  | 알림, 마이페이지, 통계를 이벤트 기반으로 분리
  v
8주차
Retry / DLQ / 보상 트랜잭션
  |
  | 실패 메시지와 Redis-DB 불일치 복구
```

---

## **28. 3주차 핵심 설계 기준**

3주차 설계에서 반드시 생각해야 하는 기준은 다음이다.

```
1. 쿠폰 수량보다 많이 통과시키면 안 된다.
2. 같은 사용자가 여러 번 통과하면 안 된다.
3. 수량 확인과 사용자 추가는 원자적으로 처리해야 한다.
4. Redis SUCCESS는 최종 발급 성공이 아니다.
5. Redis SUCCESS인 요청만 Kafka로 발행해야 한다.
6. Kafka 발행 성공 후에도 사용자에게는 최종 성공이 아니라 PENDING을 응답해야 한다.
7. Kafka 발행 실패 시 Redis 성공 상태를 어떻게 처리할지 고민해야 한다.
8. DB는 최종 발급 기록의 마지막 방어선이어야 한다.
9. 품절 이후 요청은 DB까지 보내지 않는 것이 좋다.
10. INCR만 단독으로 쓰면 중복 사용자가 선착순 자리를 차지할 수 있다.
11. 따라서 중복 확인, 현재 통과 사용자 수 확인, 사용자 추가를 Lua Script 안에서 원자적으로 묶는 것이 안전하다
12. request-count는 최종 발급 수량이 아니라 선착순 판정용 요청 순번에 가깝다.
13. Redis 데이터는 임시 판정 상태이고, 최종 원본은 DB에 저장해야 한다.
```

---
