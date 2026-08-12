# **3주차 과제 해설**

# **Redis 기반 선착순 쿠폰 판정 구조 설계**

---

## **이번 주차의 핵심**

```
Redis 명령 하나하나는 원자적으로 실행된다.

하지만

SCARD
→ 수량 비교
→ SADD

처럼 여러 명령을 나누어 실행하면,
명령 사이에 다른 요청이 끼어들 수 있다.

따라서 다음 작업을 Lua Script 하나로 묶어야 한다.

1. 중복 사용자 확인
2. 현재 통과 사용자 수 확인
3. 제한 수량 비교
4. 통과 사용자 추가
5. 통과 자리의 requestId 소유권과 TTL 저장
```

---

## **상태 구분**

```
Redis SUCCESS

= Redis 기준 선착순 판정을 통과했다.
= 아직 최종 쿠폰 발급 성공은 아니다.

REQUEST PENDING

= Kafka에 비동기 처리 요청이 접수되었다.
= 아직 Consumer와 DB 처리가 남아 있다.

DB ISSUED

= DB에 발급 기록이 저장되고 COMMIT되었다.
= 최종 쿠폰 발급 성공이다.
```

```
Redis SUCCESS ≠ 최종 발급 성공

REQUEST PENDING ≠ 최종 발급 성공

DB ISSUED = 최종 발급 성공
```

---

# **과제 1. Redis 기반 선착순 처리 구조 그리기**

## **1-1. 전체 요청 처리 흐름 다이어그램**

<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/b677ef15-cb46-4867-981c-7f8b829c1600" />

---

## **1-2. Redis의 역할**

```
Redis는 Kafka와 DB 앞단에서
“이 요청을 선착순 처리 흐름으로 통과시켜도 되는가?”를 판단한다.

1. 이미 통과한 사용자인지 확인한다.
2. 현재 통과한 고유 사용자 수를 확인한다.
3. 제한 수량과 비교한다.
4. 조건을 만족하면 통과 사용자 목록에 추가한다.
5. SUCCESS인 요청만 Kafka로 넘긴다.
```

Redis는 최종 발급 원장은 아니지만, 선착순 판정을 담당하는 중요한 상태 저장소다.

---

## **1-3. Kafka의 역할**

```
Kafka는 Redis를 통과한 요청을
API Server에서 Consumer로 비동기 전달한다.

Kafka가 담당하는 것

- 메시지 저장과 전달
- Producer와 Consumer의 처리 속도 분리
- 순간적인 트래픽 완충
- 처리 완료 후 Offset을 Commit하도록 설계하면
  Consumer 장애 시 완료되지 않은 메시지를 재처리할 수 있다.
  - Kafka 재처리는 동일 메시지의 중복 처리를 발생시킬 수 있으므로
    DB 기반 Consumer 멱등 처리가 필요하다.
```

```
Kafka가 자동으로 보장하지 않는 것

- 쿠폰 수량 초과 방지
- 같은 사용자의 중복 발급 방지
- DB 저장 성공
- 최종 쿠폰 발급 성공
```

Kafka는 메시지를 전달할 뿐, 쿠폰의 수량이나 중복 여부를 판단하지 않는다.

---

## **1-4. DB의 역할**

DB는 최종 쿠폰 발급 기록을 저장하고 최종 정합성을 보장한다.

DB에는 목적이 다른 방어선 두 개가 필요하다.

```
사용자 중복 발급 방지

UNIQUE(event_id, user_id)
```

```sql
-- 쿠폰이 남아 있을 때만 발급 수량을 증가시킨다.
UPDATE coupon_event
SET issued_count = issued_count + 1
WHERE id = :eventId
  AND issued_count < issue_limit;
```

```
affected rows = 1
→ 수량 확보 성공

affected rows = 0
→ DB 기준 최종 SOLD_OUT
```

`UNIQUE(event_id, user_id)`는 같은 사용자의 중복 발급을 막는다.

하지만 서로 다른 사용자 1,001명의 발급은 막지 못하므로 전체 수량은 조건부 UPDATE로 별도 방어해야 한다.

---

## **1-5. Redis를 통과한 요청만 Kafka에 발행하는 이유**

`DUPLICATE`와 `SOLD_OUT` 요청까지 Kafka에 발행하면 성공할 수 없는 요청이 Consumer와 DB까지 도달한다.

```
- Kafka 메시지 증가
- Consumer Lag 증가
- 불필요한 역직렬화
- DB Connection 사용 증가
- UNIQUE 위반 반복
- 동일 재고 행 Lock 경합 증가
```

따라서 다음처럼 처리한다.

```
SUCCESS
→ Kafka 발행

DUPLICATE
→ Kafka 발행하지 않음

SOLD_OUT
→ Kafka 발행하지 않음
```

---

# **과제 2. Redis가 필요한 이유 설명하기**

기준 상황은 다음과 같다.

```
쿠폰 수량: 1,000개
요청 수: 100,000건
조건: 사용자 한 명당 쿠폰 1개
```

## **2-1. Redis 없이 Kafka와 DB만 사용하는 구조의 문제점**

```
요청 100,000건
→ Kafka 100,000건
→ Consumer 100,000건 처리
→ DB에서 수량과 중복 판정
```

실제로 성공할 수 있는 요청은 최대 1,000건이다.

나머지 요청도 DB까지 도달한 뒤에야 실패가 결정되므로 시스템 자원이 낭비된다.

---

## **2-2. 모든 요청이 Kafka, Consumer, DB까지 도달하면**

```
- Kafka 메시지가 과도하게 쌓인다.
- Consumer Lag이 증가한다.
- Consumer가 실패할 요청까지 처리한다.
- DB Connection Pool 사용량이 증가한다.
- 동일한 재고 행에 Lock 경합이 발생한다.
- 정상적으로 성공할 요청까지 느려진다.
```

---

## **2-3. 품절 이후에도 요청이 계속 처리되면**

쿠폰이 모두 소진된 이후 요청은 전부 `SOLD_OUT`이다.

그런데 Kafka와 DB까지 전달하면 다음 작업이 반복된다.

```
Kafka 메시지 저장
→ Consumer 처리
→ DB Connection 획득
→ 트랜잭션 시작
→ 조건부 UPDATE
→ affected rows = 0
→ 트랜잭션 종료
```

Redis에서 바로 `SOLD_OUT`을 반환하면 이 과정을 생략할 수 있다.

---

## **2-4. 같은 사용자가 버튼을 여러 번 누르면**

중복 방지가 없으면 같은 사용자의 요청이 여러 메시지로 발행된다.

```
user:1 첫 번째 요청
→ Kafka 발행

user:1 두 번째 요청
→ Kafka 발행

user:1 세 번째 요청
→ Kafka 발행
```

그 결과 다음 문제가 생긴다.

```
- Kafka 메시지 증가
- Consumer 중복 처리
- DB 중복 INSERT 시도
- UNIQUE 제약 위반 반복
```

Redis Set으로 이미 통과한 사용자를 관리하면 중복 요청을 앞단에서 차단할 수 있다.

---

## **2-5. Redis가 앞단에서 차단하는 요청**

```
이미 통과한 사용자의 요청
→ DUPLICATE

제한 수량을 모두 채운 이후 요청
→ SOLD_OUT
```

이 요청들은 Kafka에 발행하지 않는다.

---

# **과제 3. Redis Key와 자료구조 설계하기**

## **3-1. Redis Key 목록**

| Redis Key | 자료구조 | 저장 값 | 역할 |
| --- | --- | --- | --- |
| `coupon:{eventId}:issued-users` | Set | Redis 판정을 통과한 userId | 중복 확인과 고유 통과자 수 확인 |
| `coupon:{eventId}:admission-requests` | Hash | `userId -> requestId` | 통과 자리의 소유 요청 확인과 안전한 보상 |
| `coupon:{eventId}:metadata` | Hash | `limit`, `startAt`, `endAt`, `cleanupAt` | 모든 API Server가 공유하는 판정·정리 기준 |
| `coupon:{eventId}:request-count` | String | 전체 요청 횟수 | INCR 방식 비교용이며 최종 설계에서는 생략 가능 |

---

## **3-2. 이벤트 ID가 100일 때 실제 Redis Key**

단일 Redis를 사용한다면 다음과 같다.

```
coupon:100:issued-users
coupon:100:admission-requests
coupon:100:metadata
```

Redis Cluster를 사용한다면 같은 Hash Slot에 배치하기 위해 다음처럼 사용할 수 있다.

```
coupon:{100}:issued-users
coupon:{100}:admission-requests
coupon:{100}:metadata
```

Lua Script에서 여러 Key를 함께 사용할 경우 Redis Cluster에서는 모든 Key가 같은 Hash Slot에 있어야 한다. 따라서 이벤트 ID를 `{100}`과 같은 동일한 hash tag로 표현한다.

---

## **3-3. issued-users에는 어떤 값이 저장되는가**

```
coupon:100:issued-users

{
  user:1,
  user:5,
  user:20
}
```

여기에 저장된 사용자는 최종 발급 완료 사용자가 아니다.

정확한 의미는 다음과 같다.

```
Redis 선착순 판정을 통과한 사용자
```

실무에서는 의미를 더 정확하게 표현하기 위해 다음 이름을 사용할 수도 있다.

```
coupon:{eventId}:reserved-users
coupon:{eventId}:admitted-users
```

---

## **3-4. request-count를 사용한다면 어떤 역할을 하는가**

```
INCR coupon:100:request-count
→ 1, 2, 3, 4 ...
```

`request-count`는 다음을 나타낼 수 있다.

```
- 전체 요청 시도 횟수
- 요청이 도착한 순번
```

하지만 다음을 의미하지 않는다.

```
- 고유 통과 사용자 수
- 최종 쿠폰 발급 수량
```

같은 사용자가 여러 번 요청해도 매번 증가하기 때문이다.

최종 설계에서는 다음처럼 역할을 나누는 것이 좋다.

```
고유 통과 사용자 수
→ SCARD issued-users

전체 요청 수 관측
→ 애플리케이션 메트릭
```

---

## **3-5. Redis Key에 TTL이 필요한 이유**

이벤트 종료 후에도 Key가 남아 있으면 Redis 메모리가 계속 증가한다.

```
- 메모리 사용량 증가
- 운영 비용 증가
- maxmemory 도달
- 쓰기 실패 또는 Eviction 위험
- 수동 정리 부담
```

TTL의 목적은 이벤트 간 데이터가 섞이는 것을 막는 것이 아니다.

eventId가 다르면 Key 자체가 다르기 때문이다.

```
coupon:100:issued-users
coupon:200:issued-users
```

TTL의 목적은 종료된 이벤트 데이터를 자동으로 정리하는 것이다.

---

## **3-6. 이벤트가 끝난 뒤 Redis Key를 정리하는 방법**

### **방법 1. TTL 자동 만료**

```
EXPIREAT coupon:100:metadata <cleanupAt>
```

상대 시간보다 절대 만료 시각을 사용하는 편이 안전하다.

```
endAt
= 이벤트 참여 종료 시각

cleanupAt
= Redis Key 삭제 시각
```

두 값은 다를 수 있다.

예를 들어 이벤트가 끝난 뒤 고객 문의를 위해 데이터를 3일 더 보관할 수 있다.

`issued-users`와 `admission-requests`는 첫 통과 요청 전에 존재하지 않을 수 있다. 존재하지 않는 Key에 `EXPIREAT`을 먼저 실행하면 TTL이 예약되지 않는다. 따라서 이 두 Key의 TTL은 첫 `SADD`/`HSET`과 같은 Lua Script 안에서 `cleanupAt`으로 설정한다. `metadata`는 이벤트 준비 단계에서 생성하므로 생성 직후 `EXPIREAT`을 설정한다.

### **방법 2. 정리 배치**

```
- 종료된 이벤트 Key 확인
- TTL이 없는 Key에 만료 시각 설정
- 불필요한 Key 삭제
```

운영 환경에서는 전체 Key 공간을 한 번에 순회하는 `KEYS`보다 점진적으로 순회하는 `SCAN`을 사용하는 것이 좋다.

---

# **과제 4. Redis Set 기반 중복 요청 방지 설계하기**

## **4-1. Redis Set을 사용하는 이유**

Set은 같은 값을 중복 저장하지 않는다.

이 특성이 다음 요구사항과 일치한다.

```
한 사용자는 하나의 이벤트에서 한 번만 통과한다.
```

필요한 명령은 다음과 같다.

```
SISMEMBER
→ 사용자가 이미 있는지 확인

SADD
→ 사용자 추가

SCARD
→ Set에 저장된 고유 사용자 수 확인
```

---

## **4-2. SADD 반환값 1과 0의 의미**

```
SADD key member
```

```
반환값 1

기존에 없던 값이 새롭게 추가되었다.
```

```
반환값 0

이미 같은 값이 존재하므로 추가되지 않았다.
```

---

## **4-3. 같은 사용자가 두 번 요청했을 때**

### **첫 번째 요청**

```
SISMEMBER → 0
SCARD → 999
999 < 1000
SADD → 1

결과: SUCCESS
```

### **두 번째 요청**

```
SISMEMBER → 1

결과: DUPLICATE
Kafka 발행하지 않음
```

---

## **4-4. Redis Set 구조**

```
coupon:100:issued-users

+------------------+
| user:1           |
| user:5           |
| user:20          |
+------------------+

SCARD = 3
```

`user:30`이 처음 요청하면 다음과 같다.

```
SADD user:30
→ 1
```

```
+------------------+
| user:1           |
| user:5           |
| user:20          |
| user:30          |
+------------------+

SCARD = 4
```

`user:5`가 다시 요청하면 다음과 같다.

```
SADD user:5
→ 0

SCARD = 4
```

---

## **4-5. Set만 사용하면 수량 제한까지 해결되는가**

Set의 중복 제거 기능만으로는 수량 제한이 자동으로 해결되지 않는다.

Set은 같은 사용자의 중복만 막는다.

서로 다른 사용자라면 계속 저장된다.

```
user:1
user:2
...
user:1000
user:1001
```

따라서 다음 작업이 필요하다.

```
1. SISMEMBER
2. SCARD
3. limit 비교
4. SADD
```

그리고 이 작업들을 Lua Script 하나로 묶어야 한다.

---

## **4-6. Set에 저장된 사용자 수를 확인하는 명령어**

```
SCARD coupon:100:issued-users
```

Set은 중복 값을 저장하지 않으므로 `SCARD` 결과는 Redis를 통과한 고유 사용자 수다.

다만 다음 세 값은 서로 다르다.

```
request-count
= 전체 요청 시도 횟수

SCARD
= Redis를 통과한 고유 사용자 수

DB ISSUED
= 최종 쿠폰 발급 완료 수
```

`request-count`를 운용하고 장애나 Redis Key 유실이 없는 정상 흐름에서는 다음 관계를 기대할 수 있다.

```
request-count ≥ SCARD ≥ DB ISSUED
```

하지만 TTL 만료, Eviction, Failover, 관리자 직접 발급 등이 발생하면 이 관계는 깨질 수 있다.

---

# **과제 5. 잘못된 Redis 명령 조합의 Race Condition 분석하기**

## **5-1. SCARD 후 SADD 방식의 처리 흐름**

```
currentCount = SCARD issued-users
        |
        v
currentCount < limit?
        |
        +-- NO → SOLD_OUT
        |
        +-- YES
              |
              v
         SADD userId
              |
              v
          SUCCESS
```

`SCARD`와 `SADD`는 각각 원자적으로 실행된다.

하지만 `SCARD → 비교 → SADD` 전체 과정은 원자적이지 않다.

---

## **5-2. user:A와 user:B가 동시에 요청했을 때**

```
제한 수량: 1,000
현재 통과 인원: 999
남은 자리: 1
```

```
시간      user:A                         user:B
----------------------------------------------------------------

T1        SCARD → 999

T2                                       SCARD → 999

T3        999 < 1000
          통과 가능 판단

T4                                       999 < 1000
                                         통과 가능 판단

T5        SADD user:A
          Set 크기 → 1,000

T6                                       SADD user:B
                                         Set 크기 → 1,001
```

---

## **5-3. 두 사용자 모두 발급 가능하다고 판단하는 이유**

두 요청 모두 다른 사용자가 추가되기 전에 `SCARD = 999`를 읽었다.

```
user:A
→ 999를 읽음

user:B
→ 999를 읽음
```

둘 다 자신이 마지막 자리를 사용할 수 있다고 판단한다.

---

## **5-4. 최종적으로 1,001명이 통과할 수 있는 이유**

`user:A`와 `user:B`는 서로 다른 값이다.

```
SADD user:A → 1
SADD user:B → 1
```

Set은 같은 값의 중복만 막는다.

서로 다른 사용자에 대한 전체 수량 제한은 자동으로 처리하지 않는다.

---

## **5-5. 핵심 원인**

```
Check

SCARD로 현재 인원을 확인한다.
limit과 비교한다.

Act

SADD로 사용자를 추가한다.
```

Check와 Act 사이에 다른 요청이 실행될 수 있다.

```
확인한 순간의 상태
≠
행동하는 순간의 상태
```

이를 Check-Then-Act Race Condition이라고 한다.

---

## **5-6. 하나로 묶어야 하는 작업**

```
1. SISMEMBER
2. SCARD
3. limit 비교
4. SADD
```

네 작업 사이에 다른 요청이 끼어들 수 없어야 한다.

Redis Lua Script를 사용하면 이 흐름을 하나로 실행할 수 있다.

---

# **과제 6. Redis Lua Script 기반 원자적 처리 설계하기**

## **6-1. Lua Script로 묶어야 하는 작업**

```
1. SISMEMBER
   이미 통과한 사용자인지 확인

2. SCARD
   현재 통과 인원 확인

3. limit 비교
   수량이 남아 있는지 확인

4. SADD
   조건을 만족하면 사용자 추가

5. HSET
   통과 자리를 만든 requestId 저장

6. EXPIREAT
   첫 생성된 통과 Key에 cleanupAt 적용
```

---

## **6-2. Lua Script 처리 흐름**

```
[Lua Script 시작]
       |
       v
SISMEMBER
       |
       +-- 이미 존재
       |      → DUPLICATE
       |
       v
SCARD
       |
       v
currentCount >= limit?
       |
       +-- YES
       |      → SOLD_OUT
       |
       v
SADD + HSET
       |
       v
EXPIREAT
       |
       v
SUCCESS
       |
       v
[Lua Script 종료]
```

Script가 끝날 때까지 다른 클라이언트 명령은 중간에 들어올 수 없다.

---

## **6-3. Lua Script 반환값**

| 반환값 | 의미 | API Server 처리 |
| --- | --- | --- |
| `SUCCESS` | Redis 선착순 판정 통과 | Kafka 메시지 발행 |
| `DUPLICATE` | 이미 통과한 사용자 | 중복 응답 |
| `SOLD_OUT` | 제한 수량 도달 | 품절 응답 |

metadata 누락, 잘못된 Key 타입, 잘못된 인자는 비즈니스 결과 세 가지로 반환하지 않고 Redis error로 종료한다. API Server는 이를 서버 구성·운영 오류로 기록하고 Kafka에 발행하지 않아야 한다.

---

## **6-4. Lua Script가 Race Condition을 해결하는 방법**

```
현재 통과 인원: 999
limit: 1,000
```

### **user:A의 Script**

```
SISMEMBER → 0
SCARD → 999
999 < 1000
SADD user:A
SUCCESS
```

Set 크기는 1,000이 된다.

그다음 `user:B`의 Script가 실행된다.

```
SISMEMBER → 0
SCARD → 1,000
1000 >= 1000
SOLD_OUT
```

`user:B`는 `SADD`를 실행하지 않는다.

---

## **6-5. 기본 Lua Script**

```lua
-- KEYS[1]: 선착순 통과 사용자 Set
-- KEYS[2]: userId -> requestId Hash
-- KEYS[3]: 이벤트 metadata Hash
-- ARGV[1]: userId
-- ARGV[2]: requestId

local issuedUsersKey = KEYS[1]
local admissionRequestsKey = KEYS[2]
local metadataKey = KEYS[3]
local userId = ARGV[1]
local requestId = ARGV[2]

if not userId or userId == '' or not requestId or requestId == '' then
    return redis.error_reply('INVALID_REQUEST')
end

-- WRONGTYPE으로 첫 쓰기 뒤 Script가 중단되는 상황을 막기 위해 미리 검증한다.
local issuedUsersType = redis.call('TYPE', issuedUsersKey).ok
local admissionRequestsType = redis.call('TYPE', admissionRequestsKey).ok
local metadataType = redis.call('TYPE', metadataKey).ok

if issuedUsersType ~= 'none' and issuedUsersType ~= 'set' then
    return redis.error_reply('INVALID_ISSUED_USERS_TYPE')
end

if admissionRequestsType ~= 'none' and admissionRequestsType ~= 'hash' then
    return redis.error_reply('INVALID_ADMISSION_REQUESTS_TYPE')
end

if metadataType ~= 'hash' then
    return redis.error_reply('INVALID_METADATA_TYPE')
end

-- API Server의 로컬 캐시가 아니라 Redis metadata를 공통 기준으로 사용한다.
local limit = tonumber(redis.call('HGET', metadataKey, 'limit'))
local cleanupAt = tonumber(redis.call('HGET', metadataKey, 'cleanupAt'))

-- 쓰기 전에 모든 기준값을 검증한다.
if not limit or limit <= 0 then
    return redis.error_reply('INVALID_EVENT_LIMIT')
end

if not cleanupAt or cleanupAt <= 0 then
    return redis.error_reply('INVALID_CLEANUP_AT')
end

-- 이미 통과한 사용자인지 확인한다.
if redis.call('SISMEMBER', issuedUsersKey, userId) == 1 then
    return 'DUPLICATE'
end

-- 현재 통과한 고유 사용자 수를 확인한다.
local currentCount = redis.call('SCARD', issuedUsersKey)

-- 제한 수량에 도달했다면 품절이다.
if currentCount >= limit then
    return 'SOLD_OUT'
end

-- 모든 검증을 통과한 뒤 사용자를 추가한다.
redis.call('SADD', issuedUsersKey, userId)
redis.call('HSET', admissionRequestsKey, userId, requestId)

-- 두 Key는 첫 요청에서 생성될 수 있으므로 쓰기와 같은 Script에서 TTL을 설정한다.
-- cleanupAt은 metadata 생성 시 현재 시각보다 뒤인 값으로 검증해야 한다.
redis.call('EXPIREAT', issuedUsersKey, cleanupAt)
redis.call('EXPIREAT', admissionRequestsKey, cleanupAt)

return 'SUCCESS'
```

Redis Lua Script는 다른 명령이 중간에 끼어들지 못하게 실행된다.

다만 관계형 DB 트랜잭션처럼 중간 오류 발생 시 이전 쓰기를 자동 롤백해주는 것은 아니다.

따라서 모든 검증을 첫 번째 쓰기 명령보다 앞에 배치해야 한다.

Redis Cluster에서는 `KEYS[1]`, `KEYS[2]`, `KEYS[3]`을 모두 `coupon:{eventId}:...`로 만들어 같은 Hash Slot에 배치해야 한다. Script가 접근하는 Key 이름은 모두 `KEYS`로 명시적으로 전달한다.

---

## **6-6. Lua Script 성공이 최종 발급 완료가 아닌 이유**

Redis SUCCESS 이후에도 다음 과정이 남아 있다.

```
Kafka 메시지 발행
→ Consumer 처리
→ DB 조건부 UPDATE
→ coupon_issue INSERT
→ DB COMMIT
```

따라서 다음처럼 구분해야 한다.

```
Redis SUCCESS
= 선착순 자리 확보

DB ISSUED
= 실제 쿠폰 발급 완료
```

---

# **과제 7. INCR 기반 선착순 판정 방식 분석하기**

## **7-1. 단순 INCR 선행 방식의 기본 흐름**

```
Client
  |
  v
API Server
  |
  | INCR request-count
  v
Redis
  |
  | 요청 순번 반환
  v
API Server
  |
  | 순번 <= limit?
  |
  +-- NO → SOLD_OUT
  |
  +-- YES
         |
         | 중복 사용자 확인
         v
      SADD
         |
         v
      Kafka 발행
```

사용하는 Key는 다음과 같다.

```
coupon:100:request-count
coupon:100:issued-users
```

---

## **7-2. INCR이 보장하는 것**

`INCR`은 원자적으로 Counter를 1씩 증가시킨다.

```
요청 A → 1
요청 B → 2
요청 C → 3
```

동시에 여러 요청이 실행되어도 성공한 요청은 서로 다른 값을 받는다.

따라서 요청에 순번을 부여하는 데에는 적합하다.

---

## **7-3. INCR 명령 하나로 보장하지 못하는 것**

```
- 같은 사용자의 중복 요청 차단
- 고유 사용자 수 계산
- 사용자당 한 번만 Counter 증가
- Kafka 발행 성공
- DB 최종 발급 성공
- 실패한 요청의 Counter 복구
```

`INCR` 명령 자체는 요청자가 누구인지 모르고 숫자만 증가시킨다. 다만 Lua Script에서 먼저 중복을 확인하고 신규 사용자일 때만 `INCR`하는 변형 설계는 가능하다. 이 경우에도 Set·Counter 간 일치와 보상 로직을 함께 설계해야 한다.

---

## **7-4. 주어진 상황에서 발생하는 문제**

```
쿠폰 수량: 3개
```

```
user:1 첫 요청
→ INCR = 1
→ 통과 후보

user:1 두 번째 요청
→ INCR = 2
→ 통과 후보

user:1 세 번째 요청
→ INCR = 3
→ 통과 후보

user:2 첫 요청
→ INCR = 4
→ SOLD_OUT
```

실제 고유 사용자는 `user:1` 한 명뿐이다.

하지만 중복 요청이 세 자리를 모두 소비해 `user:2`가 부당하게 품절 처리된다.

---

## **7-5. 다이어그램**

```
쿠폰 수량: 3개

+---------+---------+---------+---------+
| INCR=1  | INCR=2  | INCR=3  | INCR=4  |
+---------+---------+---------+---------+
| user:1  | user:1  | user:1  | user:2  |
+---------+---------+---------+---------+
| 통과    | 중복    | 중복    | 품절    |
+---------+---------+---------+---------+
```

반면 Set을 기준으로 보면 다음과 같다.

```
coupon:100:issued-users

+------------------+
| user:1           |
+------------------+

SCARD = 1
```

`user:1`이 여러 번 요청해도 Set에는 한 번만 저장된다.

---

## **7-6. request-count를 최종 발급 수량으로 보면 안 되는 이유**

```
request-count
= 전체 요청 횟수
= 중복 클릭 포함
= 품절 이후 요청 포함
```

```
SCARD issued-users
= Redis 판정을 통과한 고유 사용자 수
```

```
DB ISSUED 수
= 최종 발급 완료 수
```

세 값은 서로 다른 의미를 가진다.

---

## **7-7. 실제 통과 사용자 수의 기준**

```
Redis 기준 통과 사용자 수
→ SCARD coupon:{eventId}:issued-users

최종 발급 사용자 수
→ DB의 ISSUED 발급 기록 수
```

사용자에게 보여주거나 정산에 사용하는 최종 수치는 DB를 기준으로 해야 한다.

---

## **7-8. INCR 방식과 Set + Lua Script 비교**

| 구분 | 단순 INCR 선행 | Set + Lua Script |
| --- | --- | --- |
| 수량 기준 | 전체 요청 순번 | 고유 통과 사용자 수 |
| 중복 방지 | INCR만으로 불가능 | Set으로 가능 |
| 장점 | 단순하고 순번 부여가 쉬움 | 중복과 수량을 함께 처리 |
| 단점 | 중복 요청이 자리를 소비함 | Script 관리가 필요함 |
| 권장 여부 | 비교·관측용 | 선착순 판정에 권장 |

```
요청마다 먼저 실행하는 INCR은 요청을 센다.

SCARD는 사람을 센다.

사용자 한 명당 쿠폰 하나인 선착순에서는
고유 사용자를 세어야 한다.
```

즉 중복 요청이 자리를 소비하는 것은 `INCR`의 필연적 특성이 아니라, **중복 확인보다 INCR을 먼저 실행하는 단순 구현**의 한계다.

---

# **과제 8. Redis 판정 결과에 따른 API 응답 설계하기**

## **상황 A. Redis SUCCESS + Kafka 발행 성공**

### **8-A-1. API 응답 JSON**

```json
{
  "status": "PENDING",
  "message": "쿠폰 발급 요청이 접수되었습니다.",
  "requestId": "550e8400-e29b-41d4-a716-446655440000"
}
```

### **8-A-2. HTTP Status Code**

```
202 Accepted
```

요청은 접수되었지만 Consumer와 DB 처리가 아직 완료되지 않았기 때문이다.

### **8-A-3. 최종 발급 성공을 의미하지 않는 이유**

```
완료된 것

- Redis 선착순 판정
- Kafka 메시지 발행

남은 것

- Consumer 처리
- DB 수량 확보
- 발급 기록 저장
- DB COMMIT
```

### **8-A-4. Consumer 처리**

```
1. Kafka 메시지 소비
2. DB 트랜잭션 시작
3. 최종 수량 확보
4. 발급 기록 저장
5. COMMIT
6. ISSUED 상태 확정
```

조건부 UPDATE와 발급 기록 INSERT는 하나의 트랜잭션으로 묶어야 한다.

---

## **상황 B. Redis DUPLICATE**

### **8-B-1. API 응답 JSON**

```json
{
  "status": "DUPLICATE",
  "message": "이미 선착순 판정을 통과한 이력이 있습니다. 발급 상태를 확인해 주세요."
}
```

### **8-B-2. HTTP Status Code**

```
409 Conflict
```

요청 형식이 잘못된 것은 아니지만, 이미 통과한 사용자라는 현재 상태와 충돌한다.

프로젝트 정책에 따라 `200 OK`와 비즈니스 상태값을 사용할 수도 있다.

중요한 것은 API 전체에서 일관된 방식을 사용하는 것이다.

### **8-B-3. Kafka에 발행하면 안 되는 이유**

```
- Consumer 중복 처리
- DB 중복 INSERT
- UNIQUE 위반
- 불필요한 예외 처리
```

### **8-B-4. 사용자 UX**

```
- 첫 클릭 후 버튼 비활성화
- 처리 중 상태 표시
- DUPLICATE를 에러 화면이 아닌 기존 요청 안내로 표시
```

이 해설의 최종 설계에서는 `coupon:{eventId}:admission-requests` Hash에 `userId → requestId`를 저장한다. DUPLICATE 응답에 기존 requestId를 반환하거나 발급 상태 조회 API로 연결할 수 있다.

---

## **상황 C. Redis SOLD_OUT**

### **8-C-1. API 응답 JSON**

```json
{
  "status": "SOLD_OUT",
  "message": "쿠폰이 모두 소진되었습니다."
}
```

### **8-C-2. HTTP Status Code**

```
409 Conflict
```

이벤트는 존재하지만 현재 품절 상태와 요청이 충돌한다.

### **8-C-3. Kafka에 발행하면 안 되는 이유**

Redis 기준으로 선착순 통과 가능 인원이 모두 찬 요청이다.

정상적인 흐름에서는 Kafka와 DB까지 전달해도 성공할 수 없으므로 Kafka에 발행하지 않는다.

다만 Kafka 발행 실패, DB 저장 실패, 보상 처리 실패 등으로 Redis와 DB 상태가 어긋난 경우에는 Redis `SOLD_OUT`이 DB의 최종 품절 상태와 다를 수 있다.

### **8-C-4. 품절 요청을 Redis에서 차단해야 하는 이유**

```
쿠폰 1,000개가 짧은 시간에 소진

이후 수만 건의 요청 계속 유입
```

이 요청을 전부 DB까지 보내면 동일한 `coupon_event` 행에 Lock 경합이 집중된다.

Redis에서 차단하면 Kafka, Consumer, DB를 보호할 수 있다.

---

# **과제 9. Redis SUCCESS 이후 Kafka 발행 실패 분석하기**

## **9-1. 전체 흐름**

```
Client
  |
  v
API Server
  |
  v
Redis Lua Script
  |
  | SADD 성공
  | SUCCESS
  v
API Server
  |
  | Kafka 발행 시도
  v
Kafka 장애
  |
  v
발행 실패
```

최종 상태는 다음과 같다.

```
Redis
→ 사용자가 통과 목록에 있음

Kafka
→ 메시지가 없음

DB
→ 발급 기록이 없음
```

---

## **9-2. Redis에 남는 상태**

```
coupon:100:issued-users

+------------------+
| user:1           |
+------------------+
```

사용자는 선착순 자리를 차지했지만 Kafka 메시지는 발행되지 않았다.

---

## **9-3. Kafka에 생기는 문제**

명확한 발행 실패라면 Kafka에는 해당 요청 메시지가 없다.

Kafka 입장에서는 이 요청이 존재하지 않는다.

---

## **9-4. Consumer가 처리할 수 없는 이유**

Consumer는 Kafka Topic을 구독한다.

메시지가 없으면 처리할 방법이 없다.

```
Redis에는 있음
Kafka에는 없음
DB에는 없음
```

이를 유령 통과자라고 볼 수 있다.

---

## **9-5. 사용자에게 PENDING을 줘도 되는가**

안 된다.

```
PENDING
= 요청이 비동기 처리 흐름에 접수되었다.
```

Kafka 발행이 명확하게 실패했다면 접수되지 않은 것이다.

```json
{
  "status": "ACCEPT_FAILED",
  "message": "요청 접수에 실패했습니다. 잠시 후 다시 시도해 주세요."
}
```

```
HTTP 503 Service Unavailable
```

---

## **9-6. 가능한 보상 처리 방법**

```
1. Kafka 발행 재시도

2. 실패 기록 저장

3. 명확한 발행 실패라면
   requestId 소유권을 확인한 뒤 Redis 자리 반납

4. 복구 배치를 통한 재처리

5. 운영자 알림
```

```
Kafka 발행 실패
       |
       v
제한된 횟수만큼 재시도
       |
       v
실패 기록 저장
       |
       v
requestId 비교 후 SREM + HDEL 보상
       |
       v
503 응답
```

실패 기록을 먼저 저장해야 복구 과정에서 어떤 요청이 실패했는지 알 수 있다.

Set에서 `userId`만 무조건 `SREM`하면 지연된 예전 보상 작업이 같은 사용자의 새 요청으로 만들어진 자리까지 삭제할 수 있다. 따라서 `admission-requests` Hash의 현재 requestId가 실패한 requestId와 같을 때만 두 Key를 정리해야 한다.

```lua
-- KEYS[1]: issued-users Set
-- KEYS[2]: admission-requests Hash
-- ARGV[1]: userId
-- ARGV[2]: 보상할 requestId

local ownerRequestId = redis.call('HGET', KEYS[2], ARGV[1])

if ownerRequestId ~= ARGV[2] then
    return 'NOT_OWNER'
end

redis.call('SREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])

return 'RELEASED'
```

---

## **9-7. Redis 보상 처리도 실패할 수 있는 이유**

```
Redis SUCCESS
→ Kafka 발행 실패
→ requestId 비교 보상 Script 시도
→ Redis 연결 장애
→ 보상 실패
```

또는 보상 Script를 실행하기 전에 API Server가 종료될 수 있다.

따라서 단순한 `try-catch`만으로 해결할 수 없다.

```
- 내구성 있는 실패 기록
- requestId 기반 멱등성
- 재시도
- 복구 배치
- 모니터링
```

이 필요하다.

---

## **9-8. 이후 주차와의 연결**

```
5주차

requestId 기반 멱등성
요청 상태 저장
중복 처리 방지
```

```
6주차

DB 변경과 결과 이벤트 발행의 정합성을 위한 Outbox Pattern
```

```
8주차

Retry Topic
DLQ
보상 처리
장애 복구
```

---

## **Kafka Timeout 주의**

Kafka 발행 결과는 다음 세 가지로 구분할 수 있다.

```
1. 명확한 성공

ACK 조건 충족

2. 명확한 실패

직렬화 오류
메시지 크기 초과
Broker 전송 전 로컬 오류

3. 결과 불명확

Broker가 메시지를 저장했을 수 있지만
ACK를 받지 못한 Timeout
```

결과가 불명확한 상황에서 무조건 자리를 반납하면 안 된다.

실제로 Kafka에는 메시지가 저장되어 있을 수 있기 때문이다.

이 경우 다음이 필요하다.

```
- enable.idempotence 사용
- requestId 포함
- Consumer 멱등 처리
- 요청 상태를 UNKNOWN 또는 확인 필요 상태로 기록
- 일정 시간 후 재처리 또는 보상
```

Kafka Topic을 DB처럼 `requestId`로 직접 조회해서 저장 여부를 판단하는 방식은 일반적으로 어렵다.

`enable.idempotence` 설정은 Producer가 같은 batch를 재시도하며 만드는 중복을 줄여주지만, API 재요청과 Consumer 재처리까지 없애주는 end-to-end 멱등성은 아니다. 따라서 requestId와 DB 기반 Consumer 멱등 처리가 별도로 필요하다.

---

# **과제 10. Redis, Kafka, DB 역할 구분과 이후 주차 연결**

## **10-1. Redis, Kafka, DB 역할 비교**

| 구분 | Redis | Kafka | DB |
| --- | --- | --- | --- |
| 주요 역할 | 빠른 선착순 1차 판정 | 비동기 메시지 전달 | 최종 발급과 정합성 |
| 저장 데이터 | 통과 사용자, 판정 기준 | 발급 요청 메시지 | 이벤트 재고, 발급 기록 |
| 강점 | 빠른 O(1) 연산, Lua Script | 트래픽 완충, 재처리 | 트랜잭션, 제약 조건 |
| 한계 | Failover·Eviction·장애 가능 | 비즈니스 규칙을 모름 | 대량 요청 시 Lock 병목 |
| 실패 시 문제 | 유령 통과자·중복 판정 유실 | 요청 유실·중복·지연 | 최종 발급 실패·트랜잭션 롤백 |
| 핵심 | 중복·수량 1차 차단 | SUCCESS 요청 전달 | 최종 수량·중복 방어 |

---

## **10-2. Redis SUCCESS, REQUEST PENDING, DB ISSUED 차이**

```
Redis SUCCESS

Redis Set에 사용자가 등록되었다.
```

```
REQUEST PENDING

Kafka Producer의 발행 Future가 성공하고 설정된 ACK 조건을 충족해
비동기 요청이 접수되었다. 이 과제에서는 `acks=all`과 idempotent Producer를 전제로 한다.
```

```
DB ISSUED

수량 확보와 발급 기록 저장이
하나의 트랜잭션으로 COMMIT되었다.
```

---

## **10-3. 상태 흐름 다이어그램**

```
쿠폰 요청
   |
   v
Redis 판정
   |
   +-- DUPLICATE
   |
   +-- SOLD_OUT
   |
   +-- SUCCESS
          |
          v
      Kafka 발행
          |
          +-- 명확한 실패
          |      → ACCEPT_FAILED
          |
          +-- 발행 성공
                 |
                 v
              PENDING
                 |
                 v
          Consumer + DB
                 |
          +------+------+
          |             |
          v             v
       ISSUED         FAILED
```

---

## **10-4. Redis만 믿고 DB UNIQUE를 두지 않으면**

Redis 뒤에서도 중복이 발생할 수 있다.

```
- Kafka 메시지 중복 소비
- Consumer 재처리
- Redis Failover
- Redis Key Eviction
- 관리자 직접 발급
- Redis를 우회하는 코드
```

따라서 DB에 다음 제약이 필요하다.

```
UNIQUE(event_id, user_id)
```

하지만 UNIQUE만으로 전체 수량은 막을 수 없다.

```
사용자 중복
→ UNIQUE

전체 수량
→ 조건부 UPDATE
```

---

## **10-5. Kafka만으로 수량과 중복을 막기 어려운 이유**

Kafka는 다음 정보를 모른다.

```
- 쿠폰이 몇 개 남았는가
- 이 사용자가 이미 쿠폰을 받았는가
- 이 메시지가 재처리된 메시지인가
```

Kafka는 메시지를 전달한다.

비즈니스 판단은 Redis와 DB가 수행해야 한다.

---

## **10-6. 4주차 — DB 최종 정합성**

### **10-6-1. Redis를 사용해도 DB 제약이 필요한 이유**

Redis는 빠른 1차 판정을 담당한다.

하지만 장애나 우회 경로가 있을 수 있으므로 최종 정확성은 DB가 강제해야 한다.

### **10-6-2. UNIQUE가 막는 문제와 막지 못하는 문제**

```
막는 것

같은 사용자가 같은 이벤트에서
쿠폰을 두 번 받는 문제
```

```
막지 못하는 것

서로 다른 사용자 1,001명이
쿠폰을 받는 문제
```

따라서 다음 두 장치가 필요하다.

```
UNIQUE(event_id, user_id)

+

조건부 UPDATE
```

---

## **10-7. 5주차 — requestId 기반 멱등성**

### 10-7-1. 같은 요청이 여러 번 처리될 수 있는 이유

- Kafka 중복 전달
- Consumer 재시작
- Offset 재처리
- 복구 배치 재발행

이 경우 동일한 requestId를 기준으로
Consumer와 DB에서 멱등 처리한다.

사용자의 중복 클릭과 응답 유실 후 API 재시도까지 같은 요청으로 식별하려면 클라이언트가 동일한Idempotency-Key를 다시 전달해야 한다.

클라이언트가 Idempotency-Key를 사용하지 않는다면 eventId + userId를 기준으로 기존 requestId를 조회해 기존 요청 상태를 안내할 수 있다.

### **10-7-2. Redis 중복 체크와 requestId 멱등성 차이**

```
Redis 중복 체크

기준: userId
질문: 이 사용자가 이미 통과했는가?
위치: API 앞단
```

```
requestId 멱등성

기준: requestId
질문: 이 요청을 이미 처리했는가?
위치: Consumer와 DB
```

```
DB UNIQUE

기준: eventId + userId
질문: 이 사용자가 이미 최종 발급받았는가?
위치: DB 최종 방어선
```

세 장치는 서로 다른 중복을 막는다.

---

## **10-8. 6주차 — Outbox Pattern**

### **10-8-1. DB 저장 후 Kafka 발행 실패 문제**

```
DB 발급 기록 저장 성공
→ COMMIT

Kafka coupon.issued 이벤트 발행
→ 실패
```

그 결과 다음처럼 상태가 달라진다.

```
DB
→ 발급 완료

알림·마이페이지·통계
→ 발급 사실을 모름
```

### **10-8-2. Outbox Pattern이 필요한 이유**

```
[하나의 DB 트랜잭션]

coupon_issue INSERT

outbox_event INSERT

COMMIT
```

이후 별도 Publisher가 Outbox를 읽어 Kafka에 발행한다.

발행이 실패해도 DB에 이벤트가 남아 있으므로 재시도할 수 있다.

다만 동일 이벤트가 중복 발행될 수 있으므로 Event ID와 Consumer 멱등 처리가 필요하다.

또한 Outbox는 다음 문제를 해결한다.

```
DB 변경
+
결과 이벤트 발행
```

Redis의 `SADD`와 Kafka 요청 발행을 하나의 트랜잭션으로 묶어주는 구조는 아니다.

---

## **10-9. 7주차 — EDA 기반 서비스 분리**

### **10-9-1. 쿠폰 발급 후 서비스를 직접 호출하면**

```
Coupon Service
   |
   +-- Notification Service
   +-- MyPage Service
   +-- Analytics Service
```

다음 문제가 생긴다.

```
- 서비스 간 결합도 증가
- 후속 서비스 장애가 쿠폰 서비스로 전파
- 응답 시간 증가
- 새로운 서비스 추가 시 쿠폰 서비스 수정
```

### **10-9-2. 이벤트 기반으로 분리하면**

```
Coupon Service
   |
   | coupon.issued
   v
Kafka
   |
   +-- Notification Consumer
   +-- MyPage Consumer
   +-- Analytics Consumer
```

장점은 다음과 같다.

```
- 서비스 간 결합도 감소
- 장애 격리
- 비동기 처리
- 서비스별 독립 확장
- 읽기 모델 분리 가능
```

대신 최종 일관성과 이벤트 추적 복잡성을 받아들여야 한다.

---

## **10-10. 8주차 — Retry, DLQ, 보상 처리**

### **10-10-1. Redis SUCCESS 이후 DB 저장 실패**

DB 처리 결과는 세 종류로 구분해야 한다.

```
일시적 실패

- DB 연결 오류
- Deadlock
- 네트워크 문제

→ 재시도 가능
```

```
멱등 성공

- 같은 requestId의 ISSUED 기록이 이미 존재

→ 이전 처리가 이미 성공한 것
→ 기존 ISSUED 결과를 반환하고 메시지 처리 완료
```

```
최종적 비즈니스 실패

- DB 기준 SOLD_OUT
- 다른 requestId로 이미 발급된 사용자

→ 재시도해도 결과가 같음
```

`UNIQUE(event_id, user_id)` 위반을 무조건 `FAILED`로 처리하면 안 된다. Consumer가 같은 Kafka 메시지를 재처리한 것일 수 있으므로 requestId로 기존 발급 기록을 먼저 확인한다.

DB가 최종 SOLD_OUT이라면 Redis 자리를 반납해 다른 사용자를 통과시키면 안 된다.

DB에 남은 쿠폰이 없기 때문에 새 사용자도 실패한다.

### **10-10-2. Retry Topic과 DLQ**

```
coupon.issue.requested
        |
        | 실패
        v
coupon.issue.retry.5s
        |
        | 실패
        v
coupon.issue.retry.1m
        |
        | 실패
        v
coupon.issue.dlq
```

```
Retry Topic

일시적 장애가 복구될 시간을 주고
원본 Partition을 계속 처리하게 한다.
```

다만 별도 Retry Topic으로 이동하면 원래 메시지 순서가 깨질 수 있다.

```
DLQ

반복 재시도 후에도 실패하는 메시지를 격리한다.

운영자가 원인을 분석하고
수정 후 재처리하거나 수동 보상한다.
```

### **10-10-3. 보상 트랜잭션 예시**

```
Redis SUCCESS
→ Kafka 발행 명확한 실패

보상:
실패 기록 저장
requestId 소유권 비교 후 SREM + HDEL
재시도 안내
```

```
Redis SUCCESS
→ DB 최종 SOLD_OUT

처리:
FAILED 상태 확정
운영자 알림
불일치 원인 조사

DB에 실제 남은 쿠폰이 없으므로 Redis 자리를 반납하지 않고 이후 요청도 계속 차단
```

```
DB 발급 성공
→ 후속 이벤트 발행 실패

보상:
쿠폰 회수가 아니라
Outbox 이벤트 재발행
```

분산 시스템에서는 시간을 되돌려 롤백하기보다 반대 작업이나 재시도를 통해 상태를 올바른 방향으로 수렴시킨다.

---

# **보너스 과제. 나쁜 Redis 설계의 문제점 찾기**

## **필수 수준**

| 나쁜 설계 | 문제 | 개선 |
| --- | --- | --- |
| `SCARD`와 `SADD`를 따로 실행 | Race Condition으로 제한 수량 초과 | 확인·비교·추가를 Lua Script로 묶기 |
| 요청마다 `INCR`부터 실행 | 중복 요청이 자리를 소비 | Set + Lua로 고유 통과자 수 판정 |
| Redis SUCCESS를 최종 성공으로 응답 | Kafka·DB 실패 시 거짓 성공 안내 | REQUEST PENDING과 DB ISSUED 구분 |
| DUPLICATE·SOLD_OUT도 Kafka에 발행 | Consumer·DB 불필요한 부하 | SUCCESS만 발행 |
| Kafka 발행 실패 후 Redis 상태를 방치 | 유령 통과자 발생 | 실패 기록, 재시도, 소유권 기반 보상 |
| 무조건 `SREM userId` 보상 | 예전 보상이 새 requestId의 자리를 삭제 | 현재 소유 requestId 비교 후 제거 |
| `SADD`와 `SISMEMBER`의 userId 형식이 다름 | 중복 차단이 조용히 무력화 | member 직렬화 규칙 통일 |
| DB UNIQUE만으로 전체 수량 방어 | 서로 다른 1,001명은 막지 못함 | UNIQUE + 조건부 UPDATE |

## **심화 수준**

| 나쁜 설계 | 문제 | 개선 |
| --- | --- | --- |
| API Server가 Lua에 서로 다른 limit 전달 | 서버별 캐시·배포 차이로 판정 기준 불일치 | Lua가 Redis metadata의 limit을 읽기 |
| Cluster에서 Lua Key를 서로 다른 Slot에 배치 | `CROSSSLOT` 오류로 판정 실패 | 동일 `{eventId}` hash tag와 명시적 `KEYS` 사용 |
| 존재하지 않는 Set에 `EXPIREAT`만 선행 | 나중에 생성된 Set에 TTL이 없음 | 첫 쓰기 Lua에서 생성과 TTL 설정 |
| 과거 cleanupAt을 사용 | 첫 통과 직후 Key가 삭제되어 초과 통과 가능 | metadata 생성 시 현재 시각과 cleanupAt 검증 |
| 일반 캐시 Redis에서 중요 Key도 Eviction | 통과 목록 유실로 중복·초과 판정 | 용도 분리, 메모리 모니터링, 적절한 maxmemory policy |
| Lua Script가 오류 시 자동 롤백된다고 가정 | 오류 전 쓰기가 남을 수 있음 | 모든 검증을 첫 쓰기 앞에 배치 |
| Kafka Timeout을 무조건 실패로 판단 | 실제 저장된 메시지가 있는데 자리를 반납 | 명확한 실패와 UNKNOWN 구분 |
| `enable.idempotence`만으로 전 구간 멱등성을 보장한다고 가정 | API 재요청·Consumer 재처리 중복은 남음 | requestId + Consumer/DB 멱등 처리 |
| UNIQUE 위반을 무조건 FAILED로 처리 | 이미 성공한 같은 requestId의 재처리일 수 있음 | 기존 requestId 결과 확인 후 멱등 성공 처리 |
| Outbox가 Redis SADD와 Kafka 요청 발행을 원자화한다고 가정 | Redis와 RDB는 다른 저장소 | Outbox는 DB 변경과 결과 이벤트 발행에 적용 |
