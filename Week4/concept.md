# 4주차 개념 설명: DB 테이블 설계와 최종 정합성 보장

## 주제

Consumer를 통해 DB에 도달한 발급 요청에 대해 DB 제약 조건과 트랜잭션으로 사용자 중복 발급과 전체 수량 초과를 방지한다.

---

## 1. 4주차 목표

1주차에서는 요청 접수와 실제 발급 처리를 분리했고,
2주차에서는 Kafka를 이용해 발급 요청을 비동기적으로 전달했다.
3주차에서는 Redis를 이용해 수량 초과와 사용자 중복 요청을 빠르게 차단했다.

4주차에서는 Redis와 Kafka를 통과한 요청을 DB에 정확하게 저장하는 방법을 다룬다.

이번 주차의 목표는 다음과 같다.

```
1. Redis 판정 통과가 최종 쿠폰 발급 성공이 아닌 이유를 이해한다.
2. coupon_event와 coupon_issue 테이블의 역할을 이해한다.
3. UNIQUE(event_id, user_id)가 필요한 이유와 한계를 이해한다.
4. 전체 발급 수량 초과를 DB에서 다시 방어하는 방법을 이해한다.
5. 수량 증가와 발급 기록 저장을 하나의 트랜잭션으로 묶는 이유를 이해한다.
6. Kafka 메시지가 중복 소비되어도 DB에서 중복 발급을 막는 방법을 이해한다.
7. 비관적 락, 낙관적 락, 조건부 UPDATE의 차이를 이해한다.
```

이번 주차의 핵심은 다음 문장이다.

```
Redis는 빠른 선착순 판정을 담당하고,
DB는 최종 발급 기록과 최종 정합성을 보장한다.
```

---

## 2. 전체 처리 구조와 상태 구분

<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/a6e9e88d-6148-4b80-a87e-c39cc75a2ff0" />

3주차까지 설계한 구조에 DB 저장 단계를 추가하면 다음과 같다.

```
Client
  |
  | 1. 쿠폰 발급 요청
  v
API Server
  |
  | 2. 인증 및 이벤트 기본 검증
  v
Redis
  |
  | 3. 중복 사용자 및 잔여 수량을 원자적으로 판정
  v
API Server
  |
  | 4. 판정을 통과한 요청을 Kafka에 발행
  v
Kafka
  |
  | 5. 발급 요청 메시지 전달
  v
Consumer
  |
  | 6. DB 트랜잭션으로 최종 발급 처리
  v
DB
```

각 단계의 성공은 의미가 다르다.

```
Redis ACCEPTED
= Redis 기준으로 선착순 판정을 통과했다.

Kafka PUBLISHED
= 발급 요청 메시지가 Kafka에 저장되었다.

DB ISSUED
= 발급 기록 저장 트랜잭션이 커밋되었다.
```

`Redis ACCEPTED`는 3주차에서 사용한 `Redis SUCCESS`와 같은 단계다. 최종 발급 성공과 혼동하지 않도록 이번 주차에서는 `ACCEPTED`로 표기한다.

따라서 다음과 같이 구분해야 한다.

```
Redis ACCEPTED ≠ 최종 발급 성공
Kafka PUBLISHED ≠ 최종 발급 성공
DB ISSUED      = 최종 발급 성공
```

Kafka는 메시지를 전달하는 역할을 한다. 별도의 요청 상태 저장소가 없다면 Kafka가 `PENDING`이라는 도메인 상태를 관리하는 것은 아니다.

---

## 3. DB가 최종 방어선이어야 하는 이유

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/c146c8cc-ec83-41e2-84ab-563d788a85eb" />

Redis가 요청을 정확히 판정하더라도 다음 문제가 발생할 수 있다.

```
1. Redis 판정 통과 후 Kafka 발행이 실패할 수 있다.
2. Kafka 메시지를 처리하던 Consumer가 중간에 실패할 수 있다.
3. DB 저장 후 Consumer Group Offset 커밋 전에 Consumer가 종료될 수 있다.
4. Consumer가 같은 메시지를 다시 처리할 수 있다.
5. Redis 데이터가 유실되거나 초기화될 수 있다.
6. 애플리케이션 버그로 같은 발급 요청이 DB에 여러 번 도달할 수 있다.
```

대표적인 중복 소비 상황은 다음과 같다.

```
Consumer가 메시지 수신
  |
  v
DB 발급 트랜잭션 커밋
  |
  v
Consumer Group Offset 커밋 전 Consumer 장애
  |
  v
Consumer 재시작
  |
  v
같은 메시지 재처리
```

DB에 아무 제약 없이 발급 기록을 INSERT하면 같은 사용자가 같은 이벤트의 쿠폰을 두 번 받을 수 있다.

따라서 애플리케이션의 사전 조회만 믿지 않고 DB 제약 조건을 마지막 방어선으로 사용해야 한다.

---

## 4. DB가 보장해야 하는 불변식

<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/cd24c807-a7c6-430e-aa3b-ff288f1bfc99" />

불변식은 시스템이 어떤 상황에서도 지켜야 하는 조건이다.

이번 주차에서는 다음 불변식을 DB에서 보장한다.

```
불변식 1.
한 사용자는 하나의 이벤트에서 쿠폰을 최대 한 번만 발급받는다.

불변식 2.
발급 수량은 0보다 작거나 전체 수량보다 많을 수 없다.

불변식 3.
issued_count 증가와 coupon_issue 저장은 함께 성공하거나 함께 실패한다.
```

각 불변식은 다음 장치로 보호한다.

| 불변식 | DB 방어 장치 |
| --- | --- |
| 사용자 중복 발급 방지 | `UNIQUE(event_id, user_id)` |
| 전체 수량 초과 방지 | 조건부 `UPDATE`, 수량 `CHECK` |
| 두 테이블의 일관성 | DB Transaction |

이번 주차에서는 발급 취소와 재발급을 다루지 않는다. `coupon_issue`는 최종 발급에 성공한 기록만 저장하고, 모든 행의 상태는 `ISSUED`라고 가정한다.

취소를 지원하려면 다음 정책을 추가로 결정해야 한다.

```
- 취소 시 issued_count를 감소시킬 것인가?
- 취소된 수량을 다른 사용자에게 다시 발급할 것인가?
- 취소한 사용자의 재발급을 허용할 것인가?
- 재발급을 허용한다면 UNIQUE 제약을 어떻게 변경할 것인가?
```

---

## 5. 테이블 관계

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/97f0426c-ee05-4742-9334-3098ca0c1fce" />

최소한 다음 두 테이블이 필요하다.

| 테이블 | 역할 |
| --- | --- |
| `coupon_event` | 쿠폰 이벤트, 전체 수량, DB 기준 발급 수량을 저장한다. |
| `coupon_issue` | 어떤 사용자가 어떤 이벤트 쿠폰을 발급받았는지 저장한다. |

두 테이블의 관계는 1:N이다.

```
+----------------------+
| coupon_event         |
|----------------------|
| id                   |
| name                 |
| total_quantity       |
| issued_count         |
| start_at             |
| end_at               |
| status               |
| version              |
+----------------------+
          |
          | 1 : N
          v
+----------------------+
| coupon_issue         |
|----------------------|
| id                   |
| event_id             |
| user_id              |
| status               |
| issued_at            |
+----------------------+
```

---

## 6. coupon_event 테이블

`coupon_event`는 쿠폰 이벤트 자체와 DB 기준 발급 수량을 저장한다.

PostgreSQL을 기준으로 다음과 같이 설계할 수 있다.

```sql
CREATE TABLE coupon_event (
    id BIGSERIAL PRIMARY KEY,

    name VARCHAR(100) NOT NULL,

    -- 전체 발급 가능 수량
    total_quantity INT NOT NULL,

    -- DB 트랜잭션이 완료된 발급 수량
    issued_count INT NOT NULL DEFAULT 0,

    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,

    -- READY, OPEN, CLOSED
    status VARCHAR(20) NOT NULL,

    -- 낙관적 락을 선택할 경우 사용한다.
    version BIGINT NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_coupon_event_quantity
        CHECK (
            total_quantity >= 0
            AND issued_count >= 0
            AND issued_count <= total_quantity
        ),

    CONSTRAINT chk_coupon_event_period
        CHECK (start_at < end_at),

    CONSTRAINT chk_coupon_event_status
        CHECK (status IN ('READY', 'OPEN', 'CLOSED'))
);
```

`TIMESTAMPTZ`는 시간대가 다른 서버와 DB 사이에서 동일한 시점을 안전하게 다루기 위해 사용한다.

`updated_at`의 기본값은 INSERT 시점에만 적용된다. 데이터가 변경될 때마다 값을 갱신하려면 애플리케이션 코드 또는 DB 트리거가 별도로 필요하다.

### issued_count를 저장하는 이유

실제 발급 기록 수는 `coupon_issue`를 집계해서 구할 수도 있다. 하지만 이벤트 row에 `issued_count`를 저장하면 다음 장점이 있다.

```
1. 남은 수량을 빠르게 판단할 수 있다.
2. DB 기준 발급 현황을 빠르게 조회할 수 있다.
3. Redis와 DB의 수량 차이를 비교할 수 있다.
4. DB에서 전체 수량 초과를 다시 방어할 수 있다.
```

대신 `issued_count`와 발급 기록이 서로 어긋나지 않도록 반드시 같은 트랜잭션에서 변경해야 한다.

---

## 7. coupon_issue 테이블

`coupon_issue`는 최종 발급에 성공한 사용자별 기록을 저장한다.

```sql
CREATE TABLE coupon_issue (
    id BIGSERIAL PRIMARY KEY,

    event_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,

    -- 이번 주차에서는 ISSUED만 저장한다.
    status VARCHAR(20) NOT NULL DEFAULT 'ISSUED',

    issued_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_coupon_issue_event
        FOREIGN KEY (event_id)
        REFERENCES coupon_event(id),

    CONSTRAINT chk_coupon_issue_status
        CHECK (status = 'ISSUED'),

    CONSTRAINT uk_coupon_issue_event_user
        UNIQUE (event_id, user_id)
);
```

가장 중요한 제약은 다음과 같다.

```sql
UNIQUE (event_id, user_id)
```

이 제약은 같은 사용자가 같은 이벤트 쿠폰을 두 번 발급받지 못하게 한다.

```
(event_id=100, user_id=10) 첫 INSERT  → 성공
(event_id=100, user_id=10) 재INSERT   → UNIQUE 위반

(event_id=100, user_id=11) INSERT     → 성공
(event_id=101, user_id=10) INSERT     → 성공
```

중복 여부를 먼저 조회한 뒤 INSERT하는 애플리케이션 코드만으로는 동시 요청을 안전하게 막을 수 없다.

두 요청이 동시에 조회하면 둘 다 “기존 기록 없음”을 확인할 수 있기 때문이다. 최종 중복 방지는 DB의 `UNIQUE` 제약이 담당해야 한다.

---

## 8. UNIQUE 제약이 막을 수 없는 문제

<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/bc68df69-1746-43f8-bad6-43b6cb70ae18" />

`UNIQUE(event_id, user_id)`는 사용자 중복 발급만 막는다. 전체 수량 초과는 단독으로 막지 못한다.

예를 들어 전체 수량이 1,000개일 때 서로 다른 사용자 1,001명이 INSERT를 시도하면 모든 `(event_id, user_id)` 조합이 다르다.

```
user_id=1    → UNIQUE 통과
user_id=2    → UNIQUE 통과
...
user_id=1000 → UNIQUE 통과
user_id=1001 → UNIQUE 통과 가능
```

따라서 두 문제를 별도로 방어해야 한다.

```
사용자 중복 발급
→ UNIQUE(event_id, user_id)

전체 수량 초과
→ issued_count 조건부 UPDATE
```

---

## 9. 조건부 UPDATE로 수량 초과 방지

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/3d768967-231d-4f21-9b26-7ce6539ba017" />

> 위 도식의 `상태·기간 조건 불충족`은 해당 조건을 `WHERE` 절에 추가했을 때만 영향받은 row 수가 0인 원인이 된다. `이미 처리된 중복 메시지`는 아래 조건부 UPDATE가 0을 반환하는 직접 원인이 아니라, 결과가 0인 뒤 기존 발급 기록을 조회해 구분하는 경우다.

수량이 남아 있을 때만 `issued_count`를 증가시키는 SQL은 다음과 같다.

```sql
UPDATE coupon_event
SET issued_count = issued_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :eventId
  AND issued_count < total_quantity;
```

여러 트랜잭션이 동시에 실행해도 DB는 같은 row의 갱신을 직렬화한다. 각 요청은 갱신 시점의 최신 값을 기준으로 조건을 다시 판단하므로 `issued_count`가 `total_quantity`를 넘어가지 않는다.

실행 결과는 영향받은 row 수로 판단한다.

```
영향받은 row 수 = 1
→ 수량 확보 성공

영향받은 row 수 = 0
→ 수정된 행이 없음
→ 남은 쿠폰 수량을 확보하지 못함
→ 현재 SQL에서는 이벤트가 없거나 쿠폰 수량이 소진된 경우
→ 이후 기존 발급 기록을 조회해 중복 메시지인지 추가로 구분
```

현재 SQL에서 결과가 0인 직접 원인은 이벤트가 존재하지 않거나 수량이 모두 소진된 경우다.

이벤트 상태나 기간까지 UPDATE 조건에 포함한다면 `READY`, `CLOSED`, 기간 밖인 경우에도 0이 반환될 수 있다. 사용자에게 정확한 실패 원인을 알려야 한다면 별도 조회로 원인을 구분해야 한다.

이번 구조에서는 Redis 판정 시점을 발급 자격 판단 시점으로 정한다. API와 Redis가 이벤트 상태와 기간을 먼저 검증하고, Consumer는 이를 다시 판정하지 않고 DB의 최종 수량과 중복 발급만 방어한다.

---

## 10. 발급 트랜잭션

<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/f4e675e3-805f-451a-a74f-1957c3648f5f" />

최종 발급 처리는 다음 두 작업으로 이루어진다.

```
1. coupon_event.issued_count 증가
2. coupon_issue 발급 기록 INSERT
```

두 작업이 따로 커밋되면 데이터가 어긋날 수 있다.

```
issued_count 증가 성공 + coupon_issue INSERT 실패
→ 수량은 줄었지만 발급받은 사용자가 없음

coupon_issue INSERT 성공 + issued_count 증가 실패
→ 발급 사용자는 있지만 수량에 반영되지 않음
```

따라서 하나의 트랜잭션으로 묶어야 한다.

```
Transaction 시작
  |
  v
조건부 UPDATE로 수량 확보
  |
  +-- 결과 = 0
  |     → 기존 coupon_issue 기록 조회
  |     → 중복 메시지와 실제 발급 실패 구분
  |
  +-- 결과 = 1
        |
        v
      coupon_issue INSERT
        |
        +-- 성공 → Commit
        |
        +-- 실패 → Rollback
```

SQL의 실행 흐름은 다음과 같다.

```sql
-- 하나의 트랜잭션 안에서 실행한다.

UPDATE coupon_event
SET issued_count = issued_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :eventId
  AND issued_count < total_quantity;

-- UPDATE 결과가 1일 때만 실행한다.
INSERT INTO coupon_issue (
    event_id,
    user_id,
    status
) VALUES (
    :eventId,
    :userId,
    'ISSUED'
);
```

중복 메시지의 처리 결과는 수량이 남아 있는지에 따라 달라질 수 있다.

수량이 남아 있다면 조건부 UPDATE가 성공한 뒤 INSERT에서 `UNIQUE` 위반이 발생한다. 이 경우 트랜잭션 전체가 Rollback되므로 앞에서 증가한 `issued_count`도 원래 값으로 복구된다.

하지만 이미 수량이 모두 소진되었다면 조건부 UPDATE 결과가 0이므로 INSERT까지 실행되지 않는다.

```
중복 메시지 재처리
  |
  v
조건부 UPDATE
  |
  +-- 결과 = 1
  |     |
  |     v
  |   coupon_issue INSERT
  |     |
  |     v
  |   UNIQUE 위반
  |     |
  |     v
  |   트랜잭션 전체 Rollback
  |     |
  |     v
  |   issued_count 증가도 취소
  |
  +-- 결과 = 0
        |
        v
      동일한 coupon_issue 기록 조회
        |
        +-- 기록 존재
        |     → 이미 처리된 중복 메시지
        |
        +-- 기록 없음
              → 실제 수량 소진 또는 이벤트 없음
```

따라서 조건부 UPDATE 결과가 0이라고 무조건 품절로 판단하면 안 된다. 동일한 `(event_id, user_id)` 발급 기록이 존재하는지 확인한 뒤 중복 메시지와 실제 수량 소진을 구분해야 한다.

---

## 11. Kafka 중복 소비 처리

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/6355211e-6877-4f76-9e04-dbf715c1957e" />

Kafka Consumer는 같은 메시지를 다시 처리할 수 있다.

```
1. Consumer가 메시지를 읽는다.
2. DB 발급 트랜잭션을 커밋한다.
3. Consumer Group Offset 커밋 전에 Consumer가 종료된다.
4. Consumer가 재시작된다.
5. 같은 메시지를 다시 읽는다.
```

재처리된 메시지가 같은 `(event_id, user_id)`로 INSERT를 시도하면 `UNIQUE` 제약이 중복 발급을 차단한다. 다만 이미 수량이 소진되었다면 조건부 UPDATE에서 먼저 0이 반환되어 INSERT까지 도달하지 않을 수 있으므로 두 경로를 모두 처리해야 한다.

Consumer는 이를 다음과 같이 처리할 수 있다.

```
조건부 UPDATE
  |
  +-- 결과 = 0
  |     |
  |     v
  |   동일한 coupon_issue 기록 조회
  |     |
  |     +-- 기록 존재
  |     |     → 이미 처리된 메시지로 정상 처리
  |     |
  |     +-- 기록 없음
  |           → 이벤트 없음 또는 수량 소진으로 처리
  |           → 로그·메트릭을 남기고 메시지 처리 완료
  |           → 불일치 복구는 별도 절차로 수행
  |
  +-- 결과 = 1
        |
        v
      coupon_issue INSERT
        |
        +-- 성공
        |     → 트랜잭션 Commit
        |     → 메시지 처리 완료
        |
        +-- UNIQUE 위반
              → 현재 트랜잭션 Rollback
              → 새 트랜잭션에서 기존 기록 조회
              → 기록 존재 시 중복 메시지로 정상 처리
              → 기록이 없으면 예상하지 못한 오류로 처리
```

PostgreSQL에서는 제약 조건 위반이 발생한 트랜잭션을 그대로 계속 사용할 수 없다. 반드시 먼저 Rollback한 후 기존 기록을 조회해야 한다.

DB 처리가 최종 성공했거나 이미 동일한 발급 기록이 존재함을 확인한 경우에는 메시지 처리를 완료한다. 이벤트 없음이나 수량 소진처럼 재시도로 해결되지 않는 비즈니스 실패도 실패 처리를 기록한 뒤 메시지를 완료하고, Redis-DB 불일치는 별도 복구 대상으로 남긴다. 반면 일시적인 DB 오류처럼 재시도가 필요한 실패에서는 Offset을 커밋하지 않는다.

이번 문서에서는 자동 커밋을 사용하지 않고, DB 처리 결과가 확정된 뒤 수동 커밋이나 프레임워크의 acknowledgment를 통해 Consumer Group Offset을 커밋한다고 가정한다.

`UNIQUE(event_id, user_id)`는 동일 사용자의 중복 발급을 막는 비즈니스 멱등성을 제공한다. `requestId`를 이용해 동일한 요청 자체를 식별하고 처리 결과를 관리하는 방식은 5주차에서 다룬다.

---

## 12. 동시성 제어 방식 비교

### 12.1 조건부 UPDATE

```sql
UPDATE coupon_event
SET issued_count = issued_count + 1
WHERE id = :eventId
  AND issued_count < total_quantity;
```

수량 확인과 증가를 하나의 SQL로 수행한다.

```
장점
- 구현이 단순하다.
- 별도의 조회와 애플리케이션 재시도 없이 수량 초과를 막을 수 있다.
- 영향받은 row 수로 성공 여부를 판단할 수 있다.

주의점
- 같은 이벤트 row에 갱신이 집중되므로 DB 내부의 row lock 대기가 발생할 수 있다.
- INSERT와 반드시 같은 트랜잭션으로 묶어야 한다.
```

### 12.2 비관적 락

```sql
SELECT *
FROM coupon_event
WHERE id = :eventId
FOR UPDATE;
```

이벤트 row를 먼저 잠근 뒤 수량을 확인하고 발급한다.

```
장점
- 처리 순서와 정합성을 이해하기 쉽다.
- 락을 얻은 트랜잭션이 안전하게 수량을 확인하고 변경할 수 있다.

단점
- 요청이 몰리면 락 대기가 길어진다.
- 트랜잭션이 길어지면 DB 처리량이 크게 감소할 수 있다.
```

### 12.3 낙관적 락

```sql
UPDATE coupon_event
SET issued_count = issued_count + 1,
    version = version + 1
WHERE id = :eventId
  AND version = :oldVersion
  AND issued_count < total_quantity;
```

처음부터 명시적인 락을 잡지 않고, UPDATE 시 읽었던 `version`과 현재 `version`이 같은지 확인한다.

```
장점
- 충돌이 적은 환경에서는 긴 락 대기를 줄일 수 있다.

단점
- 충돌하면 UPDATE가 실패하므로 재시도가 필요하다.
- 선착순 이벤트처럼 하나의 row에 요청이 집중되면 충돌과 재시도가 많아질 수 있다.
```

세 방식과 `UNIQUE`의 역할은 서로 다르다.

| 방식 | 역할 |
| --- | --- |
| 조건부 UPDATE | 남은 수량 확인과 증가를 원자적으로 수행한다. |
| 비관적 락 | 같은 row를 수정하는 트랜잭션을 명시적으로 대기시킨다. |
| 낙관적 락 | version이 달라졌는지 확인해 동시 수정 충돌을 감지한다. |
| UNIQUE 제약 | 같은 사용자의 중복 발급을 막는다. |

이번 구조에서는 Redis에서 대부분의 요청을 먼저 걸러내고, DB에서는 다음 조합으로 마지막 방어선을 만든다.

```
조건부 UPDATE + UNIQUE 제약 + Transaction
```

---

## 13. Redis와 DB가 일시적으로 다를 수 있는 이유

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/2c66ff58-d34a-47b8-9ad6-3a98cd9f47bd" />

Redis와 DB는 서로 다른 저장소이므로 상태가 일시적으로 다를 수 있다.

### Redis 통과 후 Kafka 발행 실패

```
Redis: 사용자가 통과 사용자 Set에 존재
Kafka: 발행 실패로 레코드 없음
DB: 발급 기록 없음
```

### Kafka 발행 후 DB 저장 실패

```
Redis: 통과 기록 존재
Kafka: 발행된 레코드 존재, Consumer Group Offset 미커밋
DB: 발급 기록 없음
```

### DB 저장 후 Redis 데이터 유실

```
Redis: 통과 기록 없음
Kafka: 발행된 레코드 존재, Consumer Group Offset은 처리 시점에 따라 커밋 또는 미커밋
DB: 발급 기록 존재
```

Kafka 레코드는 소비 후 바로 삭제되지 않고 보존 정책에 따라 유지된다. 따라서 처리 여부는 레코드의 존재 여부가 아니라 해당 Consumer Group의 Offset 커밋 여부로 구분해야 한다.

최종 발급 여부를 판단할 때는 DB의 `coupon_issue` 기록을 기준으로 한다.

```
Redis = 빠른 선착순 판정을 위한 상태
Kafka = 발급 요청 레코드와 Consumer Group Offset을 관리하는 메시지 브로커
DB    = 최종 발급 결과의 기준 데이터
```

Redis 판정과 Kafka 발행 사이의 원자성, 실패 보상, 재처리 정책은 별도의 설계가 필요하다. 이번 주차에서는 Kafka를 통해 Consumer까지 도달한 요청을 DB에 중복 없이 저장하는 문제에 집중한다.

---

## 14. 4주차 이후에 다룰 문제

이번 주차에서는 DB 발급 기록이 안전하게 커밋되는 지점까지 다룬다.

```
5주차
- requestId 기반 멱등성
- 동일 요청의 상태와 결과 관리

6주차
- Outbox Pattern
- DB 저장과 후속 이벤트 발행 사이의 정합성

7주차
- EDA 기반 서비스 분리
- 알림, 마이페이지, 통계 서비스의 이벤트 구독

8주차
- Retry와 DLQ
- 실패 메시지 및 Redis-DB 불일치 복구
```

특히 다음 두 문제는 구분해야 한다.

```
Consumer가 발급 기록을 DB에 정확히 저장하는 문제
→ 4주차의 범위

DB 저장 후 CouponIssued 이벤트를 다른 서비스에 안전하게 전달하는 문제
→ 6주차 Outbox Pattern의 범위
```

---
