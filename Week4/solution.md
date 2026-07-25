# **4주차 과제 해설**

# **DB 테이블 설계와 최종 정합성 보장 구조 설계**

---

## **이번 주차의 핵심**

```
Redis와 Kafka는 요청을 거르고 전달할 뿐,
최종 정확성을 보장하지 않는다.

Redis
→ 유실, Failover, Eviction 가능

Kafka
→ 중복 전달 가능

따라서 DB는 앞단을 신뢰하지 않고
스스로 다음을 방어해야 한다.

1. 사용자 중복 발급 → UNIQUE(event_id, user_id)
2. 전체 수량 초과 → issued_count 조건부 UPDATE
3. 수량과 발급 기록의 일관성 → 하나의 Transaction
```

---

## **상태 구분**

```
Redis SUCCESS

= Redis 기준 선착순 판정을 통과했다.

Kafka PUBLISHED

= 발급 요청 메시지가 Broker에 저장되었다.

DB ISSUED

= issued_count 증가와 coupon_issue INSERT가
  하나의 트랜잭션으로 COMMIT되었다.
= 최종 쿠폰 발급 성공이다.
```

```
Redis SUCCESS ≠ 최종 발급 성공

Kafka PUBLISHED ≠ 최종 발급 성공

DB ISSUED = 최종 발급 성공
```

---

# **과제 1. DB 최종 발급 처리 구조 그리기**

## **1-1. 전체 요청 처리 흐름 다이어그램**

```
Client
  |
  | POST /coupons/issue
  v
API Server
  |
  | Lua Script 실행
  v
Redis
  |
  +-- DUPLICATE → 409 응답, Kafka 발행 없음
  |
  +-- SOLD_OUT  → 409 응답, Kafka 발행 없음
  |
  +-- SUCCESS
        |
        | Kafka 발행
        v
coupon.issue.requested Topic
        |
        v
Coupon Issue Consumer
        |
        | [DB Transaction 시작]
        |
        | 1. coupon_event 조건부 UPDATE
        |    issued_count = issued_count + 1
        |
        | 2. coupon_issue INSERT
        |
        | [COMMIT]
        v
      DB ISSUED
        |
        v
   Kafka Offset Commit
```

---

## **1-2. Redis 판정 통과가 최종 발급 성공이 아닌 이유**

Redis SUCCESS 이후에도 다음 과정이 남아 있다.

```
Kafka 발행
→ Consumer 처리
→ DB 수량 확보
→ 발급 기록 저장
→ COMMIT
```

이 중 하나라도 실패하면 발급은 완료되지 않는다.

```
- Kafka 발행 실패
- Consumer 장애
- DB 기준 SOLD_OUT
- 트랜잭션 Rollback
```

또한 Redis 자체가 유실될 수 있으므로
Redis 통과 기록은 최종 원장이 될 수 없다.

---

## **1-3. Kafka 메시지 발행 성공이 최종 발급 성공이 아닌 이유**

Kafka PUBLISHED는 다음만 의미한다.

```
발급 요청 메시지가 Broker에 저장되었다.
```

Kafka는 다음을 모른다.

```
- 쿠폰이 몇 개 남았는가
- 이 사용자가 이미 발급받았는가
- Consumer가 처리에 성공할 것인가
```

Consumer가 메시지를 읽은 뒤
DB에서 최종 SOLD_OUT이 되거나
트랜잭션이 실패할 수 있다.

---

## **1-4. 최종 발급 성공으로 판단할 수 있는 시점**

```
issued_count 증가

+

coupon_issue INSERT

이 두 작업이 하나의 트랜잭션으로
COMMIT된 시점
```

COMMIT 이전에는 언제든 Rollback될 수 있으므로
어떤 중간 상태도 최종 성공이 아니다.

---

## **1-5. Redis, Kafka, DB 단계의 상태 차이**

| **단계** | **의미** | **최종 발급 성공 여부** |
| --- | --- | --- |
| Redis SUCCESS | 선착순 판정 통과, 자리 확보 | 아니다 |
| Kafka PUBLISHED | 발급 요청 메시지가 Broker에 저장됨 | 아니다 |
| DB ISSUED | 수량 확보 + 발급 기록이 트랜잭션으로 COMMIT됨 | 최종 발급 성공 |

---

# **과제 2. DB가 보장해야 하는 불변식 정의하기**

## **2-1. 불변식이 필요한 이유**

불변식은 시스템이 어떤 장애·동시성 상황에서도
깨지면 안 되는 조건이다.

```
Redis 유실, Kafka 중복 전달, Consumer 재시작,
애플리케이션 버그가 발생해도

"한 명당 한 장"과 "총 1,000장"이라는
비즈니스 약속은 지켜져야 한다.
```

애플리케이션 코드는 경로가 여러 개이고 버그가 있을 수 있다.

DB 제약과 트랜잭션은 모든 경로가 반드시 통과하는
마지막 방어선에서 불변식을 강제한다.

---

## **2-2. 각 불변식이 깨졌을 때 발생하는 문제**

| **불변식** | **깨졌을 때 발생하는 문제** |
| --- | --- |
| 사용자별 최대 1회 발급 | 한 사용자가 쿠폰을 여러 장 확보. 다른 사용자의 기회 박탈, 비용 손실, 정산 오류 |
| 전체 수량 이하 발급 | 1,000개 예산 이벤트에서 1,001개 이상 발급. 마케팅 예산 초과, 재고 정합성 붕괴 |
| 수량과 발급 기록의 동시 성공·실패 | issued_count와 실제 발급 기록 수가 불일치. 실제보다 일찍 품절되거나, 기록 없는 수량 증가 발생 |

---

## **2-3. 각 불변식을 보호하는 DB 장치**

| **불변식** | **DB 방어 장치** |
| --- | --- |
| 사용자 중복 발급 방지 | `UNIQUE(event_id, user_id)` |
| 전체 수량 초과 방지 | `issued_count < total_quantity` 조건부 UPDATE (+ CHECK 제약) |
| 두 테이블의 일관성 | 두 작업을 하나의 DB Transaction으로 묶기 |

---

## **2-4. 애플리케이션에서 중복 여부를 먼저 조회하는 것만으로 불변식을 보장할 수 없는 이유**

SELECT와 INSERT는 서로 다른 명령이다.

```
Transaction A: SELECT → 기록 없음
Transaction B: SELECT → 기록 없음
Transaction A: INSERT
Transaction B: INSERT
```

두 조회 시점에는 둘 다 기록이 없었으므로
둘 다 “발급 가능”으로 판단한다.

이는 3주차의 SCARD → SADD와 같은
Check-Then-Act Race Condition이다.

```
확인한 순간의 상태
≠
행동하는 순간의 상태
```

동시 요청을 원천 차단하려면
DB가 원자적으로 강제하는 UNIQUE 제약이 필요하다.

---

# **과제 3. coupon_event와 coupon_issue 테이블 설계하기**

## **3-1. coupon_event 테이블의 역할**

```
쿠폰 이벤트의 기준 정보와 재고 상태를 저장한다.

- 이벤트 이름, 기간, 상태
- 전체 수량 total_quantity
- 현재 발급 수량 issued_count

전체 수량 초과 방지의 기준이 되는 테이블이다.
```

---

## **3-2. coupon_issue 테이블의 역할**

```
최종 발급에 성공한 기록을 사용자 단위로 저장한다.

- 누가(user_id)
- 어떤 이벤트에서(event_id)
- 언제(issued_at) 발급받았는가

사용자 중복 발급 방지의 기준이 되는
최종 발급 원장이다.
```

---

## **3-3. 두 테이블의 관계 다이어그램**

```
coupon_event (1)
   |
   | 하나의 이벤트는 여러 발급 기록을 가진다
   |
   v
coupon_issue (N)

coupon_event
+----------------+
| id (PK)        |
| name           |
| total_quantity |
| issued_count   |
| start_at       |
| end_at         |
| status         |
| version        |
+----------------+
        ^
        | FK (event_id)
        |
coupon_issue
+---------------------------+
| id (PK)                   |
| event_id (FK)             |
| user_id                   |
| status                    |
| issued_at                 |
| UNIQUE(event_id, user_id) |
+---------------------------+
```

---

## **3-4. coupon_event 테이블 DDL**

```sql
CREATE TABLE coupon_event (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name           VARCHAR(100) NOT NULL,
    total_quantity INTEGER      NOT NULL,
    issued_count   INTEGER      NOT NULL DEFAULT 0,
    start_at       TIMESTAMPTZ  NOT NULL,
    end_at         TIMESTAMPTZ  NOT NULL,
    status         VARCHAR(20)  NOT NULL DEFAULT 'READY',
    version        BIGINT       NOT NULL DEFAULT 0,

    -- 수량 불변식: 0 <= issued_count <= total_quantity
    CONSTRAINT chk_coupon_event_quantity
        CHECK (
            total_quantity > 0
            AND issued_count >= 0
            AND issued_count <= total_quantity
        ),

    -- 이벤트 기간 불변식
    CONSTRAINT chk_coupon_event_period
        CHECK (start_at < end_at)
);
```

CHECK 제약은 조건부 UPDATE가 누락된 우회 경로가 있어도
`issued_count`가 범위를 벗어나는 것을 DB 차원에서 막는
이중 방어선이다.

---

## **3-5. coupon_issue 테이블 DDL**

```sql
CREATE TABLE coupon_issue (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id  BIGINT      NOT NULL,
    user_id   BIGINT      NOT NULL,
    status    VARCHAR(20) NOT NULL DEFAULT 'ISSUED',
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fk_coupon_issue_event
        FOREIGN KEY (event_id) REFERENCES coupon_event (id),

    -- 사용자 중복 발급 방지의 핵심 제약
    CONSTRAINT uq_coupon_issue_event_user
        UNIQUE (event_id, user_id)
);
```

이번 주차 전제에 따라 `status`는 `ISSUED`만 사용한다.

`UNIQUE(event_id, user_id)`는 인덱스를 함께 생성하므로
“이 사용자가 이미 발급받았는가” 조회에도 사용된다.

---

## **3-6. issued_count를 별도로 저장하는 이유와 단점**

```
이유

1. 매 발급마다 COUNT(*)를 실행하면
   발급 수가 늘수록 비용이 커진다.

2. 조건부 UPDATE 한 문장으로
   "확인 + 증가"를 원자적으로 처리할 수 있다.

3. 남은 수량 조회가 O(1)이다.
```

```
단점

1. 같은 사실을 두 곳에 저장하는 비정규화다.
   coupon_issue 행 수와 issued_count가
   어긋나지 않도록 트랜잭션으로 강제해야 한다.

2. 모든 발급이 이벤트 한 행을 갱신하므로
   해당 행이 Hot Row가 되어 Lock 경합이 생긴다.

3. 운영 중 정합성 검증 쿼리
   (COUNT(*) vs issued_count)가 필요할 수 있다.
```

---

# **과제 4. UNIQUE 제약의 역할과 한계 분석하기**

## **4-1. UNIQUE(event_id, user_id)가 의미하는 규칙**

```
같은 이벤트(event_id)에서
같은 사용자(user_id)의 발급 기록은
최대 한 건만 존재할 수 있다.

= 한 사용자는 하나의 이벤트에서
  쿠폰을 최대 한 번만 발급받는다.

단, 다른 이벤트에서는 같은 사용자도 발급 가능하다.
```

---

## **4-2. INSERT 성공 여부**

기존 데이터: `event_id=100, user_id=10`

| **새로운 INSERT** | **성공 또는 실패** | **이유** |
| --- | --- | --- |
| event_id=100, user_id=10 | 실패 | (100, 10) 조합이 이미 존재하므로 UNIQUE 위반 |
| event_id=100, user_id=11 | 성공 | (100, 11)은 새로운 조합 |
| event_id=101, user_id=10 | 성공 | 이벤트가 다르므로 (101, 10)은 새로운 조합 |

---

## **4-3. SELECT 후 INSERT 방식의 Race Condition**

```
시간   Transaction A                Transaction B
------------------------------------------------------------
T1     SELECT (100, 10)
       → 기록 없음

T2                                  SELECT (100, 10)
                                    → 기록 없음

T3     "발급 가능" 판단

T4                                  "발급 가능" 판단

T5     INSERT (100, 10)
       → 성공

T6                                  INSERT (100, 10)
                                    → UNIQUE 없으면 성공
                                    → 중복 발급 발생
```

UNIQUE 제약이 없다면 T6에서 중복 기록이 저장된다.

UNIQUE 제약이 있다면 T6에서 위반 오류가 발생해
DB가 중복을 원천 차단한다.

애플리케이션 조회는 “참고”일 뿐이고,
최종 강제는 DB 제약이 담당해야 한다.

---

## **4-4. UNIQUE 제약이 전체 수량 초과를 막을 수 없는 이유**

```
전체 수량: 1,000개
서로 다른 사용자: 1,001명
```

1,001명의 `(event_id, user_id)` 조합은 모두 서로 다르다.

```
(100, user:1)    → UNIQUE 통과
(100, user:2)    → UNIQUE 통과
...
(100, user:1001) → UNIQUE 통과
```

UNIQUE는 “같은 조합의 중복”만 검사한다.

“전체 행 수가 1,000개 이하”라는 조건은
UNIQUE가 표현할 수 없는 규칙이다.

---

## **4-5. 각 문제를 막는 장치**

| **문제** | **DB 방어 장치** |
| --- | --- |
| 같은 사용자의 중복 발급 | `UNIQUE(event_id, user_id)` |
| 서로 다른 사용자에 의한 전체 수량 초과 | `issued_count < total_quantity` 조건부 UPDATE |

두 장치는 목적이 다르므로 하나로 다른 하나를 대체할 수 없다.

---

# **과제 5. 조건부 UPDATE로 전체 수량 초과 방지하기**

## **5-1. 조건부 UPDATE SQL**

```sql
UPDATE coupon_event
SET issued_count = issued_count + 1,
    version = version + 1
WHERE id = :eventId
  AND issued_count < total_quantity;
```

“수량 확인”과 “수량 증가”가 SQL 한 문장에서
원자적으로 처리된다.

---

## **5-2. 영향받은 row 수가 의미하는 것**

```
DB가 WHERE 조건을 만족하는 행을 찾아
실제로 갱신한 행의 개수

= "수량 확보에 성공했는가"에 대한
  DB의 원자적 판정 결과
```

애플리케이션은 SELECT로 다시 확인할 필요 없이
이 반환값만으로 분기하면 된다.

---

## **5-3. 영향받은 row 수가 1인 경우**

```
- 이벤트가 존재하고
- issued_count < total_quantity 조건을 만족해서
- issued_count가 1 증가했다.

= DB 기준 수량 확보 성공
→ 같은 트랜잭션에서 coupon_issue INSERT 진행
```

---

## **5-4. 영향받은 row 수가 0인 경우 가능한 원인**

```
1. 이벤트는 존재하지만
   issued_count >= total_quantity
   → DB 기준 최종 SOLD_OUT

2. eventId에 해당하는 행이 존재하지 않음
   → 잘못된 요청 또는 데이터 오류

3. 이미 처리된 중복 메시지의 재처리
   → 품절 상태에서 같은 메시지가 다시 도달한 경우
     (과제 8에서 상세 분석)
```

affected rows = 0은 하나의 의미가 아니므로
후속 확인으로 원인을 구분해야 한다.

---

## **5-5. 상황별 UPDATE 결과**

| **상황** | **issued_count** | **total_quantity** | **이벤트 존재 여부** | **영향받은 row 수** | **결과** |
| --- | --- | --- | --- | --- | --- |
| A | 999 | 1,000 | 존재 | 1 | 999 < 1000 만족, 1000으로 증가. 수량 확보 성공 |
| B | 1,000 | 1,000 | 존재 | 0 | 1000 < 1000 불만족. DB 기준 SOLD_OUT |
| C | - | - | 존재하지 않음 | 0 | 갱신할 행 없음. 요청 오류로 구분해 처리 |

---

## **5-6. 두 트랜잭션이 동시에 마지막 수량을 증가시키는 상황**

```
전체 수량: 1,000
현재 issued_count: 999
격리 수준: READ COMMITTED
```

```
시간   Transaction A                  Transaction B
--------------------------------------------------------------
T1     조건부 UPDATE 실행
       → 대상 행에 Row Lock 획득
       → 999 < 1000 만족
       → issued_count = 1000

T2                                    조건부 UPDATE 실행
                                      → 같은 행 Lock 대기
                                      (Blocked)

T3     COMMIT
       → Lock 해제

T4                                    Lock 획득
                                      → PostgreSQL이 최신
                                        커밋 버전으로 조건 재평가
                                      → 1000 < 1000 불만족
                                      → affected rows = 0

T5                                    SOLD_OUT 처리
```

---

## **5-7. issued_count가 total_quantity를 초과하지 않는 이유**

```
1. UPDATE는 대상 행에 Row Lock을 잡으므로
   같은 행의 갱신은 직렬화된다.

2. READ COMMITTED에서 대기하던 UPDATE는
   앞선 트랜잭션이 커밋한 최신 값으로
   WHERE 조건을 다시 평가한다.

3. 따라서 마지막 자리를 확보한 트랜잭션 이후의
   모든 UPDATE는 조건 불만족으로 실패한다.
```

여기에 CHECK 제약이 있으므로
만약 잘못된 경로로 초과 증가가 시도되어도
DB가 오류로 거부한다.

---

# **과제 6. 발급 트랜잭션과 Rollback 설계하기**

## **6-1. 서로 다른 트랜잭션으로 처리했을 때의 문제**

| **상황** | **발생하는 데이터 불일치** |
| --- | --- |
| issued_count 증가 성공, coupon_issue INSERT 실패 | 수량은 소비되었지만 발급 기록이 없다. 실제 발급자 없이 자리가 사라져 그만큼 일찍 품절된다 |
| coupon_issue INSERT 성공, issued_count 증가 실패 | 발급 기록은 있는데 수량은 그대로다. 실제 발급 수가 total_quantity를 초과할 수 있다 |

두 경우 모두 `COUNT(coupon_issue) = issued_count`라는
정합성이 깨진다.

---

## **6-2. 하나의 트랜잭션으로 처리하는 전체 흐름**

```
[Transaction 시작]
       |
       v
조건부 UPDATE
issued_count + 1
       |
       +-- affected = 0
       |      → ROLLBACK
       |      → 품절 / 중복 구분 처리 (과제 8)
       |
       +-- affected = 1
              |
              v
      coupon_issue INSERT
              |
              +-- UNIQUE 위반
              |      → ROLLBACK
              |      → issued_count 증가도 함께 취소
              |
              +-- 성공
                     |
                     v
                  COMMIT
                     |
                     v
                 DB ISSUED
```

---

## **6-3. 하나의 트랜잭션 흐름 SQL**

```sql
BEGIN;

-- 1. 수량이 남아 있을 때만 issued_count 증가
UPDATE coupon_event
SET issued_count = issued_count + 1,
    version = version + 1
WHERE id = :eventId
  AND issued_count < total_quantity;

-- 애플리케이션에서 affected rows 확인
-- affected = 0 → ROLLBACK 후 품절/중복 구분 처리
-- affected = 1 → 계속 진행

-- 2. 발급 기록 INSERT
INSERT INTO coupon_issue (event_id, user_id, status, issued_at)
VALUES (:eventId, :userId, 'ISSUED', now());

-- 두 작업이 모두 성공하면
COMMIT;
```

---

## **6-4. UPDATE 성공 후 INSERT에서 UNIQUE 위반이 발생한 경우**

```
현재 트랜잭션

PostgreSQL에서 오류가 발생한 트랜잭션은
aborted 상태가 된다.
ROLLBACK으로 종료해야 한다.
```

```
앞에서 증가한 issued_count

같은 트랜잭션 안의 변경이므로
ROLLBACK과 함께 자동으로 취소된다.
수량이 잘못 소비되지 않는다.
```

```
기존 발급 기록 조회 시점

aborted 상태의 트랜잭션에서는 조회할 수 없다.
ROLLBACK 이후 새 트랜잭션에서
(event_id, user_id)로 기존 기록을 조회해
이미 발급된 사용자로 처리한다.
```

이 UNIQUE 위반은 대부분
“이미 발급된 사용자의 중복 메시지”를 의미하므로
장애가 아니라 멱등 처리 대상으로 다룬다.

---

## **6-5. PostgreSQL에서 제약 위반 후 같은 트랜잭션을 계속 사용할 수 없는 이유**

```
PostgreSQL은 트랜잭션 안에서 오류가 발생하면
해당 트랜잭션을 aborted 상태로 전환한다.

이후의 모든 명령은

ERROR: current transaction is aborted,
commands ignored until end of transaction block

으로 거부된다.
```

트랜잭션의 원자성을 지키기 위한 동작이다.

오류가 난 트랜잭션의 일부만 살리는 것을 허용하지 않는다.

미리 SAVEPOINT를 설정한 경우에만
해당 지점으로 부분 롤백 후 계속 진행할 수 있다.

---

## **6-6. 모든 발급 경로가 같은 트랜잭션을 사용해야 하는 이유**

```
발급 경로는 하나가 아닐 수 있다.

- Kafka Consumer 정상 처리
- 복구 배치 재처리
- 운영자 수동 발급
- 관리자 도구
```

어느 한 경로라도 UPDATE와 INSERT를
분리해서 실행하면 그 경로에서 불변식 3이 깨진다.

따라서 발급 로직을 하나의 트랜잭션 단위로 캡슐화하고
모든 경로가 같은 단위를 호출해야 한다.

---

# **과제 7. Kafka 중복 소비와 DB 멱등 처리 설계하기**

## **7-1. 중복 소비가 발생하는 전체 흐름**

```
Kafka Topic
  |
  | 1. 메시지 (event:100, user:10) 소비
  v
Consumer
  |
  | 2. DB 트랜잭션 COMMIT
  |    → coupon_issue에 (100, 10) 저장 완료
  v
DB ISSUED
  |
  | 3. Offset 반영 전 Consumer 종료
  X
  |
  | 4. Consumer 재시작
  |    → 마지막 Commit된 Offset부터 다시 읽음
  v
같은 메시지 (event:100, user:10) 재소비
  |
  v
같은 발급 처리가 두 번째로 실행됨
```

DB 처리와 Offset Commit은 서로 다른 시스템의 작업이므로
원자적으로 묶을 수 없다.

따라서 “최소 한 번 전달(at-least-once)”을 전제로
중복 처리를 설계해야 한다.

---

## **7-2. DB에 UNIQUE 제약이 없다면**

```
재처리된 메시지가 그대로 다시 발급 흐름을 타면

- issued_count가 한 번 더 증가
- coupon_issue에 (100, 10) 두 번째 행 저장

한 사용자가 쿠폰 2장을 갖게 되고
불변식 1이 깨진다.

수량도 실제 사용자 수보다 많이 소비되어
다른 사용자의 자리를 잠식한다.
```

---

## **7-3. UNIQUE 제약이 중복 소비를 방어하는 과정**

```
재처리 흐름:

조건부 UPDATE
→ (수량이 남아 있다면) affected = 1

coupon_issue INSERT (100, 10)
→ 기존 기록과 충돌
→ UNIQUE 위반 오류

트랜잭션 ROLLBACK
→ 방금 증가한 issued_count도 함께 취소

최종 상태:
발급 기록 1건, 수량 소비 1회로 유지
```

애플리케이션이 중복을 놓쳐도
DB가 마지막에 반드시 막는다.

---

## **7-4. UNIQUE 위반을 “이미 처리된 메시지”로 판단하기 위한 조건**

```
1. coupon_issue에는 최종 성공 기록만 저장된다는
   전제가 지켜지고 있다.

2. 위반된 제약이 uq_coupon_issue_event_user이다.
   (다른 제약 위반과 구분해야 한다)

3. ROLLBACK 후 조회한 기존 기록이
   같은 (event_id, user_id)의 ISSUED 상태다.
```

이 조건이 확인되면 장애가 아니라
“이전 처리가 이미 성공한 메시지”이므로
성공으로 간주하고 넘어간다.

같은 requestId인지까지 구분하는 것은
5주차 멱등성 설계에서 다룬다.

---

## **7-5. 제약 위반 후 Consumer가 수행해야 하는 처리 순서**

```
1. 현재 트랜잭션을 ROLLBACK한다.

2. 새 트랜잭션에서 (event_id, user_id)로
   기존 발급 기록을 조회한다.

3. ISSUED 기록이 존재하면
   멱등 성공으로 처리하고 로그를 남긴다.

4. Kafka Offset을 Commit해서
   같은 메시지를 다시 읽지 않게 한다.
```

---

## **7-6. DB 처리보다 Kafka Offset이 먼저 반영되면 안 되는 이유**

```
Offset 먼저 반영
→ DB 처리 전에 Consumer 장애 발생
→ 재시작한 Consumer는 다음 메시지부터 읽음
→ 해당 발급 요청이 영원히 처리되지 않음

= 메시지 유실 (at-most-once)
```

```
DB 처리 후 Offset 반영
→ 최악의 경우 같은 메시지 재처리 (at-least-once)
→ UNIQUE + 멱등 처리로 방어 가능
```

유실은 복구할 수 없지만 중복은 방어할 수 있다.

따라서 “처리 완료 후 Offset Commit”을 선택하고
중복을 감당하는 구조로 설계한다.

---

## **7-7. UNIQUE(event_id, user_id)와 requestId 기반 멱등성의 차이**

```
UNIQUE(event_id, user_id)

기준: 사용자
질문: 이 사용자가 이 이벤트에서
      이미 최종 발급받았는가?
막는 것: 결과(발급 기록)의 중복
```

```
requestId 기반 멱등성

기준: 요청
질문: 이 요청을 이미 처리했는가?
막는 것: 처리 과정의 중복
         (같은 요청의 재실행, 상태 추적, 결과 재응답)
```

UNIQUE는 “발급 기록 최대 1건”이라는 결과만 보장한다.

같은 요청이 몇 번째 재처리인지,
그 요청에 어떤 응답을 돌려줘야 하는지는 모른다.

이를 다루는 것이 5주차의 requestId 멱등성이다.

---

# **과제 8. 품절 후 중복 메시지 재처리 분석하기**

기준 상황은 다음과 같다.

```
이벤트 ID: 100
전체 수량: 1개
현재 issued_count: 1
coupon_issue에 (100, 10) 기록 존재

user:10의 Kafka 메시지가 다시 전달됨
```

## **8-1. 조건부 UPDATE 결과와 이유**

```
UPDATE ... WHERE id = 100
  AND issued_count < total_quantity

→ 1 < 1 불만족
→ affected rows = 0
```

품절 상태이므로 수량 확보 단계에서 먼저 걸린다.

---

## **8-2. coupon_issue INSERT까지 실행되는가**

```
실행되지 않는다.

affected = 0이면 발급 흐름을 중단하므로
INSERT 단계에 도달하지 않는다.
```

---

## **8-3. 이 상황에서 UNIQUE 제약 위반이 발생하는가**

```
발생하지 않는다.

UNIQUE 위반은 INSERT를 시도해야 발생한다.
이 흐름에서는 INSERT 자체가 실행되지 않는다.
```

즉 중복 메시지라고 해서
항상 UNIQUE 위반으로 감지되는 것은 아니다.

품절 이후의 중복 메시지는
affected = 0 경로로 들어온다.

---

## **8-4. affected = 0을 무조건 품절로 판단하면 안 되는 이유**

affected = 0에는 서로 다른 상황이 섞여 있다.

```
상황 1. 아직 발급받지 못한 사용자의 요청
        → 진짜 SOLD_OUT

상황 2. 이미 발급받은 사용자의 중복 메시지
        → 사실은 발급 성공한 사용자
```

상황 2를 품절로 처리하면
실제로 쿠폰을 받은 사용자에게
“품절되어 발급 실패했습니다”라는
잘못된 결과를 기록하게 된다.

---

## **8-5. 기존 coupon_issue 기록을 추가로 확인해야 하는 이유**

두 상황을 구분할 수 있는 정보는
coupon_issue의 기존 기록뿐이다.

```
(event_id, user_id) 기록 존재
→ 이미 성공한 발급의 재처리

기록 없음
→ 최종 SOLD_OUT
```

이 확인이 있어야 중복 메시지를
멱등 성공으로 올바르게 종료할 수 있다.

---

## **8-6. 상황 A와 상황 B의 처리 흐름**

```
affected = 0
       |
       v
coupon_issue에서
(event_id, user_id) 조회
       |
       +-- 상황 A. ISSUED 기록 존재
       |
       |      이미 처리 완료된 중복 메시지
       |      → 멱등 성공 처리
       |      → 기존 발급 결과 유지
       |      → Offset Commit
       |
       +-- 상황 B. 기록 없음

              DB 기준 최종 SOLD_OUT
              → FAILED(SOLD_OUT) 확정
              → Redis 자리는 반납하지 않음
                (DB에 남은 쿠폰이 없으므로)
              → Offset Commit
```

---

## **8-7. 전체 Consumer 처리 흐름**

```
Kafka 메시지 소비
       |
       v
[Transaction 시작]
       |
       v
조건부 UPDATE
       |
       +-- affected = 1
       |        |
       |        v
       |   coupon_issue INSERT
       |        |
       |        +-- 성공
       |        |      → COMMIT
       |        |      → DB ISSUED
       |        |      → Offset Commit
       |        |
       |        +-- UNIQUE 위반
       |               → ROLLBACK
       |               → 기존 기록 조회
       |               → 멱등 성공 처리
       |               → Offset Commit
       |
       +-- affected = 0
                |
                → ROLLBACK
                |
                v
           기존 기록 조회
                |
                +-- 기록 존재
                |      → 멱등 성공 처리
                |      → Offset Commit
                |
                +-- 기록 없음
                       → 최종 SOLD_OUT 확정
                       → Offset Commit
```

---

# **과제 9. DB 동시성 제어 방식 비교하기**

## **9-1. 조건부 UPDATE 방식**

```
UPDATE 문장의 WHERE 절에 비즈니스 조건을 넣어
"확인과 갱신"을 한 문장으로 원자화한다.

affected rows로 성공 여부를 판정한다.

별도의 SELECT나 명시적 Lock 획득 코드가 없다.
Lock은 UPDATE가 실행되는 순간
DB가 자동으로 Row Lock을 잡는 방식으로 동작한다.
```

---

## **9-2. 비관적 락 방식**

```sql
BEGIN;

SELECT issued_count, total_quantity
FROM coupon_event
WHERE id = :eventId
FOR UPDATE;

-- 애플리케이션에서 issued_count < total_quantity 확인

UPDATE coupon_event
SET issued_count = issued_count + 1
WHERE id = :eventId;

INSERT INTO coupon_issue (event_id, user_id, status)
VALUES (:eventId, :userId, 'ISSUED');

COMMIT;
```

설명:

```
"충돌은 반드시 일어난다"고 가정하고
읽는 시점부터 FOR UPDATE로 Row Lock을 잡는다.

Lock을 잡은 트랜잭션이 끝날 때까지
다른 트랜잭션은 같은 행에서 대기한다.

확인과 갱신 사이에 끼어들 수 없어 안전하지만,
Lock 보유 시간이 길어질수록 대기가 누적된다.
```

---

## **9-3. 낙관적 락 방식**

```sql
-- 1. 조회 (Lock 없음)
SELECT issued_count, total_quantity, version
FROM coupon_event
WHERE id = :eventId;

-- 2. 애플리케이션에서 수량 확인 후 갱신 시도
UPDATE coupon_event
SET issued_count = issued_count + 1,
    version = version + 1
WHERE id = :eventId
  AND version = :readVersion;

-- affected = 0이면 다른 트랜잭션이 먼저 갱신한 것
-- → 재조회 후 재시도
```

설명:

```
"충돌은 드물다"고 가정하고 Lock 없이 진행한 뒤,
커밋 시점에 version으로 충돌을 감지한다.

version이 바뀌었으면 갱신이 실패하고
애플리케이션이 재시도해야 한다.

충돌이 드문 환경에서는 효율적이지만,
선착순처럼 한 행에 충돌이 집중되면
대부분의 요청이 실패와 재시도를 반복한다.
```

---

## **9-4. 세 방식 비교표**

| **구분** | **조건부 UPDATE** | **비관적 락** | **낙관적 락** |
| --- | --- | --- | --- |
| 수량을 보호하는 방법 | WHERE 조건 + 원자적 UPDATE | SELECT FOR UPDATE로 선점 | version 비교로 충돌 감지 |
| 충돌 시 동작 | affected = 0으로 즉시 실패 판정 | Lock 대기 후 순차 실행 | UPDATE 실패 후 재시도 |
| 재시도 필요 여부 | 불필요 (0이면 품절/중복 처리) | 불필요 (대기로 해결) | 필요 (충돌마다 재시도) |
| 장점 | 왕복 1회, 단순, Lock 보유 시간 최소 | 로직이 직관적, 복잡한 검증에 유리 | 충돌이 드물면 Lock 비용 없음 |
| 단점 | 복잡한 다단계 검증에는 한계 | Lock 대기 누적, 커넥션 점유 | 충돌 집중 시 재시도 폭증 |
| 선착순 이벤트 적합성 | 높음 | 중간 (동시성 높으면 병목) | 낮음 (충돌이 필연적) |

---

## **9-5. 하나의 이벤트 row에 요청이 집중될 때 발생하는 문제**

```
모든 발급이 coupon_event의 같은 행을 갱신한다.
→ Hot Row

- Row Lock 경합으로 트랜잭션 대기 증가
- 대기 중 DB Connection 점유
- Connection Pool 고갈
- 처리량 저하와 응답 지연
- 낙관적 락이라면 재시도 폭증
```

이것이 3주차에서 Redis로
실패할 요청을 앞단에서 차단한 이유다.

DB에 도달하는 요청 수 자체를
성공 가능한 수준으로 줄여야 한다.

---

## **9-6. 이번 구조에서 조건부 UPDATE를 선택하는 이유**

```
1. Redis가 이미 대부분의 요청을 걸렀으므로
   DB 도달 요청은 성공 가능성이 높은 소수다.

2. 필요한 검증이 "수량이 남았는가" 하나라서
   WHERE 절 하나로 표현 가능하다.

3. SELECT 없이 왕복 1회로 끝나
   Lock 보유 시간이 가장 짧다.

4. affected rows로 결과 판정이 명확하다.

5. 재시도 로직이 필요 없다.
```

---

## **9-7. 세 방식과 UNIQUE 제약의 역할이 다른 이유**

```
조건부 UPDATE / 비관적 락 / 낙관적 락

→ 같은 행(coupon_event)에 대한
  "동시 갱신"을 제어하는 방법
→ 전체 수량 초과 방지가 목적
```

```
UNIQUE(event_id, user_id)

→ 다른 테이블(coupon_issue)에서
  "같은 조합의 중복 저장"을 막는 데이터 제약
→ 사용자 중복 발급 방지가 목적
```

동시성 제어 방식은 서로 대체 가능한 선택지지만,
UNIQUE는 어떤 방식을 선택하든
항상 함께 있어야 하는 별도의 방어선이다.

---

# **과제 10. Redis, Kafka, DB 불일치 상황과 이후 주차 연결하기**

## **10-1. Redis 통과 후 Kafka 발행 실패**

| **시스템** | **남아 있는 상태** |
| --- | --- |
| Redis | issued-users에 userId 존재 (자리 소비됨) |
| Kafka | 메시지 없음 |
| DB | 발급 기록 없음 |

3주차의 유령 통과자 상황이다.

requestId 소유권 확인 후 보상(SREM + HDEL)하거나
복구 배치로 재발행해야 한다.

---

## **10-2. Kafka 발행 후 DB 저장 실패**

| **시스템** | **남아 있는 상태** |
| --- | --- |
| Redis | issued-users에 userId 존재 |
| Kafka | 메시지 존재 (Offset 미반영 시 재처리 대상) |
| DB | 발급 기록 없음 |

일시적 실패라면 Consumer 재처리로 수렴한다.

최종 실패(DB SOLD_OUT)라면 FAILED로 확정하고
Redis 자리는 반납하지 않는다.

---

## **10-3. DB 저장 후 Redis 데이터 유실**

| **시스템** | **남아 있는 상태** |
| --- | --- |
| Redis | 통과 기록 유실 (Set이 비었거나 일부 사라짐) |
| DB | 발급 기록과 issued_count 정상 존재 |

이미 발급받은 사용자가 Redis를 다시 통과할 수 있다.

하지만 DB의 UNIQUE와 조건부 UPDATE가
중복 발급과 수량 초과를 최종적으로 막는다.

이 상황이 “DB 방어선이 반드시 필요한 이유”의
가장 직접적인 근거다.

---

## **10-4. 최종 발급 여부를 DB 기준으로 판단해야 하는 이유**

```
1. Redis는 유실·Failover·Eviction이 가능한
   판정용 상태 저장소다.

2. Kafka는 "요청이 전달되었다"는 사실만 안다.

3. DB만이 트랜잭션과 제약 조건으로
   불변식을 강제하는 내구성 있는 원장이다.
```

사용자 안내, 정산, 통계 등
비즈니스적으로 의미 있는 최종 수치는
모두 DB의 ISSUED 기록을 기준으로 한다.

---

## **10-5. Redis, Kafka, DB의 역할 비교표**

| **구분** | **Redis** | **Kafka** | **DB** |
| --- | --- | --- | --- |
| 주 역할 | 빠른 선착순 1차 판정 | 비동기 메시지 전달과 완충 | 최종 발급 기록과 정합성 방어 |
| 저장하는 상태 | 통과 사용자, 판정 기준 | 발급 요청 메시지 | 이벤트 재고, 발급 원장 |
| 성공이 의미하는 것 | 선착순 자리 확보 | 요청 접수 | 최종 발급 완료 |
| 강점 | O(1) 연산, Lua 원자성 | 트래픽 완충, 재처리 | 트랜잭션, 제약 조건, 내구성 |
| 한계 | 유실·Eviction 가능 | 비즈니스 규칙을 모름, 중복 전달 | 대량 동시 요청 시 Lock 병목 |

---

## **10-6. 5주차 — requestId 기반 멱등성에서 해결할 문제**

```
이번 주차의 UNIQUE는
"이 사용자가 발급받았는가"만 답한다.

다음 질문에는 답하지 못한다.

- 이 요청은 몇 번째 재처리인가
- 이 요청에 어떤 응답을 돌려줘야 하는가
- 실패한 요청의 현재 상태는 무엇인가

5주차에서는 requestId 단위로 요청 상태를 저장해
API 재시도, Kafka 재처리, 복구 배치 재발행을
같은 요청으로 식별하고 멱등 처리한다.
```

---

## **10-7. 6주차 — Outbox Pattern에서 해결할 문제**

```
DB 발급 COMMIT 성공
→ coupon.issued 결과 이벤트 발행 실패

DB는 발급 완료인데
알림·마이페이지·통계는 발급 사실을 모른다.

6주차에서는 발급 기록과 outbox_event를
같은 트랜잭션으로 저장하고,
별도 Publisher가 Outbox를 읽어 발행함으로써
"DB 변경과 결과 이벤트 발행"의 정합성을 맞춘다.
```

---

## **10-8. Outbox가 Redis 판정과 최초 Kafka 발행을 원자적으로 묶지 못하는 이유**

```
Outbox Pattern의 원자성은
"하나의 DB 트랜잭션" 안에서만 성립한다.

비즈니스 변경 + outbox_event INSERT
→ 같은 DB, 같은 트랜잭션이므로 원자적
```

```
Redis SADD는 DB 트랜잭션 밖의
별도 저장소 작업이다.

Redis와 RDB를 하나의 트랜잭션으로
묶는 방법은 존재하지 않는다.
```

따라서 Redis 판정과 최초 Kafka 발행 사이의 불일치는
Outbox가 아니라 보상 처리와 복구 배치로 수렴시킨다.

Outbox는 “DB 저장 이후의 결과 이벤트 발행”에 적용한다.

---

## **10-9. 8주차 — Retry와 DLQ에서 해결할 문제**

```
이번 주차에서는 DB 실패를 구분만 했다.

- 일시적 실패 (연결 오류, Deadlock)
- 멱등 성공 (이미 처리된 메시지)
- 최종 실패 (SOLD_OUT)

8주차에서는 이 구분을 처리 구조로 만든다.

일시적 실패
→ Retry Topic으로 복구 시간을 주며 재시도

반복 실패
→ DLQ로 격리 후 운영자 분석·수동 보상

최종 실패
→ 재시도 없이 상태 확정과 보상 처리
```

---
