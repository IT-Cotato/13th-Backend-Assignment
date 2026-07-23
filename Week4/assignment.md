# **4주차 과제: DB 테이블과 최종 발급 정합성 보장**

## **1. 과제 목표**

이번 과제의 목표는 **Redis와 Kafka를 통과한 쿠폰 발급 요청을 DB에 정확하게 저장하고, 사용자 중복 발급과 전체 수량 초과를 방지하는 구조를 설계하는 것**이다.

1주차에서는 요청 접수와 실제 발급 처리를 분리했다.

2주차에서는 Kafka를 이용해 쿠폰 발급 요청을 비동기적으로 전달했다.

3주차에서는 Redis를 이용해 사용자 중복 요청과 수량 초과 요청을 빠르게 차단했다.

이번 4주차에서는 Consumer가 Kafka 메시지를 읽은 뒤 DB에 최종 발급 기록을 저장하는 단계를 설계한다.

기본 흐름은 다음과 같다.

```
Client
  ↓
API Server
  ↓
Redis 선착순 / 중복 판정
  ↓
ACCEPTED인 요청만 Kafka 발행
  ↓
coupon.issue.requested Topic
  ↓
Coupon Issue Consumer
  ↓
DB Transaction
  ├─ coupon_event.issued_count 증가
  └─ coupon_issue 발급 기록 저장
```

`Redis ACCEPTED`는 3주차의 `Redis SUCCESS`와 같은 단계다. 최종 발급 성공과 구분하기 위해 이번 주차에서는 `ACCEPTED`로 표기한다.

이번 과제에서 중요하게 볼 내용은 다음과 같다.

```
Redis 판정 통과와 DB 최종 발급 성공의 차이

DB가 보장해야 하는 불변식

coupon_event와 coupon_issue 테이블 설계

UNIQUE(event_id, user_id)의 역할과 한계

조건부 UPDATE를 이용한 수량 초과 방지

영향받은 row 수의 의미와 실패 원인 구분

issued_count 증가와 coupon_issue INSERT를
하나의 트랜잭션으로 묶는 이유

Kafka 중복 소비와 DB 중복 발급 방지

품절 후 중복 메시지가 재처리되는 경우

조건부 UPDATE, 비관적 락, 낙관적 락 비교

Redis, Kafka, DB의 역할과 데이터 불일치 상황
```

이번 과제의 핵심은 다음과 같다.

```
Redis는 빠른 선착순 판정을 담당한다.

Kafka는 발급 요청 메시지를 전달한다.

DB는 최종 발급 기록을 저장하고 수량 정합성을 보장한다.
```

---

## **2. 기본 상황**

서비스에서 선착순 쿠폰 이벤트를 진행한다고 가정한다.

```
이벤트 ID: 100

이벤트 이름: 여름맞이 5,000원 할인 쿠폰 이벤트

전체 쿠폰 수량: 1,000개

발급 조건:
사용자 1명당 해당 이벤트 쿠폰 1개만 발급 가능

DBMS: PostgreSQL

최종 발급 성공 기준:
coupon_issue 저장 트랜잭션 Commit
```

Redis는 정상 상황에서 최대 1,000명의 사용자만 통과시킨다.

하지만 다음과 같은 상황이 발생할 수 있다.

```
Redis 데이터 유실 또는 초기화

Redis Replica Failover 과정에서 일부 통과 기록 유실

Kafka 메시지 재처리

DB Commit 이후 Consumer Group Offset 커밋 전 Consumer 장애

애플리케이션 버그로 같은 발급 요청이 여러 번 DB에 도달
```

따라서 Redis가 앞단에서 요청을 차단하더라도 DB에는 다음 방어 장치가 필요하다.

```
사용자 중복 발급 방지
→ UNIQUE(event_id, user_id)

전체 수량 초과 방지
→ issued_count 조건부 UPDATE

두 작업의 일관성
→ DB Transaction
```

---

## **3. 이번 과제의 전제 조건**

이번 과제에서는 다음 전제를 따른다.

```
coupon_issue에는 최종 발급에 성공한 기록만 저장한다.

coupon_issue의 status는 ISSUED만 사용한다.

쿠폰 발급 취소와 재발급은 다루지 않는다.

이벤트별 전체 수량은
coupon_event.total_quantity에 저장한다.

DB에서 최종 발급된 수량은
coupon_event.issued_count에 저장한다.

모든 발급 처리는 issued_count 증가와
coupon_issue INSERT를 같은 트랜잭션에서 수행한다.

이벤트 발급 자격은 Redis 판정 시점을 기준으로 한다.
Consumer는 이벤트 상태와 기간을 다시 판정하지 않고
DB의 최종 수량과 사용자 중복 발급을 방어한다.

PostgreSQL의 기본 격리 수준인
READ COMMITTED를 기준으로 설명한다.

자동 Offset 커밋은 사용하지 않는다.
Consumer가 DB 처리 결과를 확정한 후에
Consumer Group Offset을 커밋한다고 가정한다.
```

이번 주차에서는 아래 내용의 상세 구현은 다루지 않는다.

```
requestId 기반 요청 상태 관리

Outbox Pattern

Retry Topic과 DLQ

쿠폰 발급 취소와 재발급 정책

Redis와 Kafka 사이의 원자성 보장
```

다만 이번 주차의 설계가 이후 주차의 내용과 어떻게 연결되는지는 설명해야 한다.

---

## **4. 제출 형식**

제출 파일은 아래 경로에 작성해서 PR을 생성한다.

```
submissions/Week4/기수_이름.md
```

예시는 다음과 같다.

```
submissions/Week4/13기_김기민.md
```

제출 문서에는 아래 필수 과제를 모두 포함한다.

```
과제 1. DB 최종 발급 처리 구조 그리기

과제 2. DB가 보장해야 하는 불변식 정의하기

과제 3. coupon_event와 coupon_issue 테이블 설계하기

과제 4. UNIQUE 제약의 역할과 한계 분석하기

과제 5. 조건부 UPDATE로 전체 수량 초과 방지하기

과제 6. 발급 트랜잭션과 Rollback 설계하기

과제 7. Kafka 중복 소비와 DB 중복 발급 방지 설계하기

과제 8. 품절 후 중복 메시지 재처리 분석하기

과제 9. DB 동시성 제어 방식 비교하기

과제 10. Redis, Kafka, DB 불일치 상황과
이후 주차 연결하기
```

---

# **5. 필수 과제**

---

## **과제 1. DB 최종 발급 처리 구조 그리기**

Redis 판정을 통과한 요청이 Kafka를 거쳐 DB에 최종 저장되는 전체 구조를 작성한다.

반드시 아래 구성 요소를 포함해야 한다.

```
Client

API Server

Redis

Kafka

Coupon Issue Consumer

coupon_event

coupon_issue
```

### **1. 전체 요청 처리 흐름 다이어그램**

```
Client
  |
  | 1. 쿠폰 발급 요청
  v
API Server
  |
  | 2. 이벤트 검증 및 사용자 인증
  v
Redis
  |
  | 3. 사용자 중복 및 수량 판정
  v
API Server
  |
  | 4. ACCEPTED 요청만 Kafka 발행
  v
Kafka (coupon.issue.requested)
  |
  | 5. 발급 요청 메시지 전달
  v
Coupon Issue Consumer
  |
  | 6. DB Transaction
  |      ├─ coupon_event.issued_count 증가
  |      └─ coupon_issue INSERT
  v
DB
```

### **2. Redis 판정 통과가 최종 발급 성공이 아닌 이유**

```
Redis는 선착순과 중복 여부를 빠르게 판정할 뿐이다.
Redis 통과 이후 Kafka 발행 실패, Consumer 장애, DB 저장 실패 등이 발생할 수 있으므로 Redis ACCEPTED는 최종 발급 성공을 의미하지 않는다.
```

### **3. Kafka 메시지 발행 성공이 최종 발급 성공이 아닌 이유**

```
Kafka는 발급 요청 메시지를 안전하게 전달하는 역할만 한다. 메시지가 Kafka에 저장되더라도 Consumer가 DB 저장에 실패하거나 장애가 발생하면 최종 발급은 이루어지지 않는다.
```

### **4. 최종 발급 성공으로 판단할 수 있는 시점**

```
coupon_event의 issued_count 증가와 coupon_issue INSERT가 하나의 트랜잭션으로 Commit된 시점이다.
```

### **5. Redis, Kafka, DB 단계의 상태 차이**

| **단계** | **의미** | **최종 발급 성공 여부** |
| --- | --- | --- |
| Redis ACCEPTED | Redis 기준 선착순 및 중복 판정 통과 | X |
| Kafka PUBLISHED | Kafka에 발급 요청 메시지 저장 | X |
| DB ISSUED | DB 트랜잭션 Commit 완료 | O |

---

## **과제 2. DB가 보장해야 하는 불변식 정의하기**

쿠폰 발급 시스템이 장애와 동시 요청 상황에서도 지켜야 하는 조건을 불변식으로 정의한다.

이번 과제에서는 다음 세 가지 불변식을 기준으로 작성한다.

```
불변식 1.

한 사용자는 하나의 이벤트에서
쿠폰을 최대 한 번만 발급받는다.

불변식 2.

발급 수량은 0보다 작거나
전체 수량보다 많을 수 없다.

불변식 3.

issued_count 증가와 coupon_issue 저장은
함께 성공하거나 함께 실패한다.
```

### **1. 불변식이 필요한 이유**

```
Redis나 Kafka에서 장애가 발생하거나 동시에 많은 요청이 들어오는 상황에서도 쿠폰 시스템이 항상 동일한 결과를 유지하기 위해 필요하다.

DB는 최종 발급 결과를 저장하는 마지막 방어선이므로 어떤 상황에서도 반드시 지켜야 하는 조건을 불변식으로 정의한다.
```

### **2. 각 불변식이 깨졌을 때 발생하는 문제**

| **불변식** | **깨졌을 때 발생하는 문제** |
| --- | --- |
| 사용자별 최대 1회 발급 | 같은 사용자가 여러 장의 쿠폰을 발급받는다. |
| 전체 수량 이하 발급 | 이벤트 수량보다 많은 쿠폰이 발급된다. |
| 수량과 발급 기록의 동시 성공·실패 | issued_count와 coupon_issue 데이터가 서로 달라진다. |

### **3. 각 불변식을 보호하는 DB 장치**

| **불변식** | **DB 방어 장치** |
| --- | --- |
| 사용자 중복 발급 방지 | UNIQUE(event_id, user_id) |
| 전체 수량 초과 방지 | 조건부 UPDATE, CHECK 제약 |
| 두 테이블의 일관성 | Transaction |

### **4. 애플리케이션에서 중복 여부를 먼저 조회하는 것만으로 불변식을 보장할 수 없는 이유**

```
동시에 두 개의 요청이 들어오면 두 트랜잭션 모두 아직 발급 기록이 없다고 판단할 수 있다.
이후 동시에 INSERT를 수행하면 애플리케이션의 조회만으로는 중복 발급을 막을 수 없다.
최종 중복 방지는 DB의 UNIQUE 제약이 담당해야 한다.
```

---

## **과제 3. coupon_event와 coupon_issue 테이블 설계하기**

쿠폰 이벤트와 최종 발급 기록을 저장할 테이블을 설계한다.

완전한 운영용 스키마를 만드는 것이 목적은 아니다. 이번 주차에서 필요한 불변식을 지킬 수 있도록 핵심 컬럼과 제약 조건을 작성한다.

### **1. coupon_event 테이블의 역할**

```
- 쿠폰 이벤트 정보와 DB 기준 최종 발급 수량(issued_count)을 저장한다.
- 전체 수량과 현재 발급 수량을 관리하여 최종 정합성을 보장하는 역할을 한다.
```

### **2. coupon_issue 테이블의 역할**

```
- 최종 발급에 성공한 사용자의 쿠폰 발급 기록을 저장한다.
- 동일 사용자의 중복 발급을 방지하는 기준 데이터가 된다.
```

### **3. 두 테이블의 관계를 다이어그램으로 표현하기**

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
          |
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

### **4. coupon_event 테이블 DDL 작성하기**

아래 컬럼과 제약 조건을 포함한다.

```
id

name

total_quantity

issued_count

start_at

end_at

status

version (낙관적 락을 선택할 경우)

수량 CHECK 제약

이벤트 기간 CHECK 제약
```

```sql
CREATE TABLE coupon_event (
    id BIGSERIAL PRIMARY KEY,

    name VARCHAR(100) NOT NULL,

    total_quantity INT NOT NULL,

    issued_count INT NOT NULL DEFAULT 0,

    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,

    status VARCHAR(20) NOT NULL,

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
    CHECK (
        start_at < end_at
    )
);
```

### **5. coupon_issue 테이블 DDL 작성하기**

아래 컬럼과 제약 조건을 포함한다.

```
id

event_id

user_id

status

issued_at

coupon_event를 참조하는 Foreign Key

동일 이벤트의 사용자 중복 발급을 막는 UNIQUE 제약
```

```sql
CREATE TABLE coupon_issue (
    id BIGSERIAL PRIMARY KEY,

    event_id BIGINT NOT NULL,

    user_id BIGINT NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'ISSUED',

    issued_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_coupon_issue_event
        FOREIGN KEY(event_id)
        REFERENCES coupon_event(id),

    CONSTRAINT uk_coupon_issue_event_user
        UNIQUE(event_id, user_id)
);
```

### **6. issued_count를 coupon_event에 별도로 저장하는 이유와 단점**

```
장점
- 남은 쿠폰 수량을 빠르게 확인할 수 있다.
- DB 기준 발급 현황을 즉시 조회할 수 있다.
- Redis와 DB의 발급 수량을 비교할 수 있다.
- 조건부 UPDATE를 이용해 수량 초과를 방지할 수 있다.

단점
- coupon_issue의 발급 기록과 issued_count가 서로 어긋날 수 있으므로 반드시 같은 트랜잭션에서 함께 수정해야 한다.
```

---

## **과제 4. UNIQUE 제약의 역할과 한계 분석하기**

`UNIQUE(event_id, user_id)`가 어떤 문제를 막고, 어떤 문제는 막지 못하는지 분석한다.

### **1. `UNIQUE(event_id, user_id)`가 의미하는 규칙**

```
UNIQUE(event_id, user_id)는 동일한 쿠폰 이벤트에서 동일한 사용자의 발급 기록이 두 개 이상 저장되지 않도록 제한하는 제약 조건이다.
-> 즉, 한 사용자는 하나의 이벤트에서 쿠폰을 최대 한 번만 발급받을 수 있다.

event_id와 user_id 중 하나의 값만 같다고 해서 중복으로 판단하는 것이 아니라, 두 값의 조합이 모두 동일할 때 중복으로 판단한다.

예를 들어 다음과 같다.

(event_id=100, user_id=10) → 최초 저장 가능
(event_id=100, user_id=10) → 중복 저장 불가능

(event_id=100, user_id=11) → 다른 사용자이므로 저장 가능
(event_id=101, user_id=10) → 다른 이벤트이므로 저장 가능
```

### **2. 다음 INSERT의 성공 여부와 이유**

기존 데이터는 다음과 같다.

```
event_id = 100
user_id = 10
```

| **새로운 INSERT** | **성공 또는 실패** | **이유** |
| --- | --- | --- |
| event_id=100, user_id=10 | 실패 | 기존 행과 event_id, user_id 조합이 모두 같으므로 UNIQUE 제약을 위반한다. |
| event_id=100, user_id=11 | 성공 | 이벤트는 같지만 사용자가 다르므로 새로운 조합이다. |
| event_id=101, user_id=10 | 성공 | 사용자는 같지만 이벤트가 다르므로 새로운 조합이다. |

### **3. 중복 여부를 SELECT한 뒤 INSERT하는 방식의 Race Condition**

아래 상황을 시간 순서로 표현한다.

```
Transaction A가 user:10의 발급 기록을 조회한다.

Transaction B도 user:10의 발급 기록을 조회한다.

두 트랜잭션 모두 아직 발급 기록이 없다고 판단한다.

두 트랜잭션이 동시에 INSERT를 시도한다.
```

답변:

```
시간
 |
 | Transaction A 시작
 | SELECT coupon_issue
 | WHERE event_id=100 AND user_id=10
 | → 조회 결과 없음
 |
 | Transaction B 시작
 | SELECT coupon_issue
 | WHERE event_id=100 AND user_id=10
 | → 조회 결과 없음
 |
 | Transaction A
 | “발급 기록이 없다”고 판단
 |
 | Transaction B
 | “발급 기록이 없다”고 판단
 |
 | Transaction A INSERT 시도
 | (event_id=100, user_id=10)
 |
 | Transaction B INSERT 시도
 | (event_id=100, user_id=10)
 v

두 트랜잭션이 중복 여부를 조회한 시점에는 아직 어느 트랜잭션도 발급 기록을 저장하지 않았기 때문에 두 요청 모두 “기존 발급 기록이 없다”고 판단할 수 있다.

애플리케이션에서 SELECT한 결과만 믿고 INSERT를 수행하면 두 요청이 모두 발급에 성공할 가능성이 있다.

따라서 애플리케이션의 사전 조회는 중복 여부를 빠르게 확인하거나 사용자에게 적절한 응답을 제공하기 위한 보조 수단으로 사용할 수 있지만, 최종 중복 발급 방지는 DB의 UNIQUE 제약이 담당해야 한다.

UNIQUE 제약이 적용되어 있다면 두 INSERT 중 하나만 성공하고, 나머지 하나는 제약 조건 위반으로 실패한다.
```

### **4. UNIQUE 제약이 전체 수량 초과를 막을 수 없는 이유**

아래 상황을 기준으로 설명한다.

```
전체 수량: 1,000개

서로 다른 사용자: 1,001명

모든 (event_id, user_id) 조합은 서로 다름
```

```
UNIQUE(event_id, user_id)는 같은 사용자가 같은 이벤트에서 여러 번 발급받는 것만 방지한다.
전체 수량이 1,000개이고 서로 다른 사용자 1,001명이 발급을 요청하면 모든 event_id와 user_id의 조합은 서로 다르다.

예를 들어 다음 발급 기록은 모두 UNIQUE 제약을 통과할 수 있다.

(event_id=100, user_id=1)
(event_id=100, user_id=2)
(event_id=100, user_id=3)
...
(event_id=100, user_id=1000)
(event_id=100, user_id=1001)

각 행의 user_id가 모두 다르기 때문에 UNIQUE 제약은 위반되지 않는다.
따라서 UNIQUE 제약만 사용하면 전체 수량이 1,000개임에도 1,001번째 사용자에게 쿠폰이 발급될 수 있다.
전체 수량 초과는 coupon_event의 issued_count가 total_quantity보다 작을 때만 증가하도록 하는 조건부 UPDATE를 통해 별도로 방어해야 한다.
```

### **5. 사용자 중복과 전체 수량 초과를 각각 어떤 장치로 막아야 하는가?**

| **문제** | **DB 방어 장치** |
| --- | --- |
| 같은 사용자의 중복 발급 | `UNIQUE(event_id, user_id)` |
| 서로 다른 사용자에 의한 전체 수량 초과 | `issued_count < total_quantity` 조건을 포함한 조건부 UPDATE |

---

## **과제 5. 조건부 UPDATE로 전체 수량 초과 방지하기**

쿠폰 수량이 남아 있을 때만 `issued_count`를 증가시키는 조건부 UPDATE를 설계한다.

### **1. 조건부 UPDATE SQL 작성하기**

다음 조건을 만족해야 한다.

```
event_id가 일치해야 한다.

issued_count가 total_quantity보다 작을 때만 증가해야 한다.

issued_count는 1 증가해야 한다.
```

```sql
UPDATE coupon_event
SET issued_count = issued_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :eventId
  AND issued_count < total_quantity;
```

### **2. 영향받은 row 수가 의미하는 것**

```
UPDATE 실행 후 반환되는 영향받은 row 수는 조건을 만족하여 실제로 변경된 행의 개수를 의미한다.

이번 SQL은 하나의 event_id를 대상으로 실행하므로 정상적인 결과는 1 또는 0이다.
영향받은 row 수가 1이면 -> 조건을 만족한 이벤트의 issued_count가 정상적으로 1 증가한 것이다.
영향받은 row 수가 0이면 -> WHERE 조건을 만족하는 행이 없어 수량을 확보하지 못한 것이다.
```

### **3. 영향받은 row 수가 1인 경우**

```
event_id에 해당하는 이벤트가 존재하고, issued_count가 total_quantity보다 작은 경우이다.

따라서 남은 쿠폰 수량을 성공적으로 확보했으며, 같은 트랜잭션 안에서 coupon_issue 발급 기록 INSERT를 진행할 수 있다.

처리 흐름은 다음과 같다.

조건부 UPDATE 실행
        |
        v
영향받은 row 수 = 1
        |
        v
쿠폰 수량 확보 성공
        |
        v
coupon_issue INSERT 실행
```

### **4. 영향받은 row 수가 0인 경우 가능한 원인**

과제 5-1의 SQL은 `event_id`와 수량 조건만 사용한다. 중복 메시지 여부는 UPDATE 결과가 0인 직접 원인이 아니라, 이후 기존 발급 기록을 조회해 구분한다.

```
1. event_id에 해당하는 coupon_event가 존재하지 않는 경우

2. issued_count가 이미 total_quantity와 같아서 남은 쿠폰 수량이 없는 경우 과제에서 작성한 조건부 UPDATE는 이벤트 ID와 수량 조건만 포함한다.

따라서 이벤트 상태가 READY 또는 CLOSED인 경우나 이벤트 기간이 지나간 경우는 현재 SQL의 직접적인 실패 원인이 아니다.
또한 중복 메시지 여부도 조건부 UPDATE 결과가 0이 되는 직접적인 원인이 아니다.

UPDATE 결과가 0인 이후 동일한 event_id와 user_id의 coupon_issue 기록을 추가로 조회하여, 이미 처리된 중복 메시지인지 실제 수량 소진인지 구분해야 한다.
```

### **5. 다음 상황에서 UPDATE 결과를 작성하기**

과제 5-1에서 작성한 조건부 UPDATE를 사용한다고 가정한다.

| **상황** | **issued_count** | **total_quantity** | **이벤트 존재 여부** | **영향받은 row 수** | **결과** |
| --- | --- | --- | --- | --- | --- |
| A  |          999 |          1,000 | 존재        |          1 | 조건을 만족하므로 `issued_count`가 1,000으로 증가한다.               |
| B  |        1,000 |          1,000 | 존재        |          0 | `issued_count < total_quantity` 조건을 만족하지 않아 변경되지 않는다. |
| C  |            - |              - | 존재하지 않음   |          0 | 해당 `event_id`를 가진 행이 없어 변경되지 않는다.                     |

### **6. 두 트랜잭션이 동시에 마지막 수량을 증가시키는 상황 분석**

```
전체 수량: 1,000개

현재 issued_count: 999

Transaction A와 Transaction B가
동시에 조건부 UPDATE 실행
```

두 트랜잭션의 실행 결과를 시간 순서 다이어그램으로 작성한다.

```
초기 상태
issued_count = 999
total_quantity = 1,000

Transaction A                         Transaction B
      |                                     |
      | 조건부 UPDATE 실행                     | 조건부 UPDATE 실행
      |                                     |
      | coupon_event row 갱신 락 획득          | 같은 row의 락 대기
      |                                     |
      | 999 < 1,000 조건 확인                 |
      |                                     |
      | issued_count를 1,000으로 변경          |
      |                                     |
      | Commit                              |
      |                                     |
      |                                row 락 획득
      |                                     |
      |                                최신 값 기준으로
      |                               WHERE 조건 재확인
      |                                     |
      |                              1,000 < 1,000은 거짓
      |                                     |
      |                              영향받은 row 수 = 0
      v                                     v

Transaction A
→ 영향받은 row 수 1
→ issued_count를 1,000으로 증가
→ 마지막 쿠폰 수량 확보 성공

Transaction B
→ Transaction A의 갱신이 끝날 때까지 대기
→ 최신 issued_count인 1,000을 기준으로 조건 재확인
→ issued_count < total_quantity 조건 불충족
→ 영향받은 row 수 0
→ 수량 확보 실패
```

### **7. issued_count가 total_quantity를 초과하지 않는 이유**

```
조건부 UPDATE는 수량을 조회한 뒤 애플리케이션에서 증가시키는 방식이 아니라, 수량 확인과 증가를 하나의 SQL 문으로 실행한다.

동일한 coupon_event 행에 여러 UPDATE가 동시에 요청되면 PostgreSQL은 해당 행의 갱신을 직렬화한다.

먼저 행의 락을 획득한 트랜잭션이 값을 변경하고 Commit하면, 대기하던 트랜잭션은 변경된 최신 값을 기준으로 WHERE 조건을 다시 확인한다.

따라서 issued_count가 total_quantity와 같아진 이후의 요청은 조건을 만족하지 못하고 영향받은 row 수 0을 반환한다.

추가로 다음 CHECK 제약을 설정하면 애플리케이션이나 다른 발급 경로의 오류에 대해서도 DB가 마지막 방어선 역할을 할 수 있다.
```

---

## **과제 6. 발급 트랜잭션과 Rollback 설계하기**

다음 두 작업을 하나의 DB 트랜잭션으로 묶는 이유를 설명한다.

```
1. coupon_event.issued_count 증가

2. coupon_issue 발급 기록 INSERT
```

### **1. 두 작업을 서로 다른 트랜잭션으로 처리했을 때 발생할 수 있는 문제**

| **상황** | **발생하는 데이터 불일치** |
| --- | --- |
| issued_count 증가 성공, coupon_issue INSERT 실패 | 발급 수량은 증가했지만 실제 발급 기록은 존재하지 않는다. `issued_count`와 실제 발급 건수가 달라진다. |
| coupon_issue INSERT 성공, issued_count 증가 실패 | 사용자 발급 기록은 존재하지만 발급 수량에는 반영되지 않는다. 이후 전체 수량을 초과하여 발급될 수 있다. |

### **2. 하나의 트랜잭션으로 처리하는 전체 흐름**

```
Transaction 시작
        |
        v
coupon_event 조건부 UPDATE
issued_count 증가 시도
        |
        +-----------------------------+
        |                             |
        | 영향받은 row 수 = 0         | 영향받은 row 수 = 1
        |                             |
        v                             v
기존 coupon_issue 조회          coupon_issue INSERT
        |                             |
        +-----------+                 +------------------+
        |           |                 |                  |
        | 기록 존재 | 기록 없음       | INSERT 성공      | INSERT 실패
        |           |                 |                  |
        v           v                 v                  v
중복 메시지     이벤트 없음 또는     Commit          Rollback
정상 처리       수량 소진 처리                           |
                                                        v
                                           issued_count 증가도 취소

조건부 UPDATE와 coupon_issue INSERT가 모두 성공한 경우에만 트랜잭션을 Commit한다.

두 작업 중 하나라도 실패하면 트랜잭션 전체를 Rollback하여 issued_count와 coupon_issue 데이터가 서로 다른 상태로 남지 않도록 한다.
```

### **3. 다음 SQL을 하나의 트랜잭션 흐름으로 완성하기**

```sql
-- Transaction 시작
BEGIN;

-- 1. 수량이 남아 있을 때만 issued_count 증가
UPDATE coupon_event
SET issued_count = issued_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :eventId
  AND issued_count < total_quantity;

-- 애플리케이션에서 UPDATE의 영향받은 row 수를 확인한다.

-- 2. UPDATE 결과가 1이면 발급 기록 INSERT
INSERT INTO coupon_issue (
    event_id,
    user_id,
    status,
    issued_at
) VALUES (
    :eventId,
    :userId,
    'ISSUED',
    CURRENT_TIMESTAMP
);

-- 두 작업이 모두 성공하면 Commit
COMMIT;

영향받은 row 수가 0이면 coupon_issue INSERT를 실행하지 않는다.

이 경우 동일한 사용자의 기존 발급 기록을 조회해 중복 메시지인지 확인한다.

SELECT id,
       event_id,
       user_id,
       status,
       issued_at
FROM coupon_issue
WHERE event_id = :eventId
  AND user_id = :userId;

- 기존 발급 기록이 존재한다 → 이미 처리된 중복 메시지로 판단한다.
- 기존 발급 기록이 존재하지 않는다 → 이벤트가 존재하지 않거나 수량이 소진된 것으로 판단한다.
```

### **4. UPDATE 성공 후 INSERT에서 UNIQUE 위반이 발생한 경우**

아래 항목을 모두 설명한다.

```
현재 트랜잭션은 어떻게 되는가?

앞에서 증가한 issued_count는 어떻게 되는가?

기존 발급 기록은 언제 조회해야 하는가?
```

답변:

```
- 현재 트랜잭션은 어떻게 되는가?

PostgreSQL에서 INSERT 중 UNIQUE 제약 위반이 발생하면 현재 트랜잭션은 오류 상태가 된다.
해당 트랜잭션에서는 이후의 일반 SQL을 정상적으로 실행할 수 없으므로 반드시 Rollback해야 한다.


- 앞에서 증가한 issued_count는 어떻게 되는가?

issued_count 증가와 coupon_issue INSERT가 같은 트랜잭션에 포함되어 있으므로 트랜잭션을 Rollback하면 앞에서 증가한 issued_count도 함께 취소된다.
예를 들어 issued_count가 500에서 501로 증가한 뒤 INSERT에서 UNIQUE 위반이 발생했다면, Rollback 이후 issued_count는 다시 500으로 복구된다.


- 기존 발급 기록은 언제 조회해야 하는가?

UNIQUE 위반이 발생한 현재 트랜잭션에서는 후속 SQL을 정상적으로 실행할 수 없다.
따라서 현재 트랜잭션을 먼저 Rollback한 뒤, 새로운 트랜잭션이나 별도의 조회 흐름에서 동일한 event_id와 user_id의 coupon_issue 기록을 조회해야 한다.

기록이 존재한다면 이미 처리된 메시지가 다시 전달된 것으로 판단하고, 기록이 없다면 예상하지 못한 데이터 오류로 처리한다.

전체 흐름은 다음과 같다.

Transaction 시작
        |
        v
조건부 UPDATE 성공
issued_count 증가
        |
        v
coupon_issue INSERT
        |
        v
UNIQUE 위반
        |
        v
현재 Transaction Rollback
        |
        v
issued_count 증가도 취소
        |
        v
새로운 Transaction에서 기존 발급 기록 조회
        |
        +---------------------------+
        |                           |
        | 기록 존재                 | 기록 없음
        |                           |
        v                           v
중복 메시지로 정상 처리       예상하지 못한 오류 처리
```

### **5. PostgreSQL에서 제약 조건 위반 후 같은 트랜잭션을 계속 사용할 수 없는 이유**

```
PostgreSQL은 하나의 트랜잭션 안에서 실행한 SQL이 UNIQUE, CHECK, Foreign Key 등의 제약 조건을 위반하면 해당 트랜잭션 전체를 실패 상태로 전환한다.

이 상태에서는 SELECT를 포함한 후속 SQL을 실행하더라도 현재 트랜잭션이 중단되었다는 오류가 발생한다.

오류가 발생한 상태에서 일부 SQL만 계속 실행하면 트랜잭션의 원자성과 데이터 일관성이 깨질 수 있기 때문이다.

따라서 제약 조건 위반 이후에는 반드시 현재 트랜잭션을 Rollback하고, 필요한 조회나 후속 처리는 새로운 트랜잭션에서 수행해야 한다.
```

### **6. 모든 발급 경로가 같은 트랜잭션을 사용해야 하는 이유**

```
Kafka Consumer, 관리자 수동 발급, 재처리 배치 등 모든 발급 경로가 동일한 트랜잭션 규칙을 사용해야 coupon_event와 coupon_issue의 정합성을 유지할 수 있다.

모든 발급 경로는 다음 순서를 동일하게 따라야 한다.

1. 조건부 UPDATE로 수량 확보
2. coupon_issue INSERT
3. 모두 성공하면 Commit
4. 하나라도 실패하면 Rollback

그래야 어떤 발급 경로에서도 DB의 불변식을 유지할 수 있다.
```

---

## **과제 7. Kafka 중복 소비와 DB 중복 발급 방지 설계하기**

Kafka Consumer는 같은 메시지를 다시 처리할 수 있다.

아래 상황을 기준으로 분석한다.

```
1. Consumer가 발급 요청 메시지를 읽는다.

2. DB 발급 트랜잭션을 Commit한다.

3. Consumer Group Offset 커밋 전에 Consumer가 종료된다.

4. Consumer가 재시작된다.

5. 같은 메시지를 다시 읽는다.
```

### **1. 중복 소비가 발생하는 전체 흐름 다이어그램**

```t
Kafka에 발급 요청 메시지 저장
        |
        v
Consumer가 메시지 읽기
        |
        v
DB 발급 트랜잭션 시작
        |
        v
issued_count 증가
        |
        v
coupon_issue INSERT
        |
        v
DB Transaction Commit
        |
        v
Consumer Group Offset 커밋 전 Consumer 종료
        |
        v
Consumer 재시작
        |
        v
Kafka는 Offset이 커밋되지 않은 것을 확인
        |
        v
동일한 메시지를 Consumer에게 다시 전달
        |
        v
Consumer가 동일한 발급 요청 재처리
```

### **2. DB에 UNIQUE 제약이 없다면 발생하는 문제**

```
동일한 Kafka 메시지가 다시 처리되면 같은 사용자의 coupon_issue 발급 기록이 중복으로 저장될 수 있다.
또한 재처리 과정에서 issued_count도 다시 증가하면 실제 사용자 수보다 DB의 발급 수량이 더 크게 기록될 수 있다.

예를 들어 최초 처리에서 다음 결과가 저장되었다고 가정한다.

issued_count: 10 → 11
coupon_issue(event_id=100, user_id=10) INSERT

같은 메시지가 재처리되면 다음 작업이 다시 수행될 수 있다.

issued_count: 11 → 12
coupon_issue(event_id=100, user_id=10) INSERT

그 결과 한 사용자가 동일한 이벤트에서 쿠폰을 두 번 발급받고, issued_count도 실제 발급 사용자 수보다 크게 증가한다.
```

### **3. `UNIQUE(event_id, user_id)`가 중복 소비를 방어하는 과정**

```
첫 번째 메시지 처리

1. 조건부 UPDATE로 issued_count를 증가시킨다.
2. coupon_issue에 (event_id=100, user_id=10)을 INSERT한다.
3. DB 트랜잭션을 Commit한다.

같은 메시지 재처리

1. Consumer가 동일한 메시지를 다시 읽는다.
2. 조건부 UPDATE가 성공하면 issued_count가 임시로 증가한다.
3. 동일한 (event_id=100, user_id=10)을 INSERT한다.
4. UNIQUE(event_id, user_id) 제약 위반이 발생한다.
5. 현재 트랜잭션을 Rollback한다.
6. 앞에서 증가한 issued_count도 함께 취소된다.
7. 새로운 트랜잭션에서 기존 발급 기록을 확인한다.
8. 기존 기록이 존재하면 이미 처리된 메시지로 판단한다.

따라서 중복 메시지가 전달되어도 최종 DB에는 하나의 발급 기록만 유지된다.
```

### **4. UNIQUE 위반을 단순 장애가 아니라 이미 처리된 메시지로 판단하기 위한 조건**

```
UNIQUE 위반이 발생했다는 사실만으로 무조건 이미 처리된 메시지라고 판단하면 안 된다.

다음 조건을 확인해야 한다.

1. 위반한 제약이 정확히 UNIQUE(event_id, user_id)인지 확인한다.
2. 현재 트랜잭션을 Rollback한 뒤 새로운 트랜잭션에서 동일한 event_id와 user_id의 coupon_issue 기록을 조회한다.
3. 기존 발급 기록이 실제로 존재하는지 확인한다.
4. 기존 기록의 상태가 최종 발급 성공 상태인 ISSUED인지 확인한다.

위 조건이 모두 충족되면 동일한 발급 요청이 이미 정상 처리된 것으로 판단할 수 있다.

기존 기록이 없다면 예상하지 못한 제약 위반이나 다른 데이터 문제일 수 있으므로 단순 중복으로 처리하면 안 된다.
```

### **5. 제약 위반 후 Consumer가 수행해야 하는 처리 순서**

```t
1. UNIQUE 제약 위반이 발생한 현재 DB 트랜잭션을 Rollback한다.

2. 새로운 트랜잭션에서 동일한 event_id와 user_id의
   coupon_issue 발급 기록을 조회한다.

3. 기존 발급 기록이 존재하면 이미 처리된 메시지로 판단하고,
   존재하지 않으면 예상하지 못한 오류로 처리한다.

4. 이미 처리된 메시지라면 메시지 처리를 완료하고
   Consumer Group Offset을 커밋한다.
   예상하지 못한 오류라면 Offset을 커밋하지 않고 재시도 대상으로 처리한다.
```

### **6. DB 처리보다 Consumer Group Offset이 먼저 커밋되면 안 되는 이유**

```
Consumer Group Offset은 해당 메시지의 처리가 완료되었다는 위치 정보다.

DB 처리보다 Offset을 먼저 커밋한 뒤 DB 트랜잭션에서 오류가 발생하면, Kafka는 해당 메시지가 이미 처리된 것으로 판단한다.
따라서 Consumer가 재시작되어도 같은 메시지를 다시 전달하지 않을 수 있다.

이 경우 Redis 판정과 Kafka 발행은 완료되었지만 DB에는 최종 발급 기록이 저장되지 않는 메시지 유실이 발생한다.

따라서 처리 순서는 다음과 같아야 한다.

1. Kafka 메시지 읽기
2. DB 트랜잭션 처리
3. DB 처리 결과 확정
4. Consumer Group Offset 커밋

DB 저장에 실패한 경우에는 Offset을 커밋하지 않아야 Kafka가 동일한 메시지를 다시 전달할 수 있다.
```

### **7. `UNIQUE(event_id, user_id)`와 requestId 기반 멱등성의 차이**

```
UNIQUE(event_id, user_id)는 비즈니스 결과의 중복을 방지한다.

즉, 한 사용자가 하나의 이벤트에서 쿠폰을 두 번 발급받지 못하도록 제한한다.

반면 requestId 기반 멱등성은 동일한 요청 자체가 여러 번 처리되는 것을 식별하고 관리한다.

예를 들어 같은 사용자가 같은 이벤트에 서로 다른 requestId로 여러 번 요청하더라도 UNIQUE 제약은 최종 발급을 한 번으로 제한한다.

같은 requestId가 네트워크 재시도나 Kafka 재전달로 반복되면 requestId 기반 멱등성은 해당 요청이 이미 접수되었는지, 처리 중인지, 성공했는지, 실패했는지를 구분할 수 있다.

UNIQUE(event_id, user_id)
→ 동일 사용자의 중복 발급 결과 방지

requestId 기반 멱등성
→ 동일 요청의 중복 접수와 중복 처리 방지
→ 요청별 처리 상태와 결과 추적

두 방식은 대체 관계가 아니라 서로 다른 계층의 중복 문제를 해결한다.
```

---

## **과제 8. 품절 후 중복 메시지 재처리 분석하기**

조건부 UPDATE를 먼저 실행하는 구조에서는 중복 메시지가 항상 INSERT까지 도달하는 것은 아니다.

아래 상황을 기준으로 분석한다.

```
이벤트 ID: 100

전체 수량: 1개

현재 issued_count: 1

coupon_issue에
(event_id=100, user_id=10) 기록이 이미 존재

user:10의 Kafka 메시지가 다시 전달됨
```

### **1. 조건부 UPDATE 결과와 그 이유**

```
조건부 UPDATE의 영향받은 row 수는 0이다.

현재 issued_count는 1이고 total_quantity도 1이므로 다음 조건을 만족하지 못하기 때문이다.

issued_count < total_quantity

즉, 1 < 1은 거짓이므로 issued_count는 증가하지 않는다.
```

### **2. coupon_issue INSERT까지 실행되는가?**

```
실행되지 않는다.

발급 로직은 조건부 UPDATE의 영향받은 row 수가 1일 때만 coupon_issue INSERT를 실행하도록 구성한다.
이번 상황에서는 UPDATE 결과가 0이므로 INSERT 단계로 진행하지 않고 기존 발급 기록을 조회해야 한다.
```

### **3. 이 상황에서 UNIQUE 제약 위반이 발생하는가?**

```
발생하지 않는다.

조건부 UPDATE 결과가 0이기 때문에 coupon_issue INSERT 자체가 실행되지 않는다.
따라서 기존에 동일한 (event_id=100, user_id=10) 기록이 존재하더라도 UNIQUE 제약을 검사하는 INSERT 단계까지 도달하지 않는다.
```

### **4. 영향받은 row 수가 0이라고 무조건 품절로 판단하면 안 되는 이유**

```
조건부 UPDATE 결과가 0이라는 것은 이번 요청이 수량을 새롭게 확보하지 못했다는 의미다.

하지만 수량을 확보하지 못한 이유가 현재 요청 사용자가 이미 쿠폰을 발급받았기 때문일 수도 있다.

이번 상황에서는 user:10의 발급이 이미 성공하여 issued_count가 1이 되었고, 그 메시지가 Offset 미커밋 등의 이유로 다시 전달되었다.
이를 단순 품절로 처리하면 이미 정상적으로 발급받은 사용자에게 발급 실패 또는 품절이라는 잘못된 결과를 제공할 수 있다.

따라서 UPDATE 결과가 0이면 기존 coupon_issue 기록을 추가로 조회해 중복 메시지와 실제 미발급 품절을 구분해야 한다.
```

### **5. 기존 coupon_issue 기록을 추가로 확인해야 하는 이유**

```
조건부 UPDATE 결과만으로는 다음 두 상황을 구분할 수 없다.

1. 이전 처리에서 이미 해당 사용자에게 발급된 메시지가 다시 전달된 상황
2. 해당 사용자는 발급받지 못했고 다른 사용자들이 수량을 모두 사용한 상황

동일한 event_id와 user_id의 coupon_issue 기록이 존재하면 이전 처리에서 이미 발급이 완료된 중복 메시지로 판단한다.
기존 기록이 존재하지 않으면 현재 사용자는 발급받지 못한 상태이며, 이벤트가 존재하지 않거나 수량이 소진된 것으로 판단한다.
```

### **6. 다음 두 상황을 구분하는 처리 흐름 작성하기**

각 상황에서 메시지 처리 완료 여부와 Consumer Group Offset 커밋 여부도 함께 작성한다.

```
상황 A

조건부 UPDATE 결과 = 0
동일한 coupon_issue 기록 존재

상황 B

조건부 UPDATE 결과 = 0
동일한 coupon_issue 기록 없음
```

답변:

```
상황 A

조건부 UPDATE 결과 = 0
동일한 coupon_issue 기록 존재

1. 조건부 UPDATE 결과가 0임을 확인한다.
2. 동일한 event_id와 user_id의 coupon_issue 기록을 조회한다.
3. 기존 ISSUED 기록이 존재하는 것을 확인한다.
4. 이전에 이미 정상 처리된 중복 메시지로 판단한다.
5. 메시지 처리를 성공으로 완료한다.
6. Consumer Group Offset을 커밋한다.

이 경우 재시도해도 결과가 달라지지 않으므로 Offset을 커밋하여 같은 메시지의 반복 처리를 종료한다.


상황 B

조건부 UPDATE 결과 = 0
동일한 coupon_issue 기록 없음

1. 조건부 UPDATE 결과가 0임을 확인한다.
2. 동일한 event_id와 user_id의 coupon_issue 기록을 조회한다.
3. 기존 발급 기록이 없음을 확인한다.
4. 이벤트 없음 또는 수량 소진으로 판단한다.
5. 품절 등 비즈니스 실패 결과와 로그·메트릭을 기록한다.
6. 메시지 처리를 완료한다.
7. Consumer Group Offset을 커밋한다.

이벤트 없음이나 수량 소진은 동일한 메시지를 재시도해도 일반적으로 성공으로 바뀌지 않는 비즈니스 실패다.

따라서 실패 결과를 기록한 후 Offset을 커밋한다. 다만 DB 연결 실패나 타임아웃 때문에 기존 기록을 정상적으로 조회하지 못한 경우에는 처리 결과를 확정할 수 없으므로 Offset을 커밋하지 않는다.
```

### **7. 전체 Consumer 처리 흐름 완성하기**

아래 두 경로를 모두 포함해야 한다.

```
조건부 UPDATE 결과 = 0인 경로

조건부 UPDATE 결과 = 1인 경로
```

```
Consumer가 Kafka 메시지 읽기
        |
        v
DB Transaction 시작
        |
        v
조건부 UPDATE 실행
        |
        +----------------------------------------+
        |                                        |
        | UPDATE 결과 = 0                        | UPDATE 결과 = 1
        |                                        |
        v                                        v
Transaction 종료 또는 Rollback             coupon_issue INSERT
        |                                        |
        v                              +---------+---------+
동일한 coupon_issue 조회                  |                   |
        |                              | INSERT 성공       | UNIQUE 위반
        |                              |                   |
  +-----+-----+                        v                   v
  |           |                  Transaction Commit   Transaction Rollback
  | 기록 존재 | 기록 없음                  |                   |
  |           |                        v                   v
  v           v                  메시지 처리 완료     새 Transaction에서
중복 메시지   이벤트 없음 또는              |              기존 발급 기록 조회
정상 처리     수량 소진 처리               v                    |
  |           |                  Offset 커밋       +------ --+--------+
  |           |                                  |                 |
  |           |                                  | 기록 존재         | 기록 없음
  |           |                                  |                 |
  v           v                                  v                 v
메시지 처리   실패 결과 기록                        중복 메시지       예상하지 못한
 완료        및 처리 완료                          정상 처리         오류 처리
  |           |                                  |                 |
  v           v                                  v                 v
Offset 커밋   Offset 커밋                       Offset 커밋       Offset 미커밋
                                                                  및 재시도
```

---

## **과제 9. DB 동시성 제어 방식 비교하기**

다음 세 가지 방식으로 쿠폰 수량을 제어할 수 있다.

```
조건부 UPDATE

비관적 락

낙관적 락
```

### **1. 조건부 UPDATE 방식 설명**

```
조건부 UPDATE는 수량 확인과 증가를 하나의 SQL로 수행하는 방식이다.

issued_count가 total_quantity보다 작은 경우에만 issued_count를 1 증가시킨다.

동시에 여러 요청이 같은 이벤트 row를 수정하더라도 DB가 row 단위 갱신을 직렬화하고, 각 요청은 최신 값을 기준으로 WHERE 조건을 확인한다.

영향받은 row 수가 1이면 수량 확보에 성공한 것이고, 0이면 수량을 확보하지 못한 것이다.
```

### **2. 비관적 락 방식 설명과 SQL 예시**

```sql
BEGIN;

SELECT id,
       total_quantity,
       issued_count
FROM coupon_event
WHERE id = :eventId
FOR UPDATE;

UPDATE coupon_event
SET issued_count = issued_count + 1
WHERE id = :eventId
  AND issued_count < total_quantity;

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
);

COMMIT;
```

설명:

```
비관적 락은 데이터 충돌이 발생할 가능성이 높다고 가정하고, 수량을 확인하기 전에 해당 이벤트 row를 먼저 잠그는 방식이다.

SELECT ... FOR UPDATE를 실행한 트랜잭션이 해당 row에 대한 배타적 락을 획득한다.

다른 트랜잭션은 먼저 실행된 트랜잭션이 Commit 또는 Rollback할 때까지 대기한다.

락을 획득한 트랜잭션은 안전하게 수량을 확인하고, issued_count 증가와 coupon_issue INSERT를 수행할 수 있다.

처리 순서를 명확하게 제어할 수 있지만, 요청이 많을수록 락 대기 시간이 길어지고 트랜잭션 처리량이 감소할 수 있다.
```

### **3. 낙관적 락 방식 설명과 SQL 예시**

```sql
SELECT id,
       total_quantity,
       issued_count,
       version
FROM coupon_event
WHERE id = :eventId;

UPDATE coupon_event
SET issued_count = issued_count + 1,
    version = version + 1
WHERE id = :eventId
  AND version = :oldVersion
  AND issued_count < total_quantity;
```

설명:

```
낙관적 락은 데이터 충돌이 자주 발생하지 않는다고 가정하고, 조회 시점에는 별도의 락을 획득하지 않는 방식이다.

데이터를 조회할 때 version 값을 함께 읽고, UPDATE할 때 기존 version 값이 그대로인지 확인한다.

다른 트랜잭션이 먼저 데이터를 수정했다면 version이 변경되므로 UPDATE의 영향받은 row 수가 0이 된다.

이 경우 애플리케이션은 최신 데이터를 다시 조회한 뒤 수량이 남아 있다면 재시도해야 한다.

락 대기 시간을 줄일 수 있지만, 선착순 이벤트처럼 같은 row에 충돌이 집중되면 실패와 재시도가 반복되어 DB 부하가 증가할 수 있다.
```

### **4. 세 방식 비교표**

| **구분** | **조건부 UPDATE** | **비관적 락** | **낙관적 락** |
| --- | --- | --- | --- |
| 수량을 보호하는 방법 | 수량 조건과 증가를 하나의 UPDATE에서 원자적으로 처리한다.                            | `SELECT FOR UPDATE`로 row를 먼저 잠근 뒤 수량을 확인하고 수정한다.            | version 값을 비교해 조회 이후 다른 트랜잭션의 변경 여부를 확인한다. |
| 충돌 시 동작     | 같은 row의 갱신이 DB 내부에서 순차 처리되며, 조건을 만족하지 못한 요청은 결과 0을 반환한다.       | 나중에 접근한 트랜잭션이 row 락이 해제될 때까지 대기한다.                          | version이 달라진 요청의 UPDATE가 실패한다.             |
| 재시도 필요 여부   | 일반적인 수량 소진 상황에서는 불필요하다. 일시적 DB 오류에는 필요할 수 있다.                  | 일반적으로 수량 경쟁에 대한 애플리케이션 재시도는 필요하지 않다. 데드락이나 타임아웃은 재시도할 수 있다. | 충돌 시 최신 데이터 재조회와 재시도가 필요하다.                |
| 장점          | SQL이 단순하고, 별도 SELECT 없이 수량 초과를 방지하며, 영향받은 row 수로 결과를 판단할 수 있다. | 처리 순서와 정합성을 이해하기 쉽고, 락을 획득한 트랜잭션이 안전하게 데이터를 수정할 수 있다.       | 충돌이 적을 때 락 대기 없이 높은 처리량을 얻을 수 있다.          |
| 단점          | 하나의 이벤트 row에 UPDATE가 집중되면 row lock 대기가 발생한다.                   | 요청이 몰리면 락 대기가 길어지고, 긴 트랜잭션은 전체 처리량을 낮춘다.                    | 충돌이 많으면 재시도가 반복되어 DB와 애플리케이션 부하가 증가한다.     |
| 선착순 이벤트 적합성 | 높음. 단순한 SQL로 수량 경쟁을 처리할 수 있다.                                  | 정합성은 높지만 높은 트래픽에서는 락 대기가 커질 수 있다.                           | 낮음. 동일 row의 충돌률이 높아 재시도가 반복될 가능성이 크다.      |

### **5. 하나의 이벤트 row에 요청이 집중될 때 발생하는 문제**

```
모든 쿠폰 발급 요청이 하나의 coupon_event row의 issued_count를 수정하므로 해당 row가 병목 지점이 된다.

PostgreSQL에서 하나의 row를 동시에 여러 트랜잭션이 수정할 수 없기 때문에 먼저 row를 수정 중인 트랜잭션이 완료될 때까지 다른 트랜잭션은 row lock을 기다려야 한다.

요청이 많아지면 다음 문제가 발생할 수 있다.

1. row lock 대기 시간 증가
2. DB 트랜잭션 처리 지연
3. Consumer 처리량 감소
4. Kafka Consumer Lag 증가
5. DB Connection 점유 시간 증가
6. Lock Timeout 또는 Transaction Timeout 발생 가능성
7. 특정 이벤트 row가 전체 시스템의 병목으로 작동

Redis에서 많은 요청을 사전에 차단하더라도 최종 발급 대상 요청은 동일한 DB row를 갱신하므로 DB 단계의 병목이 완전히 사라지는 것은 아니다.
```

### **6. 이번 구조에서 조건부 UPDATE를 선택하는 이유**

```
이번 쿠폰 발급 구조에서는 수량 확인과 증가만 안전하게 수행하면 되므로 조건부 UPDATE가 가장 단순하고 적합하다.

조건부 UPDATE를 선택하는 이유는 다음과 같다.

1. 수량 조회와 증가를 하나의 SQL로 처리할 수 있다.
2. SELECT와 UPDATE 사이의 Race Condition을 방지할 수 있다.
3. 별도의 명시적 SELECT FOR UPDATE가 필요하지 않다.
4. 영향받은 row 수로 수량 확보 성공 여부를 판단할 수 있다.
5. 수량이 소진된 경우 애플리케이션 재시도 없이 결과 0으로 처리할 수 있다.
6. 낙관적 락처럼 충돌 시 version 재조회와 반복 재시도가 필요하지 않다.
7. SQL과 애플리케이션 로직이 비교적 단순하다.

단, 조건부 UPDATE도 실제 row 갱신 과정에서는 DB의 row lock을 사용하므로 높은 트래픽에서는 대기가 발생할 수 있다.

따라서 Redis에서 사전 필터링하고, DB에서는 조건부 UPDATE로 최종 수량을 방어하는 구조를 사용한다.
```

### **7. 세 방식과 UNIQUE 제약의 역할이 다른 이유**

```
조건부 UPDATE, 비관적 락, 낙관적 락은 동시에 여러 요청이 수량 데이터를 수정할 때 coupon_event의 전체 발급 수량을 보호하기 위한 동시성 제어 방식이다.
반면 UNIQUE(event_id, user_id)는 동일한 사용자가 동일한 이벤트에서 두 개 이상의 발급 기록을 갖지 못하도록 하는 DB 제약이다.

즉, 보호하는 불변식이 서로 다르다.

조건부 UPDATE·비관적 락·낙관적 락
→ issued_count가 total_quantity를 초과하지 않도록 보호

UNIQUE(event_id, user_id)
→ 동일 사용자의 중복 발급을 방지

수량 제어 방식만 적용하면 서로 다른 사용자의 전체 수량은 보호할 수 있지만, 같은 사용자의 중복 발급 기록이 저장될 수 있다.
UNIQUE 제약만 적용하면 같은 사용자의 중복은 막을 수 있지만, 서로 다른 사용자에게 전체 수량을 초과하여 발급할 수 있다.

따라서 전체 수량 제어 방식과 UNIQUE 제약을 함께 적용해야 한다.
```

---

## **과제 10. Redis, Kafka, DB 불일치 상황과 이후 주차 연결하기**

Redis, Kafka, DB는 서로 다른 시스템이므로 상태가 일시적으로 달라질 수 있다.

Kafka 상태는 발행된 레코드의 존재 여부와 Consumer Group Offset 커밋 여부를 구분해서 작성한다. Kafka 레코드가 존재한다는 사실만으로 처리 대기 상태라고 판단하지 않는다.

### **1. Redis 통과 후 Kafka 발행 실패 상황**

아래 세 시스템에 남는 상태를 작성한다.

| **시스템** | **남아 있는 상태** |
| --- | --- |
| Redis   | 사용자가 발급 요청을 통과한 기록과 수량 차감 결과가 남을 수 있다. Redis 기준으로는 ACCEPTED 상태다.                     |
| Kafka   | 발행에 실패했으므로 해당 발급 요청 레코드가 존재하지 않는다. Consumer Group Offset도 생성되거나 변경되지 않는다.            |
| DB      | Consumer에게 메시지가 전달되지 않았으므로 `issued_count`와 `coupon_issue` 모두 변경되지 않는다. 최종 발급 기록이 없다. |

### **2. Kafka 발행 후 DB 저장 실패 상황**

| **시스템** | **남아 있는 상태** |
| --- | --- |
| Redis   | 사용자의 요청 통과 기록과 Redis 기준 수량 차감 결과가 남아 있다.                                                                                                            |
| Kafka   | 발급 요청 레코드는 Kafka에 존재한다. DB 처리에 실패하여 Offset을 커밋하지 않았다면 재전달 대상이다. Offset을 잘못 커밋했다면 레코드는 존재하더라도 해당 Consumer Group에서는 이미 처리한 것으로 간주되어 자동 재전달되지 않을 수 있다. |
| DB      | 발급 트랜잭션이 Rollback되어 `issued_count` 증가와 `coupon_issue` INSERT가 남지 않는다. 최종 발급은 실패한 상태다.                                                               |

### **3. DB 저장 후 Redis 데이터 유실 상황**

| **시스템** | **남아 있는 상태** |
| --- | --- |
| Redis   | 장애, 재시작 또는 데이터 유실로 사용자 발급 판정 기록이나 수량 정보가 사라질 수 있다.                             |
| DB      | `coupon_event.issued_count` 증가와 `coupon_issue` 발급 기록이 Commit되어 최종 발급 결과가 유지된다. |

### **4. 최종 발급 여부를 판단할 때 DB를 기준으로 해야 하는 이유**

```
Redis는 빠른 선착순 판정과 중복 요청 차단을 담당하지만 장애나 TTL, 재시작, 데이터 유실로 상태가 사라질 수 있다.

Kafka는 발급 요청을 전달하고 재처리할 수 있게 하지만, Kafka 레코드가 존재한다는 사실은 요청이 발행되었다는 의미일 뿐 DB 발급 성공을 의미하지 않는다.
또한 Kafka 레코드가 존재하더라도 Consumer Group Offset이 이미 커밋되었다면 해당 Consumer Group에서는 처리 완료된 메시지일 수 있다.

DB는 다음 최종 데이터를 저장한다.

1. coupon_event.issued_count
2. coupon_issue의 사용자별 발급 기록
3. UNIQUE와 CHECK 등의 제약 조건
4. 발급 수량 증가와 기록 저장의 트랜잭션 결과

따라서 최종 발급 성공은 DB 트랜잭션이 Commit되고 coupon_issue에 최종 발급 기록이 존재하는지를 기준으로 판단해야 한다.
```

### **5. Redis, Kafka, DB의 역할 비교표**

| **구분** | **Redis** | **Kafka** | **DB** |
| --- | --- | --- | --- |
| 주 역할       | 빠른 선착순 수량 판정과 중복 요청 차단                      | 발급 요청의 비동기 전달, 버퍼링 및 재처리                                                                 | 최종 발급 기록 저장과 정합성 보장                     |
| 저장하는 상태    | Redis 기준 요청 통과 여부, 사용자 중복 판정 정보, 임시 수량      | 발행된 메시지 레코드와 Consumer Group별 Offset                                                      | 이벤트 전체 수량, 최종 발급 수량, 사용자별 최종 발급 기록      |
| 성공이 의미하는 것 | Redis 기준으로 현재 요청이 판정을 통과했다는 의미              | Producer 발행 성공은 레코드가 Kafka에 저장되었다는 의미다. Offset 커밋은 특정 Consumer Group이 처리를 완료했다고 표시한 의미다. | 발급 트랜잭션이 Commit되어 최종 발급이 확정되었다는 의미      |
| 강점         | 매우 빠른 읽기·쓰기와 원자적 연산으로 대량 요청을 빠르게 필터링할 수 있다. | 시스템 간 결합도를 낮추고 트래픽을 완충하며 실패한 메시지를 재처리할 수 있다.                                             | 트랜잭션과 제약 조건을 통해 최종 데이터의 원자성과 정합성을 보장한다. |
| 한계         | 데이터 유실과 TTL 가능성이 있고 DB와 자동으로 원자성을 보장하지 않는다. | 메시지 중복 전달이 가능하며 레코드 존재만으로 DB 처리 상태를 알 수 없다.                                              | 높은 동시 요청에서 row lock과 트랜잭션 병목이 발생할 수 있다. |

### **6. 5주차 requestId 기반 멱등성에서 해결할 문제**

```
requestId 기반 멱등성은 동일한 요청이 여러 번 접수되거나 전달되는 문제를 해결한다.

클라이언트의 네트워크 재시도, API Server의 중복 요청, Kafka 메시지 재전달 등이 발생하더라도 동일한 requestId를 가진 요청을 하나의 요청으로 식별한다.

requestId별로 다음 상태를 관리할 수 있다.

1. 요청이 처음 접수되었는지
2. 현재 처리 중인지
3. 최종 발급에 성공했는지
4. 품절 등으로 실패했는지
5. 이전 요청의 응답 결과가 무엇인지

이를 통해 동일한 요청에 대해 매번 새로운 발급 절차를 수행하지 않고 기존 처리 결과를 반환할 수 있다.

UNIQUE(event_id, user_id)가 중복 발급 결과를 방지한다면, requestId는 동일 요청의 중복 실행과 처리 상태 추적을 담당한다.
```

### **7. 6주차 Outbox Pattern에서 해결할 문제**

```
Outbox Pattern은 하나의 DB 트랜잭션 안에서 비즈니스 데이터 변경과 발행할 이벤트 기록 저장을 함께 처리하여 DB Commit과 후속 메시지 발행 사이의 불일치를 줄이는 방식이다.

예를 들어 DB 발급 결과 저장 후 후속 알림 메시지나 발급 완료 이벤트를 Kafka에 발행해야 할 때 다음 문제가 발생할 수 있다.

1. DB Commit은 성공했지만 Kafka 발행은 실패
2. Kafka 발행은 성공했지만 애플리케이션이 성공 여부를 확인하지 못함
3. 재시도 과정에서 동일 이벤트가 중복 발행됨

Outbox Pattern에서는 DB 트랜잭션 안에서 coupon_issue와 outbox_event를 함께 저장한다.

별도의 Publisher가 미발행 Outbox 레코드를 조회하여 Kafka에 발행하고 발행 완료 상태를 기록한다.

이를 통해 DB 저장은 성공했지만 후속 이벤트가 영구적으로 유실되는 문제를 줄일 수 있다.
```

### **8. Outbox Pattern이 Redis 판정과 최초 Kafka 발행 사이를 원자적으로 묶어주는 것은 아닌 이유**

```
일반적인 Outbox Pattern은 하나의 관계형 DB 트랜잭션 안에서 비즈니스 데이터와 Outbox 레코드를 함께 저장하는 방식이다.
하지만 Redis 판정은 Redis에서 수행되고, 최초 Kafka 발행은 Kafka Broker에 수행된다.
Redis, Kafka, 관계형 DB는 서로 독립된 시스템이며 하나의 로컬 DB 트랜잭션에 함께 참여하지 않는다.

따라서 DB Outbox를 적용하더라도 다음 두 작업이 자동으로 하나의 원자적 작업이 되는 것은 아니다.

1. Redis에서 사용자와 수량 판정
2. 최초 발급 요청을 Kafka에 발행

Redis 판정은 성공했지만 API Server가 Kafka에 메시지를 발행하기 전에 종료되면, Redis에는 ACCEPTED 상태가 남고 Kafka에는 레코드가 없을 수 있다.
이 문제를 해결하려면 별도의 보상·복구 정책, requestId 기반 상태 관리, 재발행 작업, Redis Stream이나 다른 영속적 접수 저장소 활용 등을 검토해야 한다.

즉, Outbox Pattern은 주로 같은 DB 트랜잭션에서 생성된 후속 이벤트의 안정적인 Kafka 발행을 해결하며, Redis와 최초 Kafka 발행의 원자성을 직접 보장하지 않는다.
```

### **9. 8주차 Retry와 DLQ에서 해결할 문제**

```
Retry는 DB 일시 장애, 네트워크 오류, 타임아웃처럼 잠시 후 다시 처리하면 성공할 가능성이 있는 실패를 재처리하기 위해 사용한다.
모든 오류를 즉시 반복 재시도하면 장애가 발생한 DB나 외부 시스템에 부하가 집중될 수 있으므로 재시도 횟수와 간격, 지수 백오프 등의 정책이 필요하다.

DLQ는 정해진 재시도 횟수를 초과했거나, 데이터 형식 오류처럼 반복해도 성공할 가능성이 낮은 메시지를 일반 처리 흐름에서 분리하여 저장하는 용도로 사용한다.

Retry와 DLQ를 통해 다음 문제를 해결한다.

1. 일시적 DB 장애로 실패한 메시지의 재처리
2. 무한 재시도로 인한 Consumer 처리 중단 방지
3. 문제가 있는 하나의 메시지가 뒤 메시지 처리를 막는 현상 완화
4. 반복 실패 메시지의 격리
5. 운영자가 실패 원인을 분석하고 수동 복구할 수 있는 경로 제공
6. 재시도 횟수, 최종 실패 건수 등의 모니터링

단, 이벤트 없음이나 실제 수량 소진처럼 재시도로 결과가 바뀌지 않는 비즈니스 실패는 무조건 Retry 또는 DLQ 대상으로 보내지 않는다.
오류의 종류를 일시적 기술 실패와 재시도 불가능한 비즈니스 실패로 구분해야 한다.
```

---
