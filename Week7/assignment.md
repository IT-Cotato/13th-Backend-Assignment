# **7주차 과제: EDA 기반 서비스 분리와 조회 구조**

## **1. 과제 목표**

이번 과제의 목표는 쿠폰 발급 이후의 알림, 마이페이지와 통계 작업을 `CouponIssued` 이벤트 기반으로 분리하고, 각 Consumer가 자신의 데이터를 안전하게 만드는 구조를 설계하는 것이다.

6주차까지 다음 흐름을 만들었다.

```text
Coupon Issue Consumer
        |
        v
+------------------------------------------+
| Coupon Issue DB Transaction              |
|------------------------------------------|
| coupon_issue 저장                        |
| request ISSUED                           |
| CouponIssued Outbox 저장                 |
+------------------------------------------+
        |
        | Commit
        v
Outbox Publisher
        |
        v
Kafka coupon.issued
```

7주차에서는 `coupon.issued` 이후를 설계한다.

```text
Kafka coupon.issued
  |
  +--> Notification Consumer
  |
  +--> MyPage Consumer
  |
  +--> Analytics Consumer
```

이번 과제의 핵심 질문은 다음과 같다.

```text
- 세 Consumer가 같은 이벤트를 모두 받으려면 Group을 어떻게 나누어야 하는가?
- Coupon Service와 후속 서비스의 데이터 소유권은 어떻게 구분하는가?
- MyPage Read Model은 원본 DB와 무엇이 다른가?
- 중복 이벤트가 조회와 통계에 두 번 반영되지 않게 하려면 어떻게 하는가?
- DB ISSUED와 마이페이지 반영 사이의 지연을 사용자에게 어떻게 보여주는가?
- 이벤트 순서가 바뀌어도 최신 상태를 어떻게 지키는가?
- Read Model이 잘못되었을 때 어떻게 다시 만드는가?
```

---

## **2. 기본 상황**

서비스에서 다음 쿠폰 이벤트를 진행한다고 가정한다.

```text
쿠폰 이벤트 ID: 100
쿠폰 이름: 여름맞이 5,000원 할인 쿠폰
사용자 ID: 10
쿠폰 발급 ID: 5001

원본 결과 Topic: coupon.issued
Partition Key: couponIssueId

Coupon Service DB:
쿠폰 발급의 최종 원장

MyPage DB:
사용자 쿠폰 목록을 위한 Read Model

Notification DB:
알림 작업과 발송 상태

Analytics DB:
발급 사실과 운영자용 집계
```

기본 `CouponIssued` 이벤트는 다음과 같다.

```json
{
  "eventMessageId": "8a78e94b-5c3d-46a7-a364-538cd1f856c9",
  "eventType": "COUPON_ISSUED",
  "schemaVersion": 1,
  "aggregateVersion": 1,
  "occurredAt": "2026-08-08T03:00:01Z",
  "requestId": "4372dbe5-8cc8-4bfa-9cdf-1cd5e9f77b31",
  "data": {
    "couponIssueId": 5001,
    "couponEventId": 100,
    "userId": 10,
    "issuedAt": "2026-08-08T03:00:01Z"
  }
}
```

---

## **3. 전제 조건**

```text
Coupon Service의 coupon_issue가 발급 여부의 최종 원장이다.

CouponIssued는 DB에서 발급이 확정된 뒤 생성된 과거의 사실이다.

Notification, MyPage와 Analytics는 서로 다른 논리 Subscriber다.

Kafka Record와 동일한 논리 이벤트는 재전달·재발행될 수 있다.

각 Consumer는 자신의 DB만 변경한다.

Consumer 자동 Offset Commit은 사용하지 않는다.

Consumer DB Commit 뒤 Offset을 완료 처리한다.

이번 주차는 Retry Topic과 DLQ의 상세 구현보다
서비스 분리, 조회 모델과 Consumer 멱등성에 집중한다.
```

---

## **4. 제출 형식**

제출 파일은 다음 경로에 작성한다.

```text
submissions/Week7/기수_이름.md
```

필수 과제는 다음과 같다.

```text
과제 1. 직접 호출 구조와 EDA 비교하기
과제 2. Consumer Group과 서비스 책임 설계하기
과제 3. CQRS와 MyPage Read Model 설계하기
과제 4. Consumer 멱등 트랜잭션 설계하기
과제 5. Notification과 Analytics 처리 설계하기
과제 6. 최종적 일관성과 사용자 경험 설계하기
과제 7. 이벤트 순서와 버전 처리하기
과제 8. Read Model 재구축과 운영 지표 설계하기
과제 9. 잘못된 EDA 구현 리뷰하기
```

---

# **5. 필수 과제**

---

## **과제 1. 직접 호출 구조와 EDA 비교하기**

Coupon Service가 쿠폰 발급 뒤 세 서비스를 순서대로 호출한다고 가정한다.

```text
Coupon Service
  |
  +--> Notification Service
  |
  +--> MyPage Service
  |
  +--> Analytics Service
```

### **1. 직접 호출 구조의 결합을 분석한다**

| 결합 종류 | 발생하는 문제 | EDA 적용 후 달라지는 점 |
| --- | --- | --- |
| 시간 결합 |  |  |
| 장애 결합 |  |  |
| 변경 결합 |  |  |
| 데이터 결합 |  |  |

### **2. 다음 장애 상황을 분석한다**

```text
1. coupon_issue DB Commit 성공
2. Notification API 호출 실패
3. Coupon Service가 전체 요청 실패 응답
4. 클라이언트가 같은 요청을 다시 전송
```

```text
이미 확정된 쿠폰 발급 상태:


쿠폰 발급을 Rollback하면 안 되는 이유:


다시 실행해야 하는 작업:


다시 실행하면 안 되는 작업:

```

### **3. EDA 기반 전체 흐름을 완성한다**

다음 구성 요소를 모두 포함한다.

```text
Coupon Issue DB Transaction
CouponIssued Outbox
Outbox Publisher
Kafka coupon.issued
Notification Consumer
MyPage Consumer
Analytics Consumer
```

```text



```

### **4. 명령과 이벤트의 차이를 작성한다**

| 이름 | 종류 | 의미 | Consumer가 쿠폰을 다시 발급하는가? |
| --- | --- | --- | --- |
| `IssueCoupon` |  |  |  |
| `CouponIssued` |  |  |  |

### **5. EDA를 사용해도 남는 결합을 설명한다**

```text
직접 API 계약 대신 남는 계약:


이 계약을 관리해야 하는 이유:


EDA가 반드시 MSA를 의미하지 않는 이유:

```

---

## **과제 2. Consumer Group과 서비스 책임 설계하기**

### **1. 서비스별 Consumer Group을 작성한다**

| 서비스 | Group ID | 같은 이벤트를 받아 수행할 작업 |
| --- | --- | --- |
| Notification |  |  |
| MyPage |  |  |
| Analytics |  |  |

### **2. 세 서비스에 같은 Group ID를 사용하면 어떤 일이 발생하는지 설명한다**

```text
같은 Group 안의 Consumer 관계:


CouponIssued 한 건의 전달 결과:


세 서비스 중 일부 작업이 누락될 수 있는 이유:

```

### **3. 다음 Partition과 Consumer 배치를 분석한다**

```text
coupon.issued Partition 수: 6

notification-coupon-issued Consumer 수: 2
mypage-coupon-projection Consumer 수: 4
analytics-coupon-issued Consumer 수: 1
```

| Group | 동시에 처리 가능한 최대 Consumer 수 | 사용되지 않는 Partition 또는 Consumer | 다른 Group 지연의 영향 |
| --- | ---: | --- | --- |
| Notification |  |  |  |
| MyPage |  |  |  |
| Analytics |  |  |  |

### **4. Consumer Group의 Committed Offset 의미를 설명한다**

```text
Committed Offset이 나타내는 것:


Committed Offset만으로 증명할 수 없는 것:


Notification Group의 Offset이 MyPage Group에 영향을 주는가?

```

### **5. 서비스별 책임과 데이터 소유권을 완성한다**

| 서비스 | 소유하는 데이터 | 보장해야 하는 불변식 또는 책임 | 다른 서비스 DB 직접 수정 허용 여부 |
| --- | --- | --- | --- |
| Coupon Service |  |  |  |
| Notification Service |  |  |  |
| MyPage Service |  |  |  |
| Analytics Service |  |  |  |

### **6. 장애 격리 결과를 작성한다**

| 장애 | 쿠폰 발급 결과 | 영향받는 기능 | 영향받지 않아야 하는 기능 | 복구 단위 |
| --- | --- | --- | --- | --- |
| Notification Consumer 중단 |  |  |  |  |
| MyPage Consumer 중단 |  |  |  |  |
| Analytics Consumer 중단 |  |  |  |  |

---

## **과제 3. CQRS와 MyPage Read Model 설계하기**

### **1. Command Model과 Read Model을 비교한다**

| 구분 | Command / Write Model | Query / Read Model |
| --- | --- | --- |
| 주요 목적 |  |  |
| 대표 작업 |  |  |
| 데이터 구조 |  |  |
| 최종 원장 여부 |  |  |
| 장애 후 재구축 가능 여부 |  |  |

### **2. CQRS에 대해 다음 문장을 판단한다**

| 문장 | 맞음/틀림 | 이유 |
| --- | --- | --- |
| CQRS를 적용하려면 반드시 DB가 두 개여야 한다. |  |  |
| CQRS는 Event Sourcing과 같은 의미다. |  |  |
| 같은 애플리케이션에서도 Command와 Query 모델을 나눌 수 있다. |  |  |
| 테이블 이름만 둘로 나누면 자동으로 CQRS가 된다. |  |  |

### **3. `user_coupon_view` DDL을 완성한다**

다음 요구사항을 만족해야 한다.

```text
coupon_issue_id Primary Key
user_id와 coupon_event_id 저장
현재 status와 aggregate_version 저장
issued_at과 updated_at 저장
같은 사용자·이벤트의 중복 노출 방지
사용자별 최신 쿠폰 조회 인덱스
```

```sql
CREATE TABLE user_coupon_view (


);

CREATE INDEX idx_user_coupon_view_user_issued

;
```

### **4. 각 제약과 인덱스의 역할을 작성한다**

| 장치 | 막거나 개선하는 문제 |
| --- | --- |
| `PRIMARY KEY (coupon_issue_id)` |  |
| `UNIQUE (user_id, coupon_event_id)` |  |
| `(user_id, issued_at DESC)` Index |  |

### **5. 다음 조회 API가 Read Model을 사용하는 이유를 설명한다**

```text
GET /api/users/{userId}/coupons
```

```text
원본 발급 DB를 매번 복잡하게 조회할 때의 문제:


Read Model에 일부 데이터가 중복되어도 되는 이유:


쿠폰 발급 성공의 최종 기준 데이터:

```

### **6. 화면 표시용 데이터 전달 방법을 비교한다**

마이페이지에 `couponDisplayName`과 `discountAmount`가 필요하다고 가정한다.

| 방법 | 장점 | 단점 | 데이터 소유권·장애 결합에 미치는 영향 |
| --- | --- | --- | --- |
| `CouponIssued`에 Snapshot 포함 |  |  |  |
| `CouponEventCreated/Updated`로 메타데이터 Projection 관리 |  |  |  |
| 이벤트 처리 중 Coupon Service API 동기 호출 |  |  |  |
| Coupon Service DB 직접 조회 |  |  |  |

이벤트에 원본 Entity 전체를 넣으면 안 되는 이유도 작성한다.

```text

```

---

## **과제 4. Consumer 멱등 트랜잭션 설계하기**

같은 논리 이벤트가 다음처럼 두 번 저장되었다고 가정한다.

```text
Partition 2 / Offset 100 / eventMessageId = evt-001
Partition 2 / Offset 101 / eventMessageId = evt-001
```

### **1. Offset과 eventMessageId를 비교한다**

| 값 | 식별 대상 | 재발행 시 변경되는가? | Consumer 멱등성 Key로 적절한가? |
| --- | --- | --- | --- |
| Partition + Offset |  |  |  |
| eventMessageId |  |  |  |

### **2. `processed_event` DDL을 작성한다**

```sql
CREATE TABLE processed_event (


);
```

다음 세 값 중 올바른 `subscriber_id`를 고르고 이유를 작성한다.

```text
pod-a7f9c
server-10.0.0.12
mypage-coupon-projection
```

```text
선택:


이유:

```

### **3. MyPage Consumer 트랜잭션을 완성한다**

다음 요구사항을 만족해야 한다.

```text
ON CONFLICT DO NOTHING으로 최초 처리 권한 등록
RETURNING 결과로 최초·중복 판별
최초 이벤트일 때만 user_coupon_view 변경
processed_event와 조회 모델 변경의 원자성
```

```sql
BEGIN;

WITH accepted AS (


)
INSERT INTO user_coupon_view (

)
SELECT

FROM accepted;

COMMIT;
```

### **4. 일반 INSERT의 PK 예외를 잡고 계속 진행하면 안 되는 이유를 설명한다**

PostgreSQL 트랜잭션 상태를 포함한다.

```text

```

### **5. 다음 잘못된 처리 순서의 결과를 분석한다**

```text
1. processed_event만 Commit
2. user_coupon_view 변경 실패
3. Kafka Record 재전달
```

```text
재전달 시 Consumer 판단:


최종 조회 모델 상태:


두 작업을 같은 트랜잭션에 넣어야 하는 이유:

```

### **6. Consumer DB Commit 후 Offset Commit 전 종료를 분석한다**

```text
Consumer DB 상태:


Consumer Group Offset:


재시작 후 전달:


재전달된 이벤트 처리:

```

### **7. Offset 완료 정책을 작성한다**

다음을 반드시 포함한다.

```text
enable.auto.commit
DB Commit과 Offset Commit 순서
Listener 예외 처리
같은 Partition 병렬 처리 시 앞선 미완료 Record
```

```text

```

---

## **과제 5. Notification과 Analytics 처리 설계하기**

### **1. Notification Consumer의 새로운 Dual Write를 분석한다**

```text
CouponIssued 소비
  |
  +--> processed_event DB 저장
  |
  +--> 외부 SMS Provider 호출
```

다음 두 장애를 모두 분석한다.

| 장애 지점 | DB 상태 | 외부 SMS 상태 | 재전달 시 문제 |
| --- | --- | --- | --- |
| SMS 성공 후 `processed_event` Commit 전 종료 |  |  |  |
| `processed_event` Commit 후 SMS 호출 전 종료 |  |  |  |

### **2. Notification 내부 처리 구조를 완성한다**

다음 구성 요소를 포함한다.

```text
Notification Consumer
processed_event
notification_job
별도 Notification Sender
외부 Provider
notificationId / Idempotency Key
```

```text



```

`processed_event`와 `notification_job`을 어떤 트랜잭션으로 처리해야 하는지도 작성한다.

```text

```

### **3. 외부 Provider가 Idempotency Key를 지원하지 않는 경우의 보장 수준을 작성한다**

```text
외부 알림 전송 보장 수준:


중복 전송 가능성을 완전히 제거할 수 있는가?


운영에서 추가로 필요한 장치:

```

### **4. 다음 Analytics 구현의 문제를 설명한다**

```sql
UPDATE coupon_issue_stats
SET issued_count = issued_count + 1
WHERE coupon_event_id = :couponEventId;
```

같은 `eventMessageId`가 두 번 전달되었을 때의 결과를 작성한다.

```text

```

### **5. `coupon_issue_fact` 기반 집계 구조를 설계한다**

다음 요구사항을 만족해야 한다.

```text
coupon_issue_id 중복 저장 방지
event_message_id 추적
coupon_event_id와 user_id 저장
발급 발생 시각 저장
Fact가 실제 INSERT된 경우에만 집계 +1
Fact INSERT와 집계 변경을 같은 트랜잭션으로 처리
```

```sql
CREATE TABLE coupon_issue_fact (


);
```

```sql
BEGIN;

WITH accepted AS (


)
UPDATE coupon_issue_stats
SET issued_count = issued_count + 1
WHERE coupon_event_id = :couponEventId
  AND EXISTS (

  );

COMMIT;
```

### **6. 세 Consumer의 멱등 처리 대상을 비교한다**

| Consumer | 한 번만 반영해야 하는 비즈니스 결과 | 추가 제약 또는 Idempotency Key | 외부 시스템 포함 여부 |
| --- | --- | --- | --- |
| MyPage |  |  |  |
| Notification |  |  |  |
| Analytics |  |  |  |

---

## **과제 6. 최종적 일관성과 사용자 경험 설계하기**

### **1. 다음 시간 흐름에서 각 상태를 작성한다**

```text
10:00:00.000  Coupon Service DB ISSUED Commit
10:00:00.030  CouponIssued Kafka 저장
10:00:04.500  MyPage Consumer 처리 시작
10:00:05.200  user_coupon_view Commit
```

| 시각 | 요청 상태 API | 마이페이지 쿠폰함 | Projection 상태 |
| --- | --- | --- | --- |
| 10:00:00.010 |  |  |  |
| 10:00:02.000 |  |  |  |
| 10:00:05.300 |  |  |  |

### **2. 다음 상태가 서로 같은 의미인지 판단한다**

| 상태 비교 | 같음/다름 | 이유 |
| --- | --- | --- |
| DB `ISSUED` vs Kafka Record 존재 |  |  |
| Kafka Record 존재 vs MyPage 반영 완료 |  |  |
| MyPage Group Offset Commit vs Notification 완료 |  |  |
| Outbox `PUBLISHED` vs 모든 Consumer 완료 |  |  |

### **3. 발급 요청과 조회 API의 역할을 작성한다**

```text
POST /api/coupon-events/{couponEventId}/issue
→

GET /api/coupon-issue-requests/{requestId}
→

GET /api/users/{userId}/coupons
→
```

### **4. 발급 직후 사용자 UX를 설계한다**

다음 상황의 응답 또는 화면 문구를 작성한다.

```text
상황 A:
request = PENDING

상황 B:
request = ISSUED지만 MyPage에 아직 없음

상황 C:
request = FAILED

상황 D:
MyPage Projection Delay가 서비스 목표를 초과함
```

```text
상황 A:


상황 B:


상황 C:


상황 D:

```

### **5. Projection Delay를 계산한다**

위 시간 흐름에서 다음 값을 계산한다.

```text
occurredAt:
10:00:00.000

user_coupon_view Commit:
10:00:05.200

Projection Delay:

```

### **6. 최종적 일관성이 무제한 지연을 의미하지 않는 이유를 설명한다**

```text
허용할 지연 목표:


목표 초과 시 확인할 지표:


장시간 불일치를 복구할 방법:

```

---

## **과제 7. 이벤트 순서와 버전 처리하기**

다음 두 이벤트가 같은 `couponIssueId = 5001`에서 발생했다고 가정한다.

```text
CouponIssued
aggregateVersion = 1
status = ISSUED

CouponCancelled
aggregateVersion = 2
status = CANCELLED
```

### **1. 같은 Partition Key가 순서에 도움을 주는 조건을 작성한다**

```text
같은 Topic이어야 하는가?


같은 Key가 보장하는 범위:


전역 순서를 보장하는가?


서로 다른 Topic 사이의 순서를 보장하는가?

```

### **2. 비즈니스 발생 순서와 Kafka 저장 순서가 달라지는 시나리오를 완성한다**

다음을 포함한다.

```text
여러 Publisher
SKIP LOCKED
version 1 발행 실패
Backoff
version 2 선행 발행
```

```text
DB 발생 순서:


Publisher 처리 순서:


Kafka 저장 순서:

```

### **3. `schemaVersion`과 `aggregateVersion`을 비교한다**

| 필드 | 무엇의 버전인가? | 어떤 문제를 다루는가? | 값이 증가하는 예시 |
| --- | --- | --- | --- |
| `schemaVersion` |  |  |  |
| `aggregateVersion` |  |  |  |

### **4. 최신 상태 Projection의 UPSERT를 완성한다**

```sql
INSERT INTO user_coupon_view (
    coupon_issue_id,
    status,
    aggregate_version,
    updated_at
) VALUES (
    :couponIssueId,
    :status,
    :aggregateVersion,
    CURRENT_TIMESTAMP
)
ON CONFLICT (coupon_issue_id)
DO UPDATE
SET


WHERE

;
```

### **5. Version 2가 먼저 도착한 경우를 분석한다**

```text
먼저 처리:
CouponCancelled(version=2)

나중 처리:
CouponIssued(version=1)

최종 MyPage status:


최종 aggregate_version:


version=1 UPSERT의 affected rows:

```

### **6. 낮은 Version을 생략할 수 있는 범위를 판단한다**

| Consumer | 낮은 Version 생략 가능 여부 | 필요한 전제 또는 추가 순서 정책 |
| --- | --- | --- |
| 완전한 최신 상태만 저장하는 MyPage |  |  |
| 모든 발급·취소를 집계하는 Analytics |  |  |
| 발급·취소 알림을 각각 보내는 Notification |  |  |

### **7. Version Gap 처리 정책을 작성한다**

현재 Version이 2인데 Version 4가 먼저 도착했다고 가정한다.

```text
Gap으로 판단하는 조건:


즉시 반영해도 되는 Consumer:


모든 전이가 필요한 Consumer의 처리:


복구 방법:

```

### **8. 이벤트 스키마 변경의 호환성을 판단한다**

| 변경 | 호환 가능성 | 이유와 배포 방법 |
| --- | --- | --- |
| 선택 필드 `couponDisplayName` 추가 |  |  |
| 기존 `userId` 필드 삭제 |  |  |
| `userId` 의미를 이메일 주소로 변경 |  |  |
| 호환되지 않는 새 Payload를 같은 `schemaVersion=1`로 발행 |  |  |
| 불필요한 전화번호와 비밀번호를 이벤트에 추가 |  |  |

---

## **과제 8. Read Model 재구축과 운영 지표 설계하기**

### **1. 원본 데이터와 파생 데이터를 구분한다**

| 데이터 | Source of Truth인가? | 삭제 후 재구축 가능한가? | 재구축 근거 |
| --- | --- | --- | --- |
| Coupon Service `coupon_issue` |  |  |  |
| MyPage `user_coupon_view` |  |  |  |
| Analytics `coupon_issue_fact` |  |  |  |
| Analytics `coupon_issue_stats` |  |  |  |

### **2. `user_coupon_view_v2` 재구축 순서를 작성한다**

다음 단계를 안전한 순서로 배치하고 각 단계의 목적을 설명한다.

```text
새 Projection 테이블 생성
원본 DB Snapshot Backfill
새 Consumer Group으로 Kafka Replay
Backfill 경계 이후 이벤트 Catch-up
원본과 v2 정합성 검증
조회 API 전환
기존 Projection 보관 또는 제거
```

```text
1.

2.

3.

4.

5.

6.

7.
```

### **3. Kafka 보관 기간이 지난 경우의 복구 방법을 설명한다**

```text
Kafka만으로 전체 Replay가 가능한가?


필요한 다른 복구 기준:


Backfill 중 새 이벤트가 들어올 때 필요한 경계:

```

### **4. 운영 지표를 완성한다**

| 지표 | 의미 | 장애 신호 예시 |
| --- | --- | --- |
| Group별 Consumer Lag |  |  |
| Oldest Unprocessed Age |  |  |
| Projection Delay |  |  |
| Handler Failure Count |  |  |
| Duplicate Event Count |  |  |
| Read Model Mismatch Count |  |  |

### **5. 다음 장애 상황에서 확인할 지표를 선택한다**

```text
상황:
쿠폰 발급은 정상인데 10분 전 발급한 쿠폰이
마이페이지에 계속 보이지 않는다.
```

```text
가장 먼저 확인할 Consumer Group:


확인할 지표:


로그에서 연결할 식별자:


Coupon Service 발급을 다시 실행해도 되는가?

```

### **6. 로그에 남길 상관관계 ID를 작성한다**

```text
1.

2.

3.

4.

5.
```

각 ID로 추적할 수 있는 범위도 간단히 설명한다.

---

## **과제 9. 잘못된 EDA 구현 리뷰하기**

다음 코드는 의도적으로 여러 문제가 포함되어 있다.

```java
@KafkaListener(
    topics = "coupon.issued",
    groupId = "coupon-result"
)
public void notify(CouponIssuedEvent event) {
    smsClient.send(event.userId(), "쿠폰 발급 완료");
}

@KafkaListener(
    topics = "coupon.issued",
    groupId = "coupon-result"
)
public void updateMyPage(
    CouponIssuedEvent event,
    Acknowledgment acknowledgment
) {
    acknowledgment.acknowledge();

    if (processedEventRepository.existsById(
        event.kafkaOffset()
    )) {
        return;
    }

    userCouponViewRepository.save(
        UserCouponView.from(event)
    );

    processedEventRepository.save(
        new ProcessedEvent(event.kafkaOffset())
    );
}

@KafkaListener(
    topics = "coupon.issued",
    groupId = "coupon-result"
)
public void updateAnalytics(CouponIssuedEvent event) {
    statisticsRepository.increaseIssuedCount(
        event.couponEventId()
    );
}
```

### **1. 문제를 최소 10개 찾는다**

각 문제를 다음 표에 작성한다.

| 번호 | 문제 코드 | 발생 가능한 장애 또는 데이터 오류 | 수정 방향 |
| ---: | --- | --- | --- |
| 1 |  |  |  |
| 2 |  |  |  |
| 3 |  |  |  |
| 4 |  |  |  |
| 5 |  |  |  |
| 6 |  |  |  |
| 7 |  |  |  |
| 8 |  |  |  |
| 9 |  |  |  |
| 10 |  |  |  |

### **2. 반드시 찾아야 하는 문제를 연결한다**

| 검토 항목 | 문제가 있는 코드 | 수정 방향 |
| --- | --- | --- |
| 세 서비스가 같은 Group ID 사용 |  |  |
| 외부 SMS 직접 호출의 Dual Write |  |  |
| DB Commit 전 Offset 완료 |  |  |
| `exists` 후 `save` Race Condition |  |  |
| Kafka Offset을 멱등성 Key로 사용 |  |  |
| 조회 모델과 processed 기록의 원자성 없음 |  |  |
| Analytics `+1` 중복 반영 |  |  |
| 이벤트 Version과 순서 방어 없음 |  |  |
| 서비스별 데이터 소유권 불명확 |  |  |
| 실패·Lag 관측 정보 없음 |  |  |

### **3. 수정된 전체 구조를 그린다**

다음 구성 요소를 모두 포함한다.

```text
Kafka coupon.issued
서로 다른 세 Consumer Group
processed_event
notification_job와 Sender
user_coupon_view
coupon_issue_fact와 집계
각 Consumer DB Commit 이후 Offset 완료
```

```text



```

### **4. 수정된 구조의 보장 수준을 판단한다**

| 항목 | 보장/조건부 보장/보장하지 않음 | 이유 |
| --- | --- | --- |
| 쿠폰 발급 DB의 최종 성공 |  |  |
| 세 Group이 각자 이벤트를 소비할 기회 |  |  |
| Kafka Record의 물리적 Exactly-Once |  |  |
| MyPage 중복 반영 방지 |  |  |
| Analytics 중복 집계 방지 |  |  |
| 외부 SMS Exactly-Once |  |  |
| 발급 직후 MyPage의 즉시 일치 |  |  |

---

# **6. 보너스 과제**

## **보너스 1. 새 Consumer 추가 설계하기**

운영자 감사 기록을 만드는 `Audit Consumer`를 추가한다고 가정한다.

```text
Group ID:


소유 데이터:


멱등성 Key:


원본 이벤트에 추가로 필요한 필드:


기존 Consumer와 독립적으로 실패·복구하는 방법:

```

## **보너스 2. Projection 재구축 테스트 설계하기**

| 테스트 상황 | 준비 데이터 | 실행 방법 | 검증할 결과 |
| --- | --- | --- | --- |
| 동일 이벤트 2회 Replay |  |  |  |
| Version 2 뒤 Version 1 전달 |  |  |  |
| Backfill 중 새 이벤트 유입 |  |  |  |
| Kafka 보관 기간 이전 데이터 존재 |  |  |  |
| Consumer DB Commit 직후 종료 |  |  |  |

한 번의 정상 실행만으로 Projection 안전성을 증명할 수 없는 이유도 작성한다.

```text

```

---

# **7. 제출 전 체크리스트**

```text
[ ] 직접 호출 구조의 결합과 장애 전파를 설명했다.

[ ] CouponIssued가 이미 발생한 사실임을 설명했다.

[ ] Notification, MyPage와 Analytics에 서로 다른 Group ID를 사용했다.

[ ] 같은 Group의 Consumer가 Record를 나누어 처리함을 설명했다.

[ ] 서비스별 데이터 소유권을 구분했다.

[ ] CQRS가 반드시 MSA·DB 두 개를 의미하지 않음을 설명했다.

[ ] user_coupon_view와 조회 인덱스를 설계했다.

[ ] Read Model과 Source of Truth를 구분했다.

[ ] eventMessageId와 Kafka Offset의 차이를 설명했다.

[ ] processed_event와 조회 모델을 같은 트랜잭션에 넣었다.

[ ] DB Commit 이후 Offset을 완료하도록 설계했다.

[ ] Notification 외부 API의 Dual Write를 설명했다.

[ ] Analytics 카운터의 중복 반영을 방어했다.

[ ] DB ISSUED와 MyPage 반영 사이의 최종적 일관성을 설명했다.

[ ] 사용자에게 PENDING 또는 반영 중 상태를 보여 주는 UX를 설계했다.

[ ] schemaVersion과 aggregateVersion을 구분했다.

[ ] 낮은 aggregateVersion 처리 범위를 Consumer별로 구분했다.

[ ] Kafka 보관 기간을 고려한 Projection 재구축 방법을 작성했다.

[ ] Consumer Group별 Lag과 Projection Delay 지표를 작성했다.

[ ] 후속 작업 실패 시 쿠폰 발급을 다시 실행하지 않는다고 설명했다.
```

---

# **8. 핵심 질문**

과제를 마친 뒤 다음 질문에 한 문장씩 답한다.

```text
1. Notification, MyPage와 Analytics가 서로 다른 Group을 사용해야 하는 이유는 무엇인가?


2. CouponIssued는 왜 명령이 아니라 이미 발생한 사실이어야 하는가?


3. Coupon Service DB와 MyPage Read Model 중 발급 여부의 최종 기준은 무엇인가?


4. 같은 eventMessageId가 다른 Offset으로 전달될 수 있는 이유는 무엇인가?


5. processed_event와 Consumer 비즈니스 변경을 같은 트랜잭션에 넣는 이유는 무엇인가?


6. DB ISSUED인데 마이페이지에 쿠폰이 잠시 보이지 않을 수 있는 이유는 무엇인가?


7. aggregateVersion이 이벤트 순서 역전을 자동으로 막아 주지 않는 이유는 무엇인가?


8. 모든 Consumer가 낮은 aggregateVersion 이벤트를 버리면 안 되는 이유는 무엇인가?


9. Read Model을 다시 만들 수 있어야 하는 이유는 무엇인가?


10. Notification 장애가 발생했을 때 쿠폰 발급 로직을 다시 실행하면 안 되는 이유는 무엇인가?
```
