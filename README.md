# saga-flow

Kafka 기반 이벤트 드리븐 주문 처리 파이프라인. 주문 생성 → 재고 차감 → 결제 → 알림을 각 단계별 이벤트로 분리하고, Saga 패턴으로 실패 시 보상 트랜잭션을 처리한다.

## 학습 목표

- Kafka Producer/Consumer, 컨슈머 그룹
- Outbox 패턴 (DB 트랜잭션과 이벤트 발행의 원자성 보장)
- Saga 패턴 (분산 트랜잭션의 보상 처리)
- 멱등성 처리 (중복 이벤트 방지)
- 재시도 정책 + DLQ(Dead Letter Queue)

## 상태

정상 흐름(Happy path) 구현 완료. 보상 트랜잭션, Outbox, 멱등성, DLQ는 진행 중.

## 로컬 인프라 실행

```bash
docker compose up -d
```

- Kafka (KRaft, 단일 브로커): `localhost:9092`
- Kafka UI: http://localhost:8989
- PostgreSQL: `localhost:5432` (user/password: `saga`/`saga`)
  - 서비스별 DB: `order_db`, `inventory_db`, `payment_db`, `notification_db`
  - 초기화 스크립트는 최초 기동 시에만 실행된다. 다시 만들려면 `docker compose down -v`

## 전체 흐름 수동 검증

```
POST /orders ─▶ order-service ──ORDER_CREATED──▶ inventory-service ──STOCK_RESERVED──▶ payment-service
                     ▲                                  │ STOCK_FAILED                       │
                     └──────────── PAYMENT_COMPLETED / PAYMENT_FAILED ───────────────────────┘
                                    (notification-service는 결과 이벤트를 구독해 알림 기록)
```

| 서비스 | 포트 | DB | 구독 토픽 | 발행 토픽 |
|---|---|---|---|---|
| order-service | 8081 | `order_db` | `inventory-events`, `payment-events` | `order-events` |
| inventory-service | 8082 | `inventory_db` | `order-events`, `payment-events` | `inventory-events` |
| payment-service | 8083 | `payment_db` | `inventory-events` | `payment-events` |
| notification-service | 8084 | `notification_db` | `inventory-events`, `payment-events` | - |

### 1. 준비

```bash
docker compose up -d
```

서비스 4개를 각각 별도 터미널에서 실행한다. (테이블은 각 서비스가 기동할 때 만든다)

```bash
./gradlew :order-service:bootRun
./gradlew :inventory-service:bootRun
./gradlew :payment-service:bootRun
./gradlew :notification-service:bootRun
```

`inventory-service`가 뜬 뒤 재고를 넣는다. (상품 100번 재고 10개)

```bash
docker exec saga-postgres psql -U saga -d inventory_db \
  -c "INSERT INTO stock (product_id, quantity) VALUES (100, 10);"
```

### 2. 시나리오

**A. 정상: 재고 확보 + 결제 성공 → 주문 `COMPLETED`**

```bash
curl -X POST localhost:8081/orders -H 'Content-Type: application/json' \
  -d '{"customerId": 1, "productId": 100, "quantity": 3, "unitPrice": 5000}'
```

총액 15,000원. 재고는 10 → 7로 줄고, 주문은 `COMPLETED`가 된다.

**B. 결제 실패: 총액이 결제 한도(1,000,000원)를 넘음 → 주문 `CANCELLED`**

```bash
curl -X POST localhost:8081/orders -H 'Content-Type: application/json' \
  -d '{"customerId": 1, "productId": 100, "quantity": 2, "unitPrice": 600000}'
```

총액 1,200,000원. `PAYMENT_FAILED`가 발행되어 주문은 `CANCELLED`가 된다.
inventory-service가 이 이벤트를 받아 차감했던 재고를 되돌리고(보상) `STOCK_RELEASED`를 발행하므로, 재고는 원래 수량으로 돌아온다.

**C. 재고 부족**

```bash
curl -X POST localhost:8081/orders -H 'Content-Type: application/json' \
  -d '{"customerId": 1, "productId": 100, "quantity": 999, "unitPrice": 100}'
```

`STOCK_FAILED`가 발행되고 알림이 기록되며, 주문은 `CANCELLED`가 된다. 재고는 차감된 적이 없으므로 되돌릴 것도 없다.

### 3. 결과 확인

```bash
# 주문 상태
docker exec saga-postgres psql -U saga -d order_db \
  -c "SELECT id, status, total_amount FROM orders ORDER BY id;"
# 재고
docker exec saga-postgres psql -U saga -d inventory_db \
  -c "SELECT * FROM stock;"
# 결제
docker exec saga-postgres psql -U saga -d payment_db \
  -c "SELECT order_id, status, amount, failure_reason FROM payment ORDER BY id;"
# 알림
docker exec saga-postgres psql -U saga -d notification_db \
  -c "SELECT order_id, event_type, message FROM notification ORDER BY id;"
```

Kafka UI(http://localhost:8989)의 Topics에서 `order-events`, `inventory-events`, `payment-events`의 메시지도 볼 수 있다.

### 문제가 생기면

- **주문이 `CREATED`에서 안 바뀐다**: 서비스 로그를 순서대로(order → inventory → payment) 확인한다. 어느 단계 로그가 끊겼는지가 곧 이벤트가 멈춘 지점이다.
- **서비스가 DB에 연결하지 못한다**: 로컬 PostgreSQL이 5432를 이미 쓰고 있으면 compose의 postgres와 포트가 충돌한다. 로컬 PostgreSQL을 끄거나 compose의 호스트 포트를 바꾸고 각 서비스 `application.yml`의 datasource URL도 맞춘다.
- **이미 쌓인 이벤트를 다시 처리하고 싶다**: 컨슈머 그룹의 offset을 지우거나 `docker compose down -v`로 처음부터 시작한다.
