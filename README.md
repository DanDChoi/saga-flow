# saga-flow

Kafka 기반 이벤트 드리븐 주문 처리 파이프라인. 주문 생성 → 재고 차감 → 결제 → 알림을 각 단계별 이벤트로 분리하고, Saga 패턴으로 실패 시 보상 트랜잭션을 처리한다.

## 학습 목표

- Kafka Producer/Consumer, 컨슈머 그룹
- Outbox 패턴 (DB 트랜잭션과 이벤트 발행의 원자성 보장)
- Saga 패턴 (분산 트랜잭션의 보상 처리)
- 멱등성 처리 (중복 이벤트 방지)
- 재시도 정책 + DLQ(Dead Letter Queue)

## 상태

설계/셋업 단계.
