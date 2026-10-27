-- 서비스별 DB 분리 (마이크로서비스는 각자 자기 DB만 사용한다)
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
CREATE DATABASE notification_db;
