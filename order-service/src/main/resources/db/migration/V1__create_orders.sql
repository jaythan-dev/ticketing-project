CREATE TABLE orders (
                        id          UUID         PRIMARY KEY,
                        user_id     UUID         NOT NULL,
                        show_id     UUID         NOT NULL,
                        quantity    INTEGER      NOT NULL CHECK (quantity > 0),
                        status      VARCHAR(20)  NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'CANCELLED')),
                        created_at  TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_orders_status ON orders (status);