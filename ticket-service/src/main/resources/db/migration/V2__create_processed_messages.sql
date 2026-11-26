-- Registro das mensagens já processadas, para o consumer ser idempotente.
-- O resultado fica guardado para que uma entrega repetida da mesma mensagem possa
-- reenviar a mesma resposta sem mexer de novo no estoque.
CREATE TABLE processed_messages (
                                    message_id    UUID         PRIMARY KEY,
                                    result        VARCHAR(30)  NOT NULL,
                                    processed_at  TIMESTAMPTZ  NOT NULL
);