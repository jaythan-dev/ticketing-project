\# Ticketing Platform



Plataforma de venda de ingressos baseada em \*\*microserviços orientados a eventos\*\*, construída para praticar o projeto, o teste, a escala e o deploy de um back end Java. O cenário escolhido é o de \*\*alta concorrência\*\*: muitos clientes disputando um estoque limitado de ingressos (como num flash sale), onde a regra mais importante é nunca vender mais do que existe.



> \*\*Status:\*\* em desenvolvimento. O `order-service` está funcionando; o `ticket-service` é o próximo passo. Veja o \[roadmap](#roadmap).



\## Arquitetura



```mermaid

flowchart LR

&#x20;   Client(\[Cliente]) -->|POST /orders| OS\[order-service]

&#x20;   OS --> ODB\[(ordersdb)]

&#x20;   OS -->|OrderCreated| T1{{orders.created}}

&#x20;   T1 --> TS\[ticket-service<br/><i>planejado</i>]

&#x20;   TS --> TDB\[(ticketsdb)]

&#x20;   TS -->|TicketsReserved| T2{{tickets.reserved}}

&#x20;   TS -->|TicketsReservationFailed| T3{{tickets.reservation-failed}}

&#x20;   T2 --> OS

&#x20;   T3 --> OS

```



| Serviço | Responsabilidade | Banco |

|---|---|---|

| `order-service` | Ciclo de vida do pedido (`PENDING`, `CONFIRMED`, `CANCELLED`) | `ordersdb` |

| `ticket-service` \*(planejado)\* | Catálogo de shows e estoque de ingressos; reserva e liberação | `ticketsdb` |



Os serviços \*\*não acessam o banco um do outro\*\*: cada um é dono dos seus dados e a comunicação acontece só por eventos no Kafka.



\### Fluxo de uma compra



1\. O cliente chama `POST /orders`.

2\. O `order-service` grava o pedido como `PENDING` e publica `OrderCreated` em `orders.created`.

3\. O `ticket-service` consome o evento, tenta reservar os ingressos e publica `TicketsReserved` ou `TicketsReservationFailed`.

4\. O `order-service` consome a resposta e atualiza o pedido para `CONFIRMED` ou `CANCELLED`.



A API responde `202 Accepted` logo após o passo 2. O resultado final é consultado depois com `GET /orders/{id}`, o que caracteriza consistência eventual.



\## Stack



\- Java 25 e Spring Boot 4.1

\- Spring Data JPA (Hibernate) e PostgreSQL 16

\- Flyway para migrations versionadas (o Hibernate apenas valida o esquema)

\- Apache Kafka em modo KRaft, sem Zookeeper

\- Docker e Docker Compose

\- Maven (com wrapper)



\## Como rodar



\### Pré-requisitos



\- JDK 25

\- Docker Desktop em execução



\### 1. Subir a infraestrutura



```bash

docker compose up -d

```



Isso inicia o Kafka, cria os tópicos (`orders.created`, `tickets.reserved`, `tickets.reservation-failed`), dois bancos PostgreSQL e o Kafka UI.



| Componente | Endereço |

|---|---|

| Kafka (para aplicações na máquina) | `localhost:9092` |

| Kafka UI | http://localhost:8090 |

| PostgreSQL de pedidos | `localhost:5433` (db `ordersdb`, usuário e senha `orders`) |

| PostgreSQL de tickets | `localhost:5434` (db `ticketsdb`, usuário e senha `tickets`) |



As credenciais acima são apenas de ambiente local.



\### 2. Rodar o order-service



```bash

cd order-service

./mvnw spring-boot:run

```



No Windows, use `mvnw.cmd spring-boot:run`. O serviço sobe em http://localhost:8081 e aplica as migrations do Flyway automaticamente.



\### 3. Fazer um pedido



```bash

curl -X POST http://localhost:8081/orders \\

&#x20; -H "Content-Type: application/json" \\

&#x20; -d '{

&#x20;   "userId": "0b9f3f0e-5a38-4a6c-9d5e-1a2b3c4d5e6f",

&#x20;   "showId": "7c1d2e3f-4a5b-4c6d-8e9f-0a1b2c3d4e5f",

&#x20;   "quantity": 2

&#x20; }'

```



No PowerShell, use `curl.exe` e escape as aspas do JSON, ou envie a requisição pelo Postman.



A resposta é `202 Accepted` com o pedido em `PENDING`. Para conferir o evento, abra o Kafka UI, entre no tópico `orders.created` e veja a mensagem, cuja chave é o `showId`.



\### Parar e limpar



```bash

docker compose stop        # pausa os containers

docker compose down        # remove os containers e mantém os dados

docker compose down -v     # remove tudo, inclusive bancos e tópicos

```



\## API do order-service



| Método | Caminho | Descrição | Respostas |

|---|---|---|---|

| `POST` | `/orders` | Cria um pedido e publica `OrderCreated` | `202`, `400` |

| `GET` | `/orders/{id}` | Consulta um pedido | `200`, `404` |



Regras de validação do `POST /orders`: `userId` e `showId` são UUIDs obrigatórios, e `quantity` deve estar entre 1 e 10.



\## Contrato de eventos



Todos os eventos carregam `messageId` (UUID da mensagem, base da idempotência nos consumers) e `occurredAt`.



| Tópico | Evento | Publicado por | Consumido por | Chave |

|---|---|---|---|---|

| `orders.created` | `OrderCreated` | order-service | ticket-service | `showId` |

| `tickets.reserved` | `TicketsReserved` | ticket-service | order-service | `orderId` |

| `tickets.reservation-failed` | `TicketsReservationFailed` | ticket-service | order-service | `orderId` |



Campos de cada evento:



\- `OrderCreated`: `messageId`, `occurredAt`, `orderId`, `userId`, `showId`, `quantity`

\- `TicketsReserved`: `messageId`, `occurredAt`, `orderId`, `showId`, `quantity`

\- `TicketsReservationFailed`: `messageId`, `occurredAt`, `orderId`, `showId`, `reason`



\## Decisões de design



\- \*\*Um banco por serviço.\*\* Evita acoplamento ao esquema alheio e mantém a regra de estoque num único lugar.

\- \*\*`showId` como chave de `orders.created`.\*\* Todos os pedidos do mesmo show vão para a mesma partição e são processados em ordem. O custo é a possibilidade de uma chave quente num flash sale, tema que será medido nos testes de carga.

\- \*\*Resposta assíncrona (`202`).\*\* O pedido é aceito de imediato e confirmado depois, sem prender o cliente à reserva de estoque.

\- \*\*Flyway no controle do esquema.\*\* O Hibernate roda com `ddl-auto: validate`, então toda mudança de tabela passa por uma migration versionada.

\- \*\*Microserviços por objetivo de aprendizado.\*\* Num produto real deste tamanho, um monolito modular seria uma escolha razoável. Aqui a arquitetura distribuída é intencional, para praticar Kafka, sagas, idempotência e escala independente.



\## Limitações conhecidas



\- \*\*Gravar no banco e publicar no Kafka são duas operações separadas.\*\* Se o serviço cair entre as duas, o pedido fica `PENDING` para sempre. A correção planejada é o \*\*outbox pattern\*\*.

\- Ainda não há consumers nem tratamento de mensagens inválidas (dead letter topic).

\- Ainda não há autenticação nem gateway.



\## Estrutura do repositório



```

ticketing-platform/

├── docker-compose.yml     # Kafka, bancos e Kafka UI

├── order-service/         # Spring Boot: pedidos

│   └── src/main/

│       ├── java/com/ticketing/order/

│       └── resources/db/migration/

└── ...

```



\## Roadmap



\- \[x] Infraestrutura local com Docker Compose (Kafka KRaft, dois PostgreSQL, Kafka UI)

\- \[x] `order-service`: `POST /orders` e `GET /orders/{id}` publicando `OrderCreated`

\- \[ ] Teste de integração do fluxo com Testcontainers

\- \[ ] Tratamento padronizado de erros da API

\- \[ ] `ticket-service`: catálogo, estoque e reserva

\- \[ ] Consumers no `order-service` (`CONFIRMED` e `CANCELLED`)

\- \[ ] Idempotência nos consumers, retry e dead letter topic

\- \[ ] Outbox pattern

\- \[ ] Pipeline de CI com GitHub Actions

\- \[ ] Observabilidade (Prometheus e Grafana)

\- \[ ] Testes de carga (Gatling ou k6) e otimização do gargalo de estoque

\- \[ ] Deploy na AWS (ECS com Fargate) com Terraform e autoscaling

\- \[ ] ADRs e documentação de resultados

