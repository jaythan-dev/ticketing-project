CREATE TABLE shows (
    id          UUID          PRIMARY KEY,
    name        VARCHAR(200)  NOT NULL,
    capacity    INTEGER       NOT NULL CHECK (capacity > 0),
    available   INTEGER       NOT NULL,
    -- Última linha de defesa: mesmo com um bug na aplicação, o banco recusa estoque negativo
    CONSTRAINT chk_shows_available_range CHECK (available >= 0 AND available <= capacity)
);
