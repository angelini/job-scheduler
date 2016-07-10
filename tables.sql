CREATE TYPE STORE AS ENUM ('hdfs', 'file');
CREATE TYPE STATUS AS ENUM ('pending', 'running', 'failed', 'skipped', 'successful');

CREATE TABLE datasets (
    id    SERIAL    PRIMARY KEY,
    store STORE     NOT NULL,
    path  TEXT      NOT NULL,
    start TIMESTAMP NOT NULL,
    stop  TIMESTAMP NOT NULL
          CONSTRAINT stop_greater_than_start
          CHECK(stop >= start)
);

CREATE TABLE jobs (
    id     SERIAL  PRIMARY KEY,
    ds_id  INTEGER REFERENCES datasets NOT NULL,
    path   TEXT    NOT NULL
);

CREATE TABLE relations (
    id      SERIAL  PRIMARY KEY,
    from_id INTEGER REFERENCES datasets NOT NULL,
    to_id   INTEGER REFERENCES datasets NOT NULL
            CONSTRAINT vertices_not_equal
            CHECK(from_id <> to_id)
);

CREATE TABLE changes (
    id         SERIAL    PRIMARY KEY,
    ds_id      INTEGER   REFERENCES datasets NOT NULL,
    status     STATUS    NOT NULL,
    start      TIMESTAMP NOT NULL,
    stop       TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
               CONSTRAINT stop_greater_than_start
               CHECK(stop >= start)
);

CREATE TABLE executions (
    id          SERIAL    PRIMARY KEY,
    ds_id       INTEGER   REFERENCES datasets NOT NULL,
    status      STATUS    NOT NULL,
    created_at  TIMESTAMP NOT NULL,
    skip_reason TEXT
                CONSTRAINT reason_not_null_when_skipped
                CHECK((status = 'skipped' AND skip_reason IS NOT NULL) OR
                      (status <> 'skipped' AND skip_reason IS NULL))
);
