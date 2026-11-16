-- 合成临床试验数据 + 仪器标准化规则
-- 3 个试验、6 万患者、30 万条检验记录；3 台仪器 INST-A/B/C，初始规则 a=1、b=0。

CREATE TABLE patients (
    patient_id bigint PRIMARY KEY,
    trial_id   text NOT NULL
);
CREATE INDEX patients_trial_idx ON patients (trial_id);

-- 仪器表：active_* 为当前生效规则；rule_version 每次成功切换 +1。
CREATE TABLE instruments (
    instrument_id   text PRIMARY KEY,
    active_a        numeric(18, 8) NOT NULL,
    active_b        numeric(18, 8) NOT NULL,
    rule_version    bigint NOT NULL DEFAULT 1,
    rule_updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE labs (
    lab_id        bigint PRIMARY KEY,
    patient_id    bigint NOT NULL REFERENCES patients (patient_id),
    value         numeric(10, 2) NOT NULL,   -- 原测值，标准化流程永不修改
    instrument_id text
);
CREATE INDEX labs_patient_idx ON labs (patient_id);
CREATE INDEX labs_instrument_idx ON labs (instrument_id);

-- 持久化标准值：standard_value = round(a * value + b, 4)（四舍五入、远离零）。
-- 每行记录生成它时的规则号与 a/b，保证供数时原值/规则/标准值同属一个已提交状态。
CREATE TABLE lab_standard (
    lab_id         bigint PRIMARY KEY REFERENCES labs (lab_id) ON DELETE CASCADE,
    instrument_id  text NOT NULL,
    standard_value numeric(20, 4) NOT NULL,
    rule_version   bigint NOT NULL,
    a              numeric(18, 8) NOT NULL,
    b              numeric(18, 8) NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX lab_standard_instrument_idx ON lab_standard (instrument_id);

-- 后台重算轮次：每仪器同一时刻只允许一轮 running（部分唯一索引兜底）。
CREATE TABLE norm_jobs (
    job_id        bigserial PRIMARY KEY,
    instrument_id text NOT NULL,
    pending_a     numeric(18, 8) NOT NULL,
    pending_b     numeric(18, 8) NOT NULL,
    batch_delay   numeric(8, 3) NOT NULL DEFAULT 0,
    status        text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    pass_no       integer NOT NULL DEFAULT 0,
    total_labs    bigint NOT NULL DEFAULT 0,
    done_labs     bigint NOT NULL DEFAULT 0,
    error         text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    started_at    timestamptz,
    finished_at   timestamptz
);
CREATE INDEX norm_jobs_inst_idx ON norm_jobs (instrument_id, job_id DESC);
CREATE UNIQUE INDEX norm_jobs_one_running
    ON norm_jobs (instrument_id) WHERE status = 'running';

-- 重算暂存表：后台把新规则结果写这里，切换事务才发布到 lab_standard。
CREATE TABLE norm_job_lab (
    job_id         bigint NOT NULL REFERENCES norm_jobs (job_id) ON DELETE CASCADE,
    lab_id         bigint NOT NULL,
    instrument_id  text NOT NULL,
    raw_value      numeric(10, 2) NOT NULL,
    standard_value numeric(20, 4) NOT NULL,
    PRIMARY KEY (job_id, lab_id)
);
CREATE INDEX norm_job_lab_job_idx ON norm_job_lab (job_id, lab_id);

-- 应用层新增检验取号用（合成数据 lab_id 最大约 60 万）。
CREATE SEQUENCE lab_id_seq START WITH 1000000;

-- ---------------------------------------------------------------- 合成数据
INSERT INTO patients
SELECT g, 'T-' || (100 + (g % 3) * 100)
FROM generate_series(1, 60000) AS g;

INSERT INTO instruments (instrument_id, active_a, active_b)
VALUES ('INST-A', 1, 0), ('INST-B', 1, 0), ('INST-C', 1, 0);

INSERT INTO labs (lab_id, patient_id, value, instrument_id)
SELECT g * 10 + k,
       g,
       round((random() * 200 - 50)::numeric, 2),
       'INST-' || chr(65 + (g % 3))
FROM generate_series(1, 60000) AS g,
     generate_series(0, 4) AS k;

-- 种子标准值按初始规则 a=1、b=0 批量生成（之后的增改删交给触发器维护）。
INSERT INTO lab_standard (lab_id, instrument_id, standard_value, rule_version, a, b)
SELECT l.lab_id, l.instrument_id, round(l.value, 4), 1, 1, 0
FROM labs l
WHERE l.instrument_id IS NOT NULL;

-- ------------------------------------------------- 触发器：持续维护标准值
-- 任何连接（含独立 SQL 连接）对 labs 的增改删都在同一事务内维护 lab_standard，
-- 使“原值、规则、标准值同属一个已提交状态”对所有写入方成立。
-- 重算切换期间，对该仪器的写会在 advisory lock 上短暂等待（仅最终切换挡写）。
CREATE OR REPLACE FUNCTION labs_standard_maintain() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_a numeric(18, 8);
    v_b numeric(18, 8);
    v_ver bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.instrument_id IS NOT NULL THEN
            PERFORM pg_advisory_xact_lock(910017, hashtext(OLD.instrument_id));
        END IF;
        DELETE FROM lab_standard WHERE lab_id = OLD.lab_id;
        RETURN OLD;
    END IF;

    IF NEW.instrument_id IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(910017, hashtext(NEW.instrument_id));
        SELECT active_a, active_b, rule_version INTO v_a, v_b, v_ver
        FROM instruments WHERE instrument_id = NEW.instrument_id;
        IF NOT FOUND THEN
            -- 未注册仪器：无标准值行，待注册/换规则后由重算补齐
            DELETE FROM lab_standard WHERE lab_id = NEW.lab_id;
            RETURN NEW;
        END IF;
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.instrument_id IS NOT NULL THEN
            INSERT INTO lab_standard
                (lab_id, instrument_id, standard_value, rule_version, a, b, updated_at)
            VALUES (NEW.lab_id, NEW.instrument_id,
                    round(v_a * NEW.value + v_b, 4), v_ver, v_a, v_b, now())
            ON CONFLICT (lab_id) DO UPDATE
              SET instrument_id = EXCLUDED.instrument_id,
                  standard_value = EXCLUDED.standard_value,
                  rule_version = EXCLUDED.rule_version,
                  a = EXCLUDED.a, b = EXCLUDED.b, updated_at = now();
        END IF;
        RETURN NEW;
    END IF;

    -- UPDATE
    IF NEW.instrument_id IS DISTINCT FROM OLD.instrument_id THEN
        DELETE FROM lab_standard WHERE lab_id = NEW.lab_id;
        IF NEW.instrument_id IS NOT NULL THEN
            INSERT INTO lab_standard
                (lab_id, instrument_id, standard_value, rule_version, a, b, updated_at)
            VALUES (NEW.lab_id, NEW.instrument_id,
                    round(v_a * NEW.value + v_b, 4), v_ver, v_a, v_b, now());
        END IF;
    ELSIF NEW.value IS DISTINCT FROM OLD.value AND NEW.instrument_id IS NOT NULL THEN
        UPDATE lab_standard
           SET standard_value = round(v_a * NEW.value + v_b, 4),
               updated_at = now()
         WHERE lab_id = NEW.lab_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER labs_standard_trg
AFTER INSERT OR UPDATE OR DELETE ON labs
FOR EACH ROW EXECUTE FUNCTION labs_standard_maintain();

ANALYZE patients;
ANALYZE instruments;
ANALYZE labs;
ANALYZE lab_standard;
