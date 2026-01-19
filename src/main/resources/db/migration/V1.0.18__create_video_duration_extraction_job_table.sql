CREATE TABLE video_duration_extraction_job (
    id BIGSERIAL PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES video(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP
);

CREATE INDEX idx_video_duration_extraction_job_claimable
ON video_duration_extraction_job(id, started_at)
WHERE status = 'PENDING' OR status = 'PROCESSING';

-- Autovacuum & Storage 최적화
ALTER TABLE video_duration_extraction_job SET (
    autovacuum_vacuum_scale_factor = 0,      -- 비율 기반 끄고 절대값 사용
    autovacuum_vacuum_threshold = 1000,      -- 1,000건 변경 시 vacuum 실행
    autovacuum_vacuum_cost_delay = 0,        -- I/O 경합 없는 워커 전용이므로 즉시 실행
    fillfactor = 70                          -- 페이지의 30% 여유 공간 유지 → HOT Update 유도
)