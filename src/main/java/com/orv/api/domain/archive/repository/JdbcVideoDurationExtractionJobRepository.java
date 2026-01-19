package com.orv.api.domain.archive.repository;

import com.orv.api.domain.archive.service.dto.VideoDurationExtractionJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
@Slf4j
public class JdbcVideoDurationExtractionJobRepository implements VideoDurationExtractionJobRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcVideoDurationExtractionJobRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void create(UUID videoId) {
        String sql = "INSERT INTO video_duration_extraction_job (video_id, status) VALUES (?, 'PENDING')";
        jdbcTemplate.update(sql, videoId);
    }

    @Override
    @Transactional
    public Optional<VideoDurationExtractionJob> claimNext(Duration stuckThreshold) {
        try {
            // 1. PENDING이거나, PROCESSING이지만 started_at이 stuckThreshold 이상 지난 것 조회
            String selectSql = "SELECT id, video_id, status, created_at, started_at " +
                    "FROM video_duration_extraction_job " +
                    "WHERE status = 'PENDING' " +
                    "   OR (status = 'PROCESSING' AND started_at < ?) " +
                    "ORDER BY id " +
                    "LIMIT 1 " +
                    "FOR UPDATE SKIP LOCKED";

            LocalDateTime stuckThresholdTime = LocalDateTime.now().minus(stuckThreshold);
            VideoDurationExtractionJob job = jdbcTemplate.queryForObject(
                    selectSql,
                    new VideoDurationExtractionJobRowMapper(),
                    Timestamp.valueOf(stuckThresholdTime)
            );

            if (job == null) {
                return Optional.empty();
            }

            // 2. PROCESSING 상태로 변경 및 started_at 업데이트
            String updateSql = "UPDATE video_duration_extraction_job " +
                    "SET status = 'PROCESSING', started_at = ? " +
                    "WHERE id = ?";
            jdbcTemplate.update(updateSql, Timestamp.valueOf(LocalDateTime.now()), job.getId());

            // 3. 업데이트된 상태로 반환
            job.setStatus("PROCESSING");
            job.setStartedAt(LocalDateTime.now());

            return Optional.of(job);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public void markCompleted(Long jobId) {
        String sql = "UPDATE video_duration_extraction_job SET status = 'COMPLETED' WHERE id = ?";
        jdbcTemplate.update(sql, jobId);
    }

    @Override
    public void markFailed(Long jobId) {
        String sql = "UPDATE video_duration_extraction_job SET status = 'FAILED' WHERE id = ?";
        jdbcTemplate.update(sql, jobId);
    }

    private static class VideoDurationExtractionJobRowMapper implements RowMapper<VideoDurationExtractionJob> {
        @Override
        public VideoDurationExtractionJob mapRow(ResultSet rs, int rowNum) throws SQLException {
            VideoDurationExtractionJob job = new VideoDurationExtractionJob();
            job.setId(rs.getLong("id"));
            job.setVideoId(UUID.fromString(rs.getString("video_id")));
            job.setStatus(rs.getString("status"));

            Timestamp createdAt = rs.getTimestamp("created_at");
            if (createdAt != null) {
                job.setCreatedAt(createdAt.toLocalDateTime());
            }

            Timestamp startedAt = rs.getTimestamp("started_at");
            if (startedAt != null) {
                job.setStartedAt(startedAt.toLocalDateTime());
            }

            return job;
        }
    }
}
