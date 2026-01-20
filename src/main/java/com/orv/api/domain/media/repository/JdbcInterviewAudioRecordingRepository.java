package com.orv.api.domain.media.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import com.orv.api.domain.media.service.dto.InterviewAudioRecording;

@Repository
public class JdbcInterviewAudioRecordingRepository implements InterviewAudioRecordingRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcInterviewAudioRecordingRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public InterviewAudioRecording save(InterviewAudioRecording audioRecording) {
        if (audioRecording.getId() == null) {
            audioRecording.setId(UUID.randomUUID());
        }
        if (audioRecording.getCreatedAt() == null) {
            audioRecording.setCreatedAt(OffsetDateTime.now(ZoneOffset.UTC));
        }

        String sql = "INSERT INTO interview_audio_recording (id, storyboard_id, member_id, audio_url, created_at, running_time) VALUES (?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql,
                audioRecording.getId(),
                audioRecording.getStoryboardId(),
                audioRecording.getMemberId(),
                audioRecording.getAudioUrl(),
                audioRecording.getCreatedAt(),
                audioRecording.getRunningTime());
        return audioRecording;
    }

    @Override
    public Optional<InterviewAudioRecording> findById(UUID id) {
        String sql = "SELECT id, storyboard_id, member_id, audio_url, created_at, running_time FROM interview_audio_recording WHERE id = ?";
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(sql, new InterviewAudioRecordingRowMapper(), id));
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public void delete(UUID id) {
        String sql = "DELETE FROM interview_audio_recording WHERE id = ?";
        jdbcTemplate.update(sql, id);
    }

    private static class InterviewAudioRecordingRowMapper implements RowMapper<InterviewAudioRecording> {
        @Override
        public InterviewAudioRecording mapRow(ResultSet rs, int rowNum) throws SQLException {
            return InterviewAudioRecording.builder()
                    .id(UUID.fromString(rs.getString("id")))
                    .storyboardId(UUID.fromString(rs.getString("storyboard_id")))
                    .memberId(UUID.fromString(rs.getString("member_id")))
                    .audioUrl(rs.getString("audio_url"))
                    .createdAt(rs.getObject("created_at", OffsetDateTime.class))
                    .runningTime(rs.getInt("running_time"))
                    .build();
        }
    }
}
