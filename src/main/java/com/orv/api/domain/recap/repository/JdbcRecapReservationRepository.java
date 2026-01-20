package com.orv.api.domain.recap.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.orv.api.domain.media.service.dto.InterviewAudioRecording;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcRecapReservationRepository implements RecapReservationRepository {
    private final JdbcTemplate jdbcTemplate;
    private final SimpleJdbcInsert simpleJdbcInsert;

    public JdbcRecapReservationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("recap_reservation")
                .usingColumns("member_id", "video_id", "scheduled_at")
                .usingGeneratedKeyColumns("id", "created_at");
    }

    @Override
    public Optional<UUID> reserveRecap(UUID memberId, UUID videoId, LocalDateTime scheduledAt) {
        Map<String, Object> params = new HashMap<>();

        params.put("member_id", memberId);
        params.put("video_id", videoId);
        params.put("scheduled_at", scheduledAt);

        KeyHolder keyHolder = (KeyHolder) simpleJdbcInsert.executeAndReturnKeyHolder(new MapSqlParameterSource(params));
        Map<String, Object> keys = keyHolder.getKeys();

        if (keys != null && keys.containsKey("id")) {
            return Optional.of((UUID) keys.get("id"));
        }

        return Optional.of(null);
    }

    @Override
    public void linkAudioRecording(UUID recapReservationId, UUID audioRecordingId) {
        String sql = "UPDATE recap_reservation SET interview_audio_recording_id = ? WHERE id = ?";
        jdbcTemplate.update(sql, audioRecordingId, recapReservationId);
    }

    @Override
    public void linkRecapResult(UUID recapReservationId, UUID recapResultId) {
        String sql = "UPDATE recap_reservation SET recap_result_id = ? WHERE id = ?";
        jdbcTemplate.update(sql, recapResultId, recapReservationId);
    }

    @Override
    public Optional<InterviewAudioRecording> findAudioByRecapReservationId(UUID recapReservationId) {
        String sql = """
                SELECT iar.id, iar.storyboard_id, iar.member_id, iar.audio_url, iar.created_at, iar.running_time
                FROM interview_audio_recording iar
                JOIN recap_reservation rr ON rr.interview_audio_recording_id = iar.id
                WHERE rr.id = ?
                """;

        return jdbcTemplate.query(sql, rs -> {
            if (rs.next()) {
                return Optional.of(InterviewAudioRecording.builder()
                        .id((UUID) rs.getObject("id"))
                        .storyboardId((UUID) rs.getObject("storyboard_id"))
                        .memberId((UUID) rs.getObject("member_id"))
                        .audioUrl(rs.getString("audio_url"))
                        .createdAt(rs.getObject("created_at", OffsetDateTime.class))
                        .runningTime(rs.getInt("running_time"))
                        .build());
            }
            return Optional.empty();
        }, recapReservationId);
    }

    @Override
    public void deleteRecapReservation(UUID recapReservationId) {
        String sql = "DELETE FROM recap_reservation WHERE id = ?";
        jdbcTemplate.update(sql, recapReservationId);
    }
}
