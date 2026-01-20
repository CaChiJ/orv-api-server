package com.orv.api.domain.recap.repository;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import com.orv.api.domain.media.service.dto.InterviewAudioRecording;

public interface RecapReservationRepository {
    Optional<UUID> reserveRecap(UUID memberId, UUID videoId, LocalDateTime scheduledAt);
    void linkAudioRecording(UUID recapReservationId, UUID audioRecordingId);
    void linkRecapResult(UUID recapReservationId, UUID recapResultId);
    Optional<InterviewAudioRecording> findAudioByRecapReservationId(UUID recapReservationId);
    void deleteRecapReservation(UUID recapReservationId);
}
