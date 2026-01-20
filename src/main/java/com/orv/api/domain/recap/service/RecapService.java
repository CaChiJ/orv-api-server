package com.orv.api.domain.recap.service;

import com.orv.api.domain.recap.service.dto.RecapAudioInfo;
import com.orv.api.domain.recap.service.dto.RecapContent;
import com.orv.api.domain.recap.service.dto.RecapResultInfo;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface RecapService {
    Optional<UUID> reserveRecap(UUID memberId, UUID videoId, ZonedDateTime scheduledAt);
    void linkAudioRecording(UUID recapReservationId, UUID audioRecordingId);
    Optional<UUID> saveRecapResult(UUID recapReservationId, List<RecapContent> recapContent);
    Optional<RecapResultInfo> getRecapResult(UUID recapReservationId);
    Optional<RecapAudioInfo> getRecapAudio(UUID recapReservationId);
    void deleteRecapReservation(UUID recapReservationId);
}
