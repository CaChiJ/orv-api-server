package com.orv.api.domain.media.repository;

import java.util.Optional;
import java.util.UUID;

import com.orv.api.domain.media.service.dto.InterviewAudioRecording;

public interface InterviewAudioRecordingRepository {
    InterviewAudioRecording save(InterviewAudioRecording audioRecording);
    Optional<InterviewAudioRecording> findById(UUID id);
    void delete(UUID id);
}
