package com.orv.api.domain.archive.repository;

import com.orv.api.domain.archive.service.dto.VideoDurationExtractionJob;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

public interface VideoDurationExtractionJobRepository {
    void create(UUID videoId);

    Optional<VideoDurationExtractionJob> claimNext(Duration stuckThreshold);

    void markCompleted(Long jobId);

    void markFailed(Long jobId);
}
