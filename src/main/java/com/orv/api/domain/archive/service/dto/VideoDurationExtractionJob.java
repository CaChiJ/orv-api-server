package com.orv.api.domain.archive.service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoDurationExtractionJob {
    private Long id;
    private UUID videoId;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime startedAt;
}
