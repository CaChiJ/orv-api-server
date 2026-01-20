package com.orv.api.domain.archive.controller;

import com.orv.api.domain.archive.controller.dto.VideoMetadataUpdateForm;
import com.orv.api.domain.archive.controller.dto.VideoResponse;
import com.orv.api.domain.archive.orchestrator.ArchiveOrchestrator;
import com.orv.api.global.dto.ApiResponse;
import com.orv.api.global.dto.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/v0/archive")
@RequiredArgsConstructor
@Slf4j
public class ArchiveController {
    private final ArchiveOrchestrator archiveOrchestrator;

    @Deprecated
    @PostMapping("/recorded-video")
    public ApiResponse uploadRecordedVideo(@RequestParam("video") MultipartFile video, @RequestParam("storyboardId") String storyboardId) {
        try {
            log.warn("storyboardId: {}", storyboardId);

            String memberId = SecurityContextHolder.getContext().getAuthentication().getName();
            Optional<String> videoId = archiveOrchestrator.uploadRecordedVideo(
                    video.getInputStream(),
                    video.getContentType(),
                    video.getSize(),
                    UUID.fromString(storyboardId),
                    UUID.fromString(memberId)
            );

            if (videoId.isEmpty()) {
                log.warn("Failed to save video");
                return ApiResponse.fail(ErrorCode.UNKNOWN, 500);
            }

            return ApiResponse.success(videoId.get(), 201);
        } catch (IOException e) {
            e.printStackTrace();
            return ApiResponse.fail(ErrorCode.UNKNOWN, 500);
        }
    }


    @GetMapping("/video/{videoId}")
    public ApiResponse getVideo(@PathVariable String videoId) {
        Optional<VideoResponse> foundVideo = archiveOrchestrator.getVideo(UUID.fromString(videoId));

        if (foundVideo.isEmpty()) {
            return ApiResponse.fail(ErrorCode.NOT_FOUND, 404);
        }

        return ApiResponse.success(foundVideo.get(), 200);
    }

    @GetMapping("/videos/my")
    public ApiResponse getMyVideos() {
        String memberId = SecurityContextHolder.getContext().getAuthentication().getName();
        List<VideoResponse> videos = archiveOrchestrator.getMyVideos(UUID.fromString(memberId), 0, 100);
        return ApiResponse.success(videos, 200);
    }

    @PatchMapping("/video/{videoId}")
    public ApiResponse changeVideoMetadata(@PathVariable("videoId") String videoId, @RequestBody VideoMetadataUpdateForm updateForm) {
        try {
            if (updateForm.getTitle() != null && !updateForm.getTitle().isEmpty()) {
                boolean isSuccessful = archiveOrchestrator.updateVideoTitle(UUID.fromString(videoId), updateForm.getTitle());

                if (!isSuccessful) {
                    return ApiResponse.fail(ErrorCode.UNKNOWN, 500);
                }
            }

            return ApiResponse.success(null, 200);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.fail(ErrorCode.UNKNOWN, 500);
        }
    }

    @PutMapping("/video/{videoId}/thumbnail")
    public ApiResponse changeVideoThumbnail(@PathVariable("videoId") String videoId, @RequestPart("thumbnail") MultipartFile thumbnail) {
        try {
            boolean isSuccessful = archiveOrchestrator.updateVideoThumbnail(UUID.fromString(videoId), thumbnail.getInputStream(), thumbnail.getContentType(), thumbnail.getSize());

            if (!isSuccessful) {
                return ApiResponse.fail(ErrorCode.UNKNOWN, 500);
            }

            return ApiResponse.success(null, 200);
        } catch (IOException e) {
            e.printStackTrace();
            return ApiResponse.fail(ErrorCode.UNKNOWN, 500);
        }
    }
}
