package com.orv.api.domain.recap.orchestrator;

import com.orv.api.domain.archive.service.ArchiveService;
import com.orv.api.domain.archive.service.dto.Video;
import com.orv.api.domain.media.service.AudioService;
import com.orv.api.domain.media.service.dto.InterviewAudioRecording;
import com.orv.api.domain.recap.controller.dto.*;
import com.orv.api.domain.recap.infrastructure.RecapClient;
import com.orv.api.domain.recap.service.RecapService;
import com.orv.api.domain.recap.service.dto.RecapAudioInfo;
import com.orv.api.domain.recap.service.dto.RecapResultInfo;
import com.orv.api.domain.recap.service.dto.InterviewScenario;
import com.orv.api.domain.storyboard.repository.StoryboardRepository;
import com.orv.api.domain.storyboard.service.InterviewScenarioConverter;
import com.orv.api.domain.storyboard.service.dto.Scene;
import com.orv.api.domain.storyboard.service.dto.Storyboard;
import com.orv.api.domain.recap.infrastructure.dto.RecapServerRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class RecapOrchestrator {
    private final RecapService recapService;
    private final ArchiveService archiveService;
    private final AudioService audioService;
    private final StoryboardRepository storyboardRepository;
    private final InterviewScenarioConverter interviewScenarioFactory;
    private final RecapClient recapClient;

    public Optional<RecapReservationResponse> reserveRecap(UUID memberId, UUID videoId, ZonedDateTime scheduledAt) throws IOException {
        // 1. 예약 생성
        Optional<UUID> recapIdOptional = recapService.reserveRecap(memberId, videoId, scheduledAt);
        if (recapIdOptional.isEmpty()) {
            return Optional.empty();
        }
        UUID recapReservationId = recapIdOptional.get();
        log.info("Created recap reservation: {}", recapReservationId);

        try {
            // 2. 비디오 정보 조회
            Video video = archiveService.getVideo(videoId)
                    .orElseThrow(() -> new IOException("Video with ID " + videoId + " not found."));

            Optional<InputStream> videoStreamOptional = archiveService.getVideoStream(videoId);
            if (videoStreamOptional.isEmpty()) {
                throw new IOException("Failed to retrieve video stream for video ID " + videoId);
            }

            // 3. 오디오 추출 및 저장
            InterviewAudioRecording audioRecording;
            try (InputStream videoStream = videoStreamOptional.get()) {
                 audioRecording = audioService.extractAndSaveAudioFromVideo(
                        videoStream,
                        video.getStoryboardId(),
                        memberId,
                        video.getTitle() != null ? video.getTitle() + " (Recap Audio)" : "Recap Audio",
                        video.getRunningTime()
                );
            }
            log.info("Extracted and saved audio: {}", audioRecording.getId());

            // 4. 예약-오디오 연결 (DB)
            try {
                recapService.linkAudioRecording(recapReservationId, audioRecording.getId());
                log.info("Linked audio to recap reservation: {}", recapReservationId);
            } catch (Exception e) {
                // 연결 실패 시 오디오 삭제 및 예외 전파 (상위 catch에서 예약 삭제됨)
                log.error("Failed to link audio. Compensating audio upload...", e);
                audioService.deleteAudio(audioRecording.getId(), audioRecording.getAudioUrl());
                throw e;
            }

            // 5. 외부 리캡 서버 호출
            callRecapServer(recapReservationId, video, audioRecording.getAudioUrl());

            return Optional.of(new RecapReservationResponse(
                    recapReservationId,
                    memberId,
                    videoId,
                    scheduledAt.toLocalDateTime(),
                    LocalDateTime.now()
            ));

        } catch (Exception e) {
            // 모든 하위 단계 실패 시 예약 삭제 (보상 트랜잭션)
            log.error("Failed to reserve recap. Compensating reservation...", e);
            recapService.deleteRecapReservation(recapReservationId);
            throw e;
        }
    }

    // TODO: 외부 API 호출 방식 결정 후 적절한 클래스로 아래 로직을 이동시켜야 함.
    private void callRecapServer(UUID recapReservationId, Video video, String audioS3Url) {
        try {
            // 1. Get storyboard and scene info
            Storyboard storyboard = storyboardRepository.findById(video.getStoryboardId())
                    .orElseThrow(() -> new RuntimeException("Storyboard not found for ID: " + video.getStoryboardId()));

            List<Scene> allScenes = storyboardRepository.findScenesByStoryboardId(video.getStoryboardId())
                    .orElseThrow(() -> new RuntimeException("Scenes not found for Storyboard ID: " + video.getStoryboardId()));

            // 2. Create InterviewScenario using the factory
            InterviewScenario interviewScenario = interviewScenarioFactory.create(storyboard, allScenes);

            // 3. Create request body
            RecapServerRequest requestBody = new RecapServerRequest(audioS3Url, interviewScenario);

            // 4. Call API via RecapClient
            /*
            recapClient.requestRecap(requestBody).ifPresent(response -> {
                log.info("Successfully received recap from server for video ID: {}. Storing results...", video.getId());
                recapService.saveRecapResult(recapReservationId, response.getRecapContent())
                        .ifPresent(recapResultId -> log.info("Recap results stored successfully with result ID: {}", recapResultId));
            });
            */
            log.info("Prepared to call Recap Server for reservation ID: {}", recapReservationId);

        } catch (Exception e) {
            log.error("Failed to prepare/call recap server for reservation ID: {}", recapReservationId, e);
        }
    }

    public Optional<RecapResultResponse> getRecapResult(UUID recapReservationId) {
        return recapService.getRecapResult(recapReservationId)
                .map(this::toRecapResultResponse);
    }

    public Optional<RecapAudioResponse> getRecapAudio(UUID recapReservationId) {
        return recapService.getRecapAudio(recapReservationId)
                .map(this::toRecapAudioResponse);
    }

    private RecapResultResponse toRecapResultResponse(RecapResultInfo info) {
        List<RecapAnswerSummaryResponse> summaries = info.getAnswerSummaries().stream()
                .map(summary -> new RecapAnswerSummaryResponse(
                        summary.getSceneId(),
                        summary.getQuestion(),
                        summary.getAnswerSummary()
                ))
                .collect(Collectors.toList());

        return new RecapResultResponse(
                info.getRecapResultId(),
                info.getCreatedAt(),
                summaries
        );
    }

    private RecapAudioResponse toRecapAudioResponse(RecapAudioInfo info) {
        return new RecapAudioResponse(
                info.getAudioId(),
                info.getAudioUrl(),
                info.getRunningTime(),
                info.getCreatedAt()
        );
    }
}