package com.orv.api.domain.archive.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.net.URL;
import java.time.Instant;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.orv.api.domain.archive.repository.VideoDurationExtractionJobRepository;
import com.orv.api.domain.archive.repository.VideoRepository;
import com.orv.api.domain.archive.service.dto.ImageMetadata;
import com.orv.api.domain.archive.service.dto.PresignedUrlInfo;
import com.orv.api.domain.archive.service.dto.Video;
import com.orv.api.domain.archive.service.dto.VideoMetadata;
import com.orv.api.domain.archive.service.dto.VideoStatus;

@Service
@Slf4j
@RequiredArgsConstructor
public class ArchiveServiceImpl implements ArchiveService {
    private static final long PRESIGNED_URL_EXPIRATION_MINUTES = 60;

    private final VideoRepository videoRepository;
    private final VideoDurationExtractionJobRepository videoDurationExtractionJobRepository;

    @Value("${cloud.aws.cloudfront.domain}")
    private String cloudfrontDomain;

    @Override
    @Deprecated
    // TODO: 프론트엔드 의존성 해소 후 엔드포인트 제거
    public Optional<String> uploadRecordedVideo(InputStream videoStream, String contentType, long size, UUID storyboardId, UUID memberId) {
        try {
            // Create a temporary file to calculate running time
            File tempFile = File.createTempFile("upload-" + System.currentTimeMillis(), ".tmp");
            try {
                // Copy stream to temp file
                java.nio.file.Files.copy(videoStream, tempFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                
                // Calculate running time
                double runningTime = calculateRunningTime(tempFile);
                
                // Read the file back as InputStream for repository
                try (InputStream fileInputStream = java.nio.file.Files.newInputStream(tempFile.toPath())) {
                    Optional<String> videoId = videoRepository.save(
                            fileInputStream,
                            new VideoMetadata(
                                    storyboardId,
                                    memberId,
                                    null,
                                    contentType,
                                    (int) runningTime,
                                    size
                            )
                    );
                    
                    if (videoId.isEmpty()) {
                        log.warn("Failed to save video");
                    }
                    
                    return videoId;
                }
            } finally {
                if (tempFile.exists()) {
                    tempFile.delete();
                }
            }
        } catch (IOException e) {
            log.error("Failed to upload recorded video", e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<Video> getVideo(UUID videoId) {
        return videoRepository.findById(videoId);
    }

    @Override
    public Optional<InputStream> getVideoStream(UUID videoId) {
        return videoRepository.getVideoStream(videoId);
    }

    @Override
    public List<Video> getMyVideos(UUID memberId, int page, int pageSize) {
        int offset = page * pageSize;
        return videoRepository.findByMemberId(memberId, offset, pageSize);
    }

    @Override
    public boolean updateVideoTitle(UUID videoId, String title) {
        return videoRepository.updateTitle(videoId, title);
    }

    @Override
    public boolean updateVideoThumbnail(UUID videoId, InputStream thumbnailStream, ImageMetadata metadata) {
        return videoRepository.updateThumbnail(videoId, thumbnailStream, metadata);
    }

    @Deprecated
    // TODO: V0 API 제거 시 함께 제거
    private double calculateRunningTime(File videoFile) {
        try {
            double durationInSeconds = 0.0;
            try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(videoFile)) {
                // mp4 파일임을 명시적으로 설정
                grabber.setFormat("mp4");
                grabber.start();

                long lengthInTime = grabber.getLengthInTime();
                if (lengthInTime > 0) {
                    // 메타데이터에 duration 정보가 있을 경우 사용
                    durationInSeconds = lengthInTime / 1_000_000.0;
                } else {
                    // 메타데이터가 없을 경우, 첫 프레임과 마지막 프레임의 timestamp를 이용해 계산
                    Frame frame;
                    long firstTimestamp = -1;
                    long lastTimestamp = -1;
                    while ((frame = grabber.grabFrame()) != null) {
                        if (frame.timestamp > 0) {
                            if (firstTimestamp == -1) {
                                firstTimestamp = frame.timestamp;
                            }
                            lastTimestamp = frame.timestamp;
                        }
                    }
                    if (firstTimestamp != -1 && lastTimestamp != -1) {
                        durationInSeconds = (lastTimestamp - firstTimestamp) / 1_000_000.0;
                    }
                }
                grabber.stop();
            }
            return durationInSeconds;
        } catch (Exception e) {
            log.error("Failed to calculate running time", e);
            return 0.0;
        }
    }

    // v1 API methods below
    @Override
    public PresignedUrlInfo requestUploadUrl(UUID storyboardId, UUID memberId) {
        // 1. PENDING 상태로 video 레코드 생성
        String videoId = videoRepository.createPendingVideo(storyboardId, memberId);

        // 2. Presigned PUT URL 생성
        URL presignedUrl = videoRepository.generateUploadUrl(UUID.fromString(videoId), PRESIGNED_URL_EXPIRATION_MINUTES);

        // 3. 만료 시간 계산
        Instant expiresAt = Instant.now().plusSeconds(PRESIGNED_URL_EXPIRATION_MINUTES * 60);

        return new PresignedUrlInfo(videoId, presignedUrl.toString(), expiresAt);
    }

    @Override
    @Transactional
    public Optional<String> confirmUpload(UUID videoId, UUID memberId) {
        // 1. video 레코드 조회
        Optional<Video> videoOpt = videoRepository.findById(videoId);
        if (videoOpt.isEmpty()) {
            log.warn("Video not found: {}", videoId);
            return Optional.empty();
        }

        Video video = videoOpt.get();

        // 2. 소유권 확인
        if (!video.getMemberId().equals(memberId)) {
            log.warn("Unauthorized access to video: {} by member: {}", videoId, memberId);
            return Optional.empty();
        }

        // 3. PENDING 상태 확인
        if (!VideoStatus.PENDING.name().equals(video.getStatus())) {
            log.warn("Video is not in PENDING status: {} (current: {})", videoId, video.getStatus());
            return Optional.empty();
        }

        // 4. 파일 업로드 완료 확인
        if (!videoRepository.checkUploadComplete(videoId)) {
            log.warn("Video file not uploaded: {}", videoId);
            return Optional.empty();
        }

        // 5. video_url 및 status 업데이트
        String videoUrl = cloudfrontDomain + "/archive/videos/" + videoId;
        boolean updated = videoRepository.updateVideoUrlAndStatus(
                videoId,
                videoUrl,
                VideoStatus.UPLOADED.name()
        );

        if (!updated) {
            log.warn("Failed to update video status: {}", videoId);
            return Optional.empty();
        }

        // 6. 영상 길이 측정 태스크 추가
        videoDurationExtractionJobRepository.create(videoId);
        log.info("Created duration extraction job for video: {}", videoId);

        return Optional.of(videoId.toString());
    }

    @Override
    public boolean deleteVideo(UUID videoId) {
        return videoRepository.deleteVideo(videoId);
    }
}
