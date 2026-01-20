package com.orv.api.domain.archive.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.orv.api.domain.archive.repository.VideoRepository;
import com.orv.api.domain.archive.service.dto.ImageMetadata;
import com.orv.api.domain.archive.service.dto.PresignedUrlInfo;
import com.orv.api.domain.archive.service.dto.Video;
import com.orv.api.domain.archive.service.dto.VideoMetadata;
import com.orv.api.domain.archive.service.dto.VideoStatus;

import org.springframework.beans.factory.annotation.Value;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.time.Instant;

@Service
@Slf4j
public class ArchiveServiceImpl implements ArchiveService {
    private static final long PRESIGNED_URL_EXPIRATION_MINUTES = 60;

    private final VideoRepository videoRepository;

    @Value("${cloud.aws.cloudfront.domain}")
    private String cloudfrontDomain;

    public ArchiveServiceImpl(VideoRepository videoRepository) {
        this.videoRepository = videoRepository;
    }

    @Override
    public Optional<String> uploadRecordedVideo(InputStream videoStream, String contentType, long size, UUID storyboardId, UUID memberId) {
        try {
            // TODO: 영상 처리 작업을 별도 서버로 분리한 후 아래의 영상 처리 작업 코드를 제거해야 함.
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

    private double calculateRunningTime(File videoFile) {
        // TODO: Running Time 계산 로직을 별도 서버로 분리해야 함.
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

    // v1 API methods

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

        log.info("Confirmed video upload and updated status: {}", videoId);

        // TODO: 큐에 영상 길이 측정 태스크 추가
        // messageQueue.send(new VideoProcessingTask(videoId));

        return Optional.of(videoId.toString());
    }

    @Override
    public boolean deleteVideo(UUID videoId) {
        return videoRepository.deleteVideo(videoId);
    }
}
