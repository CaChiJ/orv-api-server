package com.orv.api.unit.domain.archive;

import com.orv.api.domain.archive.service.ArchiveServiceImpl;
import com.orv.api.domain.archive.repository.VideoDurationExtractionJobRepository;
import com.orv.api.domain.archive.repository.VideoRepository;
import com.orv.api.domain.archive.controller.dto.PresignedUrlResponse;
import com.orv.api.domain.archive.service.dto.PresignedUrlInfo;
import com.orv.api.domain.archive.service.dto.Video;
import com.orv.api.domain.archive.service.dto.VideoStatus;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ArchiveServiceImplTest {

    @InjectMocks
    private ArchiveServiceImpl archiveService;

    @Mock
    private VideoRepository videoRepository;

    @Mock
    private VideoDurationExtractionJobRepository videoDurationExtractionJobRepository;

    @Test
    @DisplayName("requestUploadUrl: PENDING 상태의 video 생성 후 Presigned URL 반환")
    void requestUploadUrl_createsPendingVideoAndReturnsPresignedUrl() throws MalformedURLException {
        // given
        UUID storyboardId = UUID.randomUUID();
        UUID memberId = UUID.randomUUID();
        String videoId = UUID.randomUUID().toString();
        URL presignedUrl = new URL("https://bucket.s3.amazonaws.com/archive/videos/" + videoId + "?X-Amz-Signature=...");

        when(videoRepository.createPendingVideo(storyboardId, memberId)).thenReturn(videoId);
        when(videoRepository.generateUploadUrl(eq(UUID.fromString(videoId)), eq(60L))).thenReturn(presignedUrl);

        // when
        PresignedUrlInfo response = archiveService.requestUploadUrl(storyboardId, memberId);

        // then
        assertThat(response.getVideoId()).isEqualTo(videoId);
        assertThat(response.getUploadUrl()).isEqualTo(presignedUrl.toString());
        assertThat(response.getExpiresAt()).isNotNull();

        verify(videoRepository).createPendingVideo(storyboardId, memberId);
        verify(videoRepository).generateUploadUrl(eq(UUID.fromString(videoId)), eq(60L));
    }

    @Test
    @DisplayName("confirmUpload: S3에 파일 존재 시 status를 UPLOADED로 변경")
    void confirmUpload_updatesStatusWhenFileExists() {
        // given
        UUID videoId = UUID.randomUUID();
        UUID memberId = UUID.randomUUID();
        String cloudfrontDomain = "https://cdn.example.com";

        Video video = new Video();
        video.setId(videoId);
        video.setMemberId(memberId);
        video.setStatus(VideoStatus.PENDING.name());

        ReflectionTestUtils.setField(archiveService, "cloudfrontDomain", cloudfrontDomain);

        when(videoRepository.findById(videoId)).thenReturn(Optional.of(video));
        when(videoRepository.checkUploadComplete(videoId)).thenReturn(true);
        when(videoRepository.updateVideoUrlAndStatus(eq(videoId), any(), eq(VideoStatus.UPLOADED.name()))).thenReturn(true);

        // when
        Optional<String> result = archiveService.confirmUpload(videoId, memberId);

        // then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(videoId.toString());

        verify(videoRepository).findById(videoId);
        verify(videoRepository).checkUploadComplete(videoId);
        verify(videoRepository).updateVideoUrlAndStatus(eq(videoId), eq(cloudfrontDomain + "/archive/videos/" + videoId), eq(VideoStatus.UPLOADED.name()));
    }

    @Test
    @DisplayName("confirmUpload: video가 존재하지 않으면 empty 반환")
    void confirmUpload_returnsEmptyWhenVideoNotFound() {
        // given
        UUID videoId = UUID.randomUUID();
        UUID memberId = UUID.randomUUID();

        when(videoRepository.findById(videoId)).thenReturn(Optional.empty());

        // when
        Optional<String> result = archiveService.confirmUpload(videoId, memberId);

        // then
        assertThat(result).isEmpty();
        verify(videoRepository).findById(videoId);
        verify(videoRepository, never()).checkUploadComplete(any());
    }

    @Test
    @DisplayName("confirmUpload: 다른 사용자의 video면 empty 반환")
    void confirmUpload_returnsEmptyWhenUnauthorized() {
        // given
        UUID videoId = UUID.randomUUID();
        UUID memberId = UUID.randomUUID();
        UUID differentMemberId = UUID.randomUUID();

        Video video = new Video();
        video.setId(videoId);
        video.setMemberId(differentMemberId);
        video.setStatus(VideoStatus.PENDING.name());

        when(videoRepository.findById(videoId)).thenReturn(Optional.of(video));

        // when
        Optional<String> result = archiveService.confirmUpload(videoId, memberId);

        // then
        assertThat(result).isEmpty();
        verify(videoRepository, never()).checkUploadComplete(any());
    }

    @Test
    @DisplayName("confirmUpload: PENDING 상태가 아니면 empty 반환")
    void confirmUpload_returnsEmptyWhenNotPending() {
        // given
        UUID videoId = UUID.randomUUID();
        UUID memberId = UUID.randomUUID();

        Video video = new Video();
        video.setId(videoId);
        video.setMemberId(memberId);
        video.setStatus(VideoStatus.UPLOADED.name());

        when(videoRepository.findById(videoId)).thenReturn(Optional.of(video));

        // when
        Optional<String> result = archiveService.confirmUpload(videoId, memberId);

        // then
        assertThat(result).isEmpty();
        verify(videoRepository, never()).checkUploadComplete(any());
    }

    @Test
    @DisplayName("confirmUpload: S3에 파일이 없으면 empty 반환")
    void confirmUpload_returnsEmptyWhenFileNotInS3() {
        // given
        UUID videoId = UUID.randomUUID();
        UUID memberId = UUID.randomUUID();

        Video video = new Video();
        video.setId(videoId);
        video.setMemberId(memberId);
        video.setStatus(VideoStatus.PENDING.name());

        when(videoRepository.findById(videoId)).thenReturn(Optional.of(video));
        when(videoRepository.checkUploadComplete(videoId)).thenReturn(false);

        // when
        Optional<String> result = archiveService.confirmUpload(videoId, memberId);

        // then
        assertThat(result).isEmpty();
        verify(videoRepository, never()).updateVideoUrlAndStatus(any(), any(), any());
    }
}
