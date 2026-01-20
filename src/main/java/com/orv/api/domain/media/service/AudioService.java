package com.orv.api.domain.media.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.orv.api.domain.archive.repository.AudioRepository;
import com.orv.api.domain.media.infrastructure.AudioCompressor;
import com.orv.api.domain.media.infrastructure.AudioExtractor;
import com.orv.api.domain.media.repository.InterviewAudioRecordingRepository;
import com.orv.api.domain.media.service.dto.InterviewAudioRecording;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@Slf4j
public class AudioService {

    private final AudioExtractor audioExtractor;
    private final AudioCompressor audioCompressor;
    private final InterviewAudioRecordingRepository audioRecordingRepository;
    private final AudioRepository audioRepository;
    private final AmazonS3 amazonS3Client;

    @Value("${cloud.aws.s3.bucket}")
    private String bucket;

    public AudioService(AudioExtractor audioExtractor,
                        AudioCompressor audioCompressor,
                        InterviewAudioRecordingRepository audioRecordingRepository,
                        AudioRepository audioRepository,
                        AmazonS3 amazonS3Client) {
        this.audioExtractor = audioExtractor;
        this.audioCompressor = audioCompressor;
        this.audioRecordingRepository = audioRecordingRepository;
        this.audioRepository = audioRepository;
        this.amazonS3Client = amazonS3Client;
    }

    public InterviewAudioRecording extractAndSaveAudioFromVideo(
            InputStream videoStream, UUID storyboardId, UUID memberId, String title, Integer runningTime) throws IOException {
        
        File tempVideoFile = null;
        File tempAudioExtractedFile = null;
        File tempAudioCompressedFile = null;

        try {
            // 1. 비디오에서 오디오 추출 및 압축 (로컬 작업)
            tempVideoFile = convertStreamToFile(videoStream);

            tempAudioExtractedFile = createTempFile("extracted_audio_", ".wav");
            audioExtractor.extractAudio(tempVideoFile, tempAudioExtractedFile, "wav");

            tempAudioCompressedFile = createTempFile("compressed_audio_", ".opus");
            audioCompressor.compress(tempAudioExtractedFile, tempAudioCompressedFile);

            // 2. S3에 오디오 업로드 (트랜잭션 밖)
            URI resourceUrl = uploadAudioToS3(tempAudioCompressedFile, storyboardId, memberId, title, runningTime);
            log.info("Uploaded audio to S3: {}", resourceUrl);

            // 3. DB에 메타데이터 저장 (트랜잭션 안)
            try {
                InterviewAudioRecording audioRecording = InterviewAudioRecording.builder()
                        .id(UUID.randomUUID())
                        .storyboardId(storyboardId)
                        .memberId(memberId)
                        .audioUrl(resourceUrl.toString())
                        .createdAt(OffsetDateTime.now())
                        .runningTime(runningTime != null ? runningTime : 0)
                        .build();

                InterviewAudioRecording saved = audioRecordingRepository.save(audioRecording);
                log.info("Saved audio metadata to database: {}", saved.getId());
                return saved;
            } catch (Exception dbException) {
                // DB 저장 실패 시 S3 파일 삭제 (보상 트랜잭션)
                compensateS3AudioUpload(resourceUrl);
                throw dbException;
            }

        } finally {
            cleanupTempFile(tempVideoFile);
            cleanupTempFile(tempAudioExtractedFile);
            cleanupTempFile(tempAudioCompressedFile);
        }
    }

    /**
     * 오디오 레코딩 삭제 (DB + S3)
     * 보상 트랜잭션을 위한 명시적 삭제 메서드
     */
    public void deleteAudio(UUID audioRecordingId, String audioS3Url) {
        try {
            // 1. S3에서 오디오 파일 삭제
            deleteAudioFromS3(audioS3Url);

            // 2. DB에서 오디오 레코드 삭제
            audioRecordingRepository.delete(audioRecordingId);
            log.info("Deleted audio recording from DB: {}", audioRecordingId);

            log.info("Deleted audio recording {} and S3 file {}", audioRecordingId, audioS3Url);
        } catch (Exception e) {
            log.error("Failed to delete audio recording: {}. Manual cleanup may be required.", audioRecordingId, e);
        }
    }

    private void deleteAudioFromS3(String audioS3Url) {
        try {
            // URL 형식: s3://bucket/archive/audios/{fileId} 또는 https://...
            URI uri = URI.create(audioS3Url);
            String s3Path = uri.getPath();
            if (s3Path.startsWith("/")) {
                s3Path = s3Path.substring(1);
            }

            amazonS3Client.deleteObject(bucket, s3Path);
            log.info("Deleted audio file from S3: {}", s3Path);
        } catch (Exception e) {
            log.error("Failed to delete audio file from S3: {}. Manual cleanup may be required.", audioS3Url, e);
            throw e;
        }
    }

    private void compensateS3AudioUpload(URI resourceUrl) {
        try {
            // URI 형식: s3://bucket/archive/audios/{fileId}
            String s3Path = resourceUrl.getPath();
            if (s3Path.startsWith("/")) {
                s3Path = s3Path.substring(1);
            }

            amazonS3Client.deleteObject(bucket, s3Path);
            log.info("Compensated S3 audio upload by deleting file: {}", s3Path);
        } catch (Exception e) {
            log.error("Failed to compensate S3 audio upload for: {}. Manual cleanup may be required.", resourceUrl, e);
        }
    }

    private URI uploadAudioToS3(File audioFile, UUID storyboardId, UUID memberId, String title, Integer runningTime) {
        String fileId = UUID.randomUUID().toString();
        String fileUrl = "archive/audios/" + fileId;

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("audio/ogg; codecs=opus");
        metadata.setContentLength(audioFile.length());

        try (FileInputStream inputStream = new FileInputStream(audioFile)) {
            amazonS3Client.putObject(bucket, fileUrl, inputStream, metadata);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload audio to S3", e);
        }

        return URI.create("s3://" + bucket + "/" + fileUrl);
    }

    private File convertStreamToFile(InputStream in) throws IOException {
        File tempFile = File.createTempFile("video_stream_", ".tmp");
        tempFile.deleteOnExit();
        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        return tempFile;
    }

    private File createTempFile(String prefix, String suffix) throws IOException {
        File tempFile = File.createTempFile(prefix, suffix);
        tempFile.deleteOnExit();
        return tempFile;
    }

    private void cleanupTempFile(File file) {
        if (file != null && file.exists()) {
            file.delete();
        }
    }
}