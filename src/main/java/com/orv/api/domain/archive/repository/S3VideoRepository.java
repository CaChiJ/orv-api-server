package com.orv.api.domain.archive.repository;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.orv.api.domain.archive.service.dto.ImageMetadata;
import com.orv.api.domain.archive.service.dto.Video;
import com.orv.api.domain.archive.service.dto.VideoMetadata;
import com.orv.api.domain.archive.service.dto.VideoStatus;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.*;

@Repository
@Slf4j
public class S3VideoRepository implements VideoRepository {
    private final AmazonS3 amazonS3Client;

    @Value("${cloud.aws.s3.bucket}")
    private String bucket;

    @Value("${cloud.aws.cloudfront.domain}")
    private String cloudfrontDomain;

    private final JdbcTemplate jdbcTemplate;
    private final SimpleJdbcInsert simpleJdbcInsertVideo;
    private final SimpleJdbcInsert simpleJdbcInsertPendingVideo;

    public S3VideoRepository(AmazonS3 amazonS3Client, JdbcTemplate jdbcTemplate) {
        this.amazonS3Client = amazonS3Client;
        this.jdbcTemplate = jdbcTemplate;
        this.simpleJdbcInsertVideo = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("video")
                .usingColumns("id", "storyboard_id", "member_id", "video_url", "thumbnail_url", "running_time", "title", "status");
        this.simpleJdbcInsertPendingVideo = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("video")
                .usingColumns("id", "storyboard_id", "member_id", "status");
    }

    public Optional<String> save(InputStream inputStream, VideoMetadata videoMetadata) {
        try {
            String fileId = UUID.randomUUID().toString();

            // AWS S3에 영상 업로드
            String fileUrl = "archive/videos/" + fileId;
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(videoMetadata.getContentType());
            metadata.setContentLength(videoMetadata.getContentLength());
            amazonS3Client.putObject(bucket, fileUrl, inputStream, metadata);
            URI downloadUri = URI.create(cloudfrontDomain + "/" + fileUrl);

            // DB에 영상 정보 저장
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("id", fileId);
            parameters.put("storyboard_id", videoMetadata.getStoryboardId().toString());
            parameters.put("member_id", videoMetadata.getOwnerId().toString());
            parameters.put("video_url", downloadUri.toString());
            parameters.put("thumbnail_url", "https://d3bdjeyz3ry3pi.cloudfront.net/static/images/default-archive-video-thumbnail.png");
            parameters.put("running_time", videoMetadata.getRunningTime());
            parameters.put("title", videoMetadata.getTitle());
            parameters.put("status", VideoStatus.UPLOADED.name());
            simpleJdbcInsertVideo.execute(new MapSqlParameterSource(parameters));

            return Optional.of(fileId.toString());
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    @Override
    public Optional<Video> findById(UUID videoId) {
        String sql = "SELECT id, storyboard_id, member_id, video_url, created_at, thumbnail_url, running_time, title, status FROM video WHERE id = ?";

        try {
            Video video = jdbcTemplate.queryForObject(sql, new Object[]{videoId}, new BeanPropertyRowMapper<>(Video.class));
            return Optional.of(video);
        } catch (EmptyResultDataAccessException e) {
            log.debug("Video not found: {}", videoId);
            return Optional.empty();
        }
    }

    @Override
    public List<Video> findByMemberId(UUID memberId, int offset, int limit) {
        String sql = "SELECT id, storyboard_id, member_id, video_url, created_at, thumbnail_url, running_time, title, status FROM video WHERE member_id = ? AND status = 'UPLOADED' ORDER BY created_at DESC LIMIT ? OFFSET ?";

        try {
            List<Video> videos = jdbcTemplate.query(sql, new Object[]{memberId, limit, offset}, new BeanPropertyRowMapper<>(Video.class));
            return videos;
        } catch (EmptyResultDataAccessException e) {
            log.debug("No videos found for member: {}", memberId);
            return Collections.emptyList();
        }
    }

    @Override
    public boolean updateTitle(UUID videoId, String title) {
        try {
            jdbcTemplate.update("UPDATE video SET title = ? WHERE id = ?", title, videoId);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateThumbnail(UUID videoId, InputStream thumbnail, ImageMetadata imageMetadata) {
        try {
            String fileId = UUID.randomUUID().toString();

            // AWS S3에 영상 업로드
            String fileUrl = "archive/images/" + fileId;
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(imageMetadata.getContentType());
            metadata.setContentLength(imageMetadata.getContentLength());
            amazonS3Client.putObject(bucket, fileUrl, thumbnail, metadata);
            URI downloadUri = URI.create(cloudfrontDomain + "/" + fileUrl);

            // DB에 썸네일 url 업데이트
            jdbcTemplate.update("UPDATE video SET thumbnail_url = ? WHERE id = ?", downloadUri.toString(), videoId);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Optional<InputStream> getVideoStream(UUID videoId) {
        String sql = "SELECT video_url FROM video WHERE id = ?";
        try {
            String videoUrl = jdbcTemplate.queryForObject(sql, String.class, videoId);
            if (videoUrl == null) {
                return Optional.empty();
            }

            URI uri = new URI(videoUrl);
            String scheme = uri.getScheme();

            if ("s3".equalsIgnoreCase(scheme)) {
                String bucketName = uri.getHost();
                String key = uri.getPath().substring(1);
                S3Object s3Object = amazonS3Client.getObject(bucketName, key);
                return Optional.of(s3Object.getObjectContent());
            } else if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
                return Optional.of(uri.toURL().openStream());
            } else {
                return Optional.empty();
            }
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    // v1 API methods

    @Override
    public String createPendingVideo(UUID storyboardId, UUID memberId) {
        String videoId = UUID.randomUUID().toString();

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", videoId);
        parameters.put("storyboard_id", storyboardId.toString());
        parameters.put("member_id", memberId.toString());
        parameters.put("status", VideoStatus.PENDING.name());

        simpleJdbcInsertPendingVideo.execute(new MapSqlParameterSource(parameters));

        return videoId;
    }

    @Override
    public URL generateUploadUrl(UUID videoId, long expirationMinutes) {
        String s3Key = "archive/videos/" + videoId;
        Date expiration = calcDateAfterMinutes(expirationMinutes);

        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket, s3Key)
                .withMethod(HttpMethod.PUT)
                .withExpiration(expiration);

        return amazonS3Client.generatePresignedUrl(request);
    }

    @Override
    public boolean checkUploadComplete(UUID videoId) {
        String s3Key = "archive/videos/" + videoId;
        try {
            ObjectMetadata metadata = amazonS3Client.getObjectMetadata(bucket, s3Key);
            return metadata != null && metadata.getContentLength() > 0;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public boolean updateVideoUrlAndStatus(UUID videoId, String videoUrl, String status) {
        try {
            String defaultThumbnail = "https://d3bdjeyz3ry3pi.cloudfront.net/static/images/default-archive-video-thumbnail.png";
            int updated = jdbcTemplate.update(
                    "UPDATE video SET video_url = ?, thumbnail_url = ?, status = ? WHERE id = ?",
                    videoUrl, defaultThumbnail, status, videoId
            );
            return updated > 0;
        } catch (Exception e) {
            log.error("Failed to update video url and status: {}", videoId, e);
            return false;
        }
    }

    private Date calcDateAfterMinutes(long minutes) {
        Date date = new Date();
        long expTimeMillis = date.getTime() + (1000 * 60 * minutes);
        date.setTime(expTimeMillis);
        return date;
    }

    @Override
    public boolean deleteVideo(UUID videoId) {
        try {
            // 1. S3에서 영상 파일 삭제 (파일이 없어도 S3 deleteObject는 예외 없이 성공 반환)
            String s3Key = "archive/videos/" + videoId;
            try {
                amazonS3Client.deleteObject(bucket, s3Key);
                log.info("Deleted video file from S3: {}", s3Key);
            } catch (AmazonS3Exception e) {
                log.warn("Failed to delete video file from S3 (continuing anyway): {}", s3Key, e);
            }

            // 2. DB에서 status를 DELETED로 변경
            int updated = jdbcTemplate.update(
                    "UPDATE video SET status = ? WHERE id = ?",
                    VideoStatus.DELETED.name(), videoId
            );
            return updated > 0;
        } catch (Exception e) {
            log.error("Failed to delete video: {}", videoId, e);
            return false;
        }
    }

    @Override
    public List<Video> findAllByMemberId(UUID memberId) {
        String sql = "SELECT id, storyboard_id, member_id, video_url, created_at, thumbnail_url, running_time, title, status FROM video WHERE member_id = ?";
        return jdbcTemplate.query(sql, new Object[]{memberId}, new BeanPropertyRowMapper<>(Video.class));
    }

    @Override
    public boolean updateRunningTime(UUID videoId, int runningTimeSeconds) {
        try {
            int updated = jdbcTemplate.update(
                    "UPDATE video SET running_time = ? WHERE id = ?",
                    runningTimeSeconds, videoId
            );
            return updated > 0;
        } catch (Exception e) {
            log.error("Failed to update running time: {}", videoId, e);
            return false;
        }
    }
}
