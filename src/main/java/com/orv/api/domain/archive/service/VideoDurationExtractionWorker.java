package com.orv.api.domain.archive.service;

import com.orv.api.domain.archive.service.dto.VideoDurationExtractionJob;
import com.orv.api.domain.archive.repository.VideoDurationExtractionJobRepository;
import com.orv.api.domain.archive.repository.VideoRepository;

import lombok.extern.slf4j.Slf4j;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(name = "worker.duration-extraction.enabled", havingValue = "true")
@Slf4j
public class VideoDurationExtractionWorker implements CommandLineRunner {

    private final VideoDurationExtractionJobRepository jobRepository;
    private final VideoRepository videoRepository;

    @Value("${worker.duration-extraction.threads:2}")
    private int threadCount;

    @Value("${worker.duration-extraction.poll-interval-ms:3000}")
    private long pollIntervalMs;

    @Value("${worker.duration-extraction.stuck-threshold-minutes:10}")
    private int stuckThresholdMinutes;

    private ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public VideoDurationExtractionWorker(
            VideoDurationExtractionJobRepository jobRepository,
            VideoRepository videoRepository) {
        this.jobRepository = jobRepository;
        this.videoRepository = videoRepository;
    }

    @Override
    public void run(String... args) {
        log.info("Starting VideoDurationExtractionWorker with {} threads", threadCount);
        log.debug("Worker configuration - Poll interval: {}ms, Stuck threshold: {} minutes",
                pollIntervalMs, stuckThresholdMinutes);

        executorService = Executors.newFixedThreadPool(threadCount);

        Duration stuckThreshold = Duration.ofMinutes(stuckThresholdMinutes);

        for (int i = 0; i < threadCount; i++) {
            final int workerId = i;
            executorService.submit(() -> workerLoop(workerId, stuckThreshold));
        }

        log.info("All {} worker threads started successfully", threadCount);
    }

    private void workerLoop(int workerId, Duration stuckThreshold) {
        log.info("Worker-{} started and ready to process jobs", workerId);

        int emptyPollCount = 0;

        while (running.get()) {
            try {
                // 1. Claim next pending job
                Optional<VideoDurationExtractionJob> jobOpt = jobRepository.claimNext(stuckThreshold);

                if (jobOpt.isEmpty()) {
                    emptyPollCount++;

                    // Log every 20 empty polls (1 minute with 3s interval)
                    if (emptyPollCount % 20 == 0) {
                        log.debug("Worker-{} polled {} times with no jobs available", workerId, emptyPollCount);
                    }

                    Thread.sleep(pollIntervalMs);
                    continue;
                }

                // Reset empty poll counter when job is found
                if (emptyPollCount > 0) {
                    log.debug("Worker-{} found job after {} empty polls", workerId, emptyPollCount);
                    emptyPollCount = 0;
                }

                VideoDurationExtractionJob job = jobOpt.get();
                log.info("Worker-{} claimed job #{} for video {}", workerId, job.getId(), job.getVideoId());

                // 2. Process the job
                long startTime = System.currentTimeMillis();
                processJob(job);
                long processingTime = System.currentTimeMillis() - startTime;

                log.info("Worker-{} completed job #{} in {}ms", workerId, job.getId(), processingTime);

            } catch (InterruptedException e) {
                log.info("Worker-{} interrupted, shutting down", workerId);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Worker-{} encountered unexpected error", workerId, e);
                // Continue processing next job
            }
        }

        log.info("Worker-{} stopped", workerId);
    }

    private void processJob(VideoDurationExtractionJob job) {
        File tempFile = null;
        try {
            // 1. Download video
            log.debug("Downloading video {}", job.getVideoId());

            Optional<InputStream> videoStreamOpt = videoRepository.getVideoStream(job.getVideoId());

            if (videoStreamOpt.isEmpty()) {
                log.error("Video file not found in S3 for job #{}: {}", job.getId(), job.getVideoId());
                jobRepository.markFailed(job.getId());
                return;
            }

            // 2. Save to temporary file
            tempFile = File.createTempFile("duration-extraction-" + job.getVideoId(), ".tmp");
            log.debug("Saving video to temporary file: {}", tempFile.getAbsolutePath());

            long downloadStart = System.currentTimeMillis();
            try (InputStream videoStream = videoStreamOpt.get()) {
                Files.copy(videoStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            long downloadTime = System.currentTimeMillis() - downloadStart;
            log.debug("Downloaded video in {}ms, file size: {} bytes", downloadTime, tempFile.length());

            // 3. Calculate duration
            log.debug("Calculating video duration for job #{}", job.getId());
            long calcStart = System.currentTimeMillis();
            double durationSeconds = calculateRunningTime(tempFile);
            long calcTime = System.currentTimeMillis() - calcStart;

            if (durationSeconds <= 0) {
                log.warn("Invalid duration calculated for job #{}: {}s", job.getId(), durationSeconds);
                jobRepository.markFailed(job.getId());
                return;
            }

            log.debug("Calculated duration: {}s (took {}ms)", (int) durationSeconds, calcTime);

            // 4. Update video table
            log.debug("Updating video running_time in database");
            boolean updated = videoRepository.updateRunningTime(job.getVideoId(), (int) durationSeconds);

            if (!updated) {
                log.error("Failed to update running_time in database for job #{}", job.getId());
                jobRepository.markFailed(job.getId());
                return;
            }

            // 5. Mark job as completed
            jobRepository.markCompleted(job.getId());
            log.info("Job #{} completed successfully - Video: {}, Duration: {}s",
                    job.getId(), job.getVideoId(), (int) durationSeconds);

        } catch (Exception e) {
            log.error("Failed to process job #{}: {}", job.getId(), e.getMessage(), e);
            jobRepository.markFailed(job.getId());
        } finally {
            // 6. Clean up temporary file
            if (tempFile != null && tempFile.exists()) {
                long fileSize = tempFile.length();
                if (!tempFile.delete()) {
                    log.warn("Failed to delete temporary file: {} ({} bytes)", tempFile.getAbsolutePath(), fileSize);
                } else {
                    log.debug("Cleaned up temporary file ({} bytes)", fileSize);
                }
            }
        }
    }

    private double calculateRunningTime(File videoFile) {
        try {
            double durationInSeconds = 0.0;
            try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(videoFile)) {
                grabber.setFormat("mp4");
                grabber.start();

                long lengthInTime = grabber.getLengthInTime();
                if (lengthInTime > 0) {
                    durationInSeconds = lengthInTime / 1_000_000.0;
                } else {
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

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down VideoDurationExtractionWorker...");
        running.set(false);

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.warn("Executor did not terminate in time, forcing shutdown");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for executor to terminate", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("VideoDurationExtractionWorker shut down complete");
    }
}
