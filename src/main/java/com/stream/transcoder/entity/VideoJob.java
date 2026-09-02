package com.stream.transcoder.entity;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "video_jobs")
@Data
public class VideoJob {
    @Id
    private String videoId;

    @Enumerated(EnumType.STRING)
    private JobStatus status;

    private String errorMessage;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public enum JobStatus {
        RECEIVED, DOWNLOADING, ASSEMBLING, ENCODING, UPLOADING, COMPLETED, FAILED, PROCESSING
    }
}
