package com.stream.transcoder.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(
    name = "videos",
    indexes = {
        @Index(name = "idx_videos_user_id", columnList = "user_id"),
        @Index(name = "idx_videos_hash_status", columnList = "composite_hash, status")
    }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Video {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "upload_id", length = 255, unique = true)
    private String uploadId;

    @Column(name = "user_id", length = 255)
    private String userId;

    @Column(name = "title", length = 255)
    private String title;

    @Column(name = "description", columnDefinition = "TEXT")
    private String description;

    @Column(name = "total_chunks")
    private Integer totalChunks;

    @Column(name = "size_bytes")
    private Long sizeBytes;

    @Column(name = "composite_hash", length = 64)
    private String compositeHash;

    @Column(name = "hls_master_s3_key", length = 512)
    private String hlsMasterS3Key;

    @Column(name = "duration_s")
    private Integer durationS;

    @Convert(converter = VideoStatusConverter.class)
    @Column(name = "status", length = 20)
    private VideoStatus status;

    @Column(name = "created_at", updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        if (this.createdAt == null) {
            this.createdAt = OffsetDateTime.now();
        }
        this.updatedAt = OffsetDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = OffsetDateTime.now();
    }
}
