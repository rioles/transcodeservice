package com.stream.transcoder.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "video_formats", indexes = {
    @Index(name = "idx_formats_s3_key", columnList = "s3_key")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VideoFormat {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "video_id", nullable = false)
    private UUID videoId;

    @Column(name = "resolution", length = 20, nullable = false)
    private String resolution;

    @Column(name = "width")
    private Integer width;

    @Column(name = "height")
    private Integer height;

    @Column(name = "s3_key", length = 512, nullable = false)
    private String s3Key;

    @Builder.Default
    @Column(name = "codec", length = 20, nullable = false)
    private String codec = "h264";

    @Column(name = "bitrate_kbps")
    private Integer bitrateKbps;

    @Builder.Default
    @Column(name = "ready", nullable = false)
    private Boolean ready = false;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    // Méthode explicite pour satisfaire format.isReady()
    public boolean isReady() {
        return Boolean.TRUE.equals(this.ready);
    }
}
