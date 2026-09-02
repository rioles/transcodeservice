package com.stream.transcoder.repository;

import com.stream.transcoder.entity.VideoFormat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface VideoFormatRepository extends JpaRepository<VideoFormat, UUID> {

    List<VideoFormat> findByVideoIdAndReadyTrue(UUID videoId);

    List<VideoFormat> findByVideoId(UUID videoId);

    Optional<VideoFormat> findByVideoIdAndResolution(UUID videoId, String resolution);

    void deleteByVideoId(UUID videoId);

    /**
     * Insère un nouveau format vidéo ou met à jour un format existant
     * en cas de conflit sur la contrainte d'unicité (video_id, resolution).
     */
    @Modifying
    @Transactional
    @Query(value = """
        INSERT INTO video_formats (
            id, video_id, resolution, width, height, s3_key, codec, bitrate_kbps, ready, created_at
        )
        VALUES (
            gen_random_uuid(), :videoId, :resolution, :width, :height, :s3Key, :codec, :bitrateKbps, :ready, NOW()
        )
        ON CONFLICT (video_id, resolution) DO UPDATE SET
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            s3_key = EXCLUDED.s3_key,
            codec = EXCLUDED.codec,
            bitrate_kbps = EXCLUDED.bitrate_kbps,
            ready = EXCLUDED.ready
        """, nativeQuery = true)
    void upsert(
        @Param("videoId") UUID videoId,
        @Param("resolution") String resolution,
        @Param("width") Integer width,
        @Param("height") Integer height,
        @Param("s3Key") String s3Key,
        @Param("codec") String codec,
        @Param("bitrateKbps") Integer bitrateKbps,
        @Param("ready") boolean ready
    );
}
