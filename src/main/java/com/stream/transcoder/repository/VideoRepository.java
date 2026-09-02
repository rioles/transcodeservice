package com.stream.transcoder.repository;

import com.stream.transcoder.entity.Video;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface VideoRepository extends JpaRepository<Video, UUID> {

    /**
     * Trouve une vidéo source déjà traitée qui possède le même composite_hash 
     * mais un ID différent de la vidéo courante.
     */
    Optional<Video> findFirstByCompositeHashAndIdNot(String compositeHash, UUID currentVideoId);
}
