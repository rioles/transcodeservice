package com.stream.transcoder.repository;

import com.stream.transcoder.entity.VideoJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface VideoJobRepository extends JpaRepository<VideoJob, String> {

/**
     * Acquiert atomiquement un job de transcodage :
     * 1. Si le job n'existe pas, l'insère directement avec le statut PROCESSING (gère le premier message SQS).
     * 2. Si le job existe avec le statut RECEIVED ou FAILED, le passe à PROCESSING.
     * 3. Si le job est déjà PROCESSING ou COMPLETED, ne fait rien et retourne 0.
     *
     * @param videoId Identifiant du job
     * @return 1 si le job a été acquis avec succès par ce worker, 0 sinon.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
        INSERT INTO video_jobs (video_id, status, created_at, updated_at)
        VALUES (:videoId, 'PROCESSING', NOW(), NOW())
        ON CONFLICT (video_id) DO UPDATE
        SET status = 'PROCESSING',
            updated_at = NOW()
        WHERE video_jobs.status IN ('RECEIVED', 'FAILED')
        """, nativeQuery = true)
    int acquireJobAtomically(@Param("videoId") String videoId);;
    
    /**
     * Acquiert atomiquement un job de transcodage :
     *
     * 1. Si le job n'existe pas, l'insère directement avec le statut PROCESSING.
     * 2. Si le job existe avec le statut RECEIVED ou FAILED, le passe à PROCESSING.
     * 3. Si le job est PROCESSING mais bloqué depuis plus de 30 minutes,
     *    permet sa reprise par un autre worker.
     * 4. Si le job est déjà PROCESSING depuis moins de 30 minutes
     *    ou COMPLETED, ne fait rien et retourne 0.
     *
     * @param videoId Identifiant du job
     * @return 1 si le job a été acquis avec succès par ce worker, 0 sinon.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
        INSERT INTO video_jobs (
            video_id,
            status,
            created_at,
            updated_at
        )
        VALUES (
            :videoId,
            'PROCESSING',
            NOW(),
            NOW()
        )
        ON CONFLICT (video_id) DO UPDATE
        SET status = 'PROCESSING',
            updated_at = NOW()
        WHERE video_jobs.status IN ('RECEIVED', 'FAILED')
           OR (
                video_jobs.status = 'PROCESSING'
                AND video_jobs.updated_at < NOW() - INTERVAL '30 minutes'
           )
        """, nativeQuery = true)
    int acquireJobAtomicallys(@Param("videoId") String videoId);
}



