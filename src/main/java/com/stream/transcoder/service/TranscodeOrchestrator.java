package com.stream.transcoder.service;

import com.stream.transcoder.dto.TranscodeJobMessage;
import com.stream.transcoder.encoder.EncoderFactory;
import com.stream.transcoder.encoder.VideoEncoder;
import com.stream.transcoder.entity.Video;
import com.stream.transcoder.entity.VideoFormat;
import com.stream.transcoder.entity.VideoJob;
import com.stream.transcoder.entity.VideoStatus;
import com.stream.transcoder.repository.VideoFormatRepository;
import com.stream.transcoder.repository.VideoJobRepository;
import com.stream.transcoder.repository.VideoRepository;
import com.stream.transcoder.resolution.ResolutionProfile;
import com.stream.transcoder.resolution.ResolutionRegistry;
import com.stream.transcoder.storage.ProgressiveUploader;
import com.stream.transcoder.storage.S3StorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Service d'orchestration du traitement des vidéos.
 * Gère l'acquisition atomique des jobs, la déduplication et le pipeline complet HLS.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TranscodeOrchestrator {

    private static final int MAX_CONCURRENT_DOWNLOADS = 10;

    private final S3StorageService s3StorageService;
    private final VideoJobRepository videoJobRepository;
    private final VideoRepository videoRepository;
    private final VideoFormatRepository videoFormatRepository;
    private final ResolutionRegistry resolutionRegistry;
    private final EncoderFactory encoderFactory;
    private final ProgressiveUploader progressiveUploader;

    @Value("${transcode.temp-dir:/tmp/transcode}")
    private String baseTempDir;

    @Value("${aws.s3.bucket}")
    private String defaultBucketName;

    /**
     * Point d'entrée pour le traitement d'un message SQS de transcodage.
     * Acquiert le verrou atomiquement en base pour prévenir les race conditions.
     */
    public void processJob(TranscodeJobMessage message) {
        if (message == null || message.getVideoId() == null) {
            throw new IllegalArgumentException("Le message de transcodage ou son videoId est nul.");
        }

        String videoIdStr = message.getVideoId();
        UUID currentVideoId = UUID.fromString(videoIdStr);

        log.info("Réception du message SQS pour le job {}", videoIdStr);

        /*
         * ÉTAPE 1 : ACQUISITION ATOMIQUE (GÈRE LES RACE CONDITIONS)
         */
        boolean acquired = acquireJob(videoIdStr);

        if (!acquired) {
            Optional<VideoJob> existingJob = videoJobRepository.findById(videoIdStr);

            if (existingJob.isPresent() && existingJob.get().getStatus() == VideoJob.JobStatus.COMPLETED) {
                log.info("Job {} déjà au statut COMPLETED. Doublon SQS ignoré.", videoIdStr);
                return;
            }

            log.info("Job {} déjà en cours de traitement par un autre worker. Message SQS ignoré.", videoIdStr);
            return;
        }

        try {
            String compositeHash = message.getCompositeHash();

            /*
             * ÉTAPE 2 : DÉDUPLICATION
             */
            boolean deduplicatedSuccessfully = tryDeduplication(
                message,
                currentVideoId,
                videoIdStr,
                compositeHash
            );

            if (deduplicatedSuccessfully) {
                log.info("Job {} terminé par déduplication.", videoIdStr);
                return;
            }

            /*
             * ÉTAPE 3 : PIPELINE COMPLET
             */
            log.info("Démarrage du pipeline d'encodage complet pour le job {}", videoIdStr);
            executeFullTranscodePipeline(message, currentVideoId, videoIdStr);

        } catch (Exception e) {
            log.error("Échec du traitement du job {}", videoIdStr, e);
            failJob(videoIdStr, e.getMessage());
            throw new RuntimeException("Job de transcodage échoué : " + videoIdStr, e);
        }
    }

    private boolean acquireJob(String videoId) {
        int updatedRows = videoJobRepository.acquireJobAtomically(videoId);
        if (updatedRows == 1) {
            log.info("Job {} acquis atomiquement par ce worker.", videoId);
            return true;
        }
        return false;
    }

    private boolean tryDeduplication(
        TranscodeJobMessage message,
        UUID currentVideoId,
        String videoIdStr,
        String compositeHash
    ) {
        if (!Boolean.TRUE.equals(message.getIsDeduplicated())) {
            return false;
        }

        log.info("Recherche d'une vidéo équivalente pour le hash {} (job {})...", compositeHash, videoIdStr);

        Optional<Video> sourceVideoOptional = videoRepository.findFirstByCompositeHashAndIdNot(
            compositeHash,
            currentVideoId
        );

        if (sourceVideoOptional.isEmpty()) {
            log.warn("Aucune vidéo source trouvée pour le hash {}. Fallback vers encodage.", compositeHash);
            return false;
        }

        Video sourceVideo = sourceVideoOptional.get();
        List<VideoFormat> sourceFormats = videoFormatRepository.findByVideoIdAndReadyTrue(sourceVideo.getId());

        if (sourceFormats.isEmpty()) {
            log.warn("Vidéo source {} trouvée mais aucun format READY associé. Fallback vers encodage.", sourceVideo.getId());
            return false;
        }

        for (VideoFormat sf : sourceFormats) {
            videoFormatRepository.upsert(
                currentVideoId,
                sf.getResolution(),
                sf.getWidth(),
                sf.getHeight(),
                sf.getS3Key(),
                sf.getCodec(),
                sf.getBitrateKbps(),
                true
            );
        }

        updateVideoMetadata(currentVideoId, sourceVideo.getHlsMasterS3Key(), VideoStatus.READY);
        updateJobStatus(videoIdStr, VideoJob.JobStatus.COMPLETED);

        log.info("Déduplication réussie pour la vidéo {}. {} formats dupliqués.", videoIdStr, sourceFormats.size());
        return true;
    }

    private void executeFullTranscodePipeline(
        TranscodeJobMessage message,
        UUID currentVideoId,
        String videoIdStr
    ) {
        File workingDir = new File(baseTempDir, videoIdStr);

        try {
            setupWorkingDirectory(workingDir);

            updateJobStatus(videoIdStr, VideoJob.JobStatus.DOWNLOADING);
            downloadChunks(message, workingDir);

            updateJobStatus(videoIdStr, VideoJob.JobStatus.ASSEMBLING);
            File assembledFile = assembleChunks(message, workingDir);

            updateJobStatus(videoIdStr, VideoJob.JobStatus.ENCODING);
            encodeAndUpload(message, assembledFile, workingDir);

            String masterS3Key = videoIdStr + "/hls/master.m3u8";

            updateVideoMetadata(currentVideoId, masterS3Key, VideoStatus.READY);
            updateJobStatus(videoIdStr, VideoJob.JobStatus.COMPLETED);

            log.info("Pipeline de transcodage terminé avec succès pour {}", videoIdStr);

        } catch (Exception e) {
            log.error("Erreur durant l'exécution du pipeline pour {}", videoIdStr, e);
            updateVideoMetadata(currentVideoId, null, VideoStatus.FAILED);
            throw new RuntimeException("Échec du pipeline de transcodage pour : " + videoIdStr, e);
        } finally {
            cleanup(workingDir);
        }
    }

    private void setupWorkingDirectory(File dir) throws IOException {
        if (dir.exists()) {
            FileSystemUtils.deleteRecursively(dir);
        }
        if (!dir.mkdirs()) {
            throw new IOException("Impossible de créer le répertoire temporaire : " + dir.getAbsolutePath());
        }
        File chunksDir = new File(dir, "chunks");
        if (!chunksDir.mkdirs()) {
            throw new IOException("Impossible de créer le sous-répertoire chunks : " + chunksDir.getAbsolutePath());
        }
    }

    private void downloadChunks(TranscodeJobMessage message, File workingDir) {
        File chunksDir = new File(workingDir, "chunks");
        Semaphore semaphore = new Semaphore(MAX_CONCURRENT_DOWNLOADS);

        if (message.getChunks() == null || message.getChunks().isEmpty()) {
            throw new IllegalArgumentException("La liste des chunks est vide ou nulle.");
        }

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Void>> futures = message.getChunks().stream()
                .map(chunk -> CompletableFuture.runAsync(() -> {
                    boolean acquired = false;
                    try {
                        semaphore.acquire();
                        acquired = true;
                        File dest = new File(chunksDir, String.format("chunk_%05d.part", chunk.getChunkIndex()));
                        s3StorageService.downloadFile(chunk.getS3Url(), dest);
                        log.debug("Chunk {} téléchargé pour la vidéo {}", chunk.getChunkIndex(), message.getVideoId());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Téléchargement interrompu pour le chunk " + chunk.getChunkIndex(), e);
                    } catch (Exception e) {
                        throw new RuntimeException("Échec du téléchargement du chunk " + chunk.getChunkIndex(), e);
                    } finally {
                        if (acquired) {
                            semaphore.release();
                        }
                    }
                }, executor))
                .toList();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        } catch (CompletionException e) {
            throw new RuntimeException("Échec du téléchargement des chunks pour la vidéo " + message.getVideoId(), e.getCause());
        }
    }

    private File assembleChunks(TranscodeJobMessage message, File workingDir) throws IOException {
        File assembledFile = new File(workingDir, "assembled.mp4");
        File chunksDir = new File(workingDir, "chunks");

        if (message.getChunks() == null || message.getChunks().isEmpty()) {
            throw new IllegalArgumentException("Aucun chunk fourni pour l'assemblage.");
        }

        List<TranscodeJobMessage.ChunkDto> sortedChunks = message.getChunks().stream()
            .sorted(Comparator.comparingInt(TranscodeJobMessage.ChunkDto::getChunkIndex))
            .toList();

        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(assembledFile))) {
            for (TranscodeJobMessage.ChunkDto chunk : sortedChunks) {
                File chunkFile = new File(chunksDir, String.format("chunk_%05d.part", chunk.getChunkIndex()));
                if (!chunkFile.exists()) {
                    throw new FileNotFoundException("Fichier chunk introuvable : " + chunkFile.getAbsolutePath());
                }
                Files.copy(chunkFile.toPath(), out);
            }
            out.flush();
        }

        return assembledFile;
    }

    private void encodeAndUpload(TranscodeJobMessage message, File assembledFile, File workingDir) throws IOException {
        String videoIdStr = message.getVideoId();
        UUID videoId = UUID.fromString(videoIdStr);
        String bucket = resolveBucketName(message);

        if (message.getTargetResolutions() == null || message.getTargetResolutions().isEmpty()) {
            throw new IllegalArgumentException("Aucune résolution cible fournie.");
        }

        VideoEncoder encoder = encoderFactory.getEncoder(EncoderFactory.EncodingFormat.HLS);
        List<String> renditionPaths;

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<String>> futures = message.getTargetResolutions().stream()
                .map(resName -> CompletableFuture.supplyAsync(
                    () -> encodeResolution(encoder, videoIdStr, assembledFile, workingDir, bucket, resName),
                    executor
                ))
                .toList();

            CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            try {
                allFutures.join();
            } catch (CompletionException e) {
                futures.forEach(f -> f.cancel(true));
                throw new RuntimeException("Échec d'encodage pour la vidéo " + videoIdStr, e.getCause());
            }

            renditionPaths = futures.stream()
                .map(CompletableFuture::join)
                .toList();
        }

        generateMasterPlaylist(workingDir, message.getTargetResolutions(), renditionPaths, bucket, videoIdStr);

        for (String resName : message.getTargetResolutions()) {
            ResolutionProfile profile = resolutionRegistry.get(resName);
            if (profile == null) {
                throw new IllegalStateException("Profil de résolution introuvable pour : " + resName);
            }

            String s3Key = videoIdStr + "/hls/" + resName + "/playlist.m3u8";

            videoFormatRepository.upsert(
                videoId,
                resName,
                profile.width(),
                profile.height(),
                s3Key,
                "H.264",
                (int) (profile.bandwidthBps() / 1000),
                true
            );
        }
    }

    private String encodeResolution(
        VideoEncoder encoder,
        String videoId,
        File assembledFile,
        File workingDir,
        String bucket,
        String resName
    ) {
        ResolutionProfile profile = resolutionRegistry.get(resName);
        if (profile == null) {
            throw new IllegalArgumentException("Profil de résolution non configuré pour : " + resName);
        }

        File resOutputDir = new File(workingDir, "hls/" + resName);
        Process process = null;

        log.info("Lancement de l'encodage de la vidéo {} en {}", videoId, resName);

        try {
            process = encoder.encode(assembledFile, profile, resOutputDir);
            String keyPrefix = videoId + "/hls/" + resName + "/";

            progressiveUploader.watchAndUpload(resOutputDir.toPath(), bucket, keyPrefix, process);

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("FFmpeg a retourné le code d'erreur " + exitCode + " pour " + resName);
            }

            return resName + "/playlist.m3u8";

        } catch (IOException e) {
            killIfAlive(process);
            throw new RuntimeException("Erreur I/O d'encodage pour la résolution " + resName, e);
        } catch (InterruptedException e) {
            killIfAlive(process);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Processus d'encodage interrompu pour " + resName, e);
        } catch (RuntimeException e) {
            killIfAlive(process);
            throw e;
        }
    }

    private void killIfAlive(Process process) {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
        }
    }

    private void generateMasterPlaylist(
        File workingDir,
        List<String> resolutionNames,
        List<String> renditions,
        String bucket,
        String videoId
    ) throws IOException {
        File masterFile = new File(workingDir, "hls/master.m3u8");
        File parentDir = masterFile.getParentFile();

        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                throw new IOException("Impossible de créer le répertoire parent : " + parentDir.getAbsolutePath());
            }
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(masterFile))) {
            writer.println("#EXTM3U");

            for (int i = 0; i < renditions.size(); i++) {
                String resName = resolutionNames.get(i);
                String rendition = renditions.get(i);
                ResolutionProfile profile = resolutionRegistry.get(resName);

                if (profile == null) {
                    throw new IllegalStateException("Profil introuvable pour : " + resName);
                }

                writer.printf(
                    "#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d%n",
                    profile.bandwidthBps(),
                    profile.width(),
                    profile.height()
                );
                writer.println(rendition);
            }
        }

        s3StorageService.uploadFile(bucket, videoId + "/hls/master.m3u8", masterFile);
    }

    private void updateVideoMetadata(UUID videoId, String hlsMasterS3Key, VideoStatus status) {
        Video video = videoRepository.findById(videoId).orElseGet(() -> {
            Video v = new Video();
            v.setId(videoId);
            v.setUploadId(videoId.toString());
            return v;
        });

        if (video.getUploadId() == null) {
            video.setUploadId(videoId.toString());
        }

        if (hlsMasterS3Key != null) {
            video.setHlsMasterS3Key(hlsMasterS3Key);
        }

        video.setStatus(status);
        videoRepository.save(video);
    }

    private void updateJobStatus(String videoId, VideoJob.JobStatus status) {
        VideoJob job = videoJobRepository.findById(videoId).orElseGet(() -> {
            VideoJob newJob = new VideoJob();
            newJob.setVideoId(videoId);
            return newJob;
        });

        job.setStatus(status);
        videoJobRepository.save(job);
    }

    private void failJob(String videoId, String error) {
        VideoJob job = videoJobRepository.findById(videoId).orElseGet(() -> {
            VideoJob newJob = new VideoJob();
            newJob.setVideoId(videoId);
            return newJob;
        });

        job.setStatus(VideoJob.JobStatus.FAILED);
        job.setErrorMessage(error);
        videoJobRepository.save(job);
    }

    private String resolveBucketName(TranscodeJobMessage message) {
        if (message.getChunks() == null || message.getChunks().isEmpty()) {
            return defaultBucketName;
        }

        String firstChunkUrl = message.getChunks().get(0).getS3Url();
        if (firstChunkUrl == null || !firstChunkUrl.contains("://")) {
            return defaultBucketName;
        }

        try {
            URI uri = URI.create(firstChunkUrl);
            String host = uri.getHost();

            if (host != null && host.contains(".s3.")) {
                return host.split("\\.s3\\.")[0];
            }

            String path = uri.getPath();
            if (path != null && path.startsWith("/")) {
                String[] parts = path.split("/");
                if (parts.length > 1 && !parts[1].isEmpty()) {
                    return parts[1];
                }
            }

            return host != null ? host : defaultBucketName;
        } catch (Exception e) {
            return defaultBucketName;
        }
    }

    private void cleanup(File workingDir) {
        if (workingDir != null && workingDir.exists()) {
            FileSystemUtils.deleteRecursively(workingDir);
        }
    }
}
