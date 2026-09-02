package com.stream.transcoder.storage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProgressiveUploader {

    private final S3StorageService s3StorageService;

    // Limite le nombre d'uploads S3 simultanés pour éviter la saturation du pool de connexions HTTP AWS (défaut SDK: 50)
    private final Semaphore s3UploadSemaphore = new Semaphore(15);

    @Value("${aws.s3.bucket-name:}")
    private String defaultBucketName;

    /**
     * Surveille le dossier de sortie HLS et téléverse les fichiers .ts au fur et à mesure.
     */
    public void watchAndUpload(Path outputDirPath, String bucket, String s3Prefix, Process ffmpegProcess) {
        String targetBucket = (bucket != null && !bucket.isBlank()) ? bucket : defaultBucketName;
        log.info("Démarrage de la surveillance progressive du dossier : {} vers S3 : {}/{}", outputDirPath, targetBucket, s3Prefix);

        Set<String> uploadedFiles = ConcurrentHashMap.newKeySet();

        try (ExecutorService uploadExecutor = Executors.newVirtualThreadPerTaskExecutor();
             WatchService watchService = FileSystems.getDefault().newWatchService()) {

            Files.createDirectories(outputDirPath);
            outputDirPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

            // 1. Écoute pendant que FFmpeg s'exécute
            while (ffmpegProcess.isAlive()) {
                drainWatchEvents(watchService, outputDirPath, targetBucket, s3Prefix, uploadedFiles, uploadExecutor);
            }

            // 2. Traitement des événements restants générés juste avant l'arrêt de FFmpeg
            log.info("Processus FFmpeg terminé. Purge des derniers événements WatchService pour : {}", outputDirPath);
            drainWatchEvents(watchService, outputDirPath, targetBucket, s3Prefix, uploadedFiles, uploadExecutor);

            // 3. Balayage final pour rattraper d'éventuels segments omis
            uploadMissingSegments(outputDirPath, targetBucket, s3Prefix, uploadedFiles, uploadExecutor);

            // 4. Attente de la fin de tous les téléversements en arrière-plan
            uploadExecutor.shutdown();
            if (!uploadExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
                log.warn("Le délai d'attente pour le téléversement des segments a expiré pour : {}", s3Prefix);
                uploadExecutor.shutdownNow();
            }

            // 5. Téléversement final des playlists .m3u8 une fois tous les .ts sur S3
            uploadPlaylistsRecursively(outputDirPath, outputDirPath, targetBucket, s3Prefix);

        } catch (IOException e) {
            log.error("Erreur d'I/O lors de la surveillance du dossier HLS : {}", outputDirPath, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Le fil de surveillance HLS a été interrompu", e);
        }
    }

    private void drainWatchEvents(WatchService watchService, Path outputDirPath, String bucket, String s3Prefix,
                                  Set<String> uploadedFiles, ExecutorService uploadExecutor) throws InterruptedException {
        WatchKey key = watchService.poll(200, TimeUnit.MILLISECONDS);
        if (key != null) {
            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                    log.warn("Événement WatchService OVERFLOW détecté pour : {}", outputDirPath);
                    continue;
                }

                Path filename = (Path) event.context();
                Path fullPath = outputDirPath.resolve(filename);

                if (filename.toString().endsWith(".ts")) {
                    processSegmentUpload(bucket, fullPath, s3Prefix, uploadedFiles, uploadExecutor);
                }
            }
            key.reset();
        }
    }

    private void processSegmentUpload(String bucket, Path filePath, String s3Prefix, Set<String> uploadedFiles, ExecutorService uploadExecutor) {
        String fileName = filePath.getFileName().toString();
        String s3Key = buildS3Key(s3Prefix, fileName);

        if (uploadedFiles.add(s3Key)) {
            uploadExecutor.submit(() -> {
                try {
                    waitForFileStability(filePath);
                    
                    s3UploadSemaphore.acquire();
                    try {
                        s3StorageService.uploadFile(bucket, s3Key, filePath.toFile());
                        log.debug("Segment téléversé avec succès : {}", s3Key);
                    } finally {
                        s3UploadSemaphore.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    uploadedFiles.remove(s3Key);
                } catch (Exception e) {
                    log.error("Échec du téléversement du segment : {}", s3Key, e);
                    uploadedFiles.remove(s3Key);
                }
            });
        }
    }

    private void uploadMissingSegments(Path dirPath, String bucket, String s3Prefix, Set<String> uploadedFiles, ExecutorService uploadExecutor) throws IOException {
        try (var stream = Files.walk(dirPath)) {
            stream.filter(Files::isRegularFile)
                  .filter(p -> p.toString().endsWith(".ts"))
                  .forEach(p -> processSegmentUpload(bucket, p, s3Prefix, uploadedFiles, uploadExecutor));
        }
    }

    private void uploadPlaylistsRecursively(Path rootPath, Path currentPath, String bucket, String s3Prefix) throws IOException {
        try (var stream = Files.list(currentPath)) {
            for (Path path : stream.toList()) {
                if (Files.isDirectory(path)) {
                    uploadPlaylistsRecursively(rootPath, path, bucket, s3Prefix);
                } else if (path.toString().endsWith(".m3u8")) {
                    String relativePath = rootPath.relativize(path).toString().replace("\\", "/");
                    String s3Key = buildS3Key(s3Prefix, relativePath);
                    s3StorageService.uploadFile(bucket, s3Key, path.toFile());
                    log.info("Playlist HLS finale téléversée : {}", s3Key);
                }
            }
        }
    }

    private String buildS3Key(String prefix, String relativePath) {
        if (prefix == null || prefix.isBlank()) {
            return relativePath;
        }
        String cleanPrefix = prefix.endsWith("/") ? prefix : prefix + "/";
        String cleanPath = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
        return cleanPrefix + cleanPath;
    }

    private void waitForFileStability(Path path) throws InterruptedException {
        long lastSize = -1;
        int maxRetries = 15; // Attente max 1.5s au lieu de 3s
        for (int i = 0; i < maxRetries; i++) {
            if (Files.exists(path)) {
                try {
                    long currentSize = Files.size(path);
                    if (currentSize > 0 && currentSize == lastSize) {
                        return;
                    }
                    lastSize = currentSize;
                } catch (IOException ignored) {
                }
            }
            Thread.sleep(100);
        }
        log.warn("Fichier non stable après attente : {}", path);
        throw new InterruptedException("Fichier non stable après attente");
    }
}
