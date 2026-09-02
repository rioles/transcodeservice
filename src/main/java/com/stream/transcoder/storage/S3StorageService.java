package com.stream.transcoder.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

@Slf4j
@Service
public class S3StorageService {

    private final S3Client s3Client;
    private final String defaultBucketName;

    public S3StorageService(
            S3Client s3Client, 
            @Value("${aws.s3.bucket}") String defaultBucketName) {
        this.s3Client = s3Client;
        this.defaultBucketName = defaultBucketName;
    }

    public void downloadFile(String s3UrlOrKey, File destination) throws IOException {
        String bucket = this.defaultBucketName;
        String key = s3UrlOrKey;

        if (s3UrlOrKey == null || s3UrlOrKey.isBlank()) {
            throw new IllegalArgumentException("Le paramètre s3UrlOrKey ne peut pas être vide");
        }

        // Gestion propre du format s3://bucket/key
        if (s3UrlOrKey.startsWith("s3://")) {
            String rawPath = s3UrlOrKey.substring(5); // Retire 's3://'
            int firstSlash = rawPath.indexOf('/');
            if (firstSlash != -1) {
                bucket = rawPath.substring(0, firstSlash);
                key = rawPath.substring(firstSlash + 1);
            } else {
                throw new IllegalArgumentException("Format S3 URI invalide : " + s3UrlOrKey);
            }
        } 
        // Gestion des URLs HTTP/HTTPS (ex: https://bucket.s3.region.amazonaws.com/key)
        else if (s3UrlOrKey.startsWith("http://") || s3UrlOrKey.startsWith("https://")) {
            try {
                var s3Uri = s3Client.utilities().parseUri(URI.create(s3UrlOrKey));
                bucket = s3Uri.bucket().orElse(this.defaultBucketName);
                key = s3Uri.key().orElseThrow(() -> 
                    new IllegalArgumentException("Key introuvable dans l'URL : " + s3UrlOrKey));
            } catch (Exception e) {
                log.warn("Impossible de parser l'URL HTTPS via SDK utilities, fallback sur clé brute: {}", e.getMessage());
            }
        }

        // Nettoyage au cas où la clé commence par un slash leading
        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        if (destination.getParentFile() != null && !destination.getParentFile().exists()) {
            destination.getParentFile().mkdirs();
        }

        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            try (ResponseInputStream<GetObjectResponse> s3is = s3Client.getObject(getObjectRequest)) {
                Files.copy(s3is, destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (S3Exception e) {
            log.error("Échec du téléchargement S3 ({}/{}) : {}", bucket, key, e.awsErrorDetails().errorMessage());
            throw new IOException("Failed to download file from S3: " + s3UrlOrKey, e);
        }
    }

    public void uploadFile(String bucket, String key, File file) throws IOException {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
        } catch (S3Exception e) {
            log.error("Échec de l'upload S3 ({}/{}) : {}", bucket, key, e.awsErrorDetails().errorMessage());
            throw new IOException("Failed to upload file to S3: " + key, e);
        }
    }
}
