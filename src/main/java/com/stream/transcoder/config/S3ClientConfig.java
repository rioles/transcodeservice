package com.stream.transcoder.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

@Configuration
public class S3ClientConfig {

    @Value("${spring.cloud.aws.region.static:us-east-1}")
    private String region;

    @Value("${aws.s3.endpoint:#{null}}")
    private String s3Endpoint;

    @Bean
    public S3Client s3Client() {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(region))
                // DefaultCredentialsProvider gère TOUT tout seul :
                // - Si AWS_ACCESS_KEY_ID existe (Local/Dev) -> il l'utilise
                // - Si c'est null (Prod AWS / Gateway Endpoint) -> il passe sur IAM/Pod Identity
                .credentialsProvider(DefaultCredentialsProvider.create());

        // Seul l'endpoint fait l'objet d'un contrôle si on est sur LocalStack
        if (s3Endpoint != null && !s3Endpoint.isBlank()) {
            builder.endpointOverride(URI.create(s3Endpoint))
                   .forcePathStyle(true); // Indispensable pour LocalStack
        }

        return builder.build();
    }
}
