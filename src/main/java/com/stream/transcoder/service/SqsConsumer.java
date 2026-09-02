package com.stream.transcoder.service;

import com.stream.transcoder.dto.TranscodeJobMessage;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SqsConsumer {

    private final TranscodeOrchestrator transcodeOrchestrator;

    /**
     * Consumes transcode job messages from the SQS queue and delegates
     * processing to TranscodeOrchestrator.
     *
     * Spring Cloud AWS resolves the queue by its name.
     * AWS SDK credentials are provided automatically through
     * EKS Pod Identity.
     *
     * Any exception prevents successful message acknowledgement,
     * allowing SQS to make the message visible again after the
     * visibility timeout.
     */
    @SqsListener("${transcode.queue.name}")
    public void listen(TranscodeJobMessage message) {

        log.info(
            "Received transcode job for video: {}",
            message.getVideoId()
        );

        try {
            transcodeOrchestrator.processJob(message);

            log.info(
                "Successfully processed job: {}",
                message.getVideoId()
            );

        } catch (Exception e) {

            log.error(
                "Error processing job: {}",
                message.getVideoId(),
                e
            );

            throw e;
        }
    }
}

