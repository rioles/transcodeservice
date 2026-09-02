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
     * processing to {@link TranscodeOrchestrator}.
     * <p>
     * Visibility heartbeat is not yet implemented. For long-running jobs,
     * a scheduled task extending message visibility via {@code SqsAsyncClient}
     * should be added to avoid premature redelivery.
     * <p>
     * Any exception thrown here prevents Spring Cloud AWS from deleting the
     * message, so the job will be retried after the visibility timeout expires.
     */
    @SqsListener(value = "${transcode.queue.url}")
    public void listen(TranscodeJobMessage message) {
        log.info("Received transcode job for video: {}", message.getVideoId());
        try {
            transcodeOrchestrator.processJob(message);
            log.info("Successfully processed job: {}", message.getVideoId());
        } catch (Exception e) {
            log.error("Error processing job: {}", message.getVideoId(), e);
            throw e;
        }
    }
}
