package com.stream.transcoder.encoder;

import com.stream.transcoder.resolution.ResolutionProfile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class HlsEncoder implements VideoEncoder {

    @Value("${transcode.hls.segment-duration-seconds:6}")
    private int segmentDuration;

    @Override
    public EncoderFactory.EncodingFormat getSupportedFormat() {
        return EncoderFactory.EncodingFormat.HLS;
    }

    @Override
    public Process encode(File assembledVideo, ResolutionProfile profile, File outputDir) throws IOException {
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        List<String> command = new ArrayList<>();
        command.add("ffmpeg");
        command.add("-i");
        command.add(assembledVideo.getAbsolutePath());

        // Video settings
        command.add("-vf");
        command.add(String.format("scale=%d:%d", profile.width(), profile.height()));
        command.add("-c:v");
        command.add("libx264");
        command.add("-b:v");
        command.add(profile.videoBitrate());
        command.add("-preset");
        command.add("veryfast");

        // Audio settings
        command.add("-c:a");
        command.add("aac");
        command.add("-b:a");
        command.add(profile.audioBitrate());

        // HLS settings
        command.add("-f");
        command.add("hls");
        command.add("-hls_time");
        command.add(String.valueOf(segmentDuration));
        command.add("-hls_playlist_type");
        command.add("vod");
        command.add("-hls_segment_filename");
        command.add(new File(outputDir, "segment_%03d.ts").getAbsolutePath());

        command.add(new File(outputDir, "playlist.m3u8").getAbsolutePath());
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        return pb.start();
    }
}
