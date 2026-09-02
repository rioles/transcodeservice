package com.stream.transcoder.encoder;

import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class EncoderFactory {

    public enum EncodingFormat {
        HLS, DASH
    }

    private final Map<EncodingFormat, VideoEncoder> encoders;

    public EncoderFactory(List<VideoEncoder> encoderList) {
        this.encoders = encoderList.stream()
                .collect(Collectors.toMap(
                        VideoEncoder::getSupportedFormat,
                        Function.identity()
                ));
    }

    public VideoEncoder getEncoder(EncodingFormat format) {
        VideoEncoder encoder = encoders.get(format);
        if (encoder == null) {
            throw new UnsupportedOperationException("Unsupported encoding format: " + format);
        }
        return encoder;
    }
}
