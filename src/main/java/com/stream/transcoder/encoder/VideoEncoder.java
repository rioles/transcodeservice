package com.stream.transcoder.encoder;

import com.stream.transcoder.resolution.ResolutionProfile;
import java.io.File;
import java.io.IOException;

public interface VideoEncoder {

    EncoderFactory.EncodingFormat getSupportedFormat();

    Process encode(File assembledVideo, ResolutionProfile profile, File outputDir) throws IOException;
}
