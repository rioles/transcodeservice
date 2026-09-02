package com.stream.transcoder.resolution;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record ResolutionProfile(
    String name,
    int width,
    int height,
    String videoBitrate,
    String audioBitrate
) {

    // Matches: "5000", "5000k", "5 M", "128kbps", "5Mbit/s", "5.5M" (case-insensitive)
    private static final Pattern BITRATE_PATTERN =
        Pattern.compile("(?i)^\\s*([\\d.]+)\\s*(k|m)?\\s*(?:bps|bit/s)?\\s*$");

    /**
     * Total bandwidth in bits per second, combining video + audio bitrate.
     * Used for the BANDWIDTH attribute in the HLS master playlist.
     */
    public long bandwidthBps() {
        return parseBitrateToBps(videoBitrate) + parseBitrateToBps(audioBitrate);
    }

    private static long parseBitrateToBps(String bitrate) {
        if (bitrate == null || bitrate.isBlank()) {
            return 0L;
        }
        Matcher matcher = BITRATE_PATTERN.matcher(bitrate);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Unparseable bitrate value: '" + bitrate + "'");
        }

        double value = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2);
        double multiplier = switch (unit == null ? "" : unit.toLowerCase()) {
            case "k" -> 1_000d;
            case "m" -> 1_000_000d;
            default -> 1d;
        };
        return Math.round(value * multiplier);
    }
}
