

package com.stream.transcoder.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TranscodeJobMessage {

    @JsonProperty("video_id")
    private String videoId;

    @JsonProperty("composite_hash")
    private String compositeHash;

    @JsonProperty("is_deduplicated")
    private Boolean isDeduplicated;

    @JsonProperty("chunks")
    private List<ChunkDto> chunks;

    @JsonProperty("target_resolutions")
    private List<String> targetResolutions;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChunkDto {

        @JsonProperty("chunk_index")
        private int chunkIndex;

        @JsonProperty("chunk_hash")
        private String chunkHash;

        @JsonProperty("s3_url")
        private String s3Url; // Contient s3_key ou URL transmise par Python
    }
}
