package com.stream.transcoder.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum VideoStatus {
    PENDING("pending"),
    QUEUED("queued"),
    PROCESSING("processing"),
    READY("ready"),
    FAILED("failed");

    private final String value;

    @JsonValue
    @Override
    public String toString() {
        return value;
    }

    @JsonCreator
    public static VideoStatus fromValue(String text) {
        for (VideoStatus status : VideoStatus.values()) {
            if (status.value.equalsIgnoreCase(text)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown VideoStatus: " + text);
    }
}
