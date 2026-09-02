package com.stream.transcoder.entity;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
public class VideoStatusConverter implements AttributeConverter<VideoStatus, String> {

    @Override
    public String convertToDatabaseColumn(VideoStatus status) {
        return status != null ? status.getValue() : null;
    }

    @Override
    public VideoStatus convertToEntityAttribute(String dbData) {
        return dbData != null ? VideoStatus.fromValue(dbData) : null;
    }
}
