package com.stream.transcoder.resolution;

import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ResolutionRegistry {
    private final Map<String, ResolutionProfile> profiles = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        register(new ResolutionProfile("1080p", 1920, 1080, "5000k", "192k"));
        register(new ResolutionProfile("720p", 1280, 720, "2500k", "128k"));
        register(new ResolutionProfile("480p", 854, 480, "1000k", "96k"));
        register(new ResolutionProfile("360p", 640, 360, "600k", "64k"));
    }

    public void register(ResolutionProfile profile) {
        profiles.put(profile.name(), profile);
    }

    public ResolutionProfile get(String name) {
        ResolutionProfile profile = profiles.get(name);
        if (profile == null) {
            throw new IllegalArgumentException("Unknown resolution profile: " + name);
        }
        return profile;
    }

    public Collection<ResolutionProfile> getAll() {
        return profiles.values();
    }
}
