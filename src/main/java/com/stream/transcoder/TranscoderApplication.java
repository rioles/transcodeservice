package com.stream.transcoder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableAsync
@EnableScheduling
public class TranscoderApplication {

    public static void main(String[] args) {
        SpringApplication.run(TranscoderApplication.class, args);
    }
}
