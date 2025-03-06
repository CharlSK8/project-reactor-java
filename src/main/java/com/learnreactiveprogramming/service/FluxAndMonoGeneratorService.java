package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;

import java.util.List;

public class FluxAndMonoGeneratorService {

    public static void main(String[] args) {
        namesFlux().subscribe(System.out::println);
    }

    public static Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"));
    }
}
