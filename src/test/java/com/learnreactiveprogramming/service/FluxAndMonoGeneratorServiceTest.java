package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.List;

class FluxAndMonoGeneratorServiceTest {

    FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

    @Test
    void namesFlux() {

        var nameFlux = fluxAndMonoGeneratorService.namesFlux();

        StepVerifier.create(nameFlux)
//                .expectNext("adam", "anna", "jack", "jenny")
//                .expectNextCount(4)
                .expectNext("adam")
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    void nameMono() {

        var nameMono = fluxAndMonoGeneratorService.nameMono();

        StepVerifier.create(nameMono)
//                .expectNext("mono")
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void namesFluxMap() {
        int stringLength = 3;

        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxMap(stringLength);

        StepVerifier.create(namesFluxMap)
                .expectNext("4-ADAM")
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    void namesFluxFlatMapWithDelay() {

        int stringLength = 3;

        var namesFluxFlatMapWithDelay = fluxAndMonoGeneratorService.namesFluxFlatMapWithDelay(stringLength);

        StepVerifier.create(namesFluxFlatMapWithDelay)
                .expectNextCount(17)
                .verifyComplete();
    }

    @Test
    void namesFluxConcatMap() {
        int stringLength = 3;

        var namesFluxConcatMap = fluxAndMonoGeneratorService.namesFluxConcatMap(stringLength);

        StepVerifier.create(namesFluxConcatMap)
                .expectNext("A","D","A","M","A","N","N","A")
                .verifyComplete();
    }

    @Test
    void nameMonoFlatMap() {
        int stringLength = 3;

        var nameMonoFlatMap = fluxAndMonoGeneratorService.nameMonoFlatMap(stringLength);

        StepVerifier.create(nameMonoFlatMap)
                .expectNext(List.of("M","O","N","O"))
                .verifyComplete();
    }

    @Test
    void nameMonoFlatMapMany() {
        int stringLength = 3;

        var nameMonoFlatMapMany = fluxAndMonoGeneratorService.nameMonoFlatMapMany(stringLength);

        StepVerifier.create(nameMonoFlatMapMany)
                .expectNext("M","O","N","O")
                .verifyComplete();
    }
}
