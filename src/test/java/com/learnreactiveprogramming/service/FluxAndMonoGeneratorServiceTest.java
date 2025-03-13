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
    void nameMonoDefaultIsEmpty() {

        var nameMono = fluxAndMonoGeneratorService.nameMonoDefaultIsEmpty();

        StepVerifier.create(nameMono)
                .expectNext("default")
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

    @Test
    void namesFluxTransform() {

        int stringLength = 3;

        var namesFluxTransform = fluxAndMonoGeneratorService.namesFluxTransform(stringLength);

        StepVerifier.create(namesFluxTransform)
                .expectNext("A","D","A","M","A","N","N","A")
                .verifyComplete();
    }

    @Test
    void namesFluxTransformDefaultEmpty() {
        int stringLength = 6;

        var namesFluxTransform = fluxAndMonoGeneratorService.namesFluxTransformDefaultEmpty(stringLength);

        StepVerifier.create(namesFluxTransform)
                .expectNext("default")
                .verifyComplete();
    }

    @Test
    void namesFluxTransformSwitchIfEmpty() {
        int stringLength = 6;

        var namesFluxTransform = fluxAndMonoGeneratorService.namesFluxTransformSwitchIfEmpty(stringLength);

        StepVerifier.create(namesFluxTransform)
                .expectNext("D","E","F","A","U","L","T")
                .verifyComplete();
    }

    @Test
    void nameMonoSwitchIfEmpty() {
        var nameMono = fluxAndMonoGeneratorService.nameMonoSwitchIfEmpty();

        StepVerifier.create(nameMono)
                .expectNext("default")
                .verifyComplete();
    }

    @Test
    void fluxConcat() {

        var fluxConcat = fluxAndMonoGeneratorService.fluxConcat();

        StepVerifier.create(fluxConcat)
                .expectNext("A","B","C","D","E","F")
                .verifyComplete();
    }

    @Test
    void fluxConcatWith() {
        var fluxConcat = fluxAndMonoGeneratorService.fluxConcatWith();

        StepVerifier.create(fluxConcat)
                .expectNext("A","B","C","D","E","F")
                .verifyComplete();
    }

    @Test
    void monoConcatWith() {

        var monoConcatWith = fluxAndMonoGeneratorService.monoConcatWith();
        StepVerifier.create(monoConcatWith)
                .expectNext("a", "b")
                .verifyComplete();
    }

    @Test
    void fluxMerge() {
        var fluxMerge = fluxAndMonoGeneratorService.fluxMerge();

        StepVerifier.create(fluxMerge)
                .expectNext("A","D","B","E","C","F")
                .verifyComplete();
    }

    @Test
    void fluxMergeWith() {
        var fluxMergeWith = fluxAndMonoGeneratorService.fluxMergeWith();

        StepVerifier.create(fluxMergeWith)
                .expectNext("A","D","B","E","C","F")
                .verifyComplete();
    }

    @Test
    void monoMergeWith() {
        var monoMergeWith = fluxAndMonoGeneratorService.monoMergeWith();

        StepVerifier.create(monoMergeWith)
                .expectNext("A","B")
                .verifyComplete();
    }
}
