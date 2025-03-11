package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Random;

public class FluxAndMonoGeneratorService {

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
//        fluxAndMonoGeneratorService.namesFlux().subscribe(System.out::println);
//        fluxAndMonoGeneratorService.nameMono().subscribe(System.out::println);
//        fluxAndMonoGeneratorService.namesFluxMap(3).subscribe(System.out::println);
//        fluxAndMonoGeneratorService.namesFluxFlatMap(3).subscribe(System.out::println);
//        fluxAndMonoGeneratorService.namesFluxFlatMapWithDelay(3).subscribe(System.out::println);
        fluxAndMonoGeneratorService.namesFluxConcatMap(3).subscribe(System.out::println);
    }

    public Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"))
                .log();
    }

    public Flux<String> namesFluxMap(int stringLength) {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .map( name -> name.length() + "-" + name)
                .log();
    }

    public Flux<String> namesFluxFlatMap(int stringLength) {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitString)
                .log();
    }

    public Flux<String> namesFluxTransform(int stringLength) {

        //Esto es útil si se requiere utilizar esta interface functional en otras partes del código
        UnaryOperator<Flux<String>> filterMap = names -> names
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitString);

        return Flux.fromIterable(List.of("adam", "anna"))
                .transform(filterMap)
                .log();
    }
    public Flux<String> namesFluxFlatMapWithDelay(int stringLength) {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitStringWithDelay)
                .log();
    }

    public Flux<String> namesFluxConcatMap(int stringLength) {
        return Flux.fromIterable(List.of("adam", "anna"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .concatMap(this::splitStringWithDelay)
                .log();
    }

    public Mono<String> nameMono(){
        return Mono.just("mono")
                .map(String::toUpperCase)
                .filter(name -> name.length()>3)
                .log();
    }

    public Mono<List<String>> nameMonoFlatMap(int stringLength){
        return Mono.just("mono")
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitStringMono)
                .log();
    }

    public Flux<String> nameMonoFlatMapMany(int stringLength){
        return Mono.just("mono")
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMapMany(this::splitString)
                .log();
    }

    private Mono<List<String>> splitStringMono(String s) {
        var charArray = s.split("");
        return Mono.just(List.of(charArray));
    }

    private Flux<String> splitString(String name){
        var charArray = name.split("");
        return Flux.fromArray(charArray);
    }

    private Flux<String> splitStringWithDelay(String name){
        var charArray = name.split("");
        var delay = new Random().nextInt(1000);
        return Flux.fromArray(charArray)
                .delayElements(Duration.ofMillis(delay));
    }

}
