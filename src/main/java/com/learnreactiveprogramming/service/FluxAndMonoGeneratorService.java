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

    /**
     * Retorna un flujo reactivo (Flux) que emite una secuencia de nombres.
     * <p>
     * Este método crea un Flux a partir de una lista inmutable de nombres y
     * registra eventos del flujo para su monitoreo y depuración.
     * <p>
     * Explicación de los métodos de Project Reactor utilizados:
     *<p>
     * - Flux.fromIterable(List.of(...)):
     *   Convierte una colección List en un Flux, emitiendo cada elemento de la lista de forma secuencial.
     *<p>
     * - log():
     *   Registra eventos internos del Flux en la consola, como la suscripción,
     *   las emisiones de datos y la cancelación.
     *
     * @return un Flux que emite una secuencia de nombres.
     */
    public Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"))
                .log();
    }

    /**
     * Retorna un flujo reactivo (Flux) que emite una secuencia de nombres procesados.
     *<p>
     * Este método parte de una lista de nombres, los convierte a mayúsculas,
     * filtra aquellos con una longitud mayor al valor proporcionado y los transforma
     * en una nueva representación con su longitud como prefijo.
     *<p>
     * Nuevos métodos de Project Reactor utilizados:
     *<p>
     * - map(String::toUpperCase):
     *   Convierte cada nombre a mayúsculas.
     *<p>
     * - filter(name -> name.length() > stringLength):
     *   Filtra los nombres cuya longitud sea mayor que el valor especificado en stringLength.
     *<p>
     * - map(name -> name.length() + "-" + name):
     *   Transforma cada nombre en un nuevo formato donde se antepone su longitud seguida de un guion.
     *
     * @param stringLength longitud mínima que deben tener los nombres para ser incluidos en el flujo.
     * @return un Flux que emite los nombres procesados con su longitud como prefijo.
     */
    public Flux<String> namesFluxMap(int stringLength) {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .map( name -> name.length() + "-" + name)
                .log();
    }

    /**
     * Retorna un flujo reactivo (Flux) que emite una secuencia de caracteres
     * provenientes de nombres procesados.
     *<p>
     * Este método parte de una lista de nombres, los convierte a mayúsculas,
     * filtra aquellos con una longitud mayor al valor proporcionado y luego
     * divide cada nombre en caracteres individuales utilizando un subflujo.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - flatMap(this::splitString):
     *   Transforma cada nombre en un nuevo flujo de caracteres individuales.
     *   A diferencia de map(), que mantiene una correspondencia uno a uno,
     *   flatMap() descompone cada elemento en múltiples emisiones dentro del mismo flujo.
     *
     * @param stringLength longitud mínima que deben tener los nombres para ser incluidos en el flujo.
     * @return un Flux que emite los caracteres individuales de los nombres procesados.
     */
    public Flux<String> namesFluxFlatMap(int stringLength) {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitString)
                .log();
    }

    /**
     * Retorna un flujo reactivo (Flux) que emite una secuencia de caracteres
     * provenientes de nombres procesados.
     *<p>
     * Este método utiliza una transformación funcional para aplicar una serie
     * de operaciones a un Flux de nombres, convirtiéndolos en mayúsculas,
     * filtrando por longitud y dividiéndolos en caracteres individuales.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - transform(filterMap):
     *   Aplica una función de transformación {@link UnaryOperator}
     *   al flujo de datos. Esto permite reutilizar la lógica de
     *   procesamiento en otros lugares del código y mantener el código más modular.
     *
     * @param stringLength longitud mínima que deben tener los nombres para ser incluidos en el flujo.
     * @return un Flux que emite los caracteres individuales de los nombres procesados.
     */
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

    /**
     * Retorna un flujo reactivo (Flux) que emite una secuencia de caracteres
     * provenientes de nombres procesados. Si no hay elementos después del
     * filtrado, se emite un valor por defecto.
     *<p>
     * Este método aplica una transformación funcional a un Flux de nombres,
     * convirtiéndolos en mayúsculas, filtrando por longitud y dividiéndolos
     * en caracteres individuales. Si el flujo resultante está vacío, se emite
     * un valor predeterminado.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - defaultIfEmpty("default"):
     *   Si el flujo queda vacío después de aplicar la transformación y el filtrado,
     *   emite el valor "default" en su lugar. Esto es útil para evitar flujos vacíos
     *   y proporcionar un fallback en caso de que no haya elementos que cumplan la condición.
     *
     * @param stringLength longitud mínima que deben tener los nombres para ser incluidos en el flujo.
     * @return un Flux que emite los caracteres individuales de los nombres procesados o
     *         el valor "default" si el flujo queda vacío.
     */
    public Flux<String> namesFluxTransformDefaultEmpty(int stringLength) {

        UnaryOperator<Flux<String>> filterMap = names -> names
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitString);

        return Flux.fromIterable(List.of("adam", "anna"))
                .transform(filterMap)
                .defaultIfEmpty("default")
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
