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

    /**
     * Retorna un flujo reactivo (Flux) que emite una secuencia de caracteres
     * provenientes de nombres procesados. Si el flujo resultante está vacío,
     * se sustituye por otro flujo predeterminado con una transformación aplicada.
     *<p>
     * Este método aplica una transformación funcional a un Flux de nombres,
     * convirtiéndolos en mayúsculas, filtrando por longitud y dividiéndolos en
     * caracteres individuales. Si el flujo resultante está vacío, se cambia a
     * un flujo alternativo con un valor por defecto transformado.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - switchIfEmpty(defaultFlux):
     *   Si el flujo principal queda vacío después de la transformación y filtrado,
     *   se cambia a un flujo alternativo (`defaultFlux`). A diferencia de
     *   `defaultIfEmpty`, que emite un solo valor si el flujo está vacío,
     *   `switchIfEmpty` permite proporcionar un flujo completo como alternativa.
     *
     * @param stringLength longitud mínima que deben tener los nombres para ser incluidos en el flujo.
     * @return un Flux que emite los caracteres individuales de los nombres procesados
     *         o, si el flujo queda vacío, un flujo alternativo con un valor por defecto.
     */
    public Flux<String> namesFluxTransformSwitchIfEmpty(int stringLength) {

        UnaryOperator<Flux<String>> filterMap = names -> names
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitString);

        Flux<String> defaultFlux = Flux.just("default").transform(filterMap);

        return Flux.fromIterable(List.of("adam", "anna"))
                .transform(filterMap)
                .switchIfEmpty(defaultFlux)
                .log();
    }


    /**
     * Retorna un flujo reactivo (Flux) que emite una secuencia de caracteres
     * provenientes de nombres procesados, introduciendo un retraso en la
     * transformación de cada elemento.
     *<p>
     * Este método convierte los nombres en mayúsculas, los filtra por longitud
     * y luego los descompone en caracteres individuales utilizando un método
     * que introduce un retraso en la emisión de cada carácter.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - flatMap(this::splitStringWithDelay):
     *   Similar a `flatMap`, pero en este caso, cada nombre es transformado
     *   asíncronamente utilizando `splitStringWithDelay`, lo que puede generar
     *   una ejecución no secuencial de los elementos. Esto es útil cuando cada
     *   transformación implica una operación asíncrona, como llamadas a bases
     *   de datos o servicios externos.
     *
     * @param stringLength longitud mínima que deben tener los nombres para ser incluidos en el flujo.
     * @return un Flux que emite los caracteres individuales de los nombres procesados con un retraso.
     */
    public Flux<String> namesFluxFlatMapWithDelay(int stringLength) {
        return Flux.fromIterable(List.of("adam", "anna", "jack", "jenny"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitStringWithDelay)
                .log();
    }

    /**
     * Retorna un flujo reactivo (Flux) que emite una secuencia de caracteres
     * provenientes de nombres procesados. Cada transformación se realiza de
     * manera secuencial, garantizando el orden de las emisiones.
     *<p>
     * Este método convierte los nombres en mayúsculas, los filtra por longitud
     * y luego los descompone en caracteres individuales, procesándolos en
     * un orden secuencial utilizando un método con un retraso.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - concatMap(this::splitStringWithDelay):
     *   Similar a `flatMap`, pero garantiza que los elementos sean procesados
     *   secuencialmente. Cada nombre es transformado de manera asíncrona, pero el
     *   procesamiento de cada elemento se realiza en el orden de aparición,
     *   lo que es útil cuando se requiere mantener la secuencia original.
     *
     * @param stringLength longitud mínima que deben tener los nombres para ser incluidos en el flujo.
     * @return un Flux que emite los caracteres individuales de los nombres procesados con retraso
     *         y en orden secuencial.
     */
    public Flux<String> namesFluxConcatMap(int stringLength) {
        return Flux.fromIterable(List.of("adam", "anna"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .concatMap(this::splitStringWithDelay)
                .log();
    }

    /**
     * Retorna un mono reactivo (Mono) que emite un único valor de tipo String,
     * después de haberlo transformado y filtrado.
     *<p>
     * Este método crea un Mono con el valor "mono", lo transforma a mayúsculas
     * y luego filtra si la longitud del nombre es mayor que 3.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - Mono.just("mono"):
     *   Crea un Mono que emite un único valor ("mono"). Esta es la forma básica
     *   de crear un Mono con un valor específico.
     *<p>
     * - map(String::toUpperCase):
     *   Aplica una transformación sincrónica al valor emitido por el Mono,
     *   convirtiendo el valor a mayúsculas.
     *<p>
     * - filter(name -> name.length() > 3):
     *   Filtra el valor emitido por el Mono, permitiendo su paso solo si cumple
     *   con la condición de que la longitud del nombre es mayor que 3. Si no
     *   cumple la condición, el Mono no emite ningún valor.
     *
     * @return un Mono que emite el valor transformado o nada si no cumple con el filtro.
     */
    public Mono<String> nameMono(){
        return Mono.just("mono")
                .map(String::toUpperCase)
                .filter(name -> name.length()>3)
                .log();
    }

    /**
     * Retorna un mono reactivo (Mono) que emite un único valor de tipo String,
     * aplicando una transformación y un filtro. Si el filtro no pasa, se emite un valor por defecto.
     *<p>
     * Este método crea un Mono con el valor "mono", lo transforma a mayúsculas,
     * luego filtra si la longitud del nombre es mayor que 6. Si no pasa el filtro,
     * emite un valor predeterminado en lugar de no emitir nada.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - defaultIfEmpty("default"):
     *   Si el Mono no emite ningún valor (lo que ocurre cuando no pasa el filtro),
     *   se emite un valor predeterminado, en este caso "default". Esto garantiza que el Mono
     *   siempre emitirá un valor, incluso si el flujo original está vacío.
     *
     * @return un Mono que emite el valor transformado si cumple el filtro,
     *         o un valor por defecto si no lo cumple.
     */
    public Mono<String> nameMonoDefaultIsEmpty(){
        return Mono.just("mono")
                .map(String::toUpperCase)
                .filter(name -> name.length()>6)
                .defaultIfEmpty("default")
                .log();
    }

    /**
     * Retorna un mono reactivo (Mono) que emite un único valor de tipo String,
     * aplicando una transformación y un filtro. Si el filtro no pasa, se cambia el flujo
     * por otro Mono con un valor por defecto.
     *<p>
     * Este método crea un Mono con el valor "mono", lo transforma a mayúsculas,
     * luego filtra si la longitud del nombre es mayor que 6. Si no pasa el filtro,
     * el flujo se redirige a otro Mono que emite un valor predeterminado.
     *<p>
     * Nuevo método de Project Reactor utilizado:
     *<p>
     * - switchIfEmpty(Mono.just("default")):
     *   Si el Mono original no emite ningún valor (cuando no pasa el filtro),
     *   `switchIfEmpty` permite redirigir el flujo hacia otro Mono alternativo
     *   que emite el valor "default". Esto asegura que el flujo siempre emitirá
     *   un valor, ya sea el original o uno predeterminado.
     *
     * @return un Mono que emite el valor transformado si cumple el filtro,
     *         o un valor por defecto si no lo cumple.
     */
    public Mono<String> nameMonoSwitchIfEmpty(){
        return Mono.just("mono")
                .map(String::toUpperCase)
                .filter(name -> name.length()>6)
                .switchIfEmpty(Mono.just("default"))
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
