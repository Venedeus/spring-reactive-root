package my.shvetsov.spring.reactive.springreactivesimpledemo;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

@Slf4j
public class ReactiveTutorial {

  private Mono<String> testMono() {
    return Mono.just("Java").log();
  }

  private Flux<String> testFlux() {
    List<String> programmingLanguages = List.of("Java", "Cpp", "Rust", "Dart");
    return Flux.fromIterable(programmingLanguages).log();
  }

  private Flux<String> testMap() {
    Flux<String> flux = Flux.just("Java", "Cpp", "Rust", "Dart");
    return flux.map(s -> s.toUpperCase(Locale.ROOT));
  }

  private Flux<String> testFlatMap() {
    Flux<String> flux = Flux.just("Java", "Cpp", "Rust", "Dart");
    return flux.flatMap(s -> Mono.just(s.toUpperCase(Locale.ROOT)));
  }

  private Flux<String> testSkip() {
    Flux<String> flux = Flux.just("Java", "Cpp", "Rust", "Dart");
    return flux.skip(2);
  }

  private Flux<String> testSkipDuration() {
    Flux<String> flux = Flux.just("Java", "Cpp", "Rust", "Dart").delayElements(Duration.ofSeconds(1));
    return flux.skip(Duration.ofMillis(2010));
  }

  private Flux<String> testSkipLast() {
    Flux<String> flux = Flux.just("Java", "Cpp", "Rust", "Dart");
    return flux.skipLast(2);
  }

  private Flux<Integer> testSkipWhile() {
    Flux<Integer> flux = Flux.range(1, 20);
    return flux.skipWhile(integer -> integer < 10);
  }

  private Flux<Integer> testSkipUntil() {
    Flux<Integer> flux = Flux.range(1, 20);
    return flux.skipUntil(integer -> integer == 14);
  }

  private Flux<Integer> testConcat() {
    Flux<Integer> flux1 = Flux.range(1, 20);
    Flux<Integer> flux2 = Flux.range(101, 20);
    return Flux.concat(flux1, flux2);
  }

  private Flux<Integer> testMerge() {
    Flux<Integer> flux1 = Flux.range(1, 20).delayElements(Duration.ofMillis(100));
    Flux<Integer> flux2 = Flux.range(101, 20).delayElements(Duration.ofMillis(100));
    return Flux.merge(flux1, flux2);
  }

  private Flux<Tuple2<Integer, Integer>> testZipTuple2() {
    Flux<Integer> flux1 = Flux.range(1, 20);
    Flux<Integer> flux2 = Flux.range(101, 30);
    return Flux.zip(flux1, flux2);
  }

  private Flux<Tuple3<Integer, Integer, Integer>> testZipTuple3() {
    Flux<Integer> flux1 = Flux.range(1, 20);
    Flux<Integer> flux2 = Flux.range(101, 30);
    Flux<Integer> flux3 = Flux.range(1001, 10);
    return Flux.zip(flux1, flux2, flux3);
  }

  private Flux<Tuple2<Integer, Integer>> testZipWithMono() {
    Flux<Integer> flux = Flux.range(1, 20);
    Mono<Integer> mono = Mono.just(1);
    return Flux.zip(flux, mono);
  }

  private Mono<List<Integer>> testCollect() {
    Flux<Integer> flux = Flux.range(1, 10).delayElements(Duration.ofMillis(100));
    return flux.collectList();
  }

  private Flux<List<Integer>> testBuffer() {
    Flux<Integer> flux = Flux.range(1, 10).delayElements(Duration.ofMillis(100));
    return flux.buffer();
  }

  private Flux<List<Integer>> testBufferSized() {
    Flux<Integer> flux = Flux.range(1, 10).delayElements(Duration.ofMillis(100));
    return flux.buffer(3);
  }

  private Flux<List<Integer>> testBufferDuration() {
    Flux<Integer> flux = Flux.range(1, 10).delayElements(Duration.ofMillis(1000));
    return flux.buffer(Duration.ofSeconds(3));
  }

  private Mono<Map<Integer, Integer>> testMapCollection() {
    Flux<Integer> flux = Flux.range(1, 10);
    return flux.collectMap(integer -> integer, integer -> integer * integer);
  }

  private Flux<Integer> testDoOnEach() {
    Flux<Integer> flux = Flux.range(1, 10);
    return flux.doOnEach(signal -> log.info("Signal: " + signal));
  }

  private Flux<Integer> testDoOnEachComplete() {
    Flux<Integer> flux = Flux.range(1, 10);
    return flux.doOnEach(signal -> {
      if (signal.getType() == SignalType.ON_COMPLETE) {
        log.info("I'm done");
      } else {
        log.info("Signal: " + signal);
      }
    });
  }

  private Flux<Integer> testDoOnComplete() {
    Flux<Integer> flux = Flux.range(1, 10);
    return flux.doOnComplete(() -> log.info("I'm done"));
  }

  private Flux<Integer> testDoOnNext() {
    Flux<Integer> flux = Flux.range(1, 10);
    return flux.doOnNext(signal -> log.info(String.valueOf(signal)));
  }

  private Flux<Integer> testDoOnSubscribe() {
    Flux<Integer> flux = Flux.range(1, 10);
    return flux.doOnSubscribe(signal -> log.info("Subscribed!"));
  }

  private Flux<Integer> testDoOnCancel() {
    Flux<Integer> flux = Flux.range(1, 10).delayElements(Duration.ofSeconds(1));
    return flux.doOnCancel(() -> log.info("Cancelled!"));
  }

  private Flux<Integer> testErrorHandlingContinue() {
    return Flux.range(1, 10).map(integer -> {
          if (integer == 5) {
            throw new RuntimeException("Unexpected number");
          }
          return integer;
        }
    ).onErrorContinue((throwable, o) -> log.error("Don't worry about: " + o));
  }

  private Flux<Integer> testErrorHandlingReturn() {
    return Flux.range(1, 10).map(integer -> {
          if (integer == 5) {
            throw new RuntimeException("Unexpected number");
          }
          return integer;
        }
    ).onErrorReturn(-1);
  }

  private Flux<Integer> testErrorHandlingReturnSpecialValue() {
    return Flux.range(1, 10).map(integer -> {
              if (integer == 5) {
                throw new RuntimeException("Unexpected number");
              }
              return integer;
            }
        ).onErrorReturn(RuntimeException.class, -1)
        .onErrorReturn(ArithmeticException.class, -1);
  }

  private Flux<Integer> testErrorHandlingResume() {
    Flux<Integer> flux = Flux.range(1, 10).map(integer -> {
              if (integer == 5) {
                throw new RuntimeException("Unexpected number");
              }
              return integer;
            }
        );
    Flux<Integer> resumeFlux = Flux.range(100, 5);
    return flux.onErrorResume(throwable -> resumeFlux);
  }

  private Flux<Integer> testErrorHandlingResumeMono() {
    Flux<Integer> flux = Flux.range(1, 10).map(integer -> {
          if (integer == 5) {
            throw new RuntimeException("Unexpected number");
          }
          return integer;
        }
    );
    Mono<Integer> resumeMono = Mono.just(100);
    return flux.onErrorResume(throwable -> resumeMono);
  }

  private Flux<Integer> testErrorHandlingMap() {
    Flux<Integer> flux = Flux.range(1, 10).map(integer -> {
          if (integer == 5) {
            throw new RuntimeException("Unexpected number");
          }
          return integer;
        }
    );
    return flux.onErrorMap(throwable -> new UnsupportedOperationException(throwable.getMessage()));
  }

  @SneakyThrows
  public static void main(String[] args) {
    ReactiveTutorial reactiveTutorial = new ReactiveTutorial();

    log.info("Testing Mono");
    reactiveTutorial.testMono().subscribe(log::info);

    log.info("Testing Flux");
    reactiveTutorial.testFlux().subscribe(log::info);

    log.info("Testing map");
    reactiveTutorial.testMap().subscribe(log::info);

    log.info("Testing flatMap");
    reactiveTutorial.testFlatMap().subscribe(log::info);

    log.info("Testing skip");
    reactiveTutorial.testSkip().subscribe(log::info);

    log.info("Testing skip duration");
    reactiveTutorial.testSkipDuration().subscribe(log::info);
    Thread.sleep(5_000);

    log.info("Testing skip last");
    reactiveTutorial.testSkipLast().subscribe(log::info);

    log.info("Testing skip while");
    reactiveTutorial.testSkipWhile().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing skip until");
    reactiveTutorial.testSkipUntil().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing concat");
    reactiveTutorial.testConcat().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing merge");
    reactiveTutorial.testMerge().subscribe(data -> log.info(String.valueOf(data)));
    Thread.sleep(5_000);

    log.info("Testing zip tuple2");
    reactiveTutorial.testZipTuple2().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing zip tuple3");
    reactiveTutorial.testZipTuple3().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing zip with mono");
    reactiveTutorial.testZipWithMono().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing collectList");
    reactiveTutorial.testCollect().subscribe(data -> log.info(String.valueOf(data)));
    Thread.sleep(5_000);

    log.info("Testing collectList blocked");
    reactiveTutorial.testCollect().block().forEach(data -> log.info(String.valueOf(data)));

    log.info("Testing buffer");
    reactiveTutorial.testBuffer().subscribe(data -> log.info(String.valueOf(data)));
    Thread.sleep(5_000);

    log.info("Testing buffer sized");
    reactiveTutorial.testBufferSized().subscribe(data -> log.info(String.valueOf(data)));
    Thread.sleep(5_000);

    log.info("Testing buffer duration");
    reactiveTutorial.testBufferDuration().subscribe(data -> log.info(String.valueOf(data)));
    Thread.sleep(5_000);

    log.info("Testing map collection");
    reactiveTutorial.testMapCollection().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing doOnEach");
    reactiveTutorial.testDoOnEach().subscribe();

    log.info("Testing doOnEach complete");
    reactiveTutorial.testDoOnEachComplete().subscribe();

    log.info("Testing doOnComplete");
    reactiveTutorial.testDoOnComplete().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing doOnNext");
    reactiveTutorial.testDoOnNext().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing doOnSubscribe");
    reactiveTutorial.testDoOnSubscribe().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing doOnCancel");
    Disposable disposable = reactiveTutorial.testDoOnCancel().subscribe(data -> log.info(String.valueOf(data)));
    Thread.sleep(3_500);
    disposable.dispose();

    log.info("Testing errorHandling continue");
    reactiveTutorial.testErrorHandlingContinue().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing errorHandling return");
    reactiveTutorial.testErrorHandlingReturn().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing errorHandling return");
    reactiveTutorial.testErrorHandlingReturnSpecialValue().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing errorHandling resume");
    reactiveTutorial.testErrorHandlingResume().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing errorHandling resume mono");
    reactiveTutorial.testErrorHandlingResumeMono().subscribe(data -> log.info(String.valueOf(data)));

    log.info("Testing errorHandling map");
    reactiveTutorial.testErrorHandlingMap().subscribe(data -> log.info(String.valueOf(data)));
  }
}
