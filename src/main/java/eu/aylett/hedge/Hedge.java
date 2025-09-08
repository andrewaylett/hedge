package eu.aylett.hedge;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Hedge {
  private final List<Long> allTimings = new ArrayList<>();
  private final AtomicLong allTimingsSum = new AtomicLong();
  private final AtomicLong currentDelay = new AtomicLong();
  private final Executor executor;

  public Hedge() {
    this(ForkJoinPool.commonPool());
  }

  public Hedge(Executor executor) {
    this.executor = executor;
  }

  protected synchronized void submitTiming(Duration duration) {
    var millis = duration.toMillis();
    if (millis <= 0) {
      return;
    }

    allTimings.add(millis);
    var allSum = allTimingsSum.addAndGet(millis);
    var mean = allSum / allTimings.size();

    var candidateDelay = currentDelay.get();
    var successful = evaluateCandidate(candidateDelay, mean);
    if (successful) {
      do {
        candidateDelay--;
      } while (evaluateCandidate(candidateDelay, mean));
      currentDelay.setRelease(candidateDelay + 1);
    } else {
      do {
        candidateDelay++;
      } while (!evaluateCandidate(candidateDelay, mean));
      currentDelay.setRelease(candidateDelay);
    }
  }

  /**
   * If we'd be (on average) faster to try a new task at this delay.
   */
  private boolean evaluateCandidate(long candidateDelay, long mean) {
    var meanOfGreaterThanCandidate = allTimings.stream().mapToLong(x -> x - candidateDelay).sum() / allTimings.size();
    return meanOfGreaterThanCandidate > mean;
  }

  public <T> T submit(Supplier<T> task) {
    var timingTask = new Supplier<T>() {
      @Override
      public T get() {
        var start = Instant.now();
        var result = task.get();

        var end = Instant.now();
        submitTiming(Duration.between(start, end));

        return result;
      }
    };
    var future = CompletableFuture.supplyAsync(timingTask, executor);
    var hedge = CompletableFuture.supplyAsync(timingTask, CompletableFuture.delayedExecutor(currentDelay(), TimeUnit.MILLISECONDS, executor));
    future.whenComplete((x, y) -> hedge.cancel(false));

    return future.applyToEither(hedge, x -> x).join();
  }

  protected long currentDelay() {
    return currentDelay.getOpaque();
  }

}
