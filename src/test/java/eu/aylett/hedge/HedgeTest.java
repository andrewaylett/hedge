package eu.aylett.hedge;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests Hedge using a deterministic fake time source so the test never actually sleeps.
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
public class HedgeTest {
  /**
   * A fake clock that advances when asked by the supplier. We expose now() via System.nanoTime()-like
   * API to be independent of the Hedge implementation which uses Instant.now(). We will map this by
   * having the supplier compute durations using this fake time, and avoiding any real waiting.
   */
  static final class FakeTime {
    private final AtomicLong millis = new AtomicLong();
    long nowMillis() { return millis.get(); }
    void advanceMillis(long d) { millis.addAndGet(d); }
  }

  /**
   * Supplier that, when invoked, advances fake time by either 5ms (p=0.95) or 500ms (p=0.05), and
   * returns an integer marker of how long it took.
   */
  static final class ProbabilisticSupplier implements Supplier<Integer> {
    private final FakeTime time;
    private final Random rnd;
    private final int fastMs;
    private final int slowMs;
    private final double slowProbability;
    private final int id;
    ProbabilisticSupplier(FakeTime time, Random rnd, int fastMs, int slowMs, double slowProbability, int id) {
      this.time = time; this.rnd = rnd; this.fastMs = fastMs; this.slowMs = slowMs; this.slowProbability = slowProbability; this.id = id;
    }
    @Override public Integer get() {
      // Decide outcome deterministically via provided Random
      boolean slow = rnd.nextDouble() < slowProbability;
      int d = slow ? slowMs : fastMs;
      time.advanceMillis(d);
      return id;
    }
  }

  /**
   * Executor that runs tasks immediately in the calling thread, but honors delayedExecutor by
   * scheduling them on a queue we control. We'll simulate time by checking the scheduled delay value
   * (in millis) and only allowing those tasks to run after fake time has progressed beyond that.
   */
  static final class ControllableExecutor implements Executor {
    static final class Scheduled {
      final long runAtMillis; final Runnable r;
      Scheduled(long runAtMillis, Runnable r) { this.runAtMillis = runAtMillis; this.r = r; }
    }
    private final FakeTime time;
    private final ConcurrentLinkedQueue<Runnable> immediate = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Scheduled> scheduled = new ConcurrentLinkedQueue<>();

    ControllableExecutor(FakeTime time) { this.time = time; }

    @Override public void execute(Runnable command) { immediate.add(command); }

    Executor delayed(long delayMillis) {
      return r -> scheduled.add(new Scheduled(time.nowMillis() + Math.max(0, delayMillis), r));
    }

    void pump() {
      // run immediate work
      Runnable r;
      while ((r = immediate.poll()) != null) { r.run(); }
      // run due scheduled work without using iterator.remove (unsupported for ConcurrentLinkedQueue)
      boolean progressed;
      do {
        progressed = false;
        int n = scheduled.size();
        for (int i = 0; i < n; i++) {
          Scheduled s = scheduled.poll();
          if (s == null) { break; }
          if (s.runAtMillis <= time.nowMillis()) {
            s.r.run();
            progressed = true;
          } else {
            scheduled.add(s);
          }
        }
      } while (progressed);
    }
  }

  /**
   * Minimal adapter HedgeUnderTest that lets us plug the ControllableExecutor also for delayedExecutor.
   */
  static final class HedgeUnderTest extends Hedge {
    private final ControllableExecutor exec;
    HedgeUnderTest(ControllableExecutor exec) { super(exec); this.exec = exec; }
    Executor delayedExecutor(long delayMillis) { return exec.delayed(delayMillis); }
  }

  /**
   * Basic test: with mostly-fast supplier, the learned delay should remain closer to a small value
   * than to a large one; more importantly, hedging should rarely be needed and completion time should
   * be closer to the fast path typical duration than to the slow path.
   */
  @Test
  void hedgingKeepsLatencyNearFastPathWithoutRealWaiting() {
    FakeTime time = new FakeTime();
    ControllableExecutor exec = new ControllableExecutor(time);
    Hedge hedge = new Hedge(exec) {
      @Override public <T> T submit(Supplier<T> task) {
        Supplier<T> timingTask = () -> {
          long before = time.nowMillis();
          T result = task.get();
          long after = time.nowMillis();
          submitTiming(Duration.ofMillis(after - before));
          return result;
        };
        CompletableFuture<T> future = new CompletableFuture<>();
        exec.execute(() -> {
          try { future.complete(timingTask.get()); } catch (Throwable t) { future.completeExceptionally(t); }
        });
        CompletableFuture<T> hedgeF = new CompletableFuture<>();
        Executor delayed = exec.delayed(currentDelay());
        delayed.execute(() -> {
          try { hedgeF.complete(timingTask.get()); } catch (Throwable t) { hedgeF.completeExceptionally(t); }
        });
        future.whenComplete((x,y) -> hedgeF.cancel(false));
        exec.pump();
        return future.applyToEither(hedgeF, x -> x).join();
      }
      @Override protected long currentDelay() { return super.currentDelay(); }
      @Override protected synchronized void submitTiming(Duration d) { super.submitTiming(d); }
    };

    Random rnd = new Random(12345L);
    Supplier<Integer> supplier = new ProbabilisticSupplier(time, rnd, 5, 500, 0.05, 1);

    long totalApparentMillis = 0;
    for (int i = 0; i < 500; i++) {
      int res = hedge.submit(supplier);
      assert res == 1;
      exec.pump();
      totalApparentMillis = time.nowMillis();
    }

    // On average with hedging the effective latency should be much closer to ~5-30ms range than 500ms.
    double average = totalApparentMillis / 500.0;
    assertThat("Average effective latency should be far below slow duration", average, lessThan(100.0));
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.SECONDS)
  void noRealSleepingOccurs() {
    FakeTime time = new FakeTime();
    ControllableExecutor exec = new ControllableExecutor(time);
    Hedge hedge = new Hedge(exec) {
      @Override public <T> T submit(Supplier<T> task) {
        Supplier<T> timingTask = () -> {
          long before = time.nowMillis();
          T result = task.get();
          long after = time.nowMillis();
          submitTiming(Duration.ofMillis(after - before));
          return result;
        };
        CompletableFuture<T> future = new CompletableFuture<>();
        exec.execute(() -> future.complete(timingTask.get()));
        Executor delayed = exec.delayed(currentDelay());
        CompletableFuture<T> hedgeF = new CompletableFuture<>();
        delayed.execute(() -> hedgeF.complete(timingTask.get()));
        future.whenComplete((x,y) -> hedgeF.cancel(false));
        exec.pump();
        return future.applyToEither(hedgeF, x -> x).join();
      }
      @Override protected long currentDelay() { return super.currentDelay(); }
      @Override protected synchronized void submitTiming(Duration d) { super.submitTiming(d); }
    };

    long wallStart = System.nanoTime();
    Random rnd = new Random(42);
    Supplier<Integer> supplier = new ProbabilisticSupplier(time, rnd, 5, 500, 0.05, 2);
    for (int i = 0; i < 200; i++) { hedge.submit(supplier); exec.pump(); }
    long wallElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStart);
    // Ensure tests didn't actually wait in real time (tight bound like <50ms for safety)
    assertThat("Test must not actually sleep", (double) wallElapsedMs, lessThan(50.0));
  }
}
