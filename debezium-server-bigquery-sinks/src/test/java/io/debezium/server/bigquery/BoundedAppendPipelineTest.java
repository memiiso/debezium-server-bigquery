package io.debezium.server.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.rpc.Status;
import io.debezium.DebeziumException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedAppendPipelineTest {
  @Test
  void oneAppendWaitsForCompletion() throws Exception {
    assertBoundAndReorderedCompletion(1);
  }

  @Test
  void configuredBoundsAndReorderedCompletionsAreSafe() throws Exception {
    assertBoundAndReorderedCompletion(2);
    assertBoundAndReorderedCompletion(4);
    assertBoundAndReorderedCompletion(8);
  }

  @Test
  void oneFailedResponseFailsAfterOtherFuturesSucceed() throws Exception {
    List<CompletableFuture<AppendRowsResponse>> completions = new CopyOnWriteArrayList<>();
    CountDownLatch submitted = new CountDownLatch(2);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> result = executor.submit(() -> BoundedAppendPipeline.append(rows(4), 2, payload -> {
        CompletableFuture<AppendRowsResponse> future = new CompletableFuture<>();
        completions.add(future);
        submitted.countDown();
        return future;
      }));
      submitted.await();
      completions.get(0).complete(AppendRowsResponse.newBuilder()
          .setError(Status.newBuilder().setCode(3).setMessage("bad append")).build());
      completions.get(1).complete(AppendRowsResponse.getDefaultInstance());
      ExecutionException error = assertThrows(ExecutionException.class, result::get);
      assertTrue(error.getCause() instanceof DebeziumException);
      assertTrue(error.getCause().getMessage().contains("bad append"));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void interruptionCancelsOutstandingAppendsWithoutWaitingIndefinitely() throws Exception {
    NeverCompletingFuture stuck = new NeverCompletingFuture();
    CompletableFuture<AppendRowsResponse> other = new CompletableFuture<>();
    AtomicInteger submitted = new AtomicInteger();
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    AtomicBoolean interruptPreserved = new AtomicBoolean();
    CountDownLatch finished = new CountDownLatch(1);
    Thread caller = new Thread(() -> {
      try {
        BoundedAppendPipeline.append(rows(2), 2,
            payload -> submitted.getAndIncrement() == 0 ? stuck : other);
      } catch (Throwable e) {
        thrown.set(e);
        interruptPreserved.set(Thread.currentThread().isInterrupted());
      } finally {
        finished.countDown();
      }
    });

    caller.start();
    stuck.getStarted.await();
    caller.interrupt();

    assertTrue(finished.await(5, TimeUnit.SECONDS));
    assertTrue(thrown.get() instanceof DebeziumException);
    assertTrue(interruptPreserved.get());
    assertTrue(stuck.cancelAttempted.get());
    assertTrue(other.isCancelled());
  }

  private static void assertBoundAndReorderedCompletion(int bound) throws Exception {
    int expected = Math.min(bound, 16);
    List<CompletableFuture<AppendRowsResponse>> completions = new CopyOnWriteArrayList<>();
    AtomicInteger outstanding = new AtomicInteger();
    AtomicInteger maximum = new AtomicInteger();
    CountDownLatch submitted = new CountDownLatch(expected);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> result = executor.submit(() -> BoundedAppendPipeline.append(rows(16), bound, payload -> {
        CompletableFuture<AppendRowsResponse> future = new CompletableFuture<>();
        completions.add(future);
        maximum.accumulateAndGet(outstanding.incrementAndGet(), Math::max);
        future.whenComplete((ignored, error) -> outstanding.decrementAndGet());
        submitted.countDown();
        return future;
      }));
      submitted.await();
      assertEquals(expected, completions.size());
      assertEquals(expected, maximum.get());
      for (int i = completions.size() - 1; i >= 0; i--) {
        completions.get(i).complete(AppendRowsResponse.getDefaultInstance());
      }
      result.get();
      assertEquals(0, outstanding.get());
    } finally {
      executor.shutdownNow();
    }
  }

  private static List<JSONObject> rows(int count) {
    List<JSONObject> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      rows.add(new JSONObject().put("id", i));
    }
    return rows;
  }

  private static class NeverCompletingFuture implements Future<AppendRowsResponse> {
    private final CountDownLatch getStarted = new CountDownLatch(1);
    private final AtomicBoolean cancelAttempted = new AtomicBoolean();

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancelAttempted.set(true);
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return false;
    }

    @Override
    public AppendRowsResponse get() throws InterruptedException {
      getStarted.countDown();
      new CountDownLatch(1).await();
      throw new AssertionError("unreachable");
    }

    @Override
    public AppendRowsResponse get(long timeout, TimeUnit unit) throws TimeoutException {
      throw new TimeoutException();
    }
  }
}
