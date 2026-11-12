package rebue.kafka.httpsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import rebue.kafka.httpsink.delivery.DeliveryResult;
import rebue.kafka.httpsink.delivery.RetryPolicy;

class RetryPolicyTest {

    private static DeliveryResult status(final int code) {
        return DeliveryResult.fromStatus(code, "", 0, 1, 0);
    }

    private static long budget(final long millis) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    }

    @Test
    void returnsOnFirstSuccess() {
        final RetryPolicy policy = new RetryPolicy(5, 1_000, 0, 0, 0);
        final DeliveryResult result = policy.execute(timeout -> status(200), budget(5_000));
        assertTrue(result.delivered());
        assertEquals(1, result.attempts());
    }

    @Test
    void retriesUntilSuccess() {
        final AtomicInteger calls = new AtomicInteger();
        final RetryPolicy policy = new RetryPolicy(5, 1_000, 0, 0, 0);
        final DeliveryResult result = policy.execute(
                timeout -> status(calls.incrementAndGet() < 3 ? 503 : 200), budget(5_000));
        assertTrue(result.delivered());
        assertEquals(3, result.attempts());
    }

    /** 4xx 重试没有意义：只会把同一条坏记录重复打给下游 */
    @Test
    void doesNotRetryPoison() {
        final AtomicInteger calls = new AtomicInteger();
        final RetryPolicy policy = new RetryPolicy(5, 1_000, 0, 0, 0);
        final DeliveryResult result = policy.execute(
                timeout -> status(calls.incrementAndGet() == 1 ? 400 : 200), budget(5_000));
        assertEquals(DeliveryResult.Kind.POISON, result.kind());
        assertEquals(1, calls.get());
    }

    @Test
    void exhaustsRetriesAndStaysRetriable() {
        final AtomicInteger calls = new AtomicInteger();
        final RetryPolicy policy = new RetryPolicy(3, 1_000, 0, 0, 0);
        final DeliveryResult result = policy.execute(timeout -> {
            calls.incrementAndGet();
            return status(503);
        }, budget(5_000));
        assertEquals(DeliveryResult.Kind.RETRYABLE, result.kind());
        assertEquals(4, calls.get());
        assertEquals(4, result.attempts());
        assertTrue(result.detail().contains("重试用尽"));
    }

    /** 预算已经用完时，一次请求都不该发出去 */
    @Test
    void skipsEverythingWhenBudgetAlreadyGone() {
        final AtomicInteger calls = new AtomicInteger();
        final RetryPolicy policy = new RetryPolicy(3, 1_000, 0, 0, 0);
        final DeliveryResult result = policy.execute(
                timeout -> {
                    calls.incrementAndGet();
                    return status(200);
                }, budget(-1));
        assertEquals(0, calls.get());
        assertEquals(DeliveryResult.Kind.RETRYABLE, result.kind());
    }

    /**
     * 退避会睡穿预算时立刻返回。
     *
     * <p>这条约束比"多试一次"重要得多：put() 里睡多久，消费者就停多久，超过
     * consumer.max.poll.interval.ms 就会触发 rebalance，把一次下游变慢放大成重复投递。
     */
    @Test
    void doesNotSleepPastTheDeadline() {
        final RetryPolicy policy = new RetryPolicy(5, 1_000, 5_000, 5_000, 0);
        final long startedAt = System.nanoTime();
        final DeliveryResult result = policy.execute(timeout -> status(503), budget(100));
        final long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;
        assertEquals(DeliveryResult.Kind.RETRYABLE, result.kind());
        assertTrue(result.detail().contains("预算"), result.detail());
        assertTrue(elapsedMs < 1_000, "不应真的睡满退避时间，实际 " + elapsedMs + "ms");
    }

    /** 单次请求的超时必须被剩余预算裁剪，否则一次请求就能吃穿整个预算 */
    @Test
    void clampsAttemptTimeoutToRemainingBudget() {
        final int[] observed = new int[1];
        final RetryPolicy policy = new RetryPolicy(0, 30_000, 0, 0, 0);
        policy.execute(timeout -> {
            observed[0] = timeout;
            return status(200);
        }, budget(120));
        assertTrue(observed[0] <= 120, "超时未裁剪: " + observed[0]);
        assertTrue(observed[0] >= 100, "裁剪过头: " + observed[0]);
    }

    @Test
    void honoursRetryAfterHeader() {
        final AtomicInteger calls = new AtomicInteger();
        final RetryPolicy policy = new RetryPolicy(1, 1_000, 0, 0, 0);
        final long startedAt = System.nanoTime();
        final DeliveryResult result = policy.execute(timeout -> {
            if (calls.incrementAndGet() == 1) {
                // 下游明确说 60ms 后再来
                return DeliveryResult.fromStatus(429, "", 60, 1, 0);
            }
            return status(200);
        }, budget(2_000));
        assertTrue(result.delivered());
        assertTrue((System.nanoTime() - startedAt) / 1_000_000 >= 55, "没有遵守 Retry-After");
    }
}
