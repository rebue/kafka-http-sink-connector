package rebue.kafka.httpsink.delivery;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.connect.errors.RetriableException;

/**
 * 单条（或单批）记录的重试循环。
 *
 * <p>设计上的两个约束都来自线上真实事故：
 *
 * <ol>
 *   <li><b>重试必须有硬预算</b>。Connect 的 {@code put()} 是同步的，里面睡多久，消费者就停多久；
 *       一旦超过 {@code consumer.max.poll.interval.ms}，协调者会把整个 task 踢出消费组，
 *       触发 rebalance，然后同一条记录被另一个 task 重新投递 —— 一次下游变慢就放大成重复投递。</li>
 *   <li><b>退避必须带抖动</b>。多个 task 被同一个下游同时拒绝时，不加抖动会一起醒来、一起再打过去。</li>
 * </ol>
 *
 * <p>预算用尽时返回一个"可重试"的结论，由上层抛 {@link RetriableException}：宁可让框架按
 * {@code errors.retry.*} 再来一轮，也不能在这里无限等下去。
 */
public final class RetryPolicy {

    /** 没有截止时间（{@code http.delivery.budget.ms=0}） */
    public static final long NO_DEADLINE = Long.MAX_VALUE;

    /** 未配置退避上限时 {@code Retry-After} 的硬上限：防止一个恶意的 86400 把 task 睡死 */
    private static final long RETRY_AFTER_HARD_CAP_MS = 60_000;

    /** 一次尝试；超时由策略按剩余预算裁剪后传入 */
    @FunctionalInterface
    public interface Attempt {
        DeliveryResult send(int timeoutMs);
    }

    private final int maxRetries;
    private final int requestTimeoutMs;
    private final int backoffMs;
    private final int backoffMaxMs;
    private final int jitterMs;

    public RetryPolicy(final int maxRetries, final int requestTimeoutMs,
                       final int backoffMs, final int backoffMaxMs, final int jitterMs) {
        this.maxRetries = Math.max(0, maxRetries);
        this.requestTimeoutMs = Math.max(1, requestTimeoutMs);
        this.backoffMs = Math.max(0, backoffMs);
        this.backoffMaxMs = Math.max(this.backoffMs, backoffMaxMs);
        this.jitterMs = Math.max(0, jitterMs);
    }

    /**
     * 反复尝试直到成功、撞上不可重试、重试用尽或预算耗尽。
     *
     * @param deadlineNanos {@link System#nanoTime()} 口径的截止点，或 {@link #NO_DEADLINE}
     */
    public DeliveryResult execute(final Attempt attempt, final long deadlineNanos) {
        final long startedAt = System.nanoTime();
        int attemptIndex = 0;
        DeliveryResult last = null;

        while (true) {
            final long remaining = remainingMs(deadlineNanos);
            if (remaining <= 0) {
                return last == null
                        ? DeliveryResult.transportFailure("投递预算已耗尽，本轮未发出请求", 0, elapsedMs(startedAt))
                        : last.appended("重试预算已耗尽").restated(attemptIndex, elapsedMs(startedAt));
            }
            last = attempt.send((int) Math.min(requestTimeoutMs, remaining));
            if (last.delivered() || !last.retriable()) {
                return last.restated(attemptIndex + 1, elapsedMs(startedAt));
            }
            if (attemptIndex >= maxRetries) {
                return last.appended("重试用尽(" + maxRetries + " 次)").restated(attemptIndex + 1, elapsedMs(startedAt));
            }
            final long sleepMs = nextBackoffMs(attemptIndex, last);
            if (sleepMs >= remaining) {
                return last.appended("剩余预算(" + remaining + "ms)不足以再退避 " + sleepMs + "ms")
                        .restated(attemptIndex + 1, elapsedMs(startedAt));
            }
            if (!sleep(sleepMs)) {
                return last.restated(attemptIndex + 1, elapsedMs(startedAt));
            }
            attemptIndex++;
        }
    }

    /**
     * 指数退避 + 抖动，并尊重下游的 {@code Retry-After}。
     *
     * <p>{@code Retry-After} 是下游明确表达的"什么时候再来"，比我们的经验值更可信；但它同样
     * 受 {@code http.retry.backoff.max.ms} 约束，避免一个恶意的 {@code Retry-After: 86400}
     * 让 task 睡一整天。
     */
    private long nextBackoffMs(final int attemptIndex, final DeliveryResult last) {
        if (last.retryAfterMs() > 0) {
            // 未配置退避上限（0）不等于"不许等"：此时给 Retry-After 一个硬上限即可，
            // 否则下游明确要求稍后再来，我们却立刻重试，等于无视对方的限流
            final long cap = backoffMaxMs > 0 ? backoffMaxMs : RETRY_AFTER_HARD_CAP_MS;
            return Math.min(last.retryAfterMs(), cap);
        }
        final long exponential = (long) backoffMs << Math.min(attemptIndex, 10);
        final long capped = Math.min(exponential, backoffMaxMs);
        if (jitterMs == 0) {
            return capped;
        }
        return capped + ThreadLocalRandom.current().nextLong(jitterMs + 1L);
    }

    /**
     * @return false 表示被中断（停机中），调用方应立刻结束而不是继续重试
     */
    private static boolean sleep(final long millis) {
        if (millis <= 0) {
            return true;
        }
        try {
            Thread.sleep(millis);
            return true;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetriableException("等待重试时被中断（任务正在停止）", e);
        }
    }

    private static long remainingMs(final long deadlineNanos) {
        if (deadlineNanos == NO_DEADLINE) {
            return Long.MAX_VALUE;
        }
        return (deadlineNanos - System.nanoTime()) / 1_000_000L;
    }

    private static long elapsedMs(final long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000L;
    }
}
