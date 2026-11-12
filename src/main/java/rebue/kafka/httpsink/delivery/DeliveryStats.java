package rebue.kafka.httpsink.delivery;

import java.util.Locale;
import java.util.concurrent.atomic.LongAdder;

/**
 * 投递计数。
 *
 * <p>Connect 的自定义指标要拿到 {@code ConnectMetrics}，而 task 侧只能通过
 * {@code SinkTaskContext} 拿到有限能力，代价明显高于收益。这里退一步：进程内计数 + 周期性
 * 汇总日志。它解决的问题是一样的 —— 出事后能回答"到底投出去多少、重试了多少、多少进了死信"，
 * 而不用去翻成百上千条逐条日志。
 *
 * <p>计数是"自上次汇总以来"的增量，避免日志里出现一眼看不出速率的大数。
 */
public final class DeliveryStats {
    private final LongAdder records = new LongAdder();
    private final LongAdder bytes = new LongAdder();
    private final LongAdder attempts = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final LongAdder errant = new LongAdder();
    private final LongAdder skipped = new LongAdder();
    private final LongAdder budgetExhausted = new LongAdder();

    /** 成功投递的记录数 */
    public void delivered(final int count, final long payloadBytes, final int attemptCount) {
        if (count <= 0) {
            return;
        }
        records.add(count);
        bytes.add(payloadBytes);
        attempts.add(attemptCount);
        if (attemptCount > 1) {
            retries.add(attemptCount - 1L);
        }
    }

    /** 交给 errant record reporter（死信）的记录数 */
    public void errant(final int count) {
        if (count > 0) {
            errant.add(count);
        }
    }

    /** 按 {@code behavior.on.error=skip} 主动丢掉的记录数 */
    public void skipped(final int count) {
        if (count > 0) {
            skipped.add(count);
        }
    }

    public void budgetExhausted() {
        budgetExhausted.increment();
    }

    /**
     * 取走自上次调用以来的汇总并清零。
     *
     * @return 一行可读摘要，没有活动时返回 null（避免刷空日志）
     */
    public String drain() {
        final long delivered = records.sumThenReset();
        final long payloadBytes = bytes.sumThenReset();
        final long attemptCount = attempts.sumThenReset();
        final long retryCount = retries.sumThenReset();
        final long errantCount = errant.sumThenReset();
        final long skippedCount = skipped.sumThenReset();
        final long exhausted = budgetExhausted.sumThenReset();
        if (delivered == 0 && errantCount == 0 && skippedCount == 0 && exhausted == 0) {
            return null;
        }
        return String.format(Locale.ROOT,
                "已投递 %d 条 / %s / 尝试 %d 次（重试 %d） / 死信 %d / 跳过 %d / 预算耗尽 %d",
                delivered, humanBytes(payloadBytes), attemptCount, retryCount,
                errantCount, skippedCount, exhausted);
    }

    private static String humanBytes(final long count) {
        if (count < 1024) {
            return count + "B";
        }
        if (count < 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1fKB", count / 1024.0);
        }
        return String.format(Locale.ROOT, "%.1fMB", count / (1024.0 * 1024.0));
    }
}
