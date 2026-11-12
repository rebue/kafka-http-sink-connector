package rebue.kafka.httpsink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * 假的 task context。
 *
 * <p>只实现测试真正关心的部分：死信上报、位点回退、暂停/恢复。其余方法留空 —— 它们属于
 * 框架行为，在单测里假装实现反而会给出虚假的安全感。
 */
final class FakeContext implements SinkTaskContext {

    /** 记录被上报到死信的轨迹 */
    static final class RecordingReporter implements ErrantRecordReporter {
        private final List<SinkRecord> records = new ArrayList<>();
        private final List<Throwable> errors = new ArrayList<>();
        private boolean failNext;

        @Override
        public Future<Void> report(final SinkRecord record, final Throwable error) {
            if (failNext) {
                final CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IllegalStateException("死信写入失败"));
                return failed;
            }
            records.add(record);
            errors.add(error);
            return CompletableFuture.completedFuture(null);
        }

        List<SinkRecord> records() {
            return records;
        }

        List<Throwable> errors() {
            return errors;
        }

        void failNextReport() {
            this.failNext = true;
        }
    }

    private final RecordingReporter reporter;
    private final Map<TopicPartition, Long> rewound = new HashMap<>();
    private final Set<TopicPartition> paused = new HashSet<>();

    FakeContext() {
        this(null);
    }

    FakeContext(final RecordingReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public Map<String, String> configs() {
        return Map.of();
    }

    @Override
    public void offset(final TopicPartition partition, final long offset) {
        rewound.put(partition, offset);
    }

    @Override
    public void offset(final Map<TopicPartition, Long> offsets) {
        rewound.putAll(offsets);
    }

    Map<TopicPartition, Long> rewound() {
        return rewound;
    }

    @Override
    public void timeout(final long timeoutMs) {
        // 单测里不需要控制 poll 超时
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Set.of();
    }

    @Override
    public void pause(final TopicPartition... partitions) {
        paused.addAll(List.of(partitions));
    }

    @Override
    public void resume(final TopicPartition... partitions) {
        paused.removeAll(List.of(partitions));
    }

    Set<TopicPartition> paused() {
        return paused;
    }

    @Override
    public void requestCommit() {
        // 无需模拟
    }

    @Override
    public ErrantRecordReporter errantRecordReporter() {
        return reporter;
    }
}
