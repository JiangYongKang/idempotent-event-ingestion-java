package com.github.highcumontoa.idempotenteventingestionjava.store;

import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于本地文件的事件存储：纯 Java 序列化，无任何外部服务依赖。
 *
 * <p>一致性要点：</p>
 * <ul>
 *   <li>所有方法在同一监视器上串行执行，{@code synchronized} 块内先改内存状态再落盘，
 *       保证“状态判断 + 插入”对并发投递是原子的，是并发幂等去重的核心；</li>
 *   <li>持久化采用“写临时文件 + 原子 move”，避免崩溃产生半截文件；</li>
 *   <li>启动加载后把残留的 PROCESSING 回退为 QUEUED（处理中途崩溃的恢复）。</li>
 * </ul>
 */
public class FileEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(FileEventStore.class);

    /** 可序列化包装，避免直接依赖 HashMap 的序列化格式。 */
    private static final class Snapshot implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private Map<String, EventRecord> records;

        Snapshot(Map<String, EventRecord> records) {
            this.records = records;
        }
    }

    private final Path dataFile;
    private final Clock clock;
    private final Map<String, EventRecord> records = new HashMap<>();

    /**
     * @param dataFile 数据文件路径
     * @param clock    与处理/调度使用的同一时钟：崩溃恢复时回退 PROCESSING 的
     *                 {@code nextAttemptAt} 必须用它计算，否则恢复出的事件可能
     *                 “到点判断”永远落在未来而一直停在原地。
     */
    public FileEventStore(Path dataFile, Clock clock) {
        this.dataFile = dataFile;
        this.clock = clock;
        load();
    }

    private void load() {
        if (!Files.exists(dataFile)) {
            log.info("数据文件不存在，从空状态启动: {}", dataFile.toAbsolutePath());
            return;
        }
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(dataFile))) {
            Snapshot snapshot = (Snapshot) in.readObject();
            if (snapshot.records != null) {
                records.putAll(snapshot.records);
            }
            Instant now = clock.instant();
            long reset = 0;
            for (EventRecord r : records.values()) {
                if (r.getStatus() == com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus.PROCESSING) {
                    r.resetProcessingAfterRestart(now);
                    reset++;
                }
            }
            log.info("从 {} 恢复事件 {} 条，崩溃后回退 PROCESSING -> QUEUED {} 条",
                    dataFile.toAbsolutePath(), records.size(), reset);
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("加载事件数据文件失败: " + dataFile, e);
        }
    }

    @Override
    public synchronized EventRecord find(String idempotencyKey) {
        return records.get(idempotencyKey);
    }

    @Override
    public synchronized EventRecord putIfAbsent(EventRecord record) {
        EventRecord existing = records.putIfAbsent(record.getIdempotencyKey(), record);
        if (existing == null) {
            persist();
            return record;
        }
        return existing;
    }

    @Override
    public synchronized Collection<EventRecord> all() {
        // 返回快照副本，调用方可安全遍历。
        return new TreeMap<>(records).values();
    }

    /** 状态被修改后落盘；调用方必须已持有本对象监视器。 */
    @Override
    public synchronized void persist() {
        Path parent = dataFile.toAbsolutePath().getParent();
        try {
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = dataFile.resolveSibling(dataFile.getFileName() + ".tmp");
            try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(tmp))) {
                out.writeObject(new Snapshot(new HashMap<>(records)));
            }
            try {
                Files.move(tmp, dataFile, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFailure) {
                Files.move(tmp, dataFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("持久化事件数据失败: " + dataFile, e);
        }
    }
}
