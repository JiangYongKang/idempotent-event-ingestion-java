package com.github.highcumontoa.idempotenteventingestionjava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于本地 JSON 快照文件 + 内存索引的事件存储。
 *
 * <p>语义：
 * <ul>
 *   <li>幂等键为主键，{@link #insertIfAbsent} 保证并发首次插入只有一个成功；</li>
 *   <li>所有状态迁移必须经 {@link #compareAndUpdate} 的版本号 CAS，杜绝旧状态覆盖新状态；</li>
 *   <li>每次变更在同一把锁内原子写入快照（临时文件 + ATOMIC_MOVE），
 *       重启后 {@link #loadAll} 得到的是最近一次完整快照，无撕裂页、无外部服务依赖。</li>
 * </ul>
 */
@Component
public class FileEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(FileEventStore.class);
    private static final String STORE_FILE = "events.json";

    private final EventingProperties properties;
    private final ObjectMapper objectMapper;
    private final Map<String, EventRecord> records = new ConcurrentHashMap<>();
    private final ReentrantLock mutationLock = new ReentrantLock();
    private Path storeFile;

    public FileEventStore(EventingProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() throws IOException {
        Path dir = Path.of(properties.getDataDir());
        Files.createDirectories(dir);
        this.storeFile = dir.resolve(STORE_FILE);
        if (Files.exists(storeFile) && Files.size(storeFile) > 0) {
            EventRecord[] loaded;
            try {
                loaded = objectMapper.readValue(storeFile.toFile(), EventRecord[].class);
            } catch (IOException e) {
                throw new IOException("事件快照损坏，拒绝静默启动: " + storeFile, e);
            }
            for (EventRecord r : loaded) {
                records.put(r.getIdempotencyKey(), r);
            }
            log.info("事件存储恢复完成: 从 {} 加载 {} 条事件记录", storeFile, loaded.length);
        } else {
            log.info("事件存储初始化: 新建快照文件目录 {}", dir.toAbsolutePath());
        }
    }

    @Override
    public List<EventRecord> loadAll() {
        mutationLock.lock();
        try {
            List<EventRecord> all = new ArrayList<>(records.size());
            for (EventRecord r : records.values()) {
                all.add(copy(r));
            }
            return all;
        } finally {
            mutationLock.unlock();
        }
    }

    @Override
    public EventRecord find(String idempotencyKey) {
        mutationLock.lock();
        try {
            EventRecord r = records.get(idempotencyKey);
            return r == null ? null : copy(r);
        } finally {
            mutationLock.unlock();
        }
    }

    @Override
    public boolean insertIfAbsent(EventRecord record) {
        mutationLock.lock();
        try {
            if (records.containsKey(record.getIdempotencyKey())) {
                return false;
            }
            record.setVersion(0);
            records.put(record.getIdempotencyKey(), copy(record));
            persist();
            return true;
        } finally {
            mutationLock.unlock();
        }
    }

    @Override
    public boolean compareAndUpdate(EventRecord record, int expectedVersion) {
        mutationLock.lock();
        try {
            EventRecord current = records.get(record.getIdempotencyKey());
            if (current == null || current.getVersion() != expectedVersion) {
                return false;
            }
            record.setVersion(expectedVersion + 1);
            records.put(record.getIdempotencyKey(), copy(record));
            persist();
            return true;
        } finally {
            mutationLock.unlock();
        }
    }

    /** 锁内全量快照原子落盘。规模为本地可接受范围，换来简单且强一致的重启恢复。 */
    private void persist() {
        Path tmp = storeFile.resolveSibling(STORE_FILE + ".tmp");
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(),
                    new ArrayList<>(records.values()));
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("事件快照持久化失败: " + storeFile, e);
        } finally {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // 临时文件清理失败不影响正确性，下次写入会覆盖
            }
        }
    }

    private EventRecord copy(EventRecord s) {
        EventRecord t = new EventRecord();
        t.setIdempotencyKey(s.getIdempotencyKey());
        t.setType(s.getType());
        t.setPayload(s.getPayload());
        t.setStatus(s.getStatus());
        t.setAttemptCount(s.getAttemptCount());
        t.setFailureReason(s.getFailureReason());
        t.setDetail(s.getDetail());
        t.setResult(s.getResult());
        t.setCreatedAt(s.getCreatedAt());
        t.setUpdatedAt(s.getUpdatedAt());
        t.setNextAttemptAt(s.getNextAttemptAt());
        t.setVersion(s.getVersion());
        return t;
    }
}
