package introdb.engine.memory;

import introdb.engine.Config;
import introdb.engine.Engine;
import introdb.engine.Record;

import java.util.*;

/**
 * All the records are stored in ArrayList
 */
public class RAMEngine implements Engine {

    private final Config config;
    private final List<RAMRecord> records;

    public RAMEngine(Config config) {
        this.config = config;
        this.records = new ArrayList<>();
    }

    @Override
    public void put(byte[] key, byte[] value) {
        put(RAMRecord.of(key, value));
    }

    void put(RAMRecord record) {
        if (record.size() > config.pageSize()) {
            throw new IllegalArgumentException("Record is too big for current size of page.");
        }
        remove(record.key());
        records.add(record);
    }

    @Override
    public Optional<? extends Record> remove(byte[] key) {
        var record = get(key);
        record.ifPresent(records::remove);

        return record;
    }

    @Override
    public Optional<? extends Record> get(byte[] key) {
        return records.stream()
                .filter(record -> Arrays.equals(record.key(), key))
                .findAny();
    }
}
