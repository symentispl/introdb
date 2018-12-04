package introdb.engine.mmf;

import introdb.engine.Config;
import introdb.engine.Engine;

import java.util.Optional;

public class MMFEngine implements Engine {

    private final Config config;
    private final MMFReader reader;
    private final MMFWriter writer;

    private Page lastPage;

    public MMFEngine(Config config, MMFReader reader, MMFWriter writer) {
        this.config = config;
        this.reader = reader;
        this.writer = writer;
        init();
    }

    public void init() {
        lastPage = Page.of(0, config.pageSize());
    }

    @Override
    public void put(byte[] key, byte[] value) {
        put(MMFRecord.of(key, value));
    }

    void put(MMFRecord mmfRecord) {
        remove(mmfRecord.key());

        if (lastPage.willFit(mmfRecord)) {
            lastPage.addRecord(mmfRecord);
        } else {
            lastPage = Page.of(lastPage.number() + 1, config.pageSize(), mmfRecord);
        }

        writer.writePage(lastPage);
    }

    @Override
    public Optional<MMFRecord> get(byte[] key) {
        return findPage(key)
                .flatMap(page -> page.getRecord(key))
                .filter(record -> !record.isDeleted());
    }

    @Override
    public Optional<MMFRecord> remove(byte[] key) {
        var page = findPage(key);
        var record = page.flatMap(it -> it.getRecord(key));
        record.ifPresent(it -> {
            it.markAsDeleted();
            writer.writePage(page.get());
        });

        return record;
    }

    // FIXME: refactor me
    private Optional<Page> findPage(byte[] recordKey) {
        if (lastPage.contains(recordKey)) {
            return Optional.of(lastPage);
        }

        for (int i = 0; i < lastPage.number(); i++) {
            var page = reader.readPage(i);
            if (page.contains(recordKey)) {
                return Optional.of(page);
            }
        }

        return Optional.empty();
    }
}
