package introdb.engine.fch;

import introdb.engine.Config;
import introdb.engine.Engine;
import introdb.engine.Record;

import java.io.IOException;
import java.util.Optional;

/**
 * InnoDB engine implementation based on FileChannel
 *
 *  Engine is using buffers for last page and currently read page.
 *
 * @author snemo
 */
class FChEngine implements Engine {

    private final Config config;
    private final PageReader reader;
    private final PageWriter writer;

    private Page lastPage;          // buffer
    private Page currentPage;       // buffer

    private FChEngine(Config config, PageReader reader, PageWriter writer) {
        this.config = config;
        this.reader = reader;
        this.writer = writer;
        init();
    }

    static FChEngine of(Config config, PageReader reader, PageWriter writer) {
        return new FChEngine(config, reader, writer);
    }

    public void init() {
        // FIXME: load last page from file.
        lastPage = Page.of(0, config.pageSize());
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        remove(key);
        var record = FChRecord.of(key, value);

        if (lastPage.willFit(record)) {
            lastPage.addRecord(record);
        } else {
            writer.write(lastPage);
            lastPage = Page.of(lastPage.number()+1, config.pageSize(), record);
        }
    }

    @Override
    public Optional<? extends Record> remove(byte[] key) throws IOException {
        var page = getPage(key);
        var record = page.flatMap(it -> it.getRecord(key));
        record.ifPresent(FChRecord::delete);

        if (record.isPresent()) {
            writer.write(page.get());
        }

        return record;
    }

    @Override
    public Optional<? extends Record> get(byte[] key) throws IOException {
        return getPage(key)
                .flatMap(page -> page.getRecord(key));
    }

    private Optional<Page> getPage(byte[] key) throws IOException {
        var bufferPage = getPageFromBuffer(key);
        if (bufferPage.isPresent()){
            return bufferPage;
        }

        PageIterator iterator = reader.iterator();
        while (iterator.hasNext()) {
            var page = iterator.next();
            if (page.contains(key)) {
                currentPage = page;
                return Optional.of(page);
            }
        }
        return Optional.empty();
    }

    private Optional<Page> getPageFromBuffer(byte[] key) {
        return Optional.ofNullable(
                checkBuffer(currentPage, key)
                .orElseGet(()-> checkBuffer(lastPage, key).orElse(null)));
    }

    private Optional<Page> checkBuffer(Page page, byte[] key) {
        return Optional.ofNullable(page)
                .filter(p -> p.contains(key));
    }
}
