package introdb.engine.mmf;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

class Page {

    private final int no;
    private final int maxSize;
    private Set<MMFRecord> records;

    public Page(int no, int maxSize) {
        this.no = no;
        this.maxSize = maxSize;
        this.records = new HashSet<>();
    }

    static Page of(int no, int maxSize) {
        return new Page(no, maxSize);
    }

    static Page of(int no, int maxSize, MMFRecord record) {
        var page = new Page(no, maxSize);
        page.addRecord(record);
        return page;
    }


    // FIXME: validation if record can fit in this page

    static Page of(byte[] bytes, int no, int maxSize) {
        var page = new Page(no, maxSize);
        var pageOffset = 0;
        while (MMFRecord.exists(bytes, pageOffset)) {
            var record = MMFRecord.of(bytes, pageOffset);
            page.addRecord(record);
            pageOffset += record.size();
        }
        return page;
    }



    Set<MMFRecord> records() {
        return this.records;
    }

    void addRecord(MMFRecord record) {
        if (!willFit(record)) {
            throw new IllegalArgumentException("Record is too big for current size of page.");
        }
        this.records.add(record);
    }

    int size() {
        return records.stream()
                .mapToInt(MMFRecord::size)
                .sum();
    }

    boolean willFit(MMFRecord record) {
        return (maxSize - size()) > record.size();
    }

    int number() {
        return no;
    }

    boolean contains(byte[] key) {
        return records.stream()
                .anyMatch(it -> it.equals(key) && !it.isDeleted());
    }

    Optional<MMFRecord> getRecord(byte[] key) {
        return records.stream()
                .filter(it -> it.equals(key))
                .findAny();
    }
}
