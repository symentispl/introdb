package introdb.heap;

import introdb.heap.Record.Mark;
import introdb.heap.pool.ObjectPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

class UnorderedHeapFile implements Store, Iterable<Record> {

    private static final ThreadLocal<ByteBuffer> T_LOCAL_BUFFER = ThreadLocal
      .withInitial(() -> ByteBuffer.allocate(4 * 1024));

    private final PageCache pagecache;
    private final int maxNrPages;
    private final int pageSize;

    private final byte[] zeroPage;
    private volatile ByteBuffer lastPage;
    private final AtomicInteger lastPageNumber = new AtomicInteger(-1);

    private final FileChannel file;

    private final ObjectPool<ReentrantReadWriteLock> locks = new ObjectPool<>(ReentrantReadWriteLock::new, l -> !l.hasQueuedThreads());

    UnorderedHeapFile(Path path, int maxNrPages, int pageSize) throws IOException {
        this.file = FileChannel.open(path, Set.of(CREATE, READ, WRITE));
        this.pagecache = new PageCache(maxNrPages, pageSize);
        this.maxNrPages = maxNrPages;
        this.pageSize = pageSize;
        this.zeroPage = new byte[pageSize];
    }

    @Override
    public synchronized void put(Entry entry) throws IOException {
            assertTooManyPages();

            var newRecord = Record.of(entry);

            assertRecordSize(newRecord);

            var iterator = cursor();
            while (iterator.hasNext()) {
                var record = iterator.next();
                if (Arrays.equals(newRecord.key(), record.key())) {
                    iterator.remove();
                    break;
                }
            }
            if (lastPage == null || lastPage.remaining() < newRecord.size()) {
                lastPageNumber.getAndIncrement();
                lastPage = ByteBuffer.allocate(pageSize);
            }

            var src = newRecord.write(() -> lastPage);
            writePage(src, lastPageNumber.get());
    }

    @Override
    public synchronized Object get(Serializable key) throws IOException, ClassNotFoundException { // TODO bo trzeba było dowiezc
        var keySer = serializeKey(key);

            var iterator = cursor();
            while (iterator.hasNext()) {
                var record = iterator.next();
                if (Arrays.equals(keySer, record.key())) {
                    return deserializeValue(record.value());
                }
            }
            return null;
    }

    public synchronized Object remove(Serializable key) throws IOException, ClassNotFoundException {
        var keySer = serializeKey(key);

            var iterator = cursor();
            while (iterator.hasNext()) {
                var record = iterator.next();
                if (Arrays.equals(keySer, record.key())) {
                    var value = deserializeValue(record.value());
                    iterator.remove();
                    return value;
                }
            }
            return null;
    }

    private void writePage(ByteBuffer page, int pageNr) throws IOException {
        int position = page.position();
        page.rewind();
        file.write(page, pageNr * pageSize);
        page.position(position);
        pagecache.remove(pageNr);
    }

    private int readPage(ByteBuffer page, int pageNr) {

        clearPage(page);
        if (pagecache.get(page, pageNr) != -1) {
            return pageSize;
        }
        try {
            var bytesRead = file.read(page, pageNr * pageSize);
            if (bytesRead != -1) {
                pagecache.put(page, pageNr);
            }
            return bytesRead;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private void assertRecordSize(Record newRecord) {
        if (newRecord.size() > pageSize) {
            throw new IllegalArgumentException("entry too big");
        }
    }

    private void assertTooManyPages() {
        if (lastPageNumber.get() >= maxNrPages) {
            throw new RuntimeException("Too many pages!");
        }
    }

    private static Object deserializeValue(byte[] value) throws IOException, ClassNotFoundException {
        var inputStream = new ByteArrayInputStream(value);

        try (var objectInput = new ObjectInputStream(inputStream)) {
            return objectInput.readObject();
        }
    }

    private static byte[] serializeKey(Serializable key) throws IOException {
        var byteArray = new ByteArrayOutputStream();
        try (var objectOutput = new ObjectOutputStream(byteArray)) {
            objectOutput.writeObject(key);
        }
        return byteArray.toByteArray();
    }

    private void clearPage(ByteBuffer page) {
        page.clear();
        page.put(zeroPage);
        page.rewind();
    }

    private ByteBuffer zeroPage() {
        ByteBuffer page = T_LOCAL_BUFFER.get();
        clearPage(page);
        return page;
    }

    class Cursor implements Iterator<Record> {

        int pageNr = 0;
        ByteBuffer page = null;
        boolean hasNext = false;
        private int inPagePosition = -1;

        @Override
        public boolean hasNext() {
            if (!hasNext) {
                inPagePosition = -1;
                do {
                    if (page == null) {
                        page = zeroPage();
                        var bytesRead = readPage(page, pageNr);
                        if (bytesRead == -1) {
                            return hasNext = false;
                        }
                        page.rewind();
                    }

                    byte mark;
                    do {
                        mark = page.get();
                        if (Mark.isPresent(mark)) {
                            return hasNext = true;
                        }
                        if (Mark.isRemoved(mark)) {
                            skip();
                        }
                    } while (!Mark.isEmpty(mark) || !page.hasRemaining());

                    page = null;
                    pageNr++;
                } while (true);
            }
            return true;
        }

        @Override
        public Record next() {
                if (hasNext || hasNext()) {
                    hasNext = false;
                    inPagePosition = page.position() - 1;
                    return record();
                }
                throw new NoSuchElementException();
        }

        @Override
        public void remove() {
                if (inPagePosition < 0) {
                    throw new IllegalStateException("next() method has not yet been called, or the remove() method has already been called");
                }
                page.put(inPagePosition, Mark.REMOVED.mark());
                try {
                    writePage(page, pageNr);
                    lastPage = null; //force page refresh
                    inPagePosition = -1;
                } catch (IOException e) {
                    throw new IOError(e);
                }
        }

        private Record record() {
            return Record.read(() -> page);
        }

        private void skip() {
            Record.skip(() -> page);
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return cursor();
    }

    Cursor cursor() {
        return new Cursor();
    }
}