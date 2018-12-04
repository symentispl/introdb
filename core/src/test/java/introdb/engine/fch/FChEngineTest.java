package introdb.engine.fch;

import introdb.engine.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Arrays.fill;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FChEngineTest {

    private FChEngine engine;
    private Path path;

    @BeforeEach
    void setUp() throws IOException {
        path = Files.createTempFile("heap", "0001");
        engine = FChFactory.createFileChannelEngine(path, Config.of(4096, 100));
    }

    @AfterEach
    public void tearDown() throws IOException {
        Files.delete(path);
    }

    @Test
    public void add_one_record() throws Exception {
        // given
        var key = new byte[32];
        fill(key, (byte)5);

        var value = new byte[64];
        fill(value, (byte)8);

        // when
        engine.put(key, value);

        // then
        var record = engine.get(key);
        assertTrue(record.isPresent());

    }

    @Test
    public void add_many_records() throws Exception {
        // given
        var key = new byte[512];
        fill(key, (byte)5);

        var value = new byte[1024];
        fill(value, (byte)8);

        // when
        for (int i = 0; i < 100; i++) {
            engine.put(key, value);
        }
    }

    @Test
    public void get_record() throws IOException {
        // given
        addRecord(1, 1024);
        addRecord(2, 512);
        addRecord(4, 2048);
        addRecord(5, 512);
        addRecord(6, 1024);

        // when
        var key = addRecord(3, 512);

        // then
        var record = engine.get(key);
        assertTrue(record.isPresent());
    }

    @Test
    public void remove_record() throws IOException {
        // given
        addRecord(1, 1024);
        addRecord(2, 512);
        var key = addRecord(3, 512);
        addRecord(4, 2048);
        addRecord(5, 512);
        addRecord(6, 1024);

        var record = engine.get(key);
        assertTrue(record.isPresent());

        // when
        var removedRecord = engine.remove(key);

        // then
        assertTrue(removedRecord.isPresent());
        assertTrue(!engine.get(key).isPresent());
    }


    private byte[] addRecord(int fill, int size) throws IOException {
        var keyB = new byte[100];
        fill(keyB, (byte)fill);
        var valueB = new byte[size];
        fill(valueB, (byte)fill);

        engine.put(keyB, valueB);

        return keyB;
    }
}