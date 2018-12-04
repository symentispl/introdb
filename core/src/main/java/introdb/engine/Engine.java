package introdb.engine;

import java.io.IOException;
import java.util.Optional;

public interface Engine {

    void put(byte[] key, byte[] value) throws IOException;

    Optional<? extends Record> remove(byte[] key) throws IOException;

    Optional<? extends Record> get(byte[] key) throws IOException;

}
