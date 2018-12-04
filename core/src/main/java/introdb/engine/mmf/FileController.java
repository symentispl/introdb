package introdb.engine.mmf;

import introdb.engine.Config;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;

class FileController {

    private final Config config;
    private final Path path;
    private MappedByteBuffer buffer;

    public FileController(Path path, Config config) {
        this.path = path;
        this.config = config;
    }

    void init() {
        try {
            var fileChannel = FileChannel.open(path, CREATE, READ, WRITE);
            this.buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0,
                    config.MaxNrPages() * config.pageSize());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    MappedByteBuffer buffer() {
        return this.buffer;
    }

    void close() {} // FIXME: not sure if we need to close this....
}
