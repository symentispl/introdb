package introdb.engine.fch;

import introdb.engine.Config;

import java.nio.file.Path;

public class FChFactory {

    public static FChEngine createFileChannelEngine(Path path, Config config) {
        var fileController = new FileController(config, path);
        fileController.init();

        var writer = new PageWriter(fileController);
        var reader = new PageReader(fileController);

        return FChEngine.of(config, reader, writer);
    }

}
