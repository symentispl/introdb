package introdb.engine.mmf;

import introdb.engine.Config;

import java.nio.file.Path;

public class MMFFactory {

    public static MMFEngine createEngine(Path heapFilePath, Config config) {
        var fileController = new FileController(heapFilePath, config);
        fileController.init();

        var writer = new MMFWriter(fileController, config);
        var reader = new MMFReader(fileController, config);

        return new MMFEngine(config, reader, writer);
    }
}
