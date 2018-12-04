package introdb.engine;

import introdb.engine.fch.FChFactory;
import introdb.engine.mmf.MMFFactory;
import introdb.engine.memory.RAMEngine;

import java.nio.file.Path;

public class EngineFactory {

    public static Engine createMMFEngine(Path heapFilePath, Config config) {
        return MMFFactory.createEngine(heapFilePath, config);
    }

    public static Engine createRAMEngine(Config config) {
        return new RAMEngine(config);
    }

    public static Engine createFileChannelEngine(Path heapFilePath, Config config) {
        return FChFactory.createFileChannelEngine(heapFilePath, config);
    }
}
