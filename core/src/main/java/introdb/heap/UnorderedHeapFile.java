package introdb.heap;

import introdb.engine.Config;
import introdb.engine.Engine;
import introdb.engine.EngineFactory;
import introdb.engine.Record;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;

import static introdb.engine.utils.SerializationUtils.deserialize;
import static introdb.engine.utils.SerializationUtils.serialize;
import static java.util.Objects.isNull;

/**
 * Implementation of the DB engine is in introdb.engine.fch package (FileChannel).
 * Rest of the implementations (Memory Mapped Files or memory) are just playground.
 */
class UnorderedHeapFile implements Store {

    private final Engine dbEngine;

	UnorderedHeapFile(Path path, int maxNrPages, int pageSize) {
//		dbEngine = EngineFactory.createRAMEngine(Config.of(pageSize, maxNrPages));
//		dbEngine = EngineFactory.createMMFEngine(path, Config.of(pageSize, maxNrPages));
		dbEngine = EngineFactory.createFileChannelEngine(path, Config.of(pageSize, maxNrPages));
    }

	@Override
	public void put(Entry entry) throws IOException, ClassNotFoundException {
		dbEngine.put(serialize(entry.key()), serialize(entry.value()));
	}
	
	@Override
	public Object get(Serializable key) throws IOException, ClassNotFoundException {
		var valueBytes = dbEngine.get(serialize(key))
                .map(Record::value)
                .orElse(null);

        return isNull(valueBytes) ? null : deserialize(valueBytes);
	}
	
	public Object remove(Serializable key) throws IOException, ClassNotFoundException {
		var valueBytes = dbEngine.remove(serialize(key))
                .map(Record::value)
                .orElse(null);

		return isNull(valueBytes) ? null : deserialize(valueBytes);
	}
}

