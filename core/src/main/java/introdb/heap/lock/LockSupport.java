package introdb.heap.lock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.function.Supplier;

public interface LockSupport {

	ReadLock readLock() throws Exception;

	<R> CompletableFuture<R> inReadLock(Supplier<R> supplier);

}
