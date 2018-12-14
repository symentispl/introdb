package introdb.heap.lock;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface LockSupport {

	<R> CompletableFuture<R> inReadLock(Supplier<R> supplier);
	
	<R> CompletableFuture<R> inWriteLock(Supplier<R> supplier);
	

}
