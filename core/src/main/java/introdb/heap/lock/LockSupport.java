package introdb.heap.lock;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface LockSupport {

	<R> CompletableFuture<R> inReadOperation(Supplier<R> supplier);
	
	<R> CompletableFuture<R> inWriteOperation(Supplier<R> supplier);
	
	String toString();
	
}
