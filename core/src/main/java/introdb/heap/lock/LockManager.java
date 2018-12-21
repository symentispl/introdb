package introdb.heap.lock;

import static java.lang.String.format;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import introdb.heap.pool.ObjectPool;

public class LockManager {

	private static final Logger LOGGER = Logger.getLogger(LockManager.class.getName());

	/**
	 * This is a neat trick, as {@link WeakReference} docs suggests we need
	 * canonical mapping of ReentrantLock.
	 * 
	 */
	class DefaultLockSupport implements LockSupport {

		private final CompletableFuture<ReentrantReadWriteLock> futureLock;

		DefaultLockSupport(CompletableFuture<ReentrantReadWriteLock> futureLock) {
			this.futureLock = futureLock;
		}

		@Override
		public <R> CompletableFuture<R> inReadLock(Supplier<R> supplier) {
			return futureLock.thenApply((rwLock) -> {
				rwLock.readLock().lock();
				try {
					return supplier.get();
				} finally {
					try {
						rwLock.readLock().unlock();
					} finally {
						objectPool.returnObject(rwLock);
					}
				}
			});
		}

		@Override
		public <R> CompletableFuture<R> inWriteLock(Supplier<R> supplier) {
			return futureLock.thenApply((rwLock) -> {
				rwLock.writeLock().lock();
				try {
					return supplier.get();
				} finally {
					try {
						rwLock.writeLock().unlock();
					} finally {
						objectPool.returnObject(rwLock);
					}
				}
			});
		}
	}

	public void shutdown() throws Exception{
		
	}

}
