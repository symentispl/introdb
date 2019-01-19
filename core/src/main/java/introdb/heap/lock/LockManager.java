package introdb.heap.lock;

import static java.lang.String.format;

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

import introdb.heap.pool.ObjectFactory;
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
		public <R> CompletableFuture<R> inReadOperation(Supplier<R> supplier) {
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
		public <R> CompletableFuture<R> inWriteOperation(Supplier<R> supplier) {
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

	/**
	 * Holds reentrant lock so we can return it to the pool when
	 * {@link DefaultLockSupport} is garbage collected.
	 *
	 */
	class LockRef extends WeakReference<DefaultLockSupport> {

		private final CompletableFuture<ReentrantReadWriteLock> futureLock;
		private final Integer pageNr;

		LockRef(Integer pageNr, DefaultLockSupport referent, CompletableFuture<ReentrantReadWriteLock> futureLock,
		        ReferenceQueue<DefaultLockSupport> q) {
			super(referent, q);
			this.pageNr = pageNr;
			this.futureLock = futureLock;
		}

		@Override
		public void clear() {
			super.clear();
			boolean removed = locks.remove(pageNr, this);
			LOGGER.finest(() -> format("lock for page %d was %s removed", pageNr, removed ? "" : "not"));
			try {
				if (futureLock.isDone() && (!futureLock.isCancelled() && !futureLock.isCompletedExceptionally()))
					objectPool.returnObject(futureLock.get());
			} catch (InterruptedException | ExecutionException e) {
				LOGGER.log(Level.SEVERE, format("failed to clear lock ref %s", this), e);
			}
		}
	}

	private final ConcurrentHashMap<Integer, LockRef> locks = new ConcurrentHashMap<>();
	private final ReferenceQueue<DefaultLockSupport> referenceQ = new ReferenceQueue<>();
	private final ExecutorService invalidator = Executors.newSingleThreadExecutor();
	private final ObjectPool<ReentrantReadWriteLock> objectPool;
	private volatile boolean running = true;

	public LockManager() {
		objectPool = new ObjectPool<>(ReentrantReadWriteLock::new,
		        l -> l.getWriteHoldCount() == 0 && l.getReadHoldCount() == 0);
		startReferenceQueue();
	}

	// visible for testing only, so we can inject mocks
	LockManager(ObjectFactory<ReentrantReadWriteLock> lockFactory) {
		objectPool = new ObjectPool<>(lockFactory, l -> l.getWriteHoldCount() == 0 && l.getReadHoldCount() == 0);
		startReferenceQueue();
	}

	public LockSupport lockForPage(Integer pageNr) {
		var futureLock = locks.compute(pageNr, (_pageNr, _lockRef) -> {
		    // handling situation when there is no mapping, 
			  // or mapping points to unreachable lock
		    // instance, which was not yet processed by invalidator
		    if (_lockRef == null || _lockRef.get() == null) {
				CompletableFuture<ReentrantReadWriteLock> future = objectPool.borrowObject();
				return new LockRef(_pageNr, new DefaultLockSupport(future), future, referenceQ);
			} else {
				return _lockRef;
			}
		});
		return futureLock.get();
	}

	public void shutdown() throws Exception {
		running = false;
		invalidator.shutdown();
		invalidator.awaitTermination(2000, TimeUnit.MILLISECONDS);
	}

	private void startReferenceQueue() {
		invalidator.execute(() -> {
      LOGGER.info("lock manager started");
			while (running) {
				try {
					var reference = referenceQ.remove(1000);
					if (reference != null) {
						LOGGER.finest(() -> format("processing reference %s", reference));
						reference.clear();
					}
				} catch (InterruptedException e) {
					LOGGER.log(Level.SEVERE, "in reference queue processing", e);
				}
			}
			LOGGER.info("lock manager shutdown");
		});
	}

}
