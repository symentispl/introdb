package introdb.heap.lock;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import introdb.heap.pool.ObjectFactory;
import introdb.heap.pool.ObjectPool;

public class LockManager {

	
	private ObjectPool<ReentrantReadWriteLock> objectPool;

	public LockManager() {
		objectPool = new ObjectPool<>(ReentrantReadWriteLock::new,l -> l.getWriteHoldCount() == 0 && l.getReadHoldCount() == 0);
	}

	// visible for testing only, so we can inject mocks
	LockManager(ObjectFactory<ReentrantReadWriteLock> lockFactory) {
		objectPool = new ObjectPool<>(lockFactory, l -> l.getWriteHoldCount() == 0 && l.getReadHoldCount() == 0);
	}

	public LockSupport lockForPage(int i) {
		return null;
	}

	public void shutdown() throws Exception{
		
	}

}
