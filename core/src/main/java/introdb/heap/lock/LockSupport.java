package introdb.heap.lock;

import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

public interface LockSupport {

	ReadLock readLock();

}
