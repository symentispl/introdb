package introdb.heap.lock;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LockManagerTest {

	private LockManager lockManager;

	private ReentrantReadWriteLock rwLockSpy;
	private ReadLock readLockSpy;
	private WriteLock writeLockSpy;

	@BeforeEach
	public void setUp() {
		ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
		ReadLock readLock = rwLock.readLock();
		WriteLock writeLock = rwLock.writeLock();
		
		rwLockSpy = Mockito.spy(rwLock);
		readLockSpy = Mockito.spy(readLock);
		writeLockSpy = Mockito.spy(writeLock);
		
		Mockito.when(rwLockSpy.readLock()).thenReturn(readLockSpy);
		Mockito.when(rwLockSpy.writeLock()).thenReturn(writeLockSpy);
		
		lockManager = new LockManager(() -> rwLockSpy);
	}

	@AfterEach
	public void tearDown() throws Exception {
		lockManager.shutdown();
	}

	@Test
	public void use_same_lock_for_page() throws Exception {
		var lockSupport = lockManager.lockForPage(0);

		assertSame(lockSupport, lockManager.lockForPage(0));
	}

	@Test
	public void get_new_lock_for_page_when_lock_gced() throws Exception {
		var lock0 = lockManager.lockForPage(0);
		
		// this is a dirty hack, as we need to
		// make lock0 unreachable,
		var lock0ToString = lock0.toString();
		lock0 = null;
		
		// force weak ref processing
		System.gc();
		
		assertNotEquals(lock0ToString, lockManager.lockForPage(0).toString());
	}
	
	@Test
	public void execute_op_in_readlock() throws Exception {
		var lockSupport = lockManager.lockForPage(0);

		var operation = lockSupport.inReadOperation(() -> "readLock");
		
		assertEquals("readLock", operation.get());
		verify(readLockSpy).lock();
		verify(readLockSpy).unlock();		
	}

	@Test
	public void execute_op_in_writelock() throws Exception {
		var lockSupport = lockManager.lockForPage(0);

		var operation = lockSupport.inWriteOperation(() -> "writeLock");
		
		assertEquals("writeLock", operation.get());
		verify(writeLockSpy).lock();
		verify(writeLockSpy).unlock();		
	}
}
