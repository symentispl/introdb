package introdb.heap.lock;

import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class LockManagerTest {
	
	private LockManager lockManager;

	@BeforeEach
	public void setUp() {
		lockManager = new LockManager();
	}
	
	@AfterEach
	public void tearDown() throws Exception {
		lockManager.shutdown();
	}
	
	@Test
	public void a() throws Exception {
		
		LockSupport lock0 = lockManager.lockForPage(0);
		ReadLock readLock0 = lock0.readLock();
		
		LockSupport lock1 = lockManager.lockForPage(1);
		ReadLock readLock1 = lock1.readLock();
		
		assertNotEquals(readLock0, readLock1);
	}

	@Test
	public void b() throws Exception {
		
		LockSupport lock0 = lockManager.lockForPage(0);
		ReadLock readLock0 = lock0.readLock();
		
		LockSupport lock1 = lockManager.lockForPage(0);
		ReadLock readLock1 = lock1.readLock();
		
		assertEquals(readLock0, readLock1);
	}

	@Test
	public void c() throws Exception {
		
		LockSupport lock0 = lockManager.lockForPage(0);
		String lock0ToString = lock0.toString();
		
		lock0 = null; // lock0 is now unreachable
		
		System.gc(); // force WeakRef processing
		
		LockSupport lock1 = lockManager.lockForPage(0);
		
		assertNotEquals(lock0ToString, lock1.toString());
	}
}
