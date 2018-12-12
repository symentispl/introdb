package introdb.heap.lock;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
	public void use_same_lock_for_page() throws Exception {
		LockSupport lockSupport = lockManager.lockForPage(0);
		CompletableFuture<String> op = lockSupport.inReadLock(() -> "readLock0");

		assertEquals("readLock0", op.get(1, TimeUnit.SECONDS));
		assertSame(lockSupport, lockManager.lockForPage(0));
	}

	@Test
	public void get_new_lock_for_page() throws Exception {
		LockSupport lock0 = lockManager.lockForPage(0);
		lock0.inReadLock(() -> "readLock0");
		
		String lock0ToString = lock0.toString();
		lock0 = null;
		
		// force weak ref processing
		System.gc();
		
		assertNotEquals(lock0ToString, lockManager.lockForPage(0).toString());
	}
	
}
