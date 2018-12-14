package introdb.heap.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class ObjectPool<T> {

    private ObjectFactory<T> fcty;
    private final ObjectValidator<T> validator;
    private final int maxPoolSize;

    private final T[] pool;
    private final ArrayBlockingQueue<T> freePool;
    private final AtomicInteger nextIndex = new AtomicInteger(0);

    public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
        this(fcty, validator, 25);
    }

    public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
        this.fcty = fcty;
        this.validator = validator;
        this.maxPoolSize = maxPoolSize;
        this.pool = (T[]) new Object[maxPoolSize];
        this.freePool = new ArrayBlockingQueue<>(maxPoolSize);
    }

    public CompletableFuture<T> borrowObject() {
        T obj;
        if (null != (obj = freePool.poll()) && validator.validate(obj)) { // if available, return
            return completedFuture(obj);
        }

        if (nextIndex.get() == maxPoolSize) {
            return spinWaitAsync();
        }

        int claimed;
        int next;
        do {
            claimed = nextIndex.get();
            next = claimed + 1;
            if (next > maxPoolSize) {
                return spinWaitAsync();
            }
        } while (!nextIndex.compareAndSet(claimed, next));

        T object = fcty.create();
        pool[claimed] = object;
        if (next == maxPoolSize) {
            fcty = null;
        }

        return completedFuture(object);
    }

    public void returnObject(T object) {
        freePool.offer(object);
    }

    public void shutdown() throws InterruptedException {
    }

    public int getPoolSize() {
        return nextIndex.get();
    }

    public int getInUse() {
        int count = 0;
        int bound = nextIndex.get();
        for (int i = 0; i < bound; i++) {
            T t = pool[i];
            if (validator.validate(t)) {
                count++;
            }
        }
        return count;
    }

    private CompletableFuture<T> spinWaitAsync() {
        return CompletableFuture.supplyAsync(() -> {
            T obj;

            while (null == (obj = freePool.poll()) || !validator.validate(obj)) {
                Thread.onSpinWait();
            }

            return obj;
        });
    }
}
