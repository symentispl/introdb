package introdb.heap.pool;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectPool<T> {

    private ObjectFactory<T> fcty;
    private final ObjectValidator<T> validator;
    private final int maxPoolSize;

    private final T[] pool;
    private final ArrayBlockingQueue<T> free;
    private final AtomicInteger head = new AtomicInteger(0);

    public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
        this(fcty, validator, 25);
    }

    public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
        this.fcty = fcty;
        this.validator = validator;
        this.maxPoolSize = maxPoolSize;
        this.pool = (T[]) new Object[maxPoolSize];
        this.free = new ArrayBlockingQueue<>(maxPoolSize);
    }

    public CompletableFuture<T> borrowObject() {
        T obj;
        if (null != (obj = free.poll())) {
            return CompletableFuture.completedFuture(obj);
        }

        if (head.get() == maxPoolSize) { // if fully initialized, wait for a free one
            return spinWaitAsync();
        }

        return CompletableFuture.completedFuture(initializeLockFreeLazily());
    }

    public void returnObject(T object) {
        free.add(object);
    }

    public void shutdown() throws InterruptedException {
    }

    public int getPoolSize() {
        return head.get();
    }

    public int getInUse() {
        int count = 0;
        int bound = head.get();
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
            T object;

            while (null == (object = free.poll())) {
                Thread.onSpinWait();
            }

            return object;
        });
    }

    private T initializeLockFreeLazily() {
        int claimed;
        int next;
        do {
            claimed = head.get();
            next = claimed + 1;
        } while (!head.compareAndSet(claimed, next));

        T object = fcty.create();
        pool[claimed] = object;
        if (next == maxPoolSize) {
            fcty = null;
        }
        return object;
    }
}
