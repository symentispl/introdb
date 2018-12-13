package introdb.heap.pool;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectPool<T> {

    private final ObjectFactory<T> fcty;
    private final ObjectValidator<T> validator;
    private final int maxPoolSize;

    private final T[] pool;
    private final AtomicInteger head = new AtomicInteger(0);

    private final ArrayBlockingQueue<T> free;

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

        return CompletableFuture.completedFuture(initializeLazily());
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
        return (int) Arrays.stream(pool, 0, head.get())
          .filter(validator::validate)
          .count();
    }

    private CompletableFuture<T> spinWaitAsync() {
        return CompletableFuture.supplyAsync(() -> {
            T object;

            do {
                object = free.poll();
            } while (object == null);

            return object;
        });
    }

    private T initializeLazily() {
        int claimed;
        int next;
        do {
            claimed = head.get();
            next = claimed + 1;
        } while (!head.compareAndSet(claimed, next));

        T object = fcty.create();
        pool[claimed] = object;
        return object;
    }
}
