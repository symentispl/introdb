package introdb.engine;

public abstract class Record {
    protected final byte[] key;
    protected final byte[] value;

    protected Record(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    abstract public int size();
}
