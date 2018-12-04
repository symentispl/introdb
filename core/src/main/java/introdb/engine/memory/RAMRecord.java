package introdb.engine.memory;

import introdb.engine.Record;

class RAMRecord extends Record {
    private RAMRecord(byte[] key, byte[] value) {
        super(key, value);
    }

    static RAMRecord of(byte[] key, byte[] value) {
        return new RAMRecord(key, value);
    }

    @Override
    public int size() {
        return  key.length + value.length;
    }
}
