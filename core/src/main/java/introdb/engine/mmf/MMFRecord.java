package introdb.engine.mmf;

import introdb.engine.Record;

import java.util.Arrays;

import static introdb.engine.utils.ByteConverterUtils.*;


class MMFRecord extends Record {

    private final Header header;

    private MMFRecord(Header header, byte[] key, byte[] value) {
        super(key, value);
        this.header = header;
    }

    static MMFRecord of(Record record) {
        return of(record.key(), record.value());
    }

    static MMFRecord of(byte[] key, byte[] value) {
        return new MMFRecord(Header.of(key, value), key, value);
    }

    static MMFRecord of(byte[] byteArray, int offset) {
        var keySize = toShort(byteArray, offset);
        var valueSize = toShort(byteArray, offset+2);
        var deleted = toBoolean(byteArray[offset+4]);

        var key = Arrays.copyOfRange(byteArray, offset+Header.SIZE, offset+Header.SIZE+keySize);
        var value = Arrays.copyOfRange(byteArray, offset+Header.SIZE+keySize, offset+Header.SIZE+keySize+valueSize);

        return new MMFRecord(Header.of(keySize, valueSize, deleted), key, value);
    }

    static boolean exists(byte[] byteArray, int offset) {
        return toShort(byteArray, offset) > 0;
    }

    Header header() {
        return header;
    }

    boolean equals(byte[] key) {
        return Arrays.equals(this.key, key);
    }

    void markAsDeleted() {
        header.delete();
    }

    boolean isDeleted() {
        return header.isDeleted();
    }

    @Override
    public int size() {
        return  key.length + value.length + header.size();
    }

    static class Header {
        static final int SIZE = 5;

        private short keySize;
        private short valueSize;
        private boolean deleted;

        private Header(short keySize, short valueSize, boolean deleted) {
            this.keySize = keySize;
            this.valueSize = valueSize;
            this.deleted = deleted;
        }

        static Header of(byte[] key, byte[] value) {
            // FIXME: check size of key/value. Throw exception if it is bigger tne 4kb
            return new Header((short) key.length, (short) value.length, false);
        }

        static Header of(short keySize, short valueSize, boolean deleted) {
            return new Header(keySize, valueSize, deleted);
        }

        short keySize() {
            return keySize;
        }

        short valueSize() {
            return valueSize;
        }

        boolean isDeleted() {
            return deleted;
        }

        int size() {
            return SIZE;
        }

        void delete() {
            deleted = true;
        }
    }
}
