package introdb.engine.fch;

import introdb.engine.Record;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static introdb.engine.utils.ByteConverterUtils.toBoolean;
import static introdb.engine.utils.ByteConverterUtils.toByte;

class FChRecord extends Record {

    private final Header header;

    FChRecord(Header header, byte[] key, byte[] value) {
        super(key, value);
        this.header = header;
    }

    static FChRecord of(byte[] key, byte[] value) {
        var header = Header.of(key, value);
        return new FChRecord(header, key, value);
    }

    static FChRecord of(ByteBuffer byteBuffer, int offset) {
        var keySize = byteBuffer.getShort(offset);
        var valueSize = byteBuffer.getShort(offset + 2);
        var deleted = toBoolean(byteBuffer.get(offset+4));

        var key = new byte[keySize];
        byteBuffer.position(offset + Header.SIZE).get(key);

        var value = new byte[valueSize];
        byteBuffer.position(offset + Header.SIZE + keySize).get(value);

        return new FChRecord(Header.of(keySize, valueSize, deleted), key, value);
    }

    static boolean exists(ByteBuffer byteBuffer, int offset) {
        byteBuffer.position(offset);
        return byteBuffer.getLong() > 0;
    }

    @Override
    public int size() {
        return key.length + value.length + header.size();
    }

    Header header() {
        return header;
    }

    boolean equalsKey(byte[] key) {
        return Arrays.equals(this.key, key);
    }

    void append(ByteBuffer byteBuffer) {
        // store headers
        byteBuffer.putShort(header().keySize());
        byteBuffer.putShort(header().valueSize());
        byteBuffer.put(toByte(header().isDeleted()));

        // store body
        byteBuffer.put(key());
        byteBuffer.put(value());
    }

    void delete() {
        header.delete();
    }

    boolean isDeleted() {
        return header.isDeleted();
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
