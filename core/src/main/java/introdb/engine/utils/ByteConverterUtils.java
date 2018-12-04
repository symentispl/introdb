package introdb.engine.utils;

public class ByteConverterUtils {

    public static short toShort(byte[] byteArray, int offset) {
        return (short) (byteArray[offset]<<8 | byteArray[offset+1] & 0xFF);
    }

    public static byte toByte(boolean val) {
        return (byte) (val ? 1 : 0);
    }

    public static boolean toBoolean(byte val) {
        return val == 1;
    }
}
