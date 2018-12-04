package introdb.engine.mmf;

import introdb.engine.Config;

import java.nio.ByteBuffer;

import static introdb.engine.utils.ByteConverterUtils.toByte;

class MMFWriter {

    private final FileController fileController;
    private final Config config;

    MMFWriter(FileController fileController, Config config) {
        this.fileController = fileController;
        this.config = config;
    }

    void writePage(Page page) {
        // temporary byte buffer to build a page
        ByteBuffer byteBuilder = ByteBuffer.allocate(config.pageSize());

        // append records to byte "builder"
        for (var record: page.records()) {
            writeRecord(record, byteBuilder);
        }

        updateHeapFile(byteBuilder, page.number() * config.pageSize());
    }

    private void writeRecord(MMFRecord record, ByteBuffer byteBuffer) {
        // store headers
        byteBuffer.putShort(record.header().keySize());
        byteBuffer.putShort(record.header().valueSize());
        byteBuffer.put(toByte(record.header().isDeleted()));

        // store body
        byteBuffer.put(record.key());
        byteBuffer.put(record.value());
    }

    private void updateHeapFile(ByteBuffer byteBuffer, int offset) {
        // move cursor to beginning of the page
        fileController.buffer().position(offset);

        // do update
        fileController.buffer().put(byteBuffer.array());
    }
}
