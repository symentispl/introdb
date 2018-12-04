package introdb.engine.mmf;

import introdb.engine.Config;

class MMFReader {

    private final FileController fileController;
    private final Config config;

    public MMFReader(FileController fileController, Config config) {
        this.fileController = fileController;
        this.config = config;
    }

    Page readPage(int pageNo) {
        var byteBuffer = fileController.buffer();
        var pageSize = config.pageSize();

        // set cursor at the beginning of the page
        byteBuffer.position(pageNo * pageSize);

        // if the size of the page is smaller then the max size use remaining length
        if (byteBuffer.remaining() < pageSize) {
            pageSize = byteBuffer.remaining();
        }

        // read page bytes from file
        byte[] pageBytes = new byte[pageSize];
        byteBuffer.get(pageBytes, 0, pageSize);

        // create Page object from bytes
        return Page.of(pageBytes, pageNo, config.pageSize());
    }
}
