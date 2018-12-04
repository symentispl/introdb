package introdb.engine.fch;

import java.io.IOException;

class PageWriter {

    private final FileController fileController;

    PageWriter(FileController fileController) {
        this.fileController = fileController;
    }

    void write(Page page) throws IOException {
        final var fileChannel = fileController.fileChannel();
        fileChannel.position(page.number() * page.maxSize());
        fileChannel.write(page.toByteBuffer());
    }
}
