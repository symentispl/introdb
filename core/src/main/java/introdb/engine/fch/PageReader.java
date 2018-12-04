package introdb.engine.fch;

import introdb.engine.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class PageReader {

    private final FileController fileController;

    PageReader(FileController fileController) {
        this.fileController = fileController;
    }

    public PageIterator iterator() throws IOException {
        return new PageIteratorImpl(fileController.fileChannel(), fileController.config());
    }

    private static class PageIteratorImpl implements PageIterator {

        private final FileChannel fileChannel;
        private final Config config;
        private ByteBuffer byteBuffer;
        private boolean pageProcessed = true;
        private int pageNo = 0;

        PageIteratorImpl(FileChannel fileChannel, Config config) throws IOException {
            this.fileChannel = fileChannel;
            this.config = config;
            fileChannel.position(0);
        }

        @Override
        public boolean hasNext() throws IOException {
            loadNextChunk();
            byteBuffer.rewind();
            return byteBuffer.getShort(0) > 0;
        }

        @Override
        public Page next() throws IOException {
            loadNextChunk();
            byteBuffer.rewind();
            var page = Page.of(pageNo, config.pageSize(), byteBuffer);
            pageNo += 1;
            pageProcessed = true;
            return page;
        }

        private void loadNextChunk() throws IOException {
            if (pageProcessed) {
                byteBuffer = ByteBuffer.allocateDirect(config.pageSize());
                fileChannel.read(byteBuffer, calcPosition());
                pageProcessed = false;
            }
        }

        private long calcPosition() {
            long newPosition = pageNo * config.pageSize();
            return newPosition < 0 ? 1 : newPosition;
        }
    }
}

interface PageIterator {
    boolean hasNext() throws IOException;
    Page next() throws IOException;
}