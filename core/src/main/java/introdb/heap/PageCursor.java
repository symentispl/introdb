package introdb.heap;

import static java.lang.String.format;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import introdb.heap.Record.Mark;

class PageCursor implements Iterator<Record> {

  private static final int UNSET = -1;

  private ByteBuffer page;
  private boolean hasNext = false;
  private int mark = UNSET;

  PageCursor(ByteBuffer page) {
    this.page = page;
  }

  @Override
  public boolean hasNext() {

    if (hasNext) {
      return true;
    }

    while (page.hasRemaining()) {
      Mark m = Mark.valueOf(page.get());
      switch (m) {
      case PRESENT:
        return hasNext = true;
      case REMOVED:
        Record.skip(() -> page);
        break;
      case EMPTY:
        // when Mark.EMPTY we should,
        // stop iterating at all
        hasNext = false;
        page.position(page.capacity());
        break;
      default:
        throw new IllegalStateException(format("not all records marks were handled by page cursor,  (%s) ", m));
      }
    }
    return hasNext = false;
  }

  @Override
  public Record next() {
    if (hasNext || hasNext()) {
      hasNext = false;
      mark = page.position()-1;
      return Record.read(page);
    }
    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    if (unset()) {
      page.put(mark, Mark.REMOVED.mark());
      mark = UNSET;
    } else {
      throw new IllegalStateException("next() was not called or remove() was called already");
    }
  }

  private boolean unset() {
    return mark >= UNSET;
  }

  ByteBuffer page() {
    return page;
  }

  void reset(ByteBuffer page) {
    this.page = page;
    this.hasNext = false;
    this.mark = UNSET;
  }

}
