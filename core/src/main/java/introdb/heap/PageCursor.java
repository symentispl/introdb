package introdb.heap;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import introdb.heap.Record.Mark;

class PageCursor implements Iterator<Record> {

  private static final int UNSET = -1;

  private final ByteBuffer page;
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
      var b = Mark.valueOf(page.get());
      if (Record.Mark.PRESENT.equals(b)) {
        return hasNext = true;
      }
      if (Record.Mark.REMOVED.equals(b)) {
        Record.skip(() -> page);
      }
    }
    return hasNext = false;
  }

  @Override
  public Record next() {
    if (hasNext || hasNext()) {
      hasNext = false;
      mark = page.position();
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
      throw new IllegalStateException();
    }
  }

  private boolean unset() {
    return mark >= UNSET;
  }

  ByteBuffer page() {
    return page;
  }

}
