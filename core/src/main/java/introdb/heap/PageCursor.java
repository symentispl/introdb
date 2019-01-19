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
  private Record record;

  PageCursor(ByteBuffer page) {
    this.page = page;
  }

  @Override
  public boolean hasNext() {

    if (hasNext) {
      return true;
    }
    
    record =null;

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
      return record=Record.read(page);
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
  
  Record record() {
    if(record==null) {
      throw new IllegalStateException("next() was not called or hasNext() was called already");
    }
    return record;
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
