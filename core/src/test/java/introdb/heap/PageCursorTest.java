package introdb.heap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.jupiter.api.Test;

import introdb.heap.Record.Mark;

class PageCursorTest {

  @Test
  void page_is_empty() {
    ByteBuffer page = ByteBuffer.allocate(4 * 1024);
    PageCursor pageCursor = new PageCursor(page);
    assertFalse(pageCursor.hasNext());
    assertThatThrownBy(() -> pageCursor.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void page_has_one_record() throws Exception {
    ByteBuffer page = ByteBuffer.allocate(4 * 1024);
    Record record = newRandomValue("0", 256);
    record.write(page);
    page.rewind();
    PageCursor pageCursor = new PageCursor(page);

    assertTrue(pageCursor.hasNext());
    assertThat(pageCursor.next()).isEqualTo(record);

    assertFalse(pageCursor.hasNext());
    assertThatThrownBy(() -> pageCursor.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void has_deleted_record() throws Exception {
    ByteBuffer page = ByteBuffer.allocate(4 * 1024);

    Record record0 = newRandomValue("0", 256);
    record0.write(page);

    Record record1 = newRemovedValue("1", 256);
    record1.write(page);

    Record record2 = newRandomValue("2", 256);
    record2.write(page);

    page.rewind();

    PageCursor pageCursor = new PageCursor(page);

    assertTrue(pageCursor.hasNext());
    assertThat(pageCursor.next()).isEqualTo(record0);

    assertTrue(pageCursor.hasNext());
    assertThat(pageCursor.next()).isEqualTo(record2);

    assertFalse(pageCursor.hasNext());
    assertThatThrownBy(() -> pageCursor.next()).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void has_next_doesnt_advance_cursor() throws Exception {
    ByteBuffer page = ByteBuffer.allocate(4 * 1024);

    Record record0 = newRandomValue("0", 256);
    record0.write(page);

    Record record1 = newRemovedValue("1", 256);
    record1.write(page);

    Record record2 = newRandomValue("2", 256);
    record2.write(page);

    page.rewind();

    PageCursor pageCursor = new PageCursor(page);

    assertTrue(pageCursor.hasNext());
    assertTrue(pageCursor.hasNext());
    assertThat(pageCursor.next()).isEqualTo(record0);
    assertThat(pageCursor.next()).isEqualTo(record2);
    assertFalse(pageCursor.hasNext());
    assertThatThrownBy(() -> pageCursor.next()).isInstanceOf(NoSuchElementException.class);

  }
  
  private Record newRandomValue(String key, int valueSize) throws Exception {
    var value = new byte[valueSize];
    new Random().nextBytes(value);
    return Record.of(new Entry(key, value));
  }

  private Record newRemovedValue(String key, int valueSize) throws Exception {
    var value = new byte[valueSize];
    new Random().nextBytes(value);
    return Record.of(new Entry(key, value), Mark.REMOVED);
  }
}
