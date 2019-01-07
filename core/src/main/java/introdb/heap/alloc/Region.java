package introdb.heap.alloc;

import java.util.Objects;

class Region {

	private final int pageNr;
	private final int offset;
	private final int size;

	Region(int pageNr, int offset, int size) {
		super();
		this.pageNr = pageNr;
		this.offset = offset;
		this.size = size;
	}

	public int pageNr() {
		return pageNr;
	}

	public int offset() {
		return offset;
	}

	public int size() {
		return size;
	}

	@Override
	public int hashCode() {
		return Objects.hash(offset, pageNr, size);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Region other = (Region) obj;
		return offset == other.offset && pageNr == other.pageNr && size == other.size;
	}

	@Override
	public String toString() {
		return new StringBuilder()
		.append("Region [pageNr=").append(pageNr)
		.append(", offset=").append(offset)
		.append(", size=").append(size)
		.append("]")
		.toString();
	}
	
	

}
