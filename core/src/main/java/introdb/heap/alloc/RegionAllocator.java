package introdb.heap.alloc;

import java.util.Optional;

public class RegionAllocator {

	
	public RegionAllocator(int initialNumberOfRegions, int maxRegionSize, int minRegionSize) {
	}

	Optional<Region> alloc(int size) {
		return Optional.empty();
	}
		
	void free(Region region) {
	}
	
}
