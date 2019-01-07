package introdb.heap.alloc;

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;

public class RegionAllocator {

	private final int initialNumberOfRegions;
	private final int maxRegionSize;
	private final int minRegionSize;

	private final ConcurrentSkipListSet<Region> freeList;
	
	public RegionAllocator(int initialNumberOfRegions, int maxRegionSize, int minRegionSize) {
		this.initialNumberOfRegions = initialNumberOfRegions;
		this.maxRegionSize = maxRegionSize;
		this.minRegionSize = minRegionSize;
		this.freeList= new ConcurrentSkipListSet<Region>((a,b) -> ((a.pageNr()*a.size())+a.offset())-((b.pageNr()*b.size())+b.offset()));
		
		// pre fill free regions
		for(int i=0;i<initialNumberOfRegions;i++) {
			freeList.add(new Region(i,0,maxRegionSize));
		}
	}

	Optional<Region> alloc(int size) {
		var iterator = freeList.iterator();
		
		
		int regionSize=size;
		if(regionSize<minRegionSize) {
			regionSize=minRegionSize;
		}
		
		while(iterator.hasNext()) {
			Region region = iterator.next();
			if(regionSize<=region.size()) {
				iterator.remove();
				freeList.add(new Region(region.pageNr(),region.offset()+regionSize,region.size()-regionSize));
				return Optional.of(new Region(region.pageNr(),region.offset(),regionSize));
			}
		}
		return Optional.empty();
	}
		
	void free(Region region) {
		freeList.add(region);
	}
	
}
