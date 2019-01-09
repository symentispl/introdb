package introdb.heap.alloc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class RegionAllocatorTest {

	private static final int MIN_REGION_SIZE = 128;
	private static final int _4Kb = 4 * 1024;
	
	private final  RegionAllocator regionAllocator = new RegionAllocator(1, _4Kb, MIN_REGION_SIZE);

	@Test
	public void alloc_full_region() {
		var region = regionAllocator.alloc(_4Kb);

		assertThat(region).get().isEqualTo(new Region(0, 0, _4Kb));

		var noRegion = regionAllocator.alloc(_4Kb);
		assertThat(noRegion).isEmpty();

	}

	@Test
	public void alloc_part_of_region() {
		var firstRegion = regionAllocator.alloc(1024);

		assertThat(firstRegion).get().isEqualTo(new Region(0, 0, 1024));

		var secondRegion = regionAllocator.alloc(1024);
		assertThat(secondRegion).get().isEqualTo(new Region(0, 1024, 1024));

	}

	@Test
	public void alloc_from_next_region() {
		RegionAllocator regionAllocator = new RegionAllocator(2, _4Kb, MIN_REGION_SIZE);

		var firstRegion = regionAllocator.alloc(_4Kb);

		assertThat(firstRegion).get().isEqualTo(new Region(0, 0, _4Kb));

		var secondRegion = regionAllocator.alloc(_4Kb);
		assertThat(secondRegion).get().isEqualTo(new Region(1, 0, _4Kb));

	}

	@Test
	public void alloc_smaller_than_min_region_size() {
		var firstRegion = regionAllocator.alloc(64);

		assertThat(firstRegion).get().isEqualTo(new Region(0, 0, MIN_REGION_SIZE));

		var secondRegion = regionAllocator.alloc(_4Kb-MIN_REGION_SIZE);
		assertThat(secondRegion).get().isEqualTo(new Region(0, 128, _4Kb-MIN_REGION_SIZE));
		
	}

	@Test
	public void free_region() {
		var region = regionAllocator.alloc(_4Kb);
		regionAllocator.free(region.get());
		region = regionAllocator.alloc(_4Kb);
		assertThat(region).get().isEqualTo(new Region(0, 0, _4Kb));
	}
	
	@Test
	public void dont_merge_not_adjacent_regions() {
		var region0 = regionAllocator.alloc(1024);
		var region1 = regionAllocator.alloc(1024);
		var region2 = regionAllocator.alloc(1024);
		
		regionAllocator.free(region0.get());
		assertNotNull(region1.get());
		regionAllocator.free(region2.get());
		
		var region3 = regionAllocator.alloc(1024);
		assertThat(region3).get().isEqualTo(new Region(0, 0, 1024));
	}

	@Test
	public void merge_adjacent_regions_in_the_same_page() {
		var region0 = regionAllocator.alloc(1024);
		var region1 = regionAllocator.alloc(1024);
		
		regionAllocator.free(region1.get());
		regionAllocator.free(region0.get());
		
		var region2 = regionAllocator.alloc(_4Kb);
		assertThat(region2).get().isEqualTo(new Region(0, 0, _4Kb));
	}


	@Test
	public void dont_merge_adjacent_regions_from_different_pages() {
		RegionAllocator regionAllocator = new RegionAllocator(2, _4Kb, MIN_REGION_SIZE);

		var region0 = regionAllocator.alloc(1024);
		var region1 = regionAllocator.alloc(1024);
		var region2 = regionAllocator.alloc(1024);
		
		assertNotNull(region0);
		regionAllocator.free(region2.get());
		regionAllocator.free(region1.get());
		
		var region3 = regionAllocator.alloc(_4Kb);
		assertThat(region3).get().isEqualTo(new Region(1, 0, _4Kb));
	}
}
