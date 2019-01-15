package introdb.heap.alloc;

import static java.util.stream.Collectors.toList;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class RegionAllocatorBenchmark {

  private RegionAllocator regionAllocator;
  private List<Integer> ofInt;
  private Iterator<Integer> iterator;

  @Setup(Level.Trial)
  public void setUp() {
    ofInt = new Random()
        .ints(128, 128, 256)
        .boxed()
        .collect(toList());
  }

  @Setup(Level.Iteration)
  public void setUpRegionAlloc() {
    regionAllocator = new RegionAllocator(1024, 4 * 1024, 128);
    iterator = ofInt.iterator();
  }

  @Benchmark
  public void allocateAndFree() {
    if (!iterator.hasNext()) {
      iterator = ofInt.iterator();
    }
    Optional<Region> alloc = regionAllocator.alloc(iterator.next());
    Blackhole.consumeCPU(100);
    regionAllocator.free(alloc.get());
  }

}
