package com.yahoo.datasketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@Fork(1)
public class EqualsBenchmark
{

  @Param({"1", "10", "1000", "100000"})
  public int size;

  private Memory mem1;
  private Memory mem2;

  @Setup(Level.Trial)
  public void allocateMem()
  {
    mem1 = WritableMemory.allocate(size);
    mem2 = WritableMemory.allocate(size);
  }

  @Benchmark
  public boolean equals()
  {
    return mem1.equalTo(mem2);
  }
}
