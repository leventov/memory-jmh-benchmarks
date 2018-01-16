package com.yahoo.datasketches;

import com.google.common.base.Utf8;
import com.google.common.primitives.Ints;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class Utf8ReadBenchmark
{
    @Param({"true", "false"})
    public boolean ascii = true;

    @Param({"10", "100"})
    public int len = 10;

    /**
     * Testing 100 randomized strings at once, to avoid branch prediction and JIT favoring some specific cases.
     */
    private static final int N = 100;

    private int[] codedLengths;
    private WritableMemory mem;
    private ByteBuffer buf;

    // State
    private StringBuilder sb;
    private ByteBuffer bb;
    private CharsetDecoder utf8Decoder;
    private CharBuffer charBuffer;

    @Setup(Level.Trial)
    public void prepareEncodedBytesAndState()
    {
        Random random = ThreadLocalRandom.current();
        char firstChar = ascii ? 'a' : 'а'; // second a is russian
        char lastChar = ascii ? 'z' : 'я';
        List<String> randomStrings = Stream
            .generate(() -> random
                .ints(firstChar, lastChar)
                .limit(len)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString()
            )
            .limit(N)
            .collect(Collectors.toList());
        codedLengths = randomStrings.stream().mapToInt(Utf8::encodedLength).toArray();
        int totalLength = IntStream.of(codedLengths).sum();

        mem = WritableMemory.allocate(totalLength);
        long memOffset = 0;
        for (int i = 0; i < N; i++) {
            long newMemOffset = Ints.checkedCast(mem.putCharsToUtf8(memOffset, randomStrings.get(i)));
            if (newMemOffset - memOffset != codedLengths[i]) {
                throw new RuntimeException();
            }
            memOffset = newMemOffset;
        }

        CharsetEncoder utf8Encoder = StandardCharsets.UTF_8.newEncoder();
        buf = ByteBuffer.allocate(totalLength);
        for (String s : randomStrings) {
            utf8Encoder.reset();
            CoderResult res = utf8Encoder.encode(CharBuffer.wrap(s), buf, true);
            if (res != CoderResult.UNDERFLOW) {
                throw new RuntimeException();
            }
        }
        buf.clear();

        // Prepare state
        sb = new StringBuilder(len);
        utf8Decoder = StandardCharsets.UTF_8.newDecoder();
        bb = ByteBuffer.allocate(IntStream.of(codedLengths).max().getAsInt());
        charBuffer = CharBuffer.allocate(len);
    }

    @Benchmark
    public int memoryGetCharsAsUtf8(Utf8ReadBenchmark st) throws IOException
    {
        int lenSum = 0;
        long offset = 0;
        WritableMemory mem = st.mem;
        CharBuffer charBuffer = st.charBuffer;
        for (int codedLength : st.codedLengths) {
            charBuffer.position(0);
            try {
                mem.getCharsFromUtf8(offset, codedLength, charBuffer);
            }
            catch (RuntimeException e) {
                byte[] bytes = new byte[codedLength];
                mem.getByteArray(offset, bytes, 0, codedLength);
                throw new RuntimeException(
                    Arrays.toString(charBuffer.array()) + " pos: " + charBuffer.position() + ", codedLength: " + codedLength + ", " + Arrays.toString(bytes),
                    e
                );
            }
            offset += codedLength;
            lenSum += charBuffer.position();
        }
        return lenSum;
    }

    @Benchmark
    public int memoryGetBytesAndDecode(Utf8ReadBenchmark st)
    {
        ByteBuffer bb = st.bb;
        byte[] ba = bb.array();
        int lenSum = 0;
        long offset = 0;
        WritableMemory mem = st.mem;
        CharsetDecoder utf8Decoder = st.utf8Decoder;
        CharBuffer charBuffer = st.charBuffer;
        for (int codedLength : st.codedLengths) {
            mem.getByteArray(offset, ba, 0, codedLength);
            bb.position(0);
            bb.limit(codedLength);
            utf8Decoder.reset();
            charBuffer.position(0);
            CoderResult res = utf8Decoder.decode(bb, charBuffer, true);
            if (res != CoderResult.UNDERFLOW) {
                throw new RuntimeException(res.toString());
            }
            offset += codedLength;
            lenSum += charBuffer.position();
        }
        return lenSum;
    }

    @Benchmark
    public int byteBufferDecode(Utf8ReadBenchmark st)
    {
        int lenSum = 0;
        ByteBuffer buf = st.buf;
        buf.clear();
        CharsetDecoder utf8Decoder = st.utf8Decoder;
        CharBuffer charBuffer = st.charBuffer;
        for (int codedLength : st.codedLengths) {
            buf.limit(buf.position() + codedLength);
            utf8Decoder.reset();
            charBuffer.position(0);
            CoderResult res = utf8Decoder.decode(buf, charBuffer, true);
            if (res != CoderResult.UNDERFLOW) {
                throw new RuntimeException(res.toString());
            }
            lenSum += charBuffer.position();
        }
        return lenSum;
    }
}
