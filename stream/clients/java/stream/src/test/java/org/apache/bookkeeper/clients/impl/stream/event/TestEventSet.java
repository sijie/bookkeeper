/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.clients.impl.stream.event;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_EVENT_SEQUENCE;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_WRITER_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet.Reader;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet.Writer;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.common.coder.VarIntCoder;
import org.apache.bookkeeper.stream.proto.CompressionCodecType;
import org.apache.bookkeeper.stream.proto.EventSetType;
import org.junit.Test;

/**
 * Unit Test for {@link EventSet}.
 */
public class TestEventSet {

    private static final Random RAND = new Random(System.currentTimeMillis());

    @Test(timeout = 10000, expected = NullPointerException.class)
    public void testBuildWriterWithNullKeyCoder() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(null)
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(8192)
            .withMaxNumEvents(8192)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = NullPointerException.class)
    public void testBuildWriterWithNullValueCoder() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(null)
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(8192)
            .withMaxNumEvents(8192)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = NullPointerException.class)
    public void testBuildWriterWithNullCompressionCodec() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(null)
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(8192)
            .withMaxNumEvents(8192)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testBuildWriterWithIllegalCompressionCodec() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(CompressionCodecType.UNRECOGNIZED)
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(8192)
            .withMaxNumEvents(8192)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = NullPointerException.class)
    public void testBuildWriterWithNullEventSetType() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(null)
            .withBufferSize(8192)
            .withMaxNumEvents(8192)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testBuildWriterWithIllegalEventSetType() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(EventSetType.UNRECOGNIZED)
            .withBufferSize(8192)
            .withMaxNumEvents(8192)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testBuildWriterWithNonPositiveBufferSize() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(0)
            .withMaxNumEvents(8192)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testBuildWriterWithIllegalMaxNumEvents() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(8192)
            .withMaxNumEvents(8192 - 1)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testBuildWriterWithNegativeMaxNumEvents() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(8192)
            .withMaxNumEvents(-1)
            .withWriterId(1234L)
            .build();
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testBuildWriterWithInvalidWriterId() {
        EventSet.<Integer, Integer>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(VarIntCoder.of())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(8192)
            .withMaxNumEvents(1024)
            .withWriterId(INVALID_WRITER_ID)
            .build();
    }

    private static <V> void assertListEquals(List<V> expected,
                                             List<V> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertTrue(Objects.equal(expected.get(i), actual.get(i)));
        }
    }

    private static <V> void assertListReferenceEquals(List<AtomicReference<V>> expected,
                                                      List<AtomicReference<V>> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertTrue(Objects.equal(expected.get(i).get(), actual.get(i).get()));
        }
    }

    private static <K, V> void eventsetWriterReaderEqual(
        List<Long> sequences,
        List<AtomicReference<K>> keys,
        List<AtomicReference<Long>> timestamps,
        List<V> values,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        CompressionCodecType codec,
        EventSetType eventSetType,
        long writerId,
        boolean idempotent) throws IOException {

        Writer<K, V> writer = EventSet.<K, V>newWriterBuilder()
            .withKeyCoder(keyCoder)
            .withValueCoder(valueCoder)
            .withCompressionCodec(codec)
            .withEventSetType(eventSetType)
            .withMaxNumEvents(1024)
            .withBufferSize(512 * 1024)
            .withWriterId(writerId)
            .withIdempotency(idempotent)
            .build();

        for (int i = 0; i < keys.size(); i++) {
            K key = keys.get(i).get();
            Long timestamp = timestamps.get(i).get();
            V value = values.get(i);

            if (idempotent) {
                assertTrue(writer.writeEvent(sequences.get(i), key, value, timestamp));
            } else {
                assertTrue(writer.writeEvent(key, value, timestamp));
            }
        }
        assertEquals(keys.size(), writer.getNumEvents());

        // complete the writer
        ByteBuf buffer = writer.complete();
        ByteBuf readBuf = buffer.retainedSlice();
        buffer.release();

        Reader<K, V> reader = null;
        try {
            // create the reader over the buffer
            reader = EventSet.<K, V>newReaderBuilder()
                .withKeyCoder(keyCoder)
                .withValueCoder(valueCoder)
                .build(readBuf);

            assertEquals("EventSet Type should be same.",
                eventSetType, reader.getEventSetType());
            assertEquals("Writer Id should be same",
                writerId, reader.getWriterId());
            assertEquals("Idempotency should be same",
                idempotent, reader.isIdempotent());

            List<Long> readSequences = Lists.newArrayListWithExpectedSize(sequences.size());
            List<AtomicReference<K>> readKeys = Lists.newArrayListWithExpectedSize(keys.size());
            List<AtomicReference<Long>> readTimestamps = Lists.newArrayListWithExpectedSize(timestamps.size());
            List<V> readValues = Lists.newArrayListWithExpectedSize(values.size());

            while (reader.hasNext()) {
                reader.advance();

                long eventSequence = reader.getEventSequence();
                if (idempotent) {
                    sequences.add(eventSequence);
                } else {
                    assertEquals(INVALID_EVENT_SEQUENCE, eventSequence);
                }

                K key = reader.getKey();
                V value = reader.getValue();
                Long timestamp = reader.getEventTime();

                readKeys.add(new AtomicReference<>(key));
                readValues.add(value);
                readTimestamps.add(new AtomicReference<>(timestamp));
            }

            if (idempotent) {
                assertListEquals(sequences, readSequences);
            }
            assertListReferenceEquals(keys, readKeys);
            assertListReferenceEquals(timestamps, readTimestamps);
            assertListEquals(values, readValues);
        } finally {
            if (null != reader) {
                reader.release();
            }
        }
    }

    private static final int[] INT_VALUES = {
        0,
        1,
        127,
        128,
        16383,
        16384,
        2097151,
        2097152,
        268435455,
        268435456,
        2147483647,
        -2147483648,
        -1,
    };

    @Test(timeout = 10000)
    public void testEventSetWriterReaderEqualWithVarIntCoder() throws Exception {
        List<AtomicReference<Integer>> keys =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<AtomicReference<Long>> timestamps =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Integer> values =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Long> sequences =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);

        for (int value : INT_VALUES) {
            keys.add(new AtomicReference<>(value));
            timestamps.add(new AtomicReference<>(System.currentTimeMillis()));
            values.add(value);
            sequences.add(INVALID_EVENT_SEQUENCE);
        }
        eventsetWriterReaderEqual(
            sequences,
            keys,
            timestamps,
            values,
            VarIntCoder.of(),
            VarIntCoder.of(),
            CompressionCodecType.NONE,
            EventSetType.DATA,
            1234L,
            false);
    }

    @Test(timeout = 10000)
    public void testEventSetWriterReaderEqualWithNullTimestamps() throws Exception {
        List<AtomicReference<Integer>> keys =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<AtomicReference<Long>> timestamps =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Integer> values =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Long> sequences =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);

        for (int value : INT_VALUES) {
            keys.add(new AtomicReference<>(value));
            timestamps.add(new AtomicReference<>(null));
            values.add(value);
            sequences.add(INVALID_EVENT_SEQUENCE);
        }
        eventsetWriterReaderEqual(
            sequences,
            keys,
            timestamps,
            values,
            VarIntCoder.of(),
            VarIntCoder.of(),
            CompressionCodecType.NONE,
            EventSetType.DATA,
            1234L,
            false);
    }

    @Test(timeout = 10000)
    public void testEventSetWriterReaderEqualWithNullKeys() throws Exception {
        List<AtomicReference<Integer>> keys =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<AtomicReference<Long>> timestamps =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Integer> values =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Long> sequences =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);

        for (int value : INT_VALUES) {
            keys.add(new AtomicReference<>(null));
            timestamps.add(new AtomicReference<>(System.currentTimeMillis()));
            values.add(value);
        }
        eventsetWriterReaderEqual(
            sequences,
            keys,
            timestamps,
            values,
            VarIntCoder.of(),
            VarIntCoder.of(),
            CompressionCodecType.NONE,
            EventSetType.DATA,
            1234L,
            false);
    }

    @Test(timeout = 10000)
    public void testEventSetWriterReaderEqualWithNullKeysTimestamps()
        throws Exception {
        List<AtomicReference<Integer>> keys =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<AtomicReference<Long>> timestamps =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Integer> values =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Long> sequences =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);

        for (int value : INT_VALUES) {
            keys.add(new AtomicReference<>(null));
            timestamps.add(new AtomicReference<>(null));
            values.add(value);
        }
        eventsetWriterReaderEqual(
            sequences,
            keys,
            timestamps,
            values,
            VarIntCoder.of(),
            VarIntCoder.of(),
            CompressionCodecType.NONE,
            EventSetType.DATA,
            1234L,
            false);
    }

    @Test(timeout = 10000)
    public void testEventSetWriterReaderEqualWithSpareValues()
        throws Exception {
        List<AtomicReference<Integer>> keys =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<AtomicReference<Long>> timestamps =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Integer> values =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Long> sequences =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);

        for (int value : INT_VALUES) {
            boolean nullValue = RAND.nextBoolean();
            keys.add(new AtomicReference<>(nullValue ? null : value));
            boolean nullTimestamp = RAND.nextBoolean();
            timestamps.add(new AtomicReference<>(nullTimestamp ? null : System.currentTimeMillis()));
            values.add(value);
        }
        eventsetWriterReaderEqual(
            sequences,
            keys,
            timestamps,
            values,
            VarIntCoder.of(),
            VarIntCoder.of(),
            CompressionCodecType.NONE,
            EventSetType.DATA,
            1234L,
            false);
    }

    @Test(timeout = 10000)
    public void testEventSetWriterReaderEqualWithIdempotency() throws Exception {
        List<AtomicReference<Integer>> keys =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<AtomicReference<Long>> timestamps =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Integer> values =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);
        List<Long> sequences =
            Lists.newArrayListWithExpectedSize(INT_VALUES.length);

        long sequence = 0L;
        for (int value : INT_VALUES) {
            keys.add(new AtomicReference<>(value));
            timestamps.add(new AtomicReference<>(System.currentTimeMillis()));
            values.add(value);
            sequences.add(sequence++);
        }
        eventsetWriterReaderEqual(
            sequences,
            keys,
            timestamps,
            values,
            VarIntCoder.of(),
            VarIntCoder.of(),
            CompressionCodecType.NONE,
            EventSetType.DATA,
            1234L,
            false);
    }

}
