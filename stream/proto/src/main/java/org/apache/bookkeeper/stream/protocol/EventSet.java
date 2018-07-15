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

package org.apache.bookkeeper.stream.protocol;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_EVENT_SEQUENCE;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_WRITER_ID;

import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import java.io.IOException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.common.coder.LengthPrependerCoder;
import org.apache.bookkeeper.stream.proto.CompressionCodecType;
import org.apache.bookkeeper.stream.proto.EventSetFormatType;
import org.apache.bookkeeper.stream.proto.EventSetFormatVersion;
import org.apache.bookkeeper.stream.proto.EventSetType;

/**
 * A set of {@code Event}s.
 * <pre>
 * Structure: Header | Payload | BitMaps
 * Bytes 0  - 3                 : Checksum (CRC32C)
 * Bytes 4  - 7                 : Metadata (version + flags)
 * Bytes 8  - 15                : Writer Id (Which client wrote this event set)
 * Bytes 16 - 19                : Number of Events
 * Bytes 20 - 23                : Original Payload Length
 * Bytes 24 - 27                : Actual Payload Length
 * Bytes 27 - (27 + length - 1) : Payload
 * Bytes (27 + length) - (27 + length + num_events / 8): key bitmap
 * Bytes (27 + length + num_events / 8 + 1) - (27 + length + 2 * num_events / 8): event time bitmap
 *
 * ------------------------------------------------------
 *
 * Metadata: Version and Flags // 32 Bits
 * --------------------------------------
 *
 * Byte 0 : version
 *
 * VO = version 0
 *
 * Byte 1 : metadata
 *
 * 0 0 0 0 0 0... 0
 * |_| |_|_| |
 *   |   |   |
 *   |   |   |
 *   |   |   |
 *   |   |   +---------- 0: non-idempotent, 1: idempotent
 *   |   |
 *   |   |______
 *   |          |
 *   |  EventSet Type: // 3 Bits (Most significant)
 *   |  -------------------------------------------
 *   |  000       : DATA
 *   |  001       : CTRL
 *   |
 *   |
 *   |_______
 *           |
 *    EventSet Format Type: // 2 Bits (Most significant)
 *    --------------------------------------------------
 *    00          : ROW Format
 *
 * Byte 2 : unused flags
 *
 * Byte 3 : compression flags
 *
 * 0 ... 0 0 0 0 0
 *           |_|_|
 *             |
 *    Compression Codec: // 3 Bits (Least significant)
 *    ------------------------------------------------
 *    000        : No Compression
 *    001        : LZ4 Compression
 *
 * </pre>
 */
@Slf4j
public class EventSet {

    /**
     * Writer to append events to a {@link EventSet}.
     *
     * @param <K> the type parameter for key
     * @param <V> the type parameter for value
     */
    public interface Writer<K, V> {

        /**
         * Get number events that has been written to this event set.
         *
         * @return number events that has been written to this event set.
         */
        int getNumEvents();

        /**
         * Attempt to write an {@code Event} to this event set.
         *
         * @param key       key of this event. it can be null.
         * @param value     value of this event. it can not be null.
         * @param eventTime the event time of this event. it can be null.
         * @return true if an event has been successfully written to the event set, otherwise false.
         */
        boolean writeEvent(@Nullable K key,
                           V value,
                           @Nullable Long eventTime);

        /**
         * Attempt to write an {@code Event} to this event set.
         *
         * <p>The provided <code>eventSequence</code> is used as the unique identifier for achieving idempotence.
         *
         * @param eventSequence event sequence number of this event.
         * @param key           key of this event. it can be null.
         * @param value         value of this event. it can not be null.
         * @param eventTime     the event time of this event. it can be null.
         * @return true if an event has been successfully written to the event set, otherwise false.
         */
        boolean writeEvent(long eventSequence,
                           @Nullable K key,
                           V value,
                           @Nullable Long eventTime);

        /**
         * Finalize the writer and return byte buf.
         *
         * <p>The implementation should close and finalize the writer.
         *
         * @return the finalized byte buf.
         */
        ByteBuf complete();

    }

    /**
     * Reader to iterate the events in a {@link EventSet}.
     *
     * @param <K> the type parameter for key
     * @param <V> the type parameter for value
     */
    public interface Reader<K, V> extends ReferenceCounted {

        /**
         * Returns whether the event set is idempotent or not.
         *
         * @return true if the event set is idempotent, otherwise false.
         */
        boolean isIdempotent();

        /**
         * Returns the number of events in this iterator.
         *
         * @return the number of events in this iterator.
         */
        int numEvents();

        /**
         * Return the type of this event set.
         *
         * @return event set type
         */
        EventSetType getEventSetType();

        /**
         * Check whether there are still any events in the {@link EventSet}.
         *
         * <p>The implementation should not advance if {@link #advance()} is not called.
         *
         * @return true if there are still any events in the event set; otherwise return false.
         */
        boolean hasNext();

        /**
         * Advance the event iterator to next event.
         *
         * @throws IOException the io exception
         */
        void advance();

        /**
         * Return the key of current event.
         *
         * @return key of current event.
         */
        K getKey();

        /**
         * Return the value of current event.
         *
         * @return value of current event.
         */
        V getValue();

        /**
         * Return the event time of current event.
         *
         * @return the event time of current event.
         */
        Long getEventTime();

        /**
         * Return the event sequence associated with current event.
         *
         * @return the event sequence of current event.
         */
        long getEventSequence();

        /**
         * Return the id of the writer who wrote this event set.
         *
         * @return the id of the writer
         */
        long getWriterId();

    }

    /**
     * The Header length.
     */
    static final int HEADER_LEN =
        4 /* CHECK SUM */
            + 4 /* METADATA */
            + 8 /* Writer Id */
            + 4 /* COUNT */
            + 8 /* LENGTHS */;

    /**
     * The constant METADATA_VERSION_MASK.
     */
    static final int METADATA_VERSION_MASK = 0xf0000000;
    /**
     * The Metadata version shift.
     */
    static final int METADATA_VERSION_SHIFT = 24;
    /**
     * The Metadata format type mask.
     */
    static final int METADATA_FORMAT_TYPE_MASK = 0x00c00000;
    /**
     * The Metadata format type shift.
     */
    static final int METADATA_FORMAT_TYPE_SHIFT = 22;
    /**
     * The Metadata eventset type mask.
     */
    static final int METADATA_EVENTSET_TYPE_MASK = 0x00038000;
    /**
     * The Metadata eventset type shift.
     */
    static final int METADATA_EVENTSET_TYPE_SHIFT = 19;
    /**
     * The Metadata eventset type mask.
     */
    static final int METADATA_IDEMPOTENT_MASK = 0x00004000;
    /**
     * The Metadata eventset type shift.
     */
    static final int METADATA_IDEMPOTENT_SHIFT = 18;
    /**
     * The Metadata compression mask.
     */
    static final int METADATA_COMPRESSION_MASK = 0x00000003;
    /**
     * The Metadata compression shift.
     */
    static final int METADATA_COMPRESSION_SHIFT = 0;

    /**
     * Create a writer builder to build {@link Writer}.
     *
     * @param <K> the type parameter for key
     * @param <V> the type parameter for value
     * @return the eventset writer builder.
     */
    public static <K, V> WriterBuilder<K, V> newWriterBuilder() {
        return new WriterBuilder<>();
    }

    /**
     * Build EventSet Writer.
     *
     * @param <K> the type parameter for key
     * @param <V> the type parameter for value
     */
    public static class WriterBuilder<K, V> {

        private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
        private static final int DEFAULT_MAX_NUM_RECORDS = 128;

        private Coder<K> keyCoder = null;
        private Coder<V> valueCoder = null;
        private CompressionCodecType codecType = CompressionCodecType.NONE;
        private EventSetType eventSetType = EventSetType.DATA;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private int maxNumEvents = DEFAULT_MAX_NUM_RECORDS;
        private long writerId = INVALID_WRITER_ID;
        private boolean idempotent = false;

        private WriterBuilder() {
        }

        /**
         * Create an eventset writer builder with a provided {@code writerId}.
         *
         * @param writerId the id of the writer who is writing this event set.
         * @return writer builder.
         */
        public WriterBuilder<K, V> withWriterId(long writerId) {
            this.writerId = writerId;
            return this;
        }

        /**
         * Create an eventset writer builder with a provided {@code keyCoder}.
         *
         * @param keyCoder key coder for the eventset writer.
         * @return writer builder.
         */
        public WriterBuilder<K, V> withKeyCoder(Coder<K> keyCoder) {
            this.keyCoder = LengthPrependerCoder.of(keyCoder);
            return this;
        }

        /**
         * Create an eventset writer builder with a provided {@code valueCoder}.
         *
         * @param valueCoder value coder for the eventset writer.
         * @return writer builder.
         */
        public WriterBuilder<K, V> withValueCoder(Coder<V> valueCoder) {
            this.valueCoder = LengthPrependerCoder.of(valueCoder);
            return this;
        }

        /**
         * Create an eventset writer builder with a provided compression {@code type}.
         *
         * @param cType the compression type
         * @return writer builder.
         */
        public WriterBuilder<K, V> withCompressionCodec(CompressionCodecType cType) {
            this.codecType = cType;
            return this;
        }

        /**
         * Create an eventset writer builder with a provided {@code bufferSize}.
         *
         * @param bufferSize the buffer size for the eventset writer to write events.
         * @return writer builder.
         */
        public WriterBuilder<K, V> withBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        /**
         * Create an eventset writer builder with a provided {@code maxNumEvents}.
         *
         * @param maxNumEvents max number of events that this set would buffer.
         * @return writer builder.
         */
        public WriterBuilder<K, V> withMaxNumEvents(int maxNumEvents) {
            this.maxNumEvents = maxNumEvents;
            return this;
        }

        /**
         * Create an eventset writer builder with a given eventset type.
         *
         * @param eventSetType eventset type
         * @return writer builder.
         */
        public WriterBuilder<K, V> withEventSetType(EventSetType eventSetType) {
            this.eventSetType = eventSetType;
            return this;
        }

        /**
         * Create an eventset writer builder with idempotency.
         *
         * @param idempotent flag to indicate whether the eventset takes idempotent events.
         * @return writer builder.
         */
        public WriterBuilder<K, V> withIdempotency(boolean idempotent) {
            this.idempotent = idempotent;
            return this;
        }

        /**
         * Validate the builder.
         */
        public void validate() {
            checkNotNull(keyCoder, "A null key coder is found");
            checkNotNull(valueCoder, "A null value coder is found");
            checkNotNull(codecType, "A null compression codec is found");
            checkArgument(CompressionCodecType.NONE == codecType || CompressionCodecType.LZ4 == codecType,
                "A unknown compression codec is found : " + codecType);
            checkNotNull(eventSetType, "A null event set type is found");
            checkArgument(EventSetType.DATA == eventSetType || EventSetType.CTRL == eventSetType,
                "A unknown eventset type is found : " + eventSetType);
            checkArgument(bufferSize > 0,
                "A non-positive buffer size is provided : " + bufferSize);
            checkArgument(maxNumEvents % 8 == 0 && maxNumEvents / 8 > 0,
                "Max num events should be more than 8 and times of 8");
            checkArgument(writerId > INVALID_WRITER_ID,
                "Invalid writer id : " + writerId);
        }

        /**
         * Create a new eventset writer.
         *
         * @return the new eventset writer.
         */
        public Writer<K, V> build() {
            validate();

            return new WriterImpl<>(
                writerId,
                keyCoder,
                valueCoder,
                codecType,
                eventSetType,
                bufferSize,
                maxNumEvents,
                idempotent);
        }

    }

    private static class WriterImpl<K, V> implements Writer<K, V> {

        private final long writerId;
        private final Coder<K> keyCoder;
        private final Coder<V> valueCoder;
        private final CompressionCodecType cType;
        private final ByteBuf keyBitMapBuf;
        private final ByteBuf etBitMapBuf;
        private final int maxNumEvents;
        private final boolean idempotent;
        private ByteBuf buffer;
        private int numEvents = 0;
        private int currentKeyBitMap = 0;
        private int currentEtBitMap = 0;
        private int numBitsInCurrentByte = 0;
        private boolean isEventSetSealed = false;

        private WriterImpl(long writerId,
                           Coder<K> keyCoder,
                           Coder<V> valueCoder,
                           CompressionCodecType cType,
                           EventSetType eventSetType,
                           int bufferSize,
                           int maxNumEvents,
                           boolean idempotent) {
            this.writerId = writerId;
            this.keyCoder = keyCoder;
            this.valueCoder = valueCoder;
            this.cType = cType;
            this.maxNumEvents = maxNumEvents;
            this.idempotent = idempotent;

            // allocate the buffers
            this.buffer = PooledByteBufAllocator.DEFAULT.buffer(Math.max(bufferSize, HEADER_LEN));
            this.keyBitMapBuf = PooledByteBufAllocator.DEFAULT.buffer(maxNumEvents / 8);
            this.etBitMapBuf = PooledByteBufAllocator.DEFAULT.buffer(maxNumEvents / 8);

            // write the eventset header
            this.buffer.writerIndex(4); // skip checksum header
            int metadata = (EventSetFormatVersion.V0.getNumber() << METADATA_VERSION_SHIFT)
                | (EventSetFormatType.ROW.getNumber() << METADATA_FORMAT_TYPE_SHIFT)
                | (eventSetType.getNumber() << METADATA_EVENTSET_TYPE_SHIFT)
                | cType.getNumber();
            if (idempotent) {
                metadata |= (1 << METADATA_IDEMPOTENT_SHIFT);
            }
            this.buffer.writeInt(metadata); // metadata
            this.buffer.writeLong(writerId); // writer id
            this.buffer.writerIndex(HEADER_LEN); // skip the header
        }

        public int getNumEvents() {
            return numEvents;
        }

        @Override
        public boolean writeEvent(@Nullable K key,
                                  V value,
                                  @Nullable Long eventTime) {
            if (idempotent) {
                throw new UnsupportedOperationException("Use #writeEvent(long, K, V, Long) for idempotent events.");
            }
            return doWriteEvent(INVALID_EVENT_SEQUENCE, key, value, eventTime);
        }

        @Override
        public boolean writeEvent(long eventSequence,
                                  @Nullable K key,
                                  V value,
                                  @Nullable Long eventTime) {
            if (!idempotent) {
                throw new UnsupportedOperationException("Use #writeEvent(K, V, Long) for non-idempotent events");
            }
            return doWriteEvent(eventSequence, key, value, eventTime);
        }

        protected boolean doWriteEvent(long eventSequence,
                                       @Nullable K key,
                                       V value,
                                       @Nullable Long eventTime) {
            if (isEventSetSealed) {
                return false;
            }
            if (numEvents >= maxNumEvents) {
                isEventSetSealed = true;
                return false;
            }

            int writerIndex = buffer.writerIndex();
            try {
                if (idempotent) {
                    buffer.writeLong(eventSequence);
                }
                if (null != key) {
                    this.currentKeyBitMap = (this.currentKeyBitMap | 0x1 << numBitsInCurrentByte);
                    keyCoder.encode(key, buffer);
                }
                if (null != eventTime) {
                    this.currentEtBitMap = (this.currentEtBitMap | 0x1 << numBitsInCurrentByte);
                    buffer.writeLong(eventTime);
                }
                valueCoder.encode(value, buffer);
            } catch (Exception ioe) {
                // we encountered exception on writing event to the set. reject the event and seal the set.
                buffer.writerIndex(writerIndex);
                isEventSetSealed = true;
                return false;
            }
            ++numEvents;
            ++numBitsInCurrentByte;
            if (8 == numBitsInCurrentByte) {
                this.keyBitMapBuf.writeByte(currentKeyBitMap);
                currentKeyBitMap = 0;
                this.etBitMapBuf.writeByte(currentEtBitMap);
                currentEtBitMap = 0;
                numBitsInCurrentByte = 0;
            }
            return true;
        }

        @Override
        public ByteBuf complete() {
            isEventSetSealed = true;
            // write bit maps
            if (numBitsInCurrentByte > 0) {
                this.keyBitMapBuf.writeByte(currentKeyBitMap);
                this.etBitMapBuf.writeByte(currentEtBitMap);
            }


            if (CompressionCodecType.NONE == cType) {
                return uncompressComplete();
            }

            throw new UnsupportedOperationException("LZ4 compression is not supported yet.");
        }

        private ByteBuf uncompressComplete() {
            int dataLen = buffer.readableBytes() - HEADER_LEN;
            // update count
            buffer.markWriterIndex();
            buffer.writerIndex(16); // skip checksum, metadata and writer id
            buffer.writeInt(numEvents);
            buffer.writeInt(dataLen);
            buffer.writeInt(dataLen);
            buffer.resetWriterIndex();
            buffer.markReaderIndex();
            buffer.readerIndex(4);
            int checksum = Crc32cIntChecksum.computeChecksum(buffer);
            buffer.resetReaderIndex();
            checksum = Crc32cIntChecksum.resumeChecksum(checksum, keyBitMapBuf);
            checksum = Crc32cIntChecksum.resumeChecksum(checksum, etBitMapBuf);
            buffer.markWriterIndex();
            buffer.writerIndex(0);
            buffer.writeInt(checksum);
            buffer.resetWriterIndex();

            if (buffer.writableBytes() > (keyBitMapBuf.readableBytes() + etBitMapBuf.readableBytes())) {
                try {
                    buffer.writeBytes(keyBitMapBuf);
                    buffer.writeBytes(etBitMapBuf);
                } finally {
                    keyBitMapBuf.release();
                    etBitMapBuf.release();
                }
                return buffer;
            } else {
                return Unpooled.compositeBuffer(3)
                    .addComponent(buffer)
                    .addComponent(keyBitMapBuf)
                    .addComponent(etBitMapBuf);
            }
        }
    }

    /**
     * New reader builder reader builder.
     *
     * @param <K> the type parameter
     * @param <V> the type parameter
     * @return the reader builder
     */
    public static <K, V> ReaderBuilder<K, V> newReaderBuilder() {
        return new ReaderBuilder<>();
    }

    /**
     * Build EventSet Reader.
     *
     * @param <K> the type parameter for key
     * @param <V> the type parameter for value
     */
    public static class ReaderBuilder<K, V> {

        private Coder<K> keyCoder = null;
        private Coder<V> valueCoder = null;

        private ReaderBuilder() {
        }

        /**
         * Create an eventset reader builder with a provided {@code keyCoder}.
         *
         * @param keyCoder key coder for the eventset reader.
         * @return reader builder.
         */
        public ReaderBuilder<K, V> withKeyCoder(Coder<K> keyCoder) {
            this.keyCoder = LengthPrependerCoder.of(keyCoder);
            return this;
        }

        /**
         * Create an eventset reader builder with a provided {@code valueCoder}.
         *
         * @param valueCoder value coder for the eventset reader.
         * @return reader builder.
         */
        public ReaderBuilder<K, V> withValueCoder(Coder<V> valueCoder) {
            this.valueCoder = LengthPrependerCoder.of(valueCoder);
            return this;
        }

        /**
         * Create a new eventset reader.
         *
         * @param buffer the buffer
         * @return the new event set reader.
         * @throws IOException the io exception
         */
        public Reader<K, V> build(ByteBuf buffer) throws IOException {
            checkNotNull(keyCoder, "A null key coder is found");
            checkNotNull(valueCoder, "A null value coder is found");

            int checksum = buffer.readInt();
            int readerIdxAfterChecksum = buffer.readerIndex();
            int metadata = buffer.readInt();
            long writerId = buffer.readLong();
            int totalEvents = buffer.readInt();
            int originalLen = buffer.readInt();
            int actualLen = buffer.readInt();

            if (buffer.readableBytes() < actualLen) {
                throw new IOException("Encountered a partial written event set :"
                    + " payload len is expected to be " + actualLen + ", but only "
                    + buffer.readableBytes() + " is found.");
            }
            int version = (metadata & METADATA_VERSION_MASK) >> METADATA_VERSION_SHIFT;
            if (EventSetFormatVersion.V0.getNumber() != version) {
                throw new IOException("Unknown version found in the event set : expected = "
                    + EventSetFormatVersion.V0.getNumber() + " but " + version + " is found.");
            }
            int formatType = (metadata & METADATA_FORMAT_TYPE_MASK) >> METADATA_FORMAT_TYPE_SHIFT;
            if (EventSetFormatType.ROW.getNumber() != formatType) {
                throw new IOException("Unknown format type found in the event set : expected = "
                    + EventSetFormatType.ROW.getNumber() + " but " + formatType + " is found.");
            }
            int eventSetTypeCode = (metadata & METADATA_EVENTSET_TYPE_MASK) >> METADATA_EVENTSET_TYPE_SHIFT;
            EventSetType eventSetType = EventSetType.forNumber(eventSetTypeCode);
            if (null == eventSetType) {
                throw new IOException("Unknown event set type found in the event set : expected = DATA or CTRL, but "
                    + eventSetTypeCode + " is found.");
            }
            int compressionCodecCode = (metadata & METADATA_COMPRESSION_MASK) >> METADATA_COMPRESSION_SHIFT;
            CompressionCodecType codecType = CompressionCodecType.forNumber(compressionCodecCode);
            if (CompressionCodecType.NONE != codecType) {
                throw new IOException("Unsupported compression codec found : " + codecType
                    + "(code = " + compressionCodecCode + ")");
            }
            if (CompressionCodecType.NONE == codecType && actualLen != originalLen) {
                throw new IOException("Inconsistent length found in an uncompressed event set : original length = "
                    + originalLen + ", actual length = " + actualLen);
            }

            boolean idempotent = (metadata & METADATA_IDEMPOTENT_MASK) == METADATA_COMPRESSION_MASK;

            int numBytesInBitMap = totalEvents / 8;
            if (totalEvents % 8 > 0) {
                numBytesInBitMap += 1;
            }
            if (buffer.readableBytes() < actualLen + 2 * numBytesInBitMap) {
                throw new IOException("Encountered a partial written event set :"
                    + " payload len + bitmaps len is expected to be "
                    + (actualLen + 2 * numBytesInBitMap) + ", but only "
                    + buffer.readableBytes() + " is found.");
            }

            ByteBuf payloadBufForCheckSum = buffer.slice(
                readerIdxAfterChecksum, actualLen + HEADER_LEN - 4);
            ByteBuf payloadBuf = buffer.slice(
                buffer.readerIndex(), actualLen);
            ByteBuf keyBitMapBuf = buffer.slice(
                buffer.readerIndex() + actualLen, numBytesInBitMap);
            ByteBuf etBitMapBuf = buffer.slice(
                buffer.readerIndex() + actualLen + numBytesInBitMap, numBytesInBitMap);

            int recomputedChecksum = Crc32cIntChecksum.computeChecksum(payloadBufForCheckSum);
            recomputedChecksum = Crc32cIntChecksum.resumeChecksum(recomputedChecksum, keyBitMapBuf);
            recomputedChecksum = Crc32cIntChecksum.resumeChecksum(recomputedChecksum, etBitMapBuf);

            if (checksum != recomputedChecksum) {
                buffer.release();

                throw new IOException("Inconsistent checksum found : expected = "
                    + checksum + " but found = " + recomputedChecksum);
            }

            return new ReaderImpl<>(
                writerId,
                keyCoder,
                valueCoder,
                totalEvents,
                buffer,
                payloadBuf,
                keyBitMapBuf,
                etBitMapBuf,
                eventSetType,
                idempotent);
        }

    }

    private static class ReaderImpl<K, V> implements Reader<K, V>, ReferenceCounted {

        private final long writerId;
        private final Coder<K> keyCoder;
        private final Coder<V> valueCoder;
        private final int totalEvents;
        private final ByteBuf eventSetBuf;
        private final ByteBuf dataBuf;
        private final ByteBuf keyBitMapBuf;
        private final ByteBuf etBitMapBuf;
        private final EventSetType eventSetType;
        private final boolean idempotent;
        /**
         * The Number events.
         */
        int numEvents = 0;
        /**
         * The Current key bit map byte.
         */
        int currentKeyBitMapByte = 0;
        /**
         * The Current et bit map byte.
         */
        int currentEtBitMapByte = 0;
        /**
         * The Current key.
         */
        K currentKey;
        /**
         * The Current value.
         */
        V currentValue;
        /**
         * The Current timestamp.
         */
        Long currentTimestamp;
        /**
         * The Current Event Seq.
         */
        long currentEventSeq = INVALID_EVENT_SEQUENCE;

        private ReaderImpl(long writerId,
                           Coder<K> keyCoder,
                           Coder<V> valueCoder,
                           int totalEvents,
                           ByteBuf eventSetBuf,
                           ByteBuf dataBuf,
                           ByteBuf keyBitMapBuf,
                           ByteBuf etBitMapBuf,
                           EventSetType eventSetType,
                           boolean idempotent) {
            this.writerId = writerId;
            this.keyCoder = keyCoder;
            this.valueCoder = valueCoder;
            this.totalEvents = totalEvents;
            this.eventSetBuf = eventSetBuf;
            this.dataBuf = dataBuf;
            this.keyBitMapBuf = keyBitMapBuf;
            this.etBitMapBuf = etBitMapBuf;
            this.eventSetType = eventSetType;
            this.idempotent = idempotent;
            if (totalEvents > 0) {
                currentKeyBitMapByte = keyBitMapBuf.readByte();
                currentEtBitMapByte = etBitMapBuf.readByte();
            }
        }

        @Override
        public boolean isIdempotent() {
            return idempotent;
        }

        @Override
        public long getWriterId() {
            return writerId;
        }

        @Override
        public int numEvents() {
            return totalEvents;
        }

        @Override
        public EventSetType getEventSetType() {
            return eventSetType;
        }

        @Override
        public boolean hasNext() {
            return numEvents < totalEvents;
        }

        @Override
        public void advance() {
            int bitIdx = numEvents % 8;
            boolean hasKey = ((currentKeyBitMapByte >> bitIdx) & 1) == 1;
            boolean hasTs = ((currentEtBitMapByte >> bitIdx) & 1) == 1;
            if (idempotent) {
                currentEventSeq = dataBuf.readLong();
            }
            if (hasKey) {
                currentKey = keyCoder.decode(dataBuf);
            } else {
                currentKey = null;
            }
            if (hasTs) {
                currentTimestamp = dataBuf.readLong();
            } else {
                currentTimestamp = null;
            }
            currentValue = valueCoder.decode(dataBuf);
            ++numEvents;
            if (hasNext() && numEvents % 8 == 0) {
                currentKeyBitMapByte = keyBitMapBuf.readByte();
                currentEtBitMapByte = etBitMapBuf.readByte();
            }
        }

        @Override
        public K getKey() {
            return currentKey;
        }

        @Override
        public V getValue() {
            return currentValue;
        }

        @Override
        public Long getEventTime() {
            return currentTimestamp;
        }

        @Override
        public long getEventSequence() {
            return currentEventSeq;
        }

        @Override
        public int refCnt() {
            return eventSetBuf.refCnt();
        }

        @Override
        public ReferenceCounted retain() {
            eventSetBuf.retain();
            return this;
        }

        @Override
        public ReferenceCounted retain(int increment) {
            eventSetBuf.retain(increment);
            return this;
        }

        @Override
        public ReferenceCounted touch() {
            eventSetBuf.touch();
            return this;
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            eventSetBuf.touch(hint);
            return this;
        }

        @Override
        public boolean release() {
            return eventSetBuf.release();
        }

        @Override
        public boolean release(int decrement) {
            return eventSetBuf.release(decrement);
        }
    }

}
