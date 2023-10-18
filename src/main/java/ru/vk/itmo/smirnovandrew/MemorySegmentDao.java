package ru.vk.itmo.smirnovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Path tablePath;

    private static final String TABLE = "table.txt";

    private final MemorySegment ssTable;

    private final Arena arena;

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segments =
            new ConcurrentSkipListMap<>(segmentComparator);

    private static final Comparator<MemorySegment> segmentComparator = (o1, o2) -> {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == o1.byteSize()) {
            return -1;
        }
        if (mismatch == o2.byteSize()) {
            return 1;
        }
        try {
            return Byte.compare(o1.getAtIndex(ValueLayout.JAVA_BYTE, mismatch),
                    o2.getAtIndex(ValueLayout.JAVA_BYTE, mismatch));
        } catch (IndexOutOfBoundsException e) {
            return 0;
        }
    };

    public MemorySegmentDao(Config config) {
        MemorySegment ssTable1;

        arena = Arena.ofShared();

        tablePath = config.basePath().resolve(TABLE);
        try {
            long fileSize = Files.size(tablePath);
            ssTable1 = assertFile(tablePath, fileSize,
                    FileChannel.MapMode.READ_ONLY, StandardOpenOption.READ);
        } catch (IOException e) {
            ssTable1 = null;
        }
        ssTable = ssTable1;
    }

    public MemorySegmentDao() {
        tablePath = null;
        ssTable = null;
        arena = null;
    }

    private MemorySegment assertFile(Path path, long fileSize,
                                     FileChannel.MapMode mapMode, OpenOption... openOptions)
            throws IOException {
        try (var fc = FileChannel.open(path, openOptions)) {
            return fc.map(mapMode, 0, fileSize, arena);
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return segments.values().iterator();
        }
        if (from == null) {
            return segments.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return segments.tailMap(from, true).values().iterator();
        }

        return segments.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var entry = segments.get(key);
        if (entry != null) {
            return entry;
        }

        if (ssTable == null) {
            return null;
        }

        long fileSize = ssTable.byteSize();

        long offset = 0;
        while (offset < fileSize) {
            long keyLen = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long valueLen = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (keyLen != key.byteSize()) {
                offset += keyLen + valueLen;
                continue;
            }

            final var currentKey = ssTable.asSlice(offset, keyLen);
            final var currentValue = valueLen == -1 ? null : ssTable.asSlice(offset + keyLen, valueLen);
            offset += valueLen + keyLen;

            if (key.mismatch(currentKey) == -1) {
                return new BaseEntry<>(currentKey, currentValue);
            }
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        segments.put(entry.key(), entry);
    }

    private long calculateSize() {
        long res = 0;
        for (final var entry : segments.values()) {
            long valueLen = entry.value() == null ? 0 : entry.value().byteSize();
            res += 2 * Long.BYTES + entry.key().byteSize() + valueLen;
        }
        return res;
    }

    private long writeSegment(MemorySegment segment, MemorySegment table, long offset) {
        if (segment == null) {
            return offset;
        }

        MemorySegment.copy(segment, 0, table, offset, segment.byteSize());

        return offset + segment.byteSize();
    }

    @Override
    public void close() throws IOException {
        long fileSize = calculateSize();

        final var ssTableClose = assertFile(tablePath, fileSize, FileChannel.MapMode.READ_WRITE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE);

        long offset = 0;
        for (final var entry : segments.values()) {
            ssTableClose.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
            offset += Long.BYTES;
            ssTableClose.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
            offset += Long.BYTES;

            offset = writeSegment(entry.key(), ssTableClose, offset);
            offset = writeSegment(entry.value(), ssTableClose, offset);
        }

        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }

}
