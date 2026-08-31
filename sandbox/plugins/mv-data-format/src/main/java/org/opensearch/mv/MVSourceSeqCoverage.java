/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Exact source-operation coverage represented as a contiguous floor plus
 * sorted inclusive ranges above that floor. Adjacent ranges are coalesced,
 * and ranges are truncated whenever they make the floor contiguous.
 */
final class MVSourceSeqCoverage {

    static final MVSourceSeqCoverage EMPTY = new MVSourceSeqCoverage(-1L, List.of());

    record Range(long start, long end) {
        Range {
            if (start < 0 || end < start) {
                throw new IllegalArgumentException("invalid source sequence range [" + start + "," + end + "]");
            }
        }
    }

    private final long floor;
    private final List<Range> aboveFloor;

    private MVSourceSeqCoverage(long floor, List<Range> aboveFloor) {
        this.floor = floor;
        this.aboveFloor = List.copyOf(aboveFloor);
    }

    static MVSourceSeqCoverage contiguous(long floor) {
        return floor < 0 ? EMPTY : new MVSourceSeqCoverage(floor, List.of());
    }

    static MVSourceSeqCoverage ofSeqNos(Collection<Long> seqNos) {
        if (seqNos.isEmpty()) {
            return EMPTY;
        }
        List<Range> ranges = seqNos.stream()
            .filter(Objects::nonNull)
            .mapToLong(Long::longValue)
            .filter(seqNo -> seqNo >= 0)
            .distinct()
            .sorted()
            .collect(
                ArrayList<Range>::new,
                (out, seqNo) -> append(out, new Range(seqNo, seqNo)),
                (left, right) -> right.forEach(range -> append(left, range))
            );
        return canonicalize(ranges);
    }

    static MVSourceSeqCoverage ofRanges(Collection<Range> ranges) {
        return canonicalize(new ArrayList<>(ranges));
    }

    long floor() {
        return floor;
    }

    List<Range> aboveFloor() {
        return aboveFloor;
    }

    long maxClaimedSeqNo() {
        return aboveFloor.isEmpty() ? floor : aboveFloor.get(aboveFloor.size() - 1).end();
    }

    boolean contains(long seqNo) {
        if (seqNo < 0) {
            return false;
        }
        if (seqNo <= floor) {
            return true;
        }
        for (Range range : aboveFloor) {
            if (seqNo < range.start()) {
                return false;
            }
            if (seqNo <= range.end()) {
                return true;
            }
        }
        return false;
    }

    MVSourceSeqCoverage union(MVSourceSeqCoverage other) {
        if (this == EMPTY) {
            return other;
        }
        if (other == EMPTY) {
            return this;
        }
        List<Range> ranges = new ArrayList<>(aboveFloor.size() + other.aboveFloor.size() + 2);
        if (floor >= 0) {
            ranges.add(new Range(0, floor));
        }
        ranges.addAll(aboveFloor);
        if (other.floor >= 0) {
            ranges.add(new Range(0, other.floor));
        }
        ranges.addAll(other.aboveFloor);
        ranges.sort(Comparator.comparingLong(Range::start));
        List<Range> merged = new ArrayList<>();
        for (Range range : ranges) {
            append(merged, range);
        }
        return canonicalize(merged);
    }

    MVSourceSeqCoverage intersection(MVSourceSeqCoverage other) {
        List<Range> left = ranges();
        List<Range> right = other.ranges();
        List<Range> common = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < left.size() && j < right.size()) {
            Range a = left.get(i);
            Range b = right.get(j);
            long start = Math.max(a.start(), b.start());
            long end = Math.min(a.end(), b.end());
            if (start <= end) {
                common.add(new Range(start, end));
            }
            if (a.end() < b.end()) {
                i++;
            } else {
                j++;
            }
        }
        return canonicalize(common);
    }

    MVSourceSeqCoverage subtract(MVSourceSeqCoverage other) {
        List<Range> retained = new ArrayList<>();
        List<Range> exclusions = other.ranges();
        int exclusionIndex = 0;
        for (Range source : ranges()) {
            long next = source.start();
            while (exclusionIndex < exclusions.size() && exclusions.get(exclusionIndex).end() < next) {
                exclusionIndex++;
            }
            int i = exclusionIndex;
            boolean exhausted = false;
            while (i < exclusions.size()) {
                Range exclusion = exclusions.get(i);
                if (exclusion.start() > source.end()) {
                    break;
                }
                if (exclusion.start() > next) {
                    retained.add(new Range(next, Math.min(source.end(), exclusion.start() - 1L)));
                }
                if (exclusion.end() >= source.end()) {
                    exhausted = true;
                    break;
                }
                next = exclusion.end() + 1L;
                i++;
            }
            if (exhausted == false && next <= source.end()) {
                retained.add(new Range(next, source.end()));
            }
        }
        return canonicalize(retained);
    }

    List<Range> ranges() {
        List<Range> ranges = new ArrayList<>(aboveFloor.size() + 1);
        if (floor >= 0) {
            ranges.add(new Range(0L, floor));
        }
        ranges.addAll(aboveFloor);
        return ranges;
    }

    MVSourceSeqCoverage through(long bound) {
        if (bound < 0) {
            return EMPTY;
        }
        List<Range> retained = new ArrayList<>();
        if (floor >= 0) {
            retained.add(new Range(0, Math.min(floor, bound)));
        }
        for (Range range : aboveFloor) {
            if (range.start() > bound) {
                break;
            }
            retained.add(new Range(range.start(), Math.min(range.end(), bound)));
        }
        return canonicalize(retained);
    }

    /** Returns the exact complement of this claim in the inclusive range [0, bound]. */
    List<Range> missingThrough(long bound) {
        if (bound < 0) {
            return List.of();
        }
        List<Range> covered = new ArrayList<>(aboveFloor.size() + 1);
        if (floor >= 0) {
            covered.add(new Range(0, Math.min(floor, bound)));
        }
        for (Range range : aboveFloor) {
            if (range.start() > bound) {
                break;
            }
            covered.add(new Range(range.start(), Math.min(range.end(), bound)));
        }
        List<Range> missing = new ArrayList<>();
        long next = 0;
        for (Range range : covered) {
            if (range.end() < next) {
                continue;
            }
            if (range.start() > next) {
                missing.add(new Range(next, range.start() - 1));
            }
            if (range.end() == Long.MAX_VALUE) {
                return List.copyOf(missing);
            }
            next = Math.max(next, range.end() + 1);
            if (next > bound) {
                return List.copyOf(missing);
            }
        }
        if (next <= bound) {
            missing.add(new Range(next, bound));
        }
        return List.copyOf(missing);
    }

    String encode() {
        StringBuilder encoded = new StringBuilder().append(floor).append(';');
        for (int i = 0; i < aboveFloor.size(); i++) {
            if (i > 0) {
                encoded.append(',');
            }
            Range range = aboveFloor.get(i);
            encoded.append(range.start()).append('-').append(range.end());
        }
        return encoded.toString();
    }

    static MVSourceSeqCoverage decode(String encoded) {
        try {
            int separator = encoded.indexOf(';');
            if (separator < 0) {
                return contiguous(Long.parseLong(encoded));
            }
            long floor = Long.parseLong(encoded.substring(0, separator));
            List<Range> ranges = new ArrayList<>();
            String tail = encoded.substring(separator + 1);
            if (tail.isEmpty() == false) {
                for (String token : tail.split(",")) {
                    int dash = token.indexOf('-');
                    ranges.add(new Range(Long.parseLong(token.substring(0, dash)), Long.parseLong(token.substring(dash + 1))));
                }
            }
            List<Range> all = new ArrayList<>(ranges.size() + 1);
            if (floor >= 0) {
                all.add(new Range(0, floor));
            }
            all.addAll(ranges);
            return canonicalize(all);
        } catch (RuntimeException e) {
            return EMPTY;
        }
    }

    void writeTo(StreamOutput out) throws IOException {
        out.writeZLong(floor);
        out.writeVInt(aboveFloor.size());
        for (Range range : aboveFloor) {
            out.writeVLong(range.start());
            out.writeVLong(range.end());
        }
    }

    static MVSourceSeqCoverage readFrom(StreamInput in) throws IOException {
        long floor = in.readZLong();
        int count = in.readVInt();
        List<Range> all = new ArrayList<>(count + 1);
        if (floor >= 0) {
            all.add(new Range(0, floor));
        }
        for (int i = 0; i < count; i++) {
            all.add(new Range(in.readVLong(), in.readVLong()));
        }
        return canonicalize(all);
    }

    private static MVSourceSeqCoverage canonicalize(List<Range> input) {
        if (input.isEmpty()) {
            return EMPTY;
        }
        List<Range> sorted = new ArrayList<>(input);
        sorted.sort(Comparator.comparingLong(Range::start));
        List<Range> merged = new ArrayList<>();
        for (Range range : sorted) {
            append(merged, range);
        }
        long floor = -1L;
        if (merged.isEmpty() == false && merged.get(0).start() == 0) {
            floor = merged.remove(0).end();
        }
        return new MVSourceSeqCoverage(floor, merged);
    }

    private static void append(List<Range> ranges, Range next) {
        if (ranges.isEmpty()) {
            ranges.add(next);
            return;
        }
        int lastIndex = ranges.size() - 1;
        Range last = ranges.get(lastIndex);
        boolean adjacent = last.end() != Long.MAX_VALUE && next.start() == last.end() + 1;
        if (next.start() <= last.end() || adjacent) {
            ranges.set(lastIndex, new Range(last.start(), Math.max(last.end(), next.end())));
        } else {
            ranges.add(next);
        }
    }

    @Override
    public boolean equals(Object other) {
        return this == other
            || (other instanceof MVSourceSeqCoverage coverage && floor == coverage.floor && aboveFloor.equals(coverage.aboveFloor));
    }

    @Override
    public int hashCode() {
        return Objects.hash(floor, aboveFloor);
    }

    @Override
    public String toString() {
        return encode();
    }
}
