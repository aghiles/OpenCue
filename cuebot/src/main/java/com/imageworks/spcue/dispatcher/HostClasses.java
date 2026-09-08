
package com.imageworks.spcue.dispatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The hosts of a group in classes of equal state, so a slot's host scan visits one host per class
 * instead of every host. Every fact the scan reads for a (host, candidate) pair is a fact of the
 * host alone (its totals, its idle resources, the layers planned on it this tick and the frames
 * each layer holds on it), a fact of the candidate alone, or one of three facts that name both:
 * the host is warm for the candidate's layer, it holds a seat in one of the candidate's license
 * pools, or it is reserved. Hosts equal in the host facts and clear of the pair facts get the same
 * verdict and the same score for any candidate, so the scan evaluates the lowest-index host of
 * each class and lets it stand for the class; the hosts with a pair fact are evaluated one by one.
 *
 * Bound: O(classes + reserved + warm(c) + seated(c)) per slot, O(log I) per placement.
 */
final class HostClasses {

    /** The host facts the scan reads that do not name a candidate; equal keys, equal verdicts. */
    private static final class Key {
        private final long[] state;
        private final Set<String> planned;
        private final Map<String, Integer> frames;

        Key(Scheduler.BookableHost h) {
            state = new long[] {h.coresTotal, h.memTotal, h.gpusTotal, h.gpuMemTotal, h.coresIdle,
                    h.memIdle, h.gpusIdle, h.gpuMemIdle};
            planned = new HashSet<>(h.planned.keySet());
            frames = new HashMap<>(h.layerFrames);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Key))
                return false;
            Key k = (Key) o;
            return Arrays.equals(state, k.state) && planned.equals(k.planned)
                    && frames.equals(k.frames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(state), planned, frames);
        }
    }

    /** One class: its hosts by index, so its first host is the one a scan of the group meets first. */
    static final class Bucket {
        private final Key key;
        private final TreeSet<Scheduler.BookableHost> members =
                new TreeSet<>(Comparator.comparingInt((Scheduler.BookableHost h) -> h.ix));

        Bucket(Key key) {
            this.key = key;
        }
    }

    private final Map<Key, Bucket> buckets = new HashMap<>();
    private final Map<String, Scheduler.BookableHost> byName = new HashMap<>();
    private final Map<String, List<Scheduler.BookableHost>> warmByLayer = new HashMap<>();
    private final List<Scheduler.BookableHost> reserved = new ArrayList<>();
    private int stamp;

    HostClasses(List<Scheduler.BookableHost> hosts) {
        for (int i = 0; i < hosts.size(); i++) {
            Scheduler.BookableHost h = hosts.get(i);
            h.ix = i;
            h.stamp = 0;
            place(h);
            byName.put(h.hostName.toLowerCase(), h);
            if (h.warmth != null) {
                for (String layerId : h.warmth.keySet())
                    warmByLayer.computeIfAbsent(layerId, k -> new ArrayList<>()).add(h);
            }
            if (h.reservation != null)
                reserved.add(h);
        }
    }

    /** Move h to the class of its current state, after a placement changed it. */
    void move(Scheduler.BookableHost h) {
        Bucket old = h.bucket;
        old.members.remove(h);
        if (old.members.isEmpty())
            buckets.remove(old.key);
        place(h);
    }

    private void place(Scheduler.BookableHost h) {
        Bucket b = buckets.computeIfAbsent(new Key(h), Bucket::new);
        b.members.add(h);
        h.bucket = b;
    }

    /**
     * The hosts a slot for c must evaluate: the lowest-index host of every class that is not special
     * for c, then every special host once: reserved, warm for c's layer, or seated in one of c's
     * host-based pools.
     */
    List<Scheduler.BookableHost> candidates(Scheduler.LayerCandidate c,
            List<LicenseSource.LicenseBudget> pools, Map<String, Set<String>> licenseSeats) {
        stamp++;
        List<Scheduler.BookableHost> specials = new ArrayList<>(reserved.size());
        for (Scheduler.BookableHost h : reserved)
            mark(h, specials);
        for (Scheduler.BookableHost h : warmByLayer.getOrDefault(c.layerId,
                Collections.emptyList()))
            mark(h, specials);
        if (pools != null) {
            for (LicenseSource.LicenseBudget b : pools) {
                for (String name : licenseSeats.get(b.name)) {
                    Scheduler.BookableHost h = byName.get(name);
                    if (h != null)
                        mark(h, specials);
                }
            }
        }
        List<Scheduler.BookableHost> visit = new ArrayList<>(buckets.size() + specials.size());
        for (Bucket b : buckets.values()) {
            for (Scheduler.BookableHost h : b.members) {
                if (h.stamp != stamp) {
                    visit.add(h);
                    break;
                }
            }
        }
        visit.addAll(specials);
        return visit;
    }

    private void mark(Scheduler.BookableHost h, List<Scheduler.BookableHost> specials) {
        if (h.stamp != stamp) {
            h.stamp = stamp;
            specials.add(h);
        }
    }
}
