
package com.imageworks.spcue.dispatcher;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

/**
 * The strand test's invariant: a resource d that c does not need is protected on h exactly when
 * some other waiting, open candidate needs d and could ever fit h's totals. The needers walk must
 * give the answer of the full linear scan after every step that drains a candidate, closes a cap,
 * reopens one, or changes nothing.
 */
public class StrandNeedersTests {

    /** The full scan the needers walk replaces. */
    private static boolean reference(Scheduler.Dim d, Scheduler.BookableHost h,
            Scheduler.LayerCandidate c, List<Scheduler.LayerCandidate> candidates) {
        for (Scheduler.LayerCandidate o : candidates) {
            if (o == c || o.waitingFrameCount <= 0 || d.need(o) <= 0)
                continue;
            if (Scheduler.hostCanEverFit(o, h) && Scheduler.openToPlace(o))
                return true;
        }
        return false;
    }

    @Test
    public void needersWalkMatchesTheFullScan() {
        Random rnd = new Random(5);
        long gb = 1L << 20;
        for (int group = 0; group < 100; group++) {
            List<Scheduler.LayerCandidate> candidates = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                boolean gpu = rnd.nextInt(10) < 3;
                candidates.add(Fix.candidate("l" + i, 100 * (1 + rnd.nextInt(8)),
                        gb * (1 + rnd.nextInt(32)),
                        gpu ? 1 + rnd.nextInt(2) : 0, gpu ? gb * (1 + rnd.nextInt(8)) : 0,
                        rnd.nextInt(4)));
            }
            List<Scheduler.BookableHost> hosts = new ArrayList<>();
            for (int i = 0; i < 5; i++)
                hosts.add(Fix.host("h" + i, 100 * (4 + rnd.nextInt(12)), gb * (8 + rnd.nextInt(56)),
                        rnd.nextBoolean() ? 2 : 0, gb * 8));
            Scheduler s = new Scheduler();
            s.bindWaiting(candidates);
            for (int step = 0; step < 100; step++) {
                for (Scheduler.Dim d : Scheduler.Dim.values())
                    for (Scheduler.BookableHost h : hosts)
                        for (Scheduler.LayerCandidate c : candidates)
                            assertEquals(reference(d, h, c, candidates), s.wantedOn(d, h, c));
                Scheduler.LayerCandidate o = candidates.get(rnd.nextInt(candidates.size()));
                switch (rnd.nextInt(4)) {
                    case 0: // the candidate drains
                        o.waitingFrameCount = 0;
                        break;
                    case 1: // its job cap closes
                        o.jobCell.used = o.jobMaxCores;
                        break;
                    case 2: // a negative-cores placement reopens it
                        o.jobCell.used = 0;
                        break;
                    default: // nothing changes
                        break;
                }
            }
        }
    }
}
