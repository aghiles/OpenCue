
package com.imageworks.spcue.dispatcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

/**
 * The class visit picks what the scan of the group picks. Random groups whose hosts share a few
 * states and hold unique ones now and then, with warm, seated and reserved hosts among them, and
 * random candidates with and without host-based license pools: the best host, the soft cap's
 * fallback and their strand-free frames from pickHost over the classes' representatives and the
 * candidate's special hosts equal those from pickHost over every host, slot after slot, as the
 * picked host takes a frame and moves class.
 */
public class HostClassesTests {

    @Test
    public void theClassVisitPicksWhatTheScanPicks() {
        Random rnd = new Random(41);
        for (int group = 0; group < 300; group++) {
            List<Scheduler.BookableHost> hosts = new ArrayList<>();
            for (int i = 0; i < 1 + rnd.nextInt(40); i++)
                hosts.add(Fix.randomHost(rnd, i));
            List<Scheduler.LayerCandidate> candidates = new ArrayList<>();
            for (int i = 0; i < 1 + rnd.nextInt(8); i++)
                candidates.add(Fix.randomCandidate(rnd, i));
            Set<String> seated = new HashSet<>();
            for (int i = 0; i < rnd.nextInt(4); i++)
                seated.add("host" + rnd.nextInt(hosts.size() + 4));
            List<LicenseSource.LicenseBudget> pools = Arrays.asList(new LicenseSource.LicenseBudget(
                    "lic", true, 0, seated.size() + rnd.nextInt(2), seated, false));
            Map<String, Set<String>> licenseSeats = new HashMap<>();
            licenseSeats.put("lic", new HashSet<>(seated));

            Scheduler s = new Scheduler();
            s.layerHostMaxFrac = rnd.nextBoolean() ? 0.25 : 0;
            s.bindWaiting(candidates);
            s.reachNeeds = Scheduler.reachNeedsOf(candidates);
            HostClasses classes = new HostClasses(hosts);
            for (int slot = 0; slot < 25; slot++) {
                Scheduler.LayerCandidate c = candidates.get(rnd.nextInt(candidates.size()));
                List<LicenseSource.LicenseBudget> cPools = c.licenses == null ? null : pools;
                Scheduler.Pick scan = s.pickHost(c, hosts, cPools, licenseSeats);
                Scheduler.Pick visit = s.pickHost(c, classes.candidates(c, cPools, licenseSeats),
                        cPools, licenseSeats);
                assertSame(scan.best, visit.best);
                assertSame(scan.fallback, visit.fallback);
                assertEquals(scan.bestStrandFree, visit.bestStrandFree);
                assertEquals(scan.fallbackStrandFree, visit.fallbackStrandFree);
                Scheduler.BookableHost best = scan.best != null ? scan.best : scan.fallback;
                if (best == null)
                    continue;
                // One frame lands as placeOnce would book it: resources, plan, frames, class.
                for (Scheduler.Dim d : Scheduler.Dim.values())
                    d.take(best, d.need(c));
                best.planned.put(c.layerId, new int[] {0, 1});
                best.layerFrames.merge(c.layerId, 1, Integer::sum);
                classes.move(best);
            }
        }
    }
}
