
package com.imageworks.spcue.dispatcher;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

/**
 * The views answer exactly what the scans answered: for every candidate, the first gate no host
 * clears (cores, memory, gpu, or fit), and for every host, whether some waiting candidate fits it.
 * Random groups with CPU and GPU hosts and candidates, compared against the scans kept here.
 */
public class GroupViewsTests {

    /** The scan classifyFragmentation replaced. */
    private static String classifyByScan(Scheduler.LayerCandidate c, List<Scheduler.BookableHost> hosts) {
        boolean anyCores = false;
        boolean anyMem = false;
        for (Scheduler.BookableHost h : hosts) {
            if (h.coresIdle < c.layerCoresMin)
                continue;
            anyCores = true;
            if (h.memIdle < c.layerMemMin)
                continue;
            anyMem = true;
            if (h.gpusIdle >= c.layerGpusMin && h.gpuMemIdle >= c.layerGpuMemMin)
                return "fit";
        }
        if (!anyCores)
            return "cores";
        if (!anyMem)
            return "memory";
        return "gpu";
    }

    /** The double loop strandedWholeCores replaced. */
    private static long strandedByScan(List<Scheduler.BookableHost> hosts,
            List<Scheduler.LayerCandidate> candidates) {
        List<Scheduler.LayerCandidate> waiting = new ArrayList<>();
        for (Scheduler.LayerCandidate c : candidates)
            if (c.waitingFrameCount > 0)
                waiting.add(c);
        if (waiting.isEmpty())
            return 0;
        long strandedCp = 0;
        for (Scheduler.BookableHost h : hosts) {
            if (h.coresIdle < Dispatcher.CORE_POINTS_RESERVED_MIN)
                continue;
            boolean sellable = false;
            for (Scheduler.LayerCandidate c : waiting) {
                if (c.layerCoresMin <= h.coresIdle && c.layerMemMin <= h.memIdle
                        && c.layerGpusMin <= h.gpusIdle && c.layerGpuMemMin <= h.gpuMemIdle) {
                    sellable = true;
                    break;
                }
            }
            if (!sellable)
                strandedCp += h.coresIdle;
        }
        return strandedCp / Scheduler.CORE_POINTS_PER_CORE;
    }

    @Test
    public void theViewsAnswerAsTheScansDid() {
        Random rnd = new Random(29);
        for (int group = 0; group < 300; group++) {
            List<Scheduler.BookableHost> hosts = new ArrayList<>();
            for (int i = 0; i < rnd.nextInt(60); i++)
                hosts.add(Fix.randomHost(rnd, i));
            List<Scheduler.LayerCandidate> candidates = new ArrayList<>();
            for (int i = 0; i < rnd.nextInt(80); i++)
                candidates.add(Fix.randomCandidate(rnd, i));
            GroupViews.IdleView idle = new GroupViews.IdleView(hosts);
            long idleSum = 0;
            int reserved = 0;
            for (Scheduler.BookableHost h : hosts) {
                idleSum += h.coresIdle;
                if (h.reservation != null)
                    reserved++;
            }
            assertEquals(idleSum, idle.idleCoresSum);
            assertEquals(reserved, idle.reserved.size());
            for (Scheduler.LayerCandidate c : candidates)
                assertEquals(classifyByScan(c, hosts), Scheduler.classifyFragmentation(c, idle));
            assertEquals(strandedByScan(hosts, candidates),
                    Scheduler.strandedWholeCores(hosts, candidates));
        }
    }

    /** The cut keeps exactly the candidates some host fits under the placement gate, in order. */
    @Test
    public void theDrawKeepsExactlyTheCandidatesSomeHostFits() {
        Random rnd = new Random(31);
        for (int group = 0; group < 300; group++) {
            List<Scheduler.BookableHost> hosts = new ArrayList<>();
            for (int i = 0; i < rnd.nextInt(60); i++)
                hosts.add(Fix.randomHost(rnd, i));
            List<Scheduler.LayerCandidate> candidates = new ArrayList<>();
            for (int i = 0; i < rnd.nextInt(80); i++)
                candidates.add(Fix.randomCandidate(rnd, i));
            List<Scheduler.LayerCandidate> byScan = new ArrayList<>();
            for (Scheduler.LayerCandidate c : candidates) {
                for (Scheduler.BookableHost h : hosts) {
                    if (Scheduler.fitsOnHost(c, h)) {
                        byScan.add(c);
                        break;
                    }
                }
            }
            assertEquals(byScan, Scheduler.fitting(candidates, new GroupViews.IdleView(hosts)));
        }
    }
}
