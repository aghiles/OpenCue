
package com.imageworks.spcue.dispatcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

/**
 * The cap cells' invariant: every cap counter is tick-wide, seeded once from the first row seen,
 * and every later read sees every earlier spend, in the same group or a later one.
 */
public class CapCellTests {

    private static Scheduler.LayerCandidate candidate(String layer, String job, String show,
            int jobInUse, int jobMax, String limit, int limitRunning, int limitMax,
            String folder, int folderMax, int folderRunning, List<String> licenses) {
        Scheduler.LayerCandidate c = new Scheduler.LayerCandidate();
        c.layerId = layer;
        c.jobId = job;
        c.showId = show;
        c.showKey = show + "@alloc";
        c.layerCoresMin = 100;
        c.jobCoresInUse = jobInUse;
        c.jobMaxCores = jobMax;
        c.showCoresInUse = 0;
        c.showBurstCores = Integer.MAX_VALUE;
        c.showSizeCores = 100;
        c.limitId = limit;
        c.limitRunning = limitRunning;
        c.limitMax = limitMax;
        c.folderId = folder;
        c.folderMax = folderMax;
        c.folderRunning = folderRunning;
        c.licenses = licenses;
        c.waitingFrameCount = 10;
        return c;
    }

    @Test
    public void candidatesOfOneKeyShareOneCellSeededFromTheFirstRow() {
        Scheduler s = new Scheduler();
        Scheduler.LayerCandidate a = candidate("a", "job", "show", 300, 500, "lim", 2, 5, "f", 1000, 200, List.of("nuke"));
        Scheduler.LayerCandidate b = candidate("b", "job", "show", 300, 500, "lim", 2, 5, "f", 1000, 200, List.of("nuke", "maya"));
        s.bindCells(a);
        s.bindCells(b);
        assertSame(a.jobCell, b.jobCell);
        assertSame(a.showCell, b.showCell);
        assertSame(a.limitCell, b.limitCell);
        assertSame(a.folderCell, b.folderCell);
        assertSame(a.licenseCells[0], b.licenseCells[0]);
        assertEquals(300, a.jobCell.used);
        assertEquals(2, a.limitCell.used);
        assertEquals(200, a.folderCell.used);
        assertEquals(0, b.licenseCells[1].used);
        // A spend on a is a spend on b: b's job cap closes when a takes the last 200 cores.
        assertTrue(Scheduler.openToPlace(b));
        a.jobCell.used += 200;
        assertFalse(Scheduler.openToPlace(b));
        // A candidate bound later, as in a later group of the same tick, sees the spend too.
        Scheduler.LayerCandidate c = candidate("c", "job", "show", 300, 500, null, 0, 0, "f", -1, 0, null);
        s.bindCells(c);
        assertSame(a.jobCell, c.jobCell);
        assertEquals(500, c.jobCell.used);
        assertNull(c.limitCell);
        assertNull(c.folderCell);
        assertNull(c.licenseCells);
    }

    @Test
    public void theLimitCellClosesTheCandidateAtItsMax() {
        Scheduler s = new Scheduler();
        Scheduler.LayerCandidate a = candidate("a", "jobA", "show", 0, 1000, "lim", 4, 5, "f", -1, 0, null);
        Scheduler.LayerCandidate b = candidate("b", "jobB", "show", 0, 1000, "lim", 4, 5, "f", -1, 0, null);
        s.bindCells(a);
        s.bindCells(b);
        assertTrue(Scheduler.openToPlace(b));
        a.limitCell.used += 1;
        assertFalse(Scheduler.openToPlace(b));
    }
}
