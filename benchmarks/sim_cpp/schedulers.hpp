// SPDX-License-Identifier: Apache-2.0
//
// Two scheduler implementations.
//
//   LegacyScheduler  - per-host greedy first-fit by priority, with optional
//                      alloc-routing enforcement (silos = true).
//
//   PlannerScheduler   - spec-group bucketing, stranding-based placementScore,
//                      priority-keyed persistent reservations with override.
//                      Mirrors cuebot Scheduler.java.
//
// Both expose a tick() that returns a list of (host_id, frame_id) bookings.
// The simulator applies them to the cluster state.

#pragma once

#include "cluster.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace sim {

struct Booking {
    Host*  host;
    Frame* frame;
};

// ---- common helpers -------------------------------------------------------

inline bool fits_on_host_idle(const Layer& L, const Host& h) {
    return h.cores_idle      >= effective_cores(L.cores_min)
        && h.mem_idle_kb     >= L.mem_min_kb
        && h.gpus_idle       >= L.gpus_min
        && h.gpu_mem_idle_kb >= L.gpu_mem_min_kb;
}

inline bool fits_on_host_total(const Layer& L, const Host& h) {
    return static_cast<double>(h.cores_total) >= effective_cores(L.cores_min)
        && h.mem_total_kb     >= L.mem_min_kb
        && h.gpus_total       >= L.gpus_min
        && h.gpu_mem_total_kb >= L.gpu_mem_min_kb;
}

inline bool tags_compatible(const Layer& L, const Host& h) {
    if (L.tags.empty()) return true;
    for (const auto& t : L.tags) if (h.tags.count(t)) return true;
    return false;
}

inline bool os_compatible(const Layer& L, const Host& h) {
    return !L.os.has_value() || (h.os.has_value() && *h.os == *L.os);
}

inline bool alloc_compatible(const Layer& L, const Host& h) {
    if (L.allowed_allocs.empty()) return true;
    return L.allowed_allocs.count(h.alloc) > 0;
}


// ============================================================================
// Legacy: per-host greedy first-fit by priority
// ============================================================================
//
// Single-threaded idealization. For each host with idle capacity, walks the
// priority-ordered candidate set and books the first frame that fits.
// Mirrors what a "perfect" multi-threaded master Cuebot would do without
// the races. Counts every (host, layer) compatibility check as a db_op.

class LegacyScheduler {
 public:
    int64_t db_ops = 0;  // single-writer (the one thread that owns this scheduler)

    // Production CoreUnitDispatcher caps per-(host,job) dispatch at
    // dispatcher.job_frame_dispatch_max (default 8) and total per-host
    // dispatch at dispatcher.host_frame_dispatch_max (default 12). The
    // important property for KSM is that ONE call to dispatchHost(host,
    // job) loops over findNextDispatchFrames(job, host, ...) and books
    // up to job_frame_dispatch_max frames of the SAME job onto the host
    // before moving to the next job. Modeling this packing is what
    // matters for same-host KSM co-location.
    int job_frame_dispatch_max  = 8;
    int host_frame_dispatch_max = 12;

    std::vector<Booking> tick(Cluster& c, double now) {
        (void)now;
        std::vector<Booking> out;

        // Collect dispatchable layer/job pairs.
        std::vector<std::pair<Layer*, Job*>> layer_jobs;
        for (auto& j : c.jobs) {
            if (j.paused || j.state != "PENDING") continue;
            for (auto& l : j.layers) {
                if (l.waiting_frame_count() > 0)
                    layer_jobs.emplace_back(&l, &j);
            }
        }
        // Sort by priority desc, ts_started asc.
        std::sort(layer_jobs.begin(), layer_jobs.end(),
                  [](const auto& a, const auto& b) {
                      if (a.second->priority != b.second->priority)
                          return a.second->priority > b.second->priority;
                      return a.second->ts_started < b.second->ts_started;
                  });

        // For each host, walk the priority-ordered candidate list and dispatch
        // up to job_frame_dispatch_max frames of the SAME (layer, job) before
        // moving on. Once the host has booked host_frame_dispatch_max frames
        // total across any jobs, stop. Mirrors production CoreUnitDispatcher
        // dispatchHost -> dispatchHost(host, job) -> findNextDispatchFrames
        // pattern (CoreUnitDispatcher.java:135-321).
        for (auto& h : c.hosts) {
            if (h.cores_idle <= 0) continue;
            int host_booked = 0;
            for (const auto& [layer, job] : layer_jobs) {
                ++db_ops;
                if (host_booked >= host_frame_dispatch_max)        break;
                if (h.cores_idle <= 0)                              break;
                if (!alloc_compatible(*layer, h)) continue;
                if (!tags_compatible(*layer, h))  continue;
                if (!os_compatible(*layer, h))    continue;
                if (!fits_on_host_idle(*layer, h)) continue;
                if (job->cores_in_use + layer->cores_min > job->max_cores) continue;
                Show& show = c.show_of(layer->show_id);
                if (show.cores_in_use + layer->cores_min > show.burst_cores) continue;

                // Pack up to job_frame_dispatch_max frames of this (layer, job)
                // onto h before moving to the next candidate.
                int job_booked = 0;
                while (job_booked < job_frame_dispatch_max
                       && host_booked < host_frame_dispatch_max
                       && h.cores_idle > 0
                       && fits_on_host_idle(*layer, h)
                       && job->cores_in_use  + layer->cores_min <= job->max_cores
                       && show.cores_in_use  + layer->cores_min <= show.burst_cores) {
                    Frame* f = layer->next_waiting_frame();
                    if (!f) break;
                    out.push_back({&h, f});
                    h.cores_idle      -= effective_cores(layer->cores_min);
                    h.mem_idle_kb     -= layer->mem_min_kb;
                    h.gpus_idle       -= layer->gpus_min;
                    h.gpu_mem_idle_kb -= layer->gpu_mem_min_kb;
                    job->cores_in_use  += layer->cores_min;
                    show.cores_in_use  += layer->cores_min;
                    f->state           = FrameState::RUNNING;
                    ++job_booked;
                    ++host_booked;
                }
            }
        }
        return out;
    }
};


// ============================================================================
// Rust: per-layer first-fit, packs onto least-idle host (core_saturation)
// ============================================================================
//
// Models the new Rust scheduler in rust/crates/scheduler/ on master.
// HostBookingStrategy::core_saturation defaults to true: the B-tree of
// hosts keyed by idle cores is walked forward (ascending) so we pick the
// host with the FEWEST idle cores that still fits — i.e., we pack onto
// already-partially-loaded hosts. core_saturation=false would scan in
// reverse (most-idle-first, spreading).
//
//   - per-layer iteration (not per-host like Legacy)
//   - least-idle-fits host selection (packs)
//   - score-free first-fit on cores+mem+gpu
//   - no placement score, no reservations, no backfill
// Counts ONE scheduler op per booking attempt — the real scheduler pulls
// a single host candidate out of an in-memory B-tree (see
// pipeline/matcher.rs process_layer + host_cache CheckOut). The inner
// host scan that follows in this sim is an in-memory stand-in for the
// B-tree walk; it does not hit the DB and is not charged per host.

class RustScheduler {
 public:
    int64_t db_ops = 0;
    bool    core_saturation = true;   // mirrors HostBookingStrategy default
    // Production caps mirrored from rust/crates/scheduler/src/config/mod.rs:
    //   dispatch_frames_per_layer_limit = 20  (max frames per dispatch_inner call)
    //   host_candidate_attempts_per_layer = 10 (max hosts per process_layer call)
    // The 20-frame cap is what bounds each "batch" placed onto a single host
    // before the host is checked back into the B-tree and the next CheckOut
    // is made.
    int dispatch_frames_per_layer_limit  = 20;
    int host_candidate_attempts_per_layer = 10;

    std::vector<Booking> tick(Cluster& c, double now) {
        (void)now;
        std::vector<Booking> out;

        std::vector<std::pair<Layer*, Job*>> layer_jobs;
        for (auto& j : c.jobs) {
            if (j.paused || j.state != "PENDING") continue;
            for (auto& l : j.layers) {
                if (l.waiting_frame_count() > 0)
                    layer_jobs.emplace_back(&l, &j);
            }
        }
        // Production Rust orders dispatchable jobs by int_priority DESC
        // (see rust/crates/scheduler/src/dao/job_dao.rs ORDER BY clause),
        // with ts_started as a stable secondary key.
        std::sort(layer_jobs.begin(), layer_jobs.end(),
                  [](const auto& a, const auto& b) {
                      if (a.second->priority != b.second->priority)
                          return a.second->priority > b.second->priority;
                      return a.second->ts_started < b.second->ts_started;
                  });

        // Round-robin between layers, one batch per layer per round, to model
        // parallel matcher actors (matcher.rs process_layer) competing for the
        // same B-tree. Without round-robin a single-threaded sim would have
        // one layer monopolize the least-idle-fits host indefinitely, which
        // is unrealistic for the production deployment that runs many layers
        // through process_layer concurrently. Each batch places up to
        // dispatch_frames_per_layer_limit frames on a single host before the
        // next layer gets a turn. A layer makes at most
        // host_candidate_attempts_per_layer batches per tick (matches the
        // attempts cap inside process_layer).
        std::unordered_map<Layer*, int> attempts_used;
        bool progress = true;
        while (progress) {
            progress = false;
            for (const auto& [layer, job] : layer_jobs) {
                if (layer->waiting_frame_count() <= 0) continue;
                if (attempts_used[layer] >= host_candidate_attempts_per_layer) continue;
                Show& show = c.show_of(layer->show_id);
                if (job->cores_in_use + layer->cores_min > job->max_cores)   continue;
                if (show.cores_in_use + layer->cores_min > show.burst_cores) continue;

                // One scheduler op per process_layer attempt (one B-tree
                // CheckOut). The host scan below is the in-memory stand-in
                // for the tree walk and is not charged per host.
                ++db_ops;
                ++attempts_used[layer];

                Host* best         = nullptr;
                double best_cores  = core_saturation
                                       ?  std::numeric_limits<double>::infinity()
                                       : -std::numeric_limits<double>::infinity();
                for (auto& h : c.hosts) {
                    if (!alloc_compatible(*layer, h)) continue;
                    if (!tags_compatible(*layer, h))  continue;
                    if (!os_compatible(*layer, h))    continue;
                    if (!fits_on_host_idle(*layer, h)) continue;
                    if (core_saturation) {
                        if (h.cores_idle < best_cores) { best_cores = h.cores_idle; best = &h; }
                    } else {
                        if (h.cores_idle > best_cores) { best_cores = h.cores_idle; best = &h; }
                    }
                }
                if (!best) continue;

                // Pack up to dispatch_frames_per_layer_limit frames of this
                // layer onto the selected host. This is the dispatch_inner
                // frame loop in pipeline/dispatcher/actor.rs:259.
                int batch_placed = 0;
                while (batch_placed < dispatch_frames_per_layer_limit
                       && layer->waiting_frame_count() > 0
                       && fits_on_host_idle(*layer, *best)
                       && job->cores_in_use  + layer->cores_min <= job->max_cores
                       && show.cores_in_use  + layer->cores_min <= show.burst_cores) {
                    Frame* f = layer->next_waiting_frame();
                    if (!f) break;
                    out.push_back({best, f});
                    best->cores_idle      -= effective_cores(layer->cores_min);
                    best->mem_idle_kb     -= layer->mem_min_kb;
                    best->gpus_idle       -= layer->gpus_min;
                    best->gpu_mem_idle_kb -= layer->gpu_mem_min_kb;
                    job->cores_in_use     += layer->cores_min;
                    show.cores_in_use     += layer->cores_min;
                    f->state              = FrameState::RUNNING;
                    ++batch_placed;
                    progress = true;
                }
            }
        }
        return out;
    }
};


// ============================================================================
// Planner: group + stranding score + persistent reservations
// ============================================================================

inline constexpr double W_CORES   = 1.0;
inline constexpr double W_MEM     = 1.0;
inline constexpr double W_GPUS    = 4.0;
inline constexpr double W_GPU_MEM = 1.0;
inline constexpr double KB_PER_GB = 1024.0 * 1024.0;

struct HostSpecKey {
    std::string alloc;
    std::string tags_normalized;
    std::string os;
    bool        has_gpu;

    bool operator==(const HostSpecKey& o) const {
        return has_gpu == o.has_gpu
            && alloc == o.alloc
            && tags_normalized == o.tags_normalized
            && os == o.os;
    }
};

struct HostSpecKeyHash {
    size_t operator()(const HostSpecKey& k) const noexcept {
        size_t h = std::hash<std::string>{}(k.alloc);
        h ^= std::hash<std::string>{}(k.tags_normalized) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= std::hash<std::string>{}(k.os)              + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= std::hash<bool>{}(k.has_gpu)                + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return h;
    }
};

inline std::string normalize_tags(const std::set<std::string>& tags) {
    std::string out;
    for (const auto& t : tags) {
        if (!out.empty()) out += ' ';
        out += t;
    }
    return out;
}

inline HostSpecKey spec_key_of(const Host& h) {
    return HostSpecKey{
        h.alloc,
        normalize_tags(h.tags),
        h.os.value_or(std::string()),
        h.gpus_total > 0 || h.gpu_mem_total_kb > 0,
    };
}

struct Reservation {
    std::string layer_id;
    int         priority;
    // Time at which the host's existing procs are expected to clear, PINNED
    // at the moment the reservation is claimed. EASY backfill compares
    // candidate layer finish times against this fixed horizon. Recomputing
    // it live from the host's running list lets each backfilled frame
    // extend the horizon, which admits the next backfill in a livelock-
    // style failure where the reserved layer never gets the host. The
    // Mu'alem & Feitelson (2001) EASY invariant requires the reservation's
    // promised release time to be fixed at claim time.
    double      release_at;
};


class PlannerScheduler {
 public:
    int64_t db_ops = 0;  // single-writer (the one thread that owns this scheduler)

    // Optional: in silos mode the planner can be told to IGNORE alloc routing
    // (treat the heterogeneous fleet as one pool). When false (default), the
    // spec-key already includes alloc and groups follow.
    bool ignore_allocs = false;

    // Optional: model the production Scheduler.java reservation semantics --
    // a strict gate with no EASY backfill. With strict_reservations=true,
    // reservation_allows returns false for any non-mine, non-lower-priority
    // reservation regardless of frame runtime. Useful for comparing the
    // capacity cost of strict reservations vs the EASY-backfill variant.
    bool strict_reservations = false;

    int candidates_per_group_max = 2000;

    std::unordered_map<std::string, Reservation> reservations;  // host_id -> claim
    // Number of consecutive ticks each layer has appeared in the candidate
    // set without dispatching anything. Reservations are only claimed for
    // layers whose counter exceeds RESERVATION_DELAY_TICKS; layers that
    // would have dispatched naturally on the next tick don't trigger
    // reservation at all.
    std::unordered_map<std::string, int> consecutive_unfit;
    static constexpr int RESERVATION_DELAY_TICKS = 2;

    // Set at the top of tick() so helpers can do EASY backfill admission
    // and runtime-aware target picking. Valid only during a tick.
    Cluster* current_cluster_ = nullptr;
    double   current_now_     = 0.0;
    // Per-tick cache: predicted free time per host, computed once at the
    // top of tick(). Avoids the O(jobs * layers * procs) cost of recomputing
    // it on every (layer, host) admission check during the inner dispatch
    // loop.
    std::unordered_map<std::string, double> predicted_free_cache_;

    std::vector<Booking> tick(Cluster& c, double now) {
        current_cluster_ = &c;
        current_now_     = now;
        std::vector<Booking> out;

        // ---- 0) Build per-tick lookup caches ----------------------------
        // (a) layer_id -> Layer*, so predicted_free_time avoids the
        //     O(jobs * layers) scan in layer_of() for every running proc.
        // (b) host_id -> predicted free time, so reservation_allows and
        //     pick_target don't recompute it on every admission check.
        std::unordered_map<std::string, Layer*> layer_by_id;
        for (auto& j : c.jobs)
            for (auto& l : j.layers)
                layer_by_id[l.layer_id] = &l;
        predicted_free_cache_.clear();
        predicted_free_cache_.reserve(c.hosts.size());
        for (auto& h : c.hosts) {
            double t = current_now_;
            for (Frame* f : h.running) {
                auto it = layer_by_id.find(f->layer_id);
                if (it == layer_by_id.end()) continue;
                double est_finish = f->start_time + it->second->runtime_median_s;
                if (est_finish > t) t = est_finish;
            }
            predicted_free_cache_[h.host_id] = t;
        }

        // ---- 1) dispatchable layer/job pairs --------------------------
        std::vector<std::pair<Layer*, Job*>> all;
        for (auto& j : c.jobs) {
            if (j.paused || j.state != "PENDING") continue;
            for (auto& l : j.layers)
                if (l.waiting_frame_count() > 0)
                    all.emplace_back(&l, &j);
        }
        if (all.empty()) { reservations.clear(); current_cluster_ = nullptr; return out; }

        // ---- 2) group hosts by spec ----------------------------------
        std::unordered_map<HostSpecKey, std::vector<Host*>, HostSpecKeyHash> groups;
        if (ignore_allocs) {
            // Single bucket regardless of alloc. (Useful for "Planner without silos".)
            for (auto& h : c.hosts)
                groups[HostSpecKey{"<unified>", "", "", false}].push_back(&h);
        } else {
            for (auto& h : c.hosts) groups[spec_key_of(h)].push_back(&h);
        }

        std::set<std::string> seen_layer_ids;

        // ---- 3) per-group dispatch + reconcile -----------------------
        for (auto& [spec, hosts] : groups) {
            ++db_ops;  // one "candidate query" per group

            // Build the per-group candidate set. When ignore_allocs=false
            // the group is homogeneous (same alloc/tags/os) and a single
            // proto-host check is exact. When ignore_allocs=true the group
            // is the whole heterogeneous fleet, so the proto-host check is
            // invalid (e.g. proto is an elk and would falsely reject any
            // 'highend' layer). In that case admit a layer if SOME host in
            // the group is compatible; the inner dispatch loop and
            // pick_target enforce the per-host match.
            int max_cores_total = 0;
            for (Host* h : hosts) max_cores_total = std::max(max_cores_total, h->cores_total);

            auto any_host_compatible = [&](const Layer& L) {
                for (Host* h : hosts) {
                    if (!ignore_allocs && !alloc_compatible(L, *h)) continue;
                    if (!tags_compatible(L, *h))                    continue;
                    if (!os_compatible(L, *h))                      continue;
                    return true;
                }
                return false;
            };

            std::vector<std::pair<Layer*, Job*>> cands;
            cands.reserve(64);
            if (ignore_allocs) {
                for (auto& [l, j] : all) {
                    if (l->cores_min > max_cores_total) continue;
                    if (!any_host_compatible(*l))      continue;
                    cands.emplace_back(l, j);
                }
            } else {
                Host* proto = hosts.front();
                for (auto& [l, j] : all) {
                    if (!alloc_compatible(*l, *proto)) continue;
                    if (!tags_compatible(*l, *proto))  continue;
                    if (!os_compatible(*l, *proto))    continue;
                    if (l->cores_min > max_cores_total) continue;
                    cands.emplace_back(l, j);
                }
            }
            std::sort(cands.begin(), cands.end(), [](const auto& a, const auto& b) {
                if (a.second->priority != b.second->priority)
                    return a.second->priority > b.second->priority;
                return a.second->ts_started < b.second->ts_started;
            });
            if ((int)cands.size() > candidates_per_group_max)
                cands.resize(candidates_per_group_max);

            for (auto& [layer, job] : cands) {
                seen_layer_ids.insert(layer->layer_id);
                Show& show = c.show_of(layer->show_id);
                if (job->cores_in_use  + layer->cores_min > job->max_cores)   continue;
                if (show.cores_in_use  + layer->cores_min > show.burst_cores) continue;

                int dispatched_for_layer = 0;

                // dispatch loop, best-fit on stranding score with reservation rule
                while (true) {
                    Host* best        = nullptr;
                    double best_score = std::numeric_limits<double>::infinity();
                    for (Host* h : hosts) {
                        // Per-host tag/os/alloc check: when ignore_allocs=true
                        // the group is heterogeneous and the per-group
                        // pre-filter only guarantees SOMEONE matches, not
                        // every host. Without this we'd happily try to land
                        // a 'highend' frame on an elk.
                        if (ignore_allocs) {
                            if (!tags_compatible(*layer, *h)) continue;
                            if (!os_compatible(*layer, *h))   continue;
                        }
                        if (!reservation_allows(*h, *layer, job->priority)) continue;
                        if (!fits_on_host_idle(*layer, *h))                 continue;
                        double s = placement_score(*h, *layer, *job, show);
                        if (s < best_score) { best_score = s; best = h; }
                    }
                    if (!best) break;

                    Frame* f = layer->next_waiting_frame();
                    if (!f) break;
                    out.push_back({best, f});

                    best->cores_idle      -= effective_cores(layer->cores_min);
                    best->mem_idle_kb     -= layer->mem_min_kb;
                    best->gpus_idle       -= layer->gpus_min;
                    best->gpu_mem_idle_kb -= layer->gpu_mem_min_kb;
                    job->cores_in_use     += layer->cores_min;
                    show.cores_in_use     += layer->cores_min;
                    f->state              = FrameState::RUNNING;
                    ++dispatched_for_layer;

                    auto it = reservations.find(best->host_id);
                    if (it != reservations.end() && it->second.priority < job->priority) {
                        // Higher-priority layer just dispatched onto a host that
                        // was reserved for someone lower-priority: re-claim it
                        // for ourselves. Use the per-tick cached free time so
                        // the new horizon excludes the frame we just placed
                        // (the cache is built once at the top of tick()).
                        it->second = Reservation{
                            layer->layer_id, job->priority, predicted_free_time(*best)};
                    }

                    if (job->cores_in_use  + layer->cores_min > job->max_cores)   break;
                    if (show.cores_in_use  + layer->cores_min > show.burst_cores) break;
                }

                // Update "blocked for N consecutive ticks" counter for this
                // layer. Lazy reservation: dispatching anything resets it;
                // failing to dispatch increments it. reconcile only claims
                // new reservations once the counter exceeds the threshold.
                int& streak = consecutive_unfit[layer->layer_id];
                if (dispatched_for_layer > 0) streak = 0;
                else                          streak += 1;

                reconcile(*layer, hosts, c.hosts, *job, streak);
            }
        }

        // ---- 4) orphan sweep -----------------------------------------
        for (auto it = reservations.begin(); it != reservations.end(); ) {
            if (seen_layer_ids.count(it->second.layer_id) == 0)
                it = reservations.erase(it);
            else
                ++it;
        }
        for (auto it = consecutive_unfit.begin(); it != consecutive_unfit.end(); ) {
            if (seen_layer_ids.count(it->first) == 0)
                it = consecutive_unfit.erase(it);
            else
                ++it;
        }

        current_cluster_ = nullptr;
        return out;
    }

 private:
    // Time at which the host's last currently-running proc is expected to
    // finish, based on each proc's layer.runtime_median_s. Cached per tick
    // (see top of tick()). Returns current_now_ for hosts with no procs.
    double predicted_free_time(const Host& h) const {
        auto it = predicted_free_cache_.find(h.host_id);
        return it != predicted_free_cache_.end() ? it->second : current_now_;
    }

    bool reservation_allows(const Host& h, const Layer& L, int priority) {
        auto it = reservations.find(h.host_id);
        if (it == reservations.end()) return true;
        const auto& r = it->second;
        if (r.layer_id == L.layer_id) return true;
        if (r.priority < priority)    return true;     // override lower priority
        // strict mode mirrors production Scheduler.java: no EASY backfill,
        // reserved hosts sit idle until the reserved layer claims them.
        if (strict_reservations) return false;
        // EASY backfill (Mu'alem & Feitelson 2001): admit a frame onto a
        // reserved host iff the frame will finish before the reservation's
        // PINNED release horizon. release_at was set when the reservation
        // was claimed and does NOT move as new procs land — that's what
        // stops a stream of small backfills from extending the horizon
        // indefinitely and starving the reserved layer.
        double layer_finish_at = current_now_ + L.runtime_median_s;
        return layer_finish_at <= r.release_at;
    }

    int64_t compute_max_more(const Host& h, const Layer& L, const Job& job, const Show& show) {
        // Stranding score uses NOMINAL cores. Oversubscription belongs in the
        // fit check; mixing it into the score creates fractional-remainder
        // bias that pushes small frames toward big hosts.
        int64_t rem_c  = static_cast<int64_t>(h.cores_idle)    - L.cores_min;
        int64_t rem_m  = static_cast<int64_t>(h.mem_idle_kb)   - L.mem_min_kb;
        int64_t rem_g  = static_cast<int64_t>(h.gpus_idle)     - L.gpus_min;
        int64_t rem_gm = static_cast<int64_t>(h.gpu_mem_idle_kb)- L.gpu_mem_min_kb;

        int64_t mm = std::numeric_limits<int64_t>::max();
        if (L.cores_min > 0) {
            mm = std::min(mm, rem_c / L.cores_min);
            int64_t jr = static_cast<int64_t>(job.max_cores) - job.cores_in_use - L.cores_min;
            if (jr < 0) jr = 0;
            mm = std::min(mm, jr / L.cores_min);
            int64_t sr = static_cast<int64_t>(show.burst_cores) - show.cores_in_use - L.cores_min;
            if (sr < 0) sr = 0;
            mm = std::min(mm, sr / L.cores_min);
        }
        if (L.mem_min_kb     > 0) mm = std::min(mm, rem_m  / L.mem_min_kb);
        if (L.gpus_min       > 0) mm = std::min(mm, rem_g  / L.gpus_min);
        if (L.gpu_mem_min_kb > 0) mm = std::min(mm, rem_gm / L.gpu_mem_min_kb);
        if (mm == std::numeric_limits<int64_t>::max()) mm = 0;
        return std::max<int64_t>(0, mm);
    }

    double placement_score(const Host& h, const Layer& L, const Job& job, const Show& show) {
        int64_t mm     = compute_max_more(h, L, job, show);
        int64_t rem_c  = static_cast<int64_t>(h.cores_idle)    - L.cores_min;
        int64_t rem_m  = static_cast<int64_t>(h.mem_idle_kb)   - L.mem_min_kb;
        int64_t rem_g  = static_cast<int64_t>(h.gpus_idle)     - L.gpus_min;
        int64_t rem_gm = static_cast<int64_t>(h.gpu_mem_idle_kb)- L.gpu_mem_min_kb;

        double sc  = (L.cores_min     > 0) ? double(rem_c  - mm * L.cores_min)                          : 0.0;
        double sm  = (L.mem_min_kb    > 0) ? double(rem_m  - mm * L.mem_min_kb)   / KB_PER_GB           : 0.0;
        double sg  = (L.gpus_min      > 0) ? double(rem_g  - mm * L.gpus_min)                           : 0.0;
        double sgm = (L.gpu_mem_min_kb > 0)? double(rem_gm - mm * L.gpu_mem_min_kb)/ KB_PER_GB          : 0.0;

        return W_CORES * sc + W_MEM * sm + W_GPUS * sg + W_GPU_MEM * sgm;
    }

    void reconcile(Layer& L, std::vector<Host*>& group_hosts,
                    std::vector<Host>& all_hosts, const Job& job,
                    int consecutive_unfit_ticks) {
        std::vector<std::string> mine;
        for (const auto& [host_id, r] : reservations)
            if (r.layer_id == L.layer_id) mine.push_back(host_id);

        // Hosts needed = ceil(waiting_frames * layer.cores_min / target_host_total_cores).
        // Each reserved host can run multiple concurrent frames of this layer as
        // its existing procs free up cores, so the number of reservations is
        // bounded by host capacity, not by frame count. Use the largest fitting
        // host in the WHOLE CLUSTER as the divisor (optimistic; if reality lands
        // on smaller hosts, the next tick's reconcile claims more). Computing
        // it globally rather than per-group prevents the reconcile from
        // stacking reservations when a layer fits multiple groups.
        int max_target_cores = 0;
        for (auto& h : all_hosts) {
            if (!fits_on_host_total(L, h)) continue;
            if (h.cores_total > max_target_cores) max_target_cores = h.cores_total;
        }
        int frames_per_host = 1;
        if (max_target_cores > 0 && L.cores_min > 0)
            frames_per_host = std::max(1, max_target_cores / L.cores_min);
        int waiting = L.waiting_frame_count();
        int need = (waiting + frames_per_host - 1) / frames_per_host;   // ceil
        if (need < 0) need = 0;
        int have = static_cast<int>(mine.size());

        // ALWAYS drop excess reservations (e.g., waiting frame count fell
        // because we dispatched some).
        if (have > need) {
            for (int i = need; i < have; ++i) reservations.erase(mine[i]);
            return;
        }
        // Claim more only after the layer has failed to dispatch for at
        // least RESERVATION_DELAY_TICKS consecutive ticks. Layers that
        // would have dispatched on the next tick anyway don't trigger
        // any reservation, which avoids holding hosts idle for transient
        // unfit situations.
        if (have < need && consecutive_unfit_ticks >= RESERVATION_DELAY_TICKS) {
            int want = need - have;
            for (int k = 0; k < want; ++k) {
                Host* t = pick_target(L, group_hosts, job);
                if (!t) break;
                reservations[t->host_id] = Reservation{
                    L.layer_id, job.priority, predicted_free_time(*t)};
            }
        }
    }

    // Pick the host whose currently-running procs are predicted to finish
    // soonest, among hosts that could fit the layer when fully idle and
    // whose reservation (if any) allows us. EASY backfill: reservation
    // wait time is minimized when the target host's existing procs finish
    // earliest.
    Host* pick_target(const Layer& L, std::vector<Host*>& hosts, const Job& job) {
        Host*  best          = nullptr;
        double best_free_at  = std::numeric_limits<double>::infinity();
        for (Host* h : hosts) {
            // Same per-host filter rationale as in the dispatch loop: in
            // ignore_allocs mode the group is heterogeneous so we must
            // check tags/os per host before considering it as a reservation
            // target. Otherwise a 'highend' layer could reserve an elk.
            if (ignore_allocs) {
                if (!tags_compatible(L, *h)) continue;
                if (!os_compatible(L, *h))   continue;
            }
            if (!fits_on_host_total(L, *h))                            continue;
            if (!reservation_allows(*h, L, job.priority))              continue;
            double t = predicted_free_time(*h);
            if (t < best_free_at) {
                best_free_at = t;
                best         = h;
            }
        }
        return best;
    }
};

}  // namespace sim
