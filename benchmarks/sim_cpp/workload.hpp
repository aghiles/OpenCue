// SPDX-License-Identifier: Apache-2.0
//
// Workload generator with production-measured constants. Service mix and
// per-service core/memory profiles derived from SPI production telemetry
// (Cue_tasks 2024-01-01 to 2025-05-28, ~313M core-hours across the cluster).
//
// Two modes:
//
//   silos = false (default)
//     Single allocation "A1". Layers carry only their tag requirements.
//
//   silos = true
//     Three allocations: small / mid / big. Each layer's allowed_allocs
//     is set based on frame size, mirroring how real-world operators
//     manually partition their farm. Used to model the OLD operational
//     pattern (pre-cue-layer-man). Note: with the --script flag enabled,
//     silos are no longer needed because the script handles host-class
//     routing via tag rewrites.

#pragma once

#include "cluster.hpp"

#include <cmath>
#include <random>
#include <string>
#include <vector>

namespace sim {

constexpr int64_t GB_KB = static_cast<int64_t>(1024) * 1024;

// ---- production farm: 1,553 hosts, 57,248 cores ---------------------------

struct HostTypeConfig {
    std::string           prefix;
    int                   count;
    int                   cores;
    double                memory_gb;
    std::set<std::string> tags;
    std::string           silo_alloc;
};

// Host tag vocabulary mirrors the real cue-layer-man YAML rules so the
// script's per-service tag rewrites land on the right host class:
//   genhi    -> big-mem hosts (jaime)
//   genmid   -> mid-mem hosts (ram)
//   general  -> any host
//   render   -> any host that can run render workloads
//   midrange / highend  -> host capacity bands (ram / jaime)
//   desktop  -> small-mem hosts (elk; in production these are
//               artist desktops borrowed at night)
//   fx, houdini -> specialty mid-tier (ram)
//   util     -> small hosts (elk)
inline const std::vector<HostTypeConfig> HOSTS_CONFIG = {
    {"elk",   1004,  16, 125.0,
        {"general", "render", "desktop", "util"},                   "small_alloc"},
    {"ram",    303,  32, 251.0,
        {"general", "render", "midrange", "genmid", "fx", "houdini"}, "mid_alloc"},
    {"jaime",  246, 128, 503.0,
        {"general", "render", "highend", "genhi"},                  "big_alloc"},
};

// ---- service mix from production CSV -------------------------------------

struct ServiceConfig {
    std::string name;             // matches cue-layer-man.yml service key
    double      core_hours_share; // fraction of total cluster core-hours
    double      avg_cores;        // mean cores per frame (from CSV "Avg Cores")
    double      avg_mem_gb;       // mean max RSS per frame
};

// Top services by core-hours, covering ~99.4% of cluster work.
// Numbers from Cue_tasks_2024-01-01_2025-05-28.csv.
inline const std::vector<ServiceConfig> SERVICES = {
    {"arnold",       0.919,  6.07,  21.6},
    {"spotless",     0.030,  2.00,   1.1},
    {"nuke",         0.025,  3.91,   7.5},
    {"simulation16", 0.006, 16.26,  13.2},
    {"shell",        0.005,  1.53,   0.6},
    {"simulation8",  0.004,  8.27,  11.5},
    {"simulation32", 0.003, 31.74,  43.0},
    {"simulation4",  0.003,  4.16,   6.0},
    {"houdini",      0.002,  1.53,   4.5},
    {"nukehi",       0.001,  9.85,  14.1},
};

inline const std::vector<int> PRIORITIES = {10, 30, 50, 70, 90};

// ---- cluster build --------------------------------------------------------

inline Cluster build_production_cluster(int seed, double scale, bool silos) {
    Cluster c;
    int next_id = 1;
    for (const auto& cfg : HOSTS_CONFIG) {
        int n = std::max(1, static_cast<int>(std::lround(cfg.count * scale)));
        for (int i = 0; i < n; ++i) {
            Host h;
            char id_buf[32];
            std::snprintf(id_buf, sizeof(id_buf), "host-%05d", next_id);
            h.host_id          = id_buf;
            char name_buf[32];
            std::snprintf(name_buf, sizeof(name_buf), "%s-%04d", cfg.prefix.c_str(), i);
            h.name             = name_buf;
            h.alloc            = silos ? cfg.silo_alloc : std::string("A1");
            h.cores_total      = cfg.cores;
            h.mem_total_kb     = static_cast<int64_t>(cfg.memory_gb * GB_KB);
            h.gpus_total       = 0;
            h.gpu_mem_total_kb = 0;
            h.tags             = cfg.tags;
            h.os               = std::string("rhel7");
            h.cores_idle       = h.cores_total;
            h.mem_idle_kb      = h.mem_total_kb;
            h.gpus_idle        = h.gpus_total;
            h.gpu_mem_idle_kb  = h.gpu_mem_total_kb;
            c.hosts.push_back(std::move(h));
            ++next_id;
        }
    }
    if (silos) {
        c.shows.push_back(Show{"benchmark", "small_alloc", 1'000'000'000, 0});
        c.shows.push_back(Show{"benchmark", "mid_alloc",   1'000'000'000, 0});
        c.shows.push_back(Show{"benchmark", "big_alloc",   1'000'000'000, 0});
    } else {
        c.shows.push_back(Show{"benchmark", "A1", 1'000'000'000, 0});
    }
    return c;
}

// ---- workload generation --------------------------------------------------

struct WorkloadConfig {
    double target_load_fraction  = 0.7;
    int    jobs_per_burst        = 5;
    double arrival_interval_s    = 60.0;
    double simulation_seconds    = 4 * 3600.0;
    double runtime_median_s      = 600.0;
    double runtime_sigma         = 0.9;
};

// Pick a service weighted by its core-hours share.
inline const ServiceConfig& pick_service(std::mt19937_64& rng) {
    std::uniform_real_distribution<double> u(0.0, 1.0);
    double r   = u(rng);
    double acc = 0.0;
    for (const auto& s : SERVICES) {
        acc += s.core_hours_share;
        if (r <= acc) return s;
    }
    return SERVICES.front();
}

inline std::string short_uuid(std::mt19937_64& rng) {
    static const char* hex = "0123456789abcdef";
    std::string s(8, '0');
    std::uniform_int_distribution<int> u(0, 15);
    for (auto& ch : s) ch = hex[u(rng)];
    return s;
}

// Map a service's avg cores to a discrete request count. We round to the
// nearest integer; this is a coarse model but it matches what real layers
// look like after cue-layer-man (or, without it, the original layer
// specification). Pre-script tags are kept simple: {general} so the layer
// can land on any host. The script (if enabled) will rewrite both the
// cores and the tags based on the service's per-service rules.
inline Job generate_job(std::mt19937_64& rng,
                        double ts,
                        int target_cores,
                        double runtime_median_s,
                        double runtime_sigma,
                        bool silos) {
    Job j;
    j.job_id     = std::string("job-") + short_uuid(rng);
    j.show_id    = "benchmark";
    j.priority   = PRIORITIES[std::uniform_int_distribution<int>(
                       0, static_cast<int>(PRIORITIES.size()) - 1)(rng)];
    j.ts_started = ts;

    const ServiceConfig& svc = pick_service(rng);
    int cores      = std::max(1, static_cast<int>(std::lround(svc.avg_cores)));
    int n_frames   = std::max(1, target_cores / cores);

    Layer L;
    L.layer_id        = j.job_id + "-layer-0";
    L.job_id          = j.job_id;
    L.show_id         = j.show_id;
    L.service         = svc.name;
    L.cores_min       = cores;
    L.natural_cores   = cores;   // pre-script demand; script rewrites cores_min only
    L.mem_min_kb      = static_cast<int64_t>(svc.avg_mem_gb * GB_KB);
    L.gpus_min        = 0;
    L.gpu_mem_min_kb  = 0;
    L.tags            = {"general"};   // pre-script; --script may rewrite
    // In silos mode (pre-cue-layer-man pattern) route by memory:
    //   >= 70 GB  -> big_alloc only
    //   >= 20 GB  -> mid or big
    //   else      -> small_alloc
    if (silos) {
        if (svc.avg_mem_gb >= 70.0)       L.allowed_allocs = {"big_alloc"};
        else if (svc.avg_mem_gb >= 20.0)  L.allowed_allocs = {"mid_alloc", "big_alloc"};
        else                              L.allowed_allocs = {"small_alloc", "mid_alloc"};
    }
    L.runtime_median_s = runtime_median_s;
    L.runtime_sigma   = runtime_sigma;

    L.frames.reserve(n_frames);
    for (int i = 0; i < n_frames; ++i) {
        Frame f;
        char id_buf[64];
        std::snprintf(id_buf, sizeof(id_buf), "%s-frame-%04d", L.layer_id.c_str(), i);
        f.frame_id   = id_buf;
        f.layer_id   = L.layer_id;
        f.job_id     = j.job_id;
        f.int_number = i;
        L.frames.push_back(std::move(f));
    }
    j.layers.push_back(std::move(L));
    return j;
}

inline std::vector<Job> generate_arrivals(const WorkloadConfig& cfg,
                                          int64_t total_cluster_cores,
                                          int seed,
                                          bool silos) {
    std::mt19937_64 rng(static_cast<uint64_t>(seed));
    std::vector<Job> out;
    int64_t per_burst = static_cast<int64_t>(cfg.target_load_fraction
                                              * static_cast<double>(total_cluster_cores)
                                              * cfg.arrival_interval_s
                                              / cfg.runtime_median_s);
    int cores_per_job = std::max<int>(1, static_cast<int>(per_burst
                                              / std::max(1, cfg.jobs_per_burst)));
    for (double t = 0.0; t < cfg.simulation_seconds; t += cfg.arrival_interval_s) {
        for (int k = 0; k < cfg.jobs_per_burst; ++k) {
            out.push_back(generate_job(rng, t, cores_per_job,
                                       cfg.runtime_median_s,
                                       cfg.runtime_sigma, silos));
        }
    }
    return out;
}

// ---- runtime sampling -----------------------------------------------------

inline double sample_frame_runtime(std::mt19937_64& rng, const Layer& L) {
    double mu = std::log(L.runtime_median_s);
    std::lognormal_distribution<double> d(mu, L.runtime_sigma);
    return d(rng);
}

}  // namespace sim
