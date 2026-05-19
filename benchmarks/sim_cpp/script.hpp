// SPDX-License-Identifier: Apache-2.0
//
// cue-layer-man simulation: human-driven pre-scheduling.
//
// At SPI, a Python script (cue-layer-man) runs on cron and rewrites every
// layer's cores_min/cores_max/tags based on a per-show YAML rulebook
// (~50 rules per show, tuned by Noel Eaton over years). The rulebook
// encodes a hidden invariant -- match the frame's mem/core ratio to the
// target host class's mem/core ratio -- which is exactly the E-PVM
// stranding-score minimization, evaluated by hand per (service, mem-tier).
//
// Without this pre-pass, Legacy and Rust are unfairly handicapped in the
// sim because they're being compared to a Planner that has no upstream
// human scheduler doing the placement work for it. With this pre-pass,
// both dispatchers receive the same pre-routed layers they receive in
// production. Planner is run WITHOUT the script -- its E-PVM scoring
// computes per-tick what the script computes per-cron-cycle.
//
// Ruleset below is a stripped-down model of the actual cue-layer-man.yml
// (translated to the simulator's existing tag taxonomy of
// {general, render, midrange, highend}). The (mem-tier -> cores, tags)
// mapping mirrors the real YAML's structure, including the GB/core
// ratio that the PSTs have hand-tuned to ~4-8 GB/core depending on tier.

#pragma once

#include "cluster.hpp"

#include <set>
#include <string>
#include <vector>

namespace sim {

struct ScriptRule {
    double                 mem_min_gb;     // tier threshold
    int                    cores;          // forced cores_min (=cores_max)
    std::set<std::string>  tags;           // tag-rewrite to route to host class
};

// Ordered high-to-low. First matching tier wins.
// Tag mapping to the sim's existing host tag vocabulary:
//   highend  -> jaime-class hosts only
//   midrange -> ram or jaime
//   render   -> elk, ram, or jaime (any render host)
//   general  -> any host
inline const std::vector<ScriptRule>& cue_layer_man_rules() {
    static const std::vector<ScriptRule> rules = {
        {220.0, 64, {"highend"}},
        {140.0, 32, {"highend", "midrange"}},
        { 70.0, 16, {"highend", "midrange"}},
        { 35.0,  8, {"midrange", "render"}},
        { 20.0,  8, {"render"}},
        {  8.0,  4, {"render", "general"}},
        {  0.0,  0, {"general"}},   // cores=0 means "leave cores_min alone"
    };
    return rules;
}

// Apply the cue-layer-man rewrite to a single layer. Uses the layer's
// requested memory as the proxy for what real cue-layer-man computes from
// max(maxRss, minMemory). The sim doesn't track maxRss, so requested
// memory is the only signal we have -- slightly conservative because real
// cue-layer-man bumps tiers up after observed OOMs.
inline void apply_script_to_layer(Layer& L) {
    double layer_mem_gb = static_cast<double>(L.mem_min_kb) / (1024.0 * 1024.0);
    for (const auto& rule : cue_layer_man_rules()) {
        if (layer_mem_gb >= rule.mem_min_gb) {
            if (rule.cores > 0) L.cores_min = rule.cores;
            L.tags = rule.tags;
            return;
        }
    }
}

inline void apply_script_to_arrivals(std::vector<Job>& arrivals) {
    for (auto& j : arrivals) {
        for (auto& L : j.layers) {
            apply_script_to_layer(L);
        }
    }
}

}  // namespace sim
