// SPDX-License-Identifier: Apache-2.0
//
// cue-layer-man simulation: human-driven pre-scheduling, per-service.
//
// At SPI, a Python script (cue-layer-man) runs on cron and rewrites every
// layer's cores_min/cores_max/tags based on a per-show YAML rulebook.
// Each service has its own ruleset (arnold, nuke, simulation*, shell,
// houdini, spotless, ...). The rulebook encodes a hidden invariant --
// match the frame's mem/core ratio to the target host class's mem/core
// ratio -- which is exactly E-PVM stranding-score minimization, evaluated
// by hand per (service, mem-tier).
//
// Without this pre-pass, Legacy and Rust are unfairly handicapped in the
// sim because they're being compared to a Planner that has no upstream
// human scheduler doing the placement work for it. With this pre-pass,
// both dispatchers receive the same pre-routed layers they receive in
// production. Planner is run WITHOUT the script -- its E-PVM scoring
// computes per-tick what the script computes per-cron-cycle.
//
// Rulesets below are derived from the real cue-layer-man.yml. Services
// not in the map are left untouched (matches real-world behavior:
// unmanaged services keep their original cores_min/tags).

#pragma once

#include "cluster.hpp"

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace sim {

struct ScriptRule {
    double                 mem_min_gb;     // tier threshold
    int                    cores;          // forced cores_min (= cores_max)
    std::set<std::string>  tags;           // tag-rewrite to route to host class
};

// Per-service rulesets. Each list is ordered high-to-low; first matching
// tier wins. Lifted from cue-layer-man.yml (the real production YAML).
inline const std::unordered_map<std::string, std::vector<ScriptRule>>&
cue_layer_man_rulesets() {
    static const std::unordered_map<std::string, std::vector<ScriptRule>> m = {
        // arnold: dominant service (~92% of cluster work). Ruleset escalates
        // memory tiers aggressively because most arnold frames are 20-100 GB.
        {"arnold", {
            {225.0, 64, {"genhi", "general"}},
            {140.0, 32, {"genhi"}},
            { 70.0, 16, {"genhi", "genmid", "desktop"}},
            { 35.0,  8, {"genmid", "desktop"}},
            { 30.0,  8, {"genmid", "general", "desktop"}},
            { 20.0,  8, {"general"}},
            {  0.0,  4, {"general"}},
        }},

        // nuke: comp workloads, mid memory.
        {"nuke", {
            {225.0, 64, {"genhi", "general"}},
            {140.0, 32, {"genhi", "general"}},
            { 70.0, 16, {"genhi", "genmid", "desktop"}},
            { 35.0,  8, {"genmid", "desktop"}},
            { 30.0,  8, {"desktop", "genmid", "util"}},
            { 20.0,  8, {"general", "util"}},
            {  8.0,  4, {"general", "util"}},
            {  0.0,  2, {"general", "util"}},
        }},

        // nukehi: high-mem comp variant.
        {"nukehi", {
            {180.0, 64, {"genhi", "general"}},
            { 90.0, 32, {"genhi", "general"}},
            { 30.0, 16, {"genmid", "desktop"}},
            {  0.0,  8, {"general", "util"}},
        }},

        // shell: light glue work. Stays at 1 core regardless of memory.
        {"shell", {
            { 25.0,  1, {"genmid", "desktop"}},
            {  0.0,  1, {"general"}},
        }},

        // houdini render frames.
        {"houdini", {
            {225.0, 64, {"genhi", "general"}},
            {140.0, 32, {"genhi", "fx", "general"}},
            { 70.0, 16, {"genhi", "genmid", "houdini"}},
            { 35.0,  8, {"genmid", "houdini"}},
            { 16.0,  4, {"general", "houdini"}},
            {  4.0,  2, {"general", "houdini"}},
            {  0.0,  1, {"general", "houdini"}},
        }},

        // simulation: shared base ruleset used by simulation and simulation4.
        {"simulation", {
            {220.0, 64, {"genhi", "general"}},
            {140.0, 32, {"genhi", "fx", "general"}},
            { 70.0, 16, {"genhi", "genmid"}},
            { 40.0,  8, {"genmid", "houdini"}},
            {  0.0,  4, {"general", "houdini"}},
        }},
        {"simulation4", {
            {220.0, 64, {"genhi", "general"}},
            {140.0, 32, {"genhi", "fx", "general"}},
            { 70.0, 16, {"genhi", "genmid"}},
            { 40.0,  8, {"genmid", "houdini"}},
            {  0.0,  4, {"general", "houdini"}},
        }},

        // simulation8: floors at 8 cores.
        {"simulation8", {
            {220.0, 64, {"genhi", "general"}},
            {140.0, 32, {"genhi", "fx", "general"}},
            { 70.0, 16, {"genhi", "genmid"}},
            {  0.0,  8, {"general", "houdini"}},
        }},

        // simulation16: floors at 16 cores.
        {"simulation16", {
            {220.0, 64, {"genhi", "general"}},
            {140.0, 32, {"genhi", "fx", "general"}},
            {  0.0, 16, {"genhi", "genmid", "general"}},
        }},

        // simulation32 not in the original YAML; mirror simulation16 with
        // a 32-core floor since that's what the data implies (avg 31.74).
        {"simulation32", {
            {220.0, 64, {"genhi", "general"}},
            {140.0, 32, {"genhi", "fx", "general"}},
            {  0.0, 32, {"genhi", "genmid"}},
        }},

        // spotless: denoiser. Stays mid-tier.
        {"spotless", {
            { 70.0, 16, {"desktop", "genmid"}},
            { 30.0,  8, {"desktop", "genmid"}},
            { 20.0,  8, {"general"}},
            {  8.0,  4, {"general"}},
            {  0.0,  2, {"general"}},
        }},
    };
    return m;
}

// Apply the cue-layer-man rewrite to a single layer. Uses the layer's
// requested memory as the proxy for what real cue-layer-man computes from
// max(maxRss, minMemory). Layers whose service is not in the ruleset are
// left untouched, matching the real script's behavior.
inline void apply_script_to_layer(Layer& L) {
    const auto& rulesets = cue_layer_man_rulesets();
    auto it = rulesets.find(L.service);
    if (it == rulesets.end()) return;   // service not managed

    double layer_mem_gb = static_cast<double>(L.mem_min_kb) / (1024.0 * 1024.0);
    for (const auto& rule : it->second) {
        if (layer_mem_gb >= rule.mem_min_gb) {
            L.cores_min = rule.cores;
            L.tags      = rule.tags;
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
