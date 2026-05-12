#pragma once
// Smart routing: filter the data-owner fanout list down to the subset whose
// shard could actually contain matching rows for a given Query.
//
// Loaded from the partitions_<scheme>/manifest.json next to the CSV shards.
// If the manifest carries no routing metadata (e.g. round_robin scheme) then
// every owner is eligible — same behavior as before.

#include <map>
#include <string>
#include <vector>
#include <utility>

#include "mini2.pb.h"

namespace mini2 {

struct OwnerMetadata {
    // One of: "round_robin", "trip_distance", "pu_location_id",
    // "pickup_datetime", "consistent_hash", or "" if no manifest was loaded.
    std::string scheme;

    // owner -> [lo, hi] for trip_distance scheme.
    std::map<std::string, std::pair<double, double>> dist_range;
    // owner -> sorted vector of zone IDs.
    std::map<std::string, std::vector<int>>          zones;
    // owner -> month numbers (1..12).
    std::map<std::string, std::vector<int>>          months;
    // consistent_hash scheme: vnodes are striped across the owners list in
    // overlay order (vnode_id 0..vnodes_per_owner-1 -> first owner, etc.).
    int hash_bucket_multiplier = 100;   // bucket = int(value * multiplier)
    int vnodes_per_owner       = 0;     // 0 disables this scheme
    int total_vnodes           = 0;
    std::string hash_column;            // which column the hash applies to
    std::vector<std::string> owners_order;  // owner names in stripe order

    bool empty() const {
        return scheme.empty()
            || (dist_range.empty() && zones.empty() && months.empty()
                && vnodes_per_owner == 0);
    }
};

// Best-effort load of partitions_<scheme>/manifest.json. Returns an empty
// OwnerMetadata if the file is missing or has no routing fields.
OwnerMetadata load_owner_metadata(const std::string& data_dir);

// Compute the subset of `all_owners` that could match the query. If the
// metadata is empty or none of the predicates are on a clustering column,
// returns the full list (i.e. no smart routing applied).
std::vector<std::string> eligible_owners(
    const std::vector<std::string>& all_owners,
    const Query&                    query,
    const OwnerMetadata&            meta);

} // namespace mini2
