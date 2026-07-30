// Schema evolution of the per-card risk profile, the way a consumer does
// it: bump the state type's version, register a v1->v2 migration, and let
// clink's pre-deploy check confirm a savepoint taken by the old binary can
// restore into the new one. Plain main(), non-zero exit on failure.
//
// This exercises the consumer-facing contract - the migration function and
// the compatibility gate - which is the part a downstream job author writes
// and owns. clink's own suite covers the restore-pipeline integration that
// invokes the migration at snapshot restore.

#include <cstdint>
#include <iostream>
#include <span>
#include <string>

#include <clink/core/types.hpp>
#include <clink/state/schema_version.hpp>
#include <clink/state/state_migration_on_restore.hpp>

#include "risk_profile.hpp"
#include "risk_profile_v2.hpp"

namespace {

int g_failures = 0;
void check(bool ok, const char* what, int line) {
    if (!ok) {
        std::cerr << "FAIL (line " << line << "): " << what << "\n";
        ++g_failures;
    }
}
#define CHECK(cond) check((cond), #cond, __LINE__)

}  // namespace

int main() {
    using namespace clink;

    // A v1 savepoint value: a profile the old binary persisted (6 fields,
    // no first_ts). Card 9001 after its card-testing burst, say.
    cs::RiskProfile v1;
    v1.auths = 7;
    v1.declines = 6;
    v1.total = 492.31;
    v1.max_amount = 486.31;
    v1.last_ts = 1'753'920'123'000;
    v1.last_country = "SG";
    const auto v1_bytes = cs::profile_codec().encode(v1);

    // A private registry (avoid cross-test bleed in the process global).
    StateMigrationRegistry reg;
    cs::register_risk_profile_migration(reg);

    // 1. The migration runs and backfills first_ts, preserving every v1 field.
    CHECK(reg.has_path(cs::kRiskProfileStateType, 1, 2));
    const auto v2_bytes = reg.migrate(cs::kRiskProfileStateType, 1, 2,
                                      std::span<const std::byte>{v1_bytes.data(), v1_bytes.size()});
    const auto v2 = cs::profile_v2_codec().decode(v2_bytes);
    CHECK(v2.has_value());
    if (v2.has_value()) {
        CHECK(v2->auths == 7 && v2->declines == 6);
        CHECK(v2->max_amount == 486.31 && v2->last_country == "SG");
        CHECK(v2->last_ts == 1'753'920'123'000);
        CHECK(v2->first_ts == v1.last_ts);  // documented backfill
    }

    // 2. The pre-deploy gate: a savepoint stamped v1 restoring into a job
    // that expects v2 is COMPATIBLE once the migration is registered.
    const OperatorId op = operator_id_from_uid("cs-risk-profile");
    StateVersionMap stored;
    stored.set(op, cs::kRiskProfileStateType, 1);
    StateVersionMap expected;
    expected.set(op, cs::kRiskProfileStateType, 2);
    CHECK(check_restore_compatibility(stored, expected, reg).empty());

    // 3. Without the migration, the SAME upgrade is flagged incompatible -
    // the gate stops a data-losing deploy before it starts.
    StateMigrationRegistry empty_reg;
    const auto incompat = check_restore_compatibility(stored, expected, empty_reg);
    CHECK(incompat.size() == 1);
    if (incompat.size() == 1) {
        CHECK(incompat[0].from_version == 1 && incompat[0].to_version == 2);
        CHECK(incompat[0].state_type == cs::kRiskProfileStateType);
    }

    // 4. Version traits declare the evolution.
    CHECK(schema_version_v<cs::RiskProfile> == 1);
    CHECK(schema_version_v<cs::RiskProfileV2> == 2);

    if (g_failures != 0) {
        std::cerr << g_failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "schema evolution: v1 savepoint migrates to v2 (first_ts backfilled), "
                 "pre-deploy gate passes with the migration and flags its absence\n";
    return 0;
}
