#include "engines/brute_force.hpp"
#include "engines/hdt.hpp"
#include "storage/snapshot.hpp"
#include <iostream>
#include <map>
int main(int argc, char **argv) {
    using namespace fraud;
    if (argc != 2)
        return 2;
    storage::Image image;
    image.capacity = 50000;
    std::map<Id, Tenant<BruteForce>> tenants;
    auto replay = storage::recover(argv[1], {}, false, [&](const storage::Frame &frame) {
        auto bytes = storage::encode(frame);
        image.boundary_crc = storage::checksum(std::span(bytes).first(bytes.size() - 4));
        for (auto &e : frame) {
            auto &t = tenants.try_emplace(e.tenant, e.tenant).first->second;
            std::vector<Alert> alerts;
            if (t.apply_record(e, alerts) != Status::ok)
                throw std::runtime_error("duplicate or invalid WAL transition");
        }
    });
    image.epoch = replay.epoch;
    image.lsn = replay.lsn;
    image.time = replay.time;
    for (auto &[id, t] : tenants)
        image.tenants.push_back(t.image());
    auto restored = storage::deserialize(storage::serialize(image));
    for (auto &state : restored.tenants) {
        Tenant<HdtEngine> oracle(state.tenant, {}, HdtEngine(image.capacity));
        if (!oracle.restore(state))
            return 1;
        for (auto [producer, seq] : state.sequences) {
            auto result = oracle.submit(image.epoch, image.lsn + 1, image.time, Tick{},
                                        ProducerSequence{producer, seq});
            if (result.status != Status::duplicate)
                return 1;
        }
    }
    std::cout << storage::logical_checksum(image) << "\n";
}
