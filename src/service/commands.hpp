#pragma once
#include "core/types.hpp"
#include "fraud_engine.pb.h"
namespace fraud::service {
inline EdgeUpsert command(const rpc::UpsertEdgeRequest &r) {
    return {{r.u(), r.v(), r.edge_type()}, r.expiry_ms()};
}
inline EdgeDelete command(const rpc::DeleteEdgeRequest &r) { return {{r.u(), r.v(), r.edge_type()}}; }
inline VertexUpdate command(const rpc::UpdateVertexRequest &r) {
    return {r.vertex_id(), {r.volume(), r.is_flagged()}};
}
} // namespace fraud::service
