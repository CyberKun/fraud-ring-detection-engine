#include "replica/election.hpp"
#include <curl/curl.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <openssl/evp.h>
#include <stdexcept>

namespace fraud::replica {
namespace {
using json = nlohmann::json;
std::string b64(const std::string &s) {
    std::string out(4 * ((s.size() + 2) / 3) + 1, '\0');
    int n = EVP_EncodeBlock(reinterpret_cast<unsigned char *>(out.data()),
                            reinterpret_cast<const unsigned char *>(s.data()), s.size());
    out.resize(n);
    return out;
}
std::string unbase(const std::string &s) {
    if (s.empty())
        return {};
    std::string out(s.size(), '\0');
    int n = EVP_DecodeBlock(reinterpret_cast<unsigned char *>(out.data()),
                            reinterpret_cast<const unsigned char *>(s.data()), s.size());
    if (n < 0)
        throw std::runtime_error("etcd base64 value");
    n -= s.back() == '=';
    n -= s.size() > 1 && s[s.size() - 2] == '=';
    out.resize(n);
    return out;
}
json post(const std::string &url, json body) {
    using Curl = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>;
    Curl c(curl_easy_init(), curl_easy_cleanup);
    if (!c)
        throw std::runtime_error("etcd curl initialization");
    auto request = body.dump();
    std::string response;
    curl_easy_setopt(c.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(c.get(), CURLOPT_POSTFIELDS, request.c_str());
    curl_easy_setopt(c.get(), CURLOPT_TIMEOUT_MS, 1000L);
    curl_easy_setopt(c.get(), CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(
        c.get(), CURLOPT_WRITEFUNCTION, +[](char *data, size_t n, size_t k, void *buf) -> size_t {
            auto &s = *static_cast<std::string *>(buf);
            if (s.size() + n * k > 65536)
                return 0;
            s.append(data, n * k);
            return n * k;
        });
    curl_easy_setopt(c.get(), CURLOPT_WRITEDATA, &response);
    auto result = curl_easy_perform(c.get());
    long status{};
    curl_easy_getinfo(c.get(), CURLINFO_RESPONSE_CODE, &status);
    if (result != CURLE_OK || status != 200)
        throw std::runtime_error("etcd unavailable: " + url);
    return json::parse(response);
}
} // namespace
Election::Election(std::string endpoint, Id shard)
    : endpoint_(std::move(endpoint)), key_(b64("/shards/" + std::to_string(shard) + "/leader")) {}
Lease Election::lookup() const {
    auto r = post(endpoint_ + "/v3/kv/range", {{"key", key_}});
    if (!r.contains("kvs") || r["kvs"].empty())
        return {};
    auto &kv = r["kvs"][0];
    return {unbase(kv.at("value")), kv.at("lease"), std::stoull(kv.at("create_revision").get<std::string>())};
}
Lease Election::acquire(const std::string &owner, int ttl) const {
    auto lease =
        post(endpoint_ + "/v3/lease/grant", {{"TTL", std::to_string(ttl)}}).at("ID").get<std::string>();
    json cmp = {{"key", key_}, {"target", "CREATE"}, {"result", "EQUAL"}, {"create_revision", "0"}};
    json put = {{"request_put", {{"key", key_}, {"value", b64(owner)}, {"lease", lease}}}};
    auto r =
        post(endpoint_ + "/v3/kv/txn", {{"compare", json::array({cmp})}, {"success", json::array({put})}});
    if (!r.value("succeeded", false))
        return {};
    return {owner, lease, std::stoull(r.at("header").at("revision").get<std::string>())};
}
bool Election::renew(const Lease &l) const {
    auto r = post(endpoint_ + "/v3/lease/keepalive", {{"ID", l.lease}}).at("result");
    return r.at("ID") == l.lease && std::stoll(r.value("TTL", "0")) > 0;
}
bool Election::holds(const Lease &l) const {
    auto current = lookup();
    return current.epoch == l.epoch && current.owner == l.owner && current.lease == l.lease && l.epoch;
}
} // namespace fraud::replica
