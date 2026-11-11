#pragma once
#include <cstdlib>
#include <filesystem>
#include <stdexcept>

namespace fraud::test {
class TempDir {
public:
    TempDir() {
        char pattern[]="/tmp/fraud-engine-XXXXXX";
        if (!::mkdtemp(pattern)) throw std::runtime_error("mkdtemp failed");
        path=pattern;
    }
    ~TempDir(){std::error_code ec;std::filesystem::remove_all(path,ec);}
    TempDir(const TempDir&)=delete;
    std::filesystem::path path;
};
}
