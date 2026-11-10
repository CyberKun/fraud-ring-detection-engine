#pragma once
#include <algorithm>
#include <vector>

namespace fraud::test {
// Delta-debug complements down to single deletions. Re-test after every
// successful deletion, yielding a 1-minimal trace for the failure predicate.
template<class T, class F>
std::vector<T> shrink(std::vector<T> trace, F fails) {
    std::size_t parts=2;
    while (!trace.empty()) {
        std::size_t width=(trace.size()+parts-1)/parts;
        bool reduced=false;
        for (std::size_t start=0; start<trace.size(); start+=width) {
            auto finish=std::min(start+width,trace.size());
            std::vector<T> candidate;
            candidate.reserve(trace.size()-(finish-start));
            candidate.insert(candidate.end(),trace.begin(),trace.begin()+start);
            candidate.insert(candidate.end(),trace.begin()+finish,trace.end());
            if (!fails(candidate)) continue;
            trace=std::move(candidate);
            parts=std::max<std::size_t>(2,parts-1);
            reduced=true;
            break;
        }
        if (reduced) continue;
        if (parts>=trace.size()) break;
        parts=std::min(trace.size(),parts*2);
    }
    return trace;
}
} // namespace fraud::test
