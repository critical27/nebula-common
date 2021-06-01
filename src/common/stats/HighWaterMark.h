/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_STATS_HIGHWATERMARK_H_
#define COMMON_STATS_HIGHWATERMARK_H_

#include "common/base/Base.h"

namespace nebula {
namespace stats {

// Lock-free integer that keeps track of the highest value seen.
// Similar to Impala's RuntimeProfile::HighWaterMarkCounter.
// HighWaterMark::max() returns the highest value seen;
// HighWaterMark::current() returns the current value.
class HighWaterMark {
public:
    explicit HighWaterMark(int64_t initial)
        : cur_(initial), max_(initial) {}

    HighWaterMark(const HighWaterMark&) = delete;
    HighWaterMark& operator=(const HighWaterMark&) = delete;
    HighWaterMark(HighWaterMark&& x) = delete;
    HighWaterMark& operator=(HighWaterMark&& x) = delete;

    // Return the current value.
    int64_t current() const {
        return cur_.load(std::memory_order_acquire);
    }

    // Return the max value.
    int64_t max() const {
        return max_.load(std::memory_order_acquire);
    }

    // If current value + 'delta' is <= 'max', increment current value by 'delta' and return true;
    bool tryAdd(int64_t delta, int64_t max) {
        while (true) {
            int64_t cur = current();
            int64_t expect = cur + delta;
            if (expect > max) {
                return false;
            }
            if (cur_.compare_exchange_strong(cur, expect, std::memory_order_acq_rel)) {
                updateMax(expect);
                return true;
            }
        }
    }

    void add(int64_t amount) {
        updateMax(cur_.fetch_add(amount, std::memory_order_acq_rel) + amount);
    }

    void set(int64_t value) {
        cur_.store(value, std::memory_order_release);
        updateMax(value);
    }

private:
    void updateMax(int64_t value) {
        int64_t max = max_.load(std::memory_order_acquire);
        while (max < value) {
            if (max_.compare_exchange_strong(max, value, std::memory_order_acq_rel)) {
                break;
            }
        }
    }

    std::atomic<int64_t> cur_;
    std::atomic<int64_t> max_;
};

}  // namespace stats
}  // namespace nebula

#endif  // COMMON_STATS_HIGHWATERMARK_H_
