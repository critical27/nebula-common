/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_STATS_MEMTRACKER_H_
#define COMMON_STATS_MEMTRACKER_H_

#include "common/base/Base.h"
#include <sys/sysinfo.h>
#include <folly/memory/MallctlHelper.h>
#include <gtest/gtest_prod.h>
#include "common/base/StatusOr.h"
#include "common/stats/HighWaterMark.h"
#include "common/time/WallClock.h"

DEFINE_int64(memory_limit_in_bytes, 0,
             "Maximum amount of memory this daemon should use, in bytes. "
             "A value of 0 autosizes based on the total system memory. "
             "A value of -1 disables all memory limiting.");
DEFINE_double(memory_limit_ratio, 0.9,
              "If memory_limit_in_bytes is 0, then memoy limit is "
              "set to memory_limit_ratio * Available RAM.");
DEFINE_int64(memory_consumption_interval_secs, 10, "interval between comsuption update");

namespace nebula {
namespace stats {

using GcFunc = std::function<void(int64_t bytesToFree)>;
using ConsumptionFuc = std::function<StatusOr<int64_t>()>;

class MemTracker : public std::enable_shared_from_this<MemTracker> {
    FRIEND_TEST(MemTrackerTest, MockOutOfMemory);

public:
    explicit MemTracker(const std::string& id,
                        MemTracker* parent = nullptr,
                        bool addToParent = true)
        : MemTracker(-1, id, parent, addToParent) {}

    // Create a mem tracker and add it to the tree.
    // The limit is in bytes, limit < 0 means no limit. The id is used for indentify mem tracker
    // which must be unique. The tracker will be added to the tree if addToParent is true. The
    // parent must outlive the child, if parent is nullptr, this tracker will be the child of
    // root tracker.
    MemTracker(int64_t limit,
               const std::string& id,
               MemTracker* parent = nullptr,
               bool addToParent = true)
        : limit_(limit), id_(id), addToParent_(addToParent) {
        parent_ = parent ? parent : &rootTracker();
        if (addToParent_) {
            parent_->addChild(this);
        }
        MemTracker* tracker = this;
        while (tracker != nullptr) {
            ancestors_.emplace_back(tracker);
            if (tracker->hasLimit()) {
                limitAncestors_.emplace_back(tracker);
            }
            tracker = tracker->parent_;
        }
    }

    ~MemTracker() {
        if (addToParent_) {
            parent_->release(consumption());
            unregisterFromParent();
        }
    }

    MemTracker(const MemTracker&) = delete;
    MemTracker& operator=(const MemTracker&) = delete;
    MemTracker(MemTracker&& x) = delete;
    MemTracker& operator=(MemTracker&& x) = delete;

    static MemTracker& rootTracker() {
        static MemTracker rootTracker;
        return rootTracker;
    }

    int64_t consumption() const {
        return consumption_.current();
    }

    // Should only used by root tracker, it will try to update consumption after a interval,
    // set force to true to always get latest stat.
    int64_t getUpdatedConsumption(bool force = false) {
        updateConsumption(force);
        return consumption();
    }

    void consume(int64_t bytes) {
        CHECK_GE(bytes, 0);
        if (bytes == 0) {
            return;
        }

        // for directly call from root tracker since it has no parent
        if (consumptionFunc_) {
            updateConsumption();
            return;
        }

        for (auto& tracker : ancestors_) {
            if (!consumptionFunc_) {
                tracker->consumption_.add(bytes);
            } else {
                updateConsumption();
            }
        }
    }

    bool tryConsume(int64_t bytes) {
        CHECK_GE(bytes, 0);
        if (bytes == 0) {
            return true;
        }

        // Walk the tracker tree top-down. If i is -1, consume succeed, otherwise i is the
        // first failed tracker because exceeds limit.
        int32_t i;
        for (i = ancestors_.size() - 1; i >= 0; --i) {
            auto* tracker = ancestors_[i];
            if (tracker->limit_ < 0) {
                // directly add if it is a unlimited tracker
                tracker->consumption_.add(bytes);
                continue;
            } else if (tracker->consumptionFunc_) {
                // update stats if it is a root tracker
                tracker->updateConsumption();
            }

            if (!tracker->consumption_.tryAdd(bytes, tracker->limit_)) {
                // Attempt to GC memory, for simplicity, we don't retry if gc succeed.
                tracker->gcMemory(tracker->limit_ - bytes);
                break;
            }
        }
        if (i == -1) {
            return true;
        }
        // Revert previous changes if failed to consume
        for (int32_t j = ancestors_.size() - 1; j > i; --j) {
            ancestors_[j]->consumption_.add(-bytes);
        }
        return false;
    }

    void release(int64_t bytes) {
        CHECK_GE(bytes, 0);
        if (bytes == 0) {
            return;
        }

        // for directly call from root tracker since it has no parent
        if (consumptionFunc_) {
            updateConsumption();
            return;
        }

        for (auto& tracker : ancestors_) {
            if (!consumptionFunc_) {
                tracker->consumption_.add(-bytes);
            } else {
                updateConsumption();
            }
        }
    }

    bool hasLimit() const {
        return limit_ > 0;
    }

    bool limitExceeded() {
        if (checkLimitExceeded()) {
            return gcMemory(limit_);
        }
        return false;
    }

    bool anyLimitExceeded() {
        for (const auto& tracker : limitAncestors_) {
            if (tracker->limitExceeded()) {
                return true;
            }
        }
        return false;
    }

    void AddGcFunction(GcFunc func) {
        std::lock_guard<std::mutex> guard(gcLock_);
        gcFunc_.emplace_back(func);
    }

    void listDescendantTrackers(std::vector<MemTracker*>& result, bool onlyChildren = false) {
        size_t begin = result.size();
        {
            std::lock_guard<std::mutex> guard(childLock_);
            for (auto* child : children_) {
                result.emplace_back(child);
            }
        }
        if (!onlyChildren) {
            size_t end = result.size();
            for (size_t i = begin; i != end; ++i) {
                result[i]->listDescendantTrackers(result, onlyChildren);
            }
        }
    }

    // Returns a list of all the valid trackers.
    static std::vector<MemTracker*> listAllTrackers() {
        std::vector<MemTracker*> result;
        auto& root = rootTracker();
        result.emplace_back(&root);
        root.listDescendantTrackers(result);
        return result;
    }

    static void jemallocStats() {
        #ifdef JEMALLOC_ENABLED
        // Perhaps we could use folly::mallctlRead if we don't need to refresh epoch
        uint64_t epoch = 1;
        size_t size = sizeof(epoch);
        mallctl("epoch", &epoch, &size, &epoch, size);

        size_t allocated, active, mapped;
        size = sizeof(size_t);
        if (mallctl("stats.allocated", &allocated, &size, nullptr, 0) == 0 &&
            mallctl("stats.active", &active, &size, nullptr, 0) == 0 &&
            mallctl("stats.mapped", &mapped, &size, nullptr, 0) == 0) {
            LOG(INFO) << "allocated/active/mapped: " << allocated << " " << active << " " << mapped;
        }
        #endif
    }

private:
    // root tracker ctor
    MemTracker() {
        limit_ = FLAGS_memory_limit_in_bytes;
        if (FLAGS_memory_limit_in_bytes == 0) {
            struct sysinfo info;
            CHECK_EQ(0, sysinfo(&info));
            limit_ = info.totalram * info.mem_unit * FLAGS_memory_limit_ratio;
            LOG(INFO) << "Root memory tracker limit is " << limit_ << " bytes";
        }
        #ifdef JEMALLOC_ENABLED
            consumptionFunc_ = std::bind(&MemTracker::getJemallocStat, "stats.active");
        #endif
        id_ = "root";
        parent_ = nullptr;
    }

    void addChild(MemTracker* child) {
        std::lock_guard<std::mutex> guard(childLock_);
        child->childIter_ = children_.insert(children_.end(), child);
    }

    void unregisterFromParent() {
        std::lock_guard<std::mutex> guard(childLock_);
        parent_->children_.erase(childIter_);
        childIter_ = parent_->children_.end();
    }

    bool checkLimitExceeded() const {
        return limit_ >= 0 && limit_ < consumption();
    }

    // If consumption is higher than expect, attempts to free memory by calling any GC functions
    // which tries to make consumption lower than expect. Returns true if consumption is still
    // greater than expect.
    bool gcMemory(int64_t expect) {
        if (expect < 0) {
            // Impossible to GC enough memory to reach the goal.
            return true;
        }

        std::lock_guard<std::mutex> guard(gcLock_);
        int64_t current = consumption();
        // Check if someone gc'd before us
        if (current <= expect) {
            return false;
        }

        // Try to free up some memory
        for (size_t i = 0; i < gcFunc_.size(); ++i) {
            // Try to free up the amount we are over plus some extra so that we don't have to
            // immediately GC again. Don't free all the memory since that can be unnecessarily
            // expensive.
            const int64_t EXTRA_BYTES_TO_FREE = 512L * 1024L * 1024L;
            int64_t bytesToFree = current - expect + EXTRA_BYTES_TO_FREE;
            gcFunc_[i](bytesToFree);
            current = consumption();
            if (current - expect >= EXTRA_BYTES_TO_FREE) {
                break;
            }
        }

        return current > expect;
    }

    static StatusOr<int64_t> getJemallocStat(const char* name) {
    #ifdef JEMALLOC_ENABLED
        int64_t epoch = 1;
        size_t size = sizeof(epoch);
        mallctl("epoch", &epoch, &size, &epoch, size);

        int64_t value;
        size = sizeof(value);
        if (mallctl(name, &value, &size, nullptr, 0) == 0) {
            return value;
        }
        return Status::Error("Failed to mallctl");
    #else
        return Status::Error("Jemalloc is not enabled");
    #endif
    }

    void updateConsumption(bool force = false) {
        if (consumptionFunc_) {
            auto now = time::WallClock::fastNowInSec();
            if (force ||
                now > consumptionLastUpdateTime_ + FLAGS_memory_consumption_interval_secs) {
                consumptionLastUpdateTime_ = now;
                auto value = consumptionFunc_();
                if (value.ok()) {
                    consumption_.set(value.value());
                }
            }
        }
    }

private:
    int64_t limit_;
    std::string id_;
    MemTracker* parent_{nullptr};
    bool addToParent_{false};

    // this tracker plus all of its ancestors, in bottom-up order.
    // this tracker comes at first
    std::vector<MemTracker*> ancestors_;
    // ancestors_ with valid limits
    std::vector<MemTracker*> limitAncestors_;

    // direct children of this tracker
    std::list<MemTracker*> children_;
    // childIter_ is the iterator in parent's children_, for convience to remove from parent
    std::list<MemTracker*>::iterator childIter_;

    // Functions to call after the limit is reached to free memory.
    std::vector<GcFunc> gcFunc_;

    HighWaterMark consumption_{0};

    // consumptionFunc_ is used to get memory stats of process, such as jemalloc.
    // It should only be used in root tracker for now, we could use it in children tracker
    // as well if necessary.
    ConsumptionFuc consumptionFunc_;
    std::atomic<int64_t> consumptionLastUpdateTime_{-1};

    std::mutex childLock_;
    std::mutex gcLock_;
};

}  // namespace stats
}  // namespace nebula

#endif  // COMMON_STATS_MEMTRACKER_H_
