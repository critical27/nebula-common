
/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/stats/MemTracker.h"

namespace nebula {
namespace stats {

TEST(MemTrackerTest, SingleTrackerNoLimit) {
    MemTracker tracker("tracker");
    EXPECT_FALSE(tracker.hasLimit());
    tracker.consume(10);
    EXPECT_EQ(tracker.consumption(), 10);
    tracker.consume(10);
    EXPECT_EQ(tracker.consumption(), 20);
    tracker.release(15);
    EXPECT_EQ(tracker.consumption(), 5);
    EXPECT_FALSE(tracker.limitExceeded());
    tracker.release(5);
    EXPECT_EQ(tracker.consumption(), 0);
}

TEST(MemTrackerTest, SingleTrackerWithLimit) {
    MemTracker tracker(11, "tracker");
    EXPECT_TRUE(tracker.hasLimit());
    tracker.consume(10);
    EXPECT_EQ(tracker.consumption(), 10);
    EXPECT_FALSE(tracker.limitExceeded());
    tracker.consume(10);
    EXPECT_EQ(tracker.consumption(), 20);
    EXPECT_TRUE(tracker.limitExceeded());
    tracker.release(15);
    EXPECT_EQ(tracker.consumption(), 5);
    EXPECT_FALSE(tracker.limitExceeded());
    tracker.release(5);
}

TEST(MemTrackerTest, TrackerHierarchy) {
    MemTracker p(100, "parent");
    MemTracker c1(80, "c1", &p);
    MemTracker c2(50, "c2", &p);

    // everything below limits
    c1.consume(60);
    EXPECT_EQ(c1.consumption(), 60);
    EXPECT_FALSE(c1.limitExceeded());
    EXPECT_FALSE(c1.anyLimitExceeded());
    EXPECT_EQ(c2.consumption(), 0);
    EXPECT_FALSE(c2.limitExceeded());
    EXPECT_FALSE(c2.anyLimitExceeded());
    EXPECT_EQ(p.consumption(), 60);
    EXPECT_FALSE(p.limitExceeded());
    EXPECT_FALSE(p.anyLimitExceeded());

    // p goes over limit
    c2.consume(50);
    EXPECT_EQ(c1.consumption(), 60);
    EXPECT_FALSE(c1.limitExceeded());
    EXPECT_TRUE(c1.anyLimitExceeded());
    EXPECT_EQ(c2.consumption(), 50);
    EXPECT_FALSE(c2.limitExceeded());
    EXPECT_TRUE(c2.anyLimitExceeded());
    EXPECT_EQ(p.consumption(), 110);
    EXPECT_TRUE(p.limitExceeded());
    EXPECT_TRUE(p.anyLimitExceeded());

    // c2 goes over limit, p drops below limit
    c1.release(20);
    c2.consume(10);
    EXPECT_EQ(c1.consumption(), 40);
    EXPECT_FALSE(c1.limitExceeded());
    EXPECT_FALSE(c1.anyLimitExceeded());
    EXPECT_EQ(c2.consumption(), 60);
    EXPECT_TRUE(c2.limitExceeded());
    EXPECT_TRUE(c2.anyLimitExceeded());
    EXPECT_EQ(p.consumption(), 100);
    EXPECT_FALSE(p.limitExceeded());
    EXPECT_FALSE(p.anyLimitExceeded());
}

TEST(MemTrackerTest, TryConsumeInTrackerHierarchy) {
    MemTracker p(100, "parent");
    MemTracker c1(80, "c1", &p);
    MemTracker c2(50, "c2", &p);

    // everything below limits
    c1.consume(70);
    EXPECT_EQ(c1.consumption(), 70);
    EXPECT_FALSE(c1.limitExceeded());
    EXPECT_FALSE(c1.anyLimitExceeded());
    EXPECT_EQ(c2.consumption(), 0);
    EXPECT_FALSE(c2.limitExceeded());
    EXPECT_FALSE(c2.anyLimitExceeded());
    EXPECT_EQ(p.consumption(), 70);
    EXPECT_FALSE(p.limitExceeded());
    EXPECT_FALSE(p.anyLimitExceeded());

    // c2 try consume failed because parent goes over limit
    EXPECT_FALSE(c2.tryConsume(40));
    EXPECT_EQ(c1.consumption(), 70);
    EXPECT_FALSE(c1.limitExceeded());
    EXPECT_FALSE(c1.anyLimitExceeded());
    EXPECT_EQ(c2.consumption(), 0);
    EXPECT_FALSE(c2.limitExceeded());
    EXPECT_FALSE(c2.anyLimitExceeded());
    EXPECT_EQ(p.consumption(), 70);
    EXPECT_FALSE(p.limitExceeded());
    EXPECT_FALSE(p.anyLimitExceeded());

    // c2 try consume ok
    EXPECT_TRUE(c2.tryConsume(20));
    EXPECT_EQ(c1.consumption(), 70);
    EXPECT_FALSE(c1.limitExceeded());
    EXPECT_FALSE(c1.anyLimitExceeded());
    EXPECT_EQ(c2.consumption(), 20);
    EXPECT_FALSE(c2.limitExceeded());
    EXPECT_FALSE(c2.anyLimitExceeded());
    EXPECT_EQ(p.consumption(), 90);
    EXPECT_FALSE(p.limitExceeded());
    EXPECT_FALSE(p.anyLimitExceeded());
}

#ifdef JEMALLOC_ENABLED
TEST(MemTrackerTest, LogTest) {
    MemTracker::jemallocStats();
}

TEST(MemTrackerTest, JeMallocRootTracker) {
    bool force = true;
    auto& root = MemTracker::rootTracker();
    // force to get initial consumption
    auto value = root.getUpdatedConsumption(force);

    // As to root root, explicit consume and release should have no effect on consumption.
    root.consume(2000);
    ASSERT_EQ(value, root.consumption());
    root.release(1000);
    ASSERT_EQ(value, root.consumption());

    value = root.getUpdatedConsumption(force);
    // check memory size after we allocate a buffer
    constexpr size_t size = 4 * 1024 * 1024;
    std::unique_ptr<char[]> big(new char[size]);
    // do not optimize away
    UNUSED(big);
    ASSERT_GE(root.getUpdatedConsumption(force), value + size);
}

TEST(MemTrackerTest, MockOutOfMemory) {
    FLAGS_memory_consumption_interval_secs = 1;
    bool force = true;
    auto& root = MemTracker::rootTracker();
    // force to get initial consumption
    auto value = root.getUpdatedConsumption(force);
    // set the root tracker limit to a bigger size. In reality, the limit is preset by gflags
    root.limit_ = value + 10 * 1024 * 1024;

    MemTracker::jemallocStats();
    LOG(INFO) << "Begin to acquire memory";
    // add a unlimited tracker as child of root
    MemTracker tracker("tracker");
    std::vector<std::unique_ptr<char[]>> buffers;
    constexpr size_t size = 1 * 1024 * 1024;
    // try to allocate memory exceeds root limit
    int32_t count = 0;
    for (; count < 100; count++) {
        if (tracker.tryConsume(size)) {
            buffers.emplace_back(new char[size]);
        } else {
            break;
        }
        // sleep to make sure root tracker has update consumption by stats
        sleep(FLAGS_memory_consumption_interval_secs);
        MemTracker::jemallocStats();
    }
    CHECK_LE(count, 100);
    LOG(INFO) << "Allocation done";
    ASSERT_FALSE(root.limitExceeded());
    ASSERT_FALSE(tracker.limitExceeded());
    ASSERT_FALSE(tracker.anyLimitExceeded());
    MemTracker::jemallocStats();

    LOG(INFO) << "Release memory";
    buffers.clear();
    tracker.release(count * size);
    // trigger gc by manual
    mallctl("arena." NEBULA_STRINGIFY(MALLCTL_ARENAS_ALL) ".decay", nullptr, nullptr, nullptr, 0);
    ASSERT_TRUE(tracker.tryConsume(size));
}
#endif

TEST(MemTrackerTest, UnregisterFromParent) {
    auto parent = std::make_unique<MemTracker>("parent");
    auto child = std::make_unique<MemTracker>("child", parent.get());

    // Three trackers: root, parent, and child.
    auto all = MemTracker::listAllTrackers();
    ASSERT_EQ(3, all.size());

    child.reset();

    // Now only two because the child cannot be found from the root, though it is still alive.
    all = MemTracker::listAllTrackers();
    ASSERT_EQ(2, all.size());
}

}   // namespace stats
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
