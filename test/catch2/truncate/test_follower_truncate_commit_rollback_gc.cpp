/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "src/cursor/cur_layered_private.h"
#include "../misc_tests/layered_disagg_utils.h"
#include "../wrappers/connection_wrapper.h"
#include "truncate_list_helpers.hpp"
#include "../../utility/test_util.h"

namespace {

const std::string TABLE_URI = "table:test_follower_trunc_commit_gc";
const std::string LEADER_HOME = "WT_TEST.follower_trunc_commit_gc_leader";
const std::string FOLLOWER_HOME = "WT_TEST.follower_trunc_commit_gc_follower";

class disagg_home_directory {
public:
    disagg_home_directory()
    {
        cleanup();
        std::filesystem::create_directories(LEADER_HOME + "/kv_home");
        std::filesystem::create_directories(FOLLOWER_HOME);
        std::filesystem::create_directory_symlink(
          "../" + LEADER_HOME + "/kv_home", FOLLOWER_HOME + "/kv_home");
    }

    ~disagg_home_directory()
    {
        cleanup();
    }

private:
    static void
    cleanup()
    {
        std::error_code ec;
        std::filesystem::remove_all(LEADER_HOME, ec);
        std::filesystem::remove_all(FOLLOWER_HOME, ec);
    }
};

void
put_range(WT_SESSION *session, int start, int stop, const char *commit_ts)
{
    WT_CURSOR *cursor;
    char cfg[64], key[16], value[16];

    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);

    for (int i = start; i <= stop; ++i) {
        testutil_snprintf(key, sizeof(key), "%03d", i);
        testutil_snprintf(value, sizeof(value), "val_%03d", i);

        cursor->set_key(cursor, key);
        cursor->set_value(cursor, value);

        REQUIRE(cursor->insert(cursor) == 0);
    }

    testutil_snprintf(cfg, sizeof(cfg), "commit_timestamp=%s", commit_ts);

    REQUIRE(session->commit_transaction(session, cfg) == 0);
    REQUIRE(cursor->close(cursor) == 0);
}

size_t
locked_truncate_list_size(WT_SESSION_IMPL *session, WT_LAYERED_TABLE *layered_table)
{
    __wt_readlock(session, &layered_table->truncate_list.lock);

    const size_t size = truncate_list_helpers::truncate_list_size(*layered_table);

    __wt_readunlock(session, &layered_table->truncate_list.lock);
    return size;
}

} // namespace

TEST_CASE("Follower truncate commit failure does not race GC via premature committed state",
  "[truncate_list][layered][truncate]")
{
    disagg_home_directory homes;

    connection_wrapper conn_leader_wrap(LEADER_HOME, layered_disagg_build_cfg("leader").c_str());
    WT_CONNECTION *conn_leader = conn_leader_wrap.get_wt_connection();
    WT_SESSION *session_leader = (WT_SESSION *)conn_leader_wrap.create_session();

    const char *table_cfg = "key_format=S,value_format=S,block_manager=disagg,type=layered";
    REQUIRE(session_leader->create(session_leader, TABLE_URI.c_str(), table_cfg) == 0);
    REQUIRE(conn_leader->set_timestamp(conn_leader, "oldest_timestamp=1") == 0);

    /* Establish baseline data in the table on leader and pick it up on follower. */
    put_range(session_leader, 1, 50, "a");
    layered_disagg_leader_checkpoint(conn_leader, session_leader, 0x0a);

    connection_wrapper conn_follow_wrap(
      FOLLOWER_HOME, layered_disagg_build_cfg("follower").c_str());
    WT_CONNECTION *conn_follow = conn_follow_wrap.get_wt_connection();
    WT_SESSION *session_follow = (WT_SESSION *)conn_follow_wrap.create_session();
    WT_SESSION *session_gc = (WT_SESSION *)conn_follow_wrap.create_session();
    REQUIRE(session_follow->create(session_follow, TABLE_URI.c_str(), table_cfg) == 0);
    layered_disagg_pickup_latest_checkpoint(conn_follow, session_gc);

    WT_CURSOR *c_start, *c_stop;
    REQUIRE(session_follow->open_cursor(
              session_follow, TABLE_URI.c_str(), nullptr, nullptr, &c_start) == 0);
    REQUIRE(session_follow->open_cursor(
              session_follow, TABLE_URI.c_str(), nullptr, nullptr, &c_stop) == 0);
    c_start->set_key(c_start, "010");
    c_stop->set_key(c_stop, "040");

    auto *clayered = reinterpret_cast<WTI_CURSOR_LAYERED *>(c_start);
    auto *layered_table = reinterpret_cast<WT_LAYERED_TABLE *>(clayered->dhandle);
    auto *s_follow = reinterpret_cast<WT_SESSION_IMPL *>(session_follow);
    auto *s_gc = reinterpret_cast<WT_SESSION_IMPL *>(session_gc);

    REQUIRE(session_follow->begin_transaction(session_follow, nullptr) == 0);
    REQUIRE(session_follow->truncate(session_follow, nullptr, c_start, c_stop, nullptr) == 0);
    REQUIRE(__wt_session_gen(s_follow, WT_GEN_DISAGG_CKPT) != 0);

    /* Pause commit before cannot_fail so GC and stable_timestamp validation can race it. */
    REQUIRE(conn_follow->reconfigure(
              conn_follow, "timing_stress_for_test=[commit_transaction_slow]") == 0);
    REQUIRE(
      FLD_ISSET(S2C(s_follow)->timing_stress_flags, WT_TIMING_STRESS_COMMIT_TRANSACTION_SLOW));

    std::atomic<bool> commit_started{false};
    int commit_ret = 0;
    std::thread committer([&]() {
        commit_started.store(true, std::memory_order_release);
        commit_ret = session_follow->commit_transaction(session_follow, "commit_timestamp=14");
    });

    bool timed_out = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!commit_started.load(std::memory_order_acquire) ||
      __wt_session_gen(s_follow, WT_GEN_DISAGG_CKPT) != 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            timed_out = true;
            break;
        }
        std::this_thread::yield();
    }

    int ts_ret = 0;
    size_t size_before_abort = 0;
    if (!timed_out) {
        /*
         * Force __txn_check_if_stable_has_moved_ahead_commit_ts() to abort the commit when the
         * committer wakes from WT_TIMING_STRESS_COMMIT_TRANSACTION_SLOW, then run GC while it is
         * paused before cannot_fail.
         */
        ts_ret = conn_follow->set_timestamp(conn_follow, "stable_timestamp=14");
        __wt_layered_table_truncate_prune(s_gc, layered_table, 0x14);
    }
    size_before_abort = locked_truncate_list_size(s_gc, layered_table);

    committer.join();

    const char *desc = nullptr, *pvalue = nullptr;
    int64_t gc_runs = 0, gc_removed = 0, rb_removed = 0;
    WT_CURSOR *c_stat;
    REQUIRE(session_gc->open_cursor(session_gc, "statistics:", nullptr, nullptr, &c_stat) == 0);
    c_stat->set_key(c_stat, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_RUNS);
    REQUIRE(c_stat->search(c_stat) == 0);
    REQUIRE(c_stat->get_value(c_stat, &desc, &pvalue, &gc_runs) == 0);
    c_stat->set_key(c_stat, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_GC_ENTRIES_REMOVED);
    REQUIRE(c_stat->search(c_stat) == 0);
    REQUIRE(c_stat->get_value(c_stat, &desc, &pvalue, &gc_removed) == 0);
    c_stat->set_key(c_stat, WT_STAT_CONN_LAYERED_TRUNCATE_LIST_ROLLBACK_ENTRIES_REMOVED);
    REQUIRE(c_stat->search(c_stat) == 0);
    REQUIRE(c_stat->get_value(c_stat, &desc, &pvalue, &rb_removed) == 0);
    REQUIRE(c_stat->close(c_stat) == 0);

    CAPTURE(gc_runs, gc_removed, rb_removed, size_before_abort, commit_ret);
    REQUIRE(!timed_out);
    REQUIRE(ts_ret == 0);
    REQUIRE(gc_runs >= 1);
    REQUIRE(gc_removed == 0);
    REQUIRE(rb_removed == 1);
    REQUIRE(size_before_abort == 1);
    REQUIRE(commit_ret == EINVAL);
    REQUIRE(locked_truncate_list_size(s_gc, layered_table) == 0);

    REQUIRE(conn_follow->reconfigure(conn_follow, "timing_stress_for_test=[]") == 0);

    /* Verify the rolled-back truncate did not delete keys from the table. */
    WT_CURSOR *c_verify;
    REQUIRE(session_follow->open_cursor(
              session_follow, TABLE_URI.c_str(), nullptr, nullptr, &c_verify) == 0);
    REQUIRE(session_follow->begin_transaction(session_follow, nullptr) == 0);
    c_verify->set_key(c_verify, "025");
    REQUIRE(c_verify->search(c_verify) == 0);
    REQUIRE(session_follow->rollback_transaction(session_follow, nullptr) == 0);
    REQUIRE(c_verify->close(c_verify) == 0);

    REQUIRE(c_start->close(c_start) == 0);
    REQUIRE(c_stop->close(c_stop) == 0);
}
