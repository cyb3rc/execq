/*
 * MIT License
 *
 * Copyright (c) 2018 Alkenso (Vladimir Vashurkin)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "execq.h"
#include <gmock/gmock.h>

using namespace execq;

struct ActiveTasksCounter
{
    std::atomic_int active{0};
    std::atomic_int maxActive{0};

    void operator()(const std::atomic_bool& isCanceled, int)
    {
        if (isCanceled)
            return;

        int now = active.fetch_add(1) + 1;
        maxActive.store(std::max(maxActive.load(), now));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        active.fetch_sub(1);
    }
};

TEST(ExecutionPool, ScaleUp)
{
    auto pool = CreateExecutionPool(2);

    ActiveTasksCounter counter;

    auto queue = CreateConcurrentExecutionQueue<void, int>(pool, std::ref(counter));

    for (int i = 0; i < 20; ++i)
        queue->push(i);

    pool->setThreadCount(4);

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    EXPECT_EQ(counter.maxActive.load(), 5);
}

TEST(ExecutionPool, ScaleDown)
{
    auto pool = CreateExecutionPool(4);

    ActiveTasksCounter counter;

    auto queue = CreateConcurrentExecutionQueue<void, int>(pool, std::ref(counter));

    for (int i = 0; i < 50; ++i)
        queue->push(i);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    pool->setThreadCount(2);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    counter.maxActive.store(0);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(counter.maxActive.load(), 3);
}

TEST(ExecutionPool, ScaleDown_InvalidValue)
{
    auto pool = CreateExecutionPool(4);

    ActiveTasksCounter counter;

    auto queue = CreateConcurrentExecutionQueue<void, int>(pool, std::ref(counter));

    for (int i = 0; i < 50; ++i)
        queue->push(i);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    pool->setThreadCount(1); // Invalid thread count
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    counter.maxActive.store(0);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(counter.maxActive.load(), 5);
}

TEST(ExecutionPool, ReconfigureWhileBusy_NoTaskLost)
{
    auto pool = CreateExecutionPool(2);

    constexpr int tasksCount = 500;
    std::atomic_int done{0};

    auto queue = CreateConcurrentExecutionQueue<void, int>(pool,
        [&](const std::atomic_bool&, int){
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            done.fetch_add(1);
        });

    constexpr auto timeout = std::chrono::seconds(15);
    auto deadline = std::chrono::steady_clock::now() + timeout;

    for (int i = 0; i < tasksCount; ++i)
    {
        queue->push(i);
        if (i % 50 == 0)
            pool->setThreadCount((i % 3) + 2);
        
        std::chrono::microseconds(100);
    }

    while (done.load() < tasksCount && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();

    EXPECT_EQ(done.load(), tasksCount);
}

