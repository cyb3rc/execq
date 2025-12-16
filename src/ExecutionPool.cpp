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

#include "ExecutionPool.h"

namespace
{
static constexpr uint64_t packThreadsCount(uint32_t target, uint32_t current)
{
    return (uint64_t(target) << 32) | uint64_t(current);
}
static constexpr uint32_t unpackTarget(uint64_t threadCount)
{
    return uint32_t(threadCount >> 32);
}
static constexpr uint32_t unpackCurrent(uint64_t threadCount)
{
    return uint32_t(threadCount & 0xFFFFFFFFu);
}
}

execq::impl::ExecutionPool::ExecutionPool(const uint32_t threadCount, const IThreadWorkerFactory& workerFactory)
    : m_workerFactory { workerFactory }
{
    m_threadCount.store(packThreadsCount(threadCount, threadCount));

    std::scoped_lock lock(m_workersMutex);
    m_workers.reserve(threadCount);

    for (uint32_t i = 0; i < threadCount; i++)
    {
        ThreadStopCb cb = [this]() { return this->shouldWorkerExit(); };
        m_workers.emplace_back(workerFactory.createWorker(m_providerGroup, cb));
    }
}

void execq::impl::ExecutionPool::addProvider(ITaskProvider& provider)
{
    m_providerGroup.addProvider(provider);
}

void execq::impl::ExecutionPool::removeProvider(ITaskProvider& provider)
{
    m_providerGroup.removeProvider(provider);
}

bool execq::impl::ExecutionPool::notifyOneWorker()
{
    std::lock_guard<std::mutex> lock(m_workersMutex);
    return details::NotifyWorkers(m_workers, true);
}

void execq::impl::ExecutionPool::notifyAllWorkers()
{
    std::lock_guard<std::mutex> lock(m_workersMutex);
    details::NotifyWorkers(m_workers, false);
}
void execq::impl::ExecutionPool::setThreadCount(uint32_t threadCount)
{
    if (threadCount < 2)
        return;

    uint32_t toAdd = 0;
    bool shrinking = false;

    for (;;) {
        uint64_t tc = m_threadCount.load();
        uint32_t cur = unpackCurrent(tc);

        if (threadCount > cur) {
            uint32_t localToAdd = threadCount - cur;
            uint64_t desired = packThreadsCount(threadCount, threadCount);

            if (m_threadCount.compare_exchange_weak(tc, desired)) {
                toAdd = localToAdd;
                shrinking = false;
                break;
            }
        } else {
            uint64_t desired = packThreadsCount(threadCount, cur);

            if (m_threadCount.compare_exchange_weak(tc, desired)) {
                toAdd = 0;
                shrinking = (threadCount < cur);
                break;
            }
        }
    }

    ThreadWorkers toDestroy;

    {
        std::scoped_lock lock(m_workersMutex);

        extractFinishedWorkers(toDestroy);

        if (toAdd > 0) {
            m_workers.reserve(m_workers.size() + toAdd);

            for (uint32_t i = 0; i < toAdd; ++i) {
                ThreadStopCb cb = [this]() { return this->shouldWorkerExit(); };
                m_workers.emplace_back(m_workerFactory.createWorker(m_providerGroup, cb));
            }
        }

        if (shrinking || toAdd > 0) {
            details::NotifyWorkers(m_workers, false);
        }
    }

    toDestroy.clear();
}

void execq::impl::ExecutionPool::extractFinishedWorkers(ThreadWorkers& workers)
{
    auto it = m_workers.begin();
    while (it != m_workers.end()) {
        if ((*it)->finished()) {
            workers.emplace_back(std::move(*it));
            it = m_workers.erase(it);
        } else {
            ++it;
        }
    }
}

bool execq::impl::ExecutionPool::shouldWorkerExit()
{
    for (;;)
    {
        uint64_t threadsCount = m_threadCount.load();
        uint32_t target  = unpackTarget(threadsCount);
        uint32_t current = unpackCurrent(threadsCount);

        if (current <= target)
            return false;

        uint64_t desired = packThreadsCount(target, current - 1);

        if (m_threadCount.compare_exchange_weak(threadsCount, desired))
            return true;
    }
}


// Details

bool execq::impl::details::NotifyWorkers(const ThreadWorkers& workers, const bool single)
{
    bool notified = false;
    for (const auto& worker : workers)
    {
        notified |= worker->notifyWorker();
        if (notified && single)
        {
            return true;
        }
    }
    
    return notified;
}
