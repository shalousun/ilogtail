/*
 * Copyright 2024 iLogtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <vector>

#include "checkpoint/RangeCheckpoint.h"
#include "logger/Logger.h"
#include "queue/FeedbackQueueKey.h"
#include "queue/SenderQueueInterface.h"
#include "queue/SenderQueueItem.h"

namespace logtail {

// not thread-safe, should be protected explicitly by queue manager
class ExactlyOnceSenderQueue : public SenderQueueInterface {
public:
    // mFlusher will be set on first push
    ExactlyOnceSenderQueue(const std::vector<RangeCheckpointPtr>& checkpoints, QueueKey key)
        : SenderQueueInterface(checkpoints.size(), checkpoints.size() - 1, checkpoints.size(), key),
          mRangeCheckpoints(checkpoints) {
        mQueue.resize(checkpoints.size());
    }

    bool Push(std::unique_ptr<SenderQueueItem>&& item) override;
    bool Remove(SenderQueueItem* item) override;
    void GetAllAvailableItems(std::vector<SenderQueueItem*>& items, bool withLimits = true) override;

    void Reset(const std::vector<RangeCheckpointPtr>& checkpoints);

private:
    size_t Size() const override { return mSize; }

    std::vector<std::unique_ptr<SenderQueueItem>> mQueue;
    size_t mWrite = 0;
    size_t mSize = 0;

    bool mIsInitialised = false;
    std::vector<RangeCheckpointPtr> mRangeCheckpoints;

#ifdef APSARA_UNIT_TEST_MAIN
    friend class ExactlyOnceSenderQueueUnittest;
    friend class ExactlyOnceQueueManagerUnittest;
#endif
};

} // namespace logtail
