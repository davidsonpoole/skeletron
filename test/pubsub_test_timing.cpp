#include <gtest/gtest.h>
#include <algorithm>
#include <numeric>

#include "../src/pubsub.h"

class PubSubTest : public testing::Test {
   protected:
    Manager m;

    void SetUp() override { 
        m.start(); 
    }
    std::vector<std::vector<uint64_t>> results;
};

struct TestMessage {
    uint64_t seq_num;     // 8 bytes
    uint64_t publish_ts;  // 8 bytes
    uint8_t padding[48];  // pad to fixed 64 bytes, or your target payload size
};

// nanos since epoch
uint64_t read_timestamp() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
}

// Demonstrate some basic assertions.
TEST_F(PubSubTest, PublishSubscribeTiming_Baseline) {

    const int warmupCount = 100000;
    const int measuredCount = 1000000;
    const int totalCount = warmupCount + measuredCount;

    std::vector<uint64_t> latencies;
    latencies.reserve(measuredCount);

    std::atomic<int> counter{0};

    // Pre-allocate messages
    std::vector<unsigned char*> messages;
    for (int i=0; i<totalCount; i++) {
        messages.push_back(new unsigned char[sizeof(TestMessage)]);
    }

    m.subscribe("/abc", [&](unsigned char* msg) {
        uint64_t recv_ts = read_timestamp();
        const TestMessage* m = reinterpret_cast<const TestMessage*>(msg);

        // only record latency after warmup
        if (m->seq_num >= warmupCount) {
            uint64_t latency = recv_ts - m->publish_ts;
            latencies.push_back(latency);
        }

        // need to use memory order release so that results[] is valid
        counter.fetch_add(1, std::memory_order_release);
        delete[] msg;  // Free the heap-allocated buffer
    });

    // Publish concurrently from multiple threads
    for (int i=0; i<totalCount; i++) {
        TestMessage* testMsg = reinterpret_cast<TestMessage*>(messages[i]);
        testMsg->seq_num = i;
        testMsg->publish_ts = read_timestamp();
        m.publish("/abc", messages[i]);
    }

    // using memory_order_acquire so we are guaranteed to see write results
    while (counter.load(std::memory_order_acquire) < totalCount) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    // Analyze results
    std::sort(latencies.begin(), latencies.end());

    double mean = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();

    std::cout << "Latency Statistics (nanoseconds):\n";
    std::cout << "  Min:  " << latencies.front() << "\n";
    std::cout << "  p50:  " << latencies[latencies.size() / 2] << "\n";
    std::cout << "  p95:  " << latencies[latencies.size() * 95 / 100] << "\n";
    std::cout << "  p99:  " << latencies[latencies.size() * 99 / 100] << "\n";
    std::cout << "  Max:  " << latencies.back() << "\n";
    std::cout << "  Mean: " << mean << "\n";
}
