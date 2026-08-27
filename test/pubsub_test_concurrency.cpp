#include <gtest/gtest.h>

#include "../src/pubsub.h"

class PubSubTest : public testing::Test {
protected:
    Manager m;

    void SetUp() override {
        m.start();
    }
};

// Demonstrate some basic assertions.
TEST_F(PubSubTest, ConcurrentPublishSubscribe) {
    const int NUM_PUBLISHERS = 100;
    const int NUM_SUBSCRIBERS = 50;
    const int MESSAGES_PER_PUBLISHER = 10;

    std::atomic<int> messagesReceived{0};
    std::mutex mtx;

    // Subscribe first
    std::vector<std::thread> subscribers;
    for (int i = 0; i < NUM_SUBSCRIBERS; i++) {
      m.subscribe("/abc", [&messagesReceived](unsigned char* msg) {
        messagesReceived++;
      });
    }

    // Publish concurrently from multiple threads
    std::vector<std::thread> publishers;
    for (int i = 0; i < NUM_PUBLISHERS; i++) {
      publishers.emplace_back([this, i]() {
        for (int j = 0; j < MESSAGES_PER_PUBLISHER; j++) {
          m.publish("/abc", (unsigned char*)"Test message");
        }
      });
    }

    // Wait for all publishers to finish
    for (auto& t : publishers) {
      t.join();
    }

    // Give time for messages to be delivered
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify: each message should be received by all subscribers
    int expectedMessages =
        NUM_PUBLISHERS * MESSAGES_PER_PUBLISHER * NUM_SUBSCRIBERS;
    EXPECT_EQ(messagesReceived.load(), expectedMessages);
}