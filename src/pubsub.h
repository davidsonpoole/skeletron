#include <condition_variable>
#include <mutex>
#include <string>
#include <functional>
#include <map>
#include <deque>
#include <queue>
#include <thread>
#include <iostream>
#include <memory>
#include <array>
#include <atomic>
#include <algorithm>

struct Subscriber {

    int handle;
    int thread_id;
    std::function<void(unsigned char*)> fn;
};

class Manager {
public:
    Manager(size_t num_threads = std::thread::hardware_concurrency() * 2) 
    : NUM_THREADS(num_threads) {
        workQueue.resize(NUM_THREADS);
        workQueueLock.reserve(NUM_THREADS);
        workQueueCV.reserve(NUM_THREADS);

        for (int i=0; i<NUM_THREADS; i++) {
            workQueueLock.push_back(std::make_unique<std::mutex>());
            workQueueCV.push_back(std::make_unique<std::condition_variable>());
            freeList.push(i);
        }
    }
    
    ~Manager() {
        // Signal all threads to stop
        shutdown = true;
        for (int i=0; i<NUM_THREADS; i++) {
            (*workQueueCV[i]).notify_all();
        }
        // Wait for all threads to finish
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
    
    void start() {
        std::unique_lock<std::mutex> lock(startupLock);
        for (int i=0; i<NUM_THREADS; i++) {
            threads.push_back(std::thread(&Manager::thread_fn, this, i));
        }
        // Wait for all threads to be ready
        startupCV.wait(lock, [this]() { return readyCount == NUM_THREADS; });
    }
    void publish(std::string topic, unsigned char* msg) {

        std::lock_guard<std::mutex> lock(topicLock);

        if (topics.find(topic) == topics.end()) {
            return;
        }

        auto& v = topics[topic];


        for (auto& s : v) {
            int thread_id = s->thread_id;
            std::lock_guard<std::mutex> lock(*workQueueLock[thread_id]);
            workQueue[thread_id].push({s->fn, msg});
            (*workQueueCV[thread_id]).notify_all();
        }
    }

    int subscribe(std::string topic, std::function<void(unsigned char*)> fn) {


        std::lock_guard<std::mutex> lock(topicLock);

        if (freeList.empty()) {
            std::cerr << "No more threads available. Please try again later." << std::endl;
            return -1;
        }

        auto s = std::make_unique<Subscriber>();
        s->fn = fn;

        auto& v = topics[topic];

        int handle = ++topicHandles[topic];
        s->handle = handle;
        s->thread_id = freeList.front();
        freeList.pop();
        
        topics[topic].push_back(std::move(s));

        return handle;
    }

    void unsubscribe(std::string topic, int handle) {

        std::lock_guard<std::mutex> lock(topicLock);

        if (topics.find(topic) == topics.end()) {
            std::cout << "Topic not found " << std::endl;
            return;
        }

        auto& v = topics[topic];
        
        // Find the subscriber with matching handle
        auto it = std::find_if(v.begin(), v.end(), 
            [handle](const std::unique_ptr<Subscriber>& s) {
                return s->handle == handle;
            });
        
        if (it != v.end()) {
            // grab thread id before erasing
            int thread_id = (*it)->thread_id;

            // remove from topics (this will delete the unique_ptr)
            v.erase(it);

            // clear work queue
            std::lock_guard<std::mutex> lockI(*workQueueLock[thread_id]);

            while (!workQueue[thread_id].empty()) {
                workQueue[thread_id].pop();
            }

            // free back to the pool
            freeList.push(thread_id);
        } 
    }

private:
    std::map<std::string, std::deque<std::unique_ptr<Subscriber>>> topics;
    std::map<std::string, int> topicHandles;
    std::mutex topicLock;
    std::queue<int> freeList;
    std::mutex threadPoolLock;

    std::vector<std::thread> threads;
    
    // Thread startup synchronization
    int readyCount = 0;
    std::mutex startupLock;
    std::condition_variable startupCV;
    
    // Shutdown flag
    std::atomic<bool> shutdown{false};

    const size_t NUM_THREADS;
    std::vector<std::queue<std::pair<std::function<void(unsigned char*)>, unsigned char*>>> workQueue;
    std::vector<std::unique_ptr<std::mutex>> workQueueLock;
    std::vector<std::unique_ptr<std::condition_variable>> workQueueCV;

    void thread_fn(int id) {
        
        // Signal that this thread is ready
        {
            std::lock_guard<std::mutex> lock(startupLock);
            readyCount++;
            startupCV.notify_one();
        }

        while (!shutdown) {
            std::unique_lock<std::mutex> lock(*workQueueLock[id]);

            (*workQueueCV[id]).wait(lock, [this, id]() {return !workQueue[id].empty() || shutdown;});
            
            if (shutdown) break;

            auto [fn, msg] = workQueue[id].front();
            workQueue[id].pop();

            fn(msg);
        }
    }
};