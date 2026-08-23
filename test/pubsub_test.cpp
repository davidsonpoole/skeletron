#include "../src/pubsub.h"
#include <cassert>
#include <mutex>
#include <thread>
#include <chrono>

std::mutex logLock;

int main() {
    std::cout << "Running tests..." << std::endl;
    Manager manager;
    manager.start(); 

    // nothing should happen
    std::cout << "--- Test: Publishing with no subscribers ---" << std::endl;
    manager.publish("/abc", (unsigned char*)"hello");
    std::cout << "Test Passed!" << std::endl << std::endl;;

    // one subscriber
    std::cout << "--- Test: One subscriber ---" << std::endl;
    int handle = manager.subscribe("/abc", [](unsigned char* msg) { 
        std::lock_guard<std::mutex> lock(logLock);
        std::cout << "[1] Message: " << msg << std::endl; 
    });
    assert(handle == 1);
    manager.publish("/abc", (unsigned char*)"First message");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "Test Passed!" << std::endl << std::endl;;

    // five subscribers
    std::cout << "--- Test: Five subscribers ---" << std::endl;
    for (int i=2; i<6; i++) {
        manager.subscribe("/abc", [i](unsigned char* msg) { 
            std::lock_guard<std::mutex> lock(logLock);
            std::cout << "[" << i << "] Message: " << msg << std::endl;
        });
    }

    manager.publish("/abc", (unsigned char*)"Second message");
    manager.publish("/def", (unsigned char*)"Hidden message");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "Test Passed!" << std::endl << std::endl;;

    // unsubscribe 1
    std::cout << "--- Test: Unsubscribe first subscriber ---" << std::endl;
    manager.unsubscribe("/abc", 1);

    manager.publish("/abc", (unsigned char*)"Third message");

    // subscribe 1
    int new_handle = manager.subscribe("/abc", [](unsigned char* msg) {});
    assert(new_handle == 6);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "Test Passed!" << std::endl << std::endl;;
    
    // Give threads time to process the message before destroying manager
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    std::cout << "Test complete" << std::endl;
}