#include <mutex>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <atomic>
#include <csignal>
#include <iostream>
#include <map>
#include <set>
#include "messaging.h"

const char* SOCKET_PATH = "/tmp/my_socket";
std::atomic<bool> running{true};

// Topic manager stuff
std::mutex topicLock;
std::map<std::string, std::set<int>> topicToSubscribers;
std::map<std::string, std::set<int>> topicToPublishers;

std::map<int, std::set<std::string>> clientToSubscribedTopic;

void disconnect(int fd) {

    std::lock_guard<std::mutex> lock(topicLock);

    if (clientToSubscribedTopic.find(fd) != clientToSubscribedTopic.end()) {

        auto& s = clientToSubscribedTopic[fd];

        for (auto& topic : s) {
            topicToSubscribers[topic].erase(fd);
            if (topicToSubscribers[topic].size() == 0) {
                topicToSubscribers.erase(topic);
            }
        }

        clientToSubscribedTopic.erase(fd);
    }

    close(fd);

    std::cout << "Client disconnected (fd=" << fd << ")" << std::endl;
}

void unsubscribe(int fd, char* topic) {

    std::lock_guard<std::mutex> topicL(topicLock);

    topicToSubscribers[topic].erase(fd);

    auto& topics = clientToSubscribedTopic[fd];
    clientToSubscribedTopic[fd].erase(topic);

    // might be unnecessary
    if (clientToSubscribedTopic[fd].size() == 0) {
        clientToSubscribedTopic.erase(fd);
    }
}

bool handlePublish(Message& msg, int client_fd) {
    // read entire message
    read_exact(client_fd, msg.topic, sizeof(msg.topic));
    read_exact(client_fd, &msg.msgLen, sizeof(msg.msgLen));
    msg.msg = (char*) malloc(msg.msgLen);
    read_exact(client_fd, msg.msg, msg.msgLen);

    // find all subscribers
    std::lock_guard<std::mutex> lock(topicLock);
    if (topicToSubscribers.find(msg.topic) == topicToSubscribers.end()) {
        return true;
    }

    auto& v = topicToSubscribers[msg.topic];

    for (auto sub_fd : v) {
        // write to subscriber fd
        write(sub_fd, &msg.msgType, 1);
        write(sub_fd, msg.topic, sizeof(msg.topic));
        write(sub_fd, &msg.msgLen, sizeof(msg.msgLen));
        write(sub_fd, msg.msg, msg.msgLen);
    }
    return true;
}

bool handleSubscribe(Message& msg, int client_fd) {
    read_exact(client_fd, msg.topic, sizeof(msg.topic));
    read_exact(client_fd, &msg.msgLen, sizeof(msg.msgLen));
    if (msg.msgLen != 0) {
        std::cerr << "Subscribe messages must have msgLen of 0!" << std::endl;
        return false;
    }

    std::lock_guard<std::mutex> topicL(topicLock);
    topicToSubscribers[msg.topic].insert(client_fd);
    clientToSubscribedTopic[client_fd].insert(msg.topic);

    std::cout << "Client " << client_fd << " subscribed to topic \"" << msg.topic << "\"" << std::endl;
    return true;
}

bool handleUnsubscribe(Message& msg, int client_fd) {
    read_exact(client_fd, msg.topic, sizeof(msg.topic));
    read_exact(client_fd, &msg.msgLen, sizeof(msg.msgLen));
    if (msg.msgLen != 0) {
        std::cerr << "Unsubscribe messages must have msgLen of 0!" << std::endl;
        return false;
    }

    unsubscribe(client_fd, msg.topic);

    std::cout << "Client " << client_fd << " unsubscribed from topic \"" << msg.topic << "\"" << std::endl;
    return true;

}

void handle_client(int client_fd) {
    char buf[256];
    Message msg;
    while (running) {
        if (read(client_fd, &msg.msgType, 1) == 0) {
            std::cout << "Client disconnected" << std::endl;
            disconnect(client_fd);
            return;
        }

        switch (msg.msgType) {
            case MessageType::Publish:
                // publish
                if (!handlePublish(msg, client_fd)) {
                    disconnect(client_fd);
                    return;
                }
                break;
            case MessageType::Subscribe:
                // subscribe
                if (!handleSubscribe(msg, client_fd)) {
                    disconnect(client_fd);
                    return;
                }
                break;
            case MessageType::Unsubscribe:
                // unsubscribe
                if (!handleUnsubscribe(msg, client_fd)) {
                    disconnect(client_fd);
                    return;
                }
            default:
                // disconnect client
                std::cerr << "Unknown message type" << std::endl;
                disconnect(client_fd);
                return;
        }
    }
    disconnect(client_fd);
}

int main() {
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd == -1) { 
        std::cerr << "Socket error" << std::endl; 
        return 1; 
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    unlink(SOCKET_PATH);

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) == -1) {
        std::cerr << "Error on bind" << std::endl; 
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, /*backlog=*/16) == -1) {
        std::cerr << "Error on listen" << std::endl; 
        close(server_fd);
        return 1;
    }

    std::cout << "Listening on " << SOCKET_PATH << std::endl;

    std::vector<std::thread> threads;

    while (running) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd == -1) {
            std::cerr << "Error on accept" << std::endl; 
            continue; // don't crash the whole server on one bad accept
        }

        std::cout << "New client connected (fd=" << client_fd << ")" << std::endl;
        threads.emplace_back(handle_client, client_fd);
        threads.back().detach(); // let it clean itself up when done
    }

    close(server_fd);
    unlink(SOCKET_PATH);
    return 0;
}