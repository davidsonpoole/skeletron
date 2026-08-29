#include <mutex>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
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
            topicToPublishers[topic].erase(fd);
            if (topicToPublishers[topic].size() == 0) {
                topicToPublishers.erase(topic);
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

void run_accept_loop(int fd) {

    while (running) {
        int client_fd = accept(fd, nullptr, nullptr);
        if (client_fd == -1) {
            std::cerr << "Error on accept" << std::endl; 
            continue; // don't crash the whole server on one bad accept
        }

        std::cout << "New client connected (fd=" << client_fd << ")" << std::endl;
        std::thread(handle_client, client_fd).detach();
    }
}

int main() {
    int unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (unix_fd == -1) { 
        std::cerr << "Socket error" << std::endl; 
        return 1; 
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    unlink(SOCKET_PATH);

    if (bind(unix_fd, (sockaddr*)&addr, sizeof(addr)) == -1) {
        std::cerr << "Error on bind" << std::endl; 
        close(unix_fd);
        return 1;
    }

    if (listen(unix_fd, /*backlog=*/16) == -1) {
        std::cerr << "Error on listen" << std::endl; 
        close(unix_fd);
        return 1;
    }

    std::cout << "Listening on " << SOCKET_PATH << std::endl;

    int inet_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (inet_fd == -1) { 
        std::cerr << "Socket error" << std::endl; 
        return 1; 
    }

    sockaddr_in inetAddr{};
    inetAddr.sin_family = AF_INET;
    inetAddr.sin_port = htons(7001);
    inetAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(inet_fd, (sockaddr*)&inetAddr, sizeof(inetAddr)) == -1) {
        std::cerr << "Error on bind" << std::endl; 
        close(inet_fd);
        return 1;
    }

    if (listen(inet_fd, /*backlog=*/16) == -1) {
        std::cerr << "Error on listen" << std::endl; 
        close(inet_fd);
        return 1;
    }

    std::cout << "Listening on " << ntohs(inetAddr.sin_port) << std::endl;

    std::thread unix_thread(run_accept_loop, unix_fd);
    std::thread inet_thread(run_accept_loop, inet_fd);

    unix_thread.join();
    inet_thread.join();

    close(unix_fd);
    close(inet_fd);
    unlink(SOCKET_PATH);
    return 0;
}
