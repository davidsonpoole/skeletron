#include <chrono>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
#include <iostream>
#include "messaging.h"

/* This application is responsible for publishing data. 
It will connect to the manager node
*/

int main() {
    const char* SOCKET_PATH = "/tmp/my_socket";

    // 1. Create the socket
    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd == -1) {
        std::cerr << "Socket error" << std::endl;
        return 1;
    }

    // 2. Set up the address struct (same path as server)
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    // 3. Connect
    if (connect(sock_fd, (sockaddr*)&addr, sizeof(addr)) == -1) {
        std::cerr << "Error on connect" << std::endl;
        close(sock_fd);
        return 1;
    }

    // 4. Read/write data
    int i=0;
    while (true) {

        Message msg;
        msg.msgType = MessageType::Publish;
        strcpy(msg.topic, "/abc");
        std::string text("Hello from the publisher " + std::to_string(i++));
        msg.msgLen = text.size();
        msg.msg = (char*) malloc(msg.msgLen);
        strcpy(msg.msg, text.c_str());

        write(sock_fd, &msg.msgType, 1);
        writeExact(sock_fd, msg.topic, sizeof(msg.topic));
        writeExact(sock_fd, &msg.msgLen, sizeof(msg.msgLen));
        writeExact(sock_fd, msg.msg, msg.msgLen);

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    close(sock_fd);
    return 0;
}