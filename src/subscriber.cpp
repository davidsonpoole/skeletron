#include <sys/socket.h>
#include <sys/un.h>
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

    // 4. Send subscribe message

    Message msg;
    msg.msgType = MessageType::Subscribe;
    strcpy(msg.topic, "/abc");
    msg.msgLen = 0;
    write(sock_fd, &msg.msgType, 1);
    writeExact(sock_fd, msg.topic, sizeof(msg.topic));
    writeExact(sock_fd, &msg.msgLen, sizeof(msg.msgLen));

    // 5. Read/write data

    while (true) {
        
        if (read(sock_fd, &msg.msgType, 1) == 0) {
            break;
        }
        read_exact(sock_fd, msg.topic, sizeof(msg.topic));
        read_exact(sock_fd, &msg.msgLen, sizeof(msg.msgLen));
        msg.msg = (char*) malloc(msg.msgLen);
        read_exact(sock_fd, msg.msg, msg.msgLen);

        std::cout << "Message received. Topic=" << msg.topic << " msg=" << msg.msg << std::endl;
    }

    close(sock_fd);
    return 0;
}