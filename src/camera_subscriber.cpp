#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include "messaging.h"

/* This application is responsible for subscribing to data. 
It will connect to the manager node
*/

int main() {
    const char* SOCKET_PATH = "/tmp/my_socket";

    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd == -1) {
        std::cerr << "Socket error" << std::endl;
        return 1;
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock_fd, (sockaddr*)&addr, sizeof(addr)) == -1) {
        std::cerr << "Error on connect" << std::endl;
        close(sock_fd);
        return 1;
    }

    Message msg;
    msg.msgType = MessageType::Subscribe;
    strcpy(msg.topic, "/abc");
    msg.msgLen = 0;
    write(sock_fd, &msg.msgType, 1);
    writeExact(sock_fd, msg.topic, sizeof(msg.topic));
    writeExact(sock_fd, &msg.msgLen, sizeof(msg.msgLen));

    int count{0};

    while (true) {
        
        if (read(sock_fd, &msg.msgType, 1) == 0) {
            break;
        }
        read_exact(sock_fd, msg.topic, sizeof(msg.topic));
        read_exact(sock_fd, &msg.msgLen, sizeof(msg.msgLen));
        msg.msg = (char*) malloc(msg.msgLen);
        read_exact(sock_fd, msg.msg, msg.msgLen);

        // write to disk
        std::ofstream file("frame_" + std::to_string(count++) + ".raw", std::ios::binary);
        if (file.is_open()) {
            file.write(reinterpret_cast<const char*>(msg.msg), msg.msgLen);
        }
        file.close();
    }

    close(sock_fd);
    return 0;
}
