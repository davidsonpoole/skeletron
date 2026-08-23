#include <unistd.h>
#include <sys/socket.h>
#include <iostream>

#define TOPIC_LENGTH 12

enum class MessageType : char {
    Publish = 'P',
    Subscribe = 'S',
    Unsubscribe = 'U'
};

struct Message {
    MessageType msgType;
    char topic[TOPIC_LENGTH];
    int msgLen;
    char* msg;
};

inline bool read_exact(int fd, void* buf, size_t len) {
    char *p = static_cast<char*>(buf);
    size_t total = 0;
    while (total < len) {
        ssize_t n = read(fd, p + total, len - total);
        if (n > 0) {
            total += n;
        } else if (n == 0) {
            // connection closed
            return false;
        } else {
            if (errno == EINTR) continue;
            return false;
        }
    }
    return true;
}

inline bool writeExact(int fd, const void* buf, size_t len) {
    size_t total = 0;
    const char* p = static_cast<const char*>(buf);
    while (total < len) {
        ssize_t n = write(fd, p + total, len - total);
        if (n < 0) {
            if (errno == EINTR) continue; // interrupted, just retry
            return false; // real error
        }
        if (n == 0) return false; // shouldn't normally happen for write(), but be safe
        total += static_cast<size_t>(n);
    }
    return true;
}