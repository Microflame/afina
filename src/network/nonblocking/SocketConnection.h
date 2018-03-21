#ifndef AFINA_NETWORK_NONBLOCKING_SOCKET_CONNECTION_H
#define AFINA_NETWORK_NONBLOCKING_SOCKET_CONNECTION_H

#include <sys/socket.h>
#include <unistd.h>

#include <errno.h>
#include "AbstractConnection.h"

namespace Afina {
namespace Network {
namespace NonBlocking {

/**
 * This class represents simple connection with client.
 * Each client has instance of SocketConnection class associated with.
 * Service should be performed here.
 */
class SocketConnection : public AbstractConnection {
public:
    SocketConnection(int client_socket, std::shared_ptr<Afina::Storage> ps, std::atomic<bool>& running)
        : AbstractConnection(client_socket, -1, ps, running) {}
    ~SocketConnection() override {
        close_connection();
    }

    int read_head(int fd, char *buf, size_t len, int flags) override {
        ssize_t bytes_read = recv(fd, buf, len, flags);
        if (bytes_read <= 0) {
            if ((errno == EWOULDBLOCK || errno == EAGAIN) && bytes_read < 0 && running.load()) {
                ret_val = 0;
            } else {
                ret_val = -1;
            }
        }
        return bytes_read;
    }

    int read_body(int fd, char *buf, size_t len, int flags) override {
        ssize_t bytes_read = recv(fd, buf, len, flags);
        if (bytes_read <= 0) {
            if ((errno == EWOULDBLOCK || errno == EAGAIN) && bytes_read < 0 && running.load()) {
                ret_val = 0;
            } else {
                ret_val = -1;
            }
        }
        return bytes_read;
    }

    int send_body(int fd, const char *buf, size_t len, int flags) override {
        ssize_t bytes_sent = send(fd, buf, len, flags);
        if (bytes_sent < 0) {
            if ((errno == EWOULDBLOCK || errno == EAGAIN) && running.load()) {
                ret_val = 0;
            } else {
                ret_val = -1;
            }
        }
        return bytes_sent;
    }

    void close_connection() {
        close(fd);
    }
};

} // namespace NonBlocking
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_NONBLOCKING_SOCKET_CONNECTION_H