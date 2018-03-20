#ifndef AFINA_NETWORK_NONBLOCKING_FIFO_CONNECTION_H
#define AFINA_NETWORK_NONBLOCKING_FIFO_CONNECTION_H

#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include <errno.h>
#include "AbstractConnection.h"

namespace Afina {
namespace Network {
namespace NonBlocking {

/**
 * This class represents simple connection by fifo.
 * There only need for a single instance of FifoConnection.
 */
class FifoConnection : public AbstractConnection {
public:
    FifoConnection(int fifo_fd, std::string fifo_read, std::shared_ptr<Afina::Storage> ps, std::atomic<bool>& running)
        : AbstractConnection(fifo_fd, 0, ps, running), fifo_read(fifo_read) {
            ret_val = 0;
        }
    ~FifoConnection() override {
        close_connection();
    }

    int read_head(int fd, char *buf, size_t len, int flags) override {
        ssize_t bytes_read = read(fd, buf, len);
        return bytes_read;
    }

    int read_body(int fd, char *buf, size_t len, int flags) override {
        ssize_t bytes_read = read(fd, buf, len);
        return bytes_read;
    }

    int send_body(int fd, const char *buf, size_t len, int flags) override {
        ssize_t bytes_sent = len; // todo: implement
        return bytes_sent;
    }

    void close_connection() {
        close(fd);
        unlink(fifo_read.c_str());
    }
private:
    const std::string fifo_read;
};

} // namespace NonBlocking
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_NONBLOCKING_FIFO_CONNECTION_H