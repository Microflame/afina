#ifndef AFINA_NETWORK_NONBLOCKING_CONNECTION_H
#define AFINA_NETWORK_NONBLOCKING_CONNECTION_H

#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>

#include <sys/socket.h>

#include <errno.h>

#include <protocol/Parser.h>
#include <afina/execute/Command.h>

namespace Afina {

// Forward declaration, see afina/Storage.h
class Storage;

namespace Network {
namespace NonBlocking {

/**
 * This class represents simple connection with client.
 * Each client has instance of Connection class associated with.
 * Service should be performed here.
 */
class Connection {
public:
    Connection(int client_socket, std::shared_ptr<Afina::Storage> ps, std::atomic<bool>& running) :
        client_socket(client_socket), pStorage(ps), running(running) {}
    ~Connection() {}

    int read() {
        Protocol::Parser parser;
        bool error = false;
        ssize_t position = 0;

        while (!error && running.load()) {
            std::string out;
            try {
                size_t parsed;
                while (!parser.Parse(buffer, position, parsed)) {
                    std::memmove(buffer, buffer + parsed, position - parsed);
                    position -= parsed;
    
                    ssize_t bytes_read = recv(client_socket, buffer + position, BUFFER_CAPACITY - position, 0);
                    if (bytes_read <= 0) {
                        if ((errno == EWOULDBLOCK || errno == EAGAIN) && bytes_read < 0) {
                            return 0;
                        }
                        return -1;
                    }
                    position += bytes_read;
                }
                std::memmove(buffer, buffer + parsed, position - parsed);
                position -= parsed;
    
                uint32_t body_size;
                auto cmd = parser.Build(body_size);
                parser.Reset();
    
                std::string body;
                if (body_size) {
                    body_size += 2; // trailing '\r\n'
    
                    while (body_size > position) {
                        body_size -= position;
                        body.append(buffer, position);
                        position = recv(client_socket, buffer, BUFFER_CAPACITY, 0);
                        if (position <= 0) {
                            return -1;
                        }
                    }
    
                    body.append(buffer, body_size);
                    std::memmove(buffer, buffer + body_size, position - body_size);
                    position -= body_size;
    
                    body = body.substr(0, body.length() - 2);
                }
    
                cmd->Execute(*pStorage, body, out);
            } catch (std::runtime_error &e) {
                out = std::string("SERVER_ERROR ") + e.what() + std::string("\r\n");
                error = true;
            }
    
            if (out.size()) {
                size_t bytes_sent_total = 0;
                while (bytes_sent_total < out.size()) {
                    ssize_t bytes_sent = send(client_socket, out.data() + bytes_sent_total, out.size() - bytes_sent_total, 0);
                    if (bytes_sent <= 0) {
                        return -1;
                    }
                    bytes_sent_total += bytes_sent;
                }
            }
        }
        return -1;
    }

    void close_connection() {
        close(client_socket);
    }

private:
    std::shared_ptr<Afina::Storage> pStorage;
    std::atomic<bool>& running;
    static const size_t BUFFER_CAPACITY = 2048;
    const int client_socket;
    char buffer[BUFFER_CAPACITY];
};

} // namespace NonBlocking
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_NONBLOCKING_CONNECTION_H