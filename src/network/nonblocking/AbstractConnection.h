#ifndef AFINA_NETWORK_NONBLOCKING_ABSTRACT_CONNECTION_H
#define AFINA_NETWORK_NONBLOCKING_ABSTRACT_CONNECTION_H

#include <memory>
#include <string>
#include <cstring>

#include <protocol/Parser.h>
#include <afina/execute/Command.h>
#include <stdexcept>

#include <iostream>

namespace Afina {

// Forward declaration, see afina/Storage.h
class Storage;

namespace Network {
namespace NonBlocking {

class AbstractConnection {
public:
    /**
     * fd is a file descriptor for this connection.
     * finish is an int value that is returned by read() if an internal error occured or runnig is no more set to true.
     */
    AbstractConnection(int fd, int finish, std::shared_ptr<Afina::Storage> ps, std::atomic<bool>& running)
        : fd(fd), finish(finish), pStorage(ps), running(running), position(0), parsing(true) {
        }
    virtual ~AbstractConnection() {}

    virtual int read_head(int fd, char *buf, size_t len, int flags) = 0;

    virtual int read_body(int fd, char *buf, size_t len, int flags) = 0;

    virtual int send_body(int fd, const char *buf, size_t len, int flags) = 0;

    int Read() {
        bool error = false;

        while (!error && running.load()) {
            std::string out;
            try {
                if (parsing) {
                size_t parsed = 0;
                while (!parser.Parse(buffer, position, parsed)) {
                    std::memmove(buffer, buffer + parsed, position - parsed);
                    position -= parsed;
    
                    ssize_t bytes_read = read_head(fd, buffer + position, BUFFER_CAPACITY - position, 0);
                    if (bytes_read <= 0) {
                        return ret_val;
                    }
                    position += bytes_read;
                }
                std::memmove(buffer, buffer + parsed, position - parsed);
                position -= parsed;
    
                cmd = parser.Build(body_size);
                body_size += 2;
                parser.Reset();  
                }
                
                parsing = false;

                body.clear();
                if (body_size > 2) {
                    while (body_size > position) {
                        body_size -= position;
                        body.append(buffer, position);
                        ssize_t bytes_read = read_body(fd, buffer, BUFFER_CAPACITY, 0);
                        if (bytes_read <= 0) {
                            return ret_val;
                        }
                        position = bytes_read;
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

            parsing = true;
    
            if (out.size()) {
                size_t bytes_sent_total = 0;
                while (bytes_sent_total < out.size()) {
                    ssize_t bytes_sent = send_body(fd, out.data() + bytes_sent_total, out.size() - bytes_sent_total, 0);
                    if (bytes_sent <= 0) {
                        return ret_val;
                    }
                    bytes_sent_total += bytes_sent;
                }
            }
        }
        return finish;
    }

    const int fd;

protected:
    int ret_val;
    std::atomic<bool>& running;
private:
    Protocol::Parser parser;
    std::shared_ptr<Afina::Storage> pStorage;
    static const size_t BUFFER_CAPACITY = 2048;
    char buffer[BUFFER_CAPACITY];
    const int finish;
    ssize_t position = 0;

    bool parsing = 0;
    std::string body;
    uint32_t body_size;
    std::unique_ptr<Execute::Command> cmd;
};

} // namespace NonBlocking
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_NONBLOCKING_ABSTRACT_CONNECTION_H