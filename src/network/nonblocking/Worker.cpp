#include "Worker.h"

#include <iostream>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>
#include <errno.h>

#include "Utils.h"
#include "SocketConnection.h"
#include "FifoConnection.h"

#include <string>
#include <chrono>
#include <thread>

namespace Afina {
namespace Network {
namespace NonBlocking {

// See Worker.h
Worker::Worker(std::shared_ptr<Afina::Storage> ps) : pStorage(ps) {
    // TODO: implementation here
}

// See Worker.h
Worker::~Worker() {
    // TODO: implementation here
}

// See Worker.h
void Worker::Start(int server_socket) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    this->server_socket.store(server_socket);
    running.store(true);
    if (pthread_create(&thread, NULL, OnRun, this) < 0) {
        throw std::runtime_error("Worker failed to strart");
    }
}

// See Worker.h
void Worker::Stop() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    running.store(false);
    shutdown(server_socket, SHUT_RDWR);
}

// See Worker.h
void Worker::Join() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    pthread_join(thread, NULL);
}

void Worker::SetFifo(std::string read, std::string write) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    fifo_read = read;
    fifo_write = write;
}

// See Worker.h
void* Worker::OnRun(void *args) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;

    Worker &worker = *static_cast<Worker*>(args);
    int server_socket = worker.server_socket.load();

    int epfd = 0;
    if ((epfd = epoll_create(EPOLL_MAX_EVENTS)) < 0) {
        throw std::runtime_error("Worker failed to create epoll file descriptor");
    }

    epoll_event event, events_buffer[EPOLL_MAX_EVENTS];

    event.events = EPOLLEXCLUSIVE | EPOLLIN | EPOLLHUP | EPOLLERR;
    event.data.ptr = NULL; // server event has NULL data as identifier. Connection instances associated with clients.

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, server_socket, &event) == -1) {
        throw std::runtime_error("Worker failed to assign sever socket to epoll");
    }

    if (worker.fifo_read != "") {
        mkfifo(worker.fifo_read.c_str(), 0777);
        int fd = open(worker.fifo_read.c_str(), O_RDWR | O_NONBLOCK);
        event.events = EPOLLIN;
        auto connection = new FifoConnection(fd, worker.fifo_read, worker.pStorage, worker.running);
        event.data.ptr = connection;
        worker.connections.emplace(fd, connection);
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) == -1) {
            throw std::runtime_error("Worker failed to assign fifo_read to epoll");
        }
    }

    while (worker.running.load()) {
        int n_fds_ready = 0;
        if ((n_fds_ready = epoll_wait(epfd, events_buffer, EPOLL_MAX_EVENTS, -1)) == -1) {
            throw std::runtime_error("Worker failed to do epoll_wait");
        }

        for (int i = 0; i < n_fds_ready; ++i) {
            int fd = 0;
            if (events_buffer[i].data.ptr == NULL) { // todo: put in loop
                if ((fd = accept(server_socket, NULL, NULL)) == -1) {
                    if ((errno != EWOULDBLOCK) && (errno != EAGAIN)) {
                        close(server_socket);
                        if (worker.running.load()) {
                            throw std::runtime_error("Worker failed to accept");
                        }
                    }
                } else {
                    make_socket_non_blocking(fd);
                    event.events = EPOLLIN | EPOLLHUP | EPOLLERR;
                    auto connection = new SocketConnection(fd, worker.pStorage, worker.running);
                    event.data.ptr = connection;
                    worker.connections.emplace(fd, connection);
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) == -1) {
                        throw std::runtime_error("Worker failed to assign client socket to epoll");
                    }
                }
            } else {
                AbstractConnection& connection = *static_cast<AbstractConnection*>(events_buffer[i].data.ptr);
                fd = connection.fd;

                // char buffer[128];
                // int rd = read(fd, buffer, 128);
                // if (rd > 0) {
                //     std::cout << std::string(buffer, rd) << std::endl;
                // } else {
                //     std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                //     std::cout << "sleeping" << std::endl;
                //     int rd = read(fd, buffer, 128);
                //     if (rd > 0) {
                //         std::cout << "sleepy: " << std::string(buffer, rd) << std::endl;
                //     }
                // }
                // rd = read(fd, buffer, 128);
                // if (rd > 0) {
                //     std::cout << std::string(buffer, rd) << std::endl;
                // } else {
                //     std::this_thread::sleep_for(std::chrono::milliseconds(100));
                //     std::cout << "sleeping" << std::endl;
                //     int rd = read(fd, buffer, 128);
                //     if (rd > 0) {
                //         std::cout << "sleepy: " << std::string(buffer, rd) << std::endl;
                //     }
                // }
                if (events_buffer[i].events & (EPOLLERR | EPOLLHUP)) {
                    std::cout << "EPOLLERR | EPOLLHUP" << std::endl;
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    worker.connections.erase(fd);
                } else if (events_buffer[i].events & EPOLLIN) {
                    if (connection.Read() == -1) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                        worker.connections.erase(fd);
                    } 
                } else {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    worker.connections.erase(fd);
                    throw std::runtime_error("unknown event");
                }
            }
        }
    }

    for (const auto &p : worker.connections) {
        epoll_ctl(epfd, EPOLL_CTL_DEL, p.first, NULL);
    }
    worker.connections.clear();
}

} // namespace NonBlocking
} // namespace Network
} // namespace Afina
