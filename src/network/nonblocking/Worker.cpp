#include "Worker.h"

#include <iostream>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <unistd.h>
#include <errno.h>

#include "Utils.h"
#include "Connection.h"


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

    while (worker.running.load()) {
        int n_fds_ready = 0;
        if ((n_fds_ready = epoll_wait(epfd, events_buffer, EPOLL_MAX_EVENTS, -1)) == -1) {
            throw std::runtime_error("Worker failed to do epoll_wait");
        }

        for (int i = 0; i < n_fds_ready; ++i) {
            int client_socket = 0;
            if (events_buffer[i].data.ptr == NULL) { // put in loop
                if (((client_socket = accept(server_socket, NULL, NULL)) == -1) && ((errno != EWOULDBLOCK) || (errno != EAGAIN))) {
                    close(server_socket);
                    if (worker.running.load()) {
                        throw std::runtime_error("Worker failed to accept");
                    }
                    return NULL;
                }
                make_socket_non_blocking(client_socket);
                event.events = EPOLLIN | EPOLLHUP | EPOLLERR;
                auto connection = new Connection(client_socket, worker.pStorage, worker.running); // todo: fix memory leak;
                event.data.ptr = connection;
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_socket, &event) == -1) {
                    throw std::runtime_error("Worker failed to assign client socket to epoll");
                }
            } else {
                Connection& connection = *static_cast<Connection*>(events_buffer[i].data.ptr);
                if (events_buffer[i].events & (EPOLLERR | EPOLLHUP)) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, client_socket, NULL);
                    connection.close_connection();
                    delete &connection;
                } else if (events_buffer[i].events & EPOLLIN) {
                    if (connection.read() == -1) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, client_socket, NULL);
                        connection.close_connection();
                        delete &connection;
                    } 
                } else {
                    connection.close_connection();
                    throw std::runtime_error("unknown event");
                }
            }
        }
    }


    // TODO: implementation here
    // 1. Create epoll_context here
    // 2. Add server_socket to context
    // 3. Accept new connections, don't forget to call make_socket_nonblocking on
    //    the client socket descriptor
    // 4. Add connections to the local context
    // 5. Process connection events
    //
    // Do not forget to use EPOLLEXCLUSIVE flag when register socket
    // for events to avoid thundering herd type behavior.
}

} // namespace NonBlocking
} // namespace Network
} // namespace Afina
