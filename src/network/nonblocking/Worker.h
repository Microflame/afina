#ifndef AFINA_NETWORK_NONBLOCKING_WORKER_H
#define AFINA_NETWORK_NONBLOCKING_WORKER_H

#include <memory>
#include <pthread.h>

#include <unordered_map>
#include <atomic>

#include "AbstractConnection.h"

namespace Afina {

// Forward declaration, see afina/Storage.h
class Storage;

namespace Network {
namespace NonBlocking {

/**
 * # Thread running epoll
 * On Start spaws background thread that is doing epoll on the given server
 * socket and process incoming connections and its data
 */
class Worker {
public:
    Worker(std::shared_ptr<Afina::Storage> ps);
    ~Worker();

    /**
     * Spaws new background thread that is doing epoll on the given server
     * socket. Once connection accepted it must be registered and being processed
     * on this thread
     */
    void Start(int server_socket);

    /**
     * Signal background thread to stop. After that signal thread must stop to
     * accept new connections and must stop read new commands from existing. Once
     * all readed commands are executed and results are send back to client, thread
     * must stop
     */
    void Stop();

    /**
     * Blocks calling thread until background one for this worker is actually
     * been destoryed
     */
    void Join();

    void SetFifo(std::string read, std::string write);
protected:
    /**
     * Method executing by background thread
     */
    static void* OnRun(void *args);

private:
    pthread_t thread;
    std::shared_ptr<Afina::Storage> pStorage;
    std::atomic<int> server_socket;
    std::atomic<bool> running;
    std::unordered_map<int, std::unique_ptr<AbstractConnection>> connections;
    std::string fifo_read;
    std::string fifo_write;

    static const int EPOLL_MAX_EVENTS = 8;
};

} // namespace NonBlocking
} // namespace Network
} // namespace Afina
#endif // AFINA_NETWORK_NONBLOCKING_WORKER_H
