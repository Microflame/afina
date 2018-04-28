#pragma once

#include <functional>

#include <pthread.h>


/**
 * Create new thread local variable
 */
template <typename T> class ThreadLocal {
public:
    ThreadLocal(T *initial = nullptr, void (*destructor)(void*) = nullptr) {
        pthread_key_create(&_th_key, destructor);
        set(initial);
    }

    inline T *get() {
        T *val = static_cast<T*>(pthread_getspecific(_th_key));
        return val;
    }

    inline void set(T *val) {
        pthread_setspecific(_th_key, val);
    }

    T &operator*() { return *get(); }

private:
    pthread_key_t _th_key;
};