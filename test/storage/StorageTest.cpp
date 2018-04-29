#include "gtest/gtest.h"
#include <iostream>
#include <set>
#include <vector>

#include <pthread.h>

#include <storage/MapBasedGlobalLockImpl.h>
#include <storage/MapBasedFlatCombineImpl.h>
#include <afina/execute/Get.h>
#include <afina/execute/Set.h>
#include <afina/execute/Add.h>
#include <afina/execute/Append.h>
#include <afina/execute/Delete.h>

using namespace Afina::Backend;
using namespace Afina::Execute;
using namespace std;

// using Storage = MapBasedGlobalLockImpl;
using Storage = MapBasedFlatCombineImpl;

TEST(StorageTest, PutGet) {
    Storage storage;

    EXPECT_TRUE(storage.Put("KEY1", "val1"));
    EXPECT_TRUE(storage.Put("KEY2", "val2"));

    std::string value;
    EXPECT_TRUE(storage.Get("KEY1", value));
    EXPECT_TRUE(value == "val1");

    EXPECT_TRUE(storage.Get("KEY2", value));
    EXPECT_TRUE(value == "val2");
}

TEST(StorageTest, PutOverwrite) {
    Storage storage(2 * (3 + 1));

    storage.Put("KEY1", "val1");
    storage.Put("KEY1", "val2");

    std::string value;
    EXPECT_TRUE(storage.Get("KEY1", value));
    EXPECT_TRUE(value == "val2");
}

TEST(StorageTest, PutIfAbsent) {
    Storage storage;

    EXPECT_TRUE(storage.PutIfAbsent("KEY1", "val1"));
    EXPECT_FALSE(storage.PutIfAbsent("KEY1", "val2"));

    std::string value;
    EXPECT_TRUE(storage.Get("KEY1", value));
    EXPECT_TRUE(value == "val1");
}

TEST(StorageTest, Delete) {
    Storage storage;

    storage.PutIfAbsent("KEY1", "val1");
    storage.Delete("KEY1");
    storage.PutIfAbsent("KEY1", "val2");

    std::string value;
    EXPECT_TRUE(storage.Get("KEY1", value));
    EXPECT_TRUE(value == "val2");
}

TEST(StorageTest, BigTest) {
    Storage storage(2 * (3 + 6) * 100000);

    std::stringstream ss;

    for(long i=0; i<100000; ++i)
    {
        ss << "Key" << std::setw(6) << std::setfill('0') << i;
        std::string key = ss.str();
        ss.str("");
        ss << "Val" << std::setw(6) << std::setfill('0') << i;
        std::string val = ss.str();
        ss.str("");
        storage.Put(key, val);
    }
    
    for(long i=99999; i>=0; --i)
    {
        ss << "Key" << std::setw(6) << std::setfill('0') << i;
        std::string key = ss.str();
        ss.str("");
        ss << "Val" << std::setw(6) << std::setfill('0') << i;
        std::string val = ss.str();
        ss.str("");
        
        std::string res;
        storage.Get(key, res);

        EXPECT_TRUE(val == res);
    }

}

TEST(StorageTest, MaxTest) {
    Storage storage(2 * (3 + 5) * 1000);

    std::stringstream ss;

    for(long i=0; i<1100; ++i)
    {
        ss << "Key" << std::setw(5) << std::setfill('0') << i;
        std::string key = ss.str();
        ss.str("");
        ss << "Val" << std::setw(5) << std::setfill('0') << i;
        std::string val = ss.str();
        ss.str("");
        storage.Put(key, val);
    }
    
    for(long i=100; i<1100; ++i)
    {
        ss << "Key" << std::setw(5) << std::setfill('0') << i;
        std::string key = ss.str();
        ss.str("");
        ss << "Val" << std::setw(5) << std::setfill('0') << i;
        std::string val = ss.str();
        ss.str("");
        
        std::string res;
        storage.Get(key, res);

        EXPECT_TRUE(val == res);
    }
    
    for(long i=0; i<100; ++i)
    {
        ss << "Key" << std::setw(5) << std::setfill('0') << i;
        std::string key = ss.str();
        ss.str("");
        
        std::string res;
        EXPECT_FALSE(storage.Get(key, res));
    }
}

struct ThreadData {
    size_t thread_id;
    size_t n_elements;
    Storage* storage;
};

void *put_many(void *args) {
    ThreadData tdata = *static_cast<ThreadData*>(args);
    size_t n_elements = tdata.n_elements;
    size_t thread_id = tdata.thread_id;
    Storage& storage = *tdata.storage;

    std::stringstream ss;
    for (int i = 0; i < n_elements; ++i) {
        ss << "Key_" << thread_id << "_" << i;
        std::string key = ss.str();
        ss.str("");
        ss << "Val_" << thread_id << "_" << i;
        std::string val = ss.str();
        ss.str("");
        storage.Put(key, val);
    }
}

void *get_many(void *args) {
    ThreadData tdata = *static_cast<ThreadData*>(args);
    size_t n_elements = tdata.n_elements;
    size_t thread_id = tdata.thread_id;
    Storage& storage = *tdata.storage;

    std::stringstream ss;
    for (int i = 0; i < n_elements; ++i) {
        ss << "Key_" << thread_id << "_" << i;
        std::string key = ss.str();
        ss.str("");
        ss << "Val_" << thread_id << "_" << i;
        std::string val = ss.str();
        ss.str("");
        
        std::string res;
        storage.Get(key, res);

        EXPECT_TRUE(val == res);
    }
}

TEST(StorageTest, MultithreadedPutGet) {
    Storage storage(100000000);

    size_t n_threads = 100;
    size_t n_elements = 1000;
    pthread_t r_thrds[n_threads];
    pthread_t w_thrds[n_threads];
    ThreadData tdata[n_threads];

    for (int i = 0; i < n_threads; ++i) {
        tdata[i].storage = &storage;
        tdata[i].n_elements = n_elements;
        tdata[i].thread_id = i;
    }

    for (int i = 0; i < n_threads; ++i) {
        pthread_create(&w_thrds[i], NULL, put_many, tdata + i);
    }

    for (int i = 0; i < n_threads; ++i) {
        pthread_join(w_thrds[i], NULL);
    }

    for (int i = 0; i < n_threads; ++i) {
        pthread_create(&r_thrds[i], NULL, get_many, tdata + i);
    }

    for (int i = 0; i < n_threads; ++i) {
        pthread_join(r_thrds[i], NULL);
    }
}

void *flood(void *args) {
    ThreadData tdata = *static_cast<ThreadData*>(args);
    size_t n_elements = tdata.n_elements;
    size_t thread_id = tdata.thread_id;
    size_t n_keys = 10;
    Storage& storage = *tdata.storage;

    std::stringstream ss;
    for (int i = 0; i < n_elements; ++i) {
        ss << "Key_" << (i + thread_id) % n_keys;
        std::string key = ss.str();
        ss.str("");
        std::string val = "Val";
        switch ((i + thread_id) % 5) {
            case (0): {
                storage.Put(key, val);
                break;
            }
            case (1): {
                storage.PutIfAbsent(key, val);
                break;
            }
            case (2): {
                storage.Get(key, val);
                break;
            }
            case (3): {
                storage.Set(key, val);
                break;
            }
            case (4): {
                storage.Delete(key);
                break;
            }
        }
    }
}

TEST(StorageTest, MultithreadedPerformanceTest) {
    Storage storage;

    size_t n_threads = 16;
    size_t n_elements = 100000;
    pthread_t thrds[n_threads];
    ThreadData tdata[n_threads];

    for (int i = 0; i < n_threads; ++i) {
        tdata[i].storage = &storage;
        tdata[i].n_elements = n_elements;
        tdata[i].thread_id = i;
    }

    for (int i = 0; i < n_threads; ++i) {
        pthread_create(thrds + i, NULL, flood, tdata + i);
    }

    for (int i = 0; i < n_threads; ++i) {
        pthread_join(thrds[i], NULL);
    }
}