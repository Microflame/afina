#include "gtest/gtest.h"
#include <iostream>
#include <set>
#include <vector>

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

TEST(StorageTest, PutGet) {
    MapBasedFlatCombineImpl storage;

    EXPECT_TRUE(storage.Put("KEY1", "val1"));
    EXPECT_TRUE(storage.Put("KEY2", "val2"));

    std::string value;
    EXPECT_TRUE(storage.Get("KEY1", value));
    EXPECT_TRUE(value == "val1");

    EXPECT_TRUE(storage.Get("KEY2", value));
    EXPECT_TRUE(value == "val2");
}

TEST(StorageTest, PutOverwrite) {
    MapBasedFlatCombineImpl storage(2 * (3 + 1));

    storage.Put("KEY1", "val1");
    storage.Put("KEY1", "val2");

    std::string value;
    EXPECT_TRUE(storage.Get("KEY1", value));
    EXPECT_TRUE(value == "val2");
}

TEST(StorageTest, PutIfAbsent) {
    MapBasedFlatCombineImpl storage;

    EXPECT_TRUE(storage.PutIfAbsent("KEY1", "val1"));
    EXPECT_FALSE(storage.PutIfAbsent("KEY1", "val2"));

    std::string value;
    EXPECT_TRUE(storage.Get("KEY1", value));
    EXPECT_TRUE(value == "val1");
}

TEST(StorageTest, Delete) {
    MapBasedFlatCombineImpl storage;

    storage.PutIfAbsent("KEY1", "val1");
    storage.Delete("KEY1");
    storage.PutIfAbsent("KEY1", "val2");

    std::string value;
    EXPECT_TRUE(storage.Get("KEY1", value));
    EXPECT_TRUE(value == "val2");
}

TEST(StorageTest, BigTest) {
    MapBasedFlatCombineImpl storage(2 * (3 + 6) * 100000);

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
    MapBasedFlatCombineImpl storage(2 * (3 + 5) * 1000);

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