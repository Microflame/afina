#include <afina/Storage.h>
#include <afina/execute/Set.h>

#include <iostream>

// #include <chrono>
// #include <thread>

namespace Afina {
namespace Execute {

// memcached protocol: "set" means "store this data".
void Set::Execute(Storage &storage, const std::string &args, std::string &out) {
    std::cout << "Set(" << _key << "): " << args << std::endl;
    // std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    storage.Put(_key, args);
    out = "STORED";
}

} // namespace Execute
} // namespace Afina
