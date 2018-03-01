#ifndef AFINA_STORAGE_MAP_BASED_GLOBAL_LOCK_IMPL_H
#define AFINA_STORAGE_MAP_BASED_GLOBAL_LOCK_IMPL_H

#include <unordered_map>
#include <mutex>
#include <string>

#include <functional>

#include <afina/Storage.h>
#include "LinkedList.h"

namespace Afina {
namespace Backend {

/**
 * # Map based implementation with global lock
 *
 *
 */
class MapBasedGlobalLockImpl : public Afina::Storage {
public:
    MapBasedGlobalLockImpl(size_t max_size = 1024) : _max_size(max_size), _size(0) {}
    ~MapBasedGlobalLockImpl() {}

    // Implements Afina::Storage interface
    bool Put(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool PutIfAbsent(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool Set(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool Delete(const std::string &key) override;

    // Implements Afina::Storage interface
    bool Get(const std::string &key, std::string &value) const override;

private:
    bool Set(LinkedList::Entry* e, const std::string& value);
    bool PutFast(const std::string &key, const std::string &value);
    bool DeleteUnsafe(const std::string &key);
    void Trim();

    size_t _max_size;
    std::unordered_map<std::reference_wrapper<const std::string>,
                        LinkedList::Entry*,
                        std::hash<std::string>,
                        std::equal_to<std::string>> _backend;

    mutable LinkedList _list;
    size_t _size;
    mutable std::mutex _general_mutex;
};

} // namespace Backend
} // namespace Afina

#endif // AFINA_STORAGE_MAP_BASED_GLOBAL_LOCK_IMPL_H
