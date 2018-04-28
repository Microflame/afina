#ifndef AFINA_STORAGE_MAP_BASED_FLAT_COMBINE_IMPL_H
#define AFINA_STORAGE_MAP_BASED_FLAT_COMBINE_IMPL_H

#include <unordered_map>
#include <string>

#include <functional>

#include <afina/Storage.h>
#include "LinkedList.h"
#include "FlatCombiner.h"
#include "ThreadLocal.h"

namespace Afina {
namespace Backend {

class MapBasedFlatCombineImpl;

struct Node {
    enum OpCode {
        Get, Set, Put, PutIfAbsent, Delete
    };

    OpCode opcode;
    const std::string *key;
    std::string *value;
    MapBasedFlatCombineImpl *fc;

    bool result;
};

/**
 * # Map based flat combine implementation
 *
 *
 */
class MapBasedFlatCombineImpl : public Afina::Storage {
public:
    MapBasedFlatCombineImpl(size_t max_size = 1048576) : _max_size(max_size), _size(0), flat_combiner(combine) {}
    ~MapBasedFlatCombineImpl() {}

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
    static void combine(FlatCombiner<Node>::Slot **start, FlatCombiner<Node>::Slot **end);

    bool _Put(const std::string &key, const std::string &value);
    bool _PutIfAbsent(const std::string &key, const std::string &value);
    bool _Get(const std::string &key, std::string &value) const;
    bool _Set(const std::string &key, const std::string &value);
    bool _Set(LinkedList::Entry* e, const std::string& value);
    bool _Delete(const std::string &key);
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
    mutable FlatCombiner<Node> flat_combiner;
};

} // namespace Backend
} // namespace Afina

#endif // AFINA_STORAGE_MAP_BASED_FLAT_COMBINE_IMPL_H
