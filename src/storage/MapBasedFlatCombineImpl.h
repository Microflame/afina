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
struct CombineKeyData;

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
    using Map = std::unordered_map<std::reference_wrapper<const std::string>,
                        LinkedList::Entry*,
                        std::hash<std::string>,
                        std::equal_to<std::string>>;
    MapBasedFlatCombineImpl(size_t max_size = 1048576) : _max_size(max_size), _size(0), flat_combiner(combine) {
            priority[Node::OpCode::Delete] = 0;
            priority[Node::OpCode::Put] = 1;
            priority[Node::OpCode::PutIfAbsent] = 2;
            priority[Node::OpCode::Set] = 3;
            priority[Node::OpCode::Get] = 4;
    }
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
    static int priority[5];

    bool _Put(CombineKeyData &data);
    bool _PutIfAbsent(CombineKeyData &data);
    bool _Set(CombineKeyData &data);
    bool _Delete(CombineKeyData &data);
    bool _Get(CombineKeyData &data) const;

    bool _PutFast(CombineKeyData &data);
    bool _Set(LinkedList::Entry* e, const std::string& value);
    bool _DeleteUnsafe(Map::iterator it);
    void _Trim();

    size_t _max_size;
    Map _backend;

    mutable LinkedList _list;
    size_t _size;
    mutable FlatCombiner<Node> flat_combiner;
};

struct CombineKeyData {
    MapBasedFlatCombineImpl::Map::iterator it;
    MapBasedFlatCombineImpl::Map::iterator hint;
    bool delete_called;
    bool put_called;
    bool put_if_absent_called;
    bool set_called;
    bool existed_initially;
    const std::string *key;
    std::string *value;
    std::string initial_value;

};

} // namespace Backend
} // namespace Afina

#endif // AFINA_STORAGE_MAP_BASED_FLAT_COMBINE_IMPL_H
