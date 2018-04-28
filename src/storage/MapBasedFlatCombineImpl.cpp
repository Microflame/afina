#include "MapBasedFlatCombineImpl.h"

#include <iostream>

namespace Afina {
namespace Backend {

// See MapBasedFlatCombineImpl.h
bool MapBasedFlatCombineImpl::Put(const std::string &key, const std::string &value) {
    FlatCombiner<Node>::Slot *slot = flat_combiner.get_slot();
    slot->user_op.opcode = Node::OpCode::Put;
    slot->user_op.key = &key;
    slot->user_op.value = const_cast<std::string*>(&value);
    slot->user_op.fc = this;

    flat_combiner.apply_slot(*slot);

    return slot->user_op.result;
}

bool MapBasedFlatCombineImpl::_Put(const std::string &key, const std::string &value) {
    if ((key.size() + value.size()) > _max_size) {
        return false;
    }

    auto it = _backend.find(key);
    if (it != _backend.end()) {
        return _Set(it->second, value);
    }

    return PutFast(key, value);
}

// See MapBasedFlatCombineImpl.h
bool MapBasedFlatCombineImpl::PutIfAbsent(const std::string &key, const std::string &value) {
    FlatCombiner<Node>::Slot *slot = flat_combiner.get_slot();
    slot->user_op.opcode = Node::OpCode::PutIfAbsent;
    slot->user_op.key = &key;
    slot->user_op.value = const_cast<std::string*>(&value);
    slot->user_op.fc = this;

    flat_combiner.apply_slot(*slot);

    return slot->user_op.result;
}

bool MapBasedFlatCombineImpl::_PutIfAbsent(const std::string &key, const std::string &value) {
    if ((key.size() + value.size()) > _max_size) {
     return false;
    }
    auto it = _backend.find(key);
    if (it != _backend.end()) {
     return false;
    }

    return PutFast(key, value);
}

bool MapBasedFlatCombineImpl::PutFast(const std::string &key, const std::string &value) {
    auto e = _list.Put(key, value);
    _backend[e->key] = e;
    _size += e->size();

    Trim();

    return true;
}

// See MapBasedFlatCombineImpl.h
bool MapBasedFlatCombineImpl::Set(const std::string &key, const std::string &value) {
    FlatCombiner<Node>::Slot *slot = flat_combiner.get_slot();
    slot->user_op.opcode = Node::OpCode::Set;
    slot->user_op.key = &key;
    slot->user_op.value = const_cast<std::string*>(&value);
    slot->user_op.fc = this;

    flat_combiner.apply_slot(*slot);

    return slot->user_op.result;
}

bool MapBasedFlatCombineImpl::_Set(const std::string &key, const std::string &value) {
    if ((key.size() + value.size()) > _max_size) {
        return false;
    }
    auto it = _backend.find(key);
    if (it == _backend.end()) {
        return false;
    }

    _Set(it->second, value);

    return true;
}

bool MapBasedFlatCombineImpl::_Set(LinkedList::Entry* e, const std::string& value) {
    _size -= e->size();
    e->value = value;
    _size += e->size();
    _list.Up(e);

    Trim();

    return true;
}

// See MapBasedFlatCombineImpl.h
bool MapBasedFlatCombineImpl::Delete(const std::string &key) {
    FlatCombiner<Node>::Slot *slot = flat_combiner.get_slot();
    slot->user_op.opcode = Node::OpCode::Delete;
    slot->user_op.key = &key;
    slot->user_op.fc = this;

    flat_combiner.apply_slot(*slot);

    return slot->user_op.result;
}

bool MapBasedFlatCombineImpl::DeleteUnsafe(const std::string &key) {
    auto it = _backend.find(key);
    if (it == _backend.end()) {
     return false;
    }

    _size -= it->second->size();
    _list.Delete(it->second);
    _backend.erase(it);

    return true;
}

// See MapBasedFlatCombineImpl.h
bool MapBasedFlatCombineImpl::Get(const std::string &key, std::string &value) const {
    FlatCombiner<Node>::Slot *slot = flat_combiner.get_slot();
    slot->user_op.opcode = Node::OpCode::Get;
    slot->user_op.key = &key;
    slot->user_op.value = &value;
    slot->user_op.fc = const_cast<MapBasedFlatCombineImpl*>(this);

    flat_combiner.apply_slot(*slot);

    return slot->user_op.result;
}

bool MapBasedFlatCombineImpl::_Get(const std::string &key, std::string &value) const {
    auto it = _backend.find(key);
    if (it == _backend.end()) {
        return false;
    }

    value = it->second->value;
    _list.Up(it->second);

    return true;
}

void MapBasedFlatCombineImpl::Trim() {
    while (_size > _max_size) {
        DeleteUnsafe(_list.Tail()->key);
    }
}

void MapBasedFlatCombineImpl::combine(FlatCombiner<Node>::Slot **start, FlatCombiner<Node>::Slot **end) {
    for (auto p = start; p < end; ++p) {
        auto k = (*p)->user_op.key;
        auto v = (*p)->user_op.value;
        auto fc = (*p)->user_op.fc;

        switch ((*p)->user_op.opcode) {
            case (Node::OpCode::Put): {
                (*p)->user_op.result = fc->_Put(*k, *v);
                break;
            }
            case (Node::OpCode::PutIfAbsent): {
                (*p)->user_op.result = fc->_PutIfAbsent(*k, *v);
                break;
            }
            case (Node::OpCode::Get): {
                (*p)->user_op.result = fc->_Get(*k, *((*p)->user_op.value));
                break;
            }
            case (Node::OpCode::Set): {
                (*p)->user_op.result = fc->_Set(*k, *v);
                break;
            }
            case (Node::OpCode::Delete): {
                (*p)->user_op.result = fc->DeleteUnsafe(*k);
                break;
            }
            default:
                break;
        }
        (*p)->done.store(true, std::memory_order_release);
    }
}

} // namespace Backend
} // namespace Afina






