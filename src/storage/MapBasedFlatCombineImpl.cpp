#include "MapBasedFlatCombineImpl.h"

#include <iostream>
#include <cassert>
#include <algorithm>

namespace Afina {
namespace Backend {

int MapBasedFlatCombineImpl::priority[5];

// See MapBasedFlatCombineImpl.h
bool MapBasedFlatCombineImpl::Put(const std::string &key, const std::string &value) {
    if ((key.size() + value.size()) > _max_size) {
        return false;
    }

    FlatCombiner<Node>::Slot *slot = flat_combiner.get_slot();
    slot->user_op.opcode = Node::OpCode::Put;
    slot->user_op.key = &key;
    slot->user_op.value = const_cast<std::string*>(&value);
    slot->user_op.fc = this;

    flat_combiner.apply_slot(*slot);

    return slot->user_op.result;
}

bool MapBasedFlatCombineImpl::_Put(CombineKeyData &data) {
    if ((data.put_called) || (data.delete_called)) {
        data.put_called = true;
        return true;
    }

    data.put_called = true;

    if (data.it != _backend.end()) {
        return _Set(data.it->second, *data.value);
    }

    return _PutFast(data);
}

// See MapBasedFlatCombineImpl.h
bool MapBasedFlatCombineImpl::PutIfAbsent(const std::string &key, const std::string &value) {
    if ((key.size() + value.size()) > _max_size) {
        return false;
    }

    FlatCombiner<Node>::Slot *slot = flat_combiner.get_slot();
    slot->user_op.opcode = Node::OpCode::PutIfAbsent;
    slot->user_op.key = &key;
    slot->user_op.value = const_cast<std::string*>(&value);
    slot->user_op.fc = this;

    flat_combiner.apply_slot(*slot);

    return slot->user_op.result;
}

bool MapBasedFlatCombineImpl::_PutIfAbsent(CombineKeyData &data) {
    if ((data.put_called) || (data.existed_initially) || (data.put_if_absent_called)) {
        data.put_if_absent_called = true;
        return false;
    }

    data.put_if_absent_called = true;

    if ((data.delete_called)) {
        return true;
    }

    if (data.it != _backend.end()) {
        abort();
    }

    return _PutFast(data);
}

bool MapBasedFlatCombineImpl::_PutFast(CombineKeyData &data) {
    auto e = _list.Put(*data.key, *data.value);

    auto it = _backend.insert(data.hint, std::make_pair(std::cref(e->key), e));
    data.it = it;
    data.hint = it;
    _size += e->size();

    _Trim();

    return true;
}

// See MapBasedFlatCombineImpl.h
bool MapBasedFlatCombineImpl::Set(const std::string &key, const std::string &value) {
    if ((key.size() + value.size()) > _max_size) {
        return false;
    }

    FlatCombiner<Node>::Slot *slot = flat_combiner.get_slot();
    slot->user_op.opcode = Node::OpCode::Set;
    slot->user_op.key = &key;
    slot->user_op.value = const_cast<std::string*>(&value);
    slot->user_op.fc = this;

    flat_combiner.apply_slot(*slot);

    return slot->user_op.result;
}

bool MapBasedFlatCombineImpl::_Set(CombineKeyData &data) {
    if (!data.existed_initially) {
        return false;
    }

    if ((data.set_called) || (data.put_called) || (data.delete_called)) {
        data.set_called = true;
        return true;
    }

    data.set_called = true;
    
    if (data.it == _backend.end()) {
        abort();
    }

    _Set(data.it->second, *data.value);

    return true;
}

bool MapBasedFlatCombineImpl::_Set(LinkedList::Entry* e, const std::string& value) {
    _size -= e->size();
    e->value = value;
    _size += e->size();
    _list.Up(e);

    _Trim();

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

bool MapBasedFlatCombineImpl::_Delete(CombineKeyData &data) {
    if (data.delete_called) {
        return false;
    }
    data.delete_called = true;
    if (data.it == _backend.end()) {
        return false;
    }
    data.hint = _backend.begin();
    return _DeleteUnsafe(data.it);
}

bool MapBasedFlatCombineImpl::_DeleteUnsafe(MapBasedFlatCombineImpl::Map::iterator it) {
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

bool MapBasedFlatCombineImpl::_Get(CombineKeyData &data) const {
    if (!data.existed_initially) {
        return false;
    }

    *data.value = data.initial_value;
    _list.Up(data.it->second);

    return true;
}

void MapBasedFlatCombineImpl::_Trim() {
    while (_size > _max_size) {
        auto it = _backend.find(_list.Tail()->key);
        if (it != _backend.end()) {
            _DeleteUnsafe(it);
        }
    }
}

void MapBasedFlatCombineImpl::combine(FlatCombiner<Node>::Slot **start, FlatCombiner<Node>::Slot **end) {
    // static size_t max_combine_n = 0;
    // if ((end - start) > max_combine_n) {
    //     max_combine_n = end - start;
    //     std::cout << "New max combine size: " << max_combine_n << std::endl;
    // }

    struct {
        bool operator()(FlatCombiner<Node>::Slot *a, FlatCombiner<Node>::Slot *b) {
            const std::string &ka = *(a->user_op.key);
            const std::string &kb = *(b->user_op.key);

            if (ka != kb) {
                return ka < kb;
            }

            Node::OpCode aop = a->user_op.opcode;
            Node::OpCode bop = b->user_op.opcode;

            return priority[aop] < priority[bop];
        }
    } prioritized_key_comparator;

    std::sort(start, end, prioritized_key_comparator);

    auto fc = (*start)->user_op.fc;
    auto hint = fc->_backend.begin();
    std::string prev_key = "";
    CombineKeyData cdata;
    for (auto p = start; p < end; ++p) {
        const std::string &cur_key = *((*p)->user_op.key);
        std::string &cur_val = *((*p)->user_op.value);

        if (cur_key != prev_key) {
            prev_key = cur_key;
            cdata = CombineKeyData();
            cdata.it = fc->_backend.find(cur_key);
            cdata.delete_called = false;
            cdata.put_called = false;
            cdata.put_if_absent_called = false;
            cdata.set_called = false;
            cdata.existed_initially = cdata.it != fc->_backend.end();
            if (cdata.existed_initially) {
                cdata.initial_value = cdata.it->second->value;
            }
            cdata.key = &cur_key;
            cdata.hint = hint;
        }
        cdata.value = (*p)->user_op.value;

        switch ((*p)->user_op.opcode) {
            case (Node::OpCode::Delete): {
                (*p)->user_op.result = fc->_Delete(cdata);
                break;
            }
            case (Node::OpCode::Put): {
                (*p)->user_op.result = fc->_Put(cdata);
                break;
            }
            case (Node::OpCode::PutIfAbsent): {
                (*p)->user_op.result = fc->_PutIfAbsent(cdata);
                break;
            }
            case (Node::OpCode::Set): {
                (*p)->user_op.result = fc->_Set(cdata);
                break;
            }
            case (Node::OpCode::Get): {
                (*p)->user_op.result = fc->_Get(cdata);
                break;
            }
            default:
                break;
        }

        if ((cdata.hint != fc->_backend.begin()) && (cdata.hint != fc->_backend.end())) {
            hint = cdata.hint;
        }

        (*p)->done.store(true, std::memory_order_release);
    }
}

} // namespace Backend
} // namespace Afina






