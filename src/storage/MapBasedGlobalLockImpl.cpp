#include "MapBasedGlobalLockImpl.h"

#include <mutex>

namespace Afina {
namespace Backend {

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Put(const std::string &key, const std::string &value) {
	if ((key.size() + value.size()) > _max_size) {
		return false;
	}
	std::lock_guard<std::mutex> lock(_general_mutex);
	auto it = _backend.find(key);
	if (it != _backend.end()) {
		return Set(it->second, value);
	}

	return PutFast(key, value);
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::PutIfAbsent(const std::string &key, const std::string &value) {
	if ((key.size() + value.size()) > _max_size) {
		return false;
	}
	std::lock_guard<std::mutex> lock(_general_mutex);
	auto it = _backend.find(key);
	if (it != _backend.end()) {
		return false;
	}

	return PutFast(key, value);
}

bool MapBasedGlobalLockImpl::PutFast(const std::string &key, const std::string &value) {
	auto e = _list.Put(key, value);
	_backend[e->key] = e;
	_size += e->size();

	Trim();

	return true;
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Set(const std::string &key, const std::string &value) {
	if ((key.size() + value.size()) > _max_size) {
		return false;
	}
	std::lock_guard<std::mutex> lock(_general_mutex);
	auto it = _backend.find(key);
	if (it == _backend.end()) {
		return false;
	}

	Set(it->second, value);

	return true;
}

bool MapBasedGlobalLockImpl::Set(LinkedList::Entry* e, const std::string& value) {
	_size -= e->size();
	e->value = value;
	_size += e->size();
	_list.Up(e);

	Trim();

	return true;
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Delete(const std::string &key) {
	std::lock_guard<std::mutex> lock(_general_mutex);
	return DeleteUnsafe(key);
}

bool MapBasedGlobalLockImpl::DeleteUnsafe(const std::string &key) {
	auto it = _backend.find(key);
	if (it == _backend.end()) {
		return false;
	}

	_size -= it->second->size();
	_list.Delete(it->second);
	_backend.erase(it);

	return true;
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Get(const std::string &key, std::string &value) const {
	std::lock_guard<std::mutex> lock(_general_mutex);
	auto it = _backend.find(key);
	if (it == _backend.end()) {
		return false;
	}

	value = it->second->value;
	_list.Up(it->second);

	return true;
}

void MapBasedGlobalLockImpl::Trim() {
	while (_size > _max_size) {
		DeleteUnsafe(_list.Tail()->key);
	}
}

} // namespace Backend
} // namespace Afina
