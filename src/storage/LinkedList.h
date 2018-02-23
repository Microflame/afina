#ifndef AFINA_STORAGE_LINKED_LIST_H
#define AFINA_STORAGE_LINKED_LIST_H

#include <string>

namespace Afina {
namespace Backend {

/**
 * # Simple linked list
 *
 *
 */
class LinkedList {
public:
    class Entry;

    LinkedList() {
        header.next = &header; // First element (Head)
        header.prev = &header; // Last element (Tail)
    }

    ~LinkedList() {
        auto e = header.next;
        while (e != &header) {
            auto next = e->next;
            delete e;
            e = next;
        }
    }

    Entry* Put(const std::string& key, const std::string& value) {
        Entry* e = new Entry(key, value, header.next, &header);
        header.next->prev = e;
        header.next = e;
        return e;
    }

    Entry* Tail() const {
        return header.prev;
    }

    void Up(Entry* e) {
        e->next->prev = e->prev;
        e->prev->next = e->next;

        e->next = header.next;
        e->prev = &header;
        header.next->prev = e;
        header.next = e;
    }

    void Delete(Entry* e) {
        e->prev->next = e->next;
        e->next->prev = e->prev;
        delete e;
    }

    class Entry {
        friend class LinkedList;
    public:
        Entry(const std::string& key, const std::string& value, Entry* next, Entry* prev) : key(key), value(value), next(next), prev(prev) {}

        const std::string& key;
        std::string value;

        size_t size() {
            return key.size() + value.size();
        }

    private:
        Entry() : key("HEADER") {}

        Entry* next;
        Entry* prev;
    };

private:
    Entry header = Entry();
};

} // namespace Backend
} // namespace Afina

#endif // AFINA_STORAGE_LINKED_LIST_H
