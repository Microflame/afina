#pragma once

#include <functional>
#include <atomic>
#include <cassert>
#include <iostream>

#include "ThreadLocal.h"

/**
 * Create new flat combine synchronizaion primitive
 *
 * @template_param OpNode
 * Class for a single pending operation descriptor. Must provides following API:
 * - complete() returns true is operation gets completed and false otherwise
 * - error(const std::exception &ex) set operation as failed. After this call return,
 *   subsequent calls to complete() method must return true
 *
 * @template_param QMS
 * Maximum array size that could be passed to a single Combine function call
 */
template <typename OpNode, std::size_t QMS = 64> class FlatCombiner {
    struct Slot;
public:
    // User defined type for the pending operations description, must be plain object without
    // virtual functions
    using pending_operation = OpNode;

    // Function that combine multiple operations and apply it onto data structure
    using combiner = std::function<void(Slot **, Slot **)>;

    // Maximum number of pernding operations could be passed to a single Combine call
    static const std::size_t max_call_size = QMS;
    
    /**
     * @param Combine function that aplly pending operations onto some data structure. It accepts array
     * of pending ops and allowed to modify it in any way except delete pointers
     */
    FlatCombiner(std::function<void(Slot **, Slot **)> combine) : _slot(nullptr, Destructor), _combine(combine) {
        _header.next_and_alive.store((uint64_t) &_header, std::memory_order_relaxed);
        _lock.store(0);
    }
    ~FlatCombiner() { /* dequeue all slot, think about slot deletition */ }

    /**
     * Return pending operation slot to the calling thread, object stays valid as long
     * as current thread is alive or got called detach method
     */
    Slot *get_slot() {
        Slot *result = _slot.get();
        if (result == nullptr) {
            result = new Slot();
            // TODO: setup usage bit in the pointer
            // result->next_and_alive.store(LCK_BIT_MASK);
            _slot.set(result);
        }

        return result;
    }

    /**
     * Put pending operation in the queue and try to execute it. Method gets blocked until
     * slot gets complete, in other words until slot.complete() returns false
     */
    void apply_slot(Slot &slot) {
        // TODO: assert slot params
        // TODO: enqueue slot if needs
        // TODO: try to become executor (cquire lock)
        // TODO: scan qeue, dequeue stale nodes, prepare array to be passed
        // to Combine call
        // TODO: call Combine function
        // TODO: unlock
        // TODO: if lock fails, do thread_yeild and goto 3 TODO
        
        slot.done.store(false, std::memory_order_release);

        while (!slot.done.load(std::memory_order_acquire)) {
            if (!(slot.next_and_alive.load(std::memory_order_relaxed) & Slot::PTR_MASK)) {
                auto head = _header.next_and_alive.load(std::memory_order_relaxed);
                do {
                    slot.next_and_alive.store((uint64_t)head | Slot::ALV_MASK, std::memory_order_relaxed);
                } while(!_header.next_and_alive.compare_exchange_weak(head, (uint64_t) &slot,
                    std::memory_order_release, std::memory_order_relaxed));
            }

            uint64_t gen = try_lock(std::memory_order_acquire, std::memory_order_relaxed);
            if (gen) {
                gen++;
                size_t combine_size = 0;
                auto current_slot = _header.next();
                while (current_slot != &_header) {
                    if ((!current_slot->done.load(std::memory_order_acquire)) && (combine_size < QMS)) { // even consume would work
                        _combine_shot[combine_size++] = current_slot;
                        current_slot->generation = gen;
                    }
                    current_slot = current_slot->next();
                }

                _combine(&_combine_shot.front(), &_combine_shot.front() + combine_size);

                // cleanup
                auto parent = _header.next();
                current_slot = parent->next();
                while (current_slot != &_header) {
                    if ((current_slot->generation + 5) < gen) {
                        auto next = current_slot->next();
                        dequeue_slot(parent, current_slot);
                        current_slot = next;
                    } else {
                        parent = current_slot;
                        current_slot = current_slot->next();
                    }
                }
                unlock(std::memory_order_release, std::memory_order_relaxed);
            } else {
                pthread_yield();
            }
        }
    }

    /**
     * Detach calling thread from this flat combiner, in other word
     * destroy thread slot in the queue
     */
    void detach() {
        pending_operation *result = _slot.get();
        if (result != nullptr) { // do we really need check this
            _slot.set(nullptr);
        }
        orphan_slot(result);
    }

    // Extend user provided pending operation type with fields required for the
    // flat combine algorithm to work
    using Slot = struct Slot {
        static constexpr uint64_t ALV_MASK = uint64_t(1) << 63L;
        static constexpr uint64_t PTR_MASK = uint64_t(-1) >> 16L;
        static constexpr uint64_t DAT_MASK = ~(PTR_MASK);
        // User pending operation to be complete
        OpNode user_op;

        // When last time this slot was detected as been in use
        uint64_t generation;

        // Pointer to the next slot. One bit of pointer is stolen to
        // mark if owner thread is still alive, based on this information
        // combiner/thread_local destructor able to take decission about
        // deleting node.
        //
        // So if stolen bit is set then the only reference left to this slot
        // if the queue. If pointer is zero and bit is set then the only ref
        // left is thread_local storage. If next is zero there are no
        // link left and slot could be deleted
        std::atomic<uint64_t> next_and_alive;

        std::atomic<bool> done;

        /**
         * Remove alive bit from the next_and_alive pointer and return
         * only correct pointer to the next slot
         */
        Slot *next() {
            // TODO: implement
            Slot *res = (Slot*)(next_and_alive.load(std::memory_order_relaxed) & PTR_MASK);
            return res;
        }
    };

protected:
    /**
     * Try to acquire "lock", in case of success returns current generation. If
     * fails the return 0
     *
     * @param suc memory barier to set in case of success lock
     * @param fail memory barrier to set in case of failure
     */
    uint64_t try_lock(std::memory_order suc = std::memory_order_seq_cst, std::memory_order fail = std::memory_order_seq_cst) {
        // TODO: implements
        uint64_t exp = 0;
        uint64_t gen = 0;
        uint64_t prev = 0;
        bool succ = 0;
        do {
            prev = exp;
            exp = gen & GEN_VAL_MASK;
            succ = _lock.compare_exchange_strong(exp, LCK_BIT_MASK | gen, suc, fail);
            gen = exp & GEN_VAL_MASK;
        } while (!(succ || (prev == exp)));
        return succ * (gen + 1);
    }

    /**
     * Try to release "lock". Increase generation number in case of sucess
     *
     * @param suc memory barier to set in case of success lock
     * @param fail memory barrier to set in case of failure
     */
    void unlock(std::memory_order suc = std::memory_order_seq_cst, std::memory_order fail = std::memory_order_seq_cst) {
        // TODO: implements
        auto lock_value = _lock.load(std::memory_order_relaxed);
        auto exp = lock_value & GEN_VAL_MASK | LCK_BIT_MASK;
        bool res = _lock.compare_exchange_strong(exp, (exp + 1) & GEN_VAL_MASK, suc, fail);
        assert(res);
    }

    /**
     * Remove slot from the queue. Note that method must be called only
     * under "lock" to eliminate concurrent queue modifications
     *
     */
    void dequeue_slot(Slot *parent, Slot *slot2remove) {
        // TODO: remove node from the queue
        // TODO: set pointer pare of "next" to null, DO NOT modify usage bit
        // TODO: if next == 0, delete pointer

        auto parent_naa = parent->next_and_alive.load(std::memory_order_relaxed);
        auto next_naa = slot2remove->next_and_alive.load(std::memory_order_relaxed);
        auto next_ptr = next_naa & Slot::PTR_MASK;
        while (!parent->next_and_alive.compare_exchange_weak(parent_naa, (parent_naa & Slot::DAT_MASK) | next_ptr, 
            std::memory_order_relaxed));
        while (!slot2remove->next_and_alive.compare_exchange_weak(next_naa, next_naa & Slot::DAT_MASK, 
            std::memory_order_relaxed));
        if ((next_naa & Slot::DAT_MASK) == 0) {
            // std::cout << "Slot may be deleted by dequeue_slot (";
            // std::cout << std::hex << std::setfill('0') << std::setw(16) << next_naa << ")" << std::endl;
            delete slot2remove;
        }
    }

    /**
     * Function called once thread owning this slot is going to die or to
     * destory slot in some other way
     *
     * @param Slot pointer to the slot is being to orphan
     */
    void orphan_slot(Slot *) {}

private:
    static constexpr uint64_t LCK_BIT_MASK = uint64_t(1) << 63L;
    static constexpr uint64_t GEN_VAL_MASK = ~LCK_BIT_MASK;

    // First bit is used to see if lock is acquired already or no. Rest of bits is
    // a counter showing how many "generation" has been passed. One generation is a
    // single call of flat_combine function.
    //
    // Based on that counter stale slots found and gets removed from the pending
    // operations queue
    std::atomic<uint64_t> _lock;

    // Pending operations queue. Each operation to be applied to the protected
    // data structure is ends up in this queue and then executed as a batch by
    // flat_combine method call
    // std::atomic<Slot *> _queue;
    
    // Function to call in order to execute operations
    combiner _combine;

    // Usual strategy for the combine flat would be sort operations by some creteria
    // and optimize it somehow. That array is using by executor thread to prepare
    // number of ops to pass to combine
    std::array<Slot *, QMS> _combine_shot;

    // Slot of the current thread. If nullptr then cur thread gets access in the
    // first time or after a long period when slot has been deleted already
    ThreadLocal<Slot> _slot;

    // This slot is pointing to the first element in the list of pending operations.
    // Last slot in the list of pending operations should point to this slot
    // instead of having nullptr. Having nullptr means being extracted from the
    // list of pending operation
    Slot _header;

    static void Destructor(void *arg) {
        Slot* slot = static_cast<Slot*>(arg);

        auto slot_naa = slot->next_and_alive.load(std::memory_order_relaxed);
        while (!slot->next_and_alive.compare_exchange_weak(slot_naa, slot_naa & Slot::PTR_MASK), 
            std::memory_order_relaxed, std::memory_order_relaxed);
        if ((slot_naa & Slot::PTR_MASK) == 0) {
            // std::cout << "Slot may be deleted by Destructor (" << std::hex << slot_naa << ")" << std::endl;
            delete slot;
        }
    }
};





