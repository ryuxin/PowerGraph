#ifndef BI_GRAPH_ALLOCATOR_HPP
#define BI_GRAPH_ALLOCATOR_HPP

#include <iostream>
#include <limits>
#include "bi_stub.hpp"

namespace graphlab {
   template <typename T>
   class BI_Alloctor {
     public:
       // type definitions
       typedef T        value_type;
       typedef T*       pointer;
       typedef const T* const_pointer;
       typedef T&       reference;
       typedef const T& const_reference;
       typedef std::size_t    size_type;
       typedef std::ptrdiff_t difference_type;

       // rebind allocator to type U
       template <class U>
       struct rebind {
           typedef BI_Alloctor<U> other;
       };

       // return address of values
       pointer address (reference value) const {
           return &value;
       }
       const_pointer address (const_reference value) const {
           return &value;
       }

       /* constructors and destructor
        * - nothing to do because the allocator has no state
        */
       BI_Alloctor() throw() {
       }
       BI_Alloctor(const BI_Alloctor&) throw() {
       }
       template <class U>
         BI_Alloctor (const BI_Alloctor<U>&) throw() {
       }
       ~BI_Alloctor() throw() {
       }

       // return maximum number of elements that can be allocated
       size_type max_size () const throw() {
           return std::numeric_limits<std::size_t>::max() / sizeof(T);
       }

       // allocate but don't initialize num elements of type T
       pointer allocate (size_type num, const void* = 0) {
           // print message and allocate memory with global new
           // std::cerr << "allocate " << num << " element(s)"
           //          << " of size " << sizeof(T) << std::endl;
           pointer ret = bi_malloc(num*sizeof(T));
           //pointer ret = malloc(num*sizeof(T));
           // std::cerr << " allocated at: " << (void*)ret << std::endl;
           return ret;
       }

       // initialize elements of allocated storage p with value value
       void construct (pointer p, const T& value) {
           // initialize memory with placement new
           new((void*)p)T(value);
       }

       // destroy elements of initialized storage p
       void destroy (pointer p) {
           // destroy objects by calling their destructor
           p->~T();
       }

       // deallocate storage p of deleted elements
       void deallocate (pointer p, size_type num) {
           // print message and deallocate memory with global delete
           // std::cerr << "deallocate " << num << " element(s)"
           //          << " of size " << sizeof(T)
           //          << " at: " << (void*)p << std::endl;
           bi_free((void*)p);
          //if (num <= 16) bi_free((void*)p);
           //free((void*)p);
       }

       template <class T2> bool
       operator==(BI_Alloctor<T2> const&) const { return true; }

       template <class T2> bool
       operator!=(BI_Alloctor<T2> const&) const { return false; }
   };

}; //namespace  

#endif
