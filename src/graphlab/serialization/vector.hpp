/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#ifndef GRAPHLAB_SERIALIZE_VECTOR_HPP
#define GRAPHLAB_SERIALIZE_VECTOR_HPP
#include <vector>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <graphlab/serialization/iterator.hpp>


namespace graphlab {
  namespace archive_detail {
    /**
     * We re-dispatch vectors because based on the contained type,
     * it is actually possible to serialize them like a POD
     */
    template <typename OutArcType, typename ValueType, template<typename> typename Graph_alloctor, bool IsPOD>
    struct vector_serialize_impl {
      static void exec(OutArcType& oarc, const ValueType& vec) {
        // really this is an assert false. But the static assert
        // must depend on a template parameter 
        BOOST_STATIC_ASSERT(sizeof(OutArcType) == 0);
        assert(false);
      };
    };
    /**
     * We re-dispatch vectors because based on the contained type,
     * it is actually possible to deserialize them like iarc POD
     */
    template <typename InArcType, typename ValueType, template<typename> typename Graph_alloctor, bool IsPOD>
    struct vector_deserialize_impl {
      static void exec(InArcType& iarc, ValueType& vec) {
        // really this is an assert false. But the static assert
        // must depend on a template parameter 
        BOOST_STATIC_ASSERT(sizeof(InArcType) == 0);
        assert(false);
      };
    };
    
    /// If contained type is not a POD use the standard serializer
    template <typename OutArcType, typename ValueType, template<typename> typename Graph_alloctor>
    struct vector_serialize_impl<OutArcType, ValueType, Graph_alloctor, false > {
      static void exec(OutArcType& oarc, const std::vector<ValueType, Graph_alloctor<ValueType>>& vec) {
        oarc << size_t(vec.size());
        serialize_iterator(oarc,vec.begin(), vec.end());
      }
    };

    /// Fast vector serialization if contained type is a POD
    template <typename OutArcType, typename ValueType, template<typename> typename Graph_alloctor>
    struct vector_serialize_impl<OutArcType, ValueType, Graph_alloctor, true > {
      static void exec(OutArcType& oarc, const std::vector<ValueType, Graph_alloctor<ValueType>>& vec) {
        oarc << size_t(vec.size());
        serialize(oarc, &(vec[0]),sizeof(ValueType)*vec.size());
      }
    };

    /// If contained type is not a POD use the standard deserializer
    template <typename InArcType, typename ValueType, template<typename> typename Graph_alloctor>
    struct vector_deserialize_impl<InArcType, ValueType, Graph_alloctor, false > {
      static void exec(InArcType& iarc, std::vector<ValueType, Graph_alloctor<ValueType>>& vec){
        size_t len;
        iarc >> len;
        vec.clear(); vec.reserve(len);
        deserialize_iterator<InArcType, ValueType>(iarc, std::inserter(vec, vec.end()));
      }
    };

    /// Fast vector deserialization if contained type is a POD
    template <typename InArcType, typename ValueType, template<typename> typename Graph_alloctor>
    struct vector_deserialize_impl<InArcType, ValueType, Graph_alloctor, true > {
      static void exec(InArcType& iarc, std::vector<ValueType, Graph_alloctor<ValueType>>& vec){
        size_t len;
        iarc >> len;
        vec.clear(); vec.resize(len);
        deserialize(iarc, &(vec[0]), sizeof(ValueType)*vec.size());
      }
    };

    
    
    /**
       Serializes a vector */
    template <typename OutArcType, typename ValueType, template<typename> typename Graph_alloctor>
    struct serialize_impl<OutArcType, std::vector<ValueType, Graph_alloctor<ValueType>>, false > {
      static void exec(OutArcType& oarc, const std::vector<ValueType, Graph_alloctor<ValueType>>& vec) {
        vector_serialize_impl<OutArcType, ValueType, Graph_alloctor, 
          gl_is_pod_or_scaler<ValueType>::value >::exec(oarc, vec);
      }
    };
    /**
       deserializes a vector */
    template <typename InArcType, typename ValueType, template<typename> typename Graph_alloctor>
    struct deserialize_impl<InArcType, std::vector<ValueType, Graph_alloctor<ValueType> >, false > {
      static void exec(InArcType& iarc, std::vector<ValueType, Graph_alloctor<ValueType> >& vec){
        vector_deserialize_impl<InArcType, ValueType, Graph_alloctor,  
          gl_is_pod_or_scaler<ValueType>::value >::exec(iarc, vec);
      }
    };
  } // archive_detail
} // namespace graphlab

#endif 

