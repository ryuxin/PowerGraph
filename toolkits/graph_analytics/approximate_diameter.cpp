/*
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

#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <map>
#include <time.h>

#include <graphlab.hpp>

#define TEST_FILE_NAME "/lfs/cache_test"
#define TEST_FILE_SIZE (25*1024*1024*1024UL)
//#define TEST_FILE_ADDR ((void *)0x7ff5a8000000)
#define TEST_FILE_ADDR ((void *)NULL)

typedef std::vector<bool, graphlab::BI_Alloctor<bool>> vec_bool_type;
//typedef std::vector<bool> vec_bool_type;
typedef std::vector<vec_bool_type, graphlab::BI_Alloctor<vec_bool_type>> vec_vec_bool_type;
//typedef std::vector<vec_bool_type> vec_vec_bool_type;
//helper function
float myrand() {
  return static_cast<float>(rand() / (RAND_MAX + 1.0));
}

//helper function to return a hash value for Flajolet & Martin bitmask
size_t hash_value() {
  size_t ret = 0;
  while (myrand() < 0.5) {
    ret++;
  }
  return ret;
}

const size_t DUPULICATION_OF_BITMASKS = 10;

static int num_node, num_core, id_node = 0;

struct vdata {
  //use two bitmasks for consistency
  vec_vec_bool_type bitmask1;
  vec_vec_bool_type bitmask2;
  //indicate which is the bitmask for reading (or writing)
  bool odd_iteration;
  vdata() :
      bitmask1(), bitmask2(), odd_iteration(true) {
  }
  //for exact counting (but needs large memory)
  void create_bitmask(size_t id) {
    vec_bool_type mask1(id + 2, 0);
    mask1[id] = 1;
    bitmask1.push_back(mask1);
    vec_bool_type mask2(id + 2, 0);
    mask2[id] = 1;
    bitmask2.push_back(mask2);
  }
  //for approximate Flajolet & Martin counting
  void create_hashed_bitmask(size_t id) {
    for (size_t i = 0; i < DUPULICATION_OF_BITMASKS; ++i) {
      size_t hash_val = hash_value();
      vec_bool_type mask1(hash_val + 2, 0);
      mask1[hash_val] = 1;
      bitmask1.push_back(mask1);
      vec_bool_type mask2(hash_val + 2, 0);
      mask2[hash_val] = 1;
      bitmask2.push_back(mask2);
    }
  }

  void save(graphlab::oarchive& oarc) const {
    size_t num = bitmask1.size();
    oarc << num;
    for (size_t a = 0; a < num; ++a) {
      size_t size = bitmask1[a].size();
      oarc << size;
      for (size_t i = 0; i < size; ++i)
        oarc << (bool)bitmask1[a][i];
      for (size_t i = 0; i < size; ++i)
        oarc << (bool)bitmask2[a][i];
    }
    oarc << odd_iteration;
  }
  void load(graphlab::iarchive& iarc) {
    bitmask1.clear();
    bitmask2.clear();
    size_t num = 0;
    iarc >> num;
    for (size_t a = 0; a < num; ++a) {
      size_t size = 0;
      iarc >> size;
      vec_bool_type mask1;
      for (size_t i = 0; i < size; ++i) {
        bool element = true;
        iarc >> element;
        mask1.push_back(element);
      }
      bitmask1.push_back(mask1);
      vec_bool_type mask2;
      for (size_t i = 0; i < size; ++i) {
        bool element = true;
        iarc >> element;
        mask2.push_back(element);
      }
      bitmask2.push_back(mask2);
    }
    iarc >> odd_iteration;
  }
};


//helper function to compute bitwise-or
void bitwise_or(vec_vec_bool_type& v1,
    const vec_vec_bool_type& v2) {
#ifdef ENABLE_BI_GRAPH 
	if (v1.size() > v2.size()) return ;
#endif
  for (size_t a = 0; a < v1.size() && a<v2.size(); ++a) {
    while (v1[a].size() < v2[a].size()) {
      v1[a].push_back(false);
    }
    for (size_t i = 0; i < v2[a].size(); ++i) {
      v1[a][i] = v1[a][i] || v2[a][i];
    }
  }
}

struct bitmask_gatherer {
  vec_vec_bool_type bitmask;

  bitmask_gatherer() :
    bitmask() {
  }
  explicit bitmask_gatherer(const vec_vec_bool_type & in_b) :
    bitmask(){
    for(size_t i=0;i<in_b.size();++i){
      bitmask.push_back(in_b[i]);
    }
  }

  //bitwise-or
  bitmask_gatherer& operator+=(const bitmask_gatherer& other) {
    bitwise_or(bitmask, other.bitmask);
    return *this;
  }

  bool operator!=(const bitmask_gatherer& other) {
    return true;
  }

  void save(graphlab::oarchive& oarc) const {
    size_t num = bitmask.size();
    oarc << num;
    for (size_t a = 0; a < num; ++a) {
      size_t size = bitmask[a].size();
      oarc << size;
      for (size_t i = 0; i < size; ++i)
        oarc << (bool)bitmask[a][i];
    }
  }
  void load(graphlab::iarchive& iarc) {
    bitmask.clear();
    size_t num = 0;
    iarc >> num;
    for (size_t a = 0; a < num; ++a) {
      size_t size = 0;
      iarc >> size;
      vec_bool_type mask1;
      for (size_t i = 0; i < size; ++i) {
        bool element = true;
        iarc >> element;
        mask1.push_back(element);
      }
      bitmask.push_back(mask1);
    }
  }
};

//typedef graphlab::distributed_graph<vdata, graphlab::empty> graph_type;
typedef graphlab::distributed_graph<vdata, graphlab::empty, bitmask_gatherer, graphlab::BI_Alloctor> graph_type;

//initialize bitmask
void initialize_vertex(graph_type::vertex_type& v) {
	v.data().create_bitmask(v.id());
}
//initialize bitmask
void initialize_vertex_with_hash(graph_type::vertex_type& v) {
	v.data().create_hashed_bitmask(v.id());
}

//The next bitmask b(h + 1; i) of i at the hop h + 1 is given as:
//b(h + 1; i) = b(h; i) BITWISE-OR {b(h; k) | source = i & target = k}.
class one_hop: public graphlab::ivertex_program<graph_type, bitmask_gatherer>,
    public graphlab::IS_POD_TYPE {
public:
  //gather on out edges
  edge_dir_type gather_edges(icontext_type& context,
      const vertex_type& vertex) const {
    return graphlab::OUT_EDGES;
  }

  //for each edge gather the bitmask of the edge
  bitmask_gatherer gather(icontext_type& context, const vertex_type& vertex,
      edge_type& edge) const {
    if (vertex.data().odd_iteration) {
      return bitmask_gatherer(edge.target().data().bitmask2);
    } else {
      return bitmask_gatherer(edge.target().data().bitmask1);
    }
  }

  //get bitwise-ORed bitmask and switch bitmasks
  void apply(icontext_type& context, vertex_type& vertex,
      const gather_type& total) {
    if (vertex.data().odd_iteration) {
      if (total.bitmask.size() > 0)
        bitwise_or(vertex.data().bitmask1, total.bitmask);
      vertex.data().odd_iteration = false;
    } else {
      if (total.bitmask.size() > 0)
        bitwise_or(vertex.data().bitmask2, total.bitmask);
      vertex.data().odd_iteration = true;
    }
  }

  edge_dir_type scatter_edges(icontext_type& context,
      const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
  void scatter(icontext_type& context, const vertex_type& vertex,
      edge_type& edge) const {
  }
#ifdef ENABLE_BI_GRAPH 
  bool vertex_change_visible(const vertex_type& vertex) const {
    return vertex.has_replica();
  }
#endif
};

//copy the updated bitmask to the other
void copy_bitmasks(graph_type::vertex_type& vdata) {
	if (vdata.data().odd_iteration == false) { //odd_iteration has just finished
		vdata.data().bitmask2 = vdata.data().bitmask1;
	} else {
		vdata.data().bitmask1 = vdata.data().bitmask2;
	}
}

//count the number of vertices reached in the current hop
size_t absolute_vertex_data(const graph_type::vertex_type& vertex) {
    size_t count = 0;
    for (size_t i = 0; i < vertex.data().bitmask1[0].size(); ++i)
      if (vertex.data().bitmask1[0][i])
        count++;
    return count;
}

//count the number of vertices reached in the current hop with Flajolet & Martin counting method
size_t approximate_pair_number(vec_vec_bool_type bitmask) {
  float sum = 0.0;
  for (size_t a = 0; a < bitmask.size(); ++a) {
    for (size_t i = 0; i < bitmask[a].size(); ++i) {
      if (bitmask[a][i] == 0) {
        sum += (float) i;
        break;
      }
    }
  }
  return (size_t) (pow(2.0, sum / (float) (bitmask.size())) / 0.77351);
}
//count the number of notes reached in the current hop
size_t absolute_vertex_data_with_hash(
    const graph_type::vertex_type& vertex) {
    size_t count = approximate_pair_number(vertex.data().bitmask1);
    return count;
}

int main(int argc, char** argv) {
  std::cout << "Approximate graph diameter\n\n";
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;

  std::string datafile;
  float termination_criteria = 0.0001;
  //parse command line
  graphlab::command_line_options clopts(
                "Approximate graph diameter. "
                "Directions of edges are considered.");
  std::string graph_dir;
  std::string format = "adj";
  bool use_sketch = true;
  std::string exec_type = "synchronous";
  clopts.attach_option("graph", graph_dir,
                       "The graph file. This is not optional");
  clopts.add_positional("graph");
  clopts.attach_option("format", format,
                       "The graph file format");
  clopts.attach_option("tol", termination_criteria,
                       "The permissible change at convergence.");
  clopts.attach_option("use-sketch", use_sketch,
                       "If true, will use Flajolet & Martin bitmask, "
                       "which is more compact and faster.");
  clopts.attach_option("nnode", num_node,
		       "number of partitions.");

  if (!clopts.parse(argc, argv)){
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }
  if (graph_dir == "") {
    std::cout << "--graph is not optional\n";
    return EXIT_FAILURE;
  }

  // BI init ------------------------------------------------------
  void *mem;
  struct Mem_layout *layout;
  num_core = clopts.get_ncpus();
  id_node = dc.procid();
  std::cout << "proc id " << dc.procid() << " #nodes " << num_node << " #cores " << num_core <<std::endl;
  if (num_core > NUM_CORE_PER_NODE) thd_set_affinity_to_core(pthread_self(), num_core-1);
  else thd_set_affinity(pthread_self(), id_node, num_core-1);
  if (!id_node) {
       	  mem = bi_global_init_master(id_node, num_node, num_core,
			              TEST_FILE_NAME, TEST_FILE_SIZE, TEST_FILE_ADDR,
				      "approximate diameter");
  } else {
	 mem = bi_global_init_slave(id_node, num_node, num_core,
			            TEST_FILE_NAME, TEST_FILE_SIZE, TEST_FILE_ADDR);
	 mem_mgr_init();
  }
  setup_core_id(num_core-1);
  layout = (struct Mem_layout *)mem;
  std::cout<<"proc "<<dc.procid()<< " magic: " << layout->magic <<std::endl;

  //load graph
  graph_type graph(dc, clopts);
  dc.cout() << "Loading graph in format: "<< format << std::endl;
  graph.load_format(graph_dir, format);
  graph.finalize();

  time_t start, end, tot = 0, ts, te;
  //initialize vertices
  if (use_sketch == false)
    graph.transform_vertices(initialize_vertex);
  else
    graph.transform_vertices(initialize_vertex_with_hash);

  graphlab::omni_engine<one_hop> engine(dc, graph, exec_type, clopts);

#ifdef ENABLE_BI_AUTO_FLUSH
    graphlab::start_bi_server(id_node, num_core-1);
#endif

  //main iteration
  size_t previous_count = 0;
  size_t diameter = 0;
  time(&ts);
  for (size_t iter = 0; iter < 100; ++iter) {
    engine.signal_all();
    time(&start);
    engine.start();
    time(&end);

    graph.transform_vertices(copy_bitmasks);
//    std::cout<<"dbg after ransfor"<<std::endl;

    size_t current_count = 0;
    if (use_sketch == false)  current_count = graph.map_reduce_vertices<size_t>(absolute_vertex_data);
    else current_count = graph.map_reduce_vertices<size_t>(absolute_vertex_data_with_hash);
    dc.cout() << iter + 1 << "-th hop: " << current_count << " vertex pairs are reached time "<<end-start<<" sec\n";
    tot += (end-start);
    if (iter > 0 && 
	(float) current_count < (float) previous_count * (1.0 + termination_criteria)) {
      diameter = iter;
      dc.cout() << "converge\n";
      break;
    }
    previous_count = current_count;
  }
  time(&te);

#ifdef ENABLE_BI_AUTO_FLUSH
    bi_server_stop();
#endif

  dc.cout() << "graph  engine tot "<<tot<<" calculation time is " << te-ts << " sec\n";
  dc.cout() << "The approximate diameter is " << diameter << "\n";

  graphlab::mpi_tools::finalize();

  return EXIT_SUCCESS;
}

