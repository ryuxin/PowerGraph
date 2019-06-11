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

#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>
// #include <graphlab/macros_def.hpp>

#define TEST_FILE_NAME "/lfs/cache_test"
#define TEST_FILE_SIZE (5*1024*1024*1024UL)
#define TEST_FILE_ADDR ((void *)0x7ff968000000)

// Global random reset probability
float RESET_PROB = 0.15;

float TOLERANCE = 1.0E-2;

static int num_node, num_core, id_node = 0;

// The vertex data is just the pagerank value (a float)
typedef float vertex_data_type;
typedef float gather_data_type;

// There is no edge data in the pagerank application
typedef graphlab::empty edge_data_type;

// The graph type is determined by the vertex and edge data types
#ifdef ENABLE_BI_GRAPH 
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type, gather_data_type, graphlab::BI_Alloctor> graph_type;
#else
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;
#endif

/*
 * A simple function used by graph.transform_vertices(init_vertex);
 * to initialize the vertes data.
 */
void init_vertex(graph_type::vertex_type& vertex) { vertex.data() = 1; }



/*
 * The factorized page rank update function extends ivertex_program
 * specifying the:
 *
 *   1) graph_type
 *   2) gather_type: float (returned by the gather function). Note
 *      that the gather type is not strictly needed here since it is
 *      assumed to be the same as the vertex_data_type unless
 *      otherwise specified
 *
 * In addition ivertex program also takes a message type which is
 * assumed to be empty. Since we do not need messages no message type
 * is provided.
 *
 * pagerank also extends graphlab::IS_POD_TYPE (is plain old data type)
 * which tells graphlab that the pagerank program can be serialized
 * (converted to a byte stream) by directly reading its in memory
 * representation.  If a vertex program does not exted
 * graphlab::IS_POD_TYPE it must implement load and save functions.
 */
class pagerank :
  public graphlab::ivertex_program<graph_type, gather_data_type>,
  public graphlab::IS_POD_TYPE {
  float last_change;
public:
  /* Gather the weighted rank of the adjacent page   */
  float gather(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    return ((1.0 - RESET_PROB) / edge.source().num_out_edges()) *
      edge.source().data();
  }

  /* Use the total rank of adjacent pages to update this page */
  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
    const double newval = total + RESET_PROB;
    last_change = std::fabs(newval - vertex.data());
    vertex.data() = newval;
  }

  /* The scatter edges depend on whether the pagerank has converged */
  edge_dir_type scatter_edges(icontext_type& context,
                              const vertex_type& vertex) const {
    if (last_change > TOLERANCE) return graphlab::OUT_EDGES;
    else return graphlab::NO_EDGES;
  }

  /* The scatter function just signal adjacent pages */
  void scatter(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    context.signal(edge.target());
  }
}; // end of factorized_pagerank update functor


/*
 * We want to save the final graph so we define a write which will be
 * used in graph.save("path/prefix", pagerank_writer()) to save the graph.
 */
struct pagerank_writer {
  std::string save_vertex(graph_type::vertex_type v) {
    std::stringstream strm;
    strm << v.id() << "\t" << v.data() << "\n";
    return strm.str();
  }
  std::string save_edge(graph_type::edge_type e) { return ""; }
}; // end of pagerank writer



int main(int argc, char** argv) {
  // Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO);

  // Parse command line options -----------------------------------------------
  graphlab::command_line_options clopts("PageRank algorithm.");
  std::string graph_dir;
  std::string format = "adj";
  std::string exec_type = "synchronous";
  clopts.attach_option("graph", graph_dir,
                       "The graph file. Required ");
  clopts.add_positional("graph");
  clopts.attach_option("format", format,
                       "The graph file format");
  clopts.attach_option("engine", exec_type, 
                       "The engine type synchronous or asynchronous");
  clopts.attach_option("tol", TOLERANCE,
                       "The permissible change at convergence.");
  std::string saveprefix;
  clopts.attach_option("saveprefix", saveprefix,
                       "If set, will save the resultant pagerank to a "
                       "sequence of files with prefix saveprefix");
  clopts.attach_option("nnode", num_node,
                       "number of partitions.");
  clopts.attach_option("ncore", num_core,
                       "number of cores in each partition.");

  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }

  if (graph_dir == "") {
    dc.cout() << "Graph not specified. Cannot continue";
    return EXIT_FAILURE;
  }

  // BI init ------------------------------------------------------
  void *mem;
  struct Mem_layout *layout;
  std::cout << "proc id " << dc.procid() << std::endl;
  id_node = dc.procid();
  thd_set_affinity(pthread_self(), id_node, 1);
  if (!id_node) {
	mem = bi_global_init_master(id_node, num_node, num_core,
				    TEST_FILE_NAME, TEST_FILE_SIZE, TEST_FILE_ADDR, 
				    "graph page rank benchmark");
  } else {
	mem = bi_global_init_slave(id_node, num_node, num_core,
				   TEST_FILE_NAME, TEST_FILE_SIZE, TEST_FILE_ADDR);
	mem_mgr_init();
  }
  layout = (struct Mem_layout *)mem;
  std::cout<< "magic: " << layout->magic <<std::endl;
  // Build the graph ----------------------------------------------------------
  graphlab::BI_Alloctor<graph_type> graph_alloctor;
  graph_type *graph_mem, *graph;
  graph_mem = graph_alloctor.allocate(1);
  graph = new (graph_mem) graph_type(dc, clopts);
  dc.cout() << "Loading graph in format: "<< format << std::endl;
  graph->load_format(graph_dir, format);
  // must call finalize before querying the graph
  graph->finalize();
  dc.cout() << "#vertices: " << graph->num_vertices()
            << " #edges:" << graph->num_edges() << std::endl;

  // Initialize the vertex data
  graph->transform_vertices(init_vertex);
  if (!id_node) {
	  clwb_range(mem, TEST_FILE_SIZE);
	  sleep(10);
	  bi_set_barrier(2);
  } else {
	  bi_wait_barrier(2);
	  clflush_range(mem, TEST_FILE_SIZE);
  }
  std::cout << "++++++++++++  init done ++++++++++++++" << std::endl;
  // Running The Engine -------------------------------------------------------
  graphlab::omni_engine<pagerank> engine(dc, *graph, exec_type, clopts);
  engine.signal_all();
  engine.start();
  const float runtime = engine.elapsed_seconds();
  dc.cout() << "Finished Running engine in " << runtime
            << " seconds." << std::endl;

  // Save the final graph -----------------------------------------------------
  if (saveprefix != "") {
    graph->save(saveprefix, pagerank_writer(),
               false,    // do not gzip
               true,     // save vertices
               false);   // do not save edges
  }

  // Tear-down communication layer and quit -----------------------------------
  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
} // End of main



