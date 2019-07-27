#ifndef BI_FLUSH_HPP
#define BI_FLUSH_HPP

#include <iostream>
#include <limits>
#include "bi_stub.hpp"

#ifdef ENABLE_BI_AUTO_FLUSH
namespace graphlab {
	struct thread_data {
		int nd, cd, ncore;
	} __attribute__((aligned(CACHE_LINE), packed));

	void
	flush_wlogs(void)
	{
		int i, ncpu;
		ncpu = get_active_core_num();
		for(i=0; i<ncpu; i++) {
			bi_wlog_flush(i);
		}
	}

	void
	server_loop(void)
	{
		uint64_t flush_prev, tsc_prev, curr;

		flush_prev = bi_local_rdtsc();
		tsc_prev   = bi_local_rdtsc();
		while (running_cores) {
			curr = bi_local_rdtsc();
			if (curr - flush_prev > QUISE_FLUSH_PERIOD) {
				//std::cout<<"dbg bi peroid flush"<<std::endl;
				flush_wlogs();
				//bi_time_flush();
				//bi_smr_flush();
				flush_prev = curr;
			}
			if (curr - tsc_prev > GLOBAL_TSC_PERIOD) {
				if (NODE_ID() == 0) bi_global_rtdsc();
				else clflush_range(&global_layout->time, CACHE_LINE);
				tsc_prev = curr;
			}
		}
	}

	void
	server_run(void *arg)
	{
		struct thread_data *mythd = (struct thread_data *)arg;
		thd_set_affinity(pthread_self(), mythd->nd, mythd->cd);
		bi_local_init_server(mythd->cd, mythd->ncore);
		server_loop();
	}

	void
	start_bi_server(int nd, int ncore)
	{
		struct thread_data thdarg;
		pthread_t pthd;
		int r;

		thdarg.nd    = nd;
		thdarg.cd    = 0;
		thdarg.ncore = ncore;
		r = pthread_create(&pthd, 0, server_run, &thdarg);
		if (r) {
			std::cout<<"create bi server node "<<nd<<" fail"<<std::endl;
			exit(-1);
		}
	}
}; //namespace  
#endif

#endif

