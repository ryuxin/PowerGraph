#ifndef BI_STUB_HPP
#define BI_STUB_HPP

extern "C" {
#include <bi/bi.h>

#define	ENABLE_BI_GRAPH 1

#ifdef ENABLE_BI_GRAPH
#define	ENABLE_BI_AUTO_FLUSH 1
#endif

};

#endif
