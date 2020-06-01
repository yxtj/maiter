#include "client/client.h"
#include "examples.h"

using namespace dsm;
using namespace std;


void RegisterExamples()
{
	//REGISTER_RUNNER(Adsorption);

	//REGISTER_RUNNER(Katz);

	REGISTER_RUNNER(Pagerank);
	REGISTER_RUNNER(Shortestpath);
	
	//REGISTER_RUNNER(Simrank);
}


