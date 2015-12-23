/*
 * SyncUnit.h
 *
 *  Created on: Dec 22, 2015
 *      Author: tzhou
 */

#ifndef DRIVER_TOOLS_SYNCUNIT_H_
#define DRIVER_TOOLS_SYNCUNIT_H_

#include <condition_variable>

namespace dsm{

struct SyncUnit{
	void wait(){
		if(ready)	return;
		std::unique_lock<std::mutex> ul(m);
		if(ready)	return;
		cv.wait(ul,[&](){return ready;});
	}
	void notify(){
		ready=true;
		cv.notify_all();
	}
	void reset(){
		ready=false;
	}
private:
	std::mutex m;
	std::condition_variable cv;
	bool ready=false;
};

} //namespace dsm;


#endif /* DRIVER_TOOLS_SYNCUNIT_H_ */
