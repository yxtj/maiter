/*
 * SyncUnit.h
 *
 *  Created on: Dec 22, 2015
 *      Author: tzhou
 */

#ifndef MASTER_SYNCUNIT_H_
#define MASTER_SYNCUNIT_H_

#include <condition_variable>

namespace dsm{

struct SyncUnit{
	void wait(){
		ready=false;
		std::unique_lock<std::mutex> ul(m);
		cv.wait(ul,[&](){return ready;});
	}
	void notify(){
		ready=true;
		cv.notify_all();
	}
private:
	std::mutex m;
	std::condition_variable cv;
	bool ready=false;
};

} //namespace dsm;


#endif /* MASTER_SYNCUNIT_H_ */
