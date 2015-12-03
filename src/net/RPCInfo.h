/*

 * RPCInfo.h
 *
 *  Created on: Dec 2, 2015
 *      Author: tzhou
 */

#ifndef NET_RPCINFO_H_
#define NET_RPCINFO_H_

namespace dsm {

struct RPCInfo{
	int source;
	int dest;
	int tag;
};

} //namespace dsm

#endif /* NET_RPCINFO_H_ */
