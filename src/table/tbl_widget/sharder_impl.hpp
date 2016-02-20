/*
 * sharder.h
 *
 *  Created on: Dec 3, 2015
 *      Author: tzhou
 */

#ifndef KERNEL_SHARDER_IMPL_HPP_
#define KERNEL_SHARDER_IMPL_HPP_

#include <string>
#include <stdint.h>

#include "sharder.h"

namespace dsm {

#ifndef SWIG

// Commonly used sharding operators.
struct Sharders{
	struct String: public Sharder<std::string> {
		int operator()(const std::string& k, int shards){
			//return StringPiece(k).hash() % shards;
			return std::hash<std::string>()(k) % shards;
		}
	};
	struct Mod_str: public Sharder<std::string> {   //only for simrank
		int operator()(const std::string& k, int shards){
			size_t pos = k.find("_");
			pos = std::stoi(k.substr(0, pos));
			return (pos % shards);
		}
	};
	struct Mod: public Sharder<int> {
		int operator()(const int& key, int shards){
			return key % shards;
		}
	};

	struct UintMod: public Sharder<uint32_t> {
		int operator()(const uint32_t& key, int shards){
			return key % shards;
		}
	};
};

#endif

} /* namespace dsm */

#endif /* KERNEL_SHARDER_IMPL_HPP_ */
