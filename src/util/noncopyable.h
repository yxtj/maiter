/*
 * noncopyable.h
 *
 *  Created on: Dec 11, 2015
 *      Author: tzhou
 */

#ifndef UTIL_NONCOPYABLE_H_
#define UTIL_NONCOPYABLE_H_

namespace dsm {

class noncopyable{
#if __cplusplus >= 201103L
protected:
	noncopyable() = default;
	~noncopyable() = default;
	noncopyable(const noncopyable&) = delete;
	noncopyable& operator=(const noncopyable&) = delete;
#else
protected:
	noncopyable(){}
	~noncopyable(){}
private:  // emphasize the following members are private
	noncopyable( const noncopyable&);
	noncopyable& operator=( const noncopyable&);
#endif
};

}

#endif /* UTIL_NONCOPYABLE_H_ */
