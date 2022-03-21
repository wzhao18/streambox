#ifndef _SINK_H
#define _SINK_H

#include <iostream>

#include "config.h"

extern "C" {
#include "log.h"
#include "measure.h"
}

#include "core/Transforms.h"

using namespace std;

///////////////////////////////////////////////////////

/* A bunch of records. This is good for that each window outputs a single value
 * @T: element type, e.g. long */
template<class T>
class RecordBundleSink : public PTransform {
	using InputBundleT = RecordBundle<T>;

	public:
	RecordBundleSink(string name) : PTransform(name) { }

	static void printBundle(const InputBundleT & input_bundle) {
		I("got one bundle");
	}

	void ExecEvaluator(int nodeid, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr) override;

};

///////////////////////////////////////////////////////

/* @T: element type, e.g. long */
template<class T>
class RecordBitmapBundleSink : public PTransform {
	using InputBundleT = RecordBitmapBundle<T>;

	public:
	RecordBitmapBundleSink(string name) : PTransform(name) { }

	/* default behavior.
	 * can't do const. see below */
	static void printBundle(InputBundleT & input_bundle) {
		I("got one bundle");
	}

	void ExecEvaluator(int nodeid, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr) override;

	static bool report_progress(InputBundleT & input_bundle){ return false;} //hym: defined in Sink.cpp

	private:
	//hym: for measuring throughput(report_progress())
	static uint64_t total_bytes;
	static uint64_t total_records;
	static uint64_t last_bytes;
	static uint64_t last_records;
	static ptime last_check;
	static ptime start_time;
	static int once;
#if 0
	/* internal accounting */
	uint64_t total_bytes = 0, total_records = 0;
	/* last time we report */
	uint64_t last_bytes = 0, last_records = 0;
	ptime last_check, start_time;
	int once = 1;
#endif
};
template<class T>
uint64_t RecordBitmapBundleSink<T>::total_bytes = 0;
template<class T>
uint64_t RecordBitmapBundleSink<T>::total_records = 0;
template<class T>
uint64_t RecordBitmapBundleSink<T>::last_bytes = 0;
template<class T>
uint64_t RecordBitmapBundleSink<T>::last_records = 0;

template<class T>
ptime RecordBitmapBundleSink<T>::last_check = boost::posix_time::microsec_clock::local_time();
template<class T>
ptime RecordBitmapBundleSink<T>::start_time = boost::posix_time::microsec_clock::local_time();;

template<class T>
int RecordBitmapBundleSink<T>::once = 1;
///////////////////////////////////////////////////////

template<class T>
class WindowsBundleSink : public PTransform {
	using InputBundleT = WindowsBundle<T>;

	public:
	WindowsBundleSink(string name)  : PTransform(name) { }

	static void printBundle(const InputBundleT & input_bundle) {
		I("got one bundle");
	}

	void ExecEvaluator(int nodeid, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr) override;

};

#endif // _SINK_H
