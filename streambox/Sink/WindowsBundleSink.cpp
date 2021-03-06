#define K2_NO_DEBUG 1

#include "WindowsBundleSinkEvaluator.h"

/* ----  specialization: different ways to dump bundle contents  --- */

/* for wc */
template<>
void WindowsBundleSink<pair<creek::string, long>>::printBundle
	(const WindowsBundle<pair<creek::string, long>> & input_bundle) {
    W("got one bundle: ");
#ifndef NDEBUG
    for (auto && win_frag: input_bundle.vals) {
    	auto && win = win_frag.first;
    	auto && pfrag = win_frag.second;
    	cout << "==== window ===== " << endl;
    	cout << to_simplest_string1(win.window_start()).str() << endl;
    	for (auto && rec : pfrag->vals) {
    		cout << rec.data.first << ": " << rec.data.second << endl;
    	}
    	cout << "----------------" << endl;
    }
#endif
}


/* -------------------- ExecEvaluator --------------------
 * out of line to avoid circular dependency
 */


template<class T>
void WindowsBundleSink<T>::ExecEvaluator(
		int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr)
{

#ifndef NDEBUG /* if evaluators get stuck ...*/
	static atomic<int> outstanding (0);
#endif
	WindowsBundleSinkEvaluator<T> eval(nodeid);
	//eval.evaluate(this, c, bundle_ptr);

#ifndef NDEBUG		// for debug
//	I("begin eval...");
	outstanding ++;
#endif
  
	eval.evaluate(this, c, bundle_ptr);

#ifndef NDEBUG   // for debug
	outstanding --; I("end eval... outstanding = %d", outstanding);
#endif
}


/* ---- instantiation for concrete types --- */


/* for word count */
template
void WindowsBundleSink<pair<creek::string, long>>::ExecEvaluator(
		int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);

/* for netmon */
template
void WindowsBundleSink<pair<creek::ippair, long>>::ExecEvaluator(
		int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);


/* for test */
template
void WindowsBundleSink<long>::ExecEvaluator(
		int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);
