#include "FixedWindowInto.h"
#include "FixedWindowIntoEvaluator.h"

template <typename T, template<class> class InputBundle>
void FixedWindowInto<T, InputBundle>::ExecEvaluator(int nodeid, EvaluationBundleContext *c,
		shared_ptr<BundleBase> bundle_ptr) {
	FixedWindowIntoEvaluator<T, InputBundle> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

/* -------instantiation concrete classes------- */

template
void FixedWindowInto<string_range, RecordBundle>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);

template
void FixedWindowInto<creek::string, RecordBundle>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);

/* todo: more types */
