#ifndef WINDOWED_KEY_REC_H
#define WINDOWED_KEY_REC_H

#include "Values.h"
#include "core/StatefulTransform.h"

/* Stateful reducer of windowed/keyed records.
 * Execute DoFn on each window / each key
 *
 * InputT : WindowKeyedFragment<KVPair>
 *
 */
template <
	typename KVPairIn,
	/* the input windowkeyfrag. its vcontainer must be the same as the
	 * upstream's output, e.g wingbk.
	 * the format can be really cheap, i.e. no concurrent support.
	 */
	template<class> class InputWinKeyFragT,
	/* internal state. concurrent d/s
	 * can be specialized based on key/val distribution.
	 * regarding the d/s for holding eval's local aggregation results,
	 * see comment below. */
	template<class> class InternalWinKeyFragT, // = WindowKeyedFragment
	typename KVPairOut,
	template<class> class OutputBundleT_
	>
class WinKeyReducer
		: public StatefulTransform<
		  		WinKeyReducer<KVPairIn,InputWinKeyFragT,InternalWinKeyFragT,KVPairOut,OutputBundleT_>,
		  		InputWinKeyFragT<KVPairIn>,			/* InputT */
		  		shared_ptr<InternalWinKeyFragT<KVPairIn>>,		/* WindowResultT */
		  		shared_ptr<InternalWinKeyFragT<KVPairIn>>		/* LocalWindowResultT */
//		  		shared_ptr<InputWindowKeyedFragmentT<KVPair>>		/* LocalWindowResultT */
		  >
{

public:
	using K = decltype(KVPairIn::first);
	using V = decltype(KVPairIn::second);

	/* container can be unsafe */
	using InputValueContainerT = typename InputWinKeyFragT<KVPairIn>::ValueContainerT;
	/* container must be safe */
	using InternalValueContainerT = typename InternalWinKeyFragT<KVPairIn>::ValueContainerT;

	//  using windows_map = map<Window, WindowKeyedFragment<KVPair>, Window>;
	using TransformT = WinKeyReducer<KVPairIn, InputWinKeyFragT, InternalWinKeyFragT,
	      KVPairOut,OutputBundleT_>;
	using InputT = InputWinKeyFragT<KVPairIn>;

	/* Aggregation types
	 *
	 * localwindowresult: used by eval to hold local result
	 * windowresult: used for transform's internal state.
	 *
	 * the intention of separating these two is that localwindowresult does not have
	 * to be concurrent d/s and therefore can be cheap.
	 * However, when making localwindowresult as based on InputWindowKeyedFragmentT
	 * (e.g. std::unordered_map with SimpleValueContainerUnsafe), the performance
	 * is even worse than (localwindowresult == windowresult).
	 *
	 * This can be tested by toggling the comments below.
	 */
	using WindowResultT = shared_ptr<InternalWinKeyFragT<KVPairIn>>;
	using LocalWindowResultT = WindowResultT;
	//  using LocalWindowResultT = shared_ptr<InputWindowKeyedFragmentT<KVPair>>;

	/* AggResultT and LocalAggResultT are decl in StatefulTransform */
	//  using LocalAggResultT = map<Window, LocalWindowResultT, Window>;

#if 0
	// can't do this. each window cannot be associated with multiple "fragments"
	// where the keys may be duplicated
	//  typedef map<Window,
	//              vector<shared_ptr<WindowKeyedFragment<K,V>>>, Window>
	//              windows_map;

private:
	unsigned long window_count = 0;
	/* holds per-window states. ordered by window time, thus can be iterated.
	   each window contains multiple keys and the associated value containers */
	windows_map windows;
	mutex _mutex;

protected:
	// the age of the oldest internal record. *not* the watermark
	ptime min_ts = max_date_time;
#endif

public:
	WinKeyReducer(string name)
		: StatefulTransform<TransformT, InputT, WindowResultT, LocalWindowResultT>(name) { }

	////////////////////////////////////////////////////////////////////

	////////////////////////////////////////////////////////////////////
#if 0
	iterator begin() {
		return iterator(&windows, windows.begin(), windows.begin()->second.vals.begin());
	}

	iterator end() {
		/* internal iterator should point to the end() kvcontainer of the last
		 * valid window (not .end())
		 *
		 * XXX what should we return if no window at all? */
		assert(!windows.empty());

		/* if the last window is non-empty, we return its vals.end();
		   otherwise, we return the last window's vals.begin() == vals.end()  */
#if 0 /* having problems with reverse iterator */
		return iterator(&windows, windows.rbegin(),
				windows.rbegin()->second.vals.end());
#endif
#if 0
		return iterator(&windows, windows.end() - 1,
				(windows.end() - 1)->second.vals.end());
#endif

		auto lastbutone = windows.end();
		lastbutone --;
		return iterator(&windows, lastbutone, lastbutone->second.vals.end());
	}
	////////////////////////////////////////////////////////////////////

	unsigned long GetRecordCount() {
		unique_lock<mutex> lock(_mutex);
		unsigned long count = 0;

		// go through each window ...
		for (auto it = windows.begin(); it != windows.end(); it++) {
			auto& frag = it->second;
			V("window --- ");
			for (auto& kvs: frag->vals ) {
				V("key %ld size %lu",
						kvs.first, kvs.second.size());
				count += kvs.second.size();
			}
		}
		return count;
	}
#endif

	// reduce on one specific key and all its values (as in a value container)
	virtual KVPairIn do_reduce(K const &, InternalValueContainerT const &);
	virtual KVPairIn do_reduce_unsafe(K const &, InputValueContainerT const &);

	/* Take one InputT. to be specialized */
	static WindowResultT const & aggregate_init(WindowResultT * acc) {
		xzl_assert(acc);
		//  	(*acc) = make_shared<InternalWindowKeyedFragmentT<KVPair>>();
		(*acc) = make_shared<typename WindowResultT::element_type>();
		return *acc;
	}

	static LocalWindowResultT const & local_aggregate_init(LocalWindowResultT * acc) {
		xzl_assert(acc);
		//  	(*acc) = make_shared<InputWindowKeyedFragmentT<KVPair>>();
		//  	(*acc) = make_shared<InternalWindowKeyedFragmentT<KVPair>>();
		/* more generic -- don't have to change back/forth */
		(*acc) = make_shared<typename LocalWindowResultT::element_type>();
		return *acc;
	}

	static WindowResultT const & aggregate(WindowResultT * acc, InputT const & in);

	/* combine the evaluator's (partial) aggregation results (for a particular window)
	 * to the tran's internal state.
	 */
	static WindowResultT const & combine(WindowResultT & mine, LocalWindowResultT const & others);

#if 0 /* obsoleted by StatefulTransform */
	// accumulate the (task-local) bundle onto the transform-internal state.
	// assume this is fast. (only moving around pointers)
	// XXX a lock free version? XXX
	void AddWindowsKeyedBundle(shared_ptr<WindowsKeyedBundle<KVPair>> const bundle) {
		unique_lock<mutex> lock(_mutex); /* since we may open new window */

		// go over windows in the bundle
		//    for (auto it = bundle->vals.begin(); it != bundle->vals.end(); it++) {
		for (auto&& windowed : bundle->vals) {
			// dispatch the incoming value containers to the internal state of
			// various windows.
			auto & win = windowed.first;

			// we assume that each incoming "window" is non empty
			if (windows.count(win) == 0) // the incoming window didn't exist
				window_count ++;

			for (auto&& kvs : windowed.second.vals) {
				auto & key = kvs.first;
				auto & v_container = kvs.second;
#if 1
				for (auto&& v_basic_container_ptr : v_container.vals) {
					//                I("v_basic_container has %u elements. %ld %ld ...",
					//                    v_basic_container_ptr->size(),
					//                    (*v_basic_container_ptr)[0],
					//                    (*v_basic_container_ptr)[1]);
					// will update the internal v container's min_ts
					windows[win].vals[key].add_basic_container(v_basic_container_ptr);
#endif
				}
			}
		}
		/* XXX
		   if (watermark > bundle->min_ts) {
		   W("warning -- got a stale bundle. transform watermark: (%s), bundle ts: (%s)",
		   to_simple_string(watermark).c_str(),
		   to_simple_string(bundle->min_ts).c_str());
		   }

		   assert(watermark <= bundle->min_ts);
		 */
		// update the transform's internal min_ts (NOT the watermark)
		if (min_ts > bundle->min_ts) {
			VV("min_ts %s -> %s",
					to_simple_string(min_ts).c_str(),
					to_simple_string(bundle->min_ts).c_str());

			min_ts = bundle->min_ts;
		}
	}

	// return the top N "windows" and purge them from the internal state
	// if n unspecified, get all existing windows.
	//
	// @ purge: whether to purge the internal window states.
	windows_map RetrieveWindows(bool purge = false,
			ptime wm = max_date_time, int n = -1) {
		unique_lock<mutex> lock(_mutex);

		windows_map ret;

		auto it = windows.begin();
		for (int count = 0; it != windows.end(); it++, count++ ) {
			//        ptime window_end = (it->first).start + (it->first).duration;
			// watermark has not yet exceed this window end.
			// don't close this window yet.
			if (wm < it->first.window_end()) {
				W("skip window. specified watermk %s < window_end %s",
						to_simplest_string(wm).c_str(),
						to_simple_string(it->first.window_end()).c_str());
				// no need to check more recent windows:
				// we assume that windows are ordered.
				break;
			}

			if (count == n)
				break;
			ret[it->first] = it->second; // makes a copy of the window container.
			window_count --;
		}

		if (purge) {
			if (windows.begin() == it) // no window returned nor purged.
				goto done;

			windows.erase(windows.begin(), it);

			// XXX only look at the first window in the current state
			// XXX suboptimal. within one window there are multiple "value containers"
			// and we have to look at each of them to determine lowest ts
			if (windows.begin() == windows.end()) // no window left at all
				min_ts = max_date_time;
			else {
				// Now we've purged some windows, and there are still some left.
				// We need to find the min_ts among the remaining windows
				min_ts = max_date_time;  // reset min_ts

				auto& frag = windows.begin()->second;
				ptime frag_min_ts = frag.min_ts();
				if (frag_min_ts < min_ts) {
					DD("min_ts %s -> %s",
							to_simple_string(min_ts).c_str(),
							to_simple_string(output->min_ts).c_str());
					min_ts = frag_min_ts;
				}
			}
		}

done:
		return ret;
	}
#endif

	/* recalculate the local wm based on available information.
	 * although this tranform is stateful, PTransform::RefreshWatermark is okay */
#if 0
	virtual ptime RefreshWatermark() override {
		PValue* v = getFirstInput(); // XXX deal with multiple ins
		assert(v);
		PTransform *upstream = v->producer;
		assert(upstream);

		unique_lock<mutex> lock(mtx_watermark);

		// also consider all inflight bundles
		ptime min_ts_flight = max_date_time;
		for (auto && b : inflight_bundles) {
			if (b->min_ts < min_ts_flight)
				min_ts_flight = b->min_ts;
		}

		ptime pt = min(min_ts_flight,
				min(v->min_ts, min(upstream->watermark, min_ts)));

		/* for debugging */
		I("%s: watermark: current %s new %s\n"
				" \t\t (input %s upstream(%s %lx) %s internal %s",
				name.c_str(),
				to_simplest_string(watermark).c_str(),
				to_simplest_string(pt).c_str(),
				to_simplest_string(v->min_ts).c_str(),
				upstream->getName().c_str(), (unsigned long)(upstream),
				to_simplest_string(upstream->watermark).c_str(),
				to_simplest_string(min_ts).c_str());

		if (watermark > pt) {

			W("warning: existing watermark %s > the new watermark",
					to_simplest_string(watermark).c_str());

			W("%s: watermark to be updated to: %s"
					" (input %s upstream %s internal %s", name.c_str(),
					to_simplest_string(pt).c_str(),
					to_simplest_string(v->min_ts).c_str(),
					to_simplest_string(upstream->watermark).c_str(),
					to_simplest_string(min_ts).c_str());

			assert(0);
		}

		if (watermark < pt) {
			watermark = pt;
			W("%s: update new watermark: %s\n"
					"\t\t input %s\n \t\t upstream %s\n \t\t internal %s",
					name.c_str(),
					to_simplest_string(watermark).c_str(),
					to_simplest_string(v->min_ts).c_str(),
					to_simplest_string(upstream->watermark).c_str(),
					to_simplest_string(min_ts).c_str());
		}
		return pt;
	}
#endif

	void ExecEvaluator(int nodeid, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr) override;
	};


	/* the default version that outputs WindowsBundle */
	template <
		typename KVPairIn,
			 template<class> class InputWinKeyFragT,
			 template<class> class InternalWinKeyFragT // = WindowKeyedFragment
				 >
				 using WinKeyReducer_winbundle = WinKeyReducer<KVPairIn,
			 InputWinKeyFragT, InternalWinKeyFragT,
			 KVPairIn, /* kv out */
			 WindowsBundle>;

#endif // WINDOWED_KEY_REC_H
