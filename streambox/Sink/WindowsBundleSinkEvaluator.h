#ifndef WINDOWSBUNDLESINKEVALUATOR_H_
#define WINDOWSBUNDLESINKEVALUATOR_H_

#include "core/SingleInputTransformEvaluator.h"
#include "Sink/Sink.h"

/* InputT: the element type of the record bundle */
template <typename InputT>
class WindowsBundleSinkEvaluator
    : public SingleInputTransformEvaluator<WindowsBundleSink<InputT>,
//      WindowsKeyedBundle<InputT>, WindowsBundle<InputT>> {
      WindowsBundle<InputT>, WindowsBundle<InputT>> {   // sufficient for wc & topK?

	using TransformT = WindowsBundleSink<InputT>;
	using InputBundleT = WindowsBundle<InputT>;
//	using InputBundleT = WindowsKeyedBundle<InputT>;
	//using InputBundleT = WindowsKeyedBundle<KVPair>;
	using OutputBundleT = WindowsBundle<InputT>;

public:

	WindowsBundleSinkEvaluator(int node)
	: SingleInputTransformEvaluator<TransformT, InputBundleT, OutputBundleT>(node) { }

  bool evaluateSingleInput (TransformT* trans,
        shared_ptr<InputBundleT> input_bundle,
        shared_ptr<OutputBundleT> output_bundle) override {

    //XXX TransformT::printBundle(*input_bundle);
    //TransformT::report_progress(* input_bundle);
    return false; /* no output bundle */
  }

};

#endif /* WINDOWSBUNDLESINKEVALUATOR_H_ */
