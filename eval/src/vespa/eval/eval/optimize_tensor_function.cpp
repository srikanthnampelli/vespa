// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "optimize_tensor_function.h"
#include "tensor_function.h"
#include "tensor_engine.h"
#include "simple_value.h"

#include <vespa/eval/instruction/dense_dot_product_function.h>
#include <vespa/eval/instruction/dense_xw_product_function.h>
#include <vespa/eval/instruction/dense_matmul_function.h>
#include <vespa/eval/instruction/dense_multi_matmul_function.h>
#include <vespa/eval/tensor/dense/dense_fast_rename_optimizer.h>
#include <vespa/eval/tensor/dense/dense_add_dimension_optimizer.h>
#include <vespa/eval/tensor/dense/dense_single_reduce_function.h>
#include <vespa/eval/tensor/dense/dense_remove_dimension_optimizer.h>
#include <vespa/eval/instruction/dense_lambda_peek_optimizer.h>
#include <vespa/eval/tensor/dense/dense_lambda_function.h>
#include <vespa/eval/instruction/dense_simple_expand_function.h>
#include <vespa/eval/tensor/dense/dense_simple_join_function.h>
#include <vespa/eval/instruction/join_with_number_function.h>
#include <vespa/eval/tensor/dense/dense_pow_as_map_optimizer.h>
#include <vespa/eval/tensor/dense/dense_simple_map_function.h>
#include <vespa/eval/tensor/dense/vector_from_doubles_function.h>
#include <vespa/eval/tensor/dense/dense_tensor_create_function.h>
#include <vespa/eval/instruction/dense_tensor_peek_function.h>

#include <vespa/log/log.h>
LOG_SETUP(".eval.eval.optimize_tensor_function");

namespace vespalib::eval {

namespace {

using namespace vespalib::tensor;

const TensorFunction &optimize_for_factory(const ValueBuilderFactory &factory, const TensorFunction &expr, Stash &stash) {
    if (&factory == &SimpleValueBuilderFactory::get()) {
        // never optimize simple value evaluation
        return expr;
    }
    using Child = TensorFunction::Child;
    Child root(expr);
    {
        std::vector<Child::CREF> nodes({root});
        for (size_t i = 0; i < nodes.size(); ++i) {
            nodes[i].get().get().push_children(nodes);
        }
        while (!nodes.empty()) {
            const Child &child = nodes.back().get();
            child.set(DenseDotProductFunction::optimize(child.get(), stash));
            child.set(DenseXWProductFunction::optimize(child.get(), stash));
            child.set(DenseMatMulFunction::optimize(child.get(), stash));
            child.set(DenseMultiMatMulFunction::optimize(child.get(), stash));
            nodes.pop_back();
        }
    }
    {
        std::vector<Child::CREF> nodes({root});
        for (size_t i = 0; i < nodes.size(); ++i) {
            nodes[i].get().get().push_children(nodes);
        }
        while (!nodes.empty()) {
            const Child &child = nodes.back().get();
            child.set(DenseSimpleExpandFunction::optimize(child.get(), stash));
            child.set(DenseAddDimensionOptimizer::optimize(child.get(), stash));
            child.set(DenseRemoveDimensionOptimizer::optimize(child.get(), stash));
            child.set(VectorFromDoublesFunction::optimize(child.get(), stash));
            child.set(DenseTensorCreateFunction::optimize(child.get(), stash));
            child.set(DenseTensorPeekFunction::optimize(child.get(), stash));
            child.set(DenseLambdaPeekOptimizer::optimize(child.get(), stash));
            child.set(DenseFastRenameOptimizer::optimize(child.get(), stash));
            child.set(DensePowAsMapOptimizer::optimize(child.get(), stash));
            child.set(DenseSimpleMapFunction::optimize(child.get(), stash));
            child.set(DenseSimpleJoinFunction::optimize(child.get(), stash));
            child.set(JoinWithNumberFunction::optimize(child.get(), stash));
            child.set(DenseSingleReduceFunction::optimize(child.get(), stash));
            nodes.pop_back();
        }
    }
    return root.get();
}

} // namespace vespalib::eval::<unnamed>

const TensorFunction &optimize_tensor_function(EngineOrFactory engine, const TensorFunction &function, Stash &stash) {
    LOG(debug, "tensor function before optimization:\n%s\n", function.as_string().c_str());
    const TensorFunction &optimized = (engine.is_engine())
                                      ? engine.engine().optimize(function, stash)
                                      : optimize_for_factory(engine.factory(), function, stash);
    LOG(debug, "tensor function after optimization:\n%s\n", optimized.as_string().c_str());
    return optimized;
}

} // namespace vespalib::eval
