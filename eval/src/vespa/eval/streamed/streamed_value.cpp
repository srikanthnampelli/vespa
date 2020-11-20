// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "streamed_value.h"
#include <vespa/log/log.h>

LOG_SETUP(".vespalib.eval.streamed.streamed_value");

namespace vespalib::eval {

template <typename T>
StreamedValue<T>::~StreamedValue() = default;

template <typename T>
MemoryUsage
StreamedValue<T>::get_memory_usage() const
{
    return self_memory_usage<StreamedValue<T>>();
}

template class StreamedValue<double>;
template class StreamedValue<float>;

} // namespace

