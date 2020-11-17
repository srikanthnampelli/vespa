// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "streamed_value_store.h"
#include "tensor_attribute.h"

namespace search::tensor {

/**
 * Attribute vector class used to store tensors for all documents in memory.
 */
class SerializedTensorAttribute : public TensorAttribute {
    StreamedValueStore _serializedTensorStore; // data store for serialized tensors
public:
    SerializedTensorAttribute(vespalib::stringref baseFileName, const Config &cfg);
    virtual ~SerializedTensorAttribute();
    virtual void setTensor(DocId docId, const vespalib::eval::Value &tensor) override;
    virtual std::unique_ptr<vespalib::eval::Value> getTensor(DocId docId) const override;
    virtual bool onLoad() override;
    virtual std::unique_ptr<AttributeSaver> onInitSave(vespalib::stringref fileName) override;
    virtual void compactWorst() override;
};

}
