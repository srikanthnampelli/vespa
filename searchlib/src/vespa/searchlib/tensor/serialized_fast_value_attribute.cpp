#include "serialized_fast_value_attribute.h"
#include "streamed_value_saver.h"
#include <vespa/eval/eval/value.h>
#include <vespa/eval/eval/fast_value.hpp>
#include <vespa/fastlib/io/bufferedfile.h>
#include <vespa/searchlib/attribute/readerbase.h>
#include <vespa/searchlib/util/fileutil.h>
#include <vespa/vespalib/util/rcuvector.hpp>
#include <vespa/log/log.h>

LOG_SETUP(".searchlib.tensor.serialized_fast_value_attribute");

#include "blob_sequence_reader.h"
#include "tensor_attribute.hpp"

using namespace vespalib;
using namespace vespalib::eval;

namespace search::tensor {

namespace {

constexpr uint32_t TENSOR_ATTRIBUTE_VERSION = 0;

struct ValueBlock {
    size_t subspace_index;
    ConstArrayRef<vespalib::stringref> address;
    TypedCells cells;
    operator bool() const { return (cells.size != 0); }
};

struct LabelStream {
    nbostream_longlivedbuf source;
    LabelStream(vespalib::ConstArrayRef<char> buf)
      : source(buf.begin(), buf.size())
    {}
    vespalib::stringref next_label() {
        size_t str_size = source.getInt1_4Bytes();
        vespalib::stringref label(source.peek(), str_size);
        source.adjustReadPos(str_size);
        return label;
    }
};

class ValueBlockStream {
private:
    const StreamedValueStore::DataFromType &_from_type;
    const StreamedValueStore::StreamedValueData &_from_store;
    size_t _subspace_index;
    std::vector<vespalib::stringref> _current_address;
    LabelStream _labels;
    const char *_cells_ptr;

    size_t num_blocks() const { return _from_store.num_subspaces; }
    size_t dsss() const { return _from_type.dense_subspace_size; }
    auto cell_type() const { return _from_store.cells_ref.type; }
public:
    ValueBlock next_block() {
        if (_subspace_index < num_blocks()) {
            for (auto & label : _current_address) {
                label = _labels.next_label();
            }
            TypedCells subspace_cells(_cells_ptr, cell_type(), dsss());
            _cells_ptr += CellTypeUtils::mem_size(cell_type(), dsss());
            return ValueBlock{_subspace_index++, _current_address, subspace_cells};
        } else {
            return {};
        }
    }
    
    ValueBlockStream(const StreamedValueStore::DataFromType &from_type,
                     const StreamedValueStore::StreamedValueData &from_store)
      : _from_type(from_type),
        _from_store(from_store),
        _subspace_index(0),
        _current_address(from_type.num_mapped_dimensions),
        _labels(from_store.labels_buffer),
        _cells_ptr((const char *)from_store.cells_ref.data)
    {}
    
    ~ValueBlockStream();
};

ValueBlockStream::~ValueBlockStream() = default;

class OnlyFastValueIndex : public Value {
private:
    const ValueType &_type;
    TypedCells _cells;
    FastValueIndex my_index;
public:
    OnlyFastValueIndex(const ValueType &type,
                       const StreamedValueStore::DataFromType &from_type,
                       const StreamedValueStore::StreamedValueData &from_store)
      : _type(type),
        _cells(from_store.cells_ref),
        my_index(from_type.num_mapped_dimensions,
                 from_store.num_subspaces)
    {
        assert(_type.cell_type() == _cells.type);
        std::vector<vespalib::stringref> address(from_type.num_mapped_dimensions);
        auto block_stream = ValueBlockStream(from_type, from_store);
        size_t ss = 0;
        while (auto block = block_stream.next_block()) {
            size_t idx = my_index.map.add_mapping(block.address);
            if (idx != ss) {
                LOG(error, "add_mapping returned idx=%zu for subspace %zu", idx, ss);
            }
            ++ss;
        }
        if (ss != from_store.num_subspaces) {
            LOG(error, "expected %zu subspaces but got %zu", from_store.num_subspaces, ss);
            abort();
        }
    }

    ~OnlyFastValueIndex();
    
    const ValueType &type() const final override { return _type; }
    TypedCells cells() const final override { return _cells; }
    const Index &index() const final override { return my_index; }
    vespalib::MemoryUsage get_memory_usage() const final override {
        auto usage = self_memory_usage<OnlyFastValueIndex>();
        usage.merge(my_index.map.estimate_extra_memory_usage());
        return usage;
    }
};

OnlyFastValueIndex::~OnlyFastValueIndex() = default;

}

SerializedFastValueAttribute::SerializedFastValueAttribute(stringref name, const Config &cfg)
  : TensorAttribute(name, cfg, _streamedValueStore),
    _tensor_type(cfg.tensorType()),
    _streamedValueStore(_tensor_type),
    _data_from_type(_tensor_type)
{
}


SerializedFastValueAttribute::~SerializedFastValueAttribute()
{
    getGenerationHolder().clearHoldLists();
    _tensorStore.clearHoldLists();
}

void
SerializedFastValueAttribute::setTensor(DocId docId, const vespalib::eval::Value &tensor)
{
    EntryRef ref = _streamedValueStore.store_tensor(tensor);
    setTensorRef(docId, ref);
    if (!ref.valid()) {
        checkTensorType(tensor);
    }
}

std::unique_ptr<Value>
SerializedFastValueAttribute::getTensor(DocId docId) const
{
    EntryRef ref;
    if (docId < getCommittedDocIdLimit()) {
        ref = _refVector[docId];
    }
    if (!ref.valid()) {
        return {};
    }
    if (auto data_from_store = _streamedValueStore.get_tensor_data(ref)) {
        return std::make_unique<OnlyFastValueIndex>(_tensor_type,
                                                    _data_from_type,
                                                    data_from_store);
    }
    return {};
}


bool
SerializedFastValueAttribute::onLoad()
{
    BlobSequenceReader tensorReader(*this);
    if (!tensorReader.hasData()) {
        return false;
    }
    setCreateSerialNum(tensorReader.getCreateSerialNum());
    assert(tensorReader.getVersion() == TENSOR_ATTRIBUTE_VERSION);
    uint32_t numDocs(tensorReader.getDocIdLimit());
    _refVector.reset();
    _refVector.unsafe_reserve(numDocs);
    vespalib::Array<char> buffer(1024);
    for (uint32_t lid = 0; lid < numDocs; ++lid) {
        uint32_t tensorSize = tensorReader.getNextSize();
        if (tensorSize != 0) {
            if (tensorSize > buffer.size()) {
                buffer.resize(tensorSize + 1024);
            }
            tensorReader.readBlob(&buffer[0], tensorSize);
            vespalib::nbostream source(&buffer[0], tensorSize);
            EntryRef ref = _streamedValueStore.store_encoded_tensor(source);
            _refVector.push_back(ref);
        } else {
            EntryRef invalid;
            _refVector.push_back(invalid);
        }
    }
    setNumDocs(numDocs);
    setCommittedDocIdLimit(numDocs);
    return true;
}


std::unique_ptr<AttributeSaver>
SerializedFastValueAttribute::onInitSave(vespalib::stringref fileName)
{
    vespalib::GenerationHandler::Guard guard(getGenerationHandler().
                                             takeGuard());
    return std::make_unique<StreamedValueSaver>
        (std::move(guard),
         this->createAttributeHeader(fileName),
         getRefCopy(),
         _streamedValueStore);
}

void
SerializedFastValueAttribute::compactWorst()
{
    doCompactWorst<StreamedValueStore::RefType>();
}

}
