// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "same_element_blueprint.h"
#include "same_element_search.h"
#include <vespa/searchlib/fef/termfieldmatchdata.h>
#include <vespa/vespalib/objects/visit.hpp>
#include <algorithm>
#include <map>

namespace search::queryeval {

SameElementBlueprint::SameElementBlueprint()
    : ComplexLeafBlueprint(FieldSpecBaseList()),
      _estimate(),
      _layout(),
      _terms()
{
}

FieldSpec
SameElementBlueprint::getNextChildField(const vespalib::string &field_name, uint32_t field_id)
{
    return FieldSpec(field_name, field_id, _layout.allocTermField(field_id), false);
}

void
SameElementBlueprint::addTerm(Blueprint::UP term)
{
    const State &childState = term->getState();
    assert(childState.numFields() == 1);
    HitEstimate childEst = childState.estimate();
    if (_terms.empty() ||  (childEst < _estimate)) {
        _estimate = childEst;
        setEstimate(_estimate);
    }
    _terms.push_back(std::move(term));
}

void
SameElementBlueprint::optimize_self()
{
    std::sort(_terms.begin(), _terms.end(),
              [](const auto &a, const auto &b)
              {
                  return (a->getState().estimate() < b->getState().estimate());
              });
}

void
SameElementBlueprint::fetchPostings(bool strict)
{
    for (size_t i = 0; i < _terms.size(); ++i) {
        _terms[i]->fetchPostings(strict && (i == 0));
    }
}

SearchIterator::UP
SameElementBlueprint::createLeafSearch(const search::fef::TermFieldMatchDataArray &tfmda,
                                       bool strict) const
{
    (void) tfmda;
    assert(!tfmda.valid());
    fef::MatchData::UP md = _layout.createMatchData();
    search::fef::TermFieldMatchDataArray childMatch;
    std::vector<SearchIterator::UP> children(_terms.size());
    for (size_t i = 0; i < _terms.size(); ++i) {
        const State &childState = _terms[i]->getState();
        assert(childState.numFields() == 1);
        childMatch.add(childState.field(0).resolve(*md));
        children[i] = _terms[i]->createSearch(*md, (strict && (i == 0)));
    }
    return std::make_unique<SameElementSearch>(std::move(md), std::move(children), childMatch, strict);
}

void
SameElementBlueprint::visitMembers(vespalib::ObjectVisitor &visitor) const
{
    ComplexLeafBlueprint::visitMembers(visitor);
    visit(visitor, "terms", _terms);
}

}
