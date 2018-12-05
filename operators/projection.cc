#include <algorithm>
#include <iterator>
#include "projection.h"

namespace ToyDBMS {

    Row Projection::next() {
        if (!current_row)
            current_row = table->next();

        if (!current_row) return {};

        std::vector <Value> values;
        for (size_t col : columns) {
            values.push_back(std::move(current_row[col]));
        }
        current_row = {};
        return {header_ptr, std::move(values)};
    }

    void Projection::reset() {
        table->reset();
        current_row = {};
    }

    Header Projection::construct_header(const Header &h, const std::vector<std::string> &attrs) {
        Header res;
        for (size_t i = 0; i < attrs.size(); ++i) {
            res.push_back(attrs[i]);
            size_t column = h.index(attrs[i]);
            columns.push_back(column);
        }
        return res;
    }
}
