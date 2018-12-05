#pragma once
#include <memory>
#include "operator.h"

namespace ToyDBMS {

    class Projection : public Operator {
        std::unique_ptr<Operator> table;
        std::vector<std::string> attrs;
        std::vector<size_t> columns;
        Row current_row;
        std::shared_ptr<Header> header_ptr;

        Header construct_header(const Header &h, const std::vector<std::string> &attrs);

    public:
        Projection(std::unique_ptr<Operator> table, const std::vector<std::string> &attrs)
                : table(std::move(table)), attrs(attrs),
                  header_ptr(std::make_shared<Header>(
                          construct_header(this->table->header(), this->attrs))
                  ) {}

        const Header &header() override { return *header_ptr; }
        Row next() override;
        void reset() override;
    };
}
