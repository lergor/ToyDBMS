#include <algorithm>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <unordered_map>
#include <set>
#include <algorithm>
#include "constructor.h"

#include "../operators/datasource.h"
#include "../operators/filter.h"
#include "../operators/join.h"
#include "../operators/projection.h"

namespace ToyDBMS {

    static std::vector<const AttributePredicate *> find_joins(const Predicate &predicate) {
        std::vector<const AttributePredicate *> sequential_joins;
        switch (predicate.type) {
            case Predicate::Type::CONST:
                return std::vector<const AttributePredicate *>();
            case Predicate::Type::ATTR: {
                sequential_joins.push_back(dynamic_cast<const AttributePredicate *>(&predicate));
                return sequential_joins;
            }
            case Predicate::Type::AND: {
                const auto &pred = dynamic_cast<const ANDPredicate &>(predicate);
                auto found_left = find_joins(*pred.left);
                auto found_right = find_joins(*pred.right);
                found_left.insert(found_left.end(), found_right.begin(), found_right.end());
                return found_left;
            }
            default:
                throw std::runtime_error("encountered a predicate not yet supported");
        }
    }

    static void for_each_filter(const Predicate &predicate, std::function<void(const ConstPredicate &)> action) {
        switch (predicate.type) {
            case Predicate::Type::CONST:
                action(dynamic_cast<const ConstPredicate &>(predicate));
                break;
            case Predicate::Type::ATTR:
                return;
            case Predicate::Type::AND: {
                const auto &pred = dynamic_cast<const ANDPredicate &>(predicate);
                for_each_filter(*pred.left, action);
                for_each_filter(*pred.right, action);
                break;
            }
            default:
                throw std::runtime_error("encountered a predicate not yet supported");
        }
    }

    static std::string table_name(std::string attribute_name) {
        return {attribute_name.begin(), std::find(attribute_name.begin(), attribute_name.end(), '.')};
    }

    struct Source {
        std::string table;
        std::unique_ptr <Predicate> predicate;
        std::unique_ptr <DataSource> datasource;

        Source(std::string table, std::unique_ptr <Predicate> predicate, std::unique_ptr <DataSource> ds)
                : table(std::move(table)), predicate(std::move(predicate)), datasource(std::move(ds)) {}

        std::unique_ptr <Operator> construct() {
            if (predicate)
                return std::make_unique<Filter>(
                        std::move(datasource), std::move(predicate)
                );
            else return std::move(datasource);
        }
    };

    std::unique_ptr <Operator> create_plan(const Query &query) {
        std::vector <Source> sources;
        for (const auto &table : query.from) {
            if (table->type == FromPart::Type::QUERY)
                throw std::runtime_error("queries in the FROM clause are not yet implemented");
            std::string table_name = dynamic_cast<const FromTable &>(*table).table_name;
            sources.emplace_back(
                    table_name,
                    std::unique_ptr < Predicate > {},
                    std::make_unique<DataSource>("tables/" + table_name + ".csv")
            );
        }

        std::vector<bool> visited(sources.size(), false);
        std::unique_ptr <Operator> result = nullptr;

        if (query.where) {
            for_each_filter(*query.where, [&sources](const auto &pred) {
                std::string table = table_name(pred.attribute);
                bool found = false;
                for (auto &source : sources) {
                    if (source.table == table) {
                        found = true;
                        if (!source.predicate)
                            source.predicate = std::make_unique<ConstPredicate>(pred);
                        else
                            source.predicate = std::make_unique<ANDPredicate>(
                                    std::make_unique<ConstPredicate>(pred),
                                    std::move(source.predicate)
                            );
                    }
                }
                if (!found) throw std::runtime_error("couldn't find a source for a predicate");
            });
        }
        std::vector<const AttributePredicate *> joins;
        if (query.where) {
            joins = find_joins(*query.where);
        }
        std::unordered_map <std::string, size_t> name_to_index;
        for (size_t i = 0; i < sources.size(); ++i) {
            name_to_index[sources[i].table] = i;
        }

        std::unordered_map <std::string,
        std::set<const AttributePredicate *>> table_name_to_joins;
        size_t cur_index = sources.size();
        for (auto &join : joins) {
            table_name_to_joins[join->left_table].insert(join);
            table_name_to_joins[join->right_table].insert(join);
            cur_index = std::min(
                    std::min(name_to_index[join->left_table], cur_index),
                    name_to_index[join->right_table]);
        }
        if (cur_index == sources.size()) cur_index = 0;

        std::string cur_table;
        result = sources[cur_index].construct();
        visited[cur_index] = true;
        while (true) {
            while (cur_index < sources.size()) {
                cur_table = sources[cur_index].table;
                if (table_name_to_joins[cur_table].empty() || !visited[cur_index]) {
                    ++cur_index;
                } else {
                    const AttributePredicate *join = *(table_name_to_joins[cur_table].begin());
                    std::string table_to_join = (cur_table == join->left_table) ? join->right_table
                                                                                : join->left_table;
                    std::string left = join->left;
                    std::string right = join->right;
                    if (cur_table != join->left_table) std::swap(left, right);

                    size_t index = name_to_index[table_to_join];
                    if (index < cur_index) {
                        cur_index = index;
                    }
                    if(visited[index]) {
                        result = std::make_unique<Filter>(std::move(result),
                                                          std::make_unique<AttributePredicate>(join->left, join->right, join->relation));
                    } else {
                        visited[index] = true;
                        result = std::make_unique<NLJoin>(std::move(result), sources[index].construct(),
                                                          left, right);
                    }
                    table_name_to_joins[cur_table].erase(join);
                    table_name_to_joins[table_to_join].erase(join);
                }
            }
            for (size_t j = 0; j < visited.size(); ++j) {
                if (!visited[j]) {
                    result = std::make_unique<CrossJoin>(std::move(result), sources[j].construct());
                    visited[j] = true;
                    cur_index = j;
                    break;
                }
            }
            if (cur_index == sources.size()) break;
        }
        if (query.selection.type == SelectionClause::Type::LIST) {
            std::vector<std::string> selection_parts;
            for (auto& p : query.selection.attrs) {
                selection_parts.push_back(p.attribute);
            }
            if(selection_parts.size()) {
                result = std::make_unique<Projection>(std::move(result), selection_parts);
            }
        }
        return std::move(result);
    }
}
