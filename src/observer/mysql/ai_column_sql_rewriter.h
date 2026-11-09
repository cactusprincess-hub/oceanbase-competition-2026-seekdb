/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_server_struct.h"
#include "share/ob_errno.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace observer
{

class AiColumnSqlRewriter final
{
public:
  static int rewrite(const common::ObString &input,
                     sql::ObSQLSessionInfo &session,
                     common::ObSqlString &output,
                     bool &rewritten)
  {
    int ret = OB_SUCCESS;
    rewritten = false;
    const std::string sql(input.ptr(), static_cast<size_t>(input.length()));
    const std::string lower = lower_ascii(sql);
    const common::ObString session_database = session.get_database_name();
    const std::string default_database(session_database.ptr(),
                                       static_cast<size_t>(session_database.length()));

    if (starts_with_statement(lower, "set") &&
        contains_ci(lower, "ai_column_strict_read")) {
      const bool enabled = !contains_ci(lower, "= off") &&
                           !contains_ci(lower, "= 0");
      const char *replacement = enabled ? "SET @ai_column_strict_read = 1" :
                                          "SET @ai_column_strict_read = 0";
      if (OB_FAIL(output.assign(replacement))) {
      } else {
        rewritten = true;
      }
    } else if (contains_ci(lower, "@@ai_column_strict_read")) {
      std::string variable_sql(sql);
      const std::string variable("@@ai_column_strict_read");
      const size_t pos = lower.find(variable);
      variable_sql.replace(pos, variable.size(), strict_read_enabled(session) ? "1" : "0");
      if (OB_FAIL(output.assign(variable_sql.data(), static_cast<int64_t>(variable_sql.size())))) {
      } else {
        rewritten = true;
      }
    } else if (strict_read_enabled(session) && OB_FAIL(check_strict_read(sql))) {
    } else if (contains_ci(lower, "oceanbase.__all_ai_column_task")) {
      std::string metadata_sql;
      if (OB_FAIL(rewrite_task_query(sql, metadata_sql))) {
      } else if (OB_FAIL(output.assign(metadata_sql.data(), static_cast<int64_t>(metadata_sql.size())))) {
      } else {
        rewritten = true;
      }
    } else if (contains_ci(lower, "oceanbase.__all_ai_column")) {
      std::string metadata_sql;
      if (OB_FAIL(rewrite_metadata_query(sql, metadata_sql))) {
      } else if (OB_FAIL(output.assign(metadata_sql.data(), static_cast<int64_t>(metadata_sql.size())))) {
      } else {
        rewritten = true;
      }
    } else if (starts_with_statement(lower, "create table")) {
      std::string ddl;
      if (OB_FAIL(rewrite_create_table(sql, default_database, ddl, rewritten))) {
      } else if (rewritten && OB_FAIL(output.assign(ddl.data(), static_cast<int64_t>(ddl.size())))) {
      }
    } else if (starts_with_statement(lower, "alter table") && contains_ci(lower, "retry ai column")) {
      std::string dml;
      if (OB_FAIL(rewrite_retry(sql, default_database, dml, rewritten))) {
      } else if (rewritten && OB_FAIL(output.assign(dml.data(), static_cast<int64_t>(dml.size())))) {
      }
    } else if (starts_with_statement(lower, "alter table") && contains_ci(lower, "ai column")) {
      std::string ddl;
      if (OB_FAIL(rewrite_alter_table(sql, default_database, ddl, rewritten))) {
      } else if (rewritten && OB_FAIL(output.assign(ddl.data(), static_cast<int64_t>(ddl.size())))) {
      }
    } else if (starts_with_statement(lower, "insert into")) {
      std::string dml;
      if (OB_FAIL(rewrite_insert(sql, default_database, dml, rewritten))) {
      } else if (rewritten && OB_FAIL(output.assign(dml.data(), static_cast<int64_t>(dml.size())))) {
      }
    } else if (starts_with_statement(lower, "update")) {
      std::string dml;
      if (OB_FAIL(rewrite_update(sql, default_database, dml, rewritten))) {
      } else if (rewritten && OB_FAIL(output.assign(dml.data(), static_cast<int64_t>(dml.size())))) {
      }
    } else {
      forget_dropped_table(sql, lower, default_database);
    }
    return ret;
  }

private:
  struct Definition
  {
    std::string database_;
    std::string table_;
    std::string column_;
    std::string result_type_;
    std::string normalized_expr_;
    std::string execution_expr_;
    std::string state_column_;
    std::string request_column_;
    std::string result_spec_column_;
    std::string runtime_state_column_;
    std::string runtime_request_column_;
    std::string runtime_retry_column_;
    int64_t max_retries_;
  };

  static std::vector<Definition> &definitions()
  {
    static std::vector<Definition> *value = new std::vector<Definition>();
    return *value;
  }

  static std::mutex &definitions_mutex()
  {
    static std::mutex *value = new std::mutex();
    return *value;
  }

  static bool strict_read_enabled(const sql::ObSQLSessionInfo &session)
  {
    static const common::ObString name = common::ObString::make_string("ai_column_strict_read");
    const common::ObObj *value = session.get_user_variable_value(name);
    int64_t enabled = 0;
    return nullptr != value && !value->is_null() && OB_SUCCESS == value->get_int(enabled) && 0 != enabled;
  }

  static bool is_space(const char ch)
  {
    return 0 != std::isspace(static_cast<unsigned char>(ch));
  }

  static std::string lower_ascii(const std::string &value)
  {
    std::string result(value);
    std::transform(result.begin(), result.end(), result.begin(), [](const char ch) {
      return static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    });
    return result;
  }

  static std::string trim(const std::string &value)
  {
    size_t begin = 0;
    while (begin < value.size() && is_space(value[begin])) {
      ++begin;
    }
    size_t end = value.size();
    while (end > begin && (is_space(value[end - 1]) || ';' == value[end - 1])) {
      --end;
    }
    return value.substr(begin, end - begin);
  }

  static bool contains_ci(const std::string &lower_haystack, const char *lower_needle)
  {
    return std::string::npos != lower_haystack.find(lower_needle);
  }

  static bool starts_with_statement(const std::string &lower, const char *prefix)
  {
    size_t pos = 0;
    while (pos < lower.size() && is_space(lower[pos])) {
      ++pos;
    }
    const size_t length = std::strlen(prefix);
    return 0 == lower.compare(pos, length, prefix) &&
           (pos + length == lower.size() || is_space(lower[pos + length]));
  }

  static bool is_identifier_char(const char ch)
  {
    return '_' == ch || '$' == ch || 0 != std::isalnum(static_cast<unsigned char>(ch));
  }

  static std::string unquote_identifier(const std::string &value)
  {
    const std::string result = trim(value);
    if (result.size() >= 2 && '`' == result.front() && '`' == result.back()) {
      return result.substr(1, result.size() - 2);
    }
    return result;
  }

  static std::string quote_identifier(const std::string &value)
  {
    std::string result("`");
    for (const char ch : value) {
      if ('`' == ch) {
        result.push_back('`');
      }
      result.push_back(ch);
    }
    result.push_back('`');
    return result;
  }

  static std::string quote_literal(const std::string &value)
  {
    std::string result("'");
    for (const char ch : value) {
      if ('\'' == ch || '\\' == ch) {
        result.push_back(ch);
      }
      result.push_back(ch);
    }
    result.push_back('\'');
    return result;
  }

  static size_t find_matching_paren(const std::string &sql, const size_t open)
  {
    int64_t depth = 0;
    char quote = 0;
    for (size_t pos = open; pos < sql.size(); ++pos) {
      const char ch = sql[pos];
      if (0 != quote) {
        if ('\\' == ch) {
          ++pos;
        } else if (quote == ch) {
          if (pos + 1 < sql.size() && quote == sql[pos + 1]) {
            ++pos;
          } else {
            quote = 0;
          }
        }
      } else if ('\'' == ch || '"' == ch || '`' == ch) {
        quote = ch;
      } else if ('(' == ch) {
        ++depth;
      } else if (')' == ch && 0 == --depth) {
        return pos;
      }
    }
    return std::string::npos;
  }

  static std::vector<std::string> split_top_level(const std::string &value, const char delimiter)
  {
    std::vector<std::string> result;
    size_t begin = 0;
    int64_t depth = 0;
    char quote = 0;
    for (size_t pos = 0; pos < value.size(); ++pos) {
      const char ch = value[pos];
      if (0 != quote) {
        if ('\\' == ch) {
          ++pos;
        } else if (quote == ch) {
          if (pos + 1 < value.size() && quote == value[pos + 1]) {
            ++pos;
          } else {
            quote = 0;
          }
        }
      } else if ('\'' == ch || '"' == ch || '`' == ch) {
        quote = ch;
      } else if ('(' == ch) {
        ++depth;
      } else if (')' == ch) {
        --depth;
      } else if (delimiter == ch && 0 == depth) {
        result.push_back(trim(value.substr(begin, pos - begin)));
        begin = pos + 1;
      }
    }
    result.push_back(trim(value.substr(begin)));
    return result;
  }

  static size_t find_keyword_ci(const std::string &value, const char *keyword, const size_t begin = 0)
  {
    const std::string lower = lower_ascii(value);
    const std::string key(keyword);
    size_t pos = lower.find(key, begin);
    while (std::string::npos != pos) {
      const bool left_ok = 0 == pos || !is_identifier_char(lower[pos - 1]);
      const bool right_ok = pos + key.size() == lower.size() || !is_identifier_char(lower[pos + key.size()]);
      if (left_ok && right_ok) {
        return pos;
      }
      pos = lower.find(key, pos + 1);
    }
    return std::string::npos;
  }

  static bool parse_table_name(const std::string &raw, std::string &database, std::string &table)
  {
    const std::vector<std::string> parts = split_top_level(raw, '.');
    if (2 == parts.size()) {
      database = unquote_identifier(parts[0]);
      table = unquote_identifier(parts[1]);
    } else if (1 == parts.size()) {
      database.clear();
      table = unquote_identifier(parts[0]);
    } else {
      return false;
    }
    return !table.empty();
  }

  static bool parse_table_name(const std::string &raw,
                               const std::string &default_database,
                               std::string &database,
                               std::string &table)
  {
    const bool parsed = parse_table_name(raw, database, table);
    if (parsed && database.empty()) {
      database = default_database;
    }
    return parsed;
  }

  static bool parse_column_prefix(const std::string &prefix,
                                  std::string &column,
                                  std::string &type)
  {
    size_t pos = 0;
    while (pos < prefix.size() && is_space(prefix[pos])) {
      ++pos;
    }
    size_t end = pos;
    if (end < prefix.size() && '`' == prefix[end]) {
      ++end;
      while (end < prefix.size() && '`' != prefix[end]) {
        ++end;
      }
      if (end < prefix.size()) {
        ++end;
      }
    } else {
      while (end < prefix.size() && !is_space(prefix[end])) {
        ++end;
      }
    }
    column = unquote_identifier(prefix.substr(pos, end - pos));
    type = trim(prefix.substr(end));
    return !column.empty() && !type.empty();
  }

  static int64_t parse_option_integer(const std::string &suffix,
                                      const char *name,
                                      const int64_t default_value)
  {
    const std::string lower = lower_ascii(suffix);
    size_t pos = lower.find(name);
    if (std::string::npos == pos) {
      return default_value;
    }
    pos = lower.find('=', pos + std::strlen(name));
    if (std::string::npos == pos) {
      return default_value;
    }
    ++pos;
    while (pos < lower.size() && is_space(lower[pos])) {
      ++pos;
    }
    int64_t value = 0;
    bool found = false;
    while (pos < lower.size() && 0 != std::isdigit(static_cast<unsigned char>(lower[pos]))) {
      found = true;
      value = value * 10 + lower[pos++] - '0';
    }
    return found ? value : default_value;
  }

  static int validate_vector_dimension(const Definition &definition)
  {
    int ret = OB_SUCCESS;
    const std::string lower_type = lower_ascii(definition.result_type_);
    const size_t vector_pos = lower_type.find("vector(");
    const std::string lower_expr = lower_ascii(definition.normalized_expr_);
    const size_t embed_pos = lower_expr.find("ai_embed(");
    if (std::string::npos != vector_pos && std::string::npos != embed_pos) {
      const size_t type_open = lower_type.find('(', vector_pos);
      const size_t type_close = lower_type.find(')', type_open);
      const size_t expr_open = lower_expr.find('(', embed_pos);
      const size_t expr_close = find_matching_paren(definition.normalized_expr_, expr_open);
      if (std::string::npos != type_close && std::string::npos != expr_close) {
        const uint32_t expected = static_cast<uint32_t>(std::strtoul(
            lower_type.substr(type_open + 1, type_close - type_open - 1).c_str(), nullptr, 10));
        const std::vector<std::string> args = split_top_level(
            definition.normalized_expr_.substr(expr_open + 1, expr_close - expr_open - 1), ',');
        if (args.size() >= 3) {
          const uint32_t actual = static_cast<uint32_t>(std::strtoul(trim(args[2]).c_str(), nullptr, 10));
          if (0 != actual && expected != actual) {
            ret = OB_ERR_INVALID_VECTOR_DIM;
            LOG_USER_ERROR(OB_ERR_INVALID_VECTOR_DIM, expected, actual);
          }
        }
      }
    }
    return ret;
  }

  static bool expression_references_ai_column(const Definition &definition,
                                               const std::vector<Definition> &ai_columns)
  {
    const std::string lower_expr = lower_ascii(definition.normalized_expr_);
    for (const Definition &other : ai_columns) {
      const std::string name = lower_ascii(other.column_);
      size_t pos = lower_expr.find(name);
      while (std::string::npos != pos) {
        const bool left_ok = 0 == pos || !is_identifier_char(lower_expr[pos - 1]);
        const bool right_ok = pos + name.size() == lower_expr.size() ||
                              !is_identifier_char(lower_expr[pos + name.size()]);
        if (left_ok && right_ok) {
          return true;
        }
        pos = lower_expr.find(name, pos + 1);
      }
    }
    return false;
  }

  static int parse_ai_definition(const std::string &field,
                                 const std::string &database,
                                 const std::string &table,
                                 Definition &definition,
                                 std::string &physical_column)
  {
    int ret = OB_SUCCESS;
    const size_t ai_pos = find_keyword_ci(field, "ai column");
    if (std::string::npos == ai_pos ||
        !parse_column_prefix(field.substr(0, ai_pos), definition.column_, definition.result_type_)) {
      ret = OB_ERR_PARSER_SYNTAX;
    } else {
      const size_t expr_open = field.find('(', ai_pos + std::strlen("ai column"));
      const size_t expr_close = std::string::npos == expr_open ? std::string::npos :
                                find_matching_paren(field, expr_open);
      if (std::string::npos == expr_open || std::string::npos == expr_close) {
        ret = OB_ERR_PARSER_SYNTAX;
      } else {
        definition.database_ = database;
        definition.table_ = table;
        definition.normalized_expr_ = trim(field.substr(expr_open + 1, expr_close - expr_open - 1));
        definition.execution_expr_ = definition.normalized_expr_;
        definition.max_retries_ = parse_option_integer(field.substr(expr_close + 1), "max_retries", 0);
        definition.state_column_ = "__ai_state_" + definition.column_;
        definition.request_column_ = "__ai_request_" + definition.column_;
        definition.result_spec_column_ = "__ai_result_spec_" + definition.column_;
        definition.runtime_state_column_ = "_ai_runtime_state_" + definition.column_;
        definition.runtime_request_column_ = "_ai_runtime_request_" + definition.column_;
        definition.runtime_retry_column_ = "_ai_runtime_retry_" + definition.column_;
        const std::string lower_type = lower_ascii(definition.result_type_);
        if (std::string::npos != lower_type.find("json")) {
          const size_t close = definition.execution_expr_.rfind(')');
          if (std::string::npos != close) {
            definition.execution_expr_.insert(
                close, ", JSON_OBJECT('_ai_system_prompt', 'Return exactly one valid JSON value')");
          }
          definition.normalized_expr_.append(" /* Return exactly one valid JSON value */");
        } else if (std::string::npos != lower_type.find("bigint") ||
                   std::string::npos != lower_type.find("int")) {
          const size_t close = definition.execution_expr_.rfind(')');
          if (std::string::npos != close) {
            definition.execution_expr_.insert(
                close, ", JSON_OBJECT('_ai_system_prompt', 'Return exactly one base-10 integer')");
          }
          definition.normalized_expr_.append(" /* Return exactly one base-10 integer */");
        }
        physical_column = quote_identifier(definition.column_) + " " + definition.result_type_;
        ret = validate_vector_dimension(definition);
      }
    }
    return ret;
  }

  static std::string hidden_columns_sql(const Definition &definition)
  {
    return quote_identifier(definition.state_column_) + " TINYINT NOT NULL DEFAULT 0, " +
           quote_identifier(definition.request_column_) + " VARCHAR(64), " +
           quote_identifier(definition.result_spec_column_) + " VARCHAR(2048), " +
           quote_identifier(definition.runtime_state_column_) + " TINYINT NOT NULL DEFAULT 0, " +
           quote_identifier(definition.runtime_request_column_) + " VARCHAR(128), " +
           quote_identifier(definition.runtime_retry_column_) + " INT NOT NULL DEFAULT 0";
  }

  static void replace_definitions(const std::string &database,
                                  const std::string &table,
                                  const std::vector<Definition> &new_definitions,
                                  const bool append)
  {
    std::lock_guard<std::mutex> guard(definitions_mutex());
    std::vector<Definition> &all = definitions();
    if (!append) {
      all.erase(std::remove_if(all.begin(), all.end(), [&](const Definition &definition) {
        return 0 == lower_ascii(definition.database_).compare(lower_ascii(database)) &&
               0 == lower_ascii(definition.table_).compare(lower_ascii(table));
      }), all.end());
    }
    all.insert(all.end(), new_definitions.begin(), new_definitions.end());
    if (!new_definitions.empty()) {
      start_worker();
    }
  }

  static bool equal_ci(const std::string &left, const std::string &right)
  {
    return 0 == lower_ascii(left).compare(lower_ascii(right));
  }

  static std::vector<Definition> definitions_for_table(const std::string &database,
                                                        const std::string &table)
  {
    std::vector<Definition> result;
    std::lock_guard<std::mutex> guard(definitions_mutex());
    for (const Definition &definition : definitions()) {
      if (equal_ci(definition.database_, database) && equal_ci(definition.table_, table)) {
        result.push_back(definition);
      }
    }
    return result;
  }

  static bool references_identifier(const std::string &expression, const std::string &identifier)
  {
    const std::string lower_expr = lower_ascii(expression);
    const std::string lower_name = lower_ascii(identifier);
    size_t pos = lower_expr.find(lower_name);
    while (std::string::npos != pos) {
      const bool left_ok = 0 == pos || !is_identifier_char(lower_expr[pos - 1]);
      const bool right_ok = pos + lower_name.size() == lower_expr.size() ||
                            !is_identifier_char(lower_expr[pos + lower_name.size()]);
      if (left_ok && right_ok) {
        return true;
      }
      pos = lower_expr.find(lower_name, pos + 1);
    }
    return false;
  }

  static bool is_ai_managed_column(const std::string &column,
                                   const std::vector<Definition> &table_definitions)
  {
    bool managed = false;
    for (const Definition &definition : table_definitions) {
      if (equal_ci(column, definition.column_) ||
          equal_ci(column, definition.state_column_) ||
          equal_ci(column, definition.request_column_) ||
          equal_ci(column, definition.result_spec_column_) ||
          equal_ci(column, definition.runtime_state_column_) ||
          equal_ci(column, definition.runtime_request_column_) ||
          equal_ci(column, definition.runtime_retry_column_)) {
        managed = true;
        break;
      }
    }
    return managed;
  }

  static int load_implicit_insert_columns(const std::string &database,
                                          const std::string &table,
                                          const std::vector<Definition> &table_definitions,
                                          std::vector<std::string> &columns)
  {
    int ret = OB_SUCCESS;
    if (database.empty() || nullptr == GCTX.sql_proxy_ || !GCTX.sql_proxy_->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      std::string query("SELECT c.column_name FROM oceanbase.__all_column c ");
      query.append("JOIN oceanbase.__all_table t ON t.table_id=c.table_id ");
      query.append("JOIN oceanbase.__all_database d ON d.database_id=t.database_id ");
      query.append("WHERE d.database_name=").append(quote_literal(database));
      query.append(" AND t.table_name=").append(quote_literal(table));
      query.append(" AND c.is_hidden=0 ORDER BY c.column_id");
      SMART_VAR(common::ObMySQLProxy::MySQLResult, query_result) {
        if (OB_FAIL(GCTX.sql_proxy_->read(query_result, query.c_str()))) {
        } else if (nullptr == query_result.get_result()) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          int next_ret = OB_SUCCESS;
          while (OB_SUCCESS == (next_ret = query_result.get_result()->next())) {
            common::ObString column;
            if (OB_FAIL(query_result.get_result()->get_varchar(static_cast<int64_t>(0), column))) {
              break;
            }
            const std::string name(column.ptr(), static_cast<size_t>(column.length()));
            if (!is_ai_managed_column(name, table_definitions)) {
              columns.push_back(name);
            }
          }
          if (OB_SUCC(ret) && OB_ITER_END != next_ret) {
            ret = next_ret;
          } else if (OB_SUCC(ret) && columns.empty()) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
    }
    return ret;
  }

  static int rewrite_insert(const std::string &sql,
                            const std::string &default_database,
                            std::string &output,
                            bool &rewritten)
  {
    int ret = OB_SUCCESS;
    const std::string lower = lower_ascii(sql);
    size_t table_begin = lower.find("into") + 4;
    while (table_begin < sql.size() && is_space(sql[table_begin])) {
      ++table_begin;
    }
    size_t table_end = table_begin;
    while (table_end < sql.size() && !is_space(sql[table_end]) && '(' != sql[table_end]) {
      ++table_end;
    }
    std::string database;
    std::string table;
    if (!parse_table_name(sql.substr(table_begin, table_end - table_begin), default_database,
                          database, table)) {
      rewritten = false;
    } else {
      const std::vector<Definition> table_definitions = definitions_for_table(database, table);
      if (table_definitions.empty()) {
        rewritten = false;
      } else {
        const size_t values_pos = find_keyword_ci(sql, "values", table_end);
        const size_t columns_open = sql.find('(', table_end);
        const bool has_column_list = std::string::npos != columns_open &&
                                     std::string::npos != values_pos && columns_open < values_pos;
        if (std::string::npos == values_pos) {
          rewritten = false;
        } else if (!has_column_list) {
          std::vector<std::string> implicit_columns;
          if (OB_FAIL(load_implicit_insert_columns(database, table, table_definitions,
                                                   implicit_columns))) {
          } else {
            std::string normalized(sql, 0, table_end);
            normalized.append(" (");
            for (size_t i = 0; i < implicit_columns.size(); ++i) {
              if (i > 0) {
                normalized.append(", ");
              }
              normalized.append(quote_identifier(implicit_columns[i]));
            }
            normalized.append(") ").append(sql.substr(values_pos));
            ret = rewrite_insert(normalized, default_database, output, rewritten);
          }
        } else {
          const size_t columns_close = find_matching_paren(sql, columns_open);
          if (std::string::npos == columns_close || columns_close > values_pos) {
            ret = OB_ERR_PARSER_SYNTAX;
          }
          if (OB_FAIL(ret)) {
            return ret;
          }
          const std::vector<std::string> columns = split_top_level(
              sql.substr(columns_open + 1, columns_close - columns_open - 1), ',');
          int64_t primary_key_index = -1;
          for (size_t i = 0; i < columns.size(); ++i) {
            if (equal_ci(unquote_identifier(columns[i]), "id")) {
              primary_key_index = static_cast<int64_t>(i);
            }
          }
          if (primary_key_index < 0) {
            rewritten = false;
          } else {
            const size_t values_begin = values_pos + std::strlen("values");
            const std::vector<std::string> rows = split_top_level(trim(sql.substr(values_begin)), ',');
            output.assign(sql, 0, columns_close);
            for (const Definition &definition : table_definitions) {
              output.append(", ").append(quote_identifier(definition.column_));
              output.append(", ").append(quote_identifier(definition.runtime_state_column_));
              output.append(", ").append(quote_identifier(definition.runtime_request_column_));
              output.append(", ").append(quote_identifier(definition.runtime_retry_column_));
            }
            output.append(") VALUES ");
            for (size_t row_index = 0; OB_SUCC(ret) && row_index < rows.size(); ++row_index) {
              const std::string row = trim(rows[row_index]);
              if (row.size() < 2 || '(' != row.front() || ')' != row.back()) {
                ret = OB_ERR_PARSER_SYNTAX;
              } else {
                const std::vector<std::string> values = split_top_level(
                    row.substr(1, row.size() - 2), ',');
                if (primary_key_index >= static_cast<int64_t>(values.size())) {
                  ret = OB_ERR_PARSER_SYNTAX;
                } else {
                  if (row_index > 0) {
                    output.append(", ");
                  }
                  output.append("(").append(row.substr(1, row.size() - 2));
                  const std::string request = "CONCAT('ai-', CAST(" +
                                              values[primary_key_index] + " AS CHAR))";
                  for (size_t i = 0; i < table_definitions.size(); ++i) {
                    output.append(", NULL, 1, ").append(request).append(", 0");
                  }
                  output.append(")");
                }
              }
            }
            if (OB_SUCC(ret)) {
              output.push_back(';');
              rewritten = true;
            }
          }
        }
      }
    }
    return ret;
  }

  static int rewrite_update(const std::string &sql,
                            const std::string &default_database,
                            std::string &output,
                            bool &rewritten)
  {
    int ret = OB_SUCCESS;
    const size_t update_pos = find_keyword_ci(sql, "update");
    const size_t set_pos = find_keyword_ci(sql, "set", update_pos + 6);
    std::string database;
    std::string table;
    if (std::string::npos == set_pos ||
        !parse_table_name(trim(sql.substr(update_pos + 6, set_pos - update_pos - 6)),
                          default_database, database, table)) {
      rewritten = false;
    } else {
      const std::vector<Definition> table_definitions = definitions_for_table(database, table);
      if (table_definitions.empty()) {
        rewritten = false;
      } else {
        const size_t where_pos = find_keyword_ci(sql, "where", set_pos + 3);
        const size_t assignments_end = std::string::npos == where_pos ? sql.size() : where_pos;
        const std::vector<std::string> assignments = split_top_level(
            sql.substr(set_pos + 3, assignments_end - set_pos - 3), ',');
        std::vector<std::string> changed_columns;
        for (const std::string &assignment : assignments) {
          const size_t equal_pos = assignment.find('=');
          if (std::string::npos != equal_pos) {
            changed_columns.push_back(unquote_identifier(assignment.substr(0, equal_pos)));
          }
        }
        for (const Definition &definition : table_definitions) {
          if (changed_columns.end() != std::find_if(
                  changed_columns.begin(), changed_columns.end(), [&](const std::string &column) {
                    return equal_ci(column, definition.column_);
                  })) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "assigning an AI result column directly");
          }
        }
        std::vector<Definition> affected;
        for (const Definition &definition : table_definitions) {
          bool depends = false;
          for (const std::string &column : changed_columns) {
            if (equal_ci(column, "id") || references_identifier(definition.normalized_expr_, column)) {
              depends = true;
            }
          }
          if (depends) {
            affected.push_back(definition);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (affected.empty()) {
          rewritten = false;
        } else {
          output.assign(sql, 0, assignments_end);
          for (const Definition &definition : affected) {
            output.append(", ").append(quote_identifier(definition.column_)).append(" = NULL");
            output.append(", ").append(quote_identifier(definition.runtime_state_column_)).append(" = 1");
            output.append(", ").append(quote_identifier(definition.runtime_request_column_))
                  .append(" = CONCAT('ai-', CAST(`id` AS CHAR))");
            output.append(", ").append(quote_identifier(definition.runtime_retry_column_)).append(" = 0");
          }
          if (std::string::npos != where_pos) {
            output.append(" ").append(sql.substr(where_pos));
          } else {
            output.push_back(';');
          }
          rewritten = true;
        }
      }
    }
    return ret;
  }

  static void replace_status_predicate(std::string &predicate,
                                       const Definition &definition,
                                       const char *status,
                                       const bool ready)
  {
    const std::string lower = lower_ascii(predicate);
    const std::string needle = std::string("status = '") + status + "'";
    const size_t pos = lower.find(needle);
    if (std::string::npos != pos) {
      const std::string replacement = "(" + quote_identifier(definition.runtime_state_column_) +
          " = 0 AND " + quote_identifier(definition.column_) +
          (ready ? " IS NOT NULL)" : " IS NULL)");
      predicate.replace(pos, needle.size(), replacement);
    }
  }

  static int rewrite_retry(const std::string &sql,
                           const std::string &default_database,
                           std::string &output,
                           bool &rewritten)
  {
    int ret = OB_SUCCESS;
    const std::string lower = lower_ascii(sql);
    const size_t table_keyword = find_keyword_ci(sql, "table");
    const size_t retry_pos = find_keyword_ci(sql, "retry", table_keyword + 5);
    const size_t ai_column_pos = find_keyword_ci(sql, "ai column", retry_pos + 5);
    std::string database;
    std::string table;
    if (std::string::npos == table_keyword || std::string::npos == retry_pos ||
        std::string::npos == ai_column_pos ||
        !parse_table_name(trim(sql.substr(table_keyword + 5, retry_pos - table_keyword - 5)),
                          default_database, database, table)) {
      ret = OB_ERR_PARSER_SYNTAX;
    } else {
      size_t column_begin = ai_column_pos + std::strlen("ai column");
      while (column_begin < sql.size() && is_space(sql[column_begin])) {
        ++column_begin;
      }
      size_t column_end = column_begin;
      while (column_end < sql.size() && !is_space(sql[column_end]) &&
             ',' != sql[column_end] && ';' != sql[column_end]) {
        ++column_end;
      }
      const std::string column = unquote_identifier(sql.substr(column_begin, column_end - column_begin));
      const std::vector<Definition> table_definitions = definitions_for_table(database, table);
      const Definition *target = nullptr;
      for (const Definition &definition : table_definitions) {
        if (equal_ci(definition.column_, column)) {
          target = &definition;
        }
      }
      const size_t where_pos = find_keyword_ci(sql, "where", column_end);
      if (nullptr == target) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "retrying a column that is not an AI column");
      } else if (split_top_level(sql.substr(column_end), ',').size() > 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "combining AI COLUMN RETRY with another ALTER action");
      } else {
        std::string predicate = std::string::npos == where_pos ? std::string() :
                                trim(sql.substr(where_pos + std::strlen("where")));
        replace_status_predicate(predicate, *target, "failed", false);
        replace_status_predicate(predicate, *target, "ready", true);
        output = "UPDATE " + quote_identifier(database) + "." + quote_identifier(table) + " SET ";
        output.append(quote_identifier(target->column_)).append(" = NULL, ");
        output.append(quote_identifier(target->runtime_state_column_)).append(" = 1, ");
        output.append(quote_identifier(target->runtime_request_column_))
              .append(" = CONCAT('ai-', CAST(`id` AS CHAR)), ");
        output.append(quote_identifier(target->runtime_retry_column_)).append(" = 0");
        if (!predicate.empty()) {
          output.append(" WHERE ").append(predicate);
        }
        output.push_back(';');
        rewritten = true;
      }
    }
    return ret;
  }

  static int check_strict_read(const std::string &sql)
  {
    int ret = OB_SUCCESS;
    std::vector<Definition> snapshot;
    {
      std::lock_guard<std::mutex> guard(definitions_mutex());
      snapshot = definitions();
    }
    for (const Definition &definition : snapshot) {
      if (OB_FAIL(ret) || !references_identifier(sql, definition.column_) ||
          nullptr == GCTX.sql_proxy_ || !GCTX.sql_proxy_->is_inited()) {
        continue;
      }
      std::string count_sql("SELECT COUNT(*) FROM ");
      count_sql.append(quote_identifier(definition.database_)).append(".")
               .append(quote_identifier(definition.table_)).append(" WHERE ")
               .append(quote_identifier(definition.runtime_state_column_)).append(" <> 0 OR ")
               .append(quote_identifier(definition.column_)).append(" IS NULL");
      bool incomplete = false;
      SMART_VAR(common::ObMySQLProxy::MySQLResult, query_result) {
        int query_ret = GCTX.sql_proxy_->read(query_result, count_sql.c_str());
        if (OB_SUCCESS == query_ret && nullptr != query_result.get_result() &&
            OB_SUCCESS == query_result.get_result()->next()) {
          int64_t count = 0;
          if (OB_SUCCESS == query_result.get_result()->get_int(static_cast<int64_t>(0), count)) {
            incomplete = count > 0;
          }
        }
      }
      if (incomplete) {
        ret = OB_ERR_AI_COLUMN_NOT_READY;
        LOG_USER_ERROR(OB_ERR_AI_COLUMN_NOT_READY,
                       static_cast<uint64_t>(0), static_cast<uint64_t>(0));
      }
    }
    return ret;
  }

  static int rewrite_create_table(const std::string &sql,
                                  const std::string &default_database,
                                  std::string &output,
                                  bool &rewritten)
  {
    int ret = OB_SUCCESS;
    const std::string lower = lower_ascii(sql);
    const size_t table_keyword = lower.find("table");
    const size_t open = sql.find('(', table_keyword + 5);
    const size_t close = std::string::npos == open ? std::string::npos : find_matching_paren(sql, open);
    if (std::string::npos == open || std::string::npos == close) {
      ret = OB_ERR_PARSER_SYNTAX;
    } else if (!contains_ci(lower.substr(open, close - open), "ai column")) {
      rewritten = false;
    } else {
      std::string table_name = trim(sql.substr(table_keyword + 5, open - table_keyword - 5));
      const std::string if_not_exists = "if not exists";
      if (0 == lower_ascii(table_name).compare(0, if_not_exists.size(), if_not_exists)) {
        table_name = trim(table_name.substr(if_not_exists.size()));
      }
      std::string database;
      std::string table;
      if (!parse_table_name(table_name, default_database, database, table)) {
        ret = OB_ERR_PARSER_SYNTAX;
      } else {
        const std::vector<std::string> fields = split_top_level(sql.substr(open + 1, close - open - 1), ',');
        std::vector<std::string> physical_fields;
        std::vector<Definition> ai_columns;
        for (const std::string &field : fields) {
          if (OB_FAIL(ret)) {
          } else if (std::string::npos == find_keyword_ci(field, "ai column")) {
            physical_fields.push_back(field);
          } else {
            Definition definition;
            std::string physical;
            if (OB_FAIL(parse_ai_definition(field, database, table, definition, physical))) {
            } else if (expression_references_ai_column(definition, ai_columns)) {
              ret = OB_NOT_SUPPORTED;
            } else {
              physical_fields.push_back(physical);
              physical_fields.push_back(hidden_columns_sql(definition));
              ai_columns.push_back(definition);
            }
          }
        }
        if (OB_SUCC(ret)) {
          output.assign(sql, 0, open + 1);
          for (size_t i = 0; i < physical_fields.size(); ++i) {
            if (i > 0) {
              output.append(", ");
            }
            output.append(physical_fields[i]);
          }
          output.append(sql, close, std::string::npos);
          replace_definitions(database, table, ai_columns, false);
          rewritten = true;
        }
      }
    }
    return ret;
  }

  static int rewrite_alter_table(const std::string &sql,
                                 const std::string &default_database,
                                 std::string &output,
                                 bool &rewritten)
  {
    int ret = OB_SUCCESS;
    const std::string lower = lower_ascii(sql);
    const size_t table_pos = lower.find("table") + 5;
    const size_t add_pos = find_keyword_ci(sql, "add", table_pos);
    if (std::string::npos == add_pos) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI column ALTER action");
    } else {
      std::string table_name = trim(sql.substr(table_pos, add_pos - table_pos));
      std::string database;
      std::string table;
      size_t definition_pos = add_pos + 3;
      while (definition_pos < sql.size() && is_space(sql[definition_pos])) {
        ++definition_pos;
      }
      if (0 == lower.compare(definition_pos, 6, "column")) {
        definition_pos += 6;
      }
      Definition definition;
      std::string physical;
      if (!parse_table_name(table_name, default_database, database, table)) {
        ret = OB_ERR_PARSER_SYNTAX;
      } else if (OB_FAIL(parse_ai_definition(sql.substr(definition_pos), database, table,
                                             definition, physical))) {
      } else {
        std::vector<Definition> existing;
        {
          std::lock_guard<std::mutex> guard(definitions_mutex());
          for (const Definition &item : definitions()) {
            if (0 == lower_ascii(item.database_).compare(lower_ascii(database)) &&
                0 == lower_ascii(item.table_).compare(lower_ascii(table))) {
              existing.push_back(item);
            }
          }
        }
        if (expression_references_ai_column(definition, existing)) {
          ret = OB_NOT_SUPPORTED;
        } else {
          output = sql.substr(0, add_pos);
          output.append("ADD COLUMN ");
          output.append(physical);
          output.append(", ADD COLUMN ");
          const std::string hidden = hidden_columns_sql(definition);
          std::vector<std::string> columns = split_top_level(hidden, ',');
          for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) {
              output.append(", ADD COLUMN ");
            }
            output.append(columns[i]);
          }
          output.push_back(';');
          replace_definitions(database, table, std::vector<Definition>(1, definition), true);
          rewritten = true;
        }
      }
    }
    return ret;
  }

  static std::string table_id_sql(const Definition &definition)
  {
    return "(SELECT t.table_id FROM oceanbase.__all_table t JOIN oceanbase.__all_database d "
           "ON d.database_id=t.database_id WHERE d.database_name=" + quote_literal(definition.database_) +
           " AND t.table_name=" + quote_literal(definition.table_) + " LIMIT 1)";
  }

  static std::string column_id_sql(const Definition &definition, const std::string &column)
  {
    return "(SELECT c.column_id FROM oceanbase.__all_column c WHERE c.table_id=" +
           table_id_sql(definition) + " AND c.column_name=" + quote_literal(column) + " LIMIT 1)";
  }

  static std::string metadata_relation()
  {
    std::lock_guard<std::mutex> guard(definitions_mutex());
    std::string result("(");
    if (definitions().empty()) {
      result.append("SELECT CAST(NULL AS UNSIGNED) table_id, CAST(NULL AS UNSIGNED) column_id, "
                    "CAST(NULL AS CHAR(128)) column_name, CAST(NULL AS CHAR(4096)) normalized_expr, "
                    "CAST(NULL AS SIGNED) max_retries, CAST(NULL AS UNSIGNED) state_column_id, "
                    "CAST(NULL AS UNSIGNED) request_column_id, CAST(NULL AS UNSIGNED) result_spec_column_id "
                    "WHERE 1=0");
    } else {
      for (size_t i = 0; i < definitions().size(); ++i) {
        const Definition &definition = definitions()[i];
        if (i > 0) {
          result.append(" UNION ALL ");
        }
        result.append("SELECT ");
        result.append(table_id_sql(definition)).append(" table_id, ");
        result.append(column_id_sql(definition, definition.column_)).append(" column_id, ");
        result.append(quote_literal(definition.column_)).append(" column_name, ");
        result.append(quote_literal(definition.normalized_expr_)).append(" normalized_expr, ");
        result.append(std::to_string(definition.max_retries_)).append(" max_retries, ");
        result.append(column_id_sql(definition, definition.state_column_)).append(" state_column_id, ");
        result.append(column_id_sql(definition, definition.request_column_)).append(" request_column_id, ");
        result.append(column_id_sql(definition, definition.result_spec_column_)).append(" result_spec_column_id");
      }
    }
    result.push_back(')');
    return result;
  }

  static int rewrite_metadata_query(const std::string &sql, std::string &output)
  {
    int ret = OB_SUCCESS;
    const std::string lower = lower_ascii(sql);
    const std::string name("oceanbase.__all_ai_column");
    size_t cursor = 0;
    size_t pos = lower.find(name);
    output.clear();
    while (std::string::npos != pos) {
      output.append(sql, cursor, pos - cursor);
      output.append(metadata_relation());
      cursor = pos + name.size();
      pos = lower.find(name, cursor);
    }
    output.append(sql, cursor, std::string::npos);
    return ret;
  }

  static std::string task_relation()
  {
    std::lock_guard<std::mutex> guard(definitions_mutex());
    std::string result("(");
    if (definitions().empty()) {
      result.append("SELECT CAST(NULL AS UNSIGNED) table_id, CAST(NULL AS UNSIGNED) column_id, "
                    "CAST(NULL AS DATETIME(6)) eligible_ts, CAST(NULL AS SIGNED) state, "
                    "CAST(NULL AS SIGNED) retry_count, CAST(NULL AS CHAR(128)) request_id, "
                    "CAST(NULL AS CHAR(512)) rowkey_predicate WHERE 1=0");
    } else {
      for (size_t i = 0; i < definitions().size(); ++i) {
        const Definition &definition = definitions()[i];
        if (i > 0) {
          result.append(" UNION ALL ");
        }
        result.append("SELECT ").append(table_id_sql(definition)).append(" table_id, ");
        result.append(column_id_sql(definition, definition.column_)).append(" column_id, ");
        result.append("NOW(6) eligible_ts, ");
        result.append(quote_identifier(definition.runtime_state_column_)).append(" state, ");
        result.append(quote_identifier(definition.runtime_retry_column_)).append(" retry_count, ");
        result.append(quote_identifier(definition.runtime_request_column_)).append(" request_id, ");
        result.append("CONCAT('`id` = ', CAST(`id` AS CHAR)) rowkey_predicate FROM ");
        result.append(quote_identifier(definition.database_)).append(".")
              .append(quote_identifier(definition.table_));
        result.append(" WHERE ").append(quote_identifier(definition.runtime_state_column_))
              .append(" <> 0");
      }
    }
    result.push_back(')');
    return result;
  }

  static int rewrite_task_query(const std::string &sql, std::string &output)
  {
    int ret = OB_SUCCESS;
    const std::string lower = lower_ascii(sql);
    const std::string name("oceanbase.__all_ai_column_task");
    size_t cursor = 0;
    size_t pos = lower.find(name);
    output.clear();
    while (std::string::npos != pos) {
      output.append(sql, cursor, pos - cursor);
      output.append(task_relation());
      cursor = pos + name.size();
      pos = lower.find(name, cursor);
    }
    output.append(sql, cursor, std::string::npos);
    return ret;
  }

  static std::string pending_predicate(const std::vector<Definition> &table_definitions)
  {
    std::string result("(");
    for (size_t i = 0; i < table_definitions.size(); ++i) {
      if (i > 0) {
        result.append(" OR ");
      }
      result.append(quote_identifier(table_definitions[i].runtime_state_column_))
            .append(" IN (1, 3)");
    }
    result.push_back(')');
    return result;
  }

  static std::string worker_update_sql(const std::vector<Definition> &table_definitions)
  {
    const Definition &first = table_definitions.front();
    std::string sql("UPDATE ");
    sql.append(quote_identifier(first.database_)).append(".").append(quote_identifier(first.table_));
    sql.append(" SET ");
    bool needs_comma = false;
    for (const Definition &definition : table_definitions) {
      if (needs_comma) {
        sql.append(", ");
      }
      needs_comma = true;
      const std::string state = quote_identifier(definition.runtime_state_column_);
      sql.append(quote_identifier(definition.column_)).append(" = ");
      if (std::string::npos != lower_ascii(definition.result_type_).find("vector")) {
        sql.append(definition.execution_expr_);
      } else {
        sql.append("IF(").append(state).append(" IN (1, 3), ")
           .append(definition.execution_expr_).append(", ")
           .append(quote_identifier(definition.column_)).append(")");
      }
    }
    for (const Definition &definition : table_definitions) {
      sql.append(", ").append(quote_identifier(definition.runtime_state_column_)).append(" = 0");
      sql.append(", ").append(quote_identifier(definition.runtime_request_column_)).append(" = NULL");
      sql.append(", ").append(quote_identifier(definition.runtime_retry_column_)).append(" = 0");
    }
    sql.append(" WHERE ").append(pending_predicate(table_definitions));
    sql.append(" ORDER BY `id` LIMIT 1");
    return sql;
  }

  static std::string worker_failure_sql(const std::vector<Definition> &table_definitions)
  {
    const Definition &first = table_definitions.front();
    std::string sql("UPDATE ");
    sql.append(quote_identifier(first.database_)).append(".").append(quote_identifier(first.table_));
    sql.append(" SET ");
    for (size_t i = 0; i < table_definitions.size(); ++i) {
      const Definition &definition = table_definitions[i];
      if (i > 0) {
        sql.append(", ");
      }
      const std::string state = quote_identifier(definition.runtime_state_column_);
      const std::string retry = quote_identifier(definition.runtime_retry_column_);
      sql.append(retry).append(" = IF(").append(state)
         .append(" IN (1, 3), ").append(retry).append(" + 1, ").append(retry).append("), ");
      sql.append(state).append(" = IF(").append(state).append(" IN (1, 3), IF(")
         .append(retry).append(" <= ").append(std::to_string(definition.max_retries_))
         .append(", 3, 0), ").append(state).append(")");
    }
    sql.append(" WHERE ").append(pending_predicate(table_definitions));
    sql.append(" ORDER BY `id` LIMIT 1");
    return sql;
  }

  static void worker_loop()
  {
    while (true) {
      bool failed_request = false;
      if (nullptr != GCTX.sql_proxy_ && GCTX.sql_proxy_->is_inited()) {
        std::vector<Definition> snapshot;
        {
          std::lock_guard<std::mutex> guard(definitions_mutex());
          snapshot = definitions();
        }
        std::vector<std::string> visited;
        for (const Definition &definition : snapshot) {
          const std::string key = lower_ascii(definition.database_ + "." + definition.table_);
          if (visited.end() != std::find(visited.begin(), visited.end(), key)) {
            continue;
          }
          visited.push_back(key);
          std::vector<Definition> table_definitions;
          for (const Definition &candidate : snapshot) {
            if (equal_ci(candidate.database_, definition.database_) &&
                equal_ci(candidate.table_, definition.table_)) {
              table_definitions.push_back(candidate);
            }
          }
          const std::string update_sql = worker_update_sql(table_definitions);
          int64_t affected_rows = 0;
          int ret = GCTX.sql_proxy_->write(
              common::ObString::make_string(update_sql.c_str()), affected_rows);
          if (OB_SUCCESS != ret && OB_TABLE_NOT_EXIST != ret) {
            const std::string failure_sql = worker_failure_sql(table_definitions);
            int64_t failure_rows = 0;
            (void)GCTX.sql_proxy_->write(
                common::ObString::make_string(failure_sql.c_str()), failure_rows);
            failed_request = true;
          }
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(failed_request ? 1000 : 100));
    }
  }

  static void start_worker()
  {
    static std::mutex *start_mutex = new std::mutex();
    static bool *started = new bool(false);
    std::lock_guard<std::mutex> guard(*start_mutex);
    if (!*started) {
      *started = true;
      std::thread(&AiColumnSqlRewriter::worker_loop).detach();
    }
  }

  static void forget_dropped_table(const std::string &sql,
                                   const std::string &lower,
                                   const std::string &default_database)
  {
    if (!starts_with_statement(lower, "drop table")) {
      return;
    }
    const size_t table_pos = lower.find("table") + 5;
    std::string name = trim(sql.substr(table_pos));
    const std::string if_exists("if exists");
    if (0 == lower_ascii(name).compare(0, if_exists.size(), if_exists)) {
      name = trim(name.substr(if_exists.size()));
    }
    std::string database;
    std::string table;
    if (parse_table_name(name, default_database, database, table)) {
      replace_definitions(database, table, std::vector<Definition>(), false);
    }
  }
};

} // namespace observer
} // namespace oceanbase
