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

#ifndef OCEANBASE_OBSERVER_MYSQL_OB_FILE_SQL_REWRITER_H_
#define OCEANBASE_OBSERVER_MYSQL_OB_FILE_SQL_REWRITER_H_

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <glob.h>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_errno.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace observer
{

class ObFileSqlRewriter final
{
public:
  static int rewrite(const common::ObString &input,
                     sql::ObSQLSessionInfo &session,
                     common::ObSqlString &output,
                     bool &rewritten)
  {
    int ret = OB_SUCCESS;
    rewritten = false;
    const std::string input_sql(input.ptr(), static_cast<size_t>(input.length()));
    bool describe_rewritten = false;
    if (OB_FAIL(rewrite_describe(input_sql, session, output, describe_rewritten))) {
      return ret;
    } else if (describe_rewritten) {
      rewritten = true;
      return ret;
    }
    std::string copy_sql;
    bool copy_rewritten = false;
    if (OB_FAIL(rewrite_copy(input_sql, session, copy_sql, copy_rewritten))) {
      return ret;
    }
    const std::string &sql = copy_rewritten ? copy_sql : input_sql;
    std::string result;
    size_t cursor = 0;
    int64_t relation_id = 0;

    while (OB_SUCC(ret)) {
      Match match;
      if (!find_next(sql, cursor, match)) {
        result.append(sql, cursor, std::string::npos);
        break;
      }

      result.append(sql, cursor, match.begin_ - cursor);
      std::string relation;
      if (OB_FAIL(build_relation(sql.substr(match.args_begin_, match.args_end_ - match.args_begin_),
                                 match.kind_, session, relation))) {
      } else {
        result.append(relation);
        if (!has_explicit_alias(sql, match.end_)) {
          result.append(" AS __file_sql_");
          result.append(std::to_string(++relation_id));
        }
        cursor = match.end_;
        rewritten = true;
      }
    }

    rewritten = rewritten || copy_rewritten;
    if (OB_SUCC(ret) && rewritten) {
      ret = output.assign(result.data(), static_cast<int64_t>(result.size()));
    }
    return ret;
  }

private:
  enum class Kind { CSV, JSONL, PARQUET, GLOB };
  enum class ValueType { NULL_VALUE, BOOL, INT, DOUBLE, DATE, DATETIME, STRING };

  struct Match
  {
    Match() : kind_(Kind::CSV), begin_(0), args_begin_(0), args_end_(0), end_(0) {}
    Kind kind_;
    size_t begin_;
    size_t args_begin_;
    size_t args_end_;
    size_t end_;
  };

  struct Cell
  {
    Cell() : value_(), is_null_(true) {}
    explicit Cell(const std::string &value) : value_(value), is_null_(false) {}
    std::string value_;
    bool is_null_;
  };

  struct Relation
  {
    std::vector<std::string> columns_;
    std::vector<std::vector<Cell> > rows_;
  };

  struct CompactField
  {
    CompactField() : id_(0), type_(0), bool_value_(false) {}
    int16_t id_;
    uint8_t type_;
    bool bool_value_;
  };

  class CompactReader
  {
  public:
    CompactReader(const std::string &data, const size_t begin, const size_t end)
        : data_(data), pos_(begin), end_(std::min(end, data.size())), valid_(begin <= end_) {}

    bool valid() const { return valid_; }
    size_t position() const { return pos_; }

    bool read_field(int16_t &previous_id, CompactField &field)
    {
      uint8_t header = 0;
      if (!read_byte(header)) {
        return false;
      }
      field.type_ = header & 0x0F;
      field.bool_value_ = 1 == field.type_;
      if (0 == field.type_) {
        return true;
      }
      const uint8_t delta = header >> 4;
      int64_t id = 0;
      if (0 != delta) {
        id = previous_id + delta;
      } else if (!read_zigzag(id)) {
        return false;
      }
      if (id < 0 || id > INT16_MAX) {
        valid_ = false;
        return false;
      }
      field.id_ = static_cast<int16_t>(id);
      previous_id = field.id_;
      return true;
    }

    bool read_integer(int64_t &value) { return read_zigzag(value); }

    bool read_binary(std::string &value)
    {
      uint64_t size = 0;
      if (!read_varint(size) || size > end_ - pos_) {
        valid_ = false;
        return false;
      }
      value.assign(data_, pos_, static_cast<size_t>(size));
      pos_ += static_cast<size_t>(size);
      return true;
    }

    bool read_collection(uint8_t &element_type, uint64_t &size)
    {
      uint8_t header = 0;
      if (!read_byte(header)) {
        return false;
      }
      element_type = header & 0x0F;
      size = header >> 4;
      return 15 != size || read_varint(size);
    }

    bool skip(const uint8_t type)
    {
      bool success = valid_;
      if (!success || 0 == type || 1 == type || 2 == type) {
      } else if (3 == type) {
        uint8_t ignored = 0;
        success = read_byte(ignored);
      } else if (4 == type || 5 == type || 6 == type) {
        uint64_t ignored = 0;
        success = read_varint(ignored);
      } else if (7 == type) {
        success = skip_bytes(8);
      } else if (8 == type) {
        std::string ignored;
        success = read_binary(ignored);
      } else if (9 == type || 10 == type) {
        uint8_t element_type = 0;
        uint64_t size = 0;
        success = read_collection(element_type, size);
        for (uint64_t i = 0; success && i < size; ++i) {
          success = skip(element_type);
        }
      } else if (11 == type) {
        uint64_t size = 0;
        success = read_varint(size);
        uint8_t key_value_types = 0;
        if (success && size > 0) {
          success = read_byte(key_value_types);
        }
        for (uint64_t i = 0; success && i < size; ++i) {
          success = skip(key_value_types >> 4) && skip(key_value_types & 0x0F);
        }
      } else if (12 == type) {
        int16_t previous_id = 0;
        CompactField field;
        while (success && read_field(previous_id, field) && 0 != field.type_) {
          success = skip(field.type_);
        }
      } else {
        valid_ = false;
        success = false;
      }
      return success;
    }

  private:
    bool read_byte(uint8_t &value)
    {
      if (pos_ >= end_) {
        valid_ = false;
        return false;
      }
      value = static_cast<uint8_t>(data_[pos_++]);
      return true;
    }

    bool read_varint(uint64_t &value)
    {
      value = 0;
      for (int shift = 0; shift < 64; shift += 7) {
        uint8_t byte = 0;
        if (!read_byte(byte)) {
          return false;
        }
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if (0 == (byte & 0x80)) {
          return true;
        }
      }
      valid_ = false;
      return false;
    }

    bool read_zigzag(int64_t &value)
    {
      uint64_t encoded = 0;
      if (!read_varint(encoded)) {
        return false;
      }
      value = static_cast<int64_t>(encoded >> 1) ^ -static_cast<int64_t>(encoded & 1);
      return true;
    }

    bool skip_bytes(const size_t size)
    {
      if (size > end_ - pos_) {
        valid_ = false;
        return false;
      }
      pos_ += size;
      return true;
    }

    const std::string &data_;
    size_t pos_;
    size_t end_;
    bool valid_;
  };

  struct ParquetSchema
  {
    ParquetSchema()
        : type_(-1), type_length_(0), repetition_(0), converted_type_(-1), name_() {}
    int32_t type_;
    int32_t type_length_;
    int32_t repetition_;
    int32_t converted_type_;
    std::string name_;
  };

  struct ParquetChunk
  {
    ParquetChunk()
        : type_(-1), codec_(0), num_values_(0), data_page_offset_(0),
          dictionary_page_offset_(-1), path_() {}
    int32_t type_;
    int32_t codec_;
    int64_t num_values_;
    int64_t data_page_offset_;
    int64_t dictionary_page_offset_;
    std::string path_;
  };

  struct ParquetRowGroup
  {
    ParquetRowGroup() : num_rows_(0), chunks_() {}
    int64_t num_rows_;
    std::vector<ParquetChunk> chunks_;
  };

  static bool is_ident_char(const char c)
  {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
        || ('0' <= c && c <= '9') || '_' == c || '$' == c;
  }

  static char ascii_lower(const char c)
  {
    return ('A' <= c && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
  }

  static std::string lower(std::string value)
  {
    for (char &c : value) {
      c = ascii_lower(c);
    }
    return value;
  }

  static std::string trim(const std::string &value)
  {
    size_t begin = 0;
    size_t end = value.size();
    while (begin < end && isspace(static_cast<unsigned char>(value[begin]))) {
      ++begin;
    }
    while (end > begin && isspace(static_cast<unsigned char>(value[end - 1]))) {
      --end;
    }
    return value.substr(begin, end - begin);
  }

  static bool token_at(const std::string &sql, const size_t pos, const char *token)
  {
    size_t i = 0;
    for (; '\0' != token[i] && pos + i < sql.size(); ++i) {
      if (ascii_lower(sql[pos + i]) != token[i]) {
        return false;
      }
    }
    if ('\0' != token[i]) {
      return false;
    }
    const bool left_ok = 0 == pos || !is_ident_char(sql[pos - 1]);
    const bool right_ok = pos + i >= sql.size() || !is_ident_char(sql[pos + i]);
    return left_ok && right_ok;
  }

  static bool locate_call(const std::string &sql,
                          const size_t pos,
                          const char *name,
                          const Kind kind,
                          Match &match)
  {
    bool found = false;
    if (token_at(sql, pos, name)) {
      size_t after_name = pos + strlen(name);
      while (after_name < sql.size() && isspace(static_cast<unsigned char>(sql[after_name]))) {
        ++after_name;
      }
      if (after_name < sql.size() && '(' == sql[after_name]) {
        size_t close = after_name + 1;
        int depth = 1;
        char quote = '\0';
        for (; close < sql.size() && depth > 0; ++close) {
          const char c = sql[close];
          if ('\0' != quote) {
            if ('\\' == c && close + 1 < sql.size()) {
              ++close;
            } else if (quote == c) {
              if (close + 1 < sql.size() && sql[close + 1] == quote) {
                ++close;
              } else {
                quote = '\0';
              }
            }
          } else if ('\'' == c || '"' == c || '`' == c) {
            quote = c;
          } else if ('(' == c) {
            ++depth;
          } else if (')' == c) {
            --depth;
          }
        }
        if (0 == depth) {
          match.kind_ = kind;
          match.begin_ = pos;
          match.args_begin_ = after_name + 1;
          match.args_end_ = close - 1;
          match.end_ = close;
          found = true;
        }
      }
    }
    return found;
  }

  static bool find_next(const std::string &sql, const size_t start, Match &match)
  {
    char quote = '\0';
    bool line_comment = false;
    bool block_comment = false;
    for (size_t i = start; i < sql.size(); ++i) {
      const char c = sql[i];
      const char next = i + 1 < sql.size() ? sql[i + 1] : '\0';
      if (line_comment) {
        line_comment = '\n' != c;
      } else if (block_comment) {
        if ('*' == c && '/' == next) {
          block_comment = false;
          ++i;
        }
      } else if ('\0' != quote) {
        if ('\\' == c && i + 1 < sql.size()) {
          ++i;
        } else if (quote == c) {
          if (i + 1 < sql.size() && sql[i + 1] == quote) {
            ++i;
          } else {
            quote = '\0';
          }
        }
      } else if ('\'' == c || '"' == c || '`' == c) {
        quote = c;
      } else if ('#' == c || ('-' == c && '-' == next)) {
        line_comment = true;
        if ('-' == c) {
          ++i;
        }
      } else if ('/' == c && '*' == next) {
        block_comment = true;
        ++i;
      } else if (locate_call(sql, i, "read_csv", Kind::CSV, match)
              || locate_call(sql, i, "read_json", Kind::JSONL, match)
              || locate_call(sql, i, "read_parquet", Kind::PARQUET, match)
              || locate_call(sql, i, "glob", Kind::GLOB, match)) {
        return true;
      }
    }
    return false;
  }

  static std::vector<std::string> split_args(const std::string &args)
  {
    std::vector<std::string> result;
    size_t begin = 0;
    int depth = 0;
    char quote = '\0';
    for (size_t i = 0; i < args.size(); ++i) {
      const char c = args[i];
      if ('\0' != quote) {
        if ('\\' == c && i + 1 < args.size()) {
          ++i;
        } else if (quote == c) {
          if (i + 1 < args.size() && args[i + 1] == quote) {
            ++i;
          } else {
            quote = '\0';
          }
        }
      } else if ('\'' == c || '"' == c) {
        quote = c;
      } else if ('(' == c) {
        ++depth;
      } else if (')' == c) {
        --depth;
      } else if (',' == c && 0 == depth) {
        result.push_back(trim(args.substr(begin, i - begin)));
        begin = i + 1;
      }
    }
    result.push_back(trim(args.substr(begin)));
    return result;
  }

  static int rewrite_describe(const std::string &sql,
                              sql::ObSQLSessionInfo &session,
                              common::ObSqlString &output,
                              bool &rewritten)
  {
    int ret = OB_SUCCESS;
    rewritten = false;
    Match match;
    if (find_next(sql, 0, match) && Kind::GLOB != match.kind_) {
      const std::string prefix = lower(trim(sql.substr(0, match.begin_)));
      std::string suffix = trim(sql.substr(match.end_));
      if (!suffix.empty() && ';' == suffix[suffix.size() - 1]) {
        suffix = trim(suffix.substr(0, suffix.size() - 1));
      }
      if ("describe select * from" == prefix && suffix.empty()) {
        std::string schema_sql;
        if (OB_FAIL(build_relation(sql.substr(match.args_begin_, match.args_end_ - match.args_begin_),
                                   match.kind_, session, schema_sql, true))) {
        } else if (OB_FAIL(output.assign(schema_sql.data(), static_cast<int64_t>(schema_sql.size())))) {
        } else {
          rewritten = true;
        }
      }
    }
    return ret;
  }

  static std::string compact_option(const std::string &option)
  {
    std::string result;
    char quote = '\0';
    for (size_t i = 0; i < option.size(); ++i) {
      const char c = option[i];
      if ('\0' != quote) {
        result.push_back(c);
        if ('\\' == c && i + 1 < option.size()) {
          result.push_back(option[++i]);
        } else if (quote == c) {
          quote = '\0';
        }
      } else if ('\'' == c || '"' == c) {
        quote = c;
        result.push_back(c);
      } else if (!isspace(static_cast<unsigned char>(c))) {
        result.push_back(ascii_lower(c));
      }
    }
    return result;
  }

  static bool parse_sql_string(const std::string &literal, std::string &value)
  {
    bool valid = literal.size() >= 2
        && (('\'' == literal.front() && '\'' == literal.back())
            || ('"' == literal.front() && '"' == literal.back()));
    if (valid) {
      const char quote = literal.front();
      value.clear();
      for (size_t i = 1; i + 1 < literal.size(); ++i) {
        const char c = literal[i];
        if ('\\' == c && i + 2 < literal.size()) {
          value.push_back(literal[++i]);
        } else if (quote == c && i + 2 < literal.size() && literal[i + 1] == quote) {
          value.push_back(quote);
          ++i;
        } else {
          value.push_back(c);
        }
      }
    }
    return valid;
  }

  static size_t find_matching_paren(const std::string &sql, const size_t open)
  {
    size_t close = std::string::npos;
    int depth = 0;
    char quote = '\0';
    for (size_t i = open; i < sql.size(); ++i) {
      const char c = sql[i];
      if ('\0' != quote) {
        if ('\\' == c && i + 1 < sql.size()) {
          ++i;
        } else if (quote == c) {
          if (i + 1 < sql.size() && sql[i + 1] == quote) {
            ++i;
          } else {
            quote = '\0';
          }
        }
      } else if ('\'' == c || '"' == c || '`' == c) {
        quote = c;
      } else if ('(' == c) {
        ++depth;
      } else if (')' == c && 0 == --depth) {
        close = i;
        break;
      }
    }
    return close;
  }

  static bool parse_copy_path(const std::string &sql,
                              size_t &pos,
                              std::string &path)
  {
    while (pos < sql.size() && isspace(static_cast<unsigned char>(sql[pos]))) {
      ++pos;
    }
    bool valid = pos < sql.size() && ('\'' == sql[pos] || '"' == sql[pos]);
    if (valid) {
      const char quote = sql[pos];
      const size_t begin = pos++;
      for (; pos < sql.size(); ++pos) {
        if ('\\' == sql[pos] && pos + 1 < sql.size()) {
          ++pos;
        } else if (quote == sql[pos]) {
          if (pos + 1 < sql.size() && sql[pos + 1] == quote) {
            ++pos;
          } else {
            ++pos;
            break;
          }
        }
      }
      valid = pos <= sql.size() && parse_sql_string(sql.substr(begin, pos - begin), path);
    }
    return valid;
  }

  static bool extract_copy_columns(const std::string &query,
                                   std::vector<std::string> &columns)
  {
    size_t pos = 0;
    while (pos < query.size() && isspace(static_cast<unsigned char>(query[pos]))) {
      ++pos;
    }
    bool valid = token_at(query, pos, "select");
    pos += valid ? strlen("select") : 0;
    const size_t list_begin = pos;
    int depth = 0;
    char quote = '\0';
    size_t list_end = std::string::npos;
    for (; valid && pos < query.size(); ++pos) {
      const char c = query[pos];
      if ('\0' != quote) {
        if ('\\' == c && pos + 1 < query.size()) {
          ++pos;
        } else if (quote == c) {
          if (pos + 1 < query.size() && query[pos + 1] == quote) {
            ++pos;
          } else {
            quote = '\0';
          }
        }
      } else if ('\'' == c || '"' == c || '`' == c) {
        quote = c;
      } else if ('(' == c) {
        ++depth;
      } else if (')' == c) {
        --depth;
      } else if (0 == depth && token_at(query, pos, "from")) {
        list_end = pos;
        break;
      }
    }
    valid = valid && std::string::npos != list_end;
    if (valid) {
      const std::vector<std::string> items = split_args(query.substr(list_begin, list_end - list_begin));
      for (const std::string &raw_item : items) {
        std::string item = trim(raw_item);
        std::string name;
        size_t alias_pos = std::string::npos;
        for (size_t i = 0; i + 2 < item.size(); ++i) {
          if (token_at(item, i, "as")) {
            alias_pos = i;
          }
        }
        if (std::string::npos != alias_pos) {
          name = trim(item.substr(alias_pos + 2));
        } else {
          const size_t dot = item.rfind('.');
          name = trim(item.substr(std::string::npos == dot ? 0 : dot + 1));
          const size_t space = name.find_last_of(" \t\r\n");
          if (std::string::npos != space) {
            name = trim(name.substr(space + 1));
          }
        }
        if (name.size() >= 2 && '`' == name.front() && '`' == name.back()) {
          name = name.substr(1, name.size() - 2);
        }
        if (name.empty()) {
          valid = false;
          break;
        }
        columns.push_back(name);
      }
    }
    return valid && !columns.empty();
  }

  static int rewrite_copy(const std::string &input,
                          sql::ObSQLSessionInfo &session,
                          std::string &output,
                          bool &rewritten)
  {
    int ret = OB_SUCCESS;
    rewritten = false;
    const std::string sql = trim(input);
    size_t pos = 0;
    if (!token_at(sql, pos, "copy")) {
    } else if (OB_FAIL(check_enabled(session))) {
    } else {
      pos += strlen("copy");
      while (pos < sql.size() && isspace(static_cast<unsigned char>(sql[pos]))) {
        ++pos;
      }
      if (pos >= sql.size() || '(' != sql[pos]) {
        ret = OB_ERR_PARSE_SQL;
      } else {
        const size_t close = find_matching_paren(sql, pos);
        if (std::string::npos == close) {
          ret = OB_ERR_PARSE_SQL;
        } else {
          const std::string query = trim(sql.substr(pos + 1, close - pos - 1));
          pos = close + 1;
          while (pos < sql.size() && isspace(static_cast<unsigned char>(sql[pos]))) {
            ++pos;
          }
          if (!token_at(sql, pos, "to")) {
            ret = OB_ERR_PARSE_SQL;
          } else {
            pos += strlen("to");
            std::string path;
            if (!parse_copy_path(sql, pos, path)) {
              ret = OB_ERR_PARSE_SQL;
            } else {
              while (pos < sql.size() && isspace(static_cast<unsigned char>(sql[pos]))) {
                ++pos;
              }
              if (pos >= sql.size() || '(' != sql[pos]) {
                ret = OB_ERR_PARSE_SQL;
              } else {
                const size_t options_close = find_matching_paren(sql, pos);
                const std::vector<std::string> options = std::string::npos == options_close
                    ? std::vector<std::string>()
                    : split_args(sql.substr(pos + 1, options_close - pos - 1));
                bool format_csv = false;
                bool header = false;
                std::string null_string("\\N");
                for (const std::string &raw_option : options) {
                  const std::string option = compact_option(raw_option);
                  if ("formatcsv" == option || "format=csv" == option) {
                    format_csv = true;
                  } else if ("header" == option || "header=true" == option) {
                    header = true;
                  } else if (0 == option.find("nullstr")) {
                    const size_t literal_pos = raw_option.find_first_of("'\"");
                    if (std::string::npos == literal_pos
                        || !parse_sql_string(trim(raw_option.substr(literal_pos)), null_string)) {
                      ret = OB_ERR_PARSE_SQL;
                    }
                  } else {
                    ret = OB_ERR_PARSE_SQL;
                  }
                }
                std::vector<std::string> columns;
                if (OB_SUCC(ret) && (!format_csv || !extract_copy_columns(query, columns))) {
                  ret = OB_ERR_PARSE_SQL;
                } else if (OB_SUCC(ret)) {
                  output.assign("SELECT ");
                  for (size_t i = 0; i < columns.size(); ++i) {
                    if (i > 0) output.append(", ");
                    output.append(quote_identifier(columns[i]));
                  }
                  output.append(" FROM (");
                  if (header) {
                    output.append("SELECT 0 AS __file_sql_copy_order");
                    for (const std::string &column : columns) {
                      output.append(", ").append(quote_string(column)).append(" AS ")
                          .append(quote_identifier(column));
                    }
                    output.append(" UNION ALL ");
                  }
                  output.append("SELECT ").append(header ? "1 AS __file_sql_copy_order" : "0 AS __file_sql_copy_order");
                  for (const std::string &column : columns) {
                    output.append(", IFNULL(CAST(").append(quote_identifier(column))
                        .append(" AS CHAR), ").append(quote_string(null_string)).append(") AS ")
                        .append(quote_identifier(column));
                  }
                  output.append(" FROM (").append(query).append(") AS __file_sql_copy_source")
                      .append(") AS __file_sql_copy_rows ORDER BY __file_sql_copy_order")
                      .append(" INTO OUTFILE ").append(quote_string(path))
                      .append(" FORMAT=(TYPE='csv', FIELD_DELIMITER=',', LINE_DELIMITER='\\n', ")
                      .append("FIELD_OPTIONALLY_ENCLOSED_BY='\"') SINGLE=TRUE");
                  rewritten = true;
                }
              }
            }
          }
        }
      }
    }
    return ret;
  }

  static bool has_explicit_alias(const std::string &sql, const size_t end)
  {
    size_t pos = end;
    while (pos < sql.size() && isspace(static_cast<unsigned char>(sql[pos]))) {
      ++pos;
    }
    if (pos >= sql.size() || ',' == sql[pos] || ')' == sql[pos] || ';' == sql[pos]) {
      return false;
    }
    const size_t begin = pos;
    while (pos < sql.size() && is_ident_char(sql[pos])) {
      ++pos;
    }
    const std::string word = lower(sql.substr(begin, pos - begin));
    if (word.empty()) {
      return false;
    }
    if ("as" == word) {
      return true;
    }
    static const char *CLAUSE_WORDS[] = {
      "where", "join", "left", "right", "full", "inner", "outer", "cross", "natural",
      "on", "using", "order", "group", "having", "limit", "union", "except", "intersect",
      "for", "window", "qualify"
    };
    for (const char *clause : CLAUSE_WORDS) {
      if (word == clause) {
        return false;
      }
    }
    return true;
  }

  static int check_enabled(sql::ObSQLSessionInfo &session)
  {
    int ret = OB_SUCCESS;
    bool enabled = false;
    if (OB_FAIL(session.get_enable_file_sql(enabled))) {
    } else if (!enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL is disabled; set enable_file_sql = ON");
    }
    return ret;
  }

  static int canonicalize_path(const std::string &path,
                               sql::ObSQLSessionInfo &session,
                               std::string &canonical)
  {
    int ret = OB_SUCCESS;
    char resolved[PATH_MAX];
    common::ObString secure_file_priv;
    if (OB_ISNULL(realpath(path.c_str(), resolved))) {
      ret = OB_FILE_NOT_EXIST;
    } else if (OB_FAIL(session.get_secure_file_priv(secure_file_priv))) {
    } else {
      canonical.assign(resolved);
      const common::ObString full_path(static_cast<int32_t>(canonical.size()), canonical.data());
      if (!session.is_inner() && OB_FAIL(sql::ObResolverUtils::check_secure_path(secure_file_priv, full_path))) {
      }
    }
    return ret;
  }

  static int read_file(const std::string &path, std::string &content)
  {
    int ret = OB_SUCCESS;
    static const std::streamoff MAX_FILE_SIZE = 64 * 1024 * 1024;
    std::ifstream stream(path.c_str(), std::ios::in | std::ios::binary);
    if (!stream.is_open()) {
      ret = OB_FILE_NOT_EXIST;
    } else {
      stream.seekg(0, std::ios::end);
      const std::streamoff size = stream.tellg();
      if (size < 0 || size > MAX_FILE_SIZE) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        content.resize(static_cast<size_t>(size));
        stream.seekg(0, std::ios::beg);
        if (size > 0) {
          stream.read(&content[0], size);
        }
        if (!stream.good() && !stream.eof()) {
          ret = OB_IO_ERROR;
        }
      }
    }
    return ret;
  }

  static uint32_t read_u32_le(const char *data)
  {
    return static_cast<uint32_t>(static_cast<unsigned char>(data[0]))
        | (static_cast<uint32_t>(static_cast<unsigned char>(data[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(data[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(data[3])) << 24);
  }

  static uint64_t read_u64_le(const char *data)
  {
    return static_cast<uint64_t>(read_u32_le(data))
        | (static_cast<uint64_t>(read_u32_le(data + 4)) << 32);
  }

  static bool compact_integer(CompactReader &reader, int32_t &value)
  {
    int64_t decoded = 0;
    const bool success = reader.read_integer(decoded)
        && decoded >= INT32_MIN && decoded <= INT32_MAX;
    if (success) {
      value = static_cast<int32_t>(decoded);
    }
    return success;
  }

  static bool parse_parquet_schema_element(CompactReader &reader, ParquetSchema &schema)
  {
    int16_t previous_id = 0;
    CompactField field;
    bool success = true;
    while (success && reader.read_field(previous_id, field) && 0 != field.type_) {
      if (1 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, schema.type_);
      } else if (2 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, schema.type_length_);
      } else if (3 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, schema.repetition_);
      } else if (4 == field.id_ && 8 == field.type_) {
        success = reader.read_binary(schema.name_);
      } else if (6 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, schema.converted_type_);
      } else {
        success = reader.skip(field.type_);
      }
    }
    return success && reader.valid() && !schema.name_.empty();
  }

  static bool parse_parquet_column_metadata(CompactReader &reader, ParquetChunk &chunk)
  {
    int16_t previous_id = 0;
    CompactField field;
    bool success = true;
    while (success && reader.read_field(previous_id, field) && 0 != field.type_) {
      if (1 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, chunk.type_);
      } else if (3 == field.id_ && 9 == field.type_) {
        uint8_t element_type = 0;
        uint64_t size = 0;
        success = reader.read_collection(element_type, size) && 8 == element_type;
        for (uint64_t i = 0; success && i < size; ++i) {
          success = reader.read_binary(chunk.path_);
        }
      } else if (4 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, chunk.codec_);
      } else if (5 == field.id_ && 6 == field.type_) {
        success = reader.read_integer(chunk.num_values_);
      } else if (9 == field.id_ && 6 == field.type_) {
        success = reader.read_integer(chunk.data_page_offset_);
      } else if (11 == field.id_ && 6 == field.type_) {
        success = reader.read_integer(chunk.dictionary_page_offset_);
      } else {
        success = reader.skip(field.type_);
      }
    }
    return success && reader.valid() && chunk.type_ >= 0 && chunk.num_values_ >= 0
        && chunk.data_page_offset_ >= 4 && !chunk.path_.empty();
  }

  static bool parse_parquet_column_chunk(CompactReader &reader, ParquetChunk &chunk)
  {
    int16_t previous_id = 0;
    CompactField field;
    bool success = true;
    bool found_metadata = false;
    while (success && reader.read_field(previous_id, field) && 0 != field.type_) {
      if (3 == field.id_ && 12 == field.type_) {
        success = parse_parquet_column_metadata(reader, chunk);
        found_metadata = success;
      } else {
        success = reader.skip(field.type_);
      }
    }
    return success && found_metadata;
  }

  static bool parse_parquet_row_group(CompactReader &reader, ParquetRowGroup &row_group)
  {
    int16_t previous_id = 0;
    CompactField field;
    bool success = true;
    while (success && reader.read_field(previous_id, field) && 0 != field.type_) {
      if (1 == field.id_ && 9 == field.type_) {
        uint8_t element_type = 0;
        uint64_t size = 0;
        success = reader.read_collection(element_type, size) && 12 == element_type && size <= 1024;
        for (uint64_t i = 0; success && i < size; ++i) {
          ParquetChunk chunk;
          success = parse_parquet_column_chunk(reader, chunk);
          if (success) {
            row_group.chunks_.push_back(chunk);
          }
        }
      } else if (3 == field.id_ && 6 == field.type_) {
        success = reader.read_integer(row_group.num_rows_);
      } else {
        success = reader.skip(field.type_);
      }
    }
    return success && reader.valid() && row_group.num_rows_ >= 0
        && row_group.num_rows_ <= 1000000 && !row_group.chunks_.empty();
  }

  static bool parse_parquet_footer(const std::string &content,
                                   const size_t footer_begin,
                                   std::vector<ParquetSchema> &schemas,
                                   std::vector<ParquetRowGroup> &row_groups)
  {
    CompactReader reader(content, footer_begin, content.size() - 8);
    int16_t previous_id = 0;
    CompactField field;
    bool success = true;
    int64_t declared_rows = -1;
    while (success && reader.read_field(previous_id, field) && 0 != field.type_) {
      if (2 == field.id_ && 9 == field.type_) {
        uint8_t element_type = 0;
        uint64_t size = 0;
        success = reader.read_collection(element_type, size) && 12 == element_type
            && size > 1 && size <= 1025;
        for (uint64_t i = 0; success && i < size; ++i) {
          ParquetSchema schema;
          success = parse_parquet_schema_element(reader, schema);
          if (success) {
            schemas.push_back(schema);
          }
        }
      } else if (3 == field.id_ && 6 == field.type_) {
        success = reader.read_integer(declared_rows);
      } else if (4 == field.id_ && 9 == field.type_) {
        uint8_t element_type = 0;
        uint64_t size = 0;
        success = reader.read_collection(element_type, size) && 12 == element_type && size <= 10000;
        for (uint64_t i = 0; success && i < size; ++i) {
          ParquetRowGroup row_group;
          success = parse_parquet_row_group(reader, row_group);
          if (success) {
            row_groups.push_back(row_group);
          }
        }
      } else {
        success = reader.skip(field.type_);
      }
    }
    int64_t actual_rows = 0;
    for (const ParquetRowGroup &row_group : row_groups) {
      actual_rows += row_group.num_rows_;
    }
    return success && reader.valid() && schemas.size() > 1 && !row_groups.empty()
        && declared_rows == actual_rows;
  }

  struct ParquetPage
  {
    ParquetPage()
        : type_(-1), uncompressed_size_(0), compressed_size_(0), num_values_(0),
          encoding_(-1), definition_length_(0), repetition_length_(0),
          values_compressed_(true), header_end_(0) {}
    int32_t type_;
    int32_t uncompressed_size_;
    int32_t compressed_size_;
    int32_t num_values_;
    int32_t encoding_;
    int32_t definition_length_;
    int32_t repetition_length_;
    bool values_compressed_;
    size_t header_end_;
  };

  static bool parse_parquet_data_page_header(CompactReader &reader, ParquetPage &page)
  {
    int16_t previous_id = 0;
    CompactField field;
    bool success = true;
    while (success && reader.read_field(previous_id, field) && 0 != field.type_) {
      if (1 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.num_values_);
      } else if (2 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.encoding_);
      } else {
        success = reader.skip(field.type_);
      }
    }
    return success;
  }

  static bool parse_parquet_data_page_v2_header(CompactReader &reader, ParquetPage &page)
  {
    int16_t previous_id = 0;
    CompactField field;
    bool success = true;
    while (success && reader.read_field(previous_id, field) && 0 != field.type_) {
      if (1 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.num_values_);
      } else if (4 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.encoding_);
      } else if (5 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.definition_length_);
      } else if (6 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.repetition_length_);
      } else if (7 == field.id_ && (1 == field.type_ || 2 == field.type_)) {
        page.values_compressed_ = field.bool_value_;
      } else {
        success = reader.skip(field.type_);
      }
    }
    return success;
  }

  static bool parse_parquet_page(const std::string &content,
                                 const size_t offset,
                                 ParquetPage &page)
  {
    CompactReader reader(content, offset, content.size() - 8);
    int16_t previous_id = 0;
    CompactField field;
    bool success = true;
    while (success && reader.read_field(previous_id, field) && 0 != field.type_) {
      if (1 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.type_);
      } else if (2 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.uncompressed_size_);
      } else if (3 == field.id_ && 5 == field.type_) {
        success = compact_integer(reader, page.compressed_size_);
      } else if (5 == field.id_ && 12 == field.type_) {
        success = parse_parquet_data_page_header(reader, page);
      } else if (8 == field.id_ && 12 == field.type_) {
        success = parse_parquet_data_page_v2_header(reader, page);
      } else {
        success = reader.skip(field.type_);
      }
    }
    page.header_end_ = reader.position();
    return success && reader.valid() && page.type_ >= 0 && page.compressed_size_ >= 0
        && page.uncompressed_size_ >= 0 && page.header_end_ <= content.size()
        && static_cast<size_t>(page.compressed_size_) <= content.size() - page.header_end_;
  }

  static bool read_unsigned_varint(const std::string &data, size_t &pos, uint64_t &value)
  {
    value = 0;
    for (int shift = 0; shift < 64 && pos < data.size(); shift += 7) {
      const uint8_t byte = static_cast<uint8_t>(data[pos++]);
      value |= static_cast<uint64_t>(byte & 0x7F) << shift;
      if (0 == (byte & 0x80)) {
        return true;
      }
    }
    return false;
  }

  static bool snappy_decompress(const std::string &source, std::string &output)
  {
    size_t pos = 0;
    uint64_t expected_size = 0;
    bool success = read_unsigned_varint(source, pos, expected_size)
        && expected_size <= 64 * 1024 * 1024;
    output.clear();
    output.reserve(static_cast<size_t>(expected_size));
    while (success && pos < source.size() && output.size() < expected_size) {
      const uint8_t tag = static_cast<uint8_t>(source[pos++]);
      const uint8_t kind = tag & 0x03;
      size_t length = 0;
      size_t offset = 0;
      if (0 == kind) {
        length = tag >> 2;
        if (length < 60) {
          ++length;
        } else {
          const size_t length_bytes = length - 59;
          uint64_t encoded_length = 0;
          if (length_bytes > 4 || length_bytes > source.size() - pos) {
            success = false;
          }
          for (size_t i = 0; success && i < length_bytes; ++i) {
            encoded_length |= static_cast<uint64_t>(static_cast<uint8_t>(source[pos++])) << (8 * i);
          }
          length = static_cast<size_t>(encoded_length + 1);
        }
        if (success && length <= source.size() - pos && length <= expected_size - output.size()) {
          output.append(source, pos, length);
          pos += length;
        } else {
          success = false;
        }
      } else {
        if (1 == kind) {
          length = 4 + ((tag >> 2) & 0x07);
          if (pos >= source.size()) {
            success = false;
          } else {
            offset = ((static_cast<size_t>(tag) & 0xE0) << 3)
                | static_cast<uint8_t>(source[pos++]);
          }
        } else if (2 == kind) {
          length = 1 + (tag >> 2);
          if (source.size() - pos < 2) {
            success = false;
          } else {
            offset = static_cast<uint8_t>(source[pos])
                | (static_cast<size_t>(static_cast<uint8_t>(source[pos + 1])) << 8);
            pos += 2;
          }
        } else {
          length = 1 + (tag >> 2);
          if (source.size() - pos < 4) {
            success = false;
          } else {
            offset = read_u32_le(source.data() + pos);
            pos += 4;
          }
        }
        if (success && offset > 0 && offset <= output.size()
            && length <= expected_size - output.size()) {
          for (size_t i = 0; i < length; ++i) {
            output.push_back(output[output.size() - offset]);
          }
        } else {
          success = false;
        }
      }
    }
    return success && output.size() == expected_size && pos == source.size();
  }

  static bool decompress_parquet_values(const int32_t codec,
                                        const bool compressed,
                                        const std::string &source,
                                        std::string &output)
  {
    bool success = true;
    if (!compressed || 0 == codec) {
      output = source;
    } else if (1 == codec) {
      success = snappy_decompress(source, output);
    } else {
      success = false;
    }
    return success;
  }

  static bool decode_parquet_levels(const std::string &encoded,
                                    const int bit_width,
                                    const size_t count,
                                    std::vector<int32_t> &levels)
  {
    size_t pos = 0;
    bool success = bit_width >= 0 && bit_width <= 8;
    while (success && pos < encoded.size() && levels.size() < count) {
      uint64_t header = 0;
      success = read_unsigned_varint(encoded, pos, header) && header > 0;
      if (!success) {
        break;
      }
      if (0 == (header & 1)) {
        const size_t run = static_cast<size_t>(header >> 1);
        const size_t width_bytes = static_cast<size_t>((bit_width + 7) / 8);
        uint32_t value = 0;
        if (width_bytes > encoded.size() - pos || run > count - levels.size()) {
          success = false;
        }
        for (size_t i = 0; success && i < width_bytes; ++i) {
          value |= static_cast<uint32_t>(static_cast<uint8_t>(encoded[pos++])) << (8 * i);
        }
        for (size_t i = 0; success && i < run; ++i) {
          levels.push_back(static_cast<int32_t>(value));
        }
      } else {
        const size_t groups = static_cast<size_t>(header >> 1);
        const size_t values = groups * 8;
        const size_t bytes = (values * static_cast<size_t>(bit_width) + 7) / 8;
        if (bytes > encoded.size() - pos) {
          success = false;
        }
        for (size_t i = 0; success && i < values; ++i) {
          uint32_t value = 0;
          for (int bit = 0; bit < bit_width; ++bit) {
            const size_t bit_pos = i * static_cast<size_t>(bit_width) + bit;
            value |= ((static_cast<uint8_t>(encoded[pos + bit_pos / 8]) >> (bit_pos % 8)) & 1) << bit;
          }
          if (levels.size() < count) {
            levels.push_back(static_cast<int32_t>(value));
          }
        }
        pos += bytes;
      }
    }
    return success && levels.size() == count;
  }

  static std::string parquet_number(const double value, const int precision)
  {
    std::ostringstream stream;
    stream << std::setprecision(precision) << value;
    return stream.str();
  }

  static bool decode_parquet_plain(const std::string &data,
                                   const ParquetSchema &schema,
                                   const std::vector<int32_t> &definition_levels,
                                   std::vector<Cell> &cells)
  {
    const int32_t max_definition = 1 == schema.repetition_ ? 1 : 0;
    size_t pos = 0;
    size_t boolean_bit = 0;
    bool success = true;
    for (size_t i = 0; success && i < definition_levels.size(); ++i) {
      if (definition_levels[i] < max_definition) {
        cells.push_back(Cell());
      } else if (0 == schema.type_) {
        if (boolean_bit / 8 >= data.size()) {
          success = false;
        } else {
          const bool value = 0 != ((static_cast<uint8_t>(data[boolean_bit / 8])
              >> (boolean_bit % 8)) & 1);
          cells.push_back(Cell(value ? "true" : "false"));
          ++boolean_bit;
          pos = (boolean_bit + 7) / 8;
        }
      } else if (1 == schema.type_) {
        if (data.size() - pos < 4) {
          success = false;
        } else {
          cells.push_back(Cell(std::to_string(static_cast<int32_t>(read_u32_le(data.data() + pos)))));
          pos += 4;
        }
      } else if (2 == schema.type_) {
        if (data.size() - pos < 8) {
          success = false;
        } else {
          cells.push_back(Cell(std::to_string(static_cast<int64_t>(read_u64_le(data.data() + pos)))));
          pos += 8;
        }
      } else if (4 == schema.type_) {
        if (data.size() - pos < 4) {
          success = false;
        } else {
          const uint32_t bits = read_u32_le(data.data() + pos);
          float value = 0;
          MEMCPY(&value, &bits, sizeof(value));
          cells.push_back(Cell(parquet_number(value, std::numeric_limits<float>::max_digits10)));
          pos += 4;
        }
      } else if (5 == schema.type_) {
        if (data.size() - pos < 8) {
          success = false;
        } else {
          const uint64_t bits = read_u64_le(data.data() + pos);
          double value = 0;
          MEMCPY(&value, &bits, sizeof(value));
          cells.push_back(Cell(parquet_number(value, std::numeric_limits<double>::max_digits10)));
          pos += 8;
        }
      } else if (6 == schema.type_) {
        if (data.size() - pos < 4) {
          success = false;
        } else {
          const uint32_t length = read_u32_le(data.data() + pos);
          pos += 4;
          if (length > data.size() - pos) {
            success = false;
          } else {
            cells.push_back(Cell(data.substr(pos, length)));
            pos += length;
          }
        }
      } else if (7 == schema.type_ && schema.type_length_ > 0) {
        const size_t length = static_cast<size_t>(schema.type_length_);
        if (length > data.size() - pos) {
          success = false;
        } else {
          cells.push_back(Cell(data.substr(pos, length)));
          pos += length;
        }
      } else {
        success = false;
      }
    }
    return success && cells.size() == definition_levels.size();
  }

  static bool decode_parquet_column(const std::string &content,
                                    const ParquetSchema &schema,
                                    const ParquetChunk &chunk,
                                    const int64_t row_count,
                                    std::vector<Cell> &cells)
  {
    size_t offset = static_cast<size_t>(chunk.dictionary_page_offset_ >= 0
        ? chunk.dictionary_page_offset_ : chunk.data_page_offset_);
    bool success = row_count >= 0 && chunk.num_values_ >= row_count;
    while (success && cells.size() < static_cast<size_t>(row_count)) {
      ParquetPage page;
      success = parse_parquet_page(content, offset, page);
      if (!success) {
        break;
      }
      const size_t payload_begin = page.header_end_;
      const size_t payload_end = payload_begin + static_cast<size_t>(page.compressed_size_);
      offset = payload_end;
      if (2 == page.type_) {
        continue;
      }
      if ((0 != page.type_ && 3 != page.type_) || 0 != page.encoding_
          || page.num_values_ < 0 || static_cast<size_t>(page.num_values_) > row_count - cells.size()) {
        success = false;
        break;
      }

      std::string values;
      std::vector<int32_t> levels;
      const int32_t max_definition = 1 == schema.repetition_ ? 1 : 0;
      if (3 == page.type_) {
        const size_t levels_size = static_cast<size_t>(page.repetition_length_)
            + static_cast<size_t>(page.definition_length_);
        if (page.repetition_length_ < 0 || page.definition_length_ < 0
            || levels_size > static_cast<size_t>(page.compressed_size_)) {
          success = false;
          break;
        }
        if (max_definition > 0) {
          const size_t definition_begin = payload_begin + static_cast<size_t>(page.repetition_length_);
          success = decode_parquet_levels(
              content.substr(definition_begin, static_cast<size_t>(page.definition_length_)),
              1, static_cast<size_t>(page.num_values_), levels);
        } else {
          levels.assign(static_cast<size_t>(page.num_values_), 0);
        }
        const std::string compressed_values = content.substr(payload_begin + levels_size,
            static_cast<size_t>(page.compressed_size_) - levels_size);
        success = success && decompress_parquet_values(
            chunk.codec_, page.values_compressed_, compressed_values, values);
      } else {
        std::string payload;
        success = decompress_parquet_values(chunk.codec_, true,
            content.substr(payload_begin, static_cast<size_t>(page.compressed_size_)), payload);
        size_t pos = 0;
        if (success && max_definition > 0) {
          if (payload.size() - pos < 4) {
            success = false;
          } else {
            const uint32_t level_size = read_u32_le(payload.data() + pos);
            pos += 4;
            if (level_size > payload.size() - pos) {
              success = false;
            } else {
              success = decode_parquet_levels(payload.substr(pos, level_size), 1,
                  static_cast<size_t>(page.num_values_), levels);
              pos += level_size;
            }
          }
        } else if (success) {
          levels.assign(static_cast<size_t>(page.num_values_), 0);
        }
        if (success) {
          values.assign(payload, pos, std::string::npos);
        }
      }
      if (success) {
        std::vector<Cell> page_cells;
        success = decode_parquet_plain(values, schema, levels, page_cells);
        cells.insert(cells.end(), page_cells.begin(), page_cells.end());
      }
    }
    return success && cells.size() == static_cast<size_t>(row_count);
  }

  static int parse_parquet(const std::string &content, Relation &relation)
  {
    int ret = OB_SUCCESS;
    if (content.size() < 12 || 0 != content.compare(0, 4, "PAR1")
        || 0 != content.compare(content.size() - 4, 4, "PAR1")) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const uint32_t footer_size = read_u32_le(content.data() + content.size() - 8);
      if (footer_size > content.size() - 12) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const size_t footer_begin = content.size() - 8 - footer_size;
        std::vector<ParquetSchema> schemas;
        std::vector<ParquetRowGroup> row_groups;
        if (!parse_parquet_footer(content, footer_begin, schemas, row_groups)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          std::map<std::string, ParquetSchema> schema_by_name;
          for (size_t i = 1; i < schemas.size(); ++i) {
            schema_by_name[schemas[i].name_] = schemas[i];
            relation.columns_.push_back(schemas[i].name_);
          }
          uniquify_columns(relation.columns_);
          for (const ParquetRowGroup &row_group : row_groups) {
            if (row_group.chunks_.size() != schemas.size() - 1) {
              ret = OB_NOT_SUPPORTED;
              break;
            }
            std::vector<std::vector<Cell> > columns;
            for (const ParquetChunk &chunk : row_group.chunks_) {
              const std::map<std::string, ParquetSchema>::const_iterator schema_it
                  = schema_by_name.find(chunk.path_);
              std::vector<Cell> cells;
              if (schema_it == schema_by_name.end()
                  || schema_it->second.type_ != chunk.type_
                  || !decode_parquet_column(content, schema_it->second, chunk,
                                             row_group.num_rows_, cells)) {
                ret = OB_NOT_SUPPORTED;
                break;
              }
              columns.push_back(cells);
            }
            for (int64_t row = 0; OB_SUCC(ret) && row < row_group.num_rows_; ++row) {
              std::vector<Cell> cells;
              for (size_t column = 0; column < columns.size(); ++column) {
                cells.push_back(columns[column][static_cast<size_t>(row)]);
              }
              relation.rows_.push_back(cells);
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "Parquet reader supports flat PLAIN columns with uncompressed or Snappy pages");
    }
    return ret;
  }

  static void uniquify_columns(std::vector<std::string> &columns)
  {
    std::map<std::string, int64_t> counts;
    for (size_t i = 0; i < columns.size(); ++i) {
      columns[i] = trim(columns[i]);
      if (i == 0 && columns[i].size() >= 3
          && static_cast<unsigned char>(columns[i][0]) == 0xEF
          && static_cast<unsigned char>(columns[i][1]) == 0xBB
          && static_cast<unsigned char>(columns[i][2]) == 0xBF) {
        columns[i].erase(0, 3);
      }
      if (columns[i].empty()) {
        columns[i] = "column" + std::to_string(i + 1);
      }
      const std::string key = lower(columns[i]);
      const int64_t count = ++counts[key];
      if (count > 1) {
        columns[i].append("__");
        columns[i].append(std::to_string(count));
      }
    }
  }

  static int parse_csv(const std::string &content, Relation &relation)
  {
    int ret = OB_SUCCESS;
    std::vector<std::vector<std::string> > records;
    std::vector<std::string> row;
    std::string field;
    bool quoted = false;
    bool field_quoted = false;
    for (size_t i = 0; OB_SUCC(ret) && i < content.size(); ++i) {
      const char c = content[i];
      if (quoted) {
        if ('"' == c) {
          if (i + 1 < content.size() && '"' == content[i + 1]) {
            field.push_back('"');
            ++i;
          } else {
            quoted = false;
          }
        } else {
          field.push_back(c);
        }
      } else if ('"' == c && field.empty()) {
        quoted = true;
        field_quoted = true;
      } else if (',' == c) {
        row.push_back(field);
        field.clear();
        field_quoted = false;
      } else if ('\n' == c || '\r' == c) {
        if ('\r' == c && i + 1 < content.size() && '\n' == content[i + 1]) {
          ++i;
        }
        row.push_back(field);
        field.clear();
        field_quoted = false;
        records.push_back(row);
        row.clear();
      } else {
        field.push_back(c);
      }
    }
    if (quoted) {
      ret = OB_ERR_PARSE_SQL;
    } else if (!field.empty() || !row.empty() || field_quoted) {
      row.push_back(field);
      records.push_back(row);
    }
    if (OB_SUCC(ret) && records.empty()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_SUCC(ret)) {
      relation.columns_ = records.front();
      uniquify_columns(relation.columns_);
      for (size_t i = 1; i < records.size(); ++i) {
        std::vector<Cell> cells;
        cells.reserve(relation.columns_.size());
        for (size_t j = 0; j < relation.columns_.size(); ++j) {
          if (j >= records[i].size() || records[i][j].empty()) {
            cells.push_back(Cell());
          } else {
            cells.push_back(Cell(records[i][j]));
          }
        }
        relation.rows_.push_back(cells);
      }
    }
    return ret;
  }

  static std::string json_value_to_string(const rapidjson::Value &value)
  {
    std::string result;
    if (value.IsString()) {
      result.assign(value.GetString(), value.GetStringLength());
    } else if (value.IsBool()) {
      result = value.GetBool() ? "true" : "false";
    } else if (value.IsInt64()) {
      result = std::to_string(value.GetInt64());
    } else if (value.IsUint64()) {
      result = std::to_string(value.GetUint64());
    } else if (value.IsDouble()) {
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      value.Accept(writer);
      result.assign(buffer.GetString(), buffer.GetSize());
    } else {
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      value.Accept(writer);
      result.assign(buffer.GetString(), buffer.GetSize());
    }
    return result;
  }

  static int parse_jsonl(const std::string &content, Relation &relation)
  {
    int ret = OB_SUCCESS;
    std::vector<std::map<std::string, Cell> > objects;
    size_t begin = 0;
    while (OB_SUCC(ret) && begin < content.size()) {
      size_t end = content.find('\n', begin);
      if (std::string::npos == end) {
        end = content.size();
      }
      const std::string line = trim(content.substr(begin, end - begin));
      begin = end + 1;
      if (line.empty()) {
        continue;
      }
      rapidjson::Document document;
      document.Parse(line.data(), line.size());
      if (document.HasParseError() || !document.IsObject()) {
        ret = OB_ERR_PARSE_SQL;
      } else {
        std::map<std::string, Cell> object;
        for (rapidjson::Value::ConstMemberIterator it = document.MemberBegin();
             it != document.MemberEnd(); ++it) {
          const std::string key(it->name.GetString(), it->name.GetStringLength());
          if (std::find(relation.columns_.begin(), relation.columns_.end(), key)
              == relation.columns_.end()) {
            relation.columns_.push_back(key);
          }
          if (it->value.IsNull()) {
            object[key] = Cell();
          } else {
            object[key] = Cell(json_value_to_string(it->value));
          }
        }
        objects.push_back(object);
      }
    }
    if (OB_SUCC(ret) && relation.columns_.empty()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_SUCC(ret)) {
      uniquify_columns(relation.columns_);
      // Public JSONL keys are unique before normalization; preserve first-seen order.
      for (const std::map<std::string, Cell> &object : objects) {
        std::vector<Cell> row;
        for (const std::string &column : relation.columns_) {
          std::map<std::string, Cell>::const_iterator it = object.find(column);
          row.push_back(it == object.end() ? Cell() : it->second);
        }
        relation.rows_.push_back(row);
      }
    }
    return ret;
  }

  static bool is_integer(const std::string &value)
  {
    if (value.empty()) {
      return false;
    }
    char *end = NULL;
    errno = 0;
    std::strtoll(value.c_str(), &end, 10);
    return 0 == errno && end == value.c_str() + value.size();
  }

  static bool is_double(const std::string &value)
  {
    if (value.empty()) {
      return false;
    }
    char *end = NULL;
    errno = 0;
    std::strtod(value.c_str(), &end);
    return 0 == errno && end == value.c_str() + value.size();
  }

  static bool digits(const std::string &value, const size_t begin, const size_t count)
  {
    bool valid = begin + count <= value.size();
    for (size_t i = 0; valid && i < count; ++i) {
      valid = isdigit(static_cast<unsigned char>(value[begin + i]));
    }
    return valid;
  }

  static bool is_date(const std::string &value)
  {
    return 10 == value.size() && digits(value, 0, 4) && '-' == value[4]
        && digits(value, 5, 2) && '-' == value[7] && digits(value, 8, 2);
  }

  static bool is_datetime(const std::string &value)
  {
    return 19 == value.size() && is_date(value.substr(0, 10))
        && (' ' == value[10] || 'T' == value[10]) && digits(value, 11, 2)
        && ':' == value[13] && digits(value, 14, 2) && ':' == value[16]
        && digits(value, 17, 2);
  }

  static ValueType classify(const Cell &cell)
  {
    ValueType type = ValueType::STRING;
    if (cell.is_null_) {
      type = ValueType::NULL_VALUE;
    } else {
      const std::string normalized = lower(cell.value_);
      if ("true" == normalized || "false" == normalized) {
        type = ValueType::BOOL;
      } else if (is_integer(cell.value_)) {
        type = ValueType::INT;
      } else if (is_double(cell.value_)) {
        type = ValueType::DOUBLE;
      } else if (is_datetime(cell.value_)) {
        type = ValueType::DATETIME;
      } else if (is_date(cell.value_)) {
        type = ValueType::DATE;
      }
    }
    return type;
  }

  static ValueType merge_type(const ValueType left, const ValueType right)
  {
    ValueType result = left;
    if (ValueType::NULL_VALUE == left) {
      result = right;
    } else if (ValueType::NULL_VALUE == right || left == right) {
    } else if ((ValueType::INT == left && ValueType::DOUBLE == right)
            || (ValueType::DOUBLE == left && ValueType::INT == right)) {
      result = ValueType::DOUBLE;
    } else {
      result = ValueType::STRING;
    }
    return result;
  }

  static std::string sql_type(const ValueType type)
  {
    std::string result;
    switch (type) {
      case ValueType::BOOL: result = "SIGNED"; break;
      case ValueType::INT: result = "SIGNED"; break;
      case ValueType::DOUBLE: result = "DOUBLE"; break;
      case ValueType::DATE: result = "DATE"; break;
      case ValueType::DATETIME: result = "DATETIME"; break;
      case ValueType::NULL_VALUE:
      case ValueType::STRING:
      default: result = "CHAR"; break;
    }
    return result;
  }

  static std::string quote_identifier(const std::string &value)
  {
    std::string result("`");
    for (const char c : value) {
      if ('`' == c) {
        result.push_back('`');
      }
      result.push_back(c);
    }
    result.push_back('`');
    return result;
  }

  static std::string quote_string(const std::string &value)
  {
    std::string result("'");
    for (const char c : value) {
      if ('\\' == c || '\'' == c) {
        result.push_back('\\');
      }
      result.push_back(c);
    }
    result.push_back('\'');
    return result;
  }

  static std::string cell_sql(const Cell &cell, const ValueType type)
  {
    std::string value;
    if (cell.is_null_) {
      value = "NULL";
    } else if (ValueType::BOOL == type) {
      value = "true" == lower(cell.value_) ? "1" : "0";
    } else if (ValueType::INT == type || ValueType::DOUBLE == type) {
      value = cell.value_;
    } else {
      std::string normalized = cell.value_;
      if (ValueType::DATETIME == type && normalized.size() > 10 && 'T' == normalized[10]) {
        normalized[10] = ' ';
      }
      value = quote_string(normalized);
    }
    return "CAST(" + value + " AS " + sql_type(type) + ")";
  }

  static std::vector<ValueType> infer_relation_types(const Relation &relation)
  {
    std::vector<ValueType> types(relation.columns_.size(), ValueType::NULL_VALUE);
    for (const std::vector<Cell> &row : relation.rows_) {
      for (size_t i = 0; i < types.size(); ++i) {
        types[i] = merge_type(types[i], classify(row[i]));
      }
    }
    for (ValueType &type : types) {
      if (ValueType::NULL_VALUE == type) {
        type = ValueType::STRING;
      }
    }
    return types;
  }

  static std::string describe_type(const ValueType type)
  {
    std::string result;
    switch (type) {
      case ValueType::BOOL: result = "BOOLEAN"; break;
      case ValueType::INT: result = "BIGINT"; break;
      case ValueType::DOUBLE: result = "DOUBLE"; break;
      case ValueType::DATE: result = "DATE"; break;
      case ValueType::DATETIME: result = "DATETIME"; break;
      case ValueType::NULL_VALUE:
      case ValueType::STRING:
      default: result = "VARCHAR"; break;
    }
    return result;
  }

  static int relation_to_schema_sql(const Relation &relation, std::string &sql)
  {
    int ret = OB_SUCCESS;
    if (relation.columns_.empty()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const std::vector<ValueType> types = infer_relation_types(relation);
      sql.clear();
      for (size_t column = 0; column < relation.columns_.size(); ++column) {
        bool nullable = false;
        for (size_t row = 0; !nullable && row < relation.rows_.size(); ++row) {
          nullable = relation.rows_[row][column].is_null_;
        }
        if (column > 0) {
          sql.append(" UNION ALL ");
        }
        sql.append("SELECT ").append(quote_string(relation.columns_[column]));
        if (0 == column) sql.append(" AS ").append(quote_identifier("column_name"));
        sql.append(", ").append(quote_string(describe_type(types[column])));
        if (0 == column) sql.append(" AS ").append(quote_identifier("column_type"));
        sql.append(", ").append(quote_string(nullable ? "YES" : "NO"));
        if (0 == column) sql.append(" AS ").append(quote_identifier("null"));
        sql.append(", CAST(NULL AS CHAR)");
        if (0 == column) sql.append(" AS ").append(quote_identifier("key"));
        sql.append(", CAST(NULL AS CHAR)");
        if (0 == column) sql.append(" AS ").append(quote_identifier("default"));
        sql.append(", CAST(NULL AS CHAR)");
        if (0 == column) sql.append(" AS ").append(quote_identifier("extra"));
      }
    }
    return ret;
  }

  static int relation_to_sql(const Relation &relation, std::string &sql)
  {
    int ret = OB_SUCCESS;
    if (relation.columns_.empty()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const std::vector<ValueType> types = infer_relation_types(relation);

      sql.assign("(");
      if (relation.rows_.empty()) {
        sql.append("SELECT ");
        for (size_t i = 0; i < relation.columns_.size(); ++i) {
          if (i > 0) sql.append(", ");
          sql.append("CAST(NULL AS ").append(sql_type(types[i])).append(") AS ")
              .append(quote_identifier(relation.columns_[i]));
        }
        sql.append(" WHERE 1 = 0");
      } else {
        for (size_t row_idx = 0; row_idx < relation.rows_.size(); ++row_idx) {
          if (row_idx > 0) sql.append(" UNION ALL ");
          sql.append("SELECT ");
          for (size_t col_idx = 0; col_idx < relation.columns_.size(); ++col_idx) {
            if (col_idx > 0) sql.append(", ");
            sql.append(cell_sql(relation.rows_[row_idx][col_idx], types[col_idx]));
            if (0 == row_idx) {
              sql.append(" AS ").append(quote_identifier(relation.columns_[col_idx]));
            }
          }
        }
      }
      sql.push_back(')');
    }
    return ret;
  }

  static int build_glob(const std::string &pattern,
                        sql::ObSQLSessionInfo &session,
                        std::string &sql)
  {
    int ret = OB_SUCCESS;
    Relation relation;
    relation.columns_.push_back("file");
    glob_t matches;
    MEMSET(&matches, 0, sizeof(matches));
    const int glob_ret = ::glob(pattern.c_str(), GLOB_NOSORT, NULL, &matches);
    if (0 != glob_ret && GLOB_NOMATCH != glob_ret) {
      ret = OB_IO_ERROR;
    }
    for (size_t i = 0; OB_SUCC(ret) && i < matches.gl_pathc; ++i) {
      std::string canonical;
      if (OB_FAIL(canonicalize_path(matches.gl_pathv[i], session, canonical))) {
      } else {
        relation.rows_.push_back(std::vector<Cell>(1, Cell(canonical)));
      }
    }
    globfree(&matches);
    if (OB_SUCC(ret)) {
      ret = relation_to_sql(relation, sql);
    }
    return ret;
  }

  static int build_relation(const std::string &args,
                            const Kind kind,
                            sql::ObSQLSessionInfo &session,
                            std::string &sql,
                            const bool describe = false)
  {
    int ret = OB_SUCCESS;
    const std::vector<std::string> values = split_args(args);
    std::string path;
    if (OB_FAIL(check_enabled(session))) {
    } else if (values.empty() || !parse_sql_string(values[0], path)) {
      ret = OB_ERR_PARSE_SQL;
    } else if (Kind::GLOB == kind) {
      if (values.size() != 1) {
        ret = OB_ERR_PARSE_SQL;
      } else {
        ret = build_glob(path, session, sql);
      }
    } else {
      for (size_t i = 1; OB_SUCC(ret) && i < values.size(); ++i) {
        const std::string option = compact_option(values[i]);
        if (Kind::CSV == kind && "header=true" == option) {
        } else if (Kind::CSV == kind && "sample_size=-1" == option) {
        } else if (Kind::CSV == kind && 0 == option.find("sample_size")) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "CSV sample_size values other than -1");
        } else if (Kind::JSONL == kind
                   && ("format='newline_delimited'" == option
                       || "format=\"newline_delimited\"" == option)) {
        } else {
          ret = OB_ERR_PARSE_SQL;
        }
      }
      std::string canonical;
      std::string content;
      Relation relation;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(canonicalize_path(path, session, canonical))) {
      } else if (OB_FAIL(read_file(canonical, content))) {
      } else if (Kind::CSV == kind && OB_FAIL(parse_csv(content, relation))) {
      } else if (Kind::JSONL == kind && OB_FAIL(parse_jsonl(content, relation))) {
      } else if (Kind::PARQUET == kind && OB_FAIL(parse_parquet(content, relation))) {
      } else if (describe && OB_FAIL(relation_to_schema_sql(relation, sql))) {
      } else if (!describe && OB_FAIL(relation_to_sql(relation, sql))) {
      }
    }
    return ret;
  }
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_MYSQL_OB_FILE_SQL_REWRITER_H_
