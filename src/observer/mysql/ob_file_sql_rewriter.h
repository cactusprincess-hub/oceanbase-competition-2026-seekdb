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
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <glob.h>
#include <map>
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
  enum class Kind { CSV, JSONL, GLOB };
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

  static int relation_to_sql(const Relation &relation, std::string &sql)
  {
    int ret = OB_SUCCESS;
    if (relation.columns_.empty()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
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
                            std::string &sql)
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
      } else if (OB_FAIL(relation_to_sql(relation, sql))) {
      }
    }
    return ret;
  }
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_MYSQL_OB_FILE_SQL_REWRITER_H_
