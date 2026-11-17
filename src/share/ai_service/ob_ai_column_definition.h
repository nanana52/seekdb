// Copyright (c) 2026 OceanBase competition contributors.
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace oceanbase { namespace share { namespace ai_column {

struct SqlToken {
  enum Kind { IDENTIFIER, STRING, NUMBER, SYMBOL } kind;
  std::string value;
  size_t first, last;
  bool quoted;
  bool is(const char *name) const {
    if (quoted || kind != IDENTIFIER) { return false; }
    std::string upper(value);
    for (char &c : upper) { c = static_cast<char>(std::toupper(static_cast<unsigned char>(c))); }
    return upper == name;
  }
};

inline std::string lower_identifier(std::string text) {
  for (char &c : text) { c = static_cast<char>(std::tolower(static_cast<unsigned char>(c))); }
  return text;
}
inline std::string quote_identifier(const std::string &text) {
  std::string out("`");
  for (char c : text) { out += c == '`' ? "``" : std::string(1, c); }
  return out + "`";
}
inline std::string quote_literal(const std::string &text) {
  std::string out("'");
  for (char c : text) {
    if (c == '\\') { out += "\\\\"; }
    else if (c == '\'') { out += "''"; }
    else if (c == '\0') { out += "\\0"; }
    else { out += c; }
  }
  return out + "'";
}

inline std::vector<SqlToken> tokenize_definition(const std::string &sql) {
  std::vector<SqlToken> result;
  for (size_t i = 0; i < sql.size();) {
    unsigned char c = static_cast<unsigned char>(sql[i]);
    if (std::isspace(c)) { ++i;continue; }
    if (sql[i] == '#' || (i + 2 < sql.size() && sql[i] == '-' && sql[i+1] == '-' &&
                         std::isspace(static_cast<unsigned char>(sql[i+2])))) {
      while (i < sql.size() && sql[i] != '\n') { ++i; }
      continue;
    }
    if (i + 1 < sql.size() && sql[i] == '/' && sql[i+1] == '*') {
      const size_t end = sql.find("*/", i+2);
      if (end == std::string::npos) { throw std::invalid_argument("Unterminated SQL comment"); }
      if (i + 2 < sql.size() && sql[i+2] == '!') {
        throw std::invalid_argument("Executable comments are unsupported in AI declarations");
      }
      i = end + 2;continue;
    }
    const size_t start = i;
    if (c == '`' || c == '\'' || c == '"') {
      const char delimiter = sql[i++];std::string value;bool closed = false;
      while (i < sql.size()) {
        const char current = sql[i++];
        if (current == delimiter) {
          if (i < sql.size() && sql[i] == delimiter) { value += delimiter;++i; }
          else { closed = true;break; }
        } else if (current == '\\' && delimiter != '`') {
          if (i == sql.size()) { break; }
          const char escaped = sql[i++];
          value += escaped == 'n' ? '\n' : escaped == 'r' ? '\r' : escaped == 't' ? '\t' : escaped;
        } else { value += current; }
      }
      if (!closed) { throw std::invalid_argument("Unterminated SQL quoted value"); }
      result.push_back({delimiter == '`' ? SqlToken::IDENTIFIER : SqlToken::STRING,value,start,i,true});
    } else if (std::isalpha(c) || c == '_' || c == '$' || c >= 128) {
      ++i;
      while (i < sql.size()) {
        c = static_cast<unsigned char>(sql[i]);
        if (!std::isalnum(c) && c != '_' && c != '$' && c < 128) { break; }
        ++i;
      }
      result.push_back({SqlToken::IDENTIFIER,sql.substr(start,i-start),start,i,false});
    } else if (std::isdigit(c)) {
      ++i;while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) { ++i; }
      result.push_back({SqlToken::NUMBER,sql.substr(start,i-start),start,i,false});
    } else {
      result.push_back({SqlToken::SYMBOL,std::string(1,sql[i++]),start,i,false});
    }
  }
  return result;
}

struct VectorDimensionMismatch final : std::length_error {
  uint32_t expected, actual;
  VectorDimensionMismatch(uint32_t wanted,uint32_t got)
      : std::length_error("AI embedding dimension differs from result column"),expected(wanted),actual(got) {}
};

struct NativeColumnDefinition {
  std::string name, type_sql;
  bool ai = false;
  bool embedding = false;
  std::string model_name, expression_sql, input_expression;
  std::vector<std::string> dependencies;
  int64_t dimension = 0;
  int64_t max_retries = 2;
  std::string typed_instruction;
};

inline size_t matching_parenthesis(const std::vector<SqlToken> &tokens, size_t open, size_t end) {
  if (open >= end || tokens[open].value != "(") { throw std::invalid_argument("Expected opening parenthesis"); }
  int depth = 0;
  for (size_t at = open; at < end; ++at) {
    if (tokens[at].kind != SqlToken::SYMBOL) { continue; }
    if (tokens[at].value == "(") { ++depth; }
    else if (tokens[at].value == ")" && --depth == 0) { return at; }
  }
  throw std::invalid_argument("Unterminated SQL expression");
}
inline std::vector<std::pair<size_t,size_t>> split_arguments(
    const std::vector<SqlToken> &tokens, size_t begin, size_t end) {
  std::vector<std::pair<size_t,size_t>> parts;size_t start = begin;int depth = 0;
  for (size_t at = begin; at < end; ++at) {
    if (tokens[at].kind != SqlToken::SYMBOL) { continue; }
    if (tokens[at].value == "(") { ++depth; }
    else if (tokens[at].value == ")") { --depth; }
    else if (tokens[at].value == "," && depth == 0) { parts.push_back({start,at});start=at+1; }
    if (depth < 0) { throw std::invalid_argument("Invalid SQL nesting"); }
  }
  if (start < end) { parts.push_back({start,end}); }
  return parts;
}

inline NativeColumnDefinition parse_native_column(
    const std::string &sql, const std::vector<SqlToken> &tokens, size_t begin, size_t end) {
  if (begin + 1 >= end || tokens[begin].kind != SqlToken::IDENTIFIER) {
    throw std::invalid_argument("Invalid AI column definition");
  }
  NativeColumnDefinition column;column.name=tokens[begin].value;
  size_t marker=end;int depth=0;
  for (size_t i=begin+1;i+1<end;++i) {
    if (tokens[i].kind==SqlToken::SYMBOL) {
      if (tokens[i].value=="(") { ++depth; }
      else if (tokens[i].value==")") { --depth; }
    }
    if (depth==0 && tokens[i].is("AI") && tokens[i+1].is("COLUMN")) { marker=i;break; }
  }
  if (marker==end) { return column; }
  column.ai=true;
  column.type_sql=sql.substr(tokens[begin+1].first,tokens[marker].first-tokens[begin+1].first);
  const auto &type=tokens[begin+1];
  if (marker+3>=end || tokens[marker+2].value!="(") { throw std::invalid_argument("Expected AI generation expression"); }
  const size_t close=matching_parenthesis(tokens,marker+2,end);
  const size_t function=marker+3;
  if (function+2>=close || (!tokens[function].is("AI_COMPLETE") && !tokens[function].is("AI_EMBED")) ||
      tokens[function+1].value!="(" || matching_parenthesis(tokens,function+1,close)!=close-1) {
    throw std::invalid_argument("AI COLUMN requires exactly one supported AI function");
  }
  column.embedding=tokens[function].is("AI_EMBED");
  const auto args=split_arguments(tokens,function+2,close-1);
  if (args.size()<2 || args.size()>(column.embedding?3U:2U) || args[0].second-args[0].first!=1 ||
      tokens[args[0].first].kind!=SqlToken::STRING) { throw std::invalid_argument("Invalid AI function arguments"); }
  column.model_name=tokens[args[0].first].value;
  if (column.model_name.empty()) { throw std::invalid_argument("Empty AI model name"); }
  column.expression_sql=sql.substr(tokens[function].first,tokens[close-1].last-tokens[function].first);
  column.input_expression=sql.substr(tokens[args[1].first].first,
      tokens[args[1].second-1].last-tokens[args[1].first].first);
  if (column.embedding) {
    if (!type.is("VECTOR") || begin+4>=marker || tokens[begin+2].value!="(" ||
        tokens[begin+3].kind!=SqlToken::NUMBER || tokens[begin+4].value!=")") {
      throw std::invalid_argument("AI_EMBED requires a VECTOR result column");
    }
    column.dimension=std::stoll(tokens[begin+3].value);
    if (column.dimension<=0 || column.dimension>65535) { throw std::invalid_argument("Invalid vector dimension"); }
    if (args.size()==3 && (args[2].second-args[2].first!=1 || tokens[args[2].first].kind!=SqlToken::NUMBER ||
        std::stoll(tokens[args[2].first].value)!=column.dimension)) {
      throw VectorDimensionMismatch(column.dimension,tokens[args[2].first].kind==SqlToken::NUMBER?std::stoll(tokens[args[2].first].value):0);
    }
  } else if (type.is("JSON")) { column.typed_instruction="Return exactly one valid JSON value"; }
  else if (type.is("BIGINT")||type.is("INT")||type.is("INTEGER")||type.is("SMALLINT")||type.is("TINYINT")) {
    column.typed_instruction="Return exactly one valid signed integer";
  } else if (type.is("DOUBLE")||type.is("FLOAT")||type.is("DECIMAL")) {
    column.typed_instruction="Return exactly one valid finite number";
  } else if (!type.is("VARCHAR") && !type.is("TEXT") && !type.is("LONGTEXT") && !type.is("CHAR")) {
    throw std::invalid_argument("Unsupported AI result type");
  }
  for (size_t i=args[1].first;i<args[1].second;++i) {
    if (tokens[i].is("AI_COMPLETE")||tokens[i].is("AI_EMBED")||tokens[i].is("AI_PROMPT")||
        tokens[i].is("SELECT")||tokens[i].is("OVER")||
        (tokens[i].kind==SqlToken::SYMBOL && tokens[i].value==";")) {
      throw std::invalid_argument("Nested AI or subquery generation expressions are unsupported");
    }
  }
  size_t option=close+1;
  if (option<end && tokens[option].is("WITH")) {
    const size_t last=matching_parenthesis(tokens,option+1,end);
    if (last+1!=end) { throw std::invalid_argument("Unexpected AI column suffix"); }
    std::set<std::string> seen;
    for (const auto &part:split_arguments(tokens,option+2,last)) {
      const size_t key=part.first;
      if (part.second-key!=3 || tokens[key+1].value!="=") { throw std::invalid_argument("Invalid AI column option"); }
      const std::string name=lower_identifier(tokens[key].value);
      if (!seen.insert(name).second) { throw std::invalid_argument("Duplicate AI column option"); }
      if (name=="sync_mode") {
        if (tokens[key+2].kind!=SqlToken::STRING || lower_identifier(tokens[key+2].value)!="async") {
          throw std::invalid_argument("Only asynchronous AI columns are supported");
        }
      } else if (name=="max_retries") {
        if (tokens[key+2].kind!=SqlToken::NUMBER) { throw std::invalid_argument("Invalid retry limit"); }
        column.max_retries=std::stoll(tokens[key+2].value);
        if (column.max_retries<0 || column.max_retries>100) { throw std::invalid_argument("Invalid retry limit"); }
      } else { throw std::invalid_argument("Unknown AI column option"); }
    }
  } else if (option!=end) { throw std::invalid_argument("Unexpected AI column suffix"); }
  return column;
}

inline void bind_native_dependencies(std::vector<NativeColumnDefinition> &columns) {
  for (auto &column:columns) {
    if (!column.ai) { continue; }
    const auto tokens=tokenize_definition(column.input_expression);
    std::set<std::string> seen;
    for (size_t i=0;i<tokens.size();++i) {
      if (tokens[i].kind!=SqlToken::IDENTIFIER || (i+1<tokens.size() && tokens[i+1].value=="(")) { continue; }
      const auto name=lower_identifier(tokens[i].value);
      for (const auto &candidate:columns) {
        if (lower_identifier(candidate.name)!=name) { continue; }
        if (candidate.ai) { throw std::invalid_argument("AI columns cannot depend on AI columns"); }
        if (seen.insert(name).second) { column.dependencies.push_back(candidate.name); }
      }
    }
  }
}

}}} // namespace oceanbase::share::ai_column
