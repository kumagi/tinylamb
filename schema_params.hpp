#ifndef TINYLAMB_SCHEMA_PARAMS_HPP
#define TINYLAMB_SCHEMA_PARAMS_HPP

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/string.hpp>

struct AttributeParameter {
  enum class ValueType : uint8_t {
    UNKNOWN_TYPE,
    INT64_TYPE,
    VARCHAR_TYPE
  };

  enum ValueType type;
  std::string name;
  size_t max_size;

  AttributeParameter() : type(ValueType::UNKNOWN_TYPE), name(), max_size(0) {}

  static AttributeParameter IntType(std::string_view name) {
    AttributeParameter result;
    result.type = ValueType::INT64_TYPE;
    result.name = name;
    result.max_size = 8;
    return result;
  }

  static AttributeParameter VarcharType(std::string_view name, size_t max) {
    AttributeParameter result;
    result.type = ValueType::VARCHAR_TYPE;
    result.name = name;
    result.max_size = max;
    return result;
  }

  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive& ar, unsigned int /*version*/)
  {
    ar & boost::serialization::make_nvp("type", type);
    ar & boost::serialization::make_nvp("name", name);
    ar & boost::serialization::make_nvp("size", max_size);
  }
};

struct SchemaParams {
  SchemaParams() = default;
  SchemaParams(std::string_view n, std::initializer_list<AttributeParameter> a)
      : name(n), attrs(a) {}
  std::string name;
  std::vector<AttributeParameter> attrs;
};

#endif  // TINYLAMB_SCHEMA_PARAMS_HPP
