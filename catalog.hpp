#ifndef TINYLAMB_CATALOG_HPP
#define TINYLAMB_CATALOG_HPP

#include <string>

#include "page_manager.hpp"
#include "schema_params.hpp"
#include "page.hpp"
#include "macro.hpp"

namespace tinylamb {

enum class ValueType : uint8_t {
  kUnknownType = 0,
  kInt64Type,
  kVarCharType
};

class Catalog {
  struct Attribute {
    MAPPING_ONLY(Attribute);

    uint16_t name_length;

    uint16_t value_length;

    enum ValueType value_type;

    char* name[0];

    [[nodiscard]] std::string_view GetName() const {
      return std::string_view(reinterpret_cast<const char*>(name),
                              name_length);
    }

    static size_t EstimateSize(const AttributeParameter& param) {
      return sizeof(Attribute) + param.name.size();
    }

    void Initialize(const AttributeParameter& param) {
      name_length = param.name.size();
      switch (param.type) {
        case AttributeParameter::ValueType::UNKNOWN_TYPE:
          value_type = ValueType::kUnknownType;
          throw std::runtime_error("cannot init an attribute with unknown type");
        case AttributeParameter::ValueType::INT64_TYPE:
          value_type = ValueType::kInt64Type;
          value_length = 8;
          break;
        case AttributeParameter::ValueType::VARCHAR_TYPE:
          value_type = ValueType::kVarCharType;
          value_length = param.max_size;
          break;
      }
      std::memcpy(name, param.name.data(), param.name.size());
    }

    [[nodiscard]] uint64_t Hash() const {
      uint64_t result = std::hash<std::string_view>()(GetName());
      for (uint16_t i = 0; i < value_length; ++i) {
        result += std::hash<std::string_view>()(
            std::string_view(reinterpret_cast<const char*>(name),
                             name_length));
        result += std::hash<uint16_t>()(value_length);
        result += std::hash<uint8_t>()(static_cast<uint8_t>(value_type));
      }
      return result;
    }

    static Attribute* AsAttribute(uint8_t* ptr) {
      return reinterpret_cast<Attribute*>(ptr);
    }
    static const Attribute* AsAttribute(const uint8_t* ptr) {
      return reinterpret_cast<const Attribute*>(ptr);
    }
    void DebugDump(std::ostream& o) const {
      o << GetName() << ": ";
      switch (value_type) {
        case ValueType::kUnknownType:
          o << "UNKNOWN";
          break;
        case ValueType::kInt64Type:
          o << "Int64";
          break;
        case ValueType::kVarCharType:
          o << "VarChar(" << value_length << ")";
          break;
      }
    }
  };

  struct SchemaInfo {
    MAPPING_ONLY(SchemaInfo);

    // Physical length of this SchemaInfo.
    uint16_t total_length;

    // Length of this schema name. Actual payload begins from tail of page.
    uint16_t schema_name_length;

    // Count of attributes.
    uint16_t attributes_length;

    // AttributeParameter information of this schema. Actual payload begins from just
    // before of this page's name in reverse order.
    uint16_t attributes[0];

    uint8_t* Head() {
      return reinterpret_cast<uint8_t*>(this);
    }
    [[nodiscard]] const uint8_t* Head() const {
      return reinterpret_cast<const uint8_t*>(this);
    }

    static uint16_t EstimateSize(const SchemaParams& params) {
      uint16_t ret = sizeof(total_length) +
                     sizeof(schema_name_length) +
                     sizeof(attributes_length);
      ret += params.name.size();
      ret += params.attrs.size() * sizeof(Attribute);
      for (const auto& attr : params.attrs) {
        ret += attr.name.size() + sizeof(uint16_t);
      }
      return ret;
    }

    void Initialize(const SchemaParams& params) {
      schema_name_length = params.name.length();
      total_length = EstimateSize(params);
      char* begin_pos = NameBeginOffset();
      std::memcpy(begin_pos, params.name.data(), schema_name_length);
      attributes_length = params.attrs.size();
      uint16_t offset = total_length - schema_name_length;
      for (size_t i = 0; i < params.attrs.size(); ++i) {
        offset -= Attribute::EstimateSize(params.attrs[i]);
        attributes[i] = offset;
        Attribute& new_attribute = GetAttribute(i);
        new_attribute.Initialize(params.attrs[i]);
      }
      assert(offset == sizeof(*this) + sizeof(uint16_t) * params.attrs.size());
    }

    [[nodiscard]] uint64_t Hash() const {
      uint64_t result = 0xbeef + std::hash<std::string_view>()(GetName());
      for (uint16_t i = 0; i < attributes_length; ++i)
        result += GetAttribute(i).Hash();
      return result;
    }

    [[nodiscard]] std::string_view GetName() const {
      return std::string_view(NameBeginOffset(), schema_name_length);
    }

    [[nodiscard]] Attribute& GetAttribute(uint16_t i) {
      return *Attribute::AsAttribute(Head() + attributes[i]);
    }

    [[nodiscard]] uint16_t Attributes() const {
      return attributes_length;
    }

    [[nodiscard]] const Attribute& GetAttribute(uint16_t i) const {
      return *Attribute::AsAttribute(Head() + attributes[i]);
    }

    [[nodiscard]] char* NameBeginOffset() {
      return reinterpret_cast<char*>(this) + total_length - schema_name_length;
    }

    [[nodiscard]] const char* NameBeginOffset() const {
      return reinterpret_cast<const char*>(this) +
             total_length - schema_name_length;
    }
    void DebugDump(std::ostream& o) const {
      o << GetName() << ": "
        << "total_length: " << total_length << ", "
        << "attributes: " << attributes_length << ", [";
      for (uint16_t i = 0; i < attributes_length; ++i) {
        if (0 < i) o << ", ";
        GetAttribute(i).DebugDump(o);
      }
      o << "]\n";
    }
  };

  struct CatalogMetaPage {
    MAPPING_ONLY(CatalogMetaPage);
    // Next page ID of schema info (0 means there is no more schema info page).
    uint64_t next_page_id;

    // Next data inserted to this page will have the tail to this position..
    uint16_t next_end;

    uint16_t schema_count;

    uint64_t data_page;

    uint32_t checksum;

    // Array of start positions of each schemas.
    uint16_t schema[0];

    void Initialize() {
      next_page_id = 0;
      next_end = Page::PayloadSize();
      schema_count = 0;
      data_page = 0;
      UpdateChecksum();
    }

    [[nodiscard]] bool IsValid() const {
      return CalcHash() == checksum;
    }

    [[nodiscard]] uint64_t CalcHash() const {
      uint32_t result = 0xdeadbeef +
                        std::hash<uint64_t>()(next_page_id) +
                        std::hash<uint64_t>()(next_end) +
                        std::hash<uint16_t>()(schema_count) +
                        std::hash<uint64_t>()(data_page);
      for (uint16_t i = 0; i < schema_count; ++i) {
        result += GetSchema(i).Hash();
      }
      return result;
    }

    void UpdateChecksum() {
      uint64_t new_checksum = CalcHash();
      checksum = new_checksum;
    }

    SchemaInfo& MapSchemaInfo(uint16_t start_position) {
      auto* buffer = reinterpret_cast<uint8_t*>(this);
      return *reinterpret_cast<SchemaInfo*>(buffer + start_position);
    }

    bool AddSchema(const SchemaParams& params, uint64_t page_id) {
      const uint16_t schema_size = SchemaInfo::EstimateSize(params);
      if (reinterpret_cast<uint8_t*>(this) + next_end - schema_size <=
          reinterpret_cast<uint8_t*>(schema + schema_count)) {
        // There is no enough space to store the metadata.
        return false;
      }
      next_end -= schema_size;
      schema[schema_count] = next_end;
      schema_count += 1;
      data_page = page_id;
      SchemaInfo& new_schema = MapSchemaInfo(next_end);
      new_schema.Initialize(params);
      UpdateChecksum();
      return true;
    }

    [[nodiscard]] const SchemaInfo& GetSchema(uint16_t id) const {
      assert(id < schema_count);
      const uint8_t* schema_head_pos = OffsetBase() + schema[id];
      return *reinterpret_cast<const SchemaInfo*>(schema_head_pos);
    }

    static CatalogMetaPage* AsCatalogMetaPage(Page* page) {
      return reinterpret_cast<CatalogMetaPage*>(page->payload);
    }

    [[nodiscard]] const uint8_t* OffsetBase() const {
      return reinterpret_cast<const uint8_t*>(this);
    }
  };

 public:
  explicit Catalog(PageManager* pm)
      : pm_(pm) {
    auto* catalog_page = GetCatalogPage();
    if (!catalog_page->IsValid()) {
      catalog_page->Initialize();
    }
    pm_->UnpinCatalogPage();
  }
  void Initialize() {
    auto* catalog_page = GetCatalogPage();
    catalog_page->Initialize();
    pm_->UnpinCatalogPage();
  }

  // Return false if there is the same name table is already exists.
  bool CreateTable(const SchemaParams& params) {
    auto* catalog_page = GetCatalogPage();
    Page* data_page = pm_->AllocateNewPage();
    bool result = catalog_page->AddSchema(params, data_page->header.page_id);
    pm_->Unpin(data_page->header.page_id);
    pm_->UnpinCatalogPage();
    return result;
  }

  const SchemaInfo* GetTable(std::string_view table_name) {
    CatalogMetaPage* meta_page = GetCatalogPage();
    const SchemaInfo* result = nullptr;
    for (size_t i = 0; i < meta_page->schema_count; ++i) {
      const SchemaInfo& schema = meta_page->GetSchema(i);
      if (schema.GetName() == table_name) {
        result = &schema;
      }
    }
    pm_->UnpinCatalogPage();
    return result;
  }

  uint16_t Schemas() {
    const auto* meta_page = GetCatalogPage();
    const uint16_t ret = meta_page->schema_count;
    pm_->UnpinCatalogPage();
    return ret;
  }

  [[maybe_unused]] void DebugDump(std::ostream& o) {
    auto* page = GetCatalogPage();
    o << "MetaPage: {\n  schema_count: " << page->schema_count
      << ", next_end: " << page->next_end
      << ", next_page_id: " << page->next_page_id
      << ", checksum: " << std::hex << "0x" <<page->checksum << std::dec << " [\n";
    for (uint16_t i = 0; i < page->schema_count; ++i) {
      const SchemaInfo& schema = page->GetSchema(i);
      o << "    ";
      schema.DebugDump(o);
    }
    o << "  ]\n}\n";
    pm_->UnpinCatalogPage();
  }

 private:
  CatalogMetaPage* GetCatalogPage() {
    return CatalogMetaPage::AsCatalogMetaPage(pm_->GetCatalogPage());
  }

 private:
  PageManager* pm_;
};

} // namespace tinylamb


#endif  // TINYLAMB_CATALOG_HPP
