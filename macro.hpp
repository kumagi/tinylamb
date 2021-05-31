#ifndef TINYLAMB_MACRO_HPP
#define TINYLAMB_MACRO_HPP

#define MAPPING_ONLY(struct_name) \
struct_name() = delete; \
struct_name(const struct_name&) = delete; \
struct_name(struct_name&&) = delete;      \
struct_name operator=(const struct_name&) = delete; \
struct_name operator=(struct_name&&) = delete;

#endif  // TINYLAMB_MACRO_HPP
