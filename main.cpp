#include "database/database.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    LOG(FATAL) << "Set DB file";
    return 1;
  }
  tinylamb::Database db(argv[1]);

  return 0;
}
