#include "plugin_bbo_parquet_recorder.h"
#include <boost/program_options.hpp>
#include <date/date.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>

#include <mdh/exchange/nasdaq/itch5_book_builder.h>
#include <mdh/exchange/nasdaq/itch5_ticker_directory.h>
#include <mdh/parser/nasdaq/itch5_parser.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <mdh/atlas/atlas.h>
#include <mdh/utils/time_utils.h>

using namespace boost::program_options;
using json = nlohmann::json;
using namespace date;
using namespace std::chrono;
std::string get_env(const char *env, std::string default_value = "") {
  const char *env_p = std::getenv(env);
  if (env_p)
    return std::string(env_p);
  return default_value;
}

int main(int argc, char *argv[]) {

  int rval = 0;
#if 0
  options_description desc("options");
  desc.add_options()("help,h", "print usage message");

  if (get_env("MDH_DATE").empty()) {
    desc.add_options()("date", value<std::string>(), "date YYYY-MM-DD");
  } else {
    desc.add_options()("date",
                       value<std::string>()->default_value(get_env("MDH_DATE")),
                       "date");
  }

  if (get_env("MDH_DST").empty()) {
    desc.add_options()("dst", value<std::string>(), "destination file");
  } else {
    desc.add_options()(
        "dst", value<std::string>()->default_value(get_env("MDH_DST")), "dst");
  }

  desc.add_options()("src", value<std::string>(), "src file");

  variables_map vm;
  boost::program_options::store(
      boost::program_options::parse_command_line(argc, argv, desc), vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  sys_days sample_day;
  if (vm.count("date")) {
    tickup::from_string(vm["date"].as<std::string>(), &sample_day);
  } else {
    std::cout << "--date must be specified" << std::endl;
    return -1;
  }

  LOG(INFO) << "date: " << sample_day;
  LOG(INFO) << "days since epoch " << sample_day.time_since_epoch().count();

  auto atlas_url = get_env("MDH_ATLAS_URL", "10.1.32.3:50051");
  atlas::security_atlas_client atlas_client(
      grpc::CreateChannel(atlas_url, grpc::InsecureChannelCredentials()));
  LOG(INFO) << "-------------- get_tickdata_files --------------";
  auto result2 =
      atlas_client.get_tickdata_files("nasdaq", "itch50", sample_day);

  auto tz = date::locate_zone(result2.timezone);
  LOG(INFO) << "timezone: " << tz->name();

  for (const auto &uri : result2.uris)
    LOG(INFO) << uri;

  // for nasdaq itch this has to be 1
  if (result2.uris.size() != 1) {
    LOG(ERROR) << "did not get one src back for " << sample_day;
    LOG(FATAL) << "did not get one src back for " << sample_day;
  }

  std::string src = result2.uris[0];
  if (vm.count("src")) {
    src = vm["src"].as<std::string>();
    LOG(INFO) << " src file override...";
  }

  // this only applies to filesrc - not s3
  if (!std::filesystem::exists(src)) {
    std::cout << "src file not found " << src << std::endl;
    return -1;
  }

  std::string dst;
  if (vm.count("dst")) {
    dst = vm["dst"].as<std::string>();
  } else {
    std::cout << "--dst must be specified" << std::endl;
    return -1;
  }
#endif
  try {

    sys_days sample_day;
    std::string src = "/home/joakimb/resources/S060618-v50.txt";//S010220-v50.txt";
    std::string dst = "nisse.prq";
    auto tz = date::locate_zone("EST5EDT");

    auto atlas_url = get_env("MDH_ATLAS_URL", "10.1.32.3:50051");
    atlas::security_atlas_client atlas_client(
        grpc::CreateChannel(atlas_url, grpc::InsecureChannelCredentials()));
    LOG(INFO) << "-------------- get_tickdata_files --------------";
    auto result2 =
        atlas_client.get_tickdata_files("nasdaq", "itch50", sample_day);

    LOG(INFO) << "src    : " << src;
    LOG(INFO) << "dst    : " << dst;

    json global_config;
    global_config["in_memory"] = false;
    global_config["protocol"] = "itch";
    std::cerr << global_config.dump(2) << std::endl;

    json plugin_config;
    plugin_config["destination"] = dst;
    std::cerr << plugin_config.dump(2) << std::endl;

    itch5_parser parser(sample_day, tz);
    itch5_ticker_directory ticker_directory(parser);
    indexed_books all_itch_books(itch5_book_builder::MAX_BOOKS);
    itch5_book_builder bb(parser, all_itch_books);
    atlas_stock_lookup atlas_lookup(ticker_directory, atlas_client);

    auto stats1 = std::make_unique<plugin_bbo_parquet_recorder>(
        plugin_config, bb, all_itch_books, ticker_directory, atlas_lookup);
    // Exchange plugins
    LOG(INFO) << "adding daily stat plugin";

    auto reader = std::make_unique<file_reader>();
    if( !reader->init(src, false) )
      LOG(INFO) << "reader->init fail";



    // process parser
    int error_code = 0;
    int nmsg = 0;
    std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
    while (parser.parse_next(*reader, error_code) > 0) {
      if (nmsg++ % 100000000 == 0) {

        size_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - start)
                        .count();
        LOG(INFO) << ns / 1e8 << " ns per message";
        start = std::chrono::steady_clock::now();
      }
    }

    stats1->close();

    reader->close();
    if (error_code) {
      LOG(ERROR) << reader->err_str();
      LOG(FATAL) << reader->err_str();
      return -1;
    }

  } catch (std::exception &e) {
    LOG(ERROR) << "unhandled exception - exiting: " << e.what();
    return -1;
  }
  return rval;
}
