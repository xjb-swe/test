/** Order book plugin for recording BBO and trades to disk */
#include "plugin_bbo_parquet_recorder.h"
#include "arrow/builder.h"
#include <filesystem>
#include <glog/logging.h>

#include <mdh/exchange/book/exchange_book.h>
#include <mdh/exchange/book_builder.h>
#include <mdh/utils/parquet.h>
#include <parquet/properties.h>
#include <parquet/stream_writer.h>

static std::shared_ptr<parquet::schema::GroupNode> get_parquet_schema();

plugin_bbo_parquet_recorder::plugin_bbo_parquet_recorder(
    const nlohmann::json &config, book_builder &bb, order_books &books,
    ticker_directory &tickers, atlas_stock_lookup &atlas_directory)
    : trades_only_(false), total_written_(0), total_msg_(0), book_builder_(bb),
      date_(book_builder_.date()), ts_midnight_(bb.get_time_offset()),
      books_(books), ticker_directory_(tickers),
      atlas_directory_(atlas_directory) {
  bests_.resize(bb.max_books());
  if (config.count("destination")) {
    filename_ = config.at("destination");
    parquet_schema_ = get_parquet_schema();
    parquet_os_ = open_parquet_stream(filename_, parquet_schema_);
  } else {
    throw std::invalid_argument("destination configuration is missing");
  }
  ticker_directory_.add_on_ticker_callback([this](auto p) {
    bests_[p->book_id] =
        std::make_shared<best_t>(p, atlas_directory_.get(p->book_id));
  });

  book_builder_.add_callback(order_book_message_t::ADD,
                             [this](const symbol_order_book& book, const order_book_message_t & m ){ this->callback_execute(book,m); });

  book_builder_.add_callback(order_book_message_t::EXECUTE,
                             [this](const symbol_order_book& book, const order_book_message_t & m ){ this->callback_execute(book,m); });

  book_builder_.add_callback(order_book_message_t::DELETE,
                             [this](const symbol_order_book& book, const order_book_message_t & m ){ this->callback_execute(book,m); });

  book_builder_.add_callback(order_book_message_t::CANCEL,
                             [this](const symbol_order_book& book, const order_book_message_t & m ){ this->callback_execute(book,m); });

  book_builder_.add_callback(order_book_message_t::REPLACE,
                             [this](const symbol_order_book& book, const order_book_message_t & m ){ this->callback_execute(book,m); });

  book_builder_.add_callback(
      book_builder::exchange_status_event_t::EXCHANGE_OPEN,
      [](auto status, auto m) { LOG(INFO) << "exchange open"; });

  if (config.count("trades_only"))
    trades_only_ = std::string(config.at("trades_only")) == "true";
  else
    trades_only_ = false;

  if (config.count("ticker_report"))
    ticker_report_ = std::string(config.at("ticker_report"));

  LOG(INFO) << "Record trades only: " << (trades_only_ ? "true" : "false");
}

/** Get current configuration */
nlohmann::json plugin_bbo_parquet_recorder::get_config() const {
  nlohmann::json j;
  j["destination"] = filename_;
  j["trade_only"] = trades_only_;
  j["ticker_report"] = ticker_report_;
  return j;
}

/** Close log file and write ticker report */
void plugin_bbo_parquet_recorder::close() {
  *parquet_os_ << parquet::EndRowGroup;
  LOG(INFO) << "closing parqet file, " + std::to_string(total_written_) +
                   " daily stats for tickers recorded";
}

/* Log BBO and executed volume */
void plugin_bbo_parquet_recorder::callback_execute( const symbol_order_book& book,
    const order_book_message_t &m) {

  best_t *pbest = bests_[m.book_id].get();

  if ( pbest == nullptr) {
    LOG(ERROR) << "missing book" << m.book_id;
    LOG(FATAL) << "missing book" << m.book_id;
    return;
  }
  ++total_msg_;

  const auto &bids = book.bids_;
  const auto &offers = book.offers_;
  std::optional<sprice_t> bb;
  std::optional<sprice_t> bo;

  if (!bids.empty())
    bb = bids.rbegin()->price_;

  if (!offers.empty())
    bo = -offers.rbegin()->price_;

  /* Save if BBO changes */
  if ((bb != pbest->bid || bo != pbest->offer) && !trades_only_) {
    pbest->bid = bb;
    pbest->offer = bo;
    if (m.type != order_book_message_t::EXECUTE) {
      write_parquet(pbest, {m.time_ns, pbest->bid, pbest->offer, m.volume, abs(m.price),
                            m.book_id, m.type});
    }
  }

  /* And always save on execute */
  if (m.type == order_book_message_t::EXECUTE) {
    write_parquet(pbest, {m.time_ns, pbest->bid, -pbest->offer, m.volume,
                          abs(m.price), m.book_id, m.type});
  }
}

/* Log traded volume TODO ska detta vara här???? ser ut som vi skriver fel
 * bid/offer */
void plugin_bbo_parquet_recorder::callback_trade(const symbol_order_book& book,
    const order_book_message_t &m) {
  //  push_back({m.time_ns, 0, 0, m.volume, m.price, m.book_id, m.type});
}

/** Helper function for extracting time */
static inline time_ns_t make_absolute_time(time_ns_t offset_at_midgnight, uint64_t ns_since_midnight) {
  return (offset_at_midgnight + time_ns_t(ns_since_midnight));
}
void plugin_bbo_parquet_recorder::write_parquet(const best_t *pbest,
                                                const bbo_t &bbo) {
  using namespace parquet;
  ++total_written_;


  *parquet_os_ << date_;
  int64_t event_time_ns = make_absolute_time(ts_midnight_, bbo.event_ts_ns) +
                          1; // since we init this with (now -1)

  *parquet_os_ << std::chrono::milliseconds{event_time_ns / 1000000};
  *parquet_os_ << event_time_ns;
  parquet_os_->WriteTimeMS32Raw(bbo.event_ts_ns / 1000000); // local time in ms since midnight

  *parquet_os_ << pbest->td_->name;
  if (pbest->meta_) {
    *parquet_os_ << pbest->meta_->figi;
    *parquet_os_ << pbest->meta_->isin;
    *parquet_os_ << pbest->meta_->algoseek_secid;
  } else {
    *parquet_os_ << StreamWriter::optional<std::string>();
    *parquet_os_ << StreamWriter::optional<std::string>();
    *parquet_os_ << StreamWriter::optional<int32_t>();
  }
  *parquet_os_ << bbo.best_bid;
  *parquet_os_ << bbo.best_offer;
  *parquet_os_ << (int64_t)bbo.volume;
  *parquet_os_ << bbo.price;
  //*parquet_os_ << bbo.book_id;
  //*parquet_os_ << std::string({(char) bbo.type});
  *parquet_os_ << parquet::EndRow;
}

static std::shared_ptr<parquet::schema::GroupNode> get_parquet_schema() {

  using namespace parquet;
  using namespace parquet::schema;

  NodeVector nv;
  nv.push_back(make_date32_node("ts_date", Repetition::REQUIRED));
  nv.push_back(make_timestamp_ms_node("ts_ms", Repetition::REQUIRED));
  nv.push_back(make_i64_node("ts_ns", Repetition::REQUIRED));
  nv.push_back(make_local_time_ms_node("local_time_ms", Repetition::REQUIRED));

  nv.push_back(make_string_node("ticker", parquet::Repetition::REQUIRED));
  nv.push_back(make_string_node("figi", parquet::Repetition::OPTIONAL));
  nv.push_back(make_string_node("isin", parquet::Repetition::OPTIONAL));
  nv.push_back(make_i32_node("algoseek_secid", parquet::Repetition::OPTIONAL));

  nv.push_back(make_node<decimal94>("best_bid", parquet::Repetition::OPTIONAL));
  nv.push_back(make_node<decimal94>("best_offer", parquet::Repetition::OPTIONAL));
  nv.push_back(make_volume_node("volume", parquet::Repetition::REQUIRED));

  nv.push_back(make_node<decimal94>("price", parquet::Repetition::REQUIRED));

  //nv.push_back(make_count_node("book_id", Repetition::REQUIRED));
  //nv.push_back(make_string_node("type", Repetition::REQUIRED));
  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, nv));
}
