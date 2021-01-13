/** Order book plugin for recording BBO and traded volume to disk */
#include <filesystem>
#include <mdh/atlas/atlas.h>
#include <mdh/exchange/book/exchange_book.h>
#include <mdh/exchange/book_builder.h>
#include <mdh/processors/atlas_stock_lookup.h>
#include <mdh/utils/parquet.h>
#include <nlohmann/json.hpp>
#include <parquet/stream_writer.h>

#pragma once

/** Best bid and offer */

/** Record bbo and traded volume to file

    \note In the file, volume is 0 for non execute or trade changes,
          and BBO are both 0 for trade only events
*/
class plugin_bbo_parquet_recorder {
public:
  plugin_bbo_parquet_recorder(const nlohmann::json &config, book_builder &bb,
                              order_books &books, ticker_directory &tickers,
                              atlas_stock_lookup &atlas_directory);

  plugin_bbo_parquet_recorder() = delete;

  nlohmann::json get_config() const;

  void close();

protected:
  void callback_execute(const symbol_order_book& book, const order_book_message_t &m);

  void callback_trade(const symbol_order_book& book, const order_book_message_t &m);

  /** Data structure for recording BBO events */
  struct bbo_t {
    time_ns_t event_ts_ns;
    std::optional<sprice_t> best_bid;   /**< Best bid */
    std::optional<sprice_t> best_offer; /**< Best offer */
    volume_t volume;     /**< Volume in message */
    sprice_t price;      /**< Price in message */
    book_id_t book_id;   /**< Numerical exchange book ID */
    order_type_t type;   /**< Message type triggering event */
  };

  /** BBO for specific ticker */
  struct best_t {
    best_t(const ticker_descriptor *td, const atlas::ticker_metadata *meta)
        : td_(td), meta_(meta) {}
    best_t() = delete;
    const ticker_descriptor *td_;
    const atlas::ticker_metadata *meta_;
    std::optional<sprice_t> bid;
    std::optional<sprice_t> offer;
  };

  void write_parquet(const best_t *pbest, const bbo_t &bbo);

  /** Record only trades and executions */
  bool trades_only_ = false;

  size_t total_written_ = 0; /**< Number of lines written to parquet file */
  size_t total_msg_ = 0;     /**< Number of messages recieved */

  book_builder &book_builder_;
  const date::sys_days date_;
  time_ns_t ts_midnight_;

  order_books &books_;
  std::vector<std::shared_ptr<best_t>> bests_;

  ticker_directory &ticker_directory_;
  atlas_stock_lookup &atlas_directory_;

  /** Path to ticker report file */
  std::string ticker_report_;

  /** Path to parquet file */
  std::string filename_;

  /** Parquet output */
  std::shared_ptr<parquet::schema::GroupNode> parquet_schema_;
  std::shared_ptr<parquet::StreamWriterEx> parquet_os_;
};
