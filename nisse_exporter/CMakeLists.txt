add_executable(bbo_exporter bbo_exporter.cpp plugin_bbo_parquet_recorder.cpp)
target_link_libraries(bbo_exporter ${ALL_LIBS})
INSTALL(TARGETS bbo_exporter RUNTIME DESTINATION bin)
