/**
 * DatabaseUtils.h - Lightweight SQLite3 wrapper for radar frame indexing
 */

#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <sqlite3.h>
#include <nlohmann/json.hpp>

namespace levelii {

class SQLiteDatabase {
public:
    explicit SQLiteDatabase(const std::string& db_path);
    ~SQLiteDatabase();

    // Disable copy
    SQLiteDatabase(const SQLiteDatabase&) = delete;
    SQLiteDatabase& operator=(const SQLiteDatabase&) = delete;

    /**
     * @brief Execute a non-query SQL statement (INSERT, UPDATE, DELETE, etc.)
     */
    bool execute(const std::string& sql);

    /**
     * @brief Execute an INSERT/REPLACE with parameters
     */
    bool insert_frame(const std::string& table,
                     const std::string& station,
                     int product_code,
                     const std::string& product_name,
                     const std::string& timestamp,
                     const std::string& filename);

    /**
     * @brief Execute a query and return results as a list of JSON objects
     */
    nlohmann::json query(const std::string& sql);

    /**
     * @brief Purge old records for a station
     */
    bool purge_old_records(const std::string& table, const std::string& station, int keep_count);

    /**
     * @brief Delete a specific record
     */
    bool delete_record(const std::string& table, const std::string& station, const std::string& product_name, const std::string& timestamp, const std::string& filename);

private:
    sqlite3* db_ = nullptr;
    std::mutex db_mutex_;
    std::string db_path_;

    void initialize_schema();
};

} // namespace levelii
