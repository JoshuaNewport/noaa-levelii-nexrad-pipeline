/**
 * DatabaseUtils.cpp - Lightweight SQLite3 wrapper implementation
 */

#include "levelii/DatabaseUtils.h"
#include <iostream>
#include <stdexcept>

namespace levelii {

SQLiteDatabase::SQLiteDatabase(const std::string& db_path) : db_path_(db_path) {
    int rc = sqlite3_open(db_path.c_str(), &db_);
    if (rc != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        throw std::runtime_error("Can't open database: " + err);
    }

    // Enable WAL mode for concurrency
    execute("PRAGMA journal_mode=WAL;");
    execute("PRAGMA synchronous=NORMAL;");

    initialize_schema();
}

SQLiteDatabase::~SQLiteDatabase() {
    if (db_) {
        sqlite3_close(db_);
    }
}

void SQLiteDatabase::initialize_schema() {
    const char* sql = 
        "CREATE TABLE IF NOT EXISTS levelii_frames ("
        "    station TEXT,"
        "    product_code INTEGER,"
        "    product_name TEXT,"
        "    timestamp TEXT,"
        "    filename TEXT,"
        "    PRIMARY KEY (station, product_name, timestamp, filename)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_levelii_station_product ON levelii_frames (station, product_name);"
        "CREATE INDEX IF NOT EXISTS idx_levelii_timestamp ON levelii_frames (timestamp);";

    if (!execute(sql)) {
        std::cerr << "Failed to initialize SQLite schema" << std::endl;
    }
}

bool SQLiteDatabase::execute(const std::string& sql) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    char* err_msg = nullptr;
    int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << err_msg << " (SQL: " << sql << ")" << std::endl;
        sqlite3_free(err_msg);
        return false;
    }
    return true;
}

bool SQLiteDatabase::insert_frame(const std::string& table,
                                const std::string& station,
                                int product_code,
                                const std::string& product_name,
                                const std::string& timestamp,
                                const std::string& filename) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    sqlite3_stmt* stmt;
    std::string sql = "INSERT OR REPLACE INTO " + table + 
                     " (station, product_code, product_name, timestamp, filename) "
                     "VALUES (?, ?, ?, ?, ?);";

    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db_) << std::endl;
        return false;
    }

    sqlite3_bind_text(stmt, 1, station.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 2, product_code);
    sqlite3_bind_text(stmt, 3, product_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, timestamp.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, filename.c_str(), -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        std::cerr << "Execution failed: " << sqlite3_errmsg(db_) << std::endl;
        sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_finalize(stmt);
    return true;
}

nlohmann::json SQLiteDatabase::query(const std::string& sql) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    sqlite3_stmt* stmt;
    nlohmann::json results = nlohmann::json::array();

    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed to prepare query: " << sqlite3_errmsg(db_) << std::endl;
        return results;
    }

    int col_count = sqlite3_column_count(stmt);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        nlohmann::json row;
        for (int i = 0; i < col_count; ++i) {
            const char* col_name = sqlite3_column_name(stmt, i);
            int col_type = sqlite3_column_type(stmt, i);

            if (col_type == SQLITE_INTEGER) {
                row[col_name] = sqlite3_column_int(stmt, i);
            } else if (col_type == SQLITE_FLOAT) {
                row[col_name] = sqlite3_column_double(stmt, i);
            } else if (col_type == SQLITE_TEXT) {
                row[col_name] = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
            } else if (col_type == SQLITE_NULL) {
                row[col_name] = nullptr;
            }
        }
        results.push_back(row);
    }

    sqlite3_finalize(stmt);
    return results;
}

bool SQLiteDatabase::purge_old_records(const std::string& table, const std::string& station, int keep_count) {
    std::string sql = "DELETE FROM " + table + " WHERE rowid NOT IN ("
                      "SELECT rowid FROM " + table + " WHERE station = '" + station + "' "
                      "ORDER BY timestamp DESC LIMIT " + std::to_string(keep_count) + ");";
    return execute(sql);
}

bool SQLiteDatabase::delete_record(const std::string& table, const std::string& station, const std::string& product_name, const std::string& timestamp, const std::string& filename) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    sqlite3_stmt* stmt;
    std::string sql = "DELETE FROM " + table + " WHERE station = ? AND product_name = ? AND timestamp = ? AND filename = ?;";

    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return false;

    sqlite3_bind_text(stmt, 1, station.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, product_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, timestamp.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, filename.c_str(), -1, SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE;
}

} // namespace levelii
