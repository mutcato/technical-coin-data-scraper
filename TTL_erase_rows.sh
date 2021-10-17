#!/bin/bash

# This file runs periodicaly by cronjob
# Deletes rows from technical_data table which is in coinmove.db Sqlite database
# It deletes according to ttl (time to live) column
# TTL column represents end of lifetime of that row
echo "DELETE FROM technical_data WHERE ttl<" $(date +%s) | sqlite3 /home/pi/Public/coinmove/technical-coin-data-scraper/coinmove.db
echo "SELECT COUNT(*) FROM technical_data;" | sqlite3 /home/pi/Public/coinmove/technical-coin-data-scraper/coinmove.db
