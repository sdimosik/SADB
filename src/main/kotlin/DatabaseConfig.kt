package org.shadowliner.project

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction

object DatabaseConfig {

    var resetDatabase = false

    fun connect() {
        Database.connect(
            url = "jdbc:postgresql://localhost:5432/deduplication",
            driver = "org.postgresql.Driver",
            user = "dedup_user",
            password = "password"
        )

        if (resetDatabase) {
            resetTables()
        }
    }

    fun resetTables() {
        transaction {
            println("Сбрасываем таблицы...")

            // Удаляем таблицы, если они существуют
            SchemaUtils.drop(FileBlocksTable, BlocksTable)

            // Пересоздаём таблицы
            println("Пересоздаём таблицы...")
            SchemaUtils.create(BlocksTable, FileBlocksTable)

            println("Таблицы успешно пересозданы.")
        }
    }
}
