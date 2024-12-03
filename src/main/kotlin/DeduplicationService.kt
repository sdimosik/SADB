package org.shadowliner.project

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import java.security.MessageDigest

class DeduplicationService {

    private var collisionCount = 0 // Счётчик коллизий

    fun restoreFiles(outputFolder: File) {
        if (!outputFolder.exists()) {
            outputFolder.mkdirs() // Создаём папку, если её нет
        }

        transaction {
            val files = FileBlocksTable.slice(FileBlocksTable.fileId, FileBlocksTable.fileExtension)
                .selectAll()
                .withDistinct()
                .map { it[FileBlocksTable.fileId] to it[FileBlocksTable.fileExtension] }

            files.forEach { (fileId, fileExtension) ->
                // Собираем блоки для файла
                val blocks = FileBlocksTable.join(BlocksTable, JoinType.INNER, FileBlocksTable.blockId, BlocksTable.id)
                    .slice(BlocksTable.data, FileBlocksTable.offset)
                    .select { FileBlocksTable.fileId eq fileId }
                    .orderBy(FileBlocksTable.offset)
                    .map { it[BlocksTable.data] }

                // Определяем имя файла с расширением
                val restoredFileName = if (fileExtension.isNullOrEmpty()) {
                    fileId
                } else {
                    "$fileId.$fileExtension"
                }

                // Восстанавливаем файл
                val restoredFile = File(outputFolder, restoredFileName)
                restoredFile.outputStream().buffered().use { output ->
                    blocks.forEach { block ->
                        output.write(block)
                    }
                }

                println("Файл $restoredFileName восстановлен в ${restoredFile.absolutePath}")
            }
        }
    }


    fun processLargeFile(file: File, blockSize: Int, fileId: String) {
        collisionCount = 0
        val fileExtension = file.extension

        val existingHashes = preloadHashes()
        file.inputStream().buffered().use { input ->
            val buffer = ByteArray(blockSize)
            var offset = 0

            transaction {
                while (true) {
                    val bytesRead = input.read(buffer)
                    if (bytesRead == -1) break

                    val chunk = if (bytesRead < blockSize) buffer.copyOf(bytesRead) else buffer
                    val hash = hashBlock(chunk)

                    if (!existingHashes.contains(hash)) {
                        val blockId = saveBlock(hash, chunk)
                        linkBlockToFile(fileId, blockId, offset, fileExtension)
                        existingHashes.add(hash)
                    } else {
                        val blockId = getBlockIdByHash(hash)
                        if (blockId == null || !isBlockDataEqual(blockId, chunk)) {
                            collisionCount++
                            saveBlock(hash, chunk)
                        } else {
                            linkBlockToFile(fileId, blockId, offset, fileExtension)
                        }
                    }

                    offset += bytesRead
                }
            }
        }
    }

    /**
     * Предварительная загрузка всех хэшей для ускорения проверок
     */
    private fun preloadHashes(): MutableSet<String> {
        return transaction {
            BlocksTable.slice(BlocksTable.hash)
                .selectAll()
                .map { it[BlocksTable.hash] }
                .toMutableSet()
        }
    }

    /**
     * Сохранение нового блока в таблицу blocks
     */
    private fun saveBlock(hash: String, block: ByteArray): Int {
        val newBlockId = BlocksTable.insertAndGetId {
            it[BlocksTable.hash] = hash
            it[BlocksTable.data] = block
        }.value

        return newBlockId
    }


    /**
     * Получение ID блока по его хэшу
     */
    private fun getBlockIdByHash(hash: String): Int? {
        return BlocksTable.select { BlocksTable.hash eq hash }
            .singleOrNull()?.get(BlocksTable.id)?.value
    }

    /**
     * Связь блока с файлом
     */
    private fun linkBlockToFile(fileId: String, blockId: Int, offset: Int, fileExtension: String?) {
        FileBlocksTable.insert {
            it[FileBlocksTable.fileId] = fileId
            it[FileBlocksTable.blockId] = blockId
            it[FileBlocksTable.offset] = offset
            it[FileBlocksTable.fileExtension] = fileExtension
        }
    }

    /**
     * Хэширование блока данных
     */
    private fun hashBlock(block: ByteArray): String {
        val digest = MessageDigest.getInstance("SHA-256")
        return digest.digest(block).joinToString("") { "%02x".format(it) }
    }


    fun getCompressionRatio(originalSize: Long): Double {
        val uniqueDataSize = getUniqueDataSize() // Сумма размеров всех уникальных блоков

        println("Исходный размер: $originalSize байт, Размер уникальных данных: $uniqueDataSize байт")

        return if (originalSize > 0) {
            1 - (uniqueDataSize.toDouble() / originalSize.toDouble())
        } else {
            0.0
        }
    }

    fun getUniqueDataSize(): Long {
        return transaction {
            BlocksTable.slice(BlocksTable.data).selectAll().sumOf { it[BlocksTable.data].size.toLong() }
        }
    }

    private fun isBlockDataEqual(blockId: Int, data: ByteArray): Boolean {
        return transaction {
            val existingData = BlocksTable.select { BlocksTable.id eq blockId }
                .singleOrNull()?.get(BlocksTable.data)
            existingData?.contentEquals(data) ?: false
        }
    }

    fun getCollisionCount(): Int {
        return collisionCount // Возвращаем количество коллизий
    }
}
