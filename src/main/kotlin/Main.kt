package org.shadowliner.project

import java.io.File

fun main() {
    DatabaseConfig.resetDatabase = true
    DatabaseConfig.connect()

    val deduplicationService = DeduplicationService()
    val folder = File("/Users/sdmmay/sadbData")
    val outputFolder = File("/Users/sdmmay/sadbRestored") // Основная папка для восстановленных файлов
    val resultFile = File("/Users/sdmmay/sadbResults.txt") // Файл для аналитики
    val blockSizes = listOf(4, 8, 16, 32, 64) // Разные размеры блоков

    if (!folder.exists() || !folder.isDirectory) {
        println("Папка ${folder.absolutePath} не существует или не является директорией.")
        return
    }

    clearFolder(outputFolder)

    // Очищаем файл для результатов
    if (resultFile.exists()) {
        resultFile.delete()
    }
    resultFile.createNewFile()

    blockSizes.forEach { blockSize ->
        println("Тестируем размер блока: $blockSize байт")

        DatabaseConfig.resetTables()

        var totalOriginalSize = 0L
        var totalProcessingTime = 0L // Суммарное время обработки всех файлов
        var compressionRatio = 0.0

        val fileProcessingTimes = mutableMapOf<String, Long>() // Хранение времени обработки каждого файла
        val blockResults = StringBuilder()

        folder.listFiles { file -> file.isFile }?.forEach { file ->
            val startTime = System.currentTimeMillis()

            val originalSize = file.length()
            totalOriginalSize += originalSize

            deduplicationService.processLargeFile(file, blockSize, file.nameWithoutExtension)

            val endTime = System.currentTimeMillis()
            val processingTime = endTime - startTime
            totalProcessingTime += processingTime

            fileProcessingTimes[file.name] = processingTime
        }

        compressionRatio = deduplicationService.getCompressionRatio(totalOriginalSize)

        blockResults.appendLine("Результаты для размера блока: $blockSize байт")
        blockResults.appendLine("Исходный размер данных: $totalOriginalSize байт")
        blockResults.appendLine("Размер уникальных данных: ${deduplicationService.getUniqueDataSize()} байт")
        blockResults.appendLine("Степень сжатия: ${"%.2f".format(compressionRatio * 100)}%")
        blockResults.appendLine("Количество коллизий: ${deduplicationService.getCollisionCount()}")
        blockResults.appendLine("Суммарное время обработки: $totalProcessingTime мс")
        blockResults.appendLine()

        fileProcessingTimes.forEach { (fileName, processingTime) ->
            blockResults.appendLine("Файл: $fileName, Время обработки: $processingTime мс")
        }
        blockResults.appendLine()

        // Записываем результаты в файл
        resultFile.appendText(blockResults.toString())

        // Восстанавливаем файлы
        val blockFolder = File(outputFolder, "block_size_$blockSize")
        if (!blockFolder.exists()) {
            blockFolder.mkdirs()
        }
        deduplicationService.restoreFiles(blockFolder)
    }

    println("Аналитика сохранена в файл: ${resultFile.absolutePath}")
}

fun clearFolder(folder: File) {
    if (folder.exists() && folder.isDirectory) {
        folder.listFiles()?.forEach { file ->
            if (file.isDirectory) {
                clearFolder(file) // Рекурсивно удаляем вложенные папки
            }
            file.delete()
        }
    }
}
