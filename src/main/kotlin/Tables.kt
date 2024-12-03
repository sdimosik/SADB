package org.shadowliner.project

import org.jetbrains.exposed.dao.id.IntIdTable

object BlocksTable : IntIdTable("blocks") {
    val hash = varchar("hash", 256).uniqueIndex()
    val data = binary("data")
}

object FileBlocksTable : IntIdTable("file_blocks") {
    val fileId = varchar("file_id", 100)
    val blockId = reference("block_id", BlocksTable)
    val offset = integer("block_offset")
    val fileExtension = varchar("file_extension", 10).nullable()
}
