package software.amazon.ionhiveserde.configuration

import org.junit.Test
import software.amazon.ion.IonCatalog
import software.amazon.ion.SymbolTable
import software.amazon.ion.impl.PrivateUtils
import software.amazon.ion.system.IonReaderBuilder
import software.amazon.ion.system.SimpleCatalog
import java.io.File
import java.io.File.createTempFile
import java.io.FileInputStream
import java.io.FileWriter
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

private const val CATALOG = """
    ${'$'}ion_shared_symbol_table::{
        name: "sst",
        version: 1,
        symbols: [ "foo" ]
    }
"""

private fun writeTempCatalog(): File {
    val catalogFile = createTempFile("catalog", ".ion")!!
    FileWriter(catalogFile).use { it.write(CATALOG) }

    return catalogFile
}

class TestCatalog : IonCatalog {
    private val catalog = SimpleCatalog()

    init {
        val catalogFile = writeTempCatalog()

        IonReaderBuilder.standard().build(FileInputStream(catalogFile)).use { reader ->
            while (reader.next() != null) {
                val symbolTable = PrivateUtils.newSharedSymtab(reader, true)
                catalog.putTable(symbolTable)
            }
        }
    }

    override fun getTable(name: String?, version: Int) = catalog.getTable(name, version)!!
    override fun getTable(name: String?) = catalog.getTable(name)!!
}

class CatalogConfigTest {

    private fun makeConfig(map: Map<String, String>) = CatalogConfig(MapBasedRawConfiguration(map))

    private fun assertCatalog(subject: CatalogConfig) {
        val symbolTable = subject.catalog.getTable("sst")

        assertSst(symbolTable)
    }

    private fun assertSst(symbolTable: SymbolTable) {
        assertNotNull(symbolTable)
        assertEquals("sst", symbolTable.name)
        assertEquals(1, symbolTable.version)
        assertEquals(1, symbolTable.maxId)
        assertEquals("foo", symbolTable.findKnownSymbol(1))
    }

    @Test
    fun catalogFromFile() {
        val catalogFile = writeTempCatalog()

        val subject = makeConfig(mapOf("ion.catalog.file" to catalogFile.absolutePath))

        assertCatalog(subject)
    }

    @Test
    fun catalogFromURL() {
        val catalogFile = writeTempCatalog()

        val subject = makeConfig(mapOf("ion.catalog.url" to catalogFile.toURI().toURL().toString()))

        assertCatalog(subject)
    }

    @Test
    fun catalogFromClass() {
        val subject = makeConfig(mapOf("ion.catalog.class" to TestCatalog::class.java.name))

        assertCatalog(subject)
    }

    @Test
    fun getSymbolTableImports() {
        val catalogFile = writeTempCatalog()
        val subject = makeConfig(mapOf(
                "ion.catalog.file" to catalogFile.absolutePath,
                "ion.symbol_table_imports" to "sst"
        ))

        assertEquals(1, subject.symbolTableImports.size)
        assertSst(subject.symbolTableImports[0])
    }
}