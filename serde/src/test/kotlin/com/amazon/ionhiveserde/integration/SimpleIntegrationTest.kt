package com.amazon.ionhiveserde.integration

import com.klarna.hiverunner.HiveRunnerExtension
import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.annotations.HiveSQL
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.io.File

@ExtendWith(HiveRunnerExtension::class)
class SimpleIntegrationTest {

    @field:HiveSQL(files = ["create_table.hql"], autoStart = false)
    private lateinit var hiveShell: HiveShell

    @BeforeEach
    fun startItUp(): Unit {
        hiveShell.addResource(
            "\${hiveconf:hadoop.tmp.dir}/beers",
            File(javaClass.getResource("/beers.ion")!!.toURI()))
        hiveShell.start()
    }

    @Test
    fun doThisWork() {
        println("Starting at ${System.currentTimeMillis()}")
        val result = hiveShell.executeStatement("SELECT * FROM beers")
        //result.onEach {
            //println("${it[0]}, ${it[1]}, ${it[2]}")
        //}
        println("Finished at ${System.currentTimeMillis()}")
    }
}
