package com.beust.stormlin

import java.sql.DriverManager

class DbTest {
    val urlMemory = "jdbc:sqlite::memory:"
    val connection = DriverManager.getConnection(urlMemory)

    fun run() {
        listOf(
            "create table test(id integer primary key, text varchar(80))",
            "insert into test values(1, \"foo\")",
            "insert into test values(2, \"bar\")"
        ).forEach {
            val rs = connection.createStatement().execute(it)
        }
        val rs = connection.createStatement().executeQuery("select * from test where id = 2")
        while (rs.next()) {
            val result = rs.getString(2)
            println("Read: $result")
        }

    }

}

fun main(argv: Array<String>) {
    DbTest().run()
    println("")
}
