package com.beust.stormlin

import org.assertj.core.api.Assertions
import org.testng.annotations.Test
import java.sql.DriverManager

class DbTest {
    val urlMemory = "jdbc:sqlite::memory:"
    val connection = DriverManager.getConnection(urlMemory)

    @Test
    fun simpleInMemoryTest() {
        listOf(
            "create table test(id integer primary key, text varchar(80))",
            "insert into test values(1, \"foo\")",
            "insert into test values(2, \"bar\")"
        ).forEach {
            val rs = connection.createStatement().execute(it)
        }

        fun getId(id: Int) : String {
            val rs = connection.createStatement().executeQuery("select * from test where id = $id")
            if (rs.next()) return rs.getString(2)
            else throw AssertionError("Couldn't find id $id")
        }

        Assertions.assertThat(getId(1)).isEqualTo("foo")
        Assertions.assertThat(getId(2)).isEqualTo("bar")
    }
}
