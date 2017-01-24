package com.beust.stormlin

import org.assertj.core.api.Assertions
import org.testng.annotations.DataProvider
import org.testng.annotations.Test
import java.sql.Connection
import java.sql.DriverManager

class SqlBuilderTest {
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/perry", "root", "aaaa")
    val orm = Orm(conn)

    @DataProvider
    fun dpSql(): Array<Array<Any>> {
        return arrayOf(
            arrayOf(SqlBuilder<Any>().select().from("cycles"), "SELECT * FROM cycles"),
            arrayOf(SqlBuilder<Any>().select("a", "b").from("cycles"), "SELECT a,b FROM cycles"),
            arrayOf(SqlBuilder<Any>().select().from("cycles").where("a").eq(2000),
                    "SELECT * FROM cycles WHERE a = 2000"),
            arrayOf(SqlBuilder<Any>().select().from("cycles").where("a").eq("foo"),
                    "SELECT * FROM cycles WHERE a = 'foo'")
        )
    }

    @Test(dataProvider = "dpSql")
    fun sqlShouldBeGeneratedCorrectly(builder: SqlBuilder<*>, expectedSql: String) {
        val sql = builder.toSql()
        Assertions.assertThat(sql).isEqualToIgnoringCase(expectedSql)
    }
}
