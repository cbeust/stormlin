package com.beust.stormlin

import org.assertj.core.api.Assertions
import org.sqlite.SQLiteException
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import java.io.InputStream
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.*

class DbTest {
    private val urlFile = "jdbc:sqlite:/tmp/stormlinTest.db"
    private val urlMemory = "jdbc:sqlite::memory:"
    private val connection = DriverManager.getConnection(urlMemory)

    @BeforeClass
    fun bc() {
        val res = javaClass.classLoader.getResourceAsStream("perry-small.sql")
        if (res != null) {
            importSql(connection, res)
        } else {
            throw IllegalArgumentException("Couldn't find perry-small.sql")
        }
    }

    private fun importSql(conn: Connection, ins: InputStream) {
        val s = Scanner(ins)
        s.useDelimiter("(;(\r)?\n)|(--(\r)?\n)")
//        s.useDelimiter("\n--\n")
        var st: Statement? = null
        var line: String? = null
        try {
            st = conn.createStatement()
            while (s.hasNext()) {
                line = s.next()
                if (line.startsWith("/*!") && line.endsWith("*/")) {
                    val i = line.indexOf(' ')
                    line = line.substring(i + 1, line.length - " */".length)
                }

                if (!line!!.startsWith("--") && line.trim().length > 0) {
                    st.execute(line);
                }
            }
        } catch(ex: Exception) {
            throw IllegalArgumentException("Couldn't parse line $line", ex)
        } finally {
            if (st != null) st.close();
        }
    }

    @Test
    fun simpleInMemoryTest() {
        listOf(
            "drop table if exists test",
            "create table test(id integer primary key, text varchar(80))",
            "insert into test values(1, \"foo\")",
            "insert into test values(2, \"bar\")"
        ).forEach {
            try {
                connection.createStatement().execute(it)
            } catch(ex: SQLiteException) {
                println("ERROR while running $it")
                ex.printStackTrace()
            }
        }

        fun getId(id: Int) : String {
            val rs = connection.createStatement().executeQuery("select * from test where id = $id")
            if (rs.next()) return rs.getString(2)
            else throw AssertionError("Couldn't find id $id")
        }

        Assertions.assertThat(getId(1)).isEqualTo("foo")
        Assertions.assertThat(getId(2)).isEqualTo("bar")
    }

    @Test
    fun simpleSelect() {
        val storm = Orm(connection)
        val cycles = storm
                .into({ -> Cycle() })
                .query(select().from("cycles").where("number").eq(7))
                .run()
        Assertions.assertThat(cycles.size).isEqualTo(1)
        Assertions.assertThat(cycles[0].englishTitle).isEqualTo("The Cappins")
    }
}
