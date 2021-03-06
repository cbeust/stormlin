package com.beust.stormlin

import org.assertj.core.api.Assertions.assertThat
import org.sqlite.SQLiteException
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import java.io.File
import java.io.InputStream
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.*

class DbTest {
    private val file = File(System.getProperty("java.io.tmpdir"), "stormlinTest.db")
    private val urlFile = "jdbc:sqlite:" + file.absolutePath
    private val urlMemory = "jdbc:sqlite::memory:"
    private val connection = DriverManager.getConnection(urlFile)

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
        conn.createStatement()
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

    private fun execute(connection: Connection, sql: List<String>) {
        sql.forEach {
            try {
                connection.createStatement().execute(it)
            } catch(ex: SQLiteException) {
                println("ERROR while running $it")
                throw ex
            }
        }
    }

    @Test
    fun simpleInMemoryTest() {
        execute(connection, listOf (
            "drop table if exists test",
            "create table test(id integer primary key, text varchar(80))",
            "insert into test values(1, \"foo\")",
            "insert into test values(2, \"bar\")"))

        fun getId(id: Int) : String {
            val rs = connection.createStatement().executeQuery("select * from test where id = $id")
            if (rs.next()) return rs.getString(2)
            else throw AssertionError("Couldn't find id $id")
        }

        assertThat(getId(1)).isEqualTo("foo")
        assertThat(getId(2)).isEqualTo("bar")
    }

    @Test
    fun simpleSelect() {
        val storm = Orm(connection)
        val cycles = storm
                .into { -> Cycle() }
                .query(select().from("cycles").where("number").eq(7))
                .run()
        assertThat(cycles.size).isEqualTo(1)
        assertThat(cycles[0].englishTitle).isEqualTo("The Cappins")
    }

    @Test
    fun multipleSelect() {
        val storm = Orm(connection)
        val cycles = storm
                .into { -> Cycle() }
                .query(select().from("cycles").whereAll("number >= 2 and number <= 4"))
                .run()
        assertThat(cycles.size).isEqualTo(3)
        assertThat(cycles[0].start).isEqualTo(50)
        assertThat(cycles[1].start).isEqualTo(100)
        assertThat(cycles[2].start).isEqualTo(150)
    }

    private fun <T> insertOne(connection: Connection, test: T, factory: () -> T,
            orm: Orm? = null) : Orm where T: Any {
        execute(connection, listOf (
                "drop table if exists test",
                "create table test(id integer primary key autoincrement, text varchar(80))"))

        (orm ?: Orm(connection)).let { storm ->
            storm.save(test)
            return storm
        }
    }

    @Entity("test")
    data class TestEntity(var id: Int? = null, var text: String? = null)

    @Test
    fun insertWithoutKey() {

        insertOne(connection, TestEntity(null, "Cedric"), { -> TestEntity() }).let { storm ->
            val insertedTest = storm.into { -> TestEntity() }
                    .query(select().where("text").eq("Cedric"))
                    .runUnique()
            assertThat(insertedTest?.text).isEqualTo("Cedric")
            assertThat(insertedTest?.id).isNotNull()
        }
    }

    @Test
    fun insertWithKey() {
        insertOne(connection, TestEntity(42, "Cedric"), { -> TestEntity() }).let { storm ->
            val insertedTest = storm.into { -> TestEntity() }
                    .query(select().where("id").eq(42))
                    .runUnique()
            assertThat(insertedTest?.text).isEqualTo("Cedric")
            assertThat(insertedTest?.id).isEqualTo(42)
        }
    }

    @Test
    fun insertTwice() {
        val storm = Orm(connection)

        insertOne(connection, TestEntity(42, "Cedric"), { -> TestEntity() }, storm)
        insertOne(connection, TestEntity(42, "Cedric"), { -> TestEntity() }, storm).let { storm ->
            val insertedTest = storm.into { -> TestEntity() }
                    .query(select().where("id").eq(42))
                    .runUnique()
            assertThat(insertedTest?.text).isEqualTo("Cedric")
            assertThat(insertedTest?.id).isEqualTo(42)
        }
    }

    @Test
    fun overrideSameObject() {
        @Entity("test")
        data class Test(var id: Int? = null, var text: String? = null)

        val storm = Orm(connection)
        val text = "Cedric"
        fun insertWithId(id: Int) {
            insertOne(connection, Test(id, text), { -> Test() }, storm).let { storm ->
                val insertedTest = storm.into { -> Test() }
                        .query(select().where("text").eq(text))
                        .runUnique()
                assertThat(insertedTest?.text).isEqualTo(text)
                assertThat(insertedTest?.id).isEqualTo(id)
            }
        }
        insertWithId(42)
        insertWithId(43)
    }

}
