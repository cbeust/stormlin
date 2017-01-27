package com.beust.stormlin

import java.sql.Connection
import java.sql.SQLException
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaSetter

interface HasTable {
    val tableName: String
}

fun select(vararg fields: String) : SqlBuilder {
    return SqlBuilder(operation = SqlBuilder.Operation.SELECT).apply {
        if (fields.any()) this.fields = fields.toList()
    }
}

object SqlUtils {
    fun toSqlValue(value: Any) = if (value is String) "'$value'" else value.toString()
}

data class SqlBuilder(var operation: Operation = SqlBuilder.Operation.SELECT,
        var table: String? = null,
        var fields : List<String> = listOf("*"),
        val whereClauses: MutableList<Where> = mutableListOf()) {

    enum class Operation {
        SELECT
    }

    fun from(table: String) : SqlBuilder {
        return this.copy(table = table)
    }

    fun toSql() : String {
        val result = StringBuilder().apply {
            when(operation) {
                Operation.SELECT -> append("SELECT " + fields.joinToString(",") + " ")
            }

            table?.let {
                append("FROM $it")
            }

            if (whereClauses.any()) {
                append(" WHERE " + whereClauses.map { it.toString() }.joinToString(", "))
            }
        }

        return result.toString()
    }

    enum class Conditional(val sqlOp: String) {
        EQ("=");

        fun toSql() = sqlOp
    }

    open class Where(open val builder: SqlBuilder, val clause: String) {
        override fun toString() : String {
            return clause
        }
    }

    class WhereClause(override val builder: SqlBuilder, var field: String? = null,
            var conditional: Conditional? = null,
            var arg: String? = null) : Where(builder, field + " " + Conditional.EQ.toSql() + " $arg") {
        fun eq(arg: Any) : SqlBuilder {
            return builder.apply {
                val sqlArg = SqlUtils.toSqlValue(arg)
                whereClauses.add(WhereClause(this, field, Conditional.EQ, sqlArg))
            }
        }
    }

    fun where(s: String): WhereClause {
        return WhereClause(this, field = s)
    }

    fun whereAll(s: String) : SqlBuilder {
        whereClauses.add(Where(this, s))
        return this
    }

//    fun  query(eq: Any): Any {}
}

class Orm(val conn: Connection) {
    inner class Runner<T>(val factory: () -> T) where T: Any{
        var builder: SqlBuilder? = null

        fun query(builder: SqlBuilder) : Runner<T> {
            this.builder = builder
            return this
        }

        fun run() : List<T> {
            if (builder != null) {
                builder?.let { b ->
                    logSql(b.toSql())
                    if (b.table == null) {
                        b.table = tableNameFromEntity(factory())
                    }
                }
                return runQuery(conn, builder!!.toSql(), factory)
            } else {
                throw IllegalArgumentException(if (builder == null) "Null SqlBuilder" else "Null factory")
            }
        }

        fun runUnique() : T? {
            val all = run()
            if (all.size == 1) return all[0]
            else throw IllegalArgumentException("Expected exactly one result but received ${all.size}")
        }
    }


    fun <T : Any> into(factory: () -> T) : Runner<T> {
        return Runner<T>(factory)
    }


//    fun <T> createProxy(itf: KClass<*>): T where T: Any {
//        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/perry", "root", "aaaa")
//        println("Connection established: $conn")
//
//        class DbHandler : InvocationHandler {
//            override fun invoke(proxy: Any, method: Method, args: Array<out Any>?): Any {
//                val queries = method.getAnnotationsByType(Query::class.java)
//                val query = queries[0].value
//                val rt = queries[0].type
//                var query2 = query
//                if (args != null) {
//                    (0..args.size - 1).forEach {
//                        query2 = query2.replace("{$it}", args[it].toString())
//                    }
//                }
//                val actualType =
//                    if (rt == Any::class) {
//                        if (Collection::class.java.isAssignableFrom(method.returnType)) {
//                            throw IllegalArgumentException("Please specify a type= attribute on your @Query annotation")
//
//                        }
//                        method.returnType
//                    } else {
//                        rt.java
//                    }
//                val factory = actualType.getConstructor()
//                val queryResult = runQuery(conn, query2, factory as () -> T)
//                if (Collection::class.java.isAssignableFrom(method.returnType)) {
//                    return queryResult
//                } else {
//                    val c = queryResult as Collection<*>
//                    if (c.size == 1) return c.iterator().next()!!
//                    else throw IllegalArgumentException("The query $query returned more than one result for method" +
//                            " ${method.name}")
//                }
//            }
//        }
//
//        val proxy = Proxy.newProxyInstance(this.javaClass.classLoader, arrayOf(itf.java),
//                DbHandler()) as T
//        return proxy
//    }


    val LOG_LEVEL = 1

    fun log(level: Int, s: String) = if (level <= LOG_LEVEL) println(s) else {}

    fun logSql(sql: String) = log(1, "[SQL] $sql")

    fun  save(entity: Any) : Result {
        var result = Result()
        val columns = arrayListOf<Pair<String, String>>()
        entity::class.memberProperties.forEach { property ->
            val columnAnnotations = property.annotations.filter { it -> it.annotationClass.simpleName!! == "Column" }
            val columnName =
                if (columnAnnotations.size == 1) (columnAnnotations[0] as Column).name
                else property.name
            val value = property.call(entity)
            if (value != null) columns.add(Pair(columnName, SqlUtils.toSqlValue(value)))
        }
        log(2, "Found columns: $columns")
        if (columns.any()) {
            val tableName = tableNameFromEntity(entity)
            val sql = "INSERT INTO $tableName (" + columns.map { it.first }.joinToString(", ") + ")" +
                " VALUES(" + columns.map { it.second }.joinToString(", ") + ")"
            logSql(sql)
            result = runQuery(conn, sql)
        }
        return result
    }

    private fun tableNameFromEntity(instance: Any) : String {
        val entity = instance::class.annotations.find { it.annotationClass.simpleName == "Entity" }
        if (entity !is Entity) {
            throw IllegalArgumentException("Couldn't find @Entity on class ${entity::class}")
        } else {
            return entity.name
        }
    }
}


private fun runQuery(conn: Connection, query: String) : Result {
    var result = Result()
    conn.createStatement().let { statement ->
        try {
            val rowCount = statement.executeUpdate(query)
            result = Result(true, "$rowCount affected rows")
        } catch(ex: SQLException) {
            result = Result(false, throwable = ex)
        }
    }
    return result
}

private fun <T> runQuery(conn: Connection, query: String, factory: () -> T) : List<T> where T: Any {
    fun columnName(property: KProperty1<*, *>) : String {
        val columnName = property.annotations.find { it.annotationClass == Column::class }?.let {
            (it as Column).name
        } ?: property.name
        return columnName
    }

    val result = arrayListOf<T>()
    val instance = factory()
    val kclass = instance::class
    conn.createStatement().let { statement ->
        val rs = statement.executeQuery(query)
        val fieldNames = hashMapOf<String, KMutableProperty1<*, *>>()
        kclass.memberProperties.forEach {
            if (it is KMutableProperty1) {
                fieldNames.put(columnName(it), it)
            } else {
                throw IllegalArgumentException("Property \"${it.name}\" on entity class ${kclass.simpleName} " +
                        "needs to be a var")
            }
        }

        fun toKotlinValue(v: Any) : Any {
            return when (v) {
                is Integer -> v.toInt()
                else -> v
            }
        }

        while (rs.next()) {
            val current = factory()
            (1..rs.metaData.columnCount).forEach { index ->
                val value = rs.getObject(index)
                val columnName = rs.metaData.getColumnName(index)
                val property = fieldNames[columnName]
                if (property != null) {
                    try {
                        property.javaSetter!!.invoke(current, value)
                    } catch(ex: Exception) {
                        warn("Couldn't set $value of type ${value.javaClass} on object property $property " +
                                "for object $current")
                    }
//                        println("Mapping $value to $columnName")
                } else {
                    warn("Ignoring value $value, no mapping found for $columnName in class " + instance::class)
                }
            }
            result.add(current)
        }
    }
    return result
}

fun warn(s: String) = println("[Warning] $s")

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Query(val value: String, val type: KClass<out Any> = Any::class)

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Column(val name: String)

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Entity(val name: String)
