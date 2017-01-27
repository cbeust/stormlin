package com.beust.stormlin

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.DriverManager
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.javaSetter
import kotlin.reflect.full.memberProperties

interface HasTable {
    val tableName: String
}

fun select(vararg fields: String) : SqlBuilder {
    return SqlBuilder(operation = SqlBuilder.Operation.SELECT).apply {
        if (fields.any()) this.fields = fields.toList()
    }
}

data class SqlBuilder(var operation: Operation = SqlBuilder.Operation.SELECT,
        var table: String? = null,
        var fields : List<String> = listOf("*"),
        val whereClauses: MutableList<WhereClause> = mutableListOf()) {

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

    class WhereClause(val builder: SqlBuilder, var field: String? = null,
            var conditional: Conditional? = null,
            var arg: String? = null) {
        fun eq(arg: Any) : SqlBuilder {
            return builder.apply {
                val sqlArg = if (arg is String) "'${arg.toString()}'" else arg.toString()
                whereClauses.add(WhereClause(this, field, Conditional.EQ, sqlArg))
            }
        }

        override fun toString() : String {
            return field + " " + Conditional.EQ.toSql() + " $arg"
        }
    }

    fun where(s: String): WhereClause {
        return WhereClause(this, field = s)
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
                println("SQL: \"" + builder!!.toSql() + "\"")
                return runQuery(conn, builder!!.toSql(), factory)
            } else {
                throw IllegalArgumentException(if (builder == null) "Null SqlBuilder" else "Null factory")
            }
        }
    }


    fun <T : Any> into(factory: () -> T) : Runner<T> {
        return Runner<T>(factory)
    }


    fun <T> createProxy(itf: KClass<*>): T where T: Any {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/perry", "root", "aaaa")
        println("Connection established: $conn")

        class DbHandler : InvocationHandler {
            override fun invoke(proxy: Any, method: Method, args: Array<out Any>?): Any {
                val queries = method.getAnnotationsByType(Query::class.java)
                val query = queries[0].value
                val rt = queries[0].type
                var query2 = query
                if (args != null) {
                    (0..args.size - 1).forEach {
                        query2 = query2.replace("{$it}", args[it].toString())
                    }
                }
                val actualType =
                    if (rt == Any::class) {
                        if (Collection::class.java.isAssignableFrom(method.returnType)) {
                            throw IllegalArgumentException("Please specify a type= attribute on your @Query annotation")

                        }
                        method.returnType
                    } else {
                        rt.java
                    }
                val factory = actualType.getConstructor()
                val queryResult = runQuery(conn, query2, factory as () -> T)
                if (Collection::class.java.isAssignableFrom(method.returnType)) {
                    return queryResult
                } else {
                    val c = queryResult as Collection<*>
                    if (c.size == 1) return c.iterator().next()!!
                    else throw IllegalArgumentException("The query $query returned more than one result for method" +
                            " ${method.name}")
                }
            }
        }

        val proxy = Proxy.newProxyInstance(this.javaClass.classLoader, arrayOf(itf.java),
                DbHandler()) as T
        return proxy
    }


    fun log(s: String) = println(s)
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
                warn("Ignoring read-only property $it")
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
