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
import kotlin.reflect.memberProperties

class Orm(val conn: Connection) {
    private fun <T> runQuery(conn: Connection, query: String, cls: Class<T>, kclass: KClass<*>) : List<T> {
        fun columnName(property: KProperty1<*, *>) : String {
            val columnName = property.annotations.find { it.annotationClass == Column::class }?.let {
                (it as Column).name
            } ?: property.name
            return columnName
        }

        val result = arrayListOf<T>()

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
                val current = cls.newInstance() as T
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
                        warn("Ignoring value $value, no mapping found for $columnName in class " + cls.name)
                    }
                }
                result.add(current)
            }
        }
        return result
    }

    fun <T> createProxy(itf: KClass<*>): T {
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
                val queryResult = runQuery(conn, query2, actualType, actualType.kotlin)
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

    fun warn(s: String) = println("[Warning] $s")

    fun log(s: String) = println(s)
}

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Query(val value: String, val type: KClass<out Any> = Any::class)

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Column(val name: String)
