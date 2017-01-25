package com.beust.stormlin

import java.sql.Date
import java.sql.DriverManager

fun main(argv: Array<String>) {
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/perry", "root", "aaaa")
    val orm = Orm(conn)

    val cycles = orm
        .into(::Cycle)
        .query(select("a", "b").from("cycles").where("number").eq("ced"))
        .run()
}

fun queryStrings(orm: Orm) {
    val perry = orm.createProxy<Perry>(Perry::class)

    val cycles = perry.getCycles()
    println("Found ${cycles.size} cycles: " + cycles[0])

    val book = perry.getBook(2000)
    println("Found book: $book")

    val books = perry.getBooks(650, 652)
    println("Found books:\n")
    books.forEach(::println)
}

interface Perry {
    @Query("select * from cycles", type = Cycle::class)
    fun getCycles() : List<Cycle>

    @Query("select * from hefte where number = {0}")
    fun getBook(book: Int) : Book?

    @Query("select * from hefte where {0} <= number && number <= {1}" , type = Book::class)
    fun getBooks(start: Int, end: Int) : List<Book>
}

data class Book(
        var number: Int? = null,
        var title: String? = null,
        var author: String? = null,
        @Column(name = "published")
        var publishedDate: Date? = null,
        @Column(name = "german_file")
        var germanFile: String? = null)

data class Cycle(
    var number: Int? = null,
    var start: Int? = null,
    var end: Int? = null,

    @Column(name = "german_title")
    var germanTitle: String? = null,

    @Column(name = "english_title")
    var englishTitle: String? = null,

    @Column(name = "short_title")
    var shortTitle: String? = null
)

