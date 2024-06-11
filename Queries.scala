object Queries {

  def killJackSparrow(t: Table): Option[Table] = queryT(Some(t), "FILTER", Not(Field("name", _ == "Jack")))

  def insertLinesThenSort(db: Database): Option[Table] = {
    val toInsert = List(
      Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
      Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
      Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
      Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")
    )
    queryDB(Some(db), "CREATE", "Inserted Fellas").flatMap(database =>
      queryT(database.tables.find(_.name == "Inserted Fellas"), "INSERT", toInsert).flatMap(table =>
        queryT(Some(table), "SORT", "age")
      )
    )
  }

  def youngAdultHobbiesJ(db: Database): Option[Table] = {
    val myDB = queryDB(Some(db), "JOIN", "People", "name", "Hobbies", "name")
    myDB.flatMap(database => {
      database.tables.find(_.name == "People").flatMap(table => {
        queryT(Some(table), "FILTER", Field("age", _ < "25") && Field("name", _.startsWith("J"))).flatMap(filteredTb => {
          queryT(Some(filteredTb), "EXTRACT", List("name", "hobby"))
        })
      })
    })
  }

}
