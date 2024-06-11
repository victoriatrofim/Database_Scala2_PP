case class Database(tables: List[Table]) {
  override def toString: String = {
    tables.map(_.name).mkString(", ")
  }

  def create(tableName: String): Database = {
    if (tables.exists(_.name == tableName)) this
    else {
      val newTable = Table(tableName, List.empty)
      Database(tables :+ newTable)
    }
  }

  def drop(tableName: String): Database = {
    Database(tables.filterNot(_.name == tableName))
  }

  def selectTables(tableNames: List[String]): Option[Database] = {
    val selected = tables.filter(table => tableNames.contains(table.name))
    if (selected.length == tableNames.length) Some(Database(selected))
    else None
  }

  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
    val t1 = tables.find(_.name == table1)
    val t2 = tables.find(_.name == table2)

    if (t1.isEmpty) t2
    else if (t2.isEmpty) t1
    else if (t1.isEmpty && t2.isEmpty) None

    else {
      val joinResult = t1.get.join(t2.get, c1, c2)
      Some(joinResult)
    }
  }

   def apply(tableName: String): Option[Table] = {
      tables.find(_.name == tableName)
    }
  }
  extension (database: Database) {
    def apply(i: Int): Table = {
      if (i >= 0 && i < database.tables.length) {
        database.tables(i)
      } else {
        Table("Database", database.tables.flatMap(_.data))
      }
    }
  }