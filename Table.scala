type Row = Map[String, String]
type Tabular = List[Row]

case class Table(tableName: String, tableData: Tabular) {
  override def toString: String = {
    val header = data.head.keys.mkString(",")
    val info = data.map(row => row.values.mkString(",")).mkString("\n")
    s"$header\n$info"
  }

  def insert(row: Row): Table = {
    val rowExists = data.find(_ == row)
    rowExists match {
      case Some(_) => Table(name, data)
      case None => Table(name, data :+ row)
    }
  }

  def delete(row: Row): Table = Table(name, data.filterNot(_ == row))

  def sort(column: String): Table = {
    val sorted = data.sortBy(_.getOrElse(column, ""))
    Table(name, sorted)
  }

  def name: String = tableName

  def data: Tabular = tableData

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val updatedData = data.map { row =>

      if (f.eval(row).contains(true)) {
        val updatedRow = row.map { case (colName, value) =>
          updates.get(colName) match {

            case Some(newValue) => (colName, newValue)
            case None => (colName, value)
          }
        }
        updatedRow
      } else {
        row
      }
    }
    Table(name, updatedData)
  }

  def filter(f: FilterCond): Table = {
    val filteredData = data.filter(row => f.eval(row).contains(true))
    Table(name, filteredData)
  }

  extension (table: Table) {
    def apply(i: Int): Table = {
      if (i >= 0 && i < data.length) {
        val neededRow = data.zipWithIndex.collect {
          case (row, index) if index == i => row
        }
        Table(table.name, neededRow)
      } else {
        table
      }
    }
  }

  def select(columns: List[String]): Table = {
    val newTable = data.map { row =>
      val selectedRow = columns.flatMap { col =>
        row.get(col).map(value => col -> value)
      }.toMap
      selectedRow
    }
    new Table(name, newTable)
  }

  def header: List[String] = data.headOption.map(tuple => tuple.keys.toList).getOrElse(List.empty)

  def join(other: Table, c1: String, c2: String): Table = {
    // get the final headers
    val hdTable1 = this.header // Get headers from first table
    val hdTable2 = other.header.filter(_ != c2) // Get headers from second table, excluding c2
    val allHeaders = (hdTable1 ++ hdTable2).distinct // Combine headers, remove duplicates

    val joinedData = for {
      row1 <- this.data // For each row in first table
      row2 <- other.data // For each row in second table
      // Filter rows where values in c1 and c2 match
      if row1.getOrElse(c1, "") == row2.getOrElse(c2, "")
    } yield {
      // Merge rows, excluding c2 from row2
      val mergedRow = row1 ++ row2.filterNot { case (k, _) => k == c2 }
      // Combine values from both rows based on common headers
      allHeaders.foldLeft(Map.empty[String, String]) { (acc, hd) =>
        val val1 = row1.getOrElse(hd, "")
        val val2 = row2.getOrElse(hd, "")
        // If both tables have values for the same header
        if (val1.nonEmpty && val2.nonEmpty) {
            // If values are equal, keep one value
            if (val1 == val2) acc + (hd -> val1)
            // If values are different, concatenate values
            else acc + (hd -> (val1 + ";" + val2))
        } else acc + (hd -> (val1 + val2))
      }
    }

    // Filter rows from first table that don't exist in second table
    val rowsOnlyTb1 = this.data.filter(row1 => !other.data.exists(row2 =>
      row1.getOrElse(c1, "") == row2.getOrElse(c2, "")))

    // Add missing keys to rows from first table
    val rowsWithNewKeys = rowsOnlyTb1.map(row => allHeaders.foldLeft(row) { (acc, hd) =>
      if (!acc.contains(hd)) acc + (hd -> "")
      else acc
    })

    // Filter rows from second table that don't exist in first table
    val rowsOnlyTb2 = other.data.filter(row2 => !this.data.exists(row1 =>
      row1.getOrElse(c1, "") == row2.getOrElse(c2, "")))

    // Add rows from second table with empty values for missing keys
    val rowsWithEmptyValues = rowsOnlyTb2.map(row =>
      allHeaders.foldLeft(Map.empty[String, String]) { (acc, hd) =>
        // case hd is c1, combine values from c2 in c1
        if (hd == c1) acc + (hd -> row.getOrElse(c2, ""))
        else acc + (hd -> row.getOrElse(hd, ""))
      }
    )

    // Combine all data into final table
    val finalData = joinedData ++ rowsWithNewKeys ++ rowsWithEmptyValues

    Table(this.name, finalData)
  }
}

object Table {
  def apply(name: String, s: String): Table = {
    val lines = s.split("\n").map(_.split(",").toList)
    val header = lines.head
    val data = lines.tail.map(row => (header zip row).toMap)
    new Table(name, data.toList)
  }
}








