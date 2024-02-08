package ec.edu.utpl.computacion.pfr.pi

import com.github.tototoshi.csv._
import org.nspl._
import org.nspl.awtrenderer._
import org.nspl.data.HistogramData
import java.io.File

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object App {
  @main
  def pintegra(): Unit = {
    // Segunda Clase Zoom
    val pathDataFile2: String = "C://Users//StarMedia//Desktop//dataset//dsAlineacionesXTorneo-2.csv"
    val reader2 = CSVReader.open(new File(pathDataFile2))
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()
    reader2.close()

    val pathDataFile3: String = "C://Users//StarMedia//Desktop//dataset//dsPartidosYGoles.csv"
    val reader3 = CSVReader.open(new File(pathDataFile3)) // Corregir aquí
    val contentFile3: List[Map[String, String]] = reader3.allWithHeaders()
    reader3.close()

    //Llamada a las funciones
    charting(contentFile2)
    camiseta(contentFile2)
    datosGrafica(contentFile2)
    capacidadEstadios(contentFile3)
  }
  def charting(data: List[Map[String, String]]): Unit = {
    val listNroShirt: List[Double] = data
      .filter(row => row.contains("squads_position_name") && row("squads_position_name") == "forward" && row.contains("squads_shirt_number") && row("squads_shirt_number") != "0")
      .map(row => row("squads_shirt_number").toDouble)
    val histForwardShirtNumber = xyplot(HistogramData(listNroShirt, 10) -> bar())(par.xlab("Shirt number").ylab("freq.").main("Forward shirt number"))
    pngToFile(new File("C://Users//StarMedia//Desktop//estadisticaPracticum//grafico.png"), histForwardShirtNumber.build, 1000)
  }

  def camiseta(data: List[Map[String, String]]): Unit = {
    val numcamiseta: List[Double] = data.filter(row => row("squads_position_name") == "midfielder").map(row => row("squads_shirt_number").toDouble)
    val densidad = xyplot(density(numcamiseta.toIndexedSeq) -> line())(
      par.xlab("Shirt Numb").ylab("Frecuenci").main("posicion Shirt Number")
    )
    pngToFile(new File("C://Users//StarMedia//Desktop//estadisticaPracticum//2gráfica.png"), densidad.build, 1000)
  }

  def datosGrafica(data: List[Map[String, String]]): List[(String, Int)] = {
    val dataGoles = data
      .map(row => (
        row("toirnaments_tournament_name"),
        row("matches_match_id"),
        row("matches_home_team_score"),
        row("matches_away_team_score")
      ))
      .distinct.map(t4 => (t4._1, t4._3.toInt + t4._4.toInt))
      .groupBy(_._1).map(t2 => (t2._1, t2._2.map(_._2).sum))
      .toList
      .sortBy(_._1)

    dataGoles.foreach(println)
    dataGoles
  }

  def capacidadEstadios(data: List[Map[String, String]]): Unit = {
    val capaEstadiosData = data
      .filter(row => row.contains("matches_stadium_id") && row.contains("stadiums_stadium_capacity")) // Verificar presencia de claves
      .filterNot(row => row("stadiums_stadium_capacity").equals("NA")) // Filtrar valores no válidos
      .map(row => row("stadiums_stadium_capacity").toDouble) // Convertir a Double

    if (capaEstadiosData.nonEmpty) {
      val histograma = xyplot(HistogramData(capaEstadiosData, 20) -> bar())(
        par
          .xlab("Capacidad")
          .ylab("freq.")
          .main("Capacidad de los estadios")
      )

      pngToFile(new File("C://Users//StarMedia//Desktop//estadisticaPracticum//3gráfica.png"), histograma.build, width = 1000)
    } else {
      println("No hay datos válidos para construir el histograma de capacidad de estadios.")
    }
  }
  
}

