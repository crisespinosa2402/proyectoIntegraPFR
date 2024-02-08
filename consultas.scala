package ec.edu.utpl.computacion.pfr.pi

import doobie._
import doobie.implicits._
import cats.effect.IO
import org.nspl._
import org.nspl.data.HistogramData
import org.nspl.awtrenderer._
import cats.effect.unsafe.implicits.global

case class Partido(matchId: String, matchDate: String, matchTime: String)
case class Estadio(stadiumName: String, cityName: String)
case class Jugador(givenName: String, familyName: String, positionName: String)

object consultas {

  def partidosConTiempoExtraYPenales: ConnectionIO[List[Partido]] =
    sql"""
         SELECT match_id, match_date, match_time
         FROM MATCHES
         WHERE extra_time = TRUE AND penalty_shootout = TRUE
       """.query[Partido].to[List]

  def estadiosDeTorneoEspecifico(tournamentId: String): ConnectionIO[List[Estadio]] =
    sql"""
         SELECT STADIUM.name, STADIUM.city_name
         FROM MATCHES
         JOIN STADIUM ON MATCHES.stadium_id = STADIUM.stadium_id
         WHERE MATCHES.tournament_id = $tournamentId
       """.query[Estadio].to[List]

  def jugadoresDeEquipoEspecifico(teamId: String): ConnectionIO[List[Jugador]] =
    sql"""
         SELECT given_name, family_name, position_name
         FROM PLAYER
         WHERE team_id = $teamId
       """.query[Jugador].to[List]

}

object Graficas {

  def crearGraficaPartidos(partidos: List[Partido]): Unit = {
    val datosGrafica = xyplot(HistogramData(partidos.map(_.matchId.hashCode.toDouble), 10) -> bar())(
      par.xlab("Match ID").ylab("Frecuencia").main("Partidos con tiempo extra y penales")
    )
    pngToFile(new java.io.File("C://Users//StarMedia//Desktop//estadisticaConexion//grafica1.png"), datosGrafica.build, 1000)
  }

  def crearGraficaEstadios(estadios: List[Estadio]): Unit = {
    val datosGrafica = xyplot(HistogramData(estadios.map(_.stadiumName.hashCode.toDouble), 10) -> bar())(
      par.xlab("Stadium Name").ylab("Frecuencia").main("Estadios de un torneo específico")
    )
    pngToFile(new java.io.File("C://Users//StarMedia//Desktop//estadisticaConexion//grafica2.png"), datosGrafica.build, 1000)
  }

  def crearGraficaJugadores(jugadores: List[Jugador]): Unit = {
    val datosGrafica = xyplot(HistogramData(jugadores.map(_.givenName.hashCode.toDouble), 10) -> bar())(
      par.xlab("Given Name").ylab("Frecuencia").main("Jugadores de un equipo específico")
    )
    pngToFile(new java.io.File("C://Users//StarMedia//Desktop//estadisticaConexion//grafica3.png"), datosGrafica.build, 1000)
  }

}

object Main extends App {

  val xa = Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver",
    url = "jdbc:mysql://localhost:3306/mundiales",
    user = "root",
    password = "UTPL2023",
    logHandler = None
  )

  val partidos = consultas.partidosConTiempoExtraYPenales.transact(xa).unsafeRunSync()
  Graficas.crearGraficaPartidos(partidos)

  val estadios = consultas.estadiosDeTorneoEspecifico("WC-2014").transact(xa).unsafeRunSync()
  Graficas.crearGraficaEstadios(estadios)

  val jugadores = consultas.jugadoresDeEquipoEspecifico("T-25").transact(xa).unsafeRunSync()
  Graficas.crearGraficaJugadores(jugadores)
  
}


