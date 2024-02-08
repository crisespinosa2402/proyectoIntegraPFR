import java.sql.{Date, Time}
import cats.effect.IO
import doobie._
import doobie.implicits._
import cats.effect.unsafe.implicits.global
import cats.syntax.parallel._

case class Tournament(tournament_id: String,
                      name: String,
                      year: Int,
                      host_country: String,
                      winner: String,
                      count_teams: Int)

case class Team(team_id: String,
                name: String,
                mens_team: Boolean,
                womens_team: Boolean,
                region_name: String)

case class Player(player_id: String,
                  team_id: String,
                  shirt_number: Int,
                  position_name: String,
                  family_name: String,
                  given_name: String,
                  birth_date: Date,
                  female: Boolean,
                  goal_keeper: Boolean,
                  defender: Boolean,
                  midfielder: Boolean,
                  forward: Boolean)

case class Stadium(stadium_id: String,
                   name: String,
                   city_name: String,
                   country_name: String,
                   capacity: Int)

case class Match(match_id: String,
                 tournament_id: String,
                 away_team_id: String,
                 home_team_id: String,
                 stadium_id: String,
                 match_date: Date,
                 match_time: Time,
                 stage_name: String,
                 home_team_score: Int,
                 away_team_score: Int,
                 extra_time: Boolean,
                 penalty_shootout: Boolean,
                 home_team_score_penalties: Int,
                 away_team_score_penalties: Int,
                 result: String)

case class Goal(goal_id: String,
                match_id: String,
                team_id: String,
                player_id: String,
                minute_regulation: Int,
                minute_stoppage: Int,
                match_period: String,
                own_goal: Boolean,
                penalty: Boolean)

object DBDemo {
  val xa = Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver",
    url = "jdbc:mysql://localhost:3306/mundiales",
    user = "root",
    password = "UTPL2023",
    logHandler = None
  )

  def obtenerMinAnioTorneo: IO[Option[Int]] =
    sql"SELECT MIN(year) FROM TOURNAMENT".query[Option[Int]].unique.transact(xa)

  def obtenerPromedioEquiposPorTorneo: IO[Option[Double]] =
    sql"SELECT AVG(count_teams) FROM TOURNAMENT".query[Option[Double]].unique.transact(xa)

  def obtenerNumeroCamisetaMaximo: IO[Option[Int]] =
    sql"SELECT MAX(shirt_number) FROM PLAYER".query[Option[Int]].unique.transact(xa)

  def obtenerFrecuenciaEstadios: IO[List[(String, Long)]] =
    sql"SELECT stadium_id, COUNT(*) FROM MATCHES GROUP BY stadium_id".query[(String, Long)].to[List].transact(xa)

  def obtenerModaResultadoPartido: IO[Option[String]] =
    sql"SELECT result, COUNT(*) AS count FROM MATCHES GROUP BY result ORDER BY count DESC LIMIT 1".query[Option[String]].unique.transact(xa)

  @main
  def estadistica(): Unit = {
    println("ESTADISTICAS:")

    val minYearResult: IO[Unit] = obtenerMinAnioTorneo.map(minYear => println(s"Mínimo año de torneo: $minYear"))

    val avgTeamsCountResult: IO[Unit] = obtenerPromedioEquiposPorTorneo.map(avgTeamsCount => println(s"Promedio de equipos por torneo: $avgTeamsCount"))

    val maxShirtNumberResult: IO[Unit] = obtenerNumeroCamisetaMaximo.map(maxShirtNumber => println(s"Máximo número de camiseta: $maxShirtNumber"))

    val stadiumFrequencyResult: IO[Unit] = obtenerFrecuenciaEstadios.map { frequencies =>
      println("Frecuencia de estadios:")
      frequencies.foreach(println)
    }

    val matchResultModeResult: IO[Unit] = obtenerModaResultadoPartido.map(matchResultMode => println(s"Moda del resultado de los partidos: $matchResultMode"))

    val allResults: IO[Unit] = minYearResult.flatMap(_ =>
      avgTeamsCountResult.flatMap(_ =>
        maxShirtNumberResult.flatMap(_ =>
          stadiumFrequencyResult.flatMap(_ =>
            matchResultModeResult.map(_ => ())
          )
        )
      )
    )

    val allResultsConcurrent: IO[Unit] = (
      minYearResult,
      avgTeamsCountResult,
      maxShirtNumberResult,
      stadiumFrequencyResult,
      matchResultModeResult
    ).parTupled.map(_ => ())

    allResultsConcurrent.unsafeRunSync()
  }
}

