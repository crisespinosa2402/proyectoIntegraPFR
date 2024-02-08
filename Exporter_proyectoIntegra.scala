package ec.edu.utpl.computacion.pfr.pi

import com.github.tototoshi.csv._
import java.io.File
import java.util.Locale

object Exporter_proyectoIntegra {
  @main
  def funcExporter(): Unit = {
    val path2DataFile = "C://Users//StarMedia//Desktop//dataset//dsPartidosYGoles.csv"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] =
      reader.allWithHeaders()

    reader.close()
    val path2DataFile2 = "C://Users//StarMedia//Desktop//dataset//dsAlineacionesXTorneo-2.csv"
    val reader2 = CSVReader.open(new File(path2DataFile2))
    val contentFile2: List[Map[String, String]] =
      reader2.allWithHeaders()

    reader2.close()

    generateDataTournament(contentFile)
    generateDataTeam(contentFile)
    val playerInserts = generateDataPlayer(contentFile2)
    playerInserts.foreach(println)

  }

  def generateDataTournament(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO TOURNAMENT (tournament_id, name, year, host_country, winner, count_teams) VALUES('%s', '%s', %d, '%s', '%s', %d);"
    val tournaments = data

      .map(
        row => (row("matches_tournament_id").trim,
          row("tournaments_tournament_name").replaceAll("'", ""),
          row("tournaments_year").toInt,
          row("tournaments_host_country"),
          row("tournaments_winner"),
          row("tournaments_count_teams").toInt)
      ).distinct
      .map(t7 => sqlInsert.formatLocal(java.util.Locale.US, t7._1, t7._2, t7._3, t7._4, t7._5, t7._6))

    tournaments.foreach(println)
  }

  def generateDataTeam(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO TEAM (team_id, name, mens_team, womens_team, region_name) VALUES ('%s', '%s', %d, %d, '%s');"

    val distinctTeams = data
      .map(row => (row("matches_home_team_id")))
      .distinct
      .map { teamId =>
        val teamData = data.find(_("matches_home_team_id") == teamId).getOrElse(Map.empty)
        (
          teamId,
          teamData("home_team_name"),
          teamData("home_mens_team").toInt,
          teamData("home_womens_team").toInt,
          teamData("home_region_name")
        )
      }

    val teamInserts = distinctTeams.map { t =>
      sqlInsert.formatLocal(java.util.Locale.US, t._1, t._2, t._3, t._4, t._5)
    }

    distinctTeams.foreach(println)
  }

  def generateDataPlayer(data: List[Map[String, String]]): List[String] = {
    val sqlInsert =
      s"INSERT INTO PLAYER (player_id, team_id, shirt_number, position_name," +
        s" family_name, given_name, birth_date, female, goal_keeper, defender, midfielder, forward) VALUES" +
        s" ('%s', '%s', %d, '%s', '%s', '%s','%s', %d, %d, %d, %d, %d);"

    val distinctPlayers = data
      .map(row => (row("squads_player_id").trim, row("squads_team_id")))
      .distinctBy(identity)
      .map { case (playerId, teamId) =>
        val playerData = data.find(row =>
          row("squads_player_id").trim == playerId && row("squads_team_id") == teamId
        ).getOrElse(Map.empty)
        (
          playerId,
          teamId,
          playerData("squads_shirt_number").toInt,
          playerData("squads_position_name"),
          playerData("players_family_name"),
          playerData("players_given_name"),
          playerData("players_birth_date"),
          playerData("players_female").toInt,
          playerData("players_goal_keeper").toInt,
          playerData("players_defender").toInt,
          playerData("players_midfielder").toInt,
          playerData("players_forward").toInt
        )
      }

    val playerInserts = distinctPlayers.map { t =>
      sqlInsert.formatLocal(
        java.util.Locale.US,
        t._1,
        t._2,
        t._3,
        t._4,
        t._5,
        t._6,
        t._7,
        t._8,
        t._9,
        t._10,
        t._11,
        t._12
      )
    }
    playerInserts
  }
}
