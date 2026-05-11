connection: "jala_tara_bq"
include: "/views/*.view.lkml"

explore: water_balance {
  label: "Village Water Balance"
  join: waste_index {
    type: left_outer
    sql_on: ${water_balance.date} = ${waste_index.date}
        AND ${water_balance.village_ward} = ${waste_index.village_ward} ;;
    relationship: one_to_one
  }
  join: anomaly_flags {
    type: left_outer
    sql_on: ${water_balance.date} = ${anomaly_flags.date}
        AND ${water_balance.village_ward} = ${anomaly_flags.village_ward} ;;
    relationship: one_to_many
  }
}

explore: peak_demand {
  label: "Peak Demand Hours"
}
