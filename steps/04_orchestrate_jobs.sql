use role accountadmin;
use schema quickstart_prod.gold;

-- ========================================================================
-- Declarative target table of pipeline
-- ========================================================================
create or alter table vacation_spots (
    city varchar,
    airport varchar,
    co2_emissions_kg_per_person float,
    punctual_pct float,
    avg_temperature_air_f float,
    avg_relative_humidity_pct float,
    avg_cloud_cover_pct float,
    precipitation_probability_pct float,
    aquarium_cnt int,
    zoo_cnt int,
    korean_restaurant_cnt int
) data_retention_time_in_days = 1;

-- ========================================================================
-- Task: Merge pipeline results into target table
-- ========================================================================
create or alter task vacation_spots_update
    schedule = '1440 minute'
    warehouse = 'quickstart_wh'
    ERROR_ON_NONDETERMINISTIC_MERGE = false
as
merge into vacation_spots
using (
    select *
    from silver.flights_from_home flight
    join silver.weather_joined_with_major_cities city 
        on city.geo_name = flight.arrival_city
    join silver.attractions att
        on att.geo_name = city.geo_name
) as harmonized_vacation_spots
on vacation_spots.city = harmonized_vacation_spots.arrival_city
   and vacation_spots.airport = harmonized_vacation_spots.arrival_airport
when matched then
    update set
        vacation_spots.co2_emissions_kg_per_person = harmonized_vacation_spots.co2_emissions_kg_per_person,
        vacation_spots.punctual_pct = harmonized_vacation_spots.punctual_pct,
        vacation_spots.avg_temperature_air_f = harmonized_vacation_spots.avg_temperature_air_f,
        vacation_spots.avg_relative_humidity_pct = harmonized_vacation_spots.avg_relative_humidity_pct,
        vacation_spots.avg_cloud_cover_pct = harmonized_vacation_spots.avg_cloud_cover_pct,
        vacation_spots.precipitation_probability_pct = harmonized_vacation_spots.precipitation_probability_pct,
        vacation_spots.aquarium_cnt = harmonized_vacation_spots.aquarium_cnt,
        vacation_spots.zoo_cnt = harmonized_vacation_spots.zoo_cnt,
        vacation_spots.korean_restaurant_cnt = harmonized_vacation_spots.korean_restaurant_cnt
when not matched then 
    insert values (
        harmonized_vacation_spots.arrival_city,
        harmonized_vacation_spots.arrival_airport,
        harmonized_vacation_spots.co2_emissions_kg_per_person,
        harmonized_vacation_spots.punctual_pct,
        harmonized_vacation_spots.avg_temperature_air_f,
        harmonized_vacation_spots.avg_relative_humidity_pct,
        harmonized_vacation_spots.avg_cloud_cover_pct,
        harmonized_vacation_spots.precipitation_probability_pct,
        harmonized_vacation_spots.aquarium_cnt,
        harmonized_vacation_spots.zoo_cnt,
        harmonized_vacation_spots.korean_restaurant_cnt
    );

-- ========================================================================
-- Task: Email notification for perfect vacation spots
-- ========================================================================
create or alter task email_notification
    warehouse = 'quickstart_wh'
    after vacation_spots_update
as 
begin
    let options varchar := (
        select to_varchar(array_agg(object_construct(*)))
        from vacation_spots
        where true
          and punctual_pct >= 50
          and avg_temperature_air_f >= 70
          and korean_restaurant_cnt > 0
          and (zoo_cnt > 0 or aquarium_cnt > 0)
        limit 10
    );

    if (:options = '[]') then
        CALL SYSTEM$SEND_EMAIL(
            'email_integration',
            'vansika.sonthalia@tredence.com',
            'New data successfully processed: No suitable vacation spots found.',
            'The query did not return any results. Consider adjusting your filters.'
        );
    end if;

    let query varchar := 'Considering the data provided below in JSON format, pick the best city for a family vacation in summer?
Explain your choice, offer a short description of the location and provide tips on what to pack for the vacation considering the weather conditions? 
Finally, could you provide a detailed plan of daily activities for a one week long vacation covering the highlights of the chosen destination?\n\n';

    let response varchar := (
        select SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', :query || :options)
    );

    CALL SYSTEM$SEND_EMAIL(
        'email_integration',
        'vansika.sonthalia@tredence.com',
        'New data successfully processed: The perfect place for your summer vacation has been found.',
        :response
    );
exception
    when EXPRESSION_ERROR then
        CALL SYSTEM$SEND_EMAIL(
            'email_integration',
            'vansika.sonthalia@tredence.com',
            'New data successfully processed: Cortex LLM function inaccessible.',
            'It appears that the Cortex LLM functions are not available in your region'
        );
end;

-- Resume follow-up task
alter task email_notification resume;

-- Manually initiate a full execution of the DAG
execute task vacation_spots_update;
