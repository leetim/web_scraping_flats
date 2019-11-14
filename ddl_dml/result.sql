with 
		house_duplicate as (
		-- Получаем дубликаты адресов
						select 
								longitude,
								latitude,		
								count(*) as count_items,
								cast(longitude as text) ||'/'|| cast(latitude as text) as index_column
						from 
								staging_tables.geocoder_house as geocoder
						where 
								longitude is not null
								or latitude is not null
						group by 
								longitude,
								latitude
						having 
								count(*) > 1
		), 
		house_coordinates as (
		-- Получаем координаты без дубликатов
						select 
								geocoder.*,
								cast(geocoder.longitude as text) ||'/'|| cast(geocoder.latitude as text) as index_column
						from 
								staging_tables.geocoder_house as geocoder
						where 
								cast(geocoder.longitude as text) ||'/'|| cast(geocoder.latitude as text) not in (
																												select distinct 
																														index_column
																												from 
																														house_duplicate
																														) 
		), 
		house as (
		-- Получаем дома в связке с координатами
					select 
							house.*,
							house_coordinates.longitude,
							house_coordinates.latitude,
							ST_SetSrid(ST_MakePoint(house_coordinates.longitude, house_coordinates.latitude),4326) as geom
					from 
							staging_tables.house as house
					inner join house_coordinates on house.address = house_coordinates.address
		), 
		farpost as (
		-- Получаем объявления farpost
					select 
							farpost.*,
							ST_SetSrid(ST_MakePoint(longitude, latitude),4326) as geom
					from 
							farpost.farpost as farpost
		),
		distance as (
		-- Связываем дома и объявление, расстояние должно быть не мменее 37 метров 
						select 
								farpost.id as farpost_id 
								,farpost.geom as farpost_geom
								,farpost.longitude as farpost_longitude
								,farpost.latitude as farpost_latitude
								,farpost.address as farpost_address
								,farpost.title as farpost_title
								,farpost."text" as farpost_text
								,farpost.clean_text as farpost_clean_text
								,farpost.lem_text as farpost_lem_text
								,farpost.status_house as farpost_status_house
								,farpost.is_builder as farpost_is_builder
								,farpost.price as farpost_price
								,farpost.area as farpost_area
								,farpost.floor as farpost_floor
								,farpost.is_balcony as farpost_is_balcony
								,farpost.is_mortage as farpost_is_mortage
								,house.houseguid as house_houseguid
								,house.address as house_address
								,house.built_year as house_built_year
								,house.floor_count_max as house_floor_count_max
								,house.floor_count_min as house_floor_count_min
								,house.entrance_count as house_entrance_count
								,house.elevators_count as house_elevators_count
								,house.living_quarters_count as house_living_quarters_count
								,house.area_residential as house_area_residential
								,house.chute_count as house_chute_count
								,house.parking_square as house_parking_square
								,house.wall_material as house_wall_material
								,house.geom as house_geom
								,house.longitude as house_longitude
								,house.latitude as house_latitude
								,ST_Distance(ST_Transform(farpost.geom::geometry, 3857), ST_Transform(house.geom::geometry, 3857)) as distance
						from 
								farpost, house
						where 
								ST_Distance(ST_Transform(farpost.geom::geometry, 3857), ST_Transform(house.geom::geometry, 3857)) <= 37
								
		),
		result as (
					select 
							distance.farpost_id 
							,distance.farpost_geom
							,distance.house_geom
							,distance.distance
							,distance.farpost_address
							,distance.house_address
							,distance.farpost_longitude
							,distance.farpost_latitude
							,distance.farpost_address
							,distance.farpost_title
							,distance.farpost_text
							,distance.farpost_clean_text
							,distance.farpost_lem_text
							,distance.farpost_status_house
							,distance.farpost_is_builder
							,distance.farpost_price
							,distance.farpost_area
							,distance.farpost_floor
							,distance.farpost_is_balcony
							,distance.farpost_is_mortage
							,distance.house_houseguid		
							,distance.house_built_year
							,distance.house_floor_count_max
							,distance.house_floor_count_min
							,distance.house_entrance_count
							,distance.house_elevators_count
							,distance.house_living_quarters_count
							,distance.house_area_residential
							,distance.house_chute_count
							,distance.house_parking_square
							,distance.house_wall_material
							,distance.house_houseguid		
							,distance.house_longitude
							,distance.house_latitude 
					from 
							distance
					where 
					-- На 1 объявление должна приходится 1 строка
							distance.farpost_id  not in (
														select 
																farpost_id
														from 
																distance
														group by 
																farpost_id
														having count(*) > 1
							)
		)
-- Выводим результат
select 
		*
from 
		result
