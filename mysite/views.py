from rest_framework.response import Response
from rest_framework.decorators import api_view
from mysite import settings
import requests
import json
import psycopg2

@api_view(['GET'])
def getGeoData(request):
    filter_value = request.query_params.get('filter')
    threshold = request.query_params.get('threshold')
    cell = request.query_params.get('cell')

    wcell = ""
    if cell != "0":
        wcell = "WHERE id_source=" + cell + ""

    condition2 = ""

    if filter_value == "":
        condition2 = "-1"
    elif len(filter_value) >= 1:
        filter_value = filter_value.split(",")
        for filter in filter_value:
            if filter.isnumeric():
                condition2 += filter+" OR geo_data.service = "
        condition2 = condition2[:len(condition2)-23]
    else:
        condition1 = condition2 = filter_value


    if cell == '0':
        #Query that
        query = f"""WITH geo as(SELECT ogc_fid, source, identifier, geo_data.category, geo_data.service, name, address,  ST_X(ST_Centroid(geom)) as longitude, ST_Y(ST_Centroid(geom)) as latitude, service_class
                   FROM geo_data JOIN service ON geo_data.service = service.service WHERE geo_data.service = """ + condition2 + """)
                   SELECT json_agg(geo) FROM geo"""

    elif threshold == '0':
        query = f"""WITH services as (SELECT ogc_fid, source, identifier, geo_data.service, name, address, ST_X(ST_Centroid(geo_data.geom)) as longitude, ST_Y(ST_Centroid(geo_data.geom)) as latitude, service_class, id 
                        FROM geo_data
                        JOIN turin_pop_grid as pop
                        ON ST_Intersects(geo_data.geom, pop.geom)
                        JOIN service ON service.service = geo_data.service
                        WHERE geo_data.service = """ + condition2 + """),
                        result AS (
                        SELECT services.service, min(walk_minutes) AS walk_min
                        FROM services
                        JOIN distances AS dist
                        ON (services.id=id_dest)
                        """ + wcell + """
                        GROUP BY (services.service)),
                        json AS(
                        SELECT ogc_fid, source, identifier, ser.service, name, address, latitude, longitude, service_class
                        FROM services as ser JOIN distances ON (ser.id=id_dest) JOIN result ON (result.service=ser.service AND result.walk_min=distances.walk_minutes)
                        """ + wcell + """)
                        SELECT json_agg(json) FROM json"""

    else:
        query = f"""WITH walk_access AS (SELECT *
                                FROM distances AS dist
                                JOIN turin_pop_grid AS grid ON (grid.id=dist.id_dest)
                                """ + wcell + """ AND dist.walk_minutes <= """+threshold+"""
                                ORDER BY id_source)
                                    ,geo AS(SELECT DISTINCT ogc_fid, source, identifier, geo_data.category, geo_data.service, name, address, ST_X(ST_centroid(geo_data.geom)) as longitude, ST_Y(ST_centroid(geo_data.geom)) as latitude, service_class
                                     FROM geo_data JOIN service ON geo_data.service = service.service
                                     JOIN walk_access ON ST_Intersects(geo_data.geom, walk_access.geom) WHERE geo_data.service = """ + condition2 + """)
                                     SELECT 
                                        json_agg(geo) FROM geo"""



    my_database = settings.DATABASES["default"]

    connection = psycopg2.connect(
        database = my_database["NAME"],
        user = my_database["USER"],
        password=my_database["PASSWORD"],
        host=my_database["HOST"],
    )

    cursor = connection.cursor()

    cursor.execute(query)

    response = cursor.fetchone()

    if response[0] == None:
        result = []
    else:
        result = response[0]

    return Response(result)

@api_view(['GET'])
def getTaxonomy(request):
    query = f"""SELECT json_build_object('id', category.category,'name',category.category_class, 'services',
        json_agg(DISTINCT CAST(row(service.service,service.service_class) as temp_type)))
           FROM geo_data JOIN service ON (service.service = geo_data.service) 
           JOIN category ON(category.category = geo_data.category)
           GROUP BY geo_data.category, category.category,category.category_class"""
    my_database = settings.DATABASES["default"]

    connection = psycopg2.connect(
        database=my_database["NAME"],
        user=my_database["USER"],
        password=my_database["PASSWORD"],
        host=my_database["HOST"],
    )

    cursor = connection.cursor()

    cursor.execute(query)

    response = cursor.fetchall()

    return Response(response)

@api_view(['GET'])
def getAddress(request):
    address = request.query_params.get('address')

    dictionary = []
    if address != "":
        response = requests.get('https://api.mapbox.com/geocoding/v5/mapbox.places/'+address+',torino.json?access_token=pk.eyJ1IjoiZmFiaW9kIiwiYSI6ImNsOGxncmNyNTA5Y3Azb3FjeW53eGplbWEifQ.oWzNOzyeLZ42s9AWBjTBwA').text
        json_r = json.loads(response)

        my_database = settings.DATABASES["default"]

        connection = psycopg2.connect(
            database=my_database["NAME"],
            user=my_database["USER"],
            password=my_database["PASSWORD"],
            host=my_database["HOST"],
        )

        for item in json_r['features']:
            for ctx in item['context']:
                if(ctx['id'] == "place.58394736"):
                    if dictionary == []:
                        query_access = f""" SELECT json_build_object('contour',ST_AsGeoJSON(geom) :: json->'coordinates', 'id',id,
                                                                        'population',population, 'quart',quart, 'x',x, 'y',y, 'latitude',ST_Y(ST_Centroid(geom)), 'longitude',ST_X(ST_Centroid(geom))) 
                                                                         FROM (SELECT * FROM turin_pop_grid as pop
                                                                             WHERE ST_intersects(pop.geom,'SRID=4326;POINT(""" + str(item['center'][0]) + """ """ + str(item['center'][1]) + """)'::geography)) AS tot"""
                        cursor = connection.cursor()

                        cursor.execute(query_access)

                        accessibility = cursor.fetchone()
                        dictionary = [{'name' : item['place_name'], 'address': "", 'latitude': item['center'][1], 'longitude': item['center'][0], 'cell_info': accessibility}]

    return Response(dictionary)

@api_view(['GET'])
def getGrid(request):
    my_database = settings.DATABASES["default"]

    connection = psycopg2.connect(
        database=my_database["NAME"],
        user=my_database["USER"],
        password=my_database["PASSWORD"],
        host=my_database["HOST"],
    )

    filter_value = request.query_params.get('filter')
    threshold = request.query_params.get('threshold')
    cell = request.query_params.get('cell')
    index = request.query_params.get('index')


    condition = "WHERE "
    select = ""
    wcell = ""
    if cell != "0":
        wcell = "WHERE dist.id_source="+cell+""

    if filter_value == "":
        condition += "(c_a.service = -1"
    elif len(filter_value) >= 1:
        condition += " (c_a.service = "
        filter_value = filter_value.split(",")
        for filter in filter_value:
            select += "\"" + filter + "\"+ "
            condition += str(filter) + " OR c_a.service = "
        condition = condition[:len(condition) - 17]
        select = select[:len(select) - 2]


    if filter_value == "":
        if cell != "0":
            wcell = "WHERE id=" + cell + ""
        query_access = f"""SELECT json_build_object('contour',ST_AsGeoJSON(geom) :: json->'coordinates', 'id',id,
                            'population',population, 'quart',quart, 'x',x, 'y',y, 'latitude',ST_Y(ST_Centroid(geom)), 'longitude',ST_X(ST_Centroid(geom)), 'accessibility',0, 'rank',ROW_NUMBER() OVER( ORDER BY population DESC), 'color', json_build_object('r',38,'g',149,'b',252)) 
                             FROM turin_pop_grid AS pop """+wcell
    elif index == "1":
        query_access = f"""WITH services AS (SELECT dist.id_source, c_a.service
                                    FROM distances AS dist
                                    JOIN turin_pop_grid AS grid ON (grid.id=dist.id_dest)
                                    JOIN cell_access AS c_a ON grid.id = c_a.id """+condition+""")
                                    AND dist.walk_minutes <= 15
                                    GROUP BY (dist.id_source, c_a.service)
                                    ORDER BY id_source),
                                    walk_access AS ( SELECT id_source, COUNT(service) AS accessibility, 255/(1.0/12*(COUNT(service)+1)*7) AS color
                                        FROM services
                                        GROUP BY(id_source)
                                    )
                                    SELECT json_build_object('contour',ST_AsGeoJSON(geom) :: json->'coordinates', 'id',id,
                                        'population',population, 'quart',quart, 'x',x, 'y',y, 'latitude',ST_Y(ST_Centroid(geom)), 'longitude',ST_X(ST_Centroid(geom)), 'accessibility',accessibility, 'rank',rank, 'color', json_build_object('r',255,'g',color,'b',48)) 
                                         FROM (SELECT * 
                                                    FROM(SELECT *, ROW_NUMBER() OVER( ORDER BY (accessibility,population) DESC) as rank 
                                                    FROM turin_pop_grid as pop
                                                    JOIN walk_access AS dist ON (pop.id = id_source) 
                                                    )AS dist
                                                """+wcell+""") AS res"""
    else:
        if threshold == '0':
            # 154 min is the highest time on walk between 2 cell
            query_access = f"""WITH t_access AS (SELECT id_source, min(walk_minutes) AS accessibility, c_a.service
                                FROM distances as dist
                                JOIN turin_pop_grid as grid ON (grid.id=dist.id_dest)
                                JOIN cell_access AS c_a ON grid.id = c_a.id """+condition+""")
                                GROUP BY (id_source, c_a.service)
                                ORDER BY id_source ASC)
                                , average AS (SELECT id_source, AVG(accessibility) AS accessibility, (1.0/154*AVG(accessibility)*7)*255 AS color
                                             FROM t_access
                                             GROUP BY (id_source))
                                SELECT json_build_object('contour',ST_AsGeoJSON(geom) :: json->'coordinates', 'id',id,
                                        'population',population, 'quart',quart, 'x',x, 'y',y, 'latitude',ST_Y(ST_Centroid(geom)), 'longitude',ST_X(ST_Centroid(geom)), 'accessibility',accessibility, 'rank',rank, 'color', json_build_object('r',255,'g',color,'b',48)) 
                                         FROM (SELECT *
                                                FROM (SELECT *, ROW_NUMBER() OVER( ORDER BY  accessibility ASC ,population DESC) as rank 
                                                FROM turin_pop_grid as pop
                                                RIGHT JOIN average AS dist ON (pop.id = id_source) 
                                                )AS dist
                                                """+wcell+""")AS res"""

        else:
            if filter_value == "":
                query_access = f"""WITH walk_access AS (SELECT pop.id AS id_source, 0 AS accessibility, 255/(1.0/69*1*7) AS color
                                FROM turin_pop_grid AS pop
                                JOIN distances AS dist ON (pop.id=dist.id_source))
                                SELECT json_build_object('contour',ST_AsGeoJSON(geom) :: json->'coordinates', 'id',id,
                                        'population',population, 'quart',quart, 'x',x, 'y',y, 'latitude',ST_Y(ST_Centroid(geom)), 'longitude',ST_X(ST_Centroid(geom)), 'accessibility',accessibility, 'rank',rank, 'color', json_build_object('r',255,'g',color,'b',48)) 
                                         FROM (SELECT *
                                                FROM (SELECT *, ROW_NUMBER() OVER( ORDER BY  accessibility ASC ,population DESC) as rank 
                                                FROM turin_pop_grid as pop
                                                JOIN walk_access AS dist ON (pop.id = id_source) 
                                                )AS dist
                                                """+wcell+""")AS res"""
            else:
                # there is a subquery because we want to calculate the maximum accessibility value of each query to make the color cell more intuitive
                query_access = f"""WITH walk_access AS (SELECT *, 255/(0.06*MAX(access_info.accessibility)*(access_info.accessibility+1)) AS color
                                    FROM 
                                        (SELECT dist.id_source, SUM(occur) AS accessibility
                                        FROM distances AS dist
                                        JOIN turin_pop_grid AS grid ON (grid.id=dist.id_dest)
                                        JOIN cell_access AS c_a ON grid.id = c_a.id """+condition+""")
                                        AND dist.walk_minutes <= """+threshold+"""
                                        GROUP BY (dist.id_source)
                                        ORDER BY id_source) as access_info
                                    GROUP BY (id_source,accessibility))
                                    SELECT json_build_object('contour',ST_AsGeoJSON(geom) :: json->'coordinates', 'id',id,
                                        'population',population, 'quart',quart, 'x',x, 'y',y, 'latitude',ST_Y(ST_Centroid(geom)), 'longitude',ST_X(ST_Centroid(geom)), 'accessibility',accessibility, 'rank',rank, 'color', json_build_object('r',255,'g',color,'b',48)) 
                                         FROM (SELECT * 
                                                    FROM(SELECT *, ROW_NUMBER() OVER( ORDER BY (accessibility,population) DESC) as rank 
                                                    FROM turin_pop_grid as pop
                                                    JOIN walk_access AS dist ON (pop.id = id_source) 
                                                    )AS dist
                                                """+wcell+""") AS res"""

    cursor = connection.cursor()

    cursor.execute(query_access)

    accessibility = cursor.fetchall()
    if accessibility == []:
        if cell != "0":
            wcell = "WHERE id=" + cell + ""
        query_access = f"""SELECT json_build_object('contour',ST_AsGeoJSON(geom) :: json->'coordinates', 'id',id,
                                                        'population',population, 'quart',quart, 'x',x, 'y',y, 'latitude',ST_Y(ST_Centroid(geom)), 'longitude',ST_X(ST_Centroid(geom)), 'accessibility',0, 'rank', ROW_NUMBER() OVER( ORDER BY  accessibility ASC ,population DESC) as rank, 'color', json_build_object('r',38,'g',149,'b',252)) 
                                                         FROM turin_pop_grid
                                                        """ + wcell
        cursor = connection.cursor()

        cursor.execute(query_access)

        accessibility = cursor.fetchall()

    return Response(accessibility)
