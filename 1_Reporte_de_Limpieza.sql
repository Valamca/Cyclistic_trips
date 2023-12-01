/*
	LIMPIEZA DE DATOS

		Este proceso abordar� la limpieza de datos que se realiz� para obtener un conjunto de datos integro
	y coherente, con el que fuese como poder realizar el �n�lisis de datos sobre la empresa de bicicletas 
	Cyclistic.

	Los diferente archivos pueden ser encontrados en la siguiente direcci�n:
	https://divvy-tripdata.s3.amazonaws.com/index.html

	Los meses utilizados para este procedimiento corresponden a los meses desde Enero hasta Dicimebre del
	a�o 2022.
*/


--	El primer paso es explorar el cojunto de datos para comprender su estructura y distribuci�n, con el 
--  siguiente comando que se realiz� para todas las tablas del a�o 2022.

	USE [Cyclistic]

	SELECT *
	FROM dbo.Dim_TripENE_2022; 

/*
		Una vez obtenida una vista previa del conjunto de datos completo y observar que la mayor parte de datos
	faltante eran de las siguiente columnas: start_station_name, start_station_id, end_station_name y 
	end_station_id, se tom� la decisi�n de abordar los datos faltantes con los existentes dentro del cojunto.
	Mientras que los datos que son mucho m�s constantes, y con los cuales obtedremos nuestros datos faltantes
	son registros de ubicaci�n como Latitudes y Longitudes.
	
		Debido a la variabilidad de coordenadas de Latitud y Longitud que se encuentran en el conjunto de datos
	para una misma ubicaci�n, se opt� por generar una lista con los siguientes elementos:
		
		* Nombres �nicos de estaci�n.
		* IDs �nicos de estaci�n.
		* Rangos m�nimos y m�ximos de latitudes y longitudes para cada estaci�n.

		Con lo anterior nos podemos asegurar que si un registro tiene sus coordenadas dentro de cierto rango,
	podemos asignar el nombre de estaci�n correspondiente a ese rango de coordenadas.
	
	Con la siguiente consulta: 
*/

	USE [Cyclistic]

	SELECT *
	INTO Estaciones -- <------- Nombre de la tabla d�nde se guardar� nuestra informaci�n resultante
	FROM
		(
		SELECT
			start_station_name,
			start_station_id,
			MIN(start_lat) AS Latitud_minima,
			MAX(start_lat) AS Latitud_maxima,
			MIN(start_lng) AS Longitud_minima,
			MAX(start_lng) AS Longitud_maxima
		FROM(
			SELECT start_station_name,start_station_id, start_lat, start_lng FROM Cyclistic.dbo.Dim_TripENE_2022
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripFEB_2022
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripMAR_2022
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripABR_2022 
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripMAY_2022 
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripJUN_2022
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripJUL_2022 
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripAGO_2022 
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripSEP_2022 
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripOCT_2022 
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripNOV_2022 
			UNION ALL
			SELECT start_station_name,start_station_id, start_lat, start_lng  FROM Cyclistic.dbo.Dim_TripDIC_2022
			) AS Resultados
		GROUP BY
			start_station_name, start_station_id) AS Estaciones;

/*
		Una vez realizada la consulta de Estaciones y obtener el rango de coordenadas para cada estaci�n y su respectivo ID,
	lanzamos la siguiente consulta para asegurarnos que no haya dato duplicados, y en caso de que los haya s�lo nos mostrar�
	aquellos resgistros que compartan el mismo ID.
*/

	SELECT
		start_station_id, 
		COUNT(start_station_id) AS row_count 
	FROM 
		ESTACIONES
	GROUP BY
		start_station_id
	HAVING
		COUNT(start_station_id) > 1;

/*
	C	�mo en este caso s� tenemos registros duplicado lanzaremos la siguiente consulta para ubicar exactamente que estaciones
	comparte el mismo ID, cuando encuentre dichos resgitros nos mostrar� el nombre de cada registro y su ID para aseguranos
	de que efecivamente est� repetido.
*/

	SELECT 
		A.start_station_name,
		B.start_station_id
	FROM
		Estaciones AS A
	INNER JOIN 
		(SELECT
			start_station_id, 
			COUNT(start_station_id) AS row_count
		FROM 
			ESTACIONES
		GROUP BY
			start_station_id
		HAVING
			COUNT(start_station_id) > 1
		) AS B
	ON
		A.start_station_id = B.start_station_id
	ORDER BY
		A.start_station_id;

/*
		Los resultado obtenidos son interesantes porque la mayor�a de los registros duplicados corresponden, tanto a la 
	misma ubicaci�n, y comparten el mismo nombre de estaci�n, con una ligera variaci�n que es el texto "Public Rack -"
	al inicio, lo cual agregando el factor de que por lo general son bicicletas acopladas o el�tricas nos habla de una
	estaci�n que est� en la misma ubicaci�n pero que es de autoservicio, por lo cu�l se ignorar�n estos registros.

		Para los dem�s registros que son duplicados y que no comparten ning�n elemento en com�n m�s all� del ID 
	duplicado se recurri� a la siguiente consulta para obtener m�s informaci�n de las estaciones:

*/

	SELECT 
		start_station_name,
		start_station_id
	FROM 
		Estaciones
	WHERE
		start_station_name LIKE'%Bissell St & Armitage Ave*'; -- <--- El texto se intercambio con los diferentes duplicados

/*
		La consulta anterior realiza una busqueda de registros que sean parecidos al texto condicional, en este caso se 
	buscan estaciones que compartan un nombre de calle o avenida, para observar s� es posible alguna tendencia en los 
	IDs que nos ayude a corregir los datos.

		C�mo los resultados obtenidos nos ayudan a comprender de que forma solucionar estos datos, entonces obtamos 
	por observar s� los resgitros eran significativos para nuestro pr�ximo an�lisis y o s� era despreciables y 
	pudiesemos eliminarlos.

	Para lo anterior se realiz� la siguiente consulta:
*/

	SELECT 
		start_station_name,
		start_station_id,
		COUNT(start_station_name) AS num_of_trips
	FROM 
		[Cyclistic].[dbo].[Dim_Trips_2022]
	GROUP BY
		start_station_name,start_station_id
	ORDER BY
		COUNT(start_station_name) DESC;

/*
		Gracias a los resultado obtenidos y que nuestros registros no tienen un impacto mayor en nuestro conjunto de 
	datos fueron eliminados del conjuto de datos, pero de los datos que eran duplicados se eliminaron solo aquellos 
	que tuviesen el menor n�mero de viajes y para esto se hizo cada eliminaci�n manual para tener la mayor precisi�n.

	Con la siguiente consulta:
*/

	DELETE FROM dbo.Dim_TripENE_2022
	WHERE start_station_name = 'station' AND start_station_id = 'TA1309000032';

/*
	Ahora que tenemos nombres e IDs �nicos por estaci�n, proseguiremos a crear una tabla que contenga los registros de
	todos los meses, y se realiza este procedimiento hasta ahora, porque era mucho m�s sencillo realizar las observaciones
	anteriores en tablas mas peque�as, debido a que son m�s precisas y cortas las tablas resultantes.
*/

	SELECT 
		* INTO [dbo].[Dim_Trips_2022] -- <---- Nombre de tabla para los registros del a�o 2022
	FROM
		(
		SELECT * FROM [dbo].[Dim_TripENE_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripFEB_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripMAR_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripABR_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripMAY_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripJUN_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripJUL_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripAGO_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripSEP_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripOCT_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripNOV_2022]
		UNION ALL
		SELECT * FROM [dbo].[Dim_TripDIC_2022]
		) AS Trips;


/*
	Ahora con que tenemos una tabla consolidada, podemos realizar la exploraci�n de datos faltantes:
*/

	SELECT 
		*
	FROM
		[dbo].[Dim_Trips_2022]
	WHERE
		ride_id IS NULL OR 
		rideable_type IS NULL OR
		started_at IS NULL OR
		ended_at IS NULL OR
		start_station_name IS NULL OR
		start_station_id IS NULL OR
		end_station_name IS NULL OR
		end_station_id IS NULL OR
		start_lat IS NULL OR 
		start_lng IS NULL OR
		end_lat IS NULL OR
		end_lng IS NULL OR
		member_casual IS NULL;

/*
		Por los resultados obtenidos, confirmamos que la gran parte de datos faltantes son los nombres de estaciones
	o sus ids, tanto para los nombres de estacion inicial c�mo para las finales. Ahora ya que tenemos una tabla
	donde ya almacenamos los nombres de estaciones, sus IDs, as� como el rango en que fluctuan sus coordenadas de
	Latitud y Longitud procederemos a realizar una asignaci�n de datos basados en los valores de sus coordenadas, 
	que un gran porcentaje de los datos contienen.
*/

-- PROCEDIMIENTO DE ASIGNACI�N DE DATOS

	/* 
		1.- La siguiente consulta realiza la asignaci�n del nombre de estaci�n inicial tomando como base los valores
		de Latitud y Longitud iniciales de cada registro, busca el rango al que pertenecen estos registros y asigna 
		el nombre correspondiente, siempre y cuando la celda del nombre de estacion inicial sea NULO.
			A su vez cada consulta de este procedimiento se lanz� como una TRANSACCI�N, ya que s� algunos de los
		valores no era correctos se puede hacer un ROLLBACK y volver a correr la consulta sin afectar la integridad
		de los datos de la tabla.
	*/
		BEGIN TRANSACTION
			UPDATE [dbo].[Dim_Trips_2022]
			SET 
				start_station_name = Estaciones.start_station_name
			FROM 
				[dbo].[Dim_Trips_2022]
			INNER JOIN 
				Estaciones 
				ON 
				[dbo].[Dim_Trips_2022].start_lat >= Estaciones.latitud_minima AND 
				[dbo].[Dim_Trips_2022].start_lat <= Estaciones.Latitud_maxima AND 
				[dbo].[Dim_Trips_2022].start_lng >= Estaciones.Longitud_minima AND 
				[dbo].[Dim_Trips_2022].start_lng <= Estaciones.Longitud_maxima
			WHERE 
				[dbo].[Dim_Trips_2022].start_station_name IS NULL;

		
		COMMIT TRANSACTION;
		ROLLBACK TRANSACTION;

	/*
		2.- La siguiente consulta realiza la asignaci�n del ID de la estaci�n inicial tomando como base los valores
		de Longitud y Latitud iniciales de cada registro, y su nombre de estaci�n. Este proceso s�lo se aplica a 
		aquellas celdas que tengan valores NULOS en el campo de start_station_id:
		
	*/

		BEGIN TRANSACTION
			UPDATE [dbo].[Dim_Trips_2022]
			SET 
				start_station_id = Estaciones.start_station_id 
			FROM 
				[dbo].[Dim_Trips_2022]
			INNER JOIN 
				Estaciones 
			ON 
				[dbo].[Dim_Trips_2022].start_lat >= Estaciones.latitud_minima AND 
				[dbo].[Dim_Trips_2022].start_lat <= Estaciones.Latitud_maxima AND 
				[dbo].[Dim_Trips_2022].start_lng >= Estaciones.Longitud_minima AND 
				[dbo].[Dim_Trips_2022].start_lng <= Estaciones.Longitud_maxima AND
				[dbo].[Dim_Trips_2022].start_station_name = Estaciones.start_station_name
			WHERE
				[dbo].[Dim_Trips_2022].start_station_id IS NULL; 

		
		COMMIT TRANSACTION;
		ROLLBACK TRANSACTION;

	/*
		3.- La siguiente consulta realiza la asignaci�n de los nombres de las estaciones finales, tomando como base
		los valores de Latitud y Longitud finales, busca el rango al que pertenecen estos registros y asigna el 
		nombre correspondiente, siempre y cuando la celda del nombre de estacion final sea NULO:
	*/

		BEGIN TRANSACTION
			UPDATE [dbo].[Dim_Trips_2022]
			SET 
				end_station_name = Estaciones.start_station_name
			FROM 
				[dbo].[Dim_Trips_2022]
			INNER JOIN 
				Estaciones 
				ON 
				[dbo].[Dim_Trips_2022].end_lat >= Estaciones.latitud_minima AND 
				[dbo].[Dim_Trips_2022].end_lat <= Estaciones.Latitud_maxima AND 
				[dbo].[Dim_Trips_2022].end_lng >= Estaciones.Longitud_minima AND 
				[dbo].[Dim_Trips_2022].end_lng <= Estaciones.Longitud_maxima
			WHERE
				[dbo].[Dim_Trips_2022].end_station_name IS NULL;

		COMMIT TRANSACTION;
		ROLLBACK TRANSACTION;

	/*
		4.- Para finalizar el procedimiento, la siguiente consulta asigna el ID de estaci�n final tomando como base
		los valores de Latitud y Longitud final de cada registro as� como el nombre de la estaci�n final, busca el 
		rango de valores al que pertenece y asigna el ID correpondiente:
	*/

		BEGIN TRANSACTION
			UPDATE [dbo].[Dim_Trips_2022]
			SET 
				end_station_id = Estaciones.start_station_id 
			FROM 
				[dbo].[Dim_Trips_2022]
			INNER JOIN 
				Estaciones 
			ON 
				[dbo].[Dim_Trips_2022].end_lat >= Estaciones.latitud_minima AND   
				[dbo].[Dim_Trips_2022].end_lat <= Estaciones.Latitud_maxima AND 
				[dbo].[Dim_Trips_2022].end_lng >= Estaciones.Longitud_minima AND 
				[dbo].[Dim_Trips_2022].end_lng <= Estaciones.Longitud_maxima AND
				[dbo].[Dim_Trips_2022].end_station_name = Estaciones.start_station_name
			WHERE
				[dbo].[Dim_Trips_2022].end_station_id IS NULL; 

		COMMIT TRANSACTION;
		ROLLBACK TRANSACTION;

/*
	Una vez termiando el procedimiento de asignaci�n de datos, el siguiente paso es validar que todos los datos
	haya sido asignados con la consulta que se utiliz� antes del procedimiento:
*/

	SELECT 
		*
	FROM
		[dbo].[Dim_Trips_2022]
	WHERE
		ride_id IS NULL OR 
		rideable_type IS NULL OR
		started_at IS NULL OR
		ended_at IS NULL OR
		start_station_name IS NULL OR
		start_station_id IS NULL OR
		end_station_name IS NULL OR
		end_station_id IS NULL OR
		start_lat IS NULL OR 
		start_lng IS NULL OR
		end_lat IS NULL OR
		end_lng IS NULL OR
		member_casual IS NULL;

/*
	Con base en los resultados podemos observar que el cambio fue significativo, ya que la tabla resultante trae 2 tipos
	de registros: unos son registros que no tienen sus valores de latitud y longitud completos por lo que no se les 
	asignaron valores y como no es posible obteneros, estos resgitros ser�n eliminados. El siguiente tipo de registros 
	son aquellos que s� contienen datos de Latitud y Longitud, pero que no aparecen como coordenadas asignadas a 
	ninguna estaci�n, por lo que no es posible asignarles una arbitrariamente y tambi�n ser�n eliminados con la 
	siguiente consulta:
*/

	BEGIN TRANSACTION
		DELETE FROM 
			[dbo].[Dim_Trips_2022]
		WHERE
			ride_id IS NULL OR 
			rideable_type IS NULL OR
			started_at IS NULL OR
			ended_at IS NULL OR
			start_station_name IS NULL OR
			start_station_id IS NULL OR
			end_station_name IS NULL OR
			end_station_id IS NULL OR
			start_lat IS NULL OR 
			start_lng IS NULL OR
			end_lat IS NULL OR
			end_lng IS NULL OR
			member_casual IS NULL;

	COMMIT TRANSACTION;
	ROLLBACK TRANSACTION;

/*
		Con esto terminamos con el tratamiento estructural de este conjunto de datos perteneciente a los viajes 
	realizados en 2022 por la empresa Cyclistic.

	Los siguientes a esta limpieza se encontrar�n en el Reporte_Cyclistic_2022, tanto para archivo PDF, como en
	documento de lenguaje R.
*/
