-- Workshop 3 - Consultas para los KPIs del dashboard

-- KPI 1: Error promedio de prediccion
-- error_absoluto_promedio = MAE. El error se guarda con signo (predicho - real), por eso se usa ABS.
-- sesgo_promedio: si es cercano a 0, el modelo no tiende a pasarse ni a quedarse corto.
SELECT COUNT(*)                                              AS total_predicciones,
       ROUND(AVG(ABS(prediction_error)), 3)                  AS error_absoluto_promedio,
       ROUND(SQRT(AVG(prediction_error * prediction_error)), 3) AS rmse,
       ROUND(AVG(prediction_error), 3)                       AS sesgo_promedio
FROM fact_predictions;

-- KPI 2: Predicciones por pais
SELECT c.country_name                              AS pais,
       COUNT(*)                                    AS predicciones,
       ROUND(AVG(ABS(f.prediction_error)), 3)      AS error_absoluto_promedio
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name
ORDER BY predicciones DESC, pais;

-- KPI 3: Puntaje predicho vs real (una fila por prediccion)
SELECT f.prediction_id,
       c.country_name        AS pais,
       f.observation_year    AS anio,
       f.actual_score        AS puntaje_real,
       f.predicted_score     AS puntaje_predicho,
       f.prediction_error    AS error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
ORDER BY f.prediction_id;

-- KPI 4: Tendencia de las predicciones en el tiempo (por anio de la observacion)
SELECT observation_year                            AS anio,
       COUNT(*)                                    AS predicciones,
       ROUND(AVG(actual_score), 3)                 AS puntaje_real_promedio,
       ROUND(AVG(predicted_score), 3)              AS puntaje_predicho_promedio,
       ROUND(AVG(ABS(prediction_error)), 3)        AS error_absoluto_promedio
FROM fact_predictions
GROUP BY observation_year
ORDER BY observation_year;

-- Extra: Estado de los eventos recibidos (confiabilidad del pipeline)
SELECT processing_status AS estado,
       COUNT(*)          AS eventos
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY estado;