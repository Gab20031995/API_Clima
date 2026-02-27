# 🌦️ Pipeline Multivariable de Datos Climáticos (ETL)
**Curso:** Administración de Bases de Datos  
**Profesor:** Alejandro Zamora  
**Desarrollado por:** Grupo 5  

---

## 📌 Descripción del Proyecto
Este proyecto implementa un pipeline de datos (ETL) robusto para la extracción, procesamiento y almacenamiento de variables meteorológicas multivariables. El flujo está diseñado bajo principios de ingeniería de calidad, incluyendo una capa de **Staging** para persistencia de datos crudos y orquestación mediante **Prefect Cloud**.

## 🏗️ Arquitectura del Pipeline
El sistema se divide en tres fases principales siguiendo el flujo de datos:

1.  **Extract & Staging:** Extracción de datos desde la API de **Open-Meteo** utilizando un cliente con sistema de *cache* y reintentos automáticos (*retry session*). La data cruda se almacena en archivos JSON locales dentro de la carpeta `/staging` antes de ser procesada.
2.  **Transform:** Procesamiento de datos mediante la librería **Pandas**, donde se normalizan unidades, se gestionan marcas de tiempo (timestamps) y se filtran los registros relevantes.
3.  **Load:** Carga de los datos transformados en un **Data Warehouse** basado en **MongoDB Atlas** (NoSQL).



---

## 🛠️ Tecnologías Utilizadas
* **Lenguaje:** Python 3.13+
* **Orquestador:** Prefect Cloud (Orquestación administrada)
* **Base de Datos:** MongoDB Atlas
* **Control de Versiones:** GitHub
* **Librerías Clave:** `pandas`, `pymongo`, `openmeteo-requests`, `requests-cache`, `python-dotenv`.

## 🚀 Configuración del Entorno

### 1. Variables de Entorno
Cree un archivo `.env` en la raíz del proyecto con el siguiente formato:
```env
MONGO_URI="su_conexion_a_mongo_atlas"
DB_NAME="clima_data"
COLLECTION_NAME="clima_data"
