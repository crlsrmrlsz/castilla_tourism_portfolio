# castilla_tourism_portfolio
An analysis of tourism data for the capital cities of Castilla La Mancha to demonstrate data processing, analysis, and visualization skills





## External Data Sources

This project uses external data from official sources for validation and credibility checks. The raw files can be downloaded from their original sources using the links below and should be placed in the `/data/raw/ine_data/` directory.

### 1. FRONTUR - International Tourist Arrivals (National INE)
Provides monthly data on international tourist arrivals, used to validate the `Extranjero` visitor counts at the national level.
- **Link:** [INEbase - FRONTUR Statistics](https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736176996&menu=ultiDatos&idp=1254735576863)

### 2. Hotel Occupancy Survey (National INE)
Provides monthly data on travelers (`viajeros`) who stayed in hotels, broken down by Spanish and foreign residents. This is a primary source for validation.
- **Link:** [INEbase - Hotel Occupancy Survey](https://www.ine.es/dynt3/inebase/es/index.htm?padre=238&dh=1)

### 3. Yearly Population Data (National INE Padrón)
Provides official, annual population figures from the Municipal Register. This is the source for yearly resident numbers for specific cities or the entire community.
- **Link:** [INE - Population by Municipality, Sex, and Year](https://www.ine.es/jaxiT3/Tabla.htm?t=2902&L=0)

### 4. Quarterly Population Data (Regional - Castilla-La Mancha)
Provides more frequent (quarterly) population data at the provincial level for Castilla-La Mancha, including data for 2024.
- **Link:** [Estadísticas de Demografía de Castilla-La Mancha](https://estadistica.castillalamancha.es/estadisticas-por-temas/demografia)