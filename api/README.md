# ShopStream API

API REST desplegada en AWS Lambda con Zappa.

## URL Base
https://lz6d3q5skk.execute-api.us-east-1.amazonaws.com/production/health 

## Endpoints

### GET /health
Verifica que la API está funcionando.

### GET /pages/top
Retorna top páginas por métrica.
- metric: time_on_page o bounce_rate
- date: fecha YYYY-MM-DD (requerido)
- limit: número de resultados (default 10)

### GET /sessions/summary
Retorna resumen de sesiones por dispositivo y país.
- date: fecha YYYY-MM-DD (requerido)
- country: código de país (opcional)
- device: tipo de dispositivo (opcional)

### GET /anomalies
Retorna sesiones anómalas detectadas.
- date: fecha YYYY-MM-DD (requerido)

## Tests
13 tests passed