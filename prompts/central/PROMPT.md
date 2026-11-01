# Prompts usados para el desarrollo de la central

## Prompt 1:
A partir del enunciado de WaterManagement, resume las responsabilidades que debe asumir CENTRAL y distingue cuáles corresponden a la gestión de las estaciones, cuáles a la autorización de solicitudes de riego y cuáles a la monitorización.


## Prompt 2:
En nuestra práctica, CENTRAL debe comunicarse con estaciones de riego y aplicaciones Field Operator. Ayúdame a identificar qué tipos de mensajes necesita intercambiar con cada componente según el enunciado. Explica qué información debería contener cada mensaje y qué respuestas serían necesarias.


## Prompt 3:
Queremos revisar la lógica de autorización de una petición de riego recibida desde Field Operator. Según los requisitos, CENTRAL debe comprobar que la estación está disponible antes de autorizarla. Ayúdame a plantear las condiciones que deben verificarse y los casos en los que se debe denegar la solicitud.


## Prompt 5:
CENTRAL recibe mensajes de varias estaciones y solicitudes de operarios mientras actualiza el panel de monitorización. Explícame qué problemas de concurrencia pueden aparecer al modificar el estado compartido de las estaciones y qué mecanismos sencillos de Python permiten protegerlo. Prioriza una solución fácil de mantener.


## Prompt 6:
Voy a compartir una parte de nuestro código de CENTRAL y el error que aparece al recibir mensajes de una estación. Analiza el flujo de ejecución, identifica la causa probable y propón el cambio mínimo necesario. No reescribas el módulo ni cambies el protocolo existente.