# Prompts usados para el desarrollo de la water station

## Prompt 1:
Según el enunciado de WaterManagement, explica los estados que puede tener una estación de riego y qué eventos pueden provocar una transición entre ellos. Distingue entre estar disponible, regando, presentar una fuga, estar fuera de servicio y estar desconectada.


## Prompt 2:
Nuestra estación debe registrarse en CENTRAL enviando su identificador y ubicación. Ayúdame a entender qué debería ocurrir durante el arranque, qué respuesta debe esperar y cómo actuar si CENTRAL todavía no está disponible.


## Prompt 3:
La estación debe informar del caudal en litros por minuto y del volumen acumulado durante el riego. Explícame cómo calcular el volumen acumulado a partir del caudal y del tiempo transcurrido, teniendo en cuenta que el proceso puede ejecutarse periódicamente. Incluye un ejemplo numérico y señala los errores que podrian producirse.

## Prompt 4:
Cuando una estación cambia de estado CENTRAL necesita recibir esa información para actualizar la monitorización. Ayúdame a definir qué información mínima debería comunicar la estación y cómo evitar enviar mensajes redundantes en cada iteración del bucle.

## Prompt 5:
Diseña una lista de pruebas manuales para comprobar una estación de riego de forma aislada: registro, inicio y parada del riego, actualización de caudal y volumen, detección de fuga y recuperación del servicio. Para cada prueba indica el estado inicial, la acción y el resultado esperado.