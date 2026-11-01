# Prompts usados para el desarrollo del file operator


## Prompt 1:
A partir del enunciado, describe el flujo que sigue Field Operator cuando solicita activar el riego de una estación. Explica qué responsabilidad tiene FO y cuáles corresponden a CENTRAL y a la estación. Quiero evitar que FO tome decisiones que deberían estar centralizadas.


## Prompt 2:
Nuestra aplicación Field Operator envía solicitudes de riego a CENTRAL y también permite consultar estaciones. Ayúdame a revisar qué datos necesita cada tipo de solicitud y cómo validar los argumentos introducidos por el usuario antes de enviarlos.


## Prompt 3:
Field Operator debe distinguir entre una solicitud autorizada, denegada y una operación que ha finalizado. Explícame cómo organizar el tratamiento de esas respuestas para que se reciba la información clara y no se confundan los mensajes de autorización con los mensajes de progreso del riego.


## Prompt 4:
En nuestra aplicación FO, las notificaciones de CENTRAL pueden llegar mientras el operario está esperando una respuesta a su solicitud. Explícame por qué conviene separar la recepción de mensajes de la interacción por consola y qué problemas aparecerían si ambas tareas se ejecutaran en el mismo flujo.


## Prompt 5:
El enunciado permite automatizar solicitudes de riego mediante un archivo. Ayúdame a definir cómo debería validar cada línea, detectar datos incorrectos y continuar o detener el procesamiento ante un error. Ten en cuenta que el archivo no debe provocar que se envíen solicitudes inválidas a CENTRAL.


## Prompt 6:
Voy a compartir el código de Field Operator y el comportamiento observado al solicitar un riego. Ayúdame a seguir el recorrido del mensaje desde FO hasta CENTRAL y de vuelta, identificando si el problema está en el formato enviado, en la respuesta recibida o en el tratamiento de las notificaciones. Propón pruebas concretas para verificar la corrección sin modificar innecesariamente el código.