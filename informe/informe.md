# Metrics && Alert Server

## Introducción

*Metrics && Alert Server* es un sistema de consulta de métricas y generación de alertas distribuido. El mismo está compuesto por 3 aplicaciones que se comunican vía socket:

* Server: Recibe conexiones de clientes y recibe mensajes de agregar métricas o consultas

* Metric Pusher: se conecta al servidor y envía mensajes de inserción de métricas

* Alert client: se conecta al servidor y envía consultas periódicas de las alertas configuradas


## Descripción de los componentes

![Diagrama de robustez](robustez.png){width=80%}

Ambas aplicaciones cliente se conectan a un load balancer que distribuye las conexiones entre los distintos hilos manejadores del servidor. El load balancer actual envía sockets a un pool de threads. Este balancer será reemplazado por un load balancer (como `haproxy`) que me permita repartir las conexiones entre procesos independientes.


### Conexiones

Las conexiones son atendidas por instancias de ConnectionHandler dentro del pool. Los sockets con las conexiones son encolados en un set de **blocking queues** que me permite comunicar el hilo del balancer con este pool de handlers.
En el pool de handlers se realiza la lectura del mensaje enviado por socket.

Las conexiones se encolan usando round robin. Cuando son encoladas, se les asocia un timestamp. Cuando son desencoladas, si el timestamp asociado es muy antiguo, se escribe "Error" por el socket y se cierra la conexión para tomar otro socket encolado.

Cada handler tiene un set de senders que utiliza para comunicarse con un pool de threads para escribir metricas, y otro pool de threads para consultas. Estos canales son **single consumer - multiple producer**.

### Procesamiento de los mensajes

Cuando un mensaje es recibido en un connection handler, se procesa para saber si es una consulta o un agregado de métrica. En ambos casos es encolado en uno de los canales que comunica al handler on el Metric Writer o Query Handler según corresponda. De estos dos objetos también hay un pool con múltiples instancias, y los mensajes son enviados según **afinidad** (utilizando un módulo sobre el hash del metricId).

**Nota** Esta afinidad puede ser una desventaja si la función de hash es mala o recibo una cantidad no balanceada de métricas

**Problema detectado**: si las alertas van al mismo load balancer que los clientes, existe la posibilidad de descartar alertas. ¿Deberían ir en un puerto con mayor prioridad?


### Metric writer

Cada *metric writer* es una instancia independiente de las demas. Dado que los mensajes que reciben son por afinidad, cada writer escribe en un archivo independiente de los otros writers.

Cada archivo que se escribe corresponde a `<id_writer>.metrics.bin`. El `<id_writer>` corresponde al `hash() / mod` de los mensajes recibidos. Los archivos se escriben en orden FIFO, en un formato binario. Al escribir una métrica se le asocia el timestamp del momento en que se escribe.
 
**Importante**: esto implica que las consultas se van a hacer con los timestamps correspondientes a las escrituras a disco y no con los de envío del cliente. De esta manera nos facilita las consultas sobre los archivos de métricas

Únicamente el writer puede acceder a ese archivo de métricas. Cada N segundos renombra el archivo `<id_writer>.metrics.bin` a un archivo `<id_writer>.<timestamp>.metrics.bin` y crea un archivo de métricas nuevo.


### Metric reader

Análogo al writer, tengo un pool de readers que consumen consultas de los handlers, manejandose también por afinidad.

Cuando una consulta llega, parsea el rango de tiempo y consume todos los `<id_reader>.<timestamp>.metrics.bin` correspondientes. Notar que no lee los archivos de los metric writers, por lo que no hay necesidad de locks para estos.

**Posibilidad de mejora 1**: En el caso del metric writer sólo puedo tener 1 writer por archivo, pero en este caso puedo tener más de 1 reader para el mismo archivo, para mejorar el rendimiento del sistema.

Del archivo se extraen todos los registros correspondientes a la métrica buscada.

**Nota**: en cada archivo puede haber más de un metricId, ya que se usan hashes y no el valor del metricId. Esto se debe a que en principio no se cuántos metrics Ids distintos voy a tener ni sobre que file system voy a trabajar, por lo que asumo que usar un archivo por métria (y por timestamp) no es viable

**Posibilidad de mejora 2**: lanzar un proceso "optimizer" que ordene y aglomere estos archivos de métricas generados. En este caso debo agregar un estado compartido entre los procesos que me indique hasta dónde está optimizado, para evitar lockear los archivos

## Protocolo

Las métricas y las consultas se ingresan en formato JSON.

Por socket se envía:
* 1 byte para indicar si es una consulta o un ingreso ('I' para ingresos, 'Q' para consultas)

Para los ingresos

* MetricId con largo preconcatenado (big endian)

* Value como float en big endian

Para las consultas:

* MetricId con largo preconcatenado (big endian)

* Byte indicando si hay timestamps de rango

En caso positivo:

	* Byte 'Y'

	* Dos timestamps que se envían como enteros de 64bits, big endian

En caso negativo, el byte enviado es 'N'

* Byte indicando tipo de agregación: 'a': AVG, 'm': MIN, 'M': Max, 'c': COUNT

* Entero float de 32 bits como ventana de tiempo.


## Despliegue

//TODO