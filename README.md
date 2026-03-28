# Gossip Protocol - P2P Message Broker

Este proyecto implementa un protocolo Gossip distribuido desde cero en Go, siguiendo criterios de **Staff Engineer** para sistemas distribuidos de alta resiliencia.

## Arquitectura Staff-Level

- **Sin Líder Central**: El sistema es totalmente descentralizado, eliminando el Single Point of Failure (SPOF). Cada nodo es un igual que se descubre y comunica de forma autónoma.
- **Protocolo Gossip (Infection-style)**: Utiliza propagación aleatoria de mensajes sobre UDP. Cada segundo, los nodos eligen 3 vecinos al azar para sincronizar su estado.
- **Networking UDP Raw**: Implementación manual de transporte sobre UDP para maximizar el rendimiento y minimizar el overhead de conexión.
- **Membresía Dinámica**: Los nodos mantienen una lista de membresía basada en heartbeats, permitiendo el descubrimiento automático de nuevos nodos y la detección de fallos.
- **Convergencia Eventual con CRDTs**: Implementa un **G-Counter** (Grow-only Counter) que permite actualizaciones concurrentes en múltiples nodos sin coordinación, garantizando que todos convergerán al mismo valor matemático.
- **Ordenamiento Causal con Vector Clocks**: Cada nodo rastrea la causalidad de los eventos usando Vector Clocks para entender qué eventos ocurrieron antes, después o de forma concurrente, esencial para la consistencia en sistemas sin reloj global sincronizado.

## Estructura del Proyecto

- `pkg/network`: Abstracción de transporte UDP.
- `pkg/membership`: Gestión de la lista de nodos y heartbeats.
- `pkg/data`: Estructuras de datos distribuidas (Vector Clocks, CRDTs).
- `pkg/gossip`: Lógica del protocolo de propagación y sincronización.

## Cómo Ejecutar

Para lanzar un cluster de 3 nodos localmente:

1. **Nodo 1**:

   ```bash
   go run main.go -id node1 -port 8001
   ```

2. **Nodo 2** (apuntando al nodo 1 para entrar al cluster):

   ```bash
   go run main.go -id node2 -port 8002 -peer 127.0.0.1:8001
   ```

3. **Nodo 3** (apuntando al nodo 2):

   ```bash
   go run main.go -id node3 -port 8003 -peer 127.0.0.1:8002
   ```

## Verificación de Resiliencia

El sistema puede soportar la caída de nodos y la partición de la red. Una vez que la conectividad se restaura, el protocolo Gossip propagará los cambios faltantes y el estado de los datos convergerá automáticamente gracias a las propiedades de idempotencia y conmutatividad del CRDT.
